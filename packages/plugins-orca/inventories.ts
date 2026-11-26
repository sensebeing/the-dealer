import { addAbortListener } from 'node:events';
import { realpathSync } from 'node:fs';
import { resolve } from 'node:path';
import { checkedCommand, CommandNotStartedError, runCommand } from '../subprocess/index.ts';
import type { CommandRequest, CommandRunner } from '../subprocess/index.ts';

const MAX_PROBE_ENTRIES = 128;
const MAX_SELECTOR_AMBIGUOUS_MESSAGE_LENGTH = 200;

interface InventoryProbe {
  command: string;
  workspacePath?: string;
  selector?: string;
  stdout: Promise<string>;
  expiresAt?: number;
}

const probes = new Map<string, InventoryProbe>();

/** Single owner of Orca `--worktree` selector formatting across probes and mutations. */
export function buildOrcaWorktreeSelector(workspacePath: string, worktreeId?: string): string {
  const trimmedId = worktreeId?.trim();
  if (trimmedId) {
    if (/^(?:id|folder|identity):/.test(trimmedId)) {
      return trimmedId;
    }
    return `id:${trimmedId}`;
  }
  return `path:${workspacePath}`;
}

export class OrcaWorktreeSelectorAmbiguousError extends Error {
  readonly selector: string;

  constructor(selector: string, message?: string, options?: { cause?: unknown }) {
    const raw = (message ?? `Orca worktree selector is ambiguous (${selector}); bind an explicit worktreeId`)
      .replace(/\s+/g, ' ')
      .trim();
    const bounded = raw.length <= MAX_SELECTOR_AMBIGUOUS_MESSAGE_LENGTH
      ? raw
      : `${raw.slice(0, MAX_SELECTOR_AMBIGUOUS_MESSAGE_LENGTH - 3)}...`;
    super(bounded, options);
    this.name = 'OrcaWorktreeSelectorAmbiguousError';
    this.selector = selector;
  }
}

function isSelectorAmbiguousPayload(parsed: unknown): boolean {
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return false;
  }
  const record = parsed as { ok?: unknown; error?: unknown };
  if (record.ok !== false || record.error === null || typeof record.error !== 'object' || Array.isArray(record.error)) {
    return false;
  }
  return (record.error as { code?: unknown }).code === 'selector_ambiguous';
}

function tryParseJsonPayload(text: string): unknown {
  const trimmed = text.trim();
  if (!trimmed) return undefined;
  try {
    return JSON.parse(trimmed);
  } catch {
    const start = trimmed.indexOf('{');
    if (start <= 0) return undefined;
    try {
      return JSON.parse(trimmed.slice(start));
    } catch {
      return undefined;
    }
  }
}

/** Detect structured Orca `selector_ambiguous` failures and wrap into a bounded single-line error. */
export function parseOrcaSelectorAmbiguousError(
  error: unknown,
  selector: string,
): OrcaWorktreeSelectorAmbiguousError | undefined {
  if (error instanceof OrcaWorktreeSelectorAmbiguousError) {
    return error;
  }
  const candidates: string[] = [];
  if (typeof error === 'string') {
    candidates.push(error);
  } else if (error !== null && typeof error === 'object') {
    const record = error as { stdout?: unknown; stderr?: unknown; message?: unknown };
    if (typeof record.stdout === 'string') candidates.push(record.stdout);
    if (typeof record.stderr === 'string') candidates.push(record.stderr);
    if (typeof record.message === 'string') candidates.push(record.message);
  }
  for (const candidate of candidates) {
    if (isSelectorAmbiguousPayload(tryParseJsonPayload(candidate))) {
      return new OrcaWorktreeSelectorAmbiguousError(selector, undefined, { cause: error });
    }
  }
  return undefined;
}

function workspaceKey(workspacePath: string): string {
  try {
    return realpathSync(workspacePath);
  } catch {
    return resolve(workspacePath);
  }
}

async function waitForProbe(stdout: Promise<string>, signal?: AbortSignal): Promise<string> {
  if (!signal) return stdout;
  let listener: ReturnType<typeof addAbortListener> | undefined;
  try {
    return await Promise.race([
      stdout,
      new Promise<never>((_, reject) => {
        listener = addAbortListener(signal, () => reject(signal.reason));
      }),
    ]);
  } finally {
    listener?.[Symbol.dispose]();
  }
}

function readInventory(
  runner: CommandRunner,
  request: CommandRequest,
  ttlMs: number,
  workspacePath?: string,
  advisory = false,
  selector?: string,
): Promise<string> {
  const run = (request: CommandRequest) => checkedCommand(runner, request).then(result => result.stdout);
  if (!advisory || runner !== runCommand) return run(request);

  const { command, signal } = request;
  if (signal?.aborted) return Promise.reject(new CommandNotStartedError(signal.reason));
  const now = Date.now();
  const key = JSON.stringify([command, workspacePath, selector]);
  for (const [key, entry] of probes) {
    if (entry.expiresAt !== undefined && entry.expiresAt <= now) probes.delete(key);
  }
  const cached = probes.get(key);
  if (cached) return waitForProbe(cached.stdout, signal);

  if (probes.size >= MAX_PROBE_ENTRIES) {
    const evictable = [...probes].find(([, entry]) => entry.expiresAt !== undefined);
    if (!evictable) return run(request);
    probes.delete(evictable[0]);
  }

  // A caller can cancel its wait without aborting the read for other slots.
  const entry: InventoryProbe = { command, workspacePath, selector, stdout: run({ ...request, signal: undefined }) };
  probes.set(key, entry);
  void entry.stdout.then(() => {
    entry.expiresAt = Date.now() + ttlMs;
  }, () => {
    if (probes.get(key) === entry) probes.delete(key);
  });
  return waitForProbe(entry.stdout, signal);
}

export function readOrcaWorktreePs(runner: CommandRunner, command: string, cwd?: string, signal?: AbortSignal, advisory = false): Promise<string> {
  return readInventory(runner, {
    command, args: ['worktree', 'ps', '--agent-wait', '--limit', '10000', '--json'], cwd, signal,
  }, 1_500, undefined, advisory);
}

export function readOrcaTerminalList(
  runner: CommandRunner,
  command: string,
  workspacePath: string,
  cwd: string,
  signal?: AbortSignal,
  advisory = false,
  worktreeId?: string,
): Promise<string> {
  const selector = buildOrcaWorktreeSelector(workspacePath, worktreeId);
  const cacheWorkspace = advisory ? workspaceKey(workspacePath) : undefined;
  const cacheSelector = advisory ? (worktreeId?.trim() ? selector : `path:${cacheWorkspace}`) : undefined;
  return readInventory(
    runner,
    {
      command,
      args: ['terminal', 'list', '--worktree', selector, '--json'],
      cwd,
      signal,
    },
    1_000,
    cacheWorkspace,
    advisory,
    cacheSelector,
  ).then(
    stdout => {
      const ambiguous = parseOrcaSelectorAmbiguousError(stdout, selector);
      if (ambiguous) throw ambiguous;
      return stdout;
    },
    error => {
      throw parseOrcaSelectorAmbiguousError(error, selector) ?? error;
    },
  );
}

/** Drop inventory before a write begins and after it settles, including ambiguous failures. */
export function orcaMutationRunner(runner: CommandRunner, workspacePath?: string, worktreeId?: string): CommandRunner {
  if (runner !== runCommand) return runner;
  const workspace = workspacePath === undefined ? undefined : workspaceKey(workspacePath);
  const selector = worktreeId?.trim() ? buildOrcaWorktreeSelector(workspacePath ?? '', worktreeId) : undefined;
  return async request => {
    const invalidate = () => {
      for (const [key, entry] of probes) {
        if (entry.command !== request.command) continue;
        const matches =
          entry.workspacePath === undefined ||
          (workspace === undefined && selector === undefined) ||
          (selector !== undefined
            ? entry.selector === selector || (workspace !== undefined && entry.workspacePath === workspace && Boolean(entry.selector?.startsWith('path:')))
            : workspace !== undefined && entry.workspacePath === workspace);
        if (matches) probes.delete(key);
      }
    };
    invalidate();
    try {
      return await runner(request);
    } finally {
      invalidate();
    }
  };
}
