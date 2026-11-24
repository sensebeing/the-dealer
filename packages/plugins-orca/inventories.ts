import { addAbortListener } from 'node:events';
import { realpathSync } from 'node:fs';
import { resolve } from 'node:path';
import { checkedCommand, CommandNotStartedError, runCommand } from '../subprocess/index.ts';
import type { CommandRequest, CommandRunner } from '../subprocess/index.ts';

const MAX_PROBE_ENTRIES = 128;

interface InventoryProbe {
  command: string;
  workspacePath?: string;
  stdout: Promise<string>;
  expiresAt?: number;
}

const probes = new Map<string, InventoryProbe>();

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
): Promise<string> {
  const run = (request: CommandRequest) => checkedCommand(runner, request).then(result => result.stdout);
  if (!advisory || runner !== runCommand) return run(request);

  const { command, signal } = request;
  if (signal?.aborted) return Promise.reject(new CommandNotStartedError(signal.reason));
  const now = Date.now();
  const key = JSON.stringify([command, workspacePath]);
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
  const entry: InventoryProbe = { command, workspacePath, stdout: run({ ...request, signal: undefined }) };
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
  runner: CommandRunner, command: string, workspacePath: string, cwd: string, signal?: AbortSignal, advisory = false,
): Promise<string> {
  return readInventory(runner, {
    command, args: ['terminal', 'list', '--worktree', `path:${workspacePath}`, '--json'], cwd, signal,
  }, 1_000, advisory ? workspaceKey(workspacePath) : undefined, advisory);
}

/** Drop inventory before a write begins and after it settles, including ambiguous failures. */
export function orcaMutationRunner(runner: CommandRunner, workspacePath?: string): CommandRunner {
  if (runner !== runCommand) return runner;
  const workspace = workspacePath === undefined ? undefined : workspaceKey(workspacePath);
  return async request => {
    const invalidate = () => {
      for (const [key, entry] of probes) {
        if (entry.command === request.command &&
          (entry.workspacePath === undefined || workspace === undefined || entry.workspacePath === workspace)) probes.delete(key);
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
