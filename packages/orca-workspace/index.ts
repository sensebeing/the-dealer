import { existsSync, mkdirSync, readFileSync, realpathSync, renameSync, writeFileSync, openSync, closeSync, fsyncSync, rmSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { basename, dirname, join, resolve } from 'node:path';
import { homedir, tmpdir } from 'node:os';
import { spawn } from 'node:child_process';

/** A single external command invocation; always executed with shell: false. */
export interface CommandRequest {
  command: string;
  args: readonly string[];
  cwd?: string;
  input?: string;
  signal?: AbortSignal;
}

export type CommandRunner = (
  request: CommandRequest,
) => Promise<{ stdout: string; stderr?: string; exitCode?: number }>;

/** Only the process boundary may prove that no external process was started. */
export class CommandNotStartedError extends Error {
  constructor(cause: unknown) {
    super(`Command did not start: ${String(cause)}`, { cause });
    this.name = 'CommandNotStartedError';
  }
}

export const runCommand: CommandRunner = request =>
  new Promise((resolve, reject) => {
    if (request.signal?.aborted) {
      reject(new CommandNotStartedError(request.signal.reason));
      return;
    }
    let child;
    try {
      child = spawn(request.command, [...request.args], {
        cwd: request.cwd,
        signal: request.signal,
        stdio: ['pipe', 'pipe', 'pipe'],
        shell: false,
      });
    } catch (error) {
      reject(new CommandNotStartedError(error));
      return;
    }
    let started = false;
    child.once('spawn', () => { started = true; });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', chunk => {
      stdout += chunk;
    });
    child.stderr.on('data', chunk => {
      stderr += chunk;
    });
    child.on('error', error => reject(
      started || child.pid !== undefined ? error : new CommandNotStartedError(error),
    ));
    child.on('close', code =>
      code === 0
        ? resolve({ stdout, stderr, exitCode: code })
        : reject(new Error(`${request.command} exited ${code}: ${(stderr || stdout).trim()}`)),
    );
    child.stdin.on('error', reject);
    child.stdin.end(request.input);
  });

export async function checkedCommand(runner: CommandRunner, request: CommandRequest) {
  const result = await runner(request);
  if (result.exitCode !== undefined && result.exitCode !== 0) {
    throw new Error(
      `${request.command} exited ${result.exitCode}: ${(result.stderr || result.stdout).trim()}`,
    );
  }
  return result;
}

export interface OrcaIdleResult {
  idle: boolean;
  composerPrefilled?: boolean;
  reason?: string;
}

export interface OrcaTerminalRow {
  handle: string;
  connected: boolean;
  writable?: boolean;
  orphaned?: boolean;
  preview?: unknown;
  agentIdentity?: unknown;
  lastOutputAt?: unknown;
}

/** Collapse arbitrary text into exactly one line; wakeup pointers must never span lines. */
export function singleLine(value: string): string {
  return value.replace(/\s+/g, ' ').trim();
}

/** Recognize an Autopilot recommendation copied into the terminal composer. */
export function isAutopilotComposerPrefill(preview: string): boolean {
  const lines = preview.split(/\r?\n/);
  return lines.some((line, index) => {
    const draft = /^\s*>\s+(\S.*)$/.exec(line)?.[1]?.trim();
    return draft !== undefined && lines.slice(0, index).some(earlier =>
      /下一步建议|,1（主推）|,1 \(主推\)|自动预填/.test(earlier) || earlier.includes(draft),
    );
  });
}

/**
 * Authoritative recursive idle inspection: every workspace in the root's subtree must report no
 * unverifiable terminals, every agent must be done, and every connected writable terminal must be
 * free of an unsent composer draft. Synthetic inventories are confirmed with `terminal wait`.
 */
export async function checkOrcaWorkspaceIdle(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
): Promise<OrcaIdleResult> {
  const output = await checkedCommand(runner, {
    command,
    args: ['worktree', 'ps', '--agent-wait', '--limit', '10000', '--json'],
    cwd,
    signal,
  });
  const raw = JSON.parse(output.stdout);
  const inventory = raw.result;
  if (raw.ok !== true || !inventory || inventory.truncated || !Array.isArray(inventory.worktrees)) {
    return { idle: false, reason: 'Incomplete workspace inventory' };
  }
  type Workspace = {
    path: string;
    worktreeId: string;
    childWorktreeIds?: string[];
    unverifiableTerminalCount?: number;
    agents?: { state?: string }[];
  };
  const rows: Workspace[] = inventory.worktrees;
  const root = rows.find(row => row.path === cwd);
  if (!root) {
    return { idle: false, reason: 'Workspace not found' };
  }
  let composerPrefilled = false;
  const visited = new Set<string>();
  const idleTree = async (row: Workspace): Promise<boolean> => {
    if (visited.has(row.worktreeId)) {
      return false;
    }
    visited.add(row.worktreeId);
    if (
      row.unverifiableTerminalCount !== 0 ||
      !row.agents ||
      row.agents.some(agent => agent.state !== 'done')
    ) {
      return false;
    }
    const listed = await checkedCommand(runner, {
      command,
      args: ['terminal', 'list', '--worktree', `path:${row.path}`, '--json'],
      cwd,
      signal,
    });
    const terminals = JSON.parse(listed.stdout);
    if (
      terminals.ok !== true ||
      terminals.result?.truncated ||
      !Array.isArray(terminals.result?.terminals)
    ) {
      return false;
    }
    const handles: OrcaTerminalRow[] = terminals.result.terminals;
    // A chat-only or otherwise unobservable agent needs an injected authoritative idleCheck.
    const observableHandles = handles.filter(t => t.orphaned !== true || t.connected);
    if (observableHandles.length < row.agents.length) {
      return false;
    }
    for (const terminal of observableHandles) {
      if (!terminal.connected || !terminal.writable) {
        return false;
      }
      const realOrcaTerminal =
        typeof terminal.preview === 'string' || typeof terminal.agentIdentity === 'string';
      if (!realOrcaTerminal) {
        // Synthetic or older inventory without TUI metadata: ask Orca directly.
        try {
          const waited = await checkedCommand(runner, {
            command,
            args: [
              'terminal',
              'wait',
              '--terminal',
              terminal.handle,
              '--for',
              'tui-idle',
              '--timeout-ms',
              '1',
              '--json',
            ],
            cwd,
            signal,
          });
          if (JSON.parse(waited.stdout).ok !== true) {
            return false;
          }
        } catch {
          return false;
        }
        continue;
      }
      if (typeof terminal.preview === 'string') {
        // Autopilot recommendations may prefill the composer below an idle agent.
        const lastLine = terminal.preview
          .split(/\r?\n/)
          .filter(line => line.trim())
          .at(-1);
        if (isAutopilotComposerPrefill(terminal.preview)) {
          if (row.path === cwd) {
            composerPrefilled = true;
          }
          continue;
        }
        if (lastLine?.trimStart().startsWith('> ')) return false;
      }
    }
    for (const id of row.childWorktreeIds ?? []) {
      const child = rows.find(item => item.worktreeId === id);
      if (!child || !(await idleTree(child))) {
        return false;
      }
    }
    return true;
  };
  const idle = await idleTree(root);
  return { idle, ...(idle && composerPrefilled ? { composerPrefilled: true } : {}), reason: idle ? undefined : 'Workspace or descendant is busy or unverifiable' };
}

/** Bounded `terminal wait --for tui-idle`; timeout, busy, or command failure is never idle. */
export async function waitOrcaTerminalIdle(
  runner: CommandRunner,
  command: string,
  terminalHandle: string,
  cwd?: string,
  timeoutMs = 5000,
  signal?: AbortSignal,
): Promise<OrcaIdleResult> {
  try {
    const waited = await checkedCommand(runner, {
      command,
      args: [
        'terminal',
        'wait',
        '--terminal',
        terminalHandle,
        '--for',
        'tui-idle',
        '--timeout-ms',
        String(timeoutMs),
        '--json',
      ],
      cwd,
      signal,
    });
    if (JSON.parse(waited.stdout).ok !== true) {
      return { idle: false, reason: 'Terminal is busy' };
    }
    return { idle: true };
  } catch (error) {
    return { idle: false, reason: `Terminal idle wait failed: ${String(error)}` };
  }
}

/** Probe one terminal without injecting anything; used when no workspace path is known. */
export async function probeOrcaTerminalIdle(
  runner: CommandRunner,
  command: string,
  terminalHandle: string,
  cwd?: string,
  signal?: AbortSignal,
): Promise<OrcaIdleResult> {
  const result = await waitOrcaTerminalIdle(runner, command, terminalHandle, cwd, 1, signal);
  return result.idle
    ? result
    : { idle: false, reason: `Terminal idle probe failed: ${result.reason ?? 'Terminal is busy'}` };
}

/** Shared interactive Antigravity launcher for fixed and task workspaces. */
export function defaultAntigravityLaunchCommand(): string {
  const magpie = join(homedir(), '.local/bin/magpie');
  return existsSync(magpie)
    ? `'${magpie.replace(/'/g, "'\\''")}' agy run --dangerously-skip-permissions`
    : 'agy --dangerously-skip-permissions';
}

export type SupportedOrcaAgent = 'antigravity' | 'claude' | 'codex';

export function defaultAgentLaunchCommand(agent: SupportedOrcaAgent = 'antigravity'): string {
  if (agent === 'claude') return 'claude --dangerously-skip-permissions';
  if (agent === 'codex') return 'codex --dangerously-bypass-approvals-and-sandbox';
  return defaultAntigravityLaunchCommand();
}

/** Resolve the freshest connected target-agent terminal in an Orca workspace, spawning one if needed. */
export async function resolveOrSpawnOrcaWorkspaceTerminal(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
  options?: { agent?: SupportedOrcaAgent; interactiveCommand?: string },
): Promise<{ handle: string; spawned: boolean; shellBootstrapped?: boolean }> {
  const targetAgent = options?.agent ?? 'antigravity';
  const launchCommand = options?.interactiveCommand?.trim() || defaultAgentLaunchCommand(targetAgent);
  const listed = await checkedCommand(runner, {
    command,
    args: ['terminal', 'list', '--worktree', `path:${cwd}`, '--json'],
    cwd,
    signal,
  });
  const inventory = JSON.parse(listed.stdout);
  if (inventory.ok !== true || inventory.result?.truncated || !Array.isArray(inventory.result?.terminals)) {
    throw new Error('Incomplete Orca terminal inventory');
  }
  const terminals = inventory.result.terminals as OrcaTerminalRow[];
  const nonOrphanConnected = terminals.filter(
    t => t && t.orphaned !== true && t.connected && t.writable !== false,
  );
  const connected = nonOrphanConnected.length > 0
    ? nonOrphanConnected
    : terminals.filter(
      t => t && t.connected && t.writable !== false &&
        typeof t.agentIdentity === 'string' && t.agentIdentity.trim().length > 0,
    );
  const freshest = (rows: OrcaTerminalRow[]) => rows.sort(
    (a, b) => (Number(b.lastOutputAt) || 0) - (Number(a.lastOutputAt) || 0),
  )[0];
  const preferred = freshest(connected.filter(t => t.agentIdentity === targetAgent || (targetAgent === 'claude' && t.agentIdentity === 'claude-code')));
  if (preferred) return { handle: String(preferred.handle), spawned: false };
  const agent = freshest(connected.filter(t => typeof t.agentIdentity === 'string' && t.agentIdentity.trim()));
  if (agent) return { handle: String(agent.handle), spawned: false };
  const synthetic = freshest(connected.filter(t => typeof t.preview !== 'string' && typeof t.agentIdentity !== 'string'));
  if (synthetic) return { handle: String(synthetic.handle), spawned: false };
  const bareShell = freshest(connected.filter(t => typeof t.preview === 'string' && !t.agentIdentity));
  if (bareShell) {
    const handle = String(bareShell.handle);
    await sendOrcaTerminalText(runner, command, handle, launchCommand, { cwd, signal });
    return { handle, spawned: true, shellBootstrapped: true };
  }
  const spawned = await checkedCommand(runner, {
    command,
    args: [
      'terminal',
      'create',
      '--worktree',
      `path:${cwd}`,
      '--command',
      launchCommand,
      '--json',
    ],
    cwd,
    signal,
  });
  const raw = JSON.parse(spawned.stdout);
  const handle = raw.result?.terminal?.handle ?? raw.result?.handle;
  if (handle === undefined || handle === null || handle === '') {
    throw new Error('Orca terminal create returned no handle');
  }
  return { handle: String(handle), spawned: true };
}

/** Send one line (plus Enter) to a terminal; the caller must have proven it idle first. */
export async function sendOrcaTerminalText(
  runner: CommandRunner,
  command: string,
  terminalHandle: string,
  text: string,
  options: { cwd?: string; signal?: AbortSignal; waitSubmit?: number; clearComposer?: boolean } = {},
): Promise<void> {
  if (options.clearComposer === true) {
    const cleared = await checkedCommand(runner, {
      command,
      args: ['terminal', 'send', '--terminal', terminalHandle, '--text', '\x15', '--json'],
      cwd: options.cwd,
      signal: options.signal,
    });
    if (JSON.parse(cleared.stdout).ok === false) throw new Error('Orca composer clear failed');
  }
  const args = [
    'terminal',
    'send',
    '--terminal',
    terminalHandle,
    '--text',
    singleLine(text),
    '--enter',
    ...(options.waitSubmit !== undefined
      ? ['--wait-submit', String(options.waitSubmit)]
      : []),
    '--json',
  ];
  const output = await checkedCommand(runner, {
    command,
    args,
    cwd: options.cwd,
    signal: options.signal,
  });
  if (JSON.parse(output.stdout).ok === false) {
    throw new Error('Orca terminal send failed');
  }
}

/** Set the visible worktree card comment; `current` is used when no workspace path is known. */
export async function setOrcaWorkspaceComment(
  runner: CommandRunner,
  command: string,
  workspacePath: string | undefined,
  comment: string,
  options: { cwd?: string; signal?: AbortSignal } = {},
): Promise<void> {
  const output = await checkedCommand(runner, {
    command,
    args: [
      'worktree',
      'set',
      '--worktree',
      workspacePath ? `path:${workspacePath}` : 'current',
      '--comment',
      singleLine(comment),
      '--json',
    ],
    cwd: options.cwd ?? workspacePath,
    signal: options.signal,
  });
  if (JSON.parse(output.stdout).ok === false) {
    throw new Error('Orca workspace comment update failed');
  }
}

/**
 * Trust a workspace in Antigravity by adding it to trustedWorkspaces.
 * Missing settings files start from an empty object, then the file is replaced atomically.
 */
export function trustAntigravityWorkspace(workspacePath: string, settingsPath?: string): void {
  const file = settingsPath ?? join(homedir(), '.gemini/antigravity-cli/settings.json');
  let settings: Record<string, unknown> = {};
  if (existsSync(file)) {
    const parsed: unknown = JSON.parse(readFileSync(file, 'utf8'));
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      throw new Error(`Antigravity settings at ${file} must be a JSON object`);
    }
    settings = parsed as Record<string, unknown>;
  }
  const trusted = Array.isArray(settings.trustedWorkspaces)
    ? settings.trustedWorkspaces.filter((entry): entry is string => typeof entry === 'string')
    : [];
  const candidates = existsSync(workspacePath)
    ? [workspacePath, realpathSync(workspacePath)]
    : [workspacePath];
  for (const candidate of candidates) {
    if (!trusted.includes(candidate)) {
      trusted.push(candidate);
    }
  }
  settings.trustedWorkspaces = trusted;
  mkdirSync(dirname(file), { recursive: true });
  const temporary = `${file}.${randomUUID()}.tmp`;
  const fd = openSync(temporary, 'wx', 0o600);
  try {
    writeFileSync(fd, `${JSON.stringify(settings, null, 2)}\n`);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  try {
    renameSync(temporary, file);
  } finally {
    rmSync(temporary, { force: true });
  }
}

export async function ensureOrcaWorkspace(
  runner: CommandRunner,
  command: string,
  options: { workspacePath: string; repoRoot?: string; comment?: string; trustAntigravity?: boolean; agySettingsPath?: string; signal?: AbortSignal },
): Promise<{ workspacePath: string; created: boolean }> {
  const cwd = options.repoRoot ?? dirname(options.workspacePath);
  const listed = await checkedCommand(runner, { command, args: ['worktree', 'list', '--json'], cwd: existsSync(cwd) ? cwd : undefined, signal: options.signal });
  const raw = JSON.parse(listed.stdout);
  const rows = raw.result?.worktrees ?? raw.worktrees ?? (Array.isArray(raw.result) ? raw.result : undefined);
  if (raw.ok === false || !Array.isArray(rows) || raw.result?.truncated) throw new Error('Incomplete Orca workspace inventory');
  const canonical = (path: string): string => {
    const absolute = resolve(path);
    if (existsSync(absolute)) return realpathSync(absolute);
    const parent = dirname(absolute);
    return parent === absolute ? absolute : join(canonical(parent), basename(absolute));
  };
  const found = rows.find(row => typeof row.path === 'string' && canonical(row.path) === canonical(options.workspacePath));
  if (found) return { workspacePath: found.path, created: false };
  if (!options.repoRoot) throw new Error('Creating a fixed Orca workspace requires a receiver-local repoRoot');
  const created = await checkedCommand(runner, {
    command, args: ['worktree', 'create', '--no-parent', '--name', basename(options.workspacePath), ...(options.comment ? ['--comment', options.comment] : []), '--json'],
    cwd: options.repoRoot, signal: options.signal,
  });
  const response = JSON.parse(created.stdout);
  const path = response.result?.path ?? response.result?.worktree?.path;
  if (response.ok === false || typeof path !== 'string' || !path.trim()) throw new Error('Orca workspace creation returned no path');
  const settings = options.agySettingsPath ?? join(homedir(), '.gemini/antigravity-cli/settings.json');
  const temporaryRoot = tmpdir();
  const mayTrust = options.agySettingsPath !== undefined || !temporaryRoot || !canonical(path).startsWith(canonical(temporaryRoot));
  if (options.trustAntigravity !== false && mayTrust && (options.agySettingsPath !== undefined || existsSync(settings))) {
    trustAntigravityWorkspace(path, settings);
  }
  return { workspacePath: path, created: true };
}

export async function resolveOrcaWorkspaceTerminal(runner: CommandRunner, command: string, cwd: string, signal?: AbortSignal): Promise<string> {
  return (await resolveOrSpawnOrcaWorkspaceTerminal(runner, command, cwd, signal)).handle;
}
