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

export const runCommand: CommandRunner = request =>
  new Promise((resolve, reject) => {
    const child = spawn(request.command, [...request.args], {
      cwd: request.cwd,
      signal: request.signal,
      stdio: ['pipe', 'pipe', 'pipe'],
      shell: false,
    });
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
    child.on('error', reject);
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
  reason?: string;
}

export interface OrcaTerminalRow {
  handle: string;
  connected: boolean;
  writable?: boolean;
  preview?: unknown;
  agentIdentity?: unknown;
  lastOutputAt?: unknown;
}

/** Collapse arbitrary text into exactly one line; wakeup pointers must never span lines. */
export function singleLine(value: string): string {
  return value.replace(/\s+/g, ' ').trim();
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
    if (handles.length < row.agents.length) {
      return false;
    }
    for (const terminal of handles) {
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
        // A trailing "> " is an unsent composer draft; a bare ">" is an idle prompt.
        const lastLine = terminal.preview
          .split(/\r?\n/)
          .filter(line => line.trim())
          .at(-1);
        if (lastLine?.trimStart().startsWith('> ')) {
          return false;
        }
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
  return { idle, reason: idle ? undefined : 'Workspace or descendant is busy or unverifiable' };
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

/** Resolve the freshest connected antigravity terminal in an Orca workspace, spawning one if needed. */
export async function resolveOrcaWorkspaceTerminal(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
): Promise<string> {
  const listed = await checkedCommand(runner, {
    command,
    args: ['terminal', 'list', '--worktree', `path:${cwd}`, '--json'],
    cwd,
    signal,
  });
  const terminals = (JSON.parse(listed.stdout)?.result?.terminals ?? []) as OrcaTerminalRow[];
  const connected = terminals.filter(t => t && t.connected && t.writable !== false);
  const preferred = connected.filter(t => t.agentIdentity === 'antigravity');
  const matched = (preferred.length ? preferred : connected).sort(
    (a, b) => (Number(b.lastOutputAt) || 0) - (Number(a.lastOutputAt) || 0),
  );
  if (matched.length) {
    return String(matched[0].handle);
  }
  const spawned = await checkedCommand(runner, {
    command,
    args: [
      'terminal',
      'spawn',
      '--worktree',
      `path:${cwd}`,
      '--agent',
      'antigravity',
      '--wait',
      '--json',
    ],
    cwd,
    signal,
  });
  const raw = JSON.parse(spawned.stdout);
  const handle = raw.result?.terminal?.handle ?? raw.result?.handle;
  if (handle === undefined || handle === null || handle === '') {
    throw new Error('Orca terminal spawn returned no handle');
  }
  return String(handle);
}

/** Send one line (plus Enter) to a terminal; the caller must have proven it idle first. */
export async function sendOrcaTerminalText(
  runner: CommandRunner,
  command: string,
  terminalHandle: string,
  text: string,
  options: { cwd?: string; signal?: AbortSignal; waitSubmit?: number } = {},
): Promise<void> {
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
