import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { defaultAgentLaunchCommand, trustAntigravityWorkspace } from '../plugins-agent-host/hosts.ts';
import { isOrcaComposerEmpty } from './tui-quirks.ts';
import { beforeExternalEffect } from '../plugins-executor/index.ts';
import { execFileSync } from 'node:child_process';
import { randomInt } from 'node:crypto';
import { basename, dirname, join, resolve } from 'node:path';
import { homedir, tmpdir } from 'node:os';
import { CommandNotStartedError, runCommand, checkedCommand, singleLine } from '../subprocess/index.ts';
import type { CommandRunner } from '../subprocess/index.ts';

export async function findOrcaWorktreeByName(
  runner: CommandRunner,
  command: string,
  name: string,
  cwd?: string,
): Promise<{ workspacePath: string; worktreeId: string } | undefined> {
  const result = await checkedCommand(runner, {
    command,
    args: ['worktree', 'ps', '--agent-wait', '--limit', '10000', '--json'],
    ...(cwd ? { cwd } : {}),
  });
  const raw = JSON.parse(result.stdout);
  if (raw?.ok !== true) throw new Error('Orca worktree ps failed');
  if (!raw.result || raw.result.truncated || !Array.isArray(raw.result.worktrees)) {
    throw new Error('Incomplete Orca workspace inventory');
  }
  const row = raw.result.worktrees.find(
    (item: any) =>
      item &&
      item.isArchived !== true &&
      item.archived !== true &&
      ((typeof item.displayName === 'string' && item.displayName === name) ||
        (typeof item.path === 'string' && basename(item.path) === name)),
  );
  if (!row || typeof row.path !== 'string' || typeof row.worktreeId !== 'string') return undefined;
  return { workspacePath: row.path, worktreeId: row.worktreeId };
}

export interface OrcaIdleResult {
  idle: boolean;
  reason?: string;
}

export interface OrcaTerminalRow {
  worktreePath?: string;
  handle: string;
  connected: boolean;
  writable?: boolean;
  orphaned?: boolean;
  preview?: unknown;
  agentIdentity?: unknown;
  lastOutputAt?: unknown;
  recordedPaneKey?: unknown;
  tabId?: string;
  leafId?: string;
}

export interface OrcaWorkspaceIdleOptions {
  /**
   * The terminal this wakeup will write into. `null` means delivery creates an independent session
   * tab first, so retained tabs must never gate the wakeup; a handle restricts composer inspection
   * to that terminal (or, when it was closed, to the terminals a rebind could reuse).
   */
  targetTerminalHandle?: string | null;
  targetPaneKey?: string;
  /**
   * With a `null` target handle: the terminal this wakeup replaces. A rotation may only open the
   * replacement tab once the retiring slot's own agent and its child worktrees are done.
   */
  retiringTerminalHandle?: string;
  retiringPaneKey?: string;
  /** Preferred target agent when the bound terminal is gone and a rebind picks a replacement. */
  agent?: SupportedOrcaAgent;
  excludedHandles?: ReadonlySet<string>;
}

/** Reuse candidates in the same order terminal resolution falls back through them. */
function reusableTerminalCandidates(
  terminals: readonly OrcaTerminalRow[],
  agent?: SupportedOrcaAgent,
): OrcaTerminalRow[] {
  const nonOrphaned = terminals.filter((terminal) => terminal.orphaned !== true);
  const pool = nonOrphaned.length > 0 ? nonOrphaned : [...terminals];
  const matchesAgent = (terminal: OrcaTerminalRow): boolean =>
    agent !== undefined &&
    (terminal.agentIdentity === agent || (agent === 'claude' && terminal.agentIdentity === 'claude-code'));
  const tiers = [
    pool.filter(matchesAgent),
    pool.filter(
      (terminal) => typeof terminal.agentIdentity === 'string' && terminal.agentIdentity.trim().length > 0,
    ),
    pool.filter(
      (terminal) => typeof terminal.preview !== 'string' && typeof terminal.agentIdentity !== 'string',
    ),
    pool.filter((terminal) => typeof terminal.preview === 'string' && !terminal.agentIdentity),
  ];
  return tiers.find((tier) => tier.length > 0) ?? pool;
}

/**
 * Authoritative recursive idle inspection: every workspace in the root's subtree must report no
 * unverifiable terminals, every agent must be done with at least as many connected terminals as
 * agents, and every terminal the wakeup could write into must be connected, writable and free of
 * an unsent composer draft. When a fresh session tab will receive the pointer, retained tabs in
 * the root workspace neither block the wakeup nor mark the composer prefilled; only a rotation's
 * retiring terminal gates the replacement, through its own agent and child worktrees. Synthetic
 * inventories are confirmed with `terminal wait`. A `> ` composer line is treated as an empty
 * Antigravity composer only when it matches the exact mode-placeholder vocabulary and Orca's
 * readiness probe also reports idle; every other `> ` line stays busy as an unsent human draft.
 */
export async function checkOrcaWorkspaceIdle(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
  options?: OrcaWorkspaceIdleOptions,
): Promise<OrcaIdleResult> {
  const output = await checkedCommand(runner, {
    command,
    args: ['worktree', 'ps', '--agent-wait', '--limit', '10000', '--json'],
    // Only this read-only inventory probe narrows a removed workspace directory to the daemon's
    // cwd: the gone path must be reported as "Workspace not found" before anything is spawned
    // inside it, while the shared runner keeps strict cwd semantics for every other caller.
    cwd: runner === runCommand && !existsSync(cwd) ? undefined : cwd,
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
    agents?: { state?: string; paneKey?: string; parentPaneKey?: string }[];
  };
  const rows: Workspace[] = inventory.worktrees;
  const root = rows.find((row) => row.path === cwd);
  if (!root) {
    return { idle: false, reason: 'Workspace not found' };
  }
  // A fresh session tab has its own empty composer, so nothing retained can block it. Replacing a
  // bound terminal is the exception: the retiring slot must be quiescent before its tab is closed.
  if (options?.targetTerminalHandle === null) {
    if (!options.retiringTerminalHandle?.trim() && !options.retiringPaneKey?.trim()) {
      return { idle: true, reason: undefined };
    }
  }
  const visited = new Set<string>();
  const idleTree = async (row: Workspace): Promise<boolean> => {
    if (visited.has(row.worktreeId)) {
      return false;
    }
    visited.add(row.worktreeId);
    const listed = await checkedCommand(runner, {
      command,
      args: ['terminal', 'list', '--worktree', `path:${row.path}`, '--json'],
      cwd,
      signal,
    });
    const terminals = JSON.parse(listed.stdout);
    if (terminals.ok !== true || terminals.result?.truncated || !Array.isArray(terminals.result?.terminals)) {
      return false;
    }
    const handles: OrcaTerminalRow[] = terminals.result.terminals;
    const isRoot = row.path === cwd;
    // Ownership limits the root tab selection; a relevant descendant still has to prove readiness.
    const canUse = (terminal: OrcaTerminalRow) => !isRoot || !options?.excludedHandles?.has(String(terminal.handle));
    const eligibleHandles = handles.filter(canUse);
    // A rotation opens a brand-new tab, so only the slot it retires can gate it.
    const rotating = isRoot && options?.targetTerminalHandle === null;
    const retiringHandle = options?.retiringTerminalHandle?.trim();
    const targetTerm = isRoot
      ? rotating
        ? (retiringHandle
            ? eligibleHandles.find(t => String(t.handle) === retiringHandle)
            : undefined) ?? eligibleHandles.find(t => matchesPane(t, options?.retiringPaneKey))
        : eligibleHandles.find(t => typeof options?.targetTerminalHandle === 'string' && String(t.handle) === options.targetTerminalHandle)
          ?? eligibleHandles.find(t => matchesPane(t, options?.targetPaneKey))
      : undefined;
    if (isRoot && !rotating && options?.targetPaneKey?.trim() && !targetTerm) return true;
    const targetPaneKeys = new Set<string>();
    if (typeof targetTerm?.tabId === 'string' && targetTerm.tabId && typeof targetTerm.leafId === 'string' && targetTerm.leafId)
      targetPaneKeys.add(`${targetTerm.tabId}:${targetTerm.leafId}`);
    if (typeof targetTerm?.recordedPaneKey === 'string' && targetTerm.recordedPaneKey) targetPaneKeys.add(targetTerm.recordedPaneKey);
    if (rotating && options?.retiringPaneKey?.trim()) targetPaneKeys.add(options.retiringPaneKey.trim());
    // The retiring terminal is already gone and carries no pane key to attribute children with.
    if (rotating && !targetTerm && targetPaneKeys.size === 0) return true;
    const ownedAgentsBusy = (): boolean =>
      !row.agents ||
      row.agents
        .filter((agent) => (typeof agent.paneKey === 'string' && targetPaneKeys.has(agent.paneKey)) || (!agent.paneKey && handles.length <= 1))
        .some((agent) => agent.state !== 'done');
    const allAgentsBusy = (): boolean => !row.agents || row.agents.some((agent) => agent.state !== 'done');
    if (targetPaneKeys.size > 0) {
      if (ownedAgentsBusy()) return false;
    } else if (rotating) {
      // A rotation waits only for the retiring slot's own agent; retained and unverifiable tabs
      // never block opening a new session tab. A retiring terminal that cannot be pane-attributed
      // fails closed instead of guessing, so a busy workspace never gains a second live tab.
      if (targetTerm && allAgentsBusy()) return false;
    } else if (row.unverifiableTerminalCount !== 0 || allAgentsBusy()) {
      return false;
    }
    // A chat-only or otherwise unobservable agent needs an injected authoritative idleCheck.
    const observableHandles = handles.filter((t) => t.orphaned !== true || t.connected);
    if (
      !rotating &&
      targetPaneKeys.size === 0 &&
      observableHandles.filter((t) => Boolean(t.connected)).length < (row.agents?.length ?? 0)
    ) {
      return false;
    }
    // Liveness and composer state only matter for terminals this wakeup may actually write into:
    // a fresh tab gets its own empty composer, so retained tabs never block it or mark it
    // prefilled; a bound tab is the only composer that receives the pointer (or the terminals a
    // rebind could reuse when it was closed). A rotation never inspects the retiring composer:
    // an unsent human draft there must not keep the replacement tab from opening.
    const rootTarget = isRoot
      ? rotating
        ? null
        : targetTerm
          ? String(targetTerm.handle)
          : options?.targetTerminalHandle
      : undefined;
    const eligibleObservableHandles = observableHandles.filter(canUse);
    const targetPresent =
      typeof rootTarget === 'string' && eligibleObservableHandles.some((t) => String(t.handle) === rootTarget);
    const reuseHandles =
      typeof rootTarget === 'string' && !targetPresent
        ? new Set(reusableTerminalCandidates(eligibleObservableHandles, options?.agent).map((t) => String(t.handle)))
        : undefined;
    const gatesWakeup = (terminal: OrcaTerminalRow): boolean => {
      if (!isRoot) return true;
      if (rootTarget === null) return false;
      if (rootTarget === undefined) return true;
      if (String(terminal.handle) === rootTarget) return true;
      // The bound terminal was closed: only the terminals a rebind could reuse are inspected.
      return reuseHandles?.has(String(terminal.handle)) === true;
    };
    for (const terminal of eligibleObservableHandles) {
      if (!gatesWakeup(terminal)) {
        continue;
      }
      if (!terminal.connected || !terminal.writable) {
        return false;
      }
      if (
        typeof terminal.preview === 'string' &&
        !isOrcaComposerEmpty(terminal.preview, terminal.agentIdentity)
      )
        return false;
      // A recognized agent (or inventory without preview metadata) needs authoritative TUI readiness.
      // A bare shell has no agent TUI yet; its workspace proof permits only the bootstrap path.
      if (
        (terminal.agentIdentity || typeof terminal.preview !== 'string') &&
        !(await waitOrcaTerminalIdle(runner, command, terminal.handle, cwd, 1, signal)).idle
      ) return false;
    }
    for (const id of row.childWorktreeIds ?? []) {
      const child = rows.find((item) => item.worktreeId === id);
      if (!child) return false;
      if (
        targetPaneKeys.size > 0 && child.agents?.length &&
        (handles.length <= 1 || child.unverifiableTerminalCount === 0) &&
        child.agents.every(agent =>
          typeof agent.parentPaneKey === 'string' && agent.parentPaneKey.length > 0 && !targetPaneKeys.has(agent.parentPaneKey),
        )
      ) continue;
      if (!(await idleTree(child))) {
        return false;
      }
    }
    return true;
  };
  const idle = await idleTree(root);
  return {
    idle,
    reason: idle ? undefined : 'Workspace or descendant is busy or unverifiable',
  };
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
    return {
      idle: false,
      reason: `Terminal idle wait failed: ${String(error)}`,
    };
  }
}

/** Read the actual terminal surface; older injected inventories may omit the terminal record. */
export async function readOrcaTerminal(
  runner: CommandRunner,
  command: string,
  terminalHandle: string,
  cwd?: string,
  signal?: AbortSignal,
): Promise<OrcaTerminalRow | undefined> {
  const shown = await checkedCommand(runner, {
    command,
    args: ['terminal', 'show', '--terminal', terminalHandle, '--json'],
    cwd,
    signal,
  });
  const parsed = JSON.parse(shown.stdout);
  if (parsed?.ok === false) throw new Error('Orca terminal show failed');
  const terminal = parsed?.result?.terminal;
  if (terminal && typeof terminal === 'object' && !Array.isArray(terminal)) return terminal;
  if (runner === runCommand) throw new Error('Orca terminal show returned no terminal');
  return undefined;
}

/** Every terminal-only target receives the same composer protection as workspace targets. */
export async function probeOrcaTerminalIdle(
  runner: CommandRunner,
  command: string,
  terminalHandle: string,
  cwd?: string,
  signal?: AbortSignal,
): Promise<OrcaIdleResult> {
  try {
    const terminal = await readOrcaTerminal(runner, command, terminalHandle, cwd, signal);
    if (terminal && (terminal.connected !== true || terminal.writable === false))
      return {
        idle: false,
        reason: 'Terminal is disconnected or not writable',
      };
    if (
      typeof terminal?.preview === 'string' &&
      !isOrcaComposerEmpty(terminal.preview, terminal.agentIdentity)
    )
      return { idle: false, reason: 'Terminal composer has unsent draft' };
    const result = await waitOrcaTerminalIdle(runner, command, terminalHandle, cwd, 1, signal);
    return result.idle
      ? result
      : {
          idle: false,
          reason: `Terminal idle probe failed: ${result.reason ?? 'Terminal is busy'}`,
        };
  } catch (error) {
    return {
      idle: false,
      reason: `Terminal idle probe failed: ${String(error)}`,
    };
  }
}

export type SupportedOrcaAgent = 'antigravity' | 'claude' | 'codex';

/**
 * Create an independent agent terminal tab in an Orca workspace. Session rotation and first-wave
 * delivery both use this so a retained conversation tab is never reset or written into.
 */


/** Parse a simple host command, or validate standalone placeholders in an explicit template. */
function continuationCommandTokens(command: string): string[] {
  const tokens: string[] = [];
  let token = '';
  let tokenStarted = false;
  let quote = '';
  let escaped = false;
  const reject = () => { throw new OrcaTerminalLaunchRejectedError(`Unsupported Orca session continuation command: ${command}`); };
  for (let i = 0; i < command.length; i++) {
    const char = command[i];
    if (/[\0\r\n]/.test(char)) reject();
    if ((!quote && /[#;|&$<>`]/.test(char)) || (quote === '"' && /[$`]/.test(char))) reject();
    if (command.startsWith('{sessionId}', i)) {
      if (quote || escaped || (i > 0 && !/\s/.test(command[i - 1])) ||
          (i + 11 < command.length && !/\s/.test(command[i + 11]))) reject();
      token += '{sessionId}';
      tokenStarted = true;
      i += 10;
      continue;
    }
    if (escaped) { token += char; escaped = false; continue; }
    if (char === '\\' && quote !== "'") {
      if (!quote) reject();
      escaped = true;
      continue;
    }
    if (quote) {
      if (char === quote) quote = '';
      else token += char;
    } else if (char === "'" || char === '"') { quote = char; tokenStarted = true; }
    else if (/\s/.test(char)) {
      if (tokenStarted) tokens.push(token);
      token = '';
      tokenStarted = false;
    } else { token += char; tokenStarted = true; }
  }
  if (quote || escaped) reject();
  if (tokenStarted) tokens.push(token);
  if (tokens.length === 0 || !tokens[0]) reject();
  return tokens;
}

function quoteOrcaCommandToken(token: string): string {
  return /^[A-Za-z0-9_./:=+-]+$/.test(token) ? token : "'" + token.replaceAll("'", "'\\''") + "'";
}

function knownOrcaHostForBinary(token: string): SupportedOrcaAgent | undefined {
  const binary = basename(token);
  return binary === 'agy' ? 'antigravity' : binary === 'claude' || binary === 'codex' ? binary : undefined;
}

function hasConflictingFreshHostArgs(host: SupportedOrcaAgent, args: readonly string[]): boolean {
  return host === 'antigravity'
    ? args.some(arg => ['run', '--conversation', '-c', '--continue', '-p', '--print', '--prompt', '--new-project', '--'].includes(arg) || arg.startsWith('--conversation=') || arg.startsWith('--prompt='))
    : host === 'claude'
      ? args.some(arg => ['-p', '--print', '-r', '--resume', '--session-id', '-c', '--continue', '--bg', '--background', '--'].includes(arg) || arg.startsWith('--resume=') || arg.startsWith('--session-id='))
      : args.some(arg => ['exec', 'resume', 'queue', 'login', 'mcp', 'completion', '--last', '--'].includes(arg));
}

export function buildOrcaAgentLaunchCommand(options?: {
  agent?: SupportedOrcaAgent; interactiveCommand?: string; resumeCommand?: string; sessionId?: string;
}): string {
  const sessionId = options?.sessionId;
  if (sessionId !== undefined && /[\0\r\n]/.test(sessionId)) throw new OrcaTerminalLaunchRejectedError('Invalid Orca conversation ID');
  const trimmedSession = sessionId?.trim() || undefined;
  const agent = options?.agent ?? 'antigravity';
  const binary = agent === 'antigravity' ? 'agy' : agent;
  const defaultCmd = defaultAgentLaunchCommand(agent);
  const customCmd = options?.interactiveCommand?.trim() || undefined;
  const resumeCmd = options?.resumeCommand?.trim() || undefined;
  if (customCmd?.includes('{sessionId}')) {
    throw new OrcaTerminalLaunchRejectedError('Orca interactiveCommand must be a fresh-session command without {sessionId}; configure resumeCommand for templated continuation');
  }
  const customTokens = customCmd ? continuationCommandTokens(customCmd) : undefined;
  const freshCmd = customTokens?.map(quoteOrcaCommandToken).join(' ');
  const resumeTokens = resumeCmd ? continuationCommandTokens(resumeCmd) : undefined;
  if (customTokens) {
    const customHost = knownOrcaHostForBinary(customTokens[0]);
    if (customHost !== undefined) {
      if (customHost !== agent) throw new OrcaTerminalLaunchRejectedError(`Orca interactiveCommand host ${customHost} does not match target agent ${agent}: ${customCmd}`);
      if (hasConflictingFreshHostArgs(customHost, customTokens.slice(1))) throw new OrcaTerminalLaunchRejectedError(`Orca interactiveCommand must be a fresh interactive launch command without embedded continuation or non-interactive flags: ${customCmd}`);
    }
  }
  if (resumeTokens) {
    if (!resumeTokens.includes('{sessionId}')) throw new OrcaTerminalLaunchRejectedError('Orca resumeCommand requires a standalone {sessionId} token');
    const resumeHost = knownOrcaHostForBinary(resumeTokens[0]);
    if (resumeHost !== undefined) {
      if (resumeHost !== agent) throw new OrcaTerminalLaunchRejectedError('Orca resumeCommand must match the target host');
      if (resumeTokens.slice(1).some(arg => ['run', 'exec', 'queue', '-p', '--print', '--prompt', '--bg', '--background', '-c', '--continue', '--last', '--'].includes(arg) || ['--prompt=', '--conversation=', '--resume=', '--session-id='].some(prefix => arg.startsWith(prefix)))) {
        throw new OrcaTerminalLaunchRejectedError('Orca resumeCommand contains conflicting continuation or non-interactive arguments');
      }
      if (customTokens && knownOrcaHostForBinary(customTokens[0]) !== agent) throw new OrcaTerminalLaunchRejectedError('Orca resumeCommand and interactiveCommand must use the same host');
    } else if (!customTokens || customTokens[0] !== resumeTokens[0]) {
      throw new OrcaTerminalLaunchRejectedError('Custom Orca resumeCommand requires a matching fresh-session interactiveCommand');
    }
  }
  if (!trimmedSession) return freshCmd ?? defaultCmd;
  const quoted = "'" + trimmedSession.replaceAll("'", "'\\''") + "'";
  if (resumeTokens) return resumeTokens.map(token => token === '{sessionId}' ? quoted : quoteOrcaCommandToken(token)).join(' ');
  let command = defaultCmd;
  if (customTokens && customCmd !== defaultCmd) {
    if (basename(customTokens[0]) !== binary) {
      throw new OrcaTerminalLaunchRejectedError(`Unsupported Orca session continuation command for ${agent}: ${customCmd}`);
    }
    command = freshCmd!;
  }
  return agent === 'codex' ? `${command} resume ${quoted}`
    : `${command} ${agent === 'claude' ? '--resume' : '--conversation'} ${quoted}`;
}

/** Both fresh launch and continuation must remain valid when retaining host configuration. */
export function isOrcaLaunchConfigCompatible(options: {
  agent?: SupportedOrcaAgent; previousAgent?: SupportedOrcaAgent; interactiveCommand?: string; resumeCommand?: string;
}): boolean {
  const interactiveCommand = options.interactiveCommand?.trim() || undefined;
  const resumeCommand = options.resumeCommand?.trim() || undefined;
  if (!interactiveCommand && !resumeCommand) return true;
  if (!options.agent) return options.previousAgent === undefined;
  try {
    buildOrcaAgentLaunchCommand({ ...options, sessionId: undefined });
    buildOrcaAgentLaunchCommand({ ...options, sessionId: 'probe-session' });
    const firstToken = continuationCommandTokens(interactiveCommand ?? resumeCommand!)[0];
    const binary = options.agent === 'antigravity' ? 'agy' : options.agent;
    return basename(firstToken) === binary ||
      (options.previousAgent !== undefined && options.previousAgent === options.agent);
  } catch (error) {
    if (error instanceof OrcaTerminalLaunchRejectedError) return false;
    throw error;
  }
}

/**
 * Create an independent agent terminal tab in an Orca workspace. Session rotation and first-wave
 * delivery both use this so a retained conversation tab is never reset or written into.
 */
export class OrcaTerminalLaunchRejectedError extends Error {}

export async function spawnOrcaWorkspaceTerminal(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
  options: { agent?: SupportedOrcaAgent; interactiveCommand?: string; resumeCommand?: string; sessionId?: string } = {},
): Promise<{ handle: string; raw: unknown }> {
  const launchCommand = buildOrcaAgentLaunchCommand(options);
  let spawned;
  try {
    spawned = await runner({
      command,
      args: ['terminal', 'create', '--worktree', `path:${cwd}`, '--command', launchCommand, '--json'],
      cwd,
      signal,
    });
  } catch (error) {
    if (error instanceof CommandNotStartedError || error instanceof OrcaTerminalLaunchRejectedError)
      throw error;
    const failure = error as { stdout?: unknown; exitCode?: unknown } | null;
    if (typeof failure?.stdout === 'string' && failure.exitCode !== null) {
      let parsed;
      try {
        parsed = JSON.parse(failure.stdout.trim());
      } catch {
        // Unparseable output cannot confirm refusal.
      }
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed) && parsed.ok === false) {
        throw new OrcaTerminalLaunchRejectedError(error instanceof Error ? error.message : String(error), {
          cause: error,
        });
      }
    }
    throw error;
  }
  let parsed;
  try {
    parsed = JSON.parse(spawned.stdout.trim());
  } catch {
    // Classified below as an unknown outcome.
  }
  const record = parsed && typeof parsed === 'object' && !Array.isArray(parsed);
  if (record && parsed.ok === false) {
    throw new OrcaTerminalLaunchRejectedError(
      `${command} rejected terminal launch: ${(spawned.stderr || spawned.stdout).trim()}`,
    );
  }
  if (spawned.exitCode !== undefined && spawned.exitCode !== 0) {
    throw new Error(`${command} exited ${spawned.exitCode}: ${(spawned.stderr || spawned.stdout).trim()}`);
  }
  if (!record) throw new Error('Orca terminal create returned invalid JSON');
  const handle = parsed.result?.terminal?.handle ?? parsed.result?.handle ?? parsed.handle;
  if (typeof handle !== 'string' || !handle.trim())
    throw new Error('Orca terminal create returned no handle');
  return { handle: handle.trim(), raw: parsed };
}

/** A persisted handle is reusable only while Orca still reports it connected and writable. */
function matchesPane(terminal: OrcaTerminalRow, paneKey?: string): boolean {
  return Boolean(paneKey) && terminal.connected === true && terminal.writable !== false &&
    (terminal.recordedPaneKey === paneKey ||
      (typeof terminal.tabId === 'string' && typeof terminal.leafId === 'string' &&
        `${terminal.tabId}:${terminal.leafId}` === paneKey));
}

export async function isOrcaWorkspaceTerminalLive(
  runner: CommandRunner,
  command: string,
  cwd: string,
  terminalHandle?: string,
  signal?: AbortSignal,
  preferredPaneKey?: string,
): Promise<boolean> {
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
  const terminal = (inventory.result.terminals as OrcaTerminalRow[]).find(
    row => row && row.connected === true && row.writable !== false &&
      ((Boolean(terminalHandle) && String(row.handle) === terminalHandle) || matchesPane(row, preferredPaneKey)),
  );
  return terminal !== undefined && terminal.connected === true && terminal.writable !== false;
}

/** Read-only terminal selection: callers must fence the returned launch plan before executing it. */
export async function planOrcaWorkspaceTerminal(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
  options?: { agent?: SupportedOrcaAgent; interactiveCommand?: string; resumeCommand?: string; sessionId?: string; preferredHandle?: string; preferredPaneKey?: string; dedicatedTab?: boolean; excludedHandles?: ReadonlySet<string> },
): Promise<{ kind: 'existing'; handle: string } | { kind: 'shell'; handle: string } | { kind: 'create' }> {
  const targetAgent = options?.agent ?? 'antigravity';
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
  const terminals = (inventory.result.terminals as OrcaTerminalRow[]).filter(
    terminal => terminal && !options?.excludedHandles?.has(String(terminal.handle)),
  );
  // A workspace already bound to a terminal reuses exactly that terminal: orphaned only means the
  // workspace registry lost track of it, not that its live TUI stopped accepting wakeups.
  const preferredHandle = options?.preferredHandle;
  if (preferredHandle) {
    const bound = terminals.find(
      (t) => t && String(t.handle) === preferredHandle && t.connected && t.writable !== false,
    );
    if (bound) return { handle: String(bound.handle), kind: 'existing' };
  }
  const pane = terminals.find(t => t && matchesPane(t, options?.preferredPaneKey));
  if (pane) return { kind: 'existing', handle: String(pane.handle) };
  if (options?.dedicatedTab || options?.sessionId?.trim() || options?.preferredPaneKey?.trim()) return { kind: 'create' };
  const nonOrphanConnected = terminals.filter(
    (t) => t && t.orphaned !== true && t.connected && t.writable !== false,
  );
  const connected =
    nonOrphanConnected.length > 0
      ? nonOrphanConnected
      : terminals.filter(
          (t) =>
            t &&
            t.connected &&
            t.writable !== false &&
            typeof t.agentIdentity === 'string' &&
            t.agentIdentity.trim().length > 0,
        );
  const freshest = (rows: OrcaTerminalRow[]) =>
    rows.sort((a, b) => (Number(b.lastOutputAt) || 0) - (Number(a.lastOutputAt) || 0))[0];
  const preferred = freshest(
    connected.filter(
      (t) =>
        t.agentIdentity === targetAgent || (targetAgent === 'claude' && t.agentIdentity === 'claude-code'),
    ),
  );
  if (preferred) return { handle: String(preferred.handle), kind: 'existing' };
  const agent = freshest(
    connected.filter((t) => typeof t.agentIdentity === 'string' && t.agentIdentity.trim()),
  );
  if (agent) return { handle: String(agent.handle), kind: 'existing' };
  const synthetic = freshest(
    connected.filter((t) => typeof t.preview !== 'string' && typeof t.agentIdentity !== 'string'),
  );
  if (synthetic) return { handle: String(synthetic.handle), kind: 'existing' };
  const bareShell = freshest(connected.filter((t) => typeof t.preview === 'string' && !t.agentIdentity));
  return bareShell ? { kind: 'shell', handle: String(bareShell.handle) } : { kind: 'create' };
}

/** Resolve and launch for callers that do not own a durable mailbox obligation. */
export async function resolveOrSpawnOrcaWorkspaceTerminal(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
  options?: { agent?: SupportedOrcaAgent; interactiveCommand?: string; resumeCommand?: string; sessionId?: string; preferredHandle?: string; preferredPaneKey?: string; dedicatedTab?: boolean },
): Promise<{ handle: string; spawned: boolean; shellBootstrapped?: boolean }> {
  const plan = await planOrcaWorkspaceTerminal(runner, command, cwd, signal, options);
  if (plan.kind === 'existing') return { handle: plan.handle, spawned: false };
  if (plan.kind === 'shell') {
    await sendOrcaTerminalText(
      runner,
      command,
      plan.handle,
      buildOrcaAgentLaunchCommand(options),
      { cwd, signal },
    );
    return { handle: plan.handle, spawned: true, shellBootstrapped: true };
  }
  const spawned = await spawnOrcaWorkspaceTerminal(runner, command, cwd, signal, options);
  return { handle: spawned.handle, spawned: true };
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
    ...(options.waitSubmit !== undefined ? ['--wait-submit', String(options.waitSubmit)] : []),
    '--json',
  ];
  let output;
  try {
    output = await checkedCommand(runner, { command, args, cwd: options.cwd, signal: options.signal });
  } catch (error) {
    if (!String(error).includes('no_active_sender_terminal')) throw error;
    output = await checkedCommand(runner, {
      command,
      args: [...args, '--allow-self'],
      cwd: options.cwd,
      signal: options.signal,
      env: { ...process.env, ORCA_TERMINAL_ID: terminalHandle },
    });
  }
  if (JSON.parse(output.stdout).ok === false) {
    throw new Error('Orca terminal send failed');
  }
}

/**
 * Close a terminal whose replacement has already been committed. Failures propagate so the caller
 * keeps the retired handle persisted, reports the error and retries the close on the next wakeup;
 * a silently swallowed failure would leak a live terminal into the slot.
 */
export async function closeOrcaWorkspaceTerminal(
  runner: CommandRunner,
  command: string,
  cwd: string,
  terminalHandle: string,
  signal?: AbortSignal,
): Promise<void> {
  const handle = terminalHandle.trim();
  if (!handle) return;
  const output = await checkedCommand(runner, {
    command,
    args: ['terminal', 'close', '--terminal', handle, '--json'],
    cwd,
    signal,
  });
  if (JSON.parse(output.stdout)?.ok === false) throw new Error('Orca terminal close failed');
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

export async function ensureOrcaWorkspace(
  runner: CommandRunner,
  command: string,
  options: {
    workspacePath: string;
    repoRoot?: string;
    comment?: string;
    trustAntigravity?: boolean;
    agySettingsPath?: string;
    signal?: AbortSignal;
  },
): Promise<{ workspacePath: string; created: boolean }> {
  const cwd = options.repoRoot ?? dirname(options.workspacePath);
  const listed = await checkedCommand(runner, {
    command,
    args: ['worktree', 'list', '--json'],
    cwd: existsSync(cwd) ? cwd : undefined,
    signal: options.signal,
  });
  const raw = JSON.parse(listed.stdout);
  const rows = raw.result?.worktrees ?? raw.worktrees ?? (Array.isArray(raw.result) ? raw.result : undefined);
  if (raw.ok === false || !Array.isArray(rows) || raw.result?.truncated)
    throw new Error('Incomplete Orca workspace inventory');
  const canonical = (path: string): string => {
    const absolute = resolve(path);
    if (existsSync(absolute)) return realpathSync(absolute);
    const parent = dirname(absolute);
    return parent === absolute ? absolute : join(canonical(parent), basename(absolute));
  };
  const found = rows.find(
    (row) => typeof row.path === 'string' && canonical(row.path) === canonical(options.workspacePath),
  );
  if (found) return { workspacePath: found.path, created: false };
  if (!options.repoRoot)
    throw new Error('Creating a fixed Orca workspace requires a receiver-local repoRoot');
  const created = await checkedCommand(runner, {
    command,
    args: [
      'worktree',
      'create',
      '--no-parent',
      '--name',
      basename(options.workspacePath),
      ...(options.comment ? ['--comment', options.comment] : []),
      '--json',
    ],
    cwd: options.repoRoot,
    signal: options.signal,
  });
  const response = JSON.parse(created.stdout);
  const path = response.result?.path ?? response.result?.worktree?.path;
  if (response.ok === false || typeof path !== 'string' || !path.trim())
    throw new Error('Orca workspace creation returned no path');
  const settings = options.agySettingsPath ?? join(homedir(), '.gemini/antigravity-cli/settings.json');
  const temporaryRoot = tmpdir();
  const mayTrust =
    options.agySettingsPath !== undefined ||
    (runner === runCommand && (!temporaryRoot || !canonical(path).startsWith(canonical(temporaryRoot))));
  if (
    options.trustAntigravity !== false &&
    mayTrust &&
    (options.agySettingsPath !== undefined || existsSync(settings))
  ) {
    trustAntigravityWorkspace(path, settings);
  }
  return { workspacePath: path, created: true };
}

export async function resolveOrcaWorkspaceTerminal(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
): Promise<string> {
  return (await resolveOrSpawnOrcaWorkspaceTerminal(runner, command, cwd, signal)).handle;
}
export function orcaAgent(value: unknown): SupportedOrcaAgent {
  if (value === undefined) return 'antigravity';
  if (value === 'antigravity' || value === 'antigravity-cli') return 'antigravity';
  if (value === 'claude' || value === 'claude-code') return 'claude';
  if (value === 'codex' || value === 'codex-cli') return 'codex';
  throw beforeExternalEffect(new Error(`Unsupported Orca agent: ${String(value)}`));
}

/** Orca-compatible creature vocabulary for randomly suffixed worktree names. */
export const SEA_CREATURE_POOL = [
  'nautilus',
  'seahorse',
  'starfish',
  'dolphin',
  'octopus',
  'jellyfish',
  'stingray',
  'manatee',
  'narwhal',
  'walrus',
  'beluga',
  'dugong',
  'axolotl',
  'coral',
  'halibut',
  'lamprey',
  'mackerel',
  'oarfish',
  'pollock',
  'remora',
  'sardine',
  'triggerfish',
  'bonefish',
  'cuttlefish',
  'manta',
  'killifish',
  'lionfish',
  'mudskipper',
  'needlefish',
  'paddlefish',
  'triton',
  'rockfish',
  'sawfish',
  'tilefish',
  'weakfish',
  'yellowtail',
  'wahoo',
  'tautog',
  'sculpin',
  'pomfret',
  'coelacanth',
  'lumpfish',
  'hogfish',
  'porpoise',
  'dace',
  'blenny',
  'abalone',
  'barnacle',
  'cockle',
  'krill',
  'limpet',
  'mussel',
  'oyster',
  'scallop',
  'urchin',
  'whelk',
  'conch',
  'prawn',
  'hammerhead',
  'anemone',
  'wobbegong',
  'hydroid',
  'isopod',
  'nudibranch',
  'salp',
  'tunicate',
  'chimaera',
  'copepod',
  'amphipod',
  'anglerfish',
] as const;

export interface PickSeaCreatureNameOptions {
  runner: CommandRunner;
  command: string;
  cwd?: string;
  issuedNames?: Set<string>;
  signal?: AbortSignal;
  randomBelow?: (max: number) => number;
}

export async function pickSeaCreatureName(options: PickSeaCreatureNameOptions): Promise<string> {
  try {
    const { runner, command, cwd, issuedNames, signal, randomBelow = (max) => randomInt(max) } = options;
    const listed = await checkedCommand(runner, {
      command,
      args: ['worktree', 'list', '--json'],
      cwd,
      signal,
    });
    const raw = JSON.parse(listed.stdout) as {
      ok?: boolean;
      result?: { worktrees?: { path?: unknown; displayName?: unknown; branch?: unknown }[] };
    };
    if (raw.ok === false) {
      throw new Error('Orca worktree listing failed');
    }
    const existing = new Set<string>();
    for (const row of raw.result?.worktrees ?? []) {
      if (typeof row.path === 'string' && row.path) {
        existing.add(basename(row.path).trim().toLowerCase());
      }
      if (typeof row.displayName === 'string' && row.displayName) {
        existing.add(row.displayName.trim().toLowerCase());
      }
      // Branch refs such as refs/heads/ben.xu/seahorse collide once their basename matches a creature.
      if (typeof row.branch === 'string' && row.branch) {
        existing.add(basename(row.branch).trim().toLowerCase());
      }
    }
    // Local branches outlive removed worktrees; never reuse a creature whose branch still exists.
    if (typeof cwd === 'string' && existsSync(cwd)) {
      try {
        const branches = execFileSync(
          'git',
          ['-C', cwd, 'for-each-ref', '--format=%(refname:short)', 'refs/heads'],
          { encoding: 'utf8' },
        );
        for (const branch of branches.split('\n')) {
          if (branch.trim()) {
            existing.add(branch.trim().toLowerCase());
            existing.add(basename(branch.trim()).toLowerCase());
          }
        }
      } catch {
        // A non-git directory has no local branches to collide with.
      }
    }
    for (const name of issuedNames ?? []) {
      existing.add(name.trim().toLowerCase());
    }
    const checkedRandomBelow = (max: number): number => {
      const value = randomBelow(max);
      if (!Number.isInteger(value) || value < 0 || value >= max) {
        throw new RangeError(`randomBelow(${max}) must return an integer in [0, ${max})`);
      }
      return value;
    };
    for (let attempt = 0; attempt < 10_000; attempt += 1) {
      const creature = SEA_CREATURE_POOL[checkedRandomBelow(SEA_CREATURE_POOL.length)];
      const suffix = 100_000 + checkedRandomBelow(900_000);
      const candidate = `${creature}-${suffix}`;
      if (!existing.has(candidate)) {
        issuedNames?.add(candidate);
        return candidate;
      }
    }
    throw new Error('Sea-creature worktree names exhausted');
  } catch (error) {
    throw beforeExternalEffect(error instanceof Error ? error : new Error(String(error)));
  }
}

export { defaultAgentLaunchCommand } from '../plugins-agent-host/hosts.ts';
