import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { defaultAgentLaunchCommand, trustAntigravityWorkspace } from '../plugins-agent-host/hosts.ts';
import { hasOrcaComposerPrompt, isOrcaComposerEmpty } from './tui-quirks.ts';
import { beforeExternalEffect } from '../plugins-executor/index.ts';
import { execFileSync } from 'node:child_process';
import { randomInt } from 'node:crypto';
import { basename, dirname, join, resolve } from 'node:path';
import { homedir, tmpdir } from 'node:os';
import { CommandNotStartedError, runCommand, checkedCommand, singleLine } from '../subprocess/index.ts';
import type { CommandRequest, CommandRunner } from '../subprocess/index.ts';
import { orcaMutationRunner, readOrcaTerminalList, readOrcaWorktreePs } from './inventories.ts';

export async function findOrcaWorktreeByName(
  runner: CommandRunner,
  command: string,
  name: string,
  cwd?: string,
): Promise<{ workspacePath: string; worktreeId: string } | undefined> {
  const raw = JSON.parse(await readOrcaWorktreePs(runner, command, cwd || undefined));
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

/** Synchronize a standing workspace only when its local history can fast-forward. */
export async function tryFastForwardCleanWorktree(
  runner: CommandRunner,
  workspacePath: string,
  canMerge: () => Promise<boolean>,
): Promise<void> {
  const git = (args: string[], signal?: AbortSignal) => checkedCommand(runner, {
    command: 'git', args: ['-C', workspacePath, '-c', 'core.fsmonitor=false', ...args], signal,
  });
  const isClean = async () => (await git(['status', '--porcelain', '--untracked-files=all'])).stdout.trim() === '';
  if (!(await canMerge()) || !(await isClean())) return;
  await git(['fetch', '--quiet', 'origin', 'main'], AbortSignal.timeout(8_000));
  if ((await git(['rev-list', '--count', 'origin/main..HEAD'])).stdout.trim() !== '0') return;
  if ((await git(['rev-list', '--count', 'HEAD..origin/main'])).stdout.trim() === '0') return;
  // Fetch can take seconds, during which another slot or a local edit can appear.
  if (!(await isClean()) || !(await canMerge())) return;
  await git(['merge', '--ff-only', '--quiet', 'origin/main'], AbortSignal.timeout(8_000));
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

interface OrcaProbeOptions {
  /** Slot probes report invalid observations through their caller. */
  throwOnProbeError?: boolean;
}

export interface OrcaWorkspaceIdleOptions extends OrcaProbeOptions {
  /** Cached inventories may inform readiness displays only. */
  advisory?: boolean;
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
  /** Preferred agent for rebinds and required identity for a live delivery target. */
  agent?: SupportedOrcaAgent;
  excludedHandles?: ReadonlySet<string>;
  /** Delivery must still be addressing the exact, live terminal that it selected. */
  requireTarget?: boolean;
  /** Workspace changes require unscoped activity checks and at most the slot's own tab. */
  exclusiveTerminal?: { handle?: string; paneKey?: string };
}

function matchesOrcaAgentIdentity(agentIdentity: unknown, expectedAgent: SupportedOrcaAgent): boolean {
  return agentIdentity === expectedAgent || (expectedAgent === 'claude' && agentIdentity === 'claude-code');
}

function orcaAgentReadinessFailure(terminal: OrcaTerminalRow, expectedAgent?: SupportedOrcaAgent): OrcaIdleResult | undefined {
  if (!expectedAgent || typeof terminal.preview !== 'string') return undefined;
  if (!matchesOrcaAgentIdentity(terminal.agentIdentity, expectedAgent)) {
    return { idle: false, reason: 'Target agent is not ready in terminal' };
  }
  if (expectedAgent === 'antigravity' && !hasOrcaComposerPrompt(terminal.preview)) {
    return { idle: false, reason: 'Antigravity composer prompt is not ready' };
  }
  return undefined;
}

/** Select the same reuse candidates for workspace readiness and terminal resolution. */
function reusableTerminalCandidates(
  terminals: readonly OrcaTerminalRow[],
  agent?: SupportedOrcaAgent,
): { kind: 'existing' | 'shell'; terminals: OrcaTerminalRow[] } | undefined {
  const hasIdentity = (terminal: OrcaTerminalRow) =>
    typeof terminal.agentIdentity === 'string' && terminal.agentIdentity.trim().length > 0;
  const connected = terminals.filter(isUsableTerminal);
  const eligible = agent === undefined ? connected : connected.filter(terminal =>
    !hasIdentity(terminal) || matchesOrcaAgentIdentity(terminal.agentIdentity, agent));
  const nonOrphaned = eligible.filter(terminal => terminal.orphaned !== true);
  const pool = nonOrphaned.length > 0 ? nonOrphaned : eligible.filter(hasIdentity);
  const preferred = pool.filter(terminal => matchesOrcaAgentIdentity(terminal.agentIdentity, agent ?? 'antigravity'));
  if (preferred.length > 0) return { kind: 'existing', terminals: preferred };
  const recognized = pool.filter(hasIdentity);
  if (recognized.length > 0) return { kind: 'existing', terminals: recognized };
  const synthetic = pool.filter(terminal =>
    typeof terminal.preview !== 'string' && typeof terminal.agentIdentity !== 'string');
  if (synthetic.length > 0) return { kind: 'existing', terminals: synthetic };
  const shells = pool.filter(terminal => typeof terminal.preview === 'string' && !terminal.agentIdentity);
  return shells.length > 0 ? { kind: 'shell', terminals: shells } : undefined;
}

/**
 * Root terminals eligible for pointer injection must be connected, writable, have an empty
 * composer and pass `tui-idle` when an agent is present or the preview is unavailable.
 * A required live target also verifies its configured agent and Antigravity prompt.
 * A fresh tab has its own composer, and rotation checks the retiring slot's execution activity.
 * Non-excluded descendant worktrees prove execution quiescence with complete agent and
 * terminal inventories, no unverifiable terminals, done agents and connected terminal coverage.
 */
export async function checkOrcaWorkspaceIdle(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
  options?: OrcaWorkspaceIdleOptions,
): Promise<OrcaIdleResult> {
  // A removed workspace must be reported as "Workspace not found" from the global
  // inventory before another command can spawn inside it.
  const inventoryCwd = runner === runCommand && !existsSync(cwd) ? undefined : cwd;
  const raw = JSON.parse(await readOrcaWorktreePs(runner, command, inventoryCwd, signal, options?.advisory));
  const inventory = raw.result;
  if (raw.ok !== true || !inventory || inventory.truncated || !Array.isArray(inventory.worktrees)) {
    if (options?.throwOnProbeError) throw new Error('Incomplete workspace inventory');
    return { idle: false, reason: 'Incomplete workspace inventory' };
  }
  type Workspace = {
    path: string;
    worktreeId: string;
    childWorktreeIds?: string[];
    liveTerminalCount?: number;
    unverifiableTerminalCount?: number;
    agents?: { state?: string; paneKey?: string; parentPaneKey?: string | null }[];
  };
  const rows: Workspace[] = inventory.worktrees;
  const rowsById = new Map(rows.map(row => [row.worktreeId, row]));
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
  const slotScoped = options?.targetTerminalHandle !== undefined || Boolean(
    options?.targetPaneKey?.trim() || options?.retiringTerminalHandle?.trim() || options?.retiringPaneKey?.trim(),
  );
  const visited = new Set<string>([root.worktreeId]);
  const terminalInventories = new Map<string, Promise<OrcaTerminalRow[] | undefined>>();
  const terminalHandlesByWorktreeId = new Map<string, ReadonlySet<string>>();
  const fetchWorktreeTerminals = async (row: Workspace): Promise<OrcaTerminalRow[] | undefined> => {
    const terminals = JSON.parse(await readOrcaTerminalList(runner, command, row.path, cwd, signal, options?.advisory));
    if (terminals.ok !== true || terminals.result?.truncated || !Array.isArray(terminals.result?.terminals)) {
      return undefined;
    }
    const handles: OrcaTerminalRow[] = terminals.result.terminals;
    terminalHandlesByWorktreeId.set(row.worktreeId, new Set(handles.map(terminal => String(terminal.handle))));
    return handles;
  };
  const readWorktreeTerminals = (row: Workspace): Promise<OrcaTerminalRow[] | undefined> => {
    let terminals = terminalInventories.get(row.worktreeId);
    if (!terminals) {
      terminals = fetchWorktreeTerminals(row);
      terminalInventories.set(row.worktreeId, terminals);
    }
    return terminals;
  };
  const readWorktreeTerminalHandles = async (node: Workspace): Promise<ReadonlySet<string>> => {
    await readWorktreeTerminals(node);
    const handles = terminalHandlesByWorktreeId.get(node.worktreeId);
    if (!handles) throw new Error('Incomplete Orca terminal inventory');
    return handles;
  };
  const descendantsOfRoot = (): Workspace[] => {
    const descendants: Workspace[] = [];
    const seen = new Set([root.worktreeId]);
    const visit = (row: Workspace): void => {
      for (const id of row.childWorktreeIds ?? []) {
        const child = rowsById.get(id);
        if (!child || seen.has(id)) continue;
        seen.add(id);
        descendants.push(child);
        visit(child);
      }
    };
    visit(root);
    return descendants;
  };
  const isDescendantQuiescent = async (row: Workspace): Promise<boolean> => {
    if (visited.has(row.worktreeId)) return false;
    visited.add(row.worktreeId);
    const handles = await readWorktreeTerminals(row);
    if (!handles) {
      if (options?.throwOnProbeError) throw new Error('Incomplete Orca terminal inventory');
      return false;
    }
    const agents = row.agents;
    if (!Array.isArray(agents)) return false;
    if (row.unverifiableTerminalCount !== 0 || agents.some(agent => agent.state !== 'done')) return false;
    const observableHandles = handles.filter(t => t.orphaned !== true || t.connected);
    if (observableHandles.filter(t => Boolean(t.connected)).length < agents.length) return false;
    for (const id of row.childWorktreeIds ?? []) {
      const child = rowsById.get(id);
      if (!child || !(await isDescendantQuiescent(child))) return false;
    }
    return true;
  };

  const notIdle = (): OrcaIdleResult => ({ idle: false, reason: 'Workspace or descendant is busy or unverifiable' });
  const handles = await readWorktreeTerminals(root);
  if (!handles) {
    if (options?.throwOnProbeError) throw new Error('Incomplete Orca terminal inventory');
    return notIdle();
  }
  const canUse = (terminal: OrcaTerminalRow) => !options?.excludedHandles?.has(String(terminal.handle));
  const eligibleHandles = handles.filter(canUse);
  // A rotation opens a brand-new tab, so only the slot it retires can gate it.
  const rotating = options?.targetTerminalHandle === null;
  const retiringHandle = options?.retiringTerminalHandle?.trim();
  const targetTerm = rotating
    ? (retiringHandle
        ? eligibleHandles.find(t => String(t.handle) === retiringHandle)
        : undefined) ?? eligibleHandles.find(t => matchesPane(t, options?.retiringPaneKey))
    : eligibleHandles.find(t => typeof options?.targetTerminalHandle === 'string' && String(t.handle) === options.targetTerminalHandle)
      ?? eligibleHandles.find(t => matchesPane(t, options?.targetPaneKey));
  const explicitTargetPaneKey = rotating ? undefined : options?.targetPaneKey?.trim();
  const missingTargetPane = Boolean(explicitTargetPaneKey && !targetTerm);
  const targetPaneKeys = new Set(targetTerm ? terminalPaneKeys(targetTerm) : []);
  if (explicitTargetPaneKey) targetPaneKeys.add(explicitTargetPaneKey);
  if (rotating && options?.retiringPaneKey?.trim()) targetPaneKeys.add(options.retiringPaneKey.trim());
  const ownHandle = rotating ? retiringHandle : targetTerm
    ? String(targetTerm.handle)
    : typeof options?.targetTerminalHandle === 'string' ? options.targetTerminalHandle.trim() : undefined;
  const observableHandles = handles.filter((t) => t.orphaned !== true || t.connected);
  if (options?.exclusiveTerminal) {
    const { handle, paneKey } = options.exclusiveTerminal;
    if (
      !Array.isArray(root.childWorktreeIds) ||
      (root.liveTerminalCount !== undefined &&
        root.liveTerminalCount !== observableHandles.filter(terminal => terminal.connected === true).length) ||
      observableHandles.length > 1 ||
      observableHandles.some(terminal => String(terminal.handle) !== handle && !matchesPane(terminal, paneKey))
    ) return notIdle();
  }
  const eligibleObservableHandles = observableHandles.filter(canUse);
  const rootTarget = rotating || missingTargetPane
    ? null
    : targetTerm ? String(targetTerm.handle) : options?.targetTerminalHandle;
  const targetPresent =
    typeof rootTarget === 'string' && eligibleObservableHandles.some((t) => String(t.handle) === rootTarget);
  if (options?.requireTarget && !targetPresent) return notIdle();
  const needsReuse = (typeof rootTarget === 'string' && !targetPresent) ||
    (rootTarget === undefined && options?.agent !== undefined);
  const reuseHandles = needsReuse
    ? new Set(reusableTerminalCandidates(eligibleObservableHandles, options?.agent)?.terminals.map(t => String(t.handle)))
    : undefined;
  const siblingTerminals = slotScoped
    ? handles.filter(terminal =>
      isUsableTerminal(terminal) && terminal !== targetTerm &&
      String(terminal.handle) !== ownHandle && String(terminal.handle) !== retiringHandle &&
      !reuseHandles?.has(String(terminal.handle)) &&
      !terminalPaneKeys(terminal).some(key => targetPaneKeys.has(key)),
    )
    : [];
  const siblingHandles = new Set(siblingTerminals.map(terminal => String(terminal.handle)));
  const knownSiblingPaneKeys = new Set(siblingTerminals.flatMap(terminalPaneKeys));
  const allowedSiblingHandle = (handle: string | undefined): handle is string =>
    Boolean(handle && handle !== ownHandle && handle !== retiringHandle && !reuseHandles?.has(handle));
  const canExcludeByLineage = siblingHandles.size > 0 ||
    [...(options?.excludedHandles ?? [])].some(allowedSiblingHandle);
  const agents = root.agents;
  if (!Array.isArray(agents)) return notIdle();
  const ownedAgentsBusy = (): boolean =>
    agents
      .filter((agent) =>
        (typeof agent.paneKey === 'string' && targetPaneKeys.has(agent.paneKey)) ||
        // A new tab still has to wait for work that cannot be attributed to a sibling.
        (!agent.paneKey?.trim() && (rotating || missingTargetPane || handles.length <= 1)),
      )
      .some((agent) => agent.state !== 'done');
  const allAgentsBusy = (): boolean => agents.some((agent) => agent.state !== 'done');
  if (targetPaneKeys.size > 0) {
    if (ownedAgentsBusy()) return notIdle();
  } else if (rotating) {
    // A rotation waits only for the retiring slot's own agent; retained and unverifiable tabs
    // never block opening a new session tab. A retiring terminal that cannot be pane-attributed
    // fails closed instead of guessing; its child worktrees are still inspected below.
    if (targetTerm
      ? allAgentsBusy()
      : agents.some(agent =>
        agent.state !== 'done' && !(typeof agent.paneKey === 'string' && knownSiblingPaneKeys.has(agent.paneKey)),
      )
    ) {
      return notIdle();
    }
  } else if (root.unverifiableTerminalCount !== 0 || allAgentsBusy()) {
    return notIdle();
  }
  // A chat-only or otherwise unobservable agent needs an injected authoritative idleCheck.
  if (
    !rotating &&
    targetPaneKeys.size === 0 &&
    observableHandles.filter((t) => Boolean(t.connected)).length < agents.length
  ) {
    return notIdle();
  }
  // Liveness and composer state only matter for terminals this wakeup may actually write into:
  // a fresh tab gets its own empty composer, so retained tabs never block it or mark it
  // prefilled; a bound tab is the only composer that receives the pointer (or the terminals a
  // rebind could reuse when it was closed). A rotation never inspects the retiring composer:
  // an unsent human draft there must not keep the replacement tab from opening.
  const gatesWakeup = (terminal: OrcaTerminalRow): boolean => {
    if (rootTarget === null) return false;
    if (reuseHandles) return reuseHandles.has(String(terminal.handle));
    return rootTarget === undefined || String(terminal.handle) === rootTarget;
  };
  for (const terminal of eligibleObservableHandles) {
    if (!gatesWakeup(terminal)) {
      continue;
    }
    if (!terminal.connected || !terminal.writable) {
      return notIdle();
    }
    if (options?.requireTarget) {
      const agentUnready = orcaAgentReadinessFailure(terminal, options.agent);
      if (agentUnready) return agentUnready;
    }
    if (
      typeof terminal.preview === 'string' &&
      !isOrcaComposerEmpty(terminal.preview, terminal.agentIdentity)
    )
      return notIdle();
    // A recognized agent (or inventory without preview metadata) needs authoritative TUI readiness.
    // A bare shell has no agent TUI yet; its workspace proof permits only the bootstrap path.
    if (
      (terminal.agentIdentity || typeof terminal.preview !== 'string') &&
      !(await waitOrcaTerminalIdle(runner, command, terminal.handle, cwd, 1, signal, options)).idle
    ) return notIdle();
  }

  const siblingAttribution = (node: Workspace): 'pane' | 'creator' | undefined => {
    if (!Array.isArray(node.agents)) return undefined;
    let allAgentsExplicitlyAttributed = node.agents.length > 0;
    for (const agent of node.agents) {
      const pane = typeof agent.parentPaneKey === 'string' ? agent.parentPaneKey.trim() : '';
      if (!pane) {
        allAgentsExplicitlyAttributed = false;
        continue;
      }
      // Explicit dispatch provenance takes precedence over creator lineage.
      const siblingPane = targetPaneKeys.size > 0 ? !targetPaneKeys.has(pane) : knownSiblingPaneKeys.has(pane);
      if (!siblingPane) return undefined;
    }
    const paneAttributedWithSingleTerminal = allAgentsExplicitlyAttributed && targetPaneKeys.size > 0 && handles.length <= 1;
    if (!paneAttributedWithSingleTerminal && node.unverifiableTerminalCount !== 0) return undefined;
    return allAgentsExplicitlyAttributed ? 'pane' : 'creator';
  };
  // Return the worktrees that still need creator proof, or undefined if any node
  // in the subtree cannot be attributed to a sibling.
  const siblingLineageCandidates = (subtree: Workspace): Workspace[] | undefined => {
    const candidates: Workspace[] = [];
    const seen = new Set<string>();
    const visit = (node: Workspace): boolean => {
      if (seen.has(node.worktreeId)) return false;
      seen.add(node.worktreeId);
      const attribution = siblingAttribution(node);
      if (!attribution) return false;
      if (attribution === 'creator') candidates.push(node);
      for (const id of node.childWorktreeIds ?? []) {
        const descendant = rowsById.get(id);
        if (!descendant || !visit(descendant)) return false;
      }
      return true;
    };
    return visit(subtree) ? candidates : undefined;
  };
  let childCreators: Map<string, string> | undefined;
  let ownerByTerminalHandle: Promise<Map<string, Workspace | null>> | undefined;
  const siblingProofMemo = new Map<string, boolean>();
  const indexDescendantTerminalOwners = async (): Promise<Map<string, Workspace | null>> => {
    const owners = new Map<string, Workspace | null>();
    for (const descendant of descendantsOfRoot()) {
      if (!siblingAttribution(descendant)) continue;
      for (const handle of await readWorktreeTerminalHandles(descendant)) {
        owners.set(handle, owners.has(handle) ? null : descendant);
      }
    }
    return owners;
  };
  const isWorktreeProvenSibling = async (node: Workspace, visiting = new Set<string>()): Promise<boolean> => {
    const id = node.worktreeId;
    const cached = siblingProofMemo.get(id);
    if (cached !== undefined) return cached;
    const remember = (result: boolean): boolean => {
      siblingProofMemo.set(id, result);
      return result;
    };
    if (visiting.has(id)) return remember(false);
    const attribution = siblingAttribution(node);
    if (!attribution) return remember(false);
    if (attribution === 'pane') return remember(true);
    const creator = childCreators?.get(id);
    if (!allowedSiblingHandle(creator)) return remember(false);
    if (siblingHandles.has(creator) || options?.excludedHandles?.has(creator)) return remember(true);
    const owners = await (ownerByTerminalHandle ??= indexDescendantTerminalOwners());
    const owner = owners.get(creator);
    if (!owner || owner.worktreeId === id) return remember(false);
    // A unique owner is the only next edge, so this walk can share one visiting set.
    visiting.add(id);
    return remember(await isWorktreeProvenSibling(owner, visiting));
  };
  for (const id of root.childWorktreeIds ?? []) {
    const child = rowsById.get(id);
    if (!child) return notIdle();
    const lineageCandidates = slotScoped ? siblingLineageCandidates(child) : undefined;
    if (lineageCandidates?.length === 0) continue;
    if (await isDescendantQuiescent(child)) continue;
    if (canExcludeByLineage && lineageCandidates) {
      try {
        childCreators ??= await readOrcaChildCreators(runner, command, cwd, signal);
        let proven = true;
        for (const candidate of lineageCandidates) {
          if (await isWorktreeProvenSibling(candidate)) continue;
          proven = false;
          break;
        }
        if (proven) continue;
      } catch (error) {
        if (options?.throwOnProbeError) throw error;
        return notIdle();
      }
    }
    return notIdle();
  }
  if (options?.requireTarget && runner === runCommand && typeof rootTarget === 'string') {
    return checkOrcaTerminalComposer(runner, command, rootTarget, cwd, signal, {
      requirePreview: true, expectedAgent: options.agent,
    });
  }
  return { idle: true, reason: undefined };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/** Bounded `terminal wait --for tui-idle`; timeout, busy, or command failure is never idle. */
export async function waitOrcaTerminalIdle(
  runner: CommandRunner,
  command: string,
  terminalHandle: string,
  cwd?: string,
  timeoutMs = 5000,
  signal?: AbortSignal,
  options?: OrcaProbeOptions,
): Promise<OrcaIdleResult> {
  try {
    const waited = await runner({
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
    }).catch((error: unknown) => {
      // The real runner rejects nonzero exits; Orca may still have returned a structured timeout.
      if (!isRecord(error) || typeof error.exitCode !== 'number' || typeof error.stdout !== 'string') throw error;
      return {
        stdout: error.stdout,
        stderr: typeof error.stderr === 'string' ? error.stderr : undefined,
        exitCode: error.exitCode,
      };
    });
    const stderr = waited.stderr?.trim();
    let response: unknown;
    try {
      response = JSON.parse(waited.stdout);
    } catch (error) {
      const diagnostic = stderr || waited.stdout.trim();
      if (diagnostic) throw new SyntaxError(`Invalid Orca terminal wait response: ${diagnostic}`, { cause: error });
      throw error;
    }
    if (!isRecord(response) || typeof response.ok !== 'boolean') {
      throw new SyntaxError('Invalid Orca terminal wait response');
    }
    const successfulExit = waited.exitCode === undefined || waited.exitCode === 0;
    if (response.ok) {
      if (!successfulExit) {
        throw new Error(`${command} exited ${waited.exitCode}: ${stderr || waited.stdout.trim()}`);
      }
      return { idle: true };
    }
    const detail = isRecord(response.error) ? response.error : undefined;
    const code = typeof detail?.code === 'string' ? detail.code : undefined;
    const isBoundedTimeout = response.error === undefined ? successfulExit : code === 'timeout';
    if (isBoundedTimeout) return { idle: false, reason: 'Terminal is busy' };
    const diagnostic = (typeof detail?.message === 'string' && detail.message.trim()) || stderr;
    throw new Error([code, diagnostic].filter(Boolean).join(': ') || `Orca terminal wait failed (exit ${waited.exitCode ?? 'unknown'})`);
  } catch (error) {
    if (options?.throwOnProbeError) throw error;
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

async function checkOrcaTerminalComposer(
  runner: CommandRunner, command: string, terminalHandle: string, cwd?: string, signal?: AbortSignal,
  options?: { requirePreview?: boolean; expectedAgent?: SupportedOrcaAgent },
): Promise<OrcaIdleResult> {
  const terminal = await readOrcaTerminal(runner, command, terminalHandle, cwd, signal);
  if (terminal && (terminal.connected !== true || terminal.writable === false))
    return { idle: false, reason: 'Terminal is disconnected or not writable' };
  if (options?.requirePreview && typeof terminal?.preview !== 'string')
    return { idle: false, reason: 'Terminal composer is unverifiable' };
  if (typeof terminal?.preview === 'string') {
    const agentUnready = orcaAgentReadinessFailure(terminal, options?.expectedAgent);
    if (agentUnready) return agentUnready;
    if (!isOrcaComposerEmpty(terminal.preview, terminal.agentIdentity))
      return { idle: false, reason: 'Terminal composer has unsent draft' };
  }
  return { idle: true };
}

/** Every terminal-only target receives the same composer protection as workspace targets. */
export async function probeOrcaTerminalIdle(
  runner: CommandRunner,
  command: string,
  terminalHandle: string,
  cwd?: string,
  signal?: AbortSignal,
  options?: OrcaProbeOptions & { verifyComposerLast?: boolean; expectedAgent?: SupportedOrcaAgent },
): Promise<OrcaIdleResult> {
  try {
    const expectedAgent = options?.verifyComposerLast ? options.expectedAgent : undefined;
    const composer = await checkOrcaTerminalComposer(runner, command, terminalHandle, cwd, signal, { expectedAgent });
    if (!composer.idle) return composer;
    const result = await waitOrcaTerminalIdle(runner, command, terminalHandle, cwd, 1, signal, options);
    if (result.idle && options?.verifyComposerLast && runner === runCommand) {
      return await checkOrcaTerminalComposer(runner, command, terminalHandle, cwd, signal, {
        requirePreview: true, expectedAgent,
      });
    }
    return result.idle
      ? result
      : {
          idle: false,
          reason: `Terminal idle probe failed: ${result.reason ?? 'Terminal is busy'}`,
        };
  } catch (error) {
    if (options?.throwOnProbeError) throw error;
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
    spawned = await orcaMutationRunner(runner, cwd)({
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

function terminalPaneKeys(terminal: OrcaTerminalRow): string[] {
  const keys: string[] = [];
  if (typeof terminal.tabId === 'string' && terminal.tabId && typeof terminal.leafId === 'string' && terminal.leafId) {
    keys.push(`${terminal.tabId}:${terminal.leafId}`);
  }
  if (typeof terminal.recordedPaneKey === 'string' && terminal.recordedPaneKey) keys.push(terminal.recordedPaneKey);
  return keys;
}

function isUsableTerminal(terminal: OrcaTerminalRow): boolean {
  return terminal.connected === true && terminal.writable !== false;
}

function matchesPane(terminal: OrcaTerminalRow, paneKey?: string): boolean {
  return isUsableTerminal(terminal) && paneKey !== undefined && terminalPaneKeys(terminal).includes(paneKey);
}

export async function inspectOrcaWorkspaceTerminal(
  runner: CommandRunner,
  command: string,
  cwd: string,
  terminalHandle?: string,
  signal?: AbortSignal,
  preferredPaneKey?: string,
  advisory = false,
): Promise<'live' | 'unusable' | 'absent'> {
  const inventory = JSON.parse(await readOrcaTerminalList(runner, command, cwd, cwd, signal, advisory));
  if (inventory.ok !== true || inventory.result?.truncated || !Array.isArray(inventory.result?.terminals)) {
    throw new Error('Incomplete Orca terminal inventory');
  }
  let presence: 'unusable' | 'absent' = 'absent';
  for (const row of inventory.result.terminals as OrcaTerminalRow[]) {
    if (!row || !((terminalHandle && String(row.handle) === terminalHandle) ||
      (preferredPaneKey && terminalPaneKeys(row).includes(preferredPaneKey)))) continue;
    if (isUsableTerminal(row)) return 'live';
    presence = 'unusable';
  }
  return presence;
}

/** A persisted handle is reusable only while Orca still reports it connected and writable. */
export async function isOrcaWorkspaceTerminalLive(
  runner: CommandRunner,
  command: string,
  cwd: string,
  terminalHandle?: string,
  signal?: AbortSignal,
  preferredPaneKey?: string,
  advisory = false,
): Promise<boolean> {
  return (await inspectOrcaWorkspaceTerminal(runner, command, cwd, terminalHandle, signal, preferredPaneKey, advisory)) === 'live';
}

/** Read-only terminal selection: callers must fence the returned launch plan before executing it. */
export async function planOrcaWorkspaceTerminal(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
  options?: { agent?: SupportedOrcaAgent; interactiveCommand?: string; resumeCommand?: string; sessionId?: string; preferredHandle?: string; preferredPaneKey?: string; dedicatedTab?: boolean; excludedHandles?: ReadonlySet<string> },
): Promise<{ kind: 'existing'; handle: string } | { kind: 'shell'; handle: string } | { kind: 'create' }> {
  const inventory = JSON.parse(await readOrcaTerminalList(runner, command, cwd, cwd, signal));
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
    const bound = terminals.find(t => String(t.handle) === preferredHandle && isUsableTerminal(t));
    if (bound) return { handle: String(bound.handle), kind: 'existing' };
  }
  const pane = terminals.find(t => t && matchesPane(t, options?.preferredPaneKey));
  if (pane) return { kind: 'existing', handle: String(pane.handle) };
  if (options?.dedicatedTab || options?.sessionId?.trim() || options?.preferredPaneKey?.trim()) return { kind: 'create' };
  const candidates = reusableTerminalCandidates(terminals, options?.agent);
  if (!candidates) return { kind: 'create' };
  const [freshest] = candidates.terminals.sort(
    (a, b) => (Number(b.lastOutputAt) || 0) - (Number(a.lastOutputAt) || 0),
  );
  return { kind: candidates.kind, handle: String(freshest.handle) };
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

/** Orca refused the send before writing any bytes to the terminal. */
export class OrcaTerminalSendRefusedError extends Error {
  readonly refusedReason?: string;

  constructor(refusedReason?: string) {
    super(refusedReason ? `Orca terminal send refused: ${refusedReason}` : 'Orca terminal send refused');
    this.refusedReason = refusedReason;
  }
}

function orcaTerminalSendRefusal(send: unknown): OrcaTerminalSendRefusedError | undefined {
  if (!isRecord(send) || send.accepted !== false) return undefined;
  return new OrcaTerminalSendRefusedError(typeof send.refusedReason === 'string' ? send.refusedReason : undefined);
}

/** Send one line (plus Enter) to a terminal; the caller must have proven it idle first. */
export async function sendOrcaTerminalText(
  runner: CommandRunner,
  command: string,
  terminalHandle: string,
  text: string,
  options: { cwd?: string; signal?: AbortSignal; waitSubmit?: number; expectedAgent?: SupportedOrcaAgent } = {},
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
  const mutationRunner = orcaMutationRunner(runner, options.cwd);
  const sendOnce = async (request: CommandRequest): Promise<unknown> => {
    let returned: Awaited<ReturnType<CommandRunner>> | undefined;
    let output;
    try {
      output = await checkedCommand(async request => {
        returned = await mutationRunner(request);
        return returned;
      }, request);
    } catch (error) {
      // Orca emits its refusal receipt before exiting 1. Preserve stdout whether the runner
      // returned the non-zero status or attached it to a rejection.
      const failed = returned ?? (error as { stdout?: unknown; exitCode?: unknown } | null);
      if (typeof failed?.exitCode === 'number' && failed.exitCode !== 0 && typeof failed.stdout === 'string') {
        let parsed;
        try {
          parsed = JSON.parse(failed.stdout);
        } catch {
          throw error;
        }
        const refusal = orcaTerminalSendRefusal(parsed?.result?.send);
        if (refusal) throw refusal;
      }
      throw error;
    }
    const parsed = JSON.parse(output.stdout);
    if (parsed?.ok === false) throw new Error('Orca terminal send failed');
    const send = parsed?.result?.send;
    const refusal = orcaTerminalSendRefusal(send);
    if (refusal) throw refusal;
    return parsed;
  };
  let parsed: unknown;
  try {
    parsed = await sendOnce({ command, args, cwd: options.cwd, signal: options.signal });
  } catch (error) {
    const missingSender = 'no_active_sender_terminal';
    const canUseSelf = error instanceof OrcaTerminalSendRefusedError
      ? error.refusedReason === missingSender
      : !(error instanceof SyntaxError) && String(error).includes(missingSender);
    if (!canUseSelf) throw error;
    parsed = await sendOnce({
      command,
      args: [...args, '--allow-self'],
      cwd: options.cwd,
      signal: options.signal,
      env: { ...process.env, ORCA_TERMINAL_ID: terminalHandle },
    });
  }
  if (options.waitSubmit === undefined && options.expectedAgent === undefined) return;
  if (!isRecord(parsed) || parsed.ok !== true) throw new Error('Orca terminal send receipt did not verify input acceptance');
  // Older injected runners can acknowledge a send with only an OK envelope.
  if (!Object.hasOwn(parsed, 'result')) return;
  const send = isRecord(parsed.result) ? parsed.result.send : undefined;
  if (!isRecord(send) || send.accepted !== true) throw new Error('Orca terminal send receipt did not verify input acceptance');
  const prompt = send.prompt;
  if (!isRecord(prompt)) throw new Error('Orca terminal send is missing prompt receipt');
  const { provider, observation, stages } = prompt;
  const requestId = prompt.requestId ?? send.requestId ?? 'unknown';
  if (observation === 'permission' || observation === 'incarnation_replaced') {
    throw new Error(`Orca prompt delivery unverified: ${observation} (request ${requestId})`);
  }
  if (provider === 'old-host') return;
  if (typeof provider !== 'string' || !provider.trim()) {
    throw new Error(`Orca prompt receipt is missing provider (request ${requestId})`);
  }
  if (options.expectedAgent && !matchesOrcaAgentIdentity(provider, options.expectedAgent)) {
    throw new Error(`Orca prompt provider ${provider} did not verify delivery to ${options.expectedAgent} (request ${requestId})`);
  }
  if (observation !== (provider === 'unsupported' ? 'unsupported' : 'supported')) {
    throw new Error(`Orca prompt receipt has invalid observation (request ${requestId}, provider ${provider}, observation ${observation})`);
  }
  if (!Array.isArray(stages) || stages.some(stage => typeof stage !== 'string')) {
    throw new Error(`Orca prompt receipt has invalid stages (request ${requestId}, provider ${provider}, stages ${JSON.stringify(stages)})`);
  }
  if (options.waitSubmit !== undefined && provider !== 'unsupported' && !stages.includes('turn_started')) {
    throw new Error(`Orca prompt turn not observed (request ${requestId}, provider ${provider}, observation ${observation}, stages ${JSON.stringify(stages)})`);
  }
}

/** A deterministic Orca refusal (for example a pinned tab), which retrying cannot resolve. */
export class OrcaTerminalCloseRejectedError extends Error {}

/**
 * Close a terminal whose replacement has already been committed. An already-absent terminal
 * settles the retirement; a deterministic refusal is typed so the caller can park instead of
 * retrying forever; an unverifiable failure propagates so the caller keeps the handle durable.
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
  let result: { stdout: string; stderr?: string; exitCode?: number } | undefined;
  let failure: unknown;
  try {
    result = await orcaMutationRunner(runner, cwd)({
      command,
      args: ['terminal', 'close', '--terminal', handle, '--json'],
      cwd,
      signal,
    });
  } catch (error) {
    failure = error;
  }
  let parsed: unknown;
  try {
    // `runCommand` rejects on a non-zero exit and attaches the captured output, so a refusal has
    // to be read from the thrown error exactly like a returned result.
    const rejected = failure as { stdout?: unknown; exitCode?: unknown } | null;
    const stdout = typeof rejected?.stdout === 'string' && rejected.exitCode !== null
      ? rejected.stdout.trim()
      : result?.stdout;
    parsed = stdout ? JSON.parse(stdout) : undefined;
  } catch {
    parsed = undefined;
  }
  const refused = Boolean(parsed) && typeof parsed === 'object' && !Array.isArray(parsed) &&
    (parsed as { ok?: unknown }).ok === false;
  if (
    failure === undefined &&
    result !== undefined &&
    (result.exitCode === undefined || result.exitCode === 0) &&
    !refused
  ) {
    return;
  }
  const closeError = failure ?? new Error(`Orca terminal close failed: ${handle}`);
  // The close did not confirm. A terminal already absent from the workspace inventory satisfies
  // the one-live-terminal invariant, so only a still-present terminal keeps the retirement open.
  let inventory: unknown;
  try {
    inventory = JSON.parse(await readOrcaTerminalList(runner, command, cwd, cwd, signal));
  } catch {
    throw closeError;
  }
  if (!inventory || typeof inventory !== 'object' || Array.isArray(inventory)) throw closeError;
  const parsedInventory = inventory as { ok?: unknown; result?: { truncated?: unknown; terminals?: unknown } };
  // A truncated inventory cannot prove absence: the missing handle may simply be cut off.
  if (parsedInventory.ok !== true || parsedInventory.result?.truncated || !Array.isArray(parsedInventory.result?.terminals)) throw closeError;
  const stillPresent = (parsedInventory.result.terminals as unknown[]).some(
    (row) => row !== null && typeof row === 'object' && String((row as { handle?: unknown }).handle) === handle,
  );
  if (!stillPresent) return;
  if (refused) {
    const detail = (parsed as { error?: unknown }).error;
    const code = detail && typeof detail === 'object' && !Array.isArray(detail)
      ? (detail as { code?: unknown }).code : undefined;
    throw new OrcaTerminalCloseRejectedError(
      `Orca refused to close terminal ${handle}${typeof code === 'string' && code ? ` (${code})` : ''}`,
    );
  }
  throw closeError;
}

/** Set the visible worktree card comment; `current` is used when no workspace path is known. */
export async function setOrcaWorkspaceComment(
  runner: CommandRunner,
  command: string,
  workspacePath: string | undefined,
  comment: string,
  options: { cwd?: string; signal?: AbortSignal } = {},
): Promise<void> {
  const output = await checkedCommand(orcaMutationRunner(runner, workspacePath), {
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

export async function listOrcaWorkspaces(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
): Promise<{ path?: unknown }[]> {
  return readOrcaWorkspaceList(runner, command, cwd, signal);
}

interface OrcaListedWorktree {
  path?: unknown;
  id?: unknown;
  worktreeId?: unknown;
  lineage?: { worktreeId?: unknown; createdByTerminalHandle?: unknown };
}

async function readOrcaWorkspaceList(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
): Promise<OrcaListedWorktree[]> {
  const listed = await checkedCommand(runner, {
    command,
    args: ['worktree', 'list', '--json'],
    cwd: existsSync(cwd) ? cwd : undefined,
    signal,
  });
  const raw = JSON.parse(listed.stdout);
  const rows = raw?.result?.worktrees ?? raw?.worktrees ?? (Array.isArray(raw?.result) ? raw.result : undefined);
  if (raw?.ok === false || raw?.result?.truncated || !Array.isArray(rows)) {
    throw new Error('Incomplete Orca workspace inventory from Orca worktree list');
  }
  return rows;
}

async function readOrcaChildCreators(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
): Promise<Map<string, string>> {
  const worktrees = await readOrcaWorkspaceList(runner, command, cwd, signal);
  const creators = new Map<string, string>();
  for (const worktree of worktrees) {
    const handle = worktree?.lineage?.createdByTerminalHandle;
    const creator = typeof handle === 'string' ? handle.trim() : '';
    if (!creator) continue;
    for (const id of [worktree.id, worktree.worktreeId, worktree.lineage?.worktreeId]) {
      if (typeof id === 'string' && id) creators.set(id, creator);
    }
  }
  return creators;
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
  const rows = await listOrcaWorkspaces(runner, command, cwd, options.signal);
  const canonical = (path: string): string => {
    const absolute = resolve(path);
    if (existsSync(absolute)) return realpathSync(absolute);
    const parent = dirname(absolute);
    return parent === absolute ? absolute : join(canonical(parent), basename(absolute));
  };
  const found = rows.find(
    (row): row is { path: string } => typeof row?.path === 'string' && canonical(row.path) === canonical(options.workspacePath),
  );
  if (found) return { workspacePath: found.path, created: false };
  if (!options.repoRoot)
    throw new Error('Creating a fixed Orca workspace requires a receiver-local repoRoot');
  const created = await checkedCommand(orcaMutationRunner(runner), {
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
