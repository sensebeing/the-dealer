import { existsSync, realpathSync } from 'node:fs';
import { resolve } from 'node:path';
import {
  OrcaTerminalCloseRejectedError,
  OrcaTerminalLaunchRejectedError,
  readOrcaTerminal,
  checkOrcaWorkspaceIdle,
  closeOrcaWorkspaceTerminal,
  ensureOrcaWorkspace,
  inspectOrcaWorkspaceTerminal,
  isOrcaWorkspaceTerminalLive,
  listOrcaWorkspaces,
  planOrcaWorkspaceTerminal,
  probeOrcaTerminalIdle,
  sendOrcaTerminalText,
  setOrcaWorkspaceComment,
  spawnOrcaWorkspaceTerminal,
  waitOrcaTerminalIdle,
  findOrcaWorktreeByName,
} from './workspace.ts';
import type { OrcaIdleResult, SupportedOrcaAgent } from './workspace.ts';
import { buildOrcaAgentLaunchCommand, isOrcaLaunchConfigCompatible } from './workspace.ts';
import { checkedCommand, CommandNotStartedError, runCommand } from '../subprocess/index.ts';
import type { CommandRunner } from '../subprocess/index.ts';
import type { AgentConversationMode } from '../core-types/index.ts';
import type { MailboxCheckResult, MailboxStore, WakeTargetState } from '../mailbox/store.ts';
import { baseStreamKeyOf, normalizeMailboxId } from '../mailbox/store.ts';
import {
  buildWakeupPointer,
  maxPreviewSequence,
  recordWakeFailure,
  settleWakeDeliveryFailure,
  SettledWakeLaunchError,
} from '../mailbox/wakeup.ts';
import type { SlotProbeResult, WakeupDriver, WakeupDriverContext, WakeupDriverOutcome, WakeupProbeContext } from '../mailbox/wakeup.ts';

export interface OrcaTerminalDriverConfig {
  orcaCommand?: string;
  agent?: SupportedOrcaAgent;
  interactiveCommand?: string;
  sessionId?: string;
  paneKey?: string;
  resumeCommand?: string;
  configuredSessionId?: string;
  configuredTerminalHandle?: string;
  retiredSessionId?: string;
  /** Terminal replaced by the current binding; persisted until its close is confirmed. */
  retiredTerminalHandle?: string;
  worktreeId?: string;
  workspacePath?: string;
  terminalHandle?: string;
  repoRoot?: string;
  autoCreateWorkspace?: boolean;
  workspaceComment?: string;
}
export interface OrcaRunDriverConfig extends OrcaTerminalDriverConfig {
  runId?: string;
  dedicatedSpawn?: boolean;
}

type OrcaWakeTarget = WakeTargetState & OrcaRunDriverConfig;

export function readOrcaDriverConfig(target: Pick<WakeTargetState, 'driverConfig'>): OrcaRunDriverConfig {
  const config = target.driverConfig ?? {};
  for (const field of [
    'orcaCommand',
    'interactiveCommand', 'sessionId', 'paneKey', 'resumeCommand', 'configuredSessionId', 'configuredTerminalHandle', 'retiredSessionId',
    'retiredTerminalHandle',
    'worktreeId',
    'workspacePath',
    'terminalHandle',
    'repoRoot',
    'workspaceComment',
    'runId',
  ]) {
    if (config[field] !== undefined && typeof config[field] !== 'string')
      throw new Error(`Invalid Orca driver configuration: ${field}`);
  }
  if (config.agent !== undefined && !['antigravity', 'claude', 'codex'].includes(config.agent as string))
    throw new Error('Invalid Orca driver agent');
  if (config.autoCreateWorkspace !== undefined && typeof config.autoCreateWorkspace !== 'boolean')
    throw new Error('Invalid Orca workspace creation policy');
  if (config.dedicatedSpawn !== undefined && typeof config.dedicatedSpawn !== 'boolean')
    throw new Error('Invalid Orca dedicated spawn policy');
  return config as OrcaRunDriverConfig;
}

export function serializeOrcaDriverConfig(config: OrcaRunDriverConfig): Record<string, unknown> {
  const value = Object.fromEntries(Object.entries(config).filter(([, entry]) => entry !== undefined));
  readOrcaDriverConfig({ driverConfig: value });
  return value;
}

function readOrcaWakeTarget(target: WakeTargetState): OrcaWakeTarget {
  return { ...readOrcaDriverConfig(target), ...target };
}

function isDedicatedSpawn(target: OrcaWakeTarget): boolean {
  return target.dedicatedSpawn === true || target.mailboxId.startsWith('task:');
}

export interface OrcaWakeupDriverOptions {
  commandRunner?: CommandRunner;
  orcaCommand?: string;
  /** Authoritative probe supplied by an embedded host or test harness. */
  idleCheck?: () => Promise<OrcaIdleResult>;
}

/** Idle verdict plus the delivery plan it was measured against. */
interface WakeupIdleDecision extends OrcaIdleResult {
  /** The pointer needs its own session tab, so retained composers were not inspected. */
  spawnFreshSession?: boolean;
}

/** Fixed Orca workspaces open one independent session tab per wave unless configured otherwise. */
export const DEFAULT_ORCA_WORKSPACE_ROTATE_AFTER_WAVES = 1;

export interface OrcaWaveRotateAfterWavesInput {
  mode?: AgentConversationMode;
  /** Threshold configured on the resolved topic binding. */
  bindingRotateAfterWaves?: number;
  /** Threshold configured once for the whole wave executor/host. */
  waveRotateAfterWaves?: number;
  /** Threshold supplied by a deployment whose receiver configured no local target. */
  fallbackRotateAfterWaves?: number;
  /** Whether the receiver manages a fixed Orca workspace that can host session tabs. */
  managesWorkspace: boolean;
  /** An Orca Run owns its coordinator session, so it never rotates tabs. */
  runId?: string;
  /** An explicitly pinned terminal keeps its conversation instead of rotating. */
  pinnedTerminalHandle?: string;
}

/**
 * The single owner of the managed Orca fixed-workspace rotation policy. Preset generation,
 * binding refresh, and wave enqueue all resolve their effective threshold here so the rule can
 * never diverge between entry paths.
 */
export function resolveOrcaWaveRotateAfterWaves(input: OrcaWaveRotateAfterWavesInput): number | undefined {
  const explicit =
    input.bindingRotateAfterWaves ?? input.waveRotateAfterWaves ?? input.fallbackRotateAfterWaves;
  if (explicit !== undefined) {
    return explicit;
  }
  if (input.mode === 'fixed') return undefined;
  return input.managesWorkspace && !input.runId && !input.pinnedTerminalHandle
    ? DEFAULT_ORCA_WORKSPACE_ROTATE_AFTER_WAVES
    : undefined;
}

/**
 * A completed wave (or rotation window) without an outstanding lease is the single rotation
 * trigger: the next wakeup must open its own session tab instead of reusing the retained one.
 */
function needsSessionRotation(
  target: Pick<WakeTargetState, 'driver' | 'rotateAfterWaves' | 'completedWaves'>,
  preview: Pick<MailboxCheckResult, 'deliveryId'>,
): boolean {
  return (
    !preview.deliveryId &&
    target.driver === 'orca-terminal' &&
    target.rotateAfterWaves !== undefined &&
    target.completedWaves >= target.rotateAfterWaves
  );
}

function orcaWakeTargetPatch(patch: Partial<OrcaWakeTarget>): Partial<WakeTargetState> {
  const { driver, completedWaves, ...driverConfig } = patch;
  return { ...(driver !== undefined ? { driver } : {}), ...(completedWaves !== undefined ? { completedWaves } : {}), driverConfig };
}

export function buildLaunchedOrcaWakeTargetPatch(
  _mailboxId: string,
  current: WakeTargetState,
  options: { handle: string; resetCompletedWaves?: boolean },
): Partial<WakeTargetState> {
  const binding = readOrcaDriverConfig(current);
  return orcaWakeTargetPatch({
    terminalHandle: options.handle, paneKey: undefined,
    ...(options.resetCompletedWaves ? {
      completedWaves: 0, sessionId: undefined,
      retiredSessionId: binding.sessionId ?? binding.configuredSessionId ?? binding.retiredSessionId,
      // Rotation commits the replacement and the tab it replaces together, so a failed close can be
      // retried from durable state instead of leaking a second live terminal into the slot.
      retiredTerminalHandle: binding.terminalHandle && binding.terminalHandle !== options.handle
        ? binding.terminalHandle
        : binding.retiredTerminalHandle,
    } : {}),
  });
}

export interface CallerOrcaIdentity {
  mailboxId: string;
  workspacePath?: string;
  worktreeId?: string;
  terminalHandle?: string;
  paneKey?: string;
  agent?: SupportedOrcaAgent;
  sessionId?: string;
}

function sameCallerWorkspace(a?: string, b?: string): boolean {
  if (!a || !b || resolve(a) === resolve(b)) return true;
  try { return realpathSync(a) === realpathSync(b); } catch { return false; }
}

/**
 * Handles no other mailbox may select. A retired terminal stays reserved until its owner confirms
 * the close, so a sibling flush cannot bind a tab that is about to disappear; the close boundary
 * re-reads the same ledger without retirements to decide whether the tab is still ours to close.
 */
function claimedOrcaTerminalHandles(
  store: MailboxStore,
  mailboxId: string,
  bound: OrcaTerminalDriverConfig,
  options: { includeRetired?: boolean } = {},
): Set<string> {
  const includeRetired = options.includeRetired !== false;
  const handles = new Set<string>();
  for (const item of store.listWakeTargets()) {
    if (item.mailboxId === mailboxId || item.mailboxId.startsWith('terminal:') || item.driver !== 'orca-terminal') continue;
    const { terminalHandle, retiredTerminalHandle, workspacePath, paneKey } = readOrcaDriverConfig(item);
    if (includeRetired && retiredTerminalHandle) handles.add(retiredTerminalHandle);
    if (!terminalHandle) continue;
    const isSessionAlias = mailboxId.startsWith('terminal:') ||
      mailboxId.startsWith('worktree:') || item.mailboxId.startsWith('worktree:');
    // A recorded pane retains its session identity when Orca replaces the terminal handle.
    const sameTab = terminalHandle === bound.terminalHandle || Boolean(paneKey && paneKey === bound.paneKey);
    if (isSessionAlias && sameTab && sameCallerWorkspace(bound.workspacePath, workspacePath)) continue;
    handles.add(terminalHandle);
  }
  return handles;
}

function callerOwnsExplicitOrcaWakeTarget(
  id: string, existing: WakeTargetState | undefined, caller: CallerOrcaIdentity, store?: MailboxStore,
): boolean {
  if (id === caller.mailboxId) return true;
  if (caller.terminalHandle && id === `terminal:${caller.terminalHandle}`) {
    return !existing || existing.driver === 'passive' || sameCallerWorkspace(readOrcaDriverConfig(existing).workspacePath, caller.workspacePath);
  }
  if (existing?.driver !== 'orca-terminal') return false;
  const bound = readOrcaDriverConfig(existing);
  if (!sameCallerWorkspace(bound.workspacePath, caller.workspacePath)) return false;
  if (caller.terminalHandle && store && claimedOrcaTerminalHandles(store, id, bound).has(caller.terminalHandle)) return false;
  return Boolean((caller.terminalHandle && bound.terminalHandle === caller.terminalHandle)
    || (caller.paneKey && bound.paneKey === caller.paneKey));
}

export function reconcileCallerOrcaWakeTarget(
  store: MailboxStore, mailboxId: string, caller: CallerOrcaIdentity | undefined,
  options: { requireBoundOwner?: boolean } = {},
): WakeTargetState | undefined {
  if (!caller) return undefined;
  const id = normalizeMailboxId(mailboxId);
  const existing = store.getWakeTarget(id);
  if (existing?.driver === 'orca-run' || existing?.driver === 'agent-host') return undefined;
  if (options.requireBoundOwner && !callerOwnsExplicitOrcaWakeTarget(id, existing, caller, store)) return undefined;
  const updated = store.upsertWakeTarget(id, resolveCallerOrcaWakeTargetPatch(existing, caller));
  const handle = readOrcaDriverConfig(updated).terminalHandle;
  if (updated.rotateAfterWaves !== undefined && handle) pinOrcaTerminalSessionMailbox(store, id, handle);
  return updated;
}

/** Single owner of caller-driven Orca conversation identity transitions. */
export function resolveCallerOrcaWakeTargetPatch(
  snapshot: WakeTargetState | undefined,
  caller: { workspacePath?: string; worktreeId?: string; terminalHandle?: string; paneKey?: string; agent?: SupportedOrcaAgent; sessionId?: string },
): Partial<WakeTargetState> {
  const existing = snapshot && readOrcaWakeTarget(snapshot);
  const switchedIdentity =
    (caller.workspacePath && existing?.workspacePath && caller.workspacePath !== existing.workspacePath) ||
    (caller.sessionId && existing?.sessionId && caller.sessionId !== existing.sessionId) ||
    (caller.paneKey && existing?.paneKey && caller.paneKey !== existing.paneKey) ||
    (caller.agent && existing?.agent && caller.agent !== existing.agent) ||
    (caller.terminalHandle && existing?.terminalHandle && caller.terminalHandle !== existing.terminalHandle &&
      (!caller.paneKey || !existing.paneKey || caller.paneKey !== existing.paneKey) &&
      (!caller.sessionId || !existing.sessionId || caller.sessionId !== existing.sessionId));
  const nextAgent = switchedIdentity ? caller.agent : (caller.agent ?? existing?.agent);
  const clearLaunchConfig = (existing?.interactiveCommand !== undefined || existing?.resumeCommand !== undefined) &&
    !isOrcaLaunchConfigCompatible({ agent: nextAgent, previousAgent: existing?.agent,
      interactiveCommand: existing?.interactiveCommand, resumeCommand: existing?.resumeCommand });
  return orcaWakeTargetPatch({
    driver: 'orca-terminal',
    ...(caller.workspacePath ? { workspacePath: caller.workspacePath } : {}),
    ...(caller.worktreeId ? { worktreeId: caller.worktreeId } : {}),
    ...(clearLaunchConfig ? { interactiveCommand: undefined, resumeCommand: undefined } : {}),
    ...(switchedIdentity ? {
      completedWaves: 0, terminalHandle: caller.terminalHandle, paneKey: caller.paneKey, sessionId: caller.sessionId, agent: caller.agent,
      retiredTerminalHandle: undefined,
    } : {
      ...(caller.terminalHandle ? { terminalHandle: caller.terminalHandle } : {}),
      ...(caller.paneKey ? { paneKey: caller.paneKey } : {}),
      ...(caller.sessionId ? { sessionId: caller.sessionId } : {}),
      ...(caller.agent ? { agent: caller.agent } : {}),
    }),
  });
}

/** Apply configuration once; rotation retires the runtime session without erasing its provenance. */
export function resolveConfiguredOrcaWakeTargetPatch(
  snapshot: WakeTargetState | undefined,
  input: {
    mailboxId?: string; runId?: string; workspacePath?: string; configuredTerminalHandle?: string;
    configuredAgent?: SupportedOrcaAgent; configuredSessionId?: string;
    configuredInteractiveCommand?: string; configuredResumeCommand?: string; hasTopicBinding: boolean; authoritativeConfig: boolean;
  },
): Partial<WakeTargetState> {
  const existing = snapshot && readOrcaWakeTarget(snapshot);
  const commandPatch = input.authoritativeConfig || input.configuredInteractiveCommand !== undefined
    ? { interactiveCommand: input.configuredInteractiveCommand } : {};
  const resumeCommandPatch = input.authoritativeConfig || input.configuredResumeCommand !== undefined
    ? { resumeCommand: input.configuredResumeCommand } : {};
  const nextConfiguredHandle = input.configuredTerminalHandle?.trim() || undefined;
  if (input.runId) return orcaWakeTargetPatch({
    driver: 'orca-run', completedWaves: 0, retiredSessionId: undefined, terminalHandle: nextConfiguredHandle, paneKey: undefined,
    sessionId: undefined, configuredSessionId: undefined, configuredTerminalHandle: undefined,
    retiredTerminalHandle: undefined,
    ...(input.hasTopicBinding || input.configuredAgent !== undefined ? { agent: input.configuredAgent } : {}),
    ...commandPatch, ...resumeCommandPatch,
  });
  const nextConfiguredSession = input.configuredSessionId?.trim() || undefined;
  const prevConfiguredSession = existing?.configuredSessionId;
  const effectiveWorkspacePath = input.authoritativeConfig ? input.workspacePath : (input.workspacePath ?? existing?.workspacePath);
  const sameWorkspace = existing?.driver === 'orca-terminal' && existing.workspacePath === effectiveWorkspacePath;
  const handleUnchangedSinceConfig = nextConfiguredHandle !== undefined && nextConfiguredHandle === existing?.configuredTerminalHandle;
  const samePinnedHandle = nextConfiguredHandle === undefined || nextConfiguredHandle === existing?.terminalHandle || handleUnchangedSinceConfig;
  const sameAgent = input.configuredAgent === undefined || existing?.agent === undefined || input.configuredAgent === existing.agent;
  const compatible = sameWorkspace && samePinnedHandle && sameAgent;
  const alreadyRetired = compatible && nextConfiguredSession !== undefined && nextConfiguredSession === existing?.retiredSessionId;
  const unchangedConfigured = nextConfiguredSession !== undefined && (nextConfiguredSession === prevConfiguredSession || alreadyRetired);
  const addedOrChanged = input.hasTopicBinding && nextConfiguredSession !== undefined && !unchangedConfigured;
  const removed = input.hasTopicBinding && nextConfiguredSession === undefined && prevConfiguredSession !== undefined;
  const preserved = {
    terminalHandle: handleUnchangedSinceConfig ? existing?.terminalHandle : (nextConfiguredHandle ?? existing?.terminalHandle),
    paneKey: existing?.paneKey, sessionId: existing?.sessionId,
    agent: input.configuredAgent ?? existing?.agent,
    retiredTerminalHandle: existing?.retiredTerminalHandle,
  };
  let identity: Partial<OrcaWakeTarget>;
  if (addedOrChanged) {
    const handleBelongsToPrevious = nextConfiguredHandle !== undefined &&
      (nextConfiguredHandle === existing?.terminalHandle || handleUnchangedSinceConfig);
    identity = compatible && existing?.sessionId === nextConfiguredSession ? preserved : {
      completedWaves: 0, retiredSessionId: undefined,
      terminalHandle: handleBelongsToPrevious ? undefined : nextConfiguredHandle, paneKey: undefined, sessionId: nextConfiguredSession,
      agent: input.configuredAgent ?? (sameWorkspace && sameAgent ? existing?.agent : undefined),
      retiredTerminalHandle: undefined,
    };
    identity = { ...identity, configuredSessionId: nextConfiguredSession };
  } else if (removed) {
    identity = existing?.sessionId === prevConfiguredSession || !compatible ? {
      completedWaves: 0, retiredSessionId: undefined,
      terminalHandle: nextConfiguredHandle, paneKey: undefined, sessionId: undefined,
      agent: input.configuredAgent ?? (compatible ? existing?.agent : undefined),
      retiredTerminalHandle: undefined,
    } : preserved;
    identity = { ...identity, configuredSessionId: undefined, retiredSessionId: undefined };
  } else if (unchangedConfigured) {
    identity = compatible ? { ...preserved, retiredSessionId: existing?.retiredSessionId } : {
      completedWaves: 0, retiredSessionId: undefined,
      terminalHandle: nextConfiguredHandle, paneKey: undefined,
      sessionId: nextConfiguredSession, agent: input.configuredAgent,
      retiredTerminalHandle: undefined,
    };
    identity = { ...identity, configuredSessionId: nextConfiguredSession };
  } else {
    identity = compatible ? { ...preserved, configuredSessionId: input.hasTopicBinding ? undefined : existing?.configuredSessionId } : {
      completedWaves: 0, retiredSessionId: undefined,
      terminalHandle: nextConfiguredHandle, paneKey: undefined, sessionId: undefined,
      configuredSessionId: undefined, agent: input.configuredAgent,
      retiredTerminalHandle: undefined,
    };
  }
  const clearLaunchConfig = !input.authoritativeConfig && input.configuredInteractiveCommand === undefined && input.configuredResumeCommand === undefined &&
    (existing?.interactiveCommand !== undefined || existing?.resumeCommand !== undefined) &&
    !isOrcaLaunchConfigCompatible({ agent: identity.agent, previousAgent: existing?.agent,
      interactiveCommand: existing?.interactiveCommand, resumeCommand: existing?.resumeCommand });
  return orcaWakeTargetPatch({
    driver: 'orca-terminal', ...identity,
    configuredTerminalHandle: input.hasTopicBinding || nextConfiguredHandle !== undefined ? nextConfiguredHandle : existing?.configuredTerminalHandle,
    ...commandPatch, ...resumeCommandPatch,
    ...(clearLaunchConfig ? { interactiveCommand: undefined, resumeCommand: undefined } : {}),
  });
}

/** Persist custody before any terminal creation or shell bootstrap, including their unknown outcomes. */
export async function launchFencedOrcaWorkspaceTerminal(
  store: MailboxStore,
  runner: CommandRunner,
  mailboxId: string,
  options: {
    command: string;
    workspacePath: string;
    agent?: WakeTargetState['agent'];
    interactiveCommand?: string;
    resumeCommand?: string;
    sessionId?: string;
    signal?: AbortSignal;
    now?: number;
    resetCompletedWaves?: boolean;
    expectedRevision?: number;
    shellTerminalHandle?: string;
  },
) {
  const resumeSessionId = options.resetCompletedWaves ? undefined : options.sessionId;
  const expectedRevision = options.expectedRevision ?? store.getWakeTarget(mailboxId)?.bindingRevision ?? 0;
  try {
    const launchCommand = buildOrcaAgentLaunchCommand({ ...options, sessionId: resumeSessionId });
    const fenced = store.beginWakeDelivery(mailboxId, expectedRevision);
    if (!fenced) return { committed: false as const, handle: '', raw: undefined, target: readOrcaWakeTarget(store.getWakeTarget(mailboxId)!) };
    let launched: { handle: string; raw: unknown };
    if (options.shellTerminalHandle) {
      await sendOrcaTerminalText(runner, options.command, options.shellTerminalHandle,
        launchCommand,
        { cwd: options.workspacePath, signal: options.signal });
      launched = { handle: options.shellTerminalHandle, raw: undefined };
    } else {
      launched = await spawnOrcaWorkspaceTerminal(runner, options.command, options.workspacePath, options.signal, { ...options, sessionId: resumeSessionId });
    }
    const settled = store.commitWakeLaunch(mailboxId, fenced.bindingRevision ?? 0, current =>
      buildLaunchedOrcaWakeTargetPatch(mailboxId, current, { handle: launched.handle, resetCompletedWaves: options.resetCompletedWaves }));
    return { ...launched, ...settled, target: readOrcaWakeTarget(settled.target) };
  } catch (error) {
    if (error instanceof CommandNotStartedError || error instanceof OrcaTerminalLaunchRejectedError) {
      const deferred = store.deferWakeAttempt(mailboxId, expectedRevision, error, options.now);
      if (deferred.superseded) throw new SettledWakeLaunchError('Wake launch superseded by a newer binding');
    } else {
      store.parkWakeDelivery(mailboxId, error);
    }
    throw new SettledWakeLaunchError(String(error), { cause: error });
  }
}

/**
 * Single owner for the spawned terminal's initial wake. A busy or unverifiable terminal is never
 * injected; it keeps `pendingWake` and the persisted handle so the redrive service resumes it.
 * Only the terminal send crosses the external boundary and can create an unknown-outcome fence.
 */
export async function deliverSpawnedOrcaTerminalWakeup(
  store: MailboxStore,
  runner: CommandRunner,
  mailboxId: string,
  options: { signal?: AbortSignal; now?: number } = {},
): Promise<{ pointer?: string; terminalHandle?: string; sent: boolean }> {
  const now = options.now ?? Date.now();
  let deliveryBegun = false;
  let expectedRevision = 0;
  try {
    const state = store.getWakeTarget(mailboxId);
    expectedRevision = state?.bindingRevision ?? 0;
    const target = state ? readOrcaWakeTarget(state) : undefined;
    if (!target?.terminalHandle) throw new Error(`Orca terminal wake target missing handle: ${mailboxId}`);
    const command = target.orcaCommand ?? 'orca';
    const preview = store.check({ mailboxId, peek: true });
    const ready = await waitOrcaTerminalIdle(
      runner,
      command,
      target.terminalHandle,
      target.workspacePath,
      5000,
      options.signal,
    );
    const idle = !ready.idle
      ? ready
      : target.workspacePath
        ? await checkOrcaWorkspaceIdle(runner, command, target.workspacePath, options.signal, {
            targetTerminalHandle: target.terminalHandle,
            agent: target.agent,
          })
        : await probeOrcaTerminalIdle(runner, command, target.terminalHandle, undefined, options.signal);
    if (!idle.idle) {
      store.retainPendingWakeIfCurrent(mailboxId, expectedRevision);
      return { terminalHandle: target.terminalHandle, sent: false };
    }
    const pointer = buildWakeupPointer(mailboxId, target, preview);
    if (!pointer) {
      store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview), {}, undefined, expectedRevision);
      return { terminalHandle: target.terminalHandle, sent: false };
    }
    const fenced = store.beginWakeDelivery(mailboxId, expectedRevision);
    if (!fenced) return { terminalHandle: target.terminalHandle, sent: false };
    deliveryBegun = true;
    await sendOrcaTerminalText(runner, command, target.terminalHandle, pointer, {
      cwd: target.workspacePath,
      signal: options.signal,
      waitSubmit: 5,
    });
    const completed = store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview), {}, undefined, expectedRevision);
    if (completed.superseded) return { pointer, terminalHandle: target.terminalHandle, sent: false };
    return { pointer, terminalHandle: target.terminalHandle, sent: true };
  } catch (error) {
    const fenced = store.getWakeTarget(mailboxId)?.deliveryInDoubt === true;
    if (fenced) {
      // A pre-existing fence refuses the attempt before the boundary; it keeps the original
      // unknown-outcome reason. Only this attempt's own post-boundary failure may settle it.
      if (deliveryBegun) settleWakeDeliveryFailure(store, mailboxId, error, now, expectedRevision);
    } else {
      store.deferWakeAttempt(mailboxId, expectedRevision, error, now);
    }
    throw error;
  }
}

function buildPinnedOrcaSessionDriverConfig(
  target: OrcaWakeTarget,
  existingSession: WakeTargetState | undefined,
  terminalHandle: string,
): Record<string, unknown> {
  const existing = existingSession?.driver === 'orca-terminal' ? readOrcaDriverConfig(existingSession) : undefined;
  const sameHandle = !target.terminalHandle || target.terminalHandle === terminalHandle;
  return {
    terminalHandle,
    sessionId: existing?.sessionId ?? (sameHandle ? target.sessionId : undefined),
    paneKey: existing?.paneKey ?? (sameHandle ? target.paneKey : undefined),
    configuredSessionId: undefined,
    configuredTerminalHandle: undefined,
    retiredSessionId: undefined,
    retiredTerminalHandle: undefined,
  };
}

export function pinOrcaTerminalSessionMailbox(
  store: MailboxStore,
  mailboxId: string,
  terminalHandle?: string,
  options: { inferredCallerMailboxId?: string } = {},
): string {
  const id = normalizeMailboxId(mailboxId);
  const handle = terminalHandle?.trim();
  if (!handle) return id;
  const target = store.getWakeTarget(id);
  let rotatingOwner = target?.driver === 'orca-terminal' && target.rotateAfterWaves !== undefined ? target : undefined;
  if (!rotatingOwner && id === options.inferredCallerMailboxId && id.startsWith('worktree:')) {
    rotatingOwner = store.listWakeTargets().find(item => item.driver === 'orca-terminal' && item.rotateAfterWaves !== undefined
      && readOrcaDriverConfig(item).terminalHandle === handle);
  }
  if (!rotatingOwner) return id;
  const sessionMailboxId = `terminal:${handle}`;
  // The live target can be advertised as a reply address. Wake completion pins committed routes.
  return store.pinSessionMailbox(rotatingOwner.mailboxId, sessionMailboxId, {
    driver: 'orca-terminal',
    driverConfig: buildPinnedOrcaSessionDriverConfig(readOrcaWakeTarget(rotatingOwner), store.getWakeTarget(sessionMailboxId), handle),
  });
}

class OrcaWakeDelivery {
  private readonly store: MailboxStore;
  private readonly runner: CommandRunner;
  private readonly command: string;
  private readonly idleCheck?: () => Promise<OrcaIdleResult>;
  private readonly now: () => number;
  private readonly report: (error: unknown) => void;

  constructor(context: Pick<WakeupDriverContext, 'store' | 'now' | 'reportError'>, options: OrcaWakeupDriverOptions) {
    this.store = context.store;
    this.now = context.now;
    this.report = context.reportError;
    this.runner = options.commandRunner ?? runCommand;
    this.command = options.orcaCommand ?? 'orca';
    this.idleCheck = options.idleCheck;
  }

  async probe(context: WakeupProbeContext): Promise<SlotProbeResult> {
    const { mailboxId, hasPendingWave } = context;
    try {
      const { target, excludedHandles } = this.excludeClaimedTerminal(mailboxId, readOrcaWakeTarget(context.target));
      if (hasPendingWave) {
        return await this.probeLeasedWave(target, excludedHandles);
      }

      const command = target.orcaCommand ?? this.command;
      const preview: MailboxCheckResult = { mailboxId, deliveryId: null, redelivered: false, newerQueuedCount: 0, items: [] };
      const retired = target.retiredTerminalHandle?.trim();
      if (needsSessionRotation(target, preview) && retired && retired !== target.terminalHandle?.trim() &&
        target.workspacePath && !this.idleCheck) {
        const activeClaims = claimedOrcaTerminalHandles(this.store, mailboxId, target, { includeRetired: false });
        if (!activeClaims.has(retired)) {
          const retiredIdle = await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, {
            targetTerminalHandle: null, retiringTerminalHandle: retired, agent: target.agent, excludedHandles,
            throwOnProbeError: true,
          });
          if (!retiredIdle.idle) return { busy: true, ready: false, reason: retiredIdle.reason ?? 'Retired terminal is busy' };
        }
      }

      const idle = !this.idleCheck && target.driver === 'orca-run'
        ? await this.probeOrcaRunActivity(target, command, excludedHandles)
        : await this.authoritativeIdle(target, preview, this.needsDedicatedTab(mailboxId, target), excludedHandles, true);
      if (!idle.idle && idle.reason === 'Workspace not found' && !this.idleCheck &&
        target.driver === 'orca-terminal' && target.autoCreateWorkspace === true) {
        if (!target.repoRoot?.trim()) {
          throw new Error('Creating a fixed Orca workspace requires a receiver-local repoRoot');
        }
        if (await this.canProvisionMissingWorkspace(target, command)) return { busy: false, ready: true };
      }
      return idle.idle ? { busy: false, ready: true } : { busy: true, ready: false, reason: idle.reason };
    } catch (error) {
      this.report(error);
      return { busy: true, ready: false, reason: String(error) };
    }
  }

  private async probeLeasedWave(
    target: OrcaWakeTarget,
    excludedHandles: ReadonlySet<string>,
  ): Promise<SlotProbeResult> {
    const quiescent = { busy: false, ready: false, reason: 'quiescent_leased_wave' };
    const unbound = { busy: false, ready: false, reason: 'unbound_leased_wave' };
    const waveActivity = (idle: OrcaIdleResult): SlotProbeResult => ({
      busy: !idle.idle, ready: false, reason: idle.reason ?? (idle.idle ? quiescent.reason : undefined),
    });
    if (this.idleCheck) {
      return waveActivity(await this.idleCheck());
    }
    const command = target.orcaCommand ?? this.command;
    const { workspacePath, terminalHandle, paneKey, agent } = target;
    if (target.driver === 'orca-run') {
      return waveActivity(await this.probeOrcaRunActivity(target, command, excludedHandles));
    }
    if (!workspacePath) {
      if (!terminalHandle) return unbound;
      return waveActivity(await probeOrcaTerminalIdle(this.runner, command, terminalHandle, undefined, undefined, {
        throwOnProbeError: true,
      }));
    }
    if (!terminalHandle && !paneKey) return unbound;

    const presence = await inspectOrcaWorkspaceTerminal(this.runner, command, workspacePath, terminalHandle, undefined, paneKey);
    if (presence === 'unusable') {
      return { busy: true, ready: false, reason: 'Terminal is disconnected or not writable' };
    }
    if (presence === 'live') {
      return waveActivity(await checkOrcaWorkspaceIdle(this.runner, command, workspacePath, undefined, {
        targetTerminalHandle: terminalHandle, targetPaneKey: paneKey, agent, excludedHandles,
        throwOnProbeError: true,
      }));
    }
    const idle = await checkOrcaWorkspaceIdle(this.runner, command, workspacePath, undefined, {
      targetTerminalHandle: null, retiringTerminalHandle: terminalHandle, retiringPaneKey: paneKey, agent, excludedHandles,
      throwOnProbeError: true,
    });
    return { busy: !idle.idle, ready: false, reason: idle.idle ? 'terminal_gone' : idle.reason };
  }

  private async probeOrcaRunActivity(
    target: OrcaWakeTarget,
    command: string,
    excludedHandles: ReadonlySet<string>,
  ): Promise<OrcaIdleResult> {
    const { workspacePath, terminalHandle, paneKey, agent } = target;
    if (workspacePath) {
      const workspace = await checkOrcaWorkspaceIdle(this.runner, command, workspacePath, undefined, {
        targetTerminalHandle: terminalHandle, targetPaneKey: paneKey, agent, excludedHandles, throwOnProbeError: true,
      });
      if (!workspace.idle) return workspace;
    }
    const runId = target.runId?.trim();
    if (!runId) return { idle: false, reason: 'orca-run target requires runId' };

    const settledStates = new Set(['succeeded', 'failed', 'cancelled', 'abandoned']);
    const seenCursors = new Set<string>();
    let cursor: string | undefined;
    do {
      const listed = await checkedCommand(this.runner, {
        command,
        args: ['orchestration', 'worker-list', '--run', runId, ...(cursor ? ['--cursor', cursor] : []), '--json'],
        cwd: workspacePath,
      });
      const inventory = JSON.parse(listed.stdout);
      if (inventory?.ok !== true || !Array.isArray(inventory.result?.workers)) {
        throw new Error('Invalid Orca run worker inventory');
      }
      const workers: { outcome?: unknown; state?: unknown }[] = inventory.result.workers;
      if (workers.some(worker => worker.outcome === 'in_progress' || !settledStates.has(String(worker.state ?? '')))) {
        return { idle: false, reason: 'Orca run has active workers' };
      }
      cursor = inventory.result.nextCursor;
      if (cursor !== undefined) {
        if (typeof cursor !== 'string' || !cursor || seenCursors.has(cursor)) {
          throw new Error('Invalid Orca run worker inventory cursor');
        }
        seenCursors.add(cursor);
      }
    } while (cursor);

    if (workspacePath) return { idle: true };
    let coordinatorHandle = terminalHandle?.trim();
    if (!coordinatorHandle) {
      const shown = await checkedCommand(this.runner, {
        command, args: ['orchestration', 'run-show', '--id', runId, '--full', '--json'],
      });
      const metadata = JSON.parse(shown.stdout);
      if (metadata?.ok !== true) throw new Error('Invalid Orca run metadata');
      const handle = metadata.result?.run?.coordinator_handle;
      coordinatorHandle = typeof handle === 'string' ? handle.trim() : undefined;
    }
    if (!coordinatorHandle) return { idle: false, reason: 'Orca run coordinator activity is unobservable' };

    const terminal = await readOrcaTerminal(this.runner, command, coordinatorHandle);
    const coordinatorWorkspace = typeof terminal?.worktreePath === 'string' ? terminal.worktreePath.trim() : undefined;
    if (coordinatorWorkspace) {
      return await checkOrcaWorkspaceIdle(this.runner, command, coordinatorWorkspace, undefined, {
        targetTerminalHandle: coordinatorHandle, agent, excludedHandles, throwOnProbeError: true,
      });
    }
    return await probeOrcaTerminalIdle(this.runner, command, coordinatorHandle, undefined, undefined, {
      throwOnProbeError: true,
    });
  }

  private async canProvisionMissingWorkspace(target: OrcaWakeTarget, command: string): Promise<boolean> {
    const repoRoot = target.repoRoot;
    if (target.driver !== 'orca-terminal' || !target.workspacePath?.trim() ||
      target.autoCreateWorkspace !== true || !repoRoot?.trim()) return false;
    if (this.runner === runCommand && !existsSync(repoRoot)) return false;
    await listOrcaWorkspaces(this.runner, command, repoRoot);
    return true;
  }

  private excludeClaimedTerminal(mailboxId: string, target: OrcaWakeTarget) {
    const excludedHandles = target.driver === 'orca-terminal' ? claimedOrcaTerminalHandles(this.store, mailboxId, target) : new Set<string>();
    if (target.terminalHandle && excludedHandles.has(target.terminalHandle)) {
      return { target: { ...target, terminalHandle: undefined, paneKey: undefined }, excludedHandles };
    }
    return { target, excludedHandles };
  }

  async run(context: WakeupDriverContext): Promise<WakeupDriverOutcome> {
    const { mailboxId, preview } = context;
    let activeRevision = context.target.bindingRevision ?? 0;
    try {
      let target = readOrcaWakeTarget(context.target);
      if (
        !this.idleCheck &&
        target.driver === 'orca-terminal' &&
        target.workspacePath &&
        target.autoCreateWorkspace === true
      ) {
        const workspace = await ensureOrcaWorkspace(this.runner, target.orcaCommand ?? this.command, {
          workspacePath: target.workspacePath,
          repoRoot: target.repoRoot,
          comment: target.workspaceComment,
        });
        if (workspace.workspacePath !== target.workspacePath) {
          const updated = this.store.updateWakeDriverConfigIfCurrent(mailboxId, target.bindingRevision ?? 0, { workspacePath: workspace.workspacePath });
          if (updated.superseded) return { kind: 'noop', reason: 'Wake launch superseded by a newer binding' };
          target = readOrcaWakeTarget(updated.target);
        }
      }
      activeRevision = target.bindingRevision ?? 0;
      const scoped = this.excludeClaimedTerminal(mailboxId, target);
      target = scoped.target;
      const { excludedHandles } = scoped;
      const dedicatedTab = this.needsDedicatedTab(mailboxId, target);
      const idle = await this.authoritativeIdle(target, preview, dedicatedTab, excludedHandles);
      if (!idle.idle) {
        // A workspace whose directory is gone can never become idle again; retrying it forever
        // would only inflate the backoff ledger. The mail stays queued for a human or a rebind.
        if (
          !this.idleCheck &&
          idle.reason === 'Workspace not found' &&
          target.autoCreateWorkspace !== true &&
          target.workspacePath &&
          !existsSync(target.workspacePath)
        ) {
          return { kind: 'disarm', reason: 'Workspace not found', expectedRevision: target.bindingRevision ?? 0 };
        }
        return this.defer(mailboxId, target, idle.reason ?? 'Workspace is busy');
      }
      return await this.deliver(mailboxId, target, preview, {
        freshSession: idle.spawnFreshSession === true, dedicatedTab, excludedHandles,
      }, revision => { activeRevision = revision; });
    } catch (error) {
      if (error instanceof SettledWakeLaunchError) throw error;
      if (this.store.getWakeTarget(mailboxId)?.deliveryInDoubt) {
        const superseded = settleWakeDeliveryFailure(this.store, mailboxId, error, this.now(), activeRevision);
        return { kind: 'noop', reason: superseded ? 'Wake launch superseded by a newer binding' : String(error) };
      }
      return { kind: 'defer', reason: String(error), expectedRevision: activeRevision };
    }
  }

  private needsDedicatedTab(mailboxId: string, target: OrcaWakeTarget): boolean {
    if (isDedicatedSpawn(target) || target.rotateAfterWaves !== undefined) return true;
    if (!mailboxId.startsWith('stream:')) return false;
    return baseStreamKeyOf(mailboxId.slice('stream:'.length)) !== undefined ||
      this.store.listWakeTargets().some(item => baseStreamKeyOf(item.mailboxId) === mailboxId);
  }

  /**
   * Rotating a completed wave and delivering the first wave to an unbound workspace both open an
   * independent session tab; a live bound tab keeps receiving the rest of its own wave.
   */
  private async spawnsFreshSession(
    target: OrcaWakeTarget,
    preview: MailboxCheckResult,
    command: string,
    dedicatedTab: boolean,
  ): Promise<boolean> {
    if (target.driver !== 'orca-terminal' || !target.workspacePath) {
      return false;
    }
    const workspacePath = target.workspacePath;
    // Only the real runner consults the filesystem here; an injected runner or authoritative idle
    // check owns that decision. This mirrors `checkOrcaWorkspaceIdle`'s exact invariant, so a
    // missing workspace still falls through to the probe that disarms it.
    if (this.runner === runCommand && !this.idleCheck && !existsSync(workspacePath)) {
      return false;
    }
    if (needsSessionRotation(target, preview)) return true;
    if (isDedicatedSpawn(target) || target.sessionId?.trim() || target.paneKey?.trim()) {
      if (!target.terminalHandle && !target.paneKey) return true;
      return !this.idleCheck && !(await isOrcaWorkspaceTerminalLive(
        this.runner, command, workspacePath, target.terminalHandle, undefined, target.paneKey,
      ));
    }
    if (!dedicatedTab) return false;
    if (!target.terminalHandle && !target.paneKey) return true;
    return !(await isOrcaWorkspaceTerminalLive(
      this.runner, command, workspacePath, target.terminalHandle, undefined, target.paneKey,
    ));
  }

  private async authoritativeIdle(
    target: OrcaWakeTarget,
    preview: MailboxCheckResult,
    dedicatedTab: boolean,
    excludedHandles: ReadonlySet<string>,
    throwOnProbeError = false,
  ): Promise<WakeupIdleDecision> {
    const command = target.orcaCommand ?? this.command;
    const spawnFreshSession = await this.spawnsFreshSession(target, preview, command, dedicatedTab);
    const withPlan = (idle: OrcaIdleResult): WakeupIdleDecision =>
      spawnFreshSession ? { ...idle, spawnFreshSession: true } : idle;
    if (this.idleCheck) {
      return withPlan(await this.idleCheck());
    }
    if (target.driver === 'orca-run') {
      if (target.workspacePath) {
        return withPlan(await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, { throwOnProbeError }));
      }
      // Orchestration mail queues without touching a TUI composer, so no keystroke race exists.
      return withPlan({ idle: true });
    }
    if (target.workspacePath) {
      // A fresh tab is created empty, so its retained siblings must not block the wakeup; a bound
      // tab is the only composer that can receive the pointer. A rotation additionally waits for
      // the slot it replaces to finish, because that tab is closed after the launch commits.
      return withPlan(
        await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, {
          targetTerminalHandle: spawnFreshSession ? null : target.terminalHandle,
          targetPaneKey: spawnFreshSession ? undefined : target.paneKey,
          retiringTerminalHandle: spawnFreshSession ? target.terminalHandle : undefined,
          retiringPaneKey: spawnFreshSession ? target.paneKey : undefined,
          agent: target.agent,
          excludedHandles,
          throwOnProbeError,
        }),
      );
    }
    if (target.terminalHandle) {
      return withPlan(
        await probeOrcaTerminalIdle(this.runner, command, target.terminalHandle, target.workspacePath, undefined, { throwOnProbeError }),
      );
    }
    return withPlan({
      idle: false,
      reason: 'Authoritative terminal idle check required',
    });
  }

  private defer(
    mailboxId: string,
    target: OrcaWakeTarget,
    reason: string,
  ): Extract<WakeupDriverOutcome, { kind: 'defer' }> {
    return { kind: 'defer', reason, expectedRevision: target.bindingRevision ?? 0 };
  }

  /**
   * Wait for a freshly opened tab to become the only composer this wakeup may write into. The
   * workspace probe ignores retained tabs (including an unsent human draft) and confirms the new
   * tab itself is idle before the pointer is written.
   */
  private async waitFreshTabIdle(
    target: OrcaWakeTarget,
    terminalHandle: string,
    command: string,
    excludedHandles: ReadonlySet<string>,
  ): Promise<OrcaIdleResult> {
    if (this.idleCheck) {
      return this.idleCheck();
    }
    const terminal = await waitOrcaTerminalIdle(
      this.runner,
      command,
      terminalHandle,
      target.workspacePath,
      5000,
    );
    if (!terminal.idle) {
      return terminal;
    }
    if (target.workspacePath) {
      return checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, {
        targetTerminalHandle: terminalHandle,
        agent: target.agent,
        excludedHandles,
      });
    }
    return probeOrcaTerminalIdle(this.runner, command, terminalHandle);
  }

  /**
   * Retire one replaced terminal, retrying a close that failed on an earlier wakeup. The retired
   * handle is durable only while the close is unconfirmed: a tab another mailbox actively claimed
   * since the launch is cleared without touching it, a transient failure reports and stays
   * retryable, and a deterministic Orca refusal parks the target for an operator.
   */
  private async retireReplacedTerminal(
    mailboxId: string,
    target: OrcaWakeTarget,
    terminalHandle: string | undefined,
    command: string,
    revision: number,
  ): Promise<{
    warning?: string;
    revision: number;
    target?: OrcaWakeTarget;
    parked?: OrcaTerminalCloseRejectedError;
  }> {
    const retiredHandle = target.retiredTerminalHandle?.trim();
    if (!retiredHandle) return { revision };
    const clearRetirement = (): { revision: number; target?: OrcaWakeTarget } => {
      const updated = this.store.updateWakeDriverConfigIfCurrent(mailboxId, revision, { retiredTerminalHandle: undefined });
      if (updated.superseded) return { revision };
      const next = readOrcaWakeTarget(updated.target);
      return { revision: next.bindingRevision ?? revision, target: next };
    };
    // Ownership is re-read at the destructive boundary: the launch was authorized from an earlier
    // snapshot, but a sibling may have bound this tab since. Only active claims block the close.
    const claims = claimedOrcaTerminalHandles(this.store, mailboxId, target, { includeRetired: false });
    if (retiredHandle === terminalHandle?.trim() || claims.has(retiredHandle)) return clearRetirement();
    try {
      await closeOrcaWorkspaceTerminal(this.runner, command, target.workspacePath ?? '', retiredHandle);
      return clearRetirement();
    } catch (error) {
      if (error instanceof OrcaTerminalCloseRejectedError) {
        // Retrying a deterministic refusal would loop forever on the 10s backoff.
        this.store.parkWakeDelivery(mailboxId, error);
        return { parked: error, revision };
      }
      this.report(error);
      return {
        warning: `Retired terminal close failed (${retiredHandle}): ${error instanceof Error ? error.message : String(error)}`,
        revision,
      };
    }
  }

  private async deliver(
    mailboxId: string,
    target: OrcaWakeTarget,
    preview: MailboxCheckResult,
    options: { freshSession: boolean; dedicatedTab: boolean; excludedHandles: ReadonlySet<string> },
    setActiveRevision: (revision: number) => void,
  ): Promise<WakeupDriverOutcome> {
    const command = target.orcaCommand ?? this.command;
    let activeRevision = target.bindingRevision ?? 0;
    let rebound = false;
    let terminalHandle = target.terminalHandle;
    let warning: string | undefined;
    const appendWarning = (message: string): void => {
      warning = warning === undefined ? message : `${warning}; ${message}`;
    };
    let retirementAttempted = false;
    const attemptRetirement = async (): Promise<{ parked?: OrcaTerminalCloseRejectedError }> => {
      retirementAttempted = true;
      const retirement = await this.retireReplacedTerminal(mailboxId, target, terminalHandle, command, activeRevision);
      if (retirement.warning !== undefined) appendWarning(retirement.warning);
      // Clearing the durable retirement bumps the binding revision; later CAS steps must see it.
      if (retirement.target) target = retirement.target;
      activeRevision = retirement.revision;
      setActiveRevision(activeRevision);
      return { parked: retirement.parked };
    };
    const needsRotation = needsSessionRotation(target, preview);
    const spawnFreshSession = !needsRotation && options.freshSession;
    let rotated = false;
    const dedicatedSpawnTab =
      isDedicatedSpawn(target) && target.driver === 'orca-terminal' && Boolean(target.workspacePath);
    const replaceSpawnTab =
      dedicatedSpawnTab &&
      !(needsRotation || spawnFreshSession) &&
      ((!terminalHandle && !target.paneKey) ||
        (!this.idleCheck &&
          !(await isOrcaWorkspaceTerminalLive(this.runner, command, target.workspacePath!, terminalHandle, undefined, target.paneKey))));

    if (needsRotation || spawnFreshSession || replaceSpawnTab) {
      // Rotation never resets an existing TUI: it opens a new session tab and retires the
      // replaced tab once the launch commits. A missing workspace makes that impossible and must
      // fail loudly; a fresh-tab plan has already proven its workspace in `spawnsFreshSession`.
      if (!target.workspacePath) {
        throw new Error(`Orca session rotation requires a workspacePath: ${mailboxId}`);
      }
      const workspacePath = target.workspacePath;
      // A previous rotation's close may still be unconfirmed. Retry it before this launch, because
      // the launch patch would overwrite the persisted retired handle; until it closes, the next
      // rotation is deferred so a slot never loses track of a live retired terminal.
      if (needsRotation && target.retiredTerminalHandle) {
        const prior = await this.retireReplacedTerminal(mailboxId, target, terminalHandle, command, activeRevision);
        if (prior.parked) return { kind: 'noop', reason: prior.parked.message };
        const refreshed = this.store.getWakeTarget(mailboxId);
        if (refreshed) {
          target = readOrcaWakeTarget(refreshed);
          activeRevision = target.bindingRevision ?? 0;
          setActiveRevision(activeRevision);
        }
        if (target.retiredTerminalHandle) {
          return this.defer(mailboxId, target, prior.warning ?? 'Retired terminal close pending before next rotation');
        }
      }
      const spawned = await launchFencedOrcaWorkspaceTerminal(this.store, this.runner, mailboxId, {
        command,
        workspacePath,
        agent: target.agent,
        interactiveCommand: target.interactiveCommand, resumeCommand: target.resumeCommand, sessionId: target.sessionId, expectedRevision: activeRevision,
        now: this.now(),
        resetCompletedWaves: needsRotation,
      });
      if (!spawned.committed) return { kind: 'noop', reason: 'Wake launch superseded by a newer binding' };
      terminalHandle = spawned.handle;
      target = spawned.target;
      activeRevision = target.bindingRevision ?? 0;
      setActiveRevision(activeRevision);
      rotated = needsRotation;
      // One slot keeps at most one live terminal: the replacement is committed, so the retired
      // tab is closed before the pointer is written. A deterministic refusal parks the target.
      const retirement = await attemptRetirement();
      if (retirement.parked) return { kind: 'noop', reason: retirement.parked.message };
      const idle = await this.waitFreshTabIdle(target, terminalHandle, command, options.excludedHandles);
      if (!idle.idle) {
        const deferred = this.defer(
          mailboxId,
          target,
          needsRotation
            ? 'Session rotated; waiting for tui-idle before wakeup'
            : 'Terminal spawned; waiting for tui-idle before wakeup',
        );
        return needsRotation ? { ...deferred, rotated: true } : deferred;
      }
    } else if (
      ((!terminalHandle && !target.paneKey) || !this.idleCheck) &&
      target.workspacePath &&
      target.driver === 'orca-terminal'
    ) {
      // Real Orca mode re-resolves before every send so a terminal closed since the last delivery
      // is rebound (or respawned) before the external boundary is crossed; `preferredHandle` keeps
      // a live binding without spawning a second terminal. Tests that inject an authoritative idle
      // check only resolve when no handle exists yet.
      const plan = await planOrcaWorkspaceTerminal(this.runner, command, target.workspacePath, undefined, {
        agent: target.agent,
        preferredHandle: target.terminalHandle, preferredPaneKey: target.paneKey, sessionId: target.sessionId,
        dedicatedTab: options.dedicatedTab,
        excludedHandles: options.excludedHandles,
      });
      // Another mailbox may have claimed a tab while Orca was listing the workspace.
      if (plan.kind !== 'create' && (
        claimedOrcaTerminalHandles(this.store, mailboxId, target).has(plan.handle) ||
        (!options.dedicatedTab && plan.handle !== target.terminalHandle && this.needsDedicatedTab(mailboxId, target))
      )) {
        return this.defer(mailboxId, target, 'Orca terminal ownership changed during selection');
      }
      if (plan.kind === 'existing') {
        terminalHandle = plan.handle;
        rebound = plan.handle !== target.terminalHandle;
      } else {
        const launched = await launchFencedOrcaWorkspaceTerminal(this.store, this.runner, mailboxId, {
          command,
          workspacePath: target.workspacePath,
          agent: target.agent,
          interactiveCommand: target.interactiveCommand, resumeCommand: target.resumeCommand, sessionId: target.sessionId, expectedRevision: activeRevision,
          now: this.now(),
          ...(plan.kind === 'shell' ? { shellTerminalHandle: plan.handle } : {}),
        });
        if (!launched.committed) return { kind: 'noop', reason: 'Wake launch superseded by a newer binding' };
        terminalHandle = launched.handle;
        target = launched.target;
        activeRevision = target.bindingRevision ?? 0;
        setActiveRevision(activeRevision);
        // A close from an earlier rotation may still be pending; retry it before the pointer.
        // A deterministic refusal parks the target instead of writing a pointer.
        const pending = await attemptRetirement();
        if (pending.parked) return { kind: 'noop', reason: pending.parked.message };
        if (plan.kind === 'shell') {
          return this.defer(mailboxId, target, 'Terminal spawned; waiting for tui-idle before wakeup');
        }
        const idle = this.idleCheck
          ? await this.idleCheck()
          : await this.waitFreshTabIdle(target, terminalHandle, command, options.excludedHandles);
        if (!idle.idle)
          return this.defer(mailboxId, target, 'Terminal spawned; waiting for tui-idle before wakeup');
      }
    }
    // A retired terminal whose close failed earlier is retried on every wakeup, even when this
    // flush reuses a live binding instead of launching a replacement.
    if (!retirementAttempted) {
      const pending = await attemptRetirement();
      if (pending.parked) return { kind: 'noop', reason: pending.parked.message };
    }

    const pointer = buildWakeupPointer(mailboxId, target, preview);
    if (!pointer) {
      const completed = this.store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview), {}, undefined, activeRevision);
      if (completed.superseded) return { kind: 'noop', reason: 'Wake launch superseded by a newer binding' };
      return { kind: 'noop', reason: 'Nothing new to deliver' };
    }

    if (target.driver === 'orca-run' && !target.runId) throw new Error('orca-run wake target requires runId');
    if (target.driver === 'orca-terminal' && !terminalHandle)
      throw new Error('orca-terminal wake target requires terminalHandle or workspacePath');
    if (target.driver === 'orca-terminal' && terminalHandle && claimedOrcaTerminalHandles(this.store, mailboxId, target).has(terminalHandle)) {
      return this.defer(mailboxId, target, 'Orca terminal ownership changed before delivery');
    }
    if (rebound) {
      // Publish the recovered handle before sending so a sibling cannot select it during the send.
      const updated = this.store.updateWakeDriverConfigIfCurrent(mailboxId, activeRevision, { terminalHandle, paneKey: target.paneKey });
      if (updated.superseded) return { kind: 'noop', reason: 'Wake launch superseded by a newer binding' };
      activeRevision = updated.target.bindingRevision ?? 0;
      setActiveRevision(activeRevision);
    }
    // The pointer is the only external effect left; fence it so a crash or an unconfirmed send
    // stays parked instead of being replayed by the next tick or restart.
    if (!this.store.beginWakeDelivery(mailboxId, activeRevision)) return { kind: 'noop', reason: 'Wake launch superseded by a newer binding' };
    if (target.driver === 'orca-run') {
      const output = await this.runner({
        command,
        args: [
          'orchestration',
          'send',
          '--to',
          `run:${target.runId}`,
          '--subject',
          `Dealer wakeup (${mailboxId})`,
          '--body',
          pointer,
          '--json',
        ],
        cwd: target.workspacePath,
      });
      if (output.exitCode !== undefined && output.exitCode !== 0) {
        throw new Error(`Orca orchestration send exited ${output.exitCode}`);
      }
      if (JSON.parse(output.stdout).ok === false) {
        throw new Error('Orca wakeup delivery failed');
      }
    } else {
      await sendOrcaTerminalText(this.runner, command, terminalHandle!, pointer, {
        cwd: target.workspacePath,
      });
    }

    const comment =
      target.workspaceComment ??
      (mailboxId.startsWith('stream:') ? `Dealer: ${mailboxId} pending` : undefined);
    if (comment !== undefined) {
      try {
        await setOrcaWorkspaceComment(this.runner, command, target.workspacePath, comment);
      } catch (error) {
        // The pointer is already delivered; retrying it would duplicate a wakeup.
        appendWarning(String(error));
        this.report(error);
      }
    }

    const completed = this.store.completeWakeDelivery(
      mailboxId,
      maxPreviewSequence(preview),
      {},
      terminalHandle && target.rotateAfterWaves !== undefined
        ? {
            sessionMailboxId: `terminal:${terminalHandle}`,
            driver: 'orca-terminal',
            driverConfig: buildPinnedOrcaSessionDriverConfig(target, this.store.getWakeTarget(`terminal:${terminalHandle}`), terminalHandle),
          }
        : undefined,
      activeRevision,
    );
    if (completed.superseded) return { kind: 'noop', reason: 'Wake launch superseded by a newer binding' };
    return {
      kind: 'delivered',
      pointer,
      rotated,
      ...(warning !== undefined ? { warning } : {}),
    };
  }
}

export function createOrcaTerminalWakeupDriver(options: OrcaWakeupDriverOptions = {}): WakeupDriver {
  return {
    name: 'orca-terminal',
    deliver: (context) => new OrcaWakeDelivery(context, options).run(context),
    probeSlot: (context) => new OrcaWakeDelivery(context, options).probe(context),
    sessionMailboxIdFor(target) {
      const { terminalHandle } = readOrcaDriverConfig(target);
      const handle = terminalHandle?.trim();
      return target.rotateAfterWaves !== undefined && handle ? `terminal:${handle}` : undefined;
    },
    async resolveSessionTarget(mailboxId) {
      if (mailboxId.startsWith('terminal:') && mailboxId.length > 9) {
        let workspacePath: string | undefined;
        try {
          const terminal = await readOrcaTerminal(
            options.commandRunner ?? runCommand,
            options.orcaCommand ?? 'orca',
            mailboxId.slice(9),
          );
          if (typeof terminal?.worktreePath === 'string' && terminal.worktreePath.trim())
            workspacePath = terminal.worktreePath;
        } catch {
          // Optional address enrichment only: the delivery-time terminal probe still fails closed.
        }
        return {
          driver: 'orca-terminal',
          driverConfig: {
            terminalHandle: mailboxId.slice(9),
            orcaCommand: options.orcaCommand ?? 'orca',
            ...(workspacePath ? { workspacePath } : {}),
          },
        };
      }
      if (mailboxId.startsWith('worktree:') && mailboxId.length > 9) {
        const found = await findOrcaWorktreeByName(
          options.commandRunner ?? runCommand,
          options.orcaCommand ?? 'orca',
          mailboxId.slice(9),
        );
        if (found)
          return {
            driver: 'orca-terminal',
            driverConfig: {
              ...found,
              orcaCommand: options.orcaCommand ?? 'orca',
            },
          };
      }
      return undefined;
    },
  };
}

export function createOrcaRunWakeupDriver(options: OrcaWakeupDriverOptions = {}): WakeupDriver {
  return {
    name: 'orca-run',
    deliver: (context) => new OrcaWakeDelivery(context, options).run(context),
    probeSlot: (context) => new OrcaWakeDelivery(context, options).probe(context),
  };
}

export function createOrcaWakeupDrivers(options: OrcaWakeupDriverOptions = {}): readonly WakeupDriver[] {
  return [createOrcaTerminalWakeupDriver(options), createOrcaRunWakeupDriver(options)];
}
