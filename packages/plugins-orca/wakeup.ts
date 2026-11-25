import { existsSync, realpathSync } from 'node:fs';
import { resolve } from 'node:path';
import {
  OrcaTerminalCloseRejectedError,
  OrcaTerminalLaunchRejectedError,
  OrcaTerminalSendRefusedError,
  readOrcaTerminal,
  checkOrcaWorkspaceIdle,
  closeOrcaWorkspaceTerminal,
  ensureOrcaWorkspace,
  inspectOrcaWorkspaceTerminal,
  listOrcaWorkspaces,
  planOrcaWorkspaceTerminal,
  probeOrcaTerminalIdle,
  sendOrcaTerminalText,
  setOrcaWorkspaceComment,
  spawnOrcaWorkspaceTerminal,
  waitOrcaTerminalIdle,
  findOrcaWorktreeByName,
  tryFastForwardCleanWorktree,
} from './workspace.ts';
import { orcaMutationRunner } from './inventories.ts';
import type { OrcaIdleResult, SupportedOrcaAgent } from './workspace.ts';
import { buildOrcaAgentLaunchCommand, isOrcaLaunchConfigCompatible } from './workspace.ts';
import { checkedCommand, CommandNotStartedError, runCommand } from '../subprocess/index.ts';
import type { CommandRunner } from '../subprocess/index.ts';
import { readAgentHostDriverConfig } from '../plugins-agent-host/wakeup.ts';
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

export const DEFAULT_ORCA_WAIT_SUBMIT_SECONDS = 15;

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
  enableReadinessSubscription?: boolean;
  freshTabIdleTimeoutMs?: number;
  readScreen?: boolean;
}

function orcaReadinessSubscription(options: OrcaWakeupDriverOptions): WakeupDriver['subscribeReadiness'] {
  const runner = options.commandRunner ?? runCommand;
  const enabled = options.enableReadinessSubscription ?? runner === runCommand;
  if (options.idleCheck || !enabled) return undefined;

  return ({ target, onSettled }, onReady) => {
    const { terminalHandle, workspacePath, orcaCommand } = readOrcaDriverConfig(target);
    if (!terminalHandle) return undefined;
    const controller = new AbortController();
    void waitOrcaTerminalIdle(runner, orcaCommand ?? options.orcaCommand ?? 'orca', terminalHandle,
      workspacePath, 60_000, controller.signal).then(result => {
      if (controller.signal.aborted) return;
      if (result.timedOut) onSettled?.();
      else onReady();
    });
    return { dispose: () => controller.abort() };
  };
}

type OrcaWakePlan =
  | { kind: 'rotate' }
  | { kind: 'spawn-unbound' }
  | { kind: 'recover-absent' }
  | { kind: 'defer'; reason: string }
  | { kind: 'reuse' };

interface WakeupIdleDecision extends OrcaIdleResult {
  plan: OrcaWakePlan;
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

export interface OrcaManagedAutoCreateWorkspaceInput {
  /** A receiver-local binding decision always wins over the managed default. */
  explicitAutoCreate?: boolean;
  /** Whether the receiver manages a standing Orca workspace it can provision. */
  managesWorkspace: boolean;
  /** Provisioning needs a receiver-local repository root; sender payloads never supply one. */
  hasRepoRoot: boolean;
  /** An Orca Run owns its workspace lifecycle. */
  runId?: string;
  /** An explicitly pinned terminal must already exist. */
  pinnedTerminalHandle?: string;
}

/**
 * The single owner of the managed standing-workspace provisioning policy. Preset generation,
 * topic binding refresh, and wave enqueue all resolve their effective default here so a
 * receiver-managed `fixed`/`wave` workspace can never diverge between entry paths, while a
 * sender-supplied payload (no receiver repo root) can never trigger workspace creation.
 */
export function resolveOrcaManagedAutoCreateWorkspace(
  input: OrcaManagedAutoCreateWorkspaceInput,
): boolean | undefined {
  if (input.explicitAutoCreate !== undefined) return input.explicitAutoCreate;
  return input.managesWorkspace && input.hasRepoRoot && !input.runId && !input.pinnedTerminalHandle
    ? true
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
function claimedOrcaTerminals(
  store: MailboxStore,
  mailboxId: string,
  bound: OrcaTerminalDriverConfig,
  options: { includeRetired?: boolean } = {},
): { handles: Set<string>; paneClaimed: boolean } {
  const includeRetired = options.includeRetired !== false;
  const handles = new Set<string>();
  let paneClaimed = false;
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
    if (paneKey && paneKey === bound.paneKey && sameCallerWorkspace(bound.workspacePath, workspacePath)) paneClaimed = true;
  }
  return { handles, paneClaimed };
}

function checkOrcaPointerIdle(
  runner: CommandRunner, command: string, target: OrcaWakeTarget, terminalHandle: string,
  { excludedHandles, signal, readScreen }: { excludedHandles?: ReadonlySet<string>; signal?: AbortSignal; readScreen?: boolean },
): Promise<OrcaIdleResult> {
  return target.workspacePath
    ? checkOrcaWorkspaceIdle(runner, command, target.workspacePath, signal, {
        targetTerminalHandle: terminalHandle, requireTarget: true, agent: target.agent, excludedHandles, readScreen,
      })
    : probeOrcaTerminalIdle(runner, command, terminalHandle, undefined, signal, {
        verifyComposerLast: true, expectedAgent: target.agent, readScreen,
      });
}

async function waitFreshOrcaTabIdle(
  runner: CommandRunner, command: string, target: OrcaWakeTarget, terminalHandle: string,
  options: { excludedHandles?: ReadonlySet<string>; signal?: AbortSignal; timeoutMs?: number; readScreen?: boolean },
): Promise<OrcaIdleResult> {
  const { signal, timeoutMs = 25_000 } = options;
  const ready = await waitOrcaTerminalIdle(runner, command, terminalHandle, target.workspacePath, timeoutMs, signal);
  return ready.idle
    ? checkOrcaPointerIdle(runner, command, target, terminalHandle, options)
    : ready;
}

function settleOrcaWakeDeliveryFailure(
  store: MailboxStore, mailboxId: string, error: unknown, now: number, expectedRevision: number,
): boolean {
  if (error instanceof OrcaTerminalSendRefusedError) {
    return store.deferWakeAttempt(mailboxId, expectedRevision, error, now).superseded;
  }
  return settleWakeDeliveryFailure(store, mailboxId, error, now, expectedRevision);
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
  if (caller.terminalHandle && store && claimedOrcaTerminals(store, id, bound).handles.has(caller.terminalHandle)) return false;
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
  const bound = existing?.driver === 'orca-terminal' ? readOrcaDriverConfig(existing) : undefined;
  const preserveDelivery = bound && !isCallerOrcaIdentitySwitch(bound, caller) &&
    sameCallerWorkspace(bound.workspacePath, caller.workspacePath) &&
    Boolean((caller.terminalHandle && bound.terminalHandle === caller.terminalHandle) || (caller.paneKey && bound.paneKey === caller.paneKey));
  const updated = store.upsertWakeTarget(id, resolveCallerOrcaWakeTargetPatch(existing, caller),
    preserveDelivery ? { preservePendingDeliveryFromRevision: existing?.bindingRevision ?? 0 } : undefined);
  const handle = readOrcaDriverConfig(updated).terminalHandle;
  if (updated.rotateAfterWaves !== undefined && handle) pinOrcaTerminalSessionMailbox(store, id, handle);
  return updated;
}

function isCallerOrcaIdentitySwitch(
  existing: OrcaTerminalDriverConfig | undefined, caller: Omit<CallerOrcaIdentity, 'mailboxId'>,
): boolean {
  return Boolean(
    (caller.workspacePath && existing?.workspacePath && caller.workspacePath !== existing.workspacePath) ||
    (caller.sessionId && existing?.sessionId && caller.sessionId !== existing.sessionId) ||
    (caller.paneKey && existing?.paneKey && caller.paneKey !== existing.paneKey) ||
    (caller.agent && existing?.agent && caller.agent !== existing.agent) ||
    (caller.terminalHandle && existing?.terminalHandle && caller.terminalHandle !== existing.terminalHandle &&
      (!caller.paneKey || !existing.paneKey || caller.paneKey !== existing.paneKey) &&
      (!caller.sessionId || !existing.sessionId || caller.sessionId !== existing.sessionId)),
  );
}

/** Single owner of caller-driven Orca conversation identity transitions. */
export function resolveCallerOrcaWakeTargetPatch(
  snapshot: WakeTargetState | undefined,
  caller: Omit<CallerOrcaIdentity, 'mailboxId'>,
): Partial<WakeTargetState> {
  const existing = snapshot && readOrcaWakeTarget(snapshot);
  const switchedIdentity = isCallerOrcaIdentitySwitch(existing, caller);
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
    if (
      error instanceof CommandNotStartedError ||
      error instanceof OrcaTerminalLaunchRejectedError ||
      error instanceof OrcaTerminalSendRefusedError
    ) {
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
  let terminalHandle: string | undefined;
  try {
    const state = store.getWakeTarget(mailboxId);
    expectedRevision = state?.bindingRevision ?? 0;
    const target = state ? readOrcaWakeTarget(state) : undefined;
    if (!target?.terminalHandle) throw new Error(`Orca terminal wake target missing handle: ${mailboxId}`);
    terminalHandle = target.terminalHandle;
    const command = target.orcaCommand ?? 'orca';
    const preview = store.check({ mailboxId, peek: true });
    const idle = await waitFreshOrcaTabIdle(runner, command, target, target.terminalHandle, options);
    if (!idle.idle) {
      store.retainPendingWakeIfCurrent(mailboxId, expectedRevision);
      return { terminalHandle: target.terminalHandle, sent: false };
    }
    const pointer = buildWakeupPointer(mailboxId, target, preview);
    if (!pointer) {
      store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview), {}, undefined, expectedRevision);
      return { terminalHandle: target.terminalHandle, sent: false };
    }
    const deliveredSequence = maxPreviewSequence(preview);
    const pinnedSession = pinnedOrcaDeliverySession(target, terminalHandle);
    const fenced = store.beginWakeDelivery(mailboxId, expectedRevision, { deliveredSequence, pinnedSession });
    if (!fenced) return { terminalHandle: target.terminalHandle, sent: false };
    deliveryBegun = true;
    await sendOrcaTerminalText(runner, command, target.terminalHandle, pointer, {
      cwd: target.workspacePath,
      signal: options.signal,
      waitSubmit: DEFAULT_ORCA_WAIT_SUBMIT_SECONDS,
      expectedAgent: target.agent,
    });
    const completed = store.completeWakeDelivery(mailboxId, deliveredSequence, {}, pinnedSession, expectedRevision);
    if (completed.superseded) return { pointer, terminalHandle: target.terminalHandle, sent: false };
    return { pointer, terminalHandle: target.terminalHandle, sent: true };
  } catch (error) {
    const current = store.getWakeTarget(mailboxId);
    if (deliveryBegun && !current?.deliveryInDoubt) {
      const sameTerminal = current?.driver === 'orca-terminal' && readOrcaDriverConfig(current).terminalHandle === terminalHandle;
      return { terminalHandle, sent: current?.bindingRevision === expectedRevision || sameTerminal };
    }
    if (deliveryBegun) {
      settleOrcaWakeDeliveryFailure(store, mailboxId, error, now, expectedRevision);
    } else if (!current?.deliveryInDoubt) {
      store.deferWakeAttempt(mailboxId, expectedRevision, error, now);
    }
    throw error;
  }
}

function pinnedOrcaDeliverySession(target: OrcaWakeTarget, terminalHandle?: string) {
  if (target.rotateAfterWaves === undefined || !terminalHandle) return undefined;
  const sameHandle = !target.terminalHandle || target.terminalHandle === terminalHandle;
  return {
    sessionMailboxId: `terminal:${terminalHandle}`,
    driver: 'orca-terminal',
    driverConfig: { terminalHandle },
    ...(!sameHandle && { omitSourceDriverConfig: ['sessionId', 'paneKey'] }),
    unsetDriverConfig: ['configuredSessionId', 'configuredTerminalHandle', 'retiredSessionId', 'retiredTerminalHandle'],
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
  const pinnedSession = rotatingOwner && pinnedOrcaDeliverySession(readOrcaWakeTarget(rotatingOwner), handle);
  if (!pinnedSession || !rotatingOwner) return id;
  // The live target can be advertised as a reply address. Wake completion pins committed routes.
  return store.pinSessionMailbox(rotatingOwner.mailboxId, pinnedSession.sessionMailboxId, pinnedSession);
}

class OrcaWakeDelivery {
  private readonly store: MailboxStore;
  private readonly runner: CommandRunner;
  private readonly command: string;
  private readonly idleCheck?: () => Promise<OrcaIdleResult>;
  private readonly freshTabIdleTimeoutMs?: number;
  private readonly readScreen?: boolean;
  private readonly now: () => number;
  private readonly report: (error: unknown) => void;

  constructor(context: Pick<WakeupDriverContext, 'store' | 'now' | 'reportError'>, options: OrcaWakeupDriverOptions) {
    this.store = context.store;
    this.now = context.now;
    this.report = context.reportError;
    this.runner = options.commandRunner ?? runCommand;
    this.command = options.orcaCommand ?? 'orca';
    this.idleCheck = options.idleCheck;
    this.freshTabIdleTimeoutMs = options.freshTabIdleTimeoutMs;
    this.readScreen = options.readScreen;
  }

  async probe(context: WakeupProbeContext): Promise<SlotProbeResult> {
    const { mailboxId, hasPendingWave } = context;
    try {
      const preview: MailboxCheckResult = { mailboxId, deliveryId: null, redelivered: false, newerQueuedCount: 0, items: [] };
      const { target, excludedHandles, dedicatedTab } = this.scopeWakeTarget(
        mailboxId, readOrcaWakeTarget(context.target), hasPendingWave ? undefined : preview,
      );
      if (hasPendingWave) {
        return await this.probeLeasedWave(target, excludedHandles);
      }

      const command = target.orcaCommand ?? this.command;
      const retired = target.retiredTerminalHandle?.trim();
      if (needsSessionRotation(target, preview) && retired && retired !== target.terminalHandle?.trim() &&
        target.workspacePath && !this.idleCheck) {
        const activeClaims = claimedOrcaTerminals(this.store, mailboxId, target, { includeRetired: false }).handles;
        if (!activeClaims.has(retired)) {
          const retiredIdle = await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, {
            targetTerminalHandle: null, retiringTerminalHandle: retired, agent: target.agent, excludedHandles,
            advisory: true, throwOnProbeError: true,
          });
          if (!retiredIdle.idle) return { busy: true, ready: false, reason: retiredIdle.reason ?? 'Retired terminal is busy' };
        }
      }

      const idle = !this.idleCheck && target.driver === 'orca-run'
        ? await this.probeOrcaRunActivity(target, command, excludedHandles)
        : await this.authoritativeIdle(target, preview, dedicatedTab, excludedHandles, true);
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

    const presence = await inspectOrcaWorkspaceTerminal(this.runner, command, workspacePath, terminalHandle, undefined, paneKey, true);
    if (presence === 'unusable') {
      return { busy: true, ready: false, reason: 'Terminal is disconnected or not writable' };
    }
    if (presence === 'live') {
      return waveActivity(await checkOrcaWorkspaceIdle(this.runner, command, workspacePath, undefined, {
        targetTerminalHandle: terminalHandle, targetPaneKey: paneKey, agent, excludedHandles,
        advisory: true, throwOnProbeError: true,
      }));
    }
    const idle = await checkOrcaWorkspaceIdle(this.runner, command, workspacePath, undefined, {
      targetTerminalHandle: null, retiringTerminalHandle: terminalHandle, retiringPaneKey: paneKey, agent, excludedHandles,
      advisory: true, throwOnProbeError: true,
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
        targetTerminalHandle: terminalHandle, targetPaneKey: paneKey, agent, excludedHandles, advisory: true, throwOnProbeError: true,
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
        targetTerminalHandle: coordinatorHandle, agent, excludedHandles, advisory: true, throwOnProbeError: true,
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

  private scopeWakeTarget(mailboxId: string, target: OrcaWakeTarget, preview?: MailboxCheckResult) {
    if (target.driver !== 'orca-terminal') return { target, excludedHandles: new Set<string>(), dedicatedTab: false };
    const dedicatedTab = this.needsDedicatedTab(mailboxId, target);
    const { handles: excludedHandles, paneClaimed } = claimedOrcaTerminals(this.store, mailboxId, target);
    const hasSession = Boolean(target.sessionId?.trim());
    const claimedHandle = target.terminalHandle && excludedHandles.has(target.terminalHandle);
    // Keep the original identity as recovery evidence when another slot owns the terminal.
    // Rotation opens a different conversation, and pane-only slots can take an independent tab.
    if ((claimedHandle && (!hasSession || (preview && needsSessionRotation(target, preview)))) ||
      (!target.terminalHandle && !hasSession && paneClaimed)) {
      return { target: { ...target, terminalHandle: undefined, paneKey: undefined }, excludedHandles, dedicatedTab };
    }
    return { target, excludedHandles, dedicatedTab };
  }

  async run(context: WakeupDriverContext): Promise<WakeupDriverOutcome> {
    const { mailboxId, preview } = context;
    let activeRevision = context.target.bindingRevision ?? 0;
    let deliveryBegun = false;
    try {
      let { target, excludedHandles, dedicatedTab } = this.scopeWakeTarget(mailboxId, readOrcaWakeTarget(context.target), preview);
      activeRevision = target.bindingRevision ?? 0;
      let idle = await this.authoritativeIdle(target, preview, dedicatedTab, excludedHandles);
      // A standing receiver-managed workspace is provisioned only when the authoritative probe
      // proves it missing; a healthy workspace never pays for an inventory subprocess.
      if (
        !this.idleCheck &&
        !idle.idle &&
        idle.reason === 'Workspace not found' &&
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
          activeRevision = target.bindingRevision ?? 0;
          ({ target, excludedHandles, dedicatedTab } = this.scopeWakeTarget(mailboxId, target, preview));
        }
        idle = await this.authoritativeIdle(target, preview, dedicatedTab, excludedHandles);
      }
      if (!idle.idle || idle.plan.kind === 'defer') {
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
        plan: idle.plan, dedicatedTab, excludedHandles, signal: context.signal,
        onDeliveryBegun: () => { deliveryBegun = true; },
      }, revision => { activeRevision = revision; });
    } catch (error) {
      if (error instanceof SettledWakeLaunchError) throw error;
      const current = this.store.getWakeTarget(mailboxId);
      const fenced = current?.deliveryInDoubt === true;
      if (deliveryBegun && !fenced) {
        return { kind: 'noop', reason: current?.bindingRevision === activeRevision
          ? 'Wake delivery already confirmed by consumer' : 'Wake launch superseded by a newer binding' };
      }
      if (fenced) {
        const superseded = deliveryBegun && settleOrcaWakeDeliveryFailure(this.store, mailboxId, error, this.now(), activeRevision);
        return { kind: 'noop', reason: superseded ? 'Wake launch superseded by a newer binding' : String(error) };
      }
      return { kind: 'defer', reason: String(error), expectedRevision: activeRevision };
    }
  }

  private needsDedicatedTab(mailboxId: string, target: OrcaWakeTarget): boolean {
    if (target.paneKey?.trim() || isDedicatedSpawn(target) || target.rotateAfterWaves !== undefined) return true;
    if (!mailboxId.startsWith('stream:')) return false;
    return baseStreamKeyOf(mailboxId.slice('stream:'.length)) !== undefined ||
      this.store.listWakeTargets().some(item => baseStreamKeyOf(item.mailboxId) === mailboxId);
  }

  private async resolveSessionPlan(
    target: OrcaWakeTarget,
    preview: MailboxCheckResult,
    command: string,
    dedicatedTab: boolean,
    excludedHandles: ReadonlySet<string>,
    advisory: boolean,
  ): Promise<OrcaWakePlan> {
    if (target.driver !== 'orca-terminal') return { kind: 'reuse' };
    const workspacePath = target.workspacePath;
    // Only the real runner consults the filesystem here; an injected runner or authoritative idle
    // check owns that decision. This mirrors `checkOrcaWorkspaceIdle`'s exact invariant, so a
    // missing workspace still falls through to the probe that disarms it.
    if (workspacePath && this.runner === runCommand && !this.idleCheck && !existsSync(workspacePath)) {
      return { kind: 'reuse' };
    }
    if (needsSessionRotation(target, preview)) return { kind: 'rotate' };
    if (!workspacePath) return { kind: 'reuse' };
    const { terminalHandle, paneKey } = target;
    const exclusive = dedicatedTab || Boolean(target.sessionId?.trim());
    if (!terminalHandle && !paneKey) {
      return exclusive ? { kind: 'spawn-unbound' } : { kind: 'reuse' };
    }
    const claimedHandle = terminalHandle && excludedHandles.has(terminalHandle) ? terminalHandle : undefined;
    if (claimedHandle) {
      const reason = await this.retainedHandleBlockReason(command, workspacePath, claimedHandle, advisory);
      if (reason) return { kind: 'defer', reason };
    }
    if (this.idleCheck) return { kind: 'reuse' };
    const unclaimedHandle = claimedHandle ? undefined : terminalHandle;
    const presence = unclaimedHandle || paneKey
      ? await inspectOrcaWorkspaceTerminal(this.runner, command, workspacePath, unclaimedHandle, undefined, paneKey, advisory)
      : 'absent';
    if (presence === 'unusable') {
      return { kind: 'defer', reason: 'Terminal is disconnected or not writable' };
    }
    return presence === 'absent' && exclusive ? { kind: 'recover-absent' } : { kind: 'reuse' };
  }

  private async authoritativeIdle(
    target: OrcaWakeTarget,
    preview: MailboxCheckResult,
    dedicatedTab: boolean,
    excludedHandles: ReadonlySet<string>,
    advisory = false,
  ): Promise<WakeupIdleDecision> {
    const command = target.orcaCommand ?? this.command;
    let plan: OrcaWakePlan;
    try {
      plan = await this.resolveSessionPlan(target, preview, command, dedicatedTab, excludedHandles, advisory);
    } catch (error) {
      if (!target.workspacePath) throw error;
      // The workspace census can still prove absence when the terminal inventory fails.
      const workspace = await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, {
        targetTerminalHandle: null, advisory, throwOnProbeError: advisory,
      });
      if (workspace.reason !== 'Workspace not found') throw error;
      return { ...workspace, plan: { kind: 'reuse' } };
    }
    const withPlan = (idle: OrcaIdleResult): WakeupIdleDecision => ({ ...idle, plan });
    if (plan.kind === 'defer') return withPlan({ idle: false, reason: plan.reason });
    if (this.idleCheck) {
      return withPlan(await this.idleCheck());
    }
    if (target.driver === 'orca-run') {
      if (target.workspacePath) {
        return withPlan(await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, { advisory, throwOnProbeError: advisory }));
      }
      // Orchestration mail queues without touching a TUI composer, so no keystroke race exists.
      return withPlan({ idle: true });
    }
    if (target.workspacePath) {
      const opensTab = plan.kind !== 'reuse';
      const verifyAbsentForRecovery = plan.kind === 'recover-absent' ||
        (plan.kind === 'spawn-unbound' && Boolean(target.sessionId?.trim()));
      // A fresh tab is created empty, so its retained siblings must not block the wakeup; a bound
      // tab is the only composer that can receive the pointer. A rotation additionally waits for
      // the slot it replaces to finish, because that tab is closed after the launch commits.
      return withPlan(
        await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, {
          targetTerminalHandle: opensTab ? null : target.terminalHandle,
          targetPaneKey: opensTab ? undefined : target.paneKey,
          retiringTerminalHandle: opensTab ? target.terminalHandle : undefined,
          retiringPaneKey: opensTab ? target.paneKey : undefined,
          verifyAbsentForRecovery,
          agent: target.agent,
          excludedHandles,
          advisory, throwOnProbeError: advisory, readScreen: this.readScreen,
        }),
      );
    }
    if (target.terminalHandle) {
      return withPlan(
        await probeOrcaTerminalIdle(this.runner, command, target.terminalHandle, target.workspacePath, undefined, {
          throwOnProbeError: advisory, readScreen: this.readScreen,
        }),
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
  private waitFreshTabIdle(
    target: OrcaWakeTarget,
    terminalHandle: string,
    command: string,
    excludedHandles: ReadonlySet<string>,
    signal?: AbortSignal,
  ): Promise<OrcaIdleResult> {
    if (this.idleCheck) {
      return this.idleCheck();
    }
    return waitFreshOrcaTabIdle(this.runner, command, target, terminalHandle, {
      excludedHandles, signal, timeoutMs: this.freshTabIdleTimeoutMs, readScreen: this.readScreen,
    });
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
    const otherClaims = () => claimedOrcaTerminals(this.store, mailboxId, target, { includeRetired: false }).handles;
    if (retiredHandle === terminalHandle?.trim() || otherClaims().has(retiredHandle)) return clearRetirement();
    try {
      if (!this.idleCheck) {
        const idle = target.workspacePath
          ? await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, {
              targetTerminalHandle: null, retiringTerminalHandle: retiredHandle, agent: target.agent,
              excludedHandles: otherClaims(),
            })
          : await probeOrcaTerminalIdle(this.runner, command, retiredHandle);
        if (!idle.idle) return { revision, warning: idle.reason ?? 'Retired terminal is busy' };
      }
      if (this.store.getWakeTarget(mailboxId)?.bindingRevision !== revision) return { revision };
      if (otherClaims().has(retiredHandle)) return clearRetirement();
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

  /** Git changes affect the whole checkout, including sessions outside this slot's wake scope. */
  private async canSyncStandingWorktree(
    mailboxId: string, target: OrcaWakeTarget, command: string, workspacePath: string, revision: number,
  ): Promise<boolean> {
    const ownsWorkspace = () => {
      if (this.store.getWakeTarget(mailboxId)?.bindingRevision !== revision) return false;
      return !this.store.listWakeTargets().some(binding => {
        if (binding.mailboxId === mailboxId || binding.driver === 'passive') return false;
        const pending = binding.pendingWake || binding.deliveryInDoubt === true;
        if (binding.driver === 'agent-host') {
          const { workspacePath: path } = readAgentHostDriverConfig(binding);
          return path ? sameCallerWorkspace(workspacePath, path) : pending;
        }
        if (binding.driver !== 'orca-terminal' && binding.driver !== 'orca-run') return pending;
        const { workspacePath: path, terminalHandle, paneKey } = readOrcaDriverConfig(binding);
        if (path && !sameCallerWorkspace(workspacePath, path)) return false;
        const sharesTab = Boolean(terminalHandle && terminalHandle === target.terminalHandle) ||
          Boolean(paneKey && paneKey === target.paneKey);
        // Dormant session addresses remain available for thread continuation. Terminal inventory
        // below detects any retained tab that is still present, including aliases without a path.
        if (binding.driver === 'orca-terminal' && binding.mailboxId.startsWith('terminal:')) {
          return pending && (Boolean(path) || sharesTab);
        }
        // Without a workspace or coordinator address, an Orca run's scope is unknown.
        return Boolean(path) || sharesTab || (binding.driver === 'orca-run' && !terminalHandle && !paneKey);
      });
    };
    return ownsWorkspace() && (await checkOrcaWorkspaceIdle(this.runner, command, workspacePath, undefined, {
      exclusiveTerminal: { handle: target.terminalHandle, paneKey: target.paneKey },
      throwOnProbeError: true,
    })).idle && ownsWorkspace();
  }

  /** A restored pane can move to a new handle only after its original handle disappears. */
  private async retainedHandleBlockReason(
    command: string,
    workspacePath: string,
    terminalHandle: string,
    advisory = false,
  ): Promise<string | undefined> {
    const presence = await inspectOrcaWorkspaceTerminal(
      this.runner, command, workspacePath, terminalHandle, undefined, undefined, advisory,
    );
    if (presence === 'live') return 'Bound terminal is still live';
    if (presence === 'unusable') return 'Terminal is disconnected or not writable';
    return undefined;
  }

  private async boundTerminalReappeared(
    target: OrcaWakeTarget,
    command: string,
    workspacePath: string,
  ): Promise<boolean> {
    if (this.idleCheck || (!target.terminalHandle && !target.paneKey)) return false;
    return (await inspectOrcaWorkspaceTerminal(
      this.runner, command, workspacePath, target.terminalHandle, undefined, target.paneKey,
    )) !== 'absent';
  }

  private async deliver(
    mailboxId: string,
    target: OrcaWakeTarget,
    preview: MailboxCheckResult,
    options: {
      plan: Exclude<OrcaWakePlan, { kind: 'defer' }>;
      dedicatedTab: boolean;
      excludedHandles: ReadonlySet<string>;
      signal?: AbortSignal;
      onDeliveryBegun: () => void;
    },
    setActiveRevision: (revision: number) => void,
  ): Promise<WakeupDriverOutcome> {
    const command = target.orcaCommand ?? this.command;
    let activeRevision = target.bindingRevision ?? 0;
    let rebound = false;
    let checkedTerminal = false;
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
    const needsRotation = options.plan.kind === 'rotate';
    let rotated = false;

    if (options.plan.kind !== 'reuse') {
      // Rotation opens a new session tab and retires the replaced tab after launch commits.
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
        if (this.store.getWakeTarget(mailboxId)?.bindingRevision !== prior.revision) {
          return { kind: 'noop', reason: 'Wake launch superseded by a newer binding' };
        }
        if (prior.target) target = prior.target;
        activeRevision = prior.revision;
        setActiveRevision(activeRevision);
        if (target.retiredTerminalHandle) {
          return this.defer(mailboxId, target, prior.warning ?? 'Retired terminal close pending before next rotation');
        }
      }
      if (target.rotateAfterWaves !== undefined && !this.idleCheck) {
        try {
          await tryFastForwardCleanWorktree(this.runner, workspacePath,
            () => this.canSyncStandingWorktree(mailboxId, target, command, workspacePath, activeRevision));
        } catch (error) {
          this.report(error);
        }
      }
      if (options.plan.kind === 'recover-absent' && await this.boundTerminalReappeared(target, command, workspacePath)) {
        return this.defer(mailboxId, target, 'Bound Orca terminal reappeared before replacement launch');
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
      const idle = await this.waitFreshTabIdle(target, terminalHandle, command, options.excludedHandles, options.signal);
      if (!idle.idle) {
        const deferred = this.defer(
          mailboxId,
          target,
          idle.reason ?? (needsRotation
            ? 'Session rotated; waiting for tui-idle before wakeup'
            : 'Terminal spawned; waiting for tui-idle before wakeup'),
        );
        return needsRotation ? { ...deferred, rotated: true } : deferred;
      }
      checkedTerminal = true;
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
      if (plan.kind === 'unusable') return this.defer(mailboxId, target, 'Terminal is disconnected or not writable');
      // Another mailbox may have claimed a tab while Orca was listing the workspace.
      if (plan.kind !== 'create' && (
        claimedOrcaTerminals(this.store, mailboxId, target).handles.has(plan.handle) ||
        (!options.dedicatedTab && plan.handle !== target.terminalHandle && this.needsDedicatedTab(mailboxId, target))
      )) {
        return this.defer(mailboxId, target, 'Orca terminal ownership changed during selection');
      }
      if (plan.kind !== 'create' && target.terminalHandle && plan.handle !== target.terminalHandle) {
        const reason = await this.retainedHandleBlockReason(command, target.workspacePath, target.terminalHandle);
        if (reason) return this.defer(mailboxId, target, reason);
      }
      if (plan.kind === 'existing') {
        terminalHandle = plan.handle;
        rebound = plan.handle !== target.terminalHandle;
      } else {
        if (plan.kind === 'create' && (target.terminalHandle || target.paneKey || target.sessionId?.trim())) {
          const idle = await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, {
            targetTerminalHandle: null,
            retiringTerminalHandle: target.terminalHandle,
            retiringPaneKey: target.paneKey,
            agent: target.agent,
            excludedHandles: options.excludedHandles,
            verifyAbsentForRecovery: true,
          });
          if (!idle.idle) return this.defer(mailboxId, target, idle.reason ?? 'Workspace is busy');
          if (await this.boundTerminalReappeared(target, command, target.workspacePath)) {
            return this.defer(mailboxId, target, 'Bound Orca terminal reappeared before replacement launch');
          }
        }
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
        const idle = await this.waitFreshTabIdle(target, terminalHandle, command, options.excludedHandles, options.signal);
        if (!idle.idle)
          return this.defer(mailboxId, target, idle.reason ?? 'Terminal spawned; waiting for tui-idle before wakeup');
        checkedTerminal = true;
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
    if (target.driver === 'orca-terminal' && terminalHandle && !this.idleCheck && !checkedTerminal) {
      const idle = await checkOrcaPointerIdle(this.runner, command, target, terminalHandle, {
        excludedHandles: claimedOrcaTerminals(this.store, mailboxId, target).handles,
        signal: options.signal, readScreen: this.readScreen,
      });
      if (!idle.idle) return this.defer(mailboxId, target, idle.reason ?? 'Terminal is busy');
    }
    if (target.driver === 'orca-terminal' && terminalHandle && claimedOrcaTerminals(this.store, mailboxId, target).handles.has(terminalHandle)) {
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
    const deliveredSequence = maxPreviewSequence(preview);
    const pinnedSession = pinnedOrcaDeliverySession(target, terminalHandle);
    const fenced = this.store.beginWakeDelivery(mailboxId, activeRevision, { deliveredSequence, pinnedSession });
    if (!fenced) return { kind: 'noop', reason: 'Wake launch superseded by a newer binding' };
    options.onDeliveryBegun();
    if (target.driver === 'orca-run') {
      const output = await orcaMutationRunner(this.runner)({
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
        waitSubmit: DEFAULT_ORCA_WAIT_SUBMIT_SECONDS,
        expectedAgent: target.agent,
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
      deliveredSequence,
      {},
      pinnedSession,
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
    subscribeReadiness: orcaReadinessSubscription(options),
    deliver: (context) => new OrcaWakeDelivery(context, options).run(context),
    probeSlot: (context) => new OrcaWakeDelivery(context, options).probe(context),
    /**
     * An uninitialized secondary slot observes the same standing workspace as its base stream
     * without persisting a copy. Slot-private conversation identity is stripped so the inherited
     * projection can never point a probe or a wakeup at a sibling's retained tab.
     */
    inheritSlotTarget(mailboxId, baseTarget) {
      const base = readOrcaWakeTarget(baseTarget);
      if (
        baseTarget.driver !== 'orca-terminal' ||
        !base.workspacePath?.trim() ||
        base.configuredTerminalHandle?.trim() ||
        base.runId?.trim()
      ) {
        return undefined;
      }
      return {
        mailboxId,
        driver: 'orca-terminal',
        coalesceMs: baseTarget.coalesceMs,
        rotateAfterWaves: baseTarget.rotateAfterWaves,
        wakeupPrefix: baseTarget.wakeupPrefix,
        completedWaves: 0,
        pendingWake: false,
        earliestWakeAt: 0,
        attemptCount: 0,
        bindingRevision: 0,
        driverConfig: serializeOrcaDriverConfig({
          orcaCommand: base.orcaCommand,
          agent: base.agent,
          workspacePath: base.workspacePath,
          worktreeId: base.worktreeId,
          repoRoot: base.repoRoot,
          autoCreateWorkspace: base.autoCreateWorkspace,
          interactiveCommand: base.interactiveCommand,
          resumeCommand: base.resumeCommand,
        }),
      };
    },
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
    subscribeReadiness: orcaReadinessSubscription(options),
    deliver: (context) => new OrcaWakeDelivery(context, options).run(context),
    probeSlot: (context) => new OrcaWakeDelivery(context, options).probe(context),
  };
}

export function createOrcaWakeupDrivers(options: OrcaWakeupDriverOptions = {}): readonly WakeupDriver[] {
  return [createOrcaTerminalWakeupDriver(options), createOrcaRunWakeupDriver(options)];
}
