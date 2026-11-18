import { existsSync, realpathSync } from 'node:fs';
import { resolve } from 'node:path';
import {
  OrcaTerminalLaunchRejectedError,
  readOrcaTerminal,
  checkOrcaWorkspaceIdle,
  ensureOrcaWorkspace,
  isOrcaWorkspaceTerminalLive,
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
import { CommandNotStartedError, runCommand } from '../subprocess/index.ts';
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
import type { WakeupDriver, WakeupDriverContext, WakeupDriverOutcome } from '../mailbox/wakeup.ts';

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
  worktreeId?: string;
  workspacePath?: string;
  terminalHandle?: string;
  repoRoot?: string;
  autoCreateWorkspace?: boolean;
  workspaceComment?: string;
}
export interface OrcaRunDriverConfig extends OrcaTerminalDriverConfig {
  runId?: string;
}

type OrcaWakeTarget = WakeTargetState & OrcaRunDriverConfig;

export function readOrcaDriverConfig(target: Pick<WakeTargetState, 'driverConfig'>): OrcaRunDriverConfig {
  const config = target.driverConfig ?? {};
  for (const field of [
    'orcaCommand',
    'interactiveCommand', 'sessionId', 'paneKey', 'resumeCommand', 'configuredSessionId', 'configuredTerminalHandle', 'retiredSessionId',
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

function claimedOrcaTerminalHandles(store: MailboxStore, excludeMailboxId: string): Set<string> {
  const handles = new Set<string>();
  for (const item of store.listWakeTargets()) {
    if (item.mailboxId === excludeMailboxId || item.mailboxId.startsWith('terminal:') || item.driver !== 'orca-terminal') continue;
    const { terminalHandle } = readOrcaDriverConfig(item);
    if (terminalHandle) handles.add(terminalHandle);
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
  if (caller.terminalHandle && store && claimedOrcaTerminalHandles(store, id).has(caller.terminalHandle)) return false;
  if (existing?.driver !== 'orca-terminal') return false;
  const bound = readOrcaDriverConfig(existing);
  if (!sameCallerWorkspace(bound.workspacePath, caller.workspacePath)) return false;
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
  };
  let identity: Partial<OrcaWakeTarget>;
  if (addedOrChanged) {
    const handleBelongsToPrevious = nextConfiguredHandle !== undefined &&
      (nextConfiguredHandle === existing?.terminalHandle || handleUnchangedSinceConfig);
    identity = compatible && existing?.sessionId === nextConfiguredSession ? preserved : {
      completedWaves: 0, retiredSessionId: undefined,
      terminalHandle: handleBelongsToPrevious ? undefined : nextConfiguredHandle, paneKey: undefined, sessionId: nextConfiguredSession,
      agent: input.configuredAgent ?? (sameWorkspace && sameAgent ? existing?.agent : undefined),
    };
    identity = { ...identity, configuredSessionId: nextConfiguredSession };
  } else if (removed) {
    identity = existing?.sessionId === prevConfiguredSession || !compatible ? {
      completedWaves: 0, retiredSessionId: undefined,
      terminalHandle: nextConfiguredHandle, paneKey: undefined, sessionId: undefined,
      agent: input.configuredAgent ?? (compatible ? existing?.agent : undefined),
    } : preserved;
    identity = { ...identity, configuredSessionId: undefined, retiredSessionId: undefined };
  } else if (unchangedConfigured) {
    identity = compatible ? { ...preserved, retiredSessionId: existing?.retiredSessionId } : {
      completedWaves: 0, retiredSessionId: undefined,
      terminalHandle: nextConfiguredHandle, paneKey: undefined,
      sessionId: nextConfiguredSession, agent: input.configuredAgent,
    };
    identity = { ...identity, configuredSessionId: nextConfiguredSession };
  } else {
    identity = compatible ? { ...preserved, configuredSessionId: input.hasTopicBinding ? undefined : existing?.configuredSessionId } : {
      completedWaves: 0, retiredSessionId: undefined,
      terminalHandle: nextConfiguredHandle, paneKey: undefined, sessionId: undefined,
      configuredSessionId: undefined, agent: input.configuredAgent,
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
  };
}

export function pinOrcaTerminalSessionMailbox(
  store: MailboxStore,
  mailboxId: string,
  terminalHandle?: string,
): string {
  const id = normalizeMailboxId(mailboxId);
  const target = store.getWakeTarget(id);
  const handle = terminalHandle?.trim();
  if (!handle || target?.driver !== 'orca-terminal' || target.rotateAfterWaves === undefined) return id;
  return store.pinSessionMailbox(id, `terminal:${handle}`, {
    driver: 'orca-terminal',
    driverConfig: buildPinnedOrcaSessionDriverConfig(readOrcaWakeTarget(target), store.getWakeTarget(`terminal:${handle}`), handle),
  });
}

class OrcaWakeDelivery {
  private readonly store: MailboxStore;
  private readonly runner: CommandRunner;
  private readonly command: string;
  private readonly idleCheck?: () => Promise<OrcaIdleResult>;
  private readonly now: () => number;
  private readonly report: (error: unknown) => void;

  constructor(context: WakeupDriverContext, options: OrcaWakeupDriverOptions) {
    this.store = context.store;
    this.now = context.now;
    this.report = context.reportError;
    this.runner = options.commandRunner ?? runCommand;
    this.command = options.orcaCommand ?? 'orca';
    this.idleCheck = options.idleCheck;
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
      const excludedHandles = target.driver === 'orca-terminal' ? this.excludedHandles(mailboxId, target) : new Set<string>();
      if (target.terminalHandle && excludedHandles.has(target.terminalHandle)) {
        target = { ...target, terminalHandle: undefined, paneKey: undefined };
      }
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

  private excludedHandles(mailboxId: string, target: OrcaWakeTarget): Set<string> {
    const handles = claimedOrcaTerminalHandles(this.store, mailboxId);
    // A pinned session mailbox intentionally shares its own handle with the active stream.
    if (target.terminalHandle && mailboxId === `terminal:${target.terminalHandle}`) handles.delete(target.terminalHandle);
    return handles;
  }

  private needsDedicatedTab(mailboxId: string, target: OrcaWakeTarget): boolean {
    if (mailboxId.startsWith('task:') || target.rotateAfterWaves !== undefined) return true;
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
    if (target.mailboxId.startsWith('task:') || target.sessionId?.trim() || target.paneKey?.trim()) {
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
        return withPlan(await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath));
      }
      // Orchestration mail queues without touching a TUI composer, so no keystroke race exists.
      return withPlan({ idle: true });
    }
    if (target.workspacePath) {
      // A fresh tab is created empty, so its retained siblings must not block the wakeup; a bound
      // tab is the only composer that can receive the pointer.
      return withPlan(
        await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, {
          targetTerminalHandle: spawnFreshSession ? null : target.terminalHandle,
          targetPaneKey: spawnFreshSession ? undefined : target.paneKey,
          agent: target.agent,
          excludedHandles,
        }),
      );
    }
    if (target.terminalHandle) {
      return withPlan(
        await probeOrcaTerminalIdle(this.runner, command, target.terminalHandle, target.workspacePath),
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
    const needsRotation = needsSessionRotation(target, preview);
    const spawnFreshSession = !needsRotation && options.freshSession;
    let rotated = false;
    const dedicatedTaskTab =
      mailboxId.startsWith('task:') && target.driver === 'orca-terminal' && Boolean(target.workspacePath);
    const replaceTaskTab =
      dedicatedTaskTab &&
      !(needsRotation || spawnFreshSession) &&
      ((!terminalHandle && !target.paneKey) ||
        (!this.idleCheck &&
          !(await isOrcaWorkspaceTerminalLive(this.runner, command, target.workspacePath!, terminalHandle, undefined, target.paneKey))));

    if (needsRotation || spawnFreshSession || replaceTaskTab) {
      // Rotation never resets an existing TUI: it opens a new session tab and leaves every old
      // tab untouched. A missing workspace makes that impossible and must fail loudly; a
      // fresh-tab plan has already proven its workspace in `spawnsFreshSession`.
      if (!target.workspacePath) {
        throw new Error(`Orca session rotation requires a workspacePath: ${mailboxId}`);
      }
      const workspacePath = target.workspacePath;
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
        this.excludedHandles(mailboxId, target).has(plan.handle) ||
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

    const pointer = buildWakeupPointer(mailboxId, target, preview);
    if (!pointer) {
      const completed = this.store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview), {}, undefined, activeRevision);
      if (completed.superseded) return { kind: 'noop', reason: 'Wake launch superseded by a newer binding' };
      return { kind: 'noop', reason: 'Nothing new to deliver' };
    }

    if (target.driver === 'orca-run' && !target.runId) throw new Error('orca-run wake target requires runId');
    if (target.driver === 'orca-terminal' && !terminalHandle)
      throw new Error('orca-terminal wake target requires terminalHandle or workspacePath');
    if (target.driver === 'orca-terminal' && terminalHandle && this.excludedHandles(mailboxId, target).has(terminalHandle)) {
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

    let warning: string | undefined;
    const comment =
      target.workspaceComment ??
      (mailboxId.startsWith('stream:') ? `Dealer: ${mailboxId} pending` : undefined);
    if (comment !== undefined) {
      try {
        await setOrcaWorkspaceComment(this.runner, command, target.workspacePath, comment);
      } catch (error) {
        // The pointer is already delivered; retrying it would duplicate a wakeup.
        warning = String(error);
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
  };
}

export function createOrcaWakeupDrivers(options: OrcaWakeupDriverOptions = {}): readonly WakeupDriver[] {
  return [createOrcaTerminalWakeupDriver(options), createOrcaRunWakeupDriver(options)];
}
