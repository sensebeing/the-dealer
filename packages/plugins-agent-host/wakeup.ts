import { ACTIVE_CONVERSATION_MODES } from '../core-types/index.ts';
import {
  buildAgentHostInvocation,
  normalizeAgentHost,
  parseAgentHost,
  resolveAgentHostContinuation,
  resolveAgentSessionId,
  trustAntigravityWorkspace,
} from './hosts.ts';
import type { SupportedAgentHost, ActiveConversationMode } from './hosts.ts';
import { runCodexAppServerTurn } from './codex-app-server.ts';
import { checkedCommand, runCommand } from '../subprocess/index.ts';
import type { CommandRunner, StdioProcessSpawner } from '../subprocess/index.ts';
import {
  buildWakeupPointer,
  maxPreviewSequence,
  settleWakeDeliveryFailure,
  SettledWakeLaunchError,
} from '../mailbox/wakeup.ts';
import type { WakeupDriver } from '../mailbox/wakeup.ts';
import type { MailboxStore, WakeTargetState } from '../mailbox/store.ts';

export interface AgentHostDriverConfig {
  hostAgent?: SupportedAgentHost;
  conversationMode?: ActiveConversationMode;
  workspacePath?: string;
  interactiveCommand?: string;
  agentProfile?: string;
  targetBot?: string;
  sessionId?: string;
  configuredSessionId?: string;
  rotateAfterMs?: number;
  sessionStartedAt?: number;
}

export function serializeAgentHostDriverConfig(config: AgentHostDriverConfig): Record<string, unknown> {
  const value = Object.fromEntries(Object.entries(config).filter(([, entry]) => entry !== undefined));
  readAgentHostDriverConfig({ driverConfig: value });
  return value;
}

export function readAgentHostDriverConfig(
  target: Pick<WakeTargetState, 'driverConfig'>,
): AgentHostDriverConfig {
  const config = target.driverConfig ?? {};
  if (config.hostAgent !== undefined) parseAgentHost(config.hostAgent);
  if (
    config.conversationMode !== undefined &&
    !ACTIVE_CONVERSATION_MODES.includes(config.conversationMode as ActiveConversationMode)
  ) {
    throw new Error('Invalid host conversation mode');
  }
  for (const field of ['workspacePath', 'interactiveCommand', 'agentProfile', 'targetBot', 'sessionId', 'configuredSessionId']) {
    if (config[field] !== undefined && typeof config[field] !== 'string')
      throw new Error(`Invalid host configuration: ${field}`);
  }
  if (
    config.rotateAfterMs !== undefined &&
    (typeof config.rotateAfterMs !== 'number' || !Number.isSafeInteger(config.rotateAfterMs) || config.rotateAfterMs < 1)
  ) {
    throw new Error('Invalid host configuration: rotateAfterMs');
  }
  if (
    config.sessionStartedAt !== undefined &&
    (typeof config.sessionStartedAt !== 'number' || !Number.isFinite(config.sessionStartedAt) || config.sessionStartedAt < 0)
  ) {
    throw new Error('Invalid host configuration: sessionStartedAt');
  }
  return config as AgentHostDriverConfig;
}

function rotatedSessionId(
  hostAgent: SupportedAgentHost,
  mailboxId: string,
  now: number,
): string | undefined {
  const canonical = normalizeAgentHost(hostAgent);
  if (canonical === 'codex' || canonical === 'antigravity') return undefined;
  if (canonical === 'claude-code') {
    return `${resolveAgentSessionId(mailboxId, undefined, hostAgent)}_r${now}`;
  }
  return `${mailboxId}#${now}`;
}

/**
 * Single owner for one passive host delivery attempt. Validation, pointer construction and
 * invocation building happen before `beginWakeDelivery`; only the host invocation itself crosses
 * the external boundary, so unknown outcomes stay fenced while never-started commands retry.
 */
export async function deliverAgentHostWakeTarget(
  store: MailboxStore,
  runner: CommandRunner,
  mailboxId: string,
  options: {
    signal?: AbortSignal;
    now?: number;
    stdioSpawner?: StdioProcessSpawner;
    agySettingsPath?: string;
  } = {},
): Promise<{ pointer: string; stdout: string; stderr?: string }> {
  const now = options.now ?? Date.now();
  let deliveryBegun = false;
  let expectedRevision = 0;
  try {
    const state = store.getWakeTarget(mailboxId);
    expectedRevision = state?.bindingRevision ?? 0;
    const target = state ? { ...readAgentHostDriverConfig(state), ...state } : undefined;
    if (!target?.hostAgent) throw new Error(`Passive wake target missing host agent: ${mailboxId}`);
    const preview = store.check({ mailboxId, peek: true });
    const pointer = buildWakeupPointer(mailboxId, target, preview);
    if (!pointer) throw new Error(`No pending Dealer messages in ${mailboxId}`);
    const hasOutstandingLease = Boolean(preview.deliveryId);
    const waveRotationDue =
      !hasOutstandingLease &&
      target.rotateAfterWaves !== undefined &&
      target.completedWaves >= target.rotateAfterWaves;
    const ageRotationDue =
      !hasOutstandingLease &&
      target.rotateAfterMs !== undefined &&
      target.sessionStartedAt !== undefined &&
      now - target.sessionStartedAt >= target.rotateAfterMs;
    const shouldRotate =
      Boolean(target.sessionId) &&
      target.conversationMode !== 'spawn' &&
      (waveRotationDue || ageRotationDue);
    const canonicalHost = normalizeAgentHost(target.hostAgent);
    const effectiveMode: ActiveConversationMode = shouldRotate
      ? 'wave'
      : canonicalHost === 'claude-code' &&
          target.conversationMode === 'wave' &&
          Boolean(target.sessionId) &&
          target.sessionStartedAt !== undefined
        ? 'fixed'
        : (target.conversationMode ?? 'fixed');
    const effectiveSessionId = shouldRotate
      ? rotatedSessionId(target.hostAgent, mailboxId, now)
      : target.sessionId;
    const invocation = buildAgentHostInvocation({
      agent: target.hostAgent,
      conversationMode: effectiveMode,
      mailboxId,
      promptText: pointer,
      cwd: target.workspacePath,
      command: target.interactiveCommand,
      agentProfile: target.agentProfile,
      targetBot: target.targetBot,
      sessionId: effectiveSessionId,
    });
    const mayTrustWorkspace = options.agySettingsPath !== undefined || runner === runCommand;
    if (
      mayTrustWorkspace &&
      (canonicalHost === 'antigravity' || canonicalHost === 'antigravity-app') &&
      target.workspacePath
    ) {
      trustAntigravityWorkspace(target.workspacePath, options.agySettingsPath);
    }
    const preRotationCompletedWaves = target.completedWaves;
    const fence = store.tryBeginWakeDelivery(mailboxId, expectedRevision, {
      requireNoOutstandingLease: shouldRotate,
    });
    if (fence.kind === 'superseded') {
      throw new SettledWakeLaunchError('Wake launch superseded by a newer binding');
    }
    if (fence.kind === 'outstanding_lease') {
      // A lease was acquired concurrently before rotation fenced; defer without fencing
      // or launching a fresh rotated session while that lease is outstanding.
      throw new Error(`Cannot rotate session while delivery lease ${fence.leaseId} is outstanding`);
    }
    deliveryBegun = true;
    const output =
      canonicalHost === 'codex'
        ? await runCodexAppServerTurn({
            command: target.interactiveCommand ?? 'codex',
            cwd: target.workspacePath,
            mode: effectiveMode,
            sessionId: effectiveSessionId,
            promptText: pointer,
            agentProfile: target.agentProfile,
            signal: options.signal,
            stdioSpawner: options.stdioSpawner,
          })
        : await checkedCommand(runner, {
            ...invocation,
            cwd: target.workspacePath,
            signal: options.signal,
          });
    const continuation = resolveAgentHostContinuation(
      target.hostAgent,
      mailboxId,
      effectiveSessionId,
      output.stdout,
      effectiveMode,
    );
    const sessionStartedAt = continuation.sessionId
      ? (shouldRotate || !target.sessionId || continuation.sessionId !== target.sessionId
          ? now
          : (target.sessionStartedAt ?? now))
      : undefined;
    const nextDriverConfig = continuation.hostAgent
      ? serializeAgentHostDriverConfig({
          hostAgent: continuation.hostAgent,
          conversationMode: continuation.conversationMode,
          sessionId: continuation.sessionId,
          ...(target.rotateAfterMs !== undefined ? { rotateAfterMs: target.rotateAfterMs } : {}),
          ...(sessionStartedAt !== undefined ? { sessionStartedAt } : {}),
        })
      : undefined;
    const completed = store.completeWakeDelivery(
      mailboxId,
      maxPreviewSequence(preview),
      lockedCurrent => ({
        driver: continuation.hostAgent ? 'agent-host' : 'passive',
        driverConfig: nextDriverConfig,
        ...(shouldRotate
          ? { completedWaves: Math.max(0, lockedCurrent.completedWaves - preRotationCompletedWaves) }
          : {}),
      }),
      undefined,
      expectedRevision,
    );
    if (completed.superseded) throw new SettledWakeLaunchError('Wake launch superseded by a newer binding');
    return { pointer, stdout: output.stdout, stderr: output.stderr };
  } catch (error) {
    const fenced = store.getWakeTarget(mailboxId)?.deliveryInDoubt === true;
    if (fenced) {
      // A pre-existing fence refuses the attempt before the boundary; it keeps the original
      // unknown-outcome reason. Only this attempt's own post-boundary failure may settle it.
      if (deliveryBegun && settleWakeDeliveryFailure(store, mailboxId, error, now, expectedRevision)) {
        throw new SettledWakeLaunchError('Wake launch superseded by a newer binding');
      }
    } else {
      if (!(error instanceof SettledWakeLaunchError) && store.deferWakeAttempt(mailboxId, expectedRevision, error, now).superseded) {
        throw new SettledWakeLaunchError('Wake launch superseded by a newer binding');
      }
    }
    throw error;
  }
}

export function createAgentHostWakeupDriver(
  options: {
    commandRunner?: CommandRunner;
    stdioSpawner?: StdioProcessSpawner;
    agySettingsPath?: string;
  } = {},
): WakeupDriver {
  return {
    name: 'agent-host',
    async deliver({ store, mailboxId, now, signal }) {
      try {
        const delivered = await deliverAgentHostWakeTarget(
          store,
          options.commandRunner ?? runCommand,
          mailboxId,
          {
            now: now(),
            signal,
            stdioSpawner: options.stdioSpawner,
            agySettingsPath: options.agySettingsPath,
          },
        );
        return { kind: 'delivered', pointer: delivered.pointer };
      } catch (error) {
        if (error instanceof SettledWakeLaunchError) throw error;
        throw new SettledWakeLaunchError(String(error), { cause: error });
      }
    },
  };
}
