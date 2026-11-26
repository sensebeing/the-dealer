import { ACTIVE_CONVERSATION_MODES } from '../core-types/index.ts';
import type { MessageEnvelope, SessionLinkResolution } from '../core-types/index.ts';
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
import { checkedCommand, runCommand, singleLine } from '../subprocess/index.ts';
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

const MAX_CODEX_THREAD_NAME_LENGTH = 80;

export interface CodexThreadNamePreview {
  readonly latestQueued?: { readonly envelope: Pick<MessageEnvelope, 'subject'> };
  readonly items: readonly { readonly envelope: Pick<MessageEnvelope, 'subject'> }[];
}

function formatLocalMonthDay(now: number): string {
  const date = new Date(now);
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${month}-${day}`;
}

function capThreadName(value: string): string {
  const normalized = singleLine(value);
  const chars = Array.from(normalized);
  if (chars.length <= MAX_CODEX_THREAD_NAME_LENGTH) return normalized;
  return `${chars.slice(0, MAX_CODEX_THREAD_NAME_LENGTH - 1).join('').trimEnd()}…`;
}

/** Build a concise, single-line sidebar title (<= 80 chars) for a newly started Codex thread. */
export function buildCodexThreadName(
  mailboxId: string,
  mode: ActiveConversationMode,
  preview: CodexThreadNamePreview,
  options: { now?: number; rotates?: boolean } = {},
): string {
  if (mode === 'spawn') {
    const rawSubject = preview.latestQueued?.envelope.subject ?? preview.items.at(-1)?.envelope.subject;
    const subject = typeof rawSubject === 'string' ? singleLine(rawSubject) : '';
    return capThreadName(subject || `Dealer · ${mailboxId}`);
  }
  const label = mailboxId.startsWith('stream:') ? mailboxId.slice(7) : mailboxId;
  if (options.rotates) {
    return capThreadName(`Dealer · ${label} (${formatLocalMonthDay(options.now ?? Date.now())})`);
  }
  return capThreadName(`Dealer · ${label}`);
}

/** Single authoritative predicate for whether an agent-host wake target is due to rotate sessions. */
export function isAgentHostRotationDue(
  target: WakeTargetState,
  options: { now?: number; hasOutstandingLease: boolean },
): boolean {
  if (target.driver !== 'agent-host') return false;
  const config = readAgentHostDriverConfig(target);
  if (!config.sessionId?.trim() || config.conversationMode === 'spawn' || options.hasOutstandingLease) {
    return false;
  }
  const now = options.now ?? Date.now();
  const waveRotationDue =
    target.rotateAfterWaves !== undefined &&
    target.completedWaves >= target.rotateAfterWaves;
  const ageRotationDue =
    config.rotateAfterMs !== undefined &&
    config.sessionStartedAt !== undefined &&
    now - config.sessionStartedAt >= config.rotateAfterMs;
  return waveRotationDue || ageRotationDue;
}

/**
 * Derive a host-neutral receiver-local session deep link from an `agent-host` wake target.
 * Returns `{ state: 'pending' }` when a Codex session has not yet been committed or is due to
 * rotate (suppressing any retiring thread link), and `undefined` for non-link hosts.
 */
export function resolveAgentHostSessionLink(
  target: WakeTargetState | undefined,
  options: { now?: number; hasOutstandingLease?: boolean } = {},
): SessionLinkResolution | undefined {
  if (!target || target.driver !== 'agent-host') return undefined;
  const config = readAgentHostDriverConfig(target);
  if (!config.hostAgent || normalizeAgentHost(config.hostAgent) !== 'codex') return undefined;
  const sessionId = config.sessionId?.trim();
  const rotationDue = isAgentHostRotationDue(target, {
    now: options.now,
    hasOutstandingLease: options.hasOutstandingLease ?? false,
  });
  if (!sessionId || rotationDue) {
    return { state: 'pending' };
  }
  return {
    state: 'ready',
    label: 'ChatGPT App',
    url: `codex://threads/${encodeURIComponent(sessionId)}`,
  };
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
    if (!state || !target?.hostAgent) throw new Error(`Passive wake target missing host agent: ${mailboxId}`);
    const preview = store.check({ mailboxId, peek: true });
    const pointer = buildWakeupPointer(mailboxId, target, preview);
    if (!pointer) throw new Error(`No pending Dealer messages in ${mailboxId}`);
    const hasOutstandingLease = Boolean(preview.deliveryId);
    const shouldRotate = isAgentHostRotationDue(state, { now, hasOutstandingLease });
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
    const resumeThread = effectiveMode === 'fixed' || (effectiveMode === 'wave' && Boolean(effectiveSessionId));
    const threadName = !resumeThread
      ? buildCodexThreadName(mailboxId, effectiveMode, preview, {
          now,
          rotates: target.rotateAfterWaves !== undefined || target.rotateAfterMs !== undefined,
        })
      : undefined;
    const output =
      canonicalHost === 'codex'
        ? await runCodexAppServerTurn({
            command: target.interactiveCommand ?? 'codex',
            cwd: target.workspacePath,
            mode: effectiveMode,
            sessionId: effectiveSessionId,
            threadName,
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
