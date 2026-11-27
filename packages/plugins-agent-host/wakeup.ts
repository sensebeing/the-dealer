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

export interface AgentHostDeliveredSession {
  readonly fromSequence: number;
  readonly throughSequence: number;
  readonly sessionId: string;
}

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
  deliveredSessions?: readonly AgentHostDeliveredSession[];
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
  if (config.deliveredSessions !== undefined) {
    if (
      !Array.isArray(config.deliveredSessions) ||
      config.deliveredSessions.some(entry => {
        if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return true;
        const record = entry as Record<string, unknown>;
        return (
          typeof record.fromSequence !== 'number' ||
          !Number.isSafeInteger(record.fromSequence) ||
          record.fromSequence < 0 ||
          typeof record.throughSequence !== 'number' ||
          !Number.isSafeInteger(record.throughSequence) ||
          record.throughSequence < record.fromSequence ||
          typeof record.sessionId !== 'string' ||
          record.sessionId.trim().length === 0
        );
      })
    ) {
      throw new Error('Invalid host configuration: deliveredSessions');
    }
  }
  return config as AgentHostDriverConfig;
}

function appendAgentHostDeliveredSession(
  existing: readonly AgentHostDeliveredSession[] | undefined,
  fromSequence: number,
  throughSequence: number,
  sessionId: string,
  minRetainedSequence?: number,
): AgentHostDeliveredSession[] | undefined {
  const trimmed = sessionId.trim();
  if (!trimmed || throughSequence < 0) return existing ? existing.map(entry => ({ ...entry })) : undefined;
  const normalizedFrom = Math.max(0, Math.min(fromSequence, throughSequence));
  const list = existing ? existing.map(entry => ({ ...entry })) : [];
  const last = list.at(-1);
  if (last && last.sessionId === trimmed) {
    last.fromSequence = Math.min(last.fromSequence, normalizedFrom);
    last.throughSequence = Math.max(last.throughSequence, throughSequence);
  } else {
    list.push({ fromSequence: normalizedFrom, throughSequence, sessionId: trimmed });
  }
  if (minRetainedSequence === undefined) {
    return list;
  }
  const retained = list.filter(
    (entry, index) => index === list.length - 1 || entry.throughSequence >= minRetainedSequence,
  );
  return retained;
}

function minPreviewSequence(
  preview: { items: readonly { sequence: number }[]; latestQueued?: { sequence: number } },
  maxSequence: number,
): number {
  let min = preview.latestQueued ? preview.latestQueued.sequence : maxSequence;
  for (const item of preview.items) {
    if (item.sequence < min) min = item.sequence;
  }
  return Math.max(0, Math.min(min, maxSequence));
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
 * When `itemSequence` is provided, checks the committed `deliveredSessions` association first so
 * historical/acked/rotated items always resolve to the exact session that received them.
 * For undelivered items, returns `{ state: 'pending' }` for fresh `spawn` targets (ignoring any
 * pre-launch configured `sessionId`) and when a Codex session has not yet been committed or is
 * due to rotate, and `undefined` for non-link hosts.
 */
export function resolveAgentHostSessionLink(
  target: WakeTargetState | undefined,
  options: { now?: number; hasOutstandingLease?: boolean; itemSequence?: number } = {},
): SessionLinkResolution | undefined {
  if (!target || target.driver !== 'agent-host') return undefined;
  const config = readAgentHostDriverConfig(target);
  if (!config.hostAgent || normalizeAgentHost(config.hostAgent) !== 'codex') return undefined;
  const deliveredSessions = config.deliveredSessions;
  if (options.itemSequence !== undefined && deliveredSessions && deliveredSessions.length > 0) {
    for (let index = deliveredSessions.length - 1; index >= 0; index -= 1) {
      const entry = deliveredSessions[index];
      if (options.itemSequence >= entry.fromSequence && options.itemSequence <= entry.throughSequence) {
        return {
          state: 'ready',
          label: 'ChatGPT App',
          url: `codex://threads/${encodeURIComponent(entry.sessionId)}`,
          committed: true,
        };
      }
    }
    const maxCommitted = deliveredSessions.reduce((max, entry) => Math.max(max, entry.throughSequence), -1);
    if (options.itemSequence <= maxCommitted) {
      return { state: 'unavailable' };
    }
  }
  // A spawn target always starts a fresh Codex thread via thread/start, even if a configured
  // sessionId was seeded on the wake target before launch; wait for committed thread identity.
  if (config.conversationMode === 'spawn') {
    return { state: 'pending' };
  }
  const sessionId = config.sessionId?.trim();
  const rotationDue = isAgentHostRotationDue(target, {
    now: options.now,
    // While a wave launch is fenced under deliveryInDoubt, the agent acquiring its lease mid-turn
    // must not mask a pending rotation and leak the retiring sessionId before completeWakeDelivery.
    hasOutstandingLease: target.deliveryInDoubt ? false : (options.hasOutstandingLease ?? false),
  });
  if (!sessionId || rotationDue) {
    return { state: 'pending' };
  }
  return {
    state: 'ready',
    label: 'ChatGPT App',
    url: `codex://threads/${encodeURIComponent(sessionId)}`,
    ...(options.itemSequence !== undefined ? { committed: false } : {}),
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
): Promise<{ pointer: string; stdout: string; stderr?: string; rotated?: boolean }> {
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
    const allowActiveWriterFallback = canonicalHost === 'codex' && resumeThread && !hasOutstandingLease;
    const canFallbackOnActiveWriter = (): boolean => {
      const latestTarget = store.getWakeTarget(mailboxId);
      if (!latestTarget || (latestTarget.bindingRevision ?? 0) !== expectedRevision) return false;
      return !store.check({ mailboxId, peek: true }).deliveryId;
    };
    const threadName = !resumeThread || allowActiveWriterFallback
      ? buildCodexThreadName(mailboxId, effectiveMode, preview, {
          now,
          rotates:
            target.rotateAfterWaves !== undefined ||
            target.rotateAfterMs !== undefined ||
            allowActiveWriterFallback,
        })
      : undefined;
    const codexOutput =
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
            fallbackOnActiveWriter: allowActiveWriterFallback ? canFallbackOnActiveWriter : false,
          })
        : undefined;
    const output =
      codexOutput ??
      (await checkedCommand(runner, {
        ...invocation,
        cwd: target.workspacePath,
        signal: options.signal,
      }));
    const rotatedByActiveWriter = Boolean(codexOutput?.rotatedFromThreadId);
    const rotatedSession = shouldRotate || rotatedByActiveWriter;
    const continuation = resolveAgentHostContinuation(
      target.hostAgent,
      mailboxId,
      rotatedByActiveWriter ? undefined : effectiveSessionId,
      output.stdout,
      rotatedByActiveWriter ? 'wave' : effectiveMode,
    );
    const sessionStartedAt = continuation.sessionId
      ? (rotatedSession || !target.sessionId || continuation.sessionId !== target.sessionId
          ? now
          : (target.sessionStartedAt ?? now))
      : undefined;
    const deliveredSequence = maxPreviewSequence(preview);
    const deliveredFromSequence = minPreviewSequence(preview, deliveredSequence);
    const completed = store.completeWakeDelivery(
      mailboxId,
      deliveredSequence,
      (lockedCurrent, completionContext) => {
        const currentConfig =
          lockedCurrent.driver === 'agent-host' ? readAgentHostDriverConfig(lockedCurrent) : undefined;
        const consumedThroughSequence = Math.max(deliveredSequence, completionContext.maxConsumedSequence);
        const deliveredSessions = continuation.sessionId
          ? appendAgentHostDeliveredSession(
              currentConfig?.deliveredSessions,
              deliveredFromSequence,
              consumedThroughSequence,
              continuation.sessionId,
              completionContext.minRetainedSequence,
            )
          : currentConfig?.deliveredSessions;
        const nextDriverConfig = continuation.hostAgent
          ? serializeAgentHostDriverConfig({
              hostAgent: continuation.hostAgent,
              conversationMode: continuation.conversationMode,
              sessionId: continuation.sessionId,
              ...(target.rotateAfterMs !== undefined ? { rotateAfterMs: target.rotateAfterMs } : {}),
              ...(sessionStartedAt !== undefined ? { sessionStartedAt } : {}),
              ...(deliveredSessions !== undefined ? { deliveredSessions } : {}),
            })
          : undefined;
        return {
          driver: continuation.hostAgent ? 'agent-host' : 'passive',
          driverConfig: nextDriverConfig,
          ...(rotatedSession
            ? { completedWaves: Math.max(0, lockedCurrent.completedWaves - preRotationCompletedWaves) }
            : {}),
        };
      },
      undefined,
      expectedRevision,
    );
    if (completed.superseded) throw new SettledWakeLaunchError('Wake launch superseded by a newer binding');
    return {
      pointer,
      stdout: output.stdout,
      stderr: output.stderr,
      ...(rotatedSession ? { rotated: true } : {}),
    };
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
        return {
          kind: 'delivered',
          pointer: delivered.pointer,
          ...(delivered.rotated ? { rotated: true } : {}),
        };
      } catch (error) {
        if (error instanceof SettledWakeLaunchError) throw error;
        throw new SettledWakeLaunchError(String(error), { cause: error });
      }
    },
  };
}
