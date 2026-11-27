import { randomBytes, randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { lstatSync, readFileSync } from 'node:fs';
import { extname, resolve } from 'node:path';
import {
  canonicalJson,
  committedOutboxTargets,
  isValidMailboxId,
  validateSemanticHeader,
  verifyEnvelope,
} from '../core-types/index.ts';
import type { Disposable, MessageEnvelope, OutboxItem, SettlementOutcome } from '../core-types/index.ts';
import { normalizeConversationModeInput } from '../core-types/index.ts';
import type { AgentConversationMode } from '../core-types/index.ts';
import { withOwnedLock } from '../fs-atomic/index.ts';
import { netKey } from './network-key.ts';
import { isTerminalHistory } from './outbox.ts';
import type { OutboxStore } from './outbox.ts';
import { MailboxSqliteBackend } from './sqlite-backend.ts';
import type { MailboxInitialization, ThreadRetentionRow } from './sqlite-backend.ts';
export { CONVERSATION_MODES, ACTIVE_CONVERSATION_MODES } from '../core-types/index.ts';
export type { AgentConversationMode, ActiveConversationMode } from '../core-types/index.ts';
export { normalizeConversationModeInput } from '../core-types/index.ts';

export const MAILBOX_SCHEMA = 'the-dealer-mailbox/v2';
export const LEGACY_MAILBOX_V1_SCHEMA = 'the-dealer-mailbox/v1';
const LEGACY_INBOX_SCHEMA = 'the-dealer-inbox/v1';
const MAX_ACKED_LEASE_HISTORY = 256;
export const MAX_RETAINED_ACKED_ITEMS = 200;
export const MAX_RETAINED_WAVE_THREADS = 100;
export const MAX_RETAINED_SETTLED_THREADS = 200;
const MAX_RETAINED_REPLIED_THREADS = 200;
const MAX_WAKEUP_HINT_LENGTH = 120;
const DEFAULT_WAIT_TIMEOUT_MS = 30_000;
const MAX_WAIT_TIMEOUT_MS = 3_600_000;
const LEASE_ID_PATTERN = /^dlv_[0-9a-f]{12}$/;
const WAKE_DRIVER_ID_PATTERN = /^[a-z0-9][a-z0-9:_-]{0,63}$/i;

export type MailboxItemStatus = 'queued' | 'leased' | 'acked';

export interface MailboxItem {
  readonly id: string;
  readonly mailboxId: string;
  readonly status: MailboxItemStatus;
  readonly leaseId?: string;
  readonly receivedAt: string;
  readonly leasedAt?: string;
  readonly ackedAt?: string;
  readonly wakeupHint?: string;
  /** Monotonic insertion order, used to break ties between equal received instants. */
  readonly sequence: number;
  readonly envelope: MessageEnvelope;
  readonly signedEnvelope?: MessageEnvelope;
}

export interface DeliveryLease {
  readonly leaseId: string;
  readonly mailboxId: string;
  readonly itemIds: readonly string[];
  readonly createdAt: string;
  readonly wakeCount: number;
}

export interface MailboxStateRecord {
  readonly mailboxId: string;
  readonly outstanding?: DeliveryLease;
  readonly ackedLeaseIds: readonly string[];
}

const DEFAULT_BACKOFF_MS = [1_000, 2_000, 5_000] as const;
const MAX_BACKOFF_MS = 10_000;

/** Exponential busy-backoff: 1s -> 2s -> 5s -> capped at 10s. */
export function wakeupBackoffDelay(attemptCount: number): number {
  if (!Number.isSafeInteger(attemptCount) || attemptCount < 1) {
    throw new Error('Invalid wake attempt count');
  }
  if (attemptCount <= DEFAULT_BACKOFF_MS.length) {
    return DEFAULT_BACKOFF_MS[attemptCount - 1] ?? MAX_BACKOFF_MS;
  }
  return MAX_BACKOFF_MS;
}

export interface WakeTargetState {
  readonly bindingRevision?: number;
  readonly mailboxId: string;
  /** Registered driver id, or passive for in-process waitForMail targets. */
  readonly driver: string;
  /** JSON configuration owned and interpreted exclusively by the registered driver. */
  readonly driverConfig?: Readonly<Record<string, unknown>>;
  readonly wakeupPrefix?: string;
  readonly coalesceMs: number;
  readonly rotateAfterWaves?: number;
  readonly completedWaves: number;
  readonly pendingWake: boolean;
  readonly earliestWakeAt: number;
  readonly attemptCount: number;
  readonly lastWakeError?: string;
  /** Persisted before an external delivery; remains fenced while its outcome is unknown. */
  readonly deliveryInDoubt?: boolean;
  readonly pendingDelivery?: PendingWakeDelivery;
}

type PinnedSessionMailbox = SessionMailboxPatch & { sessionMailboxId: string };

export interface PendingWakeDelivery {
  readonly bindingRevision: number;
  readonly originRevision?: number;
  readonly deliveredSequence: number;
  readonly queuedItemIds: readonly string[];
  readonly pinnedSession?: PinnedSessionMailbox;
}

export interface ThreadRouteRecord {
  readonly envelope?: MessageEnvelope;
  readonly threadId: string;
  readonly messageId: string;
  readonly direction: 'inbound' | 'outbound';
  readonly peerNodeId: string;
  readonly peerNodeIds: readonly string[];
  readonly networkId: string;
  readonly peerReplyMailbox: string;
  readonly localMailbox: string;
  /** False for inbound history recorded before or without durable mailbox delivery. */
  readonly mailboxCommitted?: boolean;
  readonly traceId: string;
  readonly kind: string;
  readonly subject: string;
  readonly state: 'open' | 'replied' | 'settled';
  readonly settlement?: {
    readonly outcome: SettlementOutcome;
    readonly summary: string;
    readonly settledAt: string;
  };
  readonly createdAt: string;
  readonly updatedAt: string;
}

export interface ThreadSummaryRecord {
  threadId: string;
  rootMessageId: string;
  latestMessageId: string;
  kind: string;
  subject: string;
  state: ThreadRouteRecord['state'];
  settlement?: ThreadRouteRecord['settlement'];
  localMailbox: string;
  peerNodeIds: string[];
  hopCount: number;
  createdAt: string;
  updatedAt: string;
  rootEnvelope?: MessageEnvelope;
  latestEnvelope?: MessageEnvelope;
}

/** Result of removing one orphan outbound thread record and recomputing the thread. */
export interface PruneOutboundThreadResult {
  pruned: ThreadRouteRecord;
  threadId: string;
  remainingCount: number;
  state: ThreadRouteRecord['state'];
  settlement?: ThreadRouteRecord['settlement'];
}

export interface ResolvedReplyTarget {
  threadId: string;
  messageId: string;
  peerNodeId: string;
  networkId: string;
  targetMailbox: string;
  localMailbox: string;
  traceId: string;
  kind: string;
  subject: string;
}

export type BeginWakeDeliveryResult =
  | { kind: 'fenced'; target: WakeTargetState }
  | { kind: 'superseded'; target: WakeTargetState }
  | { kind: 'outstanding_lease'; target: WakeTargetState; leaseId: string };

/** Authoritative mailbox sequence bounds observed inside the completeWakeDelivery transaction. */
export interface CompleteWakeDeliveryContext {
  /** Highest sequence covered by the launch preview or leased/acked in this mailbox. */
  readonly maxConsumedSequence: number;
  /** Lowest sequence still protected by mailbox retention or the catch-up watermark. */
  readonly minRetainedSequence?: number;
}

export interface MailboxStoreOptions {
  /** Legacy flat inbox file migrated once into the default mailbox when no mailbox exists. */
  legacyInboxPath?: string;
  /** Existing directory lock used only during legacy cutover. */
  lockDir?: string;
  readOnly?: boolean;
  /** Peer snapshot used to retain thread routes that outbox reconciliation could restore. */
  outbox?: OutboxStore;
  /** Highest item sequence durably consumed by a catch-up reader, if one is configured. */
  readConsumedSequence?: () => number | undefined;
}

export interface MailboxEnqueueOptions {
  signedEnvelopes?: Readonly<Record<string, MessageEnvelope>>;
  mailboxId?: string;
  proposedSpawn?: boolean;
  wakeupHint?: string;
  wakeTarget?: Partial<WakeTargetState> | ((lockedCurrent: WakeTargetState | undefined) => Partial<WakeTargetState>);
}

export interface MailboxEnqueueResult {
  inserted: string[];
  duplicateIds: string[];
  mailboxIds: string[];
  envelopeMailboxes: Record<string, string>;
  claimedSpawn?: boolean;
}

export interface MailboxCheckOptions {
  mailboxId?: string;
  ackLeaseId?: string;
  peek?: boolean;
}

export interface MailboxCheckResult {
  mailboxId: string;
  deliveryId: string | null;
  redelivered: boolean;
  newerQueuedCount: number;
  latestQueued?: MailboxItem;
  /** Sequence witness is independent of receivedAt ordering (including clock rollback). */
  maxQueuedSequence?: number;
  items: MailboxItem[];
}

export interface SlotObligationSnapshot {
  hasPendingWave: boolean;
  immediateBusyReason?: 'wakeup_pending' | 'session_reply_pending' | 'local_outbound_pending' | 'outbound_thread_open';
}

export interface MailboxWaitOptions extends MailboxCheckOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

export interface MailboxWaitResult extends MailboxCheckResult {
  timedOut: boolean;
}

export interface MailboxAckResult {
  mailboxId: string;
  leaseId: string;
  itemIds: string[];
  alreadyAcknowledged: boolean;
}

export interface MailboxListOptions {
  mailboxId?: string;
  includeAcked?: boolean;
  limit?: number;
}

export interface MailboxListResult {
  mailboxId?: string;
  totalCount: number;
  queuedCount: number;
  leasedCount: number;
  ackedCount: number;
  items: MailboxItem[];
}

export interface QueueStatusOptions {
  mailboxId?: string;
  includeIdle?: boolean;
  now?: number;
}

export interface QueueSummary {
  activeMailboxes: number;
  totalMailboxes: number;
  queuedItems: number;
  leasedItems: number;
  pendingWakes: number;
  fencedTargets: number;
}

export interface QueueStatus {
  mailboxId: string;
  queuedCount: number;
  leasedCount: number;
  activeLease?: { leaseId: string; itemCount: number; createdAt: string; ageMs: number };
  pendingWake: boolean;
  deliveryInDoubt: boolean;
  attemptCount: number;
  lastWakeError?: string;
  nextWakeInMs: number | null;
  driver: string | null;
  driverConfig?: WakeTargetState['driverConfig'];
}

interface LegacyMailboxState {
  sequence: number;
  items: MailboxItem[];
  mailboxes: Record<string, MailboxStateRecord>;
  wakeTargets: Record<string, WakeTargetState>;
  threads: Record<string, ThreadRouteRecord>;
}

interface LegacyInboxAttachment {
  name: string;
  mimeType: string;
  sizeBytes: number;
  blake3Hash: string;
  localPath: string;
}

interface LegacyInboxItem {
  id: string;
  networkId: string;
  topic: string;
  priority: MessageEnvelope['priority'];
  from: MessageEnvelope['from'];
  subject: string;
  body: string;
  payload: Record<string, unknown>;
  attachments: LegacyInboxAttachment[];
  createdAt: string;
  receivedAt: string;
  status: 'unread' | 'read';
  readAt?: string;
}

function invalidStore(): Error {
  return new Error('Invalid mailbox store');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

/** Validate a logical mailbox id and return it unchanged. */
export function normalizeMailboxId(value: unknown, label = 'mailbox id'): string {
  if (!isValidMailboxId(value)) {
    const shown =
      typeof value === 'string' ? JSON.stringify(value.slice(0, 64)) : typeof value;
    throw new Error(`Invalid ${label}: ${shown}`);
  }
  return value;
}

/**
 * Deterministic logical mailbox resolution shared by the inbox executor, wake targets and
 * thread reverse routes. Single-owner precedence: semantic target, thread-local affinity,
 * explicit payload mailbox, mode/stream topic binding, default.
 */
export interface MailboxTopicBinding {
  mailboxId?: string;
  streamKey?: string;
  mode?: AgentConversationMode;
  conversationMode?: AgentConversationMode;
  /** Receiver-owned addressing policy independent of the delivery driver. */
  stream?: boolean;
}

export function resolveEffectiveConversationMode(options: {
  binding?: Partial<MailboxTopicBinding>;
  payload?: Record<string, unknown>;
  sources?: ReadonlyArray<{ mode?: unknown; conversationMode?: unknown } | undefined>;
  fallbackMode?: AgentConversationMode;
} = {}): AgentConversationMode {
  const layers = options.sources ?? [options.payload, options.binding, options.fallbackMode !== undefined ? { mode: options.fallbackMode } : undefined];
  for (const layer of layers) {
    const resolved = normalizeConversationModeInput(layer);
    if (resolved !== undefined) return resolved;
  }
  return options.binding?.streamKey !== undefined ? 'wave' : 'inbox-only';
}

export function isStreamConversationBinding(binding: Partial<MailboxTopicBinding>, fallbackMode?: AgentConversationMode): boolean {
  return binding.stream === true || resolveEffectiveConversationMode({ binding, fallbackMode }) === 'wave';
}

export function baseStreamKeyOf(streamKey: string): string | undefined {
  return /^(.+):([1-9]\d*)$/.exec(streamKey)?.[1];
}

/** An explicit compound kind owns its stream. A payload stream qualifies a top-level kind. */
export function resolveTopicAddress(kind: string, payloadStreamKey?: string) {
  const separator = kind.indexOf(':');
  const topic = separator < 0 ? kind : kind.slice(0, separator);
  const streamKey = separator < 0 ? payloadStreamKey?.trim() || undefined : kind.slice(separator + 1);
  return {
    topic,
    kind: separator < 0 && streamKey !== undefined ? `${topic}:${streamKey}` : kind,
    streamKey,
  };
}

export function resolveBoundMailboxId(kind: string, binding: MailboxTopicBinding, fallbackMode?: AgentConversationMode): string {
  if (binding.mailboxId !== undefined) return normalizeMailboxId(binding.mailboxId);
  if (!isStreamConversationBinding(binding, fallbackMode)) return 'default';
  const { streamKey: kindStreamKey } = resolveTopicAddress(kind);
  if (kindStreamKey !== undefined && binding.streamKey !== undefined && baseStreamKeyOf(kindStreamKey) === binding.streamKey) {
    return normalizeMailboxId(`stream:${kindStreamKey}`);
  }
  return normalizeMailboxId(`stream:${binding.streamKey ?? kindStreamKey ?? kind}`);
}

/** A concrete semantic target wins over thread affinity. The default mailbox confers neither. */
export function resolveSessionOrThreadMailbox(
  envelope: { semantic?: { targetMailbox?: string } },
  threadLocalMailbox?: string,
): string | undefined {
  const target = envelope.semantic?.targetMailbox;
  if (target !== undefined && target !== 'default') {
    return normalizeMailboxId(target, 'semantic targetMailbox');
  }
  if (threadLocalMailbox !== undefined && threadLocalMailbox !== 'default') {
    return normalizeMailboxId(threadLocalMailbox, 'thread local mailbox');
  }
  return undefined;
}

export function resolveSpawnMailboxId(messageId: string): string {
  return normalizeMailboxId(`task:${messageId}`, 'task mailbox');
}

export function envelopeThreadId(envelope: MessageEnvelope): string {
  return envelope.semantic?.threadId ?? envelope.id;
}

export function hasExplicitExecutor({ payload }: MessageEnvelope): boolean {
  return typeof payload?.executor === 'string' && payload.executor.trim().length > 0;
}

export function canInheritThreadMailbox(envelope: MessageEnvelope): boolean {
  const target = envelope.semantic?.targetMailbox;
  return !hasExplicitExecutor(envelope) && (target === undefined || target === 'default');
}

const networkMessageKey = (networkId: string, messageId: string) => JSON.stringify([networkId, messageId]);

/** Associate unforced, untargeted follow-ups before selecting a delivery mechanism. */
export function groupThreadBatch(batch: readonly MessageEnvelope[], store?: MailboxStore): MessageEnvelope[][] {
  const keyOf = (envelope: MessageEnvelope) => networkMessageKey(envelope.networkId, envelopeThreadId(envelope));
  const roots = new Map<string, MessageEnvelope>();
  for (const envelope of batch) {
    const key = keyOf(envelope);
    if (envelope.id === envelopeThreadId(envelope) || (!roots.has(key) && canInheritThreadMailbox(envelope))) {
      roots.set(key, envelope);
    }
  }
  const groups = new Map<MessageEnvelope, MessageEnvelope[]>();
  for (const envelope of batch) {
    let root = canInheritThreadMailbox(envelope) ? roots.get(keyOf(envelope))! : envelope;
    if (root !== envelope && !canInheritThreadMailbox(root) && store?.resolveThreadLocalMailbox(
      envelopeThreadId(envelope), envelope.networkId, { excludeInboundMessageId: envelope.id },
    )) root = envelope;
    const group = groups.get(root) ?? [root];
    if (envelope !== root) group.push(envelope);
    groups.set(root, group);
  }
  return [...groups.values()];
}

export function resolveMailboxId(
  envelope: MessageEnvelope,
  binding?: MailboxTopicBinding,
  threadLocalMailbox?: string,
  fallbackMode?: AgentConversationMode,
): string {
  const sessionMailbox = resolveSessionOrThreadMailbox(envelope, threadLocalMailbox);
  if (sessionMailbox !== undefined) return sessionMailbox;
  const semantic = envelope.semantic;
  const payload = isRecord(envelope.payload)
    ? (envelope.payload as Record<string, unknown>)
    : undefined;
  const { kind: bindingKind, topic, streamKey } = resolveTopicAddress(
    envelope.kind,
    typeof payload?.streamKey === 'string' ? payload.streamKey : undefined,
  );
  const control = ['question', 'reply', 'settle'].includes(semantic?.type ?? '');
  // Control frames follow the topic binding only when the sender named no destination.
  if (binding && control && (semantic?.targetMailbox === undefined || semantic.targetMailbox === 'default')) {
    const bound = resolveBoundMailboxId(bindingKind, binding, fallbackMode);
    if (bound !== 'default') {
      return bound;
    }
  }
  if (semantic?.targetMailbox !== undefined) {
    return normalizeMailboxId(semantic.targetMailbox, 'semantic targetMailbox');
  }
  // An explicit payload mailbox names a concrete destination and outranks the generic topic
  // binding, matching the resolution order that predates thread affinity.
  const explicit = payload?.mailbox;
  if (isNonEmptyString(explicit)) {
    return normalizeMailboxId(explicit, 'payload mailbox');
  }
  const mode = resolveEffectiveConversationMode({ binding, payload, fallbackMode });
  if (binding && !control && !isNonEmptyString(payload?.executor)) {
    if (isStreamConversationBinding(binding, fallbackMode) && topic === envelope.kind && streamKey !== undefined && binding.mailboxId === undefined) {
      return normalizeMailboxId(`stream:${streamKey}`);
    }
    if (binding.mailboxId !== undefined || isStreamConversationBinding(binding, fallbackMode) || mode !== 'spawn') {
      return resolveBoundMailboxId(bindingKind, binding, fallbackMode);
    }
  }
  if (mode === 'wave' || binding?.stream === true) {
    return normalizeMailboxId(`stream:${streamKey ?? binding?.streamKey ?? envelope.kind}`, 'stream mailbox');
  }
  if (mode === 'spawn') {
    return resolveSpawnMailboxId(envelope.id);
  }
  return 'default';
}

function sanitizeWakeupHint(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }
  const hint = value.replace(/\s+/g, ' ').trim().slice(0, MAX_WAKEUP_HINT_LENGTH).trimEnd();
  return hint || undefined;
}

function defaultWakeTarget(mailboxId: string): WakeTargetState {
  return {
    mailboxId,
    driver: 'passive',
    coalesceMs: 0,
    completedWaves: 0,
    pendingWake: false,
    earliestWakeAt: 0,
    attemptCount: 0,
  };
}

const WAKE_TARGET_KEYS = new Set([
  'bindingRevision', 'mailboxId', 'driver', 'driverConfig', 'wakeupPrefix', 'coalesceMs', 'rotateAfterWaves',
  'completedWaves', 'pendingWake', 'earliestWakeAt', 'attemptCount', 'lastWakeError', 'deliveryInDoubt', 'pendingDelivery',
]);

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return isRecord(value) && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
}

function isJsonValue(value: unknown): boolean {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return true;
  if (typeof value === 'number') return Number.isFinite(value);
  if (Array.isArray(value)) return Array.from(value).every(isJsonValue);
  return isPlainRecord(value) && Object.values(value).every(isJsonValue);
}

function mergeWakeTarget(current: WakeTargetState, patch?: Partial<WakeTargetState>): WakeTargetState {
  if (!patch) return current;
  if (Object.keys(patch).some(key => !WAKE_TARGET_KEYS.has(key))) throw invalidStore();
  const next = { ...current, ...patch, mailboxId: current.mailboxId };
  if (!next.deliveryInDoubt) delete next.pendingDelivery;
  // A driver change cannot inherit another driver's opaque configuration.
  const sameDriver = patch.driver === undefined || patch.driver === current.driver;
  if (Object.hasOwn(patch, 'driverConfig')) {
    if (patch.driverConfig === undefined) delete next.driverConfig;
    else {
      if (!isPlainRecord(patch.driverConfig)) throw invalidStore();
      const config = { ...(sameDriver ? current.driverConfig : {}), ...patch.driverConfig };
      for (const key of Object.keys(config)) if (config[key] === undefined) delete config[key];
      next.driverConfig = structuredClone(config);
    }
  } else if (!sameDriver) delete next.driverConfig;
  for (const key of ['driver', 'coalesceMs', 'completedWaves', 'pendingWake', 'earliestWakeAt', 'attemptCount'] as const) {
    if (patch[key] === undefined) Object.assign(next, { [key]: current[key] });
  }
  if (patch.earliestWakeAt === undefined && patch.coalesceMs !== undefined &&
      next.attemptCount === 0 && current.earliestWakeAt > 0) {
    next.earliestWakeAt = next.coalesceMs === 0 ? 0 : Math.min(current.earliestWakeAt, Date.now() + next.coalesceMs);
  }
  const changed = current.driver !== next.driver || current.rotateAfterWaves !== next.rotateAfterWaves ||
    canonicalJson(current.driverConfig ?? {}) !== canonicalJson(next.driverConfig ?? {});
  return { ...next, bindingRevision: (current.bindingRevision ?? 0) + (changed ? 1 : 0) };
}

function validateWakeTarget(value: unknown): WakeTargetState {
  if (!isRecord(value) || Object.keys(value).some(key => !WAKE_TARGET_KEYS.has(key))) throw invalidStore();
  const target = value as unknown as WakeTargetState;
  if (target.bindingRevision !== undefined && !isNonNegativeInteger(target.bindingRevision)) throw invalidStore();
  if (!isValidMailboxId(target.mailboxId) || typeof target.driver !== 'string' ||
      !WAKE_DRIVER_ID_PATTERN.test(target.driver)) throw invalidStore();
  if (target.driverConfig !== undefined && (!isPlainRecord(target.driverConfig) || !isJsonValue(target.driverConfig))) throw invalidStore();
  if (target.deliveryInDoubt !== undefined && typeof target.deliveryInDoubt !== 'boolean') throw invalidStore();
  const pending = target.pendingDelivery;
  if (pending !== undefined && (!target.deliveryInDoubt || !isPlainRecord(pending) ||
      Object.keys(pending).some(key => !['bindingRevision', 'originRevision', 'deliveredSequence', 'queuedItemIds', 'pinnedSession'].includes(key)) ||
      !isNonNegativeInteger(pending.bindingRevision) || !Number.isSafeInteger(pending.deliveredSequence) ||
      (pending.originRevision !== undefined &&
        (!isNonNegativeInteger(pending.originRevision) || pending.originRevision > pending.bindingRevision)) ||
      pending.deliveredSequence < -1 || !Array.isArray(pending.queuedItemIds) ||
      !Array.from(pending.queuedItemIds).every(isNonEmptyString) ||
      (pending.pinnedSession !== undefined && !validPinnedSession(pending.pinnedSession, target.mailboxId)))) throw invalidStore();
  if (typeof target.coalesceMs !== 'number' || !Number.isFinite(target.coalesceMs) || target.coalesceMs < 0) throw invalidStore();
  if (!isNonNegativeInteger(target.completedWaves) || !isNonNegativeInteger(target.attemptCount)) throw invalidStore();
  if (typeof target.pendingWake !== 'boolean') throw invalidStore();
  if (typeof target.earliestWakeAt !== 'number' || !Number.isFinite(target.earliestWakeAt)) throw invalidStore();
  if (target.rotateAfterWaves !== undefined && (!isNonNegativeInteger(target.rotateAfterWaves) || target.rotateAfterWaves === 0)) throw invalidStore();
  for (const field of ['wakeupPrefix', 'lastWakeError'] as const) {
    if (target[field] !== undefined && typeof target[field] !== 'string') throw invalidStore();
  }
  return target;
}

export interface SessionMailboxPatch {
  driver?: string;
  driverConfig?: Record<string, unknown>;
  /** Exclude source-only values while preserving the destination's own configuration. */
  omitSourceDriverConfig?: readonly string[];
  /** Keep key deletions explicit when the patch is persisted as JSON before delivery. */
  unsetDriverConfig?: readonly string[];
}

function validPinnedSession(session: PinnedSessionMailbox, mailboxId: string): boolean {
  return isPlainRecord(session) && isValidMailboxId(session.sessionMailboxId) && session.sessionMailboxId !== mailboxId &&
    Object.keys(session).every(key => ['sessionMailboxId', 'driver', 'driverConfig', 'omitSourceDriverConfig', 'unsetDriverConfig'].includes(key)) &&
    (session.driver === undefined || (typeof session.driver === 'string' && WAKE_DRIVER_ID_PATTERN.test(session.driver))) &&
    (session.driverConfig === undefined || (isPlainRecord(session.driverConfig) && isJsonValue(session.driverConfig))) &&
    [session.omitSourceDriverConfig, session.unsetDriverConfig].every(keys =>
      keys === undefined || (Array.isArray(keys) && Array.from(keys).every(isNonEmptyString)));
}

function pinSessionTarget(db: MailboxSqliteBackend, id: string, sessionMailboxId: string, patch: SessionMailboxPatch = {}): string {
  const current = db.wakeTarget(id);
  if (current?.rotateAfterWaves === undefined) return id;
  const sessionMailbox = normalizeMailboxId(sessionMailboxId);
  if (sessionMailbox === id) throw new Error('Session mailbox must differ from rotating mailbox');
  const existing = db.wakeTarget(sessionMailbox);
  const nextDriver = patch.driver ?? existing?.driver ?? current.driver;
  const inherited = nextDriver === current.driver ? { ...current.driverConfig } : {};
  for (const key of patch.omitSourceDriverConfig ?? []) delete inherited[key];
  const hasQueued = db.hasQueuedItems(sessionMailbox);
  const prefix = current.wakeupPrefix?.replace(new RegExp(`(--mailbox\\s+)${id.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?=\\s|$)`), `$1${sessionMailbox}`);
  const next = validateWakeTarget(mergeWakeTarget(existing ?? defaultWakeTarget(sessionMailbox), {
    ...current, ...(existing ?? {}), mailboxId: sessionMailbox,
    driver: nextDriver,
    driverConfig: {
      ...inherited,
      ...(existing?.driver === nextDriver ? existing.driverConfig : undefined),
      ...patch.driverConfig,
      ...Object.fromEntries((patch.unsetDriverConfig ?? []).map(key => [key, undefined])),
    },
    rotateAfterWaves: undefined, completedWaves: existing?.completedWaves ?? 0,
    wakeupPrefix: prefix ?? existing?.wakeupPrefix ?? current.wakeupPrefix,
    pendingWake: Boolean(existing?.pendingWake || hasQueued), attemptCount: existing?.attemptCount ?? 0,
    earliestWakeAt: existing?.earliestWakeAt ?? 0,
    lastWakeError: existing?.lastWakeError,
    deliveryInDoubt: existing?.deliveryInDoubt ? true : undefined,
    pendingDelivery: existing?.deliveryInDoubt ? existing.pendingDelivery : undefined,
  }));
  db.writeWakeTarget(next, existing);
  return sessionMailbox;
}

const THREAD_STATES: readonly ThreadRouteRecord['state'][] = ['open', 'replied', 'settled'];
const SETTLEMENT_OUTCOMES: readonly SettlementOutcome[] = ['succeeded', 'failed', 'cancelled'];

function validateThreadRecord(value: unknown): ThreadRouteRecord {
  if (!isRecord(value)) {
    throw invalidStore();
  }
  const record = value as unknown as ThreadRouteRecord;
  if (
    !isNonEmptyString(record.threadId) ||
    !isNonEmptyString(record.messageId) ||
    !['inbound', 'outbound'].includes(record.direction) ||
    typeof record.peerNodeId !== 'string' ||
    !Array.isArray(record.peerNodeIds) ||
    record.peerNodeIds.some(peer => typeof peer !== 'string') ||
    typeof record.networkId !== 'string' ||
    !isValidMailboxId(record.peerReplyMailbox) ||
    !isValidMailboxId(record.localMailbox) ||
    (record.mailboxCommitted !== undefined && typeof record.mailboxCommitted !== 'boolean') ||
    typeof record.traceId !== 'string' ||
    typeof record.kind !== 'string' ||
    typeof record.subject !== 'string' ||
    !THREAD_STATES.includes(record.state) ||
    (record.createdAt !== undefined && typeof record.createdAt !== 'string') ||
    typeof record.updatedAt !== 'string'
  ) {
    throw invalidStore();
  }
  if (record.settlement !== undefined) {
    const settlement = record.settlement;
    if (
      !isRecord(settlement) ||
      !SETTLEMENT_OUTCOMES.includes(settlement.outcome) ||
      typeof settlement.summary !== 'string' ||
      typeof settlement.settledAt !== 'string'
    ) {
      throw invalidStore();
    }
  }
  if (record.envelope !== undefined) validateStoredEnvelope(record.envelope);
  return {
    ...record,
    createdAt: record.createdAt ?? record.updatedAt,
  };
}

function threadKey(record: Pick<ThreadRouteRecord, 'direction' | 'messageId'>): string {
  return `${record.direction}:${record.messageId}`;
}

function outboundPeerMailbox(envelope: MessageEnvelope): string {
  return normalizeMailboxId(envelope.semantic?.targetMailbox ?? 'default', 'peer reply mailbox');
}

function recordedPeerMailbox(record: ThreadRouteRecord): string {
  return record.direction === 'outbound' && record.envelope
    ? outboundPeerMailbox(record.envelope) : record.peerReplyMailbox;
}

function recordMatchesPeer(record: ThreadRouteRecord, peerNodeId: string): boolean {
  return record.direction === 'inbound'
    ? record.peerNodeId === peerNodeId
    : record.peerNodeIds.length === 1 && record.peerNodeIds[0] === peerNodeId;
}

const THREAD_STATE_RANK: Readonly<Record<ThreadRouteRecord['state'], number>> = {
  open: 0,
  replied: 1,
  settled: 2,
};

function mergeThreadState(
  current: ThreadRouteRecord['state'],
  next: ThreadRouteRecord['state'],
): ThreadRouteRecord['state'] {
  return THREAD_STATE_RANK[next] > THREAD_STATE_RANK[current] ? next : current;
}

function compareThreadRecords(a: ThreadRouteRecord, b: ThreadRouteRecord): number {
  return a.createdAt.localeCompare(b.createdAt) || a.messageId.localeCompare(b.messageId);
}

function validateStoredEnvelope(value: unknown): MessageEnvelope {
  if (!isRecord(value)) {
    throw invalidStore();
  }
  const envelope = value as unknown as MessageEnvelope;
  if (
    !isNonEmptyString(envelope.id) ||
    typeof envelope.networkId !== 'string' ||
    typeof envelope.traceId !== 'string' ||
    !isRecord(envelope.from) ||
    !isNonEmptyString(envelope.from.nodeId) ||
    typeof envelope.from.member !== 'string' ||
    typeof envelope.from.nodeName !== 'string' ||
    !isRecord(envelope.routing) ||
    !['unicast', 'multicast', 'broadcast', 'auto'].includes(envelope.routing.mode) ||
    typeof envelope.kind !== 'string' ||
    typeof envelope.subject !== 'string' ||
    typeof envelope.body !== 'string' ||
    !['urgent', 'high', 'normal', 'low'].includes(envelope.priority) ||
    !isRecord(envelope.payload) ||
    !Array.isArray(envelope.attachments) ||
    envelope.attachments.some(
      attachment =>
        !isRecord(attachment) ||
        typeof attachment.name !== 'string' ||
        typeof attachment.mimeType !== 'string' ||
        typeof attachment.sizeBytes !== 'number' ||
        typeof attachment.blake3Hash !== 'string' ||
        typeof attachment.ticket !== 'string',
    ) ||
    typeof envelope.createdAt !== 'string' ||
    typeof envelope.signature !== 'string'
  ) {
    throw invalidStore();
  }
  if (envelope.semantic !== undefined) {
    try {
      validateSemanticHeader(envelope.semantic);
    } catch {
      throw invalidStore();
    }
  }
  return envelope;
}

function validateItem(value: unknown): MailboxItem {
  if (!isRecord(value)) {
    throw invalidStore();
  }
  const item = value as unknown as MailboxItem;
  if (
    !isNonEmptyString(item.id) ||
    !isValidMailboxId(item.mailboxId) ||
    !['queued', 'leased', 'acked'].includes(item.status) ||
    typeof item.receivedAt !== 'string' ||
    !isNonNegativeInteger(item.sequence) ||
    (item.leaseId !== undefined && !LEASE_ID_PATTERN.test(item.leaseId)) ||
    (item.leasedAt !== undefined && typeof item.leasedAt !== 'string') ||
    (item.ackedAt !== undefined && typeof item.ackedAt !== 'string') ||
    (item.wakeupHint !== undefined && typeof item.wakeupHint !== 'string')
  ) {
    throw invalidStore();
  }
  const envelope = validateStoredEnvelope(item.envelope);
  if (envelope.id !== item.id) {
    throw invalidStore();
  }
  if (item.signedEnvelope !== undefined && validateStoredEnvelope(item.signedEnvelope).id !== item.id) {
    throw invalidStore();
  }
  return item;
}

function validateLease(value: unknown): DeliveryLease {
  if (!isRecord(value)) {
    throw invalidStore();
  }
  const lease = value as unknown as DeliveryLease;
  if (
    typeof lease.leaseId !== 'string' ||
    !LEASE_ID_PATTERN.test(lease.leaseId) ||
    !isValidMailboxId(lease.mailboxId) ||
    !Array.isArray(lease.itemIds) ||
    lease.itemIds.some(id => !isNonEmptyString(id)) ||
    typeof lease.createdAt !== 'string' ||
    !isNonNegativeInteger(lease.wakeCount)
  ) {
    throw invalidStore();
  }
  return lease;
}

function validateMailboxRecord(key: string, value: unknown): MailboxStateRecord {
  if (!isRecord(value)) {
    throw invalidStore();
  }
  const record = value as unknown as MailboxStateRecord;
  if (
    record.mailboxId !== key ||
    !isValidMailboxId(record.mailboxId) ||
    !Array.isArray(record.ackedLeaseIds) ||
    record.ackedLeaseIds.some(id => typeof id !== 'string')
  ) {
    throw invalidStore();
  }
  if (record.outstanding !== undefined) {
    const lease = validateLease(record.outstanding);
    if (lease.mailboxId !== record.mailboxId) {
      throw invalidStore();
    }
  }
  return record;
}

function readLegacyFile(path: string): string | undefined {
  try {
    return readFileSync(path, 'utf8');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    return undefined;
  }
}

function mailboxPaths(mailboxPath: string, legacyInboxPath?: string) {
  const input = resolve(mailboxPath);
  const extension = extname(input);
  const base = input.slice(0, input.length - extension.length);
  const sqlite = extension === '.json' ? `${base}.sqlite` : input;
  const legacyMailbox = `${base}.json`;
  const inbox = legacyInboxPath === undefined ? undefined : resolve(legacyInboxPath);
  return { sqlite, legacyMailbox, legacyInbox: inbox === sqlite ? undefined : inbox };
}

function pathExists(path: string): boolean {
  return lstatSync(path, { throwIfNoEntry: false }) !== undefined;
}

function hasLegacyMailbox(paths: ReturnType<typeof mailboxPaths>): boolean {
  return pathExists(paths.legacyMailbox) || (paths.legacyInbox !== undefined && pathExists(paths.legacyInbox));
}

export class MailboxMigrationRequiredError extends Error {
  constructor() {
    super('Mailbox migration required. Start the daemon with dealer node up or run a writable inbox command.');
  }
}

function loadLegacyMailbox(raw: string): LegacyMailboxState {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw invalidStore();
  }
  if (!isRecord(parsed) || (parsed.schema !== MAILBOX_SCHEMA && parsed.schema !== LEGACY_MAILBOX_V1_SCHEMA)) {
    throw invalidStore();
  }
  if (!isNonNegativeInteger(parsed.sequence) || !Array.isArray(parsed.items)) {
    throw invalidStore();
  }
  const items = parsed.items.map(validateItem);
  const mailboxes = parsed.mailboxes ?? {};
  const wakeTargets = parsed.wakeTargets ?? {};
  const threads = parsed.threads ?? {};
  if (!isRecord(mailboxes) || !isRecord(wakeTargets) || !isRecord(threads)) {
    throw invalidStore();
  }
  for (const [key, value] of Object.entries(mailboxes)) {
    validateMailboxRecord(key, value);
  }
  for (const [key, value] of Object.entries(wakeTargets)) {
    if (parsed.schema === LEGACY_MAILBOX_V1_SCHEMA) {
      if (!isRecord(value) || typeof value.mode !== 'string') throw invalidStore();
      const core: Record<string, unknown> = {};
      const driverConfig: Record<string, unknown> = {};
      for (const [field, entry] of Object.entries(value)) {
        if (field === 'mode' || entry === undefined) continue;
        if (WAKE_TARGET_KEYS.has(field) && field !== 'driver' && field !== 'driverConfig') core[field] = entry;
        else driverConfig[field] = entry;
      }
      const configured = Object.keys(driverConfig).length > 0;
      const driver = value.mode === 'passive' && isNonEmptyString(driverConfig.hostAgent) ? 'agent-host' : value.mode;
      wakeTargets[key] = { ...core, driver, ...(configured ? { driverConfig } : {}) };
    }
    if (validateWakeTarget(wakeTargets[key]).mailboxId !== key) {
      throw invalidStore();
    }
  }
  const validatedWakeTargets = wakeTargets as Record<string, WakeTargetState>;
  const normalizedThreads: Record<string, ThreadRouteRecord> = {};
  let deliveredMailboxes: Map<string, string> | undefined;
  for (const [key, value] of Object.entries(threads)) {
    const record = validateThreadRecord(value);
    const expected = threadKey(record);
    // Records are always stored under directional keys; any other key is a corrupted store.
    if (key !== expected) {
      throw invalidStore();
    }
    if (record.direction === 'inbound' && record.mailboxCommitted === undefined) {
      if (!deliveredMailboxes) {
        deliveredMailboxes = new Map();
        for (const item of items) {
          const itemKey = networkMessageKey(item.envelope.networkId, item.id);
          if (!deliveredMailboxes.has(itemKey)) deliveredMailboxes.set(itemKey, item.mailboxId);
        }
      }
      const delivered = deliveredMailboxes.get(networkMessageKey(record.networkId, record.messageId));
      const deliveryTarget = delivered === undefined ? undefined : validatedWakeTargets[delivered];
      const pinnedTarget = Object.hasOwn(wakeTargets, record.localMailbox)
        ? validatedWakeTargets[record.localMailbox] : undefined;
      // Legacy session pinning kept the delivered item in the rotating mailbox.
      const preservesLegacyPin = record.localMailbox !== delivered && deliveryTarget?.rotateAfterWaves !== undefined &&
        pinnedTarget !== undefined && pinnedTarget.rotateAfterWaves === undefined &&
        pinnedTarget.driver !== 'passive' && pinnedTarget.driver === deliveryTarget.driver;
      normalizedThreads[expected] = {
        ...record,
        mailboxCommitted: delivered !== undefined,
        localMailbox: preservesLegacyPin ? record.localMailbox : delivered ?? record.localMailbox,
      };
    } else {
      normalizedThreads[expected] = record;
    }
  }
  const sequence = items.reduce((next, item) => Math.max(next, item.sequence + 1), parsed.sequence);
  if (!isNonNegativeInteger(sequence)) throw invalidStore();
  return {
    sequence,
    items,
    mailboxes: mailboxes as Record<string, MailboxStateRecord>,
    wakeTargets: validatedWakeTargets,
    threads: normalizedThreads,
  };
}

function loadLegacyInbox(raw: string): LegacyInboxItem[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error('Invalid inbox store');
  }
  if (!isRecord(parsed) || parsed.schema !== LEGACY_INBOX_SCHEMA || !Array.isArray(parsed.items)) {
    throw new Error('Invalid inbox store');
  }
  const items = parsed.items;
  const valid = items.every(
    item =>
      isRecord(item) &&
      ['id', 'networkId', 'topic', 'subject', 'body', 'createdAt', 'receivedAt'].every(key =>
        typeof item[key] === 'string',
      ) &&
      isRecord(item.from) &&
      ['nodeId', 'member', 'nodeName'].every(key => typeof item.from[key] === 'string') &&
      ['urgent', 'high', 'normal', 'low'].includes(item.priority as string) &&
      ['unread', 'read'].includes(item.status as string) &&
      (item.readAt === undefined || typeof item.readAt === 'string') &&
      isRecord(item.payload) &&
      Array.isArray(item.attachments) &&
      item.attachments.every(
        attachment =>
          isRecord(attachment) &&
          typeof attachment.name === 'string' &&
          typeof attachment.mimeType === 'string' &&
          typeof attachment.sizeBytes === 'number' &&
          typeof attachment.blake3Hash === 'string' &&
          typeof attachment.localPath === 'string',
      ),
  );
  if (!valid) {
    throw new Error('Invalid inbox store');
  }
  return items as unknown as LegacyInboxItem[];
}

function migrateLegacyItem(item: LegacyInboxItem, sequence: number): MailboxItem {
  const localAttachments = item.attachments.map(attachment => attachment.localPath);
  const payload: Record<string, unknown> = { ...item.payload };
  if (localAttachments.some(path => path)) {
    payload.localAttachments = localAttachments;
  }
  const hint = sanitizeWakeupHint(item.subject);
  const ackedAt =
    item.status === 'read' ? (item.readAt ?? item.receivedAt) : undefined;
  return {
    id: item.id,
    mailboxId: 'default',
    status: item.status === 'unread' ? 'queued' : 'acked',
    receivedAt: item.receivedAt,
    ...(ackedAt !== undefined ? { ackedAt } : {}),
    ...(hint ? { wakeupHint: hint } : {}),
    sequence,
    envelope: {
      id: item.id,
      networkId: item.networkId,
      traceId: item.id,
      from: { ...item.from },
      routing: { mode: 'broadcast' },
      kind: item.topic,
      priority: item.priority,
      subject: item.subject,
      body: item.body,
      payload,
      attachments: item.attachments.map(attachment => ({
        name: attachment.name,
        mimeType: attachment.mimeType,
        sizeBytes: attachment.sizeBytes,
        blake3Hash: attachment.blake3Hash,
        ticket: '',
      })),
      createdAt: item.createdAt,
      signature: '',
    },
  };
}

function isOneWayWaveEnvelope(envelope: MessageEnvelope | undefined): boolean {
  return envelope !== undefined && isOneWayWave(envelope.kind, envelope.replyToId, envelope.semantic?.type);
}

function isOneWayWave(kind: string, replyToId?: string, semanticType?: string): boolean {
  return kind.startsWith('wave.') && replyToId === undefined && (semanticType === undefined || semanticType === 'message');
}

function activeMailboxKeys(db: MailboxSqliteBackend): { messages: ReadonlySet<string>; threads: ReadonlySet<string> } {
  const messages = new Set<string>();
  const threads = new Set<string>();
  for (const item of db.items({ unacked: true })) {
    const envelope = item.signedEnvelope ?? item.envelope;
    threads.add(netKey(envelope.networkId, envelopeThreadId(envelope)));
    for (const id of [item.id, envelope.replyToId]) {
      if (id === undefined) continue;
      messages.add(netKey(envelope.networkId, id));
      for (const direction of ['inbound', 'outbound']) {
        const record = db.thread(`${direction}:${id}`);
        if (record?.networkId === envelope.networkId) threads.add(netKey(record.networkId, record.threadId));
      }
    }
  }
  return { messages, threads };
}

function isAnsweredThread(records: ThreadRetentionRow[], confirmedReplies?: ReadonlySet<string>): boolean {
  const delivered = (record: ThreadRetentionRow): boolean =>
    record.direction === 'inbound'
      ? record.mailboxCommitted
      : confirmedReplies?.has(netKey(record.networkId, record.messageId)) === true ||
        records.some(reply => reply.direction === 'inbound' && reply.semanticType === 'reply' &&
          reply.mailboxCommitted && reply.messageId === record.messageId && reply.replyToId === record.replyToId);
  return records.every(record => {
    if (record.semanticType === 'reply') return delivered(record);
    if (record.direction === 'inbound' && record.semanticType !== 'question') return true;
    // A thread-level replied state can come from a different leg. Require an addressed
    // answer for each leg that could still own an outstanding reply.
    return records.some(reply => reply.direction !== record.direction && reply.semanticType === 'reply' &&
      delivered(reply) && reply.replyToId === record.messageId &&
      (record.direction === 'inbound' || reply.localMailbox === (record.replyMailbox ?? record.localMailbox)));
  });
}

function compactThreads(db: MailboxSqliteBackend, outbox?: OutboxStore): void {
  const records = db.threadRetentionRows(MAX_RETAINED_WAVE_THREADS);
  if (records.length === 0) return;
  const groups = new Map<string, ThreadRetentionRow[]>();
  for (const record of records) {
    const key = netKey(record.networkId, record.threadId);
    const group = groups.get(key);
    if (group) group.push(record);
    else groups.set(key, [record]);
  }
  const wave: { key: string; updatedAt: string; records: ThreadRetentionRow[] }[] = [];
  const settled: typeof wave = [];
  const replied: typeof wave = [];
  for (const [key, records] of groups) {
    const updatedAt = records.reduce((latest, record) => record.updatedAt > latest ? record.updatedAt : latest, '');
    const group = { key, updatedAt, records };
    if (records.every(record => record.state === 'settled')) {
      settled.push(group);
    } else if (records.every(record => record.hasEnvelope && record.state === 'replied')) {
      replied.push(group);
    } else if (records.every(record =>
      (outbox !== undefined || record.direction === 'inbound') && record.state === 'open' &&
      record.hasEnvelope && isOneWayWave(record.kind, record.replyToId, record.semanticType))) {
      wave.push(group);
    }
  }
  const histories = [[wave, MAX_RETAINED_WAVE_THREADS], [settled, MAX_RETAINED_SETTLED_THREADS],
    [replied, MAX_RETAINED_REPLIED_THREADS]] as const;
  if (histories.every(([history, limit]) => history.length <= limit)) return;
  const activeKeys = activeMailboxKeys(db);
  const owners = outbox && readOutboxThreadOwners(outbox, activeKeys);
  // Outbox routes that can still be restored keep the corresponding thread alive.
  const outboxThreads = new Set<string>();
  for (const item of owners?.items ?? []) {
    if (owners?.confirmedReplies.has(netKey(item.networkId, item.id))) continue;
    outboxThreads.add(netKey(item.networkId, envelopeThreadId(item.envelope)));
  }
  for (const [history, limit] of histories) {
    if (history.length <= limit) continue;
    const eligible = history.filter(group => !activeKeys.threads.has(group.key) && !outboxThreads.has(group.key) &&
      (history !== replied || isAnsweredThread(group.records, owners?.confirmedReplies)));
    eligible.sort((a, b) => b.updatedAt.localeCompare(a.updatedAt) || a.key.localeCompare(b.key));
    for (const group of eligible.slice(limit)) {
      for (const record of group.records) db.deleteThread(record.key);
    }
  }
}

function receivedInstant(item: MailboxItem): number {
  const parsed = Date.parse(item.receivedAt);
  return Number.isFinite(parsed) ? parsed : 0;
}

function compareOldestFirst(a: MailboxItem, b: MailboxItem): number {
  return receivedInstant(a) - receivedInstant(b) || a.sequence - b.sequence;
}

function compareNewestFirst(a: MailboxItem, b: MailboxItem): number {
  return receivedInstant(b) - receivedInstant(a) || b.sequence - a.sequence;
}

function exactThreadRecord(
  db: MailboxSqliteBackend,
  messageId: string,
): ThreadRouteRecord | undefined {
  return db.thread(`inbound:${messageId}`) ?? db.thread(`outbound:${messageId}`);
}

function threadRecordsFor(
  db: MailboxSqliteBackend,
  threadId: string,
): ThreadRouteRecord[] {
  return db.threads({ threadId }).sort(compareThreadRecords);
}

function lookupThread(
  db: MailboxSqliteBackend,
  messageOrThreadId: string,
): ThreadRouteRecord | undefined {
  // A message id can exist on both loopback legs. Prefer the inbound leg because ask/reply/
  // settle address a message the caller received; fall back to its outbound leg otherwise.
  const exact = exactThreadRecord(db, messageOrThreadId);
  if (exact) {
    return exact;
  }
  return threadRecordsFor(db, messageOrThreadId).at(-1);
}

function settlementOf(
  envelope: MessageEnvelope,
  now: string,
): ThreadRouteRecord['settlement'] | undefined {
  if (envelope.semantic?.type !== 'settle') {
    return undefined;
  }
  const payload = isRecord(envelope.payload) ? envelope.payload : {};
  return {
    outcome: envelope.semantic.outcome as SettlementOutcome,
    summary: typeof payload.summary === 'string' ? payload.summary : envelope.body,
    settledAt: now,
  };
}

function stateForSemantic(envelope: MessageEnvelope): ThreadRouteRecord['state'] {
  if (envelope.semantic?.type === 'settle') {
    return 'settled';
  }
  if (envelope.semantic?.type === 'reply') {
    return 'replied';
  }
  return 'open';
}

function outboundThreadRecord(
  envelope: MessageEnvelope, peers: readonly string[], localMailbox: string, createdAt: string, updatedAt: string,
): ThreadRouteRecord {
  const settlement = settlementOf(envelope, createdAt);
  return {
    threadId: envelope.semantic?.threadId ?? envelope.id,
    messageId: envelope.id,
    envelope,
    direction: 'outbound',
    peerNodeId: peers[0] ?? '',
    peerNodeIds: peers,
    networkId: envelope.networkId,
    peerReplyMailbox: outboundPeerMailbox(envelope),
    localMailbox,
    traceId: envelope.traceId,
    kind: envelope.kind,
    subject: envelope.subject,
    state: stateForSemantic(envelope),
    ...(settlement ? { settlement } : {}),
    createdAt,
    updatedAt,
  };
}

function outboxThreadRecord(item: OutboxItem): ThreadRouteRecord {
  return outboundThreadRecord(
    item.envelope, committedOutboxTargets(item), normalizeMailboxId(item.envelope.semantic?.replyMailbox ?? 'default'),
    item.createdAt, item.updatedAt,
  );
}

function readOutboxThreadOwners(outbox: OutboxStore, activeKeys: ReturnType<typeof activeMailboxKeys>) {
  const items = outbox.list({ status: 'all' });
  const definitivelyFailed = outbox.readDefinitivelyFailedKeys(items);
  const confirmedReplies = new Set<string>();
  return {
    definitivelyFailed,
    confirmedReplies,
    items: items.filter(item => {
      const messageKey = netKey(item.networkId, item.id);
      if (item.supersededById !== undefined || definitivelyFailed.has(messageKey)) return false;
      if (item.envelope.semantic?.type === 'reply' && item.status === 'delivered' && isTerminalHistory(item) &&
          Object.values(item.deliveredReceipts).some(receipt => receipt.stage !== 'queued')) {
        confirmedReplies.add(messageKey);
      }
      if (!isOneWayWaveEnvelope(item.envelope) || !isTerminalHistory(item)) return true;
      const threadKey = netKey(item.networkId, envelopeThreadId(item.envelope));
      return activeKeys.messages.has(messageKey) || activeKeys.threads.has(threadKey);
    }),
  };
}

function createAbortError(): Error {
  const error = new Error('Mailbox wait aborted');
  error.name = 'AbortError';
  return error;
}

function normalizeWaitTimeout(timeoutMs: number): number {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
    throw new Error('Timeout must be a positive integer');
  }
  return Math.min(timeoutMs, MAX_WAIT_TIMEOUT_MS);
}

/**
 * Single durable source of truth for logical mailboxes, single-outstanding delivery leases,
 * wake targets and thread reverse routes. Writers use SQLite row transactions and readers
 * use WAL snapshots. In-process waiters are woken by events.
 */
export class MailboxStore {
  readonly path: string;
  readonly lockDir: string;
  private readonly outbox?: OutboxStore;
  private readonly readConsumedSequence?: MailboxStoreOptions['readConsumedSequence'];
  private readonly events = new EventEmitter();
  private readonly db: MailboxSqliteBackend;

  /** Open a durable snapshot without creating state for an unused mailbox. */
  static openReadOnly(mailboxPath: string, legacyInboxPath?: string): MailboxStore | undefined {
    const paths = mailboxPaths(mailboxPath, legacyInboxPath);
    if (pathExists(paths.sqlite)) {
      return new MailboxStore(mailboxPath, { readOnly: true, legacyInboxPath });
    }
    if (hasLegacyMailbox(paths)) throw new MailboxMigrationRequiredError();
    return undefined;
  }

  constructor(mailboxPath: string, options: MailboxStoreOptions = {}) {
    const paths = mailboxPaths(mailboxPath, options.legacyInboxPath);
    this.path = paths.sqlite;
    this.lockDir = resolve(options.lockDir ?? `${paths.legacyMailbox}.lock`);
    this.outbox = options.outbox;
    this.readConsumedSequence = options.readConsumedSequence;
    this.events.setMaxListeners(0);
    this.db = new MailboxSqliteBackend(this.path, {
      item: validateItem, mailbox: validateMailboxRecord, wakeTarget: validateWakeTarget, thread: validateThreadRecord,
    }, options.readOnly);
    try {
      const state = this.db.initializationState(MAILBOX_SCHEMA);
      if (state !== 'ready') {
        if (options.readOnly) throw new MailboxMigrationRequiredError();
        this.initializeWritable(paths, state);
      }
      if (pathExists(paths.legacyMailbox)) {
        throw new Error(`Legacy mailbox ledger reappeared after SQLite cutover: ${paths.legacyMailbox}`);
      }
    } catch (error) {
      this.db.close();
      throw error;
    }
  }

  private initializeWritable(paths: ReturnType<typeof mailboxPaths>, state: MailboxInitialization): void {
    if (state === 'uninitialized' && !hasLegacyMailbox(paths)) {
      this.db.initialize(MAILBOX_SCHEMA);
      if (this.db.initializationState(MAILBOX_SCHEMA) === 'ready') return;
    }
    withOwnedLock(this.lockDir, 'Mailbox store', () => {
      this.db.initialize(MAILBOX_SCHEMA, () => this.importLegacy(paths.legacyMailbox, paths.legacyInbox));
      if (this.db.initializationState(MAILBOX_SCHEMA) === 'archive-pending') this.db.finishArchive();
    });
  }

  private importLegacy(mailboxPath: string, legacyInboxPath?: string): { source: string; backup: string } | undefined {
    const rawMailbox = readLegacyFile(mailboxPath);
    if (rawMailbox !== undefined) {
      const state = loadLegacyMailbox(rawMailbox);
      for (const item of state.items) this.db.insertItem(item);
      for (const record of Object.values(state.mailboxes)) this.db.writeMailbox(record);
      for (const target of Object.values(state.wakeTargets)) this.db.writeWakeTarget(target);
      for (const [key, record] of Object.entries(state.threads)) this.db.writeThread(key, record);
      this.db.setSequence(state.sequence);
      return { source: mailboxPath, backup: `${mailboxPath}.v2-migrated` };
    }
    const rawInbox = legacyInboxPath === undefined ? undefined : readLegacyFile(legacyInboxPath);
    if (rawInbox !== undefined && legacyInboxPath !== undefined) {
      const items = loadLegacyInbox(rawInbox);
      for (const [sequence, item] of items.entries()) this.db.insertItem(migrateLegacyItem(item, sequence));
      this.db.setSequence(items.length);
      return { source: legacyInboxPath, backup: `${legacyInboxPath}.migrated` };
    }
    return undefined;
  }

  close(): void {
    this.db.close();
    this.events.emit('close');
    this.events.removeAllListeners();
  }

  [Symbol.dispose](): void {
    this.close();
  }

  enqueue(
    batch: readonly MessageEnvelope[],
    options: MailboxEnqueueOptions = {},
  ): MailboxEnqueueResult {
    if (!Array.isArray(batch)) {
      throw new Error('Enqueue requires an envelope array');
    }
    const explicitMailbox =
      options.mailboxId === undefined ? undefined : normalizeMailboxId(options.mailboxId);
    if (options.proposedSpawn && explicitMailbox === undefined) throw new Error('A proposed spawn requires a mailbox id');
    const now = new Date().toISOString();
    const nowMs = Date.now();
    const insertedByMailbox = new Map<string, number>();
    const conversationalByMailbox = new Map<string, boolean>();
    const result = this.db.write(() => {
      const inserted: string[] = [];
      const duplicateIds: string[] = [];
      const mailboxIds = new Set<string>();
      const configureWakeTargets = new Set<string>();
      const envelopeMailboxes = new Map<string, string>();
      let claimedSpawn = false;
      let sequence: number | undefined;
      for (const envelope of batch) {
        if (!isRecord(envelope) || !isNonEmptyString(envelope.id)) {
          throw new Error('Invalid envelope: missing id');
        }
        const owner = canInheritThreadMailbox(envelope)
          ? this.threadLocalMailbox(envelopeThreadId(envelope), envelope.networkId, envelope.id)
          : undefined;
        const mailboxId = owner ?? explicitMailbox ?? resolveMailboxId(envelope);
        mailboxIds.add(mailboxId);
        envelopeMailboxes.set(envelope.id, mailboxId);
        const duplicate = this.db.item(mailboxId, envelope.id);
        const mayConfigureWakeTarget = options.proposedSpawn
          ? owner === undefined && !duplicate
          : owner === undefined || owner === explicitMailbox;
        if (mayConfigureWakeTarget) configureWakeTargets.add(mailboxId);
        if (duplicate) {
          const inbound = this.db.thread(threadKey({ direction: 'inbound', messageId: envelope.id }));
          if (!inbound || inbound.mailboxCommitted === false) {
            this.upsertInboundThread(duplicate.signedEnvelope ?? duplicate.envelope, mailboxId, now, true);
          }
          duplicateIds.push(envelope.id);
          continue;
        }
        const cloned = structuredClone(envelope);
        let signedEnvelope: MessageEnvelope | undefined;
        if (options.signedEnvelopes && Object.hasOwn(options.signedEnvelopes, cloned.id)) {
          const rawSigned = validateStoredEnvelope(options.signedEnvelopes[cloned.id]);
          if (rawSigned.id !== cloned.id) throw invalidStore();
          signedEnvelope = structuredClone(rawSigned);
        }
        const payload = isRecord(cloned.payload) ? cloned.payload : {};
        const hint = sanitizeWakeupHint(
          options.wakeupHint ?? payload.wakeupHint ?? cloned.subject,
        );
        sequence ??= this.db.sequence();
        this.db.insertItem(validateItem({
          id: cloned.id,
          mailboxId,
          status: 'queued',
          receivedAt: now,
          ...(hint ? { wakeupHint: hint } : {}),
          sequence,
          envelope: cloned,
          ...(signedEnvelope ? { signedEnvelope } : {}),
        }));
        sequence += 1;
        conversationalByMailbox.set(mailboxId,
          (conversationalByMailbox.get(mailboxId) ?? true) &&
          (['question', 'reply', 'settle'].includes(envelope.semantic?.type ?? '') ||
            Boolean(envelope.semantic?.threadId &&
              threadRecordsFor(this.db, envelope.semantic.threadId)
                .some(record => record.networkId === envelope.networkId))),
        );
        this.upsertInboundThread(signedEnvelope ?? cloned, mailboxId, now, true);
        claimedSpawn ||= options.proposedSpawn === true && owner === undefined;
        inserted.push(cloned.id);
        insertedByMailbox.set(mailboxId, (insertedByMailbox.get(mailboxId) ?? 0) + 1);
      }
      if (sequence !== undefined) this.db.setSequence(sequence);
      for (const mailboxId of mailboxIds) {
        const previous = this.db.wakeTarget(mailboxId);
        let target = previous;
        if (options.wakeTarget && configureWakeTargets.has(mailboxId)) {
          const wakePatch = typeof options.wakeTarget === 'function'
            ? options.wakeTarget(previous ? structuredClone(previous) : undefined)
            : options.wakeTarget;
          target = validateWakeTarget(
            mergeWakeTarget(previous ?? defaultWakeTarget(mailboxId), wakePatch),
          );
        }
        if (target && (insertedByMailbox.get(mailboxId) ?? 0) > 0) {
          target = validateWakeTarget({
            ...target,
            pendingWake: true,
            earliestWakeAt: conversationalByMailbox.get(mailboxId) && target.attemptCount === 0
              ? nowMs
              : target.attemptCount > 0
                ? Math.max(previous?.earliestWakeAt ?? 0, nowMs + target.coalesceMs)
                : nowMs + target.coalesceMs,
          });
        }
        if (target && target !== previous) this.db.writeWakeTarget(target, previous);
      }
      return {
        inserted, duplicateIds, mailboxIds: [...mailboxIds], envelopeMailboxes: Object.fromEntries(envelopeMailboxes),
        ...(options.proposedSpawn ? { claimedSpawn } : {}),
      };
    });
    for (const [mailboxId, count] of insertedByMailbox) {
      if (count > 0) {
        this.events.emit('mail', mailboxId);
      }
    }
    return result;
  }

  check(options: MailboxCheckOptions = {}): MailboxCheckResult {
    const mailboxId = normalizeMailboxId(options.mailboxId ?? 'default');
    if (options.ackLeaseId !== undefined) {
      this.ack(options.ackLeaseId, mailboxId);
    }
    if (options.peek) return this.db.read(() => this.checkMailbox(mailboxId, false));
    let wakeChanged = false;
    let pinnedWakeMailbox: string | undefined;
    const result = this.db.write(() => {
      const checked = this.checkMailbox(mailboxId, true);
      if (checked.deliveryId === null) return checked;
      const target = this.db.wakeTarget(mailboxId);
      if (!target || checked.redelivered) return checked;
      const pending = this.matchingPendingDelivery(target, checked.items.map(item => item.id));
      if (pending) {
        ({ pinnedWakeMailbox } = this.settleWakeDeliveryInTx(target, target, pending.deliveredSequence, {}, pending.pinnedSession));
        wakeChanged = true;
      } else if (!target.deliveryInDoubt && target.pendingWake) {
        this.db.writeWakeTarget({
          ...target, lastWakeError: undefined, attemptCount: 0, earliestWakeAt: 0, pendingWake: false,
        }, target);
        wakeChanged = true;
      }
      return checked;
    });
    if (wakeChanged) this.events.emit('target', mailboxId);
    if (pinnedWakeMailbox) this.events.emit('target', pinnedWakeMailbox);
    return result;
  }

  private matchingPendingDelivery(target: WakeTargetState, consumedItemIds: readonly string[]): PendingWakeDelivery | undefined {
    const pending = target.pendingDelivery;
    if (target.deliveryInDoubt && pending?.queuedItemIds.length && (target.bindingRevision ?? 0) === pending.bindingRevision) {
      const consumed = new Set(consumedItemIds);
      if (pending.queuedItemIds.some(id => consumed.has(id))) return pending;
    }
    return undefined;
  }

  private checkMailbox(mailboxId: string, claim: boolean): MailboxCheckResult {
    const record = this.db.mailbox(mailboxId);
    const queued = this.db.items({ mailboxId, status: 'queued' }).sort(compareOldestFirst);
    if (record?.outstanding) {
      const lease = record.outstanding;
      if (claim) this.db.writeMailbox({ ...record, outstanding: { ...lease, wakeCount: lease.wakeCount + 1 } });
      return {
        mailboxId,
        deliveryId: lease.leaseId,
        redelivered: true,
        newerQueuedCount: queued.length,
        maxQueuedSequence: queued.reduce((max, item) => Math.max(max, item.sequence), -1),
        ...(queued.length ? { latestQueued: queued.at(-1)! } : {}),
        items: this.db.items({ mailboxId, status: 'leased' })
          .filter(item => item.leaseId === lease.leaseId).sort(compareOldestFirst),
      };
    }
    if (!claim || queued.length === 0) {
      return { mailboxId, deliveryId: null, redelivered: false, newerQueuedCount: 0, items: queued };
    }
    const leaseId = mintLeaseId(this.db);
    const leasedAt = new Date().toISOString();
    this.db.leaseItems(mailboxId, leaseId, leasedAt);
    this.db.writeMailbox({
      mailboxId,
      ackedLeaseIds: record?.ackedLeaseIds ?? [],
      outstanding: { leaseId, mailboxId, itemIds: queued.map(item => item.id), createdAt: leasedAt, wakeCount: 0 },
    });
    return {
      mailboxId,
      deliveryId: leaseId,
      redelivered: false,
      newerQueuedCount: 0,
      items: queued.map(item => ({ ...item, status: 'leased', leaseId, leasedAt })),
    };
  }

  resolveLeaseMailbox(leaseId: string): string | undefined {
    return this.db.leaseMailbox(leaseId)?.mailboxId;
  }

  /**
   * Read-only preflight for a caller-supplied `--ack` lease. It mirrors `ack()`'s lookup
   * but never mutates the mailbox, so a command that later fails (or sends nothing) cannot
   * burn the lease that proves the caller still owns the inbound batch.
   */
  assertAckableLease(leaseId: string, mailboxId?: string): void {
    const requestedMailbox =
      mailboxId === undefined ? undefined : normalizeMailboxId(mailboxId);
    const record = requestedMailbox === undefined ? this.db.leaseMailbox(leaseId) : this.db.mailbox(requestedMailbox);
    if (record?.outstanding?.leaseId === leaseId || record?.ackedLeaseIds.includes(leaseId)) return;
    throw new Error(`Unknown or mismatched delivery lease: ${leaseId}`);
  }

  ack(leaseId: string, mailboxId?: string): MailboxAckResult {
    const requestedMailbox =
      mailboxId === undefined ? undefined : normalizeMailboxId(mailboxId);
    let wakeMailbox: string | undefined;
    let pinnedWakeMailbox: string | undefined;
    const result = this.db.write(() => {
      const record = requestedMailbox === undefined ? this.db.leaseMailbox(leaseId) : this.db.mailbox(requestedMailbox);
      if (!record || (record.outstanding?.leaseId !== leaseId && !record.ackedLeaseIds.includes(leaseId))) {
        throw new Error(`Unknown or mismatched delivery lease: ${leaseId}`);
      }
      const id = record.mailboxId;
      if (record.outstanding?.leaseId !== leaseId) {
        return { mailboxId: id, leaseId, itemIds: [], alreadyAcknowledged: true };
      }
      const ackedAt = new Date().toISOString();
      const itemIds = [...new Set(record.outstanding.itemIds)];
      for (const itemId of itemIds) this.db.ackItem(id, itemId, ackedAt);
      this.db.writeMailbox({
        mailboxId: id,
        ackedLeaseIds: [...record.ackedLeaseIds, leaseId].slice(-MAX_ACKED_LEASE_HISTORY),
      });
      const target = this.db.wakeTarget(id);
      if (target) {
        const pending = this.matchingPendingDelivery(target, itemIds);
        const completedWaves = target.completedWaves + 1;
        if (pending) {
          ({ pinnedWakeMailbox } = this.settleWakeDeliveryInTx(
            target, target, pending.deliveredSequence, { completedWaves }, pending.pinnedSession, this.db.hasQueuedItems(id),
          ));
          wakeMailbox = id;
        } else if (target.deliveryInDoubt) {
          this.db.writeWakeTarget({ ...target, completedWaves, pendingWake: false }, target);
        } else {
          // Queued mail behind the completed wave may now rotate to a fresh session.
          const pendingWake = target.pendingWake || this.db.hasQueuedItems(id);
          this.db.writeWakeTarget({
            ...target, completedWaves, pendingWake, lastWakeError: undefined, attemptCount: 0, earliestWakeAt: 0,
          }, target);
          if (pendingWake) wakeMailbox = id;
        }
      }
      if (this.db.hasAckedHistoryBeyond(MAX_RETAINED_ACKED_ITEMS)) {
        const consumed = this.readConsumedSequence?.() ?? Number.MAX_SAFE_INTEGER;
        this.db.trimAckedItems(consumed, MAX_RETAINED_ACKED_ITEMS);
      }
      compactThreads(this.db, this.outbox);
      return { mailboxId: id, leaseId, itemIds, alreadyAcknowledged: false };
    });
    if (wakeMailbox !== undefined) {
      this.events.emit('target', wakeMailbox);
    }
    if (pinnedWakeMailbox) this.events.emit('target', pinnedWakeMailbox);
    return result;
  }

  hasConfirmedEnvelope(networkId: string, senderNodeId: string, entityId: string, minRevision: number): boolean {
    return this.db.items().some(item => {
      const authenticated = item.signedEnvelope ?? item.envelope;
      const revision = authenticated.payload?.claimVersion ?? authenticated.payload?.revision ?? 0;
      return authenticated.id === item.id && verifyEnvelope(authenticated) &&
        authenticated.networkId === networkId && authenticated.from.nodeId === senderNodeId &&
        (authenticated.idempotencyKey ?? authenticated.id) === entityId &&
        typeof revision === 'number' && Number.isSafeInteger(revision) && revision >= minRevision;
    });
  }

  list(options: MailboxListOptions = {}): MailboxListResult {
    const mailboxId =
      options.mailboxId === undefined ? undefined : normalizeMailboxId(options.mailboxId);
    if (
      options.limit !== undefined &&
      (!Number.isSafeInteger(options.limit) || options.limit <= 0)
    ) {
      throw new Error('Limit must be a positive integer');
    }
    return this.db.read(() => {
      const counts = this.db.countItems(mailboxId);
      const ordered = this.db.items({ mailboxId, unacked: options.includeAcked !== true }).sort(compareNewestFirst);
      return {
        ...(mailboxId !== undefined ? { mailboxId } : {}),
        totalCount: counts.queued + counts.leased + counts.acked,
        queuedCount: counts.queued,
        leasedCount: counts.leased,
        ackedCount: counts.acked,
        items: options.limit === undefined ? ordered : ordered.slice(0, options.limit),
      };
    });
  }

  summarizeQueues(options: QueueStatusOptions = {}): { summary: QueueSummary; queues: QueueStatus[] } {
    const filter = options.mailboxId === undefined ? undefined : normalizeMailboxId(options.mailboxId);
    const now = options.now ?? Date.now();
    if (!Number.isFinite(now)) throw new Error('Invalid queue status instant');
    return this.db.read(() => {
      const summary: QueueSummary = {
        activeMailboxes: 0, totalMailboxes: 0, queuedItems: 0, leasedItems: 0, pendingWakes: 0, fencedTargets: 0,
      };
      const queues: QueueStatus[] = [];
      for (const { mailboxId, queuedCount, leasedCount, lease, wakeTarget } of this.db.queueRows(filter)) {
        const pendingWake = wakeTarget?.pendingWake ?? false;
        const deliveryInDoubt = wakeTarget?.deliveryInDoubt ?? false;
        const active = queuedCount > 0 || leasedCount > 0 || lease !== undefined || pendingWake || deliveryInDoubt;
        summary.totalMailboxes++;
        summary.activeMailboxes += Number(active);
        summary.queuedItems += queuedCount;
        summary.leasedItems += leasedCount;
        summary.pendingWakes += Number(pendingWake);
        summary.fencedTargets += Number(deliveryInDoubt);
        if (!active && options.includeIdle !== true) continue;
        const activeLease = lease && {
          leaseId: lease.leaseId, itemCount: lease.itemIds.length, createdAt: lease.createdAt,
          ageMs: Math.max(0, now - Date.parse(lease.createdAt)) || 0,
        };
        queues.push({
          mailboxId, queuedCount, leasedCount, ...(activeLease ? { activeLease } : {}),
          pendingWake, deliveryInDoubt, attemptCount: wakeTarget?.attemptCount ?? 0,
          lastWakeError: wakeTarget?.lastWakeError,
          nextWakeInMs: wakeTarget?.pendingWake && !deliveryInDoubt ? Math.max(0, wakeTarget.earliestWakeAt - now) : null,
          driver: wakeTarget?.driver ?? null, driverConfig: wakeTarget?.driverConfig,
        });
      }
      return { summary, queues };
    });
  }

  async waitForMail(options: MailboxWaitOptions = {}): Promise<MailboxWaitResult> {
    return MailboxStore.waitForMail(this, options);
  }

  /** Wait against a current snapshot, including an absent, uninitialized mailbox. */
  static async waitForMail(store: MailboxStore | undefined, options: MailboxWaitOptions = {}): Promise<MailboxWaitResult> {
    const events = store?.events;
    const mailboxId = normalizeMailboxId(options.mailboxId ?? 'default');
    const timeoutMs =
      options.timeoutMs === undefined
        ? DEFAULT_WAIT_TIMEOUT_MS
        : normalizeWaitTimeout(options.timeoutMs);
    const checkOptions: MailboxCheckOptions = {
      mailboxId,
      ...(options.ackLeaseId !== undefined ? { ackLeaseId: options.ackLeaseId } : {}),
      ...(options.peek === true ? { peek: true } : {}),
    };
    if (options.signal?.aborted) {
      throw createAbortError();
    }
    return new Promise<MailboxWaitResult>((resolve, reject) => {
      let settled = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const cleanup = () => {
        events?.off('mail', onMail);
        events?.off('close', onAbort);
        if (timer !== undefined) {
          clearTimeout(timer);
        }
        options.signal?.removeEventListener('abort', onAbort);
      };
      const finish = (action: () => void) => {
        if (settled) {
          return;
        }
        settled = true;
        cleanup();
        action();
      };
      const attempt = () => {
        try {
          const result = store?.check(checkOptions);
          if (result && result.items.length > 0) {
            finish(() => resolve({ ...result, timedOut: false }));
          }
        } catch (error) {
          finish(() => reject(error));
        }
      };
      const onMail = (id: string) => {
        if (id === mailboxId) {
          attempt();
        }
      };
      const onAbort = () => finish(() => reject(createAbortError()));
      // Subscribe before the first check: mail arriving mid-check must not be lost.
      events?.on('mail', onMail);
      events?.once('close', onAbort);
      options.signal?.addEventListener('abort', onAbort, { once: true });
      if (options.signal?.aborted) {
        onAbort();
        return;
      }
      timer = setTimeout(
        () =>
          finish(() =>
            resolve({
              mailboxId,
              deliveryId: null,
              redelivered: false,
              newerQueuedCount: 0,
              items: [],
              timedOut: true,
            }),
          ),
        timeoutMs,
      );
      attempt();
    });
  }

  upsertWakeTarget(
    mailboxId: string,
    patch: Partial<WakeTargetState> = {},
    options?: { preservePendingDeliveryFromRevision?: number },
  ): WakeTargetState {
    const id = normalizeMailboxId(mailboxId);
    let notify = false;
    const merged = this.db.write(() => {
      const previous = this.db.wakeTarget(id);
      const current = previous ?? defaultWakeTarget(id);
      let next = validateWakeTarget(mergeWakeTarget(current, patch));
      const fromRevision = options?.preservePendingDeliveryFromRevision;
      const pending = current.pendingDelivery;
      if (fromRevision !== undefined && fromRevision === (current.bindingRevision ?? 0) &&
          current.deliveryInDoubt && pending?.bindingRevision === fromRevision && next.deliveryInDoubt) {
        next = {
          ...next,
          pendingDelivery: {
            ...pending, originRevision: pending.originRevision ?? fromRevision, bindingRevision: next.bindingRevision ?? 0,
          },
        };
      }
      this.db.writeWakeTarget(next, previous);
      notify = current.pendingWake || next.pendingWake || Boolean(current.deliveryInDoubt) !== Boolean(next.deliveryInDoubt);
      return structuredClone(next);
    });
    if (notify) {
      this.events.emit('target', id);
    }
    return merged;
  }

  /**
   * Atomically fence a wake delivery inside SQLite when the binding revision (and optional
   * no-outstanding-lease precondition) still holds, returning a structured outcome.
   */
  tryBeginWakeDelivery(
    mailboxId: string,
    expectedRevision?: number,
    options: {
      deliveredSequence?: number;
      pinnedSession?: PinnedSessionMailbox;
      requireNoOutstandingLease?: boolean;
    } = {},
  ): BeginWakeDeliveryResult {
    const id = normalizeMailboxId(mailboxId);
    const result = this.db.write<BeginWakeDeliveryResult>(() => {
      const previous = this.db.wakeTarget(id);
      const current = previous ?? defaultWakeTarget(id);
      const revision = current.bindingRevision ?? 0;
      if (expectedRevision !== undefined && revision !== expectedRevision) {
        return { kind: 'superseded', target: structuredClone(current) };
      }
      if (current.deliveryInDoubt) {
        throw new Error(`Dealer wake delivery requires reconciliation: ${id}`);
      }
      if (options.requireNoOutstandingLease) {
        const outstanding = this.db.mailbox(id)?.outstanding;
        if (outstanding) {
          return { kind: 'outstanding_lease', target: structuredClone(current), leaseId: outstanding.leaseId };
        }
      }
      const pendingDelivery = options.deliveredSequence !== undefined ? {
        deliveredSequence: options.deliveredSequence,
        ...(options.pinnedSession !== undefined ? { pinnedSession: options.pinnedSession } : {}),
        bindingRevision: revision,
        originRevision: revision,
        queuedItemIds: this.db.items({ mailboxId: id, status: 'queued', throughSequence: options.deliveredSequence }).map(item => item.id),
      } : undefined;
      const next = validateWakeTarget(mergeWakeTarget(current, {
        deliveryInDoubt: true, pendingWake: false,
        lastWakeError: 'External wake delivery outcome is not yet confirmed',
        pendingDelivery,
      }));
      this.db.writeWakeTarget(next, previous);
      return { kind: 'fenced', target: structuredClone(next) };
    });
    if (result.kind === 'fenced') this.events.emit('target', id);
    return result;
  }

  /**
   * Fence before an external delivery so a crash or an unknown outcome cannot silently replay it.
   * Pointer deliveries can supply an observation that a matching consumer lease may confirm.
   */
  beginWakeDelivery(
    mailboxId: string, expectedRevision?: number, pointer?: { deliveredSequence: number; pinnedSession?: PinnedSessionMailbox },
  ): WakeTargetState | undefined {
    const result = this.tryBeginWakeDelivery(mailboxId, expectedRevision, pointer);
    return result.kind === 'fenced' ? result.target : undefined;
  }

  /** Commit only against the binding that authorized this launch. */
  commitWakeLaunch(
    mailboxId: string,
    expectedRevision: number,
    buildPatch: (lockedCurrent: WakeTargetState) => Partial<WakeTargetState>,
  ): { committed: boolean; target: WakeTargetState } {
    const id = normalizeMailboxId(mailboxId);
    const result = this.db.write(() => {
      const previous = this.db.wakeTarget(id);
      const current = previous ?? defaultWakeTarget(id);
      const committed = (current.bindingRevision ?? 0) === expectedRevision;
      const patch = committed ? buildPatch(structuredClone(current)) : { attemptCount: 0, earliestWakeAt: 0 };
      let next = validateWakeTarget(mergeWakeTarget(current, {
        ...patch, deliveryInDoubt: undefined, pendingWake: true, lastWakeError: undefined,
      }));
      if (committed) next = { ...next, bindingRevision: Math.max(next.bindingRevision ?? 0, expectedRevision + 1) };
      this.db.writeWakeTarget(next, previous);
      return { committed, target: structuredClone(next) };
    });
    if (!result.committed) this.events.emit('target', id);
    return result;
  }

  /** Settle a retryable attempt only against its binding and, if supplied, its completed wave count. */
  deferWakeAttempt(
    mailboxId: string, expectedRevision: number | undefined, error: unknown, now = Date.now(),
    patch: Partial<WakeTargetState> = {}, expectedCompletedWaves?: number,
  ): { superseded: boolean; acknowledged?: true; target: WakeTargetState } {
    const id = normalizeMailboxId(mailboxId);
    const result = this.db.write(() => {
      const previous = this.db.wakeTarget(id);
      const current = previous ?? defaultWakeTarget(id);
      const superseded = expectedRevision !== undefined && (current.bindingRevision ?? 0) !== expectedRevision;
      if (!superseded && !current.deliveryInDoubt && expectedCompletedWaves !== undefined &&
          current.completedWaves > expectedCompletedWaves) {
        return { superseded: false, acknowledged: true as const, target: structuredClone(current) };
      }
      const attemptCount = superseded ? 0 : current.attemptCount + 1;
      const next = validateWakeTarget(mergeWakeTarget(current, {
        ...(superseded ? {} : patch), pendingWake: true, deliveryInDoubt: undefined, attemptCount,
        earliestWakeAt: superseded ? 0 : now + wakeupBackoffDelay(attemptCount),
        lastWakeError: superseded ? undefined : String(error),
      }));
      this.db.writeWakeTarget(next, previous);
      return { superseded, target: structuredClone(next) };
    });
    if (result.superseded || expectedRevision === undefined) this.events.emit('target', id);
    return result;
  }

  retainPendingWakeIfCurrent(mailboxId: string, expectedRevision: number): WakeTargetState {
    const id = normalizeMailboxId(mailboxId);
    const next = this.db.write(() => {
      const previous = this.db.wakeTarget(id);
      const current = previous ?? defaultWakeTarget(id);
      const superseded = (current.bindingRevision ?? 0) !== expectedRevision;
      const target = validateWakeTarget(mergeWakeTarget(current, {
        pendingWake: true, ...(superseded ? { attemptCount: 0, earliestWakeAt: 0 } : {}),
      }));
      this.db.writeWakeTarget(target, previous);
      return structuredClone(target);
    });
    this.events.emit('target', id);
    return next;
  }

  disarmWakeTargetIfCurrent(mailboxId: string, expectedRevision: number): { superseded: boolean; target: WakeTargetState } {
    const id = normalizeMailboxId(mailboxId);
    const result = this.db.write(() => {
      const previous = this.db.wakeTarget(id);
      const current = previous ?? defaultWakeTarget(id);
      const superseded = (current.bindingRevision ?? 0) !== expectedRevision;
      const next = validateWakeTarget(mergeWakeTarget(current, superseded ? { pendingWake: true } : {
        pendingWake: false, attemptCount: 0, earliestWakeAt: 0, lastWakeError: undefined,
      }));
      this.db.writeWakeTarget(next, previous);
      return { superseded, target: structuredClone(next) };
    });
    this.events.emit('target', id);
    return result;
  }

  updateWakeDriverConfigIfCurrent(mailboxId: string, expectedRevision: number, driverConfig: Record<string, unknown>): { superseded: boolean; target: WakeTargetState } {
    const id = normalizeMailboxId(mailboxId);
    const result = this.db.write(() => {
      const previous = this.db.wakeTarget(id);
      const current = previous ?? defaultWakeTarget(id);
      if ((current.bindingRevision ?? 0) !== expectedRevision) return { superseded: true, target: structuredClone(current) };
      const next = validateWakeTarget(mergeWakeTarget(current, { driverConfig }));
      this.db.writeWakeTarget(next, previous);
      return { superseded: false, target: structuredClone(next) };
    });
    if (!result.superseded && result.target.pendingWake && result.target.bindingRevision !== expectedRevision) {
      this.events.emit('target', id);
    }
    return result;
  }

  /** Keep an unconfirmed external outcome fenced; later mail, forced flushes and restarts never replay it. */
  parkWakeDelivery(mailboxId: string, error: unknown): WakeTargetState {
    const id = normalizeMailboxId(mailboxId);
    const parked = this.db.write(() => {
      const previous = this.db.wakeTarget(id);
      const current = previous ?? defaultWakeTarget(id);
      const next = validateWakeTarget(mergeWakeTarget(current, {
        deliveryInDoubt: true, pendingWake: false, earliestWakeAt: 0,
        pendingDelivery: current.deliveryInDoubt && current.pendingDelivery?.bindingRevision === (current.bindingRevision ?? 0)
          ? current.pendingDelivery : undefined,
        attemptCount: current.attemptCount + 1, lastWakeError: String(error),
      }));
      this.db.writeWakeTarget(next, previous);
      return structuredClone(next);
    });
    this.events.emit('target', id);
    return parked;
  }

  clearWakeFence(options: { mailboxId?: string; all?: boolean }): { cleared: string[]; woke: string[] } {
    if (Boolean(options.mailboxId) === Boolean(options.all)) throw new Error('Choose either a mailbox id or all wake fences');
    const mailboxId = options.mailboxId === undefined ? undefined : normalizeMailboxId(options.mailboxId);
    const result = this.db.write(() => {
      const cleared: string[] = [];
      const woke: string[] = [];
      const targets = mailboxId === undefined ? this.db.wakeTargets() : [this.db.wakeTarget(mailboxId)];
      for (const target of targets) {
        if (target?.deliveryInDoubt !== true) continue;
        const id = target.mailboxId;
        const pendingWake = this.db.hasQueuedItems(id) && !this.db.mailbox(id)?.outstanding;
        this.db.writeWakeTarget({
          ...target, deliveryInDoubt: undefined, lastWakeError: undefined,
          pendingDelivery: undefined, pendingWake, attemptCount: 0, earliestWakeAt: 0,
        }, target);
        cleared.push(id);
        if (pendingWake) woke.push(id);
      }
      return { cleared, woke };
    });
    for (const id of result.cleared) this.events.emit('target', id);
    return result;
  }

  /** Settle only the observed wave; messages arriving during delivery retain their wake. */
  completeWakeDelivery(
    mailboxId: string,
    deliveredSequence: number,
    patch:
      | Partial<WakeTargetState>
      | ((lockedCurrent: WakeTargetState, context: CompleteWakeDeliveryContext) => Partial<WakeTargetState>) = {},
    pinnedSession?: PinnedSessionMailbox,
    expectedRevision?: number,
  ): WakeTargetState & { superseded?: boolean } {
    const id = normalizeMailboxId(mailboxId);
    let pinnedWakeMailbox: string | undefined;
    let notify = false;
    const completed = this.db.write(() => {
      const previous = this.db.wakeTarget(id);
      const current = previous ?? defaultWakeTarget(id);
      const revision = current.bindingRevision ?? 0;
      const pending = current.pendingDelivery;
      const currentOrigin = current.deliveryInDoubt && pending?.bindingRevision === revision
        ? pending.originRevision ?? pending.bindingRevision : undefined;
      if (expectedRevision !== undefined && revision !== expectedRevision && currentOrigin !== expectedRevision) {
        if (currentOrigin !== undefined) return { ...structuredClone(current), superseded: true };
        const hasQueued = this.db.hasQueuedItems(id);
        const next = validateWakeTarget(mergeWakeTarget(current, {
          deliveryInDoubt: undefined, lastWakeError: undefined,
          pendingWake: Boolean(current.pendingWake || hasQueued), attemptCount: 0, earliestWakeAt: 0,
        }));
        this.db.writeWakeTarget(next, previous);
        notify = next.pendingWake || Boolean(current.deliveryInDoubt);
        return Object.assign(structuredClone(next), { superseded: true });
      }

      const bounds = this.db.mailboxSequenceBounds(id);
      const consumed = this.readConsumedSequence?.();
      const minRetainedSequence =
        bounds.minSequence !== undefined && consumed !== undefined
          ? Math.min(bounds.minSequence, consumed + 1)
          : bounds.minSequence ?? (consumed !== undefined ? consumed + 1 : undefined);
      const context: CompleteWakeDeliveryContext = {
        maxConsumedSequence: Math.max(deliveredSequence, bounds.maxConsumedSequence ?? -1),
        ...(minRetainedSequence !== undefined ? { minRetainedSequence } : {}),
      };
      const resolvedPatch = typeof patch === 'function' ? patch(structuredClone(current), context) : patch;
      const settled = this.settleWakeDeliveryInTx(current, previous, deliveredSequence, resolvedPatch, pinnedSession);
      pinnedWakeMailbox = settled.pinnedWakeMailbox;
      notify = current.pendingWake || settled.target.pendingWake || Boolean(current.deliveryInDoubt);
      return structuredClone(settled.target);
    });
    if (notify) this.events.emit('target', id);
    if (pinnedWakeMailbox) this.events.emit('target', pinnedWakeMailbox);
    return completed;
  }

  private settleWakeDeliveryInTx(
    current: WakeTargetState, previous: WakeTargetState | undefined, deliveredSequence: number,
    patch: Partial<WakeTargetState>, pinnedSession?: PinnedSessionMailbox, wakeNext?: boolean,
  ): { target: WakeTargetState; pinnedWakeMailbox?: string } {
    const pendingWake = wakeNext ?? this.db.hasQueuedItems(current.mailboxId, deliveredSequence);
    const target = validateWakeTarget(mergeWakeTarget(current, {
      ...patch, pendingWake, attemptCount: 0, lastWakeError: undefined,
      earliestWakeAt: wakeNext === undefined && pendingWake ? current.earliestWakeAt : 0,
      deliveryInDoubt: undefined, pendingDelivery: undefined,
    }));
    this.db.writeWakeTarget(target, previous);
    const pinnedWakeMailbox = pinnedSession && this.pinDeliveredThreads(current, deliveredSequence, pinnedSession);
    return { target, pinnedWakeMailbox };
  }

  private pinDeliveredThreads(target: WakeTargetState, sequence: number, session: PinnedSessionMailbox): string | undefined {
    if (target.rotateAfterWaves === undefined) return undefined;
    const id = target.mailboxId;
    const termMailbox = pinSessionTarget(this.db, id, session.sessionMailboxId, session);
    const delivered = this.db.items({ mailboxId: id, throughSequence: sequence });
    const keys = new Set(delivered.flatMap(item => [item.envelope.semantic?.threadId, item.envelope.id].filter((v): v is string => typeof v === 'string')));
    const updatedAt = new Date().toISOString();
    for (const record of this.db.threads({ localMailbox: id })) {
      const key = threadKey(record);
      if (record.mailboxCommitted !== false && (keys.has(key) || keys.has(record.threadId) || keys.has(record.messageId))) {
        this.db.writeThread(key, {
          ...record, localMailbox: termMailbox, updatedAt,
          ...(record.direction === 'inbound' ? { mailboxCommitted: true } : {}),
        });
      }
    }
    const pinned = this.db.wakeTarget(termMailbox);
    return pinned?.pendingWake && !pinned.deliveryInDoubt ? termMailbox : undefined;
  }

  pinSessionMailbox(mailboxId: string, sessionMailboxId: string, sessionPatch?: SessionMailboxPatch): string {
    const id = normalizeMailboxId(mailboxId);
    let notify = false;
    const pinned = this.db.write(() => {
      const mailbox = pinSessionTarget(this.db, id, sessionMailboxId, sessionPatch);
      const target = this.db.wakeTarget(mailbox);
      notify = Boolean(target?.pendingWake && !target.deliveryInDoubt);
      return mailbox;
    });
    if (notify) this.events.emit('target', pinned);
    return pinned;
  }

  getWakeTarget(mailboxId: string): WakeTargetState | undefined {
    const id = normalizeMailboxId(mailboxId);
    return this.db.wakeTarget(id);
  }

  inspectSlotObligations(
    mailboxId: string,
    options?: { sessionMailboxId?: string; outbox?: OutboxStore },
  ): SlotObligationSnapshot {
    const id = normalizeMailboxId(mailboxId);
    const sessionId = options?.sessionMailboxId ? normalizeMailboxId(options.sessionMailboxId) : undefined;
    return this.db.read(() => this.readSlotObligations(id, sessionId, options?.outbox ?? this.outbox));
  }

  private readSlotObligations(id: string, sessionId?: string, outboxStore?: OutboxStore): SlotObligationSnapshot {
    const target = this.db.wakeTarget(id);
    const sessionTarget = sessionId && sessionId !== id ? this.db.wakeTarget(sessionId) : undefined;
    if (target?.pendingWake || target?.deliveryInDoubt) {
      return { hasPendingWave: this.db.hasUnackedItems(id), immediateBusyReason: 'wakeup_pending' };
    }
    if (sessionTarget?.pendingWake || sessionTarget?.deliveryInDoubt || (sessionId && sessionId !== id && this.db.hasUnackedItems(sessionId))) {
      return { hasPendingWave: this.db.hasUnackedItems(id), immediateBusyReason: 'session_reply_pending' };
    }
    const ownedMailboxes = new Set(sessionId ? [id, sessionId] : [id]);
    const selfInboundMessages = new Set<string>();
    const localUnackedMessages = new Set<string>();
    const localUnackedThreads = new Set<string>();
    let activeSince: string | undefined;
    for (const item of this.db.items()) {
      const { networkId, semantic } = item.envelope;
      const messageKey = netKey(networkId, item.id);
      if (ownedMailboxes.has(item.mailboxId)) {
        selfInboundMessages.add(messageKey);
        if (item.status === 'acked') continue;
        if (item.mailboxId === id && (activeSince === undefined || item.receivedAt < activeSince)) activeSince = item.receivedAt;
      } else if (item.status !== 'acked') {
        localUnackedMessages.add(messageKey);
        localUnackedThreads.add(netKey(networkId, semantic?.threadId ?? item.envelope.id));
      }
    }

    const hasPendingWave = activeSince !== undefined;
    const records = this.db.threads();
    const outbox = outboxStore && readOutboxThreadOwners(outboxStore, activeMailboxKeys(this.db));
    const recordKeys = new Set(records.map(threadKey));
    for (const item of outbox?.items ?? []) {
      if (!recordKeys.has(threadKey({ direction: 'outbound', messageId: item.id }))) records.push(outboxThreadRecord(item));
    }
    const settledThreads = new Set<string>();
    const ownedOutboundMessages = new Set<string>();
    const inboundByThread = new Map<string, ThreadRouteRecord[]>();
    for (const record of records) {
      const key = netKey(record.networkId, record.threadId);
      // Settlement snapshots are copied to other legs. Prefer the individual envelope's semantics.
      if (record.envelope ? record.envelope.semantic?.type === 'settle' : record.settlement !== undefined) {
        settledThreads.add(key);
      }
      if (record.direction === 'inbound') {
        if (record.mailboxCommitted !== false && ownedMailboxes.has(record.localMailbox)) {
          selfInboundMessages.add(netKey(record.networkId, record.messageId));
        }
        const inbound = inboundByThread.get(key) ?? [];
        inbound.push(record);
        inboundByThread.set(key, inbound);
      } else if (ownedMailboxes.has(record.envelope?.semantic?.replyMailbox ?? record.localMailbox)) {
        ownedOutboundMessages.add(netKey(record.networkId, record.messageId));
      }
    }

    const postAckMailboxes = target?.rotateAfterWaves !== undefined
      ? new Set(sessionId ? [sessionId] : [])
      : ownedMailboxes;
    const remoteMessageKeys: string[] = [];
    for (const record of records) {
      if (record.direction !== 'outbound') continue;
      const messageKey = netKey(record.networkId, record.messageId);
      if (selfInboundMessages.has(messageKey)) continue;
      const semantic = record.envelope?.semantic;
      const replyMailbox = semantic?.replyMailbox ?? record.localMailbox;
      const threadKey = netKey(record.networkId, record.threadId);
      const isReplyOrSettlement = semantic?.type === 'reply' || semantic?.type === 'settle';
      const ownsReplyMailbox = ownedMailboxes.has(replyMailbox);
      // Local custody lasts until acknowledgement, even after the wave or thread ends.
      if (
        ownsReplyMailbox &&
        (localUnackedMessages.has(messageKey) || (!isReplyOrSettlement && localUnackedThreads.has(threadKey)))
      ) {
        return { hasPendingWave, immediateBusyReason: 'local_outbound_pending' };
      }

      if (isReplyOrSettlement || settledThreads.has(threadKey)) continue;
      const isOwnedQuestion = semantic?.type === 'question' && postAckMailboxes.has(replyMailbox);
      const isActiveWaveOutbound = activeSince !== undefined && record.createdAt >= activeSince && ownsReplyMailbox;
      if (!isOwnedQuestion && !isActiveWaveOutbound) continue;

      const answered = inboundByThread.get(threadKey)?.some(inbound => {
        // The candidate's reply mailbox already belongs to this slot or its session.
        if (inbound.messageId === record.messageId ||
          inbound.envelope?.semantic?.type !== 'reply' ||
          ownedOutboundMessages.has(netKey(inbound.networkId, inbound.messageId)) ||
          !ownedMailboxes.has(inbound.localMailbox)) return false;
        const replyToId = inbound.envelope.replyToId;
        if (replyToId === record.messageId) return true;
        // Only an unaddressed reply can fall back to thread and time correlation.
        return replyToId === undefined && inbound.messageId !== record.envelope?.replyToId &&
          inbound.createdAt >= record.createdAt;
      });
      if (answered) continue;
      remoteMessageKeys.push(messageKey);
    }
    if (remoteMessageKeys.some(key => !outbox?.definitivelyFailed.has(key))) {
      return { hasPendingWave, immediateBusyReason: 'outbound_thread_open' };
    }
    return { hasPendingWave };
  }

  listWakeTargets(): WakeTargetState[] {
    return this.db.wakeTargets();
  }

  /** Arrival notification for one mailbox; never emitted by read-only operations. */
  onMail(listener: (mailboxId: string) => void): Disposable {
    this.events.on('mail', listener);
    return { dispose: () => this.events.off('mail', listener) };
  }

  /** Notification of committed changes affecting wake scheduling or readiness. */
  onWakeTarget(listener: (mailboxId: string) => void): Disposable {
    this.events.on('target', listener);
    return { dispose: () => this.events.off('target', listener) };
  }

  listPendingWakeTargets(nowMs = Date.now()): WakeTargetState[] {
    if (typeof nowMs !== 'number' || !Number.isFinite(nowMs)) {
      throw new Error('Invalid wake target instant');
    }
    return this.db.wakeTargets(nowMs).sort(
      (a, b) => a.earliestWakeAt - b.earliestWakeAt || a.mailboxId.localeCompare(b.mailboxId),
    );
  }

  private upsertThreadRecord(
    record: ThreadRouteRecord,
  ): ThreadRouteRecord {
    const key = threadKey(record);
    const existing = this.db.thread(key);
    if (record.mailboxCommitted === false && existing && existing.networkId === record.networkId &&
      existing.mailboxCommitted !== false) return existing;
    const now = record.updatedAt;
    let threadState = mergeThreadState(existing?.state ?? 'open', record.state);
    let threadSettlement = record.settlement ?? existing?.settlement;
    for (const current of this.db.threads({ threadId: record.threadId })) {
      if (current.networkId !== record.networkId) continue;
      const nextSettlement = record.settlement ?? current.settlement;
      const nextState = mergeThreadState(current.state, record.state);
      const currentKey = threadKey(current);
      if (currentKey !== key) {
        this.db.writeThread(currentKey, {
          ...current,
          state: nextState,
          ...(nextSettlement !== undefined ? { settlement: nextSettlement } : {}),
          updatedAt: now,
        });
      }
      threadState = mergeThreadState(threadState, nextState);
      if (nextSettlement !== undefined) {
        threadSettlement = nextSettlement;
      }
    }
    const stored: ThreadRouteRecord = {
      ...record,
      state: threadState,
      ...(threadSettlement !== undefined ? { settlement: threadSettlement } : {}),
      createdAt: existing?.createdAt ?? record.createdAt,
    };
    this.db.writeThread(key, stored);
    return stored;
  }

  private upsertInboundThread(
    envelope: MessageEnvelope,
    localMailbox: string,
    now: string,
    committed: boolean,
  ): ThreadRouteRecord {
    validateStoredEnvelope(envelope);
    const peerReplyMailbox = normalizeMailboxId(
      envelope.semantic?.replyMailbox ?? 'default',
      'peer reply mailbox',
    );
    const settlement = settlementOf(envelope, now);
    return this.upsertThreadRecord({
      threadId: envelope.semantic?.threadId ?? envelope.id,
      messageId: envelope.id,
      envelope: structuredClone(envelope),
      direction: 'inbound',
      peerNodeId: envelope.from.nodeId,
      peerNodeIds: [envelope.from.nodeId],
      networkId: envelope.networkId,
      peerReplyMailbox,
      localMailbox,
      mailboxCommitted: committed,
      traceId: envelope.traceId,
      kind: envelope.kind,
      subject: envelope.subject,
      state: stateForSemantic(envelope),
      ...(settlement ? { settlement } : {}),
      createdAt: now,
      updatedAt: now,
    });
  }

  recordInboundEnvelope(
    envelope: MessageEnvelope,
    localMailbox: string,
    options: { committed?: boolean } = {},
  ): ThreadRouteRecord {
    const mailboxId = normalizeMailboxId(localMailbox, 'local mailbox');
    const now = new Date().toISOString();
    return this.db.write(() => {
      const record = this.upsertInboundThread(envelope, mailboxId, now, options.committed ?? true);
      compactThreads(this.db, this.outbox);
      return structuredClone(record);
    });
  }

  recordOutboundEnvelope(
    envelope: MessageEnvelope,
    peerNodeIds: readonly string[],
    localMailbox = 'default',
    options?: { createdAt?: string; restore?: boolean },
  ): ThreadRouteRecord {
    const local = normalizeMailboxId(localMailbox, 'local mailbox');
    if (!Array.isArray(peerNodeIds) || peerNodeIds.some(id => !isNonEmptyString(id))) {
      throw new Error('Invalid peer node ids');
    }
    if (!isRecord(envelope) || !isNonEmptyString(envelope.id)) {
      throw new Error('Invalid envelope: missing id');
    }
    if (envelope.semantic !== undefined) {
      validateSemanticHeader(envelope.semantic);
    }
    const now = new Date().toISOString();
    const record = outboundThreadRecord(envelope, [...new Set(peerNodeIds)], local, options?.createdAt ?? now, now);
    if (options?.restore) return this.restoreOutboundThreads(() => [record])[0];
    return this.db.write(() => {
      const stored = this.upsertThreadRecord(record);
      compactThreads(this.db, this.outbox);
      return structuredClone(stored);
    });
  }

  /** Lock-free references whose slot obligations still consult outbox failure evidence. */
  readUnsettledOutboundKeys(): ReadonlySet<string> {
    const keys = new Set<string>();
    for (const record of this.db.threads({ unsettledOutbound: true })) {
      keys.add(netKey(record.networkId, record.messageId));
    }
    return keys;
  }

  reconcileOutboxThreads(outbox: OutboxStore): void {
    this.restoreOutboundThreads(() => {
      const active = activeMailboxKeys(this.db);
      const { items, confirmedReplies } = readOutboxThreadOwners(outbox, active);
      return items.filter(item => {
        const key = netKey(item.networkId, item.id);
        const threadId = envelopeThreadId(item.envelope);
        if (!confirmedReplies.has(key) || active.messages.has(key) || active.threads.has(netKey(item.networkId, threadId))) return true;
        // A terminal reply can complete an existing route, but cannot recreate expired history.
        const replyToId = item.envelope.replyToId;
        if (!replyToId) return false;
        const root = this.db.thread(threadKey({ direction: 'inbound', messageId: replyToId }));
        return root?.networkId === item.networkId && root.threadId === threadId;
      }).map(outboxThreadRecord);
    }, true);
  }

  private restoreOutboundThreads(readRecords: () => readonly ThreadRouteRecord[], compact = false): ThreadRouteRecord[] {
    return this.db.write(() => {
      let changed = false;
      const restored = readRecords().map(record => {
        const existing = this.db.thread(threadKey(record));
        if (existing && (existing.peerNodeIds.length > 0 || record.peerNodeIds.length === 0)) return existing;
        changed = true;
        return this.upsertThreadRecord(existing ? { ...record, localMailbox: existing.localMailbox } : record);
      });
      if (changed || compact) compactThreads(this.db, this.outbox);
      return structuredClone(restored);
    });
  }

  /**
   * Operator reconciliation for an orphan outbound thread record left by a send that never
   * committed. Only the outbound leg may be pruned; the remaining records of the thread are
   * recomputed from their own envelopes so a stale settlement cannot outlive its message.
   */
  pruneOutboundThreadRecord(messageId: string): PruneOutboundThreadResult | undefined {
    if (!isNonEmptyString(messageId)) {
      throw new Error('Invalid message id');
    }
    return this.db.write(() => {
      const key = `outbound:${messageId}`;
      const removed = this.db.thread(key);
      if (removed === undefined) {
        if (this.db.thread(`inbound:${messageId}`) !== undefined) {
          throw new Error(`Cannot prune inbound thread record: ${messageId}`);
        }
        return undefined;
      }
      this.db.deleteThread(key);
      const remaining = threadRecordsFor(this.db, removed.threadId);
      let nextState: ThreadRouteRecord['state'] = 'open';
      let nextSettlement: ThreadRouteRecord['settlement'] | undefined;
      for (const record of remaining) {
        const ownState = record.envelope ? stateForSemantic(record.envelope) : record.state;
        const ownSettlement = record.envelope
          ? settlementOf(record.envelope, record.createdAt)
          : record.settlement;
        nextState = mergeThreadState(nextState, ownState);
        if (ownSettlement !== undefined) {
          nextSettlement = ownSettlement;
        }
      }
      const now = new Date().toISOString();
      for (const record of remaining) {
        // Drop any stale settlement first: the recomputed value is the only one that may
        // remain, and a thread with no settle envelope must not keep an old one.
        const { settlement: _discardedSettlement, ...rest } = record;
        this.db.writeThread(threadKey(record), {
          ...rest,
          state: nextState,
          updatedAt: now,
          ...(nextSettlement !== undefined ? { settlement: nextSettlement } : {}),
        });
      }
      compactThreads(this.db, this.outbox);
      return {
        pruned: structuredClone(removed),
        threadId: removed.threadId,
        remainingCount: remaining.length,
        state: nextState,
        ...(nextSettlement !== undefined ? { settlement: nextSettlement } : {}),
      };
    });
  }

  listItemsAfterSequence(afterSequence: number): MailboxItem[] {
    if (!Number.isSafeInteger(afterSequence) || afterSequence < -1) {
      throw new Error('Invalid sequence');
    }
    return this.db.items({ afterSequence });
  }

  listThreads(options: { kind?: string; state?: ThreadRouteRecord['state']; limit?: number } = {}): ThreadSummaryRecord[] {
    if (options.state !== undefined && !THREAD_STATES.includes(options.state)) {
      throw new Error('Invalid thread state');
    }
    if (options.limit !== undefined && (!Number.isSafeInteger(options.limit) || options.limit < 1)) {
      throw new Error('Invalid thread limit');
    }
    const groups = new Map<string, ThreadRouteRecord[]>();
    for (const record of this.db.threads()) {
      const group = groups.get(record.threadId) ?? [];
      group.push(record);
      groups.set(record.threadId, group);
    }
    const summaries = [...groups.values()].map(records => {
      records.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
      const root = records.find(record => record.messageId === record.threadId) ?? records[0];
      const latest = records.at(-1)!;
      return {
        threadId: root.threadId, rootMessageId: root.messageId, latestMessageId: latest.messageId,
        kind: root.kind, subject: root.subject,
        state: records.reduce((state, record) => mergeThreadState(state, record.state), 'open' as ThreadRouteRecord['state']),
        settlement: records.findLast(record => record.settlement)?.settlement,
        localMailbox: latest.localMailbox,
        peerNodeIds: [...new Set(records.flatMap(record => [...record.peerNodeIds]))],
        hopCount: records.length, createdAt: records[0].createdAt,
        updatedAt: records.reduce((time, record) => record.updatedAt > time ? record.updatedAt : time, root.updatedAt),
        rootEnvelope: root.envelope, latestEnvelope: latest.envelope,
      };
    }).filter(record => (options.kind === undefined || record.kind === options.kind) &&
      (options.state === undefined || record.state === options.state))
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt) || a.threadId.localeCompare(b.threadId));
    return structuredClone(options.limit === undefined ? summaries : summaries.slice(0, options.limit));
  }

  getThread(messageOrThreadId: string): ThreadRouteRecord | undefined {
    if (!isNonEmptyString(messageOrThreadId)) {
      throw new Error('Invalid thread reference');
    }
    return this.db.read(() => lookupThread(this.db, messageOrThreadId));
  }

  getInboundThreadRecord(messageId: string, networkId: string): ThreadRouteRecord | undefined {
    const record = this.db.thread(threadKey({ direction: 'inbound', messageId }));
    return record?.networkId === networkId ? record : undefined;
  }

  /** All directional records of a thread, oldest first; accepts a message id or thread id. */
  listThreadRecords(messageOrThreadId: string): ThreadRouteRecord[] {
    if (!isNonEmptyString(messageOrThreadId)) {
      throw new Error('Invalid thread reference');
    }
    return this.db.read(() => {
      const exact = exactThreadRecord(this.db, messageOrThreadId);
      return threadRecordsFor(this.db, exact?.threadId ?? messageOrThreadId);
    });
  }

  /**
   * The session mailbox an existing thread is already handled in: the newest non-default local
   * leg. This is the durable thread-affinity record that keeps replies with the worktree/terminal
   * session that owns the thread instead of the topic's generic binding. Records are scoped to the
   * envelope's network so a colliding thread id on another network never inherits this route.
   * An arrival can exclude its own inbound leg when checking for existing thread affinity.
   */
  resolveThreadLocalMailbox(
    threadId?: string,
    networkId?: string,
    options?: { excludeInboundMessageId?: string },
  ): string | undefined {
    if (!threadId) return undefined;
    return this.threadLocalMailbox(threadId, networkId, options?.excludeInboundMessageId);
  }

  private threadLocalMailbox(
    threadId?: string,
    networkId?: string,
    excludeInboundMessageId?: string,
  ): string | undefined {
    if (typeof threadId !== 'string' || threadId.length === 0) {
      return undefined;
    }
    const records = threadRecordsFor(this.db, threadId)
      .filter(record => networkId === undefined || record.networkId === networkId);
    for (let index = records.length - 1; index >= 0; index -= 1) {
      const { direction, messageId, localMailbox, mailboxCommitted } = records[index];
      if (direction === 'inbound' && (messageId === excludeInboundMessageId || mailboxCommitted === false)) continue;
      if (localMailbox !== 'default') {
        return normalizeMailboxId(localMailbox, 'thread local mailbox');
      }
    }
    return undefined;
  }

  /**
   * Resolve the reply route of a message or thread. `networkId` scopes the lookup to that
   * network's records, and `peerNodeId` scopes `targetMailbox` strictly to that peer so a
   * multi-peer thread or multicast outbound record never borrows another peer's mailbox.
   */
  resolveReplyTarget(
    messageOrThreadId: string,
    networkId?: string,
    peerNodeId?: string,
  ): ResolvedReplyTarget | undefined {
    if (!isNonEmptyString(messageOrThreadId)) {
      throw new Error('Invalid thread reference');
    }
    if (peerNodeId !== undefined && !isNonEmptyString(peerNodeId)) {
      throw new Error('Invalid peer node id');
    }
    return this.db.read(() => this.readReplyTarget(messageOrThreadId, networkId, peerNodeId));
  }

  private readReplyTarget(
    messageOrThreadId: string,
    networkId?: string,
    peerNodeId?: string,
  ): ResolvedReplyTarget | undefined {
    const onNetwork = (candidate: ThreadRouteRecord): boolean =>
      networkId === undefined || candidate.networkId === networkId;
    const exactRecord = exactThreadRecord(this.db, messageOrThreadId);
    const exact = exactRecord !== undefined && onNetwork(exactRecord) ? exactRecord : undefined;
    const record = exact ?? threadRecordsFor(this.db, messageOrThreadId).filter(onNetwork).at(-1);
    if (!record) {
      return undefined;
    }
    const threadId = exact?.threadId ?? record.threadId;
    const networkRecords = threadRecordsFor(this.db, threadId)
      .filter(candidate => candidate.networkId === record.networkId);
    const targetPeer = peerNodeId ?? (record.peerNodeId || undefined);
    const peerRecords = targetPeer !== undefined
      ? networkRecords.filter(candidate => recordMatchesPeer(candidate, targetPeer))
      : networkRecords;
    // A thread reference must not inherit the route of the root we sent: when the thread was
    // started without an explicit target, its outbound leg records `default` as the peer mailbox.
    // Follow the peer's newest leg instead. A message we received keeps its own recorded leg when
    // it matches the target peer.
    const exactLeg = exact !== undefined &&
        !(exact.direction === 'outbound' && exact.messageId === exact.threadId) &&
        (targetPeer === undefined || recordMatchesPeer(exact, targetPeer))
      ? exact
      : undefined;
    const targetRecord = exactLeg !== undefined && recordedPeerMailbox(exactLeg) !== 'default'
      ? exactLeg
      : (peerRecords.findLast(candidate =>
          candidate.direction === 'inbound' && recordedPeerMailbox(candidate) !== 'default',
        ) ??
        peerRecords.findLast(candidate => recordedPeerMailbox(candidate) !== 'default') ??
        (targetPeer === undefined || recordMatchesPeer(record, targetPeer) ? record : undefined));
    const localMailbox = exactLeg !== undefined && exactLeg.mailboxCommitted !== false && exactLeg.localMailbox !== 'default'
      ? exactLeg.localMailbox
      : this.threadLocalMailbox(record.threadId, record.networkId) ?? 'default';
    return {
      threadId: record.threadId,
      messageId: record.messageId,
      peerNodeId: targetPeer ?? record.peerNodeId,
      networkId: record.networkId,
      targetMailbox: targetRecord ? recordedPeerMailbox(targetRecord) : 'default',
      localMailbox,
      traceId: record.traceId,
      kind: record.kind,
      subject: record.subject,
    };
  }
}

function mintLeaseId(db: MailboxSqliteBackend): string {
  let leaseId: string;
  do {
    leaseId = `dlv_${randomBytes(6).toString('hex')}`;
  } while (db.leaseMailbox(leaseId) !== undefined);
  return leaseId;
}
