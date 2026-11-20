import { randomBytes, randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import {
  existsSync,
  readFileSync,
  renameSync,
} from 'node:fs';
import { resolve } from 'node:path';
import {
  canonicalJson,
  isValidMailboxId,
  validateSemanticHeader,
  verifyEnvelope,
} from '../core-types/index.ts';
import type { Disposable, MessageEnvelope, SettlementOutcome } from '../core-types/index.ts';
import { withOwnedLock, writeFileAtomically } from '../fs-atomic/index.ts';
import { normalizeConversationModeInput } from '../core-types/index.ts';
import type { AgentConversationMode } from '../core-types/index.ts';
export { CONVERSATION_MODES, ACTIVE_CONVERSATION_MODES } from '../core-types/index.ts';
export type { AgentConversationMode, ActiveConversationMode } from '../core-types/index.ts';
export { normalizeConversationModeInput } from '../core-types/index.ts';

export const MAILBOX_SCHEMA = 'the-dealer-mailbox/v2';
export const LEGACY_MAILBOX_V1_SCHEMA = 'the-dealer-mailbox/v1';
const LEGACY_INBOX_SCHEMA = 'the-dealer-inbox/v1';
const MAX_ACKED_LEASE_HISTORY = 256;
const MAX_WAKEUP_HINT_LENGTH = 120;
const DEFAULT_WAIT_TIMEOUT_MS = 30_000;
const MAX_WAIT_TIMEOUT_MS = 3_600_000;
const LEASE_ID_PATTERN = /^dlv_[0-9a-f]{12}$/;

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

export interface MailboxStoreOptions {
  /** Legacy flat inbox file migrated once into the default mailbox when no mailbox exists. */
  legacyInboxPath?: string;
  /** Defaults to `${mailboxPath}.lock`. */
  lockDir?: string;
}

export interface MailboxEnqueueOptions {
  signedEnvelopes?: Readonly<Record<string, MessageEnvelope>>;
  mailboxId?: string;
  proposedSpawn?: boolean;
  wakeupHint?: string;
  wakeTarget?: Partial<WakeTargetState>;
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

interface MailboxFileState {
  schema: typeof MAILBOX_SCHEMA;
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
  'completedWaves', 'pendingWake', 'earliestWakeAt', 'attemptCount', 'lastWakeError', 'deliveryInDoubt',
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
      !/^[a-z0-9][a-z0-9:_-]{0,63}$/i.test(target.driver)) throw invalidStore();
  if (target.driverConfig !== undefined && (!isPlainRecord(target.driverConfig) || !isJsonValue(target.driverConfig))) throw invalidStore();
  if (target.deliveryInDoubt !== undefined && typeof target.deliveryInDoubt !== 'boolean') throw invalidStore();
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
}

function pinSessionTargetInState(state: MailboxFileState, id: string, sessionMailboxId: string, patch: SessionMailboxPatch = {}): string {
  const current = state.wakeTargets[id];
  if (current?.rotateAfterWaves === undefined) return id;
  const sessionMailbox = normalizeMailboxId(sessionMailboxId);
  if (sessionMailbox === id) throw new Error('Session mailbox must differ from rotating mailbox');
  const existing = state.wakeTargets[sessionMailbox];
  const nextDriver = patch.driver ?? existing?.driver ?? current.driver;
  const hasQueued = state.items.some(item => item.mailboxId === sessionMailbox && item.status === 'queued');
  const prefix = current.wakeupPrefix?.replace(new RegExp(`(--mailbox\\s+)${id.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?=\\s|$)`), `$1${sessionMailbox}`);
  state.wakeTargets[sessionMailbox] = validateWakeTarget(mergeWakeTarget(existing ?? defaultWakeTarget(sessionMailbox), {
    ...current, ...(existing ?? {}), mailboxId: sessionMailbox,
    driver: nextDriver,
    driverConfig: {
      ...(nextDriver === current.driver ? current.driverConfig : undefined),
      ...(existing?.driver === nextDriver ? existing.driverConfig : undefined),
      ...patch.driverConfig,
    },
    rotateAfterWaves: undefined, completedWaves: existing?.completedWaves ?? 0,
    wakeupPrefix: prefix ?? existing?.wakeupPrefix ?? current.wakeupPrefix,
    pendingWake: Boolean(existing?.pendingWake || hasQueued), attemptCount: existing?.attemptCount ?? 0,
    earliestWakeAt: existing?.earliestWakeAt ?? 0,
    lastWakeError: existing?.deliveryInDoubt ? existing.lastWakeError : (current.deliveryInDoubt ? current.lastWakeError : existing?.lastWakeError),
    deliveryInDoubt: Boolean(existing?.deliveryInDoubt || current.deliveryInDoubt) ? true : undefined,
  }));
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

function emptyState(): MailboxFileState {
  return {
    schema: MAILBOX_SCHEMA,
    sequence: 0,
    items: [],
    mailboxes: {},
    wakeTargets: {},
    threads: {},
  };
}

function loadState(raw: string): MailboxFileState {
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
  const sequence = Math.max(
    parsed.sequence,
    ...items.map(item => item.sequence + 1),
  );
  return {
    schema: MAILBOX_SCHEMA,
    sequence,
    items,
    mailboxes: mailboxes as Record<string, MailboxStateRecord>,
    wakeTargets: validatedWakeTargets,
    threads: normalizedThreads,
  };
}

function loadLegacyInbox(path: string): LegacyInboxItem[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(path, 'utf8'));
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

function cloneItem(item: MailboxItem): MailboxItem {
  return structuredClone(item);
}

function exactThreadRecord(
  state: MailboxFileState,
  messageId: string,
): ThreadRouteRecord | undefined {
  return state.threads[`inbound:${messageId}`] ?? state.threads[`outbound:${messageId}`];
}

function threadRecordsFor(
  state: MailboxFileState,
  threadId: string,
): ThreadRouteRecord[] {
  return Object.values(state.threads)
    .filter(record => record.threadId === threadId)
    .sort(compareThreadRecords);
}

function lookupThread(
  state: MailboxFileState,
  messageOrThreadId: string,
): ThreadRouteRecord | undefined {
  // A message id can exist on both loopback legs. Prefer the inbound leg because ask/reply/
  // settle address a message the caller received; fall back to its outbound leg otherwise.
  const exact = exactThreadRecord(state, messageOrThreadId);
  if (exact) {
    return exact;
  }
  return threadRecordsFor(state, messageOrThreadId).at(-1);
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
 * wake targets and thread reverse routes. Every mutation is a crash-safe fsync + rename under
 * an exclusive `${mailboxPath}.lock` directory, so the daemon and offline CLI cannot clobber
 * each other. In-process waiters are woken by events, never by polling.
 */
export class MailboxStore {
  readonly path: string;
  readonly lockDir: string;
  private readonly legacyInboxPath?: string;
  private readonly events = new EventEmitter();

  constructor(mailboxPath: string, options: MailboxStoreOptions = {}) {
    this.path = resolve(mailboxPath);
    this.legacyInboxPath = options.legacyInboxPath
      ? resolve(options.legacyInboxPath)
      : undefined;
    this.lockDir = resolve(options.lockDir ?? `${this.path}.lock`);
    this.events.setMaxListeners(0);
    this.migrateLegacyInbox();
  }

  private migrateLegacyInbox(): void {
    const legacy = this.legacyInboxPath;
    if (!legacy || legacy === this.path || existsSync(this.path) || !existsSync(legacy)) {
      return;
    }
    this.withFileLock(() => {
      if (existsSync(this.path) || !existsSync(legacy)) {
        return;
      }
      const state = emptyState();
      for (const legacyItem of loadLegacyInbox(legacy)) {
        state.items.push(migrateLegacyItem(legacyItem, state.sequence));
        state.sequence += 1;
      }
      this.save(state);
      renameSync(legacy, `${legacy}.migrated`);
    });
  }

  private withFileLock<T>(operation: () => T): T {
    return withOwnedLock(this.lockDir, 'Mailbox store', operation);
  }

  private read(): MailboxFileState {
    let raw: string;
    try {
      raw = readFileSync(this.path, 'utf8');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        return emptyState();
      }
      throw error;
    }
    return loadState(raw);
  }

  private save(state: MailboxFileState): void {
    writeFileAtomically(this.path, JSON.stringify(state));
  }

  private mutate<T>(operation: (state: MailboxFileState) => T): T {
    return this.withFileLock(() => {
      const state = this.read();
      const result = operation(state);
      this.save(state);
      return result;
    });
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
    const result = this.mutate(state => {
      const inserted: string[] = [];
      const duplicateIds: string[] = [];
      const mailboxIds = new Set<string>();
      const configureWakeTargets = new Set<string>();
      const envelopeMailboxes = new Map<string, string>();
      let claimedSpawn = false;
      for (const envelope of batch) {
        if (!isRecord(envelope) || !isNonEmptyString(envelope.id)) {
          throw new Error('Invalid envelope: missing id');
        }
        const owner = canInheritThreadMailbox(envelope)
          ? this.threadLocalMailboxFromState(state, envelopeThreadId(envelope), envelope.networkId, envelope.id)
          : undefined;
        const mailboxId = owner ?? explicitMailbox ?? resolveMailboxId(envelope);
        mailboxIds.add(mailboxId);
        envelopeMailboxes.set(envelope.id, mailboxId);
        const duplicate = state.items.find(item => item.mailboxId === mailboxId && item.id === envelope.id);
        const mayConfigureWakeTarget = options.proposedSpawn
          ? owner === undefined && !duplicate
          : owner === undefined || owner === explicitMailbox;
        if (mayConfigureWakeTarget) configureWakeTargets.add(mailboxId);
        if (duplicate) {
          const inbound = state.threads[threadKey({ direction: 'inbound', messageId: envelope.id })];
          if (!inbound || inbound.mailboxCommitted === false) {
            this.upsertInboundThread(state, duplicate.signedEnvelope ?? duplicate.envelope, mailboxId, now, true);
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
        state.items.push({
          id: cloned.id,
          mailboxId,
          status: 'queued',
          receivedAt: now,
          ...(hint ? { wakeupHint: hint } : {}),
          sequence: state.sequence,
          envelope: cloned,
          ...(signedEnvelope ? { signedEnvelope } : {}),
        });
        state.sequence += 1;
        conversationalByMailbox.set(mailboxId,
          (conversationalByMailbox.get(mailboxId) ?? true) &&
          (['question', 'reply', 'settle'].includes(envelope.semantic?.type ?? '') ||
            Boolean(envelope.semantic?.threadId &&
              threadRecordsFor(state, envelope.semantic.threadId)
                .some(record => record.networkId === envelope.networkId))),
        );
        this.upsertInboundThread(state, signedEnvelope ?? cloned, mailboxId, now, true);
        claimedSpawn ||= options.proposedSpawn === true && owner === undefined;
        inserted.push(cloned.id);
        insertedByMailbox.set(mailboxId, (insertedByMailbox.get(mailboxId) ?? 0) + 1);
      }
      for (const mailboxId of mailboxIds) {
        const previous = state.wakeTargets[mailboxId];
        const current = previous ?? defaultWakeTarget(mailboxId);
        if (options.wakeTarget && configureWakeTargets.has(mailboxId)) {
          state.wakeTargets[mailboxId] = validateWakeTarget(
            mergeWakeTarget(current, options.wakeTarget),
          );
        }
        if ((insertedByMailbox.get(mailboxId) ?? 0) > 0) {
          const target = state.wakeTargets[mailboxId];
          if (target) {
            state.wakeTargets[mailboxId] = validateWakeTarget({
              ...target,
              pendingWake: true,
              earliestWakeAt: conversationalByMailbox.get(mailboxId) && target.attemptCount === 0
                ? nowMs
                : target.attemptCount > 0
                  ? Math.max(previous?.earliestWakeAt ?? 0, nowMs + target.coalesceMs)
                  : nowMs + target.coalesceMs,
            });
          }
        }
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
    if (options.peek === true) {
      return this.peek(mailboxId);
    }
    return this.mutate(state => {
      const record = state.mailboxes[mailboxId];
      if (record?.outstanding) {
        const queued = state.items
          .filter(item => item.mailboxId === mailboxId && item.status === 'queued')
          .sort(compareOldestFirst);
        const lease: DeliveryLease = {
          ...record.outstanding,
          wakeCount: record.outstanding.wakeCount + 1,
        };
        state.mailboxes[mailboxId] = { ...record, outstanding: lease };
        const items = state.items
          .filter(
            item =>
              item.mailboxId === mailboxId &&
              item.status === 'leased' &&
              item.leaseId === lease.leaseId,
          )
          .sort(compareOldestFirst);
        return {
          mailboxId,
          deliveryId: lease.leaseId,
          redelivered: true,
          newerQueuedCount: queued.length,
          maxQueuedSequence: queued.reduce((max, item) => Math.max(max, item.sequence), -1),
          ...(queued.length ? { latestQueued: cloneItem(queued.at(-1)!) } : {}),
          items: items.map(cloneItem),
        };
      }
      const queued = state.items
        .filter(item => item.mailboxId === mailboxId && item.status === 'queued')
        .sort(compareOldestFirst);
      if (!queued.length) {
        return {
          mailboxId,
          deliveryId: null,
          redelivered: false,
          newerQueuedCount: 0,
          items: [],
        };
      }
      const leaseId = mintLeaseId(state);
      const leasedAt = new Date().toISOString();
      const leasedIds = new Set(queued.map(item => item.id));
      state.items = state.items.map(item =>
        item.mailboxId === mailboxId && item.status === 'queued' && leasedIds.has(item.id)
          ? { ...item, status: 'leased', leaseId, leasedAt }
          : item,
      );
      state.mailboxes[mailboxId] = {
        mailboxId,
        ackedLeaseIds: record?.ackedLeaseIds ?? [],
        outstanding: {
          leaseId,
          mailboxId,
          itemIds: queued.map(item => item.id),
          createdAt: leasedAt,
          wakeCount: 0,
        },
      };
      const target = state.wakeTargets[mailboxId];
      if (target) {
        state.wakeTargets[mailboxId] = { ...target, pendingWake: false };
      }
      return {
        mailboxId,
        deliveryId: leaseId,
        redelivered: false,
        newerQueuedCount: 0,
        items: state.items
          .filter(
            item =>
              item.mailboxId === mailboxId &&
              item.status === 'leased' &&
              item.leaseId === leaseId,
          )
          .sort(compareOldestFirst)
          .map(cloneItem),
      };
    });
  }

  private peek(mailboxId: string): MailboxCheckResult {
    const state = this.read();
    const record = state.mailboxes[mailboxId];
    if (record?.outstanding) {
      const queued = state.items
        .filter(item => item.mailboxId === mailboxId && item.status === 'queued')
        .sort(compareOldestFirst);
      return {
        mailboxId,
        deliveryId: record.outstanding.leaseId,
        redelivered: true,
        newerQueuedCount: queued.length,
        maxQueuedSequence: queued.reduce((max, item) => Math.max(max, item.sequence), -1),
        ...(queued.length ? { latestQueued: cloneItem(queued.at(-1)!) } : {}),
        items: state.items
          .filter(
            item =>
              item.mailboxId === mailboxId &&
              item.status === 'leased' &&
              item.leaseId === record.outstanding!.leaseId,
          )
          .sort(compareOldestFirst)
          .map(cloneItem),
      };
    }
    return {
      mailboxId,
      deliveryId: null,
      redelivered: false,
      newerQueuedCount: 0,
      items: state.items
        .filter(item => item.mailboxId === mailboxId && item.status === 'queued')
        .sort(compareOldestFirst)
        .map(cloneItem),
    };
  }

  resolveLeaseMailbox(leaseId: string): string | undefined {
    const state = this.read();
    for (const [candidate, record] of Object.entries(state.mailboxes)) {
      if (record?.outstanding?.leaseId === leaseId || record?.ackedLeaseIds.includes(leaseId)) return candidate;
    }
    return undefined;
  }

  /**
   * Read-only preflight for a caller-supplied `--ack` lease. It mirrors `ack()`'s lookup
   * but never mutates the mailbox, so a command that later fails (or sends nothing) cannot
   * burn the lease that proves the caller still owns the inbound batch.
   */
  assertAckableLease(leaseId: string, mailboxId?: string): void {
    const requestedMailbox =
      mailboxId === undefined ? undefined : normalizeMailboxId(mailboxId);
    const state = this.read();
    const candidates = requestedMailbox ? [requestedMailbox] : Object.keys(state.mailboxes);
    for (const candidate of candidates) {
      const record = state.mailboxes[candidate];
      if (!record) {
        continue;
      }
      if (
        record.outstanding?.leaseId === leaseId ||
        record.ackedLeaseIds.includes(leaseId)
      ) {
        return;
      }
    }
    throw new Error(`Unknown or mismatched delivery lease: ${leaseId}`);
  }

  ack(leaseId: string, mailboxId?: string): MailboxAckResult {
    const requestedMailbox =
      mailboxId === undefined ? undefined : normalizeMailboxId(mailboxId);
    let wakeMailbox: string | undefined;
    const result = this.mutate(state => {
      const candidates = requestedMailbox ? [requestedMailbox] : Object.keys(state.mailboxes);
      for (const candidate of candidates) {
        const record = state.mailboxes[candidate];
        if (!record) {
          continue;
        }
        if (record.outstanding?.leaseId === leaseId) {
          const ackedAt = new Date().toISOString();
          const itemIds = new Set(record.outstanding.itemIds);
          state.items = state.items.map(item =>
            item.mailboxId === candidate && item.status === 'leased' && itemIds.has(item.id)
              ? { ...item, status: 'acked', ackedAt }
              : item,
          );
          state.mailboxes[candidate] = {
            mailboxId: candidate,
            ackedLeaseIds: [...record.ackedLeaseIds, leaseId].slice(
              -MAX_ACKED_LEASE_HISTORY,
            ),
          };
          const target = state.wakeTargets[candidate];
          if (target) {
            // A completed wave with mail already queued behind it is exactly the safe boundary
            // for session rotation: re-arm the target so the wakeup service can open the next
            // wave's session tab and point at it.
            const hasQueued = state.items.some(
              item => item.mailboxId === candidate && item.status === 'queued',
            );
            state.wakeTargets[candidate] = {
              ...target,
              completedWaves: target.completedWaves + 1,
              ...(hasQueued
                ? { pendingWake: true, earliestWakeAt: 0 }
                : {}),
            };
            if (hasQueued) {
              wakeMailbox = candidate;
            }
          }
          return {
            mailboxId: candidate,
            leaseId,
            itemIds: [...itemIds],
            alreadyAcknowledged: false,
          };
        }
        if (record.ackedLeaseIds.includes(leaseId)) {
          return {
            mailboxId: candidate,
            leaseId,
            itemIds: [],
            alreadyAcknowledged: true,
          };
        }
      }
      throw new Error(`Unknown or mismatched delivery lease: ${leaseId}`);
    });
    if (wakeMailbox !== undefined) {
      this.events.emit('target', wakeMailbox);
    }
    return result;
  }

  hasConfirmedEnvelope(networkId: string, senderNodeId: string, entityId: string, minRevision: number): boolean {
    return this.read().items.some(item => {
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
    const state = this.read();
    const scoped = state.items.filter(
      item => mailboxId === undefined || item.mailboxId === mailboxId,
    );
    const visible = options.includeAcked === true
      ? scoped
      : scoped.filter(item => item.status !== 'acked');
    const ordered = [...visible].sort(compareNewestFirst);
    return {
      ...(mailboxId !== undefined ? { mailboxId } : {}),
      totalCount: scoped.length,
      queuedCount: scoped.filter(item => item.status === 'queued').length,
      leasedCount: scoped.filter(item => item.status === 'leased').length,
      ackedCount: scoped.filter(item => item.status === 'acked').length,
      items: (options.limit === undefined ? ordered : ordered.slice(0, options.limit)).map(
        cloneItem,
      ),
    };
  }

  async waitForMail(options: MailboxWaitOptions = {}): Promise<MailboxWaitResult> {
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
        this.events.off('mail', onMail);
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
          const result = this.check(checkOptions);
          if (result.items.length > 0) {
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
      this.events.on('mail', onMail);
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
  ): WakeTargetState {
    const id = normalizeMailboxId(mailboxId);
    const merged = this.mutate(state => {
      const current = state.wakeTargets[id] ?? defaultWakeTarget(id);
      const next = validateWakeTarget(mergeWakeTarget(current, patch));
      state.wakeTargets[id] = next;
      return structuredClone(next);
    });
    if (merged.pendingWake) {
      this.events.emit('target', id);
    }
    return merged;
  }

  /**
   * Fence before an external delivery so a crash or an unknown outcome cannot silently replay it.
   * Only `completeWakeDelivery` (confirmed receipt) or an explicit listener patch clears the fence.
   */
  beginWakeDelivery(mailboxId: string, expectedRevision?: number): WakeTargetState | undefined {
    const id = normalizeMailboxId(mailboxId);
    return this.mutate(state => {
      const current = state.wakeTargets[id] ?? defaultWakeTarget(id);
      if (expectedRevision !== undefined && (current.bindingRevision ?? 0) !== expectedRevision) return undefined;
      if (current.deliveryInDoubt) {
        throw new Error(`Dealer wake delivery requires reconciliation: ${id}`);
      }
      const next = validateWakeTarget(mergeWakeTarget(current, {
        deliveryInDoubt: true, pendingWake: false,
        lastWakeError: 'External wake delivery outcome is not yet confirmed',
      }));
      state.wakeTargets[id] = next;
      return structuredClone(next);
    });
  }

  /** Commit only against the binding that authorized this launch. */
  commitWakeLaunch(
    mailboxId: string,
    expectedRevision: number,
    buildPatch: (lockedCurrent: WakeTargetState) => Partial<WakeTargetState>,
  ): { committed: boolean; target: WakeTargetState } {
    const id = normalizeMailboxId(mailboxId);
    const result = this.mutate(state => {
      const current = state.wakeTargets[id] ?? defaultWakeTarget(id);
      const committed = (current.bindingRevision ?? 0) === expectedRevision;
      const patch = committed ? buildPatch(structuredClone(current)) : { attemptCount: 0, earliestWakeAt: 0 };
      let next = validateWakeTarget(mergeWakeTarget(current, {
        ...patch, deliveryInDoubt: undefined, pendingWake: true, lastWakeError: undefined,
      }));
      if (committed) next = { ...next, bindingRevision: Math.max(next.bindingRevision ?? 0, expectedRevision + 1) };
      state.wakeTargets[id] = next;
      return { committed, target: structuredClone(next) };
    });
    if (!result.committed) this.events.emit('target', id);
    return result;
  }

  /** Settle a retryable attempt only against the binding that authorized it. */
  deferWakeAttempt(mailboxId: string, expectedRevision: number, error: unknown, now = Date.now(), patch: Partial<WakeTargetState> = {}): { superseded: boolean; target: WakeTargetState } {
    const id = normalizeMailboxId(mailboxId);
    const result = this.mutate(state => {
      const current = state.wakeTargets[id] ?? defaultWakeTarget(id);
      const superseded = (current.bindingRevision ?? 0) !== expectedRevision;
      const attemptCount = superseded ? 0 : current.attemptCount + 1;
      const next = validateWakeTarget(mergeWakeTarget(current, {
        ...(superseded ? {} : patch), pendingWake: true, deliveryInDoubt: undefined, attemptCount,
        earliestWakeAt: superseded ? 0 : now + wakeupBackoffDelay(attemptCount),
        lastWakeError: superseded ? undefined : String(error),
      }));
      state.wakeTargets[id] = next;
      return { superseded, target: structuredClone(next) };
    });
    if (result.superseded) this.events.emit('target', id);
    return result;
  }

  retainPendingWakeIfCurrent(mailboxId: string, expectedRevision: number): WakeTargetState {
    const id = normalizeMailboxId(mailboxId);
    const next = this.mutate(state => {
      const current = state.wakeTargets[id] ?? defaultWakeTarget(id);
      const superseded = (current.bindingRevision ?? 0) !== expectedRevision;
      const target = validateWakeTarget(mergeWakeTarget(current, {
        pendingWake: true, ...(superseded ? { attemptCount: 0, earliestWakeAt: 0 } : {}),
      }));
      state.wakeTargets[id] = target;
      return structuredClone(target);
    });
    this.events.emit('target', id);
    return next;
  }

  disarmWakeTargetIfCurrent(mailboxId: string, expectedRevision: number): { superseded: boolean; target: WakeTargetState } {
    const id = normalizeMailboxId(mailboxId);
    const result = this.mutate(state => {
      const current = state.wakeTargets[id] ?? defaultWakeTarget(id);
      const superseded = (current.bindingRevision ?? 0) !== expectedRevision;
      const next = validateWakeTarget(mergeWakeTarget(current, superseded ? { pendingWake: true } : {
        pendingWake: false, attemptCount: 0, earliestWakeAt: 0, lastWakeError: undefined,
      }));
      state.wakeTargets[id] = next;
      return { superseded, target: structuredClone(next) };
    });
    if (result.superseded) this.events.emit('target', id);
    return result;
  }

  updateWakeDriverConfigIfCurrent(mailboxId: string, expectedRevision: number, driverConfig: Record<string, unknown>): { superseded: boolean; target: WakeTargetState } {
    const id = normalizeMailboxId(mailboxId);
    return this.mutate(state => {
      const current = state.wakeTargets[id] ?? defaultWakeTarget(id);
      if ((current.bindingRevision ?? 0) !== expectedRevision) return { superseded: true, target: structuredClone(current) };
      const next = validateWakeTarget(mergeWakeTarget(current, { driverConfig }));
      state.wakeTargets[id] = next;
      return { superseded: false, target: structuredClone(next) };
    });
  }

  /** Keep an unconfirmed external outcome fenced; later mail, forced flushes and restarts never replay it. */
  parkWakeDelivery(mailboxId: string, error: unknown): WakeTargetState {
    const id = normalizeMailboxId(mailboxId);
    return this.mutate(state => {
      const current = state.wakeTargets[id] ?? defaultWakeTarget(id);
      const next = validateWakeTarget(mergeWakeTarget(current, {
        deliveryInDoubt: true, pendingWake: false, earliestWakeAt: 0,
        attemptCount: current.attemptCount + 1, lastWakeError: String(error),
      }));
      state.wakeTargets[id] = next;
      return structuredClone(next);
    });
  }

  /** Settle only the observed wave; messages arriving during delivery retain their wake. */
  completeWakeDelivery(
    mailboxId: string,
    deliveredSequence: number,
    patch: Partial<WakeTargetState> = {},
    pinnedSession?: SessionMailboxPatch & { sessionMailboxId: string },
    expectedRevision?: number,
  ): WakeTargetState & { superseded?: boolean } {
    const id = normalizeMailboxId(mailboxId);
    let pinnedWakeMailbox: string | undefined;
    const completed = this.mutate(state => {
      const pendingWake = state.items.some(item =>
        item.mailboxId === id && item.status === 'queued' && item.sequence > deliveredSequence,
      );
      const current = state.wakeTargets[id] ?? defaultWakeTarget(id);
      if (expectedRevision !== undefined && (current.bindingRevision ?? 0) !== expectedRevision) {
        const hasQueued = state.items.some(item => item.mailboxId === id && item.status === 'queued');
        const next = validateWakeTarget(mergeWakeTarget(current, {
          deliveryInDoubt: undefined, lastWakeError: undefined,
          pendingWake: Boolean(current.pendingWake || hasQueued), attemptCount: 0, earliestWakeAt: 0,
        }));
        state.wakeTargets[id] = next;
        return Object.assign(structuredClone(next), { superseded: true });
      }

      const next = validateWakeTarget(mergeWakeTarget(current, {
        ...patch, pendingWake, attemptCount: 0,
        earliestWakeAt: pendingWake ? current.earliestWakeAt : 0, lastWakeError: undefined,
        deliveryInDoubt: undefined,
      }));
      state.wakeTargets[id] = next;
      if (pinnedSession && current.rotateAfterWaves !== undefined) {
        const termMailbox = pinSessionTargetInState(state, id, pinnedSession.sessionMailboxId, pinnedSession);
        if (state.wakeTargets[termMailbox]?.pendingWake && !state.wakeTargets[termMailbox]?.deliveryInDoubt) pinnedWakeMailbox = termMailbox;
        const delivered = state.items.filter(item => item.mailboxId === id && item.sequence <= deliveredSequence);
        const keys = new Set(delivered.flatMap(item => [item.envelope.semantic?.threadId, item.envelope.id].filter((v): v is string => typeof v === 'string')));
        const updatedAt = new Date().toISOString();
        for (const [key, record] of Object.entries(state.threads)) {
          if (record.mailboxCommitted !== false && record.localMailbox === id &&
              (keys.has(key) || keys.has(record.threadId) || keys.has(record.messageId))) {
            state.threads[key] = {
              ...record, localMailbox: termMailbox, updatedAt,
              ...(record.direction === 'inbound' ? { mailboxCommitted: true } : {}),
            };
          }
        }
      }
      return structuredClone(next);
    });
    if (completed.pendingWake) this.events.emit('target', id);
    if (pinnedWakeMailbox) this.events.emit('target', pinnedWakeMailbox);
    return completed;
  }

  pinSessionMailbox(mailboxId: string, sessionMailboxId: string, sessionPatch?: SessionMailboxPatch): string {
    const id = normalizeMailboxId(mailboxId);
    let notify = false;
    const pinned = this.mutate(state => {
      const mailbox = pinSessionTargetInState(state, id, sessionMailboxId, sessionPatch);
      notify = Boolean(state.wakeTargets[mailbox]?.pendingWake && !state.wakeTargets[mailbox]?.deliveryInDoubt);
      return mailbox;
    });
    if (notify) this.events.emit('target', pinned);
    return pinned;
  }

  getWakeTarget(mailboxId: string): WakeTargetState | undefined {
    const id = normalizeMailboxId(mailboxId);
    const target = this.read().wakeTargets[id];
    return target ? structuredClone(target) : undefined;
  }

  listWakeTargets(): WakeTargetState[] {
    return Object.values(this.read().wakeTargets).map(target => structuredClone(target));
  }

  /** Arrival notification for one mailbox; never emitted by read-only operations. */
  onMail(listener: (mailboxId: string) => void): Disposable {
    this.events.on('mail', listener);
    return { dispose: () => this.events.off('mail', listener) };
  }

  /** Notification that a wake target became pending (for example after a spawn). */
  onWakeTarget(listener: (mailboxId: string) => void): Disposable {
    this.events.on('target', listener);
    return { dispose: () => this.events.off('target', listener) };
  }

  listPendingWakeTargets(nowMs = Date.now()): WakeTargetState[] {
    if (typeof nowMs !== 'number' || !Number.isFinite(nowMs)) {
      throw new Error('Invalid wake target instant');
    }
    return Object.values(this.read().wakeTargets)
      .filter(target => target.pendingWake && !target.deliveryInDoubt && target.earliestWakeAt <= nowMs)
      .sort(
        (a, b) =>
          a.earliestWakeAt - b.earliestWakeAt || a.mailboxId.localeCompare(b.mailboxId),
      )
      .map(target => structuredClone(target));
  }

  private upsertThreadRecord(
    state: MailboxFileState,
    record: ThreadRouteRecord,
  ): ThreadRouteRecord {
    const key = threadKey(record);
    const existing = state.threads[key];
    if (record.mailboxCommitted === false && existing && existing.networkId === record.networkId &&
      existing.mailboxCommitted !== false) return existing;
    const now = record.updatedAt;
    let threadState = mergeThreadState(existing?.state ?? 'open', record.state);
    let threadSettlement = record.settlement ?? existing?.settlement;
    for (const [currentKey, current] of Object.entries(state.threads)) {
      if (current.threadId !== record.threadId || current.networkId !== record.networkId) continue;
      const nextSettlement = record.settlement ?? current.settlement;
      const nextState = mergeThreadState(current.state, record.state);
      state.threads[currentKey] = {
        ...current,
        state: nextState,
        ...(nextSettlement !== undefined ? { settlement: nextSettlement } : {}),
        updatedAt: now,
      };
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
    state.threads[key] = stored;
    return stored;
  }

  private upsertInboundThread(
    state: MailboxFileState,
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
    return this.upsertThreadRecord(state, {
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
    return this.mutate(state => structuredClone(
      this.upsertInboundThread(state, envelope, mailboxId, now, options.committed ?? true),
    ));
  }

  recordOutboundEnvelope(
    envelope: MessageEnvelope,
    peerNodeIds: readonly string[],
    localMailbox = 'default',
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
    const peers = [...new Set(peerNodeIds)];
    const now = new Date().toISOString();
    const threadId = envelope.semantic?.threadId ?? envelope.id;
    const settlement = settlementOf(envelope, now);
    return this.mutate(state => structuredClone(this.upsertThreadRecord(state, {
      threadId,
      messageId: envelope.id,
      envelope: structuredClone(envelope),
      direction: 'outbound',
      peerNodeId: peers[0] ?? '',
      peerNodeIds: peers,
      networkId: envelope.networkId,
      peerReplyMailbox: outboundPeerMailbox(envelope),
      localMailbox: local,
      traceId: envelope.traceId,
      kind: envelope.kind,
      subject: envelope.subject,
      state: stateForSemantic(envelope),
      ...(settlement ? { settlement } : {}),
      createdAt: now,
      updatedAt: now,
    })));
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
    return this.mutate(state => {
      const key = `outbound:${messageId}`;
      const removed = state.threads[key];
      if (removed === undefined) {
        if (state.threads[`inbound:${messageId}`] !== undefined) {
          throw new Error(`Cannot prune inbound thread record: ${messageId}`);
        }
        return undefined;
      }
      delete state.threads[key];
      const remaining = threadRecordsFor(state, removed.threadId);
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
        state.threads[threadKey(record)] = {
          ...rest,
          state: nextState,
          updatedAt: now,
          ...(nextSettlement !== undefined ? { settlement: nextSettlement } : {}),
        };
      }
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
    return this.read().items.filter(item => item.sequence > afterSequence)
      .sort((a, b) => a.sequence - b.sequence).map(cloneItem);
  }

  listThreads(options: { kind?: string; state?: ThreadRouteRecord['state']; limit?: number } = {}): ThreadSummaryRecord[] {
    if (options.state !== undefined && !THREAD_STATES.includes(options.state)) {
      throw new Error('Invalid thread state');
    }
    if (options.limit !== undefined && (!Number.isSafeInteger(options.limit) || options.limit < 1)) {
      throw new Error('Invalid thread limit');
    }
    const groups = new Map<string, ThreadRouteRecord[]>();
    for (const record of Object.values(this.read().threads)) {
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
    const found = lookupThread(this.read(), messageOrThreadId);
    return found ? structuredClone(found) : undefined;
  }

  getInboundThreadRecord(messageId: string, networkId: string): ThreadRouteRecord | undefined {
    const record = this.read().threads[threadKey({ direction: 'inbound', messageId })];
    return record?.networkId === networkId ? structuredClone(record) : undefined;
  }

  /** All directional records of a thread, oldest first; accepts a message id or thread id. */
  listThreadRecords(messageOrThreadId: string): ThreadRouteRecord[] {
    if (!isNonEmptyString(messageOrThreadId)) {
      throw new Error('Invalid thread reference');
    }
    const state = this.read();
    const exact = exactThreadRecord(state, messageOrThreadId);
    const threadId = exact?.threadId ?? messageOrThreadId;
    return threadRecordsFor(state, threadId).map(record => structuredClone(record));
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
    return this.threadLocalMailboxFromState(this.read(), threadId, networkId, options?.excludeInboundMessageId);
  }

  private threadLocalMailboxFromState(
    state: MailboxFileState,
    threadId?: string,
    networkId?: string,
    excludeInboundMessageId?: string,
  ): string | undefined {
    if (typeof threadId !== 'string' || threadId.length === 0) {
      return undefined;
    }
    const records = threadRecordsFor(state, threadId)
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
   * network's records: a colliding thread id can accumulate a newer leg on another network, and
   * that leg must not shadow the route recorded for the network being sent on.
   */
  resolveReplyTarget(
    messageOrThreadId: string,
    networkId?: string,
  ): ResolvedReplyTarget | undefined {
    if (!isNonEmptyString(messageOrThreadId)) {
      throw new Error('Invalid thread reference');
    }
    const onNetwork = (candidate: ThreadRouteRecord): boolean =>
      networkId === undefined || candidate.networkId === networkId;
    const state = this.read();
    const exactRecord = exactThreadRecord(state, messageOrThreadId);
    const exact = exactRecord !== undefined && onNetwork(exactRecord) ? exactRecord : undefined;
    const record = exact ?? threadRecordsFor(state, messageOrThreadId).filter(onNetwork).at(-1);
    if (!record) {
      return undefined;
    }
    // A thread reference must not inherit the route of the root we sent: when the thread was
    // started without an explicit target, its outbound leg records `default` as the peer mailbox.
    // Follow the conversation's newest inbound leg instead. A message we received keeps its own
    // recorded leg, which is where that sender asked us to reply.
    const exactLeg = exact !== undefined &&
        !(exact.direction === 'outbound' && exact.messageId === exact.threadId)
      ? exact
      : undefined;
    const records = threadRecordsFor(state, exact?.threadId ?? messageOrThreadId)
      .filter(candidate => candidate.networkId === record.networkId);
    const targetRecord = exactLeg !== undefined && recordedPeerMailbox(exactLeg) !== 'default'
      ? exactLeg
      : (records.findLast(candidate =>
          candidate.direction === 'inbound' && recordedPeerMailbox(candidate) !== 'default',
        ) ??
        records.findLast(candidate => recordedPeerMailbox(candidate) !== 'default') ??
        record);
    const localMailbox = exactLeg !== undefined && exactLeg.mailboxCommitted !== false && exactLeg.localMailbox !== 'default'
      ? exactLeg.localMailbox
      : this.threadLocalMailboxFromState(state, record.threadId, record.networkId) ?? 'default';
    return {
      threadId: record.threadId,
      messageId: record.messageId,
      peerNodeId: record.peerNodeId,
      networkId: record.networkId,
      targetMailbox: recordedPeerMailbox(targetRecord),
      localMailbox,
      traceId: record.traceId,
      kind: record.kind,
      subject: record.subject,
    };
  }
}

function mintLeaseId(state: MailboxFileState): string {
  const known = new Set(
    Object.values(state.mailboxes).flatMap(record => [
      ...(record.outstanding ? [record.outstanding.leaseId] : []),
      ...record.ackedLeaseIds,
    ]),
  );
  let leaseId: string;
  do {
    leaseId = `dlv_${randomBytes(6).toString('hex')}`;
  } while (known.has(leaseId));
  return leaseId;
}
