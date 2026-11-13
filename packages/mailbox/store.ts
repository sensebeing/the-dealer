import { randomBytes, randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { isValidMailboxId, validateSemanticHeader, verifyEnvelope } from '../core-types/index.ts';
import type { Disposable, MessageEnvelope, SettlementOutcome } from '../core-types/index.ts';
import { holdOwnedDirectory, isDeadProcessOwner, processOwnerRecord, readProcessOwner, releaseOwnedDirectory } from '../process-lock/index.ts';

export const MAILBOX_SCHEMA = 'the-dealer-mailbox/v1';
const LEGACY_INBOX_SCHEMA = 'the-dealer-inbox/v1';
const MAX_ACKED_LEASE_HISTORY = 256;
const MAX_WAKEUP_HINT_LENGTH = 120;
const DEFAULT_WAIT_TIMEOUT_MS = 30_000;
const MAX_WAIT_TIMEOUT_MS = 3_600_000;
const LOCK_RETRY_MS = 20;
const LOCK_TIMEOUT_MS = 2_000;
const STALE_LOCK_MS = 30_000;
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

export type WakeTargetMode = 'passive' | 'orca-run' | 'orca-terminal';

export interface WakeTargetState {
  readonly hostAgent?: 'codex' | 'claude' | 'claude-code' | 'antigravity' | 'chatgpt-dot' | 'grok-bot';
  readonly conversationMode?: 'spawn' | 'fixed';
  readonly agentProfile?: string;
  readonly targetBot?: string;
  readonly sessionId?: string;

  readonly mailboxId: string;
  readonly mode: WakeTargetMode;
  readonly orcaCommand?: string;
  readonly agent?: 'antigravity' | 'claude' | 'codex';
  readonly interactiveCommand?: string;
  readonly runId?: string;
  readonly worktreeId?: string;
  readonly workspacePath?: string;
  readonly terminalHandle?: string;
  readonly repoRoot?: string;
  readonly autoCreateWorkspace?: boolean;
  readonly wakeupPrefix?: string;
  readonly workspaceComment?: string;
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
  mailboxId?: string;
  wakeupHint?: string;
  wakeTarget?: Partial<WakeTargetState>;
}

export interface MailboxEnqueueResult {
  inserted: string[];
  duplicateIds: string[];
  mailboxIds: string[];
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
 * explicit payload mailbox, topic binding / executor channel, default.
 */
export interface MailboxTopicBinding {
  executor: string;
  mailboxId?: string;
  streamKey?: string;
}

export function resolveBoundMailboxId(kind: string, binding: MailboxTopicBinding): string {
  if (binding.mailboxId !== undefined) return normalizeMailboxId(binding.mailboxId);
  return binding.executor === 'orca-wave'
    ? normalizeMailboxId(`stream:${binding.streamKey ?? (kind.includes(':') ? kind.slice(kind.indexOf(':') + 1) : kind)}`) : 'default';
}

export function resolveMailboxId(
  envelope: MessageEnvelope,
  executorName?: string,
  binding?: MailboxTopicBinding,
  threadLocalMailbox?: string,
): string {
  const semantic = envelope.semantic;
  const payload = isRecord(envelope.payload)
    ? (envelope.payload as Record<string, unknown>)
    : undefined;
  const bindingKind = typeof payload?.streamKey === 'string'
    ? `${envelope.kind}:${payload.streamKey}` : envelope.kind;
  const control = ['question', 'reply', 'settle'].includes(semantic?.type ?? '');
  // An explicit session mailbox always owns its message, including control frames: a generic
  // topic binding must never rewrite a session-specific destination. `default` means unspecified.
  if (semantic?.targetMailbox !== undefined && semantic.targetMailbox !== 'default') {
    return normalizeMailboxId(semantic.targetMailbox, 'semantic targetMailbox');
  }
  // An established thread stays with the mailbox already handling it, so replies wake the
  // originating worktree/terminal session instead of opening work in the topic's generic binding.
  if (threadLocalMailbox !== undefined && threadLocalMailbox !== 'default') {
    return normalizeMailboxId(threadLocalMailbox, 'thread local mailbox');
  }
  // Control frames follow the topic binding only when the sender named no destination.
  if (binding && control && (semantic?.targetMailbox === undefined || semantic.targetMailbox === 'default')) {
    const bound = resolveBoundMailboxId(bindingKind, binding);
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
  if (binding && !control && !isNonEmptyString(payload?.executor)) {
    if (executorName === 'orca-wave' && isNonEmptyString(payload?.streamKey) && binding.mailboxId === undefined) {
      return normalizeMailboxId(`stream:${payload.streamKey}`);
    }
    return resolveBoundMailboxId(bindingKind, binding);
  }
  if (executorName === 'orca-wave') {
    return normalizeMailboxId(`stream:${String(payload?.streamKey ?? binding?.streamKey ?? envelope.kind)}`, 'stream mailbox');
  }
  if (executorName === 'orca-spawn') {
    return normalizeMailboxId(`task:${envelope.id}`, 'task mailbox');
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
    mode: 'passive',
    coalesceMs: 0,
    completedWaves: 0,
    pendingWake: false,
    earliestWakeAt: 0,
    attemptCount: 0,
  };
}

function mergeWakeTarget(
  current: WakeTargetState,
  patch: Partial<WakeTargetState> | undefined,
): WakeTargetState {
  if (!patch) {
    return current;
  }
  // Optional fields are cleared by passing them explicitly as undefined; a JSON round trip
  // cannot express that, so own-property presence is the only reliable signal.
  const optional = <K extends keyof WakeTargetState>(key: K): WakeTargetState[K] | undefined =>
    Object.hasOwn(patch, key) ? patch[key] : current[key];
  const hostAgent = optional('hostAgent');
  const conversationMode = optional('conversationMode');
  const agentProfile = optional('agentProfile');
  const targetBot = optional('targetBot');
  const sessionId = optional('sessionId');
  const agent = optional('agent');
  const interactiveCommand = optional('interactiveCommand');
  const orcaCommand = optional('orcaCommand');
  const runId = optional('runId');
  const worktreeId = optional('worktreeId');
  const workspacePath = optional('workspacePath');
  const terminalHandle = optional('terminalHandle');
  const repoRoot = optional('repoRoot');
  const autoCreateWorkspace = optional('autoCreateWorkspace');
  const wakeupPrefix = optional('wakeupPrefix');
  const workspaceComment = optional('workspaceComment');
  const rotateAfterWaves = optional('rotateAfterWaves');
  const lastWakeError = optional('lastWakeError');
  const deliveryInDoubt = optional('deliveryInDoubt');
  const nextCoalesceMs = patch.coalesceMs ?? current.coalesceMs;
  const nextAttemptCount = patch.attemptCount ?? current.attemptCount;
  let earliestWakeAt = patch.earliestWakeAt ?? current.earliestWakeAt;
  if (patch.earliestWakeAt === undefined && patch.coalesceMs !== undefined &&
      nextAttemptCount === 0 && current.earliestWakeAt > 0) {
    earliestWakeAt = nextCoalesceMs === 0 ? 0 : Math.min(current.earliestWakeAt, Date.now() + nextCoalesceMs);
  }
  return {
    mailboxId: current.mailboxId,
    mode: patch.mode ?? current.mode,
    coalesceMs: nextCoalesceMs,
    completedWaves: patch.completedWaves ?? current.completedWaves,
    pendingWake: patch.pendingWake ?? current.pendingWake,
    earliestWakeAt,
    attemptCount: nextAttemptCount,
    ...(hostAgent !== undefined ? { hostAgent } : {}),
    ...(conversationMode !== undefined ? { conversationMode } : {}),
    ...(agentProfile !== undefined ? { agentProfile } : {}),
    ...(targetBot !== undefined ? { targetBot } : {}),
    ...(sessionId !== undefined ? { sessionId } : {}),
    ...(agent !== undefined ? { agent } : {}),
    ...(interactiveCommand !== undefined ? { interactiveCommand } : {}),
    ...(orcaCommand !== undefined ? { orcaCommand } : {}),
    ...(runId !== undefined ? { runId } : {}),
    ...(worktreeId !== undefined ? { worktreeId } : {}),
    ...(workspacePath !== undefined ? { workspacePath } : {}),
    ...(terminalHandle !== undefined ? { terminalHandle } : {}),
    ...(repoRoot !== undefined ? { repoRoot } : {}),
    ...(autoCreateWorkspace !== undefined ? { autoCreateWorkspace } : {}),
    ...(wakeupPrefix !== undefined ? { wakeupPrefix } : {}),
    ...(workspaceComment !== undefined ? { workspaceComment } : {}),
    ...(rotateAfterWaves !== undefined ? { rotateAfterWaves } : {}),
    ...(lastWakeError !== undefined ? { lastWakeError } : {}),
    ...(deliveryInDoubt !== undefined ? { deliveryInDoubt } : {}),
  };
}

const WAKE_TARGET_MODES: readonly WakeTargetMode[] = [
  'passive',
  'orca-run',
  'orca-terminal',
];

function validateWakeTarget(value: unknown): WakeTargetState {
  if (!isRecord(value)) {
    throw invalidStore();
  }
  const target = value as unknown as WakeTargetState;
  if (target.deliveryInDoubt !== undefined && typeof target.deliveryInDoubt !== 'boolean') throw invalidStore();
  if (target.hostAgent !== undefined && !['codex', 'claude', 'claude-code', 'antigravity', 'chatgpt-dot', 'grok-bot'].includes(target.hostAgent)) throw invalidStore();
  if (target.conversationMode !== undefined && !['spawn', 'fixed'].includes(target.conversationMode)) throw invalidStore();
  if (target.agent !== undefined && !['antigravity', 'claude', 'codex'].includes(target.agent)) throw invalidStore();
  if (!isValidMailboxId(target.mailboxId) || !WAKE_TARGET_MODES.includes(target.mode)) {
    throw invalidStore();
  }
  if (typeof target.coalesceMs !== 'number' || !Number.isFinite(target.coalesceMs) || target.coalesceMs < 0) {
    throw invalidStore();
  }
  if (!isNonNegativeInteger(target.completedWaves) || !isNonNegativeInteger(target.attemptCount)) {
    throw invalidStore();
  }
  if (typeof target.pendingWake !== 'boolean') {
    throw invalidStore();
  }
  if (typeof target.earliestWakeAt !== 'number' || !Number.isFinite(target.earliestWakeAt)) {
    throw invalidStore();
  }
  if (
    target.rotateAfterWaves !== undefined &&
    (!isNonNegativeInteger(target.rotateAfterWaves) || target.rotateAfterWaves === 0)
  ) {
    throw invalidStore();
  }
  for (const field of [
    'agentProfile',
    'targetBot',
    'sessionId',
    'interactiveCommand',
    'orcaCommand',
    'runId',
    'worktreeId',
    'workspacePath',
    'terminalHandle',
    'workspaceComment',
    'repoRoot',
    'wakeupPrefix',
    'lastWakeError',
  ] as const) {
    if (target[field] !== undefined && typeof target[field] !== 'string') {
      throw invalidStore();
    }
  }
  if (target.autoCreateWorkspace !== undefined && typeof target.autoCreateWorkspace !== 'boolean') {
    throw invalidStore();
  }
  return target;
}

function pinRotatingTargetInState(state: MailboxFileState, id: string, rawHandle: string): string {
  const current = state.wakeTargets[id];
  const handle = rawHandle.trim();
  if (!handle || current?.mode !== 'orca-terminal' || current.rotateAfterWaves === undefined) return id;
  const termMailbox = `terminal:${handle}`;
  const existing = state.wakeTargets[termMailbox];
  const hasQueued = state.items.some(item => item.mailboxId === termMailbox && item.status === 'queued');
  const prefix = current.wakeupPrefix?.replace(new RegExp(`(--mailbox\\s+)${id.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?=\\s|$)`), `$1${termMailbox}`);
  state.wakeTargets[termMailbox] = validateWakeTarget({
    ...current, ...(existing ?? {}), mailboxId: termMailbox, mode: 'orca-terminal', terminalHandle: handle,
    rotateAfterWaves: undefined, completedWaves: existing?.completedWaves ?? 0,
    wakeupPrefix: prefix ?? existing?.wakeupPrefix ?? current.wakeupPrefix,
    pendingWake: Boolean(existing?.pendingWake || hasQueued), attemptCount: existing?.attemptCount ?? 0,
    earliestWakeAt: existing?.earliestWakeAt ?? 0, lastWakeError: existing?.lastWakeError,
    deliveryInDoubt: existing?.deliveryInDoubt,
  } as any);
  return termMailbox;
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
  if (!isRecord(parsed) || parsed.schema !== MAILBOX_SCHEMA) {
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
    if (validateWakeTarget(value).mailboxId !== key) {
      throw invalidStore();
    }
  }
  const normalizedThreads: Record<string, ThreadRouteRecord> = {};
  for (const [key, value] of Object.entries(threads)) {
    const record = validateThreadRecord(value);
    const expected = threadKey(record);
    // Records are always stored under directional keys; any other key is a corrupted store.
    if (key !== expected) {
      throw invalidStore();
    }
    normalizedThreads[expected] = record;
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
    wakeTargets: wakeTargets as Record<string, WakeTargetState>,
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
    mkdirSync(dirname(this.lockDir), { recursive: true, mode: 0o700 });
    const ownerPath = join(this.lockDir, 'owner.json');
    const deadline = Date.now() + LOCK_TIMEOUT_MS;
    const sleeper = new Int32Array(new SharedArrayBuffer(4));
    let acquired = false;
    while (!acquired) {
      try {
        mkdirSync(this.lockDir);
        acquired = true;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'EEXIST') {
          throw error;
        }
        this.clearStaleLock(ownerPath);
        if (Date.now() >= deadline) {
          throw new Error(`Mailbox store is locked: ${this.lockDir}`);
        }
        Atomics.wait(sleeper, 0, 0, LOCK_RETRY_MS);
      }
    }
    try {
      holdOwnedDirectory(this.lockDir);
      writeFileSync(
        ownerPath,
        JSON.stringify(processOwnerRecord()),
        { mode: 0o600 },
      );
      return operation();
    } finally {
      rmSync(this.lockDir, { recursive: true, force: true });
      releaseOwnedDirectory(this.lockDir);
    }
  }

  private clearStaleLock(ownerPath: string): void {
    const owner = readProcessOwner(ownerPath);
    if (owner !== undefined) {
      // A same-PID owner is reclaimed only when its record proves the writer was a
      // previous incarnation of this container; a live or unprovable owner keeps it.
      if (isDeadProcessOwner(owner, this.lockDir)) {
        rmSync(this.lockDir, { recursive: true, force: true });
      }
      return;
    }
    try {
      if (Date.now() - statSync(this.lockDir).mtimeMs > STALE_LOCK_MS) {
        rmSync(this.lockDir, { recursive: true, force: true });
      }
    } catch {
      // The lock disappeared while we inspected it.
    }
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
    mkdirSync(dirname(this.path), { recursive: true, mode: 0o700 });
    const temporary = `${this.path}.${randomUUID()}.tmp`;
    try {
      const fd = openSync(temporary, 'wx', 0o600);
      try {
        writeFileSync(fd, JSON.stringify(state));
        fsyncSync(fd);
      } finally {
        closeSync(fd);
      }
      renameSync(temporary, this.path);
      this.syncDirectory(dirname(this.path));
    } finally {
      rmSync(temporary, { force: true });
    }
  }

  /**
   * Persist the rename itself. Some platforms/filesystems reject fsync on a read-only directory
   * descriptor; those are benign, while any other I/O failure must still surface.
   */
  private syncDirectory(directory: string): void {
    let fd: number | undefined;
    try {
      fd = openSync(directory, 'r');
      fsyncSync(fd);
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== 'EINVAL' && code !== 'EBADF' && code !== 'EPERM') {
        throw error;
      }
    } finally {
      if (fd !== undefined) {
        closeSync(fd);
      }
    }
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
    const now = new Date().toISOString();
    const nowMs = Date.now();
    const insertedByMailbox = new Map<string, number>();
    const conversationalByMailbox = new Map<string, boolean>();
    const result = this.mutate(state => {
      const inserted: string[] = [];
      const duplicateIds: string[] = [];
      const mailboxIds = new Set<string>();
      for (const envelope of batch) {
        if (!isRecord(envelope) || !isNonEmptyString(envelope.id)) {
          throw new Error('Invalid envelope: missing id');
        }
        const mailboxId = explicitMailbox ?? resolveMailboxId(
          envelope,
          'inbox',
          undefined,
          this.threadLocalMailboxFromState(
            state,
            envelope.semantic?.threadId,
            envelope.networkId,
          ),
        );
        mailboxIds.add(mailboxId);
        if (state.items.some(item => item.mailboxId === mailboxId && item.id === envelope.id)) {
          duplicateIds.push(envelope.id);
          continue;
        }
        const cloned = structuredClone(envelope);
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
        });
        state.sequence += 1;
        conversationalByMailbox.set(mailboxId,
          (conversationalByMailbox.get(mailboxId) ?? true) &&
          (['question', 'reply', 'settle'].includes(envelope.semantic?.type ?? '') ||
            Boolean(envelope.semantic?.threadId &&
              threadRecordsFor(state, envelope.semantic.threadId)
                .some(record => record.networkId === envelope.networkId))),
        );
        inserted.push(cloned.id);
        insertedByMailbox.set(mailboxId, (insertedByMailbox.get(mailboxId) ?? 0) + 1);
      }
      for (const mailboxId of mailboxIds) {
        const previous = state.wakeTargets[mailboxId];
        const current = previous ?? defaultWakeTarget(mailboxId);
        if (options.wakeTarget) {
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
      return { inserted, duplicateIds, mailboxIds: [...mailboxIds] };
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
    return this.read().items.some(({ envelope }) => {
      const revision = envelope.payload?.claimVersion ?? envelope.payload?.revision ?? 0;
      return verifyEnvelope(envelope) && envelope.networkId === networkId &&
        envelope.from.nodeId === senderNodeId && (envelope.idempotencyKey ?? envelope.id) === entityId &&
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
  beginWakeDelivery(mailboxId: string): WakeTargetState {
    const id = normalizeMailboxId(mailboxId);
    return this.mutate(state => {
      const current = state.wakeTargets[id] ?? defaultWakeTarget(id);
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
  ): WakeTargetState {
    const id = normalizeMailboxId(mailboxId);
    let pinnedWakeMailbox: string | undefined;
    const completed = this.mutate(state => {
      const pendingWake = state.items.some(item =>
        item.mailboxId === id && item.status === 'queued' && item.sequence > deliveredSequence,
      );
      const current = state.wakeTargets[id] ?? defaultWakeTarget(id);
      const next = validateWakeTarget(mergeWakeTarget(current, {
        ...patch, pendingWake, attemptCount: 0,
        earliestWakeAt: pendingWake ? current.earliestWakeAt : 0, lastWakeError: undefined,
        deliveryInDoubt: undefined,
      }));
      state.wakeTargets[id] = next;
      if (current.mode === 'orca-terminal' && current.rotateAfterWaves !== undefined &&
          typeof next.terminalHandle === 'string' && next.terminalHandle.trim()) {
        const termMailbox = pinRotatingTargetInState(state, id, next.terminalHandle);
        if (state.wakeTargets[termMailbox]?.pendingWake && !state.wakeTargets[termMailbox]?.deliveryInDoubt) pinnedWakeMailbox = termMailbox;
        const delivered = state.items.filter(item => item.mailboxId === id && item.sequence <= deliveredSequence);
        const keys = new Set(delivered.flatMap(item => [item.envelope.semantic?.threadId, item.envelope.id].filter((v): v is string => typeof v === 'string')));
        const updatedAt = new Date().toISOString();
        for (const [key, record] of Object.entries(state.threads)) {
          if (record.localMailbox === id && (keys.has(key) || keys.has(record.threadId) || keys.has(record.messageId))) {
            state.threads[key] = { ...record, localMailbox: termMailbox, updatedAt };
          }
        }
      }
      return structuredClone(next);
    });
    if (completed.pendingWake) this.events.emit('target', id);
    if (pinnedWakeMailbox) this.events.emit('target', pinnedWakeMailbox);
    return completed;
  }

  pinRotatingSessionMailbox(mailboxId: string, terminalHandle?: string): string {
    const id = normalizeMailboxId(mailboxId);
    if (typeof terminalHandle !== 'string' || !terminalHandle.trim()) return id;
    let notify = false;
    const pinned = this.mutate(state => {
      const mailbox = pinRotatingTargetInState(state, id, terminalHandle);
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

  private writeThreadRecord(
    record: ThreadRouteRecord,
    settlement: ThreadRouteRecord['settlement'] | undefined,
  ): ThreadRouteRecord {
    return this.mutate(state => {
      const key = threadKey(record);
      const existing = state.threads[key];
      const now = record.updatedAt;
      let threadState = mergeThreadState(existing?.state ?? 'open', record.state);
      let threadSettlement = settlement ?? existing?.settlement;
      for (const [currentKey, current] of Object.entries(state.threads)) {
        if (current.threadId !== record.threadId) {
          continue;
        }
        const nextSettlement = settlement ?? current.settlement;
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
        updatedAt: now,
      };
      state.threads[key] = stored;
      return structuredClone(stored);
    });
  }

  recordInboundEnvelope(
    envelope: MessageEnvelope,
    localMailbox: string,
  ): ThreadRouteRecord {
    const local = normalizeMailboxId(localMailbox, 'local mailbox');
    const rotatingTarget = this.read().wakeTargets[local];
    const effectiveLocal = rotatingTarget?.mode === 'orca-terminal' && rotatingTarget.rotateAfterWaves !== undefined &&
      typeof rotatingTarget.terminalHandle === 'string' && rotatingTarget.terminalHandle.trim() && !rotatingTarget.pendingWake
      ? `terminal:${rotatingTarget.terminalHandle.trim()}` : local;
    if (!isRecord(envelope) || !isNonEmptyString(envelope.id)) {
      throw new Error('Invalid envelope: missing id');
    }
    if (envelope.semantic !== undefined) {
      validateSemanticHeader(envelope.semantic);
    }
    const now = new Date().toISOString();
    const threadId = envelope.semantic?.threadId ?? envelope.id;
    const peerReplyMailbox = normalizeMailboxId(
      envelope.semantic?.replyMailbox ?? 'default',
      'peer reply mailbox',
    );
    const settlement = settlementOf(envelope, now);
    return this.writeThreadRecord(
      {
        threadId,
        messageId: envelope.id,
        envelope: structuredClone(envelope),
        direction: 'inbound',
        peerNodeId: envelope.from.nodeId,
        peerNodeIds: [envelope.from.nodeId],
        networkId: envelope.networkId,
        peerReplyMailbox,
        localMailbox: effectiveLocal,
        traceId: envelope.traceId,
        kind: envelope.kind,
        subject: envelope.subject,
        state: stateForSemantic(envelope),
        ...(settlement ? { settlement } : {}),
        createdAt: now,
        updatedAt: now,
      },
      settlement,
    );
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
    return this.writeThreadRecord(
      {
        threadId,
        messageId: envelope.id,
        envelope: structuredClone(envelope),
        direction: 'outbound',
        peerNodeId: peers[0] ?? '',
        peerNodeIds: peers,
        networkId: envelope.networkId,
        // The outbound target, including an explicit payload mailbox, is where replies land.
        peerReplyMailbox: normalizeMailboxId(
          envelope.semantic?.targetMailbox ?? resolveMailboxId(envelope),
          'peer reply mailbox',
        ),
        localMailbox: local,
        traceId: envelope.traceId,
        kind: envelope.kind,
        subject: envelope.subject,
        state: stateForSemantic(envelope),
        ...(settlement ? { settlement } : {}),
        createdAt: now,
        updatedAt: now,
      },
      settlement,
    );
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
   */
  resolveThreadLocalMailbox(threadId?: string, networkId?: string): string | undefined {
    return this.threadLocalMailboxFromState(this.read(), threadId, networkId);
  }

  private threadLocalMailboxFromState(
    state: MailboxFileState,
    threadId?: string,
    networkId?: string,
  ): string | undefined {
    if (typeof threadId !== 'string' || threadId.length === 0) {
      return undefined;
    }
    const records = threadRecordsFor(state, threadId)
      .filter(record => networkId === undefined || record.networkId === networkId);
    for (let index = records.length - 1; index >= 0; index -= 1) {
      const localMailbox = records[index].localMailbox;
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
    const targetMailbox = exactLeg !== undefined && exactLeg.peerReplyMailbox !== 'default'
      ? exactLeg.peerReplyMailbox
      : (records.findLast(candidate =>
          candidate.direction === 'inbound' && candidate.peerReplyMailbox !== 'default',
        ) ??
        records.findLast(candidate => candidate.peerReplyMailbox !== 'default') ??
        record).peerReplyMailbox;
    const localMailbox = exactLeg !== undefined && exactLeg.localMailbox !== 'default'
      ? exactLeg.localMailbox
      : this.threadLocalMailboxFromState(state, record.threadId, record.networkId) ??
        record.localMailbox;
    return {
      threadId: record.threadId,
      messageId: record.messageId,
      peerNodeId: record.peerNodeId,
      networkId: record.networkId,
      targetMailbox,
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
