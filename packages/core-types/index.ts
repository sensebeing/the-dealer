import {
  createCipheriv,
  createDecipheriv,
  createHash,
  createHmac,
  createPrivateKey,
  createPublicKey,
  generateKeyPairSync,
  randomBytes,
  sign,
  verify,
} from 'node:crypto';

export interface Disposable {
  dispose(): void | Promise<void>;
}
export interface SourceRegistration extends Disposable {
  readonly started?: Promise<void>;
}
export type Result<T> = { ok: true; value: T } | { ok: false; error: Error };
export type AdmissionMode = 'open' | 'approval_required' | 'dnd' | 'closed';
export type DeliveryStage =
  | 'received'
  | 'pending_approval'
  | 'queued'
  | 'dispatching'
  | 'dispatched'
  | 'settled'
  | 'rejected'
  | 'failed';

export interface NetworkMountConfig {
  networkId: string;
  passphrase: string;
  bootstrapPeers?: readonly string[];
  admissionMode?: AdmissionMode;
  exposedSkills?: readonly string[];
  listenPort?: number;
  mDnsPort?: number;
}

export interface DerivedNetworkKeys {
  networkId: string;
  swarmTopicId: string;
  rendezvousPublicKey: string;
  rendezvousPrivateKeyPem: string;
  sealedKey: Uint8Array;
}

export interface BlobAttachment {
  name: string;
  mimeType: string;
  sizeBytes: number;
  blake3Hash: string;
  ticket: string;
}

export interface SkillCapability {
  id: string;
  description: string;
  tags?: readonly string[];
  acceptedTypes?: readonly string[];
}

export interface NodeCard {
  nodeId: string;
  networkId: string;
  revision: number;
  identity: { member: string; nodeName: string };
  admissionMode: AdmissionMode;
  capabilities: { summary: string; skills: readonly SkillCapability[] };
  runtimeState: {
    load: 'idle' | 'busy' | 'overloaded';
    activeTasks: number;
    availableAgents: readonly string[];
  };
  endpointAddr?: string;
  updatedAt: string;
  signature: string;
}

export type SemanticMessageType = 'message' | 'question' | 'reply' | 'settle';
export type SettlementOutcome = 'succeeded' | 'failed' | 'cancelled';

export interface SemanticHeader {
  readonly type: SemanticMessageType;
  readonly threadId: string;
  readonly targetMailbox?: string;
  readonly replyMailbox?: string;
  readonly outcome?: SettlementOutcome;
}

/** Emitter input: a new thread defaults its threadId to the generated envelope id. */
export interface SemanticHeaderInput {
  readonly type: SemanticMessageType;
  readonly threadId?: string;
  readonly targetMailbox?: string;
  readonly replyMailbox?: string;
  readonly outcome?: SettlementOutcome;
}

export interface MessageEnvelope<T = Record<string, unknown>> {
  id: string;
  networkId: string;
  traceId: string;
  replyToId?: string;
  idempotencyKey?: string;
  semantic?: SemanticHeader;
  from: { nodeId: string; member: string; nodeName: string };
  routing: {
    mode: 'unicast' | 'multicast' | 'broadcast' | 'auto';
    targets?: readonly string[];
    topic?: string;
    hint?: string;
  };
  kind: string;
  priority: 'urgent' | 'high' | 'normal' | 'low';
  subject: string;
  body: string;
  payload: T;
  attachments: readonly BlobAttachment[];
  createdAt: string;
  signature: string;
}

/** Shared sender/receiver CAS version; null means the supplied version is invalid. */
export function envelopeClaimVersion(envelope: Pick<MessageEnvelope, 'payload'>): number | null {
  const raw = envelope.payload?.claimVersion ?? envelope.payload?.revision ?? 0;
  return typeof raw === 'number' && Number.isSafeInteger(raw) && raw >= 0 ? raw : null;
}

/** Compare caller intent independently of the envelope id and its generated thread id. */
export function sameEnvelopeIntent(left: MessageEnvelope, right: MessageEnvelope): boolean {
  const intent = ({ id, kind, subject, body, routing, payload, priority, replyToId, semantic, attachments }: MessageEnvelope) => ({
    kind, subject, body, routing, payload, priority, replyToId, attachments,
    semantic: semantic?.threadId === id ? { ...semantic, threadId: undefined } : semantic,
  });
  return canonicalJson(intent(left)) === canonicalJson(intent(right));
}

export interface DeliveryReceipt {
  messageId: string;
  networkId: string;
  fromNodeId: string;
  stage: DeliveryStage;
  detail?: string;
  executionReceipt?: Record<string, unknown>;
  updatedAt: string;
  signature: string;
}

export const DEDUP_CAS_DUPLICATE_DETAIL = 'Duplicate or stale claim version';
export const DEDUP_CAS_AMBIGUOUS_DETAIL = 'Previous delivery outcome unknown; newer revision required';

type HandoffReceipt = {
  stage?: string;
  detail?: string;
  executionReceipt?: Record<string, unknown>;
};

/** A durable queued handoff: the event is durably owned, not merely buffered in memory. */
export function isDurableQueuedReceipt(receipt: HandoffReceipt | null | undefined): boolean {
  return receipt?.stage === 'queued' && receipt.executionReceipt?.durable === true;
}

/** The sender's durable outbox owns the obligation; poll sources may advance immediately. */
export function isDurableOutboxReceipt(receipt: HandoffReceipt | null | undefined): boolean {
  return isDurableQueuedReceipt(receipt) && receipt?.executionReceipt?.executor === 'outbox';
}

export function isDedupCasDuplicateReceipt(
  receipt: HandoffReceipt | null | undefined,
  options?: { allowQueued?: boolean },
): boolean {
  if (receipt?.stage !== 'rejected' || receipt.detail !== DEDUP_CAS_DUPLICATE_DETAIL) return false;
  if (receipt.executionReceipt?.durable !== true) return false;
  const handoff = receipt.executionReceipt.handoff;
  return handoff === 'settled' || (Boolean(options?.allowQueued) && handoff === 'queued');
}

export function isRetainedApprovalReceipt(receipt: HandoffReceipt | null | undefined): boolean {
  return receipt?.stage === 'rejected' && receipt.executionReceipt?.handoff === 'approval_pending';
}

/** A signed decision that must stop retries for this target and preserve its evidence. */
export function isTerminalDeliveryFailure(
  receipt: HandoffReceipt | null | undefined,
): boolean {
  // Live dispatches await their in-process promise; a returned dispatching receipt
  // is persisted interrupted execution, never an offline obligation to requeue.
  return (receipt?.stage === 'rejected' && !isRetainedApprovalReceipt(receipt)) ||
    receipt?.stage === 'failed' || receipt?.stage === 'dispatching';
}

export function isConfirmedHandoffReceipt(
  receipt: HandoffReceipt | null | undefined,
  options?: { allowQueued?: boolean },
): boolean {
  return receipt?.stage === 'dispatched' || receipt?.stage === 'settled' ||
    (Boolean(options?.allowQueued) && isDurableQueuedReceipt(receipt)) ||
    isDedupCasDuplicateReceipt(receipt, options) ||
    isDurableOutboxReceipt(receipt);
}

/** True when a signed failure or rejection proves no external effect was attempted. */
export function isRetryablePreEffectReceipt(receipt: HandoffReceipt | null | undefined): boolean {
  if (!receipt || isConfirmedHandoffReceipt(receipt, { allowQueued: true })) return false;
  if (receipt.stage === 'dispatching') return false;
  if (receipt.stage === 'failed') return receipt.executionReceipt?.handoff === 'unattempted';
  if (receipt.stage === 'rejected') {
    const handoff = receipt.executionReceipt?.handoff;
    return receipt.detail !== DEDUP_CAS_AMBIGUOUS_DETAIL && receipt.detail !== DEDUP_CAS_DUPLICATE_DETAIL &&
      (handoff === 'unattempted' || handoff === 'rejected');
  }
  return false;
}

/** A receipt without confirmed handoff, retained approval, or positive pre-effect proof. */
export function isUnknownEffectReceipt(receipt: HandoffReceipt | null | undefined): boolean {
  if (!receipt || isConfirmedHandoffReceipt(receipt, { allowQueued: true }) || isRetainedApprovalReceipt(receipt)) {
    return false;
  }
  return !isRetryablePreEffectReceipt(receipt);
}

/** Only the receipt for this exact envelope can release its provisional custody. */
export function isRelinquishedCustodyReceipt(receipt: HandoffReceipt | null | undefined, signature: unknown): boolean {
  return isRetryablePreEffectReceipt(receipt) && receipt?.executionReceipt?.custodyRelinquished === true &&
    typeof signature === 'string' && signature.length > 0 && receipt.executionReceipt.envelopeSignature === signature;
}

/** A bound receipt applies only to its envelope. Older receipts may omit the binding. */
export function receiptMatchesEnvelopeSignature(receipt: HandoffReceipt | undefined, signature: unknown): boolean {
  const bound = receipt?.executionReceipt?.envelopeSignature;
  return typeof signature !== 'string' || signature.length === 0 || typeof bound !== 'string' || bound === signature;
}

/** True when positive evidence rules out an attempt and no obligation remains. */
export function isUnattemptedOutboxItem(item: {
  status?: string;
  envelope?: { signature?: unknown };
  supersededById?: unknown;
  pendingNodeIds?: readonly string[];
  inDoubtNodeIds?: readonly string[];
  relaySpooledNodeIds?: readonly string[];
  approvalPendingNodeIds?: readonly string[];
  deliveredReceipts?: Readonly<Record<string, HandoffReceipt>>;
  failedNodeIds?: readonly string[];
  failedReceipts?: Readonly<Record<string, HandoffReceipt>>;
  unattemptedNodeIds?: readonly string[];
  unattemptedFailure?: boolean;
} | null | undefined): boolean {
  if (!item || item.status !== 'failed' || item.supersededById !== undefined) return false;
  if ((item.pendingNodeIds?.length ?? 0) > 0 || (item.inDoubtNodeIds?.length ?? 0) > 0) return false;
  if ((item.relaySpooledNodeIds?.length ?? 0) > 0 || (item.approvalPendingNodeIds?.length ?? 0) > 0) return false;
  if (Object.keys(item.deliveredReceipts ?? {}).length > 0) return false;
  const failedReceipts = item.failedReceipts ?? {};
  const failedIds = new Set([...(item.failedNodeIds ?? []), ...Object.keys(failedReceipts)]);
  if (failedIds.size === 0) return item.unattemptedFailure === true;
  const unattemptedLocal = new Set(item.unattemptedNodeIds ?? []);
  return [...failedIds].every(id => {
    const receipt = failedReceipts[id];
    return receipt
      ? isRetryablePreEffectReceipt(receipt) && receiptMatchesEnvelopeSignature(receipt, item.envelope?.signature)
      : unattemptedLocal.has(id);
  });
}

export interface SealedNetworkFrame {
  protocol: 'the-dealer-wire/v1';
  swarmTopicId: string;
  senderNodeId: string;
  nonce: string;
  ciphertext: string;
  authTag: string;
}

/**
 * Transport-level reachability failure: the peer endpoint is unknown, refused or timed
 * out and no live tunnel or relay seed could carry the frame. Emitters use this marker to
 * queue an envelope durably instead of treating every transport error as an offline peer.
 */
export class PeerUnreachableError extends Error {
  readonly code = 'peer_unreachable';
  readonly targetNodeId: string;

  constructor(targetNodeId: string, message?: string, options?: { cause?: unknown }) {
    super(message ?? `Peer unreachable: ${targetNodeId}`, options);
    this.name = 'PeerUnreachableError';
    this.targetNodeId = targetNodeId;
  }
}

/**
 * The frame was safely accepted by an always-on relay seed's store-and-forward spool while
 * the target peer was offline. The sender still queues its own outbox entry until a live
 * end-to-end receipt proves the target received it.
 */
export class OfflinePeerSpooledError extends Error {
  readonly code = 'offline_peer_spooled';
  readonly targetNodeId: string;
  readonly seedNodeId: string;
  readonly spoolId: string;

  constructor(targetNodeId: string, seedNodeId: string, spoolId: string) {
    super(`Peer ${targetNodeId} is offline; frame spooled on relay seed ${seedNodeId} as ${spoolId}`);
    this.name = 'OfflinePeerSpooledError';
    this.targetNodeId = targetNodeId;
    this.seedNodeId = seedNodeId;
    this.spoolId = spoolId;
  }
}

export function isPeerUnreachableError(error: unknown): error is PeerUnreachableError {
  return (
    error instanceof PeerUnreachableError ||
    (typeof error === 'object' &&
      error !== null &&
      (error as { code?: unknown }).code === 'peer_unreachable')
  );
}

export function isOfflinePeerSpooledError(error: unknown): error is OfflinePeerSpooledError {
  return (
    error instanceof OfflinePeerSpooledError ||
    (typeof error === 'object' &&
      error !== null &&
      (error as { code?: unknown }).code === 'offline_peer_spooled')
  );
}

/** True only for failures that mean the target peer is currently offline or unreachable. */
export function isPeerOfflineError(error: unknown): boolean {
  return isPeerUnreachableError(error) || isOfflinePeerSpooledError(error);
}

/** Router-owned explicit selector failure; the emitter queues it instead of re-parsing text. */
export class UnresolvedTargetError extends Error {
  readonly code = 'unresolved_target';
  readonly target: string;

  constructor(target: string) {
    super(`Unresolved explicit target: ${target}`);
    this.name = 'UnresolvedTargetError';
    this.target = target;
  }
}

export function isUnresolvedTargetError(error: unknown): error is UnresolvedTargetError {
  return (
    error instanceof UnresolvedTargetError ||
    (typeof error === 'object' &&
      error !== null &&
      (error as { code?: unknown }).code === 'unresolved_target')
  );
}

/**
 * A deterministic local failure (missing/corrupt attachment, oversized frame or a rejected
 * relay-spool protocol response). Retrying the same bytes can never succeed, so durable
 * redrive must settle it as failed instead of consuming the peer-unreachable retry path.
 */
export class LocalDeliveryError extends Error {
  readonly code = 'local_delivery_failure';

  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'LocalDeliveryError';
  }
}

export function isLocalDeliveryError(error: unknown): error is LocalDeliveryError {
  return (
    error instanceof LocalDeliveryError ||
    (typeof error === 'object' &&
      error !== null &&
      (error as { code?: unknown }).code === 'local_delivery_failure')
  );
}

/**
 * A send or response wait hit its deadline after the request may already have reached the
 * target. Emitters must not auto-retry these as ordinary offline targets without a direct
 * peer pulse or an explicit force, because the remote side may already have executed.
 */
export class DeliveryTimeoutError extends Error {
  readonly code = 'delivery_timeout';

  constructor(message = 'Delivery timed out', options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'DeliveryTimeoutError';
  }
}

export function isDeliveryTimeoutError(error: unknown): error is DeliveryTimeoutError {
  return (
    error instanceof DeliveryTimeoutError ||
    (typeof error === 'object' &&
      error !== null &&
      (error as { code?: unknown }).code === 'delivery_timeout')
  );
}

/** True for a timeout directly or anywhere in a wrapped peer-unreachable cause chain. */
export function isDeliveryTimeoutFailure(error: unknown): boolean {
  let current: unknown = error;
  for (let depth = 0; depth < 5 && current !== undefined && current !== null; depth += 1) {
    if (isDeliveryTimeoutError(current)) {
      return true;
    }
    current = (current as { cause?: unknown }).cause;
  }
  return false;
}

/** The send failed before a socket write or local dispatch began. */
export function markBeforeSendError<T extends Error>(error: T): T {
  return Object.assign(error, { beforeSend: true });
}

export function isBeforeSendError(error: unknown): boolean {
  return (error as { beforeSend?: unknown } | null)?.beforeSend === true &&
    !isDeliveryTimeoutFailure(error) && !isOfflinePeerSpooledError(error);
}

/** The mount disappeared between an outbox write and a redrive attempt: retry after remount. */
export class NetworkUnmountedError extends Error {
  readonly code = 'network_unmounted';
  readonly networkId: string;

  constructor(networkId: string) {
    super(`Network not mounted: ${networkId}`);
    this.name = 'NetworkUnmountedError';
    this.networkId = networkId;
  }
}

export function isNetworkUnmountedError(error: unknown): error is NetworkUnmountedError {
  return (
    error instanceof NetworkUnmountedError ||
    (typeof error === 'object' &&
      error !== null &&
      (error as { code?: unknown }).code === 'network_unmounted')
  );
}

export interface InboundFrameEvent {
  networkId: string;
  senderNodeId: string;
  mode: 'unicast' | 'broadcast';
  subTopic?: string;
  payload: unknown;
}

/** Which authenticated evidence proved a peer reachable: its own beacon, gossip, or a tunnel. */
export type PeerReachabilityVia = 'direct' | 'relay' | 'tunnel';

/**
 * A live reachability transition owned by transport, not by identity: it fires when a peer
 * becomes reachable again (fresh sealed beacon, gossiped beacon, or its inbound tunnel) even
 * though the peer's signed `NodeCard.revision` never changed across a sleep/wake cycle.
 */
export interface PeerReachableEvent {
  networkId: string;
  nodeId: string;
  via: PeerReachabilityVia;
}

export interface MountedNetworkHandle extends Disposable {
  readonly networkId: string;
  readonly swarmTopicId: string;
  readonly endpointAddr?: string;
}

export interface TransportService {
  readonly localNodeId: string;
  readonly endpointAddr?: string;
  mountNetwork(config: NetworkMountConfig): Promise<MountedNetworkHandle>;
  unmountNetwork(networkId: string): Promise<void>;
  listMountedNetworks(): readonly string[];
  registerPeerEndpoint?(networkId: string, peerNodeId: string, endpointAddr: string): void;
  /** Make a locally available file addressable by its BLAKE3 hash for blob fetches. */
  registerBlob?(hash: string, filePath: string): void;
  /**
   * True when transport holds an unexpired authenticated discovery beacon for
   * `peerNodeId`. Reachability is transport's alone: a verified `NodeCard` proves identity,
   * never liveness, and neither does a lingering half-open tunnel whose beacon has expired,
   * so durable obligations must consult this before resending.
   */
  isPeerReachable?(networkId: string, peerNodeId: string): boolean;
  sendUnicast(
    networkId: string,
    targetNodeId: string,
    frame: Uint8Array,
    opts?: {
      timeoutMs?: number;
      /** Caller-owned policy: allow the relay seed to store this frame while the target is offline. */
      spoolIfOffline?: boolean;
      /** Opaque, stable identifier for deduplicating repeated redrives of the same obligation. */
      spoolKey?: string;
      /** Content hashes the sender offers to an always-on relay seed for offline spooling. */
      spoolBlobHashes?: readonly string[];
    },
  ): Promise<Uint8Array>;
  sendMulticast(
    networkId: string,
    targetNodeIds: readonly string[],
    frame: Uint8Array,
  ): Promise<Map<string, Result<Uint8Array>>>;
  broadcast(
    networkId: string,
    subTopic: string,
    frame: Uint8Array,
    opts?: { timeoutMs?: number },
  ): Promise<void>;
  onInboundFrame?(
    handler: (frame: InboundFrameEvent) => Promise<Uint8Array | void> | Uint8Array | void,
  ): Disposable;
  shareFile(filePath: string): Promise<BlobAttachment>;
  fetchBlobToPath(ticket: string, destPath: string): Promise<void>;
}

export interface NodeService {
  getSelfCard(networkId: string): NodeCard;
  updateSelfProfile(
    patch: Partial<{
      member: string;
      nodeName: string;
      capabilities: NodeCard['capabilities'];
    }>,
  ): Promise<void>;
  listPeers(filter?: {
    networkId?: string;
    member?: string;
    skillTag?: string;
    onlyAvailable?: boolean;
  }): readonly NodeCard[];
  resolveTarget(selector: string, defaultNetworkId?: string): readonly NodeCard[];
}

export interface EmitMessageInput<T = Record<string, unknown>> {
  networkId: string;
  routing: MessageEnvelope<T>['routing'];
  kind: string;
  subject: string;
  body: string;
  payload: T;
  priority?: MessageEnvelope<T>['priority'];
  traceId?: string;
  replyToId?: string;
  idempotencyKey?: string;
  semantic?: SemanticHeaderInput;
  attachments?: readonly BlobAttachment[];
  /** Local source paths for pre-shared attachments that a durable outbox must stage. */
  attachmentPaths?: Readonly<Record<string, string>>;
  filePaths?: readonly string[];
  /**
   * Defaults to true when the runtime configures a durable outbox: an unreachable target
   * is queued locally (and spooled on a relay seed when available) instead of failing emit.
   */
  queueIfOffline?: boolean;
}

export interface EmitResult {
  messageId: string;
  traceId: string;
  receipts: readonly DeliveryReceipt[];
}

export type OutboxItemStatus = 'queued' | 'delivered' | 'failed';

/**
 * One durable sender-side obligation. `pendingNodeIds` names the resolved targets still
 * awaiting a live signed receipt; an empty list means the explicit selector has not been
 * resolvable yet and must be retried through the emitter's router. A failed item keeps its
 * unresolved targets so an explicit operator requeue can resume routing instead of settling
 * early. Terminal decisions are retained per target, and `inDoubtNodeIds` marks timed-out
 * sends that may already have executed: they are retried only after a direct peer pulse or
 * an explicit operator flush.
 */
export interface OutboxItem {
  id: string;
  networkId: string;
  envelope: MessageEnvelope;
  explicitTargets: string[];
  pendingNodeIds: string[];
  approvalPendingNodeIds?: string[];
  deliveredReceipts: Record<string, DeliveryReceipt>;
  /** Signed terminal decisions, kept as operator evidence and never reported as delivered. */
  failedReceipts: Record<string, DeliveryReceipt>;
  /**
   * Every target with a terminal failure, with or without a signed receipt. `failedReceipts`
   * is the signed-evidence subset; entries without one are unsigned local/protocol failures
   * that an explicit operator requeue may restore.
   */
  failedNodeIds: string[];
  /** Local evidence that a failed target's send never started. */
  unattemptedNodeIds?: string[];
  /** Item-level failure before any target was attempted. */
  unattemptedFailure?: boolean;
  inDoubtNodeIds: string[];
  relaySpooledNodeIds: string[];
  stagedFiles: Record<string, string>;
  status: OutboxItemStatus;
  attempts: number;
  createdAt: string;
  updatedAt: string;
  nextAttemptAt: string;
  lastError?: string;
  /** False until a new keyed claim has a committed send outcome or is replayed. */
  sentCommitted?: boolean;
  activeAttempt?: { token: string; expiresAt: string; fencedNodeIds?: string[] };
  /** The replacement owns the key; this row still provides terminal evidence for its envelope. */
  supersededById?: string;
}

/** Targets covered by delivery, local pending work, uncertainty, or relay custody. */
export function committedOutboxTargets(item: OutboxItem): string[] {
  const unresolvedFailed = Object.entries(item.failedReceipts)
    .filter(([, receipt]) => isUnknownEffectReceipt(receipt) || isRetainedApprovalReceipt(receipt))
    .map(([nodeId]) => nodeId);
  return [...new Set([
    ...Object.keys(item.deliveredReceipts), ...item.pendingNodeIds, ...item.inDoubtNodeIds, ...item.relaySpooledNodeIds,
    ...(item.approvalPendingNodeIds ?? []), ...unresolvedFailed,
  ])];
}

/** Original targets whose prior outcome permits explicit reconciliation. */
export function outboxReconciliationTargets(item: OutboxItem): string[] {
  const approvals = new Set([
    ...(item.approvalPendingNodeIds ?? []),
    ...Object.entries(item.failedReceipts).filter(([, receipt]) => isRetainedApprovalReceipt(receipt)).map(([target]) => target),
  ]);
  if (item.status === 'queued') return [...approvals];
  const provisional = Object.entries(item.deliveredReceipts)
    .filter(([, receipt]) => isConfirmedHandoffReceipt(receipt, { allowQueued: true }) &&
      !isConfirmedHandoffReceipt(receipt, { allowQueued: false }))
    .map(([target]) => target);
  if (item.status === 'delivered') return provisional;
  const provisionalSet = new Set(provisional);
  return [...new Set([...approvals, ...provisional, ...item.failedNodeIds, ...Object.keys(item.failedReceipts)])].filter(target => {
    const failure = item.failedReceipts[target];
    return !isConfirmedHandoffReceipt(item.deliveredReceipts[target], { allowQueued: false }) &&
      (approvals.has(target) || provisionalSet.has(target) ||
        Boolean(failure && (isRetainedApprovalReceipt(failure) || isRetryablePreEffectReceipt(failure))));
  });
}

/** A relay copy remains in custody until the target confirms final execution. */
export function outstandingRelaySpooledTargets(
  nodeIds: readonly string[], deliveredReceipts: Readonly<Record<string, HandoffReceipt>>,
): string[] {
  return [...new Set(nodeIds)].filter(nodeId => !isConfirmedHandoffReceipt(deliveredReceipts[nodeId], { allowQueued: false }));
}

export interface OutboxAttemptOptions {
  leaseMs?: number;
  now?: number;
  /** Recheck unsettled outcomes on the original envelope, including provisional durable handoffs. */
  reconcile?: boolean;
}

export interface OutboxEnqueueInput {
  attemptToken?: string;
  sentCommitted?: boolean;
  /** Targets whose current attempt recorded a non-ambiguous outcome. */
  clearFenceNodeIds?: readonly string[];
  /** Replace outcomes only for these original targets; confirmed execution is immutable. */
  reconcileNodeIds?: readonly string[];
  envelope: MessageEnvelope;
  explicitTargets: string[];
  pendingNodeIds: string[];
  approvalPendingNodeIds?: readonly string[];
  deliveredReceipts?: Record<string, DeliveryReceipt>;
  failedReceipts?: Record<string, DeliveryReceipt>;
  failedNodeIds?: string[];
  unattemptedNodeIds?: readonly string[];
  inDoubtNodeIds?: string[];
  relaySpooledNodeIds?: string[];
  /** hash -> source path; copied into the durable outbox blob directory on first enqueue. */
  filePaths?: Record<string, string>;
  lastError?: string;
}

/** Atomic creation-or-join of the one durable obligation for an idempotency key. */
export interface OutboxClaimInput {
  envelope: MessageEnvelope;
  explicitTargets: string[];
  plannedTargetNodeIds?: readonly string[];
  filePaths?: Record<string, string>;
  /** How long the attempt owns the obligation before another worker may resume it. */
  leaseMs?: number;
}

export type OutboxClaimResult =
  | { claimed: true; item: OutboxItem; attemptToken: string }
  | { claimed: false; item: OutboxItem };

export interface OutboxListFilter {
  networkId?: string;
  status?: OutboxItemStatus | 'all';
}

export interface OutboxFlushOptions {
  id?: string;
  networkId?: string;
  /** Restrict a peer-triggered flush to the target that just became reachable. */
  targetNodeId?: string;
  /** Ignore the per-item retry schedule for an explicit operator flush or peer reconnect. */
  force?: boolean;
  /** Force only the target named by `targetNodeId`, leaving unrelated schedule state intact. */
  forceMatchingTarget?: boolean;
  /**
   * When combined with `forceMatchingTarget`, also retry obligations whose explicit selector
   * has not resolved to any target yet; used by identity directory updates, never by
   * transport beacon pulses.
   */
  includeUnresolved?: boolean;
}

export interface OutboxFlushResult {
  attempted: number;
  delivered: number;
  stillQueued: number;
  items: OutboxItem[];
}

export interface OutboxDeliveryFailure {
  nodeId?: string;
  receipt?: DeliveryReceipt;
  error: string;
}

/** Emitter-owned outcome of one outbox redrive attempt; mailbox only persists the result. */
export interface OutboxDeliveryOutcome {
  /** Targets resolved by routing an initially unresolved explicit selector. */
  resolvedTargetNodeIds?: readonly string[];
  /** Live signed receipts, keyed by target node. */
  deliveredReceipts?: Readonly<Record<string, DeliveryReceipt>>;
  /** Targets still retryable after this attempt. */
  pendingNodeIds?: readonly string[];
  approvalPendingNodeIds?: readonly string[];
  clearFenceNodeIds?: readonly string[];
  unattemptedNodeIds?: readonly string[];
  /** Targets whose send timed out after a possible external effect; retry only on pulse/force. */
  inDoubtNodeIds?: readonly string[];
  relaySpooledNodeIds?: readonly string[];
  /** Per-target signed rejections/failures or local integrity errors; other targets continue. */
  failures?: readonly OutboxDeliveryFailure[];
  lastError?: string;
}

/** Persistence surface the emitter needs; the concrete OutboxStore implements it. */
export interface OutboxStoreLike {
  enqueue(input: OutboxEnqueueInput): OutboxItem;
  /** Atomically reserve monotonic versions per (networkId, idempotencyKey) under the store lock. */
  claimIdempotency(input: OutboxClaimInput): OutboxClaimResult;
  /** Reserve a same-version replacement or retry of the exact unattempted obligation observed. */
  replaceUnattemptedIdempotency(input: OutboxClaimInput & { expectedId?: string }): OutboxClaimResult | undefined;
  beginAttempt(id: string, options?: OutboxAttemptOptions): { item: OutboxItem; attemptToken: string } | undefined;
  prepareSend?(id: string, attemptToken: string, targetNodeIds: readonly string[]): OutboxItem | undefined;
  markSentCommitted?(id: string): boolean;
  releaseAttempt(id: string, attemptToken: string): void;
  /** Read the highest matching version without reserving or changing an obligation. */
  findKeyedObligation?(networkId: string, key: string, minVersion: number): OutboxItem | undefined;
  /** Terminal local/routing failure with no signed target receipt. */
  markFailed(id: string, lastError: string, attemptToken?: string): OutboxItem | undefined;
}

export interface SourcePlugin {
  name: string;
  start(emitter: EmitterService): void | Promise<void>;
  stop(): void | Promise<void>;
}

export interface EmitterService {
  holdSources?(): { release(options?: { start?: boolean }): void };
  emit<T>(input: EmitMessageInput<T>): Promise<EmitResult>;
  registerSource(source: SourcePlugin): SourceRegistration;
  /** Emitter-owned outbound preparation, routing, send and receipt validation for redrive. */
  redriveOutboxItem?(
    item: OutboxItem,
    options?: OutboxRedriveDeliveryOptions,
  ): Promise<OutboxDeliveryOutcome>;
}

export interface OutboxRedriveDeliveryOptions {
  targetNodeIds?: readonly string[];
  timeoutMs?: number;
  attemptToken?: string;
}

export interface RouteDecision {
  mode: 'unicast' | 'multicast' | 'broadcast';
  targetNodeIds: readonly string[];
  matchedBy: string;
  reason: string;
}

export interface RouteJudgePlugin {
  name: string;
  priority: number;
  decide(
    envelope: MessageEnvelope,
    candidates: readonly NodeCard[],
  ): Promise<RouteDecision | null>;
}

export interface RouterService {
  route(envelope: MessageEnvelope): Promise<RouteDecision>;
  registerJudge(judge: RouteJudgePlugin): Disposable;
}

export const DND_HANDOFF_PARKED = 'dnd_handoff_parked';
export const DND_CUSTODY_RETAINED = 'dnd_custody_retained';
export const APPROVAL_CUSTODY_RETAINED = 'approval_custody_retained';
export const APPROVAL_CANCELLED = 'approval_cancelled';

export type GateVerdict =
  | { status: 'allow' }
  | { status: 'pending_approval'; approvalId: string; reason: string }
  | { status: 'queued_dnd'; reason: string; durable?: boolean }
  | { status: 'reject'; code: string; reason: string; durable?: boolean };

export interface GateRulePlugin {
  name: string;
  priority: number;
  evaluate(envelope: MessageEnvelope): Promise<GateVerdict | null>;
}

export interface PendingApprovalItem {
  id: string;
  envelope: MessageEnvelope;
  reason: string;
  createdAt: string;
}

export interface GateService {
  getMode(): AdmissionMode;
  setMode(mode: AdmissionMode): Promise<void>;
  evaluate(envelope: MessageEnvelope): Promise<GateVerdict>;
  registerRule(rule: GateRulePlugin): Disposable;
  approve(approvalId: string, opts?: { trustSender?: boolean }): Promise<void>;
  reject(approvalId: string, reason?: string): Promise<void>;
  listPendingApprovals(): readonly PendingApprovalItem[];
}

export interface ExecutionContext {
  /** Original wire-signed envelopes, preserved before receiver payload projection. */
  signedEnvelopes?: Readonly<Record<string, MessageEnvelope>>;
  networkId: string;
  nodeId: string;
  traceId: string;
  signal?: AbortSignal;
}

export interface ExecutionReceipt extends Record<string, unknown> {
  executor: string;
  executionId: string;
  detail?: string;
  custodyRelinquished?: boolean;
  /** Destinations committed by mailbox-backed execution, keyed by envelope ID. */
  envelopeMailboxes?: Readonly<Record<string, string>>;
}

/** A failed execution group leaves earlier handoffs settled and later members unattempted. */
export class PartialBatchExecutionError extends Error {
  readonly settledReceipts: Readonly<Record<string, ExecutionReceipt>>;
  readonly unattemptedIds: ReadonlySet<string>;

  constructor(
    cause: unknown,
    settledReceipts: Readonly<Record<string, ExecutionReceipt>>,
    unattemptedIds: ReadonlySet<string>,
  ) {
    super(String(cause), { cause });
    this.settledReceipts = settledReceipts;
    this.unattemptedIds = unattemptedIds;
  }
}

export interface DispatchReceipt {
  stage: DeliveryStage;
  executionReceipt?: ExecutionReceipt;
  detail?: string;
}

export interface DispatchPolicyPlugin {
  name: string;
  process(
    envelope: MessageEnvelope,
    next: (batch: readonly MessageEnvelope[]) => Promise<DispatchReceipt>,
  ): Promise<DispatchReceipt>;
}

export interface ExecutorPlugin<C = unknown> {
  name: string;
  checkIdle?(config: C): Promise<{ idle: boolean; reason?: string }>;
  execute(
    batch: readonly MessageEnvelope[],
    context: ExecutionContext,
    config: C,
  ): Promise<ExecutionReceipt>;
}

export interface DispatchErrorEvent {
  readonly stage: 'post_delivery_policy';
  readonly networkId: string;
  readonly fromNodeId: string;
  readonly messageId: string;
  readonly receipt: DispatchReceipt;
  readonly error: unknown;
}

export interface DispatcherService {
  hasExecutor(name: string): boolean;
  hasPolicy(name: string): boolean;
  dispatch(envelope: MessageEnvelope): Promise<DispatchReceipt>;
  getLastError(): DispatchErrorEvent | undefined;
  registerPolicy(policy: DispatchPolicyPlugin): Disposable;
  registerExecutor<C>(executor: ExecutorPlugin<C>, config: C): Disposable;
  /** In-flight dispatch count; zero means the dispatcher is idle. */
  activeCount(): number;
  /** Idle notification used by idle-gated restart owners. */
  onIdle(listener: () => void): Disposable;
  /** Resolves once every in-flight dispatch has settled. */
  waitForIdle(): Promise<void>;
}

const SPKI_PREFIX = Buffer.from('302a300506032b6570032100', 'hex');
const PKCS8_PREFIX = Buffer.from('302e020100300506032b657004220420', 'hex');

function keyPair(privateKey: ReturnType<typeof createPrivateKey>) {
  const publicKey = createPublicKey(privateKey);
  return {
    nodeId: `ed25519:${publicKey.export({ type: 'spki', format: 'der' }).subarray(-32).toString('hex')}`,
    publicKeyPem: publicKey.export({ type: 'spki', format: 'pem' }).toString(),
    privateKeyPem: privateKey.export({ type: 'pkcs8', format: 'pem' }).toString(),
  };
}

export function generateNodeKeyPair(): {
  nodeId: string;
  publicKeyPem: string;
  privateKeyPem: string;
} {
  return keyPair(generateKeyPairSync('ed25519').privateKey);
}

/**
 * JSON data only. Omit the outer signature; nested business fields named signature remain signed.
 * Undefined object properties are omitted and undefined array entries become null, as on the wire.
 * Non-finite numbers, class instances and cycles are rejected instead of silently losing information.
 */
export function canonicalJson(value: unknown): string {
  return serializeJson(value, true);
}

function serializeJson(value: unknown, omitSignature: boolean): string {
  const ancestors = new Set<object>();
  function encode(item: unknown, root = false): string {
    if (item === null) {
      return 'null';
    }
    if (typeof item === 'string' || typeof item === 'boolean') {
      return JSON.stringify(item);
    }
    if (typeof item === 'number' && Number.isFinite(item)) {
      return JSON.stringify(item);
    }
    if (typeof item !== 'object') {
      throw new TypeError('Canonical JSON requires finite JSON data');
    }
    if (ancestors.has(item)) {
      throw new TypeError('Canonical JSON cannot contain cycles');
    }
    ancestors.add(item);
    try {
      if (Array.isArray(item)) {
        return `[${Array.from(item, v => (v === undefined ? 'null' : encode(v))).join(',')}]`;
      }
      if (
        Object.getPrototypeOf(item) !== Object.prototype &&
        Object.getPrototypeOf(item) !== null
      ) {
        throw new TypeError('Canonical JSON requires plain JSON objects');
      }
      const object = item as Record<string, unknown>;
      return `{${Object.keys(object)
        .sort()
        .filter(k => !(root && k === 'signature') && object[k] !== undefined)
        .map(k => `${JSON.stringify(k)}:${encode(object[k])}`)
        .join(',')}}`;
    } finally {
      ancestors.delete(item);
    }
  }
  return encode(value, omitSignature);
}

export function signCanonicalPayload(payload: object, privateKeyPem: string): string {
  return signCanonicalText(canonicalJson(payload), privateKeyPem);
}

function signCanonicalText(text: string, privateKeyPem: string): string {
  return sign(null, Buffer.from(text), privateKeyPem).toString('base64url');
}

const SEMANTIC_MESSAGE_TYPES: readonly SemanticMessageType[] = [
  'message',
  'question',
  'reply',
  'settle',
];
const SETTLEMENT_OUTCOMES: readonly SettlementOutcome[] = ['succeeded', 'failed', 'cancelled'];
const MAX_MAILBOX_ID_LENGTH = 256;
const CONTROL_CHARACTER = /[\u0000-\u001f\u007f]/;

/** A mailbox id is a trimmed, non-empty logical channel name without control characters. */
export function isValidMailboxId(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value.length <= MAX_MAILBOX_ID_LENGTH &&
    value.trim() === value &&
    !CONTROL_CHARACTER.test(value)
  );
}

/** Validate and normalize the authenticated semantic header carried by an envelope. */
export function validateSemanticHeader(value: unknown): SemanticHeader {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error('Invalid semantic header');
  }
  const header = value as Partial<SemanticHeader>;
  if (!SEMANTIC_MESSAGE_TYPES.includes(header.type as SemanticMessageType)) {
    throw new Error(`Invalid semantic message type: ${String(header.type)}`);
  }
  const type = header.type as SemanticMessageType;
  if (typeof header.threadId !== 'string' || !header.threadId) {
    throw new Error('Semantic threadId must be a non-empty string');
  }
  if (header.targetMailbox !== undefined && !isValidMailboxId(header.targetMailbox)) {
    throw new Error('Invalid semantic targetMailbox');
  }
  if (header.replyMailbox !== undefined && !isValidMailboxId(header.replyMailbox)) {
    throw new Error('Invalid semantic replyMailbox');
  }
  if (type === 'settle') {
    if (!SETTLEMENT_OUTCOMES.includes(header.outcome as SettlementOutcome)) {
      throw new Error('Settlement outcome required');
    }
  } else if (header.outcome !== undefined) {
    throw new Error('Settlement outcome is only valid for settle messages');
  }
  return {
    type,
    threadId: header.threadId,
    ...(header.targetMailbox !== undefined ? { targetMailbox: header.targetMailbox } : {}),
    ...(header.replyMailbox !== undefined ? { replyMailbox: header.replyMailbox } : {}),
    ...(header.outcome !== undefined ? { outcome: header.outcome } : {}),
  };
}

/** Resolve emitter input into a complete semantic header, defaulting the thread to this envelope. */
export function resolveSemanticHeader(
  input: SemanticHeaderInput,
  fallbackThreadId: string,
): SemanticHeader {
  if (typeof input !== 'object' || input === null || Array.isArray(input)) {
    throw new Error('Invalid semantic header');
  }
  return validateSemanticHeader({
    ...input,
    threadId: input.threadId === undefined ? fallbackThreadId : input.threadId,
  });
}

/**
 * Canonical signed form of an envelope. Without a semantic header this is byte-for-byte the v1
 * representation; with one, the normalized header is covered by the Ed25519 signature.
 */
export function canonicalizeEnvelopeForSigning<T>(
  envelope: MessageEnvelope<T> | Omit<MessageEnvelope<T>, 'signature'>,
): string {
  if (envelope.semantic === undefined) {
    return canonicalJson(envelope);
  }
  return canonicalJson({ ...envelope, semantic: validateSemanticHeader(envelope.semantic) });
}

export function verifyCanonicalPayload(
  payload: object,
  signature: string,
  nodeId: string,
): boolean {
  try {
    return verifyCanonicalText(canonicalJson(payload), signature, nodeId);
  } catch {
    return false;
  }
}

function verifyCanonicalText(text: string, signature: string, nodeId: string): boolean {
  try {
    if (!/^ed25519:[0-9a-f]{64}$/.test(nodeId) || !/^[A-Za-z0-9_-]{86}$/.test(signature)) {
      return false;
    }
    const bytes = Buffer.from(signature, 'base64url');
    if (bytes.toString('base64url') !== signature) {
      return false;
    }
    const publicKey = createPublicKey({
      key: Buffer.concat([SPKI_PREFIX, Buffer.from(nodeId.slice(8), 'hex')]),
      format: 'der',
      type: 'spki',
    });
    return verify(null, Buffer.from(text), publicKey, bytes);
  } catch {
    return false;
  }
}

export function signNodeCard(card: Omit<NodeCard, 'signature'>, privateKeyPem: string): NodeCard {
  return { ...card, signature: signCanonicalPayload(card, privateKeyPem) };
}

export function verifyNodeCard(card: NodeCard): boolean {
  return verifyCanonicalPayload(card, card.signature, card.nodeId);
}

export function signEnvelope<T>(
  envelope: Omit<MessageEnvelope<T>, 'signature'>,
  privateKeyPem: string,
): MessageEnvelope<T> {
  return {
    ...envelope,
    signature: signCanonicalText(canonicalizeEnvelopeForSigning(envelope), privateKeyPem),
  };
}

export function verifyEnvelope<T>(envelope: MessageEnvelope<T>): boolean {
  try {
    return verifyCanonicalText(
      canonicalizeEnvelopeForSigning(envelope),
      envelope.signature,
      envelope.from?.nodeId,
    );
  } catch {
    return false;
  }
}

export function signReceipt(
  receipt: Omit<DeliveryReceipt, 'signature'>,
  privateKeyPem: string,
): DeliveryReceipt {
  return { ...receipt, signature: signCanonicalPayload(receipt, privateKeyPem) };
}

export function verifyReceipt(receipt: DeliveryReceipt): boolean {
  return verifyCanonicalPayload(receipt, receipt.signature, receipt.fromNodeId);
}

export function deriveNetworkKeys(networkId: string, passphrase: string): DerivedNetworkKeys {
  const master = createHash('sha256')
    .update(`the-dealer:network:v1:${networkId}:${passphrase}`)
    .digest();
  // RFC 5869 expand only, one block (32 bytes); do not perform an extra extract step.
  const expand = (info: string) =>
    createHmac('sha256', master)
      .update(info)
      .update(Buffer.from([1]))
      .digest();
  const rendezvous = keyPair(
    createPrivateKey({
      key: Buffer.concat([PKCS8_PREFIX, expand('the-dealer:rendezvous-ed25519:v1')]),
      format: 'der',
      type: 'pkcs8',
    }),
  );
  return {
    networkId,
    swarmTopicId: expand('the-dealer:swarm-topic:v1').toString('hex'),
    rendezvousPublicKey: rendezvous.nodeId,
    rendezvousPrivateKeyPem: rendezvous.privateKeyPem,
    sealedKey: expand('the-dealer:aead-sealed-key:v1'),
  };
}

function frameAAD(
  frame: Pick<SealedNetworkFrame, 'protocol' | 'swarmTopicId' | 'senderNodeId'>,
): Buffer {
  return Buffer.from(
    canonicalJson({
      protocol: frame.protocol,
      swarmTopicId: frame.swarmTopicId,
      senderNodeId: frame.senderNodeId,
    }),
  );
}

export function sealNetworkFrame(
  innerPayload: unknown,
  keys: DerivedNetworkKeys,
  senderNodeId: string,
): SealedNetworkFrame {
  const nonce = randomBytes(12);
  const header = {
    protocol: 'the-dealer-wire/v1' as const,
    swarmTopicId: keys.swarmTopicId,
    senderNodeId,
  };
  const cipher = createCipheriv('aes-256-gcm', keys.sealedKey, nonce);
  cipher.setAAD(frameAAD(header));
  // Encryption preserves every field, including the signed inner payload's signature.
  const plaintext = serializeJson(innerPayload, false);
  const ciphertext = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
  return {
    ...header,
    nonce: nonce.toString('base64'),
    ciphertext: ciphertext.toString('base64'),
    authTag: cipher.getAuthTag().toString('base64'),
  };
}

function decodeBase64(value: string, name: string, length?: number): Buffer {
  if (typeof value !== 'string') {
    throw new Error(`Invalid frame ${name}`);
  }
  const bytes = Buffer.from(value, 'base64');
  if (bytes.toString('base64') !== value || (length !== undefined && bytes.length !== length)) {
    throw new Error(`Invalid frame ${name}`);
  }
  return bytes;
}

export function unsealNetworkFrame<T = unknown>(
  frame: SealedNetworkFrame,
  keys: DerivedNetworkKeys,
): T {
  if (frame.protocol !== 'the-dealer-wire/v1') {
    throw new Error('Unsupported frame protocol');
  }
  if (frame.swarmTopicId !== keys.swarmTopicId) {
    throw new Error('Frame swarmTopicId does not match mounted network');
  }
  try {
    const decipher = createDecipheriv(
      'aes-256-gcm',
      keys.sealedKey,
      decodeBase64(frame.nonce, 'nonce', 12),
    );
    decipher.setAAD(frameAAD(frame));
    decipher.setAuthTag(decodeBase64(frame.authTag, 'authTag', 16));
    const plaintext = Buffer.concat([
      decipher.update(decodeBase64(frame.ciphertext, 'ciphertext')),
      decipher.final(),
    ]);
    return JSON.parse(plaintext.toString('utf8')) as T;
  } catch (cause) {
    throw new Error(
      'Unable to authenticate or decode network frame (wrong key or tampered frame)',
      { cause },
    );
  }
}

export const CONVERSATION_MODES = ['spawn', 'wave', 'fixed', 'inbox-only'] as const;
export type AgentConversationMode = (typeof CONVERSATION_MODES)[number];
export const ACTIVE_CONVERSATION_MODES = ['spawn', 'wave', 'fixed'] as const;
export type ActiveConversationMode = (typeof ACTIVE_CONVERSATION_MODES)[number];
export function normalizeConversationModeInput(
  input?: { mode?: unknown; conversationMode?: unknown },
): AgentConversationMode | undefined {
  if (!input) return undefined;
  if (input.mode !== undefined && input.conversationMode !== undefined && input.mode !== input.conversationMode) {
    throw new Error('Conflicting mode and conversationMode');
  }
  const raw = input.mode ?? input.conversationMode;
  if (raw === undefined) return undefined;
  if (typeof raw !== 'string' || !CONVERSATION_MODES.includes(raw as AgentConversationMode)) {
    throw new Error(`Invalid conversation mode: ${String(raw)}`);
  }
  return raw as AgentConversationMode;
}
