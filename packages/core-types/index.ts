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

export interface MessageEnvelope<T = Record<string, unknown>> {
  id: string;
  networkId: string;
  traceId: string;
  replyToId?: string;
  idempotencyKey?: string;
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

export interface SealedNetworkFrame {
  protocol: 'the-dealer-wire/v1';
  swarmTopicId: string;
  senderNodeId: string;
  nonce: string;
  ciphertext: string;
  authTag: string;
}

export interface InboundFrameEvent {
  networkId: string;
  senderNodeId: string;
  mode: 'unicast' | 'broadcast';
  subTopic?: string;
  payload: unknown;
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
  sendUnicast(
    networkId: string,
    targetNodeId: string,
    frame: Uint8Array,
    opts?: { timeoutMs?: number },
  ): Promise<Uint8Array>;
  sendMulticast(
    networkId: string,
    targetNodeIds: readonly string[],
    frame: Uint8Array,
  ): Promise<Map<string, Result<Uint8Array>>>;
  broadcast(networkId: string, subTopic: string, frame: Uint8Array): Promise<void>;
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
  attachments?: readonly BlobAttachment[];
  filePaths?: readonly string[];
}

export interface EmitResult {
  messageId: string;
  traceId: string;
  receipts: readonly DeliveryReceipt[];
}

export interface SourcePlugin {
  name: string;
  start(emitter: EmitterService): void | Promise<void>;
  stop(): void | Promise<void>;
}

export interface EmitterService {
  emit<T>(input: EmitMessageInput<T>): Promise<EmitResult>;
  registerSource(source: SourcePlugin): Disposable;
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

export type GateVerdict =
  | { status: 'allow' }
  | { status: 'pending_approval'; approvalId: string; reason: string }
  | { status: 'queued_dnd'; reason: string }
  | { status: 'reject'; code: string; reason: string };

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
  networkId: string;
  nodeId: string;
  traceId: string;
  signal?: AbortSignal;
}

export interface ExecutionReceipt extends Record<string, unknown> {
  executor: string;
  executionId: string;
  detail?: string;
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

export interface DispatcherService {
  dispatch(envelope: MessageEnvelope): Promise<DispatchReceipt>;
  registerPolicy(policy: DispatchPolicyPlugin): Disposable;
  registerExecutor<C>(executor: ExecutorPlugin<C>, config: C): Disposable;
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
  return sign(null, Buffer.from(canonicalJson(payload)), privateKeyPem).toString('base64url');
}

export function verifyCanonicalPayload(
  payload: object,
  signature: string,
  nodeId: string,
): boolean {
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
    return verify(null, Buffer.from(canonicalJson(payload)), publicKey, bytes);
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
  return { ...envelope, signature: signCanonicalPayload(envelope, privateKeyPem) };
}

export function verifyEnvelope<T>(envelope: MessageEnvelope<T>): boolean {
  return verifyCanonicalPayload(envelope, envelope.signature, envelope.from?.nodeId);
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
