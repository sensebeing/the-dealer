import { randomUUID } from 'node:crypto';
import {
  chmodSync,
  closeSync,
  copyFileSync,
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
import {
  isOfflinePeerSpooledError,
  isPeerOfflineError,
  verifyReceipt,
} from '../core-types/index.ts';
import type {
  DeliveryReceipt,
  Disposable,
  MessageEnvelope,
  NodeCard,
  RouterService,
  TransportService,
} from '../core-types/index.ts';
import {
  holdOwnedDirectory,
  isDeadProcessOwner,
  processOwnerRecord,
  readProcessOwner,
  releaseOwnedDirectory,
} from '../process-lock/index.ts';

export const OUTBOX_SCHEMA = 'the-dealer-outbox/v1';
const LOCK_RETRY_MS = 20;
const LOCK_TIMEOUT_MS = 2_000;
const STALE_LOCK_MS = 30_000;
const DEFAULT_REDRIVE_INTERVAL_MS = 2_000;
const MIN_ATTEMPT_DELAY_MS = 2_000;
const MAX_ATTEMPT_DELAY_MS = 60_000;

export type OutboxItemStatus = 'queued' | 'delivered' | 'failed';

/**
 * One durable sender-side obligation. `pendingNodeIds` names the resolved targets still
 * awaiting a live signed receipt; an empty list means the explicit selector has not been
 * resolvable yet and must be retried through the router.
 */
export interface OutboxItem {
  id: string;
  networkId: string;
  envelope: MessageEnvelope;
  explicitTargets: string[];
  pendingNodeIds: string[];
  deliveredReceipts: Record<string, DeliveryReceipt>;
  relaySpooledNodeIds: string[];
  stagedFiles: Record<string, string>;
  status: OutboxItemStatus;
  attempts: number;
  createdAt: string;
  updatedAt: string;
  nextAttemptAt: string;
  lastError?: string;
}

export interface OutboxEnqueueInput {
  envelope: MessageEnvelope;
  explicitTargets: string[];
  pendingNodeIds: string[];
  deliveredReceipts?: Record<string, DeliveryReceipt>;
  relaySpooledNodeIds?: string[];
  /** hash -> source path; copied into the durable outbox blob directory on first enqueue. */
  filePaths?: Record<string, string>;
  lastError?: string;
}

export interface OutboxListFilter {
  networkId?: string;
  status?: OutboxItemStatus | 'all';
}

export interface OutboxAttemptUpdate {
  pendingNodeIds?: string[];
  relaySpooledNodeIds?: string[];
  lastError?: string;
  nextAttemptAt: string;
}

interface OutboxFileState {
  schema: typeof OUTBOX_SCHEMA;
  items: OutboxItem[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function validEnvelope(value: unknown): value is MessageEnvelope {
  return (
    isRecord(value) &&
    typeof value.id === 'string' &&
    value.id.length > 0 &&
    typeof value.networkId === 'string' &&
    isRecord(value.from) &&
    typeof value.from.nodeId === 'string' &&
    Array.isArray(value.attachments)
  );
}

function validReceipt(value: unknown): value is DeliveryReceipt {
  return (
    isRecord(value) &&
    typeof value.messageId === 'string' &&
    typeof value.networkId === 'string' &&
    typeof value.fromNodeId === 'string' &&
    typeof value.stage === 'string' &&
    typeof value.updatedAt === 'string' &&
    typeof value.signature === 'string'
  );
}

function validItem(value: unknown): value is OutboxItem {
  if (!isRecord(value)) {
    return false;
  }
  const envelope = value.envelope;
  if (!validEnvelope(envelope)) {
    return false;
  }
  return (
    value.id === envelope.id &&
    typeof value.networkId === 'string' &&
    envelope.networkId === value.networkId &&
    Array.isArray(value.explicitTargets) &&
    value.explicitTargets.every(target => typeof target === 'string') &&
    Array.isArray(value.pendingNodeIds) &&
    value.pendingNodeIds.every(nodeId => typeof nodeId === 'string') &&
    isRecord(value.deliveredReceipts) &&
    Object.values(value.deliveredReceipts).every(validReceipt) &&
    Array.isArray(value.relaySpooledNodeIds) &&
    value.relaySpooledNodeIds.every(nodeId => typeof nodeId === 'string') &&
    isRecord(value.stagedFiles) &&
    Object.entries(value.stagedFiles).every(
      ([hash, path]) => /^[a-f0-9]{64}$/.test(hash) && typeof path === 'string',
    ) &&
    ['queued', 'delivered', 'failed'].includes(String(value.status)) &&
    Number.isSafeInteger(value.attempts) &&
    (value.attempts as number) >= 0 &&
    typeof value.createdAt === 'string' &&
    typeof value.updatedAt === 'string' &&
    typeof value.nextAttemptAt === 'string' &&
    (value.lastError === undefined || typeof value.lastError === 'string')
  );
}

/**
 * Durable sender outbox: one atomic JSON document plus staged attachment copies, guarded by
 * a crash-recoverable directory lock shared with any offline CLI reader. It never decides
 * routing or execution; it only preserves obligations until a live target receipt arrives.
 */
export class OutboxStore {
  readonly path: string;
  readonly blobDir: string;
  readonly lockDir: string;

  constructor(outboxPath: string, blobDir: string) {
    this.path = resolve(outboxPath);
    this.blobDir = resolve(blobDir);
    this.lockDir = resolve(`${this.path}.lock`);
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
          throw new Error(`Outbox store is locked: ${this.lockDir}`);
        }
        Atomics.wait(sleeper, 0, 0, LOCK_RETRY_MS);
      }
    }
    try {
      holdOwnedDirectory(this.lockDir);
      writeFileSync(ownerPath, JSON.stringify(processOwnerRecord()), { mode: 0o600 });
      return operation();
    } finally {
      rmSync(this.lockDir, { recursive: true, force: true });
      releaseOwnedDirectory(this.lockDir);
    }
  }

  private clearStaleLock(ownerPath: string): void {
    const owner = readProcessOwner(ownerPath);
    if (owner !== undefined) {
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
      // The lock disappeared while it was inspected.
    }
  }

  private read(): OutboxFileState {
    let raw: string;
    try {
      raw = readFileSync(this.path, 'utf8');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        return { schema: OUTBOX_SCHEMA, items: [] };
      }
      throw error;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw new Error('Invalid outbox store');
    }
    if (
      !isRecord(parsed) ||
      parsed.schema !== OUTBOX_SCHEMA ||
      !Array.isArray(parsed.items) ||
      !parsed.items.every(validItem)
    ) {
      throw new Error('Invalid outbox store');
    }
    return { schema: OUTBOX_SCHEMA, items: parsed.items as OutboxItem[] };
  }

  private save(state: OutboxFileState): void {
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
    } finally {
      rmSync(temporary, { force: true });
    }
  }

  private mutate<T>(operation: (state: OutboxFileState) => T): T {
    return this.withFileLock(() => {
      const state = this.read();
      const result = operation(state);
      this.save(state);
      return result;
    });
  }

  private stageFiles(
    id: string,
    files: Record<string, string> | undefined,
    existing: Record<string, string>,
  ): Record<string, string> {
    const staged = { ...existing };
    if (files === undefined) {
      return staged;
    }
    for (const [hash, source] of Object.entries(files)) {
      if (!/^[a-f0-9]{64}$/.test(hash) || typeof source !== 'string') {
        throw new Error('Invalid outbox attachment staging input');
      }
      if (staged[hash] && existsSync(staged[hash])) {
        continue;
      }
      const directory = join(this.blobDir, encodeURIComponent(id));
      mkdirSync(directory, { recursive: true, mode: 0o700 });
      const destination = join(directory, hash);
      copyFileSync(source, destination);
      chmodSync(destination, 0o600);
      staged[hash] = destination;
    }
    return staged;
  }

  private removeStagedFiles(id: string, staged: Record<string, string>): void {
    for (const path of Object.values(staged)) {
      rmSync(path, { force: true });
    }
    rmSync(join(this.blobDir, encodeURIComponent(id)), { recursive: true, force: true });
  }

  /** Idempotent by envelope id: repeat enqueues merge receipts and pending targets. */
  enqueue(input: OutboxEnqueueInput): OutboxItem {
    if (!validEnvelope(input?.envelope)) {
      throw new Error('Invalid outbox envelope');
    }
    const id = input.envelope.id;
    const now = new Date().toISOString();
    return this.mutate(state => {
      const index = state.items.findIndex(item => item.id === id);
      const existing = index >= 0 ? state.items[index] : undefined;
      const deliveredReceipts = {
        ...(existing?.deliveredReceipts ?? {}),
        ...structuredClone(input.deliveredReceipts ?? {}),
      };
      const pendingNodeIds = [
        ...new Set([...(input.pendingNodeIds ?? []), ...(existing?.pendingNodeIds ?? [])]),
      ].filter(nodeId => !deliveredReceipts[nodeId]);
      const relaySpooledNodeIds = [
        ...new Set([
          ...(existing?.relaySpooledNodeIds ?? []),
          ...(input.relaySpooledNodeIds ?? []),
        ]),
      ];
      const stagedFiles = this.stageFiles(id, input.filePaths, existing?.stagedFiles ?? {});
      const delivered =
        pendingNodeIds.length === 0 && Object.keys(deliveredReceipts).length > 0;
      if (delivered) {
        this.removeStagedFiles(id, stagedFiles);
      }
      const item: OutboxItem = {
        id,
        networkId: input.envelope.networkId,
        envelope: structuredClone(input.envelope),
        explicitTargets: [...new Set(input.explicitTargets ?? [])],
        pendingNodeIds,
        deliveredReceipts,
        relaySpooledNodeIds,
        stagedFiles: delivered ? {} : stagedFiles,
        status: delivered ? 'delivered' : 'queued',
        attempts: existing?.attempts ?? 0,
        createdAt: existing?.createdAt ?? now,
        updatedAt: now,
        nextAttemptAt: now,
        ...(input.lastError !== undefined
          ? { lastError: input.lastError }
          : existing?.lastError !== undefined
            ? { lastError: existing.lastError }
            : {}),
      };
      if (index >= 0) {
        state.items[index] = item;
      } else {
        state.items.push(item);
      }
      return clone(item);
    });
  }

  get(id: string): OutboxItem | undefined {
    const item = this.read().items.find(candidate => candidate.id === id);
    return item ? clone(item) : undefined;
  }

  list(filter: OutboxListFilter = {}): OutboxItem[] {
    if (
      filter.status !== undefined &&
      !['queued', 'delivered', 'failed', 'all'].includes(filter.status)
    ) {
      throw new Error('Invalid outbox status filter');
    }
    return this.read()
      .items.filter(
        item =>
          (filter.networkId === undefined || item.networkId === filter.networkId) &&
          (filter.status === undefined ||
            filter.status === 'all' ||
            item.status === filter.status),
      )
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id))
      .map(clone);
  }

  /** Record one live target confirmation; all targets confirmed settles the obligation. */
  markNodeDelivered(
    id: string,
    nodeId: string,
    receipt: DeliveryReceipt,
  ): OutboxItem | undefined {
    if (typeof nodeId !== 'string' || !nodeId) {
      throw new Error('Invalid outbox target');
    }
    const now = new Date().toISOString();
    return this.mutate(state => {
      const item = state.items.find(candidate => candidate.id === id);
      if (!item) {
        return undefined;
      }
      item.deliveredReceipts = {
        ...item.deliveredReceipts,
        [nodeId]: structuredClone(receipt),
      };
      item.pendingNodeIds = item.pendingNodeIds.filter(pending => pending !== nodeId);
      item.updatedAt = now;
      item.nextAttemptAt = now;
      item.lastError = undefined;
      if (item.pendingNodeIds.length === 0 && Object.keys(item.deliveredReceipts).length > 0) {
        item.status = 'delivered';
        this.removeStagedFiles(item.id, item.stagedFiles);
        item.stagedFiles = {};
      }
      return clone(item);
    });
  }

  recordAttempt(id: string, update: OutboxAttemptUpdate): OutboxItem | undefined {
    if (typeof update?.nextAttemptAt !== 'string' || !Number.isFinite(Date.parse(update.nextAttemptAt))) {
      throw new Error('Invalid outbox attempt schedule');
    }
    const now = new Date().toISOString();
    return this.mutate(state => {
      const item = state.items.find(candidate => candidate.id === id);
      if (!item) {
        return undefined;
      }
      if (update.pendingNodeIds !== undefined) {
        item.pendingNodeIds = [
          ...new Set(update.pendingNodeIds),
        ].filter(nodeId => !item.deliveredReceipts[nodeId]);
      }
      if (update.relaySpooledNodeIds?.length) {
        item.relaySpooledNodeIds = [
          ...new Set([...item.relaySpooledNodeIds, ...update.relaySpooledNodeIds]),
        ];
      }
      if (update.lastError === undefined) {
        item.lastError = undefined;
      } else {
        item.lastError = update.lastError;
      }
      item.attempts += 1;
      item.updatedAt = now;
      item.nextAttemptAt = update.nextAttemptAt;
      if (
        item.pendingNodeIds.length === 0 &&
        Object.keys(item.deliveredReceipts).length > 0 &&
        item.status !== 'delivered'
      ) {
        item.status = 'delivered';
        this.removeStagedFiles(item.id, item.stagedFiles);
        item.stagedFiles = {};
      }
      return clone(item);
    });
  }
}

export interface OutboxFlushOptions {
  id?: string;
  /** Ignore the per-item retry schedule, for an explicit operator flush or peer reconnect. */
  force?: boolean;
}

export interface OutboxFlushResult {
  attempted: number;
  delivered: number;
  stillQueued: number;
  items: OutboxItem[];
}

export interface OutboxRedriveEvents {
  on(event: 'node/peer-updated', listener: (card: NodeCard) => void): Disposable;
}

export interface OutboxRedriveOptions {
  outbox: OutboxStore;
  transport: Pick<TransportService, 'sendUnicast' | 'registerBlob'>;
  router: Pick<RouterService, 'route'>;
  events?: OutboxRedriveEvents;
  onError?: (error: unknown) => void;
  intervalMs?: number;
}

function parseTargetReceipt(
  bytes: Uint8Array,
  envelope: MessageEnvelope,
  targetNodeId: string,
): DeliveryReceipt {
  let receipt: DeliveryReceipt;
  try {
    receipt = JSON.parse(Buffer.from(bytes).toString('utf8'));
  } catch {
    throw new Error('Invalid or mismatched delivery receipt');
  }
  if (
    !verifyReceipt(receipt) ||
    receipt.messageId !== envelope.id ||
    receipt.networkId !== envelope.networkId ||
    receipt.fromNodeId !== targetNodeId
  ) {
    throw new Error('Invalid or mismatched delivery receipt');
  }
  return receipt;
}

function attemptDelayMs(attempts: number): number {
  return Math.min(MAX_ATTEMPT_DELAY_MS, MIN_ATTEMPT_DELAY_MS * 2 ** Math.min(attempts, 5));
}

/**
 * Sole redrive owner for the durable outbox. Peer announcements and a low-frequency,
 * unref'd timer trigger delivery; every send is an ordinary authenticated unicast, so a
 * relay seed may spool it again when the target is still offline.
 */
export class OutboxRedriveService {
  private readonly outbox: OutboxStore;
  private readonly transport: Pick<TransportService, 'sendUnicast' | 'registerBlob'>;
  private readonly router: Pick<RouterService, 'route'>;
  private readonly events?: OutboxRedriveEvents;
  private readonly onError: (error: unknown) => void;
  private readonly intervalMs: number;
  private timer?: ReturnType<typeof setInterval>;
  private subscription?: Disposable;
  private queue: Promise<unknown> = Promise.resolve();
  private stopped = false;
  private lastPeerPulse = new Map<string, number>();

  constructor(options: OutboxRedriveOptions) {
    this.outbox = options.outbox;
    this.transport = options.transport;
    this.router = options.router;
    this.events = options.events;
    this.onError = options.onError ?? (() => {});
    this.intervalMs = options.intervalMs ?? DEFAULT_REDRIVE_INTERVAL_MS;
  }

  start(): void {
    if (this.timer !== undefined) {
      return;
    }
    this.stopped = false;
    this.subscription = this.events?.on('node/peer-updated', card => this.onPeerUpdated(card));
    this.timer = setInterval(() => this.tick(), this.intervalMs);
    this.timer.unref();
  }

  private tick(): void {
    if (this.stopped || this.outbox.list({ status: 'queued' }).length === 0) {
      return;
    }
    void this.flush().catch(error => this.onError(error));
  }

  private onPeerUpdated(card: NodeCard): void {
    if (this.stopped) {
      return;
    }
    const queued = this.outbox.list({ status: 'queued' });
    if (
      !queued.some(
        item =>
          item.networkId === card.networkId &&
          (item.pendingNodeIds.length === 0 || item.pendingNodeIds.includes(card.nodeId)),
      )
    ) {
      return;
    }
    // Presence cards refresh several times a second. Retry at most once per peer per
    // reconnect pulse; the per-item backoff still governs ordinary timer attempts.
    const key = `${card.networkId}\n${card.nodeId}`;
    const now = Date.now();
    if (now - (this.lastPeerPulse.get(key) ?? 0) < 2_000) {
      return;
    }
    this.lastPeerPulse.set(key, now);
    void this.flush({ force: true }).catch(error => this.onError(error));
  }

  flush(options: OutboxFlushOptions = {}): Promise<OutboxFlushResult> {
    const run = this.queue.then(() => this.flushNow(options));
    this.queue = run.catch(() => {});
    return run;
  }

  private async flushNow(options: OutboxFlushOptions): Promise<OutboxFlushResult> {
    const now = Date.now();
    const candidates = this.outbox.list({ status: 'queued' }).filter(
      item => options.id === undefined || item.id === options.id,
    );
    let attempted = 0;
    let delivered = 0;
    const processed: OutboxItem[] = [];
    for (const candidate of candidates) {
      if (this.stopped) {
        break;
      }
      if (!options.force && Date.parse(candidate.nextAttemptAt) > now) {
        processed.push(candidate);
        continue;
      }
      attempted += 1;
      let item = candidate;
      let lastError: string | undefined;
      for (const [hash, path] of Object.entries(item.stagedFiles)) {
        try {
          this.transport.registerBlob?.(hash, path);
        } catch (error) {
          lastError = String(error);
        }
      }
      let targets = item.pendingNodeIds.filter(nodeId => !item.deliveredReceipts[nodeId]);
      if (targets.length === 0 && item.pendingNodeIds.length === 0) {
        // The selector was not resolvable when the envelope was emitted; retry routing now
        // that the peer directory may hold the target again.
        try {
          const decision = await this.router.route(item.envelope);
          if (decision.mode === 'broadcast' || decision.targetNodeIds.length === 0) {
            throw new Error('Outbox item has no addressable target');
          }
          targets = decision.targetNodeIds.filter(
            nodeId => !item.deliveredReceipts[nodeId],
          );
          item =
            this.outbox.recordAttempt(item.id, {
              pendingNodeIds: targets,
              nextAttemptAt: item.nextAttemptAt,
            }) ?? item;
        } catch (error) {
          lastError = String(error);
          item =
            this.outbox.recordAttempt(item.id, {
              lastError,
              nextAttemptAt: new Date(Date.now() + attemptDelayMs(item.attempts + 1)).toISOString(),
            }) ?? item;
          processed.push(item);
          continue;
        }
      }
      const relaySpooledNodeIds: string[] = [];
      for (const targetNodeId of targets) {
        try {
          const frame = Buffer.from(
            JSON.stringify({ type: 'message.envelope', envelope: item.envelope }),
          );
          const bytes = await this.transport.sendUnicast(item.networkId, targetNodeId, frame, {
            spoolBlobHashes: [
              ...new Set([
                ...Object.keys(item.stagedFiles),
                ...item.envelope.attachments.map(attachment => attachment.blake3Hash),
              ]),
            ],
          });
          const receipt = parseTargetReceipt(bytes, item.envelope, targetNodeId);
          // Any valid signed receipt is the live target confirmation the outbox awaited;
          // dedup-cas duplicate rejections are explicit confirmations, not failures.
          item =
            this.outbox.markNodeDelivered(item.id, targetNodeId, receipt) ?? item;
          delivered += 1;
        } catch (error) {
          if (isPeerOfflineError(error)) {
            if (isOfflinePeerSpooledError(error)) {
              relaySpooledNodeIds.push(targetNodeId);
            }
          } else {
            lastError = String(error);
          }
        }
      }
      const latest = this.outbox.get(item.id) ?? item;
      if (latest.status === 'queued') {
        const pendingNodeIds = targets.filter(
          targetNodeId => !latest.deliveredReceipts[targetNodeId],
        );
        item =
          this.outbox.recordAttempt(latest.id, {
            pendingNodeIds,
            ...(relaySpooledNodeIds.length ? { relaySpooledNodeIds } : {}),
            ...(lastError !== undefined ? { lastError } : {}),
            nextAttemptAt: new Date(
              Date.now() + attemptDelayMs(latest.attempts + 1),
            ).toISOString(),
          }) ?? latest;
      } else {
        item = latest;
      }
      processed.push(item);
    }
    return {
      attempted,
      delivered,
      stillQueued: this.outbox.list({ status: 'queued' }).length,
      items: processed,
    };
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer !== undefined) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    this.subscription?.dispose();
    this.subscription = undefined;
    await this.queue.catch(() => {});
  }
}
