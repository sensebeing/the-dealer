import { existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { copyFileAtomically, writeFileAtomically } from '../core-types/index.ts';
import type {
  DeliveryReceipt,
  Disposable,
  MessageEnvelope,
  NodeCard,
  OutboxDeliveryOutcome,
  OutboxEnqueueInput,
  OutboxFlushOptions,
  OutboxFlushResult,
  OutboxItem,
  OutboxItemStatus,
  OutboxListFilter,
} from '../core-types/index.ts';
import {
  holdOwnedDirectory,
  isDeadProcessOwner,
  processOwnerRecord,
  readProcessOwner,
  releaseOwnedDirectory,
} from '../process-lock/index.ts';

export type {
  OutboxDeliveryFailure,
  OutboxDeliveryOutcome,
  OutboxEnqueueInput,
  OutboxFlushOptions,
  OutboxFlushResult,
  OutboxItem,
  OutboxItemStatus,
  OutboxListFilter,
} from '../core-types/index.ts';

export const OUTBOX_SCHEMA = 'the-dealer-outbox/v1';
const LOCK_RETRY_MS = 20;
const LOCK_TIMEOUT_MS = 2_000;
const STALE_LOCK_MS = 30_000;
const DEFAULT_REDRIVE_INTERVAL_MS = 2_000;
const MIN_ATTEMPT_DELAY_MS = 2_000;
const MAX_ATTEMPT_DELAY_MS = 60_000;
const DEFAULT_REDRIVE_TIMEOUT_MS = 10_000;

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
    (value.lastError === undefined || typeof value.lastError === 'string') &&
    (value.failedReceipt === undefined ||
      (isRecord(value.failedReceipt) &&
        typeof value.failedReceipt.nodeId === 'string' &&
        validReceipt(value.failedReceipt.receipt)))
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
    writeFileAtomically(this.path, JSON.stringify(state));
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
      copyFileAtomically(source, destination);
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
        ...(existing?.failedReceipt !== undefined
          ? { failedReceipt: existing.failedReceipt }
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

  /** Reuse the one queued obligation for an idempotency key instead of duplicating it. */
  findQueuedByIdempotency(
    networkId: string,
    idempotencyKey: string,
  ): OutboxItem | undefined {
    if (!idempotencyKey) {
      return undefined;
    }
    const item = this.read().items.find(
      candidate =>
        candidate.status === 'queued' &&
        candidate.networkId === networkId &&
        candidate.envelope.idempotencyKey === idempotencyKey,
    );
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

  /** A signed rejection/failure or local integrity error: stop retries, preserve evidence. */
  markFailed(
    id: string,
    lastError: string,
    failedReceipt?: { nodeId: string; receipt: DeliveryReceipt },
  ): OutboxItem | undefined {
    const now = new Date().toISOString();
    return this.mutate(state => {
      const item = state.items.find(candidate => candidate.id === id);
      if (!item) {
        return undefined;
      }
      item.status = 'failed';
      item.lastError = lastError;
      item.updatedAt = now;
      item.nextAttemptAt = now;
      if (failedReceipt) {
        item.failedReceipt = {
          nodeId: failedReceipt.nodeId,
          receipt: structuredClone(failedReceipt.receipt),
        };
      }
      // Staged files are deliberately retained as evidence for operator inspection.
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

export interface OutboxRedriveEvents {
  on(
    event: 'node/peer-updated',
    listener: (card: NodeCard, provenance?: { direct?: boolean }) => void,
  ): Disposable;
}

export interface OutboxRedriveOptions {
  outbox: OutboxStore;
  /** Emitter-owned delivery: routing, envelope framing, transport send and receipt checks. */
  deliver: (
    item: OutboxItem,
    options?: { targetNodeIds?: readonly string[]; timeoutMs?: number },
  ) => Promise<OutboxDeliveryOutcome>;
  events?: OutboxRedriveEvents;
  onError?: (error: unknown) => void;
  /** Called once a previously unresolved explicit selector has produced real targets. */
  onTargetsResolved?: (
    envelope: MessageEnvelope,
    targetNodeIds: readonly string[],
  ) => void;
  intervalMs?: number;
  attemptTimeoutMs?: number;
}

function attemptDelayMs(attempts: number): number {
  return Math.min(MAX_ATTEMPT_DELAY_MS, MIN_ATTEMPT_DELAY_MS * 2 ** Math.min(attempts, 5));
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), timeoutMs);
    timer.unref();
  });
  return Promise.race([promise, timeout]).finally(() => {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  });
}

/**
 * Sole durable-obligation owner for the outbox. The emitter owns routing, framing and
 * receipt verification through `deliver`; this service only persists obligations,
 * schedules backoff and scopes peer-triggered retries to the peer that reconnected.
 */
export class OutboxRedriveService {
  private readonly outbox: OutboxStore;
  private readonly deliver: OutboxRedriveOptions['deliver'];
  private readonly events?: OutboxRedriveEvents;
  private readonly onError: (error: unknown) => void;
  private readonly onTargetsResolved?: (
    envelope: MessageEnvelope,
    targetNodeIds: readonly string[],
  ) => void;
  private readonly intervalMs: number;
  private readonly attemptTimeoutMs: number;
  private timer?: ReturnType<typeof setInterval>;
  private subscription?: Disposable;
  private queue: Promise<unknown> = Promise.resolve();
  private stopped = false;
  private lastPeerPulse = new Map<string, number>();

  constructor(options: OutboxRedriveOptions) {
    this.outbox = options.outbox;
    this.deliver = options.deliver;
    this.events = options.events;
    this.onError = options.onError ?? (() => {});
    this.onTargetsResolved = options.onTargetsResolved;
    this.intervalMs = options.intervalMs ?? DEFAULT_REDRIVE_INTERVAL_MS;
    this.attemptTimeoutMs = options.attemptTimeoutMs ?? DEFAULT_REDRIVE_TIMEOUT_MS;
  }

  start(): void {
    if (this.timer !== undefined) {
      return;
    }
    this.stopped = false;
    this.subscription = this.events?.on('node/peer-updated', (card, provenance) =>
      this.onPeerUpdated(card, provenance),
    );
    this.timer = setInterval(() => this.tick(), this.intervalMs);
    this.timer.unref();
  }

  private tick(): void {
    if (this.stopped || this.outbox.list({ status: 'queued' }).length === 0) {
      return;
    }
    void this.flush().catch(error => this.onError(error));
  }

  private onPeerUpdated(card: NodeCard, provenance?: { direct?: boolean }): void {
    // Presence gossip proves identity, not reachability: a gossiped third-party card must
    // never advance the retry clock for a target that has not actually reconnected.
    if (this.stopped || provenance?.direct === false) {
      return;
    }
    // Throttle before touching the outbox: presence cards refresh several times a second.
    const key = `${card.networkId}\n${card.nodeId}`;
    const now = Date.now();
    if (now - (this.lastPeerPulse.get(key) ?? 0) < 2_000) {
      return;
    }
    this.lastPeerPulse.set(key, now);
    void this.flush({
      networkId: card.networkId,
      targetNodeId: card.nodeId,
      force: true,
      forceMatchingTarget: true,
    }).catch(error => this.onError(error));
  }

  flush(options: OutboxFlushOptions = {}): Promise<OutboxFlushResult> {
    const run = this.queue.then(() => this.flushNow(options));
    this.queue = run.catch(() => {});
    return run;
  }

  private async flushNow(options: OutboxFlushOptions): Promise<OutboxFlushResult> {
    const now = Date.now();
    let candidates = this.outbox
      .list({ status: 'queued', ...(options.networkId ? { networkId: options.networkId } : {}) })
      .filter(item => options.id === undefined || item.id === options.id);
    if (options.forceMatchingTarget) {
      const targetNodeId = options.targetNodeId;
      if (!targetNodeId) {
        throw new Error('forceMatchingTarget requires targetNodeId');
      }
      // Only this target's item (or an unresolved explicit selector, which may now match
      // the newly reachable peer) participates. Unrelated backoff stays untouched.
      candidates = candidates.filter(
        item =>
          item.pendingNodeIds.includes(targetNodeId) || item.pendingNodeIds.length === 0,
      );
    }
    let attempted = 0;
    let delivered = 0;
    const processed: OutboxItem[] = [];
    for (const candidate of candidates) {
      if (this.stopped) {
        break;
      }
      // A forced peer flush bypasses only the matching item's schedule; ordinary timer
      // flushes still honour each item's exponential backoff.
      const matchesTarget =
        !options.forceMatchingTarget ||
        candidate.pendingNodeIds.length === 0 ||
        candidate.pendingNodeIds.includes(options.targetNodeId!);
      const force = options.force && matchesTarget;
      if (!force && Date.parse(candidate.nextAttemptAt) > now) {
        processed.push(candidate);
        continue;
      }
      attempted += 1;
      const requestTargets = options.forceMatchingTarget && options.targetNodeId
        ? candidate.pendingNodeIds.filter(
            nodeId =>
              nodeId === options.targetNodeId && !candidate.deliveredReceipts[nodeId],
          )
        : candidate.pendingNodeIds.filter(nodeId => !candidate.deliveredReceipts[nodeId]);
      let outcome: OutboxDeliveryOutcome;
      try {
        outcome = await withTimeout(
          this.deliver(candidate, {
            // An empty list means "route the unresolved selector", not "deliver nowhere".
            ...(requestTargets.length > 0 ? { targetNodeIds: requestTargets } : {}),
            timeoutMs: this.attemptTimeoutMs,
          }),
          this.attemptTimeoutMs,
          'Outbox redrive attempt timed out',
        );
      } catch (error) {
        const latest = this.outbox.get(candidate.id) ?? candidate;
        const failed = this.outbox.recordAttempt(latest.id, {
          lastError: String(error),
          nextAttemptAt: new Date(
            Date.now() + attemptDelayMs(latest.attempts + 1),
          ).toISOString(),
        }) ?? latest;
        processed.push(failed);
        continue;
      }
      let item = this.outbox.get(candidate.id) ?? candidate;
      for (const [nodeId, receipt] of Object.entries(outcome.deliveredReceipts ?? {})) {
        item = this.outbox.markNodeDelivered(item.id, nodeId, receipt) ?? item;
        delivered += 1;
      }
      if (outcome.failed) {
        const error = outcome.failed.error || outcome.lastError || 'Outbox redrive failed';
        item = this.outbox.markFailed(
          item.id,
          error,
          outcome.failed.receipt && outcome.failed.nodeId
            ? { nodeId: outcome.failed.nodeId, receipt: outcome.failed.receipt }
            : undefined,
        ) ?? item;
        processed.push(item);
        continue;
      }
      const resolved = outcome.resolvedTargetNodeIds;
      if (resolved?.length && candidate.pendingNodeIds.length === 0) {
        // The initial emit recorded an empty outbound route; the peer directory now knows
        // the target, so converge that durable reply route before sending.
        try {
          this.onTargetsResolved?.(candidate.envelope, resolved);
        } catch (error) {
          this.onError(error);
        }
      }
      if (item.status !== 'queued') {
        processed.push(item);
        continue;
      }
      const deliveredNodeIds = new Set(Object.keys(item.deliveredReceipts));
      const pendingNodeIds = [
        ...new Set(
          (outcome.pendingNodeIds ?? resolved ?? item.pendingNodeIds).filter(
            nodeId => !deliveredNodeIds.has(nodeId),
          ),
        ),
      ];
      item = this.outbox.recordAttempt(item.id, {
        pendingNodeIds,
        ...(outcome.relaySpooledNodeIds?.length
          ? { relaySpooledNodeIds: [...outcome.relaySpooledNodeIds] }
          : {}),
        ...(outcome.lastError !== undefined ? { lastError: outcome.lastError } : {}),
        nextAttemptAt: new Date(
          Date.now() + attemptDelayMs(item.attempts + 1),
        ).toISOString(),
      }) ?? item;
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
