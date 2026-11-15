import { existsSync, mkdirSync, readFileSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import {
  copyFileAtomically,
  withOwnedLock,
  writeFileAtomically,
} from '../fs-atomic/index.ts';
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
const DEFAULT_REDRIVE_INTERVAL_MS = 2_000;
const MIN_ATTEMPT_DELAY_MS = 2_000;
const MAX_ATTEMPT_DELAY_MS = 60_000;
const DEFAULT_REDRIVE_TIMEOUT_MS = 10_000;

export interface OutboxAttemptUpdate {
  pendingNodeIds?: string[];
  inDoubtNodeIds?: string[];
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
    (value.failedReceipts === undefined ||
      (isRecord(value.failedReceipts) &&
        Object.values(value.failedReceipts).every(validReceipt))) &&
    (value.failedNodeIds === undefined ||
      (Array.isArray(value.failedNodeIds) &&
        value.failedNodeIds.every(nodeId => typeof nodeId === 'string'))) &&
    (value.inDoubtNodeIds === undefined ||
      (Array.isArray(value.inDoubtNodeIds) &&
        value.inDoubtNodeIds.every(nodeId => typeof nodeId === 'string'))) &&
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

function normalizedItem(item: OutboxItem & {
  failedReceipts?: Record<string, DeliveryReceipt>;
  failedNodeIds?: string[];
  inDoubtNodeIds?: string[];
  failedReceipt?: { nodeId: string; receipt: DeliveryReceipt };
}): OutboxItem {
  const { failedReceipt, ...rest } = item;
  const failedReceipts = {
    ...(rest.failedReceipts ?? {}),
    ...(failedReceipt ? { [failedReceipt.nodeId]: failedReceipt.receipt } : {}),
  };
  const failedNodeIds = [...new Set([
    ...(rest.failedNodeIds ?? []),
    ...Object.keys(failedReceipts),
  ])];
  return {
    ...rest,
    failedReceipts,
    failedNodeIds,
    inDoubtNodeIds: [...new Set(rest.inDoubtNodeIds ?? [])],
    pendingNodeIds: rest.pendingNodeIds.filter(
      nodeId => !rest.deliveredReceipts[nodeId] && !failedNodeIds.includes(nodeId),
    ),
  };
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
    return withOwnedLock(this.lockDir, 'Outbox store', operation);
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
    return {
      schema: OUTBOX_SCHEMA,
      items: (parsed.items as OutboxItem[]).map(normalizedItem),
    };
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

  /**
   * One settlement rule for every mutation: delivered and terminal-failed targets leave the
   * pending set, a live obligation stays queued while any deliverable target remains, and
   * staged evidence is deleted only after every target delivered.
   */
  private settleItem(item: OutboxItem): void {
    item.pendingNodeIds = [...new Set(item.pendingNodeIds)].filter(
      nodeId => !item.deliveredReceipts[nodeId] && !item.failedNodeIds.includes(nodeId),
    );
    item.inDoubtNodeIds = [...new Set(item.inDoubtNodeIds)].filter(
      nodeId => item.pendingNodeIds.includes(nodeId),
    );
    if (item.pendingNodeIds.length > 0) {
      item.status = 'queued';
      return;
    }
    if (item.failedNodeIds.length > 0) {
      item.status = 'failed';
      // Staged files remain as evidence for every terminal target decision.
      return;
    }
    if (Object.keys(item.deliveredReceipts).length > 0) {
      item.status = 'delivered';
      if (Object.keys(item.stagedFiles).length > 0) {
        this.removeStagedFiles(item.id, item.stagedFiles);
        item.stagedFiles = {};
      }
      return;
    }
    if (item.status !== 'failed') {
      item.status = 'queued';
    }
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
      const failedReceipts = {
        ...(existing?.failedReceipts ?? {}),
        ...structuredClone(input.failedReceipts ?? {}),
      };
      const failedNodeIds = [...new Set([
        ...(existing?.failedNodeIds ?? []),
        ...(input.failedNodeIds ?? []),
        ...Object.keys(failedReceipts),
      ])].filter(nodeId => !deliveredReceipts[nodeId]);
      for (const nodeId of Object.keys(deliveredReceipts)) {
        delete failedReceipts[nodeId];
      }
      const pendingNodeIds = [
        ...new Set([...(input.pendingNodeIds ?? []), ...(existing?.pendingNodeIds ?? [])]),
      ].filter(
        nodeId => !deliveredReceipts[nodeId] && !failedNodeIds.includes(nodeId),
      );
      const inDoubtNodeIds = [
        ...new Set([
          ...(existing?.inDoubtNodeIds ?? []),
          ...(input.inDoubtNodeIds ?? []),
        ]),
      ].filter(nodeId => pendingNodeIds.includes(nodeId));
      const relaySpooledNodeIds = [
        ...new Set([
          ...(existing?.relaySpooledNodeIds ?? []),
          ...(input.relaySpooledNodeIds ?? []),
        ]),
      ];
      const stagedFiles = this.stageFiles(id, input.filePaths, existing?.stagedFiles ?? {});
      const item: OutboxItem = {
        id,
        networkId: input.envelope.networkId,
        envelope: structuredClone(input.envelope),
        explicitTargets: [...new Set(input.explicitTargets ?? [])],
        pendingNodeIds,
        deliveredReceipts,
        failedReceipts,
        failedNodeIds,
        inDoubtNodeIds,
        relaySpooledNodeIds,
        stagedFiles,
        status: 'queued',
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
      this.settleItem(item);
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

  /** Record one live target confirmation; other targets keep their own state. */
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
      delete item.failedReceipts[nodeId];
      item.failedNodeIds = item.failedNodeIds.filter(failed => failed !== nodeId);
      item.pendingNodeIds = item.pendingNodeIds.filter(pending => pending !== nodeId);
      item.inDoubtNodeIds = item.inDoubtNodeIds.filter(pending => pending !== nodeId);
      item.updatedAt = now;
      item.nextAttemptAt = now;
      if (item.failedNodeIds.length === 0) {
        item.lastError = undefined;
      }
      this.settleItem(item);
      return clone(item);
    });
  }

  /**
   * Record one target's terminal decision without abandoning the other multicast targets.
   * A signed rejection/failure is preserved verbatim as evidence; a local integrity failure
   * marks the target failed without inventing a receipt.
   */
  markNodeFailed(
    id: string,
    nodeId: string,
    lastError: string,
    receipt?: DeliveryReceipt,
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
      item.failedNodeIds = [...new Set([...item.failedNodeIds, nodeId])];
      if (receipt) {
        item.failedReceipts = {
          ...item.failedReceipts,
          [nodeId]: structuredClone(receipt),
        };
      }
      item.pendingNodeIds = item.pendingNodeIds.filter(pending => pending !== nodeId);
      item.inDoubtNodeIds = item.inDoubtNodeIds.filter(pending => pending !== nodeId);
      item.lastError = lastError;
      item.updatedAt = now;
      item.nextAttemptAt = now;
      this.settleItem(item);
      return clone(item);
    });
  }

  /** A local failure that prevents every remaining target from being attempted. */
  markFailed(id: string, lastError: string): OutboxItem | undefined {
    const now = new Date().toISOString();
    return this.mutate(state => {
      const item = state.items.find(candidate => candidate.id === id);
      if (!item) {
        return undefined;
      }
      item.status = 'failed';
      item.lastError = lastError;
      item.pendingNodeIds = [];
      item.inDoubtNodeIds = [];
      item.updatedAt = now;
      item.nextAttemptAt = now;
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
        item.pendingNodeIds = [...new Set(update.pendingNodeIds)];
      }
      if (update.inDoubtNodeIds !== undefined) {
        item.inDoubtNodeIds = [...new Set(update.inDoubtNodeIds)];
      }
      if (update.relaySpooledNodeIds?.length) {
        item.relaySpooledNodeIds = [
          ...new Set([...item.relaySpooledNodeIds, ...update.relaySpooledNodeIds]),
        ];
      }
      if (update.lastError === undefined) {
        if (item.failedNodeIds.length === 0) {
          item.lastError = undefined;
        }
      } else {
        item.lastError = update.lastError;
      }
      item.attempts += 1;
      item.updatedAt = now;
      item.nextAttemptAt = update.nextAttemptAt;
      this.settleItem(item);
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
      const eligibleTargets = candidate.pendingNodeIds.filter(
        nodeId =>
          !candidate.deliveredReceipts[nodeId] &&
          !candidate.failedNodeIds.includes(nodeId),
      );
      const requestTargets = options.forceMatchingTarget && options.targetNodeId
        ? eligibleTargets.filter(nodeId => nodeId === options.targetNodeId)
        : options.force
          // An explicit operator flush resolves in-doubt sends through the same stable
          // envelope id; the target's dedup policy decides a duplicate safely.
          ? eligibleTargets
          // Timed-out sends may already have executed. Only a direct peer pulse or an
          // operator force may retry them; the ordinary timer keeps them parked.
          : eligibleTargets.filter(
              nodeId => !candidate.inDoubtNodeIds.includes(nodeId),
            );
      if (eligibleTargets.length > 0 && requestTargets.length === 0) {
        processed.push(candidate);
        continue;
      }
      attempted += 1;
      let outcome: OutboxDeliveryOutcome;
      try {
        // The emitter owns the total per-attempt deadline and returns every receipt it
        // already obtained; an outer race here would discard confirmed targets and resend.
        outcome = await this.deliver(candidate, {
          // An empty list means "route the unresolved selector", not "deliver nowhere".
          ...(requestTargets.length > 0 ? { targetNodeIds: requestTargets } : {}),
          timeoutMs: this.attemptTimeoutMs,
        });
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
      let terminalError: string | undefined;
      for (const failure of outcome.failures ?? []) {
        if (failure.nodeId !== undefined) {
          item = this.outbox.markNodeFailed(
            item.id,
            failure.nodeId,
            failure.error,
            failure.receipt,
          ) ?? item;
        } else {
          item = this.outbox.markFailed(item.id, failure.error) ?? item;
        }
        terminalError ??= failure.error;
      }
      const resolved = outcome.resolvedTargetNodeIds;
      if (resolved?.length && candidate.pendingNodeIds.length === 0) {
        // The initial emit recorded an empty outbound route; the peer directory now knows
        // the target, so converge that durable reply route before declaring settlement.
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
      const settledNodeIds = new Set([
        ...Object.keys(item.deliveredReceipts),
        ...item.failedNodeIds,
      ]);
      const pendingNodeIds = [
        ...new Set(
          (outcome.pendingNodeIds ?? resolved ?? item.pendingNodeIds).filter(
            nodeId => !settledNodeIds.has(nodeId),
          ),
        ),
      ];
      const inDoubtNodeIds = (
        outcome.inDoubtNodeIds !== undefined
          ? outcome.inDoubtNodeIds
          : item.inDoubtNodeIds
      ).filter(nodeId => pendingNodeIds.includes(nodeId));
      item = this.outbox.recordAttempt(item.id, {
        pendingNodeIds,
        inDoubtNodeIds,
        ...(outcome.relaySpooledNodeIds?.length
          ? { relaySpooledNodeIds: [...outcome.relaySpooledNodeIds] }
          : {}),
        ...(outcome.lastError ?? terminalError !== undefined
          ? { lastError: outcome.lastError ?? terminalError }
          : {}),
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
