import { existsSync, mkdirSync, readFileSync, rmSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { join, resolve } from 'node:path';
import {
  envelopeClaimVersion, isConfirmedHandoffReceipt, isRetryablePreEffectReceipt, isUnattemptedOutboxItem,
  outboxReconciliationTargets, sameEnvelopeIntent,
} from '../core-types/index.ts';
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
  OutboxAttemptOptions,
  OutboxClaimInput,
  OutboxClaimResult,
  OutboxDeliveryOutcome,
  OutboxEnqueueInput,
  OutboxFlushOptions,
  OutboxFlushResult,
  OutboxItem,
  OutboxItemStatus,
  OutboxListFilter,
  PeerReachableEvent,
} from '../core-types/index.ts';
import { netKey } from './network-key.ts';

export type {
  OutboxDeliveryFailure,
  OutboxDeliveryOutcome,
  OutboxEnqueueInput,
  OutboxClaimInput,
  OutboxClaimResult,
  OutboxFlushOptions,
  OutboxFlushResult,
  OutboxItem,
  OutboxItemStatus,
  OutboxListFilter,
} from '../core-types/index.ts';

/** The same lookup governs read-only replay and atomic creation under the store lock. */
function highestKeyedObligation(items: readonly OutboxItem[], networkId: string, key: string, minVersion: number): OutboxItem | undefined {
  let existing: OutboxItem | undefined;
  let highestVersion = minVersion;
  for (const item of items) {
    if (item.supersededById !== undefined || item.networkId !== networkId || item.envelope.idempotencyKey !== key) continue;
    const version = envelopeClaimVersion(item.envelope) ?? 0;
    if (version >= highestVersion) {
      existing = item;
      highestVersion = version;
    }
  }
  return existing;
}

function keyedClaim(input: OutboxClaimInput): { key: string; version: number } {
  if (!validEnvelope(input?.envelope)) throw new Error('Invalid outbox envelope');
  const key = input.envelope.idempotencyKey;
  if (!key) throw new Error('Outbox claim requires an idempotency key');
  const version = envelopeClaimVersion(input.envelope);
  if (version === null) throw new Error('Invalid claim version');
  return { key, version };
}

function claimDeadline(leaseMs: number | undefined, now = Date.now()): string {
  return new Date(now + (leaseMs !== undefined && Number.isFinite(leaseMs) && leaseMs > 0
    ? leaseMs : DEFAULT_CLAIM_LEASE_MS)).toISOString();
}

export const OUTBOX_SCHEMA = 'the-dealer-outbox/v1';
const DEFAULT_REDRIVE_INTERVAL_MS = 2_000;
const MIN_ATTEMPT_DELAY_MS = 2_000;
const MAX_ATTEMPT_DELAY_MS = 60_000;
const DEFAULT_REDRIVE_TIMEOUT_MS = 10_000;
const DEFAULT_CLAIM_LEASE_MS = 60_000;

export interface OutboxAttemptUpdate {
  deliveredReceipts?: OutboxDeliveryOutcome['deliveredReceipts'];
  failures?: OutboxDeliveryOutcome['failures'];
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

function hasActiveAttempt(item: OutboxItem, now = Date.now()): boolean {
  return item.activeAttempt !== undefined && Date.parse(item.activeAttempt.expiresAt) > now;
}

function acceptsSettlement(item: OutboxItem | undefined, attemptToken: string | undefined): boolean {
  if (!item) return attemptToken === undefined;
  return item.supersededById === undefined && (attemptToken === undefined
    ? !hasActiveAttempt(item)
    : item.activeAttempt?.token === attemptToken);
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
    isRecord(value.failedReceipts) &&
    Object.values(value.failedReceipts).every(validReceipt) &&
    Array.isArray(value.failedNodeIds) &&
    value.failedNodeIds.every(nodeId => typeof nodeId === 'string') &&
    Array.isArray(value.inDoubtNodeIds) &&
    value.inDoubtNodeIds.every(nodeId => typeof nodeId === 'string') &&
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
    (value.supersededById === undefined ||
      (typeof value.supersededById === 'string' && value.supersededById.length > 0 && value.supersededById !== value.id)) &&
    (value.activeAttempt === undefined || (
      isRecord(value.activeAttempt) &&
      typeof value.activeAttempt.token === 'string' && value.activeAttempt.token.length > 0 &&
      typeof value.activeAttempt.expiresAt === 'string' && Number.isFinite(Date.parse(value.activeAttempt.expiresAt))
    ))
  );
}

function normalizeTargets(item: OutboxItem): OutboxItem {
  item.pendingNodeIds = [...new Set(item.pendingNodeIds)].filter(
    nodeId => !item.deliveredReceipts[nodeId] && !item.failedNodeIds.includes(nodeId),
  );
  // A later rejection cannot disprove an earlier delivery whose reply timed out.
  item.inDoubtNodeIds = [...new Set(item.inDoubtNodeIds)].filter(nodeId => !item.deliveredReceipts[nodeId]);
  return item;
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
      items: (parsed.items as OutboxItem[]).map(normalizeTargets),
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
    normalizeTargets(item);
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
      if (Object.keys(item.stagedFiles).length > 0 &&
          Object.values(item.deliveredReceipts).every(receipt => isConfirmedHandoffReceipt(receipt, { allowQueued: false }))) {
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
      if (!acceptsSettlement(existing, input.attemptToken)) {
        throw new Error(`Outbox attempt no longer owns obligation ${id}`);
      }
      // Retryable outcomes can be reopened. Keep durable custody and unresolved effect evidence.
      for (const nodeId of input.reconcileNodeIds ?? []) {
        if (!existing) continue;
        const failure = existing.failedReceipts[nodeId];
        if (!failure || isRetryablePreEffectReceipt(failure)) {
          delete existing.failedReceipts[nodeId];
          existing.failedNodeIds = existing.failedNodeIds.filter(id => id !== nodeId);
        }
        existing.pendingNodeIds = existing.pendingNodeIds.filter(id => id !== nodeId);
        existing.relaySpooledNodeIds = existing.relaySpooledNodeIds.filter(id => id !== nodeId);
      }
      const pendingNodeIds = [...(input.pendingNodeIds ?? []), ...(existing?.pendingNodeIds ?? [])];
      const inDoubtNodeIds = [...(existing?.inDoubtNodeIds ?? []), ...(input.inDoubtNodeIds ?? [])];
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
        deliveredReceipts: existing?.deliveredReceipts ?? {},
        failedReceipts: existing?.failedReceipts ?? {},
        failedNodeIds: existing?.failedNodeIds ?? [],
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
      for (const [nodeId, receipt] of Object.entries(input.deliveredReceipts ?? {})) {
        this.applyNodeDelivery(item, nodeId, receipt);
      }
      for (const nodeId of new Set([...(input.failedNodeIds ?? []), ...Object.keys(input.failedReceipts ?? {})])) {
        this.applyNodeFailure(item, nodeId, input.failedReceipts?.[nodeId]);
      }
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

  /** Failures with no remaining target, uncertain attempt, or confirmed delivery. */
  readDefinitivelyFailedKeys(): ReadonlySet<string> {
    const keys = new Set<string>();
    for (const item of this.read().items) {
      if (item.status === 'failed' && item.pendingNodeIds.length === 0 &&
        item.inDoubtNodeIds.length === 0 && Object.keys(item.deliveredReceipts).length === 0) {
        keys.add(netKey(item.networkId, item.id));
      }
    }
    return keys;
  }

  findKeyedObligation(networkId: string, key: string, minVersion: number): OutboxItem | undefined {
    const item = highestKeyedObligation(this.read().items, networkId, key, minVersion);
    return item ? clone(item) : undefined;
  }

  /**
   * Atomically create work or join the highest reusable obligation version of a key. The
   * check and the reservation happen under the same store lock, so concurrent emitters
   * cannot both observe "absent" and mint different envelopes/obligations. A claimed item
   * starts with no confirmed delivery and a lease: if the process dies before the first
   * send/merge, ordinary redrive later sends the same signed envelope at least once.
   */
  claimIdempotency(input: OutboxClaimInput): OutboxClaimResult {
    const { key, version } = keyedClaim(input);
    return this.mutate(state => {
      const existing = highestKeyedObligation(state.items, input.envelope.networkId, key, version);
      if (existing) {
        return { claimed: false, item: clone(existing) };
      }
      if (state.items.some(item => item.id === input.envelope.id)) throw new Error('Outbox claim id is already in use');
      const activeAttempt = this.newAttempt(input.leaseMs);
      const item = this.createClaim(input, activeAttempt);
      state.items.push(item);
      return { claimed: true, item: clone(item), attemptToken: activeAttempt.token };
    });
  }

  private newAttempt(leaseMs?: number, now?: number): NonNullable<OutboxItem['activeAttempt']> {
    return { token: randomUUID(), expiresAt: claimDeadline(leaseMs, now) };
  }

  private createClaim(input: OutboxClaimInput, activeAttempt: NonNullable<OutboxItem['activeAttempt']>): OutboxItem {
    const { envelope } = input;
    const now = Date.now();
    const createdAt = new Date(now).toISOString();
    return {
      id: envelope.id,
      networkId: envelope.networkId,
      envelope: clone(envelope),
      explicitTargets: [...new Set(input.explicitTargets ?? [])],
      pendingNodeIds: [],
      deliveredReceipts: {},
      failedReceipts: {},
      failedNodeIds: [],
      inDoubtNodeIds: [],
      relaySpooledNodeIds: [],
      stagedFiles: this.stageFiles(envelope.id, input.filePaths, {}),
      status: 'queued',
      attempts: 0,
      createdAt,
      updatedAt: createdAt,
      nextAttemptAt: activeAttempt.expiresAt,
      activeAttempt,
    };
  }

  /** Same-id retries lease the original targets, so they cannot race a retargeted send. */
  replaceUnattemptedIdempotency(input: OutboxClaimInput & { expectedId?: string }): OutboxClaimResult | undefined {
    const { key, version } = keyedClaim(input);
    return this.withFileLock(() => {
      const state = this.read();
      const existing = highestKeyedObligation(state.items, input.envelope.networkId, key, version);
      if (!existing) return undefined;
      const now = Date.now();
      if (hasActiveAttempt(existing, now) || (input.expectedId !== undefined && existing.id !== input.expectedId) ||
          envelopeClaimVersion(existing.envelope) !== version ||
          !isUnattemptedOutboxItem(existing)) return { claimed: false, item: clone(existing) };

      const retryOriginal = existing.id === input.envelope.id;
      const activeAttempt = this.newAttempt(input.leaseMs, now);
      let item: OutboxItem;
      if (retryOriginal) {
        if (existing.envelope.signature !== input.envelope.signature) return { claimed: false, item: clone(existing) };
        item = {
          ...existing,
          pendingNodeIds: [...new Set([...existing.failedNodeIds, ...Object.keys(existing.failedReceipts)])],
          failedNodeIds: [],
          failedReceipts: {},
          status: 'queued',
          lastError: undefined,
          updatedAt: new Date(now).toISOString(),
          nextAttemptAt: activeAttempt.expiresAt,
          activeAttempt,
        };
        state.items[state.items.indexOf(existing)] = item;
      } else {
        if (sameEnvelopeIntent(existing.envelope, input.envelope)) return { claimed: false, item: clone(existing) };
        if (state.items.some(candidate => candidate.id === input.envelope.id)) throw new Error('Outbox replacement id is already in use');
        item = this.createClaim(input, activeAttempt);
        state.items[state.items.indexOf(existing)] = {
          ...existing, supersededById: item.id, activeAttempt: undefined, stagedFiles: {},
        };
        state.items.push(item);
      }
      // The old attachment copies remain valid until the new obligation is durably published.
      this.save(state);
      if (!retryOriginal) this.removeStagedFiles(existing.id, existing.stagedFiles);
      return { claimed: true, item: clone(item), attemptToken: activeAttempt.token };
    });
  }

  beginAttempt(id: string, options: OutboxAttemptOptions = {}): {
    item: OutboxItem; attemptToken: string;
  } | undefined {
    return this.withFileLock(() => {
      const state = this.read();
      const item = state.items.find(candidate => candidate.id === id);
      const now = options.now ?? Date.now();
      if (!item || item.supersededById !== undefined || hasActiveAttempt(item, now)) {
        return undefined;
      }
      const eligible = options.reconcile === true ? outboxReconciliationTargets(item).length > 0 : item.status === 'queued';
      if (!eligible) return undefined;
      const activeAttempt = this.newAttempt(options.leaseMs, now);
      item.activeAttempt = activeAttempt;
      this.save(state);
      return { item: clone(item), attemptToken: activeAttempt.token };
    });
  }

  releaseAttempt(id: string, attemptToken: string): void {
    this.mutateAttempt(id, attemptToken, item => {
      delete item.activeAttempt;
    });
  }

  private mutateAttempt<T>(
    id: string, attemptToken: string | undefined, operation: (item: OutboxItem) => T,
  ): T | undefined {
    return this.withFileLock(() => {
      const state = this.read();
      const item = state.items.find(candidate => candidate.id === id);
      if (!item || !acceptsSettlement(item, attemptToken)) return undefined;
      const result = operation(item);
      this.save(state);
      return result;
    });
  }

  /**
   * Explicit operator retry for deterministic local failures. Only unsigned local-failure
   * targets move back to pending; signed terminal rejections stay terminal evidence.
   */
  requeue(id: string): OutboxItem | undefined {
    const now = new Date().toISOString();
    return this.mutateAttempt(id, undefined, item => {
      if (item.status !== 'failed') {
        throw new Error('Only a failed outbox item can be requeued');
      }
      // Signed receipts are terminal evidence and never reopen. Unsigned failed targets and
      // unresolved pending targets are the operator's recoverable obligation.
      const retryable = item.failedNodeIds.filter(
        nodeId =>
          !item.failedReceipts[nodeId] &&
          !item.deliveredReceipts[nodeId],
      );
      const pending = item.pendingNodeIds.filter(
        nodeId =>
          !item.deliveredReceipts[nodeId] &&
          !item.failedNodeIds.includes(nodeId),
      );
      // An item-level failure with no target-level evidence can mean an unresolved selector
      // that must re-enter routing. A fully delivered item has nothing to recover.
      const reroutable =
        item.failedNodeIds.length === 0 &&
        Object.keys(item.deliveredReceipts).length === 0;
      if (retryable.length === 0 && pending.length === 0 && !reroutable) {
        throw new Error('Outbox item has no locally failed targets to requeue');
      }
      const retry = new Set(retryable);
      item.failedNodeIds = item.failedNodeIds.filter(nodeId => !retry.has(nodeId));
      item.pendingNodeIds = [...new Set([...pending, ...retryable])];
      item.status = 'queued';
      delete item.activeAttempt;
      item.lastError = undefined;
      item.updatedAt = now;
      item.nextAttemptAt = now;
      // Deliberately no settleItem: an unresolved item with no pending targets must stay
      // queued for routing, and a partial delivery must never settle as delivered here.
      return clone(item);
    });
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
    attemptToken?: string,
  ): OutboxItem | undefined {
    if (typeof nodeId !== 'string' || !nodeId) {
      throw new Error('Invalid outbox target');
    }
    const now = new Date().toISOString();
    return this.mutateAttempt(id, attemptToken, item => {
      this.applyNodeDelivery(item, nodeId, receipt);
      delete item.activeAttempt;
      item.updatedAt = now;
      item.nextAttemptAt = now;
      if (item.failedNodeIds.length === 0) {
        item.lastError = undefined;
      }
      this.settleItem(item);
      return clone(item);
    });
  }

  private applyNodeDelivery(item: OutboxItem, nodeId: string, receipt: DeliveryReceipt): void {
    if (!isConfirmedHandoffReceipt(receipt, { allowQueued: true }) ||
        isConfirmedHandoffReceipt(item.deliveredReceipts[nodeId], { allowQueued: false })) return;
    item.deliveredReceipts[nodeId] = structuredClone(receipt);
    delete item.failedReceipts[nodeId];
    item.failedNodeIds = item.failedNodeIds.filter(failed => failed !== nodeId);
  }

  private applyNodeFailure(item: OutboxItem, nodeId: string, receipt?: DeliveryReceipt): void {
    if (isConfirmedHandoffReceipt(item.deliveredReceipts[nodeId], { allowQueued: true })) return;
    item.failedNodeIds = [...new Set([...item.failedNodeIds, nodeId])];
    const prior = item.failedReceipts[nodeId];
    if (receipt && (!prior || isRetryablePreEffectReceipt(prior) || !isRetryablePreEffectReceipt(receipt))) {
      item.failedReceipts[nodeId] = structuredClone(receipt);
    }
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
    attemptToken?: string,
  ): OutboxItem | undefined {
    if (typeof nodeId !== 'string' || !nodeId) {
      throw new Error('Invalid outbox target');
    }
    const now = new Date().toISOString();
    return this.mutateAttempt(id, attemptToken, item => {
      this.applyNodeFailure(item, nodeId, receipt);
      delete item.activeAttempt;
      item.lastError = lastError;
      item.updatedAt = now;
      item.nextAttemptAt = now;
      this.settleItem(item);
      return clone(item);
    });
  }

  /**
   * An item-level failure with no per-target signed decision. Unresolved targets and their
   * in-doubt flags are retained as the operator's recovery obligation: an explicit requeue
   * resumes routing without ever resurrecting a target that already has terminal evidence.
   */
  markFailed(id: string, lastError: string, attemptToken?: string): OutboxItem | undefined {
    const now = new Date().toISOString();
    return this.mutateAttempt(id, attemptToken, item => {
      delete item.activeAttempt;
      item.status = 'failed';
      item.lastError = lastError;
      item.updatedAt = now;
      item.nextAttemptAt = now;
      // Staged files are deliberately retained as evidence for operator inspection.
      return clone(item);
    });
  }

  recordAttempt(id: string, update: OutboxAttemptUpdate, attemptToken?: string): OutboxItem | undefined {
    if (typeof update?.nextAttemptAt !== 'string' || !Number.isFinite(Date.parse(update.nextAttemptAt))) {
      throw new Error('Invalid outbox attempt schedule');
    }
    const now = new Date().toISOString();
    return this.mutateAttempt(id, attemptToken, item => {
      for (const [nodeId, receipt] of Object.entries(update.deliveredReceipts ?? {})) {
        this.applyNodeDelivery(item, nodeId, receipt);
      }
      let itemFailure: string | undefined;
      for (const failure of update.failures ?? []) {
        if (failure.nodeId !== undefined) this.applyNodeFailure(item, failure.nodeId, failure.receipt);
        else itemFailure ??= failure.error;
      }
      if (update.pendingNodeIds !== undefined) {
        item.pendingNodeIds = [...new Set(update.pendingNodeIds)];
      }
      if (update.inDoubtNodeIds !== undefined) {
        item.inDoubtNodeIds.push(...update.inDoubtNodeIds);
      }
      if (update.relaySpooledNodeIds?.length) {
        item.relaySpooledNodeIds = [
          ...new Set([...item.relaySpooledNodeIds, ...update.relaySpooledNodeIds]),
        ];
      }
      const lastError = itemFailure ?? update.lastError ?? update.failures?.[0]?.error;
      if (lastError === undefined) {
        if (item.failedNodeIds.length === 0) {
          item.lastError = undefined;
        }
      } else {
        item.lastError = lastError;
      }
      item.attempts += 1;
      item.updatedAt = now;
      item.nextAttemptAt = update.nextAttemptAt;
      delete item.activeAttempt;
      this.settleItem(item);
      if (itemFailure !== undefined) item.status = 'failed';
      return clone(item);
    });
  }
}

/**
 * The two live-obligation pulses this service consumes. `node/peer-updated` is signed
 * identity (whose `revision` survives sleep/wake unchanged); `transport/peer-reachable` is
 * transport-owned liveness evidence from a fresh beacon, relay gossip, or a peer tunnel.
 */
export type OutboxRedriveEventMap = {
  'node/peer-updated': [card: NodeCard, provenance?: { direct?: boolean }];
  'transport/peer-reachable': [event: PeerReachableEvent];
};

export interface OutboxRedriveEvents {
  on<K extends keyof OutboxRedriveEventMap>(
    event: K,
    listener: (...args: OutboxRedriveEventMap[K]) => void,
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
  /** Transport-owned level check: may an in-doubt target be reconciled on its normal schedule? */
  isPeerReachable?: (networkId: string, nodeId: string) => boolean;
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
  private readonly isPeerReachable?: (networkId: string, nodeId: string) => boolean;
  private readonly onError: (error: unknown) => void;
  private readonly onTargetsResolved?: (
    envelope: MessageEnvelope,
    targetNodeIds: readonly string[],
  ) => void;
  private readonly intervalMs: number;
  private readonly attemptTimeoutMs: number;
  private timer?: ReturnType<typeof setInterval>;
  private subscriptions: Disposable[] = [];
  private queue: Promise<unknown> = Promise.resolve();
  private stopped = false;
  private lastPeerPulse = new Map<string, number>();

  constructor(options: OutboxRedriveOptions) {
    this.outbox = options.outbox;
    this.deliver = options.deliver;
    this.events = options.events;
    this.isPeerReachable = options.isPeerReachable;
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
    if (this.events) {
      this.subscriptions.push(
        this.events.on('node/peer-updated', (card, provenance) =>
          this.onPeerUpdated(card, provenance),
        ),
        this.events.on('transport/peer-reachable', event => this.onPeerReachable(event)),
      );
    }
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
    if (provenance?.direct === false) {
      return;
    }
    // Identity resolution is the only event that can turn an unresolved selector into a
    // real target, so identity pulses also retry obligations with no pending node ids.
    this.pulsePeer(card.networkId, card.nodeId, {
      bypassThrottle: false,
      includeUnresolved: true,
    });
  }

  private onPeerReachable(event: PeerReachableEvent): void {
    // A peer opening its tunnel immediately after our discovery probe must not wait out
    // the beacon pulse throttle; direct and gossiped beacons share the same throttle.
    // Beacon pulses carry no identity and cannot resolve a selector: they only touch
    // obligations already pending for this exact node.
    this.pulsePeer(event.networkId, event.nodeId, {
      bypassThrottle: event.via === 'tunnel',
      includeUnresolved: false,
    });
  }

  /** One peer-scoped force flush; identity pulses are throttled because cards refresh often. */
  private pulsePeer(
    networkId: string,
    nodeId: string,
    options: { bypassThrottle: boolean; includeUnresolved: boolean },
  ): void {
    if (this.stopped) {
      return;
    }
    const key = `${networkId}\n${nodeId}`;
    const now = Date.now();
    if (!options.bypassThrottle && now - (this.lastPeerPulse.get(key) ?? 0) < 2_000) {
      return;
    }
    if (!options.includeUnresolved) {
      const hasMatchingQueued = this.outbox
        .list({ networkId, status: 'queued' })
        .some(item => item.pendingNodeIds.includes(nodeId));
      if (!hasMatchingQueued) {
        return;
      }
    }
    this.lastPeerPulse.set(key, now);
    void this.flush({
      networkId,
      targetNodeId: nodeId,
      force: true,
      forceMatchingTarget: true,
      ...(options.includeUnresolved ? { includeUnresolved: true } : {}),
    }).catch(error => this.onError(error));
  }

  flush(options: OutboxFlushOptions = {}): Promise<OutboxFlushResult> {
    const run = this.queue.then(() => this.flushNow(options));
    this.queue = run.catch(() => {});
    return run;
  }

  private async flushNow(options: OutboxFlushOptions): Promise<OutboxFlushResult> {
    const now = Date.now();
    if (options.forceMatchingTarget && !options.targetNodeId) {
      throw new Error('forceMatchingTarget requires targetNodeId');
    }
    const scopedTargetNodeId = options.targetNodeId;
    // Causal order is per thread: `list` is oldest-first by createdAt then id, so an earlier
    // undelivered event on a thread (backoff, in-doubt gate, or excluded by --id) must block
    // every later event on that same thread from overtaking it, without blocking other
    // threads. `--id ... --force` is the explicit operator override for one obligation.
    const allQueued = this.outbox.list({
      status: 'queued',
      ...(options.networkId ? { networkId: options.networkId } : {}),
    });
    const threadKeyOf = (item: OutboxItem): string =>
      `${item.networkId}\n${item.envelope.semantic?.threadId ?? item.id}`;
    const blockedThreads = new Set<string>();
    let attempted = 0;
    let delivered = 0;
    const processed: OutboxItem[] = [];
    // One chronological pass: only an item that precedes `candidate` can have blocked its
    // thread by the time `candidate` is visited, so a scoped `--id` or peer pulse is never
    // blocked by a later event on the same thread, while a later event still may not
    // overtake an earlier parked head.
    for (const candidate of allQueued) {
      if (this.stopped) {
        break;
      }
      const threadKey = threadKeyOf(candidate);
      if (options.id !== undefined && candidate.id !== options.id) {
        // An earlier queued event on the same thread was excluded from this scoped flush:
        // without an explicit force, the requested item may not overtake it.
        if (!options.force) {
          blockedThreads.add(threadKey);
        }
        continue;
      }
      if (
        options.forceMatchingTarget &&
        !(
          candidate.pendingNodeIds.includes(scopedTargetNodeId!) ||
          (options.includeUnresolved === true && candidate.pendingNodeIds.length === 0)
        )
      ) {
        // A peer pulse advances only obligations for that peer; an earlier event on the
        // thread that still waits for another target keeps the thread's head in place.
        blockedThreads.add(threadKey);
        continue;
      }
      if (blockedThreads.has(threadKey)) {
        processed.push(candidate);
        continue;
      }
      // A forced peer flush bypasses only the matching item's schedule; ordinary timer
      // flushes still honour each item's exponential backoff.
      const matchesTarget =
        !options.forceMatchingTarget ||
        candidate.pendingNodeIds.includes(options.targetNodeId!) ||
        (options.includeUnresolved === true && candidate.pendingNodeIds.length === 0);
      const force = options.force && matchesTarget;
      if (!force && Date.parse(candidate.nextAttemptAt) > now) {
        blockedThreads.add(threadKey);
        processed.push(candidate);
        continue;
      }
      const claimed = this.outbox.beginAttempt(candidate.id, {
        leaseMs: Math.max(DEFAULT_CLAIM_LEASE_MS, this.attemptTimeoutMs),
      });
      if (!claimed) {
        if (this.outbox.get(candidate.id)?.status === 'queued') blockedThreads.add(threadKey);
        continue;
      }
      const { item, attemptToken } = claimed;
      try {
        // The snapshot orders the scan. Only the leased row can authorize a send.
        const eligibleTargets = item.pendingNodeIds;
        if ((!force && Date.parse(item.nextAttemptAt) > now) ||
            (options.forceMatchingTarget && eligibleTargets.length === 0 && options.includeUnresolved !== true)) {
          blockedThreads.add(threadKey);
          processed.push(item);
          continue;
        }
        const requestTargets = options.forceMatchingTarget && scopedTargetNodeId
          ? eligibleTargets.filter(nodeId => nodeId === scopedTargetNodeId)
          : options.force
            ? eligibleTargets
            : eligibleTargets.filter(nodeId =>
                !item.inDoubtNodeIds.includes(nodeId) ||
                this.isPeerReachable?.(item.networkId, nodeId) === true,
              );
        if (eligibleTargets.length > 0 && requestTargets.length === 0) {
          blockedThreads.add(threadKey);
          processed.push(item);
          continue;
        }
        attempted += 1;
        let outcome: OutboxDeliveryOutcome;
        try {
          // An empty target list means the emitter should route an unresolved selector.
          outcome = await this.deliver(item, {
            ...(requestTargets.length > 0 ? { targetNodeIds: requestTargets } : {}),
            timeoutMs: this.attemptTimeoutMs,
          });
        } catch (error) {
          outcome = { lastError: String(error) };
        }
        const resolved = outcome.resolvedTargetNodeIds;
        const requested = new Set(requestTargets);
        const untouchedPendingNodeIds = requestTargets.length > 0
          ? eligibleTargets.filter(nodeId => !requested.has(nodeId))
          : [];
        const updated = this.outbox.recordAttempt(item.id, {
          deliveredReceipts: outcome.deliveredReceipts,
          failures: outcome.failures,
          pendingNodeIds: [...(outcome.pendingNodeIds ?? resolved ?? eligibleTargets), ...untouchedPendingNodeIds],
          inDoubtNodeIds: outcome.inDoubtNodeIds?.slice(),
          relaySpooledNodeIds: outcome.relaySpooledNodeIds?.slice(),
          lastError: outcome.lastError,
          nextAttemptAt: new Date(Date.now() + attemptDelayMs(item.attempts + 1)).toISOString(),
        }, attemptToken);
        if (!updated) {
          if (this.outbox.get(item.id)?.status === 'queued') blockedThreads.add(threadKey);
          continue;
        }
        delivered += Object.keys(outcome.deliveredReceipts ?? {}).length;
        if (resolved?.length && eligibleTargets.length === 0) {
          try {
            this.onTargetsResolved?.(item.envelope, resolved);
          } catch (error) {
            this.onError(error);
          }
        }
        if (updated.status === 'queued') blockedThreads.add(threadKey);
        processed.push(updated);
      } finally {
        this.outbox.releaseAttempt(item.id, attemptToken);
      }
    }
    return {
      attempted,
      delivered,
      // Report the remainder through the same network scope as the candidates; a scoped
      // operator flush must not claim obligations from other networks are still queued here.
      stillQueued: this.outbox.list({
        status: 'queued',
        ...(options.networkId !== undefined ? { networkId: options.networkId } : {}),
      }).length,
      items: processed,
    };
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer !== undefined) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    for (const subscription of this.subscriptions) {
      subscription.dispose();
    }
    this.subscriptions = [];
    await this.queue.catch(() => {});
  }
}
