import { randomUUID } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { writeFileAtomically } from '../fs-atomic/index.ts';
import {
  APPROVAL_CANCELLED, APPROVAL_CUSTODY_RETAINED, DND_CUSTODY_RETAINED, DND_HANDOFF_PARKED,
  isConfirmedHandoffReceipt, verifyEnvelope,
} from '../core-types/index.ts';
import type {
  AdmissionMode,
  GateRulePlugin,
  GateService,
  GateVerdict,
  MessageEnvelope,
  PendingApprovalItem,
} from '../core-types/index.ts';
import type { Plugin } from '../kernel/index.ts';

export interface GateConfig {
  mode?: AdmissionMode;
  dndQueuePath?: string;
  maxDndItems?: number;
  networkModes?: Record<string, AdmissionMode>;
  /** This node's signing identity; lets locally originated envelopes skip approval knocks. */
  localNodeId?: string;
}

export interface DndQueueItem {
  id: string;
  envelope: MessageEnvelope;
  status: 'queued' | 'parked';
  attempts: number;
  lastError?: string;
  updatedAt: string;
}

type DndEntry = Omit<DndQueueItem, 'id'>;

export type TrustedSenderKind = 'node' | 'member';

export interface TrustedSenderRecord {
  readonly networkId: string;
  readonly kind: TrustedSenderKind;
  readonly value: string;
}

interface GateCustodyState {
  queued: Map<string, DndEntry>;
  withdrawn: Map<string, string>;
  trusted: Map<string, TrustedSenderRecord>;
}

function revision(envelope: MessageEnvelope): number {
  const value = envelope.payload?.claimVersion ?? envelope.payload?.revision ?? 0;
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : -1;
}

export interface NetworkGateService extends GateService {
  getNetworkMode(networkId: string): AdmissionMode;
  setNetworkMode(networkId: string, mode: AdmissionMode): Promise<void>;
  drainDnd(networkId?: string, opts?: { includeParked?: boolean }): Promise<void>;
  listDndQueue(): readonly DndQueueItem[];
  listRejections(): readonly { envelope: MessageEnvelope; reason: string }[];
  listTrustedSenders(networkId?: string): readonly TrustedSenderRecord[];
  trustSender(record: TrustedSenderRecord): TrustedSenderRecord;
  untrustSender(filter: { networkId?: string; kind?: TrustedSenderKind; value: string }): {
    removedCount: number;
    removed: TrustedSenderRecord[];
    remaining: TrustedSenderRecord[];
  };
}

function dndKey(envelope: MessageEnvelope): string {
  return JSON.stringify([envelope.networkId, envelope.from.nodeId, envelope.idempotencyKey ?? envelope.id]);
}

function trustedSenderKey(record: TrustedSenderRecord): string {
  return JSON.stringify([record.networkId, record.kind, record.value]);
}

function compareStrings(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

function compareTrustedSenders(a: TrustedSenderRecord, b: TrustedSenderRecord): number {
  return compareStrings(a.networkId, b.networkId)
    || compareStrings(a.kind, b.kind)
    || compareStrings(a.value, b.value);
}

function sortedTrustedSenders(
  trusted: ReadonlyMap<string, TrustedSenderRecord>,
  networkId?: string,
): TrustedSenderRecord[] {
  return [...trusted.values()]
    .filter(record => networkId === undefined || record.networkId === networkId)
    .sort(compareTrustedSenders)
    .map(record => ({ networkId: record.networkId, kind: record.kind, value: record.value }));
}

function isTrustedSenderKind(value: unknown): value is TrustedSenderKind {
  return value === 'node' || value === 'member';
}

function normalizeTrustedSender(record: TrustedSenderRecord): TrustedSenderRecord {
  if (!isRecord(record)) {
    throw new Error('Trusted sender record must be an object');
  }
  const networkId = typeof record.networkId === 'string' ? record.networkId.trim() : '';
  const value = typeof record.value === 'string' ? record.value.trim() : '';
  if (!networkId) {
    throw new Error('Trusted sender networkId must be a non-empty string');
  }
  if (!isTrustedSenderKind(record.kind)) {
    throw new Error(`Invalid trusted sender kind: ${String(record.kind)}`);
  }
  if (!value) {
    throw new Error('Trusted sender value must be a non-empty string');
  }
  return { networkId, kind: record.kind, value };
}

function loadDnd(path: string | undefined, limit: number): GateCustodyState {
  if (!path || !existsSync(path)) return { queued: new Map(), withdrawn: new Map(), trusted: new Map() };
  try {
    const saved = JSON.parse(readFileSync(path, 'utf8'));
    if (!isRecord(saved) || !isRecord(saved.queued)) throw new Error();
    const withdrawn = new Map<string, string>();
    if (saved.withdrawn !== undefined) {
      if (!isRecord(saved.withdrawn)) throw new Error();
      for (const [key, reason] of Object.entries(saved.withdrawn)) {
        if (typeof reason !== 'string') throw new Error();
        withdrawn.set(key, reason);
      }
    }
    const trusted = new Map<string, TrustedSenderRecord>();
    if (saved.trustedSenders !== undefined) {
      if (!Array.isArray(saved.trustedSenders)) throw new Error();
      for (const item of saved.trustedSenders) {
        if (
          !isRecord(item) ||
          typeof item.networkId !== 'string' || item.networkId.trim().length === 0 ||
          !isTrustedSenderKind(item.kind) ||
          typeof item.value !== 'string' || item.value.trim().length === 0
        ) {
          throw new Error();
        }
        const record: TrustedSenderRecord = {
          networkId: item.networkId,
          kind: item.kind,
          value: item.value,
        };
        trusted.set(trustedSenderKey(record), record);
      }
    }
    const entries: [string, DndEntry][] = Object.entries(saved.queued).map(([key, value]) => {
      const raw = value as DndEntry & MessageEnvelope;
      const entry: DndEntry = raw?.envelope ? raw : {
        envelope: raw, status: 'queued', attempts: 0, updatedAt: raw?.createdAt,
      };
      if (!verifyEnvelope(entry.envelope) || key !== dndKey(entry.envelope) ||
          !['queued', 'parked'].includes(entry.status) ||
          !Number.isSafeInteger(entry.attempts) || entry.attempts < 0 ||
          typeof entry.updatedAt !== 'string' || !Number.isFinite(Date.parse(entry.updatedAt)) ||
          (entry.lastError !== undefined && typeof entry.lastError !== 'string')) throw new Error();
      return [key, entry];
    });
    if (entries.length > limit) throw new Error();
    return { queued: new Map(entries), withdrawn, trusted };
  } catch {
    throw new Error('Invalid gate DND store');
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function createGatePlugin(config: GateConfig = {}): Plugin {
  return {
    name: 'gate',
    apply(ctx) {
      let mode = config.mode ?? 'open';
      const modes = new Map(Object.entries(config.networkModes ?? {}));
      const rules = new Set<GateRulePlugin>();
      const pending = new Map<string, PendingApprovalItem>();
      const maxDndItems = config.maxDndItems ?? 1000;
      if (!Number.isSafeInteger(maxDndItems) || maxDndItems < 1) {
        throw new Error('Invalid DND queue capacity');
      }
      let { queued: dnd, withdrawn, trusted } = loadDnd(config.dndQueuePath, maxDndItems);
      const persistGateState = (
        queued: Map<string, DndEntry>,
        withdrawals = withdrawn,
        trustedSet = trusted,
      ) => {
        if (config.dndQueuePath) {
          const trustedSenders = sortedTrustedSenders(trustedSet);
          writeFileAtomically(config.dndQueuePath, JSON.stringify({
            queued: Object.fromEntries(queued),
            ...(withdrawals.size > 0 ? { withdrawn: Object.fromEntries(withdrawals) } : {}),
            ...(trustedSenders.length > 0 ? { trustedSenders } : {}),
          }));
        }
        dnd = queued;
        withdrawn = withdrawals;
        trusted = trustedSet;
      };
      const updateDnd = (id: string, entry?: DndEntry) => {
        const updated = new Map(dnd);
        if (entry) updated.set(id, structuredClone(entry));
        else updated.delete(id);
        persistGateState(updated);
      };
      const parkDnd = (id: string, envelope: MessageEnvelope, error: unknown) => {
        const entry = dnd.get(id);
        if (entry?.envelope.signature !== envelope.signature) return;
        updateDnd(id, {
          ...entry, status: 'parked', attempts: entry.attempts + 1,
          lastError: error instanceof Error ? error.message : String(error),
          updatedAt: new Date().toISOString(),
        });
      };
      const approving = new Set<string>();
      const rejections: { envelope: MessageEnvelope; reason: string }[] = [];
      const key = (e: MessageEnvelope) => JSON.stringify([e.networkId, e.from.nodeId, e.id]);
      const cancellationKey = (e: MessageEnvelope) => `${key(e)}\0${e.signature}`;
      const withdrawCustody = (envelope: MessageEnvelope, reason: string) => {
        const queued = new Map(dnd);
        const id = dndKey(envelope);
        const entry = queued.get(id);
        if (entry?.status === 'queued' && entry.envelope.signature === envelope.signature) queued.delete(id);
        const withdrawals = new Map(withdrawn);
        withdrawals.set(cancellationKey(envelope), reason);
        persistGateState(queued, withdrawals);
      };
      const senderTrustRecords = (e: MessageEnvelope): TrustedSenderRecord[] => {
        const records: TrustedSenderRecord[] = [
          { networkId: e.networkId, kind: 'node', value: e.from.nodeId },
        ];
        if (typeof e.from.member === 'string' && e.from.member.trim().length > 0) {
          records.push({ networkId: e.networkId, kind: 'member', value: e.from.member });
        }
        return records;
      };
      let draining: Promise<void> = Promise.resolve();
      const evaluate = async (envelope: MessageEnvelope, drainEntry?: DndEntry): Promise<GateVerdict> => {
        let verdict = await ctx.bail('gate/evaluate', envelope);
        if (!verdict) {
          for (const rule of [...rules].sort((a, b) => b.priority - a.priority)) {
            verdict = (await rule.evaluate(envelope)) ?? undefined;
            if (verdict) {
              break;
            }
          }
        }
        if (!verdict) {
          const networkMode = service.getNetworkMode(envelope.networkId);
          const isTrustedSender = senderTrustRecords(envelope).some(r => trusted.has(trustedSenderKey(r)));
          const isLocalSender = Boolean(config.localNodeId && envelope.from.nodeId === config.localNodeId);
          // Trusted senders and self-originated local deliveries skip approval knocks,
          // while DND still queues them and closed still rejects them.
          if ((isTrustedSender || isLocalSender) && networkMode !== 'closed' && networkMode !== 'dnd') {
            verdict = { status: 'allow' };
          }
        }
        if (!verdict) {
          switch (service.getNetworkMode(envelope.networkId)) {
            case 'open':
              verdict = { status: 'allow' };
              break;
            case 'closed':
              verdict = {
                status: 'reject',
                code: 'gate_closed',
                reason: 'Node admission gate is closed',
              };
              break;
            case 'dnd':
              verdict = {
                status: 'queued_dnd',
                reason: 'Node is in DND mode; message queued',
              };
              break;
            case 'approval_required':
              verdict = {
                status: 'pending_approval',
                approvalId: randomUUID(),
                reason: 'Approval required',
              };
              break;
          }
        }
        const id = dndKey(envelope);
        const existingDnd = dnd.get(id);
        const retained = existingDnd && revision(envelope) <= revision(existingDnd.envelope);
        // Only an exact queue entry being drained may proceed past its own custody check.
        const drainingCurrent = drainEntry !== undefined && existingDnd === drainEntry &&
          service.getNetworkMode(envelope.networkId) === 'open';
        const durable = Boolean(config.dndQueuePath);
        const existingApproval = [...pending.values()].find(item => key(item.envelope) === key(envelope));
        const approvalInProgress = existingApproval && approving.has(existingApproval.id);
        if (existingApproval && (approvalInProgress || existingApproval.envelope.signature !== envelope.signature)) {
          return {
            status: 'reject', code: APPROVAL_CUSTODY_RETAINED, durable: false,
            reason: approvalInProgress ? 'Approval already in progress' : existingApproval.reason,
          };
        }
        if (retained && existingDnd.status === 'parked' && (!drainingCurrent || verdict.status === 'reject')) {
          return {
            status: 'reject', code: DND_HANDOFF_PARKED, durable,
            reason: existingDnd.lastError ?? 'Previous DND handoff outcome unknown; newer revision required',
          };
        }
        const withdrawal = withdrawn.get(cancellationKey(envelope));
        if (retained && existingDnd.status === 'queued' && !drainingCurrent) {
          if (verdict.status === 'reject' || withdrawal !== undefined ||
              (verdict.status === 'allow' && existingDnd.envelope.signature !== envelope.signature)) {
            return { status: 'reject', code: DND_CUSTODY_RETAINED, durable,
              reason: verdict.status === 'reject' ? verdict.reason : 'Queued DND copy retained' };
          }
          if (verdict.status === 'allow') {
            if (durable) return { status: 'queued_dnd', reason: 'Message queued in DND store', durable };
            updateDnd(id);
          }
        }
        if (existingApproval && (verdict.status === 'reject' || withdrawal !== undefined)) {
          return { status: 'reject', code: APPROVAL_CUSTODY_RETAINED, durable: false,
            reason: verdict.status === 'reject' ? verdict.reason : existingApproval.reason };
        }
        if (withdrawal !== undefined) {
          return { status: 'reject', code: APPROVAL_CANCELLED, reason: withdrawal, durable };
        }
        if (verdict.status === 'allow' && existingApproval) pending.delete(existingApproval.id);
        if (verdict.status === 'pending_approval') {
          if (existingApproval) {
            return { ...verdict, approvalId: existingApproval.id };
          }
          if (pending.has(verdict.approvalId)) {
            throw new Error('Approval ID collision');
          }
          const item: PendingApprovalItem = {
            id: verdict.approvalId,
            envelope: structuredClone(envelope),
            reason: verdict.reason,
            createdAt: new Date().toISOString(),
          };
          pending.set(item.id, item);
          ctx.emit('gate/pending-approval', structuredClone(item));
        } else if (verdict.status === 'queued_dnd') {
          if (!existingDnd && dnd.size >= maxDndItems) {
            if (existingApproval) {
              return { status: 'reject', code: APPROVAL_CUSTODY_RETAINED, durable: false, reason: existingApproval.reason };
            }
            return { status: 'reject', code: 'dnd_queue_full', reason: 'DND queue capacity exceeded' };
          }
          if (!retained) {
            updateDnd(id, { envelope, status: 'queued', attempts: 0, updatedAt: new Date().toISOString() });
          }
          return { ...verdict, durable };
        } else if (verdict.status === 'reject') {
          rejections.push({ envelope: structuredClone(envelope), reason: verdict.reason });
        }
        return verdict;
      };
      const service: NetworkGateService = {
        getMode: () => mode,
        getNetworkMode: id => modes.get(id) ?? mode,
        async setMode(value) {
          mode = value;
          if (value === 'open') {
            for (const network of new Set([...dnd.values()].map(e => e.envelope.networkId))) {
              if (service.getNetworkMode(network) === 'open') {
                await service.drainDnd(network);
              }
            }
          }
        },
        async setNetworkMode(id, value) {
          modes.set(id, value);
          if (value === 'open') {
            await service.drainDnd(id);
          }
        },
        drainDnd(networkId, opts) {
          const operation = draining.catch(() => {}).then(async () => {
            const errors: unknown[] = [];
            // Snapshot once: failed or re-queued items never spin within this drain.
            for (const [id, entry] of [...dnd]) {
              if (entry.status === 'parked' && opts?.includeParked !== true) continue;
              const { envelope } = entry;
              if (networkId && envelope.networkId !== networkId) continue;
              if (service.getNetworkMode(envelope.networkId) !== 'open') continue;
              try {
                if (!verifyEnvelope(envelope)) throw new Error('Invalid queued envelope signature');
                const verdict = await evaluate(envelope, entry);
                if (verdict.status === 'reject') {
                  if (entry.status === 'queued' && verdict.code !== DND_HANDOFF_PARKED && verdict.code !== DND_CUSTODY_RETAINED &&
                      verdict.code !== APPROVAL_CUSTODY_RETAINED &&
                      service.getNetworkMode(envelope.networkId) === 'open' &&
                      dnd.get(id) === entry) withdrawCustody(envelope, verdict.reason);
                  continue;
                }
                // Approval queues are volatile; retain the durable copy until approval succeeds.
                if (verdict.status !== 'allow') continue;
                if (service.getNetworkMode(envelope.networkId) !== 'open') continue;
                const receipt = await ctx.services.dispatcher.dispatch(envelope);
                if (isConfirmedHandoffReceipt(receipt, { allowQueued: receipt.executionReceipt?.executor !== 'gate-dnd' })) {
                  // A concurrently admitted newer envelope must not be deleted with this one.
                  if (dnd.get(id)?.envelope.signature === envelope.signature) updateDnd(id);
                } else {
                  throw new Error(`Unconfirmed DND handoff (${receipt.stage}): ${receipt.detail ?? 'no durable handoff evidence'}`);
                }
              } catch (error) {
                parkDnd(id, envelope, error);
                errors.push(error);
              }
            }
            if (errors.length) throw new AggregateError(errors, 'DND drain failed');
          });
          draining = operation;
          return operation;
        },
        registerRule(rule) {
          rules.add(rule);
          return {
            dispose() {
              rules.delete(rule);
            },
          };
        },
        evaluate: envelope => evaluate(envelope),
        listDndQueue: () => structuredClone([...dnd].map(([id, entry]) => ({ id, ...entry }))),
        listPendingApprovals: () => structuredClone([...pending.values()]),
        listRejections: () => structuredClone(rejections),
        listTrustedSenders: networkId => sortedTrustedSenders(trusted, networkId),
        trustSender(record) {
          const normalized = normalizeTrustedSender(record);
          const nextTrusted = new Map(trusted);
          nextTrusted.set(trustedSenderKey(normalized), normalized);
          persistGateState(dnd, withdrawn, nextTrusted);
          return { ...normalized };
        },
        untrustSender(filter) {
          if (!isRecord(filter)) {
            throw new Error('Untrust filter must be an object');
          }
          const value = typeof filter.value === 'string' ? filter.value.trim() : '';
          if (!value) {
            throw new Error('Trusted sender value must be a non-empty string');
          }
          let networkId: string | undefined;
          if (filter.networkId !== undefined) {
            networkId = typeof filter.networkId === 'string' ? filter.networkId.trim() : '';
            if (!networkId) {
              throw new Error('Trusted sender networkId must be a non-empty string');
            }
          }
          if (filter.kind !== undefined && !isTrustedSenderKind(filter.kind)) {
            throw new Error(`Invalid trusted sender kind: ${String(filter.kind)}`);
          }
          const removed: TrustedSenderRecord[] = [];
          const nextTrusted = new Map(trusted);
          for (const [entryKey, record] of trusted) {
            if (record.value !== value) continue;
            if (networkId !== undefined && record.networkId !== networkId) continue;
            if (filter.kind !== undefined && record.kind !== filter.kind) continue;
            nextTrusted.delete(entryKey);
            removed.push({ networkId: record.networkId, kind: record.kind, value: record.value });
          }
          removed.sort(compareTrustedSenders);
          if (removed.length > 0) {
            persistGateState(dnd, withdrawn, nextTrusted);
          }
          return {
            removedCount: removed.length,
            removed,
            remaining: sortedTrustedSenders(trusted, networkId),
          };
        },
        async approve(id, opts) {
          const item = pending.get(id);
          if (!item) {
            throw new Error(`Unknown approval: ${id}`);
          }
          if (approving.has(id)) throw new Error(`Approval already in progress: ${id}`);
          approving.add(id);
          try {
            const receipt = await ctx.services.dispatcher.dispatch(item.envelope);
            if (!isConfirmedHandoffReceipt(receipt, { allowQueued: receipt.executionReceipt?.executor !== 'gate-dnd' })) {
              throw new Error(`Unconfirmed approval handoff (${receipt.stage}): ${receipt.detail ?? 'no durable handoff evidence'}`);
            }
            const queuedId = dndKey(item.envelope);
            const removeQueued = dnd.get(queuedId)?.envelope.signature === item.envelope.signature;
            if (removeQueued || opts?.trustSender) {
              const nextQueued = removeQueued ? new Map(dnd) : dnd;
              if (removeQueued) nextQueued.delete(queuedId);
              const nextTrusted = opts?.trustSender ? new Map(trusted) : trusted;
              if (opts?.trustSender) {
                for (const record of senderTrustRecords(item.envelope)) {
                  nextTrusted.set(trustedSenderKey(record), record);
                }
              }
              persistGateState(nextQueued, withdrawn, nextTrusted);
            }
            pending.delete(id);
          } catch (error) {
            parkDnd(dndKey(item.envelope), item.envelope, error);
            throw error;
          } finally {
            approving.delete(id);
          }
        },
        async reject(id, reason = 'Rejected by operator') {
          const item = pending.get(id);
          if (!item) {
            throw new Error(`Unknown approval: ${id}`);
          }
          if (approving.has(id)) throw new Error(`Approval already in progress: ${id}`);
          withdrawCustody(item.envelope, reason);
          pending.delete(id);
          rejections.push({ envelope: item.envelope, reason });
        },
      };
      ctx.provide('gate', service);
    },
  };
}
