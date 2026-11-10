import { randomUUID } from 'node:crypto';
import { closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { verifyEnvelope } from '../core-types/index.ts';
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

export interface NetworkGateService extends GateService {
  getNetworkMode(networkId: string): AdmissionMode;
  setNetworkMode(networkId: string, mode: AdmissionMode): Promise<void>;
  drainDnd(networkId?: string): Promise<void>;
  listRejections(): readonly { envelope: MessageEnvelope; reason: string }[];
}

function dndKey(envelope: MessageEnvelope): string {
  return JSON.stringify([envelope.networkId, envelope.from.nodeId, envelope.idempotencyKey ?? envelope.id]);
}

function loadDnd(path: string | undefined, limit: number): Map<string, MessageEnvelope> {
  if (!path || !existsSync(path)) return new Map();
  try {
    const saved = JSON.parse(readFileSync(path, 'utf8'));
    if (!saved || typeof saved !== 'object' || Array.isArray(saved) ||
        !saved.queued || typeof saved.queued !== 'object' || Array.isArray(saved.queued)) throw new Error();
    const entries = Object.entries(saved.queued) as [string, MessageEnvelope][];
    if (entries.length > limit || entries.some(([key, envelope]) =>
      !verifyEnvelope(envelope) || key !== dndKey(envelope))) throw new Error();
    return new Map(entries);
  } catch {
    throw new Error('Invalid gate DND store');
  }
}

function persistDnd(path: string, queued: ReadonlyMap<string, MessageEnvelope>): void {
  mkdirSync(dirname(path), { recursive: true });
  const temp = `${path}.${randomUUID()}.tmp`;
  const fd = openSync(temp, 'wx', 0o600);
  try {
    writeFileSync(fd, JSON.stringify({ queued: Object.fromEntries(queued) }));
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(temp, path);
  const dir = openSync(dirname(path), 'r');
  try {
    fsyncSync(dir);
  } finally {
    closeSync(dir);
  }
}

export function createGatePlugin(config: GateConfig = {}): Plugin {
  return {
    name: 'gate',
    apply(ctx) {
      let mode = config.mode ?? 'approval_required';
      const modes = new Map(Object.entries(config.networkModes ?? {}));
      const rules = new Set<GateRulePlugin>();
      const pending = new Map<string, PendingApprovalItem>();
      const maxDndItems = config.maxDndItems ?? 1000;
      if (!Number.isSafeInteger(maxDndItems) || maxDndItems < 1) {
        throw new Error('Invalid DND queue capacity');
      }
      let dnd = loadDnd(config.dndQueuePath, maxDndItems);
      const updateDnd = (id: string, envelope?: MessageEnvelope) => {
        const updated = new Map(dnd);
        if (envelope) updated.set(id, structuredClone(envelope));
        else updated.delete(id);
        if (config.dndQueuePath) persistDnd(config.dndQueuePath, updated);
        dnd = updated;
      };
      const trusted = new Set<string>();
      const rejections: { envelope: MessageEnvelope; reason: string }[] = [];
      const key = (e: MessageEnvelope) => JSON.stringify([e.networkId, e.from.nodeId, e.id]);
      const trustKeys = (e: MessageEnvelope) => [
        JSON.stringify([e.networkId, 'node', e.from.nodeId]),
        JSON.stringify([e.networkId, 'member', e.from.member]),
      ];
      let draining: Promise<void> = Promise.resolve();
      const service: NetworkGateService = {
        getMode: () => mode,
        getNetworkMode: id => modes.get(id) ?? mode,
        async setMode(value) {
          mode = value;
          if (value === 'open') {
            for (const network of new Set([...dnd.values()].map(e => e.networkId))) {
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
        drainDnd(networkId) {
          const operation = draining.catch(() => {}).then(async () => {
            const errors: unknown[] = [];
            // Snapshot once: failed or re-queued items never spin within this drain.
            for (const [id, envelope] of [...dnd]) {
              if (networkId && envelope.networkId !== networkId) continue;
              if (service.getNetworkMode(envelope.networkId) !== 'open') continue;
              try {
                if (!verifyEnvelope(envelope)) throw new Error('Invalid queued envelope signature');
                const verdict = await service.evaluate(envelope);
                if (verdict.status === 'reject') {
                  if (dnd.get(id)?.signature === envelope.signature) updateDnd(id);
                  continue;
                }
                // Approval queues are volatile; retain the durable copy until approval succeeds.
                if (verdict.status !== 'allow') continue;
                if (service.getNetworkMode(envelope.networkId) !== 'open') continue;
                const receipt = await ctx.services.dispatcher.dispatch(envelope);
                if (receipt.stage !== 'failed') {
                  // A concurrently admitted newer envelope must not be deleted with this one.
                  if (dnd.get(id)?.signature === envelope.signature) updateDnd(id);
                } else {
                  errors.push(new Error(receipt.detail ?? 'DND dispatch failed'));
                }
              } catch (error) {
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
        async evaluate(envelope) {
          let verdict = await ctx.bail('gate/evaluate', envelope);
          if (!verdict) {
            for (const rule of [...rules].sort((a, b) => b.priority - a.priority)) {
              verdict = (await rule.evaluate(envelope)) ?? undefined;
              if (verdict) {
                break;
              }
            }
          }
          if (!verdict && trustKeys(envelope).some(k => trusted.has(k))) {
            verdict = { status: 'allow' };
          }
          if (!verdict && config.localNodeId && envelope.from.nodeId === config.localNodeId) {
            const networkMode = service.getNetworkMode(envelope.networkId);
            // Self-originated local deliveries are admission checks, not knocks. DND
            // still queues them and closed still rejects them.
            if (networkMode !== 'closed' && networkMode !== 'dnd') {
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
          if (verdict.status === 'pending_approval') {
            const existing = [...pending.values()].find(
              item => key(item.envelope) === key(envelope),
            );
            if (existing) {
              return { ...verdict, approvalId: existing.id };
            }
            if (pending.has(verdict.approvalId)) {
              throw new Error('Approval ID collision');
            }
            pending.set(verdict.approvalId, {
              id: verdict.approvalId,
              envelope: structuredClone(envelope),
              reason: verdict.reason,
              createdAt: new Date().toISOString(),
            });
          } else if (verdict.status === 'queued_dnd') {
            const id = dndKey(envelope);
            if (!dnd.has(id) && dnd.size >= maxDndItems) {
              return { status: 'reject', code: 'dnd_queue_full', reason: 'DND queue capacity exceeded' };
            }
            updateDnd(id, envelope);
            return { ...verdict, durable: Boolean(config.dndQueuePath) };
          } else if (verdict.status === 'reject') {
            rejections.push({ envelope: structuredClone(envelope), reason: verdict.reason });
          }
          return verdict;
        },
        listPendingApprovals: () => structuredClone([...pending.values()]),
        listRejections: () => structuredClone(rejections),
        async approve(id, opts) {
          const item = pending.get(id);
          if (!item) {
            throw new Error(`Unknown approval: ${id}`);
          }
          pending.delete(id);
          try {
            const receipt = await ctx.services.dispatcher.dispatch(item.envelope);
            if (receipt.stage === 'failed') throw new Error(receipt.detail ?? 'Approval dispatch failed');
            const queuedId = dndKey(item.envelope);
            if (dnd.get(queuedId)?.signature === item.envelope.signature) updateDnd(queuedId);
          } catch (error) {
            pending.set(id, item);
            throw error;
          }
          if (opts?.trustSender) {
            for (const k of trustKeys(item.envelope)) {
              trusted.add(k);
            }
          }
        },
        async reject(id, reason = 'Rejected by operator') {
          const item = pending.get(id);
          if (!item) {
            throw new Error(`Unknown approval: ${id}`);
          }
          pending.delete(id);
          const queuedId = dndKey(item.envelope);
          if (dnd.get(queuedId)?.signature === item.envelope.signature) updateDnd(queuedId);
          rejections.push({ envelope: item.envelope, reason });
        },
      };
      ctx.provide('gate', service);
    },
  };
}
