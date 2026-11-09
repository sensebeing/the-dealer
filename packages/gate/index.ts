import { randomUUID } from 'node:crypto';
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
  networkModes?: Record<string, AdmissionMode>;
}

export interface NetworkGateService extends GateService {
  getNetworkMode(networkId: string): AdmissionMode;
  setNetworkMode(networkId: string, mode: AdmissionMode): Promise<void>;
  drainDnd(networkId?: string): Promise<void>;
  listRejections(): readonly { envelope: MessageEnvelope; reason: string }[];
}

export function createGatePlugin(config: GateConfig = {}): Plugin {
  return {
    name: 'gate',
    apply(ctx) {
      let mode = config.mode ?? 'approval_required';
      const modes = new Map(Object.entries(config.networkModes ?? {}));
      const rules = new Set<GateRulePlugin>();
      const pending = new Map<string, PendingApprovalItem>();
      const dnd = new Map<string, MessageEnvelope>();
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
            for (const [id, envelope] of dnd) {
              if (networkId && envelope.networkId !== networkId) {
                continue;
              }
              await ctx.services.dispatcher.dispatch(envelope);
              dnd.delete(id);
            }
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
            dnd.set(key(envelope), structuredClone(envelope));
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
            await ctx.services.dispatcher.dispatch(item.envelope);
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
          rejections.push({ envelope: item.envelope, reason });
        },
      };
      ctx.provide('gate', service);
    },
  };
}
