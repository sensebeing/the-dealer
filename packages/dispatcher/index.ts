import { randomUUID } from 'node:crypto';
import { closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { signReceipt, verifyEnvelope, verifyReceipt } from '../core-types/index.ts';
import type { DispatchPolicyPlugin, DispatchReceipt, ExecutorPlugin, MessageEnvelope } from '../core-types/index.ts';
import type { Plugin } from '../kernel/index.ts';

export interface DispatcherConfig {
  privateKeyPem?: string;
  ledgerPath?: string;
  defaultExecutor?: string;
  kindExecutors?: Record<string, string>;
}
/** Atomic snapshots, with a dispatching marker persisted before any external effect.
 * An interrupted dispatch is intentionally not retried automatically: its external outcome is unknown.
 */
export class DeliveryLedger {
  #path?: string;
  #entries: Record<string, DispatchReceipt>;
  constructor(path?: string) {
    this.#path = path;
    const saved = path && existsSync(path) ? JSON.parse(readFileSync(path, 'utf8')) : {};
    if (!saved || typeof saved !== 'object' || Array.isArray(saved) || Object.values(saved).some(value => !value || typeof value !== 'object' || !['received', 'pending_approval', 'queued', 'dispatching', 'dispatched', 'settled', 'rejected', 'failed'].includes((value as DispatchReceipt).stage))) throw new Error('Invalid delivery ledger');
    this.#entries = saved;
  }
  get(key: string): DispatchReceipt | undefined { return this.#entries[key] && structuredClone(this.#entries[key]); }
  set(key: string, value: DispatchReceipt): void {
    const next = { ...this.#entries, [key]: value };
    if (this.#path) {
      mkdirSync(dirname(this.#path), { recursive: true });
      const temp = `${this.#path}.${randomUUID()}.tmp`;
      const fd = openSync(temp, 'wx', 0o600);
      try { writeFileSync(fd, JSON.stringify(next)); fsyncSync(fd); } finally { closeSync(fd); }
      renameSync(temp, this.#path);
      const dir = openSync(dirname(this.#path), 'r');
      try { fsyncSync(dir); } finally { closeSync(dir); }
    }
    this.#entries = next;
  }
}
export function createDispatcherPlugin(config: DispatcherConfig = {}): Plugin {
  return { name: 'dispatcher', apply(ctx) {
    const policies = new Set<DispatchPolicyPlugin>();
    const executors = new Map<string, { execute: (batch: readonly MessageEnvelope[]) => Promise<DispatchReceipt> }>();
    const ledger = new DeliveryLedger(config.ledgerPath);
    const active = new Map<string, Promise<DispatchReceipt>>();
    const controller = new AbortController();
    ctx.effect(() => async () => { controller.abort(); await Promise.allSettled(active.values()); });
    const key = (e: MessageEnvelope) => JSON.stringify([e.networkId, e.from.nodeId, e.id]);
    const select = (e: MessageEnvelope) => typeof e.payload?.executor === 'string' ? e.payload.executor : config.kindExecutors?.[e.kind] ?? config.defaultExecutor ?? executors.keys().next().value;
    const core = async (batch: readonly MessageEnvelope[]): Promise<DispatchReceipt> => {
      if (!batch.length) throw new Error('Empty execution batch');
      const name = select(batch[0]);
      if (batch.some(e => select(e) !== name || e.networkId !== batch[0].networkId)) throw new Error('Batch crosses executor or network boundary');
      const executor = name && executors.get(name);
      if (!executor) throw new Error(`Executor unavailable: ${name}`);
      for (const e of batch) {
        const prior = ledger.get(key(e));
        if (prior && prior.stage !== 'queued') throw new Error(`Execution already attempted: ${e.id} (${prior.stage})`);
      }
      for (const e of batch) ledger.set(key(e), { stage: 'dispatching' });
      try {
        const result = await executor.execute(batch);
        for (const e of batch) ledger.set(key(e), result);
        return result;
      } catch (error) {
        for (const e of batch) ledger.set(key(e), { stage: 'failed', detail: String(error) });
        throw error;
      }
    };
    ctx.provide('dispatcher', {
      registerPolicy(policy) { policies.add(policy); return { dispose() { policies.delete(policy); } }; },
      registerExecutor<C>(executor: ExecutorPlugin<C>, executorConfig: C) {
        if (executors.has(executor.name)) throw new Error(`Duplicate executor: ${executor.name}`);
        const entry = { async execute(batch: readonly MessageEnvelope[]): Promise<DispatchReceipt> {
          const first = batch[0];
          const nodeId = ctx.services.node.getSelfCard(first.networkId).nodeId;
          return { stage: 'dispatched', executionReceipt: await executor.execute(batch, { networkId: first.networkId, nodeId, traceId: first.traceId, signal: controller.signal }, executorConfig) };
        } };
        executors.set(executor.name, entry);
        return { dispose() { if (executors.get(executor.name) === entry) executors.delete(executor.name); } };
      },
      dispatch(envelope) {
        if (controller.signal.aborted) return Promise.reject(new Error('Dispatcher stopped'));
        const id = key(envelope);
        const ongoing = active.get(id);
        if (ongoing) return ongoing;
        const prior = ledger.get(id);
        if (prior) return Promise.resolve(prior);
        const chain = [...policies];
        const run = async (index: number, batch: readonly MessageEnvelope[]): Promise<DispatchReceipt> => {
          if (index === chain.length) return core(batch);
          // Policies consume individual envelopes; batching policies should be registered last.
          let result: DispatchReceipt = { stage: 'queued' };
          for (const e of batch) {
            let called = false;
            result = await chain[index].process(e, nextBatch => {
              if (called) throw new Error('Policy next may only be called once');
              called = true;
              return run(index + 1, nextBatch);
            });
          }
          return result;
        };
        const promise = Promise.resolve().then(() => run(0, [structuredClone(envelope)])).then(result => {
          // A queued policy owns its pending buffer; do not overwrite a later wave's completed state.
          if (!ledger.get(id)) ledger.set(id, result);
          return result;
        }).finally(() => active.delete(id));
        active.set(id, promise);
        return promise;
      },
    });
    ctx.on('transport/inbound', async event => {
      const frame = event.payload as { type?: string; envelope?: MessageEnvelope } | null;
      if (frame?.type !== 'message.envelope' || !frame.envelope) return;
      const envelope = frame.envelope;
      const self = ctx.services.node.getSelfCard(event.networkId);
      const signingKey = config.privateKeyPem ?? (ctx.services.node as { privateKeyPem?: string }).privateKeyPem;
      if (!signingKey) throw new Error('Dispatcher signing key is required for inbound receipts');
      const identityProof = signReceipt({ messageId: envelope.id, networkId: event.networkId, fromNodeId: self.nodeId, stage: 'received', updatedAt: new Date().toISOString() }, signingKey);
      if (!verifyReceipt(identityProof)) throw new Error('Dispatcher key does not match node identity');
      let receipt: DispatchReceipt;
      if (!verifyEnvelope(envelope) || envelope.networkId !== event.networkId || envelope.from.nodeId !== event.senderNodeId) {
        receipt = { stage: 'rejected', detail: 'Invalid envelope signature or authenticated sender/network mismatch' };
      } else {
        const verdict = await ctx.services.gate.evaluate(envelope);
        if (verdict.status === 'allow') {
          try { receipt = await ctx.services.dispatcher.dispatch(envelope); }
          catch (error) { receipt = { stage: 'failed', detail: String(error) }; }
        } else receipt = { stage: verdict.status === 'pending_approval' ? 'pending_approval' : verdict.status === 'queued_dnd' ? 'queued' : 'rejected', detail: verdict.reason };
      }
      const signed = signReceipt({ messageId: envelope.id, networkId: event.networkId, fromNodeId: self.nodeId, ...receipt, updatedAt: new Date().toISOString() }, signingKey);
      if (!verifyReceipt(signed)) throw new Error('Dispatcher key does not match node identity');
      return Buffer.from(JSON.stringify(signed));
    });
  } };
}
