import { randomUUID } from 'node:crypto';
import { signEnvelope, verifyEnvelope, verifyReceipt } from '../core-types/index.ts';
import type { DeliveryReceipt, EmitMessageInput, EmitterService, MessageEnvelope, SourcePlugin } from '../core-types/index.ts';
import type { Plugin } from '../kernel/index.ts';

export interface EmitterConfig { privateKeyPem: string; onSourceError?: (error: unknown) => void }
export function createEmitterPlugin(config?: EmitterConfig): Plugin<EmitterConfig> {
  return { name: 'emitter', inject: ['transport', 'node', 'router'], apply(ctx, supplied) {
    const settings = config ?? supplied;
    if (!settings?.privateKeyPem) throw new Error('Emitter signing key required');
    const sources = new Map<SourcePlugin, Promise<void>>();
    const service: EmitterService = {
      async emit<T>(input: EmitMessageInput<T>) {
        const { transport, node, router } = ctx.services;
        const self = node.getSelfCard(input.networkId);
        const attachments = [...input.attachments ?? [], ...await Promise.all((input.filePaths ?? []).map(fp => transport.shareFile(fp)))];
        const initial = signEnvelope({
          id: randomUUID(), traceId: input.traceId ?? randomUUID(), networkId: input.networkId,
          ...(input.replyToId ? { replyToId: input.replyToId } : {}), ...(input.idempotencyKey ? { idempotencyKey: input.idempotencyKey } : {}),
          from: { nodeId: self.nodeId, ...self.identity }, routing: input.routing, kind: input.kind,
          priority: input.priority ?? 'normal', subject: input.subject, body: input.body, payload: input.payload,
          attachments, createdAt: new Date().toISOString(),
        }, settings.privateKeyPem);
        const prepared = await ctx.waterfall('emitter/prepare', initial as MessageEnvelope);
        if (prepared.networkId !== input.networkId || prepared.from.nodeId !== self.nodeId || prepared.from.member !== self.identity.member || prepared.from.nodeName !== self.identity.nodeName) throw new Error('Prepare hook cannot change sender or network identity');
        const envelope = signEnvelope(prepared, settings.privateKeyPem);
        if (!verifyEnvelope(envelope)) throw new Error('Emitter key does not match node identity');
        const route = await router.route(envelope);
        const frame = Buffer.from(JSON.stringify({ type: 'message.envelope', envelope }));
        const receipts: DeliveryReceipt[] = [];
        const parse = (bytes: Uint8Array, target: string) => {
          const receipt: DeliveryReceipt = JSON.parse(Buffer.from(bytes).toString('utf8'));
          if (!verifyReceipt(receipt) || receipt.messageId !== envelope.id || receipt.networkId !== envelope.networkId || receipt.fromNodeId !== target) throw new Error('Invalid or mismatched delivery receipt');
          receipts.push(receipt);
        };
        if (route.mode === 'broadcast') await transport.broadcast(envelope.networkId, envelope.routing.topic ?? 'messages', frame);
        else if (route.mode === 'unicast') parse(await transport.sendUnicast(envelope.networkId, route.targetNodeIds[0], frame), route.targetNodeIds[0]);
        else {
          const results = await transport.sendMulticast(envelope.networkId, route.targetNodeIds, frame);
          for (const target of route.targetNodeIds) {
            const result = results.get(target);
            if (!result) throw new Error(`Missing multicast response: ${target}`);
            if (!result.ok) throw result.error;
            parse(result.value, target);
          }
        }
        return { messageId: envelope.id, traceId: envelope.traceId, receipts };
      },
      registerSource(source) {
        if (sources.has(source)) throw new Error('Source already registered');
        let started: Promise<void>;
        try { started = Promise.resolve(source.start(service)); } catch (error) { started = Promise.reject(error); }
        sources.set(source, started);
        void started.catch(error => settings.onSourceError?.(error));
        let disposed = false;
        const dispose = async () => {
          if (disposed) return; disposed = true;
          try { await source.stop(); await started; } finally { sources.delete(source); }
        };
        ctx.effect(() => dispose);
        return { dispose };
      },
    };
    ctx.provide('emitter', service);
  } };
}
