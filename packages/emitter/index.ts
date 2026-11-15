import { randomUUID } from 'node:crypto';
import {
  isOfflinePeerSpooledError,
  isPeerOfflineError,
  resolveSemanticHeader,
  signEnvelope,
  signReceipt,
  verifyEnvelope,
  verifyReceipt,
} from '../core-types/index.ts';
import type {
  DeliveryReceipt,
  EmitMessageInput,
  EmitterService,
  MessageEnvelope,
  NodeCard,
  RouteDecision,
  SourcePlugin,
} from '../core-types/index.ts';
import type { OutboxStore } from '../mailbox/outbox.ts';
import type { Plugin } from '../kernel/index.ts';

export interface EmitterConfig {
  privateKeyPem?: string;
  onSourceError?: (error: unknown) => void;
  /** Durable offline fallback; when absent, unreachable targets fail emit as before. */
  outbox?: OutboxStore;
}

/** Unsigned envelope built by the emitter; semantic thread ids are resolved before signing. */
export interface EnvelopeInput<T = Record<string, unknown>>
  extends Omit<MessageEnvelope<T>, 'signature'> {}

export function createEmitterPlugin(config?: EmitterConfig): Plugin<EmitterConfig> {
  return {
    name: 'emitter',
    inject: ['transport', 'node', 'router'],
    apply(ctx, supplied) {
      const settings = { ...config, ...supplied };
      const privateKeyPem =
        settings.privateKeyPem ?? (ctx.services.node as { privateKeyPem?: string }).privateKeyPem;
      if (!privateKeyPem) {
        throw new Error('Emitter signing key required');
      }
      const sources = new Map<SourcePlugin, Promise<void>>();
      const outbox = settings.outbox;

      /**
       * Locally signed queued receipt: the message is durably owned by this sender's
       * outbox, so callers may acknowledge their inbox lease instead of polling.
       */
      const queuedReceipt = (
        envelope: MessageEnvelope,
        self: NodeCard,
        targetNodeId: string | undefined,
        relaySpooled: boolean,
        detail: string,
      ): DeliveryReceipt =>
        signReceipt(
          {
            messageId: envelope.id,
            networkId: envelope.networkId,
            fromNodeId: self.nodeId,
            stage: 'queued',
            detail,
            executionReceipt: {
              executor: 'outbox',
              executionId: `outbox:${envelope.id}`,
              status: 'accepted',
              handoff: 'queued',
              durable: true,
              queuedOffline: true,
              ...(targetNodeId !== undefined ? { targetNodeId } : {}),
              relaySpooled,
              summary: detail,
            },
            updatedAt: new Date().toISOString(),
          },
          privateKeyPem,
        );

      const service: EmitterService = {
        async emit<T>(input: EmitMessageInput<T>) {
          const { transport, node, router } = ctx.services;
          const self = node.getSelfCard(input.networkId);
          const sharedFiles = await Promise.all(
            (input.filePaths ?? []).map(async filePath => ({
              filePath,
              attachment: await transport.shareFile(filePath),
            })),
          );
          const attachments = [
            ...(input.attachments ?? []),
            ...sharedFiles.map(entry => entry.attachment),
          ];
          const stagedPaths = Object.fromEntries(
            sharedFiles.map(entry => [entry.attachment.blake3Hash, entry.filePath]),
          );
          const id = randomUUID();
          const semantic = input.semantic ? resolveSemanticHeader(input.semantic, id) : undefined;
          const draft: EnvelopeInput<T> = {
            id,
            traceId: input.traceId ?? randomUUID(),
            networkId: input.networkId,
            ...(input.replyToId ? { replyToId: input.replyToId } : {}),
            ...(input.idempotencyKey ? { idempotencyKey: input.idempotencyKey } : {}),
            ...(semantic ? { semantic } : {}),
            from: { nodeId: self.nodeId, ...self.identity },
            routing: input.routing,
            kind: input.kind,
            priority: input.priority ?? 'normal',
            subject: input.subject,
            body: input.body,
            payload: input.payload,
            attachments,
            createdAt: new Date().toISOString(),
          };
          const initial = signEnvelope(draft, privateKeyPem);
          const prepared = await ctx.waterfall('emitter/prepare', initial as MessageEnvelope);
          if (prepared.id !== id) {
            throw new Error('Prepare hook cannot change message id');
          }
          if (
            prepared.networkId !== input.networkId ||
            prepared.from.nodeId !== self.nodeId ||
            prepared.from.member !== self.identity.member ||
            prepared.from.nodeName !== self.identity.nodeName
          ) {
            throw new Error('Prepare hook cannot change sender or network identity');
          }
          // Hooks may inject a semantic header without a threadId; normalize it against the
          // authenticated message id before the final signature.
          const preparedSemantic =
            prepared.semantic !== undefined
              ? resolveSemanticHeader(prepared.semantic, prepared.id)
              : undefined;
          const envelope = signEnvelope(
            preparedSemantic !== undefined
              ? { ...prepared, semantic: preparedSemantic }
              : prepared,
            privateKeyPem,
          );
          if (!verifyEnvelope(envelope)) {
            throw new Error('Emitter key does not match node identity');
          }
          let route: RouteDecision;
          try {
            route = await router.route(envelope);
          } catch (error) {
            // An unresolved explicit selector is a peering gap, not a routing bug: the
            // durable outbox retries it once the peer directory learns the member again.
            const unresolved =
              error instanceof Error &&
              error.message.startsWith('Unresolved explicit target:');
            if (
              unresolved &&
              outbox !== undefined &&
              input.queueIfOffline !== false &&
              transport.listMountedNetworks().includes(envelope.networkId)
            ) {
              const detail = error.message;
              outbox.enqueue({
                envelope,
                explicitTargets: [...(input.routing.targets ?? [])],
                pendingNodeIds: [],
                filePaths: stagedPaths,
                lastError: detail,
              });
              const receipt = queuedReceipt(envelope, self, undefined, false, detail);
              ctx.emit('emitter/sent', {
                envelope,
                targetNodeIds: [],
                receipts: [receipt],
              });
              return { messageId: envelope.id, traceId: envelope.traceId, receipts: [receipt] };
            }
            throw error;
          }
          const frame = Buffer.from(JSON.stringify({ type: 'message.envelope', envelope }));
          const spoolBlobHashes = [...new Set(envelope.attachments.map(attachment => attachment.blake3Hash))];
          const deliveredReceipts: Record<string, DeliveryReceipt> = {};
          const pendingNodeIds: string[] = [];
          const relaySpooledNodeIds: string[] = [];
          let offlineDetail: string | undefined;
          const parse = (bytes: Uint8Array, target: string): DeliveryReceipt => {
            const receipt: DeliveryReceipt = JSON.parse(
              Buffer.from(bytes).toString('utf8'),
            );
            if (
              !verifyReceipt(receipt) ||
              receipt.messageId !== envelope.id ||
              receipt.networkId !== envelope.networkId ||
              receipt.fromNodeId !== target
            ) {
              throw new Error('Invalid or mismatched delivery receipt');
            }
            return receipt;
          };
          if (route.mode === 'broadcast') {
            await transport.broadcast(envelope.networkId, envelope.routing.topic ?? 'messages', frame);
          } else {
            for (const target of route.targetNodeIds) {
              try {
                deliveredReceipts[target] = parse(
                  await transport.sendUnicast(envelope.networkId, target, frame, {
                    spoolBlobHashes,
                  }),
                  target,
                );
              } catch (error) {
                // Receipt forgery, signature failures and every other local bug must
                // surface; only a proven offline peer becomes a durable obligation.
                if (
                  !isPeerOfflineError(error) ||
                  outbox === undefined ||
                  input.queueIfOffline === false
                ) {
                  throw error;
                }
                pendingNodeIds.push(target);
                if (isOfflinePeerSpooledError(error)) {
                  relaySpooledNodeIds.push(target);
                }
                offlineDetail = error instanceof Error ? error.message : String(error);
              }
            }
          }
          const receipts: DeliveryReceipt[] = Object.values(deliveredReceipts);
          if (pendingNodeIds.length > 0) {
            outbox.enqueue({
              envelope,
              explicitTargets: [...(input.routing.targets ?? [])],
              pendingNodeIds,
              deliveredReceipts,
              relaySpooledNodeIds,
              filePaths: stagedPaths,
              ...(offlineDetail !== undefined ? { lastError: offlineDetail } : {}),
            });
            for (const targetNodeId of pendingNodeIds) {
              const spooled = relaySpooledNodeIds.includes(targetNodeId);
              const detail = spooled
                ? `Queued in local outbox and spooled on relay seed for offline peer ${targetNodeId}`
                : `Queued in local outbox for offline peer ${targetNodeId}`;
              receipts.push(queuedReceipt(envelope, self, targetNodeId, spooled, detail));
            }
          }
          ctx.emit('emitter/sent', {
            envelope,
            targetNodeIds: route.targetNodeIds,
            receipts,
          });
          return { messageId: envelope.id, traceId: envelope.traceId, receipts };
        },
        registerSource(source) {
          if (sources.has(source)) {
            throw new Error('Source already registered');
          }
          let started: Promise<void>;
          try {
            started = Promise.resolve(source.start(service));
          } catch (error) {
            started = Promise.reject(error);
          }
          sources.set(source, started);
          void started.catch(error => settings.onSourceError?.(error));
          let disposed = false;
          const dispose = async () => {
            if (disposed) {
              return;
            }
            disposed = true;
            try {
              await source.stop();
              await started;
            } finally {
              sources.delete(source);
            }
          };
          ctx.effect(() => dispose);
          return { dispose };
        },
      };
      ctx.provide('emitter', service);
    },
  };
}
