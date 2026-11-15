import { randomUUID } from 'node:crypto';
import {
  isConfirmedHandoffReceipt,
  isOfflinePeerSpooledError,
  isPeerOfflineError,
  isUnresolvedTargetError,
  LocalDeliveryError,
  resolveSemanticHeader,
  signEnvelope,
  signReceipt,
  verifyEnvelope,
  verifyReceipt,
} from '../core-types/index.ts';
import type {
  DeliveryReceipt,
  EmitMessageInput,
  EmitResult,
  EmitterService,
  MessageEnvelope,
  NodeCard,
  OutboxDeliveryFailure,
  OutboxDeliveryOutcome,
  OutboxItem,
  OutboxStoreLike,
  RouteDecision,
  SourcePlugin,
} from '../core-types/index.ts';
import type { Plugin } from '../kernel/index.ts';

export interface EmitterConfig {
  privateKeyPem?: string;
  onSourceError?: (error: unknown) => void;
  /** Durable offline fallback; when absent, unreachable targets fail emit as before. */
  outbox?: OutboxStoreLike;
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

      /** Verify one signed reply before it may settle or fail a durable obligation. */
      const parseReceipt = (
        bytes: Uint8Array,
        envelope: MessageEnvelope,
        target: string,
      ): DeliveryReceipt => {
        let receipt: DeliveryReceipt;
        try {
          receipt = JSON.parse(Buffer.from(bytes).toString('utf8'));
        } catch {
          throw new LocalDeliveryError('Invalid delivery receipt payload');
        }
        if (
          !verifyReceipt(receipt) ||
          receipt.messageId !== envelope.id ||
          receipt.networkId !== envelope.networkId ||
          receipt.fromNodeId !== target
        ) {
          throw new LocalDeliveryError('Invalid or mismatched delivery receipt');
        }
        return receipt;
      };

      /** Previously unresolved explicit selectors converge their reply route once resolved. */
      const service: EmitterService = {
        async emit<T>(input: EmitMessageInput<T>) {
          const { transport, node, router } = ctx.services;
          const self = node.getSelfCard(input.networkId);
          const mounted = transport.listMountedNetworks().includes(input.networkId);
          const queueIfOffline = outbox !== undefined && input.queueIfOffline !== false;

          // A re-emitted idempotent item converges on the existing durable obligation
          // instead of creating a second outbox entry and relay spool copy.
          const existing =
            queueIfOffline && input.idempotencyKey
              ? outbox.findQueuedByIdempotency?.(input.networkId, input.idempotencyKey)
              : undefined;
          if (existing) {
            return replayQueued(existing);
          }

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
            if (isUnresolvedTargetError(error) && queueIfOffline && mounted) {
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
          const spoolBlobHashes = [
            ...new Set([
              ...Object.keys(stagedPaths),
              ...envelope.attachments.map(attachment => attachment.blake3Hash),
            ]),
          ];
          // Spooling is caller-owned policy: an explicit queueIfOffline:false send must not
          // leave any durable relay copy, otherwise the caller could retry a queued message.
          const spoolOptions = queueIfOffline
            ? {
                spoolIfOffline: true,
                spoolKey: envelope.id,
                spoolBlobHashes,
              }
            : {};
          const deliveredReceipts: Record<string, DeliveryReceipt> = {};
          const pendingNodeIds: string[] = [];
          const relaySpooledNodeIds: string[] = [];
          let offlineDetail: string | undefined;
          if (route.mode === 'broadcast') {
            await transport.broadcast(envelope.networkId, envelope.routing.topic ?? 'messages', frame);
          } else {
            for (const target of route.targetNodeIds) {
              try {
                deliveredReceipts[target] = parseReceipt(
                  await transport.sendUnicast(envelope.networkId, target, frame, spoolOptions),
                  envelope,
                  target,
                );
              } catch (error) {
                // Receipt forgery, signature failures and every other local bug must
                // surface; only a proven offline peer becomes a durable obligation.
                if (!isPeerOfflineError(error) || !queueIfOffline) {
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
        async redriveOutboxItem(
          item: OutboxItem,
          options: { targetNodeIds?: readonly string[]; timeoutMs?: number } = {},
        ): Promise<OutboxDeliveryOutcome> {
          const { transport, node, router } = ctx.services;
          // Staged evidence must still exist before any byte is re-sent; a missing copy is
          // a deterministic local failure, not a reason to retry the same broken payload.
          for (const [hash, filePath] of Object.entries(item.stagedFiles)) {
            try {
              transport.registerBlob?.(hash, filePath);
            } catch (error) {
              return {
                failed: {
                  error: `Missing staged attachment ${hash}: ${
                    error instanceof Error ? error.message : String(error)
                  }`,
                },
              };
            }
          }
          let targets = options.targetNodeIds !== undefined
            ? [...options.targetNodeIds]
            : item.pendingNodeIds.filter(nodeId => !item.deliveredReceipts[nodeId]);
          const untouched = item.pendingNodeIds.filter(
            nodeId => !targets.includes(nodeId) && !item.deliveredReceipts[nodeId],
          );
          let resolvedTargetNodeIds: readonly string[] | undefined;
          if (targets.length === 0 && options.targetNodeIds === undefined) {
            let route: RouteDecision;
            try {
              route = await router.route(item.envelope);
            } catch (error) {
              if (isUnresolvedTargetError(error)) {
                return { lastError: error.message, pendingNodeIds: [] };
              }
              return {
                failed: { error: error instanceof Error ? error.message : String(error) },
              };
            }
            if (route.mode === 'broadcast' || route.targetNodeIds.length === 0) {
              return { failed: { error: 'Outbox item has no addressable target' } };
            }
            resolvedTargetNodeIds = route.targetNodeIds.filter(
              nodeId => !item.deliveredReceipts[nodeId],
            );
            targets = [...resolvedTargetNodeIds];
          }
          const frame = Buffer.from(
            JSON.stringify({ type: 'message.envelope', envelope: item.envelope }),
          );
          const spoolBlobHashes = [
            ...new Set([
              ...Object.keys(item.stagedFiles),
              ...item.envelope.attachments.map(attachment => attachment.blake3Hash),
            ]),
          ];
          const deliveredReceipts: Record<string, DeliveryReceipt> = {};
          const pendingNodeIds: string[] = [];
          const relaySpooledNodeIds: string[] = [];
          let failed: OutboxDeliveryFailure | undefined;
          let lastError: string | undefined;
          for (const target of targets) {
            try {
              const receipt = parseReceipt(
                await transport.sendUnicast(item.networkId, target, frame, {
                  ...(options.timeoutMs !== undefined ? { timeoutMs: options.timeoutMs } : {}),
                  spoolIfOffline: true,
                  spoolKey: item.envelope.id,
                  spoolBlobHashes,
                }),
                item.envelope,
                target,
              );
              if (
                isConfirmedHandoffReceipt(receipt, { allowQueued: true }) ||
                receipt.stage === 'pending_approval'
              ) {
                deliveredReceipts[target] = receipt;
                continue;
              }
              if (receipt.stage === 'rejected' || receipt.stage === 'failed') {
                // A signed rejection/failure is terminal: retrying identical bytes would
                // just reproduce the target's decision while hiding it from the operator.
                failed = {
                  nodeId: target,
                  receipt,
                  error: `Target ${target} returned ${receipt.stage}${
                    receipt.detail ? `: ${receipt.detail}` : ''
                  }`,
                };
                break;
              }
              // Reached the target but not durably handed off (memory-only queue/approval
              // ingress): keep the obligation and retry under the ordinary backoff.
              pendingNodeIds.push(target);
              lastError = `Target ${target} returned ${receipt.stage}`;
            } catch (error) {
              if (isPeerOfflineError(error)) {
                pendingNodeIds.push(target);
                if (isOfflinePeerSpooledError(error)) {
                  relaySpooledNodeIds.push(target);
                }
                lastError = error instanceof Error ? error.message : String(error);
                continue;
              }
              failed = {
                nodeId: target,
                error: `Local or protocol failure: ${
                  error instanceof Error ? error.message : String(error)
                }`,
              };
              break;
            }
          }
          return {
            ...(resolvedTargetNodeIds !== undefined ? { resolvedTargetNodeIds } : {}),
            deliveredReceipts,
            pendingNodeIds: [...new Set([...untouched, ...pendingNodeIds])],
            relaySpooledNodeIds,
            ...(failed !== undefined ? { failed } : {}),
            ...(lastError !== undefined ? { lastError } : {}),
          };
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

      const selfCardFor = (item: OutboxItem): NodeCard => {
        const self = ctx.services.node.getSelfCard(item.networkId);
        if (self.nodeId !== item.envelope.from.nodeId) {
          throw new Error('Outbox sender identity changed');
        }
        return self;
      };

      /** Reuse an already durable obligation without emitting a second outbox entry. */
      const replayQueued = (existing: OutboxItem): EmitResult => {
        const receipts: DeliveryReceipt[] = Object.values(existing.deliveredReceipts);
        const pending = existing.pendingNodeIds.length > 0
          ? existing.pendingNodeIds
          : [undefined];
        for (const targetNodeId of pending) {
          const spooled =
            targetNodeId !== undefined && existing.relaySpooledNodeIds.includes(targetNodeId);
          const detail =
            targetNodeId === undefined
              ? `Reused durable outbox obligation ${existing.id}`
              : spooled
                ? `Reused durable outbox obligation ${existing.id}; spooled on relay seed for offline peer ${targetNodeId}`
                : `Reused durable outbox obligation ${existing.id} for offline peer ${targetNodeId}`;
          receipts.push(queuedReceipt(existing.envelope, selfCardFor(existing), targetNodeId, spooled, detail));
        }
        return {
          messageId: existing.envelope.id,
          traceId: existing.envelope.traceId,
          receipts,
        };
      };

      ctx.provide('emitter', service);
    },
  };
}
