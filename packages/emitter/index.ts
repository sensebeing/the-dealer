import { randomUUID } from 'node:crypto';
import {
  committedOutboxTargets,
  isConfirmedHandoffReceipt,
  envelopeClaimVersion,
  isDeliveryTimeoutFailure,
  isNetworkUnmountedError,
  isOfflinePeerSpooledError,
  isPeerOfflineError,
  isRetainedApprovalReceipt,
  isTerminalDeliveryFailure,
  isUnattemptedOutboxItem,
  isUnresolvedTargetError,
  LocalDeliveryError,
  NetworkUnmountedError,
  outboxReconciliationTargets,
  outstandingRelaySpooledTargets,
  receiptMatchesEnvelopeSignature,
  resolveSemanticHeader,
  sameEnvelopeIntent,
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
  OutboxRedriveDeliveryOptions,
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
  /** Total budget for one initial or redrive send attempt; overridable in tests. */
  attemptTimeoutMs?: number;
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
      const keyLocks = new Map<string, Promise<unknown>>();
      const sources = new Map<SourcePlugin, Promise<void>>();
      let activeHolds = 0;
      const held = new Map<() => void, () => void>();
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
          receipt.fromNodeId !== target ||
          !receiptMatchesEnvelopeSignature(receipt, envelope.signature)
        ) {
          throw new LocalDeliveryError('Invalid or mismatched delivery receipt');
        }
        return receipt;
      };

      type TargetAttempt =
        | { status: 'delivered'; receipt: DeliveryReceipt }
        | { status: 'failed'; receipt: DeliveryReceipt; error: string }
        | {
            status: 'pending';
            error: string;
            inDoubt: boolean;
            relaySpooled: boolean;
            approvalPending?: boolean;
            /** Nonterminal live receipt (e.g. pending_approval) returned to the caller. */
            receipt?: DeliveryReceipt;
          }
        | { status: 'local-failure'; error: Error; beforeSend: boolean };

      const clearedFences = (targets: readonly string[], attempts: readonly (TargetAttempt | undefined)[]): string[] =>
        targets.filter((_, index) => {
          const attempt = attempts[index];
          if (attempt?.status === 'local-failure') return attempt.beforeSend;
          return attempt?.status !== 'pending' || !attempt.inDoubt;
        });

      const prepareSend = (id: string, attemptToken: string, targets: readonly string[]): void => {
        if (outbox?.prepareSend && targets.length > 0 && !outbox.prepareSend(id, attemptToken, targets)) {
          throw new LocalDeliveryError(`Outbox attempt no longer owns obligation ${id}`);
        }
      };

      /**
       * The one per-target send/classification channel. Initial emit and outbox redrive
       * differ only in how they persist the result; receipt verification, terminal
       * evidence, nonterminal ingress, offline policy, and the typed in-doubt timeout rule
       * live here exactly once.
       */
      const attemptTarget = async (
        envelope: MessageEnvelope,
        target: string,
        frame: Uint8Array,
        options: {
          deadline: number;
          mounted: boolean;
          queueIfOffline: boolean;
          spoolOptions: {
            spoolIfOffline?: boolean;
            spoolKey?: string;
            spoolBlobHashes?: readonly string[];
          };
        },
      ): Promise<TargetAttempt> => {
        const localFailure = (error: unknown, beforeSend = false): TargetAttempt => ({
          status: 'local-failure',
          error: error instanceof Error ? error : new Error(String(error)),
          beforeSend,
        });
        if (!options.mounted && target !== envelope.from.nodeId) {
          const error = new NetworkUnmountedError(envelope.networkId);
          return options.queueIfOffline
            ? {
                status: 'pending',
                error: error.message,
                inDoubt: false,
                relaySpooled: false,
              }
            : localFailure(error, true);
        }
        const remaining = options.deadline - Date.now();
        if (remaining <= 0) {
          const error = new LocalDeliveryError(
            `Delivery deadline exhausted before send to ${target}`,
          );
          return options.queueIfOffline
            ? {
                status: 'pending',
                error: error.message,
                inDoubt: false,
                relaySpooled: false,
              }
            : localFailure(error, true);
        }
        try {
          const receipt = parseReceipt(
            await ctx.services.transport.sendUnicast(envelope.networkId, target, frame, {
              timeoutMs: remaining,
              ...options.spoolOptions,
            }),
            envelope,
            target,
          );
          if (isConfirmedHandoffReceipt(receipt, { allowQueued: true })) {
            return { status: 'delivered', receipt };
          }
          if (isTerminalDeliveryFailure(receipt)) {
            return {
              status: 'failed',
              receipt,
              error: `Target ${target} returned ${receipt.stage}${
                receipt.detail ? `: ${receipt.detail}` : ''
              }`,
            };
          }
          // Return the signed receipt for a memory-only queue or approval wait. The caller
          // records this live custody when it owns a durable outbox obligation.
          const detail = `Target ${target} returned ${receipt.stage}`;
          return {
            status: 'pending',
            error: detail,
            inDoubt: false,
            relaySpooled: false,
            approvalPending: receipt.stage === 'pending_approval' || isRetainedApprovalReceipt(receipt),
            receipt,
          };
        } catch (error) {
          if (isNetworkUnmountedError(error)) {
            return options.queueIfOffline
              ? {
                  status: 'pending',
                  error: error.message,
                  inDoubt: false,
                  relaySpooled: false,
                }
              : localFailure(error);
          }
          if (isPeerOfflineError(error)) {
            if (!options.queueIfOffline) {
              return localFailure(error);
            }
            const relaySpooled = isOfflinePeerSpooledError(error);
            return {
              status: 'pending',
              error: error instanceof Error ? error.message : String(error),
              // A confirmed relay-spool handoff has a durable external owner; a raw
              // timeout may already have executed at the target and must stay parked.
              inDoubt: !relaySpooled && isDeliveryTimeoutFailure(error),
              relaySpooled,
            };
          }
          if (isDeliveryTimeoutFailure(error)) {
            if (!options.queueIfOffline) {
              return localFailure(error);
            }
            return {
              status: 'pending',
              error: error instanceof Error ? error.message : String(error),
              inDoubt: true,
              relaySpooled: false,
            };
          }
          // Receipt forgery, signature failures, malformed responses, and every other
          // deterministic local/protocol bug surface instead of becoming offline state.
          return localFailure(error);
        }
      };

      /** Previously unresolved explicit selectors converge their reply route once resolved. */
      const service: EmitterService = {
        async emit<T>(input: EmitMessageInput<T>) {
          if (input.idempotencyKey !== undefined &&
              (typeof input.idempotencyKey !== 'string' || !input.idempotencyKey.trim())) {
            throw new Error('idempotencyKey must be a non-empty string');
          }
          const { transport, node, router } = ctx.services;
          const self = node.getSelfCard(input.networkId);
          const mounted = transport.listMountedNetworks().includes(input.networkId);
          const queueIfOffline = outbox !== undefined && input.queueIfOffline !== false;

          const sharedFiles = await Promise.all(
            (input.filePaths ?? []).map(async filePath => ({
              filePath,
              attachment: await transport.shareFile(filePath),
            })),
          );
          const attachmentPaths = input.attachmentPaths ?? {};
          const preSharedFiles = await Promise.all(
            (input.attachments ?? []).map(async attachment => {
              const filePath = attachmentPaths[attachment.blake3Hash];
              if (filePath === undefined) {
                return undefined;
              }
              const shared = await transport.shareFile(filePath);
              if (
                shared.blake3Hash !== attachment.blake3Hash ||
                shared.sizeBytes !== attachment.sizeBytes
              ) {
                throw new LocalDeliveryError(
                  `Attachment path does not match declared attachment ${attachment.blake3Hash}`,
                );
              }
              return { filePath, attachment: shared };
            }),
          );
          const attachments = [
            ...(input.attachments ?? []),
            ...sharedFiles.map(entry => entry.attachment),
          ];
          const stagedPaths = Object.fromEntries([
            ...sharedFiles.map(entry => [entry.attachment.blake3Hash, entry.filePath] as const),
            ...preSharedFiles.flatMap(entry =>
              entry === undefined
                ? []
                : [[entry.attachment.blake3Hash, entry.filePath] as const],
            ),
          ]);
          const id = randomUUID();
          const semantic = input.semantic ? resolveSemanticHeader(input.semantic, id) : undefined;
          const draft: EnvelopeInput<T> = {
            id,
            traceId: input.traceId ?? randomUUID(),
            networkId: input.networkId,
            ...(input.replyToId ? { replyToId: input.replyToId } : {}),
            ...(input.idempotencyKey !== undefined ? { idempotencyKey: input.idempotencyKey } : {}),
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
          if (input.idempotencyKey !== undefined && prepared.idempotencyKey !== input.idempotencyKey) {
            throw new Error('Prepare hook cannot change caller idempotencyKey');
          }
          if (prepared.idempotencyKey !== undefined &&
              (typeof prepared.idempotencyKey !== 'string' || !prepared.idempotencyKey.trim())) {
            throw new Error('idempotencyKey must be a non-empty string');
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
          const keyedLookup = outbox !== undefined && envelope.idempotencyKey !== undefined;
          const deliver = async (): Promise<EmitResult> => {
            let claimedItem: OutboxItem | undefined;
            let attemptToken: string | undefined;
            let replaceable: OutboxItem | undefined;
            const claimInput = { envelope, explicitTargets: [...(envelope.routing.targets ?? [])], filePaths: stagedPaths };
            const mayReplace = (existing: OutboxItem): boolean =>
              envelopeClaimVersion(existing.envelope) === envelopeClaimVersion(envelope) &&
              isUnattemptedOutboxItem(existing) && !sameEnvelopeIntent(existing.envelope, envelope);
            try {
              if (keyedLookup) {
                const version = envelopeClaimVersion(envelope);
                if (version === null) throw new Error('Invalid claim version');
                const existing = outbox.findKeyedObligation?.(envelope.networkId, envelope.idempotencyKey!, version);
                if (existing) {
                  if (!mayReplace(existing)) return reconcileObligation(existing);
                  if (envelope.routing.mode === 'broadcast') {
                    throw new LocalDeliveryError('Keyed outbox obligation cannot route to broadcast');
                  }
                  replaceable = existing;
                }
              }
              const keyedClaim = keyedLookup &&
                ((queueIfOffline && envelope.routing.mode !== 'broadcast') || replaceable !== undefined);
              // Durable obligations need local sources for recovery, including replacements
              // whose caller disabled offline queueing.
              if (queueIfOffline || keyedClaim) {
                for (const attachment of envelope.attachments) {
                  if (!stagedPaths[attachment.blake3Hash]) {
                    throw new LocalDeliveryError(
                      `Attachment ${attachment.blake3Hash} requires attachmentPaths[hash] for durable outbox staging`,
                    );
                  }
                }
              }
              let route: RouteDecision | { unresolved: string };
              try {
                route = await router.route(envelope);
                if (keyedClaim && route.mode === 'broadcast') {
                  throw new LocalDeliveryError('Keyed outbox obligation cannot route to broadcast');
                }
              } catch (error) {
                // An unresolved explicit selector is a peering gap, not a routing bug: the
                // durable outbox retries it once the peer directory learns the member again.
                // Network mount state is irrelevant here; identity resolution is directory-owned.
                if (!isUnresolvedTargetError(error) || !queueIfOffline) throw error;
                route = { unresolved: error.message };
              }
              if (keyedClaim) {
                const plannedClaim = 'unresolved' in route || route.mode === 'broadcast' ? claimInput
                  : { ...claimInput, plannedTargetNodeIds: route.targetNodeIds };
                let claim = replaceable ? undefined : outbox.claimIdempotency(plannedClaim);
                if (claim && !claim.claimed && mayReplace(claim.item)) replaceable = claim.item;
                if (replaceable) {
                  claim = outbox.replaceUnattemptedIdempotency({ ...plannedClaim, expectedId: replaceable.id });
                }
                if (!claim) throw new LocalDeliveryError('Outbox obligation disappeared before replacement');
                if (!claim.claimed) return reconcileObligation(claim.item);
                claimedItem = claim.item;
                attemptToken = claim.attemptToken;
              }
              if ('unresolved' in route) {
                const queued = outbox!.enqueue({
                  ...claimInput, attemptToken, sentCommitted: true, pendingNodeIds: [], lastError: route.unresolved,
                });
                const receipt = queuedReceipt(envelope, self, undefined, false, route.unresolved);
                ctx.emit('emitter/sent', { envelope, targetNodeIds: [], receipts: [receipt], createdAt: queued.createdAt });
                return { messageId: envelope.id, traceId: envelope.traceId, receipts: [receipt] };
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
              // A live nonterminal receipt proves the target received the envelope.
              const livePendingReceipts: Record<string, DeliveryReceipt> = {};
              const failedReceipts: Record<string, DeliveryReceipt> = {};
              const failedNodeIds: string[] = [];
              const observedReceipts: DeliveryReceipt[] = [];
              const pendingNodeIds: string[] = [];
              const inDoubtNodeIds: string[] = [];
              const relaySpooledNodeIds: string[] = [];
              const approvalPendingNodeIds: string[] = [];
              let clearFenceNodeIds: readonly string[] | undefined;
              let offlineDetail: string | undefined;
              let fatalError: Error | undefined;
              if (route.mode === 'broadcast') {
                await transport.broadcast(envelope.networkId, envelope.routing.topic ?? 'messages', frame);
              } else {
                const deadline = Date.now() + (settings.attemptTimeoutMs ?? 10_000);
                const attempts = await Promise.all(route.targetNodeIds.map(target =>
                  attemptTarget(envelope, target, frame, {
                    deadline,
                    mounted,
                    queueIfOffline,
                    spoolOptions,
                  }),
                ));
                clearFenceNodeIds = clearedFences(route.targetNodeIds, attempts);
                for (const [index, target] of route.targetNodeIds.entries()) {
                  const attempt = attempts[index];
                  if (attempt.status === 'delivered') {
                    deliveredReceipts[target] = attempt.receipt;
                    observedReceipts.push(attempt.receipt);
                    continue;
                  }
                  if (attempt.status === 'failed') {
                    // A signed rejection/failure is evidence, never delivery. If another
                    // target needs the durable outbox, preserve this per-target decision.
                    failedReceipts[target] = attempt.receipt;
                    observedReceipts.push(attempt.receipt);
                    offlineDetail ??= attempt.error;
                    continue;
                  }
                  if (attempt.status === 'pending') {
                    if (attempt.receipt) {
                      observedReceipts.push(attempt.receipt);
                      livePendingReceipts[target] = attempt.receipt;
                    }
                    if (!queueIfOffline) {
                      if (claimedItem && attempt.receipt) {
                        if (attempt.approvalPending) {
                          pendingNodeIds.push(target);
                          approvalPendingNodeIds.push(target);
                        } else {
                          failedNodeIds.push(target);
                          inDoubtNodeIds.push(target);
                        }
                        offlineDetail ??= attempt.error;
                      }
                      continue;
                    }
                    pendingNodeIds.push(target);
                    if (attempt.approvalPending) approvalPendingNodeIds.push(target);
                    if (attempt.inDoubt) {
                      inDoubtNodeIds.push(target);
                    }
                    if (attempt.relaySpooled) {
                      relaySpooledNodeIds.push(target);
                    }
                    offlineDetail = attempt.error;
                    continue;
                  }
                  if (!queueIfOffline && !claimedItem) {
                    throw attempt.error;
                  }
                  // Deterministic local/protocol failure: terminal for this target, but sibling
                  // offline obligations must be durably enqueued before the error surfaces.
                  failedNodeIds.push(target);
                  if (!attempt.beforeSend) inDoubtNodeIds.push(target);
                  offlineDetail ??= attempt.error.message;
                  fatalError ??= attempt.error;
                }
              }
              if (claimedItem && route.mode !== 'broadcast' && route.targetNodeIds.length === 0) {
                outbox!.markFailed(claimedItem.id, 'Outbox item has no addressable target', attemptToken);
                throw new LocalDeliveryError('Outbox item has no addressable target');
              }
              const receipts: DeliveryReceipt[] = [...observedReceipts];
              let committedItem: OutboxItem | undefined;
              if (outbox && ((queueIfOffline && pendingNodeIds.length > 0) || claimedItem !== undefined)) {
                committedItem = outbox.enqueue({
                  attemptToken,
                  clearFenceNodeIds,
                  envelope,
                  explicitTargets: [...(envelope.routing.targets ?? [])],
                  pendingNodeIds,
                  deliveredReceipts,
                  failedReceipts,
                  failedNodeIds,
                  inDoubtNodeIds,
                  relaySpooledNodeIds,
                  ...(approvalPendingNodeIds.length > 0 ? { approvalPendingNodeIds } : {}),
                  filePaths: stagedPaths,
                  ...(offlineDetail !== undefined ? { lastError: offlineDetail } : {}),
                });
              }
              if (queueIfOffline) {
                for (const targetNodeId of pendingNodeIds) {
                  const spooled = relaySpooledNodeIds.includes(targetNodeId);
                  const detail = spooled
                    ? `Queued in local outbox and spooled on relay seed for offline peer ${targetNodeId}`
                    : `Queued in local outbox for offline peer ${targetNodeId}`;
                  receipts.push(queuedReceipt(envelope, self, targetNodeId, spooled, detail));
                }
              }
              // Only a broadcast, live handoff, or durable obligation commits a reverse route.
              const outboxTargets = new Set(claimedItem && committedItem ? committedOutboxTargets(committedItem) : pendingNodeIds);
              const committedTargetNodeIds = route.targetNodeIds.filter(
                target =>
                  deliveredReceipts[target] !== undefined ||
                  livePendingReceipts[target] !== undefined ||
                  outboxTargets.has(target),
              );
              const emitSent = (): void => {
                if (route.mode !== 'broadcast' && committedTargetNodeIds.length === 0) {
                  return;
                }
                ctx.emit('emitter/sent', {
                  envelope,
                  targetNodeIds:
                    route.mode === 'broadcast' ? route.targetNodeIds : committedTargetNodeIds,
                  receipts,
                  ...(committedItem ? { createdAt: committedItem.createdAt } : {}),
                });
              };
              if (fatalError !== undefined) {
                // Queued sibling receipts remain observable even though this deterministic
                // target failure is surfaced to the caller.
                (fatalError as Error & { receipts?: DeliveryReceipt[] }).receipts = receipts;
                emitSent();
                throw fatalError;
              }
              emitSent();
              return { messageId: envelope.id, traceId: envelope.traceId, receipts };
            } finally {
              if (attemptToken !== undefined) outbox!.releaseAttempt(envelope.id, attemptToken);
            }
          };
          if (!keyedLookup) return deliver();
          const key = `${envelope.networkId}\0${envelope.idempotencyKey}`;
          const previous = keyLocks.get(key) ?? Promise.resolve();
          const operation = previous.catch(() => {}).then(deliver);
          keyLocks.set(key, operation);
          try {
            return await operation;
          } finally {
            if (keyLocks.get(key) === operation) keyLocks.delete(key);
          }
        },
        async redriveOutboxItem(
          item: OutboxItem,
          options: OutboxRedriveDeliveryOptions = {},
        ): Promise<OutboxDeliveryOutcome> {
          const { transport, router } = ctx.services;
          const mounted = transport.listMountedNetworks().includes(item.networkId);
          // Every attachment in the signed envelope must have durable local evidence; a
          // pre-shared ticket alone is unresolvable after restart and must not be redriven.
          for (const attachment of item.envelope.attachments) {
            if (!item.stagedFiles[attachment.blake3Hash]) {
              return {
                failures: [{
                  error: `Missing staged attachment ${attachment.blake3Hash}`,
                }],
              };
            }
          }
          // Staged evidence must still exist before any byte is re-sent; a missing copy is
          // a deterministic local failure, not a reason to retry the same broken payload.
          for (const [hash, filePath] of Object.entries(item.stagedFiles)) {
            try {
              transport.registerBlob?.(hash, filePath);
            } catch (error) {
              return {
                failures: [{
                  error: `Missing staged attachment ${hash}: ${
                    error instanceof Error ? error.message : String(error)
                  }`,
                }],
              };
            }
          }
          let targets = options.targetNodeIds !== undefined
            ? [...options.targetNodeIds]
            : item.pendingNodeIds.filter(
                nodeId =>
                  !item.deliveredReceipts[nodeId] &&
                  !item.failedNodeIds.includes(nodeId),
              );
          const untouched = item.pendingNodeIds.filter(
            nodeId =>
              !targets.includes(nodeId) &&
              !item.deliveredReceipts[nodeId] &&
              !item.failedNodeIds.includes(nodeId),
          );
          // An unmounted network is handled per target by attemptTarget: external targets
          // stay queued with their in-doubt flags intact, while same-node loopback can still
          // deliver through the transport without a mount.
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
                failures: [{
                  error: error instanceof Error ? error.message : String(error),
                }],
              };
            }
            if (route.mode === 'broadcast' || route.targetNodeIds.length === 0) {
              return { failures: [{ error: 'Outbox item has no addressable target' }] };
            }
            resolvedTargetNodeIds = route.targetNodeIds.filter(
              nodeId =>
                !item.deliveredReceipts[nodeId] &&
                !item.failedNodeIds.includes(nodeId),
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
          const outcomeInDoubtNodeIds: string[] = [];
          const relaySpooledNodeIds: string[] = [];
          const approvalPendingNodeIds: string[] = [];
          const failures: OutboxDeliveryFailure[] = [];
          let lastError: string | undefined;
          // All targets share a deadline but start independently; a dead peer cannot
          // consume a live sibling's send budget. Fold evidence in target order.
          if (options.attemptToken !== undefined) prepareSend(item.id, options.attemptToken, targets);
          const deadline = Date.now() + (options.timeoutMs ?? 10_000);
          const attempts = await Promise.all(targets.map(target => {
            if (Date.now() >= deadline) return undefined;
            return attemptTarget(item.envelope, target, frame, {
              deadline,
              mounted,
              queueIfOffline: true,
              spoolOptions: {
                spoolIfOffline: true,
                spoolKey: item.envelope.id,
                spoolBlobHashes,
              },
            });
          }));
          for (const [index, target] of targets.entries()) {
            const attempt = attempts[index];
            if (attempt === undefined) {
              // Never attempted, so preserve any prior in-doubt evidence below.
              pendingNodeIds.push(target);
              continue;
            }
            if (attempt.status === 'delivered') {
              deliveredReceipts[target] = attempt.receipt;
              continue;
            }
            if (attempt.status === 'failed') {
              // A signed rejection/failure is terminal for this target only; other
              // multicast targets must still be attempted and persisted independently.
              failures.push({
                nodeId: target,
                receipt: attempt.receipt,
                error: attempt.error,
              });
              continue;
            }
            if (attempt.status === 'local-failure') {
              failures.push({
                nodeId: target,
                error: `Local or protocol failure: ${attempt.error.message}`,
              });
              if (!attempt.beforeSend) outcomeInDoubtNodeIds.push(target);
              continue;
            }
            pendingNodeIds.push(target);
            if (attempt.approvalPending) approvalPendingNodeIds.push(target);
            if (attempt.relaySpooled) {
              relaySpooledNodeIds.push(target);
            }
            if (attempt.inDoubt) {
              outcomeInDoubtNodeIds.push(target);
            }
            lastError = attempt.error;
          }
          const pending = [...new Set([...untouched, ...pendingNodeIds])];
          const inDoubtNodeIds = [...new Set([...item.inDoubtNodeIds, ...outcomeInDoubtNodeIds])]
            .filter(nodeId => !isConfirmedHandoffReceipt(deliveredReceipts[nodeId], { allowQueued: false }));
          return {
            ...(resolvedTargetNodeIds !== undefined ? { resolvedTargetNodeIds } : {}),
            deliveredReceipts,
            pendingNodeIds: pending,
            inDoubtNodeIds,
            clearFenceNodeIds: clearedFences(targets, attempts),
            relaySpooledNodeIds: outstandingRelaySpooledTargets(
              [...item.relaySpooledNodeIds, ...relaySpooledNodeIds], { ...item.deliveredReceipts, ...deliveredReceipts },
            ),
            ...(approvalPendingNodeIds.length > 0 ? { approvalPendingNodeIds } : {}),
            ...(failures.length > 0 ? { failures } : {}),
            ...(lastError !== undefined ? { lastError } : {}),
          };
        },
        holdSources() {
          activeHolds++;
          let released = false;
          return { release(options) {
            if (released) return;
            released = true;
            if (--activeHolds !== 0) return;
            const pending = [...held];
            held.clear();
            for (const [activate, cancel] of pending) {
              if (options?.start === false) cancel();
              else activate();
            }
          } };
        },
        registerSource(source) {
          if (sources.has(source)) throw new Error('Source already registered');
          let disposed = false;
          let started: Promise<void> | undefined;
          let resolveRegistrationStarted!: () => void;
          let rejectRegistrationStarted!: (error: unknown) => void;
          const registrationStarted = new Promise<void>((resolve, reject) => {
            resolveRegistrationStarted = resolve;
            rejectRegistrationStarted = reject;
          });
          void registrationStarted.catch(() => {});
          const cancel = () => {
            if (disposed || started) return;
            disposed = true;
            held.delete(activate);
            sources.delete(source);
            rejectRegistrationStarted(new Error('Source cancelled before start'));
          };
          const activate = () => {
            held.delete(activate);
            if (disposed || started) return;
            try { started = Promise.resolve(source.start(service)); }
            catch (error) { started = Promise.reject(error); }
            started.then(resolveRegistrationStarted, rejectRegistrationStarted);
            void started.catch(error => settings.onSourceError?.(error));
          };
          held.set(activate, cancel);
          sources.set(source, registrationStarted);
          if (activeHolds === 0) activate();
          const dispose = async () => {
            if (disposed) return;
            if (!started) {
              cancel();
              return;
            }
            disposed = true;
            try {
              await source.stop();
              await started;
            } finally { sources.delete(source); }
          };
          ctx.effect(() => dispose);
          return { dispose, started: registrationStarted };
        },
      };

      const selfCardFor = (item: OutboxItem): NodeCard => {
        const self = ctx.services.node.getSelfCard(item.networkId);
        if (self.nodeId !== item.envelope.from.nodeId) {
          throw new Error('Outbox sender identity changed');
        }
        return self;
      };

      /** Retry only original unsettled targets, retaining the envelope and confirmed siblings. */
      const reconcileObligation = async (observed: OutboxItem): Promise<EmitResult> => {
        if (outboxReconciliationTargets(observed).length === 0) {
          return replayObligation(observed);
        }
        const retryOriginal = isUnattemptedOutboxItem(observed);
        const claim = retryOriginal
          ? outbox!.replaceUnattemptedIdempotency({
              envelope: observed.envelope, explicitTargets: observed.explicitTargets, expectedId: observed.id,
            })
          : outbox!.beginAttempt(observed.id, { reconcile: true });
        if (!claim) {
          const latest = outbox!.findKeyedObligation?.(
            observed.networkId, observed.envelope.idempotencyKey!, envelopeClaimVersion(observed.envelope) ?? 0,
          );
          return replayObligation(latest ?? observed);
        }
        if ('claimed' in claim && !claim.claimed) return replayObligation(claim.item);
        const { item: existing, attemptToken } = claim;
        try {
          const targets = retryOriginal ? existing.pendingNodeIds : outboxReconciliationTargets(existing);
          if (targets.length === 0) return replayObligation(existing);
          const { transport } = ctx.services;
          for (const attachment of existing.envelope.attachments) {
            if (!existing.stagedFiles[attachment.blake3Hash]) {
              throw new LocalDeliveryError(`Missing staged attachment ${attachment.blake3Hash}`);
            }
          }
          for (const [hash, path] of Object.entries(existing.stagedFiles)) transport.registerBlob?.(hash, path);
          const frame = Buffer.from(JSON.stringify({ type: 'message.envelope', envelope: existing.envelope }));
          prepareSend(existing.id, attemptToken, targets);
          const deadline = Date.now() + (settings.attemptTimeoutMs ?? 10_000);
          const attempts = await Promise.all(targets.map(target => attemptTarget(existing.envelope, target, frame, {
            deadline,
            mounted: transport.listMountedNetworks().includes(existing.networkId),
            queueIfOffline: true,
            spoolOptions: { spoolIfOffline: true, spoolKey: existing.id,
              spoolBlobHashes: Object.keys(existing.stagedFiles) },
          })));
          const deliveredReceipts: Record<string, DeliveryReceipt> = {};
          const livePendingReceipts: Record<string, DeliveryReceipt> = {};
          const failedReceipts: Record<string, DeliveryReceipt> = {};
          const failedNodeIds: string[] = [];
          const pendingNodeIds: string[] = [];
          const inDoubtNodeIds: string[] = [];
          const relaySpooledNodeIds: string[] = [];
          const approvalPendingNodeIds: string[] = [];
          let fatalError: Error | undefined;
          let lastError: string | undefined;
          for (const [index, target] of targets.entries()) {
            const attempt = attempts[index];
            if (attempt.status === 'delivered') deliveredReceipts[target] = attempt.receipt;
            else if (attempt.status === 'failed') {
              failedReceipts[target] = attempt.receipt;
              failedNodeIds.push(target);
              lastError = attempt.error;
            } else if (attempt.status === 'local-failure') {
              failedNodeIds.push(target);
              if (!attempt.beforeSend) inDoubtNodeIds.push(target);
              fatalError ??= attempt.error;
              lastError = attempt.error.message;
            } else {
              if (attempt.receipt) livePendingReceipts[target] = attempt.receipt;
              if (attempt.inDoubt) inDoubtNodeIds.push(target);
              if (attempt.relaySpooled) relaySpooledNodeIds.push(target);
              if (!isConfirmedHandoffReceipt(existing.deliveredReceipts[target], { allowQueued: true })) {
                pendingNodeIds.push(target);
                if (attempt.approvalPending) approvalPendingNodeIds.push(target);
              }
              lastError = attempt.error;
            }
          }
          const updated = outbox!.enqueue({ attemptToken, envelope: existing.envelope, explicitTargets: existing.explicitTargets,
            reconcileNodeIds: targets, deliveredReceipts, failedReceipts, failedNodeIds, pendingNodeIds,
            clearFenceNodeIds: clearedFences(targets, attempts),
            ...(approvalPendingNodeIds.length > 0 ? { approvalPendingNodeIds } : {}),
            inDoubtNodeIds, relaySpooledNodeIds, ...(lastError !== undefined ? { lastError: fatalError?.message ?? lastError } : {}) });
          // Return the live rejection for this attempt while retaining prior custody in the store.
          const receipts = obligationReceipts(updated)
            .filter(receipt => !failedReceipts[receipt.fromNodeId]);
          receipts.push(...Object.values(failedReceipts), ...Object.values(livePendingReceipts));
          const newlyCommitted = targets.some(target => deliveredReceipts[target] !== undefined ||
            livePendingReceipts[target] !== undefined || pendingNodeIds.includes(target));
          const fullCommittedTargets = [...new Set([...committedOutboxTargets(updated), ...Object.keys(livePendingReceipts)])];
          if ((newlyCommitted || existing.sentCommitted === false) &&
              (fullCommittedTargets.length > 0 || updated.status === 'queued')) {
            ctx.emit('emitter/sent', {
              envelope: updated.envelope, targetNodeIds: fullCommittedTargets, receipts, createdAt: updated.createdAt,
            });
          }
          if (fatalError) throw Object.assign(fatalError, { receipts });
          return { messageId: updated.id, traceId: updated.envelope.traceId, receipts };
        } finally {
          outbox!.releaseAttempt(existing.id, attemptToken);
        }
      };

      const obligationReceipts = (existing: OutboxItem): DeliveryReceipt[] => {
        const receipts: DeliveryReceipt[] = [
          ...Object.values(existing.deliveredReceipts),
          ...Object.values(existing.failedReceipts),
        ];
        if (existing.status !== 'queued') return receipts;
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
        return receipts;
      };

      /** Reuse the durable claim and recover any commitment interrupted before the send event. */
      const replayObligation = (existing: OutboxItem): EmitResult => {
        const receipts = obligationReceipts(existing);
        if (existing.sentCommitted === false) {
          const targetNodeIds = committedOutboxTargets(existing);
          if ((existing.status === 'queued' || targetNodeIds.length > 0) && outbox?.markSentCommitted?.(existing.id) !== false) {
            ctx.emit('emitter/sent', { envelope: existing.envelope, targetNodeIds, receipts, createdAt: existing.createdAt });
          }
        }
        if (existing.status === 'failed' &&
            !(existing.failedNodeIds.length > 0 &&
              existing.failedNodeIds.every(id => Boolean(existing.failedReceipts[id])))) {
          throw Object.assign(new LocalDeliveryError(existing.lastError ?? 'Durable outbox obligation failed'), { receipts });
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
