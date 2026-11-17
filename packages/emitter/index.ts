import { randomUUID } from 'node:crypto';
import {
  isConfirmedHandoffReceipt,
  isDeliveryTimeoutFailure,
  isNetworkUnmountedError,
  isOfflinePeerSpooledError,
  isPeerOfflineError,
  isTerminalDeliveryFailure,
  isUnresolvedTargetError,
  LocalDeliveryError,
  NetworkUnmountedError,
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

      type TargetAttempt =
        | { status: 'delivered'; receipt: DeliveryReceipt }
        | { status: 'failed'; receipt: DeliveryReceipt; error: string }
        | {
            status: 'pending';
            error: string;
            inDoubt: boolean;
            relaySpooled: boolean;
            /** False when no byte was sent (deadline/unmount), preserving prior doubt flags. */
            attempted: boolean;
            /** Nonterminal live receipt (e.g. pending_approval) returned to the caller. */
            receipt?: DeliveryReceipt;
          }
        | { status: 'local-failure'; error: Error };

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
        const localFailure = (error: unknown): TargetAttempt => ({
          status: 'local-failure',
          error: error instanceof Error ? error : new Error(String(error)),
        });
        if (!options.mounted && target !== envelope.from.nodeId) {
          const error = new NetworkUnmountedError(envelope.networkId);
          return options.queueIfOffline
            ? {
                status: 'pending',
                error: error.message,
                inDoubt: false,
                relaySpooled: false,
                attempted: false,
              }
            : localFailure(error);
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
                attempted: false,
              }
            : localFailure(error);
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
          // Reached the target but not durably handed off (memory-only queue or approval
          // wait): return the signed receipt. Only queue-enabled callers also keep the
          // local outbox obligation; queueIfOffline:false must still report the stage.
          const detail = `Target ${target} returned ${receipt.stage}`;
          return {
            status: 'pending',
            error: detail,
            inDoubt: false,
            relaySpooled: false,
            attempted: true,
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
                  attempted: false,
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
              attempted: true,
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
              attempted: true,
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
          // Every attachment the outbox may redeliver must have durable local evidence.
          // A pre-shared ticket alone is not restart-safe, so queueing requires a path.
          const attachmentPaths = input.attachmentPaths ?? {};
          const preSharedFiles = await Promise.all(
            (input.attachments ?? []).map(async attachment => {
              const filePath = attachmentPaths[attachment.blake3Hash];
              if (filePath === undefined) {
                if (queueIfOffline) {
                  throw new LocalDeliveryError(
                    `Attachment ${attachment.blake3Hash} requires attachmentPaths[hash] for durable outbox staging`,
                  );
                }
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
          if (queueIfOffline) {
            for (const attachment of envelope.attachments) {
              if (!stagedPaths[attachment.blake3Hash]) {
                throw new LocalDeliveryError(
                  `Attachment ${attachment.blake3Hash} has no durable source path for offline queueing`,
                );
              }
            }
          }
          let route: RouteDecision;
          try {
            route = await router.route(envelope);
          } catch (error) {
            // An unresolved explicit selector is a peering gap, not a routing bug: the
            // durable outbox retries it once the peer directory learns the member again.
            // Network mount state is irrelevant here; identity resolution is directory-owned.
            if (isUnresolvedTargetError(error) && queueIfOffline) {
              const claimed = envelope.idempotencyKey
                ? outbox.claimIdempotency({
                    envelope,
                    explicitTargets: [...(input.routing.targets ?? [])],
                    filePaths: stagedPaths,
                  })
                : undefined;
              if (claimed && !claimed.claimed) {
                return replayQueued(claimed.item);
              }
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
          // Atomic claim after routing but before the first byte: concurrent emitters with
          // the same key converge on one envelope/obligation, and the store lock is the
          // only place that decides which caller owns the initial send.
          let claimedItem: OutboxItem | undefined;
          if (queueIfOffline && envelope.idempotencyKey && route.mode !== 'broadcast') {
            const claim = outbox.claimIdempotency({
              envelope,
              explicitTargets: [...(input.routing.targets ?? [])],
              filePaths: stagedPaths,
            });
            if (!claim.claimed) {
              return replayQueued(claim.item);
            }
            claimedItem = claim.item;
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
          // Reached but not durably handed off (for example pending_approval). The target
          // did receive the envelope, so the send is observable even though it stays out of
          // the durable pending set when queueing is disabled.
          const livePendingReceipts: Record<string, DeliveryReceipt> = {};
          const failedReceipts: Record<string, DeliveryReceipt> = {};
          const failedNodeIds: string[] = [];
          const observedReceipts: DeliveryReceipt[] = [];
          const pendingNodeIds: string[] = [];
          const inDoubtNodeIds: string[] = [];
          const relaySpooledNodeIds: string[] = [];
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
                  continue;
                }
                pendingNodeIds.push(target);
                if (attempt.inDoubt) {
                  inDoubtNodeIds.push(target);
                }
                if (attempt.relaySpooled) {
                  relaySpooledNodeIds.push(target);
                }
                offlineDetail = attempt.error;
                continue;
              }
              if (!queueIfOffline) {
                throw attempt.error;
              }
              // Deterministic local/protocol failure: terminal for this target, but sibling
              // offline obligations must be durably enqueued before the error surfaces.
              failedNodeIds.push(target);
              offlineDetail ??= attempt.error.message;
              fatalError ??= attempt.error;
            }
          }
          if (claimedItem && route.mode !== 'broadcast' && route.targetNodeIds.length === 0) {
            outbox.markFailed(claimedItem.id, 'Outbox item has no addressable target');
            throw new LocalDeliveryError('Outbox item has no addressable target');
          }
          const receipts: DeliveryReceipt[] = [...observedReceipts];
          if (queueIfOffline && (pendingNodeIds.length > 0 || claimedItem !== undefined)) {
            outbox.enqueue({
              envelope,
              explicitTargets: [...(input.routing.targets ?? [])],
              pendingNodeIds,
              deliveredReceipts,
              failedReceipts,
              failedNodeIds,
              inDoubtNodeIds,
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
          // `emitter/sent` is the single commitment signal for observers such as the thread
          // ledger: only a broadcast that left the transport, a live handoff receipt, or a
          // durable outbox obligation may create reverse-route state. Unqueued failures and
          // terminal rejections leave no orphan outbound record behind.
          const committedTargetNodeIds = route.targetNodeIds.filter(
            target =>
              deliveredReceipts[target] !== undefined ||
              livePendingReceipts[target] !== undefined ||
              (queueIfOffline && pendingNodeIds.includes(target)),
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
        },
        async redriveOutboxItem(
          item: OutboxItem,
          options: { targetNodeIds?: readonly string[]; timeoutMs?: number } = {},
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
          const failures: OutboxDeliveryFailure[] = [];
          const attemptedTargets = new Set<string>();
          let lastError: string | undefined;
          // All targets share a deadline but start independently; a dead peer cannot
          // consume a live sibling's send budget. Fold evidence in target order.
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
              attemptedTargets.add(target);
              deliveredReceipts[target] = attempt.receipt;
              continue;
            }
            if (attempt.status === 'failed') {
              // A signed rejection/failure is terminal for this target only; other
              // multicast targets must still be attempted and persisted independently.
              attemptedTargets.add(target);
              failures.push({
                nodeId: target,
                receipt: attempt.receipt,
                error: attempt.error,
              });
              continue;
            }
            if (attempt.status === 'local-failure') {
              attemptedTargets.add(target);
              failures.push({
                nodeId: target,
                error: `Local or protocol failure: ${attempt.error.message}`,
              });
              continue;
            }
            if (attempt.attempted) {
              attemptedTargets.add(target);
            }
            pendingNodeIds.push(target);
            if (attempt.relaySpooled) {
              relaySpooledNodeIds.push(target);
            }
            if (attempt.inDoubt) {
              outcomeInDoubtNodeIds.push(target);
            }
            lastError = attempt.error;
          }
          const pending = [...new Set([...untouched, ...pendingNodeIds])];
          // Requested-but-never-sent targets keep their prior in-doubt flags; only an
          // actual attempt may clear or replace them.
          const inDoubtNodeIds = [
            ...new Set([
              ...outcomeInDoubtNodeIds,
              ...item.inDoubtNodeIds.filter(
                nodeId => pending.includes(nodeId) && !attemptedTargets.has(nodeId),
              ),
            ]),
          ];
          return {
            ...(resolvedTargetNodeIds !== undefined ? { resolvedTargetNodeIds } : {}),
            deliveredReceipts,
            pendingNodeIds: pending,
            inDoubtNodeIds,
            relaySpooledNodeIds,
            ...(failures.length > 0 ? { failures } : {}),
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
        const receipts: DeliveryReceipt[] = [
          ...Object.values(existing.deliveredReceipts),
          ...Object.values(existing.failedReceipts),
        ];
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
