import { existsSync, readFileSync } from 'node:fs';
import { PartialBatchExecutionError, signReceipt, verifyEnvelope, verifyReceipt } from '../core-types/index.ts';
import { writeFileAtomically } from '../fs-atomic/index.ts';
import type {
  DispatchPolicyPlugin,
  DispatchErrorEvent,
  DispatchReceipt,
  ExecutionContext,
  ExecutorPlugin,
  MessageEnvelope,
} from '../core-types/index.ts';
import type { Plugin } from '../kernel/index.ts';

export interface DispatcherConfig {
  requiredPolicies?: () => readonly string[];
  /** Composition-root decoration, uniformly applied at the registration boundary. */
  wrapExecutor?: (executor: ExecutorPlugin<any>) => ExecutorPlugin<any>;
  withExecutionScope?: (
    batch: readonly MessageEnvelope[], context: ExecutionContext,
    execute: (scopedContext: ExecutionContext) => Promise<DispatchReceipt>,
  ) => Promise<DispatchReceipt>;
  privateKeyPem?: string;
  ledgerPath?: string;
  defaultExecutor?: string;
  resolveExecutor?: (envelope: MessageEnvelope) => string | undefined;
  resolveKindExecutor?: (kind: string, streamKey?: string) => string | undefined;
  kindExecutors?: Record<string, string>;
  onError?: (error: unknown, context: Omit<DispatchErrorEvent, 'error'>) => void;
}

/**
 * Atomic snapshots, with a dispatching marker persisted before any external effect.
 * An interrupted dispatch is intentionally not retried automatically: its external outcome is unknown.
 */
export class DeliveryLedger {
  #path?: string;
  #entries: Record<string, DispatchReceipt>;

  constructor(path?: string) {
    this.#path = path;
    const saved = path && existsSync(path) ? JSON.parse(readFileSync(path, 'utf8')) : {};
    if (
      !saved ||
      typeof saved !== 'object' ||
      Array.isArray(saved) ||
      Object.values(saved).some(
        value =>
          !value ||
          typeof value !== 'object' ||
          ![
            'received',
            'pending_approval',
            'queued',
            'dispatching',
            'dispatched',
            'settled',
            'rejected',
            'failed',
          ].includes((value as DispatchReceipt).stage),
      )
    ) {
      throw new Error('Invalid delivery ledger');
    }
    this.#entries = saved;
  }

  get(key: string): DispatchReceipt | undefined {
    return this.#entries[key] && structuredClone(this.#entries[key]);
  }

  set(key: string, value: DispatchReceipt): void {
    this.update([[key, value]]);
  }

  delete(key: string): void {
    this.update([[key, undefined]]);
  }

  update(changes: readonly (readonly [string, DispatchReceipt | undefined])[]): void {
    const next = { ...this.#entries };
    for (const [key, receipt] of changes) {
      if (receipt) next[key] = receipt;
      else delete next[key];
    }
    if (this.#path) {
      writeFileAtomically(this.#path, JSON.stringify(next));
    }
    this.#entries = next;
  }
}

export function createDispatcherPlugin(config: DispatcherConfig = {}): Plugin {
  return {
    name: 'dispatcher',
    apply(ctx) {
      const policies = new Set<DispatchPolicyPlugin>();
      const executors = new Map<
        string,
        { execute: (batch: readonly MessageEnvelope[], context: ExecutionContext) => Promise<DispatchReceipt> }
      >();
      const ledger = new DeliveryLedger(config.ledgerPath);
      const active = new Map<string, Promise<DispatchReceipt>>();
      const idleListeners = new Set<() => void>();
      const controller = new AbortController();
      let lastError: DispatchErrorEvent | undefined;
      ctx.effect(() => async () => {
        controller.abort();
        await Promise.allSettled(active.values());
      });
      const key = (e: MessageEnvelope) => JSON.stringify([e.networkId, e.from.nodeId, e.id]);
      const select = (e: MessageEnvelope) => {
        const resolved = config.resolveExecutor?.(e);
        if (resolved !== undefined) return resolved;
        // Session control always lands in a mailbox; it must never spawn new worktrees.
        const semanticType = e.semantic?.type;
        if (
          semanticType === 'question' ||
          semanticType === 'reply' ||
          semanticType === 'settle'
        ) {
          return 'inbox';
        }
        const explicit = e.payload?.executor;
        if (typeof explicit === 'string' && explicit.trim().length > 0) {
          return explicit;
        }
        return (
          config.resolveKindExecutor?.(e.kind, typeof e.payload?.streamKey === 'string' ? e.payload.streamKey : undefined) ??
          config.kindExecutors?.[e.kind] ??
          config.defaultExecutor ??
          executors.keys().next().value
        );
      };
      const assertRequiredPolicies = () => {
        for (const name of config.requiredPolicies?.() ?? []) {
          if (![...policies].some(policy => policy.name === name)) {
            throw Object.assign(new Error(`Policy unavailable: ${name}`), { beforeExternalEffect: true });
          }
        }
      };
      const executeBatch = async (
        batch: readonly MessageEnvelope[], callerKey: string, context: ExecutionContext,
      ): Promise<DispatchReceipt> => {
        if (context.signal?.aborted) {
          throw Object.assign(new Error('Dispatcher stopped'), { beforeExternalEffect: true });
        }
        const name = select(batch[0]);
        if (batch.slice(1).some(e => e.networkId !== context.networkId || select(e) !== name)) {
          throw Object.assign(new Error('Batch crosses executor or network boundary'), { beforeExternalEffect: true });
        }
        const executor = name && executors.get(name);
        if (!executor) {
          throw Object.assign(new Error(`Executor unavailable: ${name}`), { beforeExternalEffect: true });
        }
        const members = batch.map(envelope => ({ envelope, id: key(envelope), prior: ledger.get(key(envelope)) }));
        for (const { envelope, prior } of members) {
          if (prior && prior.stage !== 'queued') {
            throw new Error(`Execution already attempted: ${envelope.id} (${prior.stage})`);
          }
        }
        assertRequiredPolicies();
        ledger.update(members.map(({ id }) => [id, { stage: 'dispatching' }]));
        let result: DispatchReceipt;
        try {
          result = await executor.execute(batch, context);
        } catch (error) {
          const partial = error instanceof PartialBatchExecutionError ? error : undefined;
          const beforeEffect = (error as { beforeExternalEffect?: boolean } | null)?.beforeExternalEffect === true;
          const outcomes = members.map(({ envelope, id }) => {
            const settled = partial?.settledReceipts;
            const unattempted = beforeEffect || partial?.unattemptedIds.has(envelope.id) === true;
            const receipt: DispatchReceipt | undefined = settled && Object.hasOwn(settled, envelope.id)
              ? { stage: 'dispatched', executionReceipt: settled[envelope.id] }
              : unattempted ? undefined : { stage: 'failed', detail: String(error) };
            return { envelope, id, receipt, unattempted };
          });
          // Policies handed these members to the executor. Release unattempted claims for retry.
          if (outcomes.length === 1 && outcomes[0].receipt === undefined) ledger.delete(outcomes[0].id);
          else ledger.update(outcomes.map(({ id, receipt }) => [id, receipt]));
          for (const { envelope, receipt } of outcomes) {
            if (receipt?.stage === 'dispatched') ctx.emit('dispatcher/inbound', { envelope, receipt });
          }
          const caller = outcomes.find(({ id }) => id === callerKey);
          if (caller?.receipt?.stage === 'dispatched') return caller.receipt;
          if (caller?.unattempted && !beforeEffect) {
            throw Object.assign(new Error(String(error), { cause: error }), { beforeExternalEffect: true });
          }
          throw error;
        }
        ledger.update(members.map(({ id }) => [id, result]));
        // A policy can dispatch messages queued by earlier calls. Publish each committed delivery.
        for (const envelope of batch) ctx.emit('dispatcher/inbound', { envelope, receipt: result });
        return result;
      };
      const core = async (batch: readonly MessageEnvelope[], callerKey: string): Promise<DispatchReceipt> => {
        const first = batch[0];
        if (!first) throw Object.assign(new Error('Empty execution batch'), { beforeExternalEffect: true });
        const context = {
          networkId: first.networkId, nodeId: ctx.services.node.getSelfCard(first.networkId).nodeId,
          traceId: first.traceId, signal: controller.signal,
        };
        return config.withExecutionScope
          ? config.withExecutionScope(batch, context, scopedContext => executeBatch(batch, callerKey, scopedContext))
          : executeBatch(batch, callerKey, context);
      };
      ctx.provide('dispatcher', {
        hasExecutor(name) { return executors.has(name); },
        hasPolicy(name) { return [...policies].some(policy => policy.name === name); },
        activeCount() { return active.size; },
        getLastError() { return lastError; },
        onIdle(listener: () => void) {
          idleListeners.add(listener);
          return { dispose() { idleListeners.delete(listener); } };
        },
        waitForIdle() {
          if (active.size === 0) return Promise.resolve();
          return new Promise<void>(resolve => {
            const listener = () => { if (active.size === 0) { idleListeners.delete(listener); resolve(); } };
            idleListeners.add(listener);
          });
        },
        registerPolicy(policy) {
          if ([...policies].some(entry => entry.name === policy.name)) throw new Error(`Duplicate policy: ${policy.name}`);
          policies.add(policy);
          return {
            dispose() {
              policies.delete(policy);
            },
          };
        },
        registerExecutor<C>(executor: ExecutorPlugin<C>, executorConfig: C) {
          if (executors.has(executor.name)) {
            throw new Error(`Duplicate executor: ${executor.name}`);
          }
          const wrapped = config.wrapExecutor?.(executor) ?? executor;
          const entry = {
            async execute(batch: readonly MessageEnvelope[], context: ExecutionContext): Promise<DispatchReceipt> {
              return {
                stage: 'dispatched',
                executionReceipt: await wrapped.execute(batch, context, executorConfig),
              };
            },
          };
          executors.set(executor.name, entry);
          return {
            dispose() {
              if (executors.get(executor.name) === entry) {
                executors.delete(executor.name);
              }
            },
          };
        },
        dispatch(envelope) {
          if (controller.signal.aborted) {
            return Promise.reject(Object.assign(new Error('Dispatcher stopped'), { beforeExternalEffect: true }));
          }
          const id = key(envelope);
          const ongoing = active.get(id);
          if (ongoing) {
            return ongoing;
          }
          const prior = ledger.get(id);
          if (prior) {
            return Promise.resolve(prior);
          }
          const promise = Promise.resolve()
            .then(async () => {
              if (controller.signal.aborted) {
                throw Object.assign(new Error('Dispatcher stopped'), { beforeExternalEffect: true });
              }
              const settled = ledger.get(id);
              if (settled) return settled;
              assertRequiredPolicies();
              const chain = [...policies];
              const run = async (
                index: number,
                batch: readonly MessageEnvelope[],
                callerKey: string,
              ): Promise<DispatchReceipt> => {
                if (index === chain.length) {
                  return core(batch, callerKey);
                }
                // Policies consume individual envelopes; batching policies should be registered last.
                let result: DispatchReceipt = { stage: 'queued' };
                for (const e of batch) {
                  let called = false;
                  result = await chain[index].process(e, nextBatch => {
                    if (called) {
                      throw new Error('Policy next may only be called once');
                    }
                    called = true;
                    return run(index + 1, nextBatch, key(e));
                  });
                }
                return result;
              };
              return run(0, [structuredClone(envelope)], id);
            })
            .then(result => {
              // A queued policy owns its pending buffer; do not overwrite a later wave's completed state.
              const recorded = ledger.get(id);
              if (!recorded) {
                ledger.set(id, result);
              }
              // Record the authenticated inbound route only after the gate allowed this delivery.
              // Pending approval/DND knocks never reach here; gate.approve()/drainDnd() do.
              if (recorded?.stage !== 'dispatched' && result.stage !== 'rejected' && result.stage !== 'failed') {
                ctx.emit('dispatcher/inbound', { envelope, receipt: result });
              }
              return recorded?.stage === 'dispatched' ? recorded : result;
            }, error => {
              const recorded = ledger.get(id);
              if (recorded?.stage === 'dispatched' || recorded?.stage === 'settled') {
                const context = {
                  stage: 'post_delivery_policy' as const, networkId: envelope.networkId,
                  fromNodeId: envelope.from.nodeId, messageId: envelope.id, receipt: structuredClone(recorded),
                };
                const event = { ...context, error };
                lastError = event;
                for (const report of [() => ctx.emit('dispatcher/error', event), () => config.onError?.(error, context)]) {
                  try { report(); }
                  catch (reportError) {
                    lastError = { ...event, error: new AggregateError([lastError.error, reportError], 'Dispatcher diagnostic observer failed') };
                  }
                }
                return recorded;
              }
              throw error;
            })
            .finally(() => {
              active.delete(id);
              if (active.size === 0) for (const listener of idleListeners) listener();
            });
          active.set(id, promise);
          return promise;
        },
      });
      ctx.on('transport/inbound', async event => {
        const frame = event.payload as { type?: string; envelope?: MessageEnvelope } | null;
        if (frame?.type !== 'message.envelope' || !frame.envelope) {
          return;
        }
        const envelope = frame.envelope;
        const self = ctx.services.node.getSelfCard(event.networkId);
        const signingKey =
          config.privateKeyPem ??
          (ctx.services.node as { privateKeyPem?: string }).privateKeyPem;
        if (!signingKey) {
          throw new Error('Dispatcher signing key is required for inbound receipts');
        }
        const identityProof = signReceipt(
          {
            messageId: envelope.id,
            networkId: event.networkId,
            fromNodeId: self.nodeId,
            stage: 'received',
            updatedAt: new Date().toISOString(),
          },
          signingKey,
        );
        if (!verifyReceipt(identityProof)) {
          throw new Error('Dispatcher key does not match node identity');
        }
        let receipt: DispatchReceipt;
        if (
          !verifyEnvelope(envelope) ||
          envelope.networkId !== event.networkId ||
          envelope.from.nodeId !== event.senderNodeId
        ) {
          receipt = {
            stage: 'rejected',
            detail: 'Invalid envelope signature or authenticated sender/network mismatch',
          };
        } else {
          const verdict = await ctx.services.gate.evaluate(envelope);
          if (verdict.status === 'allow') {
            try {
              receipt = await ctx.services.dispatcher.dispatch(envelope);
            } catch (error) {
              receipt = { stage: 'failed', detail: String(error) };
            }
          } else {
            receipt = {
              stage:
                verdict.status === 'pending_approval'
                  ? 'pending_approval'
                  : verdict.status === 'queued_dnd'
                    ? 'queued'
                    : 'rejected',
              detail: verdict.reason,
              ...(verdict.status === 'queued_dnd' ? {
                executionReceipt: {
                  executor: 'gate-dnd',
                  executionId: 'gate-dnd:' + envelope.id,
                  durable: Boolean(verdict.durable),
                },
              } : {}),
            };
          }
        }
        const signed = signReceipt(
          {
            messageId: envelope.id,
            networkId: event.networkId,
            fromNodeId: self.nodeId,
            ...receipt,
            updatedAt: new Date().toISOString(),
          },
          signingKey,
        );
        if (!verifyReceipt(signed)) {
          throw new Error('Dispatcher key does not match node identity');
        }
        return Buffer.from(JSON.stringify(signed));
      });
    },
  };
}
