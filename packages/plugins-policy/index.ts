import type { DispatchPolicyPlugin, DispatchReceipt, ExecutorPlugin, MessageEnvelope } from '../core-types/index.ts';

export interface DedupCasOptions { entityKey?: (envelope: MessageEnvelope) => string }
export function createDedupCasPolicyPlugin(options: DedupCasOptions = {}): DispatchPolicyPlugin {
  const watermarks = new Map<string, number>();
  const locks = new Map<string, Promise<unknown>>();
  return { name: 'dedup-cas', async process(envelope, next) {
    const key = JSON.stringify([envelope.networkId, envelope.from.nodeId, options.entityKey?.(envelope) ?? envelope.idempotencyKey ?? envelope.id]);
    const raw = envelope.payload?.claimVersion ?? envelope.payload?.revision ?? 0;
    if (typeof raw !== 'number' || !Number.isSafeInteger(raw) || raw < 0) return { stage: 'rejected', detail: 'Invalid claim version' };
    const previous = locks.get(key) ?? Promise.resolve();
    const operation = previous.catch(() => {}).then(async (): Promise<DispatchReceipt> => {
      if ((watermarks.get(key) ?? -1) >= raw) return { stage: 'rejected', detail: 'Duplicate or stale claim version' };
      // Reserve before the external effect: a thrown call can have an unknown outcome.
      // Retrying the same claim would risk duplicate execution; a newer claim can recover it.
      watermarks.set(key, raw);
      return next([envelope]);
    });
    locks.set(key, operation);
    try { return await operation; } finally { if (locks.get(key) === operation) locks.delete(key); }
  } };
}
export interface WaveCoalescerOptions<C> {
  executor: ExecutorPlugin<C>; executorConfig: C;
  streamKey?: (envelope: MessageEnvelope) => string;
  maxWaves?: number; minAgeMs?: number;
  rotateSession?: (command: '/new', streamKey: string) => Promise<void>;
  now?: () => number;
}
export interface WaveCoalescerPolicy extends DispatchPolicyPlugin {
  flushWave(streamKey: string): Promise<DispatchReceipt>;
  pendingCount(streamKey: string): number;
}
export function createWaveCoalescerPolicyPlugin<C>(options: WaveCoalescerOptions<C>): WaveCoalescerPolicy {
  const now = options.now ?? Date.now;
  type Wave = { envelopes: MessageEnvelope[]; next: (batch: readonly MessageEnvelope[]) => Promise<DispatchReceipt>; waves: number; startedAt: number; networkId: string };
  const streams = new Map<string, Wave>();
  const locks = new Map<string, Promise<unknown>>();
  const locked = async (key: string, operation: () => Promise<DispatchReceipt>) => {
    const result = (locks.get(key) ?? Promise.resolve()).catch(() => {}).then(operation);
    locks.set(key, result);
    try { return await result; } finally { if (locks.get(key) === result) locks.delete(key); }
  };
  const flush = async (key: string): Promise<DispatchReceipt> => {
    const wave = streams.get(key);
    if (!wave?.envelopes.length) return { stage: 'queued', detail: 'No pending wave' };
    if (options.executor.checkIdle && !(await options.executor.checkIdle(options.executorConfig)).idle) return { stage: 'queued', detail: 'Executor busy' };
    if (wave.waves >= (options.maxWaves ?? Infinity) || now() - wave.startedAt >= (options.minAgeMs ?? Infinity)) {
      if (!options.rotateSession) throw new Error('Session rotation required but no rotation hook configured');
      await options.rotateSession('/new', key);
      wave.waves = 0; wave.startedAt = now();
    }
    const batch = [...wave.envelopes];
    const result = await wave.next(batch);
    if (['dispatched', 'settled'].includes(result.stage)) { wave.envelopes.splice(0, batch.length); wave.waves++; }
    return result;
  };
  return {
    name: 'wave-coalescer',
    pendingCount: key => streams.get(key)?.envelopes.length ?? 0,
    flushWave: key => locked(key, () => flush(key)),
    process(envelope, next) {
      const key = options.streamKey?.(envelope) ?? JSON.stringify([envelope.networkId, envelope.payload?.streamKey ?? envelope.kind, envelope.payload?.executor ?? options.executor.name]);
      return locked(key, async () => {
        let wave = streams.get(key);
        if (!wave) { wave = { envelopes: [], next, waves: 0, startedAt: now(), networkId: envelope.networkId }; streams.set(key, wave); }
        if (wave.networkId !== envelope.networkId) throw new Error('Wave stream cannot cross networks');
        if (!wave.envelopes.some(e => e.id === envelope.id && e.from.nodeId === envelope.from.nodeId)) wave.envelopes.push(structuredClone(envelope));
        wave.next = next;
        return flush(key);
      });
    },
  };
}
