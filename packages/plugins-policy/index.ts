import { randomUUID } from 'node:crypto';
import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from 'node:fs';
import { dirname } from 'node:path';
import type {
  DispatchPolicyPlugin,
  DispatchReceipt,
  ExecutorPlugin,
  MessageEnvelope,
} from '../core-types/index.ts';

export const DEDUP_CAS_DUPLICATE_DETAIL = 'Duplicate or stale claim version';

export function isDedupCasDuplicateReceipt(
  receipt: { stage?: string; detail?: string } | null | undefined,
): boolean {
  return (
    Boolean(receipt) &&
    typeof receipt === 'object' &&
    receipt.stage === 'rejected' &&
    receipt.detail === DEDUP_CAS_DUPLICATE_DETAIL
  );
}

export interface DedupCasOptions {
  entityKey?: (envelope: MessageEnvelope) => string;
  watermarksPath?: string;
}

function loadWatermarks(path: string): Record<string, number> {
  if (!existsSync(path)) {
    return {};
  }
  let saved: unknown;
  try {
    saved = JSON.parse(readFileSync(path, 'utf8'));
  } catch {
    throw new Error('Invalid dedup watermark store');
  }
  if (
    !saved ||
    typeof saved !== 'object' ||
    Array.isArray(saved) ||
    Object.values(saved).some(
      value => typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0,
    )
  ) {
    throw new Error('Invalid dedup watermark store');
  }
  return saved as Record<string, number>;
}

function persistWatermarks(path: string, watermarks: ReadonlyMap<string, number>): void {
  mkdirSync(dirname(path), { recursive: true });
  const temp = `${path}.${randomUUID()}.tmp`;
  const fd = openSync(temp, 'wx', 0o600);
  try {
    writeFileSync(fd, JSON.stringify(Object.fromEntries(watermarks)));
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

export function createDedupCasPolicyPlugin(options: DedupCasOptions = {}): DispatchPolicyPlugin {
  const watermarks = new Map<string, number>(
    options.watermarksPath ? Object.entries(loadWatermarks(options.watermarksPath)) : [],
  );
  const locks = new Map<string, Promise<unknown>>();
  return {
    name: 'dedup-cas',
    async process(envelope, next) {
      const key = JSON.stringify([
        envelope.networkId,
        envelope.from.nodeId,
        options.entityKey?.(envelope) ?? envelope.idempotencyKey ?? envelope.id,
      ]);
      const raw = envelope.payload?.claimVersion ?? envelope.payload?.revision ?? 0;
      if (typeof raw !== 'number' || !Number.isSafeInteger(raw) || raw < 0) {
        return { stage: 'rejected', detail: 'Invalid claim version' };
      }
      const previous = locks.get(key) ?? Promise.resolve();
      const operation = previous.catch(() => {}).then(async (): Promise<DispatchReceipt> => {
        if ((watermarks.get(key) ?? -1) >= raw) {
          return { stage: 'rejected', detail: DEDUP_CAS_DUPLICATE_DETAIL };
        }
        // Reserve before the external effect: a thrown call can have an unknown outcome.
        // Retrying the same claim would risk duplicate execution; a newer claim can recover it.
        watermarks.set(key, raw);
        if (options.watermarksPath) {
          // Persist the reservation before the effect so a restart cannot replay it.
          persistWatermarks(options.watermarksPath, watermarks);
        }
        return next([envelope]);
      });
      locks.set(key, operation);
      try {
        return await operation;
      } finally {
        if (locks.get(key) === operation) {
          locks.delete(key);
        }
      }
    },
  };
}

export interface WaveCoalescerOptions<C> {
  executor: ExecutorPlugin<C>;
  executorConfig: C;
  streamKey?: (envelope: MessageEnvelope) => string;
  maxWaves?: number;
  minAgeMs?: number;
  rotateSession?: (command: '/new', streamKey: string, streamConfig?: C) => Promise<void>;
  now?: () => number;
}

export interface WaveCoalescerPolicy extends DispatchPolicyPlugin {
  flushWave(streamKey: string): Promise<DispatchReceipt>;
  pendingCount(streamKey: string): number;
}

export function createWaveCoalescerPolicyPlugin<C>(
  options: WaveCoalescerOptions<C>,
): WaveCoalescerPolicy {
  const now = options.now ?? Date.now;
  type Wave = {
    envelopes: MessageEnvelope[];
    next: (batch: readonly MessageEnvelope[]) => Promise<DispatchReceipt>;
    waves: number;
    startedAt: number;
    networkId: string;
  };
  const streams = new Map<string, Wave>();
  const locks = new Map<string, Promise<unknown>>();
  const locked = async (key: string, operation: () => Promise<DispatchReceipt>) => {
    const result = (locks.get(key) ?? Promise.resolve()).catch(() => {}).then(operation);
    locks.set(key, result);
    try {
      return await result;
    } finally {
      if (locks.get(key) === result) {
        locks.delete(key);
      }
    }
  };
  const flush = async (key: string): Promise<DispatchReceipt> => {
    const wave = streams.get(key);
    if (!wave?.envelopes.length) {
      return { stage: 'queued', detail: 'No pending wave' };
    }
    const firstPayload = wave.envelopes[0]?.payload ?? {};
    // Each stream may target a different Orca workspace and terminal; resolve them
    // from the wave's own payload instead of one global executor configuration.
    const streamConfig = {
      ...((options.executorConfig ?? {}) as Record<string, unknown>),
      ...(typeof firstPayload.workspacePath === 'string' && firstPayload.workspacePath
        ? { cwd: firstPayload.workspacePath }
        : {}),
      ...(typeof firstPayload.cwd === 'string' && firstPayload.cwd
        ? { cwd: firstPayload.cwd }
        : {}),
      ...(typeof firstPayload.runId === 'string' && firstPayload.runId
        ? { runId: firstPayload.runId }
        : {}),
      ...(typeof firstPayload.terminalHandle === 'string' && firstPayload.terminalHandle
        ? { terminalHandle: firstPayload.terminalHandle }
        : {}),
    } as C;
    if (options.executor.checkIdle && !(await options.executor.checkIdle(streamConfig)).idle) {
      return { stage: 'queued', detail: 'Executor busy' };
    }
    const maxWaves =
      typeof firstPayload.rotateAfterWaves === 'number'
        ? firstPayload.rotateAfterWaves
        : (options.maxWaves ?? Infinity);
    const minAgeMs =
      typeof firstPayload.minAgeMs === 'number'
        ? firstPayload.minAgeMs
        : (options.minAgeMs ?? Infinity);
    if (wave.waves >= maxWaves || now() - wave.startedAt >= minAgeMs) {
      if (!options.rotateSession) {
        throw new Error('Session rotation required but no rotation hook configured');
      }
      await options.rotateSession('/new', key, streamConfig);
      wave.waves = 0;
      wave.startedAt = now();
    }
    const batch = [...wave.envelopes];
    const result = await wave.next(batch);
    if (['dispatched', 'settled'].includes(result.stage)) {
      wave.envelopes.splice(0, batch.length);
      wave.waves++;
    }
    return result;
  };
  return {
    name: 'wave-coalescer',
    pendingCount: key => streams.get(key)?.envelopes.length ?? 0,
    flushWave: key => locked(key, () => flush(key)),
    process(envelope, next) {
      const key =
        options.streamKey?.(envelope) ??
        JSON.stringify([
          envelope.networkId,
          envelope.payload?.streamKey ?? envelope.kind,
          envelope.payload?.executor ?? options.executor.name,
        ]);
      return locked(key, async () => {
        let wave = streams.get(key);
        if (!wave) {
          wave = {
            envelopes: [],
            next,
            waves: 0,
            startedAt: now(),
            networkId: envelope.networkId,
          };
          streams.set(key, wave);
        }
        if (wave.networkId !== envelope.networkId) {
          throw new Error('Wave stream cannot cross networks');
        }
        if (
          !wave.envelopes.some(
            e => e.id === envelope.id && e.from.nodeId === envelope.from.nodeId,
          )
        ) {
          wave.envelopes.push(structuredClone(envelope));
        }
        wave.next = next;
        return flush(key);
      });
    },
  };
}
