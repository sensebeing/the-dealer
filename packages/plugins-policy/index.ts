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
