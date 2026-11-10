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

export const DEDUP_CAS_AMBIGUOUS_DETAIL = 'Previous delivery outcome unknown; newer revision required';

export type DedupWatermarkStatus = 'unknown' | 'queued' | 'settled';
export interface DedupWatermarkEntry {
  revision: number;
  status: DedupWatermarkStatus;
}

type HandoffReceipt = {
  stage?: string;
  detail?: string;
  executionReceipt?: Record<string, unknown>;
};

export function isDurableQueuedReceipt(receipt: HandoffReceipt | null | undefined): boolean {
  return receipt?.stage === 'queued' && receipt.executionReceipt?.durable === true;
}

export function isDedupCasDuplicateReceipt(
  receipt: HandoffReceipt | null | undefined,
  options?: { allowQueued?: boolean },
): boolean {
  if (receipt?.stage !== 'rejected' || receipt.detail !== DEDUP_CAS_DUPLICATE_DETAIL) return false;
  if (receipt.executionReceipt?.durable !== true) return false;
  const handoff = receipt.executionReceipt.handoff;
  return handoff === 'settled' || (Boolean(options?.allowQueued) && handoff === 'queued');
}

export function isConfirmedHandoffReceipt(
  receipt: HandoffReceipt | null | undefined,
  options?: { allowQueued?: boolean },
): boolean {
  return receipt?.stage === 'dispatched' || receipt?.stage === 'settled' ||
    (Boolean(options?.allowQueued) && isDurableQueuedReceipt(receipt)) ||
    isDedupCasDuplicateReceipt(receipt, options);
}

export interface DedupCasOptions {
  entityKey?: (envelope: MessageEnvelope) => string;
  watermarksPath?: string;
  resolveLegacyWatermarkStatus?: (key: string, revision: number) => DedupWatermarkStatus;
}

function loadWatermarks(path: string, options: DedupCasOptions): Record<string, DedupWatermarkEntry> {
  if (!existsSync(path)) return {};
  try {
    const saved: unknown = JSON.parse(readFileSync(path, 'utf8'));
    if (!saved || typeof saved !== 'object' || Array.isArray(saved)) throw new Error();
    let upgraded = false;
    const normalized = Object.fromEntries(Object.entries(saved).map(([key, value]) => {
      const legacy = typeof value === 'number';
      if (legacy && (!Number.isSafeInteger(value) || value < 0)) throw new Error();
      upgraded ||= legacy;
      const entry = legacy
        ? { revision: value, status: options.resolveLegacyWatermarkStatus?.(key, value) ?? 'unknown' }
        : value;
      if (!entry || typeof entry !== 'object' || Array.isArray(entry) ||
          !Number.isSafeInteger(entry.revision) || entry.revision < 0 ||
          !['unknown', 'queued', 'settled'].includes(entry.status)) throw new Error();
      return [key, { revision: entry.revision, status: entry.status }];
    }));
    if (upgraded) persistWatermarks(path, new Map(Object.entries(normalized)));
    return normalized;
  } catch {
    throw new Error('Invalid dedup watermark store');
  }
}

function persistWatermarks(path: string, watermarks: ReadonlyMap<string, DedupWatermarkEntry>): void {
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
  const watermarks = new Map<string, DedupWatermarkEntry>(
    options.watermarksPath ? Object.entries(loadWatermarks(options.watermarksPath, options)) : [],
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
        const prior = watermarks.get(key);
        if (prior && prior.revision >= raw) {
          return {
            stage: 'rejected',
            detail: prior.status === 'unknown' ? DEDUP_CAS_AMBIGUOUS_DETAIL : DEDUP_CAS_DUPLICATE_DETAIL,
            executionReceipt: {
              executor: 'dedup-cas',
              executionId: 'dedup-cas:' + envelope.id,
              handoff: prior.status,
              durable: Boolean(options.watermarksPath),
            },
          };
        }
        const save = (entry: DedupWatermarkEntry | undefined) => {
          const updated = new Map(watermarks);
          if (entry) updated.set(key, entry);
          else updated.delete(key);
          // Publish in memory only after the durable write succeeds.
          if (options.watermarksPath) persistWatermarks(options.watermarksPath, updated);
          if (entry) watermarks.set(key, entry);
          else watermarks.delete(key);
        };
        save({ revision: raw, status: 'unknown' });
        let receipt: DispatchReceipt;
        try {
          receipt = await next([envelope]);
        } catch (error) {
          if ((error as { beforeExternalEffect?: boolean } | null)?.beforeExternalEffect === true) {
            save(prior);
          }
          throw error;
        }
        if (receipt.stage === 'dispatched' || receipt.stage === 'settled') {
          save({ revision: raw, status: 'settled' });
        } else if (isDurableQueuedReceipt(receipt)) {
          save({ revision: raw, status: 'queued' });
        } else if (['rejected', 'pending_approval', 'queued'].includes(receipt.stage)) {
          save(prior);
        }
        return receipt;
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
