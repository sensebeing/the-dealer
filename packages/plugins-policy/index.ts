import { existsSync, readFileSync } from 'node:fs';
import type {
  DispatchPolicyPlugin,
  DispatchReceipt,
  MessageEnvelope,
} from '../core-types/index.ts';
import {
  DEDUP_CAS_AMBIGUOUS_DETAIL,
  DEDUP_CAS_DUPLICATE_DETAIL,
  isConfirmedHandoffReceipt,
  isDurableQueuedReceipt,
  isRetainedApprovalReceipt,
  isRetryablePreEffectReceipt,
  envelopeClaimVersion,
} from '../core-types/index.ts';
import { writeFileAtomically } from '../fs-atomic/index.ts';
export {
  DEDUP_CAS_AMBIGUOUS_DETAIL,
  DEDUP_CAS_DUPLICATE_DETAIL,
  isDedupCasDuplicateReceipt,
  isDurableQueuedReceipt,
  isConfirmedHandoffReceipt,
  isRetryablePreEffectReceipt,
  isUnknownEffectReceipt,
  isUnattemptedOutboxItem,
} from '../core-types/index.ts';

export type DedupWatermarkStatus = 'unknown' | 'queued' | 'settled';
export interface DedupWatermarkEntry {
  revision: number;
  status: DedupWatermarkStatus;
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
  writeFileAtomically(path, JSON.stringify(Object.fromEntries(watermarks)));
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
      const raw = envelopeClaimVersion(envelope);
      if (raw === null) {
        return {
          stage: 'rejected', detail: 'Invalid claim version',
          executionReceipt: {
            executor: 'dedup-cas', executionId: 'dedup-cas:' + envelope.id,
            handoff: 'unattempted', durable: Boolean(options.watermarksPath), envelopeSignature: envelope.signature,
          },
        };
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
        if (isConfirmedHandoffReceipt(receipt, { allowQueued: false })) {
          save({ revision: raw, status: 'settled' });
        } else if (isConfirmedHandoffReceipt(receipt, { allowQueued: true })) {
          save({ revision: raw, status: 'queued' });
        } else if (
          isRetryablePreEffectReceipt(receipt) ||
          isRetainedApprovalReceipt(receipt) ||
          receipt.stage === 'pending_approval' ||
          (receipt.stage === 'queued' && !isDurableQueuedReceipt(receipt))
        ) {
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
