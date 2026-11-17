import { CommandNotStartedError, singleLine } from '../subprocess/index.ts';
import type { Disposable } from '../core-types/index.ts';
import { normalizeMailboxId } from './store.ts';
import type { MailboxCheckResult, MailboxItem, MailboxStore, WakeTargetState } from './store.ts';

export interface WakeupDriverContext {
  readonly store: MailboxStore;
  readonly mailboxId: string;
  readonly target: WakeTargetState;
  readonly preview: MailboxCheckResult;
  readonly now: () => number;
  readonly reportError: (error: unknown) => void;
}
export type WakeupDriverOutcome =
  | {
      readonly kind: 'delivered';
      readonly pointer: string;
      readonly rotated?: boolean;
      readonly warning?: string;
    }
  | { readonly kind: 'noop'; readonly reason: string }
  | { readonly kind: 'disarm'; readonly reason: string }
  | {
      readonly kind: 'defer';
      readonly reason: string;
      readonly rotated?: boolean;
      readonly patch?: Partial<WakeTargetState>;
    };
export interface WakeupDriver {
  readonly name: string;
  deliver(context: WakeupDriverContext): Promise<WakeupDriverOutcome>;
  resolveSessionTarget?(
    mailboxId: string,
    store: MailboxStore,
  ): Promise<Partial<WakeTargetState> | undefined> | Partial<WakeTargetState> | undefined;
}
export interface WakeupRedriveOptions {
  store: MailboxStore;
  drivers?: readonly WakeupDriver[];
  flushIntervalMs?: number;
  now?: () => number;
  onError?: (error: unknown) => void;
}

export interface WakeupFlushResult {
  mailboxId: string;
  driver: string;
  woke: boolean;
  reason?: string;
  /** The exact single-line pointer delivered to the run or terminal. */
  pointer?: string;
  leaseId?: string;
  newerQueuedCount?: number;
  rotated?: boolean;
  warning?: string;
}

export interface WakeupMailboxOptions {
  /** Explicit flush ignores coalesce/backoff maturity, but never bypasses an active claim. */
  force?: boolean;
}

const DEFAULT_FLUSH_INTERVAL_MS = 1_000;
const DEFAULT_BACKOFF_MS = [1_000, 2_000, 5_000] as const;
const MAX_BACKOFF_MS = 10_000;
const MAX_INLINE_BODY_CHARS = 400;

export function maxPreviewSequence(preview: MailboxCheckResult): number {
  return preview.items.reduce(
    (max, item) => Math.max(max, item.sequence),
    Math.max(preview.maxQueuedSequence ?? -1, preview.latestQueued?.sequence ?? -1),
  );
}

function formatInlinePreview(item: MailboxItem | undefined): string {
  if (!item) return '';
  const latest = item.envelope;
  const rawBody = singleLine(latest.body);
  if (!rawBody) return '';
  const kindLabel =
    latest.semantic?.type && latest.semantic.type !== 'message' ? latest.semantic.type : latest.kind;
  const fromLabel = latest.from?.member || latest.from?.nodeId || 'peer';
  const threadId = latest.semantic?.threadId ?? latest.id;
  const snippet =
    rawBody.length > MAX_INLINE_BODY_CHARS ? `${rawBody.slice(0, MAX_INLINE_BODY_CHARS)}...` : rawBody;
  return ` — Latest (${kindLabel} from ${fromLabel}, thread ${threadId}): "${snippet}"`;
}

export function buildWakeupPointer(
  mailboxId: string,
  target: Pick<WakeTargetState, 'completedWaves' | 'wakeupPrefix'>,
  preview: MailboxCheckResult,
): string | undefined {
  const body = buildPointerBody(mailboxId, target, preview);
  return body && target.wakeupPrefix?.trim() ? `${singleLine(target.wakeupPrefix)} ${body}` : body;
}

function buildPointerBody(
  mailboxId: string,
  target: Pick<WakeTargetState, 'completedWaves' | 'wakeupPrefix'>,
  preview: MailboxCheckResult,
): string | undefined {
  if (preview.deliveryId) {
    if (preview.newerQueuedCount === 0) {
      return undefined;
    }
    const latestHint = preview.latestQueued?.wakeupHint;
    const inline = formatInlinePreview(preview.latestQueued);
    return singleLine(
      `Unacknowledged Dealer batch ${preview.deliveryId} + ${preview.newerQueuedCount} newer message(s) in ${mailboxId}${latestHint ? ` [${latestHint}]` : ''}${inline}. Run \`dealer inbox check --mailbox ${mailboxId} --json\` to resume, or add \`--ack ${preview.deliveryId}\` if already handled.`,
    );
  }
  if (preview.items.length === 0) {
    return undefined;
  }
  const taskMessageId = mailboxId.startsWith('task:') ? mailboxId.slice(5) : undefined;
  const taskItem = taskMessageId ? preview.items.find((item) => item.id === taskMessageId) : undefined;
  if (taskMessageId && target.completedWaves === 0 && taskItem) {
    const rawPrompt =
      typeof taskItem.envelope.payload?.terminalPrompt === 'string' &&
      taskItem.envelope.payload.terminalPrompt.trim()
        ? taskItem.envelope.payload.terminalPrompt
        : taskItem.envelope.body || taskItem.envelope.subject;
    return buildTaskPreamble(mailboxId, taskMessageId, undefined, rawPrompt);
  }
  const latestHint = preview.items.at(-1)?.wakeupHint;
  const inline = formatInlinePreview(preview.items.at(-1));
  return singleLine(
    `You have ${preview.items.length} Dealer message(s) in ${mailboxId}${latestHint ? ` [${latestHint}]` : ''}${inline}. Run \`dealer inbox check --mailbox ${mailboxId} --json\`.`,
  );
}

/** Exponential busy-backoff: 1s -> 2s -> 5s -> capped at 10s. */
export function wakeupBackoffDelay(attemptCount: number): number {
  if (!Number.isSafeInteger(attemptCount) || attemptCount < 1) {
    throw new Error('Invalid wake attempt count');
  }
  if (attemptCount <= DEFAULT_BACKOFF_MS.length) {
    return DEFAULT_BACKOFF_MS[attemptCount - 1] ?? MAX_BACKOFF_MS;
  }
  return MAX_BACKOFF_MS;
}

/**
 * Record a pre-effect failure: the external boundary was never crossed, so the target stays
 * retryable and returns to the exponential backoff ledger.
 */
export function recordWakeFailure(
  store: MailboxStore,
  mailboxId: string,
  error: unknown,
  now = Date.now(),
  patch: Partial<WakeTargetState> = {},
): void {
  const attemptCount = (store.getWakeTarget(mailboxId)?.attemptCount ?? 0) + 1;
  store.upsertWakeTarget(mailboxId, {
    ...patch,
    deliveryInDoubt: undefined,
    pendingWake: true,
    attemptCount,
    earliestWakeAt: now + wakeupBackoffDelay(attemptCount),
    lastWakeError: String(error),
  });
}

/** This attempt already settled its launch fence; the redrive loop must not settle it twice. */
export class SettledWakeLaunchError extends Error {}

/** Share failure classification between initial executors and background redrive. */
export function settleWakeDeliveryFailure(
  store: MailboxStore,
  mailboxId: string,
  error: unknown,
  now = Date.now(),
): void {
  if (error instanceof CommandNotStartedError) {
    recordWakeFailure(store, mailboxId, error, now);
    return;
  }
  store.parkWakeDelivery(mailboxId, error);
}

function noWake(mailboxId: string, driver: string, reason: string): WakeupFlushResult {
  return { mailboxId, driver, woke: false, reason };
}

/** The one-line first-task pointer sent to a freshly spawned interactive worker. */
export function buildTaskPreamble(
  mailboxId: string,
  messageId: string,
  leaseId?: string,
  taskPrompt?: string,
): string {
  const lead = typeof taskPrompt === 'string' && taskPrompt.trim() ? `${singleLine(taskPrompt)} — ` : '';
  return singleLine(
    `${lead}Dealer Task dispatched to mailbox ${mailboxId} (Message ID: ${messageId}). Run \`dealer inbox check --mailbox ${mailboxId} --json\` to read the full spec & attachments. If blocked on decisions, run \`dealer ask --reply-to ${messageId} --mailbox ${mailboxId} --body "<question>"\` and end your turn. When finished, run \`dealer settle ${messageId} --outcome succeeded --summary "<result>" --ack ${leaseId ?? '<leaseId>'}\`.`,
  );
}

/**
 * Durable wakeup redrive backed by the mailbox ledger. A wave or task is committed before any
 * pointer is sent, so a busy driver or a daemon restart can never lose it. Drivers own delivery
 * readiness and external effects; deferred targets keep `pendingWake` and retry with backoff,
 * while a send whose external outcome is unknown stays fenced as `deliveryInDoubt` and is never
 * replayed by later mail, a forced flush or a restart.
 */
export class WakeupRedriveService {
  private readonly store: MailboxStore;
  private readonly drivers = new Map<string, WakeupDriver>();
  private readonly now: () => number;
  private readonly onError?: (error: unknown) => void;
  private readonly timer: ReturnType<typeof setInterval>;
  private readonly subscriptions: Disposable[] = [];
  private readonly inFlight = new Map<string, Promise<WakeupFlushResult>>();
  private readonly claims = new Map<string, number>();
  private stopped = false;
  private lastError?: string;

  constructor(options: WakeupRedriveOptions) {
    if (!options.store) {
      throw new Error('Wakeup redrive requires a mailbox store');
    }
    const flushIntervalMs = options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    if (!Number.isSafeInteger(flushIntervalMs) || flushIntervalMs <= 0) {
      throw new Error('Flush interval must be a positive integer');
    }
    this.store = options.store;
    for (const driver of options.drivers ?? []) this.registerDriver(driver);
    this.now = options.now ?? Date.now;
    this.onError = options.onError;
    // Zero-latency redrive for coalesceMs === 0 targets; the timer only handles delayed work.
    this.subscriptions.push(
      this.store.onMail((mailboxId) => this.notifyMailbox(mailboxId)),
      this.store.onWakeTarget((mailboxId) => this.notifyMailbox(mailboxId)),
    );
    this.timer = setInterval(() => {
      void this.flushReady().catch((error) => this.report(error));
    }, flushIntervalMs);
    this.timer.unref();
  }

  getLastError(): string | undefined {
    return this.lastError;
  }

  /**
   * Suppress redrive for a mailbox while a caller performs its own bounded idle wait and send.
   * Claims are in-process only: if the process dies, the persisted `pendingWake` takes over.
   */
  claim(mailboxId: string): Disposable {
    const id = normalizeMailboxId(mailboxId);
    this.claims.set(id, (this.claims.get(id) ?? 0) + 1);
    let released = false;
    return {
      dispose: () => {
        if (released) {
          return;
        }
        released = true;
        const remaining = (this.claims.get(id) ?? 1) - 1;
        if (remaining <= 0) {
          this.claims.delete(id);
        } else {
          this.claims.set(id, remaining);
        }
      },
    };
  }

  /** Wake immediately when a target's coalesce/backoff window has already matured. */
  notifyMailbox(mailboxId: string): void {
    if (this.stopped) {
      return;
    }
    let id: string;
    try {
      id = normalizeMailboxId(mailboxId);
    } catch {
      return;
    }
    const target = this.store.getWakeTarget(id);
    if (!target?.pendingWake || target.deliveryInDoubt || target.earliestWakeAt > this.now()) {
      return;
    }
    void this.flushMailbox(id).catch((error) => this.report(error));
  }

  /** Flush one mailbox; a mailbox has at most one in-flight wakeup. */
  flushMailbox(mailboxId: string, options: WakeupMailboxOptions = {}): Promise<WakeupFlushResult> {
    const id = normalizeMailboxId(mailboxId);
    if (this.stopped) {
      return Promise.resolve(noWake(id, 'passive', 'Wakeup service stopped'));
    }
    if (this.claims.has(id)) {
      return Promise.resolve(
        noWake(id, this.store.getWakeTarget(id)?.driver ?? 'passive', 'Wakeup suppressed by an active claim'),
      );
    }
    const ongoing = this.inFlight.get(id);
    if (ongoing) {
      return ongoing;
    }
    let delivered = false;
    // Install the in-flight guard before a driver can synchronously emit mailbox events.
    const operation = Promise.resolve()
      .then(() => this.flushInternal(id, options.force === true))
      .then((result) => {
        delivered = result.woke;
        return result;
      })
      .finally(() => {
        if (this.inFlight.get(id) === operation) {
          this.inFlight.delete(id);
          if (delivered) this.notifyMailbox(id);
        }
      });
    this.inFlight.set(id, operation);
    return operation;
  }

  /** Deterministic flush of every pending target, ignoring coalesce/backoff maturity. */
  async flushWakeups(): Promise<WakeupFlushResult[]> {
    const targets = this.store.listPendingWakeTargets(Number.MAX_SAFE_INTEGER);
    const results: WakeupFlushResult[] = [];
    for (const target of targets) {
      results.push(await this.flushMailbox(target.mailboxId, { force: true }));
    }
    return results;
  }

  /** Background flush of mature targets only. */
  async flushReady(): Promise<WakeupFlushResult[]> {
    const targets = this.store.listPendingWakeTargets(this.now());
    const results: WakeupFlushResult[] = [];
    for (const target of targets) {
      results.push(await this.flushMailbox(target.mailboxId));
    }
    return results;
  }

  async stop(): Promise<void> {
    if (this.stopped) {
      return;
    }
    this.stopped = true;
    clearInterval(this.timer);
    for (const subscription of this.subscriptions.splice(0)) {
      subscription.dispose();
    }
    await Promise.allSettled([...this.inFlight.values()]);
  }

  registerDriver(driver: WakeupDriver): Disposable {
    if (!/^[a-z0-9][a-z0-9:_-]{0,63}$/i.test(driver.name) || driver.name === 'passive')
      throw new Error(`Invalid wakeup driver: ${driver.name}`);
    if (this.drivers.has(driver.name)) throw new Error(`Wakeup driver already registered: ${driver.name}`);
    this.drivers.set(driver.name, driver);
    let disposed = false;
    return {
      dispose: () => {
        if (disposed) return;
        disposed = true;
        if (this.drivers.get(driver.name) === driver) this.drivers.delete(driver.name);
      },
    };
  }

  async resolveSessionWakeTarget(mailboxId: string): Promise<Partial<WakeTargetState> | undefined> {
    const id = normalizeMailboxId(mailboxId);
    const current = this.store.getWakeTarget(id);
    if (current && current.driver !== 'passive') return undefined;
    for (const driver of this.drivers.values()) {
      const resolved = await driver.resolveSessionTarget?.(id, this.store);
      if (resolved) return resolved;
    }
    return undefined;
  }

  private async flushInternal(mailboxId: string, force: boolean): Promise<WakeupFlushResult> {
    let target: WakeTargetState | undefined;
    try {
      target = this.store.getWakeTarget(mailboxId);
      if (!target) {
        return noWake(mailboxId, 'passive', 'No wake target');
      }
      if (target.deliveryInDoubt) {
        return noWake(
          mailboxId,
          target.driver,
          target.lastWakeError ?? 'Wake delivery requires reconciliation',
        );
      }
      if (!target.pendingWake) {
        return noWake(mailboxId, target.driver, 'No pending wake');
      }
      if (!force && target.earliestWakeAt > this.now()) {
        return noWake(mailboxId, target.driver, 'Coalescing');
      }
      if (target.driver === 'passive') {
        this.store.upsertWakeTarget(mailboxId, {
          pendingWake: false,
          attemptCount: 0,
          earliestWakeAt: 0,
          lastWakeError: undefined,
        });
        return {
          mailboxId,
          driver: target.driver,
          woke: true,
          reason: 'In-process mailbox event delivered',
        };
      }
      const preview = this.store.check({ mailboxId, peek: true });
      if (!this.hasNewMail(preview)) {
        this.store.upsertWakeTarget(mailboxId, {
          pendingWake: false,
          attemptCount: 0,
          earliestWakeAt: 0,
          lastWakeError: undefined,
        });
        return noWake(
          mailboxId,
          target.driver,
          preview.deliveryId ? 'No newer mail behind the outstanding lease' : 'No queued mail',
        );
      }
      const driver = this.drivers.get(target.driver);
      if (!driver) return this.defer(mailboxId, `Unsupported wakeup driver: ${target.driver}`);
      const outcome = await driver.deliver({
        store: this.store,
        mailboxId,
        target,
        preview,
        now: this.now,
        reportError: (error) => this.report(error),
      });
      if (outcome.kind === 'delivered') {
        return {
          mailboxId,
          driver: target.driver,
          woke: true,
          pointer: outcome.pointer,
          ...(preview.deliveryId ? { leaseId: preview.deliveryId } : {}),
          newerQueuedCount: preview.newerQueuedCount,
          ...(outcome.rotated !== undefined ? { rotated: outcome.rotated } : {}),
          ...(outcome.warning !== undefined ? { warning: outcome.warning } : {}),
        };
      }
      if (outcome.kind === 'defer') {
        const result = this.defer(mailboxId, outcome.reason, outcome.patch);
        return { ...result, ...(outcome.rotated !== undefined ? { rotated: outcome.rotated } : {}) };
      }
      if (outcome.kind === 'disarm')
        this.store.upsertWakeTarget(mailboxId, {
          pendingWake: false,
          attemptCount: 0,
          earliestWakeAt: 0,
          lastWakeError: undefined,
        });
      return noWake(mailboxId, target.driver, outcome.reason);
    } catch (error) {
      this.report(error);
      if (!target) {
        return noWake(mailboxId, 'passive', String(error));
      }
      if (error instanceof SettledWakeLaunchError) return noWake(mailboxId, target.driver, String(error));
      try {
        // A driver may have persisted a new destination before this attempt failed;
        // settle against the authoritative store state.
        const current = this.store.getWakeTarget(mailboxId);
        if (current?.deliveryInDoubt) {
          settleWakeDeliveryFailure(this.store, mailboxId, error, this.now());
          return noWake(mailboxId, target.driver, String(error));
        }
        return this.defer(mailboxId, String(error));
      } catch (settleError) {
        this.report(settleError);
        return noWake(mailboxId, target.driver, String(error));
      }
    }
  }

  private hasNewMail(preview: MailboxCheckResult): boolean {
    return preview.deliveryId ? preview.newerQueuedCount > 0 : preview.items.length > 0;
  }

  private defer(mailboxId: string, reason: string, patch?: Partial<WakeTargetState>): WakeupFlushResult {
    recordWakeFailure(this.store, mailboxId, reason, this.now(), patch);
    return noWake(mailboxId, this.store.getWakeTarget(mailboxId)?.driver ?? 'passive', reason);
  }

  private report(error: unknown): void {
    this.lastError = String(error);
    this.onError?.(error);
  }
}
