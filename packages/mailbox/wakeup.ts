import { CommandNotStartedError, singleLine } from '../subprocess/index.ts';
import type { Disposable } from '../core-types/index.ts';
import { normalizeMailboxId, baseStreamKeyOf } from './store.ts';
import type { MailboxCheckResult, MailboxItem, MailboxStore, WakeTargetState } from './store.ts';
import type { OutboxStore } from './outbox.ts';

export interface SlotProbeResult {
  readonly busy: boolean;
  readonly ready: boolean;
  readonly reason?: string;
}

export interface WakeupProbeContext {
  readonly store: MailboxStore;
  readonly mailboxId: string;
  readonly target: WakeTargetState;
  readonly hasPendingWave: boolean;
  readonly now: () => number;
  readonly reportError: (error: unknown) => void;
}

export interface WakeupDriverContext {
  readonly store: MailboxStore;
  readonly mailboxId: string;
  readonly target: WakeTargetState;
  readonly preview: MailboxCheckResult;
  readonly now: () => number;
  readonly reportError: (error: unknown) => void;
  readonly signal?: AbortSignal;
}
export interface WakeupReadinessContext {
  readonly mailboxId: string;
  readonly target: WakeTargetState;
  readonly reason?: string;
  /** Ends a wait without requesting an immediate wake attempt. */
  readonly onSettled?: () => void;
}
export type WakeupDriverOutcome =
  | {
      readonly kind: 'delivered';
      readonly pointer: string;
      readonly rotated?: boolean;
      readonly warning?: string;
    }
  | { readonly kind: 'noop'; readonly reason: string }
  | { readonly kind: 'disarm'; readonly reason: string; readonly expectedRevision?: number }
  | {
      readonly kind: 'defer';
      readonly expectedRevision?: number;
      readonly reason: string;
      readonly rotated?: boolean;
      readonly patch?: Partial<WakeTargetState>;
    };
export interface WakeupDriver {
  readonly name: string;
  deliver(context: WakeupDriverContext): Promise<WakeupDriverOutcome>;
  subscribeReadiness?(context: WakeupReadinessContext, onReady: () => void): Disposable | undefined;
  sessionMailboxIdFor?(target: WakeTargetState): string | undefined;
  probeSlot?(context: WakeupProbeContext): Promise<SlotProbeResult>;
  /**
   * Read-only projection of a base stream's target onto an uninitialized secondary slot. The
   * driver owns its opaque configuration; the core only forwards the base state it already holds.
   */
  inheritSlotTarget?(mailboxId: string, baseTarget: WakeTargetState): WakeTargetState | undefined;
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
  onTransition?: (event: WakeupTransitionEvent) => void;
  /**
   * Composition-root decision that an uninitialized slot really inherits its base stream's
   * topic binding. Core never infers inheritance from mailbox naming alone.
   */
  canInheritSlotTarget?: (mailboxId: string, baseMailboxId: string) => boolean;
}

export interface WakeupTransitionEvent {
  mailboxId: string;
  driver: string;
  state: 'deferred' | 'delivered' | 'fenced' | 'unfenced' | 'disarmed';
  reason?: string;
  attemptCount: number;
  waitMs?: number;
}

type ReportedWakeBlock =
  | { state: 'deferred'; reason?: string; since: number; attemptCount: number }
  | { state: 'fenced' };

type ReadinessWait = {
  revision: number;
  startedAt: number;
  reconciledAt: number;
  subscription?: Disposable;
};

type ReadinessState = ReadinessWait | {
  revision: number;
  suppressedAttempt: number;
};

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
const READINESS_FALLBACK_RECONCILE_MS = 30_000;
const MIN_READINESS_WAIT_MS = 250;
const MAX_INLINE_BODY_CHARS = 400;
const ACK_SUPERSEDED_WAKE = 'Wake superseded by acknowledgement of the previous wave';

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

export { wakeupBackoffDelay } from './store.ts';

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
  store.deferWakeAttempt(mailboxId, undefined, error, now, patch);
}

/** This attempt already settled its launch fence; the redrive loop must not settle it twice. */
export class SettledWakeLaunchError extends Error {}

/** Share failure classification between initial executors and background redrive. */
export function settleWakeDeliveryFailure(
  store: MailboxStore,
  mailboxId: string,
  error: unknown,
  now = Date.now(),
  expectedRevision?: number,
): boolean {
  if (error instanceof CommandNotStartedError) {
    if (expectedRevision !== undefined) return store.deferWakeAttempt(mailboxId, expectedRevision, error, now).superseded;
    else recordWakeFailure(store, mailboxId, error, now);
    return false;
  }
  store.parkWakeDelivery(mailboxId, error);
  return false;
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
  private readonly onTransition?: (event: WakeupTransitionEvent) => void;
  private readonly reportedBlocks = new Map<string, ReportedWakeBlock>();
  private readonly canInheritSlotTarget?: (mailboxId: string, baseMailboxId: string) => boolean;
  private readonly timer: ReturnType<typeof setInterval>;
  private readonly subscriptions: Disposable[] = [];
  private readonly readinessSubscriptions = new Map<string, ReadinessState>();
  private readonly stopController = new AbortController();
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
    this.onTransition = options.onTransition;
    this.canInheritSlotTarget = options.canInheritSlotTarget;
    // Zero-latency redrive for coalesceMs === 0 targets; the timer only handles delayed work.
    this.subscriptions.push(
      this.store.onMail((mailboxId) => this.notifyMailbox(mailboxId)),
      this.store.onWakeTarget((mailboxId) => this.notifyMailbox(mailboxId)),
    );
    this.timer = setInterval(() => {
      void this.flushReady().catch((error) => this.report(error));
    }, flushIntervalMs);
    this.timer.unref();
    if (this.onTransition || [...this.drivers.values()].some(driver => driver.subscribeReadiness)) {
      for (const target of this.store.listWakeTargets()) {
        this.observeTarget(target, { initial: true });
        this.syncReadiness(target);
      }
    }
  }

  getLastError(): string | undefined {
    return this.lastError;
  }

  async probeSlot(mailboxId: string, options: { outbox?: OutboxStore } = {}): Promise<SlotProbeResult> {
    const id = normalizeMailboxId(mailboxId);
    if (this.stopped) return { busy: false, ready: false, reason: 'Wakeup service stopped' };
    if (this.inFlight.has(id) || this.claims.has(id)) {
      return { busy: true, ready: false, reason: 'wakeup_in_flight' };
    }
    const target = this.store.getWakeTarget(id) ?? this.inheritSlotTarget(id);
    const driver = target ? this.drivers.get(target.driver) : undefined;
    const sessionMailboxId = target ? driver?.sessionMailboxIdFor?.(target) : undefined;
    const obligations = this.store.inspectSlotObligations(id, { sessionMailboxId, outbox: options.outbox });
    if (obligations.immediateBusyReason) {
      return { busy: true, ready: false, reason: obligations.immediateBusyReason };
    }
    const { hasPendingWave } = obligations;
    if (!target || target.driver === 'passive') {
      return { busy: false, ready: !hasPendingWave, ...(hasPendingWave ? { reason: 'quiescent_leased_wave' } : {}) };
    }
    if (!driver) {
      return { busy: true, ready: false, reason: `Unsupported wakeup driver: ${target.driver}` };
    }
    if (!driver.probeSlot) {
      return { busy: false, ready: !hasPendingWave };
    }
    return await driver.probeSlot({
      store: this.store,
      mailboxId: id,
      target,
      hasPendingWave,
      now: this.now,
      reportError: error => this.report(error),
    });
  }

  /**
   * An uninitialized secondary slot reads its base stream's wake target through the registered
   * driver only after the composition root confirms the slot really inherits the base stream's
   * topic binding. The projection is never persisted, so a probe cannot fabricate mailbox state
   * or borrow a sibling slot's private conversation identity; a missing target alone is never
   * evidence of inheritance.
   */
  private inheritSlotTarget(mailboxId: string): WakeTargetState | undefined {
    if (!mailboxId.startsWith('stream:')) return undefined;
    // `baseStreamKeyOf` matches the trailing numeric slot and returns the base mailbox id.
    const baseMailboxId = baseStreamKeyOf(mailboxId);
    if (!baseMailboxId || this.canInheritSlotTarget?.(mailboxId, baseMailboxId) !== true) return undefined;
    const baseTarget = this.store.getWakeTarget(baseMailboxId);
    if (!baseTarget) return undefined;
    const baseDriver = this.drivers.get(baseTarget.driver);
    return baseDriver?.inheritSlotTarget?.(mailboxId, baseTarget);
  }

  /**
   * Suppress redrive for a mailbox while a caller performs its own bounded idle wait and send.
   * Claims are in-process only: if the process dies, the persisted `pendingWake` takes over.
   */
  claim(mailboxId: string): Disposable {
    const id = normalizeMailboxId(mailboxId);
    this.claims.set(id, (this.claims.get(id) ?? 0) + 1);
    const readiness = this.readinessSubscriptions.get(id);
    if (readiness && 'startedAt' in readiness) this.clearReadiness(id);
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
          if (!this.stopped && !this.inFlight.has(id)) this.observeMailbox(id);
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
    if (target) this.syncReadiness(target);
    if (target && !this.inFlight.has(id) && !this.claims.has(id)) this.observeTarget(target);
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
    // Install the in-flight guard before a driver can synchronously emit mailbox events.
    const operation = Promise.resolve().then(async () => {
      let result: WakeupFlushResult;
      try {
        result = await this.flushInternal(id, options.force === true);
      } finally {
        this.inFlight.delete(id);
      }
      this.observeMailbox(id, result.woke);
      if (result.woke || result.reason === 'Wake launch superseded by a newer binding' || result.reason === ACK_SUPERSEDED_WAKE) {
        this.notifyMailbox(id);
      }
      return result;
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
      const wait = this.readinessSubscriptions.get(target.mailboxId);
      if (wait && 'startedAt' in wait && wait.revision === (target.bindingRevision ?? 0)) {
        const now = this.now();
        if (now - wait.reconciledAt < READINESS_FALLBACK_RECONCILE_MS) continue;
        if (!this.inFlight.has(target.mailboxId)) wait.reconciledAt = now;
      }
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
    this.stopController.abort();
    for (const mailboxId of this.readinessSubscriptions.keys()) this.clearReadiness(mailboxId);
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
        if (this.drivers.get(driver.name) !== driver) return;
        this.drivers.delete(driver.name);
        if (driver.subscribeReadiness && !this.stopped) {
          for (const target of this.store.listWakeTargets()) {
            if (target.driver === driver.name) this.clearReadiness(target.mailboxId);
          }
        }
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
    let initialRevision = 0;
    try {
      if (this.stopped) return noWake(mailboxId, 'passive', 'Wakeup service stopped');
      target = this.store.getWakeTarget(mailboxId);
      if (!target) {
        return noWake(mailboxId, 'passive', 'No wake target');
      }
      initialRevision = target.bindingRevision ?? 0;
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
      if (!driver) return this.defer(mailboxId, initialRevision, `Unsupported wakeup driver: ${target.driver}`);
      const outcome = await driver.deliver({
        store: this.store,
        mailboxId,
        target,
        preview,
        now: this.now,
        reportError: (error) => this.report(error),
        signal: this.stopController.signal,
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
        const revision = outcome.expectedRevision ?? initialRevision;
        const completedWaves = revision === initialRevision ? target.completedWaves : undefined;
        const result = this.defer(mailboxId, revision, outcome.reason, outcome.patch, completedWaves);
        return result.reason === 'Wake launch superseded by a newer binding' ? result : { ...result, ...(outcome.rotated !== undefined ? { rotated: outcome.rotated } : {}) };
      }
      if (outcome.kind === 'disarm') {
        const result = this.store.disarmWakeTargetIfCurrent(mailboxId, outcome.expectedRevision ?? initialRevision);
        if (result.superseded) return noWake(mailboxId, target.driver, 'Wake launch superseded by a newer binding');
        this.reportedBlocks.delete(mailboxId);
        this.emitTransition(result.target, { state: 'disarmed', reason: outcome.reason, attemptCount: target.attemptCount });
      }
      return noWake(mailboxId, target.driver, outcome.reason);
    } catch (error) {
      this.report(error);
      if (!target) {
        return noWake(mailboxId, 'passive', String(error));
      }
      if (error instanceof SettledWakeLaunchError) return noWake(mailboxId, target.driver, error.message);
      try {
        // A driver may have persisted a new destination before this attempt failed;
        // settle against the authoritative store state.
        const current = this.store.getWakeTarget(mailboxId);
        if (current?.deliveryInDoubt) {
          const superseded = settleWakeDeliveryFailure(this.store, mailboxId, error, this.now(), initialRevision);
          if (superseded) return noWake(mailboxId, target.driver, 'Wake launch superseded by a newer binding');
          return noWake(mailboxId, target.driver, String(error));
        }
        return this.defer(mailboxId, initialRevision, String(error), undefined, target.completedWaves);
      } catch (settleError) {
        this.report(settleError);
        return noWake(mailboxId, target.driver, String(error));
      }
    }
  }

  private hasNewMail(preview: MailboxCheckResult): boolean {
    return preview.deliveryId ? preview.newerQueuedCount > 0 : preview.items.length > 0;
  }

  private defer(
    mailboxId: string, expectedRevision: number, reason: string, patch?: Partial<WakeTargetState>, expectedCompletedWaves?: number,
  ): WakeupFlushResult {
    const result = this.store.deferWakeAttempt(mailboxId, expectedRevision, reason, this.now(), patch, expectedCompletedWaves);
    if (result.acknowledged) return noWake(mailboxId, result.target.driver, ACK_SUPERSEDED_WAKE);
    return noWake(mailboxId, result.target.driver, result.superseded ? 'Wake launch superseded by a newer binding' : reason);
  }

  private observeTarget(target: WakeTargetState, { delivered = false, initial = false } = {}): void {
    if (!this.onTransition) return;
    const id = target.mailboxId;
    const previous = this.reportedBlocks.get(id);
    if (target.deliveryInDoubt) {
      if (previous?.state !== 'fenced') {
        this.reportedBlocks.set(id, { state: 'fenced' });
        if (!initial) this.emitTransition(target, { state: 'fenced', reason: target.lastWakeError, attemptCount: target.attemptCount });
      }
      return;
    }
    if (!delivered && target.pendingWake && target.attemptCount > 0) {
      const reason = target.lastWakeError;
      const since = previous?.state === 'deferred' ? previous.since : this.now();
      this.reportedBlocks.set(id, { state: 'deferred', reason, since, attemptCount: target.attemptCount });
      if (!initial && (previous?.state !== 'deferred' || previous.reason !== reason)) {
        this.emitTransition(target, { state: 'deferred', reason, attemptCount: target.attemptCount });
      }
      return;
    }
    this.reportedBlocks.delete(id);
    if (delivered && previous?.state === 'deferred') {
      this.emitTransition(target, {
        state: 'delivered', attemptCount: previous.attemptCount, waitMs: Math.max(0, this.now() - previous.since),
      });
    } else if (previous?.state === 'fenced') {
      this.emitTransition(target, { state: 'unfenced', attemptCount: target.attemptCount });
    }
  }

  private observeMailbox(mailboxId: string, delivered = false): void {
    try {
      const target = this.store.getWakeTarget(mailboxId);
      if (delivered || !target) this.clearReadiness(mailboxId);
      if (target) {
        this.observeTarget(target, { delivered });
        if (!delivered) this.syncReadiness(target);
      }
    } catch (error) {
      this.report(error);
    }
  }

  private clearReadiness(mailboxId: string): void {
    const state = this.readinessSubscriptions.get(mailboxId);
    this.readinessSubscriptions.delete(mailboxId);
    try {
      if (state && 'startedAt' in state) state.subscription?.dispose();
    } catch (error) {
      this.report(error);
    }
  }

  private syncReadiness(target: WakeTargetState): void {
    const { mailboxId, attemptCount } = target;
    const driver = this.drivers.get(target.driver);
    if (this.stopped || !target.pendingWake || target.deliveryInDoubt || attemptCount === 0 || !driver?.subscribeReadiness) {
      this.clearReadiness(mailboxId);
      return;
    }
    const revision = target.bindingRevision ?? 0;
    const current = this.readinessSubscriptions.get(mailboxId);
    if (current?.revision === revision && ('startedAt' in current || current.suppressedAttempt === attemptCount)) return;
    this.clearReadiness(mailboxId);
    if (this.claims.has(mailboxId) || this.inFlight.has(mailboxId)) return;

    const startedAt = this.now();
    const wait: ReadinessWait = { revision, startedAt, reconciledAt: startedAt };
    this.readinessSubscriptions.set(mailboxId, wait);
    const finishWait = (): boolean => {
      if (this.stopped || this.readinessSubscriptions.get(mailboxId) !== wait) return false;
      this.clearReadiness(mailboxId);
      return true;
    };
    try {
      const subscription = driver.subscribeReadiness({
        mailboxId, target, reason: target.lastWakeError, onSettled: finishWait,
      }, () => {
        if (!finishWait()) return;
        const latest = this.store.getWakeTarget(mailboxId);
        if (!latest?.pendingWake || latest.deliveryInDoubt || latest.attemptCount === 0 ||
            (latest.bindingRevision ?? 0) !== revision) return;
        const elapsedMs = Math.max(0, this.now() - wait.startedAt);
        if (elapsedMs < MIN_READINESS_WAIT_MS) {
          // An already-idle host can still fail the delivery gate for this backoff cycle.
          this.readinessSubscriptions.set(mailboxId, { revision, suppressedAttempt: latest.attemptCount });
          return;
        }
        void this.flushMailbox(mailboxId, { force: true }).catch(error => this.report(error));
      });
      if (this.readinessSubscriptions.get(mailboxId) !== wait) subscription?.dispose();
      else if (!subscription) this.clearReadiness(mailboxId);
      else wait.subscription = subscription;
    } catch (error) {
      if (this.readinessSubscriptions.get(mailboxId) === wait) this.clearReadiness(mailboxId);
      this.report(error);
    }
  }

  private emitTransition(
    target: WakeTargetState, transition: Omit<WakeupTransitionEvent, 'mailboxId' | 'driver'>,
  ): void {
    try {
      this.onTransition?.({ mailboxId: target.mailboxId, driver: target.driver, ...transition });
    } catch (error) {
      this.report(error);
    }
  }

  private report(error: unknown): void {
    this.lastError = String(error);
    this.onError?.(error);
  }
}
