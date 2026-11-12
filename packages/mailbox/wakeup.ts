import { existsSync } from 'node:fs';
import { buildAgentHostInvocation, resolveAgentHostContinuation } from '../agent-host/index.ts';
import {
  CommandNotStartedError,
  checkedCommand,
  checkOrcaWorkspaceIdle,
  ensureOrcaWorkspace,
  resolveOrSpawnOrcaWorkspaceTerminal,
  probeOrcaTerminalIdle,
  runCommand,
  sendOrcaTerminalText,
  setOrcaWorkspaceComment,
  singleLine,
  waitOrcaTerminalIdle,
} from '../orca-workspace/index.ts';
import type { CommandRunner, OrcaIdleResult } from '../orca-workspace/index.ts';
import type { Disposable } from '../core-types/index.ts';
import type {
  MailboxCheckResult,
  MailboxItem,
  MailboxStore,
  WakeTargetMode,
  WakeTargetState,
} from './store.ts';
import { normalizeMailboxId } from './store.ts';

export interface WakeupRedriveOptions {
  store: MailboxStore;
  orcaCommand?: string;
  commandRunner?: CommandRunner;
  /** Authoritative host override; when provided it replaces the Orca inventory probe. */
  idleCheck?: () => Promise<OrcaIdleResult>;
  /** Background redrive cadence for coalescing and busy-backoff targets; defaults to 1000ms. */
  flushIntervalMs?: number;
  now?: () => number;
  onError?: (error: unknown) => void;
}

export interface WakeupFlushResult {
  mailboxId: string;
  mode: WakeTargetMode;
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
  return preview.items.reduce((max, item) => Math.max(max, item.sequence),
    Math.max(preview.maxQueuedSequence ?? -1, preview.latestQueued?.sequence ?? -1));
}

function formatInlinePreview(item: MailboxItem | undefined): string {
  if (!item) return '';
  const latest = item.envelope;
  const rawBody = singleLine(latest.body);
  if (!rawBody) return '';
  const kindLabel = latest.semantic?.type && latest.semantic.type !== 'message' ? latest.semantic.type : latest.kind;
  const fromLabel = latest.from?.member || latest.from?.nodeId || 'peer';
  const threadId = latest.semantic?.threadId ?? latest.id;
  const snippet = rawBody.length > MAX_INLINE_BODY_CHARS ? `${rawBody.slice(0, MAX_INLINE_BODY_CHARS)}...` : rawBody;
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
  const taskItem = taskMessageId ? preview.items.find(item => item.id === taskMessageId) : undefined;
  if (
    taskMessageId &&
    target.completedWaves === 0 &&
    taskItem
  ) {
    const rawPrompt =
      typeof taskItem.envelope.payload?.terminalPrompt === 'string' &&
      taskItem.envelope.payload.terminalPrompt.trim()
        ? taskItem.envelope.payload.terminalPrompt
        : (taskItem.envelope.body || taskItem.envelope.subject);
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

/** Share failure classification between initial executors and background redrive. */
export function settleWakeDeliveryFailure(
  store: MailboxStore,
  mailboxId: string,
  error: unknown,
  now = Date.now(),
): void {
  // A command that never started cannot have had an effect. Anything else crossed the external
  // boundary and must stay fenced until the owner confirms the outcome.
  if (error instanceof CommandNotStartedError) {
    recordWakeFailure(store, mailboxId, error, now);
    return;
  }
  store.parkWakeDelivery(mailboxId, error);
}

/**
 * Single owner for one passive host delivery attempt. Validation, pointer construction and
 * invocation building happen before `beginWakeDelivery`; only the host invocation itself crosses
 * the external boundary, so unknown outcomes stay fenced while never-started commands retry.
 */
export async function deliverPassiveWakeTarget(
  store: MailboxStore,
  runner: CommandRunner,
  mailboxId: string,
  options: { signal?: AbortSignal; now?: number } = {},
): Promise<{ pointer: string; stdout: string; stderr?: string }> {
  const now = options.now ?? Date.now();
  let deliveryBegun = false;
  try {
    const target = store.getWakeTarget(mailboxId);
    if (!target?.hostAgent) throw new Error(`Passive wake target missing host agent: ${mailboxId}`);
    const preview = store.check({ mailboxId, peek: true });
    const pointer = buildWakeupPointer(mailboxId, target, preview);
    if (!pointer) throw new Error(`No pending Dealer messages in ${mailboxId}`);
    const invocation = buildAgentHostInvocation({
      agent: target.hostAgent,
      conversationMode: target.conversationMode ?? 'fixed',
      mailboxId, promptText: pointer, cwd: target.workspacePath,
      command: target.interactiveCommand, agentProfile: target.agentProfile,
      targetBot: target.targetBot, sessionId: target.sessionId,
    });
    store.beginWakeDelivery(mailboxId);
    deliveryBegun = true;
    const output = await checkedCommand(runner, { ...invocation, cwd: target.workspacePath, signal: options.signal });
    store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview),
      resolveAgentHostContinuation(target.hostAgent, mailboxId, target.sessionId, output.stdout, target.conversationMode ?? 'fixed'));
    return { pointer, stdout: output.stdout, stderr: output.stderr };
  } catch (error) {
    const fenced = store.getWakeTarget(mailboxId)?.deliveryInDoubt === true;
    if (fenced) {
      // A pre-existing fence refuses the attempt before the boundary; it keeps the original
      // unknown-outcome reason. Only this attempt's own post-boundary failure may settle it.
      if (deliveryBegun) settleWakeDeliveryFailure(store, mailboxId, error, now);
    } else {
      recordWakeFailure(store, mailboxId, error, now);
    }
    throw error;
  }
}

/**
 * Single owner for the spawned terminal's initial wake. A busy or unverifiable terminal is never
 * injected; it keeps `pendingWake` and the persisted handle so the redrive service resumes it.
 * Only the terminal send crosses the external boundary and can create an unknown-outcome fence.
 */
export async function deliverSpawnedOrcaTerminalWakeup(
  store: MailboxStore,
  runner: CommandRunner,
  mailboxId: string,
  options: { signal?: AbortSignal; now?: number } = {},
): Promise<{ pointer?: string; terminalHandle?: string; sent: boolean }> {
  const now = options.now ?? Date.now();
  let deliveryBegun = false;
  try {
    const target = store.getWakeTarget(mailboxId);
    if (!target?.terminalHandle) throw new Error(`Orca terminal wake target missing handle: ${mailboxId}`);
    const command = target.orcaCommand ?? 'orca';
    const preview = store.check({ mailboxId, peek: true });
    const idle = await waitOrcaTerminalIdle(runner, command, target.terminalHandle, target.workspacePath, 5000, options.signal);
    if (!idle.idle) {
      store.upsertWakeTarget(mailboxId, { pendingWake: true, terminalHandle: target.terminalHandle });
      return { terminalHandle: target.terminalHandle, sent: false };
    }
    const pointer = buildWakeupPointer(mailboxId, target, preview);
    if (!pointer) {
      store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview), { terminalHandle: target.terminalHandle });
      return { terminalHandle: target.terminalHandle, sent: false };
    }
    store.beginWakeDelivery(mailboxId);
    deliveryBegun = true;
    await sendOrcaTerminalText(runner, command, target.terminalHandle, pointer, {
      cwd: target.workspacePath, signal: options.signal, waitSubmit: 5,
    });
    store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview), { terminalHandle: target.terminalHandle });
    return { pointer, terminalHandle: target.terminalHandle, sent: true };
  } catch (error) {
    const fenced = store.getWakeTarget(mailboxId)?.deliveryInDoubt === true;
    if (fenced) {
      // A pre-existing fence refuses the attempt before the boundary; it keeps the original
      // unknown-outcome reason. Only this attempt's own post-boundary failure may settle it.
      if (deliveryBegun) settleWakeDeliveryFailure(store, mailboxId, error, now);
    } else {
      recordWakeFailure(store, mailboxId, error, now);
    }
    throw error;
  }
}

function noWake(
  mailboxId: string,
  mode: WakeTargetMode,
  reason: string,
): WakeupFlushResult {
  return { mailboxId, mode, woke: false, reason };
}

/** The one-line first-task pointer sent to a freshly spawned interactive worker. */
export function buildTaskPreamble(
  mailboxId: string,
  messageId: string,
  leaseId?: string,
  taskPrompt?: string,
): string {
  const lead =
    typeof taskPrompt === 'string' && taskPrompt.trim()
      ? `${singleLine(taskPrompt)} — `
      : '';
  return singleLine(
    `${lead}Dealer Task dispatched to mailbox ${mailboxId} (Message ID: ${messageId}). Run \`dealer inbox check --mailbox ${mailboxId} --json\` to read the full spec & attachments. If blocked on decisions, run \`dealer ask --reply-to ${messageId} --mailbox ${mailboxId} --body "<question>"\` and end your turn. When finished, run \`dealer settle ${messageId} --outcome succeeded --summary "<result>" --ack ${leaseId ?? '<leaseId>'}\`.`,
  );
}

/**
 * Durable wakeup redrive backed by the mailbox ledger. A wave or task is committed before any
 * pointer is sent, so a busy workspace or a daemon restart can never lose it. Text is injected
 * only after an authoritative idle proof; busy targets keep `pendingWake` and retry with backoff,
 * while a send whose external outcome is unknown stays fenced as `deliveryInDoubt` and is never
 * replayed by later mail, a forced flush or a restart.
 */
export class WakeupRedriveService {
  private readonly store: MailboxStore;
  private readonly runner: CommandRunner;
  private readonly command: string;
  private readonly idleCheck?: () => Promise<OrcaIdleResult>;
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
    this.runner = options.commandRunner ?? runCommand;
    this.command = options.orcaCommand ?? 'orca';
    this.idleCheck = options.idleCheck;
    this.now = options.now ?? Date.now;
    this.onError = options.onError;
    // Zero-latency redrive for coalesceMs === 0 targets; the timer only handles delayed work.
    this.subscriptions.push(
      this.store.onMail(mailboxId => this.notifyMailbox(mailboxId)),
      this.store.onWakeTarget(mailboxId => this.notifyMailbox(mailboxId)),
    );
    this.timer = setInterval(() => {
      void this.flushReady().catch(error => this.report(error));
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
    void this.flushMailbox(id).catch(error => this.report(error));
  }

  /** Flush one mailbox; a mailbox has at most one in-flight wakeup. */
  flushMailbox(
    mailboxId: string,
    options: WakeupMailboxOptions = {},
  ): Promise<WakeupFlushResult> {
    const id = normalizeMailboxId(mailboxId);
    if (this.stopped) {
      return Promise.resolve(noWake(id, 'passive', 'Wakeup service stopped'));
    }
    if (this.claims.has(id)) {
      return Promise.resolve(noWake(id, this.store.getWakeTarget(id)?.mode ?? 'passive', 'Wakeup suppressed by an active claim'));
    }
    const ongoing = this.inFlight.get(id);
    if (ongoing) {
      return ongoing;
    }
    let delivered = false;
    const operation = this.flushInternal(id, options.force === true).then(result => {
      delivered = result.woke;
      return result;
    }).finally(() => {
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

  private async flushInternal(
    mailboxId: string,
    force: boolean,
  ): Promise<WakeupFlushResult> {
    let target: WakeTargetState | undefined;
    try {
      target = this.store.getWakeTarget(mailboxId);
      if (!target) {
        return noWake(mailboxId, 'passive', 'No wake target');
      }
      if (target.deliveryInDoubt) {
        return noWake(mailboxId, target.mode, target.lastWakeError ?? 'Wake delivery requires reconciliation');
      }
      if (!target.pendingWake) {
        return noWake(mailboxId, target.mode, 'No pending wake');
      }
      if (!force && target.earliestWakeAt > this.now()) {
        return noWake(mailboxId, target.mode, 'Coalescing');
      }
      if (target.mode === 'passive' && !target.hostAgent) {
        this.store.upsertWakeTarget(mailboxId, {
          pendingWake: false,
          attemptCount: 0,
          earliestWakeAt: 0,
          lastWakeError: undefined,
        });
        return {
          mailboxId,
          mode: target.mode,
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
          target.mode,
          preview.deliveryId
            ? 'No newer mail behind the outstanding lease'
            : 'No queued mail',
        );
      }
      if (target.mode === 'passive' && target.hostAgent) {
        try {
          const delivered = await deliverPassiveWakeTarget(this.store, this.runner, mailboxId, { now: this.now() });
          return { mailboxId, mode: target.mode, woke: true, pointer: delivered.pointer, reason: 'Agent host wakeup delivered' };
        } catch (error) {
          // The delivery helper owns its own settlement; re-settling here would double the backoff.
          this.report(error);
          return noWake(mailboxId, target.mode, String(error));
        }
      }
      if (!this.idleCheck && target.mode === 'orca-terminal' && target.workspacePath && target.autoCreateWorkspace === true) {
        const workspace = await ensureOrcaWorkspace(this.runner, target.orcaCommand ?? this.command, {
          workspacePath: target.workspacePath, repoRoot: target.repoRoot, comment: target.workspaceComment,
        });
        if (workspace.workspacePath !== target.workspacePath) {
          target = this.store.upsertWakeTarget(mailboxId, { workspacePath: workspace.workspacePath });
        }
      }
      const idle = await this.authoritativeIdle(target);
      if (!idle.idle) {
        // A workspace whose directory is gone can never become idle again; retrying it forever
        // would only inflate the backoff ledger. The mail stays queued for a human or a rebind.
        if (
          !this.idleCheck &&
          idle.reason === 'Workspace not found' &&
          target.autoCreateWorkspace !== true &&
          target.workspacePath &&
          !existsSync(target.workspacePath)
        ) {
          this.store.upsertWakeTarget(mailboxId, {
            pendingWake: false,
            attemptCount: 0,
            earliestWakeAt: 0,
            lastWakeError: undefined,
          });
          return noWake(mailboxId, target.mode, 'Workspace not found');
        }
        return this.defer(mailboxId, target, idle.reason ?? 'Workspace is busy');
      }
      return await this.deliver(mailboxId, target, preview, idle.composerPrefilled === true);
    } catch (error) {
      this.report(error);
      if (!target) {
        return noWake(mailboxId, 'passive', String(error));
      }
      try {
        if (this.store.getWakeTarget(mailboxId)?.deliveryInDoubt) {
          settleWakeDeliveryFailure(this.store, mailboxId, error, this.now());
          return noWake(mailboxId, target.mode, String(error));
        }
        return this.defer(mailboxId, target, String(error));
      } catch (settleError) {
        this.report(settleError);
        return noWake(mailboxId, target.mode, String(error));
      }
    }
  }

  private hasNewMail(preview: MailboxCheckResult): boolean {
    return preview.deliveryId
      ? preview.newerQueuedCount > 0
      : preview.items.length > 0;
  }

  private async authoritativeIdle(target: WakeTargetState): Promise<OrcaIdleResult> {
    if (this.idleCheck) {
      return this.idleCheck();
    }
    const command = target.orcaCommand ?? this.command;
    if (target.mode === 'orca-run') {
      if (target.workspacePath) {
        return checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath);
      }
      // Orchestration mail queues without touching a TUI composer, so no keystroke race exists.
      return { idle: true };
    }
    if (target.workspacePath) {
      return checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath);
    }
    if (target.terminalHandle) {
      return probeOrcaTerminalIdle(
        this.runner,
        command,
        target.terminalHandle,
        target.workspacePath,
      );
    }
    return { idle: false, reason: 'Authoritative terminal idle check required' };
  }

  private defer(
    mailboxId: string,
    target: WakeTargetState,
    reason: string,
  ): WakeupFlushResult {
    recordWakeFailure(
      this.store,
      mailboxId,
      reason,
      this.now(),
      target.terminalHandle !== undefined ? { terminalHandle: target.terminalHandle } : {},
    );
    return noWake(mailboxId, target.mode, reason);
  }

  private async postRotationIdle(
    target: WakeTargetState,
    terminalHandle: string,
    command: string,
  ): Promise<OrcaIdleResult> {
    if (this.idleCheck) {
      return this.idleCheck();
    }
    const terminal = await waitOrcaTerminalIdle(
      this.runner,
      command,
      terminalHandle,
      target.workspacePath,
      5000,
    );
    if (!terminal.idle) {
      return terminal;
    }
    if (target.workspacePath) {
      return checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath);
    }
    return { idle: true };
  }

  private async deliver(
    mailboxId: string,
    target: WakeTargetState,
    preview: MailboxCheckResult,
    composerPrefilled: boolean,
  ): Promise<WakeupFlushResult> {
    const command = target.orcaCommand ?? this.command;
    let terminalHandle = target.terminalHandle;
    const needsRotation = !preview.deliveryId && target.rotateAfterWaves !== undefined && target.completedWaves >= target.rotateAfterWaves;
    // Real Orca mode re-resolves before every send so a terminal closed since the last delivery
    // is rebound (or respawned) before the external boundary is crossed; `preferredHandle` keeps
    // a live binding without spawning a second terminal. Tests that inject an authoritative idle
    // check only resolve when no handle exists yet.
    if ((!terminalHandle || !this.idleCheck) && target.workspacePath && (target.mode === 'orca-terminal' || needsRotation)) {
      const resolved = await resolveOrSpawnOrcaWorkspaceTerminal(this.runner, command, target.workspacePath, undefined, { agent: target.agent, interactiveCommand: target.interactiveCommand, preferredHandle: target.terminalHandle });
      terminalHandle = resolved.handle;
      target = { ...target, terminalHandle };
      if (resolved.spawned) {
        if (resolved.shellBootstrapped === true) {
          return this.defer(mailboxId, target, 'Terminal spawned; waiting for tui-idle before wakeup');
        }
        const idle = this.idleCheck ? await this.idleCheck() : await waitOrcaTerminalIdle(this.runner, command, terminalHandle, target.workspacePath, 5000);
        if (!idle.idle) return this.defer(mailboxId, target, 'Terminal spawned; waiting for tui-idle before wakeup');
      }
    }
    let rotated = false;
    let rotationTerminal: string | undefined;
    if (needsRotation) {
      rotationTerminal = terminalHandle;
      if (rotationTerminal) {
        await sendOrcaTerminalText(
          this.runner,
          command,
          rotationTerminal,
          '/new',
          { cwd: target.workspacePath, ...(composerPrefilled ? { clearComposer: true } : {}) },
        );
        // Reset before the pointer so a crash cannot rotate twice for the same completed wave.
        this.store.upsertWakeTarget(mailboxId, { completedWaves: 0 });
        rotated = true;
      }
    }

    if (rotated && rotationTerminal) {
      // `/new` tears down and re-initializes the TUI; never race the wakeup pointer into that
      // transition. A busy target keeps pendingWake so the next idle tick delivers only the pointer.
      const idle = await this.postRotationIdle(target, rotationTerminal, command);
      composerPrefilled = idle.composerPrefilled === true;
      if (!idle.idle) {
        const deferred = this.defer(
          mailboxId,
          { ...target, completedWaves: 0 },
          'Session rotated; waiting for tui-idle before wakeup',
        );
        return { ...deferred, rotated: true };
      }
    }

    const pointer = buildWakeupPointer(mailboxId, target, preview);
    if (!pointer) {
      this.store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview));
      return noWake(mailboxId, target.mode, 'Nothing new to deliver');
    }

    if (target.mode === 'orca-run' && !target.runId) throw new Error('orca-run wake target requires runId');
    if (target.mode === 'orca-terminal' && !terminalHandle) throw new Error('orca-terminal wake target requires terminalHandle or workspacePath');
    // The pointer is the only external effect left; fence it so a crash or an unconfirmed send
    // stays parked instead of being replayed by the next tick or restart.
    this.store.beginWakeDelivery(mailboxId);
    if (target.mode === 'orca-run') {
      const output = await this.runner({
        command,
        args: [
          'orchestration',
          'send',
          '--to',
          `run:${target.runId}`,
          '--subject',
          `Dealer wakeup (${mailboxId})`,
          '--body',
          pointer,
          '--json',
        ],
        cwd: target.workspacePath,
      });
      if (output.exitCode !== undefined && output.exitCode !== 0) {
        throw new Error(`Orca orchestration send exited ${output.exitCode}`);
      }
      if (JSON.parse(output.stdout).ok === false) {
        throw new Error('Orca wakeup delivery failed');
      }
    } else {
      await sendOrcaTerminalText(
        this.runner,
        command,
        terminalHandle!,
        pointer,
        { cwd: target.workspacePath, ...(composerPrefilled ? { clearComposer: true } : {}) },
      );
    }

    let warning: string | undefined;
    const comment =
      target.workspaceComment ??
      (mailboxId.startsWith('stream:') ? `Dealer: ${mailboxId} pending` : undefined);
    if (comment !== undefined) {
      try {
        await setOrcaWorkspaceComment(
          this.runner,
          command,
          target.workspacePath,
          comment,
        );
      } catch (error) {
        // The pointer is already delivered; retrying it would duplicate a wakeup.
        warning = String(error);
        this.report(error);
      }
    }

    this.store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview), terminalHandle ? { terminalHandle } : {});
    return {
      mailboxId,
      mode: target.mode,
      woke: true,
      pointer,
      ...(preview.deliveryId ? { leaseId: preview.deliveryId } : {}),
      newerQueuedCount: preview.newerQueuedCount,
      rotated,
      ...(warning !== undefined ? { warning } : {}),
    };
  }

  private report(error: unknown): void {
    this.lastError = String(error);
    this.onError?.(error);
  }
}
