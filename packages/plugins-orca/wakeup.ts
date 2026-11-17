import { existsSync } from 'node:fs';
import {
  OrcaTerminalLaunchRejectedError,
  readOrcaTerminal,
  checkOrcaWorkspaceIdle,
  ensureOrcaWorkspace,
  isOrcaWorkspaceTerminalLive,
  planOrcaWorkspaceTerminal,
  probeOrcaTerminalIdle,
  sendOrcaTerminalText,
  setOrcaWorkspaceComment,
  spawnOrcaWorkspaceTerminal,
  waitOrcaTerminalIdle,
  findOrcaWorktreeByName,
} from './workspace.ts';
import type { OrcaIdleResult, SupportedOrcaAgent } from './workspace.ts';
import { defaultAgentLaunchCommand } from '../plugins-agent-host/hosts.ts';
import { CommandNotStartedError, runCommand } from '../subprocess/index.ts';
import type { CommandRunner } from '../subprocess/index.ts';
import type { AgentConversationMode } from '../core-types/index.ts';
import type { MailboxCheckResult, MailboxStore, WakeTargetState } from '../mailbox/store.ts';
import { normalizeMailboxId } from '../mailbox/store.ts';
import {
  buildWakeupPointer,
  maxPreviewSequence,
  recordWakeFailure,
  settleWakeDeliveryFailure,
  SettledWakeLaunchError,
} from '../mailbox/wakeup.ts';
import type { WakeupDriver, WakeupDriverContext, WakeupDriverOutcome } from '../mailbox/wakeup.ts';

export interface OrcaTerminalDriverConfig {
  orcaCommand?: string;
  agent?: SupportedOrcaAgent;
  interactiveCommand?: string;
  worktreeId?: string;
  workspacePath?: string;
  terminalHandle?: string;
  repoRoot?: string;
  autoCreateWorkspace?: boolean;
  workspaceComment?: string;
}
export interface OrcaRunDriverConfig extends OrcaTerminalDriverConfig {
  runId?: string;
}

type OrcaWakeTarget = WakeTargetState & OrcaRunDriverConfig;

export function readOrcaDriverConfig(target: Pick<WakeTargetState, 'driverConfig'>): OrcaRunDriverConfig {
  const config = target.driverConfig ?? {};
  for (const field of [
    'orcaCommand',
    'interactiveCommand',
    'worktreeId',
    'workspacePath',
    'terminalHandle',
    'repoRoot',
    'workspaceComment',
    'runId',
  ]) {
    if (config[field] !== undefined && typeof config[field] !== 'string')
      throw new Error(`Invalid Orca driver configuration: ${field}`);
  }
  if (config.agent !== undefined && !['antigravity', 'claude', 'codex'].includes(config.agent as string))
    throw new Error('Invalid Orca driver agent');
  if (config.autoCreateWorkspace !== undefined && typeof config.autoCreateWorkspace !== 'boolean')
    throw new Error('Invalid Orca workspace creation policy');
  return config as OrcaRunDriverConfig;
}

export function serializeOrcaDriverConfig(config: OrcaRunDriverConfig): Record<string, unknown> {
  const value = Object.fromEntries(Object.entries(config).filter(([, entry]) => entry !== undefined));
  readOrcaDriverConfig({ driverConfig: value });
  return value;
}

function readOrcaWakeTarget(target: WakeTargetState): OrcaWakeTarget {
  return { ...readOrcaDriverConfig(target), ...target };
}

export interface OrcaWakeupDriverOptions {
  commandRunner?: CommandRunner;
  orcaCommand?: string;
  /** Authoritative probe supplied by an embedded host or test harness. */
  idleCheck?: () => Promise<OrcaIdleResult>;
}

/** Idle verdict plus the delivery plan it was measured against. */
interface WakeupIdleDecision extends OrcaIdleResult {
  /** The pointer needs its own session tab, so retained composers were not inspected. */
  spawnFreshSession?: boolean;
}

/** Fixed Orca workspaces open one independent session tab per wave unless configured otherwise. */
export const DEFAULT_ORCA_WORKSPACE_ROTATE_AFTER_WAVES = 1;

export interface OrcaWaveRotateAfterWavesInput {
  mode?: AgentConversationMode;
  /** Threshold configured on the resolved topic binding. */
  bindingRotateAfterWaves?: number;
  /** Threshold configured once for the whole wave executor/host. */
  waveRotateAfterWaves?: number;
  /** Threshold supplied by a deployment whose receiver configured no local target. */
  fallbackRotateAfterWaves?: number;
  /** Whether the receiver manages a fixed Orca workspace that can host session tabs. */
  managesWorkspace: boolean;
  /** An Orca Run owns its coordinator session, so it never rotates tabs. */
  runId?: string;
  /** An explicitly pinned terminal keeps its conversation instead of rotating. */
  pinnedTerminalHandle?: string;
}

/**
 * The single owner of the managed Orca fixed-workspace rotation policy. Preset generation,
 * binding refresh, and wave enqueue all resolve their effective threshold here so the rule can
 * never diverge between entry paths.
 */
export function resolveOrcaWaveRotateAfterWaves(input: OrcaWaveRotateAfterWavesInput): number | undefined {
  const explicit =
    input.bindingRotateAfterWaves ?? input.waveRotateAfterWaves ?? input.fallbackRotateAfterWaves;
  if (explicit !== undefined) {
    return explicit;
  }
  if (input.mode === 'fixed') return undefined;
  return input.managesWorkspace && !input.runId && !input.pinnedTerminalHandle
    ? DEFAULT_ORCA_WORKSPACE_ROTATE_AFTER_WAVES
    : undefined;
}

/**
 * A completed wave (or rotation window) without an outstanding lease is the single rotation
 * trigger: the next wakeup must open its own session tab instead of reusing the retained one.
 */
function needsSessionRotation(
  target: Pick<WakeTargetState, 'driver' | 'rotateAfterWaves' | 'completedWaves'>,
  preview: Pick<MailboxCheckResult, 'deliveryId'>,
): boolean {
  return (
    !preview.deliveryId &&
    target.driver === 'orca-terminal' &&
    target.rotateAfterWaves !== undefined &&
    target.completedWaves >= target.rotateAfterWaves
  );
}

/** Persist custody before any terminal creation or shell bootstrap, including their unknown outcomes. */
export async function launchFencedOrcaWorkspaceTerminal(
  store: MailboxStore,
  runner: CommandRunner,
  mailboxId: string,
  options: {
    command: string;
    workspacePath: string;
    agent?: SupportedOrcaAgent;
    interactiveCommand?: string;
    signal?: AbortSignal;
    now?: number;
    resetCompletedWaves?: boolean;
    shellTerminalHandle?: string;
  },
) {
  store.beginWakeDelivery(mailboxId);
  try {
    let launched: { handle: string; raw: unknown };
    if (options.shellTerminalHandle) {
      await sendOrcaTerminalText(
        runner,
        options.command,
        options.shellTerminalHandle,
        options.interactiveCommand?.trim() || defaultAgentLaunchCommand(options.agent),
        { cwd: options.workspacePath, signal: options.signal },
      );
      launched = { handle: options.shellTerminalHandle, raw: undefined };
    } else {
      launched = await spawnOrcaWorkspaceTerminal(
        runner,
        options.command,
        options.workspacePath,
        options.signal,
        options,
      );
    }
    const target = store.upsertWakeTarget(mailboxId, {
      ...(options.resetCompletedWaves ? { completedWaves: 0 } : {}),
      deliveryInDoubt: undefined,
      pendingWake: true,
      lastWakeError: undefined,
      driverConfig: { terminalHandle: launched.handle },
    });
    return { ...launched, target: readOrcaWakeTarget(target) };
  } catch (error) {
    if (error instanceof CommandNotStartedError || error instanceof OrcaTerminalLaunchRejectedError) {
      recordWakeFailure(store, mailboxId, error, options.now);
    } else {
      store.parkWakeDelivery(mailboxId, error);
    }
    throw new SettledWakeLaunchError(String(error), { cause: error });
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
    const state = store.getWakeTarget(mailboxId);
    const target = state ? readOrcaWakeTarget(state) : undefined;
    if (!target?.terminalHandle) throw new Error(`Orca terminal wake target missing handle: ${mailboxId}`);
    const command = target.orcaCommand ?? 'orca';
    const preview = store.check({ mailboxId, peek: true });
    const ready = await waitOrcaTerminalIdle(
      runner,
      command,
      target.terminalHandle,
      target.workspacePath,
      5000,
      options.signal,
    );
    const idle = !ready.idle
      ? ready
      : target.workspacePath
        ? await checkOrcaWorkspaceIdle(runner, command, target.workspacePath, options.signal, {
            targetTerminalHandle: target.terminalHandle,
            agent: target.agent,
          })
        : await probeOrcaTerminalIdle(runner, command, target.terminalHandle, undefined, options.signal);
    if (!idle.idle) {
      store.upsertWakeTarget(mailboxId, {
        pendingWake: true,
        driverConfig: { terminalHandle: target.terminalHandle },
      });
      return { terminalHandle: target.terminalHandle, sent: false };
    }
    const pointer = buildWakeupPointer(mailboxId, target, preview);
    if (!pointer) {
      store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview), {
        driverConfig: { terminalHandle: target.terminalHandle },
      });
      return { terminalHandle: target.terminalHandle, sent: false };
    }
    store.beginWakeDelivery(mailboxId);
    deliveryBegun = true;
    await sendOrcaTerminalText(runner, command, target.terminalHandle, pointer, {
      cwd: target.workspacePath,
      signal: options.signal,
      waitSubmit: 5,
    });
    store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview), {
      driverConfig: { terminalHandle: target.terminalHandle },
    });
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

export function pinOrcaTerminalSessionMailbox(
  store: MailboxStore,
  mailboxId: string,
  terminalHandle?: string,
): string {
  const id = normalizeMailboxId(mailboxId);
  const target = store.getWakeTarget(id);
  const handle = terminalHandle?.trim();
  if (!handle || target?.driver !== 'orca-terminal' || target.rotateAfterWaves === undefined) return id;
  return store.pinSessionMailbox(id, `terminal:${handle}`, {
    driver: 'orca-terminal',
    driverConfig: { ...target.driverConfig, terminalHandle: handle },
  });
}

class OrcaWakeDelivery {
  private readonly store: MailboxStore;
  private readonly runner: CommandRunner;
  private readonly command: string;
  private readonly idleCheck?: () => Promise<OrcaIdleResult>;
  private readonly now: () => number;
  private readonly report: (error: unknown) => void;

  constructor(context: WakeupDriverContext, options: OrcaWakeupDriverOptions) {
    this.store = context.store;
    this.now = context.now;
    this.report = context.reportError;
    this.runner = options.commandRunner ?? runCommand;
    this.command = options.orcaCommand ?? 'orca';
    this.idleCheck = options.idleCheck;
  }

  async run(context: WakeupDriverContext): Promise<WakeupDriverOutcome> {
    const { mailboxId, preview } = context;
    let target = readOrcaWakeTarget(context.target);
    if (
      !this.idleCheck &&
      target.driver === 'orca-terminal' &&
      target.workspacePath &&
      target.autoCreateWorkspace === true
    ) {
      const workspace = await ensureOrcaWorkspace(this.runner, target.orcaCommand ?? this.command, {
        workspacePath: target.workspacePath,
        repoRoot: target.repoRoot,
        comment: target.workspaceComment,
      });
      if (workspace.workspacePath !== target.workspacePath) {
        target = readOrcaWakeTarget(
          this.store.upsertWakeTarget(mailboxId, {
            driverConfig: { workspacePath: workspace.workspacePath },
          }),
        );
      }
    }
    const idle = await this.authoritativeIdle(mailboxId, target, preview);
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
        return { kind: 'disarm', reason: 'Workspace not found' };
      }
      return this.defer(mailboxId, target, idle.reason ?? 'Workspace is busy');
    }
    return await this.deliver(mailboxId, target, preview, idle.spawnFreshSession === true);
  }

  /**
   * Rotating a completed wave and delivering the first wave to an unbound workspace both open an
   * independent session tab; a live bound tab keeps receiving the rest of its own wave.
   */
  private async spawnsFreshSession(
    mailboxId: string,
    target: OrcaWakeTarget,
    preview: MailboxCheckResult,
    command: string,
  ): Promise<boolean> {
    if (target.driver !== 'orca-terminal' || !target.workspacePath) {
      return false;
    }
    const workspacePath = target.workspacePath;
    // Only the real runner consults the filesystem here; an injected runner or authoritative idle
    // check owns that decision. This mirrors `checkOrcaWorkspaceIdle`'s exact invariant, so a
    // missing workspace still falls through to the probe that disarms it.
    if (this.runner === runCommand && !this.idleCheck && !existsSync(workspacePath)) {
      return false;
    }
    if (mailboxId.startsWith('task:')) {
      if (!target.terminalHandle) return true;
      return (
        !this.idleCheck &&
        !(await isOrcaWorkspaceTerminalLive(this.runner, command, workspacePath, target.terminalHandle))
      );
    }
    if (target.rotateAfterWaves === undefined) return false;
    // Rotation always creates an empty tab, so the retained tab (draft or not) cannot gate it.
    if (needsSessionRotation(target, preview)) {
      return true;
    }
    // The first wave on an unbound workspace opens its own tab instead of hijacking a human's.
    if (!target.terminalHandle) {
      return true;
    }
    // A tab closed mid-wave or mid-window is replaced instead of falling back to another tab.
    return !(await isOrcaWorkspaceTerminalLive(this.runner, command, workspacePath, target.terminalHandle));
  }

  private async authoritativeIdle(
    mailboxId: string,
    target: OrcaWakeTarget,
    preview: MailboxCheckResult,
  ): Promise<WakeupIdleDecision> {
    const command = target.orcaCommand ?? this.command;
    const spawnFreshSession = await this.spawnsFreshSession(mailboxId, target, preview, command);
    const withPlan = (idle: OrcaIdleResult): WakeupIdleDecision =>
      spawnFreshSession ? { ...idle, spawnFreshSession: true } : idle;
    if (this.idleCheck) {
      return withPlan(await this.idleCheck());
    }
    if (target.driver === 'orca-run') {
      if (target.workspacePath) {
        return withPlan(await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath));
      }
      // Orchestration mail queues without touching a TUI composer, so no keystroke race exists.
      return withPlan({ idle: true });
    }
    if (target.workspacePath) {
      // A fresh tab is created empty, so its retained siblings must not block the wakeup; a bound
      // tab is the only composer that can receive the pointer.
      return withPlan(
        await checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, {
          targetTerminalHandle: spawnFreshSession ? null : target.terminalHandle,
          agent: target.agent,
        }),
      );
    }
    if (target.terminalHandle) {
      return withPlan(
        await probeOrcaTerminalIdle(this.runner, command, target.terminalHandle, target.workspacePath),
      );
    }
    return withPlan({
      idle: false,
      reason: 'Authoritative terminal idle check required',
    });
  }

  private defer(
    mailboxId: string,
    target: OrcaWakeTarget,
    reason: string,
  ): Extract<WakeupDriverOutcome, { kind: 'defer' }> {
    return { kind: 'defer', reason };
  }

  /**
   * Wait for a freshly opened tab to become the only composer this wakeup may write into. The
   * workspace probe ignores retained tabs (including an unsent human draft) and confirms the new
   * tab itself is idle before the pointer is written.
   */
  private async waitFreshTabIdle(
    target: OrcaWakeTarget,
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
      return checkOrcaWorkspaceIdle(this.runner, command, target.workspacePath, undefined, {
        targetTerminalHandle: terminalHandle,
        agent: target.agent,
      });
    }
    return probeOrcaTerminalIdle(this.runner, command, terminalHandle);
  }

  private async deliver(
    mailboxId: string,
    target: OrcaWakeTarget,
    preview: MailboxCheckResult,
    freshSession: boolean,
  ): Promise<WakeupDriverOutcome> {
    const command = target.orcaCommand ?? this.command;
    let terminalHandle = target.terminalHandle;
    const needsRotation = needsSessionRotation(target, preview);
    const spawnFreshSession = !needsRotation && freshSession;
    let rotated = false;
    const dedicatedTaskTab =
      mailboxId.startsWith('task:') && target.driver === 'orca-terminal' && Boolean(target.workspacePath);
    const replaceTaskTab =
      dedicatedTaskTab &&
      !(needsRotation || spawnFreshSession) &&
      (!terminalHandle ||
        (!this.idleCheck &&
          !(await isOrcaWorkspaceTerminalLive(this.runner, command, target.workspacePath!, terminalHandle))));

    if (needsRotation || spawnFreshSession || replaceTaskTab) {
      // Rotation never resets an existing TUI: it opens a new session tab and leaves every old
      // tab untouched. A missing workspace makes that impossible and must fail loudly; a
      // fresh-tab plan has already proven its workspace in `spawnsFreshSession`.
      if (!target.workspacePath) {
        throw new Error(`Orca session rotation requires a workspacePath: ${mailboxId}`);
      }
      const workspacePath = target.workspacePath;
      const spawned = await launchFencedOrcaWorkspaceTerminal(this.store, this.runner, mailboxId, {
        command,
        workspacePath,
        agent: target.agent,
        interactiveCommand: target.interactiveCommand,
        now: this.now(),
        resetCompletedWaves: needsRotation,
      });
      terminalHandle = spawned.handle;
      target = spawned.target;
      rotated = needsRotation;
      const idle = await this.waitFreshTabIdle(target, terminalHandle, command);
      if (!idle.idle) {
        const deferred = this.defer(
          mailboxId,
          target,
          needsRotation
            ? 'Session rotated; waiting for tui-idle before wakeup'
            : 'Terminal spawned; waiting for tui-idle before wakeup',
        );
        return needsRotation ? { ...deferred, rotated: true } : deferred;
      }
    } else if (
      !dedicatedTaskTab &&
      (!terminalHandle || !this.idleCheck) &&
      target.workspacePath &&
      target.driver === 'orca-terminal'
    ) {
      // Real Orca mode re-resolves before every send so a terminal closed since the last delivery
      // is rebound (or respawned) before the external boundary is crossed; `preferredHandle` keeps
      // a live binding without spawning a second terminal. Tests that inject an authoritative idle
      // check only resolve when no handle exists yet.
      const plan = await planOrcaWorkspaceTerminal(this.runner, command, target.workspacePath, undefined, {
        agent: target.agent,
        preferredHandle: target.terminalHandle,
      });
      if (plan.kind === 'existing') {
        terminalHandle = plan.handle;
        target = { ...target, terminalHandle };
      } else {
        const launched = await launchFencedOrcaWorkspaceTerminal(this.store, this.runner, mailboxId, {
          command,
          workspacePath: target.workspacePath,
          agent: target.agent,
          interactiveCommand: target.interactiveCommand,
          now: this.now(),
          ...(plan.kind === 'shell' ? { shellTerminalHandle: plan.handle } : {}),
        });
        terminalHandle = launched.handle;
        target = launched.target;
        if (plan.kind === 'shell') {
          return this.defer(mailboxId, target, 'Terminal spawned; waiting for tui-idle before wakeup');
        }
        const idle = this.idleCheck
          ? await this.idleCheck()
          : await this.waitFreshTabIdle(target, terminalHandle, command);
        if (!idle.idle)
          return this.defer(mailboxId, target, 'Terminal spawned; waiting for tui-idle before wakeup');
      }
    }

    const pointer = buildWakeupPointer(mailboxId, target, preview);
    if (!pointer) {
      this.store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview));
      return { kind: 'noop', reason: 'Nothing new to deliver' };
    }

    if (target.driver === 'orca-run' && !target.runId) throw new Error('orca-run wake target requires runId');
    if (target.driver === 'orca-terminal' && !terminalHandle)
      throw new Error('orca-terminal wake target requires terminalHandle or workspacePath');
    // The pointer is the only external effect left; fence it so a crash or an unconfirmed send
    // stays parked instead of being replayed by the next tick or restart.
    this.store.beginWakeDelivery(mailboxId);
    if (target.driver === 'orca-run') {
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
      await sendOrcaTerminalText(this.runner, command, terminalHandle!, pointer, {
        cwd: target.workspacePath,
      });
    }

    let warning: string | undefined;
    const comment =
      target.workspaceComment ??
      (mailboxId.startsWith('stream:') ? `Dealer: ${mailboxId} pending` : undefined);
    if (comment !== undefined) {
      try {
        await setOrcaWorkspaceComment(this.runner, command, target.workspacePath, comment);
      } catch (error) {
        // The pointer is already delivered; retrying it would duplicate a wakeup.
        warning = String(error);
        this.report(error);
      }
    }

    this.store.completeWakeDelivery(
      mailboxId,
      maxPreviewSequence(preview),
      terminalHandle ? { driverConfig: { terminalHandle } } : {},
      terminalHandle && target.rotateAfterWaves !== undefined
        ? {
            sessionMailboxId: `terminal:${terminalHandle}`,
            driver: 'orca-terminal',
            driverConfig: { ...target.driverConfig, terminalHandle },
          }
        : undefined,
    );
    return {
      kind: 'delivered',
      pointer,
      rotated,
      ...(warning !== undefined ? { warning } : {}),
    };
  }
}

export function createOrcaTerminalWakeupDriver(options: OrcaWakeupDriverOptions = {}): WakeupDriver {
  return {
    name: 'orca-terminal',
    deliver: (context) => new OrcaWakeDelivery(context, options).run(context),
    async resolveSessionTarget(mailboxId) {
      if (mailboxId.startsWith('terminal:') && mailboxId.length > 9) {
        let workspacePath: string | undefined;
        try {
          const terminal = await readOrcaTerminal(
            options.commandRunner ?? runCommand,
            options.orcaCommand ?? 'orca',
            mailboxId.slice(9),
          );
          if (typeof terminal?.worktreePath === 'string' && terminal.worktreePath.trim())
            workspacePath = terminal.worktreePath;
        } catch {
          // Optional address enrichment only: the delivery-time terminal probe still fails closed.
        }
        return {
          driver: 'orca-terminal',
          driverConfig: {
            terminalHandle: mailboxId.slice(9),
            orcaCommand: options.orcaCommand ?? 'orca',
            ...(workspacePath ? { workspacePath } : {}),
          },
        };
      }
      if (mailboxId.startsWith('worktree:') && mailboxId.length > 9) {
        const found = await findOrcaWorktreeByName(
          options.commandRunner ?? runCommand,
          options.orcaCommand ?? 'orca',
          mailboxId.slice(9),
        );
        if (found)
          return {
            driver: 'orca-terminal',
            driverConfig: {
              ...found,
              orcaCommand: options.orcaCommand ?? 'orca',
            },
          };
      }
      return undefined;
    },
  };
}

export function createOrcaRunWakeupDriver(options: OrcaWakeupDriverOptions = {}): WakeupDriver {
  return {
    name: 'orca-run',
    deliver: (context) => new OrcaWakeDelivery(context, options).run(context),
  };
}

export function createOrcaWakeupDrivers(options: OrcaWakeupDriverOptions = {}): readonly WakeupDriver[] {
  return [createOrcaTerminalWakeupDriver(options), createOrcaRunWakeupDriver(options)];
}
