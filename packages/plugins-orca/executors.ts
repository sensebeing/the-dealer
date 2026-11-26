import { resolveConfiguredOrcaWakeTargetPatch } from './wakeup.ts';
import { execFileSync } from 'node:child_process';
import { existsSync, realpathSync } from 'node:fs';
import { basename, dirname, isAbsolute, join, relative, resolve } from 'node:path';
import { homedir } from 'node:os';
import { randomUUID } from 'node:crypto';
import type { ExecutionContext, ExecutionReceipt, ExecutorPlugin, MessageEnvelope, AgentConversationMode } from '../core-types/index.ts';
import { normalizeConversationModeInput } from '../core-types/index.ts';
import {
  canInheritThreadMailbox,
  groupThreadBatch,
  hasExplicitExecutor,
  resolveBoundMailboxId,
  resolveMailboxId,
  resolveSessionOrThreadMailbox,
  resolveSpawnMailboxId,
  resolveTopicAddress,
} from '../mailbox/index.ts';
import type { MailboxStore, MailboxEnqueueResult, MailboxTopicBinding, WakeTargetState, WakeupRedriveService } from '../mailbox/index.ts';
import { setOrcaWorkspaceComment, orcaAgent, pickSeaCreatureName } from './workspace.ts';
import type { SupportedOrcaAgent } from './workspace.ts';
import { buildOrcaWorktreeSelector, normalizeOrcaWorktreeId, orcaMutationRunner } from './inventories.ts';
import {
  trustAntigravityWorkspace,
  defaultAgentLaunchCommand,
  defaultAgentCommandArgs,
} from '../plugins-agent-host/hosts.ts';
import {
  resolveOrcaManagedAutoCreateWorkspace,
  resolveOrcaWaveRotateAfterWaves,
  createOrcaTerminalWakeupDriver,
  deliverSpawnedOrcaTerminalWakeup,
  launchFencedOrcaWorkspaceTerminal,
} from './wakeup.ts';
import { checkedCommand, runCommand } from '../subprocess/index.ts';
import type { CommandRunner } from '../subprocess/index.ts';
import {
  renderWave,
  beforeExternalEffect,
  prepareExternalEffect,
  prepareExternalEffectAsync,
  rethrowBeforeCommandStart,
  executeDeliveryGroups,
  prepareInboxDeliveries,
  serializeMailboxInitialization,
  serializeThreadDeliveries,
} from '../plugins-executor/index.ts';
import type { PreparedDelivery } from '../plugins-executor/index.ts';

export interface OrcaTopicBinding extends MailboxTopicBinding {
  /** Addressing fallback; never an explicitly requested interactive conversation mode. */
  defaultMode?: AgentConversationMode;
  executor?: string;
  agent?: string;
  noParent?: boolean;
  projectId?: string;
  workspacePath?: string;
  repoRoot?: string;
  worktreeId?: string;
  runId?: string;
  terminalHandle?: string;
  interactiveCommand?: string;
  resumeCommand?: string;
  autoCreateWorkspace?: boolean;
  workspaceComment?: string;
  wakeupPrefix?: string;
  coalesceMs?: number;
  rotateAfterWaves?: number;
}

export interface OrcaSpawnOptions {
  resolveTopicBinding?: (kind: string, streamKey?: string) => OrcaTopicBinding | undefined;
  commandRunner?: CommandRunner;
  command?: string;
  args?: readonly string[];
  cwd?: string;
  mode?: AgentConversationMode;
  conversationMode?: AgentConversationMode;
  agent?: SupportedOrcaAgent;
  interactive?: boolean;
  namePrefix?: string;
  orcaCommand?: string;
  projectRoots?: Readonly<Record<string, string>>;
  parentWorktree?: string;
  noParent?: boolean;
  seaCreatureNaming?: boolean;
  agySettingsPath?: string;
  interactiveCommand?: string;
  resumeCommand?: string;
  /** Durable task mailbox ledger; always supplied by DealerNodeRuntime. */
  store?: MailboxStore;
  wakeup?: WakeupRedriveService;
}

const shellQuote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;

function gitCommonDir(directory: string): string | undefined {
  if (!existsSync(directory)) {
    return undefined;
  }
  let commonDir: string;
  try {
    commonDir = execFileSync(
      'git',
      ['-C', directory, 'rev-parse', '--path-format=absolute', '--git-common-dir'],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] },
    ).trim();
  } catch {
    return undefined;
  }
  return existsSync(commonDir) ? realpathSync(commonDir) : resolve(commonDir);
}

function assertParentInProject(projectId: string, boundRoot: string, parentWorktree: string): void {
  const parentPath = parentWorktree.startsWith('path:')
    ? parentWorktree.slice(5)
    : /^id:[^:]+::(.+)$/.exec(parentWorktree)?.[1];
  const reject = () =>
    beforeExternalEffect(new Error(`Parent worktree does not belong to project ${projectId}`));
  if (!parentPath?.trim()) {
    throw reject();
  }
  const resolvedRoot = resolve(boundRoot);
  const resolvedParent = resolve(parentPath);
  if (resolvedParent === resolvedRoot) {
    return;
  }
  const rootCommon = gitCommonDir(resolvedRoot);
  const parentCommon = gitCommonDir(resolvedParent);
  if (rootCommon !== undefined || parentCommon !== undefined) {
    if (rootCommon === undefined || rootCommon !== parentCommon) {
      throw reject();
    }
    return;
  }
  const rel = relative(resolvedRoot, resolvedParent);
  if (rel.startsWith('..') || isAbsolute(rel)) {
    throw reject();
  }
}

type OrcaExecutorOptions = OrcaSpawnOptions & OrcaWaveOptions;

type OrcaRoute = {
  envelope: MessageEnvelope;
  envelopes: MessageEnvelope[];
  binding?: OrcaTopicBinding;
  bindingKind: string;
  explicitMode?: AgentConversationMode;
  mode: AgentConversationMode;
};

function resolveOrcaRoutes(
  batch: readonly MessageEnvelope[], options: OrcaExecutorOptions, config: OrcaExecutorOptions,
  fallbackMode: AgentConversationMode,
): OrcaRoute[] {
  const resolveBinding = options.resolveTopicBinding ?? config.resolveTopicBinding;
  return prepareExternalEffect(() => groupThreadBatch(batch, options.store ?? config.store).map(envelopes => {
    const envelope = envelopes[0];
    const { payload, kind } = envelope;
    const streamKey = typeof payload?.streamKey === 'string' ? payload.streamKey : undefined;
    const binding = resolveBinding?.(kind, streamKey);
    const explicitMode = normalizeConversationModeInput(payload) ?? normalizeConversationModeInput(binding) ??
      normalizeConversationModeInput(config) ?? normalizeConversationModeInput(options);
    return {
      envelope, envelopes, binding, bindingKind: resolveTopicAddress(kind, streamKey).kind,
      explicitMode, mode: explicitMode ?? binding?.defaultMode ?? fallbackMode,
    };
  }));
}

function createOrcaSpawnPreparation(options: OrcaSpawnOptions) {
  const issuedNames = new Set<string>();
  return (
    batch: readonly MessageEnvelope[], route: OrcaRoute, mailboxId: string,
    context: ExecutionContext, config: OrcaSpawnOptions,
    custody?: { proposedSpawn: boolean; boundMailbox: boolean },
  ) => {
    const runner = options.commandRunner ?? config.commandRunner ?? runCommand;
    const { binding, explicitMode } = route;
    const payload = route.envelope.payload ?? {};
    const command = config.orcaCommand?.trim() || options.orcaCommand || 'orca';
    const interactive =
      payload.interactive === true ||
      explicitMode === 'spawn' ||
      binding?.executor === 'orca' ||
      custody?.boundMailbox === true ||
      config.interactive === true ||
      options.interactive === true;
    const store = options.store ?? config.store;
    const effectiveAgent = orcaAgent(payload.agent ?? binding?.agent ?? config.agent ?? options.agent);
    const wakeup = options.wakeup ?? config.wakeup;
    const configuredCwd =
      binding?.workspacePath ??
      binding?.repoRoot ??
      (typeof config.cwd === 'string' && config.cwd ? config.cwd : undefined) ??
      options.cwd;
    const projectId =
      typeof payload.projectId === 'string' && payload.projectId.trim()
        ? payload.projectId.trim()
        : binding?.projectId;
    const projectRoots = { ...(options.projectRoots ?? {}), ...(config.projectRoots ?? {}) };
    const boundRoot =
      projectId && typeof projectRoots[projectId] === 'string'
        ? projectRoots[projectId].trim() || undefined
        : undefined;
    if (projectId && !boundRoot) {
      throw beforeExternalEffect(new Error(`Unbound project on receiver: ${projectId}`));
    }
    const defaultCwd = boundRoot ?? configuredCwd;
    let parentWorktree: string | undefined =
      (payload.noParent ?? binding?.noParent) === true
        ? undefined
        : defaultCwd
          ? `path:${defaultCwd}`
          : 'active';
    for (const hierarchy of [config, options]) {
      if (hierarchy.noParent === true || hierarchy.parentWorktree === 'none') {
        parentWorktree = undefined;
        break;
      }
      if (typeof hierarchy.parentWorktree === 'string' && hierarchy.parentWorktree) {
        parentWorktree = hierarchy.parentWorktree;
        break;
      }
    }
    if (projectId && boundRoot && parentWorktree !== undefined) {
      assertParentInProject(projectId, boundRoot, parentWorktree);
    }
    const worktreeCwd =
      boundRoot ??
      configuredCwd ??
      (parentWorktree?.startsWith('path:') ? parentWorktree.slice(5) : undefined);
    if (parentWorktree === undefined && !worktreeCwd) {
      throw beforeExternalEffect(
        new Error('Top-level Orca worktree requires receiver cwd or bound projectId'),
      );
    }
    if (interactive && !store) {
      throw beforeExternalEffect(new Error('Interactive orca-spawn requires a durable mailbox store'));
    }
    const explicitName =
      typeof payload.worktreeName === 'string' && payload.worktreeName.trim()
        ? payload.worktreeName.trim()
        : undefined;
    const seaCreatureNaming =
      typeof payload.seaCreatureNaming === 'boolean'
        ? payload.seaCreatureNaming
        : typeof config.seaCreatureNaming === 'boolean'
          ? config.seaCreatureNaming
          : options.seaCreatureNaming === true;
    const namePrefix = config.namePrefix?.trim() || options.namePrefix?.trim() || 'dealer';
    return async (commit: () => void): Promise<ExecutionReceipt> => {
      const name = explicitName
        ? explicitName
        : seaCreatureNaming
          ? await pickSeaCreatureName({
              runner,
              command,
              cwd: worktreeCwd,
              issuedNames,
              signal: context.signal,
            })
          : `${namePrefix}-${randomUUID()}`;
      const createArgs = parentWorktree
        ? ['worktree', 'create', '--parent-worktree', parentWorktree, '--name', name, '--json']
        : ['worktree', 'create', '--no-parent', '--name', name, '--json'];
      const created = await checkedCommand(orcaMutationRunner(runner), {
        command,
        args: createArgs,
        cwd: worktreeCwd,
        signal: context.signal,
      }).catch(rethrowBeforeCommandStart);
      const raw = JSON.parse(created.stdout);
      if (raw.ok === false) {
        throw new Error('Orca worktree creation failed');
      }
      const workspace = (raw.result ?? raw) as {
        path?: string;
        worktreeId?: string;
        worktree?: { path?: string; worktreeId?: string; id?: string };
      };
      const path = workspace.path ?? workspace.worktree?.path;
      if (!path) {
        throw new Error('Orca worktree create returned no path');
      }
      const worktreeIdCandidate = workspace.worktreeId ?? workspace.worktree?.worktreeId ?? workspace.worktree?.id;
      const worktreeId = typeof worktreeIdCandidate === 'string'
        ? normalizeOrcaWorktreeId(worktreeIdCandidate)
        : undefined;
      issuedNames.add(basename(path).trim().toLowerCase());
      if (interactive && store) {
        const explicitSettingsPath =
          typeof config.agySettingsPath === 'string' && config.agySettingsPath
            ? config.agySettingsPath
            : undefined;
        const optionSettingsPath =
          typeof options.agySettingsPath === 'string' && options.agySettingsPath
            ? options.agySettingsPath
            : undefined;
        const settingsFile =
          explicitSettingsPath ??
          optionSettingsPath ??
          join(homedir(), '.gemini/antigravity-cli/settings.json');
        const mayTrust =
          !options.commandRunner ||
          options.commandRunner === runCommand ||
          explicitSettingsPath !== undefined ||
          optionSettingsPath !== undefined;
        if (
          mayTrust &&
          effectiveAgent === 'antigravity' &&
          payload.trustAntigravity !== false &&
          (existsSync(settingsFile) || explicitSettingsPath !== undefined || optionSettingsPath !== undefined)
        ) {
          trustAntigravityWorkspace(path, settingsFile);
        }
        const interactiveCommand =
          binding?.interactiveCommand?.trim() ||
          config.interactiveCommand?.trim() ||
          options.interactiveCommand?.trim() ||
          defaultAgentLaunchCommand(effectiveAgent);
        const resumeCommand = binding?.resumeCommand ?? config.resumeCommand ?? options.resumeCommand;
        const workspaceComment =
          typeof payload.workspaceComment === 'string' && payload.workspaceComment.trim()
            ? payload.workspaceComment
            : undefined;
        const claim = wakeup?.claim(mailboxId);
        let enqueued: MailboxEnqueueResult | undefined;
        const receipt = { executor: 'orca-spawn', executionId: randomUUID(), mailboxId, worktreePath: path };
        try {
          // Once committed, the mailbox owns recovery even if Orca cannot launch the agent.
          enqueued = store.enqueue(batch, {
            ...(context?.signedEnvelopes ? { signedEnvelopes: context.signedEnvelopes } : {}),
            mailboxId, proposedSpawn: custody?.proposedSpawn,
            wakeTarget: {
              driver: 'orca-terminal',
              coalesceMs: 0,
              driverConfig: {
                agent: effectiveAgent,
                interactiveCommand, resumeCommand,
                orcaCommand: command,
                workspacePath: path,
                ...(worktreeId ? { worktreeId } : {}),
                dedicatedSpawn: true,
                ...(workspaceComment ? { workspaceComment } : {}),
              },
            },
          });
          commit();
          if (enqueued.claimedSpawn === false || !enqueued.mailboxIds.includes(mailboxId)) {
            return { ...enqueued, executor: receipt.executor, executionId: receipt.executionId,
              ...await flushOrcaMailboxes(wakeup, enqueued.mailboxIds) };
          }
          let terminal;
          try {
            const launched = await launchFencedOrcaWorkspaceTerminal(store, runner, mailboxId, {
              command,
              workspacePath: path,
              worktreeId,
              agent: effectiveAgent,
              interactiveCommand, resumeCommand,
              signal: context.signal,
            });
            if (!launched.committed) return { ...receipt, ...enqueued, woke: false, reason: 'Wake launch superseded by a newer binding' };
            terminal = launched.raw;
          } catch (launchError) {
            if (workspaceComment) {
              try {
                await setOrcaWorkspaceComment(runner, command, path, workspaceComment, {
                  cwd: path,
                  worktreeId,
                  signal: context.signal,
                });
              } catch {
                // The durable target retains the comment for a later reconciled wake.
              }
            }
            return {
              ...receipt, ...enqueued,
              woke: false,
              reason: String(launchError),
            };
          }
          let warning: string | undefined;
          if (workspaceComment) {
            try {
              await setOrcaWorkspaceComment(runner, command, path, workspaceComment, {
                cwd: path,
                worktreeId,
                signal: context.signal,
              });
            } catch (error) {
              warning = String(error);
            }
          }
          const launchReceipt = {
            ...receipt, ...enqueued, terminal,
            ...(warning !== undefined ? { warning } : {}),
          };
          try {
            const delivered = await deliverSpawnedOrcaTerminalWakeup(store, runner, mailboxId, {
              signal: context.signal,
            });
            return { ...launchReceipt, woke: delivered.sent };
          } catch (error) {
            return { ...launchReceipt, woke: false, reason: String(error) };
          }
        } catch (error) {
          if (!enqueued) throw error;
          // A persistence failure after enqueue must not undo the durable task handoff.
          return {
            ...receipt, ...enqueued,
            woke: false,
            reason: String(error),
          };
        } finally {
          claim?.dispose();
          wakeup?.notifyMailbox(mailboxId);
        }
      }
      const agent = options.command ?? 'codex';
      // Orca terminal's --command is a shell command; quote every untrusted argument.
      const agentCommand = [
        agent,
        ...(options.args ?? defaultAgentCommandArgs(agent, 'prompt')),
        renderWave(batch),
      ]
        .map(shellQuote)
        .join(' ');
      const launched = await checkedCommand(orcaMutationRunner(runner, path, worktreeId), {
        command,
        args: ['terminal', 'create', '--worktree', buildOrcaWorktreeSelector(path, worktreeId), '--command', agentCommand, '--json'],
        cwd: path,
        signal: context.signal,
      });
      const terminal = JSON.parse(launched.stdout);
      if (terminal.ok === false) {
        throw new Error('Orca terminal launch failed');
      }
      return {
        executor: 'orca-spawn',
        executionId: randomUUID(),
        worktreePath: path,
        terminal,
      };
    };
  };
}

async function prepareOrcaSpawnBatch(
  routes: readonly OrcaRoute[], context: ExecutionContext, options: OrcaSpawnOptions, config: OrcaSpawnOptions,
  prepareSpawn: ReturnType<typeof createOrcaSpawnPreparation>,
): Promise<{ sessions: PreparedDelivery[]; launches: PreparedDelivery[] }> {
  const sessions: PreparedDelivery[] = [];
  if (!routes.length) return { sessions, launches: [] };
  const store = options.store ?? config.store;
  const wakeup = options.wakeup ?? config.wakeup;
  const command = config.orcaCommand?.trim() || options.orcaCommand || 'orca';
  const runner = options.commandRunner ?? config.commandRunner ?? runCommand;
  const prepareSession = async (store: MailboxStore, mailboxId: string, envelopes: MessageEnvelope[]) => {
    const wakeTarget = await resolveOrcaSessionWakeTarget(store, wakeup, runner, command, mailboxId);
    return prepareOrcaMailboxDelivery(store, wakeup, context, mailboxId, envelopes, wakeTarget);
  };
  const { existing, launches } = prepareExternalEffect(() => {
    if (!store) {
      const route = routes[0];
      const envelopes = routes.flatMap(item => item.envelopes);
      return { existing: new Map<string, MessageEnvelope[]>(), launches: [{
        envelopes,
        deliver: prepareSpawn(envelopes, route, resolveSpawnMailboxId(route.envelope.id), context, config),
      }] };
    }
    const launches: PreparedDelivery[] = [];
    const existing = new Map<string, MessageEnvelope[]>();
    for (const route of routes) {
      const { envelope, envelopes, binding, bindingKind, mode } = route;
      const threadLocal = resolveOrcaThreadMailbox(store, envelope);
      const sessionMailbox = hasExplicitExecutor(envelope)
        ? undefined : resolveSessionOrThreadMailbox(envelope, threadLocal);
      const boundMailboxId = resolveBoundMailboxId(bindingKind, { ...binding, mode, conversationMode: undefined }, mode);
      const destination = sessionMailbox ?? boundMailboxId;
      const boundMailbox = destination === boundMailboxId && boundMailboxId !== 'default';
      if ((sessionMailbox !== undefined && !boundMailbox) ||
          (boundMailbox && shouldPreserveWakeTarget(store.getWakeTarget(destination)))) {
        const group = existing.get(destination) ?? [];
        group.push(...envelopes);
        existing.set(destination, group);
        continue;
      }
      const mailboxId = boundMailbox ? destination : resolveSpawnMailboxId(envelope.id);
      const launch = prepareSpawn(envelopes, route, mailboxId, context, config, {
        boundMailbox,
        proposedSpawn: canInheritThreadMailbox(envelope) && !(boundMailbox && threadLocal === destination),
      });
      launches.push({ envelopes, deliver: boundMailbox ? commit =>
        serializeMailboxInitialization(store, mailboxId, async release => {
          const committed = () => { release(); commit(); };
          if (prepareExternalEffect(() => shouldPreserveWakeTarget(store.getWakeTarget(mailboxId)))) {
            const session = await prepareSession(store, mailboxId, envelopes);
            return session.deliver(committed);
          }
          return launch(committed);
        }) : launch });
    }
    return { existing, launches };
  });
  if (store) {
    for (const [mailboxId, envelopes] of existing) {
      sessions.push(await prepareSession(store, mailboxId, envelopes));
    }
  }
  return { sessions, launches };
}

export function createOrcaSpawnExecutorPlugin(options: OrcaSpawnOptions = {}): ExecutorPlugin {
  const prepareSpawn = createOrcaSpawnPreparation(options);
  return {
    name: 'orca-spawn',
    async execute(batch, context, runtimeConfig) {
      if (!batch.length) throw beforeExternalEffect(new Error('Empty spawn batch'));
      const config = (runtimeConfig ?? {}) as OrcaSpawnOptions;
      const store = options.store ?? config.store;
      return serializeThreadDeliveries(store, batch, context, async commit => {
        const routes = resolveOrcaRoutes(batch, options, config, 'spawn');
        const { sessions, launches } = await prepareOrcaSpawnBatch(routes, context, options, config, prepareSpawn);
        return executeDeliveryGroups('orca-spawn', batch, [...sessions, ...launches], commit);
      });
    },
  };
}

export interface OrcaWaveOptions {
  agent?: SupportedOrcaAgent;
  interactiveCommand?: string;
  resumeCommand?: string;
  resolveTopicBinding?: (kind: string, streamKey?: string) => OrcaTopicBinding | undefined;
  runId?: string;
  terminalHandle?: string;
  worktreeId?: string;
  cwd?: string;
  orcaCommand?: string;
  commandRunner?: CommandRunner;
  /** Durable mailbox ledger; always supplied by DealerNodeRuntime. */
  store?: MailboxStore;
  wakeup?: WakeupRedriveService;
  /** Default coalescing/rotation settings; receiver binding values take precedence. */
  wave?: { minAgeMs?: number; rotateAfterWaves?: number };
}

function optionalString(value: unknown): string | undefined {
  return typeof value === 'string' && value ? value : undefined;
}

function optionalPositiveInteger(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : undefined;
}

function shouldPreserveWakeTarget(target: WakeTargetState | undefined): boolean {
  return target !== undefined && (target.driver !== 'passive' || target.deliveryInDoubt === true);
}

function resolveOrcaSessionWakeTarget(
  store: MailboxStore, wakeup: WakeupRedriveService | undefined, runner: CommandRunner, command: string, mailboxId: string,
) {
  return prepareExternalEffectAsync(async () => {
    if (shouldPreserveWakeTarget(store.getWakeTarget(mailboxId))) return undefined;
    if (wakeup) return wakeup.resolveSessionWakeTarget(mailboxId);
    return createOrcaTerminalWakeupDriver({ commandRunner: runner, orcaCommand: command })
      .resolveSessionTarget?.(mailboxId, store);
  });
}

function resolveOrcaThreadMailbox(store: MailboxStore, envelope: MessageEnvelope): string | undefined {
  return store.resolveThreadLocalMailbox(envelope.semantic?.threadId, envelope.networkId, {
    excludeInboundMessageId: envelope.id,
  });
}

async function flushOrcaMailboxes(wakeup: WakeupRedriveService | undefined, mailboxIds: readonly string[]) {
  if (!wakeup) return { woke: false };
  const outcomes = await Promise.all(mailboxIds.map(mailboxId =>
    wakeup.flushMailbox(mailboxId).catch(error => ({ woke: false, reason: String(error) })),
  ));
  const reason = outcomes.find(outcome => outcome.reason !== undefined)?.reason;
  return { woke: outcomes.some(outcome => outcome.woke), ...(reason !== undefined ? { reason } : {}) };
}

function prepareOrcaMailboxDelivery(
  store: MailboxStore, wakeup: WakeupRedriveService | undefined, context: ExecutionContext,
  mailboxId: string, envelopes: MessageEnvelope[], wakeTarget?: Partial<WakeTargetState>,
): PreparedDelivery {
  return { envelopes, async deliver(commit) {
    const enqueued = store.enqueue(envelopes, {
      signedEnvelopes: context?.signedEnvelopes, mailboxId, ...(wakeTarget ? { wakeTarget } : {}),
    });
    commit();
    return { ...enqueued, executor: 'orca-wave', executionId: randomUUID(),
      ...(enqueued.mailboxIds.length === 1 ? { mailboxId: enqueued.mailboxIds[0] } : {}),
      ...await flushOrcaMailboxes(wakeup, enqueued.mailboxIds) };
  } };
}

async function prepareOrcaWaveBatch(
  routes: readonly OrcaRoute[], context: ExecutionContext, options: OrcaWaveOptions, config: OrcaExecutorOptions,
): Promise<PreparedDelivery[]> {
  if (!routes.length) return [];
  const store = options.store ?? config.store;
  if (!store) throw beforeExternalEffect(new Error('orca-wave requires a durable mailbox store'));
  const wakeup = options.wakeup ?? config.wakeup;
  const command = options.orcaCommand ?? 'orca';
  const runner = options.commandRunner ?? config.commandRunner ?? runCommand;
  const waveConfig = config.wave ?? options.wave;

  /**
   * Receiver-owned workspace, terminal and scheduling configuration for the topic's own bound
   * mailbox. Inbound payload values are only a fallback when the deployment has no local
   * binding or executor config, so a sender can never rebind runtime-managed workspace state.
   */
  const topicWakeTarget = (
    envelopes: readonly MessageEnvelope[],
    route: OrcaRoute,
    mailboxId: string,
  ): Partial<WakeTargetState> => {
    const { mode, binding, envelope } = route;
    const payload = envelope.payload ?? {};
    const receiverCwd = binding?.workspacePath ?? config.cwd ?? options.cwd;
    const receiverRunId = binding?.runId ?? config.runId ?? options.runId;
    const receiverTerminalHandle =
      binding?.terminalHandle ?? config.terminalHandle ?? options.terminalHandle;
    const hasReceiverTarget = Boolean(receiverCwd || receiverRunId || receiverTerminalHandle);
    const cwd =
      receiverCwd ??
      (hasReceiverTarget
        ? undefined
        : (optionalString(payload.workspacePath) ?? optionalString(payload.cwd)));
    const runId = receiverRunId ?? (hasReceiverTarget ? undefined : optionalString(payload.runId));
    const terminalHandle =
      receiverTerminalHandle ?? (hasReceiverTarget ? undefined : optionalString(payload.terminalHandle));
    const coalesceMs = Math.max(0, binding?.coalesceMs ?? waveConfig?.minAgeMs ?? 0);
    const rotateAfterWaves = resolveOrcaWaveRotateAfterWaves({
      mode: mode === 'fixed' ? 'fixed' : 'wave',
      bindingRotateAfterWaves: binding?.rotateAfterWaves,
      waveRotateAfterWaves: optionalPositiveInteger(waveConfig?.rotateAfterWaves),
      fallbackRotateAfterWaves: hasReceiverTarget
        ? undefined
        : optionalPositiveInteger(payload.rotateAfterWaves),
      managesWorkspace: Boolean(cwd),
      runId,
      pinnedTerminalHandle: receiverTerminalHandle,
    });
    const worktreeId =
      binding?.worktreeId ??
      config.worktreeId ??
      options.worktreeId ??
      (hasReceiverTarget ? undefined : optionalString(payload.worktreeId));
    const workspaceComment = String(
      envelopes.at(-1)?.payload?.workspaceComment ||
        binding?.workspaceComment ||
        `Dealer: ${mailboxId} pending`,
    );
    const explicitAgent = binding?.agent ?? config.agent ?? options.agent ?? (hasReceiverTarget ? undefined : payload.agent);
    const autoCreateWorkspace = resolveOrcaManagedAutoCreateWorkspace({
      explicitAutoCreate: binding?.autoCreateWorkspace,
      managesWorkspace: Boolean(receiverCwd),
      hasRepoRoot: Boolean(binding?.repoRoot),
      runId: receiverRunId,
      pinnedTerminalHandle: receiverTerminalHandle,
    });
    const identityPatch = resolveConfiguredOrcaWakeTargetPatch(store.getWakeTarget(mailboxId), {
      mailboxId, runId, workspacePath: cwd, worktreeId, configuredTerminalHandle: terminalHandle,
      configuredAgent: explicitAgent !== undefined ? orcaAgent(explicitAgent) : undefined,
      configuredSessionId: binding?.sessionId,
      configuredInteractiveCommand: binding?.interactiveCommand ?? config.interactiveCommand ?? options.interactiveCommand,
      configuredResumeCommand: binding?.resumeCommand ?? config.resumeCommand ?? options.resumeCommand,
      hasTopicBinding: binding !== undefined, authoritativeConfig: false,
    });
    return {
      ...identityPatch,
      coalesceMs,
      ...(binding?.wakeupPrefix ? { wakeupPrefix: binding.wakeupPrefix } : {}),
      ...(mode === 'fixed'
        ? { rotateAfterWaves: undefined }
        : rotateAfterWaves !== undefined
          ? { rotateAfterWaves }
          : {}),
      driverConfig: {
        ...identityPatch.driverConfig,
        orcaCommand: command,
        ...(binding?.repoRoot ? { repoRoot: binding.repoRoot } : {}),
        autoCreateWorkspace,
        ...(runId ? { runId } : {}),
        ...(cwd ? { workspacePath: cwd } : {}),
        workspaceComment,
      },
    };
  };

  const groups = prepareExternalEffect(() => {
    const byTarget = new Map<string, {
      mailboxId: string; envelopes: MessageEnvelope[]; topic?: OrcaRoute;
    }>();
    for (const route of routes) {
      const { envelope, binding, bindingKind, mode } = route;
      const mailboxId = resolveMailboxId(envelope, binding, resolveOrcaThreadMailbox(store, envelope), 'wave');
      const boundMailboxId = binding ? resolveBoundMailboxId(bindingKind, binding, 'wave') : undefined;
      // A session or thread retains its wake target instead of inheriting topic configuration.
      const isSession = mailboxId.startsWith('worktree:') || mailboxId.startsWith('terminal:') || mailboxId.startsWith('task:');
      const topic = (binding ? mailboxId === boundMailboxId : !isSession) ? route : undefined;
      const key = JSON.stringify([mailboxId, topic ? bindingKind : null, topic ? mode : null]);
      const group = byTarget.get(key) ?? { mailboxId, envelopes: [], topic };
      group.envelopes.push(...route.envelopes);
      byTarget.set(key, group);
    }
    return byTarget.values();
  });
  const prepared: PreparedDelivery[] = [];
  for (const { mailboxId, envelopes, topic } of groups) {
    const wakeTarget = topic
      ? prepareExternalEffect(() => topicWakeTarget(envelopes, topic, mailboxId))
      : await resolveOrcaSessionWakeTarget(store, wakeup, runner, command, mailboxId);
    prepared.push(prepareOrcaMailboxDelivery(store, wakeup, context, mailboxId, envelopes, wakeTarget));
  }
  return prepared;
}

/** Durable wave ingestion precedes wakeup, so a busy workspace can resume from pendingWake. */
export function createOrcaWaveExecutorPlugin(options: OrcaWaveOptions = {}): ExecutorPlugin {
  return {
    name: 'orca-wave',
    async execute(batch, context, runtimeConfig) {
      if (!batch.length) throw beforeExternalEffect(new Error('Empty wave batch'));
      const config = (runtimeConfig ?? {}) as OrcaExecutorOptions;
      const store = options.store ?? config.store;
      return serializeThreadDeliveries(store, batch, context, async commit => {
        const routes = resolveOrcaRoutes(batch, options, config, 'wave');
        const groups = await prepareOrcaWaveBatch(routes, context, options, config);
        return executeDeliveryGroups('orca-wave', batch, groups, commit);
      });
    },
  };
}

/** The unified executor prepares each mode before any group takes custody. */
export function createOrcaExecutorPlugin(options: OrcaExecutorOptions = {}): ExecutorPlugin {
  const prepareSpawn = createOrcaSpawnPreparation(options);
  return {
    name: 'orca',
    async execute(batch, context, runtimeConfig) {
      if (!batch.length) throw beforeExternalEffect(new Error('Empty Orca batch'));
      const config = (runtimeConfig ?? {}) as OrcaExecutorOptions;
      const store = options.store ?? config.store;
      return serializeThreadDeliveries(store, batch, context, async commit => {
        const routes = resolveOrcaRoutes(batch, options, config, 'spawn');
        const inboxRoutes = routes.filter(route => route.mode === 'inbox-only');
        const spawnRoutes = routes.filter(route => route.mode === 'spawn');
        const waveRoutes = routes.filter(route => route.mode === 'wave' || route.mode === 'fixed');
        const inbox = [];
        if (inboxRoutes.length) {
          if (!store) throw beforeExternalEffect(new Error('Inbox executor requires store or inboxPath'));
          const groups = prepareExternalEffect(() => {
            const byMailbox = new Map<string, MessageEnvelope[]>();
            for (const { envelope, envelopes, binding } of inboxRoutes) {
              const mailboxId = resolveMailboxId(envelope, binding, resolveOrcaThreadMailbox(store, envelope), 'inbox-only');
              const group = byMailbox.get(mailboxId) ?? [];
              group.push(...envelopes);
              byMailbox.set(mailboxId, group);
            }
            return byMailbox;
          });
          inbox.push(...await prepareInboxDeliveries(store, options.wakeup ?? config.wakeup, groups, context));
        }
        const waves = await prepareOrcaWaveBatch(waveRoutes, context, options, config);
        const { sessions, launches } = await prepareOrcaSpawnBatch(spawnRoutes, context, options, config, prepareSpawn);
        return executeDeliveryGroups('orca', batch, [...inbox, ...waves, ...sessions, ...launches], commit);
      });
    },
  };
}
