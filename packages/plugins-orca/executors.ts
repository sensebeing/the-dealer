import { resolveConfiguredOrcaWakeTargetPatch } from './wakeup.ts';
import { execFileSync } from 'node:child_process';
import { existsSync, realpathSync } from 'node:fs';
import { basename, dirname, isAbsolute, join, relative, resolve } from 'node:path';
import { homedir } from 'node:os';
import { randomUUID } from 'node:crypto';
import type { ExecutionContext, ExecutionReceipt, ExecutorPlugin, MessageEnvelope, AgentConversationMode } from '../core-types/index.ts';
import { normalizeConversationModeInput } from '../core-types/index.ts';
import {
  MailboxStore,
  groupSpawnBatch,
  resolveBoundMailboxId,
  resolveMailboxId,
  resolveSessionOrThreadMailbox,
  resolveSpawnMailboxId,
  resolveTopicAddress,
  resolveEffectiveConversationMode,
} from '../mailbox/index.ts';
import type { MailboxEnqueueResult, MailboxTopicBinding, WakeTargetState, WakeupRedriveService } from '../mailbox/index.ts';
import { setOrcaWorkspaceComment, orcaAgent, pickSeaCreatureName } from './workspace.ts';
import type { SupportedOrcaAgent } from './workspace.ts';
import {
  trustAntigravityWorkspace,
  defaultAgentLaunchCommand,
  defaultAgentCommandArgs,
} from '../plugins-agent-host/hosts.ts';
import {
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
  createInboxExecutorPlugin,
} from '../plugins-executor/index.ts';

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

export function createOrcaSpawnExecutorPlugin(options: OrcaSpawnOptions = {}): ExecutorPlugin {
  const issuedNames = new Set<string>();
  const prepareSpawn = (batch: readonly MessageEnvelope[], context: ExecutionContext, runtimeConfig: unknown) => {
    const config = (runtimeConfig ?? {}) as Partial<OrcaSpawnOptions>;
    const runner = options.commandRunner ?? config.commandRunner ?? runCommand;
    if (!batch[0]) throw beforeExternalEffect(new Error('Empty spawn batch'));
    const payload = (batch[0]?.payload ?? {}) as Record<string, unknown>;
    const command = config.orcaCommand?.trim() || options.orcaCommand || 'orca';
    const binding = (options.resolveTopicBinding ?? config.resolveTopicBinding)?.(
      batch[0]?.kind,
      typeof batch[0]?.payload?.streamKey === 'string' ? batch[0].payload.streamKey : undefined,
    );
    const explicitMode = prepareExternalEffect(
      () => normalizeConversationModeInput(payload) ?? normalizeConversationModeInput(binding) ??
        normalizeConversationModeInput(config) ?? normalizeConversationModeInput(options),
    );
    const interactive =
      payload.interactive === true ||
      explicitMode === 'spawn' ||
      binding?.executor === 'orca' ||
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
    const mailboxId = resolveSpawnMailboxId(batch[0].id);
    return async (): Promise<ExecutionReceipt> => {
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
      const created = await checkedCommand(runner, {
        command,
        args: createArgs,
        cwd: worktreeCwd,
        signal: context.signal,
      }).catch(rethrowBeforeCommandStart);
      const raw = JSON.parse(created.stdout);
      if (raw.ok === false) {
        throw new Error('Orca worktree creation failed');
      }
      const workspace = (raw.result ?? raw) as { path?: string; worktree?: { path?: string } };
      const path = workspace.path ?? workspace.worktree?.path;
      if (!path) {
        throw new Error('Orca worktree create returned no path');
      }
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
            mailboxId,
            wakeTarget: {
              driver: 'orca-terminal',
              coalesceMs: 0,
              driverConfig: {
                agent: effectiveAgent,
                interactiveCommand, resumeCommand,
                orcaCommand: command,
                workspacePath: path,
                ...(workspaceComment ? { workspaceComment } : {}),
              },
            },
          });
          let terminal;
          try {
            const launched = await launchFencedOrcaWorkspaceTerminal(store, runner, mailboxId, {
              command,
              workspacePath: path,
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
      const launched = await checkedCommand(runner, {
        command,
        args: ['terminal', 'create', '--worktree', `path:${path}`, '--command', agentCommand, '--json'],
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
  return {
    name: 'orca-spawn',
    async execute(batch, context, runtimeConfig) {
      if (!batch.length) throw beforeExternalEffect(new Error('Empty spawn batch'));
      const config = (runtimeConfig ?? {}) as Partial<OrcaSpawnOptions>;
      const store = options.store ?? config.store;
      if (!store) return prepareExternalEffect(() => prepareSpawn(batch, context, runtimeConfig))();

      const spawnBatch: MessageEnvelope[] = [];
      const sessions = new Map<string, MessageEnvelope[]>();
      const resolveTopicBinding = options.resolveTopicBinding ?? config.resolveTopicBinding;
      for (const envelope of batch) {
        const payload = envelope.payload;
        const mailboxId = prepareExternalEffect(() => {
          if (optionalString(payload?.executor)?.trim()) return undefined;
          const sessionMailbox = resolveSessionOrThreadMailbox(envelope, store.resolveThreadLocalMailbox(
            envelope.semantic?.threadId, envelope.networkId, { excludeInboundMessageId: envelope.id },
          ));
          if (sessionMailbox === undefined) return undefined;
          const streamKey = typeof payload?.streamKey === 'string' ? payload.streamKey : undefined;
          const binding = resolveTopicBinding?.(envelope.kind, streamKey);
          const boundMailboxId = binding
            ? resolveBoundMailboxId(resolveTopicAddress(envelope.kind, streamKey).kind, binding, binding.defaultMode ?? 'spawn')
            : 'default';
          return sessionMailbox === boundMailboxId ? undefined : sessionMailbox;
        });
        if (mailboxId === undefined) {
          spawnBatch.push(envelope);
        } else {
          const group = sessions.get(mailboxId) ?? [];
          group.push(envelope);
          sessions.set(mailboxId, group);
        }
      }
      const launches = prepareExternalEffect(() =>
        groupSpawnBatch(spawnBatch).map(({ envelopes }) => prepareSpawn(envelopes, context, runtimeConfig)),
      );

      const wakeup = options.wakeup ?? config.wakeup;
      const command = config.orcaCommand?.trim() || options.orcaCommand || 'orca';
      const runner = options.commandRunner ?? config.commandRunner ?? runCommand;
      const prepared = [];
      for (const [mailboxId, envelopes] of sessions) {
        const wakeTarget = await resolveOrcaSessionWakeTarget(store, wakeup, runner, command, mailboxId);
        prepared.push({ mailboxId, envelopes, wakeTarget });
      }
      // Prepare every launch and wake target before committing any mailbox or launching Orca.
      const spawned: ExecutionReceipt[] = [];
      for (const launch of launches) {
        try {
          spawned.push(await launch());
        } catch (error) {
          if (!spawned.length) throw error;
          throw new Error(`Orca spawn batch failed after an earlier launch: ${String(error)}`, { cause: error });
        }
      }
      if (spawned.length === 1 && prepared.length === 0) return spawned[0];
      const delivered = await deliverOrcaMailboxes(store, wakeup, context, prepared);
      const envelopeMailboxes = Object.fromEntries([
        ...spawned.flatMap(receipt => Object.entries(receipt.envelopeMailboxes ?? {})),
        ...Object.entries(delivered.envelopeMailboxes),
      ]);
      const reason = spawned.find(receipt => receipt.reason !== undefined)?.reason ?? delivered.reason;
      return {
        ...delivered,
        ...spawned[0],
        executor: 'orca-spawn',
        executionId: spawned[0]?.executionId ?? randomUUID(),
        mailboxIds: [...new Set(Object.values(envelopeMailboxes))],
        envelopeMailboxes,
        inserted: [...spawned.flatMap(receipt => Array.isArray(receipt.inserted) ? receipt.inserted : []), ...delivered.inserted],
        woke: delivered.woke || spawned.some(receipt => receipt.woke === true),
        ...(reason !== undefined ? { reason } : {}),
      };
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

function resolveOrcaSessionWakeTarget(
  store: MailboxStore, wakeup: WakeupRedriveService | undefined, runner: CommandRunner, command: string, mailboxId: string,
) {
  return prepareExternalEffectAsync(async () => {
    if (wakeup) return wakeup.resolveSessionWakeTarget(mailboxId);
    const current = store.getWakeTarget(mailboxId);
    if (!current || current.driver === 'passive') {
      return createOrcaTerminalWakeupDriver({ commandRunner: runner, orcaCommand: command })
        .resolveSessionTarget?.(mailboxId, store);
    }
    return undefined;
  });
}

async function deliverOrcaMailboxes(
  store: MailboxStore, wakeup: WakeupRedriveService | undefined, context: ExecutionContext,
  groups: { mailboxId: string; envelopes: MessageEnvelope[]; wakeTarget?: Partial<WakeTargetState> }[],
) {
  const inserted: string[] = [];
  const envelopeMailboxes: [string, string][] = [];
  let woke = false;
  let reason: string | undefined;
  for (const { mailboxId, envelopes, wakeTarget } of groups) {
    const enqueued = store.enqueue(envelopes, {
      ...(context?.signedEnvelopes ? { signedEnvelopes: context.signedEnvelopes } : {}),
      mailboxId,
      ...(wakeTarget ? { wakeTarget } : {}),
    });
    inserted.push(...enqueued.inserted);
    envelopeMailboxes.push(...Object.entries(enqueued.envelopeMailboxes));
    if (wakeup) {
      try {
        const result = await wakeup.flushMailbox(mailboxId);
        woke ||= result.woke;
        reason ??= result.reason;
      } catch (error) {
        // Durable ingestion already succeeded; the redrive timer owns retry.
        reason ??= String(error);
      }
    }
  }
  return {
    ...(groups.length ? { mailboxId: groups[0].mailboxId } : {}),
    mailboxIds: groups.map(group => group.mailboxId),
    inserted,
    envelopeMailboxes: Object.fromEntries(envelopeMailboxes),
    woke,
    ...(reason !== undefined ? { reason } : {}),
  };
}

/**
 * Durable wave ingestion. Each envelope is committed to its resolved logical mailbox before any
 * pointer is attempted; a busy workspace or a daemon restart resumes from `pendingWake`, so an
 * executor never rejects a wave merely because the workspace is busy.
 */
export function createOrcaWaveExecutorPlugin(options: OrcaWaveOptions = {}): ExecutorPlugin {
  return {
    name: 'orca-wave',
    async execute(batch, context, runtimeConfig) {
      const config = (runtimeConfig ?? {}) as Partial<OrcaWaveOptions>;
      const store = options.store ?? config.store;
      if (!store) {
        throw beforeExternalEffect(new Error('orca-wave requires a durable mailbox store'));
      }
      const wakeup = options.wakeup ?? config.wakeup;
      if (!batch[0]) {
        throw beforeExternalEffect(new Error('Empty wave batch'));
      }
      const command = options.orcaCommand ?? 'orca';
      const runner = options.commandRunner ?? config.commandRunner ?? runCommand;
      const resolveTopicBinding = options.resolveTopicBinding ?? config.resolveTopicBinding;
      const waveConfig = config.wave ?? options.wave;

      /**
       * Receiver-owned workspace, terminal and scheduling configuration for the topic's own bound
       * mailbox. Inbound payload values are only a fallback when the deployment has no local
       * binding or executor config, so a sender can never rebind runtime-managed workspace state.
       */
      const topicWakeTarget = (
        envelopes: readonly MessageEnvelope[],
        binding: OrcaTopicBinding | undefined,
        mailboxId: string,
      ): Partial<WakeTargetState> => {
        const payload = (envelopes[0].payload ?? {}) as Record<string, unknown>;
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
        const effectiveMode = prepareExternalEffect(() =>
          resolveEffectiveConversationMode({
            fallbackMode: 'wave',
            binding,
            payload,
            sources: [
              payload,
              binding,
              config as { mode?: unknown; conversationMode?: unknown },
              options as { mode?: unknown; conversationMode?: unknown },
              { mode: 'wave' },
            ],
          }),
        );
        const rotateAfterWaves = resolveOrcaWaveRotateAfterWaves({
          mode:
            (payload.mode ?? binding?.mode ?? (config as { mode?: unknown }).mode) === 'fixed'
              ? 'fixed'
              : effectiveMode === 'fixed'
                ? 'fixed'
                : 'wave',
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
        const identityPatch = resolveConfiguredOrcaWakeTargetPatch(store.getWakeTarget(mailboxId), {
          mailboxId, runId, workspacePath: cwd, configuredTerminalHandle: terminalHandle,
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
          ...(effectiveMode === 'fixed'
            ? { rotateAfterWaves: undefined }
            : rotateAfterWaves !== undefined
              ? { rotateAfterWaves }
              : {}),
          driverConfig: {
            ...identityPatch.driverConfig,
            orcaCommand: command,
            ...(binding?.repoRoot ? { repoRoot: binding.repoRoot } : {}),
            ...(binding?.autoCreateWorkspace !== undefined
              ? { autoCreateWorkspace: binding.autoCreateWorkspace }
              : {}),
            ...(runId ? { runId } : {}),
            ...(cwd ? { workspacePath: cwd } : {}),
            ...(worktreeId ? { worktreeId } : {}),
            workspaceComment,
          },
        };
      };

      // Each envelope resolves through its own topic binding and thread-affinity route, so a mixed
      // batch is never conflated into one mailbox.
      const groups = new Map<
        string,
        {
          envelopes: MessageEnvelope[];
          bindingKind: string;
          binding?: OrcaTopicBinding;
        }
      >();
      for (const envelope of batch) {
        const streamKey = typeof envelope.payload?.streamKey === 'string' ? envelope.payload.streamKey : undefined;
        const binding = resolveTopicBinding?.(envelope.kind, streamKey);
        const { kind: bindingKind } = resolveTopicAddress(envelope.kind, streamKey);
        const mailboxId = prepareExternalEffect(() =>
          resolveMailboxId(
            envelope,
            binding,
            store.resolveThreadLocalMailbox(envelope.semantic?.threadId, envelope.networkId, {
              excludeInboundMessageId: envelope.id,
            }),
            'wave',
          ),
        );
        const group = groups.get(mailboxId);
        if (group) {
          group.envelopes.push(envelope);
        } else {
          groups.set(mailboxId, { envelopes: [envelope], bindingKind, binding });
        }
      }

      const prepared = [];
      for (const [mailboxId, group] of groups) {
        const boundMailboxId = group.binding
          ? prepareExternalEffect(() => resolveBoundMailboxId(group.bindingKind, group.binding, 'wave'))
          : undefined;
        // Only the topic's own bound mailbox may use the receiver binding. A thread- or
        // session-scoped mailbox keeps the wake target its originating session already bound, so
        // a reply wakes that terminal instead of rotating it into the generic topic workspace.
        const isSessionScopedMailbox = mailboxId.startsWith('worktree:') || mailboxId.startsWith('terminal:') || mailboxId.startsWith('task:');
        const usesTopicBinding = group.binding !== undefined ? mailboxId === boundMailboxId : !isSessionScopedMailbox;
        const wakeTarget = usesTopicBinding
          ? prepareExternalEffect(() => topicWakeTarget(group.envelopes, group.binding, mailboxId))
          : await resolveOrcaSessionWakeTarget(store, wakeup, runner, command, mailboxId);
        prepared.push({ mailboxId, envelopes: group.envelopes, wakeTarget });
      }
      return {
        executor: 'orca-wave',
        executionId: randomUUID(),
        ...await deliverOrcaMailboxes(store, wakeup, context, prepared),
      };
    },
  };
}

/** Unified Orca executor selecting task or stream semantics from the orthogonal mode. */
export function createOrcaExecutorPlugin(options: OrcaSpawnOptions & OrcaWaveOptions = {}): ExecutorPlugin {
  const spawnPlugin = createOrcaSpawnExecutorPlugin(options);
  const wavePlugin = createOrcaWaveExecutorPlugin(options);
  return {
    name: 'orca',
    async execute(batch, context, runtimeConfig) {
      const config = (runtimeConfig ?? {}) as Record<string, unknown>;
      const binding = (
        options.resolveTopicBinding ?? (config.resolveTopicBinding as OrcaSpawnOptions['resolveTopicBinding'])
      )?.(
        batch[0]?.kind,
        typeof batch[0]?.payload?.streamKey === 'string' ? batch[0].payload.streamKey : undefined,
      );
      const payload = batch[0]?.payload;
      const effectiveMode = prepareExternalEffect(() =>
        resolveEffectiveConversationMode({
          fallbackMode: 'spawn',
          binding,
          payload,
          sources: [
            payload,
            binding,
            config as { mode?: unknown; conversationMode?: unknown },
            options as { mode?: unknown; conversationMode?: unknown },
            { mode: binding?.defaultMode ?? 'spawn' },
          ],
        }),
      );
      const receipt =
        effectiveMode === 'inbox-only'
          ? await prepareExternalEffect(() =>
              createInboxExecutorPlugin({
                store: options.store ?? (config.store as MailboxStore | undefined),
              }),
            ).execute(batch, context, runtimeConfig)
          : effectiveMode === 'spawn'
            ? await spawnPlugin.execute(batch, context, runtimeConfig)
            : await wavePlugin.execute(batch, context, { ...runtimeConfig, mode: effectiveMode });
      return { ...receipt, executor: 'orca' };
    },
  };
}
