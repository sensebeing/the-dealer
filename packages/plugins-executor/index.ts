import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import {
  existsSync,
  readFileSync,
  realpathSync,
} from 'node:fs';
import { appendFile, mkdir } from 'node:fs/promises';
import { homedir } from 'node:os';
import { basename, dirname, isAbsolute, join, relative, resolve } from 'node:path';
import type { TopicBindingConfig } from '../cli/config.ts';
import type { ExecutionContext, ExecutorPlugin, MessageEnvelope } from '../core-types/index.ts';
import { resolveMailboxId } from '../mailbox/index.ts';
import { MailboxStore } from '../mailbox/index.ts';
import { buildTaskPreamble } from '../mailbox/index.ts';
import type { WakeupRedriveService } from '../mailbox/index.ts';
import {
  checkedCommand,
  trustAntigravityWorkspace,
  defaultAgentLaunchCommand,
  singleLine,
  runCommand,
  sendOrcaTerminalText,
} from '../orca-workspace/index.ts';
import type { CommandRunner, SupportedOrcaAgent } from '../orca-workspace/index.ts';

export function renderWave(batch: readonly MessageEnvelope[]): string {
  return batch
    .map(
      e =>
        `Subject: ${e.subject}\nMessage: ${e.id}\n${e.body}\nPayload: ${JSON.stringify(e.payload)}\nAttachments: ${JSON.stringify(e.attachments)}`,
    )
    .join('\n\n---\n\n');
}

export type SupportedAgentHost = 'orca' | 'antigravity' | 'claude' | 'claude-code' | 'codex' | 'chatgpt-dot' | 'grok-bot';
export type AgentConversationMode = 'spawn' | 'fixed' | 'inbox-only';

export interface AgentOptions {
  store?: MailboxStore;
  resolveTopicBinding?: (kind: string, streamKey?: string) => (TopicBindingConfig & { repoRoot?: string }) | undefined;
  agent?: SupportedAgentHost;
  conversationMode?: 'spawn' | 'fixed';
  agentProfile?: string;
  targetBot?: string;
  sessionId?: string;
  command?: string;
  args?: readonly string[];
  cwd?: string;
  commandRunner?: CommandRunner;
}

function agentArgs(command: string): string[] {
  return command === 'codex' ? ['exec', '-'] : command === 'claude' ? ['-p'] : [];
}

export function buildAgentHostInvocation(options: {
  agent: SupportedAgentHost;
  conversationMode: 'spawn' | 'fixed';
  mailboxId: string;
  promptText: string;
  command?: string;
  cwd?: string;
  agentProfile?: string;
  targetBot?: string;
  sessionId?: string;
}): { command: string; args: string[] } {
  const { agent, conversationMode: mode, mailboxId, promptText, cwd, agentProfile, targetBot, sessionId } = options;
  const session = sessionId || mailboxId;
  switch (agent) {
    case 'codex':
      return { command: options.command ?? 'codex', args: [
        ...(mode === 'fixed' ? ['queue', '--thread', session, '--resume'] : ['exec', '--worktree']),
        ...(agentProfile ? ['-p', agentProfile] : []),
        ...(cwd ? ['-C', cwd] : []),
        ...(mode === 'fixed' ? ['--message'] : []), promptText,
      ] };
    case 'claude':
    case 'claude-code':
      return { command: options.command ?? 'claude', args: [
        '--bg', ...(mode === 'fixed' ? ['--resume', session] : ['--name', mailboxId.replace(/[^A-Za-z0-9._-]/g, '-')]),
        ...(agentProfile ? ['--agent', agentProfile] : []), promptText,
      ] };
    case 'antigravity':
      return { command: options.command ?? 'agy', args: ['run', '--dangerously-skip-permissions', promptText] };
    case 'chatgpt-dot': {
      const dotTarget = targetBot ?? agentProfile;
      return { command: options.command ?? 'dot', args: ['send', '--session-mode', mode, '--session-key', session,
        ...(dotTarget ? ['--agent', dotTarget] : []), '--message', promptText] };
    }
    case 'grok-bot':
      if (!targetBot?.trim()) throw new Error('grok-bot requires targetBot');
      return { command: options.command ?? 'grok-bot', args: ['dispatch', '--bot', targetBot, '--session-mode', mode,
        '--session-key', session, '--message', promptText] };
    default:
      throw new Error(`Unsupported agent-pty host: ${agent}`);
  }
}

function orcaAgent(value: unknown): SupportedOrcaAgent {
  if (value === undefined) return 'antigravity';
  if (value === 'antigravity' || value === 'claude' || value === 'codex') return value;
  throw new Error(`Unsupported Orca agent: ${String(value)}`);
}

export function createAgentPtyExecutorPlugin(options: AgentOptions = {}): ExecutorPlugin {
  return {
    name: 'agent-pty',
    async execute(batch, context, runtimeConfig) {
      const config = (runtimeConfig ?? {}) as Partial<AgentOptions>;
      const first = batch[0];
      const payload = first?.payload ?? {};
      const binding = first ? (options.resolveTopicBinding ?? config.resolveTopicBinding)?.(first.kind, typeof first?.payload?.streamKey === 'string' ? first.payload.streamKey : undefined) : undefined;
      const agent = payload.agent ?? binding?.agent ?? config.agent ?? options.agent;
      const mode = payload.conversationMode ?? binding?.conversationMode ?? config.conversationMode ?? options.conversationMode;
      const hostMode = mode !== undefined || ['antigravity', 'claude-code', 'chatgpt-dot', 'grok-bot'].includes(agent);
      let request;
      if (!hostMode) {
        const command = options.command ?? 'codex';
        request = { command, args: options.args ?? agentArgs(command), cwd: options.cwd, input: renderWave(batch) };
      } else {
        if (!first) throw new Error('Empty agent batch');
        if (mode !== undefined && mode !== 'spawn' && mode !== 'fixed') throw new Error('Invalid agent conversationMode');
        const conversationMode = mode ?? 'spawn';
        const mailboxId = conversationMode === 'spawn' ? `task:${first.id}` : resolveMailboxId(first, 'inbox', binding);
        const store = options.store ?? config.store;
        const terminalPrompt = optionalString(typeof payload.terminalPrompt === 'string' ? payload.terminalPrompt.trim() : undefined);
        const promptText = store
          ? conversationMode === 'spawn'
            ? buildTaskPreamble(mailboxId, first.id, undefined, terminalPrompt ?? (first.body || first.subject))
            : singleLine(`You have ${batch.length} Dealer message(s) in ${mailboxId}. Run \`dealer inbox check --mailbox ${mailboxId} --json\`.`)
          : singleLine(terminalPrompt ?? renderWave(batch));
        const cwd = binding?.workspacePath ?? binding?.repoRoot ?? config.cwd ?? options.cwd;
        const invocation = buildAgentHostInvocation({
          agent: agent ?? 'codex', conversationMode, mailboxId, promptText, cwd,
          command: config.command ?? options.command,
          agentProfile: payload.agentProfile ?? binding?.agentProfile ?? config.agentProfile ?? options.agentProfile,
          targetBot: payload.targetBot ?? binding?.targetBot ?? config.targetBot ?? options.targetBot,
          sessionId: payload.sessionId ?? binding?.sessionId ?? config.sessionId ?? options.sessionId,
        });
        if (store) store.enqueue(batch, { mailboxId, wakeTarget: { mode: 'passive' } });
        request = { ...invocation, cwd };
      }
      const output = await checkedCommand(options.commandRunner ?? runCommand, { ...request, signal: context.signal });
      let structuredOutput: unknown;
      try {
        structuredOutput = JSON.parse(output.stdout);
      } catch {
        structuredOutput = output.stdout;
      }
      return { executor: 'agent-pty', executionId: randomUUID(), output: structuredOutput, stderr: output.stderr ?? '' };
    },
  };
}

export interface OrcaSpawnOptions extends AgentOptions {
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
  /** Durable task mailbox ledger; always supplied by DealerNodeRuntime. */
  store?: MailboxStore;
  wakeup?: WakeupRedriveService;
}

const shellQuote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;

/** Canonical worktree names used for sea-creature naming, in preference order. */
export const SEA_CREATURE_POOL = [
  'nautilus',
  'seahorse',
  'starfish',
  'dolphin',
  'octopus',
  'jellyfish',
  'stingray',
  'manatee',
  'narwhal',
  'walrus',
  'beluga',
  'dugong',
  'axolotl',
  'coral',
  'halibut',
  'lamprey',
  'mackerel',
  'oarfish',
  'pollock',
  'remora',
  'sardine',
  'triggerfish',
  'bonefish',
  'cuttlefish',
  'manta',
  'killifish',
  'lionfish',
  'mudskipper',
  'needlefish',
  'paddlefish',
  'triton',
  'rockfish',
  'sawfish',
  'tilefish',
  'weakfish',
  'yellowtail',
  'wahoo',
  'tautog',
  'sculpin',
  'pomfret',
  'coelacanth',
  'lumpfish',
  'hogfish',
  'porpoise',
  'dace',
  'blenny',
  'abalone',
  'barnacle',
  'cockle',
  'krill',
  'limpet',
  'mussel',
  'oyster',
  'scallop',
  'urchin',
  'whelk',
  'conch',
  'prawn',
  'hammerhead',
  'anemone',
  'wobbegong',
  'hydroid',
  'isopod',
  'nudibranch',
  'salp',
  'tunicate',
  'chimaera',
  'copepod',
  'amphipod',
  'anglerfish',
] as const;

async function pickSeaCreatureName(
  runner: CommandRunner,
  command: string,
  cwd: string | undefined,
  signal?: AbortSignal,
): Promise<string> {
  const listed = await checkedCommand(runner, {
    command,
    args: ['worktree', 'list', '--json'],
    cwd,
    signal,
  });
  const raw = JSON.parse(listed.stdout) as {
    ok?: boolean;
    result?: { worktrees?: { path?: unknown; displayName?: unknown; branch?: unknown }[] };
  };
  if (raw.ok === false) {
    throw new Error('Orca worktree listing failed');
  }
  const existing = new Set<string>();
  for (const row of raw.result?.worktrees ?? []) {
    if (typeof row.path === 'string' && row.path) {
      existing.add(basename(row.path).toLowerCase());
    }
    if (typeof row.displayName === 'string' && row.displayName) {
      existing.add(row.displayName.toLowerCase());
    }
    // Branch refs such as refs/heads/ben.xu/seahorse collide once their basename matches a creature.
    if (typeof row.branch === 'string' && row.branch) {
      existing.add(basename(row.branch).toLowerCase());
    }
  }
  // Local branches outlive removed worktrees; never reuse a creature whose branch still exists.
  if (typeof cwd === 'string' && existsSync(cwd)) {
    try {
      const branches = execFileSync(
        'git',
        ['-C', cwd, 'for-each-ref', '--format=%(refname:short)', 'refs/heads'],
        { encoding: 'utf8' },
      );
      for (const branch of branches.split('\n')) {
        if (branch.trim()) {
          existing.add(basename(branch.trim()).toLowerCase());
        }
      }
    } catch {
      // A non-git directory has no local branches to collide with.
    }
  }
  const available = SEA_CREATURE_POOL.find(creature => !existing.has(creature));
  return available ?? `nautilus-${randomUUID().slice(0, 8)}`;
}

function gitCommonDir(directory: string): string | undefined {
  if (!existsSync(directory)) {
    return undefined;
  }
  let commonDir: string;
  try {
    commonDir = execFileSync(
      'git', ['-C', directory, 'rev-parse', '--path-format=absolute', '--git-common-dir'],
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
  const reject = () => new Error(`Parent worktree does not belong to project ${projectId}`);
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
  return {
    name: 'orca-spawn',
    async execute(batch, context, runtimeConfig) {
      const runner = options.commandRunner ?? runCommand;
      const command = options.orcaCommand ?? 'orca';
      const payload = (batch[0]?.payload ?? {}) as Record<string, unknown>;
      const config = (runtimeConfig ?? {}) as Partial<OrcaSpawnOptions>;
      const binding = (options.resolveTopicBinding ?? config.resolveTopicBinding)?.(batch[0]?.kind, typeof batch[0]?.payload?.streamKey === 'string' ? batch[0].payload.streamKey : undefined);
      const interactive = payload.interactive === true || binding?.conversationMode === 'spawn' || config.interactive === true || options.interactive === true;
      const store = options.store ?? config.store;
      const effectiveAgent = orcaAgent(payload.agent ?? binding?.agent ?? config.agent ?? options.agent);
      const wakeup = options.wakeup ?? config.wakeup;
      const configuredCwd = binding?.workspacePath ?? binding?.repoRoot ??
        (typeof config.cwd === 'string' && config.cwd ? config.cwd : undefined) ?? options.cwd;
      const projectId =
        typeof payload.projectId === 'string' && payload.projectId.trim()
          ? payload.projectId.trim()
          : binding?.projectId;
      const projectRoots = { ...(options.projectRoots ?? {}), ...(config.projectRoots ?? {}) };
      const boundRoot = projectId && typeof projectRoots[projectId] === 'string'
        ? projectRoots[projectId].trim() || undefined
        : undefined;
      if (projectId && !boundRoot) {
        throw new Error(`Unbound project on receiver: ${projectId}`);
      }
      const defaultCwd = boundRoot ?? configuredCwd;
      let parentWorktree: string | undefined = (payload.noParent ?? binding?.noParent) === true
        ? undefined
        : defaultCwd ? `path:${defaultCwd}` : 'active';
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
      const worktreeCwd = boundRoot ?? configuredCwd ??
        (parentWorktree?.startsWith('path:') ? parentWorktree.slice(5) : undefined);
      if (parentWorktree === undefined && !worktreeCwd) {
        throw new Error('Top-level Orca worktree requires receiver cwd or bound projectId');
      }
      if (interactive && !store) {
        throw new Error('Interactive orca-spawn requires a durable mailbox store');
      }
      const explicitName =
        typeof payload.worktreeName === 'string' && payload.worktreeName.trim()
          ? payload.worktreeName.trim()
          : undefined;
      const seaCreatureNaming =
        payload.seaCreatureNaming === true || options.seaCreatureNaming === true;
      const name = explicitName
        ? explicitName
        : seaCreatureNaming
          ? await pickSeaCreatureName(runner, command, worktreeCwd, context.signal)
          : `${options.namePrefix ?? 'dealer'}-${randomUUID()}`;
      const createArgs = parentWorktree
        ? ['worktree', 'create', '--parent-worktree', parentWorktree, '--name', name, '--json']
        : ['worktree', 'create', '--no-parent', '--name', name, '--json'];
      const created = await checkedCommand(runner, {
        command,
        args: createArgs,
        cwd: worktreeCwd,
        signal: context.signal,
      });
      const raw = JSON.parse(created.stdout);
      if (raw.ok === false) {
        throw new Error('Orca worktree creation failed');
      }
      const workspace = (raw.result ?? raw) as { path?: string; worktree?: { path?: string } };
      const path = workspace.path ?? workspace.worktree?.path;
      if (!path) {
        throw new Error('Orca worktree create returned no path');
      }
      if (interactive) {
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
        if (
          effectiveAgent === 'antigravity' &&
          payload.trustAntigravity !== false &&
          (existsSync(settingsFile) ||
            explicitSettingsPath !== undefined ||
            optionSettingsPath !== undefined)
        ) {
          trustAntigravityWorkspace(path, settingsFile);
        }
        const interactiveCommand = binding?.interactiveCommand?.trim() ||
          config.interactiveCommand?.trim() || options.interactiveCommand?.trim() ||
          defaultAgentLaunchCommand(effectiveAgent);
        const launched = await checkedCommand(runner, {
          command,
          args: [
            'terminal',
            'create',
            '--worktree',
            `path:${path}`,
            '--command',
            interactiveCommand,
            '--json',
          ],
          cwd: path,
          signal: context.signal,
        });
        const terminal = JSON.parse(launched.stdout);
        if (terminal.ok === false) {
          throw new Error('Orca terminal launch failed');
        }
        const terminalHandle =
          terminal.result?.terminal?.handle ?? terminal.result?.handle ?? terminal.handle;
        if (typeof terminalHandle === 'string' && terminalHandle) {
          const handle = String(terminalHandle);
          if (typeof payload.workspaceComment === 'string' && payload.workspaceComment.trim()) {
            await checkedCommand(runner, {
              command,
              args: [
                'worktree',
                'set',
                '--worktree',
                `path:${path}`,
                '--comment',
                payload.workspaceComment,
                '--json',
              ],
              cwd: path,
              signal: context.signal,
            });
          }
          if (batch[0]) {
            // The task is durable before any keystroke. Spawn owns the first idle window; a crash
            // leaves pendingWake behind so WakeupRedriveService takes over after restart.
            const mailboxId = `task:${batch[0].id}`;
            const claim = wakeup?.claim(mailboxId);
            try {
              store.enqueue(batch, {
                mailboxId,
                wakeTarget: {
                  mode: 'orca-terminal',
                  agent: effectiveAgent,
                  interactiveCommand,
                  orcaCommand: command,
                  workspacePath: path,
                  terminalHandle: handle,
                  coalesceMs: 0,
                  ...(typeof payload.workspaceComment === 'string' &&
                  payload.workspaceComment.trim()
                    ? { workspaceComment: payload.workspaceComment }
                    : {}),
                },
              });
              let idle = false;
              try {
                const waited = await checkedCommand(runner, {
                  command,
                  args: [
                    'terminal',
                    'wait',
                    '--terminal',
                    handle,
                    '--for',
                    'tui-idle',
                    '--timeout-ms',
                    '5000',
                    '--json',
                  ],
                  cwd: path,
                  signal: context.signal,
                });
                idle = JSON.parse(waited.stdout).ok === true;
              } catch {
                idle = false;
              }
              if (idle) {
                const rawPrompt =
                  typeof payload.terminalPrompt === 'string' && payload.terminalPrompt.trim()
                    ? payload.terminalPrompt
                    : (batch[0].body || batch[0].subject);
                await sendOrcaTerminalText(
                  runner,
                  command,
                  handle,
                  buildTaskPreamble(mailboxId, batch[0].id, undefined, rawPrompt),
                  { cwd: path, signal: context.signal, waitSubmit: 5 },
                );
                store.upsertWakeTarget(mailboxId, {
                  pendingWake: false,
                  attemptCount: 0,
                  earliestWakeAt: 0,
                  lastWakeError: undefined,
                });
              } else {
                // Never inject into a busy or unverifiable terminal; the redrive service will.
                store.upsertWakeTarget(mailboxId, { pendingWake: true });
              }
            } finally {
              claim?.dispose();
            }
          }
        }
        return {
          executor: 'orca-spawn',
          executionId: randomUUID(),
          worktreePath: path,
          terminal,
        };
      }
      const agent = options.command ?? 'codex';
      // Orca terminal's --command is a shell command; quote every untrusted argument.
      const agentCommand = [
        agent,
        ...(options.args ?? (agent === 'codex' ? ['exec'] : agentArgs(agent))),
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
    },
  };
}

export interface OrcaWaveOptions {
  agent?: SupportedOrcaAgent;
  interactiveCommand?: string;
  resolveTopicBinding?: (kind: string, streamKey?: string) => (TopicBindingConfig & { repoRoot?: string }) | undefined;
  runId?: string;
  terminalHandle?: string;
  cwd?: string;
  orcaCommand?: string;
  commandRunner?: CommandRunner;
  /** Durable mailbox ledger; always supplied by DealerNodeRuntime. */
  store?: MailboxStore;
  wakeup?: WakeupRedriveService;
  /** Default coalescing/rotation settings; payload values take precedence. */
  wave?: { minAgeMs?: number; rotateAfterWaves?: number };
}

function optionalString(value: unknown): string | undefined {
  return typeof value === 'string' && value ? value : undefined;
}

function optionalPositiveInteger(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0
    ? value
    : undefined;
}

/**
 * Durable wave ingestion. The batch is committed to its logical stream mailbox before any
 * pointer is attempted; a busy workspace or a daemon restart resumes from `pendingWake`, so an
 * executor never rejects a wave merely because the workspace is busy.
 */
export function createOrcaWaveExecutorPlugin(options: OrcaWaveOptions = {}): ExecutorPlugin {
  return {
    name: 'orca-wave',
    async execute(batch, _context, runtimeConfig) {
      const config = (runtimeConfig ?? {}) as Partial<OrcaWaveOptions>;
      const store = options.store ?? config.store;
      if (!store) {
        throw new Error('orca-wave requires a durable mailbox store');
      }
      const wakeup = options.wakeup ?? config.wakeup;
      const first = batch[0];
      if (!first) {
        throw new Error('Empty wave batch');
      }
      const command = options.orcaCommand ?? 'orca';
      const payload = (first.payload ?? {}) as Record<string, unknown>;
      const binding = (options.resolveTopicBinding ?? config.resolveTopicBinding)?.(first.kind, typeof first?.payload?.streamKey === 'string' ? first.payload.streamKey : undefined);
      const mailboxId = resolveMailboxId(first, 'orca-wave', binding);
      const cwd =
        optionalString(payload.workspacePath) ??
        optionalString(payload.cwd) ??
        binding?.workspacePath ??
        config.cwd ??
        options.cwd;
      const runId =
        optionalString(payload.runId) ?? binding?.runId ?? config.runId ?? options.runId;
      const terminalHandle =
        optionalString(payload.terminalHandle) ??
        binding?.terminalHandle ??
        config.terminalHandle ??
        options.terminalHandle;
      const waveConfig = config.wave ?? options.wave;
      const requestedMinAge =
        typeof payload.minAgeMs === 'number' && Number.isFinite(payload.minAgeMs)
          ? Math.max(0, payload.minAgeMs)
          : Math.max(0, binding?.coalesceMs ?? waveConfig?.minAgeMs ?? 0);
      const rotateAfterWaves =
        optionalPositiveInteger(payload.rotateAfterWaves) ??
        binding?.rotateAfterWaves ?? optionalPositiveInteger(waveConfig?.rotateAfterWaves);
      const worktreeId = optionalString(payload.worktreeId) ?? binding?.worktreeId;
      const workspaceComment = String(
        batch.at(-1)?.payload?.workspaceComment || binding?.workspaceComment || `Dealer: ${mailboxId} pending`,
      );
      const enqueued = store.enqueue(batch, {
        mailboxId,
        wakeTarget: {
          mode: runId ? 'orca-run' : 'orca-terminal',
          agent: orcaAgent(payload.agent ?? binding?.agent ?? config.agent ?? options.agent),
          interactiveCommand: optionalString(payload.interactiveCommand) ?? binding?.interactiveCommand ?? config.interactiveCommand ?? options.interactiveCommand,
          orcaCommand: command,
          coalesceMs: requestedMinAge,
          ...(binding?.repoRoot ? { repoRoot: binding.repoRoot } : {}),
          ...(binding?.autoCreateWorkspace !== undefined ? { autoCreateWorkspace: binding.autoCreateWorkspace } : {}),
          ...(binding?.wakeupPrefix ? { wakeupPrefix: binding.wakeupPrefix } : {}),
          ...(runId ? { runId } : {}),
          ...(cwd ? { workspacePath: cwd } : {}),
          ...(worktreeId ? { worktreeId } : {}),
          ...(terminalHandle ? { terminalHandle } : {}),
          ...(rotateAfterWaves !== undefined ? { rotateAfterWaves } : {}),
          workspaceComment,
        },
      });
      let woke = false;
      let reason: string | undefined;
      if (wakeup) {
        try {
          const result = await wakeup.flushMailbox(mailboxId);
          woke = result.woke;
          reason = result.reason;
        } catch (error) {
          // Durable ingestion already succeeded; the redrive timer owns retry.
          reason = String(error);
        }
      }
      return {
        executor: 'orca-wave',
        executionId: randomUUID(),
        mailboxId,
        inserted: enqueued.inserted,
        woke,
        ...(reason !== undefined ? { reason } : {}),
      };
    },
  };
}

export interface NotifyOptions {
  inboxPath?: string;
  notify?: (batch: readonly MessageEnvelope[], context: ExecutionContext) => Promise<void>;
  commandRunner?: CommandRunner;
  larkWebhookUrl?: string;
  fetchFn?: typeof fetch;
}

export function createNotifyExecutorPlugin(options: NotifyOptions = {}): ExecutorPlugin {
  return {
    name: 'notify',
    async execute(batch, context) {
      if (options.inboxPath) {
        await mkdir(dirname(options.inboxPath), { recursive: true });
        await appendFile(
          options.inboxPath,
          `${batch.map(e => JSON.stringify(e)).join('\n')}\n`,
          { mode: 0o600 },
        );
      }
      if (options.notify) {
        await options.notify(batch, context);
      } else if (options.larkWebhookUrl) {
        const response = await (options.fetchFn ?? fetch)(options.larkWebhookUrl, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ msg_type: 'text', content: { text: renderWave(batch) } }),
          signal: context.signal,
        });
        if (!response.ok) {
          throw new Error(`Lark notification failed: ${response.status}`);
        }
        const data = (await response.json()) as { code?: number; StatusCode?: number };
        if ((data.code ?? data.StatusCode ?? 0) !== 0) {
          throw new Error('Lark notification rejected');
        }
      } else if (!options.inboxPath) {
        await checkedCommand(options.commandRunner ?? runCommand, {
          command: 'osascript',
          args: [
            '-e',
            'on run argv\ndisplay notification (item 1 of argv) with title (item 2 of argv)\nend run',
            renderWave(batch),
            'The Dealer',
          ],
          signal: context.signal,
        });
      }
      return { executor: 'notify', executionId: randomUUID(), count: batch.length };
    },
  };
}

export interface InboxExecutorOptions {
  resolveTopicBinding?: (kind: string, streamKey?: string) => (TopicBindingConfig & { repoRoot?: string }) | undefined;
  inboxPath?: string;
  store?: MailboxStore;
  /** Reactive redrive for replies/questions/settlements landing in a live wake target. */
  wakeup?: WakeupRedriveService;
}

export function createInboxExecutorPlugin(options: InboxExecutorOptions = {}): ExecutorPlugin {
  if (!options.store && !options.inboxPath) {
    throw new Error('Inbox executor requires store or inboxPath');
  }
  const store = options.store ?? new MailboxStore(options.inboxPath!);
  return {
    name: 'inbox',
    async execute(batch, _context, runtimeConfig) {
      const config = (runtimeConfig ?? {}) as InboxExecutorOptions;
      const groups = new Map<string, MessageEnvelope[]>();
      for (const envelope of batch) {
        const mailboxId = resolveMailboxId(envelope, 'inbox', (options.resolveTopicBinding ?? config.resolveTopicBinding)?.(envelope.kind, typeof envelope?.payload?.streamKey === 'string' ? envelope.payload.streamKey : undefined));
        const group = groups.get(mailboxId) ?? [];
        group.push(envelope);
        groups.set(mailboxId, group);
      }
      const inserted: string[] = [];
      for (const [mailboxId, envelopes] of groups) {
        const result = store.enqueue(envelopes, { mailboxId });
        inserted.push(...result.inserted);
        if (result.inserted.length > 0) {
          options.wakeup?.notifyMailbox(mailboxId);
        }
      }
      return {
        executor: 'inbox',
        executionId: randomUUID(),
        count: batch.length,
        inserted,
      };
    },
  };
}

export { trustAntigravityWorkspace } from '../orca-workspace/index.ts';
