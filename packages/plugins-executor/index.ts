import { buildAgentHostInvocation, resolveAgentSessionId } from '../agent-host/index.ts';
import type { SupportedAgentHost } from '../agent-host/index.ts';
import { execFileSync } from 'node:child_process';
import { randomInt, randomUUID } from 'node:crypto';
import {
  existsSync,
  realpathSync,
} from 'node:fs';
import { appendFile, mkdir } from 'node:fs/promises';
import { homedir } from 'node:os';
import { basename, dirname, isAbsolute, join, relative, resolve } from 'node:path';
import type { TopicBindingConfig } from '../cli/config.ts';
import type { ExecutionContext, ExecutorPlugin, MessageEnvelope } from '../core-types/index.ts';
import { MailboxStore, resolveMailboxId, deliverPassiveWakeTarget, deliverSpawnedOrcaTerminalWakeup } from '../mailbox/index.ts';
import type { WakeTargetState, WakeupRedriveService } from '../mailbox/index.ts';
import {
  checkedCommand,
  trustAntigravityWorkspace,
  defaultAgentLaunchCommand,
  singleLine,
  runCommand,
  findOrcaWorktreeByName,
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

export { buildAgentHostInvocation } from '../agent-host/index.ts';
export type { SupportedAgentHost, AgentConversationMode } from '../agent-host/index.ts';

export interface AgentOptions {
  wakeup?: WakeupRedriveService;
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

function orcaAgent(value: unknown): SupportedOrcaAgent {
  if (value === undefined) return 'antigravity';
  if (value === 'antigravity' || value === 'claude' || value === 'codex') return value;
  throw new Error(`Unsupported Orca agent: ${String(value)}`);
}

export function createAgentPtyExecutorPlugin(options: AgentOptions = {}): ExecutorPlugin {
  return createAgentExecutorPlugin('agent-pty', options);
}

export function createCodexExecutorPlugin(options: Omit<AgentOptions, 'agent'> = {}): ExecutorPlugin {
  return createAgentExecutorPlugin('codex', options, 'codex');
}

export function createClaudeCodeExecutorPlugin(options: Omit<AgentOptions, 'agent'> = {}): ExecutorPlugin {
  return createAgentExecutorPlugin('claude-code', options, 'claude-code');
}

export function createAntigravityExecutorPlugin(options: Omit<AgentOptions, 'agent'> & { agySettingsPath?: string } = {}): ExecutorPlugin {
  return createAgentExecutorPlugin('antigravity', options, 'antigravity', options.agySettingsPath);
}

function createAgentExecutorPlugin(name: string, options: AgentOptions, host?: SupportedAgentHost, agySettingsPath?: string): ExecutorPlugin {
  return {
    name,
    async execute(batch, context, runtimeConfig) {
      const config = (runtimeConfig ?? {}) as Partial<AgentOptions>;
      const first = batch[0];
      const payload = first?.payload ?? {};
      const binding = first ? (options.resolveTopicBinding ?? config.resolveTopicBinding)?.(first.kind, typeof first?.payload?.streamKey === 'string' ? first.payload.streamKey : undefined) : undefined;
      const agent = host ?? payload.agent ?? binding?.agent ?? config.agent ?? options.agent;
      const mode = payload.conversationMode ?? binding?.conversationMode ?? config.conversationMode ?? options.conversationMode ?? (host ? 'spawn' : undefined);
      const hostMode = mode !== undefined || ['antigravity', 'claude-code', 'chatgpt-dot', 'grok-bot'].includes(agent);
      let request;
      let mailboxId: string | undefined;
      let effectiveSessionId: string | undefined;
      let wakeDelivered = false;
      const store = options.store ?? config.store;
      const runner = options.commandRunner ?? runCommand;
      let claim: ReturnType<WakeupRedriveService['claim']> | undefined;
      try {
        if (!hostMode) {
          const command = options.command ?? 'codex';
          request = { command, args: options.args ?? agentArgs(command), cwd: options.cwd, input: renderWave(batch) };
        } else {
          if (!first) throw new Error('Empty agent batch');
          if (mode !== undefined && mode !== 'spawn' && mode !== 'fixed') throw new Error('Invalid agent conversationMode');
          const conversationMode = mode ?? 'spawn';
          mailboxId = conversationMode === 'spawn' ? `task:${first.id}` : resolveMailboxId(first, 'inbox', binding);
          const terminalPrompt = optionalString(typeof payload.terminalPrompt === 'string' ? payload.terminalPrompt.trim() : undefined);
          const cwd = binding?.workspacePath ?? binding?.repoRoot ?? config.cwd ?? options.cwd;
          const command = config.command ?? options.command;
          const agentProfile = payload.agentProfile ?? binding?.agentProfile ?? config.agentProfile ?? options.agentProfile;
          const targetBot = payload.targetBot ?? binding?.targetBot ?? config.targetBot ?? options.targetBot;
          const sessionId = payload.sessionId ?? binding?.sessionId ?? config.sessionId ?? options.sessionId;
          effectiveSessionId = resolveAgentSessionId(mailboxId, sessionId, agent ?? 'codex');
          if (host === 'antigravity' && cwd && (agySettingsPath !== undefined || (options.commandRunner === undefined || options.commandRunner === runCommand))) {
            trustAntigravityWorkspace(cwd, agySettingsPath);
          }
          claim = (options.wakeup ?? config.wakeup)?.claim(mailboxId);
          if (store) {
            store.enqueue(batch, { mailboxId, wakeTarget: {
              mode: 'passive', hostAgent: (agent ?? 'codex') as WakeTargetState['hostAgent'], conversationMode,
              ...(cwd ? { workspacePath: cwd } : {}),
              ...(command ? { interactiveCommand: command } : {}),
              ...(agentProfile ? { agentProfile } : {}),
              ...(targetBot ? { targetBot } : {}),
              sessionId: effectiveSessionId,
            } });
          } else {
            request = { ...buildAgentHostInvocation({
              agent: agent ?? 'codex', conversationMode, mailboxId,
              promptText: singleLine(terminalPrompt ?? renderWave(batch)),
              cwd, command, agentProfile, targetBot, sessionId: effectiveSessionId,
            }), cwd };
          }
        }
        let output;
        if (store && mailboxId) {
          // The mailbox owner builds the pointer and settles the lease for both initial and redrive paths.
          const delivered = await deliverPassiveWakeTarget(store, runner, mailboxId, { signal: context.signal });
          output = { stdout: delivered.stdout, stderr: delivered.stderr };
          wakeDelivered = true;
        } else {
          output = await checkedCommand(runner, { ...request, signal: context.signal });
        }
        let structuredOutput: unknown;
        try {
          structuredOutput = JSON.parse(output.stdout);
        } catch {
          structuredOutput = output.stdout;
        }
        return { executor: name, executionId: randomUUID(), output: structuredOutput, stderr: output.stderr ?? '' };
      } finally {
        claim?.dispose();
        if (wakeDelivered && mailboxId) (options.wakeup ?? config.wakeup)?.notifyMailbox(mailboxId);
      }
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

/** Orca-compatible creature vocabulary for randomly suffixed worktree names. */
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

export interface PickSeaCreatureNameOptions {
  runner: CommandRunner;
  command: string;
  cwd?: string;
  issuedNames?: Set<string>;
  signal?: AbortSignal;
  randomBelow?: (max: number) => number;
}

export async function pickSeaCreatureName(options: PickSeaCreatureNameOptions): Promise<string> {
  const { runner, command, cwd, issuedNames, signal, randomBelow = max => randomInt(max) } = options;
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
      existing.add(basename(row.path).trim().toLowerCase());
    }
    if (typeof row.displayName === 'string' && row.displayName) {
      existing.add(row.displayName.trim().toLowerCase());
    }
    // Branch refs such as refs/heads/ben.xu/seahorse collide once their basename matches a creature.
    if (typeof row.branch === 'string' && row.branch) {
      existing.add(basename(row.branch).trim().toLowerCase());
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
          existing.add(branch.trim().toLowerCase());
          existing.add(basename(branch.trim()).toLowerCase());
        }
      }
    } catch {
      // A non-git directory has no local branches to collide with.
    }
  }
  for (const name of issuedNames ?? []) {
    existing.add(name.trim().toLowerCase());
  }
  const checkedRandomBelow = (max: number): number => {
    const value = randomBelow(max);
    if (!Number.isInteger(value) || value < 0 || value >= max) {
      throw new RangeError(`randomBelow(${max}) must return an integer in [0, ${max})`);
    }
    return value;
  };
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    const creature = SEA_CREATURE_POOL[checkedRandomBelow(SEA_CREATURE_POOL.length)];
    const suffix = 100_000 + checkedRandomBelow(900_000);
    const candidate = `${creature}-${suffix}`;
    if (!existing.has(candidate)) {
      issuedNames?.add(candidate);
      return candidate;
    }
  }
  throw new Error('Sea-creature worktree names exhausted');
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
  const issuedNames = new Set<string>();
  return {
    name: 'orca-spawn',
    async execute(batch, context, runtimeConfig) {
      const runner = options.commandRunner ?? runCommand;
      const payload = (batch[0]?.payload ?? {}) as Record<string, unknown>;
      const config = (runtimeConfig ?? {}) as Partial<OrcaSpawnOptions>;
      const command = config.orcaCommand?.trim() || options.orcaCommand || 'orca';
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
        typeof payload.seaCreatureNaming === 'boolean' ? payload.seaCreatureNaming
          : typeof config.seaCreatureNaming === 'boolean' ? config.seaCreatureNaming
            : options.seaCreatureNaming === true;
      const namePrefix = config.namePrefix?.trim() || options.namePrefix?.trim() || 'dealer';
      const name = explicitName
        ? explicitName
        : seaCreatureNaming
          ? await pickSeaCreatureName({ runner, command, cwd: worktreeCwd, issuedNames, signal: context.signal })
          : `${namePrefix}-${randomUUID()}`;
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
      issuedNames.add(basename(path).trim().toLowerCase());
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
        const mayTrust = !options.commandRunner ||
          options.commandRunner === runCommand ||
          explicitSettingsPath !== undefined || optionSettingsPath !== undefined;
        if (
          mayTrust &&
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
              await deliverSpawnedOrcaTerminalWakeup(store, runner, mailboxId, { signal: context.signal });
            } finally {
              claim?.dispose();
              wakeup?.notifyMailbox(mailboxId);
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
      // The receiver owns workspace, terminal and scheduling configuration; inbound payload values
      // are only a fallback for deployments that have no local binding or executor config.
      const cwd =
        binding?.workspacePath ??
        config.cwd ??
        options.cwd ??
        optionalString(payload.workspacePath) ??
        optionalString(payload.cwd);
      const runId =
        binding?.runId ?? config.runId ?? options.runId ?? optionalString(payload.runId);
      const terminalHandle =
        binding?.terminalHandle ?? config.terminalHandle ?? options.terminalHandle ?? optionalString(payload.terminalHandle);
      const waveConfig = config.wave ?? options.wave;
      const coalesceMs = Math.max(0, binding?.coalesceMs ?? waveConfig?.minAgeMs ?? 0);
      const rotateAfterWaves =
        binding?.rotateAfterWaves ??
        optionalPositiveInteger(waveConfig?.rotateAfterWaves) ??
        optionalPositiveInteger(payload.rotateAfterWaves);
      const worktreeId =
        binding?.worktreeId ?? config.worktreeId ?? options.worktreeId ?? optionalString(payload.worktreeId);
      const workspaceComment = String(
        batch.at(-1)?.payload?.workspaceComment || binding?.workspaceComment || `Dealer: ${mailboxId} pending`,
      );
      const enqueued = store.enqueue(batch, {
        mailboxId,
        wakeTarget: {
          mode: runId ? 'orca-run' : 'orca-terminal',
          agent: orcaAgent(binding?.agent ?? config.agent ?? options.agent ?? payload.agent),
          interactiveCommand: binding?.interactiveCommand ?? config.interactiveCommand ?? options.interactiveCommand,
          orcaCommand: command,
          coalesceMs,
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
  commandRunner?: CommandRunner;
  orcaCommand?: string;
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
      const runner = options.commandRunner ?? config.commandRunner ?? runCommand;
      const orcaCommand = options.orcaCommand ?? config.orcaCommand ?? 'orca';
      const groups = new Map<string, MessageEnvelope[]>();
      for (const envelope of batch) {
        const mailboxId = resolveMailboxId(envelope, 'inbox', (options.resolveTopicBinding ?? config.resolveTopicBinding)?.(envelope.kind, typeof envelope?.payload?.streamKey === 'string' ? envelope.payload.streamKey : undefined));
        const group = groups.get(mailboxId) ?? [];
        group.push(envelope);
        groups.set(mailboxId, group);
      }
      const inserted: string[] = [];
      for (const [mailboxId, envelopes] of groups) {
        let wakeTarget: Partial<WakeTargetState> | undefined;
        if (!store.getWakeTarget(mailboxId)) {
          if (mailboxId.startsWith('terminal:') && mailboxId.length > 9) {
            wakeTarget = { mode: 'orca-terminal', terminalHandle: mailboxId.slice(9) };
          } else if (mailboxId.startsWith('worktree:') && mailboxId.length > 9) {
            const found = await findOrcaWorktreeByName(runner, orcaCommand, mailboxId.slice(9));
            if (found) wakeTarget = { mode: 'orca-terminal', ...found };
          }
        }
        const result = store.enqueue(envelopes, { mailboxId, ...(wakeTarget ? { wakeTarget } : {}) });
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
