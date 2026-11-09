import { execFileSync, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import {
  existsSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  renameSync,
  writeFileSync,
} from 'node:fs';
import { appendFile, mkdir } from 'node:fs/promises';
import { homedir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import type { ExecutionContext, ExecutorPlugin, MessageEnvelope } from '../core-types/index.ts';

export interface CommandRequest {
  command: string;
  args: readonly string[];
  cwd?: string;
  input?: string;
  signal?: AbortSignal;
}

export type CommandRunner = (
  request: CommandRequest,
) => Promise<{ stdout: string; stderr?: string; exitCode?: number }>;

export const runCommand: CommandRunner = request =>
  new Promise((resolve, reject) => {
    const child = spawn(request.command, [...request.args], {
      cwd: request.cwd,
      signal: request.signal,
      stdio: ['pipe', 'pipe', 'pipe'],
      shell: false,
    });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', chunk => {
      stdout += chunk;
    });
    child.stderr.on('data', chunk => {
      stderr += chunk;
    });
    child.on('error', reject);
    child.on('close', code =>
      code === 0
        ? resolve({ stdout, stderr, exitCode: code })
        : reject(new Error(`${request.command} exited ${code}: ${(stderr || stdout).trim()}`)),
    );
    child.stdin.on('error', reject);
    child.stdin.end(request.input);
  });

export async function checkedCommand(runner: CommandRunner, request: CommandRequest) {
  const result = await runner(request);
  if (result.exitCode !== undefined && result.exitCode !== 0) {
    throw new Error(
      `${request.command} exited ${result.exitCode}: ${(result.stderr || result.stdout).trim()}`,
    );
  }
  return result;
}

export function renderWave(batch: readonly MessageEnvelope[]): string {
  return batch
    .map(
      e =>
        `Subject: ${e.subject}\nMessage: ${e.id}\n${e.body}\nPayload: ${JSON.stringify(e.payload)}\nAttachments: ${JSON.stringify(e.attachments)}`,
    )
    .join('\n\n---\n\n');
}

export interface AgentOptions {
  command?: string;
  args?: readonly string[];
  cwd?: string;
  commandRunner?: CommandRunner;
}

function agentArgs(command: string): string[] {
  return command === 'codex' ? ['exec', '-'] : command === 'claude' ? ['-p'] : [];
}

export function createAgentPtyExecutorPlugin(options: AgentOptions = {}): ExecutorPlugin {
  return {
    name: 'agent-pty',
    async execute(batch, context) {
      const command = options.command ?? 'codex';
      const output = await checkedCommand(options.commandRunner ?? runCommand, {
        command,
        args: options.args ?? agentArgs(command),
        cwd: options.cwd,
        input: renderWave(batch),
        signal: context.signal,
      });
      let structuredOutput: unknown;
      try {
        structuredOutput = JSON.parse(output.stdout);
      } catch {
        structuredOutput = output.stdout;
      }
      return {
        executor: 'agent-pty',
        executionId: randomUUID(),
        output: structuredOutput,
        stderr: output.stderr ?? '',
      };
    },
  };
}

export interface OrcaSpawnOptions extends AgentOptions {
  namePrefix?: string;
  orcaCommand?: string;
  parentWorktree?: string;
  noParent?: boolean;
  seaCreatureNaming?: boolean;
  agySettingsPath?: string;
  interactiveCommand?: string;
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

/**
 * Trust a workspace in Antigravity by adding it to trustedWorkspaces.
 * Missing settings files start from an empty object, then the file is replaced atomically.
 */
export function trustAntigravityWorkspace(workspacePath: string, settingsPath?: string): void {
  const file = settingsPath ?? join(homedir(), '.gemini/antigravity-cli/settings.json');
  let settings: Record<string, unknown> = {};
  if (existsSync(file)) {
    const parsed: unknown = JSON.parse(readFileSync(file, 'utf8'));
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      throw new Error(`Antigravity settings at ${file} must be a JSON object`);
    }
    settings = parsed as Record<string, unknown>;
  }
  const trusted = Array.isArray(settings.trustedWorkspaces)
    ? settings.trustedWorkspaces.filter((entry): entry is string => typeof entry === 'string')
    : [];
  const candidates = existsSync(workspacePath)
    ? [workspacePath, realpathSync(workspacePath)]
    : [workspacePath];
  for (const candidate of candidates) {
    if (!trusted.includes(candidate)) {
      trusted.push(candidate);
    }
  }
  settings.trustedWorkspaces = trusted;
  mkdirSync(dirname(file), { recursive: true });
  const temporary = `${file}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(settings, null, 2)}\n`, { mode: 0o600 });
  renameSync(temporary, file);
}

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

export function createOrcaSpawnExecutorPlugin(options: OrcaSpawnOptions = {}): ExecutorPlugin {
  return {
    name: 'orca-spawn',
    async execute(batch, context, runtimeConfig) {
      const runner = options.commandRunner ?? runCommand;
      const command = options.orcaCommand ?? 'orca';
      const payload = (batch[0]?.payload ?? {}) as Record<string, unknown>;
      const config = (runtimeConfig ?? {}) as Partial<OrcaSpawnOptions>;
      const configuredCwd =
        (typeof config.cwd === 'string' && config.cwd ? config.cwd : undefined) ?? options.cwd;
      let parentWorktree: string | undefined = configuredCwd ? `path:${configuredCwd}` : 'active';
      for (const hierarchy of [config, options, payload]) {
        if (hierarchy.noParent === true || hierarchy.parentWorktree === 'none') {
          parentWorktree = undefined;
          break;
        }
        if (typeof hierarchy.parentWorktree === 'string' && hierarchy.parentWorktree) {
          parentWorktree = hierarchy.parentWorktree;
          break;
        }
      }
      const repoPath =
        typeof payload.repoPath === 'string' && payload.repoPath
          ? payload.repoPath
          : typeof payload.cwd === 'string' && payload.cwd
            ? payload.cwd
            : undefined;
      // Receiver configuration selects the local checkout; payload paths support loopback.
      const worktreeCwd =
        configuredCwd ??
        repoPath ??
        (parentWorktree?.startsWith('path:') ? parentWorktree.slice(5) : undefined);
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
      const terminalPrompt =
        typeof payload.terminalPrompt === 'string' && payload.terminalPrompt.trim()
          ? payload.terminalPrompt
          : undefined;
      if (terminalPrompt) {
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
          payload.trustAntigravity !== false &&
          (existsSync(settingsFile) ||
            explicitSettingsPath !== undefined ||
            optionSettingsPath !== undefined)
        ) {
          trustAntigravityWorkspace(path, settingsFile);
        }
        const interactiveCommand =
          typeof config.interactiveCommand === 'string' && config.interactiveCommand.trim()
            ? config.interactiveCommand.trim()
            : (options.interactiveCommand ??
              (existsSync(join(homedir(), '.local/bin/magpie'))
                ? `${shellQuote(join(homedir(), '.local/bin/magpie'))} agy run --dangerously-skip-permissions`
                : shellQuote(options.command ?? 'codex')));
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
          try {
            await runner({
              command,
              args: [
                'terminal',
                'wait',
                '--terminal',
                String(terminalHandle),
                '--for',
                'tui-idle',
                '--timeout-ms',
                '30000',
                '--json',
              ],
              cwd: path,
              signal: context.signal,
            });
          } catch {
            // A wait timeout does not invalidate an already-idle TUI; send the prompt regardless.
          }
          await checkedCommand(runner, {
            command,
            args: [
              'terminal',
              'send',
              '--terminal',
              String(terminalHandle),
              '--text',
              terminalPrompt,
              '--enter',
              '--wait-submit',
              '5',
              '--json',
            ],
            cwd: path,
            signal: context.signal,
          });
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
  runId?: string;
  terminalHandle?: string;
  cwd?: string;
  orcaCommand?: string;
  commandRunner?: CommandRunner;
  idleCheck?: () => Promise<{ idle: boolean; reason?: string }>;
}

interface OrcaTerminalRow {
  handle: string;
  connected: boolean;
  writable?: boolean;
  preview?: unknown;
  agentIdentity?: unknown;
  lastOutputAt?: unknown;
}

/** Resolve the freshest connected antigravity terminal in an Orca workspace, spawning one if needed. */
export async function resolveOrcaWorkspaceTerminal(
  runner: CommandRunner,
  command: string,
  cwd: string,
  signal?: AbortSignal,
): Promise<string> {
  const listed = await checkedCommand(runner, {
    command,
    args: ['terminal', 'list', '--worktree', `path:${cwd}`, '--json'],
    cwd,
    signal,
  });
  const terminals = (JSON.parse(listed.stdout)?.result?.terminals ?? []) as OrcaTerminalRow[];
  const connected = terminals.filter(t => t && t.connected && t.writable !== false);
  const preferred = connected.filter(t => t.agentIdentity === 'antigravity');
  const matched = (preferred.length ? preferred : connected).sort(
    (a, b) => (Number(b.lastOutputAt) || 0) - (Number(a.lastOutputAt) || 0),
  );
  if (matched.length) {
    return String(matched[0].handle);
  }
  const spawned = await checkedCommand(runner, {
    command,
    args: [
      'terminal',
      'spawn',
      '--worktree',
      `path:${cwd}`,
      '--agent',
      'antigravity',
      '--wait',
      '--json',
    ],
    cwd,
    signal,
  });
  const raw = JSON.parse(spawned.stdout);
  const handle = raw.result?.terminal?.handle ?? raw.result?.handle;
  if (handle === undefined || handle === null || handle === '') {
    throw new Error('Orca terminal spawn returned no handle');
  }
  return String(handle);
}

export function createOrcaWaveExecutorPlugin(options: OrcaWaveOptions): ExecutorPlugin {
  const runner = options.commandRunner ?? runCommand;
  const command = options.orcaCommand ?? 'orca';
  const resolveTarget = (
    runtimeConfig?: Partial<OrcaWaveOptions>,
    batch?: readonly MessageEnvelope[],
  ) => {
    const payload = batch?.[0]?.payload ?? {};
    return {
      cwd:
        typeof payload.workspacePath === 'string' && payload.workspacePath
          ? payload.workspacePath
          : typeof payload.cwd === 'string' && payload.cwd
            ? payload.cwd
            : (runtimeConfig?.cwd ?? options.cwd),
      runId:
        typeof payload.runId === 'string' && payload.runId
          ? payload.runId
          : (runtimeConfig?.runId ?? options.runId),
      terminalHandle:
        typeof payload.terminalHandle === 'string' && payload.terminalHandle
          ? payload.terminalHandle
          : (runtimeConfig?.terminalHandle ?? options.terminalHandle),
    };
  };
  const checkIdle = async (runtimeConfig?: Partial<OrcaWaveOptions>) => {
    if (options.idleCheck) {
      return options.idleCheck();
    }
    const target = resolveTarget(runtimeConfig);
    if (!target.cwd) {
      return { idle: false, reason: 'cwd or authoritative idleCheck required' };
    }
    const output = await checkedCommand(runner, {
      command,
      args: ['worktree', 'ps', '--agent-wait', '--limit', '10000', '--json'],
      cwd: target.cwd,
    });
    const raw = JSON.parse(output.stdout);
    const inventory = raw.result;
    if (raw.ok !== true || !inventory || inventory.truncated || !Array.isArray(inventory.worktrees)) {
      return { idle: false, reason: 'Incomplete workspace inventory' };
    }
    type Workspace = {
      path: string;
      worktreeId: string;
      childWorktreeIds?: string[];
      unverifiableTerminalCount?: number;
      agents?: { state?: string }[];
    };
    const rows: Workspace[] = inventory.worktrees;
    const root = rows.find(row => row.path === target.cwd);
    if (!root) {
      return { idle: false, reason: 'Workspace not found' };
    }
    const visited = new Set<string>();
    const idleTree = async (row: Workspace): Promise<boolean> => {
      if (visited.has(row.worktreeId)) {
        return false;
      }
      visited.add(row.worktreeId);
      if (
        row.unverifiableTerminalCount !== 0 ||
        !row.agents ||
        row.agents.some(agent => agent.state !== 'done')
      ) {
        return false;
      }
      const listed = await checkedCommand(runner, {
        command,
        args: ['terminal', 'list', '--worktree', `path:${row.path}`, '--json'],
        cwd: target.cwd,
      });
      const terminals = JSON.parse(listed.stdout);
      if (
        terminals.ok !== true ||
        terminals.result?.truncated ||
        !Array.isArray(terminals.result?.terminals)
      ) {
        return false;
      }
      const handles: OrcaTerminalRow[] = terminals.result.terminals;
      // A chat-only or otherwise unobservable agent needs an injected authoritative idleCheck.
      if (handles.length < row.agents.length) {
        return false;
      }
      for (const terminal of handles) {
        if (!terminal.connected || !terminal.writable) {
          return false;
        }
        const realOrcaTerminal =
          typeof terminal.preview === 'string' || typeof terminal.agentIdentity === 'string';
        if (!realOrcaTerminal) {
          // Synthetic or older inventory without TUI metadata: ask Orca directly.
          try {
            const waited = await checkedCommand(runner, {
              command,
              args: [
                'terminal',
                'wait',
                '--terminal',
                terminal.handle,
                '--for',
                'tui-idle',
                '--timeout-ms',
                '1',
                '--json',
              ],
              cwd: target.cwd,
            });
            if (JSON.parse(waited.stdout).ok !== true) {
              return false;
            }
          } catch {
            return false;
          }
          continue;
        }
        if (typeof terminal.preview === 'string') {
          // A trailing "> " is an unsent composer draft; a bare ">" is an idle prompt.
          const lastLine = terminal.preview
            .split(/\r?\n/)
            .filter(line => line.trim())
            .at(-1);
          if (lastLine?.trimStart().startsWith('> ')) {
            return false;
          }
        }
      }
      for (const id of row.childWorktreeIds ?? []) {
        const child = rows.find(item => item.worktreeId === id);
        if (!child || !(await idleTree(child))) {
          return false;
        }
      }
      return true;
    };
    const idle = await idleTree(root);
    return { idle, reason: idle ? undefined : 'Workspace or descendant is busy or unverifiable' };
  };
  return {
    name: 'orca-wave',
    checkIdle,
    async execute(batch, context, runtimeConfig) {
      const target = resolveTarget(runtimeConfig as Partial<OrcaWaveOptions>, batch);
      if (!(await checkIdle(target)).idle) {
        throw new Error('Orca workspace is busy');
      }
      let terminalHandle = target.terminalHandle;
      if (!target.runId && !terminalHandle && target.cwd) {
        terminalHandle = await resolveOrcaWorkspaceTerminal(
          runner,
          command,
          target.cwd,
          context.signal,
        );
      }
      if (!target.runId && !terminalHandle) {
        throw new Error('Orca wave requires runId or terminalHandle');
      }
      const text =
        !target.runId &&
        batch.some(e => typeof e.payload?.terminalPrompt === 'string' && e.payload.terminalPrompt)
          ? batch
              .map(e =>
                String(e.payload?.terminalPrompt || e.body)
                  .replace(/\s*\n+\s*/g, ' ')
                  .trim(),
              )
              .join(' ； ')
          : renderWave(batch);
      const args = target.runId
        ? [
            'orchestration',
            'send',
            '--to',
            `run:${target.runId}`,
            '--subject',
            `Dealer wave (${batch.length})`,
            '--body',
            renderWave(batch),
            '--json',
          ]
        : [
            'terminal',
            'send',
            '--terminal',
            terminalHandle!,
            '--text',
            text,
            '--enter',
            '--json',
          ];
      const output = await checkedCommand(runner, {
        command,
        args,
        cwd: target.cwd,
        signal: context.signal,
      });
      const delivery = JSON.parse(output.stdout);
      if (delivery.ok === false) {
        throw new Error('Orca wave delivery failed');
      }
      const comment = await checkedCommand(runner, {
        command,
        args: [
          'worktree',
          'set',
          '--worktree',
          target.cwd ? `path:${target.cwd}` : 'current',
          '--comment',
          String(
            batch.at(-1)?.payload?.workspaceComment ||
              `Dealer: dispatched ${batch.length} message(s)`,
          ),
          '--json',
        ],
        cwd: target.cwd,
        signal: context.signal,
      });
      if (JSON.parse(comment.stdout).ok === false) {
        throw new Error('Orca comment update failed after delivery');
      }
      return { executor: 'orca-wave', executionId: randomUUID(), delivery };
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
