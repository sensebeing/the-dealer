import { accessSync, constants, existsSync, readFileSync, statSync } from 'node:fs';
import { homedir } from 'node:os';
import { delimiter, join, resolve } from 'node:path';
import { validateTopicBindings } from './config.ts';
import type { TopicBindingConfig } from './config.ts';
import type { AgentConversationMode, SupportedAgentHost } from '../plugins-executor/index.ts';

export const AGENT_HOST_CATALOG = [
  { host: 'orca', command: 'orca', modes: ['spawn', 'fixed', 'inbox-only'], selectors: ['agent', 'workspacePath'], description: 'Orca managed worktrees and idle-gated fixed conversations.' },
  { host: 'antigravity', command: 'agy', modes: ['spawn', 'fixed', 'inbox-only'], selectors: ['workspacePath'], description: 'Antigravity in Orca managed worktrees; standalone agy is also available through agent-pty.' },
  { host: 'claude-code', command: 'claude', modes: ['spawn', 'fixed', 'inbox-only'], selectors: ['agentProfile', 'sessionId'], description: 'Claude Code background conversations and resumed sessions.' },
  { host: 'codex', command: 'codex', modes: ['spawn', 'fixed', 'inbox-only'], selectors: ['agentProfile', 'sessionId'], description: 'Codex worktree execution and fixed thread queues.' },
  { host: 'chatgpt-dot', command: 'dot', modes: ['spawn', 'fixed', 'inbox-only'], selectors: ['agentProfile', 'targetBot', 'sessionId'], description: 'ChatGPT Dot bridge with explicit session keys.' },
  { host: 'grok-bot', command: 'grok-bot', modes: ['spawn', 'fixed', 'inbox-only'], selectors: ['targetBot', 'sessionId'], description: 'Grok bridge dispatch to a selected bot.' },
] as const;

function pathLookup(command: string, env: NodeJS.ProcessEnv): string | undefined {
  for (const dir of (env.PATH ?? '').split(delimiter).filter(Boolean)) {
    const path = join(dir, command);
    try {
      accessSync(path, constants.X_OK);
      if (statSync(path).isFile()) return path;
    } catch (error) {
      if (!['ENOENT', 'EACCES', 'ENOTDIR'].includes((error as NodeJS.ErrnoException).code ?? '')) throw error;
    }
  }
  return undefined;
}

function readOptionalJson(path: string): any {
  return existsSync(path) ? JSON.parse(readFileSync(path, 'utf8')) : undefined;
}

/** Read-only inventory: installed does not imply a bridge supports the required argv contract. */
export function probeDealerSetup(options: {
  stateDir?: string;
  cwd?: string;
  env?: NodeJS.ProcessEnv;
  pathLookup?: (command: string, env: NodeJS.ProcessEnv) => string | undefined;
} = {}) {
  let env = options.env ?? process.env;
  const localBin = join(homedir(), '.local/bin');
  if (!options.pathLookup && !options.env && !(env.PATH ?? '').split(delimiter).includes(localBin)) {
    env = { ...env, PATH: [env.PATH, localBin].filter(Boolean).join(delimiter) };
  }
  const cwd = resolve(options.cwd ?? process.cwd());
  const stateDir = resolve(options.stateDir ?? env.DEALER_STATE_DIR ?? join(env.HOME ?? homedir(), '.dealer'));
  const lookup = options.pathLookup ?? pathLookup;
  const commands = ['orca', 'agy', 'claude', 'codex', 'dot', 'grok-bot', 'lark-cli'];
  const clis = Object.fromEntries(commands.map(command => [command, lookup(command, env) ?? null]));
  const config = readOptionalJson(join(stateDir, 'config.json'));
  const project = readOptionalJson(join(cwd, '.dealer/project.json'));
  const apps = Object.fromEntries(['Orca', 'Antigravity', 'Claude', 'Codex', 'ChatGPT', 'Grok'].map(name => [name,
    [join('/Applications', `${name}.app`), join(env.HOME ?? homedir(), 'Applications', `${name}.app`)].find(existsSync) ?? null,
  ]));
  const recommendedHost = clis.orca ? 'orca' : AGENT_HOST_CATALOG.find(host => host.host !== 'antigravity' && clis[host.command])?.host ?? (clis.agy ? 'antigravity' : null);
  return {
    cwd, stateDir, clis, apps, recommendedHost, agentCatalog: AGENT_HOST_CATALOG, project,
    onboarding: project ? {
      networkTopology: project.onboarding?.networkTopology ?? (typeof project.defaultNetworkId === 'string' && project.defaultNetworkId !== 'local' ? 'fixed' : 'selectable'),
      networkId: project.onboarding?.networkId ?? project.defaultNetworkId ?? 'local',
      requireLarkCli: Boolean(project.onboarding?.requireLarkCli),
      notifyLark: Boolean(project.onboarding?.notifyLark),
      defaultTopic: project.onboarding?.defaultTopic ?? 'collab.request.v4',
      defaultConversationMode: project.onboarding?.defaultConversationMode ?? 'fixed',
      ...(project.onboarding?.teamRegistryPath ? { teamRegistryPath: project.onboarding.teamRegistryPath } : {}),
    } : null,
    node: {
      configured: Boolean(config), controlSocketPresent: existsSync(join(stateDir, 'control.sock')),
      member: config?.member, name: config?.name, profile: config?.profile,
      networks: config?.networks?.map((network: { networkId: string }) => network.networkId) ?? [],
      projects: config?.projects ?? {},
      larkNotify: config?.larkNotify ? {
        enabled: Boolean(config.larkNotify.enabled),
        ...(config.larkNotify.command ? { command: config.larkNotify.command } : {}),
      } : null,
    },
  };
}

const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;

export function buildOnboardingPrompts(options: {
  networkId: string;
  inviteUri: string;
  member?: string;
  peerMember?: string;
  nodeName?: string;
  seed?: string | readonly string[];
  projectId?: string;
}) {
  const { networkId, inviteUri } = options;
  const member = options.member ?? 'me';
  const peerMember = options.peerMember ?? 'teammate';
  const nodeName = options.nodeName ?? 'other-machine';
  const seeds = Array.isArray(options.seed) ? options.seed.join(',') : options.seed;
  const mount = `dealer network mount ${quote(inviteUri)}${seeds ? ` --seed ${quote(String(seeds))}` : ''} --json`;
  const presetOptions = `--host <chosen-app> --mode <chosen-mode> --notify-lark${options.projectId ? ` --project ${quote(options.projectId)}` : ''}`;
  const common = (identity: string, teammate = false) => [
    'Set up the-dealer on this machine. Never overwrite ~/.dealer/identity.json or copy another machine’s private key. First check command -v dealer and node --version (Node >=22.15 required).',
    'If dealer is absent, run npm install -g git+https://github.com/sensebeing/the-dealer. For a local checkout, clone https://github.com/sensebeing/the-dealer.git into ~/the-dealer, or update it with git -C ~/the-dealer pull --ff-only after checking for local changes; then run npm --prefix ~/the-dealer link. Preserve existing installations and configuration.',
    "Install skills with npx skills add sensebeing/the-dealer --global --agent '*'. Preserve existing skill configuration.",
    'For checkout-based skill installation, create ~/.agents/skills/ and ~/.claude/skills/. Symlink each of ~/the-dealer/skills/the-dealer, ~/the-dealer/skills/the-dealer-setup, and ~/the-dealer/skills/the-dealer-extend into both directories, and into ~/.gemini/config/skills/ if that directory exists. Preserve unrelated existing files and resolve conflicting skill links before changing them.',
    'Read the installed the-dealer-setup/SKILL.md, then run dealer setup probe --json before configuring anything.',
    `Network topology is fixed to ${networkId}; do not ask to choose local or another network topology.`,
    'Before applying presets, check command -v lark-cli or ~/.local/bin/lark-cli. If missing, install with npx @larksuite/cli@latest install. Use the discovered executable for all subsequent lark-cli commands. Run LARKSUITE_CLI_NO_UPDATE_NOTIFIER=1 LARKSUITE_CLI_NO_SKILLS_NOTIFIER=1 lark-cli auth status --json --verify. If not_configured, run lark-cli config init --new. If not logged in or expired, run lark-cli auth login --recommend, then re-verify auth status with the same environment flags.',
    ...(teammate ? [`Ask for the teammate’s member handle before continuing; replace ${quote(peerMember)} below with their chosen handle. Ask for their Agent App (orca, antigravity, claude-code, codex, chatgpt-dot, grok-bot) and mode (spawn, fixed, inbox-only).`] : []),
    `dealer node up --member ${quote(identity)} --name ${quote(nodeName)} --json`, mount,
    ...(options.projectId ? [`Link this local repository using dealer project link . --json; verify its projectId is ${quote(options.projectId)}.`] : []),
    ...(teammate ? [`Generate dealer setup binding-preset ${presetOptions} --json (add --target-bot for grok-bot execution), then apply the reviewed preset with dealer setup apply-preset ${presetOptions} --json using the same options (add --set-default only to change the node default).`] : []),
    `dealer peers list --network ${quote(networkId)} --json`,
    `dealer send --network ${quote(networkId)} --to ${quote(`member:${member}`)} --topic setup.ping --body 'Please reply to verify connectivity' --json`,
    'Verify a reply in both directions; a dispatch receipt alone is not task completion. Keep approval_required until the intended peer is verified.',
  ].join('\n');
  return { networkId, inviteUri,
    crossMachinePrompt: common(member),
    teammatePrompt: `${common(peerMember, true)}\nUse a durable inbox for collab.request.v4 with the chosen conversation mode: spawn (new conversation), fixed (dedicated conversation), or inbox-only.`,
  };
}

export function buildAgentBindingPreset(options: {
  host: SupportedAgentHost;
  conversationMode?: AgentConversationMode;
  topic?: string;
  projectId?: string;
  workspacePath?: string;
  agent?: 'antigravity' | 'claude' | 'codex';
  agentProfile?: string;
  targetBot?: string;
  sessionId?: string;
  notifyLark?: boolean;
}) {
  const host = options.host === 'claude' ? 'claude-code' : options.host;
  if (!AGENT_HOST_CATALOG.some(entry => entry.host === host)) throw new Error(`Unsupported agent host: ${host}`);
  const mode = options.conversationMode ?? 'fixed';
  if (!['spawn', 'fixed', 'inbox-only'].includes(mode)) throw new Error(`Invalid conversation mode: ${mode}`);
  if (host === 'grok-bot' && mode !== 'inbox-only' && !options.targetBot?.trim()) throw new Error('grok-bot requires targetBot');
  const managed = host === 'orca' || host === 'antigravity';
  const topic = options.topic ?? 'collab.request.v4';
  const executor = mode === 'inbox-only' ? 'inbox' : managed ? (mode === 'spawn' ? 'orca-spawn' : 'orca-wave') : 'agent-pty';
  const binding: TopicBindingConfig = {
    executor, conversationMode: mode,
    agent: host === 'antigravity' ? 'antigravity' : host === 'orca' ? options.agent ?? 'antigravity' : host,
    ...(mode !== 'spawn' ? { mailboxId: `stream:${topic}` } : {}),
    ...(options.projectId ? { projectId: options.projectId } : {}),
    ...(options.workspacePath ? { workspacePath: resolve(options.workspacePath) } : {}),
    ...(options.agentProfile ? { agentProfile: options.agentProfile } : {}),
    ...(options.targetBot ? { targetBot: options.targetBot } : {}),
    ...(options.sessionId ? { sessionId: options.sessionId } : {}),
    ...(options.notifyLark !== undefined ? { notifyLark: options.notifyLark } : {}),
    ...(managed && mode === 'fixed' ? { autoCreateWorkspace: true } : {}),
  };
  validateTopicBindings({ [topic]: binding }, [executor]);
  const executorConfig = executor === 'orca-spawn'
    ? { [executor]: { agent: binding.agent, interactive: true, ...(binding.workspacePath ? { cwd: binding.workspacePath } : {}) } }
    : {};
  return { topic, binding, executorConfig, summary: `${host}: ${mode} via ${executor}; configure receiver project roots and host CLI before enabling delivery.` };
}
