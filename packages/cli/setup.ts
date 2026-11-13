import { existsSync, readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { redactProjectManifest, readProjectManifest, validateTopicBindings } from './config.ts';
import type { DealerProjectManifest, LarkNotifyConfig, LinkedProjectConfig, TopicBindingConfig } from './config.ts';
import { CONVERSATION_MODES, resolveOrcaWaveRotateAfterWaves } from '../mailbox/index.ts';
import { findExecutable } from '../orca-workspace/executable.ts';
import type { AgentConversationMode, SupportedAgentHost } from '../plugins-executor/index.ts';
import { normalizeConversationModeInput } from '../agent-host/index.ts';

export const AGENT_HOST_CATALOG = [
  { host: 'orca', command: 'orca', surface: 'cli', modes: CONVERSATION_MODES, selectors: ['agent', 'workspacePath'], description: 'Orca managed worktrees and idle-gated conversations.' },
  { host: 'antigravity', command: 'agy', surface: 'cli', modes: CONVERSATION_MODES, selectors: ['workspacePath'], description: 'Antigravity in Orca managed worktrees.' },
  { host: 'claude-code', command: 'claude', surface: 'cli', modes: CONVERSATION_MODES, selectors: ['agentProfile', 'sessionId'], description: 'Claude Code conversations.' },
  { host: 'codex', command: 'codex', surface: 'cli', modes: CONVERSATION_MODES, selectors: ['agentProfile', 'sessionId'], description: 'Codex execution.' },
  { host: 'chatgpt-dot', command: 'dot', surface: 'cli', modes: CONVERSATION_MODES, selectors: ['agentProfile', 'targetBot', 'sessionId'], description: 'ChatGPT Dot bridge.' },
  { host: 'grok-bot', command: 'grok-bot', surface: 'cli', modes: CONVERSATION_MODES, selectors: ['targetBot', 'sessionId'], description: 'Grok bridge.' },
  { host: 'claude-app', command: 'claude-app', surface: 'app', app: 'Claude', modes: CONVERSATION_MODES, selectors: ['agentProfile', 'sessionId'], description: 'Claude app bridge.' },
  { host: 'chatgpt-app', command: 'dot', surface: 'app', app: 'ChatGPT', modes: CONVERSATION_MODES, selectors: ['agentProfile', 'targetBot', 'sessionId'], description: 'ChatGPT app bridge.' },
  { host: 'antigravity-app', command: 'antigravity-app', surface: 'app', app: 'Antigravity', modes: CONVERSATION_MODES, selectors: ['workspacePath'], description: 'Antigravity app bridge.' },
  { host: 'codex-cli', command: 'codex', surface: 'cli', modes: CONVERSATION_MODES, selectors: ['agentProfile', 'sessionId'], description: 'Codex CLI bridge.' },
  { host: 'antigravity-cli', command: 'agy', surface: 'cli', modes: ['spawn', 'inbox-only'] as const, selectors: ['workspacePath'], description: 'Antigravity CLI bridge.' },
] as const;

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
  const env = options.env ?? process.env;
  const cwd = resolve(options.cwd ?? process.cwd());
  const stateDir = resolve(options.stateDir ?? env.DEALER_STATE_DIR ?? join(env.HOME ?? homedir(), '.dealer'));
  const lookup = options.pathLookup ?? ((command, environment) => findExecutable(command, environment, !options.env));
  const commands = [...new Set([...AGENT_HOST_CATALOG.map(entry => entry.command), 'lark-cli'])];
  const clis = Object.fromEntries(commands.map(command => [command, lookup(command, env) ?? null]));
  const config = readOptionalJson(join(stateDir, 'config.json'));
  const project = readOptionalJson(join(cwd, '.dealer/project.json'));
  const apps = Object.fromEntries(['Orca', 'Antigravity', 'Claude', 'Codex', 'ChatGPT', 'Grok'].map(name => [name,
    [join('/Applications', `${name}.app`), join(env.HOME ?? homedir(), 'Applications', `${name}.app`)].find(existsSync) ?? null,
  ]));
  const recommendedHost = clis.orca ? 'orca' : AGENT_HOST_CATALOG.find(host => host.surface === 'cli' && host.host !== 'antigravity' && host.host !== 'antigravity-cli' && clis[host.command])?.host ?? (clis.agy ? 'antigravity' : AGENT_HOST_CATALOG.find(host => host.surface === 'app' && host.app && apps[host.app] && clis[host.command])?.host ?? null);
  return {
    cwd, stateDir, clis, apps, recommendedHost, agentCatalog: AGENT_HOST_CATALOG,
    project: project ? redactProjectManifest(project) : project,
    onboarding: project ? {
      networkTopology: project.onboarding?.networkTopology ?? (typeof project.defaultNetworkId === 'string' && project.defaultNetworkId !== 'local' ? 'fixed' : 'selectable'),
      networkId: project.onboarding?.networkId ?? project.defaultNetworkId ?? 'local',
      hasNetworkPassphrase: Boolean(project.onboarding?.passphrase),
      ...(project.onboarding?.bootstrapPeers !== undefined ? { bootstrapPeers: project.onboarding.bootstrapPeers } : {}),
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

export function resolveHostExecutor(host: SupportedAgentHost): string {
  if (host === 'orca' || host === 'antigravity') return 'orca';
  if (host === 'claude') return 'claude-code';
  if (host === 'chatgpt-dot') return 'chatgpt-app';
  if (host === 'grok-bot') return 'agent-pty';
  return host;
}

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
    ...(teammate ? [`Ask for the teammate’s member handle before continuing; replace ${quote(peerMember)} below with their chosen handle. Ask for their Agent App (${AGENT_HOST_CATALOG.map(entry => entry.host).join(', ')}) and mode (${CONVERSATION_MODES.join(', ')}).`] : []),
    `dealer node up --member ${quote(identity)} --name ${quote(nodeName)} --json`, mount,
    ...(options.projectId ? [`Link this local repository using dealer project link . --json; verify its projectId is ${quote(options.projectId)}.`] : []),
    ...(teammate ? [`Generate dealer setup binding-preset ${presetOptions} --json (add --target-bot for grok-bot execution), then apply the reviewed preset with dealer setup apply-preset ${presetOptions} --json using the same options (add --set-default only to change the node default).`] : []),
    `dealer peers list --network ${quote(networkId)} --json`,
    `dealer send --network ${quote(networkId)} --to ${quote(`member:${member}`)} --topic setup.ping --body 'Please reply to verify connectivity' --json`,
    'Verify a reply in both directions; a dispatch receipt alone is not task completion. Built-in profiles default to the open gate; pass --gate approval_required to dealer node up / dealer network mount or run dealer gate mode approval_required when inbound approval is required.',
  ].join('\n');
  return { networkId, inviteUri,
    crossMachinePrompt: common(member),
    teammatePrompt: `${common(peerMember, true)}\nUse a durable inbox for collab.request.v4 with the chosen conversation mode: spawn (new conversation), wave (per-wave session tab in a shared stream), fixed (dedicated conversation), or inbox-only.`,
  };
}

export function buildAgentBindingPreset(options: {
  host: SupportedAgentHost;
  mode?: AgentConversationMode;
  conversationMode?: AgentConversationMode;
  topic?: string;
  projectId?: string;
  workspacePath?: string;
  agent?: 'antigravity' | 'antigravity-cli' | 'claude' | 'claude-code' | 'codex' | 'codex-cli';
  agentProfile?: string;
  targetBot?: string;
  sessionId?: string;
  notifyLark?: boolean;
  /** Session-tab rotation threshold for a managed fixed workspace; defaults to one wave. */
  rotateAfterWaves?: number;
}) {
  const host = options.host === 'claude' ? 'claude-code' : options.host;
  const entry = AGENT_HOST_CATALOG.find(item => item.host === host);
  if (!entry) throw new Error(`Unsupported agent host: ${host}`);
  const mode = normalizeConversationModeInput(options) ?? 'fixed';
  if (!(entry.modes as readonly string[]).includes(mode)) throw new Error(`Invalid conversation mode: ${mode}`);
  if (!['spawn', 'wave', 'fixed', 'inbox-only'].includes(mode)) throw new Error(`Invalid conversation mode: ${mode}`);
  if (host === 'grok-bot' && mode !== 'inbox-only' && !options.targetBot?.trim()) throw new Error('grok-bot requires targetBot');
  const managed = host === 'orca' || host === 'antigravity';
  const topic = options.topic ?? 'collab.request.v4';
  const executor = mode === 'inbox-only' ? 'inbox' : resolveHostExecutor(host);
  const binding: TopicBindingConfig = {
    executor, mode,
    agent: host === 'antigravity' ? 'antigravity' : host === 'orca' ? options.agent ?? 'antigravity' : host,
    ...(mode !== 'spawn' ? { mailboxId: `stream:${topic}` } : {}),
    ...(options.projectId ? { projectId: options.projectId } : {}),
    ...(options.workspacePath ? { workspacePath: resolve(options.workspacePath) } : {}),
    ...(options.agentProfile ? { agentProfile: options.agentProfile } : {}),
    ...(options.targetBot ? { targetBot: options.targetBot } : {}),
    ...(options.sessionId ? { sessionId: options.sessionId } : {}),
    ...(options.notifyLark !== undefined ? { notifyLark: options.notifyLark } : {}),
    ...(managed && (mode === 'fixed' || mode === 'wave')
      ? {
          autoCreateWorkspace: true,
          ...(() => {
            const rotateAfterWaves = resolveOrcaWaveRotateAfterWaves({
              mode: options.mode === 'fixed' ? 'fixed' : 'wave',
              bindingRotateAfterWaves: options.rotateAfterWaves,
              managesWorkspace: true,
            });
            return rotateAfterWaves === undefined ? {} : { rotateAfterWaves };
          })(),
        }
      : {}),
  };
  validateTopicBindings({ [topic]: binding }, [executor]);
  const executorConfig = (executor === 'orca' || executor === 'orca-spawn') && mode === 'spawn'
    ? { [executor]: { agent: binding.agent, interactive: true, ...(binding.workspacePath ? { cwd: binding.workspacePath } : {}) } }
    : {};
  return { topic, binding, executorConfig, summary: `${host}: ${mode} via ${executor}; configure receiver project roots and host CLI before enabling delivery.` };
}

/** Project defaults and node notification effects shared by preview and application. */
export function resolveAgentBindingPreset(
  options: Parameters<typeof buildAgentBindingPreset>[0],
  context: { projects?: DealerProjectManifest[]; larkNotify?: LarkNotifyConfig; cwd?: string } = {},
) {
  const projects = context.projects ?? [];
  let project = options.projectId ? projects.find(project => project.projectId === options.projectId) : undefined;
  if (!project) {
    const cwd = context.cwd ?? process.cwd();
    const localProject = existsSync(join(cwd, '.dealer/project.json')) ? readProjectManifest(cwd) : undefined;
    if (localProject && (!options.projectId || localProject.projectId === options.projectId)) {
      project = projects.find(project => project.projectId === localProject.projectId) ?? localProject;
    }
  }
  if (!project && !options.projectId && projects.length === 1) project = projects[0];
  const preset = buildAgentBindingPreset({ ...options,
    notifyLark: options.notifyLark ?? (project?.onboarding?.notifyLark === true ? true : undefined),
  });
  return { ...preset,
    ...(preset.binding.notifyLark === true ? { larkNotify: { ...context.larkNotify, enabled: true } } : {}),
  };
}

/** Offline previews read receiver-owned linked manifests without starting a node. */
export function readAgentPresetContext(stateDir: string) {
  const config = readOptionalJson(join(stateDir, 'config.json'));
  return {
    projects: (config?.projects ?? []).map((project: LinkedProjectConfig) => readProjectManifest(project.rootPath)),
    larkNotify: config?.larkNotify as LarkNotifyConfig | undefined,
  };
}
