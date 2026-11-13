import { readFileSync, writeFileSync, renameSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { isValidMailboxId } from '../core-types/index.ts';
import type { AdmissionMode, NetworkMountConfig } from '../core-types/index.ts';

export interface BuiltInProfile {
  name: string;
  gateMode: AdmissionMode;
  policies: string[];
  executors: string[];
  defaultExecutor: string;
}

export const BUILT_IN_PROFILES: Readonly<Record<string, BuiltInProfile>> = Object.freeze({
  'personal-dev': {
    name: 'personal-dev',
    gateMode: 'open',
    policies: ['dedup-cas'],
    executors: ['orca', 'orca-spawn', 'orca-wave', 'codex-cli', 'codex', 'claude-code', 'antigravity-cli', 'antigravity', 'claude-app', 'chatgpt-app', 'antigravity-app', 'agent-pty', 'notify', 'inbox'],
    defaultExecutor: 'orca-spawn',
  },
  'dedicated-worker': {
    name: 'dedicated-worker',
    gateMode: 'open',
    policies: ['dedup-cas'],
    executors: ['orca', 'orca-spawn', 'orca-wave', 'codex-cli', 'codex', 'claude-code', 'antigravity-cli', 'antigravity', 'claude-app', 'chatgpt-app', 'antigravity-app', 'agent-pty', 'inbox'],
    defaultExecutor: 'orca-spawn',
  },
  'orca-dev': {
    name: 'orca-dev',
    gateMode: 'open',
    policies: ['dedup-cas'],
    executors: ['orca-spawn', 'orca-wave', 'notify', 'inbox'],
    defaultExecutor: 'orca-spawn',
  },
  'codex-dev': {
    name: 'codex-dev',
    gateMode: 'open',
    policies: ['dedup-cas'],
    executors: ['codex', 'agent-pty', 'notify', 'inbox'],
    defaultExecutor: 'codex',
  },
  'claude-dev': {
    name: 'claude-dev',
    gateMode: 'open',
    policies: ['dedup-cas'],
    executors: ['claude-code', 'agent-pty', 'notify', 'inbox'],
    defaultExecutor: 'claude-code',
  },
  'antigravity-dev': {
    name: 'antigravity-dev',
    gateMode: 'open',
    policies: ['dedup-cas'],
    executors: ['antigravity', 'orca-spawn', 'orca-wave', 'agent-pty', 'notify', 'inbox'],
    defaultExecutor: 'antigravity',
  },
  'headless-relay': {
    name: 'headless-relay',
    gateMode: 'open',
    policies: [],
    executors: ['notify', 'inbox'],
    defaultExecutor: 'notify',
  },
});

export function getBuiltInProfile(name: string): BuiltInProfile {
  const profile = Object.hasOwn(BUILT_IN_PROFILES, name) && BUILT_IN_PROFILES[name];
  if (!profile) {
    throw new Error(`Unknown profile: ${name}`);
  }
  return structuredClone(profile);
}

export interface PluginSpec {
  type: string;
  config: Record<string, any>;
}

export interface DealerProjectPluginRef {
  name: string;
  module: string;
  config?: Record<string, any>;
}

export interface DealerTopicSpec {
  description: string;
  defaultExecutor?: string;
  streamKey?: string;
}

export interface DealerProjectOnboardingPreset {
  networkTopology?: 'fixed' | 'selectable';
  passphrase?: string;
  bootstrapPeers?: string[];
  networkId?: string;
  requireLarkCli?: boolean;
  notifyLark?: boolean;
  defaultTopic?: string;
  defaultConversationMode?: 'spawn' | 'fixed' | 'inbox-only';
  teamRegistryPath?: string;
}

export interface DealerProjectManifest {
  schema: 'the-dealer-project/v1';
  projectId: string;
  defaultNetworkId: string;
  description?: string;
  skills?: string[];
  plugins?: DealerProjectPluginRef[];
  topics?: Record<string, DealerTopicSpec>;
  onboarding?: DealerProjectOnboardingPreset;
}

export function redactProjectManifest(manifest: DealerProjectManifest): DealerProjectManifest {
  const redacted = structuredClone(manifest);
  if (redacted.onboarding) {
    delete redacted.onboarding.passphrase;
  }
  return redacted;
}

export interface LinkedProjectConfig {
  projectId: string;
  rootPath: string;
  userPatchPath?: string;
}

function isRecord(value: unknown): value is Record<string, any> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function nonempty(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function validProjectId(value: unknown): value is string {
  return typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(value);
}

export function validatePluginRefs(refs: unknown): asserts refs is DealerProjectPluginRef[] {
  if (!Array.isArray(refs)) {
    throw new Error('Plugins must be an array');
  }
  const names = new Set<string>();
  for (const ref of refs) {
    if (!isRecord(ref) || !nonempty(ref.name) || !nonempty(ref.module) ||
        (ref.config !== undefined && !isRecord(ref.config))) {
      throw new Error('Invalid plugin reference');
    }
    if (names.has(ref.name)) {
      throw new Error(`Duplicate plugin reference: ${ref.name}`);
    }
    names.add(ref.name);
  }
}

function validateManifest(manifest: DealerProjectManifest): void {
  if (!isRecord(manifest) || manifest.schema !== 'the-dealer-project/v1' ||
      !validProjectId(manifest.projectId) || !nonempty(manifest.defaultNetworkId)) {
    throw new Error('Invalid project manifest schema, projectId or defaultNetworkId');
  }
  if (manifest.description !== undefined && typeof manifest.description !== 'string') {
    throw new Error('Invalid project description');
  }
  if (manifest.skills !== undefined &&
      (!Array.isArray(manifest.skills) || manifest.skills.some(skill => !nonempty(skill)))) {
    throw new Error('Invalid project skills');
  }
  if (manifest.onboarding !== undefined) {
    const preset = manifest.onboarding;
    if (!isRecord(preset) ||
        (preset.networkTopology !== undefined && !['fixed', 'selectable'].includes(preset.networkTopology)) ||
        ['requireLarkCli', 'notifyLark'].some(key => preset[key] !== undefined && typeof preset[key] !== 'boolean') ||
        ['networkId', 'defaultTopic', 'teamRegistryPath', 'passphrase'].some(key => preset[key] !== undefined && !nonempty(preset[key])) ||
        (preset.bootstrapPeers !== undefined && (!Array.isArray(preset.bootstrapPeers) || !preset.bootstrapPeers.every(nonempty))) ||
        (preset.defaultConversationMode !== undefined && !['spawn', 'fixed', 'inbox-only'].includes(preset.defaultConversationMode))) {
      throw new Error('Invalid project onboarding preset');
    }
  }
  validatePluginRefs(manifest.plugins === undefined ? [] : manifest.plugins);
  if (manifest.topics !== undefined) {
    if (!isRecord(manifest.topics) || Object.values(manifest.topics).some(topic =>
      !isRecord(topic) || typeof topic.description !== 'string' ||
      (topic.defaultExecutor !== undefined && !nonempty(topic.defaultExecutor)) ||
      (topic.streamKey !== undefined && !nonempty(topic.streamKey)))) {
      throw new Error('Invalid project topics');
    }
  }
}

export function readProjectManifest(projectRoot: string): DealerProjectManifest {
  const manifest = JSON.parse(readFileSync(join(projectRoot, '.dealer', 'project.json'), 'utf8'));
  validateManifest(manifest);
  return manifest;
}

export function initProjectScaffold(
  projectRoot: string,
  opts: {
    projectId: string;
    defaultNetworkId: string;
    description?: string;
    skills?: string[];
    /** Seed endpoints published in the manifest so every member joins through them. */
    bootstrapPeers?: string[];
  },
): DealerProjectManifest {
  const manifest: DealerProjectManifest = {
    schema: 'the-dealer-project/v1',
    projectId: opts.projectId,
    defaultNetworkId: opts.defaultNetworkId,
    ...(opts.description !== undefined ? { description: opts.description } : {}),
    ...(opts.skills !== undefined ? { skills: opts.skills } : {}),
    ...(opts.bootstrapPeers?.length ? { onboarding: { bootstrapPeers: opts.bootstrapPeers } } : {}),
  };
  validateManifest(manifest);
  mkdirSync(join(projectRoot, '.dealer', 'plugins'), { recursive: true });
  writeFileSync(join(projectRoot, '.dealer', 'project.json'), `${JSON.stringify(manifest, null, 2)}\n`, { flag: 'wx' });
  return manifest;
}

export interface TopicBindingConfig {
  noParent?: boolean;
  assignee?: string;
  agent?: 'orca' | 'antigravity' | 'antigravity-cli' | 'antigravity-app' | 'claude' | 'claude-code' | 'claude-app' | 'codex' | 'codex-cli' | 'chatgpt-app' | 'chatgpt-dot' | 'grok-bot';
  interactiveCommand?: string;
  mode?: 'spawn' | 'wave' | 'fixed' | 'inbox-only';
  conversationMode?: 'spawn' | 'wave' | 'fixed' | 'inbox-only';
  targetBot?: string;
  agentProfile?: string;
  sessionId?: string;
  executor: string;
  streamKey?: string;
  mailboxId?: string;
  projectId?: string;
  workspacePath?: string;
  worktreeId?: string;
  runId?: string;
  terminalHandle?: string;
  autoCreateWorkspace?: boolean;
  workspaceComment?: string;
  wakeupPrefix?: string;
  coalesceMs?: number;
  rotateAfterWaves?: number;
  notifyLark?: boolean;
}

export interface LarkNotifyConfig {
  enabled: boolean;
  command?: string;
  profile?: string;
  as?: 'bot' | 'user';
  target?: { chatId?: string; userId?: string };
  mailboxes?: string[];
  excludeMailboxes?: string[];
  includeKinds?: string[];
  excludeKinds?: string[];
  skipSelfMessages?: boolean;
  notifyGateApprovals?: boolean;
  mirrorOutboundThreads?: boolean;
  statePath?: string;
  retryBaseMs?: number;
}

export function validateTopicBindings(bindings: unknown, enabledExecutors: readonly string[]): asserts bindings is Record<string, TopicBindingConfig> {
  if (!isRecord(bindings)) throw new Error('Invalid topicBindings');
  const strings = ['assignee', 'agent', 'interactiveCommand', 'mode', 'conversationMode', 'targetBot', 'agentProfile', 'sessionId', 'streamKey', 'mailboxId', 'projectId', 'workspacePath', 'worktreeId', 'runId', 'terminalHandle', 'workspaceComment', 'wakeupPrefix'];
  const booleans = ['noParent', 'autoCreateWorkspace', 'notifyLark'];
  const allowed = new Set(['executor', ...strings, ...booleans, 'coalesceMs', 'rotateAfterWaves']);
  for (const [kind, binding] of Object.entries(bindings)) {
    if (!nonempty(kind) || (kind.includes(':') && (!nonempty(kind.slice(0, kind.indexOf(':'))) || !nonempty(kind.slice(kind.indexOf(':') + 1)))) || !isRecord(binding) || !enabledExecutors.includes(binding.executor) ||
        (binding.agent !== undefined && !['orca', 'antigravity', 'antigravity-cli', 'antigravity-app', 'claude', 'claude-code', 'claude-app', 'codex', 'codex-cli', 'chatgpt-app', 'chatgpt-dot', 'grok-bot'].includes(binding.agent)) ||
        (binding.mode !== undefined && !['spawn', 'wave', 'fixed', 'inbox-only'].includes(binding.mode)) ||
        (binding.conversationMode !== undefined && !['spawn', 'wave', 'fixed', 'inbox-only'].includes(binding.conversationMode)) ||
        (binding.mode !== undefined && binding.conversationMode !== undefined && binding.mode !== binding.conversationMode) ||
        ((binding.mode ?? binding.conversationMode) === 'inbox-only' && binding.executor !== 'inbox') ||
        (['orca', 'orca-wave', 'orca-spawn'].includes(binding.executor) && binding.agent !== undefined && !['antigravity', 'antigravity-cli', 'claude', 'claude-code', 'codex', 'codex-cli'].includes(binding.agent)) ||
        Object.keys(binding).some(key => !allowed.has(key)) ||
        strings.some(key => binding[key] !== undefined && !nonempty(binding[key])) ||
        booleans.some(key => binding[key] !== undefined && typeof binding[key] !== 'boolean') ||
        (binding.coalesceMs !== undefined && (!Number.isSafeInteger(binding.coalesceMs) || binding.coalesceMs < 0)) ||
        (binding.rotateAfterWaves !== undefined && (!Number.isSafeInteger(binding.rotateAfterWaves) || binding.rotateAfterWaves < 1)) ||
        (binding.mailboxId !== undefined && !isValidMailboxId(binding.mailboxId)) ||
        (binding.streamKey !== undefined && !isValidMailboxId(`stream:${binding.streamKey}`)) ||
        (((binding.executor === 'orca-wave' || (binding.executor === 'orca' && (binding.mode ?? binding.conversationMode) !== undefined && (binding.mode ?? binding.conversationMode) !== 'spawn')) || (binding.mode ?? binding.conversationMode) === 'wave') && binding.mailboxId === undefined && !isValidMailboxId(`stream:${binding.streamKey ?? (kind.includes(':') ? kind.slice(kind.indexOf(':') + 1) : kind)}`)) ||
        (binding.projectId !== undefined && !validProjectId(binding.projectId))) {
      throw new Error(`Invalid topic binding: ${kind}`);
    }
  }
}

export function validateLarkNotify(config: unknown): asserts config is LarkNotifyConfig {
  const strings = ['command', 'profile', 'statePath'];
  const arrays = ['mailboxes', 'excludeMailboxes', 'includeKinds', 'excludeKinds'];
  const booleans = ['skipSelfMessages', 'notifyGateApprovals', 'mirrorOutboundThreads'];
  const allowed = new Set(['enabled', 'as', 'target', 'retryBaseMs', ...booleans, ...strings, ...arrays]);
  if (!isRecord(config) || typeof config.enabled !== 'boolean' ||
      Object.keys(config).some(key => !allowed.has(key)) ||
      strings.some(key => config[key] !== undefined && !nonempty(config[key])) ||
      arrays.some(key => config[key] !== undefined && (!Array.isArray(config[key]) || config[key].some((value: unknown) => !nonempty(value)))) ||
      (config.as !== undefined && !['bot', 'user'].includes(config.as)) ||
      booleans.some(key => config[key] !== undefined && typeof config[key] !== 'boolean') ||
      (config.retryBaseMs !== undefined && (!Number.isSafeInteger(config.retryBaseMs) || config.retryBaseMs < 1))) {
    throw new Error('Invalid larkNotify');
  }
  if (config.target !== undefined && (!isRecord(config.target) ||
      Object.keys(config.target).some(key => !['chatId', 'userId'].includes(key)) ||
      ['chatId', 'userId'].some(key => config.target[key] !== undefined && !nonempty(config.target[key])) ||
      (config.target.chatId !== undefined && config.target.userId !== undefined))) {
    throw new Error('Invalid larkNotify target');
  }
  for (const key of ['mailboxes', 'excludeMailboxes']) {
    if (config[key]?.some((id: string) => !isValidMailboxId(id))) throw new Error('Invalid notification mailbox');
  }
}

export interface RuntimeConfig {
  topicBindings?: Record<string, TopicBindingConfig>;
  larkNotify?: LarkNotifyConfig;
  profile: string;
  member: string;
  name: string;
  skills: string[];
  port?: number;
  gateMode: AdmissionMode;
  networks: (NetworkMountConfig & { role?: string })[];
  defaultExecutor: string;
  policies: string[];
  executors: string[];
  executorConfig: Record<string, Record<string, any>>;
  projects: LinkedProjectConfig[];
  userPlugins: DealerProjectPluginRef[];
  routes: PluginSpec[];
  sources: PluginSpec[];
  wave: {
    rotateAfterWaves?: number;
    minAgeMs?: number;
    flushIntervalMs?: number;
  };
}

export function admissionMode(value: string): AdmissionMode {
  if (!['open', 'approval_required', 'dnd', 'closed'].includes(value)) {
    throw new Error(`Invalid gate mode: ${value}`);
  }
  return value as AdmissionMode;
}

export function saveJson(path: string, data: unknown): void {
  const temporary = `${path}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(data, null, 2)}\n`, { mode: 0o600, flag: 'wx' });
  renameSync(temporary, path);
}

/**
 * Deliberately small YAML subset: nested mappings with JSON scalar/array values.
 * Reject ambiguous YAML rather than silently applying a different configuration.
 */
export function readPatch(path: string): Partial<RuntimeConfig> {
  const text = readFileSync(path, 'utf8');
  if (text.trimStart().startsWith('{')) {
    return JSON.parse(text);
  }
  const root: Record<string, any> = {};
  const stack = [{ indent: -1, value: root }];
  for (const line of text.split(/\r?\n/)) {
    if (!line.trim() || line.trimStart().startsWith('#')) {
      continue;
    }
    const match = /^( *)([A-Za-z][\w-]*):(?: +(.*))?$/.exec(line);
    if (!match) {
      throw new Error('Unsupported YAML: use nested mappings and JSON arrays/scalars');
    }
    const [, spaces, key, raw] = match;
    if (['__proto__', 'constructor', 'prototype'].includes(key)) {
      throw new Error('Unsafe patch key');
    }
    while (stack.at(-1)!.indent >= spaces.length) {
      stack.pop();
    }
    const parent = stack.at(-1)!.value;
    if (Object.hasOwn(parent, key)) {
      throw new Error(`Duplicate patch key: ${key}`);
    }
    if (raw === undefined || raw === '') {
      parent[key] = {};
      stack.push({ indent: spaces.length, value: parent[key] });
    } else {
      try {
        parent[key] = JSON.parse(raw);
      } catch {
        if (!/^[\w./@-]+$/.test(raw)) {
          throw new Error('Quote YAML strings with JSON double quotes');
        }
        parent[key] = raw;
      }
    }
  }
  return root;
}

export const BUILTIN_EXECUTORS = ['orca', 'orca-spawn', 'orca-wave', 'codex-cli', 'codex', 'claude-code', 'antigravity-cli', 'antigravity', 'claude-app', 'chatgpt-app', 'antigravity-app', 'agent-pty', 'notify', 'inbox'];

export function validateConfig(config: RuntimeConfig): void {
  validatePluginRefs(config.userPlugins);
  validateTopicBindings(config.topicBindings === undefined ? {} : config.topicBindings, config.executors);
  if (config.larkNotify !== undefined) validateLarkNotify(config.larkNotify);
  if (!Array.isArray(config.projects)) {
    throw new Error('Projects must be an array');
  }
  const projectIds = new Set<string>();
  for (const project of config.projects) {
    if (!isRecord(project) || !validProjectId(project.projectId) || !nonempty(project.rootPath) ||
        (project.userPatchPath !== undefined && !nonempty(project.userPatchPath))) {
      throw new Error('Invalid linked project');
    }
    if (projectIds.has(project.projectId)) {
      throw new Error(`Duplicate linked project: ${project.projectId}`);
    }
    projectIds.add(project.projectId);
  }
  getBuiltInProfile(config.profile);
  admissionMode(config.gateMode);
  if (!config.member || !config.name || !Array.isArray(config.skills) || config.skills.some(s => typeof s !== 'string')) {
    throw new Error('Invalid node profile');
  }
  if (config.port !== undefined && (!Number.isInteger(config.port) || config.port < 0 || config.port > 65535)) {
    throw new Error('Invalid port');
  }
  if (!config.executors.includes(config.defaultExecutor)) {
    throw new Error('Default executor must be enabled');
  }
  if (config.executors.some(e => !BUILTIN_EXECUTORS.includes(e))) {
    throw new Error('Unknown executor');
  }
  if (!isRecord(config.executorConfig)) {
    throw new Error('Invalid executorConfig');
  }
  for (const [executor, options] of Object.entries(config.executorConfig)) {
    if (!BUILTIN_EXECUTORS.includes(executor)) {
      throw new Error('Unknown executor');
    }
    if (!isRecord(options)) {
      throw new Error('Invalid executorConfig');
    }
  }
  if (!Array.isArray(config.policies)) {
    throw new Error('Policies must be an array');
  }
  // Legacy configs persist the retired wave-coalescer policy; durable mailbox wakeup replaced it.
  config.policies = config.policies.filter(policy => policy !== 'wave-coalescer');
  if (config.policies.some(policy => policy !== 'dedup-cas')) {
    throw new Error('Unknown policy');
  }
  for (const network of config.networks) {
    if (!network.networkId || !network.passphrase) {
      throw new Error('Network and passphrase required');
    }
    if (network.admissionMode) {
      admissionMode(network.admissionMode);
    }
  }
}

export function encodeNetworkInvite(network: NetworkMountConfig, ttlMs = 86_400_000): string {
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
    throw new Error('Invalid invite TTL');
  }
  const url = new URL(`dealer-net://v1/${encodeURIComponent(network.networkId)}`);
  url.searchParams.set('passphrase', network.passphrase);
  url.searchParams.set('expires', String(Date.now() + ttlMs));
  for (const peer of network.bootstrapPeers ?? []) {
    url.searchParams.append('seed', peer);
  }
  return url.toString();
}

export function decodeNetworkInvite(uri: string): NetworkMountConfig {
  const url = new URL(uri);
  if (url.protocol !== 'dealer-net:' || url.hostname !== 'v1' || !url.pathname.slice(1) || url.hash) {
    throw new Error('Invalid network invite');
  }
  const expires = Number(url.searchParams.get('expires'));
  if (!Number.isFinite(expires) || expires <= Date.now()) {
    throw new Error('Network invite expired or missing expiry');
  }
  const passphrase = url.searchParams.get('passphrase');
  if (!passphrase) {
    throw new Error('Invite has no passphrase');
  }
  return {
    networkId: decodeURIComponent(url.pathname.slice(1)),
    passphrase,
    bootstrapPeers: url.searchParams.getAll('seed'),
  };
}
