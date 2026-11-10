import { readFileSync, writeFileSync, renameSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
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
    gateMode: 'approval_required',
    policies: ['dedup-cas'],
    executors: ['orca-spawn', 'orca-wave', 'notify', 'inbox'],
    defaultExecutor: 'orca-spawn',
  },
  'dedicated-worker': {
    name: 'dedicated-worker',
    gateMode: 'open',
    policies: ['dedup-cas'],
    executors: ['orca-spawn', 'orca-wave', 'agent-pty', 'inbox'],
    defaultExecutor: 'orca-spawn',
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

export interface DealerProjectManifest {
  schema: 'the-dealer-project/v1';
  projectId: string;
  defaultNetworkId: string;
  description?: string;
  skills?: string[];
  plugins?: DealerProjectPluginRef[];
  topics?: Record<string, DealerTopicSpec>;
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
  opts: { projectId: string; defaultNetworkId: string; description?: string; skills?: string[] },
): DealerProjectManifest {
  const manifest: DealerProjectManifest = { ...opts, schema: 'the-dealer-project/v1' };
  validateManifest(manifest);
  mkdirSync(join(projectRoot, '.dealer', 'plugins'), { recursive: true });
  writeFileSync(join(projectRoot, '.dealer', 'project.json'), `${JSON.stringify(manifest, null, 2)}\n`, { flag: 'wx' });
  return manifest;
}

export interface RuntimeConfig {
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

const BUILTIN_EXECUTORS = ['orca-spawn', 'orca-wave', 'agent-pty', 'notify', 'inbox'];

export function validateConfig(config: RuntimeConfig): void {
  validatePluginRefs(config.userPlugins);
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
