import { existsSync, mkdirSync, readFileSync, rmSync, chmodSync } from 'node:fs';
import { join, resolve, basename } from 'node:path';
import { pathToFileURL } from 'node:url';
import { homedir, hostname } from 'node:os';
import { createServer } from 'node:http';
import type { IncomingMessage, Server } from 'node:http';
import { generateNodeKeyPair, canonicalJson } from '../core-types/index.ts';
import type { AdmissionMode, Disposable, ExecutorPlugin, MessageEnvelope, NetworkMountConfig, SourcePlugin } from '../core-types/index.ts';
import { Kernel } from '../kernel/index.ts';
import type { Plugin } from '../kernel/index.ts';
import { createTransportPlugin, P2PTransportService } from '../transport/index.ts';
import type { TransportPluginOptions } from '../transport/index.ts';
import { createNodePlugin, PeerDirectoryService } from '../node/index.ts';
import { createRouterPlugin } from '../router/index.ts';
import { createGatePlugin } from '../gate/index.ts';
import type { NetworkGateService } from '../gate/index.ts';
import { createDispatcherPlugin, DeliveryLedger } from '../dispatcher/index.ts';
import { createEmitterPlugin } from '../emitter/index.ts';
import { createDedupCasPolicyPlugin, isConfirmedHandoffReceipt, isDedupCasDuplicateReceipt, isDurableQueuedReceipt } from '../plugins-policy/index.ts';
import {
  createOrcaSpawnExecutorPlugin,
  createOrcaWaveExecutorPlugin,
  createCodexExecutorPlugin,
  createClaudeCodeExecutorPlugin,
  createAntigravityExecutorPlugin,
  createAgentPtyExecutorPlugin,
  createNotifyExecutorPlugin,
  createInboxExecutorPlugin,
} from '../plugins-executor/index.ts';
import { createLarkMirrorPlugin, LarkMailboxNotifier } from '../plugins-lark-notifier/index.ts';
import { runCommand } from '../orca-workspace/index.ts';
import type { CommandRunner } from '../orca-workspace/index.ts';
import { MailboxStore, resolveMailboxId, resolveBoundMailboxId, WakeupRedriveService } from '../mailbox/index.ts';
import { parseYaml, parseMarkdownFrontmatter, parseTeamRegistry, resolveTeamRegistryMembers, createTeamRegistryRouterPlugin, createLocalAgentRouterPlugin, createLlmApiRouterPlugin } from '../plugins-router/index.ts';
import { writeAtomicCursorFile, readJsonCursorFile, everyReceiptSettled, createWebhookSourcePlugin, createPollWatermarkSourcePlugin, createScriptSourcePlugin } from '../plugins-source/index.ts';
import {
  getBuiltInProfile,
  saveJson,
  validateConfig,
  admissionMode,
  readProjectManifest,
  redactProjectManifest,
  readPatch,
  validatePluginRefs,
  validateTopicBindings,
} from './config.ts';
import { buildAgentBindingPreset, resolveAgentBindingPreset } from './setup.ts';
import type { RuntimeConfig, TopicBindingConfig, PluginSpec, DealerProjectPluginRef, DealerProjectManifest } from './config.ts';

export interface ExternalPluginContext {
  projectRoot?: string;
  stateDir: string;
  runtime: DealerNodeRuntime;
  userPatch?: Record<string, any>;
  api: {
    parseYaml: typeof parseYaml;
    parseMarkdownFrontmatter: typeof parseMarkdownFrontmatter;
    parseTeamRegistry: typeof parseTeamRegistry;
    resolveTeamRegistryMembers: typeof resolveTeamRegistryMembers;
    createTeamRegistryRouterPlugin: typeof createTeamRegistryRouterPlugin;
    writeAtomicCursorFile: typeof writeAtomicCursorFile;
    readJsonCursorFile: typeof readJsonCursorFile;
    everyReceiptSettled: typeof everyReceiptSettled;
    createWebhookSourcePlugin: typeof createWebhookSourcePlugin;
    createPollWatermarkSourcePlugin: typeof createPollWatermarkSourcePlugin;
    createScriptSourcePlugin: typeof createScriptSourcePlugin;
    createDedupCasPolicyPlugin: typeof createDedupCasPolicyPlugin;
    isConfirmedHandoffReceipt: typeof isConfirmedHandoffReceipt;
    isDedupCasDuplicateReceipt: typeof isDedupCasDuplicateReceipt;
    isDurableQueuedReceipt: typeof isDurableQueuedReceipt;
    createOrcaSpawnExecutorPlugin: typeof createOrcaSpawnExecutorPlugin;
    createOrcaWaveExecutorPlugin: typeof createOrcaWaveExecutorPlugin;
    createCodexExecutorPlugin: typeof createCodexExecutorPlugin;
    createClaudeCodeExecutorPlugin: typeof createClaudeCodeExecutorPlugin;
    createAntigravityExecutorPlugin: typeof createAntigravityExecutorPlugin;
    createAgentPtyExecutorPlugin: typeof createAgentPtyExecutorPlugin;
    createNotifyExecutorPlugin: typeof createNotifyExecutorPlugin;
    createInboxExecutorPlugin: typeof createInboxExecutorPlugin;
    createLocalAgentRouterPlugin: typeof createLocalAgentRouterPlugin;
    createLlmApiRouterPlugin: typeof createLlmApiRouterPlugin;
  };
}

export type ExternalPluginFactory = (
  config: Record<string, any>,
  context: ExternalPluginContext,
) => Plugin | Promise<Plugin>;

interface LoadedExternalPlugin {
  plugin: Plugin;
  config: Record<string, any>;
}

export interface DealerRuntimeOptions {
  stateDir?: string;
  profile?: string;
  member?: string;
  name?: string;
  skills?: string[];
  port?: number;
  patch?: Partial<RuntimeConfig>;
  transport?: TransportPluginOptions;
  commandRunner?: CommandRunner;
  executorOverrides?: ExecutorPlugin[];
  /** Authoritative idle probe for wakeup redrive; defaults to the recursive Orca inventory. */
  wakeupIdleCheck?: () => Promise<{ idle: boolean; reason?: string }>;
}

export interface ControlRequestContext {
  signal: AbortSignal;
}

export interface ControlServerOptions {
  /** NDJSON keepalive cadence for POST /cli-stream; defaults to 15000ms. */
  keepaliveMs?: number;
}

const CONTROL_BODY_LIMIT = 1_048_576;
const DEFAULT_KEEPALIVE_MS = 15_000;

async function readControlBody(request: IncomingMessage): Promise<string> {
  let body = '';
  for await (const chunk of request) {
    body += chunk;
    if (body.length > CONTROL_BODY_LIMIT) {
      throw new Error('Control request too large');
    }
  }
  return body;
}

function parseControlArgv(body: string): string[] {
  const argv = JSON.parse(body);
  if (!Array.isArray(argv) || argv.some(value => typeof value !== 'string')) {
    throw new Error('Expected argv array');
  }
  return argv;
}

export function defaultStateDir(): string {
  return resolve(process.env.DEALER_STATE_DIR ?? join(homedir(), '.dealer'));
}

export class DealerNodeRuntime {
  readonly stateDir: string;
  readonly kernel = new Kernel();
  readonly options: DealerRuntimeOptions;
  config!: RuntimeConfig;
  mailbox!: MailboxStore;
  wakeups!: WakeupRedriveService;
  larkNotifier?: LarkMailboxNotifier;
  private loadedProjects = new Map<string, { manifest: DealerProjectManifest; plugins: LoadedExternalPlugin[]; userPatch?: Partial<RuntimeConfig>; rootPath: string }>();
  private loadedUserPlugins = new Map<string, LoadedExternalPlugin>();
  private externalChanges: Promise<unknown> = Promise.resolve();
  private server?: Server;
  private registerBuiltinExecutor?: (name: string) => void;
  private started = false;
  private ownsLock = false;
  lastError?: string;

  constructor(options: DealerRuntimeOptions = {}) {
    this.options = options;
    this.stateDir = resolve(options.stateDir ?? defaultStateDir());
  }

  get transport(): P2PTransportService {
    return this.kernel.getService('transport') as P2PTransportService;
  }

  get node(): PeerDirectoryService {
    return this.kernel.getService('node') as PeerDirectoryService;
  }

  get gate(): NetworkGateService {
    return this.kernel.getService('gate') as NetworkGateService;
  }

  get emitter() {
    return this.kernel.getService('emitter');
  }

  /** Single durable mailbox instance; the legacy accessor name remains as a convenience. */
  get inbox(): MailboxStore {
    return this.mailbox;
  }

  get socketPath(): string {
    return join(this.stateDir, 'control.sock');
  }

  persist(): void {
    validateConfig(this.config);
    saveJson(join(this.stateDir, 'config.json'), this.config);
  }

  /** Take over runtime.lock only when the recorded owner process is proven gone. */
  private reclaimStaleLock(lockDir: string): boolean {
    let pid: unknown;
    try {
      pid = (JSON.parse(readFileSync(join(lockDir, 'owner.json'), 'utf8')) as { pid?: unknown }).pid;
    } catch {
      return false;
    }
    if (typeof pid !== 'number' || !Number.isSafeInteger(pid) || pid <= 0) {
      return false;
    }
    try {
      process.kill(pid, 0);
      return false;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ESRCH') {
        return false;
      }
    }
    try {
      rmSync(lockDir, { recursive: true, force: true });
      mkdirSync(lockDir);
      return true;
    } catch {
      return false;
    }
  }

  async start(): Promise<this> {
    if (this.started) {
      return this;
    }
    mkdirSync(this.stateDir, { recursive: true, mode: 0o700 });
    chmodSync(this.stateDir, 0o700);
    const lockDir = join(this.stateDir, 'runtime.lock');
    try {
      mkdirSync(lockDir);
      this.ownsLock = true;
    } catch {
      // A daemon killed inside its shutdown window leaves runtime.lock behind;
      // reclaim it only when the recorded owner is proven to be gone.
      if (!this.reclaimStaleLock(lockDir)) {
        throw new Error(
          'State directory already owned; use the running daemon (remove runtime.lock only after verifying its process stopped)',
        );
      }
      this.ownsLock = true;
    }
    try {
      saveJson(join(this.stateDir, 'runtime.lock', 'owner.json'), { pid: process.pid });
      const path = join(this.stateDir, 'config.json');
      const saved = existsSync(path) ? JSON.parse(readFileSync(path, 'utf8')) : {};
      const profile = getBuiltInProfile(this.options.profile ?? saved.profile ?? 'personal-dev');
      this.config = {
        profile: profile.name,
        member: process.env.USER ?? 'local',
        name: hostname(),
        skills: [],
        gateMode: profile.gateMode,
        networks: [],
        defaultExecutor: profile.defaultExecutor,
        executors: profile.executors,
        policies: profile.policies,
        executorConfig: {},
        projects: [],
        topicBindings: {},
        userPlugins: [],
        routes: [],
        sources: [],
        wave: {},
        ...saved,
        ...(this.options.profile
          ? {
              profile: profile.name,
              gateMode: profile.gateMode,
              defaultExecutor: profile.defaultExecutor,
              executors: profile.executors,
              policies: profile.policies,
            }
          : {}),
        ...Object.fromEntries(
          (['member', 'name', 'skills', 'port'] as const)
            .filter(k => this.options[k] !== undefined)
            .map(k => [k, this.options[k]]),
        ),
        ...this.options.patch,
      };
      if (Array.isArray(this.config.executors) && !this.config.executors.includes('inbox')) {
        this.config.executors = [...this.config.executors, 'inbox'];
      }
      if (this.config.profile === 'personal-dev' && this.options.patch?.executors === undefined
        && Array.isArray(this.config.executors)
        && this.config.executors.every(name => ['orca-spawn', 'orca-wave', 'agent-pty', 'notify', 'inbox'].includes(name))) {
        this.config.executors = [...new Set([...this.config.executors, 'agent-pty', 'codex', 'claude-code', 'antigravity'])];
      }
      // validateConfig also strips a pre-Phase-2 saved 'wave-coalescer' policy in place.
      validateConfig(this.config);
      this.mailbox = new MailboxStore(join(this.stateDir, 'mailbox.json'), {
        legacyInboxPath:
          this.config.executorConfig.inbox?.inboxPath ?? join(this.stateDir, 'inbox.json'),
      });
      this.wakeups = new WakeupRedriveService({
        store: this.mailbox,
        commandRunner: this.options.commandRunner ?? runCommand,
        flushIntervalMs: this.config.wave?.flushIntervalMs ?? 1_000,
        onError: error => {
          this.lastError = String(error);
        },
        ...(this.options.wakeupIdleCheck
          ? { idleCheck: this.options.wakeupIdleCheck }
          : {}),
      });
      const keyPath = join(this.stateDir, 'identity.json');
      const keyPair = existsSync(keyPath) ? JSON.parse(readFileSync(keyPath, 'utf8')) : generateNodeKeyPair();
      if (!existsSync(keyPath)) {
        saveJson(keyPath, keyPair);
      }
      await this.kernel.use(
        createTransportPlugin({
          ...this.options.transport,
          listenPort: this.config.port,
          localNodeId: keyPair.nodeId,
        }),
      );
      await this.kernel.use(
        createNodePlugin({
          keyPair,
          member: this.config.member,
          nodeName: this.config.name,
          admissionMode: this.config.gateMode,
          capabilities: {
            summary: this.config.name,
            skills: this.config.skills.map(id => ({ id, description: id })),
          },
        }),
      );
      await this.kernel.use(createRouterPlugin());
      await this.kernel.use(
        createGatePlugin({
          mode: this.config.gateMode,
          dndQueuePath: join(this.stateDir, 'gate-dnd.json'),
          localNodeId: keyPair.nodeId,
          networkModes: Object.fromEntries(
            this.config.networks
              .filter(n => n.admissionMode)
              .map(n => [n.networkId, n.admissionMode!]),
          ),
        }),
      );
      await this.kernel.use(
        createDispatcherPlugin({
          ledgerPath: join(this.stateDir, 'deliveries.json'),
          resolveKindExecutor: (kind, streamKey) => this.resolveTopicDefaultExecutor(kind, streamKey) ?? this.config.defaultExecutor,
        }),
      );
      await this.kernel.use(
        createEmitterPlugin({
          onSourceError: error => {
            this.lastError = String(error);
          },
        }),
      );
      await this.kernel.use(this.integrationPlugin());
      for (const network of this.config.networks) {
        await this.transport.mountNetwork(network);
        if (network.admissionMode) {
          await this.node.updateNetworkAdmissionMode(network.networkId, network.admissionMode);
        }
      }
      for (const route of this.config.routes) {
        this.installRoute(route);
      }
      for (const source of this.config.sources) {
        await this.installSource(source);
      }
      for (const project of [...this.config.projects]) {
        await this.linkProject(project.rootPath, project.userPatchPath, { restore: true });
      }
      for (const ref of [...this.config.userPlugins]) {
        await this.installUserPlugin(ref);
      }
      const gate = this.kernel.ctx.services.gate as NetworkGateService;
      for (const networkId of this.transport.listMountedNetworks()) {
        if (gate.getNetworkMode(networkId) !== 'open') continue;
        try {
          await gate.drainDnd(networkId);
        } catch (error) {
          // Failed items remain durable and ledger-fenced; keep the node available for recovery.
          this.lastError = String(error);
        }
      }
      await this.activateLarkNotifier();
      this.refreshTopicBindings();
      this.persist();
      this.started = true;
      return this;
    } catch (error) {
      await this.stop();
      throw error;
    }
  }

  private changeExternal<T>(change: () => Promise<T>): Promise<T> {
    const result = this.externalChanges.then(change);
    this.externalChanges = result.catch(() => {});
    return result;
  }

  // This is the sole dynamic import boundary for explicitly trusted local modules.
  private async loadExternalPlugin(
    ref: DealerProjectPluginRef,
    config: Record<string, any>,
    context: Omit<ExternalPluginContext, 'api'>,
  ): Promise<LoadedExternalPlugin> {
    const resolvedPath = resolve(context.projectRoot ?? this.stateDir, ref.module);
    const module = await import(pathToFileURL(resolvedPath).href);
    const exported = module.createPlugin ?? module.default;
    const factoryContext: ExternalPluginContext = {
      ...context,
      api: {
        parseYaml,
        parseMarkdownFrontmatter,
        parseTeamRegistry,
        resolveTeamRegistryMembers,
        createTeamRegistryRouterPlugin,
        writeAtomicCursorFile,
        readJsonCursorFile,
        everyReceiptSettled,
        createWebhookSourcePlugin,
        createPollWatermarkSourcePlugin,
        createScriptSourcePlugin,
        createDedupCasPolicyPlugin,
        isConfirmedHandoffReceipt,
        isDedupCasDuplicateReceipt,
        isDurableQueuedReceipt,
        createOrcaSpawnExecutorPlugin,
        createOrcaWaveExecutorPlugin,
        createCodexExecutorPlugin,
        createClaudeCodeExecutorPlugin,
        createAntigravityExecutorPlugin,
        createAgentPtyExecutorPlugin,
        createNotifyExecutorPlugin,
        createInboxExecutorPlugin,
        createLocalAgentRouterPlugin,
        createLlmApiRouterPlugin,
      },
    };
    const candidate = typeof exported === 'function' ? await exported(config, factoryContext) : exported;
    if (!candidate || typeof candidate !== 'object' || typeof candidate.apply !== 'function') {
      throw new Error(`Invalid external plugin: ${ref.name}`);
    }
    const plugin = { ...candidate, name: candidate.name || ref.name };
    if (typeof plugin.name !== 'string' || !plugin.name.trim()) {
      throw new Error(`Invalid external plugin name: ${ref.name}`);
    }
    return { plugin, config };
  }

  private async replaceExternal(
    previous: LoadedExternalPlugin[],
    next: LoadedExternalPlugin[],
    commit: () => Promise<void>,
  ): Promise<void> {
    const available = new Set(previous.map(entry => entry.plugin.name));
    const occupied = new Set(this.kernel.enabledPluginNames().filter(name => !available.has(name)));
    for (const entry of next) {
      if (occupied.has(entry.plugin.name)) {
        throw new Error(`Plugin name already owned: ${entry.plugin.name}`);
      }
      occupied.add(entry.plugin.name);
    }
    const config = structuredClone(this.config);
    try {
      await this.kernel.applyPatch(previous.map(({ plugin }) => ({ name: plugin.name, enabled: false })));
      for (const entry of next) {
        await this.kernel.use(entry.plugin, entry.config);
      }
      await commit();
      this.persist();
    } catch (error) {
      this.config = config;
      try {
        await this.kernel.applyPatch([
          ...next.map(({ plugin }) => ({ name: plugin.name, plugin, enabled: false })),
          ...previous.map(entry => ({ ...entry, enabled: true })),
        ]);
        await this.publishSkills();
      } catch (rollbackError) {
        throw new AggregateError([error, rollbackError], 'External plugin change and rollback failed');
      }
      throw error;
    }
  }

  private async publishSkills(): Promise<void> {
    await this.node.updateSelfProfile({
      capabilities: {
        summary: this.config.name,
        skills: this.config.skills.map(id => ({ id, description: id })),
      },
    });
  }

  async linkProject(projectRoot: string, userPatchPath?: string, options: { restore?: boolean } = {}): Promise<{
    projectId: string;
    defaultNetworkId: string;
    loadedPlugins: string[];
    userPatchPath?: string;
  }> {
    return this.changeExternal(async () => {
      const rootPath = resolve(projectRoot);
      const manifest = readProjectManifest(rootPath);
      const onboardingNetworkId = manifest.onboarding?.networkId ?? manifest.defaultNetworkId;
      const onboardingPassphrase = manifest.onboarding?.passphrase;
      for (const [kind, topic] of Object.entries(manifest.topics ?? {})) {
        if (topic.defaultExecutor === undefined) {
          continue;
        }
        for (const [otherProjectId, other] of this.loadedProjects) {
          const existing = other.manifest.topics?.[kind]?.defaultExecutor;
          if (otherProjectId !== manifest.projectId && existing !== undefined && existing !== topic.defaultExecutor) {
            throw new Error(`Conflicting defaultExecutor for topic: ${kind}`);
          }
        }
      }
      const patchPath = resolve(userPatchPath ?? join(this.stateDir, 'projects', `${manifest.projectId}.patch.json`));
      const userPatch: Record<string, any> | undefined = existsSync(patchPath) ? readPatch(patchPath) : undefined;
      const bindings = userPatch?.topicBindings === undefined ? {} : userPatch.topicBindings;
      validateTopicBindings(bindings, this.config.executors);
      for (const [kind, binding] of Object.entries(bindings)) {
        for (const [otherId, other] of this.loadedProjects) {
          const previous = other.userPatch?.topicBindings?.[kind];
          if (otherId !== manifest.projectId && previous !== undefined &&
              (canonicalJson(previous) !== canonicalJson(binding) || (!binding.projectId && other.rootPath !== rootPath))) {
            throw new Error(`Conflicting topicBindings for topic: ${kind}`);
          }
        }
      }
      if (userPatch?.skills !== undefined &&
          (!Array.isArray(userPatch.skills) || userPatch.skills.some((skill: unknown) => typeof skill !== 'string'))) {
        throw new Error('Invalid project user patch skills');
      }
      const overrides = userPatch?.pluginConfig;
      if (overrides !== undefined && (!overrides || typeof overrides !== 'object' || Array.isArray(overrides))) {
        throw new Error('Invalid project user patch pluginConfig');
      }
      const plugins: LoadedExternalPlugin[] = [];
      for (const ref of manifest.plugins ?? []) {
        const overlay = overrides && Object.hasOwn(overrides, ref.name) ? overrides[ref.name] : undefined;
        if (overlay !== undefined && (!overlay || typeof overlay !== 'object' || Array.isArray(overlay))) {
          throw new Error(`Invalid plugin configuration overlay: ${ref.name}`);
        }
        const config = { ...ref.config, ...overlay };
        plugins.push(await this.loadExternalPlugin(ref, config, {
          projectRoot: rootPath,
          stateDir: this.stateDir,
          runtime: this,
          userPatch,
        }));
      }
      // Retain explicit paths even before creation so the next start can discover the patch.
      const resolvedPatchPath = userPatchPath !== undefined || userPatch !== undefined ? patchPath : undefined;
      const linked = {
        projectId: manifest.projectId,
        rootPath,
        ...(resolvedPatchPath ? { userPatchPath: resolvedPatchPath } : {}),
      };
      await this.replaceExternal(this.loadedProjects.get(manifest.projectId)?.plugins ?? [], plugins, async () => {
        this.config.skills = [...new Set([...this.config.skills, ...(manifest.skills ?? []), ...(userPatch?.skills ?? [])])];
        await this.publishSkills();
        this.config.projects = [...this.config.projects.filter(project => project.projectId !== manifest.projectId), linked];
        if (onboardingPassphrase && !options.restore && !this.transport.listMountedNetworks().includes(onboardingNetworkId)) {
          await this.mountNetwork({
            networkId: onboardingNetworkId,
            passphrase: onboardingPassphrase,
            ...(manifest.onboarding?.bootstrapPeers?.length ? { bootstrapPeers: manifest.onboarding.bootstrapPeers } : {}),
          });
        } else if (onboardingPassphrase && this.transport.listMountedNetworks().includes(onboardingNetworkId) &&
                   manifest.onboarding?.bootstrapPeers?.length) {
          // Linking or restoring a project also adds newly published project seeds to
          // an already-mounted network instead of requiring a manual remount.
          const current = this.transport.getNetworkConfig(onboardingNetworkId);
          const mergedSeeds = [...new Set([...(current?.bootstrapPeers ?? []), ...manifest.onboarding.bootstrapPeers])];
          if (current && mergedSeeds.length > (current.bootstrapPeers?.length ?? 0)) {
            await this.mountNetwork({ ...current, bootstrapPeers: mergedSeeds });
          }
        }
      });
      this.loadedProjects.set(manifest.projectId, { manifest, plugins, userPatch, rootPath });
      this.refreshTopicBindings();
      return {
        projectId: manifest.projectId,
        defaultNetworkId: manifest.defaultNetworkId,
        loadedPlugins: plugins.map(entry => entry.plugin.name),
        ...(resolvedPatchPath ? { userPatchPath: resolvedPatchPath } : {}),
      };
    });
  }

  async unlinkProject(projectId: string): Promise<void> {
    return this.changeExternal(async () => {
      if (!this.config.projects.some(project => project.projectId === projectId)) {
        throw new Error(`Project not linked: ${projectId}`);
      }
      await this.replaceExternal(this.loadedProjects.get(projectId)?.plugins ?? [], [], async () => {
        this.config.projects = this.config.projects.filter(project => project.projectId !== projectId);
      });
      this.loadedProjects.delete(projectId);
      this.refreshTopicBindings();
    });
  }

  async installUserPlugin(ref: DealerProjectPluginRef): Promise<{ added: string }> {
    return this.changeExternal(async () => {
      validatePluginRefs([ref]);
      const loaded = await this.loadExternalPlugin(ref, ref.config ?? {}, { stateDir: this.stateDir, runtime: this });
      const previous = this.loadedUserPlugins.get(ref.name);
      await this.replaceExternal(previous ? [previous] : [], [loaded], async () => {
        this.config.userPlugins = [...this.config.userPlugins.filter(plugin => plugin.name !== ref.name), structuredClone(ref)];
      });
      this.loadedUserPlugins.set(ref.name, loaded);
      return { added: ref.name };
    });
  }

  async removeUserPlugin(name: string): Promise<{ removed: string }> {
    return this.changeExternal(async () => {
      if (!this.config.userPlugins.some(plugin => plugin.name === name)) {
        throw new Error(`User plugin not installed: ${name}`);
      }
      const previous = this.loadedUserPlugins.get(name);
      await this.replaceExternal(previous ? [previous] : [], [], async () => {
        this.config.userPlugins = this.config.userPlugins.filter(plugin => plugin.name !== name);
      });
      this.loadedUserPlugins.delete(name);
      return { removed: name };
    });
  }

  projectStatus(projectId?: string) {
    return {
      projects: this.config.projects.filter(project => !projectId || project.projectId === projectId).map(project => {
        const loaded = this.loadedProjects.get(project.projectId);
        return {
          ...project,
          manifest: loaded ? redactProjectManifest(loaded.manifest) : undefined,
          loadedPlugins: (loaded?.plugins ?? []).map(entry => entry.plugin.name),
        };
      }),
      userPlugins: this.config.userPlugins.filter(ref => this.loadedUserPlugins.has(ref.name)).map(ref => ({
        ...ref,
        loadedName: this.loadedUserPlugins.get(ref.name)!.plugin.name,
      })),
    };
  }

  resolveTopicBinding(kind: string, streamKey?: string): { binding: TopicBindingConfig; projectRoot?: string } | undefined {
    const keys = streamKey === undefined ? [kind] : [`${kind}:${streamKey}`, kind];
    for (const key of keys) {
      for (const project of this.loadedProjects.values()) {
        const bindings = project.userPatch?.topicBindings;
        const binding = bindings && Object.hasOwn(bindings, key) ? bindings[key] : undefined;
        if (binding) return {
          binding,
          projectRoot: binding.projectId ? this.config.projects.find(item => item.projectId === binding.projectId)?.rootPath : project.rootPath,
        };
      }
      const binding = this.config.topicBindings && Object.hasOwn(this.config.topicBindings, key) ? this.config.topicBindings[key] : undefined;
      if (binding) return { binding, projectRoot: this.config.projects.find(item => item.projectId === binding.projectId)?.rootPath };
    }
    return undefined;
  }

  private executorTopicBinding = (kind: string, streamKey?: string) => {
    const resolved = this.resolveTopicBinding(kind, streamKey);
    return resolved ? { ...resolved.binding, repoRoot: resolved.projectRoot } : undefined;
  };

  private async activateLarkNotifier(): Promise<void> {
    if (!this.config.larkNotify?.enabled || this.larkNotifier) return;
    const options = {
      store: this.mailbox, config: this.config.larkNotify, stateDir: this.stateDir,
      localNodeId: this.transport.localNodeId, localMember: this.config.member,
      commandRunner: this.options.commandRunner ?? runCommand,
      onError: (error: unknown) => { this.lastError = String(error); },
    };
    this.larkNotifier = new LarkMailboxNotifier(options);
    this.refreshTopicBindings();
    await this.kernel.use(createLarkMirrorPlugin({ ...options, notifier: this.larkNotifier }));
  }

  previewAgentPreset(options: Parameters<typeof buildAgentBindingPreset>[0] & { cwd?: string }) {
    return resolveAgentBindingPreset(options, {
      projects: [...this.loadedProjects.values()].map(project => project.manifest),
      larkNotify: this.config.larkNotify, cwd: options.cwd,
    });
  }

  async applyAgentPreset(options: Parameters<typeof buildAgentBindingPreset>[0] & { setDefault?: boolean; cwd?: string }) {
    const preset = this.previewAgentPreset(options);
    const host = options.host === 'claude' ? 'claude-code' : options.host;
    const mode = options.conversationMode ?? 'fixed';
    const defaultExecutor = !options.setDefault ? this.config.defaultExecutor
      : mode === 'inbox-only' ? 'inbox'
      : host === 'orca' ? 'orca-spawn'
      : ['codex', 'claude-code', 'antigravity'].includes(host) ? host : preset.binding.executor;
    this.config.executors = [...new Set([...this.config.executors, preset.binding.executor, defaultExecutor])];
    this.config.topicBindings = { ...this.config.topicBindings, [preset.topic]: preset.binding };
    for (const [name, config] of Object.entries(preset.executorConfig)) {
      this.config.executorConfig[name] = { ...this.config.executorConfig[name], ...config };
    }
    this.config.defaultExecutor = defaultExecutor;
    if (preset.larkNotify) {
      this.config.larkNotify = preset.larkNotify;
      if (this.started) await this.activateLarkNotifier();
    }
    for (const name of this.config.executors) this.registerBuiltinExecutor?.(name);
    this.refreshTopicBindings();
    this.persist();
    return { applied: preset.topic, host, mode, defaultExecutor, binding: preset.binding, executorConfig: preset.executorConfig, ...(preset.larkNotify ? { larkNotify: preset.larkNotify } : {}) };
  }

  private refreshTopicBindings(): void {
    const kinds = new Set(Object.keys(this.config.topicBindings ?? {}));
    for (const project of this.loadedProjects.values()) {
      for (const kind of Object.keys(project.userPatch?.topicBindings ?? {})) kinds.add(kind);
    }
    const mailboxes: string[] = [];
    for (const kind of kinds) {
      const { binding, projectRoot } = this.resolveTopicBinding(kind)!;
      const mailboxId = resolveBoundMailboxId(kind, binding);
      const notifyLark = typeof binding.notifyLark === 'boolean' ? binding.notifyLark
        : !kind.includes(':') && ![...this.loadedProjects.values()].some(({ manifest }) =>
          ['orca-wave', 'orca-spawn', 'agent-pty', 'codex', 'claude-code', 'antigravity'].includes(manifest.topics?.[kind]?.defaultExecutor ?? ''));
      if (notifyLark) mailboxes.push(mailboxId);
      if (binding.executor !== 'orca-wave' || !(binding.workspacePath || binding.runId || binding.terminalHandle)) continue;
      const { workspacePath, worktreeId, runId, terminalHandle, autoCreateWorkspace, workspaceComment, wakeupPrefix, coalesceMs, rotateAfterWaves, interactiveCommand } = binding;
      this.mailbox.upsertWakeTarget(mailboxId, {
        mode: runId ? 'orca-run' : 'orca-terminal',
        workspacePath, worktreeId, runId, terminalHandle, repoRoot: projectRoot,
        agent: binding.agent as 'antigravity' | 'claude' | 'codex' | undefined, interactiveCommand,
        autoCreateWorkspace, workspaceComment, wakeupPrefix, coalesceMs: coalesceMs ?? 0, rotateAfterWaves,
      });
    }
    this.larkNotifier?.setDefaultBoundMailboxes(mailboxes);
  }

  resolveTopicDefaultExecutor(kind: string, streamKey?: string): string | undefined {
    const bound = this.resolveTopicBinding(kind, streamKey);
    if (bound) return bound.binding.executor;
    for (const { manifest } of this.loadedProjects.values()) {
      const executor = manifest.topics?.[kind]?.defaultExecutor;
      if (executor !== undefined) {
        return executor;
      }
    }
    return undefined;
  }

  resolveExecutorName(envelope: {
    kind: string;
    payload?: Record<string, any>;
    semantic?: { type?: string };
  }): string {
    // Session control must land in the target mailbox, never in a new worktree.
    const type = envelope.semantic?.type;
    if (type === 'question' || type === 'reply' || type === 'settle') {
      return 'inbox';
    }
    const explicit = envelope.payload?.executor;
    if (typeof explicit === 'string' && explicit.trim().length > 0) {
      return explicit;
    }
    return this.resolveTopicDefaultExecutor(envelope.kind, typeof envelope.payload?.streamKey === 'string' ? envelope.payload.streamKey : undefined) ?? this.config.defaultExecutor;
  }

  private integrationPlugin(): Plugin {
    return {
      name: 'runtime-plugins',
      inject: ['dispatcher', 'transport', 'emitter', 'router'],
      apply: ctx => {
        // Thread reverse routes are populated from the authenticated dispatcher/emitter boundary,
        // not from raw transport frames, so forged senders and gate-rejected messages never
        // create routes.
        ctx.on('dispatcher/inbound', ({ envelope, receipt }) => {
          if (receipt.stage === 'rejected') {
            return;
          }
          try {
            const mailboxId = resolveMailboxId(envelope, this.resolveExecutorName(envelope), this.resolveTopicBinding(envelope.kind, typeof envelope.payload?.streamKey === 'string' ? envelope.payload.streamKey : undefined)?.binding);
            this.mailbox.recordInboundEnvelope(envelope, mailboxId);
          } catch (error) {
            this.lastError = String(error);
          }
        });
        ctx.on('emitter/prepare', (envelope, next) => {
          if (envelope.semantic?.replyMailbox !== undefined) return next(envelope);
          const resolved = this.resolveTopicBinding(envelope.kind, typeof envelope.payload?.streamKey === 'string' ? envelope.payload.streamKey : undefined);
          if (!resolved) return next(envelope);
          const bindingKind = typeof envelope.payload?.streamKey === 'string'
            ? `${envelope.kind}:${envelope.payload.streamKey}` : envelope.kind;
          const replyMailbox = resolveBoundMailboxId(bindingKind, resolved.binding);
          return next(replyMailbox === 'default' ? envelope : {
            ...envelope, semantic: { ...(envelope.semantic ?? { type: 'message' as const, threadId: envelope.id }), replyMailbox },
          });
        });
        ctx.on('emitter/sent', ({ envelope, targetNodeIds }) => {
          try {
            this.mailbox.recordOutboundEnvelope(
              envelope,
              targetNodeIds,
              envelope.semantic?.replyMailbox ?? 'default',
            );
          } catch (error) {
            this.lastError = String(error);
          }
        });
        const runner = this.options.commandRunner ?? runCommand;
        const linkedProjectRoots = Object.fromEntries(
          this.config.projects.map(project => [project.projectId, project.rootPath]),
        );
        const spawnConfig = this.config.executorConfig['orca-spawn'] ?? {};
        const executors: Record<string, ExecutorPlugin> = {
          'orca-spawn': createOrcaSpawnExecutorPlugin({
            resolveTopicBinding: this.executorTopicBinding,
            ...spawnConfig,
            projectRoots: {
              ...linkedProjectRoots,
              ...(spawnConfig.projectRoots ?? {}),
            },
            commandRunner: this.options.commandRunner,
          }),
          'orca-wave': createOrcaWaveExecutorPlugin({
            ...this.config.executorConfig['orca-wave'],
            resolveTopicBinding: this.executorTopicBinding,
            commandRunner: this.options.commandRunner,
          }),
          'codex': createCodexExecutorPlugin({
            ...this.config.executorConfig['codex'],
            store: this.mailbox,
            wakeup: this.wakeups,
            resolveTopicBinding: this.executorTopicBinding,
            commandRunner: this.options.commandRunner,
          }),
          'claude-code': createClaudeCodeExecutorPlugin({
            ...this.config.executorConfig['claude-code'],
            store: this.mailbox,
            wakeup: this.wakeups,
            resolveTopicBinding: this.executorTopicBinding,
            commandRunner: this.options.commandRunner,
          }),
          'antigravity': createAntigravityExecutorPlugin({
            ...this.config.executorConfig['antigravity'],
            store: this.mailbox,
            wakeup: this.wakeups,
            resolveTopicBinding: this.executorTopicBinding,
            commandRunner: this.options.commandRunner,
          }),
          'agent-pty': createAgentPtyExecutorPlugin({
            ...this.config.executorConfig['agent-pty'],
            store: this.mailbox,
            wakeup: this.wakeups,
            resolveTopicBinding: this.executorTopicBinding,
            commandRunner: runner,
          }),
          inbox: createInboxExecutorPlugin({
            ...this.config.executorConfig.inbox,
            resolveTopicBinding: this.executorTopicBinding,
            store: this.mailbox,
            wakeup: this.wakeups,
          }),
          notify: createNotifyExecutorPlugin({
            inboxPath: join(this.stateDir, 'notifications.jsonl'),
            ...this.config.executorConfig.notify,
            commandRunner: runner,
          }),
        };
        for (const executor of this.options.executorOverrides ?? []) {
          executors[executor.name] = executor;
        }
        const registered = new Set<string>();
        this.registerBuiltinExecutor = name => {
          if (registered.has(name)) return;
          const executor = executors[name];
          const wrapped: ExecutorPlugin = {
            ...executor,
            execute: async (batch, context, config) => {
              const local = [];
              for (const envelope of batch) {
                const directory = join(this.stateDir, 'attachments', encodeURIComponent(envelope.id));
                const files: string[] = [];
                for (const [index, attachment] of envelope.attachments.entries()) {
                  mkdirSync(directory, { recursive: true, mode: 0o700 });
                  const destination = join(directory, `${index}-${basename(attachment.name)}`);
                  await this.transport.fetchBlobToPath(attachment.ticket, destination);
                  files.push(destination);
                }
                const localEnvelope = files.length
                  ? { ...envelope, payload: { ...envelope.payload, localAttachments: files } }
                  : envelope;
                local.push(localEnvelope);
              }
              return executor.execute(local, context, { ...config, ...this.config.executorConfig[name] });
            },
          };
          const registration = ctx.services.dispatcher.registerExecutor(
            wrapped,
            name === 'orca-wave'
              ? {
                  ...this.config.executorConfig[name],
                  store: this.mailbox,
                  wakeup: this.wakeups,
                  wave: this.config.wave,
                }
              : name === 'orca-spawn'
                ? {
                    ...this.config.executorConfig[name],
                    store: this.mailbox,
                    wakeup: this.wakeups,
                  }
                : (this.config.executorConfig[name] ?? {}),
          );
          ctx.effect(() => () => registration.dispose());
          registered.add(name);
        };
        for (const name of this.config.executors) this.registerBuiltinExecutor(name);
        if (this.config.policies.includes('dedup-cas')) {
          let legacyLedger: DeliveryLedger | undefined;
          const registration = ctx.services.dispatcher.registerPolicy(
            createDedupCasPolicyPlugin({
              watermarksPath: join(this.stateDir, 'dedup-watermarks.json'),
              resolveLegacyWatermarkStatus: (key, revision) => {
                const parts: unknown = JSON.parse(key);
                if (!Array.isArray(parts) || parts.length !== 3 || parts.some(part => typeof part !== 'string')) {
                  return 'unknown';
                }
                const [networkId, senderNodeId, entityId] = parts;
                legacyLedger ??= new DeliveryLedger(join(this.stateDir, 'deliveries.json'));
                const receipt = legacyLedger.get(key);
                if (this.mailbox.hasConfirmedEnvelope(networkId, senderNodeId, entityId, revision) ||
                    receipt?.stage === 'dispatched' || receipt?.stage === 'settled') return 'settled';
                return 'unknown';
              },
            }),
          );
          ctx.effect(() => () => registration.dispose());
        }
      },
    };
  }

  /** Deterministic flush of pending durable wakeups (primary test/inspection hook). */
  async flushWakeups(): Promise<void> {
    await this.wakeups.flushWakeups();
  }

  async mountNetwork(network: NetworkMountConfig & { role?: string }): Promise<void> {
    if (!network.networkId || !network.passphrase) {
      throw new Error('Network and passphrase required');
    }
    if (network.admissionMode) {
      admissionMode(network.admissionMode);
    }
    const existing = this.config.networks.find(n => n.networkId === network.networkId);
    const resolved = {
      ...network,
      ...(network.admissionMode ?? existing?.admissionMode ? { admissionMode: network.admissionMode ?? existing?.admissionMode } : {}),
      ...(network.role ?? existing?.role ? { role: network.role ?? existing?.role } : {}),
    };
    const previous = this.transport.getNetworkConfig(network.networkId);
    if (previous) await this.transport.unmountNetwork(network.networkId);
    try {
      await this.transport.mountNetwork(resolved);
    } catch (error) {
      if (previous) await this.transport.mountNetwork(previous);
      throw error;
    }
    if (resolved.admissionMode) {
      await this.gate.setNetworkMode(resolved.networkId, resolved.admissionMode);
      await this.node.updateNetworkAdmissionMode(resolved.networkId, resolved.admissionMode);
    }
    this.config.networks = this.config.networks.filter(n => n.networkId !== resolved.networkId);
    this.config.networks.push(structuredClone(resolved));
    this.persist();
  }

  async unmountNetwork(id: string): Promise<void> {
    await this.transport.unmountNetwork(id);
    this.config.networks = this.config.networks.filter(n => n.networkId !== id);
    this.persist();
  }

  async setGateMode(mode: AdmissionMode, networkId?: string): Promise<void> {
    admissionMode(mode);
    if (networkId) {
      const network = this.config.networks.find(n => n.networkId === networkId);
      if (!network) {
        throw new Error('Network not mounted');
      }
      await this.gate.setNetworkMode(networkId, mode);
      network.admissionMode = mode;
      await this.node.updateNetworkAdmissionMode(networkId, mode);
    } else {
      await this.gate.setMode(mode);
      this.config.gateMode = mode;
      for (const network of this.config.networks) {
        await this.node.updateNetworkAdmissionMode(network.networkId, this.gate.getNetworkMode(network.networkId));
      }
    }
    this.persist();
  }

  installRoute(spec: PluginSpec): Disposable {
    let judge;
    if (spec.type === 'local-agent') {
      judge = createLocalAgentRouterPlugin({ ...spec.config, commandRunner: this.options.commandRunner });
    } else if (spec.type === 'llm-api') {
      if (!spec.config.endpoint || !spec.config.model) {
        throw new Error('llm-api requires endpoint and model');
      }
      judge = createLlmApiRouterPlugin(spec.config as any);
    } else {
      throw new Error(`Unknown route type: ${spec.type}`);
    }
    const registration = this.kernel.getService('router').registerJudge(judge);
    this.kernel.ctx.effect(() => () => registration.dispose());
    return registration;
  }

  async installSource(spec: PluginSpec): Promise<Disposable> {
    let source: SourcePlugin;
    if (spec.type === 'webhook') {
      source = createWebhookSourcePlugin(spec.config);
    } else if (spec.type === 'script') {
      if (!spec.config.command) {
        throw new Error('script requires command');
      }
      source = createScriptSourcePlugin({ ...spec.config, commandRunner: this.options.commandRunner } as any);
    } else if (spec.type === 'poll-watermark') {
      if (!spec.config.url) {
        throw new Error('poll-watermark requires url returning {items,cursor}');
      }
      const cursorPath = join(this.stateDir, `cursor-${encodeURIComponent(spec.config.name ?? spec.config.url)}.json`);
      source = createPollWatermarkSourcePlugin({
        initialCursor: existsSync(cursorPath)
          ? JSON.parse(readFileSync(cursorPath, 'utf8'))
          : spec.config.initialCursor ?? null,
        intervalMs: spec.config.intervalMs ?? 10000,
        poll: async cursor => {
          const url = new URL(spec.config.url);
          if (cursor !== null) {
            url.searchParams.set('cursor', JSON.stringify(cursor));
          }
          const response = await fetch(url, { signal: AbortSignal.timeout(30000) });
          if (!response.ok) {
            throw new Error(`Poll HTTP ${response.status}`);
          }
          return response.json();
        },
        toInput: item => item as any,
        saveCursor: async cursor => saveJson(cursorPath, cursor),
        onError: error => {
          this.lastError = String(error);
        },
      });
    } else {
      throw new Error(`Unknown source type: ${spec.type}`);
    }
    let started: Promise<void> = Promise.resolve();
    const registration = this.emitter.registerSource({
      name: source.name,
      start: emitter => {
        started = Promise.resolve(source.start(emitter));
        return started;
      },
      stop: () => source.stop(),
    });
    try {
      await started;
      return registration;
    } catch (error) {
      try {
        await registration.dispose();
      } catch {
        // Ignore rollback disposal error
      }
      throw error;
    }
  }

  status() {
    return {
      running: this.started,
      nodeId: this.transport.localNodeId,
      profile: this.config.profile,
      member: this.config.member,
      name: this.config.name,
      endpoint: this.transport.endpointAddr,
      networks: this.transport.listMountedNetworks(),
      gateMode: this.gate.getMode(),
      lastError: this.lastError,
    };
  }

  async serveControl(
    handler: (
      argv: string[],
      context: ControlRequestContext,
    ) => Promise<{ code: number; stdout: string[]; stderr: string[] }>,
    options: ControlServerOptions = {},
  ): Promise<void> {
    if (this.server) {
      throw new Error('Control server already started');
    }
    const keepaliveMs = options.keepaliveMs ?? DEFAULT_KEEPALIVE_MS;
    if (!Number.isSafeInteger(keepaliveMs) || keepaliveMs <= 0) {
      throw new Error('Keepalive interval must be a positive integer');
    }
    // Only the stateDir lock owner may remove a stale socket.
    rmSync(this.socketPath, { force: true });
    const server = createServer(async (request, response) => {
      if (
        request.method !== 'POST' ||
        (request.url !== '/cli' && request.url !== '/cli-stream')
      ) {
        response.writeHead(404).end();
        return;
      }
      if (request.url === '/cli') {
        try {
          const argv = parseControlArgv(await readControlBody(request));
          const result = await handler(argv, { signal: new AbortController().signal });
          response
            .writeHead(200, { 'content-type': 'application/json' })
            .end(JSON.stringify(result));
        } catch (error) {
          if (!response.destroyed && !response.writableEnded) {
            response.writeHead(400).end(JSON.stringify({ error: String(error) }));
          }
        }
        return;
      }
      const controller = new AbortController();
      let completed = false;
      let started = false;
      let keepalive: ReturnType<typeof setInterval> | undefined;
      const abortIfActive = () => {
        if (!completed) {
          controller.abort();
        }
      };
      // A completed request body also emits 'close' on the request stream; only a premature
      // connection loss aborts the handler. The response stream reports disconnect reliably.
      request.on('close', () => {
        if (!request.complete) {
          abortIfActive();
        }
      });
      response.on('close', abortIfActive);
      response.on('error', abortIfActive);
      try {
        const argv = parseControlArgv(await readControlBody(request));
        if (!response.destroyed && !response.writableEnded) {
          response.writeHead(200, { 'content-type': 'application/x-ndjson' });
          started = true;
          keepalive = setInterval(() => {
            if (!completed && !response.destroyed && !response.writableEnded) {
              response.write(`${JSON.stringify({ type: 'keepalive', ts: Date.now() })}\n`);
            }
          }, keepaliveMs);
          keepalive.unref();
        }
        let line: Record<string, unknown>;
        try {
          const result = await handler(argv, { signal: controller.signal });
          line = { type: 'result', result };
        } catch (error) {
          line = {
            type: 'error',
            error: error instanceof Error ? error.message : String(error),
          };
        }
        completed = true;
        if (keepalive) {
          clearInterval(keepalive);
        }
        if (!response.destroyed && !response.writableEnded) {
          response.end(`${JSON.stringify(line)}\n`);
        }
      } catch (error) {
        completed = true;
        if (keepalive) {
          clearInterval(keepalive);
        }
        if (!response.destroyed && !response.writableEnded) {
          if (!started) {
            response.writeHead(400).end(JSON.stringify({ error: String(error) }));
          } else {
            response.end(`${JSON.stringify({ type: 'error', error: String(error) })}\n`);
          }
        }
      } finally {
        completed = true;
        if (keepalive) {
          clearInterval(keepalive);
        }
      }
    });
    this.server = server;
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(this.socketPath, () => {
        server.off('error', reject);
        resolve();
      });
    });
    chmodSync(this.socketPath, 0o600);
  }

  async stop(): Promise<void> {
    const hadServer = this.server !== undefined;
    if (this.server) {
      this.server.closeAllConnections?.();
      if (this.server.listening) {
        await new Promise<void>((resolve, reject) => {
          this.server!.close(error => (error ? reject(error) : resolve()));
        });
      }
    }
    this.server = undefined;
    try {
      await this.wakeups?.stop();
    } finally {
      try {
        await this.kernel.stop();
        this.larkNotifier = undefined;
      } finally {
        if (this.ownsLock) {
          // Only the instance that created the control server may remove its socket.
          if (hadServer) {
            rmSync(this.socketPath, { force: true });
          }
          rmSync(join(this.stateDir, 'runtime.lock'), { recursive: true, force: true });
          this.ownsLock = false;
        }
        this.started = false;
      }
    }
  }
}
