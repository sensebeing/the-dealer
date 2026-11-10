import { existsSync, mkdirSync, readFileSync, rmSync, chmodSync } from 'node:fs';
import { join, resolve, basename } from 'node:path';
import { pathToFileURL } from 'node:url';
import { homedir, hostname } from 'node:os';
import { createServer } from 'node:http';
import type { IncomingMessage, Server } from 'node:http';
import { generateNodeKeyPair } from '../core-types/index.ts';
import type { AdmissionMode, Disposable, ExecutorPlugin, MessageEnvelope, NetworkMountConfig, SourcePlugin } from '../core-types/index.ts';
import { Kernel } from '../kernel/index.ts';
import type { Plugin } from '../kernel/index.ts';
import { createTransportPlugin, P2PTransportService } from '../transport/index.ts';
import type { TransportPluginOptions } from '../transport/index.ts';
import { createNodePlugin, PeerDirectoryService } from '../node/index.ts';
import { createRouterPlugin } from '../router/index.ts';
import { createGatePlugin } from '../gate/index.ts';
import type { NetworkGateService } from '../gate/index.ts';
import { createDispatcherPlugin } from '../dispatcher/index.ts';
import { createEmitterPlugin } from '../emitter/index.ts';
import { createDedupCasPolicyPlugin, isDedupCasDuplicateReceipt } from '../plugins-policy/index.ts';
import {
  createOrcaSpawnExecutorPlugin,
  createOrcaWaveExecutorPlugin,
  createAgentPtyExecutorPlugin,
  createNotifyExecutorPlugin,
  createInboxExecutorPlugin,
} from '../plugins-executor/index.ts';
import { runCommand } from '../orca-workspace/index.ts';
import type { CommandRunner } from '../orca-workspace/index.ts';
import { MailboxStore, resolveMailboxId, WakeupRedriveService } from '../mailbox/index.ts';
import { createLocalAgentRouterPlugin, createLlmApiRouterPlugin } from '../plugins-router/index.ts';
import { createWebhookSourcePlugin, createPollWatermarkSourcePlugin, createScriptSourcePlugin } from '../plugins-source/index.ts';
import {
  getBuiltInProfile,
  saveJson,
  validateConfig,
  admissionMode,
  readProjectManifest,
  readPatch,
  validatePluginRefs,
} from './config.ts';
import type { RuntimeConfig, PluginSpec, DealerProjectPluginRef, DealerProjectManifest } from './config.ts';

export interface ExternalPluginContext {
  projectRoot?: string;
  stateDir: string;
  runtime: DealerNodeRuntime;
  userPatch?: Record<string, any>;
  api: {
    createWebhookSourcePlugin: typeof createWebhookSourcePlugin;
    createPollWatermarkSourcePlugin: typeof createPollWatermarkSourcePlugin;
    createScriptSourcePlugin: typeof createScriptSourcePlugin;
    createDedupCasPolicyPlugin: typeof createDedupCasPolicyPlugin;
    isDedupCasDuplicateReceipt: typeof isDedupCasDuplicateReceipt;
    createOrcaSpawnExecutorPlugin: typeof createOrcaSpawnExecutorPlugin;
    createOrcaWaveExecutorPlugin: typeof createOrcaWaveExecutorPlugin;
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
  private loadedProjects = new Map<string, { manifest: DealerProjectManifest; plugins: LoadedExternalPlugin[] }>();
  private loadedUserPlugins = new Map<string, LoadedExternalPlugin>();
  private externalChanges: Promise<unknown> = Promise.resolve();
  private server?: Server;
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

  async start(): Promise<this> {
    if (this.started) {
      return this;
    }
    mkdirSync(this.stateDir, { recursive: true, mode: 0o700 });
    chmodSync(this.stateDir, 0o700);
    try {
      mkdirSync(join(this.stateDir, 'runtime.lock'));
      this.ownsLock = true;
    } catch {
      throw new Error(
        'State directory already owned; use the running daemon (remove runtime.lock only after verifying its process stopped)',
      );
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
          defaultExecutor: this.config.defaultExecutor,
          resolveKindExecutor: kind => this.resolveTopicDefaultExecutor(kind),
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
      }
      for (const route of this.config.routes) {
        this.installRoute(route);
      }
      for (const source of this.config.sources) {
        await this.installSource(source);
      }
      for (const project of [...this.config.projects]) {
        await this.linkProject(project.rootPath, project.userPatchPath);
      }
      for (const ref of [...this.config.userPlugins]) {
        await this.installUserPlugin(ref);
      }
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
        createWebhookSourcePlugin,
        createPollWatermarkSourcePlugin,
        createScriptSourcePlugin,
        createDedupCasPolicyPlugin,
        isDedupCasDuplicateReceipt,
        createOrcaSpawnExecutorPlugin,
        createOrcaWaveExecutorPlugin,
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

  async linkProject(projectRoot: string, userPatchPath?: string): Promise<{
    projectId: string;
    defaultNetworkId: string;
    loadedPlugins: string[];
    userPatchPath?: string;
  }> {
    return this.changeExternal(async () => {
      const rootPath = resolve(projectRoot);
      const manifest = readProjectManifest(rootPath);
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
      });
      this.loadedProjects.set(manifest.projectId, { manifest, plugins });
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

  projectStatus(projectId?: string) {
    return {
      projects: this.config.projects.filter(project => !projectId || project.projectId === projectId).map(project => ({
        ...project,
        manifest: this.loadedProjects.get(project.projectId)?.manifest,
        loadedPlugins: (this.loadedProjects.get(project.projectId)?.plugins ?? []).map(entry => entry.plugin.name),
      })),
      userPlugins: this.config.userPlugins.filter(ref => this.loadedUserPlugins.has(ref.name)).map(ref => ({
        ...ref,
        loadedName: this.loadedUserPlugins.get(ref.name)!.plugin.name,
      })),
    };
  }

  resolveTopicDefaultExecutor(kind: string): string | undefined {
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
    const explicit = envelope.payload?.executor;
    if (typeof explicit === 'string' && explicit.trim().length > 0) {
      return explicit;
    }
    // Session control must land in the target mailbox, never in a new worktree.
    const type = envelope.semantic?.type;
    if (type === 'question' || type === 'reply' || type === 'settle') {
      return 'inbox';
    }
    return this.resolveTopicDefaultExecutor(envelope.kind) ?? this.config.defaultExecutor;
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
            const mailboxId = resolveMailboxId(envelope, this.resolveExecutorName(envelope));
            this.mailbox.recordInboundEnvelope(envelope, mailboxId);
          } catch (error) {
            this.lastError = String(error);
          }
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
            ...spawnConfig,
            projectRoots: {
              ...linkedProjectRoots,
              ...(spawnConfig.projectRoots ?? {}),
            },
            commandRunner: runner,
          }),
          'orca-wave': createOrcaWaveExecutorPlugin({
            ...this.config.executorConfig['orca-wave'],
            commandRunner: runner,
          }),
          'agent-pty': createAgentPtyExecutorPlugin({
            ...this.config.executorConfig['agent-pty'],
            commandRunner: runner,
          }),
          inbox: createInboxExecutorPlugin({
            ...this.config.executorConfig.inbox,
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
        for (const name of this.config.executors) {
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
              return executor.execute(local, context, config);
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
        }
        if (this.config.policies.includes('dedup-cas')) {
          const registration = ctx.services.dispatcher.registerPolicy(
            createDedupCasPolicyPlugin({
              watermarksPath: join(this.stateDir, 'dedup-watermarks.json'),
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
    await this.transport.mountNetwork(network);
    if (network.admissionMode) {
      await this.gate.setNetworkMode(network.networkId, network.admissionMode);
    }
    this.config.networks = this.config.networks.filter(n => n.networkId !== network.networkId);
    this.config.networks.push(structuredClone(network));
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
    if (this.server?.listening) {
      await new Promise<void>((resolve, reject) => {
        this.server!.close(error => (error ? reject(error) : resolve()));
      });
    }
    this.server = undefined;
    try {
      await this.wakeups?.stop();
    } finally {
      try {
        await this.kernel.stop();
      } finally {
        if (this.ownsLock) {
          rmSync(this.socketPath, { force: true });
          rmSync(join(this.stateDir, 'runtime.lock'), { recursive: true, force: true });
          this.ownsLock = false;
        }
        this.started = false;
      }
    }
  }
}
