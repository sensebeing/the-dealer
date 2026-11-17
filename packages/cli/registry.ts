import { join } from 'node:path';
import type { DispatchPolicyPlugin, ExecutorPlugin, AgentConversationMode } from '../core-types/index.ts';
import { normalizeConversationModeInput } from '../core-types/index.ts';
import type { MailboxStore, MailboxTopicBinding, WakeTargetState, WakeupRedriveService } from '../mailbox/index.ts';
import type { CommandRunner } from '../subprocess/index.ts';
import { createInboxExecutorPlugin, createNotifyExecutorPlugin } from '../plugins-executor/index.ts';
import { AGENT_HOST_DESCRIPTORS, createAgentPtyExecutorPlugin, createNamedHostExecutorPlugin, parseAgentHost, usesAgentHostInvocation } from '../plugins-agent-host/index.ts';
import { createOrcaExecutorPlugin, createOrcaSpawnExecutorPlugin, createOrcaWaveExecutorPlugin, validateOrcaBinding, orcaMailboxBinding, syncOrcaTopicWakeTarget, pinDeliveredOrcaMailbox } from '../plugins-orca/index.ts';
import { createDedupCasPolicyPlugin } from '../plugins-policy/index.ts';
import type { DedupCasOptions } from '../plugins-policy/index.ts';
import type { RuntimeConfig, TopicBindingConfig } from './config.ts';

type ExecutorConfig = Record<string, Record<string, unknown>>;
export interface BuiltinExecutorDeps {
  stateDir: string;
  config: Record<string, any>;
  store: MailboxStore;
  wakeup: WakeupRedriveService;
  wave: RuntimeConfig['wave'];
  projectRoots: Record<string, string>;
  commandRunner: CommandRunner;
  resolveTopicBinding: (kind: string, streamKey?: string) => (TopicBindingConfig & MailboxTopicBinding & { repoRoot?: string }) | undefined;
  resolveMailboxBinding: (kind: string, streamKey?: string) => MailboxTopicBinding | undefined;
}
export interface BuiltinPolicyDeps {
  stateDir: string;
  resolveLegacyWatermarkStatus: NonNullable<DedupCasOptions['resolveLegacyWatermarkStatus']>;
}
export interface ExecutorDescriptor {
  name: string;
  interactiveAgent?: boolean;
  acceptsInboxOnly?: boolean;
  validateBinding?(binding: TopicBindingConfig, kind: string): void;
  toMailboxBinding?(binding: TopicBindingConfig, executorConfig?: ExecutorConfig, payload?: Record<string, unknown>): MailboxTopicBinding;
  syncTopicWakeTarget?(ctx: { kind: string; mailboxId: string; binding: TopicBindingConfig; projectRoot?: string; store: MailboxStore; wave: RuntimeConfig['wave']; addressing: MailboxTopicBinding }): void;
  pinDeliveredInboundMailbox?(store: MailboxStore, mailboxId: string, target: WakeTargetState): string;
  create(deps: BuiltinExecutorDeps): { plugin: ExecutorPlugin; runtimeConfig?: Record<string, unknown> };
}
export interface PolicyDescriptor {
  name: string;
  create(deps: BuiltinPolicyDeps): DispatchPolicyPlugin;
}
export function isPluginName(name: unknown): name is string {
  return typeof name === 'string' && /^[a-z0-9][a-z0-9._-]{0,63}$/i.test(name);
}

/** Configuration describes available factories; the dispatcher owns live registrations. */
export class HostPluginRegistry {
  private executors = new Map<string, ExecutorDescriptor>();
  private policies = new Map<string, PolicyDescriptor>();
  registerExecutor(descriptor: ExecutorDescriptor): void {
    if (!isPluginName(descriptor.name) || this.executors.has(descriptor.name)) throw new Error(`Invalid or duplicate executor descriptor: ${descriptor.name}`);
    this.executors.set(descriptor.name, { ...descriptor, validateBinding: (binding, kind) => {
      if (binding.mode === 'inbox-only' && !descriptor.acceptsInboxOnly) throw new Error(`Invalid topic binding: ${kind}`);
      descriptor.validateBinding?.(binding, kind);
    } });
  }
  registerPolicy(descriptor: PolicyDescriptor): void {
    if (!isPluginName(descriptor.name) || this.policies.has(descriptor.name)) throw new Error(`Invalid or duplicate policy descriptor: ${descriptor.name}`);
    this.policies.set(descriptor.name, descriptor);
  }
  executor(name: string): ExecutorDescriptor | undefined { return this.executors.get(name); }
  policy(name: string): PolicyDescriptor | undefined { return this.policies.get(name); }
  executorNames(): string[] { return [...this.executors.keys()]; }
  policyNames(): string[] { return [...this.policies.keys()]; }
  hostPreset(host: string) {
    const descriptor = AGENT_HOST_DESCRIPTORS.find(entry => entry.host === host);
    if (!descriptor) throw new Error(`Unsupported agent host: ${host}`);
    return { ...descriptor,
      executor: 'bindingExecutor' in descriptor ? descriptor.bindingExecutor : 'executor' in descriptor ? descriptor.executor : descriptor.canonical,
      defaultExecutor: 'executor' in descriptor ? descriptor.executor : 'bindingExecutor' in descriptor ? descriptor.bindingExecutor : descriptor.canonical,
      managedWorkspace: 'managedWorkspace' in descriptor && descriptor.managedWorkspace,
      defaultBindingAgent: 'defaultBindingAgent' in descriptor ? descriptor.defaultBindingAgent : descriptor.host,
      acceptsBindingAgent: 'defaultBindingAgent' in descriptor,
      requiresTargetBot: 'requiresTargetBot' in descriptor && descriptor.requiresTargetBot,
    };
  }
  toMailboxBinding(binding: TopicBindingConfig, config?: ExecutorConfig, payload?: Record<string, unknown>): MailboxTopicBinding {
    return this.executor(binding.executor)?.toMailboxBinding?.(binding, config, payload)
      ?? mailboxBinding(binding, explicitMode(binding, config, payload) ?? 'spawn');
  }
  pinDeliveredInboundMailbox(store: MailboxStore, mailboxId: string, target: WakeTargetState): string {
    const hooks = new Set([...this.executors.values()].map(entry => entry.pinDeliveredInboundMailbox).filter(hook => hook !== undefined));
    for (const hook of hooks) {
      const pinned = hook(store, mailboxId, target);
      if (pinned !== mailboxId) return pinned;
    }
    return mailboxId;
  }
}

function explicitMode(binding: TopicBindingConfig, config?: ExecutorConfig, payload?: Record<string, unknown>): AgentConversationMode | undefined {
  let payloadMode: AgentConversationMode | undefined;
  // Queued unvalidated hints never gain addressing authority; execution validates them later.
  try { payloadMode = normalizeConversationModeInput(payload); } catch { /* Invalid payload has no mode authority. */ }
  return payloadMode ?? normalizeConversationModeInput(binding) ?? normalizeConversationModeInput(config?.[binding.executor]);
}
function mailboxBinding(binding: TopicBindingConfig, mode: AgentConversationMode): MailboxTopicBinding {
  return { ...(binding.mailboxId !== undefined ? { mailboxId: binding.mailboxId } : {}),
    ...(binding.streamKey !== undefined ? { streamKey: binding.streamKey } : {}), mode };
}
function validateAgent(binding: TopicBindingConfig): void {
  if (binding.agent !== undefined) parseAgentHost(binding.agent);
}
function agentOptions(deps: BuiltinExecutorDeps) {
  return { ...deps.config, store: deps.store, wakeup: deps.wakeup,
    resolveTopicBinding: deps.resolveTopicBinding, commandRunner: deps.commandRunner };
}

export function createDefaultHostPluginRegistry(): HostPluginRegistry {
  const registry = new HostPluginRegistry();
  for (const [name, factory] of Object.entries({ 'orca': createOrcaExecutorPlugin, 'orca-spawn': createOrcaSpawnExecutorPlugin, 'orca-wave': createOrcaWaveExecutorPlugin })) {
    registry.registerExecutor({ name, interactiveAgent: true,
      validateBinding: validateOrcaBinding,
      toMailboxBinding: (binding, config, payload) => orcaMailboxBinding(binding, explicitMode(binding, config, payload)),
      syncTopicWakeTarget: syncOrcaTopicWakeTarget,
      pinDeliveredInboundMailbox: pinDeliveredOrcaMailbox,
      create: deps => ({ plugin: factory({ ...agentOptions(deps), projectRoots: { ...deps.projectRoots, ...deps.config.projectRoots } }),
        runtimeConfig: { ...deps.config, store: deps.store, wakeup: deps.wakeup, wave: deps.wave } }),
    });
  }
  for (const host of AGENT_HOST_DESCRIPTORS) {
    if (!('executor' in host) || registry.executor(host.executor)) continue;
    registry.registerExecutor({ name: host.executor, interactiveAgent: true,
      validateBinding: binding => {
        validateAgent(binding);
        if (binding.mode !== undefined && !(('executorModes' in host ? host.executorModes : host.modes) as readonly string[]).includes(binding.mode)) throw new Error('Unsupported host conversation mode');
      },
      create: deps => ({ plugin: createNamedHostExecutorPlugin(host.executor, host.host, agentOptions(deps)) }),
    });
  }
  registry.registerExecutor({ name: 'agent-pty', interactiveAgent: true, validateBinding: validateAgent,
    toMailboxBinding: (binding, config, payload) => {
      const explicit = explicitMode(binding, config, payload);
      let agent: ReturnType<typeof parseAgentHost> | undefined;
      try { agent = parseAgentHost(payload?.agent ?? binding.agent ?? config?.[binding.executor]?.agent); }
      catch { /* An absent or malformed host hint cannot select host-backed addressing. */ }
      return mailboxBinding(binding, explicit ?? (usesAgentHostInvocation(agent, explicit) ? 'spawn' : 'inbox-only'));
    },
    create: deps => ({ plugin: createAgentPtyExecutorPlugin(agentOptions(deps)) }),
  });
  registry.registerExecutor({ name: 'inbox', acceptsInboxOnly: true, validateBinding: validateAgent,
    toMailboxBinding: (binding, config, payload) => mailboxBinding(binding, explicitMode(binding, config, payload) ?? 'inbox-only'),
    create: deps => ({ plugin: createInboxExecutorPlugin({ ...agentOptions(deps), resolveTopicBinding: deps.resolveMailboxBinding }) }),
  });
  registry.registerExecutor({ name: 'notify', validateBinding: validateAgent,
    toMailboxBinding: (binding, config, payload) => mailboxBinding(binding, explicitMode(binding, config, payload) ?? 'inbox-only'),
    create: deps => ({ plugin: createNotifyExecutorPlugin({ inboxPath: join(deps.stateDir, 'notifications.jsonl'), ...deps.config, commandRunner: deps.commandRunner }) }),
  });
  registry.registerPolicy({ name: 'dedup-cas', create: deps => createDedupCasPolicyPlugin({
    watermarksPath: join(deps.stateDir, 'dedup-watermarks.json'), resolveLegacyWatermarkStatus: deps.resolveLegacyWatermarkStatus,
  }) });
  return registry;
}
export const DEFAULT_HOST_PLUGIN_REGISTRY = createDefaultHostPluginRegistry();
