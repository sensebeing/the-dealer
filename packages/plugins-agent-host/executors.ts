import { randomUUID } from 'node:crypto';
import type { ExecutorPlugin, MessageEnvelope } from '../core-types/index.ts';
import { normalizeConversationModeInput } from '../core-types/index.ts';
import {
  buildAgentHostInvocation,
  resolveAgentSessionId,
  usesAgentHostInvocation,
  normalizeAgentHost,
  trustAntigravityWorkspace,
  parseAgentHost,
  defaultAgentCommandArgs,
} from './hosts.ts';
import type { SupportedAgentHost, AgentConversationMode, ActiveConversationMode } from './hosts.ts';
import { canInheritThreadMailbox, groupThreadBatch, hasExplicitExecutor, resolveBoundMailboxId, resolveMailboxId, resolveSessionOrThreadMailbox, resolveSpawnMailboxId, resolveTopicAddress } from '../mailbox/index.ts';
import type { MailboxStore, MailboxTopicBinding, WakeupRedriveService } from '../mailbox/index.ts';
import { checkedCommand, singleLine, runCommand } from '../subprocess/index.ts';
import type { CommandRequest, CommandRunner } from '../subprocess/index.ts';
import {
  prepareInboxDeliveries,
  executeDeliveryGroups,
  waitForSpawnClaims,
  withSpawnThreadLock,
  createRawCommandExecutorPlugin,
  renderWave,
  beforeExternalEffect,
  prepareExternalEffect,
  rethrowBeforeCommandStart,
} from '../plugins-executor/index.ts';
import { deliverAgentHostWakeTarget, serializeAgentHostDriverConfig } from './wakeup.ts';

export interface AgentHostTopicBinding extends MailboxTopicBinding {
  agent?: SupportedAgentHost;
  workspacePath?: string;
  repoRoot?: string;
  agentProfile?: string;
  targetBot?: string;
  sessionId?: string;
}

export interface AgentOptions {
  wakeup?: WakeupRedriveService;
  store?: MailboxStore;
  resolveTopicBinding?: (kind: string, streamKey?: string) => AgentHostTopicBinding | undefined;
  agent?: SupportedAgentHost;
  mode?: AgentConversationMode;
  conversationMode?: AgentConversationMode;
  agentProfile?: string;
  targetBot?: string;
  sessionId?: string;
  command?: string;
  args?: readonly string[];
  cwd?: string;
  commandRunner?: CommandRunner;
}

export function createAgentPtyExecutorPlugin(options: AgentOptions = {}): ExecutorPlugin {
  return createAgentExecutorPlugin('agent-pty', options);
}

export function createCodexExecutorPlugin(options: Omit<AgentOptions, 'agent'> = {}): ExecutorPlugin {
  return createNamedHostExecutorPlugin('codex', 'codex', options);
}

export function createClaudeCodeExecutorPlugin(options: Omit<AgentOptions, 'agent'> = {}): ExecutorPlugin {
  return createNamedHostExecutorPlugin('claude-code', 'claude-code', options);
}

export function createAntigravityExecutorPlugin(
  options: Omit<AgentOptions, 'agent'> & { agySettingsPath?: string } = {},
): ExecutorPlugin {
  return createNamedHostExecutorPlugin('antigravity', 'antigravity', options);
}

export function createCodexCliExecutorPlugin(options: Omit<AgentOptions, 'agent'> = {}): ExecutorPlugin {
  return createNamedHostExecutorPlugin('codex-cli', 'codex-cli', options);
}
export function createClaudeAppExecutorPlugin(options: Omit<AgentOptions, 'agent'> = {}): ExecutorPlugin {
  return createNamedHostExecutorPlugin('claude-app', 'claude-app', options);
}
export function createChatgptAppExecutorPlugin(options: Omit<AgentOptions, 'agent'> = {}): ExecutorPlugin {
  return createNamedHostExecutorPlugin('chatgpt-app', 'chatgpt-app', options);
}
export function createAntigravityCliExecutorPlugin(
  options: Omit<AgentOptions, 'agent'> = {},
): ExecutorPlugin {
  return createNamedHostExecutorPlugin('antigravity-cli', 'antigravity-cli', options);
}
export function createAntigravityAppExecutorPlugin(
  options: Omit<AgentOptions, 'agent'> = {},
): ExecutorPlugin {
  return createNamedHostExecutorPlugin('antigravity-app', 'antigravity-app', options);
}

export function createNamedHostExecutorPlugin(
  name: string,
  host: SupportedAgentHost,
  options: AgentOptions,
): ExecutorPlugin {
  return createAgentExecutorPlugin(
    name,
    options,
    host,
    (options as AgentOptions & { agySettingsPath?: string }).agySettingsPath,
  );
}

type HostDelivery = {
  type: 'host';
  mode: ActiveConversationMode;
  mailboxId: string;
  request: CommandRequest;
  driverConfig: Record<string, unknown>;
  trustWorkspace?: string;
};

function prepareAgentDelivery(
  envelope: MessageEnvelope,
  promptBatch: readonly MessageEnvelope[],
  options: AgentOptions,
  config: Partial<AgentOptions>,
  store: MailboxStore | undefined,
  host: SupportedAgentHost | undefined,
): HostDelivery | { type: 'inbox'; mailboxId: string } | { type: 'raw' } {
  const payload = envelope.payload ?? {};
  const streamKey = typeof payload.streamKey === 'string' ? payload.streamKey : undefined;
  const binding = (options.resolveTopicBinding ?? config.resolveTopicBinding)?.(envelope.kind, streamKey);
  const agentInput = host ?? payload.agent ?? binding?.agent ?? config.agent ?? options.agent;
  const agent = agentInput === undefined ? undefined : parseAgentHost(agentInput);
  let rawMode: AgentConversationMode | undefined;
  try {
    rawMode = normalizeConversationModeInput(payload) ?? normalizeConversationModeInput(binding) ??
      normalizeConversationModeInput(config) ?? normalizeConversationModeInput(options) ?? (host ? 'spawn' : undefined);
  } catch {
    throw new Error('Invalid agent conversationMode');
  }
  const usesHost = usesAgentHostInvocation(agent, rawMode, Boolean(host));
  const threadLocalMailbox = store?.resolveThreadLocalMailbox(envelope.semantic?.threadId, envelope.networkId, {
    excludeInboundMessageId: envelope.id,
  });
  const addressing: MailboxTopicBinding = {
    mailboxId: binding?.mailboxId,
    streamKey: binding?.streamKey,
    stream: binding?.stream,
    mode: rawMode ?? 'spawn',
  };
  const boundMailboxId = resolveBoundMailboxId(resolveTopicAddress(envelope.kind, streamKey).kind, addressing, rawMode);
  if (store && !hasExplicitExecutor(envelope)) {
    const sessionMailbox = resolveSessionOrThreadMailbox(envelope, threadLocalMailbox);
    if (sessionMailbox !== undefined && (sessionMailbox !== boundMailboxId || !usesHost)) {
      return { type: 'inbox', mailboxId: sessionMailbox };
    }
  }
  if (!usesHost) return { type: 'raw' };
  if (rawMode === 'inbox-only') {
    if (store && binding?.mode === 'inbox-only') {
      return { type: 'inbox', mailboxId: resolveMailboxId(envelope, binding, threadLocalMailbox) };
    }
    throw new Error('Invalid agent conversationMode');
  }
  const conversationMode: ActiveConversationMode = rawMode ?? 'spawn';
  const mailboxId = conversationMode === 'spawn'
    ? addressing.mailboxId !== undefined ? boundMailboxId : resolveSpawnMailboxId(envelope.id)
    : resolveMailboxId(envelope, addressing, threadLocalMailbox, 'inbox-only');
  const cwd = binding?.workspacePath ?? binding?.repoRoot ?? config.cwd ?? options.cwd;
  const command = config.command ?? options.command;
  const agentProfile = optionalHostString(
    payload.agentProfile ?? binding?.agentProfile ?? config.agentProfile ?? options.agentProfile, 'agentProfile',
  );
  const targetBot = optionalHostString(
    payload.targetBot ?? binding?.targetBot ?? config.targetBot ?? options.targetBot, 'targetBot',
  );
  const sessionId = resolveAgentSessionId(mailboxId, optionalHostString(
    payload.sessionId ?? binding?.sessionId ?? config.sessionId ?? options.sessionId, 'sessionId',
  ), agent ?? 'codex');
  const terminalPrompt = typeof payload.terminalPrompt === 'string' ? payload.terminalPrompt.trim() : undefined;
  const invocation = buildAgentHostInvocation({
    agent: agent ?? 'codex', conversationMode, mailboxId,
    promptText: singleLine(terminalPrompt || renderWave(promptBatch)),
    cwd, command, agentProfile, targetBot, sessionId,
  });
  const canonicalHost = agent ? normalizeAgentHost(agent) : undefined;
  return {
    type: 'host',
    mode: conversationMode,
    mailboxId,
    request: { ...invocation, cwd },
    driverConfig: serializeAgentHostDriverConfig({
      hostAgent: agent ?? 'codex', conversationMode, sessionId,
      ...(cwd ? { workspacePath: cwd } : {}),
      ...(command ? { interactiveCommand: command } : {}),
      ...(agentProfile ? { agentProfile } : {}),
      ...(targetBot ? { targetBot } : {}),
    }),
    ...(canonicalHost === 'antigravity' || canonicalHost === 'antigravity-app' ? { trustWorkspace: cwd } : {}),
  };
}

function commandOutput(output: { stdout: string; stderr?: string }) {
  let result: unknown;
  try {
    result = JSON.parse(output.stdout);
  } catch {
    result = output.stdout;
  }
  return { output: result, stderr: output.stderr ?? '' };
}

function createAgentExecutorPlugin(
  name: string,
  options: AgentOptions,
  host?: SupportedAgentHost,
  agySettingsPath?: string,
): ExecutorPlugin {
  const runner = options.commandRunner ?? runCommand;
  const mayTrustWorkspace = agySettingsPath !== undefined || runner === runCommand;
  const rawCommand = options.command ?? 'codex';
  const rawExecutor = createRawCommandExecutorPlugin({
    ...options, command: rawCommand, args: options.args ?? defaultAgentCommandArgs(rawCommand),
  });
  const trustWorkspace = (delivery: HostDelivery) => {
    if (mayTrustWorkspace && delivery.trustWorkspace) trustAntigravityWorkspace(delivery.trustWorkspace, agySettingsPath);
  };
  return {
    name,
    async execute(batch, context, runtimeConfig) {
      const first = batch[0];
      if (!first) throw beforeExternalEffect(new Error('Empty agent batch'));
      const config = (runtimeConfig ?? {}) as Partial<AgentOptions>;
      const store = options.store ?? config.store;
      const wakeup = options.wakeup ?? config.wakeup;
      if (store) await waitForSpawnClaims(store, batch);
      const threadGroups = store ? prepareExternalEffect(() => groupThreadBatch(batch, store)) : batch.map(envelope => [envelope]);
      const deliveries = threadGroups.map((envelopes, index) => ({
        envelopes,
        delivery: prepareExternalEffect(() =>
          prepareAgentDelivery(envelopes[0], !store && index === 0 ? batch : envelopes, options, config, store, host),
        ),
      }));
      if (!store) {
        const { delivery } = deliveries[0];
        if (delivery.type === 'raw') return rawExecutor.execute(batch, context, runtimeConfig);
        if (delivery.type !== 'host') throw beforeExternalEffect(new Error('Inbox delivery requires a mailbox store'));
        trustWorkspace(delivery);
        const output = await checkedCommand(runner, { ...delivery.request, signal: context.signal }).catch(rethrowBeforeCommandStart);
        return { executor: name, executionId: randomUUID(), ...commandOutput(output) };
      }

      const inboxGroups = new Map<string, MessageEnvelope[]>();
      const hostGroups = new Map<string, { delivery: HostDelivery; envelopes: MessageEnvelope[] }>();
      const spawnGroups: { delivery: HostDelivery; envelopes: MessageEnvelope[] }[] = [];
      const rawBatch: MessageEnvelope[] = [];
      for (const { envelopes, delivery } of deliveries) {
        switch (delivery.type) {
          case 'raw':
            rawBatch.push(...envelopes);
            break;
          case 'inbox': {
            const group = inboxGroups.get(delivery.mailboxId) ?? [];
            group.push(...envelopes);
            inboxGroups.set(delivery.mailboxId, group);
            break;
          }
          case 'host': {
            if (delivery.mode === 'spawn') {
              spawnGroups.push({ delivery, envelopes });
              break;
            }
            const group = hostGroups.get(delivery.mailboxId) ?? { delivery, envelopes: [] };
            group.envelopes.push(...envelopes);
            hostGroups.set(delivery.mailboxId, group);
            break;
          }
        }
      }
      const groups = await prepareInboxDeliveries(store, wakeup, inboxGroups, context);
      for (const { delivery, envelopes } of [...hostGroups.values(), ...spawnGroups]) {
        const spawning = delivery.mode === 'spawn';
        const proposedSpawn = spawning && canInheritThreadMailbox(envelopes[0]);
        groups.push({ envelopes, async deliver() {
          const { mailboxId } = delivery;
          const claim = wakeup?.claim(mailboxId);
          let woke = false;
          try {
            const enqueue = () => store.enqueue(envelopes, {
              signedEnvelopes: context?.signedEnvelopes, mailboxId, proposedSpawn,
              wakeTarget: { driver: 'agent-host', driverConfig: delivery.driverConfig },
            });
            const result = spawning ? await withSpawnThreadLock(store, envelopes[0], enqueue) : enqueue();
            const receipt = { ...result, executor: name, executionId: randomUUID(),
              ...(result.mailboxIds.length === 1 ? { mailboxId: result.mailboxIds[0] } : {}) };
            if (result.claimedSpawn === false) {
              for (const actualMailbox of result.mailboxIds) wakeup?.notifyMailbox(actualMailbox);
              return receipt;
            }
            try {
              trustWorkspace(delivery);
              const response = await deliverAgentHostWakeTarget(store, runner, mailboxId, { signal: context.signal });
              woke = true;
              return { ...receipt, woke, ...commandOutput(response) };
            } catch (error) {
              return { ...receipt, woke, reason: String(error) };
            }
          } finally {
            claim?.dispose();
            if (woke) wakeup?.notifyMailbox(mailboxId);
          }
        } });
      }
      if (rawBatch.length) groups.push({ envelopes: rawBatch, deliver: () => rawExecutor.execute(rawBatch, context, runtimeConfig) });
      return executeDeliveryGroups(name, batch, groups);
    },
  };
}

function optionalHostString(value: unknown, name: string): string | undefined {
  if (value !== undefined && typeof value !== 'string')
    throw new Error(`Invalid host configuration: ${name}`);
  return value as string | undefined;
}
