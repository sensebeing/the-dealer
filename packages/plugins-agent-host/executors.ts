import { randomUUID } from 'node:crypto';
import type { ExecutorPlugin } from '../core-types/index.ts';
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
import { MailboxStore, resolveBoundMailboxId, resolveMailboxId, resolveSessionOrThreadMailbox, resolveTopicAddress } from '../mailbox/index.ts';
import type { MailboxTopicBinding, WakeupRedriveService } from '../mailbox/index.ts';
import { checkedCommand, singleLine, runCommand } from '../subprocess/index.ts';
import type { CommandRunner } from '../subprocess/index.ts';
import {
  createInboxExecutorPlugin,
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

function createAgentExecutorPlugin(
  name: string,
  options: AgentOptions,
  host?: SupportedAgentHost,
  agySettingsPath?: string,
): ExecutorPlugin {
  return {
    name,
    async execute(batch, context, runtimeConfig) {
      const config = (runtimeConfig ?? {}) as Partial<AgentOptions>;
      const first = batch[0];
      if (!first) throw beforeExternalEffect(new Error('Empty agent batch'));
      const payload = first?.payload ?? {};
      const streamKey = typeof payload.streamKey === 'string' ? payload.streamKey : undefined;
      const binding = (options.resolveTopicBinding ?? config.resolveTopicBinding)?.(first.kind, streamKey);
      const agentInput = host ?? payload.agent ?? binding?.agent ?? config.agent ?? options.agent;
      const agent =
        agentInput === undefined ? undefined : prepareExternalEffect(() => parseAgentHost(agentInput));
      let rawMode: AgentConversationMode | undefined;
      try {
        rawMode =
          [payload, binding, config, options].reduce<AgentConversationMode | undefined>(
            (acc, layer) => acc ?? normalizeConversationModeInput(layer),
            undefined,
          ) ?? (host ? 'spawn' : undefined);
      } catch {
        throw beforeExternalEffect(new Error('Invalid agent conversationMode'));
      }
      const canonicalHost = agent ? prepareExternalEffect(() => normalizeAgentHost(agent)) : undefined;
      const hostMode = prepareExternalEffect(() => usesAgentHostInvocation(agent, rawMode, Boolean(host)));
      if (!hostMode) {
        const command = options.command ?? 'codex';
        return createRawCommandExecutorPlugin({
          ...options,
          command,
          args: options.args ?? defaultAgentCommandArgs(command),
        }).execute(batch, context, runtimeConfig);
      }
      let request;
      let mailboxId: string | undefined;
      let effectiveSessionId: string | undefined;
      let wakeDelivered = false;
      const store = options.store ?? config.store;
      const runner = options.commandRunner ?? runCommand;
      let claim: ReturnType<WakeupRedriveService['claim']> | undefined;
      try {
        {
          if (rawMode === 'inbox-only') {
            if (store && binding?.mode === 'inbox-only')
              return createInboxExecutorPlugin({ store }).execute(batch, context, runtimeConfig);
            throw beforeExternalEffect(new Error('Invalid agent conversationMode'));
          }
          if (rawMode !== undefined && rawMode !== 'spawn' && rawMode !== 'wave' && rawMode !== 'fixed')
            throw beforeExternalEffect(new Error('Invalid agent conversationMode'));
          const conversationMode = (rawMode ?? 'spawn') as ActiveConversationMode;
          const addressing = {
            mailboxId: binding?.mailboxId,
            streamKey: binding?.streamKey,
            stream: binding?.stream,
            mode: conversationMode,
          };
          const threadLocalMailbox = store?.resolveThreadLocalMailbox(first.semantic?.threadId, first.networkId, {
            excludeInboundMessageId: first.id,
          });
          if (store && !(typeof payload.executor === 'string' && payload.executor.trim())) {
            const sessionMailbox = prepareExternalEffect(() => resolveSessionOrThreadMailbox(first, threadLocalMailbox));
            if (sessionMailbox !== undefined) {
              const boundMailboxId = conversationMode === 'spawn' ? 'default' : prepareExternalEffect(() =>
                resolveBoundMailboxId(resolveTopicAddress(first.kind, streamKey).kind, addressing),
              );
              if (sessionMailbox !== boundMailboxId) {
                const receipt = await createInboxExecutorPlugin({ store, wakeup: options.wakeup ?? config.wakeup })
                  .execute(batch, context, runtimeConfig);
                return { ...receipt, executor: name };
              }
            }
          }
          mailboxId =
            conversationMode === 'spawn'
              ? `task:${first.id}`
              : prepareExternalEffect(() => resolveMailboxId(first, addressing, threadLocalMailbox, 'inbox-only'));
          const terminalPrompt =
            typeof payload.terminalPrompt === 'string'
              ? payload.terminalPrompt.trim() || undefined
              : undefined;
          const cwd = binding?.workspacePath ?? binding?.repoRoot ?? config.cwd ?? options.cwd;
          const command = config.command ?? options.command;
          const agentProfile = prepareExternalEffect(() =>
            optionalHostString(
              payload.agentProfile ?? binding?.agentProfile ?? config.agentProfile ?? options.agentProfile,
              'agentProfile',
            ),
          );
          const targetBot = prepareExternalEffect(() =>
            optionalHostString(
              payload.targetBot ?? binding?.targetBot ?? config.targetBot ?? options.targetBot,
              'targetBot',
            ),
          );
          const sessionId = prepareExternalEffect(() =>
            optionalHostString(
              payload.sessionId ?? binding?.sessionId ?? config.sessionId ?? options.sessionId,
              'sessionId',
            ),
          );
          effectiveSessionId = prepareExternalEffect(() =>
            resolveAgentSessionId(mailboxId, sessionId, agent ?? 'codex'),
          );
          const invocation = prepareExternalEffect(() =>
            buildAgentHostInvocation({
              agent: agent ?? 'codex',
              conversationMode,
              mailboxId,
              promptText: singleLine(terminalPrompt ?? renderWave(batch)),
              cwd,
              command,
              agentProfile,
              targetBot,
              sessionId: effectiveSessionId,
            }),
          );
          if (
            (canonicalHost === 'antigravity' || canonicalHost === 'antigravity-app') &&
            cwd &&
            (agySettingsPath !== undefined ||
              options.commandRunner === undefined ||
              options.commandRunner === runCommand)
          ) {
            trustAntigravityWorkspace(cwd, agySettingsPath);
          }
          claim = (options.wakeup ?? config.wakeup)?.claim(mailboxId);
          if (store) {
            store.enqueue(batch, {
              ...(context?.signedEnvelopes ? { signedEnvelopes: context.signedEnvelopes } : {}),
              mailboxId,
              wakeTarget: {
                driver: 'agent-host',
                driverConfig: serializeAgentHostDriverConfig({
                  hostAgent: agent ?? 'codex',
                  conversationMode,
                  ...(cwd ? { workspacePath: cwd } : {}),
                  ...(command ? { interactiveCommand: command } : {}),
                  ...(agentProfile ? { agentProfile } : {}),
                  ...(targetBot ? { targetBot } : {}),
                  sessionId: effectiveSessionId,
                }),
              },
            });
          } else {
            request = { ...invocation, cwd };
          }
        }
        let output;
        if (store && mailboxId) {
          // The mailbox owner builds the pointer and settles the lease for both initial and redrive paths.
          try {
            const delivered = await deliverAgentHostWakeTarget(store, runner, mailboxId, {
              signal: context.signal,
            });
            output = { stdout: delivered.stdout, stderr: delivered.stderr };
            wakeDelivered = true;
          } catch (error) {
            return {
              executor: name,
              executionId: randomUUID(),
              mailboxId,
              woke: false,
              reason: String(error),
            };
          }
        } else {
          output = await checkedCommand(runner, { ...request, signal: context.signal }).catch(
            rethrowBeforeCommandStart,
          );
        }
        let structuredOutput: unknown;
        try {
          structuredOutput = JSON.parse(output.stdout);
        } catch {
          structuredOutput = output.stdout;
        }
        return {
          executor: name,
          executionId: randomUUID(),
          output: structuredOutput,
          stderr: output.stderr ?? '',
          ...(mailboxId ? { mailboxId } : {}),
          ...(wakeDelivered ? { woke: true } : {}),
        };
      } finally {
        claim?.dispose();
        if (wakeDelivered && mailboxId) (options.wakeup ?? config.wakeup)?.notifyMailbox(mailboxId);
      }
    },
  };
}

function optionalHostString(value: unknown, name: string): string | undefined {
  if (value !== undefined && typeof value !== 'string')
    throw new Error(`Invalid host configuration: ${name}`);
  return value as string | undefined;
}
