import { ACTIVE_CONVERSATION_MODES } from '../core-types/index.ts';
import { buildAgentHostInvocation, resolveAgentHostContinuation, parseAgentHost } from './hosts.ts';
import type { SupportedAgentHost, ActiveConversationMode } from './hosts.ts';
import { checkedCommand, runCommand } from '../subprocess/index.ts';
import type { CommandRunner } from '../subprocess/index.ts';
import {
  buildWakeupPointer,
  maxPreviewSequence,
  recordWakeFailure,
  settleWakeDeliveryFailure,
  SettledWakeLaunchError,
} from '../mailbox/wakeup.ts';
import type { WakeupDriver } from '../mailbox/wakeup.ts';
import type { MailboxStore, WakeTargetState } from '../mailbox/store.ts';

export interface AgentHostDriverConfig {
  hostAgent?: SupportedAgentHost;
  conversationMode?: ActiveConversationMode;
  workspacePath?: string;
  interactiveCommand?: string;
  agentProfile?: string;
  targetBot?: string;
  sessionId?: string;
}

export function serializeAgentHostDriverConfig(config: AgentHostDriverConfig): Record<string, unknown> {
  const value = Object.fromEntries(Object.entries(config).filter(([, entry]) => entry !== undefined));
  readAgentHostDriverConfig({ driverConfig: value });
  return value;
}

export function readAgentHostDriverConfig(
  target: Pick<WakeTargetState, 'driverConfig'>,
): AgentHostDriverConfig {
  const config = target.driverConfig ?? {};
  if (config.hostAgent !== undefined) parseAgentHost(config.hostAgent);
  if (
    config.conversationMode !== undefined &&
    !ACTIVE_CONVERSATION_MODES.includes(config.conversationMode as ActiveConversationMode)
  ) {
    throw new Error('Invalid host conversation mode');
  }
  for (const field of ['workspacePath', 'interactiveCommand', 'agentProfile', 'targetBot', 'sessionId']) {
    if (config[field] !== undefined && typeof config[field] !== 'string')
      throw new Error(`Invalid host configuration: ${field}`);
  }
  return config as AgentHostDriverConfig;
}

/**
 * Single owner for one passive host delivery attempt. Validation, pointer construction and
 * invocation building happen before `beginWakeDelivery`; only the host invocation itself crosses
 * the external boundary, so unknown outcomes stay fenced while never-started commands retry.
 */
export async function deliverAgentHostWakeTarget(
  store: MailboxStore,
  runner: CommandRunner,
  mailboxId: string,
  options: { signal?: AbortSignal; now?: number } = {},
): Promise<{ pointer: string; stdout: string; stderr?: string }> {
  const now = options.now ?? Date.now();
  let deliveryBegun = false;
  try {
    const state = store.getWakeTarget(mailboxId);
    const target = state ? { ...readAgentHostDriverConfig(state), ...state } : undefined;
    if (!target?.hostAgent) throw new Error(`Passive wake target missing host agent: ${mailboxId}`);
    const preview = store.check({ mailboxId, peek: true });
    const pointer = buildWakeupPointer(mailboxId, target, preview);
    if (!pointer) throw new Error(`No pending Dealer messages in ${mailboxId}`);
    const invocation = buildAgentHostInvocation({
      agent: target.hostAgent,
      conversationMode: target.conversationMode ?? 'fixed',
      mailboxId,
      promptText: pointer,
      cwd: target.workspacePath,
      command: target.interactiveCommand,
      agentProfile: target.agentProfile,
      targetBot: target.targetBot,
      sessionId: target.sessionId,
    });
    store.beginWakeDelivery(mailboxId);
    deliveryBegun = true;
    const output = await checkedCommand(runner, {
      ...invocation,
      cwd: target.workspacePath,
      signal: options.signal,
    });
    const continuation = resolveAgentHostContinuation(
      target.hostAgent,
      mailboxId,
      target.sessionId,
      output.stdout,
      target.conversationMode ?? 'fixed',
    );
    store.completeWakeDelivery(mailboxId, maxPreviewSequence(preview), {
      driver: continuation.hostAgent ? 'agent-host' : 'passive',
      driverConfig: continuation.hostAgent ? continuation : undefined,
    });
    return { pointer, stdout: output.stdout, stderr: output.stderr };
  } catch (error) {
    const fenced = store.getWakeTarget(mailboxId)?.deliveryInDoubt === true;
    if (fenced) {
      // A pre-existing fence refuses the attempt before the boundary; it keeps the original
      // unknown-outcome reason. Only this attempt's own post-boundary failure may settle it.
      if (deliveryBegun) settleWakeDeliveryFailure(store, mailboxId, error, now);
    } else {
      recordWakeFailure(store, mailboxId, error, now);
    }
    throw error;
  }
}

export function createAgentHostWakeupDriver(options: { commandRunner?: CommandRunner } = {}): WakeupDriver {
  return {
    name: 'agent-host',
    async deliver({ store, mailboxId, now }) {
      try {
        const delivered = await deliverAgentHostWakeTarget(
          store,
          options.commandRunner ?? runCommand,
          mailboxId,
          { now: now() },
        );
        return { kind: 'delivered', pointer: delivered.pointer };
      } catch (error) {
        throw new SettledWakeLaunchError(String(error), { cause: error });
      }
    },
  };
}
