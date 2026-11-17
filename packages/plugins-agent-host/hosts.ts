import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { writeFileAtomically } from '../fs-atomic/index.ts';
import { CONVERSATION_MODES, ACTIVE_CONVERSATION_MODES } from '../core-types/index.ts';
import type { ActiveConversationMode, AgentConversationMode } from '../core-types/index.ts';
export {
  CONVERSATION_MODES,
  ACTIVE_CONVERSATION_MODES,
  normalizeConversationModeInput,
} from '../core-types/index.ts';
export type { AgentConversationMode, ActiveConversationMode } from '../core-types/index.ts';

export type SupportedAgentHost = (typeof AGENT_HOST_DESCRIPTORS)[number]['host'];
export function resolveHostSessionMode(mode: ActiveConversationMode): 'spawn' | 'fixed' {
  return mode === 'fixed' ? 'fixed' : 'spawn';
}

export function parseAgentHost(value: unknown): SupportedAgentHost {
  if (typeof value !== 'string' || !AGENT_HOST_DESCRIPTORS.some((entry) => entry.host === value))
    throw new Error(`Unsupported agent host: ${String(value)}`);
  return value as SupportedAgentHost;
}

export function normalizeAgentHost(agent: SupportedAgentHost): SupportedAgentHost {
  const descriptor = AGENT_HOST_DESCRIPTORS.find((entry) => entry.host === agent);
  if (!descriptor) throw new Error(`Unsupported agent host: ${agent}`);
  return descriptor.canonical;
}

export function resolveAgentSessionId(
  mailboxId: string,
  sessionId?: string,
  agent: SupportedAgentHost = 'claude-code',
): string | undefined {
  if (sessionId?.trim()) return sessionId;
  const host = normalizeAgentHost(agent);
  if (host === 'codex' || host === 'antigravity') return undefined;
  if (host === 'claude-code')
    return mailboxId.replace(/[^A-Za-z0-9.]/gu, (char) => `_x${char.codePointAt(0)!.toString(16)}_`);
  return mailboxId;
}

export function resolveAgentHostContinuation(
  agent: SupportedAgentHost,
  mailboxId: string,
  sessionId: string | undefined,
  stdout: string,
  mode: AgentConversationMode,
) {
  const host = normalizeAgentHost(agent);
  let resolved = resolveAgentSessionId(mailboxId, sessionId, agent);
  if (host === 'codex' && (mode === 'spawn' || (mode === 'wave' && !sessionId?.trim()))) {
    resolved = undefined;
    try {
      const receipt = JSON.parse(stdout) as { threadId?: unknown; sessionId?: unknown };
      const identity = [receipt.threadId, receipt.sessionId].find(
        (id) => typeof id === 'string' && id.trim(),
      );
      if (typeof identity === 'string') {
        resolved = identity;
      }
    } catch {
      // No receipt was emitted.
    }
  }
  if (host === 'antigravity' || !resolved)
    return { hostAgent: undefined, conversationMode: undefined, sessionId: undefined };
  const bridge =
    host === 'claude-app' ||
    host === 'chatgpt-app' ||
    host === 'chatgpt-dot' ||
    host === 'antigravity-app' ||
    host === 'grok-bot';
  return {
    hostAgent: agent === 'claude-app' ? 'claude-app' : agent,
    conversationMode: bridge ? ((mode === 'wave' ? 'wave' : 'fixed') as ActiveConversationMode) : 'fixed',
    sessionId: resolved,
  };
}

export function buildAgentHostInvocation(options: {
  agent: SupportedAgentHost;
  conversationMode: AgentConversationMode;
  mailboxId: string;
  promptText: string;
  command?: string;
  cwd?: string;
  agentProfile?: string;
  targetBot?: string;
  sessionId?: string;
}): { command: string; args: string[] } {
  const { agent, mailboxId, promptText, cwd, agentProfile, targetBot, sessionId } = options;
  const descriptor = AGENT_HOST_DESCRIPTORS.find((entry) => entry.host === agent);
  if (!descriptor) throw new Error(`Unsupported agent-pty host: ${agent}`);
  const command = options.command ?? descriptor.command;
  if (!ACTIVE_CONVERSATION_MODES.includes(options.conversationMode as ActiveConversationMode))
    throw new Error(`Invalid agent conversationMode: ${options.conversationMode}`);
  const mode = options.conversationMode as ActiveConversationMode;
  const session = resolveAgentSessionId(mailboxId, sessionId, agent);
  switch (agent) {
    case 'codex':
    case 'codex-cli':
      if (mode === 'fixed' && !session) throw new Error('Codex fixed mode requires an explicit sessionId');
      {
        const resumeCodex = mode === 'fixed' || (mode === 'wave' && Boolean(session));
        return {
          command,
          args: [
            ...(resumeCodex ? ['queue', '--thread', session!, '--resume'] : ['exec', '--worktree']),
            ...(agentProfile ? ['-p', agentProfile] : []),
            ...(cwd ? ['-C', cwd] : []),
            ...(resumeCodex ? ['--message'] : []),
            promptText,
          ],
        };
      }
    case 'claude':
    case 'claude-code':
      return {
        command,
        args: [
          '--bg',
          ...(mode === 'fixed' ? ['--resume', session!] : ['--name', session!]),
          ...(agentProfile ? ['--agent', agentProfile] : []),
          promptText,
        ],
      };
    case 'claude-app':
      if (!session) throw new Error('Claude app requires a session');
      return {
        command,
        args: [
          'send',
          '--session-mode',
          mode,
          '--session-key',
          session,
          ...(agentProfile ? ['--agent', agentProfile] : []),
          '--message',
          promptText,
        ],
      };
    case 'antigravity-app':
      if (!session) throw new Error('Antigravity app requires a session');
      return {
        command,
        args: [
          'send',
          '--session-mode',
          mode,
          '--session-key',
          session,
          ...(cwd ? ['--workspace', cwd] : []),
          '--message',
          promptText,
        ],
      };
    case 'antigravity':
    case 'antigravity-cli':
      if (mode === 'wave' || (agent === 'antigravity-cli' && mode !== 'spawn'))
        throw new Error(
          'Standalone Antigravity CLI only supports spawn mode; use orca or antigravity-app for wave/fixed sessions',
        );
      return { command, args: ['run', '--dangerously-skip-permissions', promptText] };
    case 'chatgpt-dot':
    case 'chatgpt-app': {
      if (!session) throw new Error('ChatGPT app requires a session');
      const dotTarget = targetBot ?? agentProfile;
      return {
        command,
        args: [
          'send',
          '--session-mode',
          mode,
          '--session-key',
          session,
          ...(dotTarget ? ['--agent', dotTarget] : []),
          '--message',
          promptText,
        ],
      };
    }
    case 'grok-bot':
      if (!targetBot?.trim()) throw new Error('grok-bot requires targetBot');
      if (!session) throw new Error('grok-bot requires a session');
      return {
        command,
        args: [
          'dispatch',
          '--bot',
          targetBot,
          '--session-mode',
          mode,
          '--session-key',
          session,
          '--message',
          promptText,
        ],
      };
    default:
      throw new Error(`Unsupported agent-pty host: ${agent}`);
  }
}
export function defaultAntigravityLaunchCommand(): string {
  return defaultAgentLaunchCommand('antigravity');
}

export function defaultAgentLaunchCommand(agent: 'antigravity' | 'claude' | 'codex' = 'antigravity'): string {
  const canonical = normalizeAgentHost(agent);
  const descriptor = AGENT_HOST_DESCRIPTORS.find((entry) => entry.host === canonical);
  if (!descriptor || !('launchCommand' in descriptor))
    throw new Error(`No interactive launcher for ${agent}`);
  return descriptor.launchCommand;
}

/**
 * Trust a workspace in Antigravity by adding it to trustedWorkspaces.
 * Missing settings files start from an empty object, then the file is replaced atomically.
 */
export function trustAntigravityWorkspace(workspacePath: string, settingsPath?: string): void {
  const file = settingsPath ?? join(homedir(), '.gemini/antigravity-cli/settings.json');
  let settings: Record<string, unknown> = {};
  if (existsSync(file)) {
    const parsed: unknown = JSON.parse(readFileSync(file, 'utf8'));
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      throw new Error(`Antigravity settings at ${file} must be a JSON object`);
    }
    settings = parsed as Record<string, unknown>;
  }
  const trusted = Array.isArray(settings.trustedWorkspaces)
    ? settings.trustedWorkspaces.filter((entry): entry is string => typeof entry === 'string')
    : [];
  const candidates = existsSync(workspacePath)
    ? [workspacePath, realpathSync(workspacePath)]
    : [workspacePath];
  for (const candidate of candidates) {
    if (!trusted.includes(candidate)) {
      trusted.push(candidate);
    }
  }
  settings.trustedWorkspaces = trusted;
  writeFileAtomically(file, `${JSON.stringify(settings, null, 2)}\n`);
}

export const AGENT_HOST_DESCRIPTORS = [
  {
    host: 'orca',
    canonical: 'orca',
    command: 'orca',
    surface: 'cli',
    modes: CONVERSATION_MODES,
    selectors: ['agent', 'workspacePath'],
    description: 'Orca managed worktrees and idle-gated conversations.',
  },
  {
    host: 'antigravity',
    canonical: 'antigravity',
    command: 'agy',
    launchCommand: 'agy --dangerously-skip-permissions',
    surface: 'cli',
    modes: CONVERSATION_MODES,
    selectors: ['workspacePath'],
    description: 'Antigravity in Orca managed worktrees.',
  },
  {
    host: 'claude-code',
    canonical: 'claude-code',
    command: 'claude',
    launchCommand: 'claude --dangerously-skip-permissions',
    pipeArgs: ['-p'],
    promptArgs: ['-p'],
    surface: 'cli',
    modes: CONVERSATION_MODES,
    selectors: ['agentProfile', 'sessionId'],
    description: 'Claude Code conversations.',
  },
  {
    host: 'codex',
    canonical: 'codex',
    command: 'codex',
    launchCommand: 'codex --dangerously-bypass-approvals-and-sandbox',
    pipeArgs: ['exec', '-'],
    promptArgs: ['exec'],
    surface: 'cli',
    modes: CONVERSATION_MODES,
    selectors: ['agentProfile', 'sessionId'],
    description: 'Codex execution.',
  },
  {
    host: 'chatgpt-dot',
    canonical: 'chatgpt-dot',
    command: 'dot',
    surface: 'cli',
    modes: CONVERSATION_MODES,
    selectors: ['agentProfile', 'targetBot', 'sessionId'],
    description: 'ChatGPT Dot bridge.',
  },
  {
    host: 'grok-bot',
    canonical: 'grok-bot',
    command: 'grok-bot',
    surface: 'cli',
    modes: CONVERSATION_MODES,
    selectors: ['targetBot', 'sessionId'],
    description: 'Grok bridge.',
  },
  {
    host: 'claude-app',
    canonical: 'claude-app',
    command: 'claude-app',
    surface: 'app',
    app: 'Claude',
    modes: CONVERSATION_MODES,
    selectors: ['agentProfile', 'sessionId'],
    description: 'Claude app bridge.',
  },
  {
    host: 'chatgpt-app',
    canonical: 'chatgpt-dot',
    command: 'dot',
    surface: 'app',
    app: 'ChatGPT',
    modes: CONVERSATION_MODES,
    selectors: ['agentProfile', 'targetBot', 'sessionId'],
    description: 'ChatGPT app bridge.',
  },
  {
    host: 'antigravity-app',
    canonical: 'antigravity-app',
    command: 'antigravity-app',
    surface: 'app',
    app: 'Antigravity',
    modes: CONVERSATION_MODES,
    selectors: ['workspacePath'],
    description: 'Antigravity app bridge.',
  },
  {
    host: 'codex-cli',
    canonical: 'codex',
    command: 'codex',
    surface: 'cli',
    modes: CONVERSATION_MODES,
    selectors: ['agentProfile', 'sessionId'],
    description: 'Codex CLI bridge.',
  },
  {
    host: 'antigravity-cli',
    canonical: 'antigravity',
    command: 'agy',
    surface: 'cli',
    modes: ['spawn', 'inbox-only'] as const,
    selectors: ['workspacePath'],
    description: 'Antigravity CLI bridge.',
  },
  {
    host: 'claude',
    canonical: 'claude-code',
    command: 'claude',
    surface: 'cli',
    modes: CONVERSATION_MODES,
    selectors: ['agentProfile', 'sessionId'],
    description: 'Claude Code alias.',
  },
] as const;

/** Legacy raw CLI arguments are a host convention; the generic pipe executor has no defaults. */
export function defaultAgentCommandArgs(
  command: string,
  input: 'pipe' | 'prompt' = 'pipe',
): readonly string[] {
  const descriptor = AGENT_HOST_DESCRIPTORS.find((entry) => entry.command === command);
  if (!descriptor || !('pipeArgs' in descriptor)) return [];
  return input === 'pipe' ? descriptor.pipeArgs : descriptor.promptArgs;
}
