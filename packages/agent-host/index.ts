export type SupportedAgentHost = 'orca' | 'antigravity' | 'antigravity-cli' | 'antigravity-app' | 'claude' | 'claude-code' | 'claude-app' | 'codex' | 'codex-cli' | 'chatgpt-app' | 'chatgpt-dot' | 'grok-bot';
export const CONVERSATION_MODES = ['spawn', 'wave', 'fixed', 'inbox-only'] as const;
export type AgentConversationMode = (typeof CONVERSATION_MODES)[number];
export const ACTIVE_CONVERSATION_MODES = ['spawn', 'wave', 'fixed'] as const;
export type ActiveConversationMode = (typeof ACTIVE_CONVERSATION_MODES)[number];
export function normalizeConversationModeInput(
  input?: { mode?: unknown; conversationMode?: unknown },
): AgentConversationMode | undefined {
  if (!input) return undefined;
  if (input.mode !== undefined && input.conversationMode !== undefined && input.mode !== input.conversationMode) {
    throw new Error('Conflicting mode and conversationMode');
  }
  const raw = input.mode ?? input.conversationMode;
  if (raw === undefined) return undefined;
  if (typeof raw !== 'string' || !CONVERSATION_MODES.includes(raw as AgentConversationMode)) {
    throw new Error(`Invalid conversation mode: ${String(raw)}`);
  }
  return raw as AgentConversationMode;
}
export function resolveHostSessionMode(mode: ActiveConversationMode): 'spawn' | 'fixed' {
  return mode === 'fixed' ? 'fixed' : 'spawn';
}

export function normalizeAgentHost(agent: SupportedAgentHost): SupportedAgentHost {
  if (agent === 'codex-cli') return 'codex';
  if (agent === 'antigravity-cli') return 'antigravity';
  if (agent === 'claude') return 'claude-code';
  if (agent === 'chatgpt-app') return 'chatgpt-dot';
  return agent;
}

export function resolveAgentSessionId(mailboxId: string, sessionId?: string, agent: SupportedAgentHost = 'claude-code'): string | undefined {
  if (sessionId?.trim()) return sessionId;
  const host = normalizeAgentHost(agent);
  if (host === 'codex' || host === 'antigravity') return undefined;
  if (host === 'claude-code') return mailboxId.replace(/[^A-Za-z0-9.]/gu, char => `_x${char.codePointAt(0)!.toString(16)}_`);
  return mailboxId;
}

export function resolveAgentHostContinuation(agent: SupportedAgentHost, mailboxId: string, sessionId: string | undefined, stdout: string, mode: AgentConversationMode) {
  const host = normalizeAgentHost(agent);
  let resolved = resolveAgentSessionId(mailboxId, sessionId, agent);
  if (host === 'codex' && (mode === 'spawn' || (mode === 'wave' && !sessionId?.trim()))) {
    resolved = undefined;
    try {
      const receipt = JSON.parse(stdout) as { threadId?: unknown; sessionId?: unknown };
      const identity = [receipt.threadId, receipt.sessionId]
        .find(id => typeof id === 'string' && id.trim());
      if (typeof identity === 'string') {
        resolved = identity;
      }
    } catch {
      // No receipt was emitted.
    }
  }
  if (host === 'antigravity' || !resolved) return { hostAgent: undefined, conversationMode: undefined, sessionId: undefined };
  const bridge = host === 'claude-app' || host === 'chatgpt-app' || host === 'chatgpt-dot' || host === 'antigravity-app' || host === 'grok-bot';
  return { hostAgent: agent === 'claude-app' ? 'claude-app' : agent, conversationMode: bridge ? (mode === 'wave' ? 'wave' : 'fixed') as ActiveConversationMode : 'fixed', sessionId: resolved };
}

export function buildAgentHostInvocation(options: {
  agent: SupportedAgentHost; conversationMode: AgentConversationMode; mailboxId: string; promptText: string; command?: string; cwd?: string; agentProfile?: string; targetBot?: string; sessionId?: string;
}): { command: string; args: string[] } {
  const { agent, mailboxId, promptText, cwd, agentProfile, targetBot, sessionId } = options;
  if (!ACTIVE_CONVERSATION_MODES.includes(options.conversationMode as ActiveConversationMode)) throw new Error(`Invalid agent conversationMode: ${options.conversationMode}`);
  const mode = options.conversationMode as ActiveConversationMode;
  const session = resolveAgentSessionId(mailboxId, sessionId, agent);
  switch (agent) {
    case 'codex': case 'codex-cli':
      if (mode === 'fixed' && !session) throw new Error('Codex fixed mode requires an explicit sessionId');
      { const resumeCodex = mode === 'fixed' || (mode === 'wave' && Boolean(session));
        return { command: options.command ?? 'codex', args: [...(resumeCodex ? ['queue', '--thread', session!, '--resume'] : ['exec', '--worktree']), ...(agentProfile ? ['-p', agentProfile] : []), ...(cwd ? ['-C', cwd] : []), ...(resumeCodex ? ['--message'] : []), promptText] }; }
    case 'claude': case 'claude-code':
      return { command: options.command ?? 'claude', args: ['--bg', ...(mode === 'fixed' ? ['--resume', session!] : ['--name', session!]), ...(agentProfile ? ['--agent', agentProfile] : []), promptText] };
    case 'claude-app':
      if (!session) throw new Error('Claude app requires a session');
      return { command: options.command ?? 'claude-app', args: ['send', '--session-mode', mode, '--session-key', session, ...(agentProfile ? ['--agent', agentProfile] : []), '--message', promptText] };
    case 'antigravity-app':
      if (!session) throw new Error('Antigravity app requires a session');
      return { command: options.command ?? 'antigravity-app', args: ['send', '--session-mode', mode, '--session-key', session, ...(cwd ? ['--workspace', cwd] : []), '--message', promptText] };
    case 'antigravity': case 'antigravity-cli':
      if (mode === 'wave' || (agent === 'antigravity-cli' && mode !== 'spawn')) throw new Error('Standalone Antigravity CLI only supports spawn mode; use orca or antigravity-app for wave/fixed sessions');
      return { command: options.command ?? 'agy', args: ['run', '--dangerously-skip-permissions', promptText] };
    case 'chatgpt-dot': case 'chatgpt-app': {
      if (!session) throw new Error('ChatGPT app requires a session');
      const dotTarget = targetBot ?? agentProfile;
      return { command: options.command ?? 'dot', args: ['send', '--session-mode', mode, '--session-key', session, ...(dotTarget ? ['--agent', dotTarget] : []), '--message', promptText] };
    }
    case 'grok-bot':
      if (!targetBot?.trim()) throw new Error('grok-bot requires targetBot');
      if (!session) throw new Error('grok-bot requires a session');
      return { command: options.command ?? 'grok-bot', args: ['dispatch', '--bot', targetBot, '--session-mode', mode, '--session-key', session, '--message', promptText] };
    default: throw new Error(`Unsupported agent-pty host: ${agent}`);
  }
}
