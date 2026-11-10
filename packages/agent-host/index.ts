export type SupportedAgentHost = 'orca' | 'antigravity' | 'claude' | 'claude-code' | 'codex' | 'chatgpt-dot' | 'grok-bot';
export type AgentConversationMode = 'spawn' | 'fixed' | 'inbox-only';

export function resolveAgentSessionId(mailboxId: string, sessionId?: string, agent: SupportedAgentHost = 'claude-code'): string | undefined {
  if (sessionId?.trim()) return sessionId;
  if (agent === 'codex' || agent === 'antigravity') return undefined;
  if (agent === 'claude' || agent === 'claude-code') {
    return mailboxId.replace(/[^A-Za-z0-9.]/gu, char => `_x${char.codePointAt(0)!.toString(16)}_`);
  }
  return mailboxId;
}

/** Only a caller-selected identity or a supported launch receipt establishes continuation. */
export function resolveAgentHostContinuation(agent: SupportedAgentHost, mailboxId: string, sessionId: string | undefined, stdout: string, mode: 'spawn' | 'fixed') {
  let resolved = resolveAgentSessionId(mailboxId, sessionId, agent);
  if (agent === 'codex' && mode === 'spawn') {
    resolved = undefined;
    let receipt: unknown;
    try { receipt = JSON.parse(stdout); } catch { /* Plain output carries no session receipt. */ }
    if (receipt && typeof receipt === 'object') {
      const value = receipt as { threadId?: unknown; sessionId?: unknown };
      const identity = [value.threadId, value.sessionId].find(id => typeof id === 'string' && id.trim());
      if (typeof identity === 'string') resolved = identity;
    }
  }
  if (agent === 'antigravity' || !resolved) {
    return { hostAgent: undefined, conversationMode: undefined, sessionId: undefined };
  }
  return { hostAgent: agent as Exclude<SupportedAgentHost, 'orca'>, conversationMode: 'fixed' as const, sessionId: resolved };
}

export function buildAgentHostInvocation(options: {
  agent: SupportedAgentHost;
  conversationMode: 'spawn' | 'fixed';
  mailboxId: string;
  promptText: string;
  command?: string;
  cwd?: string;
  agentProfile?: string;
  targetBot?: string;
  sessionId?: string;
}): { command: string; args: string[] } {
  const { agent, conversationMode: mode, mailboxId, promptText, cwd, agentProfile, targetBot, sessionId } = options;
  const session = resolveAgentSessionId(mailboxId, sessionId, agent);
  switch (agent) {
    case 'codex':
      if (mode === 'fixed' && !session) throw new Error('Codex fixed mode requires an explicit sessionId');
      return { command: options.command ?? 'codex', args: [
        ...(mode === 'fixed' ? ['queue', '--thread', session!, '--resume'] : ['exec', '--worktree']),
        ...(agentProfile ? ['-p', agentProfile] : []),
        ...(cwd ? ['-C', cwd] : []),
        ...(mode === 'fixed' ? ['--message'] : []), promptText,
      ] };
    case 'claude':
    case 'claude-code':
      return { command: options.command ?? 'claude', args: [
        '--bg', ...(mode === 'fixed' ? ['--resume', session!] : ['--name', session!]),
        ...(agentProfile ? ['--agent', agentProfile] : []), promptText,
      ] };
    case 'antigravity':
      return { command: options.command ?? 'agy', args: ['run', '--dangerously-skip-permissions', promptText] };
    case 'chatgpt-dot': {
      const dotTarget = targetBot ?? agentProfile;
      return { command: options.command ?? 'dot', args: ['send', '--session-mode', mode, '--session-key', session!,
        ...(dotTarget ? ['--agent', dotTarget] : []), '--message', promptText] };
    }
    case 'grok-bot':
      if (!targetBot?.trim()) throw new Error('grok-bot requires targetBot');
      return { command: options.command ?? 'grok-bot', args: ['dispatch', '--bot', targetBot, '--session-mode', mode,
        '--session-key', session!, '--message', promptText] };
    default:
      throw new Error(`Unsupported agent-pty host: ${agent}`);
  }
}

