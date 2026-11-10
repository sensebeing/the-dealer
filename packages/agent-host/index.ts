export type SupportedAgentHost = 'orca' | 'antigravity' | 'claude' | 'claude-code' | 'codex' | 'chatgpt-dot' | 'grok-bot';
export type AgentConversationMode = 'spawn' | 'fixed' | 'inbox-only';

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
  const session = sessionId || mailboxId;
  switch (agent) {
    case 'codex':
      return { command: options.command ?? 'codex', args: [
        ...(mode === 'fixed' ? ['queue', '--thread', session, '--resume'] : ['exec', '--worktree']),
        ...(agentProfile ? ['-p', agentProfile] : []),
        ...(cwd ? ['-C', cwd] : []),
        ...(mode === 'fixed' ? ['--message'] : []), promptText,
      ] };
    case 'claude':
    case 'claude-code':
      return { command: options.command ?? 'claude', args: [
        '--bg', ...(mode === 'fixed' ? ['--resume', session] : ['--name', mailboxId.replace(/[^A-Za-z0-9._-]/g, '-')]),
        ...(agentProfile ? ['--agent', agentProfile] : []), promptText,
      ] };
    case 'antigravity':
      return { command: options.command ?? 'agy', args: ['run', '--dangerously-skip-permissions', promptText] };
    case 'chatgpt-dot': {
      const dotTarget = targetBot ?? agentProfile;
      return { command: options.command ?? 'dot', args: ['send', '--session-mode', mode, '--session-key', session,
        ...(dotTarget ? ['--agent', dotTarget] : []), '--message', promptText] };
    }
    case 'grok-bot':
      if (!targetBot?.trim()) throw new Error('grok-bot requires targetBot');
      return { command: options.command ?? 'grok-bot', args: ['dispatch', '--bot', targetBot, '--session-mode', mode,
        '--session-key', session, '--message', promptText] };
    default:
      throw new Error(`Unsupported agent-pty host: ${agent}`);
  }
}

