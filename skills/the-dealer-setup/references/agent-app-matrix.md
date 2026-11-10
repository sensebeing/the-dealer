# Agent App matrix

| Host | spawn | fixed | Receiver selectors |
| --- | --- | --- | --- |
| orca | orca-spawn child worktree + task mailbox | orca-wave idle-gated workspace | agent: antigravity / claude / codex; workspacePath |
| antigravity | Orca-managed agy worktree | Orca-managed agy workspace | workspacePath; requires Orca as well as agy |
| claude-code | claude --bg --name task-ID | claude --bg --resume session | agentProfile, sessionId |
| codex | codex exec --worktree | codex queue --thread session --resume | agentProfile, sessionId |
| chatgpt-dot | dot send --session-mode spawn | dot send --session-mode fixed | targetBot or agentProfile, sessionId |
| grok-bot | grok-bot dispatch --session-mode spawn | grok-bot dispatch --session-mode fixed | required targetBot, sessionId |

All hosts support inbox-only: durable inbox without external execution. Presets
return configuration, they do not install CLIs or change gates. `claude` is an
alias for `claude-code`. Orca's default inner agent is Antigravity.

Dot and Grok require installed bridges implementing these command contracts.
Verify the installed Codex/Claude CLI supports the specified queue/background
flags; presence detection is not feature detection. Never claim a mocked runner
proves a real App integration. Nonzero host exits remain execution failures and
must not be silently replayed when external side effects are unknown.

Agent-pty persists messages before invoking the host. Spawn mailboxes use
`task:<messageId>`; fixed presets use `stream:<topic>` and optionally an explicit
host sessionId. The prompt carries an inbox pointer so the host can use Dealer's
leases, replies and settlement protocol. For standalone Antigravity, agent-pty
supports `agy run`; it does not guarantee a resumed fixed TUI, so use the managed
Orca preset for fixed conversations. Only Orca wakeups currently use the persisted
idle-gated redrive service; passive agent-pty mail remains readable after failure.
