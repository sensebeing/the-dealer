# Agent App matrix

Executor selection and conversation mode are orthogonal. The first level picks
the executor, and the second level picks how it owns a conversation:

| First-level executor | `spawn` | `wave` | `fixed` | `inbox-only` | Receiver selectors |
| --- | --- | --- | --- | --- | --- |
| `inbox` | Durable mailbox | Durable mailbox | Durable mailbox | Durable mailbox | mailboxId |
| `claude-app` | App bridge | App bridge | App bridge | Durable mailbox | agentProfile, sessionId |
| `chatgpt-app` | App bridge | App bridge | App bridge | Durable mailbox | agentProfile, targetBot, sessionId |
| `antigravity-app` | App bridge | App bridge | App bridge | Durable mailbox | workspacePath |
| `claude-code` | `claude --bg --name task-ID` | Background conversation | `claude --bg --resume session` | Durable mailbox | agentProfile, sessionId |
| `codex-cli` | CLI bridge | CLI bridge | CLI bridge | Durable mailbox | agentProfile, sessionId |
| `antigravity-cli` | CLI bridge | CLI bridge | CLI bridge | Durable mailbox | workspacePath |
| `orca` | Managed child worktree | Idle-gated workspace wave | Idle-gated workspace | Durable mailbox | agent: antigravity / claude / codex; workspacePath |

The built-in `orca-spawn` and `orca-wave` executors are the managed execution
implementations used by `orca` bindings. Other legacy bridge hosts remain
available in the catalog, and all hosts support `inbox-only` for durable manual
intake. Presets return configuration, they do not install CLIs or change gates.
`claude` is an alias for `claude-code`. Orca's default inner agent is
Antigravity.

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
Managed Orca/antigravity fixed presets default to `rotateAfterWaves: 1`: each wave
opens its own agent session tab in the workspace, and the previous tab stays
untouched (no `/new` reset).
