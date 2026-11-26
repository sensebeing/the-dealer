# Agent App matrix

Executor selection and conversation mode are orthogonal. The first level picks
the executor, and the second level picks how it owns a conversation:

| First-level executor | `spawn` | `wave` | `fixed` | `inbox-only` | Receiver selectors |
| --- | --- | --- | --- | --- | --- |
| `inbox` | Durable mailbox | Durable mailbox | Durable mailbox | Durable mailbox | mailboxId |
| `claude-app` | `claude-app send --session-mode spawn` | `claude-app send --session-mode wave` | `claude-app send --session-mode fixed` | Durable mailbox | agentProfile, sessionId |
| `chatgpt-app` | `dot send --session-mode spawn` | `dot send --session-mode wave` | `dot send --session-mode fixed` | Durable mailbox | agentProfile, targetBot, sessionId |
| `antigravity-app` | `antigravity-app send --session-mode spawn` | `antigravity-app send --session-mode wave` | `antigravity-app send --session-mode fixed` | Durable mailbox | workspacePath |
| `claude-code` | `claude --bg --name <stream-session>` | `claude --bg --name <stream-session>` then `--resume` | `claude --bg --resume session` | Durable mailbox | agentProfile, sessionId |
| `codex` / `codex-cli` | `codex app-server --stdio` (`thread/start` with `threadSource: "user"` + `thread/name/set` + `turn/start`, desktop-visible thread) | `codex app-server --stdio` (`thread/start` + `thread/name/set` then `thread/resume`, desktop-visible thread) | `codex app-server --stdio` (`thread/resume` for explicit `sessionId`, desktop-visible thread) | Durable mailbox | sessionId |
| `antigravity-cli` | `agy run --dangerously-skip-permissions` | Unsupported (use orca or antigravity-app) | Unsupported (use orca or antigravity-app) | Durable mailbox | workspacePath |
| `grok-bot` | `grok-bot dispatch --session-mode spawn` | `grok-bot dispatch --session-mode wave` | `grok-bot dispatch --session-mode fixed` | Durable mailbox | targetBot, sessionId |
| `orca` | Managed child worktree | `orca-wave` with `rotateAfterWaves: 1` | Idle-gated workspace | Durable mailbox | agent: antigravity / claude / codex; workspacePath |

The built-in `orca-spawn` and `orca-wave` executors are the managed execution
implementations used by `orca` bindings. Other legacy bridge hosts remain
available in the catalog, and all hosts support `inbox-only` for durable manual
intake. Presets return configuration, they do not install CLIs or change gates.
`claude` is an alias for `claude-code`. Orca's default inner agent is
Antigravity.

Dot and Grok require installed bridges implementing these command contracts.
Verify the installed Codex CLI supports `codex app-server --stdio` (JSON-RPC 2.0
`initialize`/`thread/start`/`thread/resume`/`turn/start`, no `--worktree` or `--profile`;
configure defaults in `~/.codex/config.toml`) and the installed Claude CLI supports
`--bg`/`--resume`; presence detection is not feature detection. Never claim a mocked runner
proves a real App integration. Nonzero host exits or non-completed turns remain execution
failures and must not be silently replayed when external side effects are unknown.

Agent-pty persists messages before invoking the host. Spawn mailboxes use
`task:<messageId>`; fixed presets use `stream:<topic>` and optionally an explicit
host sessionId. The prompt carries an inbox pointer so the host can use Dealer's
leases, replies and settlement protocol. For standalone Antigravity, agent-pty
supports `agy run`; it does not guarantee a resumed fixed TUI, so use the managed
Orca preset for fixed conversations. Non-Orca `agent-host` streams (such as
`codex` or `claude-code` in `wave` mode) support `rotateAfterWaves` (acknowledged
wave count) and `rotateAfterMs` (session age in ms) to rotate to a fresh session
between acknowledged waves; rotation never triggers while a delivery lease is
outstanding. Managed Orca/antigravity fixed presets default to `rotateAfterWaves: 1`: each wave
opens its own agent session tab in the workspace once the retiring slot's own
agent and child worktrees are done, and the retired tab is closed after the
replacement launch commits (an unsent draft there does not block rotation; no
`/new` reset).
