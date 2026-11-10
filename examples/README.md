# Core plugin examples

Copy `project-scaffold/.dealer/` and `team.example.yaml` into a repository, replace the example member names, then run `dealer project link <repo>`. The four plugins use only Node built-ins and the runtime-injected `context.api`; no machine-local Dealer imports are needed. The team router and request formatter load immediately; both polling sources are disabled until configured.

- **Team routing:** `team-router.ts` expands `area:`, `module:`, `service:`, `review:` and `git:` selectors using the YAML registry. Auto routing uses registry ownership after ordinary capability matching; avoid advertising ownership module names as generic skills.
- **CAS claims:** configure `claimEndpoint`, `networkId` and `enabled: true`. The example endpoint returns `{ items: [{ id, version, area, summary }], cursor }`, owns atomic claim/version acquisition, and must replay the same claimed page until acknowledged by the cursor. Delivery uses `orca-spawn` and settled-only receipt checks. The receiver must enable durable `dedup-cas` policy and bind `example-project` to its own repository. This is a source adapter template, not a claim server implementation.
- **Coalesced waves:** configure `batchEndpoint`, `member`, `networkId` and `enabled: true`. The endpoint returns `{ items: [{ id, revision, streamKey, summary }], cursor }`. Batch stream keys such as `tools-alert` and `quality-review` select receiver workspaces. Durable queued receipts can advance the cursor; transient queue receipts cannot. Receiver `coalesceMs` delays mailbox wakeup and permits multiple items to join a wave.
- **Collaboration requests:** send `collab.request.v4` with `{ "requestFile": "requests/example.md" }`. Markdown frontmatter requires `schema: collaboration-request/v4`, `request_id`, and a nonempty `to` member array. The plugin parses frontmatter, shares the file as an attachment, and formats the outgoing message. Paths must remain within the project.

Use `user-patches/personal.patch.json` as a project user-patch template (`dealer project link <repo> --user-patch <file>`). Replace all workspace paths, terminal handles, session IDs and bot names with receiver-owned values. It demonstrates independent streams for one topic and several optional agent hosts; install/configure only the hosts you use. `assignee` is validated binding metadata, not an identity or routing override.

`cursorPath` loads persisted JSON on construction/start and supplies atomic saving unless `saveCursor` is explicitly provided. Missing files use the initial cursor; corrupted files fail loudly. `advanceItemCursor` remains the custom per-item owner. An empty receipt list never confirms handoff. The claim example exposes localhost `POST /wake` on port 18965; set `allowedOrigins` if browser-origin restrictions are needed. Calling stop closes the HTTP listener and waits for in-flight polling.


## Host profiles and one-command presets

| Environment | Built-in profile | First-class executor |
| --- | --- | --- |
| Orca | `orca-dev` | `orca-spawn`, `orca-wave` |
| Codex | `codex-dev` | `codex` |
| Claude Code | `claude-dev` | `claude-code` |
| Antigravity | `antigravity-dev` | `antigravity` (standalone `agy`) |

All four profiles use `approval_required` and `dedup-cas`. The named Codex,
Claude Code and Antigravity executors default to `spawn` and support `fixed`;
`agent-pty` remains available for existing configurations and other bridges.

```sh
dealer node up --profile codex-dev --json
dealer setup apply-preset --host codex --mode fixed --topic collab.request.v4 --session-id <thread> --set-default --json
```

`apply-preset` persists the topic binding, enables required executors and merges
executor settings. It works offline or through the running daemon, taking effect
without a restart. Omit `--set-default` to preserve the node's current default.
`--mode inbox-only` selects durable manual intake. Supported selectors include
`--project`, `--workspace-path`, `--agent`, `--agent-profile`, `--target-bot`,
and `--session-id`; `--notify-lark` enables mailbox eligibility, not Lark credentials.

For compatibility, `apply-preset` applies the same bindings as `binding-preset`:
Orca and Antigravity use managed `orca-spawn`/`orca-wave`; Codex and Claude Code
use `agent-pty`. `--set-default` selects the named host executor for unbound
messages (or `inbox` for inbox-only). For direct named-executor topic bindings,
use the ready-to-use [host patches](user-patches/): `orca.patch.json`,
`codex.patch.json`, `claude-code.patch.json`, and `antigravity.patch.json`.
Start a node with `dealer node up --patch examples/user-patches/codex.patch.json --json`,
or link its topic bindings with `dealer project link <repo> --user-patch <file>`.
Project user patches do not change node-wide profiles; enable the corresponding
profile at node startup. Add receiver-owned workspace paths and real session IDs
when selecting fixed conversations; verify the installed CLI's argv contract.
