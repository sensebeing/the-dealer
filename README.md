# The Dealer

**Connect your agents — across workspaces, across machines, across teammates.**

The Dealer turns independent agent conversations into a collaboration mesh with routing, permissions, and a durable inbox. Start on one machine with no network setup, then connect your other machines and teammates through encrypted P2P networks using the same delivery pipeline.

```text
1  Same-Machine Mesh
   Orca | Antigravity | Claude Code | Codex | ChatGPT Dot | Grok Bot
                              |
2  Cross-Machine Personal Mesh
                 MacBook <-> Mac mini <-> Devbox
                              |
3  Cross-Teammate P2P Collaboration
   dealer-net:// -> Gate -> Durable Inbox -> spawn new conversation
                                         -> fixed dedicated conversation
```

English | [简体中文](README.zh-CN.md)

### Host profiles and one-command presets

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
use the ready-to-use [host patches](examples/user-patches/): `orca.patch.json`,
`codex.patch.json`, `claude-code.patch.json`, and `antigravity.patch.json`.
Start a node with `dealer node up --patch examples/user-patches/codex.patch.json --json`,
or link its topic bindings with `dealer project link <repo> --user-patch <file>`.
Project user patches do not change node-wide profiles; enable the corresponding
profile at node startup. Add receiver-owned workspace paths and real session IDs
when selecting fixed conversations; verify the installed CLI's argv contract.

## Get Started in 30 Seconds — Copy & Paste to Your Agent

### Prompt 1: Start or Join on Your Machine — Paste into Claude Code / Codex / Antigravity / Orca

```text
Set up The Dealer for me, preserving my existing configuration and ~/.dealer/identity.json.

1. Check `command -v dealer` and `node --version`. Require Node >=22.6.
   If dealer is absent, clone https://github.com/sensebeing/the-dealer.git into
   ~/the-dealer. If the checkout already exists, inspect local changes before
   updating it with `git -C ~/the-dealer pull --ff-only`. Then run:
   cd ~/the-dealer && npm install && npm link
   If dealer is already installed, ensure this trusted checkout is available
   for skills without replacing the existing installation.
2. Create ~/.agents/skills/ and ~/.claude/skills/. Symlink these three directories
   into both skill directories, and ~/.gemini/config/skills/ if it exists:
   ~/the-dealer/skills/the-dealer
   ~/the-dealer/skills/the-dealer-setup
   ~/the-dealer/skills/the-dealer-extend
   Preserve existing files; resolve conflicting links before replacing them.
   Read ~/the-dealer/skills/the-dealer-setup/SKILL.md.
3. Run `dealer setup probe --json` to inspect local Agent Apps and node state.
   Never overwrite ~/.dealer/identity.json or copy another machine's private key.
4. Ask whether I want (1) same-machine multi-agent dispatch, (2) to connect my
   own machines, or (3) to join/create a teammate network. Follow the setup skill.
5. For teammates, ask my member handle, Agent App (orca, antigravity, claude-code,
   codex, chatgpt-dot, grok-bot), and conversation mode (spawn: new conversation per
   request; fixed: dedicated conversation; inbox-only: manual processing).
   Generate `dealer setup binding-preset --host <app> --mode <mode> --json`;
   add --target-bot <bot> for Grok execution. Review and apply its binding and
   executorConfig through the setup skill, using receiver-owned project roots.
6. Verify actual delivery, inbox readback and a reply. Keep remote admission
   approval_required until the intended peer has been verified.
```

### Prompt 2: Invite a Teammate or Your Second Machine — Generate & Send to Colleague

On the creator's machine, mount your network, then generate a fresh invitation:

```sh
dealer setup invite-prompt --network <network-id>
# Add --json for crossMachinePrompt and teammatePrompt as separate fields.
# Optional: --peer-member <handle> --name <other-node> --seed <reachable-host:port>
```

Send `crossMachinePrompt` to your own second machine or `teammatePrompt` to a colleague. Both contain installation, skill setup, network mounting and verification steps. A live node's advertised TCP endpoint is embedded automatically when no seeds are already configured; an offline node with a configured port uses its primary LAN IPv4.

Complete template (replace `<invite-uri>` with the generated `dealer-net://...` URI and the other placeholders before executing commands):

```text
Connect this machine to my Dealer network. Preserve existing configuration and identity.

Check `command -v dealer` and Node >=22.6. If missing, clone
https://github.com/sensebeing/the-dealer.git to ~/the-dealer (or inspect and update
that checkout with `git -C ~/the-dealer pull --ff-only`), then run:
cd ~/the-dealer && npm install && npm link
If already installed, ensure the trusted ~/the-dealer checkout exists for skills.
Create ~/.agents/skills/ and ~/.claude/skills/, then symlink each of
~/the-dealer/skills/the-dealer, ~/the-dealer/skills/the-dealer-setup, and
~/the-dealer/skills/the-dealer-extend into both, and ~/.gemini/config/skills/ if
present. Preserve unrelated files and resolve link conflicts first.
Read ~/the-dealer/skills/the-dealer-setup/SKILL.md and run `dealer setup probe --json`.
Never overwrite ~/.dealer/identity.json or copy another machine's private key.

Ask whether this is my second machine or a teammate's machine. For my own machine,
reuse my member handle; for a teammate, ask their handle (do not use "teammate"
as a literal identity). Ask for the new node name, then run:
dealer node up --member '<member-handle>' --name '<node-name>' --json
dealer network mount '<invite-uri>' --json

Ask for the receiving Agent App: orca, antigravity, claude-code, codex, chatgpt-dot,
or grok-bot; and mode: spawn, fixed, or inbox-only. Generate:
dealer setup binding-preset --host <app> --mode <mode> --json
For Grok execution add --target-bot <bot>. Review and apply the returned preset
through the setup skill; bind project roots locally and verify host CLI support.
Keep approval_required admission until the intended peer is verified.

dealer peers list --network '<network-id>' --json
dealer send --network '<network-id>' --to 'member:<inviter-handle>' --topic setup.ping --body 'Please reply to verify connectivity' --json
Verify replies in both directions and durable inbox/host readback. A dispatched
receipt alone does not prove task completion. If member handles repeat across
machines, use a specific member@node selector.
```

Invites contain a shared secret: send them privately, never commit them. Generation is read-only; if the creator has not mounted the network, mount the same generated invite there too. TCP seeds work across routed subnets when the advertised address and port are reachable; they do not bypass firewalls or provide automatic NAT traversal. Use `--seed` for a reachable VPN/routed endpoint when the detected LAN address is unsuitable.

## Three Progressive Superpowers

### 1. Same-Machine Multi-Agent Dispatch

One developer, several agents, isolated workspaces. Run `dealer node up` and send to your own `member:<handle>` or local skill: no invite, shared secret, or network mount is required. Local delivery still uses the gate, dispatcher, deduplication and executor pipeline.

### 2. Cross-Machine Personal Mesh

Keep the same member handle across your MacBook, Mac mini and devbox while each machine retains its own signing identity. Mount the same invite to discover peers through local registry, UDP multicast or explicit TCP seeds, then address a particular machine with `member@node`.

### 3. Cross-Teammate Encrypted P2P & Durable Inbox

Share a network invitation and let each receiver choose admission and execution. Durable mail, delivery leases, directional threads and restart-safe wakeups keep requests available; `ask`, `reply` and `settle` separate conversation progress from executor handoff. Orca wakeups wait for authoritative idle state before injecting a pointer.

## Supported Agent Apps & Conversation Modes

| Agent App (`--host`) | Integration | Receiver configuration |
| --- | --- | --- |
| `orca` | Managed worktrees / idle-gated conversations | Choose inner agent with `--agent codex`, `claude`, or `antigravity`; receiver workspace/project |
| `antigravity` | Antigravity in Orca-managed worktrees | Receiver workspace; standalone `agy` also available via agent-pty |
| `claude-code` | Claude Code CLI | Optional profile and fixed session ID |
| `codex` | Codex CLI | Optional profile and fixed thread/session ID |
| `chatgpt-dot` | OpenAI ChatGPT Personal Agent `dot` bridge | Agent/bot selector and session ID |
| `grok-bot` | Grok Bot bridge | `--target-bot <bot>` required for execution |

- **`spawn`**: a new conversation per request; Orca creates an isolated worktree.
- **`fixed`**: a dedicated conversation for ongoing work; Orca uses an idle-gated workspace, other hosts use their configured session contract.
- **`inbox-only`**: retain durable mail for manual processing without launching an agent.

All six hosts have presets for all three modes. `binding-preset` prints a proposal; it does not apply configuration or enable execution. Probe detects installations, not CLI compatibility: verify the installed host or bridge supports the generated argv contract. See the [host matrix](skills/the-dealer-setup/references/agent-app-matrix.md).

## Extend in 3 Tiers with the-dealer-extend

1. **Personal Patch** — customize your node's profile, routing, gates and executor settings without changing a project.
2. **Project Plugins in `.dealer/project.json`** — declare project identity, topics and plugins; link the repository to receiver-owned paths.
3. **Custom Executors/Routers/Sources** — implement typed plugins for new agent hosts, routing decisions, webhooks or watermark-based sources. The kernel owns lifecycle and dependency injection.

Start with [the-dealer-extend](skills/the-dealer-extend/SKILL.md), then the [plugin guide](docs/plugin-guide.md). No external npm runtime dependencies; Node.js >=22.6 runs the TypeScript sources directly.

Manage personal plugins through the daemon; removal hot-unloads hooks and persists across restarts, leaving the module file on disk.

```sh
dealer plugin add --name personal-hook --module plugins/personal-hook.ts --config ./personal.json --json
dealer plugin list --json
dealer plugin remove personal-hook --json
```

## CLI Cheat Sheet

```sh
dealer setup probe --json
dealer node up --member alice --name laptop --json
dealer node status --json
dealer setup binding-preset --host codex --mode fixed --session-id <thread> --json
dealer network mount <network-id> --passphrase '<shared-secret>' --gate approval_required --json
dealer setup invite-prompt --network <network-id>
dealer peers list --network <network-id> --json
dealer send --network local --to member:alice --topic collab.request.v4 --body 'Review this change' --json
dealer inbox check --mailbox <mailbox-id> --json
dealer inbox ack <dlv-id> --mailbox <mailbox-id> --json
dealer thread show <message-id> --json
npm test
```

For reply/settlement workflows, see the [daily operations skill](skills/the-dealer/SKILL.md).

## Architecture & Security

```text
Sources / CLI -> Emitter -> Router -> Transport -> Gate -> Dispatcher -> Executor
                         Node identity & directory       Durable mailbox
                  Kernel: lifecycle, injection, typed events
```

Ed25519 signs identities and messages; HKDF-SHA256 derives network-specific keys and AES-256-GCM protects frames. The default transport uses TCP, encrypted discovery beacons and streaming BLAKE3 file verification; an optional Rust Iroh engine lives under `native/`. Admission is receiver-owned (`open`, `approval_required`, `dnd`, `closed`), routing decisions stay within the current network's candidate set, and sender paths cannot choose receiver project roots. Unknown external execution outcomes are never treated as safe automatic replays.

[Architecture](docs/architecture.md) · [Wire protocol](docs/wire-protocol.md) · [Setup skill](skills/the-dealer-setup/SKILL.md) · [MIT License](LICENSE)
