---
name: the-dealer-setup
description: Set up Dealer for a project, connect local agents or remote teammates, and choose an Agent App with new or fixed conversations.
---

# Dealer setup

Use this five-step guide for first-time onboarding. For daily operations use
[the-dealer](../the-dealer/SKILL.md); for custom automation use
[the-dealer-extend](../the-dealer-extend/SKILL.md).

## 1. Probe before changing anything

Run `dealer setup probe --root <repository> --json`. Inspect installed CLIs,
macOS Apps, redacted node configuration and `.dealer/project.json`. Probe is
read-only. A socket's presence is not proof of a live daemon; run
`dealer node status --json` to verify it. An installed CLI is not proof it
supports this integration's flags; inspect its help before enabling delivery.
If Dealer is missing, check Node >=22.6 and install the CLI without a manual clone:

```sh
npm install -g git+https://github.com/sensebeing/the-dealer
```

Install the skills for all supported agents:

```sh
npx skills add sensebeing/the-dealer --global --agent '*'
```

Preserve existing installations and configuration. For a local checkout, clone
`https://github.com/sensebeing/the-dealer.git` into `~/the-dealer`, or inspect local
changes before updating it with `git -C ~/the-dealer pull --ff-only`. Link the CLI
with `npm --prefix ~/the-dealer link`. To install skills from that checkout,
create `~/.agents/skills/` and `~/.claude/skills/`; symlink each of
`~/the-dealer/skills/the-dealer`, `~/the-dealer/skills/the-dealer-setup`, and
`~/the-dealer/skills/the-dealer-extend` into both, and into
`~/.gemini/config/skills/` if present. Preserve unrelated files and resolve link
conflicts before changing them. Read the installed `the-dealer-setup/SKILL.md`
and run `dealer setup probe --root <repository> --json` before configuration.
Never replace an existing identity or copy another machine's private key.

## 2. Choose network topology

Ask whether the user wants **same-machine local** (default), **connect their own machines**, or **join/create
a teammate network**. Reuse prior answers.

- Local needs no network mount, invite or passphrase.
- Join: mount the quoted `dealer-net://v1/...` invite supplied by the user.
- Create: choose networkId and generate a strong secret; mount with
  `dealer network mount <id> --passphrase <secret> --gate approval_required --json`.

Keep shared secrets out of logs and source control. An invite contains the
passphrase; its expiry limits importing, not continued network access.

## 3. Enable same-machine collaboration first

Run `dealer node up --profile personal-dev --member <member> --name <node> --json`.
If the project has no manifest, run `dealer project init --root <repo> --id <id>
--network local --json`; then `dealer project link <repo> --json`.
The default local scope supports `send`, `ask`, `reply`, `settle` and durable
mailboxes without a remote network. Choose the receiver's host and bind its
project root; never accept sender-supplied filesystem paths as project identity.

Generate a preset with `dealer setup binding-preset --host orca --mode spawn
--topic task --project <id> --workspace-path <receiver-repo> --agent codex --json`.
This prints a proposal, not an applied configuration. Merge `binding` under
`topicBindings[topic]`, merge `executorConfig` into the receiver's config, and
ensure the executor is enabled. Use the project's existing patch/configuration
lane, validate it, and reload/restart the node through its normal lifecycle.
For Orca, spawn creates a child worktree; fixed uses an idle-gated workspace.

## 4. Connect another machine

After mounting the network on this machine, run:

```sh
dealer setup invite-prompt --network <id> --member <self> --name <other-node> --ttl 1h --json
```

With no explicit or configured seeds, invite generation uses the running daemon's
advertised TCP endpoint, or the primary LAN IPv4 plus a configured offline port.
Loopback endpoints are not auto-shared. Use `--seed <reachable-host:port>` when a
VPN/routed address is needed; TCP seeds require reachability and do not bypass
NAT or firewalls. UDP multicast is not required between seed-connected peers.

Give `crossMachinePrompt` to the other machine's Agent. See
[onboarding-prompts](references/onboarding-prompts.md). Verify peer discovery
and a reply in both directions. A `dispatched` receipt proves executor handoff,
not task completion. Offline generation can create an invite for an unmounted
network; mount that same invite locally before expecting connectivity.

## 5. Connect a teammate and choose the conversation

Keep incoming work in the durable inbox. Ask for the teammate’s member handle;
replace the generated `teammate` placeholder before starting a new node. Ask for
the Agent App (`orca`, `antigravity`, `claude-code`, `codex`, `chatgpt-dot`,
`grok-bot`) and one mode:
**fixed** (recommended for ongoing collaboration), **spawn** (independent tasks),
or **inbox-only** (manual processing). Ask only for the selected host's required
selectors: Orca inner agent/workspace; Codex profile/thread; Claude role/session;
Dot agent; Grok targetBot. See [agent-app-matrix](references/agent-app-matrix.md).

Generate `binding-preset`, show its topic, executor and receiver workspace, then
apply with `dealer setup apply-preset --host <host> --mode <mode>` and the same
selectors. Add `--set-default` only when the user wants the node default changed. Grok execution
requires a nonempty targetBot. Optional `notifyLark` controls mailbox eligibility;
configure `larkNotify` credentials/target separately, never infer them.
Generate `invite-prompt --peer-member <colleague>` and share `teammatePrompt`.
Keep approval-required admission until the intended peer is verified. Check
mailbox readback and an actual host receipt before reporting onboarding complete.


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
use the ready-to-use [host patches](../../examples/user-patches/): `orca.patch.json`,
`codex.patch.json`, `claude-code.patch.json`, and `antigravity.patch.json`.
Start a node with `dealer node up --patch examples/user-patches/codex.patch.json --json`,
or link its topic bindings with `dealer project link <repo> --user-patch <file>`.
Project user patches do not change node-wide profiles; enable the corresponding
profile at node startup. Add receiver-owned workspace paths and real session IDs
when selecting fixed conversations; verify the installed CLI's argv contract.
