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
If Dealer is missing, use Node >=22.6 and `npm link` in a trusted Dealer checkout.
Never replace an existing identity or copy another machine's private key.

## 2. Choose network topology

Ask whether the user wants **same-machine local** (default), **join a colleague's
network**, or **create a new network**. Reuse prior answers.

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
dealer setup invite-prompt --network <id> --member <self> --name <other-node> --seed <reachable-host:port> --ttl 1h --json
```

Give `crossMachinePrompt` to the other machine's Agent. See
[onboarding-prompts](references/onboarding-prompts.md). Verify peer discovery
and a reply in both directions. A `dispatched` receipt proves executor handoff,
not task completion. Offline generation can create an invite for an unmounted
network; mount that same invite locally before expecting connectivity.

## 5. Connect a teammate and choose the conversation

Keep incoming work in the durable inbox. Ask for the Agent App and one mode:
**fixed** (recommended for ongoing collaboration), **spawn** (independent tasks),
or **inbox-only** (manual processing). Ask only for the selected host's required
selectors: Orca inner agent/workspace; Codex profile/thread; Claude role/session;
Dot agent; Grok targetBot. See [agent-app-matrix](references/agent-app-matrix.md).

Generate `binding-preset`, show its topic, executor and receiver workspace, then
apply through the same validated receiver configuration lane. Grok execution
requires a nonempty targetBot. Optional `notifyLark` controls mailbox eligibility;
configure `larkNotify` credentials/target separately, never infer them.
Generate `invite-prompt --peer-member <colleague>` and share `teammatePrompt`.
Keep approval-required admission until the intended peer is verified. Check
mailbox readback and an actual host receipt before reporting onboarding complete.
