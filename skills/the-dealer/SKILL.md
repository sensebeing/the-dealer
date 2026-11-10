---
name: the-dealer
description: Operate a The Dealer collaboration node from Codex, Claude Code, or Antigravity: start with zero-config same-machine multi-agent dispatch, then join encrypted cross-machine and cross-team P2P networks to discover peers, send tasks and files, manage inbound approvals, and configure routes and sources.
---

# The Dealer

Use `dealer` with Node 22.23.3 or newer. From a checkout, use
`node --experimental-strip-types bin/dealer.js`. Set `DEALER_STATE_DIR` to
select an isolated node; the default is `~/.dealer`. Every command accepts
`--json` before or after its subcommand. Nonzero exit means failure; inspect
stderr's JSON error instead of treating failure as an empty result.

`the-dealer` is a **same-machine multi-agent collaboration and dispatch system
first**, and a cross-machine/cross-team P2P collaboration network only after a
network is mounted. Prefer Step 1 for local workspaces; mount a network only
when work must reach another machine or teammate.

## Setup and extension entry points

For first-time onboarding use [the-dealer-setup](../the-dealer-setup/SKILL.md).
For personal, project and team automation use
[the-dealer-extend](../the-dealer-extend/SKILL.md). This skill owns daily operation.

```sh
dealer setup probe --root . --json
dealer setup invite-prompt --network team --peer-member bob --ttl 1h --json
dealer setup binding-preset --host codex --mode fixed --topic collab.request.v4 --session-id review --json
```

Setup commands print read-only proposals both offline and with dealerd running.
Choose spawn for independent tasks, fixed for a dedicated conversation, or
inbox-only for manual handling. Apply generated bindings through validated
receiver configuration; host availability alone does not verify its CLI contract.

## Step 1 — same-machine collaboration (zero network config)

Start the local node and address it directly. No `network mount`, passphrase,
or peer discovery is involved, yet the message still goes through the same
gate, dispatcher, policies, and executors.

```sh
dealer node up --profile personal-dev --member alice --name laptop --skills agent-runtime --json
dealer send --network local --to member:alice --topic collab.request.v1 --body '{"summary":"Run the local quality sweep"}' --json
dealer send --network local --to skill:agent-runtime --topic triage --body '{"area":"agent-runtime"}' --json
```

`local` is only a stable scope name. Self-originated deliveries bypass
`approval_required` knocks but still respect `dnd` and `closed`. A linked
project's `defaultNetworkId` is the scope its source plugins emit on.

## Step 2 — cross-machine and cross-team P2P extension

Agree on a `networkId` and shared passphrase, then mount the same network on
every participating machine. Selectors, gates, policies, and executors keep
working unchanged inside the encrypted network.

```sh
dealer network mount sensebeing --passphrase "$TEAM_SECRET" --member alice --name laptop --json
dealer peers list --network sensebeing --json
dealer send --network sensebeing --to member:bob --topic collab.request.v1 --body '{"summary":"Review the release"}' --json
```

## Inspect and connect

```sh
dealer node status --json
dealer node up --profile personal-dev --member alice --name laptop --skills ios-crash,runtime --json
dealer network mount sensebeing --passphrase "$TEAM_SECRET" --json
dealer network list --json
dealer node card --network sensebeing --json
dealer peers list --network sensebeing --skill ios-crash --status idle --json
dealer peers resolve member:bob --network sensebeing --json
```

`node up` starts a detached `dealerd`; commands then use its private Unix
socket. `dealerd` can also run in the foreground under a process supervisor.
Stop it with SIGTERM/SIGINT. Configuration and identity persist in stateDir;
protect it because it contains network passphrases and private keys. A lock
prevents two processes from controlling one stateDir. Following a crash,
verify the PID in `runtime.lock/owner.json` has stopped before removing that
lock and restarting. Never remove a live node's lock.

Use `--seed host:port,host:port` for explicit bootstrap endpoints. Network
mounts can include `--gate approval_required` and a descriptive `--role`;
role is local metadata, not an authorization grant. Wait for peer discovery
before resolving a newly connected member. Current transport uses encrypted
TCP plus local discovery; this is not a claim of production QUIC/NAT traversal.

`dealer network invite sensebeing --ttl 1h --json` returns a
`dealer-net://v1/...` URI. Mount the quoted URI as the network argument. It
contains the secret: share it only with the intended team. Expiry is checked
when importing; it cannot revoke an already learned passphrase.

## Send an authorized request

```sh
dealer send --network sensebeing --to member:bob --topic collab.request.v1 --body '{"summary":"Review the runtime boundary"}' --attach ./spec.md --attach ./repro.log --json
dealer send --network sensebeing --auto-route --topic triage --body '{"category":"ios-crash"}' --json
dealer broadcast --network sensebeing --topic team.announcement --body 'Review window starts now' --json
```

Choose exactly one of `--to` and `--auto-route`. Selectors include
`member:bob`, `bob@laptop`, `skill:ios-crash`, and a full `ed25519:` NodeId.
`--urgency normal|high|critical` maps critical to the wire's urgent priority.
JSON object bodies also become the structured payload. Routing first uses
explicit selectors, then capabilities, then configured agent/API judges.
Configure a judge before relying on freeform auto-routing.

Attachments travel as BLAKE3 content-addressed blob tickets. The receiving
runtime fetches and verifies them into its private `attachments/` directory
before executing, exposing paths in `payload.localAttachments`. The sender
must remain available until fetching finishes.

A `pending_approval` receipt means delivered to the gate, `queued` means
waiting, and `dispatched` means handed to an executor—not agent completion.
Broadcast has no per-peer delivery acknowledgements. Do not claim execution
from a successful broadcast command alone.

## Inbound gate

```sh
dealer gate mode approval_required --network sensebeing --json
dealer gate list --network sensebeing --json
dealer gate approve ENVELOPE_OR_APPROVAL_ID --trust-sender --json
dealer gate reject ENVELOPE_OR_APPROVAL_ID --reason 'Outside current scope' --json
dealer gate mode dnd --json
dealer gate drain --json
```

Modes are `open`, `approval_required`, `dnd`, and `closed`. A network override
wins over the default gate mode. `drain` explicitly dispatches DND backlog;
it is not an inspection command. `--trust-sender` trusts that node and claimed
member within the same shared-secret network. Members are not independently
certified identities: only mount networks whose members you trust. Existing
GateService trust rules can bypass mode defaults, including closed/DND;
do not use a mode change as a way to revoke previously trusted senders.

Gate pending items/trust are in-memory. CAS policy watermarks
(`dedup-watermarks.json`), identity, mounts, configuration, source cursors,
the dispatch ledger, `mailbox.json`, wake targets, and thread routes persist
on disk. A persisted `dispatching` or `failed` entry must be reconciled with
the external executor before retrying.

## Sources, routes, profiles and plugins

```sh
dealer route add --type local-agent --command codex --json
dealer route add --type llm-api --endpoint https://example.org/v1/chat/completions --model MODEL --json
dealer source add --type webhook --port 8787 --token "$WEBHOOK_TOKEN" --json
dealer source add --type poll-watermark --url http://127.0.0.1:9000/events --interval-ms 10000 --json
dealer source add --type script --command /path/to/source --args '["--jsonl"]' --json
```

`--config file.json` supplies plugin-specific settings. Only configure trusted
commands and endpoints: routes and sources execute locally or send task
content to the selected model service. Install source definitions once; do
not repeatedly add them while checking status.

Read [profile-and-patch-spec.md](references/profile-and-patch-spec.md) to
select profiles, overlay node configuration, bind Orca waves, and set rotation.
Read [writing-custom-plugins.md](references/writing-custom-plugins.md) to
implement a Source, RouteJudge, GateRule, DispatchPolicy, or WorkExecutor.

## Project and personal plugins

The three tiers are the built-in profile, repository-owned project modules, and
personal configuration/modules. Only load trusted local code.

```sh
dealer project init --root ./project --id project --network team --skills triage --json
dealer project link ./project --json
dealer project list --json
dealer project status --project project --json
dealer project unlink project --json
dealer plugin add --name personal-hook --module plugins/personal-hook.ts --config ./settings.json --json
```

`init` creates `.dealer/project.json` and `.dealer/plugins/`, refusing overwrite.
Without flags it uses the current directory, its basename as project ID, and
that ID as default network. Put project TypeScript/ESM plugins in
`.dealer/plugins/*.ts` and list their root-relative paths in manifest `plugins`.
`link` defaults to the caller's current directory. It loads modules immediately
and persists the link for restart; it does not mount the default network.

Personal overrides default to `~/.dealer/projects/<id>.patch.json`, or use
`project link --user-patch ./custom.json`. A patch can supply `skills` and
`pluginConfig: { "plugin-name": { ...settings } }`; per-plugin settings shallowly
override manifest config. Skills are merged/deduplicated and remain after unlink.
Standalone user modules such as `~/.dealer/plugins/personal-hook.ts` are installed
by `plugin add`, with module paths relative to stateDir (`DEALER_STATE_DIR` can
replace `~/.dealer`). `--config` is a JSON object file relative to the caller.

`status` returns linked manifests, loaded plugin names, and personal plugins.
Unlink cleans up only the project's registrations; it retains project files,
personal patches and personal plugins. Restart loads projects then user plugins.
Relink applies updated configuration; restart after changing module source due
to Node's import cache. `topics.<kind>.defaultExecutor` selects the receiving node's default executor for that topic when `payload.executor` is omitted (or blank). `defaultNetworkId` remains metadata.
See [the plugin guide](../../docs/plugin-guide.md#3-three-tier-onboarding-built-in-project-user)
for the manifest, factory context and lifecycle contracts.


## Local inbox

The `inbox` executor retains messages and materialized attachments in the durable
`mailbox.json` ledger. Each message is routed to a logical mailbox (`default`,
`stream:<streamKey>` for `orca-wave`, `task:<messageId>` for `orca-spawn`, or an
explicit `semantic.targetMailbox` / `payload.mailbox`). A mailbox has at most one
unacknowledged delivery lease (`dlv_<12hex>`), so re-running `check` returns the same
batch until it is acknowledged; newer mail queues behind the lease and is never mixed
into it. `list` is a read-only view and never mints a lease. Commands work against the
running daemon (streaming control socket with keepalives) or directly against the saved
mailbox when offline.

```sh
dealer inbox check --json
dealer inbox check --mailbox stream:alerts --wait --timeout-ms 900000 --json
dealer inbox check --ack <dlv_id> --json
dealer inbox ack <dlv_id> --mailbox stream:alerts --json
dealer inbox list --mailbox stream:alerts --limit 20 --json
dealer inbox list --all --json
```

`--peek` previews the current mailbox, including an outstanding lease, without
minting or mutating a lease. Repeating `ack` for the same lease is an idempotent
no-op; an unknown or mismatched lease fails loudly. Legacy `~/.dealer/inbox.json`
data is migrated once into the `default` mailbox on startup.

Executors durably enqueue mail and record a `wakeTarget` before injecting
anything into a terminal. `WakeupRedriveService` is the only wakeup owner. On
new mail and on a timer it re-reads `mailbox.json`, verifies the authoritative
Orca workspace/terminal idle gate, then sends exactly one single-line pointer:

```text
You have N Dealer message(s) in <mailbox> [hint]. Run `dealer inbox check --mailbox <mailbox> --json`.
```

If an unacknowledged lease predates newer queued mail, the pointer names the
lease instead (`Unacknowledged Dealer batch <dlv> + N newer message(s)`) and
includes the `--ack <dlv>` recovery command. A busy, timed-out, or unverifiable
terminal is never typed into: retries use 1s → 2s → 5s → 10s backoff, and a
daemon restart re-arms every pending target from disk. Acknowledge the batch
after handling it, then end your turn; `/new` rotation happens only after the
lease is acknowledged and the mailbox reaches `rotateAfterWaves`. Pending
Leases, wake targets, and thread routes are not lost on restart.

## Ask, reply, settle, and inspect a thread

These commands are non-blocking: they return as soon as the signed envelope is
dispatched. The other side is woken by the durable mailbox pointer, so never
poll or wait for a reply in the same turn.

```sh
dealer ask --network local --to member:bob --topic collab.question.v1 \
  --subject 'Runtime boundary' --body '{"question":"Who owns retries?"}' --json
dealer ask --reply-to <inbound-envelope-id> --body 'Follow-up question' --json
dealer reply <inbound-envelope-id> --body 'The dispatcher owns retries' --json
dealer settle <inbound-envelope-id> --outcome succeeded --summary 'Verified' --json
dealer thread show <envelope-or-thread-id> --json
```

`ask --reply-to`, `reply`, and `settle` resolve the thread's reverse route
(peer node, network, topic, target mailbox, reply mailbox) and can acknowledge
the local lease by passing `--ack <dlv_id>`. `dealer settle` requires
`--outcome` (`succeeded`, `failed`, or `cancelled`) and a `--summary` (or body
fallback). `thread show` returns the merged thread state and both directional
records; it works through the daemon or directly against the saved mailbox.
For plain messages, address a logical mailbox with
`dealer send --mailbox <id> --reply-mailbox <id> --subject '...'`.
