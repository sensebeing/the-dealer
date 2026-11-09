---
name: the-dealer
description: Operate a The Dealer P2P collaboration node from Codex, Claude Code, or Antigravity to discover peers, send tasks and files, manage inbound approvals, and configure routes and sources.
---

# The Dealer

Use `dealer` with Node 22.23.3 or newer. From a checkout, use
`node --experimental-strip-types bin/dealer.js`. Set `DEALER_STATE_DIR` to
select an isolated node; the default is `~/.dealer`. Every command accepts
`--json` before or after its subcommand. Nonzero exit means failure; inspect
stderr's JSON error instead of treating failure as an empty result.

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

Gate pending items/trust, CAS policy watermarks, and buffered waves are
currently in-memory. Stop only after settling inbound work, and do not
assume restart replays a queued wave. Identity, mounts, configuration, source
cursors, and the dispatch ledger persist. A persisted `dispatching` or
`failed` entry must be reconciled with the external executor before retrying.

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
