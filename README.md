# the-dealer

English | [简体中文](README.zh-CN.md)

> *"Collaborate with every agent on your machine first — then across any machine and teammate."*

**`the-dealer`** is an open-source, plugin-driven **Agent Collaboration & Dispatch System**. It starts as a **same-machine multi-agent collaboration and dispatch hub**: one developer can coordinate specialized agent workspaces (dev/prod alert waves, feedback and quality claims, prod-release handoffs, isolated child-worktree tasks) on a single machine with **zero network configuration**.

When a team mounts an encrypted collaboration network (`networkId + passphrase`), the exact same kernel, envelopes, routers, gates, and executors extend this into a **cross-machine, cross-team P2P collaboration network** — without a central server. Inspired by **DeepSeek Harness (`Cordis`)** (*"Everything is a Plugin"*) and **Iroh / QUIC P2P mesh** principles, every machine or agent workspace becomes a self-discovering peer node.

---

## Local First, P2P When You Need It

1. **Step 1 — Same-Machine Multi-Agent Collaboration (Local First, Zero Network Config)**
   Run `dealer node up` and send to your own node (`member:<self>` or `skill:<local-skill>`). No `network mount`, `passphrase`, peer discovery, or external network is involved. The message still travels the full gate → dispatcher → policy → executor pipeline, so same-machine work gets the same deduplication, wave coalescing, idle gating, and Orca workspace dispatch as remote work. A project's `defaultNetworkId` is simply the local scope its sources emit on.

2. **Step 2 — Cross-Machine & Cross-Team P2P Extension (`networkId + passphrase`)**
   Agree on a `networkId` and a shared `passphrase`, then run `dealer network mount`. HKDF-SHA256 derives the encrypted discovery channel and AEAD frame keys; signed `NodeCard`s, selectors (`member:`, `skill:`, `member@node`, `network/selector`), four-state admission gates, and every executor continue to work unchanged inside the encrypted network.

---

## Why `the-dealer`?

Modern AI engineering teams run multiple heterogeneous agents (**Antigravity**, **Claude Code**, **Codex**, **DeepSeek Harness**) across isolated workspaces (such as **Orca** worktrees) — often on one developer's machine before any cross-machine collaboration is needed. Existing approaches suffer from three bottlenecks:

1. **Git-bound collaboration requests are slow & manual**: Committing Markdown requests to Git, waiting for webhooks/Lark cards, and manually copying prompts into new local workspaces creates high latency and human toil.
2. **Siloed local dispatch scripts**: Auto-dispatching user feedback, server alerts, iOS crash symbolication, low-quality session reviews, or prod release waves typically requires custom polling daemons tightly coupled to a single machine, with no shared routing, gating, or idempotency model.
3. **Centralized HTTP protocols (e.g., A2A) lack P2P discovery & local workspace actuation**: Standard web protocols assume public HTTPS endpoints and lack zero-config NAT traversal, cryptographic team network isolation, node-level admission gates (`open` / `approval_required` / `dnd` / `closed`), and local workspace spawners.

`the-dealer` solves the same-machine case first with a zero-config local loopback, then reuses the same contracts for encrypted cross-machine collaboration instead of introducing a second dispatch stack.

---

## Core Architecture: Everything is a Plugin

```
┌───────────────────────────────────────────────────────────────────────────┐
│                   @the-dealer/kernel (Cordis-Style)                       │
│   Service Registry (inject) · Typed Events (emit/waterfall/bail/serial)   │
│         Reversible Effects (ctx.effect) · Profile + Patch Loader          │
└───────┬───────────────────┬───────────────────┬───────────────────┬───────┘
        │                   │                   │                   │
┌───────▼───────┐   ┌───────▼───────┐   ┌───────▼───────┐   ┌───────▼───────┐
│   transport   │   │     node      │   │ emitter/router│   │gate/dispatcher│
│  Iroh + P2P   │   │  Identity &   │   │  Triggers &   │   │ Admission &   │
│  Multi-Net    │   │   NodeCard    │   │   AI Judge    │   │  Actuators    │
└───────────────┘   └───────────────┘   └───────────────┘   └───────────────┘
```

### Six Orthogonal Engineering Modules

1. **`@the-dealer/kernel`**: Zero-privilege plugin microkernel with service dependency injection (`inject`), four typed event dispatch modes (`emit`, `waterfall`, `bail`, `serial`), reversible disposers (`ctx.effect`), and declarative `Profile + Patch` hot-reload composition.
2. **`@the-dealer/transport` + `native/dealer-iroh-engine`**:
   - **Same-machine loopback**: When no network is mounted, delivers self-addressed frames in-process through the same inbound handlers; when mounted, self-addressed frames use the local endpoint.
   - **Cryptographic Network Mounting (`networkId + passphrase`)**: Derives deterministic `swarmTopicId` and 32-byte `sealedKey` via `HKDF-SHA256 / BLAKE3`. Supports single-node **Multi-Network Mounting** (e.g. mounting `ben-home` and `sensebeing` simultaneously).
   - **Zero-Server Discovery & Delivery**: LAN mDNS auto-discovery, cross-network QUIC direct streams (`Unicast` / `Multicast`), `HyParView + PlumTree` epidemic gossip (`Broadcast`), and content-addressed `BLAKE3 / SHA-256` chunked `BlobAttachment` streaming for logs, screenshots, audio/video, and Git patches.
3. **`@the-dealer/node`**: Ed25519 cryptographic node identity (`nodeId`), signed per-network `NodeCard` (`member`, `nodeName`, `admissionMode`, natural-language + tagged `skills`, real-time `load`), and live `PeerDirectory`. Selector resolution prefers discovered remote peers and falls back to the local self card, so single-machine routing needs no peer.
4. **`@the-dealer/emitter` & `@the-dealer/router`**:
   - Pluggable outbound triggers (`source-webhook`, `source-poll-watermark`, `source-script`, direct CLI/MCP).
   - 3-tier intelligent routing pipeline: **Explicit Selector** (`member:oliver`, `ben.xu@mbp`) → **Capability Tag Matcher** (`skill:ios-crash-triage`) → **AI Route Judge Plugin** (`router-local-agent` via AGY/Claude/Codex or `router-llm-api` reading live `NodeCard` capabilities). The local self card is a first-class candidate.
5. **`@the-dealer/gate`**: Node-controlled inbound admission state machine (`open`, `approval_required` / Knock mode, `dnd` queue-only, `closed`) with fine-grained sender/skill trust rules. Self-originated local deliveries bypass approval knocks while still respecting `dnd` and `closed`.
6. **`@the-dealer/dispatcher` & `@the-dealer/mailbox`**:
   - **Policy Plugins**: `policy-dedup-cas` (monotonic version/watermark idempotency).
   - **Durable Mailbox & Wakeup Redrive**: `MailboxStore` atomically persists inbound mail, a single outstanding lease (`dlv_*`), wake targets, and directional thread routes in `mailbox.json`; `WakeupRedriveService` survives daemon restarts, gates every single-line pointer on authoritative Orca workspace/terminal idleness, never injects into a busy terminal, and rotates `/new` only after an acknowledged batch reaches the configured threshold.
   - **Inbox inspection and acknowledgement**: `dealer inbox check` leases the oldest unacknowledged batch, `dealer inbox ack <dlv_id>` completes it, and `dealer inbox list` is a read-only view; newer mail never mixes into an outstanding batch.
   - **Executor Plugins**: `executor-orca-spawn` (spawns an isolated Orca worktree + agent and injects the task preamble only after confirmed idle), `executor-orca-wave` (durably enqueues waves and wakes the resident workspace), `executor-agent-pty` (standalone CLI agent runner without Orca), `executor-inbox` (local mailbox retention), and `executor-notify`.
   - **Non-blocking conversation**: `dealer ask` / `reply` / `settle` / `thread show` return immediately and wake the peer through the durable mailbox, with no `--reply-to` blocking wait.

---

## Documentation

- [Architecture & Module Contracts](docs/architecture.md)
- [Wire Protocol & Cryptographic Network Rendezvous](docs/wire-protocol.md)
- [Plugin Authoring Guide](docs/plugin-guide.md)
- [Companion Agent Skill (`skills/the-dealer/SKILL.md`)](skills/the-dealer/SKILL.md)

## Quick Start

### Step 1 — Same-machine multi-agent collaboration (zero network config)

```bash
# Run the full test suite (Node.js >= 22)
npm test

# Start a local node and inspect it
./bin/dealer.js node up --profile personal-dev --member ben.xu --name mbp --skills agent-runtime --json
./bin/dealer.js node status --json

# Deliver to this same node. `local` is only a stable scope name: no mount or passphrase required.
./bin/dealer.js send --network local --to member:ben.xu --topic collab.request.v1 --body '{"summary":"Run the local quality sweep"}' --json
./bin/dealer.js send --network local --to skill:agent-runtime --topic triage --body '{"area":"agent-runtime"}' --json

# Non-blocking ask: returns immediately; the reply wakes this agent through the durable mailbox.
./bin/dealer.js ask --network local --to member:ben.xu --topic collab.question.v1 --subject 'Ownership' --body '{"question":"Who owns retries?"}' --json
./bin/dealer.js thread show <message-id> --json
./bin/dealer.js inbox check --mailbox task:w1 --json
./bin/dealer.js inbox ack <dlv_id> --mailbox task:w1 --json
./bin/dealer.js inbox list --mailbox task:w1 --json
```

A linked project's `defaultNetworkId` is the local scope its source plugins emit on; same-machine sends still route to the local self card.

### Step 2 — Cross-machine and cross-team P2P extension

```bash
# Mount the same encrypted collaboration network on each teammate's machine
./bin/dealer.js network mount sensebeing --passphrase "team-shared-secret" --member ben.xu --node mbp-main --json
./bin/dealer.js peers list --network sensebeing --json

# Send across machines inside that encrypted network
./bin/dealer.js send --network sensebeing --to member:oliver --topic collab.request.v1 --body '{"summary":"Review the release"}' --json
```

## License

[MIT](LICENSE)
