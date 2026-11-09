# the-dealer

English | [简体中文](README.zh-CN.md)

> *"Deal the right task to the right agent, across any machine."*

**`the-dealer`** is an open-source, P2P, plugin-driven **Agent Collaboration & Dispatch Network**. It enables real-time, event-driven collaboration across agents, machines, and team members—without a central server.

Inspired by **DeepSeek Harness (`Cordis`)** (*"Everything is a Plugin"*) and powered by **Iroh / QUIC P2P mesh** principles, `the-dealer` turns every machine or agent workspace into a self-discovering peer node on one or more encrypted **Collaboration Networks (`Network`)**.

---

## Why `the-dealer`?

Modern AI engineering teams run multiple heterogeneous agents (**Antigravity**, **Claude Code**, **Codex**, **DeepSeek Harness**) across isolated workspaces (such as **Orca** worktrees) and across multiple team members' machines. Existing approaches suffer from three bottlenecks:

1. **Git-bound collaboration requests are slow & manual**: Committing Markdown requests to Git, waiting for webhooks/Lark cards, and manually copying prompts into new local workspaces creates high latency and human toil.
2. **Siloed local dispatch scripts**: Auto-dispatching user feedback, server alerts, iOS crash symbolication, low-quality session reviews, or prod release waves typically requires custom polling daemons tightly coupled to a single machine.
3. **Centralized HTTP protocols (e.g., A2A) lack P2P discovery & local workspace actuation**: Standard web protocols assume public HTTPS endpoints and lack zero-config NAT traversal, cryptographic team network isolation, node-level admission gates (`open` / `approval_required` / `dnd` / `closed`), and local workspace spawners.

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
   - **Cryptographic Network Mounting (`networkId + passphrase`)**: Derives deterministic `swarmTopicId` and 32-byte `sealedKey` via `HKDF-SHA256 / BLAKE3`. Supports single-node **Multi-Network Mounting** (e.g. mounting `ben-home` and `sensebeing` simultaneously).
   - **Zero-Server Discovery & Delivery**: LAN mDNS auto-discovery, cross-network QUIC direct streams (`Unicast` / `Multicast`), `HyParView + PlumTree` epidemic gossip (`Broadcast`), and content-addressed `BLAKE3 / SHA-256` chunked `BlobAttachment` streaming for logs, screenshots, audio/video, and Git patches.
3. **`@the-dealer/node`**: Ed25519 cryptographic node identity (`nodeId`), signed per-network `NodeCard` (`member`, `nodeName`, `admissionMode`, natural-language + tagged `skills`, real-time `load`), and live `PeerDirectory`.
4. **`@the-dealer/emitter` & `@the-dealer/router`**:
   - Pluggable outbound triggers (`source-webhook`, `source-poll-watermark`, `source-script`, direct CLI/MCP).
   - 3-tier intelligent routing pipeline: **Explicit Selector** (`member:oliver`, `ben.xu@mbp`) → **Capability Tag Matcher** (`skill:ios-crash-triage`) → **AI Route Judge Plugin** (`router-local-agent` via AGY/Claude/Codex or `router-llm-api` reading live `NodeCard` capabilities).
5. **`@the-dealer/gate`**: Node-controlled inbound admission state machine (`open`, `approval_required` / Knock mode, `dnd` queue-only, `closed`) with fine-grained sender/skill trust rules.
6. **`@the-dealer/dispatcher`**:
   - **Policy Plugins**: `policy-dedup-cas` (monotonic version/watermark idempotency) and `policy-wave-coalescer` (single-watermark wave coalescing + workspace `Idle Gate` + automatic `/new` session rotation).
   - **Executor Plugins**: `executor-orca-spawn` (spawns isolated Orca worktree + launches agent), `executor-orca-wave` (delivers waves to resident Orca workspace/Run & updates card status), `executor-agent-pty` (standalone CLI agent runner without Orca), and `executor-notify`.

---

## Documentation

- [Architecture & Module Contracts](docs/architecture.md)
- [Wire Protocol & Cryptographic Network Rendezvous](docs/wire-protocol.md)
- [Plugin Authoring Guide](docs/plugin-guide.md)
- [Companion Agent Skill (`skills/the-dealer/SKILL.md`)](skills/the-dealer/SKILL.md)

## Quick Start

```bash
# Run full test suite (Node.js >= 22)
npm test

# Mount current machine onto a team network and inspect status
./bin/dealer network mount sensebeing --passphrase "team-shared-secret" --member ben.xu --node mbp-main
./bin/dealer peers list --network sensebeing
```

## License

[MIT](LICENSE)
