# Phase 4 delivery evidence

Implemented the programmatic barrel, dependency-free CLI/runtime, executable
`dealer` and `dealerd`, three profiles, configuration overlays, companion
agent skill, and multi-node integration tests.

## Verification

- Node v22 native TypeScript: `PATH=/opt/homebrew/bin:$PATH npm test` — 49
  tests passed, 0 failed (all four phases).
- `quick_validate.py skills/the-dealer` — skill is valid (isolated uv Python
  environment supplies the validator's PyYAML dependency; no npm dependency).
- `git diff --check` — passed.
- Actual executable test starts detached dealerd, issues commands from fresh
  dealer processes over the private Unix socket, and terminates the daemon.
- Multi-node tests use real Kernel, encrypted P2P transport, signed peer cards,
  signed receipts, gate, dispatcher, and BLAKE3 blob fetch. Orca command runners
  and the local model judge are injected; no real agent/worktree was launched.

## Boundaries for review

- Persisted: private identity, configuration/mounts, dispatch ledger, poll
  cursors, notification inbox, fetched attachments. State ownership is exclusive
  through a lock directory; control socket is mode 0600 inside a 0700 stateDir.
- Existing phase 1–3 gate queues/trust, policy CAS watermarks, and buffered waves
  remain in-memory. Restart does not recover pending policy buffers. This is
  explicitly documented; no durable exactly-once/restart claim is made.
- Existing gate trust semantics can override closed/DND defaults and trust
  claimed member names within a shared-secret network. This patch does not
  silently alter the established admission policy.
- The current transport is encrypted TCP/local discovery, not production Iroh
  QUIC/NAT traversal. Broadcast does not provide individual acknowledgements.
- YAML support is an explicit small subset (nested mappings plus JSON values);
  full YAML parsing is deliberately not claimed. Local roles are descriptive
  metadata, not peer authorization. Invite expiry is an import-time check.
- Startup profile/patch changes require stopping the daemon. Live route/source
  additions are supported and saved; arbitrary core hot replacement is not a
  CLI operation. The programmatic kernel retains its existing patch API.
- `orca-spawn` and `orca-wave` retain their phase 3 command contracts. Passing
  tests prove orchestration argument and receipt handling, not compatibility
  with every installed Orca version or external agent task completion.

The branch is intended for coordinator review; it has not been merged or pushed.
