# Security policy

## Supported versions

| Version | Security fixes |
| --- | --- |
| 0.1.x | Supported on the latest 0.1.x revision |
| Earlier / other versions | Not supported |

## Report a vulnerability privately

Use GitHub's [private vulnerability reporting](https://github.com/sensebeing/the-dealer/security/advisories/new)
to create a repository Security Advisory. Do not open a public issue containing an exploit,
private key, network invite or sensitive transcript. Include the affected revision, platform,
reproduction steps with synthetic data, impact, and any suggested mitigation. If private
reporting is unavailable, request a private reporting channel from a maintainer without
publishing vulnerability details. Coordinate disclosure after a fix is available.

## Security scope

- **Node identity:** Ed25519 private keys under `~/.dealer/`, state-directory permissions,
  runtime lock exclusivity and the local control socket.
- **Network authentication and isolation:** signed node cards, envelopes and receipts;
  per-network HKDF-SHA256 keys and AES-256-GCM authenticated frames; routing candidate checks.
- **Invitations:** `dealer-net://v1/...` URIs contain a network shared secret and expiry.
  They are not signed capability tokens. Import expiry does not revoke a previously mounted
  network. Disclose leaked invitations privately and coordinate shared-secret replacement.
- **Inbound admission:** `approval_required`, sender trust, network overrides, DND persistence,
  and closed gates must not be bypassed by executors or notification mirrors.
- **Attachments:** path traversal prevention, content-addressed blob handling and streaming
  BLAKE3 verification before accepting transferred data.
- **Command execution:** shell-free argument boundaries, receiver-owned project/root mappings,
  worktree hierarchy validation and protection against sender-selected filesystem paths.
- **Durable delivery:** JSON mailbox atomicity, lease isolation, deduplication and truthful
  handling of uncertain external side effects across restarts.

Host CLIs and custom plugins run with the user's permissions. Install only trusted plugins
and inspect receiver bindings before enabling execution. Keep shared secrets and private keys
out of source control, issue templates, screenshots and logs.
