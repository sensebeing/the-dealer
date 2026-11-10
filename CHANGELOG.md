# Changelog

All notable changes are documented here, following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
The project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-10

### Added

- Plugin microkernel with typed contracts, dependency injection, ordered lifecycle cleanup,
  routing policies, admission gates and delivery receipts.
- Encrypted default TCP transport and an optional Rust Iroh QUIC engine, with Ed25519
  identities, per-network authenticated encryption and streaming BLAKE3 attachment checks.
- Atomic JSON mailbox persistence, delivery leases, directional threads, durable wake targets,
  restart recovery and idle-gated wave redrive.
- Real-time session pointers with bounded inline previews and zero-coalescing conversational
  frames; host profiles and spawn/fixed/inbox-only bindings for Orca, Codex, Claude Code and
  Antigravity, subject to installed host CLI compatibility.
- Human copilot integration through the `lark-mirror` plugin and OS `notify` executor.
- Three bundled skills: `the-dealer-setup`, `the-dealer` and `the-dealer-extend`.
- Bilingual onboarding, contributor and security guidance, issue templates and package metadata.

### Fixed

- Autopilot composer-prefill detection no longer requests clearing a clean root workspace
  composer when only a child workspace has a recognized prefill.
