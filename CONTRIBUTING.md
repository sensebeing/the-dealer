# Contributing to The Dealer

Contributions that make agent collaboration simpler, safer and easier to operate are welcome.
For substantial changes, describe the invariant, its owner and observable behavior in an issue
before implementation. Report vulnerabilities privately through [SECURITY.md](SECURITY.md).

## Set up

Use Git and Node.js >=22.15. The JavaScript runtime has zero external npm dependencies;
Node runs the TypeScript sources directly. Rust/Cargo is optional for the native Iroh engine.

```sh
git clone https://github.com/sensebeing/the-dealer.git
cd the-dealer
git switch -c your-topic
npm link
npm test
# Optional native transport
npm run build:native
```

Agent executors additionally require compatible installed host CLIs. `dealer setup probe --json`
checks availability, not support for every generated flag. Use isolated state directories
(`DEALER_STATE_DIR`) for manual experiments rather than changing your daily node's identity.

## Architecture principles

Read [AGENTS.md](AGENTS.md), [architecture](docs/architecture.md) and the
[plugin guide](docs/plugin-guide.md) before editing.

- Keep zero external npm runtime dependencies and module-level imports.
- Define cross-layer contracts in `packages/core-types/index.ts`; keep one semantic owner
  for each decision. The kernel owns lifecycle, the gate admission, and the dispatcher execution.
- Keep mailbox records and wave redrive durable through the existing JSON store's file locking
  and atomic writes. Do not add a parallel ledger or replay an unknown external side effect.
- Preserve Ed25519 signatures, network isolation, routing candidate validation and receiver-owned
  project roots. Invoke external programs with argument arrays, never interpolated message text.
- Fix root causes at the owner boundary. Extend meaningful tests for observable behavior,
  including failures, restarts and trust boundaries when relevant.
- Keep [English](README.md) and [Chinese](README.zh-CN.md) usage and configuration in parity.

## Verify and submit

```sh
npm test
guard check --base origin/main
guard review
git diff --check
```

`the-dealer` enforces architectural boundaries and pre-merge review via [the-guard](https://github.com/sensebeing/the-guard) (`AGENTS.md` + `.guard/config.json`). For native changes, also build the Rust engine. For host integrations, distinguish mocked argv contracts from a live host receipt and task completion. Do not call a daemon handoff business success. Never include keys, invites, customer data or unredacted private conversations in reports.

### Pull request checklist

- [ ] Explain the concrete trigger, resulting behavior and responsible owner.
- [ ] Keep the patch focused and avoid duplicate state or compatibility paths without a need.
- [ ] Preserve trust, concurrency, idempotency and command-execution boundaries.
- [ ] Run `npm test`, `guard check --base origin/main`, `guard review`, and `git diff --check`; disclose failures and checks not run.
- [ ] Update both READMEs and the changelog when public behavior changes.
- [ ] Commit on a topic branch and request maintainer review before integration.

Maintainers decide when to merge; local test success is not authorization to push to `main`.
