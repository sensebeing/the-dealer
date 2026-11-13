# Profiles and patch overlays

| Profile | Default gate | Policies | Executors | Default executor |
| --- | --- | --- | --- | --- |
| personal-dev | open | dedup-cas | orca, orca-spawn, orca-wave, codex-cli, codex, claude-code, antigravity-cli, antigravity, claude-app, chatgpt-app, antigravity-app, agent-pty, notify, inbox | orca-spawn |
| dedicated-worker | open | dedup-cas | orca, orca-spawn, orca-wave, codex-cli, codex, claude-code, antigravity-cli, antigravity, claude-app, chatgpt-app, antigravity-app, agent-pty, inbox | orca-spawn |
| orca-dev | open | dedup-cas | orca, orca-spawn, orca-wave, notify, inbox | orca |
| codex-dev | open | dedup-cas | codex-cli, codex, agent-pty, notify, inbox | codex |
| claude-dev | open | dedup-cas | claude-code, agent-pty, notify, inbox | claude-code |
| antigravity-dev | open | dedup-cas | antigravity, antigravity-cli, antigravity-app, orca, orca-spawn, orca-wave, agent-pty, notify, inbox | antigravity |
| headless-relay | open | none | notify, inbox | notify |

The headless profile receives into a private notification inbox and can route
outbound messages; it does not automatically forward inbound traffic.
The runtime always loads transport, node, router, gate, dispatcher and emitter.
Built-ins are available through `BUILT_IN_PROFILES` / `getBuiltInProfile(name)`.
`DealerNodeRuntime({stateDir, profile, patch})` embeds the same stack in-process.

## Configuration

On fresh startup, profile defaults are overlaid with saved configuration,
explicit startup flags, then the patch. An explicitly selected profile
replaces its gate/policy/executor defaults. `dealer node up --patch patch.json`
applies the overlay when starting a daemon. Stop an existing daemon before
changing its startup profile or patch. Fields replace whole values; objects
and arrays are not recursively merged. Use only trusted patch files.

```json
{
  "gateMode": "open",
  "skills": ["ios-crash", "runtime"],
  "defaultExecutor": "orca",
  "executorConfig": {
    "orca": {
      "runId": "run_example",
      "terminalHandle": "term_example",
      "cwd": "/path/to/resident/workspace"
    }
  },
  "wave": {"rotateAfterWaves": 6, "flushIntervalMs": 1000},
  "routes": [{"type": "local-agent", "config": {"command": "codex"}}]
}
```

Allowed node fields: `profile`, `member`, `name`, `skills`, `port`, `gateMode`,
`networks`, `defaultExecutor`, `policies`, `executors`, `executorConfig`,
`routes`, `sources`, `wave`. Mount entries use `networkId`, `passphrase`,
optional `bootstrapPeers`, `admissionMode`, `exposedSkills`, and local `role`.
Avoid selecting `orca` until its target and authoritative idle check are
configured. The default check inspects the workspace and descendants.

A wake target applies only to envelopes selecting `orca` with `mode: wave`; other executors
dispatch immediately after dedup. Stream identity includes network,
`payload.streamKey` (or message kind), and executor. `orca` durably
enqueues the batch into its mailbox and records a wake target, returning
`dispatched` even when the workspace is busy. `wave.minAgeMs` is the coalescing
delay before a pointer (a message's `payload.minAgeMs` overrides it), and
`wave.flushIntervalMs` controls the redrive timer. `WakeupRedriveService` never
injects text into a busy, timed-out, or unverifiable terminal. Before the next
pointer, rotation opens a new agent session tab in the configured workspace only
after the previous lease is acknowledged and
`completedWaves >= rotateAfterWaves`; it never rotates an unacknowledged batch.
The previous tab is left untouched — the wakeup service never sends `/new` or
resets an existing TUI. A managed fixed workspace defaults to
`rotateAfterWaves: 1`, so every wave owns an independent tab and the first
wakeup never hijacks a conversation tab a human already opened; a terminal
handle bound without `rotateAfterWaves` keeps delivering to that same terminal.
`payload.executor` can select any enabled executor, so
expose only executors permitted for admitted peers.

## Dependency-free YAML subset

JSON is preferred. `.yaml` overlays support indentation-based mappings,
plain word/path values, JSON double-quoted strings, and inline JSON arrays or
objects. Block lists, anchors, tags, multiline strings, and implicit YAML
coercions are unsupported and rejected.

```yaml
gateMode: approval_required
skills: ["ios-crash", "runtime"]
defaultExecutor: orca
executorConfig:
  orca:
    runId: run_example
    terminalHandle: term_example
    cwd: /path/to/workspace
wave:
  rotateAfterWaves: 6
  flushIntervalMs: 1000
```

`route add` / `source add` register immediately and persist definitions for
restart. A source's startup must succeed before registration is reported.
A script source emits one full `EmitMessageInput` JSON object per stdout line.
The poll endpoint returns `{"items":[EmitMessageInput],"cursor":nextCursor}`;
subsequent requests include a JSON-encoded `cursor` query parameter. A cursor
advances only after all item deliveries succeed. Use stable `idempotencyKey`
and monotonically increasing `payload.revision` for event claims. Cursor state
is saved privately in stateDir. Webhooks accept the same full input via POST.

```json
{
  "networkId": "sensebeing",
  "routing": {"mode": "unicast", "targets": ["member:bob"]},
  "kind": "alert.feedback.v1",
  "subject": "Runtime feedback",
  "body": "Investigate this report",
  "payload": {"executor": "orca", "mode": "wave", "streamKey": "runtime", "revision": 1},
  "idempotencyKey": "feedback:123"
}
```
