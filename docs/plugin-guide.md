# Plugin Authoring Guide

`the-dealer` is built on a Cordis-inspired microkernel (`@the-dealer/kernel`) where **everything is a plugin**.

---

## 1. Writing a Plugin

A plugin is an object with a `name`, optional `inject` service dependencies, and an `apply(ctx, config)` function. Any side effects registered via `ctx.effect()` or `ctx.on()` are automatically unwound when the plugin is unloaded or hot-reloaded via profile patches.

```typescript
import type { Plugin } from "@the-dealer/kernel";

export interface CustomAlertSourceConfig {
  networkId: string;
  intervalMs: number;
}

export const customAlertSourcePlugin: Plugin<CustomAlertSourceConfig> = {
  name: "source-custom-alert",
  inject: ["emitter"],
  apply(ctx, config) {
    const disposable = ctx.services.emitter.registerSource({
      name: "custom-alert",
      start() {
        // Poll or subscribe and call ctx.services.emitter.emit(...)
      },
      stop() {},
    });
    ctx.effect(() => disposable.dispose());
  },
};
```

---

## 2. Five Extension Points

1. **`SourcePlugin` (`emitter.registerSource`)**: Connect external webhooks, watermark pollers (Lark Base, Source Hub feedback claims, runtime quality monitors), or custom scripts to emit messages into any mounted `Network`.
2. **`RouteJudgePlugin` (`router.registerJudge`)**: Inspect an outbound `MessageEnvelope` with `routing.mode === "auto"` and the live list of candidate `NodeCard`s in that `Network`, returning a `RouteDecision` (`unicast`, `multicast`, or `broadcast`).
3. **`GateRulePlugin` (`gate.registerRule`)**: Hook into `gate/evaluate` (`bail` mode) to allow, require human approval (`pending_approval`), queue under DND (`queued_dnd`), or reject inbound messages by sender member, skill, or message kind.
4. **`DispatchPolicyPlugin` (`dispatcher.registerPolicy`)**: Wrap inbound execution with CAS idempotency watermarks (`policy-dedup-cas`). Durable wave/task wakeup is owned by `MailboxStore` and `WakeupRedriveService`, not by a policy plugin.
5. **`ExecutorPlugin` (`dispatcher.registerExecutor`)**: Actuate admitted messages locally—e.g., spawning an isolated Orca worktree + agent terminal (`executor-orca-spawn`), delivering a coalesced wave to a resident Orca workspace (`executor-orca-wave`), or running a local CLI agent (`executor-agent-pty`).

## 3. Three-tier onboarding: built-in, project, user

The built-in profile supplies the core services and executors. A repository's
`.dealer/project.json` declares portable project plugins and metadata. Personal
settings belong in `~/.dealer/projects/<id>.patch.json`; standalone personal
plugins belong in `~/.dealer/plugins/*.ts`. `DEALER_STATE_DIR` replaces `~/.dealer`
for isolated nodes. Only link projects or install modules you trust: importing
an external module executes local code with the daemon's permissions.

```sh
dealer project init --root ./my-project --id my-project --network team --skills triage --description 'Team automation' --json
dealer project link ./my-project --json
dealer project list --json
dealer project status --project my-project --json
dealer plugin add --name personal-hook --module plugins/personal-hook.ts --config ./personal.json --json
dealer plugin list --json
dealer plugin remove personal-hook --json
dealer project unlink my-project --json
```

`plugin remove` hot-unloads the named personal plugin and persists its removal through the running daemon; it leaves the module file on disk. Use these commands instead of editing a running node's `config.json`.

`init` creates `.dealer/plugins/` and refuses to overwrite an existing manifest.
Its root defaults to the caller's working directory, ID to the root directory
name, and network to that ID. `link` also defaults to the caller's working
directory. A project ID must start with an ASCII letter or digit and contain
only letters, digits, dots, underscores or hyphens (it is also a patch filename).
Linking records metadata; it does not mount a network or grant admission.

Example `.dealer/project.json`:

```json
{
  "schema": "the-dealer-project/v1",
  "projectId": "my-project",
  "defaultNetworkId": "team",
  "skills": ["triage"],
  "plugins": [
    { "name": "project-hook", "module": ".dealer/plugins/hook.ts", "config": { "label": "team" } }
  ],
  "topics": { "triage": { "description": "Triage requests", "defaultExecutor": "notify", "streamKey": "triage" } }
}
```

Project module paths resolve relative to the project root; user plugin paths
resolve relative to stateDir. Absolute paths also work. TypeScript modules must
use syntax supported by Node's type stripping; ESM `.mjs`/`.js` modules work too.
Export `createPlugin(config, context)` or a default plugin object/factory.
Factories may be async. A missing plugin name defaults to the reference name.
Plugin names share the kernel namespace: collisions with core services, other
projects or personal plugins are rejected.

Example `.dealer/plugins/hook.ts`:

```ts
export function createPlugin(config, context) {
  return {
    name: 'project-hook',
    apply(ctx, appliedConfig) {
      ctx.on('emitter/prepare', (envelope, next) => next({
        ...envelope,
        body: `[${appliedConfig.label}] ${envelope.body}`,
      }));
    },
  };
}
```

The host also exposes `isConfirmedHandoffReceipt(receipt, { allowQueued?: boolean })`,
`isDurableQueuedReceipt(receipt)` and
`isDedupCasDuplicateReceipt(receipt, { allowQueued?: boolean })`. The latter
recognizes only durable confirmed handoffs (settled by default, queued when
explicitly allowed). An unknown external outcome is never a confirmed duplicate.
Dedup watermarks persist `revision` and `status` (`unknown`, `queued`, `settled`);
definite pre-execution failures and non-durable queues release their reservation.
Runtime DND envelopes persist in `gate-dnd.json` before a durable queued receipt
is signed. Recovery drains only open mounted networks after project and user
admission rules have loaded. Only confirmed handoffs clear DND or approval obligations.
Unconfirmed results park the full signed DND envelope with attempts, lastError and
updatedAt; inspect via `gate.listDndQueue()` and explicitly retry via
`gate.drainDnd(networkId, { includeParked: true })`. Default drains skip parked
entries; a strictly newer integer revision resets an entry to queued.
Legacy numeric watermarks upgrade atomically to unknown unless
`resolveLegacyWatermarkStatus(key, revision)` supplies durable corroboration.
Runtime uses verified mailbox envelopes or dispatched/settled delivery records
for settled status only; all other evidence, including queued receipts, pending
DND entries, failed, dispatching and absent records, remains unknown. Pending DND
obligations never self-confirm downstream dedup. Parked DND entries reject the
same or older revision until a strictly newer integer revision resets them.

The factory context includes `projectRoot` (project plugins only), `stateDir`,
`runtime`, the parsed `userPatch`, and `api`. The `api` bag exposes the built-in
webhook/poll-watermark/script source factories, dedup-CAS policy
factories, Orca spawn/wave, agent-PTY and notify executor factories, and local-agent
and LLM-API router factories under their exported `create*Plugin` names. Both
project and user factories can call, for example,
`context.api.createNotifyExecutorPlugin(config)` without importing a checkout
path. Register the resulting source, policy, executor or judge with its owning
service inside `apply`, and dispose that registration during plugin cleanup. Register effects with the kernel context
so unlinking and reloading can clean them up. Factories should construct plugin
objects; keep reversible runtime effects in `apply` and its cleanup.

Example `~/.dealer/projects/my-project.patch.json`:

```json
{
  "skills": ["personal-triage"],
  "pluginConfig": { "project-hook": { "label": "personal" } }
}
```

`--user-patch ./custom.json` selects an alternate patch (JSON or the existing
restricted YAML mapping format). The shallow per-plugin overlay wins over
manifest config. Project and patch skills merge into the node's skill list with
deduplication. Skills remain in that list after unlink; unlink deactivates the
project's plugins and removes its link, without removing personal plugins.
Topics/defaultNetworkId are exposed as project metadata, not automatic routing
or executor overrides in this stage.

Persisted projects load after the built-in profile, followed by personal plugins
on daemon start. Relinking refreshes manifest/patch configuration and replaces
that project's registrations. Node caches imported modules; restart the daemon
after editing module source. A failed activation fails the command and restores
the previous registrations/config where cleanup succeeds; cleanup failures are
reported, never treated as successful installation.
