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
4. **`DispatchPolicyPlugin` (`dispatcher.registerPolicy`)**: Wrap inbound execution with CAS idempotency watermarks (`policy-dedup-cas`) or wave coalescing + workspace idle gates + automatic `/new` session rotation (`policy-wave-coalescer`).
5. **`ExecutorPlugin` (`dispatcher.registerExecutor`)**: Actuate admitted messages locally—e.g., spawning an isolated Orca worktree + agent terminal (`executor-orca-spawn`), delivering a coalesced wave to a resident Orca workspace (`executor-orca-wave`), or running a local CLI agent (`executor-agent-pty`).
