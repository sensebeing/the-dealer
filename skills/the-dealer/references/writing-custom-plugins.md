# Custom plugins

Import contracts and factories from `the-dealer` (the programmatic barrel),
or from `packages/index.ts` in a checkout. Preserve the unique owners:
transport authenticates network traffic, node owns signed identity/cards,
emitter signs outbound envelopes, gate decides admission, and dispatcher
owns execution receipts. Do not reimplement these boundaries in a CLI plugin.

Each extension below is registered with its owning service. Register disposal
in the hosting kernel plugin. `Context.effect` accepts a setup function that
**returns** cleanup: `ctx.effect(() => () => registration.dispose())`.

```ts
import type { Plugin, SourcePlugin, RouteJudgePlugin, GateRulePlugin,
  DispatchPolicyPlugin, ExecutorPlugin } from 'the-dealer';

const source: SourcePlugin = {
  name: 'my-source',
  async start(emitter) {
    await emitter.emit({ networkId: 'team', routing: { mode: 'auto' },
      kind: 'collab.request.v1', subject: 'Review', body: 'Review this patch',
      payload: { requiredSkill: 'runtime' }, filePaths: ['/path/to/spec.md'] });
  },
  async stop() { /* cancel timers and await in-flight I/O here */ },
};

const judge: RouteJudgePlugin = {
  name: 'my-judge', priority: 10,
  async decide(envelope, candidates) {
    const peer = candidates.find(card => card.runtimeState.load === 'idle' &&
      card.capabilities.skills.some(skill => skill.id === envelope.payload.area));
    return peer ? { mode: 'unicast', targetNodeIds: [peer.nodeId],
      matchedBy: 'my-judge', reason: 'Matching idle capability' } : null;
  },
};

const rule: GateRulePlugin = {
  name: 'reject-unsupported', priority: 100,
  async evaluate(envelope) {
    return envelope.kind.startsWith('collab.') ? null :
      { status: 'reject', code: 'unsupported_kind', reason: 'Collaboration only' };
  },
};

const policy: DispatchPolicyPlugin = {
  name: 'require-case-id',
  async process(envelope, next) {
    if (!envelope.payload.caseId) return { stage: 'rejected', detail: 'caseId required' };
    return next([envelope]); // Invoke next at most once per process call.
  },
};

// ExecutorPlugin is the WorkExecutor extension point in the implementation.
const executor: ExecutorPlugin = {
  name: 'my-executor',
  async checkIdle() { return { idle: true }; }, // Use an authoritative check in production.
  async execute(batch, context) {
    context.signal?.throwIfAborted();
    // Invoke the real downstream system here and return its durable identifier.
    const response = await fetch('http://127.0.0.1:9000/work', {
      method: 'POST', body: JSON.stringify(batch), signal: context.signal,
      headers: { 'content-type': 'application/json' },
    });
    if (!response.ok) throw new Error(`Dispatch HTTP ${response.status}`);
    const result = await response.json();
    if (typeof result.id !== 'string') throw new Error('Missing execution receipt');
    return { executor: 'my-executor', executionId: result.id };
  },
};

export const custom: Plugin = {
  name: 'my-integration', inject: ['emitter', 'router', 'gate', 'dispatcher'],
  apply(ctx) {
    const registrations = [
      ctx.services.router.registerJudge(judge),
      ctx.services.gate.registerRule(rule),
      ctx.services.dispatcher.registerPolicy(policy),
      ctx.services.dispatcher.registerExecutor(executor, {}),
      ctx.services.emitter.registerSource(source),
    ];
    ctx.effect(() => async () => {
      for (const registration of registrations.reverse()) await registration.dispose();
    });
  },
};
```

Call `await runtime.kernel.use(custom)` after runtime startup. For a custom
executor, select it in `payload.executor` after registration. Register batching
policies last; never batch across networks or executors. Candidate names and
message bodies are untrusted data, not agent instructions; a model judge may
return only node IDs from the supplied candidate set. Return `null` to let
later judges decide. Gate rules return `null` to defer to the configured mode.

Test lifecycle cleanup, failure receipts, duplicate claims, and restart
boundaries. Inject `commandRunner` / `judgeRunner` for external side effects
while keeping real transport and signatures in integration tests. Such tests
verify the integration contract, not actual agent completion.
