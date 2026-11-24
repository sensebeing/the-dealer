import { toMailboxTopicBinding } from '../packages/cli/config.ts';
import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { cpSync, existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  createPollWatermarkSourcePlugin, writeAtomicCursorFile, readJsonCursorFile, everyReceiptSettled,
  isConfirmedHandoffReceipt, DEDUP_CAS_DUPLICATE_DETAIL, parseYaml, parseMarkdownFrontmatter,
  parseTeamRegistry, resolveTeamRegistryMembers, createTeamRegistryRouterPlugin,
  Kernel, createRouterPlugin, generateNodeKeyPair, signEnvelope, signNodeCard,
  DealerNodeRuntime, resolveBoundMailboxId, resolveMailboxId, validateTopicBindings,
  createOrcaSpawnExecutorPlugin, initProjectScaffold, runDealerCli, rpc,
} from '../packages/index.ts';
import type { DeliveryReceipt, EmitterService, MessageEnvelope, NodeCard, CommandRequest, PollSourceStatusEvent } from '../packages/index.ts';
import * as subprocess from '../packages/subprocess/index.ts';
import * as publicApi from '../packages/index.ts';
import { dealerCodeFingerprintEntries } from '../packages/cli/auto-update.ts';

function directory(t: TestContext): string {
  const path = mkdtempSync(join(tmpdir(), 'dealer-extractions-'));
  t.after(() => rmSync(path, { recursive: true, force: true }));
  return path;
}
const keys = generateNodeKeyPair();
function envelope(id: string, patch: Partial<MessageEnvelope> = {}): MessageEnvelope {
  return signEnvelope({ id, traceId: id, networkId: 'local', from: { nodeId: keys.nodeId, member: 'sender', nodeName: 'sender' },
    routing: { mode: 'auto' }, kind: 'wave.alert.v1', subject: id, body: 'Details', payload: {}, attachments: [],
    priority: 'normal', createdAt: new Date().toISOString(), ...patch }, keys.privateKeyPem);
}
const input = { networkId: 'local', routing: { mode: 'auto' as const }, kind: 'task', subject: 'task', body: '', payload: {} };
function emitter(receipts: readonly DeliveryReceipt[]): EmitterService {
  return { emit: async () => ({ messageId: 'receipt', traceId: 'receipt', receipts: [...receipts] }), registerSource() { throw new Error('unused'); } };
}
const receipt = (stage: string, executionReceipt?: Record<string, unknown>, detail?: string) => ({ stage, executionReceipt, detail }) as DeliveryReceipt;

test('subprocess leaf exports remain public and participate in daemon code fingerprints', () => {
  for (const name of ['runCommand', 'checkedCommand', 'CommandNotStartedError', 'findExecutable', 'singleLine'] as const) {
    assert.equal(typeof subprocess[name], 'function', name);
    assert.equal(publicApi[name], subprocess[name], name);
  }
  assert.equal(subprocess.singleLine('  command\n with\t arguments  '), 'command with arguments');
  assert.ok(dealerCodeFingerprintEntries().some(([path]) => path === 'dealer/packages/subprocess/index.ts'));
});

test('atomic cursor snapshots restart, preserve previous JSON on serialization failure, and fail on corruption', t => {
  const path = join(directory(t), 'nested/cursor.json');
  assert.equal(readJsonCursorFile(path, 7), 7);
  writeAtomicCursorFile(path, { version: 2 });
  assert.deepEqual(readJsonCursorFile(path, {}), { version: 2 });
  assert.equal(statSync(path).mode & 0o777, 0o600);
  assert.throws(() => writeAtomicCursorFile(path, 1n));
  assert.deepEqual(readJsonCursorFile(path, {}), { version: 2 });
  assert.deepEqual(readdirSync(join(path, '..')), ['cursor.json']);
  writeFileSync(path, '{broken');
  assert.throws(() => readJsonCursorFile(path, 0), SyntaxError);
});

test('watermark settlement modes require nonempty confirmed receipts and preserve cursor on partial failure', async t => {
  const path = join(directory(t), 'cursor.json');
  for (const mode of ['settled', 'queued-or-settled'] as const) {
    for (const [receipts, settled, queued] of [
      [[], false, false],
      [[receipt('dispatched')], true, true],
      [[receipt('settled')], true, true],
      [[receipt('queued')], false, false],
      [[receipt('queued', { durable: true })], false, true],
      [[receipt('rejected', { durable: true, handoff: 'settled' }, DEDUP_CAS_DUPLICATE_DETAIL)], true, true],
      [[receipt('rejected', { durable: true, handoff: 'queued' }, DEDUP_CAS_DUPLICATE_DETAIL)], false, true],
      [[receipt('dispatched'), receipt('failed')], false, false],
    ] as [DeliveryReceipt[], boolean, boolean][]) {
      writeAtomicCursorFile(path, 3);
      const source = createPollWatermarkSourcePlugin({ initialCursor: 0, cursorPath: path, settlementMode: mode,
        poll: async cursor => ({ items: [cursor], cursor: 4 }), toInput: () => input });
      assert.equal(source.getCursor(), 3);
      await source.start(emitter(receipts));
      if (mode === 'settled' ? settled : queued) {
        await source.pollOnce();
        assert.equal(source.getCursor(), 4);
        assert.equal(readJsonCursorFile(path, 0), 4);
      } else {
        await assert.rejects(source.pollOnce(), /cursor retained/);
        assert.equal(source.getCursor(), 3);
        assert.equal(readJsonCursorFile(path, 0), 3);
      }
      await source.stop();
    }
  }
  assert.equal(everyReceiptSettled([], isConfirmedHandoffReceipt), false);
});

test('explicit saveCursor and advanceItemCursor retain ownership, including restart load', async t => {
  const path = join(directory(t), 'cursor.json');
  const saved: number[] = [];
  const source = createPollWatermarkSourcePlugin({ initialCursor: 0, cursorPath: path,
    poll: async cursor => ({ items: [2], cursor }), toInput: () => input,
    saveCursor: async cursor => { saved.push(cursor); }, advanceItemCursor: (_cursor, item) => item });
  writeAtomicCursorFile(path, 1);
  await source.start(emitter([]));
  assert.equal(source.getCursor(), 1);
  await source.pollOnce();
  assert.deepEqual(saved, [2]);
  assert.equal(readJsonCursorFile(path, 0), 1);
  await source.stop();
});

test('HTTP wake validates origin and method, coalesces concurrent triggers, reports errors and closes', async t => {
  const logs = t.mock.method(console, 'error', (_line: string) => {});
  let polls = 0;
  let release!: () => void;
  let fail = false;
  const errors: unknown[] = [];
  const source = createPollWatermarkSourcePlugin({ initialCursor: 0,
    wake: { allowedOrigins: ['https://allowed.example'] },
    poll: async cursor => {
      polls++;
      if (fail) throw new Error('poll failure');
      await new Promise<void>(resolve => { release = resolve; });
      return { items: [], cursor: cursor + 1 };
    }, toInput: () => input, onError: error => errors.push(error) });
  await source.start(emitter([]));
  t.after(() => source.stop());
  const address = source.wakeAddress();
  assert.ok(address && typeof address === 'object');
  assert.equal(address.address, '127.0.0.1');
  const url = `http://127.0.0.1:${address.port}/wake`;
  assert.equal((await fetch(url, { method: 'POST', headers: { origin: 'https://denied.example' } })).status, 403);
  assert.equal((await fetch(url)).status, 405);
  assert.equal((await fetch(url + '-other', { method: 'POST' })).status, 404);
  const preflight = await fetch(url, { method: 'OPTIONS', headers: { origin: 'https://allowed.example' } });
  assert.equal(preflight.status, 204);
  assert.equal(preflight.headers.get('access-control-allow-origin'), 'https://allowed.example');
  assert.equal(polls, 0);
  const response = await fetch(url, { method: 'POST' });
  assert.deepEqual(await response.json(), { ok: true });
  await fetch(url, { method: 'POST' });
  assert.equal(polls, 1);
  release();
  await source.pollOnce();
  assert.equal(source.getCursor(), 1);
  fail = true;
  await fetch(url, { method: 'POST' });
  assert.match(String(errors[0]), /poll failure/);
  assert.ok(logs.mock.calls.every(call => !String(call.arguments[0]).startsWith('[dealer:source] ')));
  await source.stop();
  assert.equal(source.wakeAddress(), null);
  await assert.rejects(fetch(url));
  await assert.rejects(source.pollOnce(), /not started/);
});

test('stopping a poll source aborts the fetch signal and drops a legacy page that ignores it', async t => {
  const path = join(directory(t), 'cursor.json');
  writeAtomicCursorFile(path, 7);
  const emitted: number[] = [];
  const saved: number[] = [];
  let signalSeen: AbortSignal | undefined;
  let release!: () => void;
  const source = createPollWatermarkSourcePlugin<number, number>({
    initialCursor: 0,
    cursorPath: path,
    poll: (_cursor, signal) => {
      signalSeen = signal;
      return new Promise(resolve => {
        release = () => resolve({ items: [1], cursor: 99 });
      });
    },
    toInput: item => ({ ...input, idempotencyKey: `late-${item}` }),
    saveCursor: async cursor => { saved.push(cursor); },
  });
  await source.start({
    emit: async () => {
      emitted.push(1);
      return { messageId: 'late', traceId: 'late', receipts: [receipt('settled')] };
    },
    registerSource() { throw new Error('unused'); },
  });
  const pending = source.pollOnce();
  assert.equal(signalSeen?.aborted, false);
  const startedAt = performance.now();
  await source.stop();
  // The read-only fetch phase holds no external effect: a poll that ignores the signal must
  // never make stop() wait for it.
  assert.ok(performance.now() - startedAt < 500, 'stop waited for the read-only poll');
  assert.equal(signalSeen!.aborted, true);
  // A page that lands after shutdown is dropped without emitting or touching the cursor.
  release();
  await pending;
  assert.deepEqual(emitted, []);
  assert.deepEqual(saved, []);
  assert.equal(source.getCursor(), 7);
  assert.equal(readJsonCursorFile(path, 0), 7);
});

test('stopping a poll source finishes an already-resolved page settlement before resolving', async t => {
  const saved: number[] = [];
  let emitStarted!: () => void;
  const started = new Promise<void>(resolve => {
    emitStarted = resolve;
  });
  let release!: () => void;
  const gate = new Promise<void>(resolve => {
    release = resolve;
  });
  const source = createPollWatermarkSourcePlugin<number, number>({
    initialCursor: 0,
    poll: async () => ({ items: [1], cursor: 1 }),
    toInput: () => input,
    saveCursor: async cursor => { saved.push(cursor); },
  });
  await source.start({
    emit: async () => {
      emitStarted();
      await gate;
      return { messageId: 'settled', traceId: 'settled', receipts: [receipt('settled')] };
    },
    registerSource() { throw new Error('unused'); },
  });
  const polled = source.pollOnce();
  await started;
  let stopResolved = false;
  const stopping = source.stop().then(() => {
    stopResolved = true;
  });
  await new Promise(resolve => setTimeout(resolve, 50));
  // Item emission and cursor persistence are already in flight: stop must await them.
  assert.equal(stopResolved, false);
  release();
  await stopping;
  await polled;
  assert.equal(stopResolved, true);
  assert.deepEqual(saved, [1]);
  assert.equal(source.getCursor(), 1);
});

const registryYaml = `team:
  - name: alice
    git_names: ['Alice Dev']
    modules: [tools]
  - name: bob
    modules: [web]
areas:
  tools:
    owner: alice
    default_recipients: [bob]
modules:
  tools: { services: [worker] }
  web: { services: [frontend] }
review_policies:
  tools: { reviewer: bob, gate: required }
`;

test('YAML data subset and Markdown frontmatter preserve nested data, quotes, arrays, scalars and body', () => {
  const parsed = parseYaml(`items:\n  - { name: 'A # B', nested: [1, true, null, { x: false }] }\ncount: 2 # comment\nlabel: "a:b"\ntext: |-\n  first\n  second\n`);
  assert.deepEqual(parsed.items, [{ name: 'A # B', nested: [1, true, null, { x: false }] }]);
  assert.equal(parsed.count, 2);
  assert.equal(parsed.label, 'a:b');
  assert.equal(parsed.text, 'first\nsecond');
  const markdown = '\uFEFF---\r\n' + registryYaml.replaceAll('\n', '\r\n') + '---\r\n# Request\n';
  assert.deepEqual(parseMarkdownFrontmatter(markdown), { frontmatter: parseYaml(registryYaml), body: '# Request\n' });
  assert.deepEqual(parseMarkdownFrontmatter('plain'), { frontmatter: {}, body: 'plain' });
  assert.throws(() => parseMarkdownFrontmatter('---\nx: 1'), /Unclosed/);
  for (const invalid of ['__proto__: {}', 'a: 1\na: 2', 'a: [one', 'a: &anchor', 'a:\n\tb: 1']) assert.throws(() => parseYaml(invalid));
});

test('registry validates shapes and resolves all ownership selectors', () => {
  const registry = parseTeamRegistry(registryYaml);
  assert.deepEqual(parseTeamRegistry(parseYaml(registryYaml)), registry);
  for (const [selector, members] of Object.entries({ 'area:tools': ['alice', 'bob'], 'module:tools': ['alice'], 'service:worker': ['alice'], 'review:tools': ['bob'], 'git:Alice Dev': ['alice'], 'member:alice': ['alice'], alice: ['alice'], 'area:unknown': [] })) {
    assert.deepEqual(resolveTeamRegistryMembers(registry, selector), members);
  }
  assert.throws(() => parseTeamRegistry({ team: [{ name: 'alice', modules: 'tools' }], modules: {} }), /Invalid/);
  assert.throws(() => resolveTeamRegistryMembers({ ...registry, areas: { wrong: { owner: 'missing' } } }, 'area:wrong'), /Unknown registry member/);
});

function card(member: string, load: 'idle' | 'busy' | 'overloaded', networkId = 'local'): NodeCard {
  const key = generateNodeKeyPair();
  return signNodeCard({ nodeId: key.nodeId, networkId, revision: 1, identity: { member, nodeName: load }, admissionMode: 'open',
    capabilities: { summary: '', skills: [] }, runtimeState: { load, activeTasks: 0, availableAgents: [] }, updatedAt: new Date().toISOString() }, key.privateKeyPem);
}

test('registry plugin expands targets and judge prefers idle nodes per member within its network', async t => {
  const root = directory(t);
  writeFileSync(join(root, 'team.yaml'), registryYaml);
  const cards = [card('alice', 'busy'), card('alice', 'idle'), card('bob', 'busy'), card('bob', 'overloaded'), card('alice', 'idle', 'other')];
  const kernel = new Kernel();
  kernel.ctx.provide('node', { getSelfCard: () => cards[0], updateSelfProfile: async () => {}, listPeers: () => cards,
    resolveTarget: (selector, network) => cards.filter(c => c.networkId === network && `member:${c.identity.member}` === selector) });
  await kernel.use(createRouterPlugin());
  await kernel.use(createTeamRegistryRouterPlugin({ projectRoot: root, registryPath: 'team.yaml', networkId: 'local' }));
  t.after(() => kernel.stop());
  for (const selector of ['area:tools', 'module:tools', 'service:worker', 'review:tools', 'git:Alice Dev']) {
    const prepared = await kernel.ctx.waterfall('emitter/prepare', envelope(selector, { routing: { mode: 'auto', targets: [selector] } }));
    assert.deepEqual(prepared.routing.targets, resolveTeamRegistryMembers(parseTeamRegistry(registryYaml), selector).map(name => `member:${name}`));
  }
  await assert.rejects(kernel.ctx.waterfall('emitter/prepare', envelope('unknown', { routing: { mode: 'auto', targets: ['area:missing'] } })), /Unknown team selector/);
  const foreign = envelope('foreign', { networkId: 'other', routing: { mode: 'auto', targets: ['area:tools'] } });
  assert.deepEqual((await kernel.ctx.waterfall('emitter/prepare', foreign)).routing.targets, ['area:tools']);
  for (const payload of [{ area: 'tools' }, { module: 'tools' }, { service: 'worker' }]) {
    const decision = await kernel.ctx.services.router.route(envelope('judge', { payload }));
    assert.deepEqual(decision.targetNodeIds, payload.area ? [cards[1].nodeId, cards[2].nodeId] : [cards[1].nodeId]);
  }
  assert.deepEqual((await kernel.ctx.services.router.route(envelope('hint', { routing: { mode: 'auto', hint: 'review:tools' } }))).targetNodeIds, [cards[2].nodeId]);

});

test('receiver resolves compound stream bindings without sender preparation and retains signed envelopes', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({ stateDir: root, member: 'receiver', profile: 'personal-dev',
    patch: { gateMode: 'open', topicBindings: {
      'wave.alert.v1': { executor: 'inbox' },
      'wave.alert.v1:tools-alert': { executor: 'orca-wave', workspacePath: '/tools', terminalHandle: 'term_tools', coalesceMs: 60000 },
      'wave.alert.v1:quality-review': { executor: 'orca-wave', workspacePath: '/quality', terminalHandle: 'term_quality', coalesceMs: 60000 },
    } }, wakeupIdleCheck: async () => ({ idle: false }),
  }).start();
  t.after(() => runtime.stop());
  for (const [streamKey, workspace] of [['tools-alert', '/tools'], ['quality-review', '/quality']]) {
    const message = envelope(streamKey, { payload: { streamKey } });
    assert.equal(runtime.resolveExecutorName(message), 'orca-wave');
    const result = await runtime.kernel.ctx.services.dispatcher.dispatch(message);
    assert.equal(result.stage, 'dispatched');
    assert.equal(runtime.mailbox.getWakeTarget(`stream:${streamKey}`)?.driverConfig?.workspacePath, workspace);
    const items = runtime.mailbox.listItemsAfterSequence(-1).filter(item => item.mailboxId === `stream:${streamKey}`);
    assert.equal(items.length, 1);
    assert.deepEqual(items[0].signedEnvelope, message);
    assert.deepEqual(items[0].envelope, { ...message, payload: { ...message.payload, localAttachments: [] } });
    assert.equal(runtime.mailbox.listThreadRecords(message.id)[0].localMailbox, `stream:${streamKey}`);
  }
  const question = envelope('question', { payload: { streamKey: 'tools-alert' }, semantic: { type: 'question', threadId: 'tools-alert', targetMailbox: 'default' } });
  await runtime.kernel.ctx.services.dispatcher.dispatch(question);
  assert.equal(runtime.mailbox.listItemsAfterSequence(-1).find(item => item.id === question.id)?.mailboxId, 'stream:tools-alert');
  const outbound = await runtime.kernel.ctx.waterfall('emitter/prepare', envelope('outbound', {
    payload: { streamKey: 'tools-alert' }, semantic: { type: 'message', threadId: 'outbound', targetMailbox: 'remote-mailbox' },
  }));
  assert.equal(outbound.semantic?.replyMailbox, 'stream:tools-alert');
  assert.equal(outbound.semantic?.targetMailbox, 'remote-mailbox');
  assert.equal(runtime.resolveExecutorName(envelope('fallback')), 'inbox');
  assert.equal(resolveBoundMailboxId('wave.alert.v1:tools-alert', toMailboxTopicBinding({ executor: 'orca-wave' })), 'stream:tools-alert');
  assert.equal(resolveBoundMailboxId('wave.alert.v1:tools-alert', toMailboxTopicBinding({ executor: 'orca-wave', streamKey: 'custom' })), 'stream:custom');
  assert.equal(resolveMailboxId(envelope('explicit', { payload: { executor: 'orca-wave' } }), toMailboxTopicBinding({ executor: 'orca-wave', streamKey: 'bound' }), undefined, toMailboxTopicBinding({ executor: 'orca-wave' }).mode), 'stream:bound');
  for (const binding of [{ executor: 'orca-spawn', noParent: 'yes' }, { executor: 'orca-spawn', assignee: '' }, { executor: 'orca-spawn', metadata: { team: '' } }, { executor: 'orca-spawn', metadata: [] }, { executor: 'orca-spawn', worktreePath: '/typo' }, { executor: 'orca', mailbox: 'stream:x' }]) {
    assert.throws(() => validateTopicBindings({ topic: binding }, ['orca-spawn', 'orca']), /Invalid topic binding/);
  }
  for (const binding of [{ executor: 'external', agent: 'unknown-host' }, { executor: 'external', mode: 'inbox-only' }]) {
    assert.throws(() => validateTopicBindings({ topic: binding }, ['external']), /Invalid topic binding/);
  }
  validateTopicBindings({ topic: { executor: 'external', agent: 'codex', mode: 'spawn' } }, ['external']);
  const annotations = { topic: { executor: 'orca-spawn', assignee: 'alice', metadata: { team: 'tools', owner: 'receiver' } } };
  const originalAnnotations = structuredClone(annotations);
  validateTopicBindings(annotations, ['orca-spawn']);
  assert.deepEqual(annotations, originalAnnotations, 'generic metadata and legacy project fields round-trip losslessly');
  assert.throws(() => validateTopicBindings({ 'topic:': { executor: 'orca-wave' } }, ['orca-wave']), /Invalid/);
});

test('receiver noParent binding chooses top-level spawn and explicit false chooses child', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'state'), profile: 'personal-dev', patch: { topicBindings: {
    claim: { executor: 'orca-spawn', noParent: true, assignee: 'alice', workspacePath: root },
  } } }).start();
  t.after(() => runtime.stop());
  const executor = createOrcaSpawnExecutorPlugin({ resolveTopicBinding: (kind, stream) => runtime.resolveTopicBinding(kind, stream)?.binding,
    commandRunner: async request => {
      calls.push(request);
      return { stdout: request.args[0] === 'worktree' ? JSON.stringify({ path: join(root, 'child') }) : '{}' };
    } });
  for (const payload of [{}, { noParent: false }]) {
    calls.length = 0;
    await executor.execute([envelope('spawn', { kind: 'claim', payload })], { networkId: 'local', nodeId: keys.nodeId, traceId: 'spawn' }, {});
    assert.equal(calls[0].args.includes('--no-parent'), !('noParent' in payload));
    assert.equal(calls[0].args.includes('--parent-worktree'), 'noParent' in payload);
  }
});


test('compound lookup prefers specificity across project and runtime layers, then project topic defaults', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'state'), profile: 'personal-dev', patch: { topicBindings: {
    topic: { executor: 'inbox' }, 'topic:runtime': { executor: 'orca-wave', streamKey: 'runtime' },
    'topic:project': { executor: 'inbox' },
  } } }).start();
  t.after(() => runtime.stop());
  const project = join(root, 'project');
  initProjectScaffold(project, { projectId: 'lookup', defaultNetworkId: 'local' });
  const patch = join(root, 'patch.json');
  writeFileSync(patch, JSON.stringify({ topicBindings: {
    topic: { executor: 'orca-wave', streamKey: 'general' },
    'topic:project': { executor: 'orca-wave', streamKey: 'project' },
  } }));
  await runtime.linkProject(project, patch);
  assert.equal(runtime.resolveTopicBinding('topic', 'runtime')?.binding.streamKey, 'runtime');
  assert.equal(runtime.resolveTopicBinding('topic', 'project')?.binding.streamKey, 'project');
  assert.equal(runtime.resolveTopicBinding('topic', 'unknown')?.binding.streamKey, 'general');
  const prepared = await runtime.kernel.ctx.waterfall('emitter/prepare', envelope('reply', { kind: 'topic', payload: { streamKey: 'runtime' } }));
  assert.equal(prepared.semantic?.replyMailbox, 'stream:runtime');
});

test('portable sample manifest loads every plugin and formats a real request attachment', async t => {
  const root = directory(t);
  const project = join(root, 'project');
  cpSync(fileURLToPath(new URL('../examples/project-scaffold', import.meta.url)), project, { recursive: true });
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'state'), profile: 'personal-dev' }).start();
  t.after(() => runtime.stop());
  const linked = await runtime.linkProject(project);
  assert.equal(linked.loadedPlugins.length, 4);
  const personal = JSON.parse(readFileSync(new URL('../examples/user-patches/personal.patch.json', import.meta.url), 'utf8'));
  validateTopicBindings(personal.topicBindings, runtime.config.executors);
  writeFileSync(join(project, 'request.md'), '---\nschema: collaboration-request/v4\nrequest_id: REQ-1\nto: [alice]\narea: tools\n---\nPlease review.\n');
  const prepared = await runtime.kernel.ctx.waterfall('emitter/prepare', envelope('request', { kind: 'collab.request.v4', payload: { requestFile: 'request.md' } }));
  assert.equal(prepared.payload.requestId, 'REQ-1');
  assert.equal(prepared.body, 'Please review.\n');
  assert.deepEqual(prepared.routing.targets, ['member:alice']);
  assert.equal(prepared.attachments.length, 1);
  assert.ok(prepared.attachments[0].blake3Hash);
});

test('stopping while HTTP wake starts leaves no listening server or scheduled poll', async () => {
  let polls = 0;
  const source = createPollWatermarkSourcePlugin({ initialCursor: 0, wake: {}, intervalMs: 60000,
    poll: async cursor => {
      polls++;
      return { items: [], cursor };
    }, toInput: () => input });
  const started = source.start(emitter([]));
  await source.stop();
  await started;
  assert.equal(source.wakeAddress(), null);
  assert.equal(polls, 0);
});


test('user plugin CLI add, list and remove unload hooks and persist removal across restart', async t => {
  const root = directory(t);
  writeFileSync(join(root, 'personal.ts'), `export default {
    name: 'personal-hook-runtime',
    apply(ctx) {
      ctx.on('emitter/prepare', message => ({ ...message, subject: 'personal hook' }));
    },
  };`);
  let runtime = await new DealerNodeRuntime({ stateDir: root, profile: 'personal-dev' }).start();
  t.after(() => runtime.stop());
  const cli = async (...args: string[]) => {
    const output: string[] = [];
    const errors: string[] = [];
    assert.equal(await runDealerCli(['plugin', ...args, '--json'], {
      runtime, stdout: line => output.push(line), stderr: line => errors.push(line),
    }), 0, errors.join('\n'));
    return JSON.parse(output.join('\n'));
  };
  assert.deepEqual(await cli('list'), []);
  assert.deepEqual(await cli('add', '--name', 'personal', '--module', 'personal.ts'), { added: 'personal' });
  assert.deepEqual(await cli('list'), [{ name: 'personal', module: 'personal.ts', config: {}, loadedName: 'personal-hook-runtime' }]);
  assert.equal((await runtime.kernel.ctx.waterfall('emitter/prepare', envelope('before'))).subject, 'personal hook');
  assert.deepEqual(await cli('remove', 'personal'), { removed: 'personal' });
  assert.deepEqual(await cli('list'), []);
  assert.equal(runtime.kernel.enabledPluginNames().includes('personal-hook-runtime'), false);
  assert.equal((await runtime.kernel.ctx.waterfall('emitter/prepare', envelope('after'))).subject, 'after');
  assert.deepEqual(JSON.parse(readFileSync(join(root, 'config.json'), 'utf8')).userPlugins, []);
  await assert.rejects(runtime.removeUserPlugin('missing'), /User plugin not installed: missing/);
  await runtime.stop();
  runtime = await new DealerNodeRuntime({ stateDir: root }).start();
  assert.deepEqual(await cli('list'), []);
  assert.equal((await runtime.kernel.ctx.waterfall('emitter/prepare', envelope('restarted'))).subject, 'restarted');
  await cli('add', '--name', 'personal', '--module', 'personal.ts');
  assert.deepEqual(await cli('remove', '--name', 'personal'), { removed: 'personal' });
});

test('daemon drain waits for in-flight control work, wakes on its completion, then fences new requests', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'state'), profile: 'headless-relay' }).start();
  t.after(() => runtime.stop());
  let release!: () => void;
  const gate = new Promise<void>(resolve => {
    release = resolve;
  });
  let admitted!: () => void;
  const holding = new Promise<void>(resolve => {
    admitted = resolve;
  });
  await runtime.serveControl(async argv => {
    if (argv[0] === 'hold') {
      admitted();
      await gate;
    }
    return { code: 0, stdout: ['done'], stderr: [] };
  });
  const held = rpc(runtime.socketPath, ['hold']);
  await holding;
  // A timed-out drain leaves the daemon fully serving, with control.sock unfenced.
  assert.equal(await runtime.drainForStop({ drainTimeoutMs: 100 }), false);
  assert.deepEqual((await rpc(runtime.socketPath, ['probe'])).stdout, ['done']);
  // The wait is event-driven: control completion wakes it without polling.
  let drained = false;
  const draining = runtime.drainForStop({ drainTimeoutMs: 5_000 }).then(value => {
    drained = value;
    return value;
  });
  await new Promise(resolve => setTimeout(resolve, 50));
  assert.equal(drained, false);
  // An undecided drain never fences the socket: in-flight dispatches keep settling through it.
  assert.deepEqual((await rpc(runtime.socketPath, ['probe'])).stdout, ['done']);
  release();
  assert.equal(await draining, true);
  assert.deepEqual((await held).stdout, ['done']);
  // A completed drain commits the daemon to stopping: no later request is admitted.
  await assert.rejects(rpc(runtime.socketPath, ['probe']), /Daemon is draining/);
});

test('concurrent shutdown callers share one drain decision and one teardown', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'state'), profile: 'headless-relay' }).start();
  t.after(() => runtime.stop());
  let admitted = 0;
  let release!: () => void;
  const gate = new Promise<void>(resolve => {
    release = resolve;
  });
  let bothAdmitted!: () => void;
  const holding = new Promise<void>(resolve => {
    bothAdmitted = resolve;
  });
  await runtime.serveControl(async argv => {
    if (argv[0] !== 'shutdown') {
      return { code: 0, stdout: ['done'], stderr: [] };
    }
    if (++admitted === 2) {
      bothAdmitted();
    }
    await gate;
    return { code: 0, stdout: [String(await runtime.drainForStop({ callerControlRequests: 1 }))], stderr: [] };
  });
  let teardowns = 0;
  const wakeups = runtime.wakeups;
  const stopWakeups = wakeups.stop.bind(wakeups);
  wakeups.stop = async () => {
    teardowns += 1;
    await stopWakeups();
  };
  const first = rpc(runtime.socketPath, ['shutdown']);
  const second = rpc(runtime.socketPath, ['shutdown']);
  await holding;
  const began = performance.now();
  release();
  const outcomes = await Promise.all([first, second]);
  // Each shutdown caller counts in the allowance: neither may wait on the other, and both
  // observe the one shared decision.
  assert.deepEqual(outcomes.map(result => result.stdout[0]), ['true', 'true']);
  assert.ok(performance.now() - began < 2_000, 'concurrent shutdown callers waited on each other');
  // The committed daemon fences new requests and shares one teardown among stop callers.
  await assert.rejects(rpc(runtime.socketPath, ['probe']), /Daemon is draining/);
  await Promise.all([runtime.stop(), runtime.stop()]);
  assert.equal(teardowns, 1);
});

test('drain waiters holding no control slot never widen the allowance', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'state'), profile: 'headless-relay' }).start();
  t.after(() => runtime.stop());
  let releaseProbe!: () => void;
  const probeGate = new Promise<void>(resolve => {
    releaseProbe = resolve;
  });
  let probeAdmitted!: () => void;
  const probeHeld = new Promise<void>(resolve => {
    probeAdmitted = resolve;
  });
  await runtime.serveControl(async argv => {
    if (argv[0] === 'status') {
      return { code: 0, stdout: ['ok'], stderr: [] };
    }
    probeAdmitted();
    await probeGate;
    return { code: 0, stdout: ['probed'], stderr: [] };
  });
  const probe = rpc(runtime.socketPath, ['probe']);
  await probeHeld;
  // Two gracefulStop() callers hold no control slot of their own, so they tolerate no other
  // in-flight control work: the unrelated `probe` request must defer the shared decision
  // instead of being torn down under.
  const settled = await Promise.allSettled([
    runtime.gracefulStop({ drainTimeoutMs: 200 }),
    runtime.gracefulStop({ drainTimeoutMs: 200 }),
  ]);
  assert.deepEqual(settled.map(result => result.status), ['rejected', 'rejected']);
  for (const result of settled) {
    assert.match(String(result.status === 'rejected' ? result.reason : ''), /timed out/);
  }
  // A deferred drain leaves the daemon fully serving, including the request it waited for.
  assert.deepEqual((await rpc(runtime.socketPath, ['status'])).stdout, ['ok']);
  releaseProbe();
  assert.deepEqual((await probe).stdout, ['probed']);
  assert.equal(await runtime.drainForStop({ drainTimeoutMs: 2_000 }), true);
});

test('watermark scheduling backs off failures and resets on success and both wake paths', async t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval'] });
  const errors: unknown[] = [];
  const statuses: PollSourceStatusEvent[] = [];
  let polls = 0;
  let fail = true;
  const source = createPollWatermarkSourcePlugin({
    name: ' alerts ', initialCursor: 0, wake: {}, intervalMs: 10, maxBackoffMs: 50,
    poll: async cursor => {
      polls++;
      if (fail) throw new Error('poll failed');
      return { items: [], cursor };
    },
    toInput: () => input,
    onError: error => errors.push(error),
    onPollStatus: event => statuses.push(event),
  });
  t.after(() => source.stop());
  const flush = () => new Promise<void>(resolve => setImmediate(resolve));
  const tick = async (ms: number) => { t.mock.timers.tick(ms); await flush(); };
  assert.equal(source.name, 'alerts');
  assert.throws(() => source.wake(), /Poll source not started/);
  await source.start(emitter([]));
  await flush();
  assert.equal(polls, 1);
  await tick(19);
  assert.equal(polls, 1);
  await tick(1);
  assert.equal(polls, 2);
  await tick(39);
  assert.equal(polls, 2);
  await tick(1);
  assert.equal(polls, 3);
  await tick(49);
  assert.equal(polls, 3);
  fail = false;
  await tick(1);
  assert.equal(polls, 4);
  fail = true;
  await tick(9);
  assert.equal(polls, 4);
  await tick(1);
  assert.equal(polls, 5);
  await tick(20);
  assert.equal(polls, 6);

  const address = source.wakeAddress();
  assert.ok(address && typeof address === 'object');
  // Use node:http so the mock clock controls only the source's poll scheduler.
  const { request } = await import('node:http');
  await new Promise<void>((resolve, reject) => {
    const req = request({ host: '127.0.0.1', port: address.port, path: '/wake', method: 'POST' }, response => {
      assert.equal(response.statusCode, 200);
      response.resume();
      response.on('end', resolve);
    });
    req.on('error', reject);
    req.end();
  });
  await flush();
  assert.equal(polls, 7);
  await tick(19);
  assert.equal(polls, 7);
  await tick(1);
  assert.equal(polls, 8);
  assert.equal(errors.length, 7);
  source.wake();
  await flush();
  assert.equal(polls, 9);
  await tick(19);
  assert.equal(polls, 9);
  await tick(1);
  assert.equal(polls, 10);
  assert.equal(errors.length, 9);
  assert.deepEqual(statuses.slice(0, 4), [
    { source: 'alerts', state: 'failed', failures: 1, nextDelayMs: 20, error: 'poll failed' },
    { source: 'alerts', state: 'backoff', failures: 2, nextDelayMs: 40, error: 'poll failed' },
    { source: 'alerts', state: 'backoff', failures: 3, nextDelayMs: 50, error: 'poll failed' },
    { source: 'alerts', state: 'recovered', failures: 3 },
  ]);
  await source.stop();
  assert.throws(() => source.wake(), /Poll source not started/);
  await tick(100);
  assert.equal(polls, 10);
});

test('poll logs single-line failure, backoff and recovery edges and suppresses repeated capped failures', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const log = t.mock.method(console, 'error', (_line: string) => {});
  const error = Object.assign(new Error('command failed\n    at poll (source.ts:1:1)'), {
    stderr: '\n  upstream HTTP 503\r\n    at poll (source.ts:1:1)\n',
  });
  let failure: Error | undefined = error;
  const source = createPollWatermarkSourcePlugin({
    initialCursor: 0, intervalMs: 10, maxBackoffMs: 40,
    poll: async cursor => {
      if (failure) throw failure;
      return { items: [], cursor };
    },
    toInput: () => input,
  });
  t.after(() => source.stop());
  const flush = () => new Promise<void>(resolve => setImmediate(resolve));
  const tick = async (ms: number) => { t.mock.timers.tick(ms); await flush(); };
  await source.start(emitter([]));
  await flush();
  await tick(20);
  await tick(40);
  assert.equal(log.mock.callCount(), 2);
  failure = new Error('upstream HTTP 429\n    at poll (source.ts:1:1)');
  await tick(40);
  failure = undefined;
  await source.pollOnce();

  const prefix = '[dealer:source] ';
  const events = log.mock.calls.map(({ arguments: args }) => {
    assert.equal(args.length, 1);
    assert.ok(args[0].startsWith(prefix));
    assert.doesNotMatch(args[0], /[\r\n]|source\.ts/);
    return JSON.parse(args[0].slice(prefix.length));
  });
  assert.deepEqual(events, [
    { source: 'poll-watermark', state: 'failed', failures: 1, nextDelayMs: 20, error: 'upstream HTTP 503' },
    { source: 'poll-watermark', state: 'backoff', failures: 2, nextDelayMs: 40, error: 'upstream HTTP 503' },
    { source: 'poll-watermark', state: 'failed', failures: 4, nextDelayMs: 40, error: 'upstream HTTP 429' },
    { source: 'poll-watermark', state: 'recovered', failures: 4 },
  ]);
  assert.equal(publicApi.formatSingleLineError(error, 8), 'upstream');
});

test('source CLI discovers registered sources and wakes matching sources over control.sock', { timeout: 10_000 }, async t => {
  const stateDir = join(directory(t), 'state');
  const invoke = async (args: string[], runtime?: DealerNodeRuntime) => {
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(args, {
      stateDir, runtime, stdout: line => stdout.push(line), stderr: line => stderr.push(line),
    });
    return { code, stdout, stderr };
  };
  const command = (args: string[]) => invoke(['source', ...args, '--json']);
  for (const [args, message] of [
    [['list'], 'Start dealer node up before listing live sources'],
    [['wake', 'alerts'], 'Start dealer node up before waking a live source'],
  ] as const) {
    const offline = await command([...args]);
    assert.equal(offline.code, 1);
    assert.deepEqual(offline.stderr.map(line => JSON.parse(line)), [{ error: message }]);
    assert.equal(existsSync(stateDir), false);
  }

  const runtime = await new DealerNodeRuntime({ stateDir, profile: 'headless-relay' }).start();
  t.after(() => runtime.stop());
  await runtime.serveControl(args => invoke(args, runtime));
  const passive = runtime.emitter.registerSource({ name: 'passive', start() {}, stop() {} });
  const builtIn = await runtime.installSource({ type: 'poll-watermark', config: {
    name: 'alerts', url: 'data:application/json,{"items":[],"cursor":null}', intervalMs: 0,
  } });
  let polls = 0;
  const source = createPollWatermarkSourcePlugin({
    name: 'alerts', initialCursor: 0,
    poll: async (_cursor, signal) => {
      polls++;
      // The RPC must return even though the poll remains in flight.
      await new Promise<void>(resolve => signal.addEventListener('abort', () => resolve(), { once: true }));
      return { items: [], cursor: 1 };
    },
    toInput: () => input,
  });
  const registration = runtime.emitter.registerSource(source);
  await registration.started;

  const listed = await command(['list']);
  assert.equal(listed.code, 0);
  assert.deepEqual(JSON.parse(listed.stdout[0]), { sources: [
    { name: 'passive', wakeable: false },
    { name: 'alerts', wakeable: true },
    { name: 'alerts', wakeable: true },
  ] });
  const woken = await command(['wake', 'alerts']);
  assert.equal(woken.code, 0);
  assert.deepEqual(JSON.parse(woken.stdout[0]), { ok: true, name: 'alerts', woken: true });
  assert.equal(polls, 1);
  assert.equal((await command(['wake', 'alerts'])).code, 0);
  assert.equal(polls, 1);
  for (const [args, message] of [
    [['wake', 'passive'], 'Source does not support wake: passive'],
    [['wake', 'missing'], 'Unknown source: missing'],
    [['wake', ' '], 'Invalid source name'],
    [['wake'], 'Required: <name>'],
  ] as const) {
    const rejected = await command([...args]);
    assert.equal(rejected.code, 1);
    assert.deepEqual(rejected.stderr.map(line => JSON.parse(line)), [{ error: message }]);
  }
  assert.equal(readFileSync(join(stateDir, 'cursor-alerts.json'), 'utf8').trim(), 'null');
  await passive.dispose();
  await builtIn.dispose();
  await registration.dispose();
  assert.deepEqual(runtime.listSources(), []);
});
