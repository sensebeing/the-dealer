import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, realpath, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Kernel } from '../packages/kernel/index.ts';
import { generateNodeKeyPair, signEnvelope, signNodeCard, verifyEnvelope, verifyReceipt } from '../packages/core-types/index.ts';
import type { DeliveryReceipt, EmitMessageInput, MessageEnvelope, NodeCard, NodeService, TransportService, ExecutorPlugin } from '../packages/core-types/index.ts';
import { createRouterPlugin } from '../packages/router/index.ts';
import { createGatePlugin } from '../packages/gate/index.ts';
import type { NetworkGateService } from '../packages/gate/index.ts';
import { createDispatcherPlugin } from '../packages/dispatcher/index.ts';
import { createEmitterPlugin } from '../packages/emitter/index.ts';
import { createLocalAgentRouterPlugin, createLlmApiRouterPlugin } from '../packages/plugins-router/index.ts';
import { createDedupCasPolicyPlugin, createWaveCoalescerPolicyPlugin } from '../packages/plugins-policy/index.ts';
import { createOrcaSpawnExecutorPlugin, createOrcaWaveExecutorPlugin, createAgentPtyExecutorPlugin, createNotifyExecutorPlugin, trustAntigravityWorkspace, checkedCommand, runCommand } from '../packages/plugins-executor/index.ts';
import type { CommandRequest, CommandRunner } from '../packages/plugins-executor/index.ts';
import { createWebhookSourcePlugin, createPollWatermarkSourcePlugin, createScriptSourcePlugin } from '../packages/plugins-source/index.ts';

const sender = generateNodeKeyPair(), receiver = generateNodeKeyPair();
function card(keys = receiver, patch: Partial<NodeCard> = {}): NodeCard {
  return signNodeCard({ nodeId: keys.nodeId, networkId: 'test', revision: 1, identity: { member: 'ben', nodeName: 'worker' }, admissionMode: 'open', capabilities: { summary: 'Worker', skills: [{ id: 'ios', description: 'iOS debugging', tags: ['swift'], acceptedTypes: ['alert.crash'] }] }, runtimeState: { load: 'idle', activeTasks: 0, availableAgents: ['codex'] }, updatedAt: new Date().toISOString(), ...patch }, keys.privateKeyPem);
}
let counter = 0;
function envelope(patch: Partial<MessageEnvelope> = {}): MessageEnvelope {
  return signEnvelope({ id: `message-${++counter}`, traceId: 'trace', networkId: 'test', from: { nodeId: sender.nodeId, member: 'sender', nodeName: 'laptop' }, routing: { mode: 'auto' }, kind: 'task', subject: 'Investigate', body: 'Details', payload: {}, attachments: [], priority: 'normal', createdAt: new Date().toISOString(), ...patch }, sender.privateKeyPem);
}
function node(keys = receiver, peers: NodeCard[] = [card()]): NodeService {
  return { getSelfCard: networkId => card(keys, { networkId }), async updateSelfProfile() {}, listPeers: ({ networkId } = {}) => peers.filter(c => !networkId || c.networkId === networkId), resolveTarget: (selector, networkId) => peers.filter(c => c.networkId === networkId && (c.nodeId === selector || `member:${c.identity.member}` === selector || `${c.identity.member}@${c.identity.nodeName}` === selector)) };
}
async function setup(t: TestContext, options: { mode?: 'open' | 'dnd' | 'closed' | 'approval_required'; ledgerPath?: string } = {}) {
  const kernel = new Kernel();
  kernel.ctx.provide('node', node());
  await kernel.use(createGatePlugin({ mode: options.mode ?? 'open' }));
  await kernel.use(createDispatcherPlugin({ privateKeyPem: receiver.privateKeyPem, ledgerPath: options.ledgerPath }));
  const batches: readonly MessageEnvelope[][] = [];
  const calls = batches as MessageEnvelope[][];
  const executor: ExecutorPlugin = { name: 'capture', async execute(batch) { calls.push([...batch]); return { executor: 'capture', executionId: `execution-${calls.length}` }; } };
  kernel.ctx.services.dispatcher.registerExecutor(executor, {});
  t.after(() => kernel.stop());
  return { kernel, calls, executor, gate: kernel.ctx.services.gate as NetworkGateService, dispatcher: kernel.ctx.services.dispatcher };
}

test('router explicit selectors, broadcast, capability tags/types and idle preference', async t => {
  const busy = card(generateNodeKeyPair(), { runtimeState: { load: 'busy', activeTasks: 1, availableAgents: [] } });
  const kernel = new Kernel(); kernel.ctx.provide('node', node(receiver, [card(), busy])); await kernel.use(createRouterPlugin()); t.after(() => kernel.stop());
  const router = kernel.ctx.services.router;
  assert.deepEqual((await router.route(envelope({ routing: { mode: 'auto', targets: [receiver.nodeId] } }))).targetNodeIds, [receiver.nodeId]);
  assert.equal((await router.route(envelope({ routing: { mode: 'auto', targets: ['member:ben'] } }))).mode, 'multicast');
  assert.equal((await router.route(envelope({ routing: { mode: 'auto', targets: ['ben@worker'] } }))).matchedBy, 'explicit');
  assert.equal((await router.route(envelope({ routing: { mode: 'broadcast' } }))).mode, 'broadcast');
  for (const message of [envelope({ routing: { mode: 'auto', targets: ['skill:swift'] } }), envelope({ payload: { area: 'ios' } }), envelope({ kind: 'alert.crash' })]) assert.deepEqual((await router.route(message)).targetNodeIds, [receiver.nodeId]);
  await assert.rejects(router.route(envelope({ routing: { mode: 'auto', targets: ['missing'] } })), /Unresolved/);
  await assert.rejects(router.route(envelope()), /No route/);
});

test('custom judges run by priority and official local/API judges validate candidate IDs', async t => {
  const kernel = new Kernel(); kernel.ctx.provide('node', node()); await kernel.use(createRouterPlugin()); t.after(() => kernel.stop());
  const seen: string[] = [];
  const local = createLocalAgentRouterPlugin({ priority: 10, judgeRunner: async prompt => { seen.push(prompt); return JSON.stringify({ mode: 'unicast', targetNodeIds: [receiver.nodeId], reason: 'skills' }); } });
  kernel.ctx.services.router.registerJudge({ name: 'null', priority: 20, async decide() { seen.push('first'); return null; } });
  const registration = kernel.ctx.services.router.registerJudge(local);
  assert.equal((await kernel.ctx.services.router.route(envelope())).matchedBy, 'local-agent-router');
  assert.equal(seen[0], 'first'); assert.match(seen[1], /iOS debugging/); assert.match(seen[1], /admissionMode/);
  registration.dispose();
  for (const provider of ['openai', 'anthropic', 'deepseek'] as const) {
    const api = createLlmApiRouterPlugin({ endpoint: 'https://example.test/chat', model: 'test', provider, fetchFn: async (_url, init) => {
      assert.match(String(init?.body), /Investigate/);
      const content = JSON.stringify({ mode: 'unicast', targetNodeIds: [receiver.nodeId], reason: 'match' });
      return Response.json(provider === 'anthropic' ? { content: [{ type: 'text', text: content }] } : { choices: [{ message: { content } }] });
    } });
    assert.equal((await api.decide(envelope(), [card()]))?.matchedBy, 'llm-api-router');
  }
  const bad = createLocalAgentRouterPlugin({ judgeRunner: async () => '{"mode":"unicast","targetNodeIds":["unknown"],"reason":"x"}' });
  await assert.rejects(bad.decide(envelope(), [card()]), /unknown/);
});

test('gate four modes, trusted approvals, DND drains, overrides and rejection history', async t => {
  const { gate, calls } = await setup(t, { mode: 'approval_required' });
  const message = envelope(); const verdict = await gate.evaluate(message); assert.equal(verdict.status, 'pending_approval');
  assert.equal((await gate.evaluate(message)).status, 'pending_approval'); assert.equal(gate.listPendingApprovals().length, 1);
  if (verdict.status !== 'pending_approval') throw new Error('Expected pending');
  await gate.approve(verdict.approvalId, { trustSender: true }); assert.equal(calls.length, 1);
  assert.equal((await gate.evaluate(envelope())).status, 'allow');
  assert.equal((await gate.evaluate(envelope({ networkId: 'other' }))).status, 'pending_approval');
  await gate.setMode('dnd');
  const stranger = envelope({ from: { nodeId: receiver.nodeId, member: 'stranger', nodeName: 'other' } });
  assert.equal((await gate.evaluate(stranger)).status, 'queued_dnd');
  await gate.setMode('open'); assert.equal(calls.length, 2);
  await gate.setNetworkMode('test', 'closed');
  assert.equal((await gate.evaluate(stranger)).status, 'reject'); assert.equal(gate.listRejections().length, 1);
  await gate.setNetworkMode('test', 'dnd'); await gate.evaluate(envelope({ ...stranger, id: 'dnd-2' })); await gate.drainDnd(); assert.equal(calls.length, 3);
  await gate.setNetworkMode('test', 'approval_required');
  const v = await gate.evaluate(envelope({ ...stranger, id: 'pending-reject' })); if (v.status !== 'pending_approval') throw new Error('Expected pending');
  await gate.reject(v.approvalId, 'no'); assert.equal(gate.listRejections().at(-1)?.reason, 'no');
  await assert.rejects(gate.approve(v.approvalId), /Unknown/);
});

test('gate hook and priority rules apply queue state and override trust', async t => {
  const { gate, kernel } = await setup(t);
  gate.registerRule({ name: 'low', priority: 1, async evaluate() { return { status: 'allow' }; } });
  gate.registerRule({ name: 'high', priority: 100, async evaluate() { return { status: 'pending_approval', approvalId: 'manual', reason: 'review' }; } });
  assert.equal((await gate.evaluate(envelope())).status, 'pending_approval'); assert.equal(gate.listPendingApprovals().length, 1);
  kernel.ctx.on('gate/evaluate', async () => ({ status: 'reject', code: 'blocked', reason: 'blocked' }));
  assert.equal((await gate.evaluate(envelope())).status, 'reject');
});

test('CAS dedup serializes concurrent equal versions, rejects stale and permits newer', async () => {
  const policy = createDedupCasPolicyPlugin(); let executions = 0;
  const next = async () => { executions++; await Promise.resolve(); return { stage: 'dispatched' as const }; };
  const message = envelope({ idempotencyKey: 'entity', payload: { claimVersion: 2 } });
  const results = await Promise.all([policy.process(message, next), policy.process(envelope({ ...message, id: 'different' }), next)]);
  assert.equal(executions, 1); assert.equal(results[1].stage, 'rejected');
  assert.equal((await policy.process(envelope({ ...message, payload: { revision: 1 } }), next)).stage, 'rejected');
  assert.equal((await policy.process(envelope({ ...message, payload: { claimVersion: 3 } }), next)).stage, 'dispatched');
  assert.equal(executions, 2);
  assert.equal((await policy.process(envelope({ payload: { claimVersion: 'bad' } }), next)).stage, 'rejected');
});

test('wave coalescer buffers busy alerts, batches idle wave, rotates by count and age', async t => {
  const { dispatcher, executor, calls } = await setup(t);
  let idle = false, now = 0; const rotations: string[] = [];
  executor.checkIdle = async () => ({ idle });
  const wave = createWaveCoalescerPolicyPlugin({ executor, executorConfig: {}, streamKey: () => 'alerts', maxWaves: 1, minAgeMs: 100, now: () => now, rotateSession: async command => { rotations.push(command); } });
  dispatcher.registerPolicy(createDedupCasPolicyPlugin()); dispatcher.registerPolicy(wave);
  assert.equal((await dispatcher.dispatch(envelope())).stage, 'queued');
  assert.equal((await dispatcher.dispatch(envelope())).stage, 'queued');
  assert.equal(wave.pendingCount('alerts'), 2); assert.equal(calls.length, 0);
  assert.equal((await wave.flushWave('alerts')).stage, 'queued');
  idle = true; await wave.flushWave('alerts'); assert.equal(calls[0].length, 2);
  await dispatcher.dispatch(envelope()); assert.deepEqual(rotations, ['/new']);
  now = 200; await dispatcher.dispatch(envelope()); assert.deepEqual(rotations, ['/new', '/new']);
  assert.equal(wave.pendingCount('alerts'), 0);
});

test('dispatcher ledger survives restart and concurrent duplicate does not execute twice', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-ledger-')); t.after(() => rm(dir, { recursive: true, force: true }));
  const ledgerPath = join(dir, 'state.json');
  const first = await setup(t, { ledgerPath }); const message = envelope();
  await Promise.all([first.dispatcher.dispatch(message), first.dispatcher.dispatch(message)]); assert.equal(first.calls.length, 1);
  const second = await setup(t, { ledgerPath }); assert.equal((await second.dispatcher.dispatch(message)).stage, 'dispatched'); assert.equal(second.calls.length, 0);
  assert.match(await readFile(ledgerPath, 'utf8'), /dispatched/);
});

async function connected(t: TestContext) {
  const target = await setup(t); const origin = new Kernel();
  origin.ctx.provide('node', node(sender));
  const sent: MessageEnvelope[] = [];
  const deliver = async (networkId: string, bytes: Uint8Array) => {
    const payload = JSON.parse(Buffer.from(bytes).toString()); sent.push(payload.envelope);
    const result = await target.kernel.ctx.bail('transport/inbound', { networkId, senderNodeId: sender.nodeId, mode: 'unicast', payload });
    assert.ok(result); return result;
  };
  const transport: TransportService = {
    localNodeId: sender.nodeId, async mountNetwork() { throw new Error('unused'); }, async unmountNetwork() {}, listMountedNetworks: () => ['test'],
    sendUnicast: (networkId, _id, bytes) => deliver(networkId, bytes),
    async sendMulticast(networkId, ids, bytes) { return new Map(await Promise.all(ids.map(async id => [id, { ok: true as const, value: await deliver(networkId, bytes) }] as const))); },
    async broadcast(networkId, _topic, bytes) { await deliver(networkId, bytes); },
    async shareFile(path) { return { name: path, mimeType: 'text/plain', sizeBytes: 1, blake3Hash: 'hash', ticket: 'ticket' }; }, async fetchBlobToPath() {},
  };
  origin.ctx.provide('transport', transport); await origin.use(createRouterPlugin()); await origin.use(createEmitterPlugin({ privateKeyPem: sender.privateKeyPem })); t.after(() => origin.stop());
  const input: EmitMessageInput = { networkId: 'test', routing: { mode: 'unicast', targets: [receiver.nodeId] }, kind: 'task', subject: 'Task', body: 'Do it', payload: {} };
  return { ...target, origin, sent, input, emitter: origin.ctx.services.emitter };
}

test('emitter -> authenticated inbound -> gate -> dispatcher yields signed receipts and prepares signed attachments', async t => {
  const { emitter, origin, sent, input, calls, gate, kernel } = await connected(t);
  origin.ctx.on('emitter/prepare', (message, next) => next({ ...message, body: 'Prepared' }));
  const result = await emitter.emit({ ...input, filePaths: ['report.txt'] });
  assert.equal(result.receipts[0].stage, 'dispatched'); assert.ok(verifyReceipt(result.receipts[0])); assert.ok(verifyEnvelope(sent[0])); assert.equal(calls[0][0].body, 'Prepared'); assert.equal(sent[0].attachments.length, 1);
  for (const [mode, stage] of [['approval_required', 'pending_approval'], ['dnd', 'queued'], ['closed', 'rejected']] as const) { await gate.setMode(mode); assert.equal((await emitter.emit(input)).receipts[0].stage, stage); }
  const invalid = envelope(); invalid.body = 'tampered';
  const response = await kernel.ctx.bail('transport/inbound', { networkId: 'test', senderNodeId: sender.nodeId, mode: 'unicast', payload: { type: 'message.envelope', envelope: invalid } });
  assert.equal(JSON.parse(Buffer.from(response!).toString()).stage, 'rejected');
  const mismatch = await kernel.ctx.bail('transport/inbound', { networkId: 'other', senderNodeId: sender.nodeId, mode: 'unicast', payload: { type: 'message.envelope', envelope: envelope() } });
  assert.equal(JSON.parse(Buffer.from(mismatch!).toString()).stage, 'rejected');
});

test('all official executors deliver via Emitter using injected commands and notify inbox', async t => {
  const { emitter, dispatcher, input } = await connected(t); const requests: CommandRequest[] = [];
  const commandRunner: CommandRunner = async request => { requests.push(request); return { stdout: request.args[0] === 'worktree' && request.args[1] === 'create' ? JSON.stringify({ ok: true, result: { worktree: { path: '/tmp/isolated' } } }) : '{"ok":true}' }; };
  const dir = await mkdtemp(join(tmpdir(), 'dealer-inbox-')); t.after(() => rm(dir, { recursive: true, force: true }));
  for (const executor of [createOrcaSpawnExecutorPlugin({ commandRunner }), createOrcaWaveExecutorPlugin({ runId: 'run-test', commandRunner, idleCheck: async () => ({ idle: true }) }), createAgentPtyExecutorPlugin({ commandRunner }), createNotifyExecutorPlugin({ inboxPath: join(dir, 'inbox.jsonl') })]) {
    dispatcher.registerExecutor(executor, {});
    const result = await emitter.emit({ ...input, payload: { executor: executor.name } }); assert.equal(result.receipts[0].stage, 'dispatched'); assert.equal(result.receipts[0].executionReceipt?.executor, executor.name);
  }
  assert.ok(requests.some(r => r.args.includes('--parent-worktree')));
  assert.ok(requests.some(r => r.args.includes('run:run-test')));
  assert.ok(requests.some(r => r.args.includes('--comment')));
  assert.match(await readFile(join(dir, 'inbox.jsonl'), 'utf8'), /Task/);
});

test('orca-spawn honors receiver parent worktrees, explicit names and sea-creature naming', async () => {
  const requests: CommandRequest[] = [];
  const commandRunner: CommandRunner = async request => {
    requests.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'list') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: { worktrees: [{ path: '/tmp/Nautilus' }, { displayName: 'Seahorse' }] },
        }),
      };
    }
    if (request.args[0] === 'worktree' && request.args[1] === 'create') {
      return { stdout: JSON.stringify({ ok: true, result: { worktree: { path: '/tmp/child' } } }) };
    }
    return { stdout: '{"ok":true}' };
  };
  const executor = createOrcaSpawnExecutorPlugin({
    commandRunner,
    command: 'codex',
    cwd: '/workspace',
    seaCreatureNaming: true,
  });
  const context = { networkId: 'test', nodeId: receiver.nodeId, traceId: 'trace' };
  const createdArgs = () =>
    requests.find(r => r.args[0] === 'worktree' && r.args[1] === 'create')!.args;
  const flagValue = (args: readonly string[], flag: string) => args[args.indexOf(flag) + 1];

  await executor.execute(
    [envelope({ payload: { worktreeName: ' explicit ' } })],
    context,
    { parentWorktree: 'path:/ephemeral' },
  );
  assert.equal(flagValue(createdArgs(), '--parent-worktree'), 'path:/ephemeral');
  assert.equal(flagValue(createdArgs(), '--name'), 'explicit');
  assert.equal(requests.some(r => r.args[1] === 'list'), false);

  requests.length = 0;
  await executor.execute(
    [envelope({ payload: { seaCreatureNaming: true } })],
    context,
    { parentWorktree: 'path:/runtime' },
  );
  assert.equal(flagValue(createdArgs(), '--parent-worktree'), 'path:/runtime');
  assert.equal(flagValue(createdArgs(), '--name'), 'starfish');
  assert.ok(requests.some(r => r.args[0] === 'worktree' && r.args[1] === 'list'));

  requests.length = 0;
  const fallback = createOrcaSpawnExecutorPlugin({ commandRunner, command: 'claude', cwd: '/workspace' });
  await fallback.execute([envelope()], context, {});
  assert.equal(flagValue(createdArgs(), '--parent-worktree'), 'path:/workspace');
  assert.match(String(flagValue(createdArgs(), '--name')), /^dealer-/);
});

test('orca-spawn runs orca inside a path parent worktree when no cwd is configured', async t => {
  const root = await mkdtemp(join(tmpdir(), 'dealer-parent-worktree-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const missingParent = join(root, 'harlequin');
  const requests: CommandRequest[] = [];
  const commandRunner: CommandRunner = async request => {
    requests.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'list') {
      return { stdout: JSON.stringify({ ok: true, result: { worktrees: [] } }) };
    }
    if (request.args[0] === 'worktree' && request.args[1] === 'create') {
      return { stdout: JSON.stringify({ ok: true, result: { worktree: { path: '/tmp/child' } } }) };
    }
    return { stdout: '{"ok":true}' };
  };
  const executor = createOrcaSpawnExecutorPlugin({ commandRunner, command: 'codex', seaCreatureNaming: true });
  const context = { networkId: 'test', nodeId: receiver.nodeId, traceId: 'trace' };
  const request = (name: string) =>
    requests.find(r => r.args[0] === 'worktree' && r.args[1] === name)!;
  const flagValue = (args: readonly string[], flag: string) => args[args.indexOf(flag) + 1];

  await executor.execute([envelope({ payload: { seaCreatureNaming: true } })], context, {
    parentWorktree: `path:${missingParent}`,
  });
  assert.equal(request('list').cwd, missingParent);
  assert.equal(request('create').cwd, missingParent);
  assert.equal(flagValue(request('create').args, '--parent-worktree'), `path:${missingParent}`);

  requests.length = 0;
  await executor.execute([envelope({ payload: { seaCreatureNaming: true } })], context, {});
  assert.equal(request('list').cwd, undefined);
  assert.equal(request('create').cwd, undefined);
  assert.equal(flagValue(request('create').args, '--parent-worktree'), 'active');

  requests.length = 0;
  await executor.execute(
    [envelope({ payload: { seaCreatureNaming: true, noParent: true, projectId: 'alter-service' } })],
    context,
    { projectRoots: { 'alter-service': missingParent } },
  );
  assert.equal(request('list').cwd, missingParent);
  assert.equal(request('create').cwd, missingParent);
  assert.equal(request('create').args.includes('--no-parent'), true);
  assert.equal(request('create').args.includes('--parent-worktree'), false);
});

test('sea-creature naming treats Orca worktree branches as taken', async t => {
  const root = await mkdtemp(join(tmpdir(), 'dealer-branch-collision-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const requests: CommandRequest[] = [];
  const commandRunner: CommandRunner = async request => {
    requests.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'list') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            worktrees: [
              { path: '/scratch/first', branch: 'refs/heads/ben.xu/nautilus' },
              { path: '/scratch/second', branch: 'refs/heads/seahorse' },
            ],
          },
        }),
      };
    }
    if (request.args[0] === 'worktree' && request.args[1] === 'create') {
      return { stdout: JSON.stringify({ ok: true, result: { worktree: { path: '/tmp/child' } } }) };
    }
    return { stdout: '{"ok":true}' };
  };
  const executor = createOrcaSpawnExecutorPlugin({ commandRunner, command: 'codex', seaCreatureNaming: true });
  await executor.execute(
    [envelope({ payload: { seaCreatureNaming: true } })],
    { networkId: 'test', nodeId: receiver.nodeId, traceId: 'trace' },
    // A never-created parent skips the local git scan; only Orca branch rows can collide.
    { parentWorktree: `path:${join(root, 'parent')}` },
  );
  const created = requests.find(r => r.args[0] === 'worktree' && r.args[1] === 'create')!;
  assert.equal(created.args[created.args.indexOf('--name') + 1], 'starfish');
});

test('sea-creature naming treats existing local git branches as taken', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-git-branches-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const git = (...args: string[]) => execFileSync('git', ['-C', dir, ...args], { stdio: 'ignore' });
  git('init', '--quiet');
  git(
    '-c', 'user.name=dealer',
    '-c', 'user.email=dealer@example.test',
    '-c', 'commit.gpgsign=false',
    'commit', '--allow-empty', '--quiet', '-m', 'init',
  );
  git('branch', 'nautilus');
  git('branch', 'ben.xu/seahorse');
  const requests: CommandRequest[] = [];
  const commandRunner: CommandRunner = async request => {
    requests.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'list') {
      return { stdout: JSON.stringify({ ok: true, result: { worktrees: [] } }) };
    }
    if (request.args[0] === 'worktree' && request.args[1] === 'create') {
      return { stdout: JSON.stringify({ ok: true, result: { worktree: { path: '/tmp/child' } } }) };
    }
    return { stdout: '{"ok":true}' };
  };
  const executor = createOrcaSpawnExecutorPlugin({ commandRunner, command: 'codex', cwd: dir, seaCreatureNaming: true });
  await executor.execute(
    [envelope({ payload: { seaCreatureNaming: true } })],
    { networkId: 'test', nodeId: receiver.nodeId, traceId: 'trace' },
    {},
  );
  const created = requests.find(r => r.args[0] === 'worktree' && r.args[1] === 'create')!;
  assert.equal(created.args[created.args.indexOf('--name') + 1], 'starfish');
});

test('command failures surface stdout when the CLI writes errors there', async () => {
  await assert.rejects(
    runCommand({
      command: process.execPath,
      args: ['-e', "process.stdout.write('orca JSON failure'); process.exit(2)"],
    }),
    /exited 2: .*orca JSON failure/,
  );
  await assert.rejects(
    checkedCommand(
      async () => ({ stdout: '{"ok":false,"error":"Missing repo selector"}', stderr: '', exitCode: 2 }),
      { command: 'orca', args: ['worktree', 'create'] },
    ),
    /orca exited 2: .*Missing repo selector/,
  );
  await assert.rejects(
    checkedCommand(
      async () => ({ stdout: 'stdout noise', stderr: 'stderr wins', exitCode: 1 }),
      { command: 'orca', args: ['worktree', 'list'] },
    ),
    /orca exited 1: stderr wins$/,
  );
});

test('trustAntigravityWorkspace records workspace paths atomically and idempotently', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-agy-settings-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const settingsPath = join(dir, 'settings.json');
  const workspacePath = join(dir, 'workspace');
  await mkdir(workspacePath);
  await writeFile(settingsPath, JSON.stringify({ theme: 'dark', trustedWorkspaces: ['/already-trusted', 42] }));
  trustAntigravityWorkspace(workspacePath, settingsPath);
  trustAntigravityWorkspace(workspacePath, settingsPath);
  const resolved = await realpath(workspacePath);
  const expected = resolved === workspacePath ? [workspacePath] : [workspacePath, resolved];
  const settings = JSON.parse(await readFile(settingsPath, 'utf8'));
  assert.equal(settings.theme, 'dark');
  assert.deepEqual(settings.trustedWorkspaces, ['/already-trusted', ...expected]);
  assert.equal((await stat(settingsPath)).mode & 0o777, 0o600);

  trustAntigravityWorkspace('/missing/workspace', settingsPath);
  const extended = JSON.parse(await readFile(settingsPath, 'utf8'));
  assert.deepEqual(extended.trustedWorkspaces, ['/already-trusted', ...expected, '/missing/workspace']);

  await writeFile(settingsPath, JSON.stringify({ trustedWorkspaces: 'invalid' }));
  trustAntigravityWorkspace(workspacePath, settingsPath);
  const normalized = JSON.parse(await readFile(settingsPath, 'utf8'));
  assert.deepEqual(normalized.trustedWorkspaces, expected);
});

test('orca-spawn launches an interactive antigravity terminal and injects the prompt', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-agy-run-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const settingsPath = join(dir, 'settings.json');
  const requests: CommandRequest[] = [];
  let waitFails = false;
  const commandRunner: CommandRunner = async request => {
    requests.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'create') {
      return { stdout: JSON.stringify({ ok: true, result: { worktree: { path: dir } } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'create') {
      return { stdout: JSON.stringify({ ok: true, result: { terminal: { handle: 'term-agy' } } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'wait' && waitFails) {
      throw new Error('wait timed out');
    }
    return { stdout: '{"ok":true}' };
  };
  const executor = createOrcaSpawnExecutorPlugin({ commandRunner, command: 'codex' });
  const context = { networkId: 'test', nodeId: receiver.nodeId, traceId: 'trace' };
  const commandNames = () => requests.map(r => `${r.args[0]} ${r.args[1]}`);
  const flagValue = (args: readonly string[], flag: string) => args[args.indexOf(flag) + 1];

  await executor.execute(
    [
      envelope({
        payload: {
          terminalPrompt: 'Fix the flaky test',
          workspaceComment: 'dealer: antigravity',
        },
      }),
    ],
    context,
    { interactiveCommand: 'agy run --dangerously-skip-permissions', agySettingsPath: settingsPath },
  );
  assert.deepEqual(commandNames(), [
    'worktree create',
    'terminal create',
    'worktree set',
    'terminal wait',
    'terminal send',
  ]);
  assert.equal(flagValue(requests[1].args, '--command'), 'agy run --dangerously-skip-permissions');
  assert.equal(flagValue(requests[2].args, '--comment'), 'dealer: antigravity');
  assert.equal(flagValue(requests[3].args, '--timeout-ms'), '30000');
  assert.equal(flagValue(requests[4].args, '--terminal'), 'term-agy');
  assert.equal(flagValue(requests[4].args, '--text'), 'Fix the flaky test');
  assert.ok(requests[4].args.includes('--enter'));
  assert.equal(flagValue(requests[4].args, '--wait-submit'), '5');
  const trusted = JSON.parse(await readFile(settingsPath, 'utf8')).trustedWorkspaces;
  assert.ok(trusted.includes(dir));

  requests.length = 0;
  waitFails = true;
  await executor.execute(
    [
      envelope({
        payload: { terminalPrompt: 'Retry after timeout' },
      }),
    ],
    context,
    { interactiveCommand: 'agy run', agySettingsPath: settingsPath },
  );
  assert.deepEqual(commandNames(), ['worktree create', 'terminal create', 'terminal wait', 'terminal send']);

  requests.length = 0;
  const untrustedSettings = join(dir, 'untrusted.json');
  await executor.execute(
    [
      envelope({
        payload: { terminalPrompt: 'Do not trust', trustAntigravity: false },
      }),
    ],
    context,
    { agySettingsPath: untrustedSettings },
  );
  assert.equal(existsSync(untrustedSettings), false);
  assert.deepEqual(commandNames(), ['worktree create', 'terminal create', 'terminal wait', 'terminal send']);

  requests.length = 0;
  await executor.execute([envelope({ payload: {} })], context, {});
  assert.deepEqual(commandNames(), ['worktree create', 'terminal create']);
  const agentCommand = String(flagValue(requests[1].args, '--command'));
  assert.match(agentCommand, /^'codex' 'exec'/);
  assert.match(agentCommand, /Investigate/);
});

test('orca-spawn ignores inbound command and settings authority and prefers receiver cwd', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-receiver-authority-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const requests: CommandRequest[] = [];
  const commandRunner: CommandRunner = async request => {
    requests.push(request);
    return { stdout: JSON.stringify({ ok: true, result: { worktree: { path: dir } } }) };
  };
  const senderSettings = join(dir, 'sender-settings.json');
  const optionSettings = join(dir, 'option-settings.json');
  const runtimeSettings = join(dir, 'runtime-settings.json');
  const executor = createOrcaSpawnExecutorPlugin({
    commandRunner,
    cwd: '/receiver/options',
    noParent: true,
    interactiveCommand: 'receiver-options-command',
    agySettingsPath: optionSettings,
  });
  const input = envelope({ payload: {
    terminalPrompt: 'Do the work',
    interactiveCommand: 'sender-controlled-command',
    agySettingsPath: senderSettings,
    repoPath: '/sender/repo',
    cwd: '/sender/cwd',
    parentWorktree: 'path:/sender/worktree',
  } });
  const context = { networkId: 'test', nodeId: receiver.nodeId, traceId: 'trace' };
  await executor.execute([input], context, {
    cwd: '/receiver/runtime',
    parentWorktree: 'path:/receiver/parent',
    interactiveCommand: 'receiver-runtime-command',
    agySettingsPath: runtimeSettings,
  });
  assert.equal(requests[0].cwd, '/receiver/runtime');
  assert.equal(requests[0].args[requests[0].args.indexOf('--parent-worktree') + 1], 'path:/receiver/parent');
  assert.equal(requests[0].args.includes('--no-parent'), false);
  assert.equal(requests[1].args[requests[1].args.indexOf('--command') + 1], 'receiver-runtime-command');
  assert.ok(JSON.parse(await readFile(runtimeSettings, 'utf8')).trustedWorkspaces.includes(dir));
  assert.equal(existsSync(optionSettings), false);
  assert.equal(existsSync(senderSettings), false);

  requests.length = 0;
  await executor.execute([input], context, {});
  assert.equal(requests[0].cwd, '/receiver/options');
  assert.ok(requests[0].args.includes('--no-parent'));
  assert.equal(requests[0].args.includes('--parent-worktree'), false);
  assert.equal(requests[1].args[requests[1].args.indexOf('--command') + 1], 'receiver-options-command');
  assert.ok(JSON.parse(await readFile(optionSettings, 'utf8')).trustedWorkspaces.includes(dir));
  assert.equal(existsSync(senderSettings), false);

  requests.length = 0;
  await createOrcaSpawnExecutorPlugin({ commandRunner }).execute(
    [envelope({ payload: { repoPath: '/loopback/repo', cwd: '/sender/cwd', parentWorktree: 'path:/sender/parent' } })], context, {},
  );
  assert.equal(requests[0].cwd, undefined);
  assert.equal(requests[0].args[requests[0].args.indexOf('--parent-worktree') + 1], 'active');
  requests.length = 0;
  await assert.rejects(
    createOrcaSpawnExecutorPlugin({ commandRunner }).execute(
      [envelope({ payload: { noParent: true, repoPath: '/sender/repo', cwd: '/sender/cwd' } })], context, {},
    ),
    /Top-level Orca worktree requires receiver cwd or bound projectId/,
  );
  assert.equal(requests.length, 0);
});

test('webhook, watermark poller and JSON-lines script run end-to-end through emitter', async t => {
  const { emitter, input, calls } = await connected(t);
  const webhook = createWebhookSourcePlugin({ bearerToken: 'secret' }); await webhook.start(emitter); t.after(() => webhook.stop());
  const addr = webhook.address(); assert.ok(addr && typeof addr === 'object'); const url = `http://127.0.0.1:${addr.port}/`;
  assert.equal((await fetch(url, { method: 'POST', body: JSON.stringify(input) })).status, 401);
  const response = await fetch(url, { method: 'POST', headers: { authorization: 'Bearer secret' }, body: JSON.stringify(input) }); assert.equal(response.status, 200); assert.equal(calls.length, 1);
  const saved: number[] = [];
  const poller = createPollWatermarkSourcePlugin({ initialCursor: 0, poll: async cursor => ({ items: cursor === 0 ? [1, 2] : [], cursor: 2 }), toInput: item => ({ ...input, idempotencyKey: `poll-${item}` }), saveCursor: async cursor => { saved.push(cursor); } });
  await poller.start(emitter); await Promise.all([poller.pollOnce(), poller.pollOnce()]); assert.equal(poller.getCursor(), 2); assert.deepEqual(saved, [2]); assert.equal(calls.length, 3); await poller.stop();
  const script = createScriptSourcePlugin({ command: 'fixture', commandRunner: async () => ({ stdout: JSON.stringify(input) + '\n' + JSON.stringify(input) }) }); await script.start(emitter); await script.stop(); assert.equal(calls.length, 5);
});

test('scheduled watermark source polls immediately when an interval is configured', async () => {
  let polls = 0;
  const poller = createPollWatermarkSourcePlugin({
    initialCursor: 0,
    poll: async cursor => {
      polls++;
      return { items: [], cursor };
    },
    toInput: () => ({ networkId: 'test', routing: { mode: 'auto' }, kind: 'task', subject: '', body: '', payload: {} }),
    intervalMs: 60000,
  });
  const emitter = { async emit() { throw new Error('unused'); }, registerSource() { throw new Error('unused'); } };
  await poller.start(emitter);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(polls, 1);
  await poller.stop();
  assert.equal(polls, 1);
});

test('CAS watermarks persist reserved versions across restarts and reject corrupted stores', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-watermarks-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const watermarksPath = join(dir, 'dedup-watermarks.json');
  const message = envelope({ idempotencyKey: 'persistent-entity', payload: { claimVersion: 4 } });
  let executions = 0;
  const dispatch = async () => {
    executions++;
    return { stage: 'dispatched' as const };
  };
  const first = createDedupCasPolicyPlugin({ watermarksPath });
  assert.equal((await first.process(message, dispatch)).stage, 'dispatched');
  assert.equal((await stat(watermarksPath)).mode & 0o777, 0o600);
  assert.deepEqual(Object.values(JSON.parse(await readFile(watermarksPath, 'utf8'))), [4]);
  const second = createDedupCasPolicyPlugin({ watermarksPath });
  assert.equal((await second.process(envelope({ ...message, id: 'after-restart' }), dispatch)).stage, 'rejected');
  assert.equal(executions, 1);
  assert.equal((await second.process(envelope({ ...message, id: 'newer', payload: { claimVersion: 5 } }), dispatch)).stage, 'dispatched');
  assert.equal(executions, 2);
  const reserved = createDedupCasPolicyPlugin({ watermarksPath });
  await assert.rejects(
    reserved.process(envelope({ ...message, id: 'ambiguous', payload: { claimVersion: 6 } }), async () => {
      throw new Error('unknown external outcome');
    }),
    /unknown external outcome/,
  );
  const restarted = createDedupCasPolicyPlugin({ watermarksPath });
  assert.equal((await restarted.process(envelope({ ...message, id: 'replay', payload: { claimVersion: 6 } }), dispatch)).stage, 'rejected');
  assert.equal(executions, 2);
  for (const corrupt of [JSON.stringify({ key: -1 }), JSON.stringify({ key: 1.5 }), JSON.stringify({ key: 'x' }), JSON.stringify([1]), 'null', 'not json']) {
    await writeFile(watermarksPath, corrupt);
    assert.throws(() => createDedupCasPolicyPlugin({ watermarksPath }), /Invalid dedup watermark store/);
  }
});

test('poll source attempts later items after an early failure and retains the batch cursor', async () => {
  const deliveryReceipt = (item: number, stage: DeliveryReceipt['stage']): DeliveryReceipt => ({
    messageId: `message-${item}`,
    networkId: 'test',
    fromNodeId: receiver.nodeId,
    stage,
    updatedAt: new Date().toISOString(),
    signature: 'signature',
  });
  const runPoll = async (
    emit: (item: number) => Promise<{ messageId: string; traceId: string; receipts: DeliveryReceipt[] }>,
  ) => {
    const attempts: number[] = [];
    const saved: number[] = [];
    const poller = createPollWatermarkSourcePlugin({
      initialCursor: 0,
      poll: async () => ({ items: [1, 2, 3], cursor: 3 }),
      toInput: item => ({
        networkId: 'test',
        routing: { mode: 'auto' },
        kind: 'task',
        subject: '',
        body: '',
        payload: {},
        idempotencyKey: `poll-${item}`,
      }),
      saveCursor: async cursor => {
        saved.push(cursor);
      },
    });
    const emitter = {
      async emit(input: EmitMessageInput) {
        const item = Number(input.idempotencyKey!.slice('poll-'.length));
        attempts.push(item);
        return emit(item);
      },
      registerSource() {
        throw new Error('unused');
      },
    };
    await poller.start(emitter);
    return { attempts, saved, poller };
  };
  const thrown = await runPoll(async item => {
    if (item === 1) {
      throw new Error('item one offline');
    }
    return { messageId: `message-${item}`, traceId: 'trace', receipts: [deliveryReceipt(item, 'dispatched')] };
  });
  await assert.rejects(thrown.poller.pollOnce(), /item one offline/);
  assert.deepEqual(thrown.attempts, [1, 2, 3]);
  assert.equal(thrown.poller.getCursor(), 0);
  assert.deepEqual(thrown.saved, []);
  await thrown.poller.stop();

  const rejected = await runPoll(async item => ({
    messageId: `message-${item}`,
    traceId: 'trace',
    receipts: [deliveryReceipt(item, item === 2 ? 'rejected' : 'dispatched')],
  }));
  await assert.rejects(rejected.poller.pollOnce(), /cursor retained/);
  assert.deepEqual(rejected.attempts, [1, 2, 3]);
  assert.equal(rejected.poller.getCursor(), 0);
  assert.deepEqual(rejected.saved, []);
  await rejected.poller.stop();
});

test('per-item cursor mode persists converged progress immediately and retains failed items', async () => {
  const stages: Record<number, DeliveryReceipt['stage']> = {
    1: 'dispatched',
    2: 'rejected',
    3: 'failed',
    4: 'dispatched',
  };
  const attempts: number[] = [];
  const saved: { baseline: number; last: number }[] = [];
  const emitter = {
    async emit(input: EmitMessageInput) {
      const item = Number(input.idempotencyKey!.slice('poll-'.length));
      attempts.push(item);
      const stage = stages[item];
      if (stage === 'failed') {
        throw new Error('item three offline');
      }
      return {
        messageId: `message-${item}`,
        traceId: 'trace',
        receipts: [
          {
            messageId: `message-${item}`,
            networkId: 'test',
            fromNodeId: receiver.nodeId,
            stage,
            updatedAt: new Date().toISOString(),
            signature: 'signature',
          } satisfies DeliveryReceipt,
        ],
      };
    },
    registerSource() {
      throw new Error('unused');
    },
  };
  const poller = createPollWatermarkSourcePlugin<number, { baseline: number; last: number }>({
    initialCursor: { baseline: 0, last: 0 },
    poll: async () => ({ items: [1, 2, 3, 4], cursor: { baseline: 9, last: 0 } }),
    toInput: item => ({
      networkId: 'test',
      routing: { mode: 'auto' },
      kind: 'task',
      subject: '',
      body: '',
      payload: {},
      idempotencyKey: `poll-${item}`,
    }),
    advanceItemCursor: (cursor, item, receipts) =>
      receipts.some(receipt => receipt.stage === 'failed')
        ? undefined
        : { baseline: cursor.baseline, last: item },
    saveCursor: async cursor => {
      saved.push(structuredClone(cursor));
    },
  });
  await poller.start(emitter);
  await assert.rejects(poller.pollOnce(), /item three offline/);
  assert.deepEqual(attempts, [1, 2, 3, 4]);
  assert.deepEqual(saved, [
    { baseline: 9, last: 0 },
    { baseline: 9, last: 1 },
    { baseline: 9, last: 2 },
    { baseline: 9, last: 4 },
  ]);
  assert.deepEqual(poller.getCursor(), { baseline: 9, last: 4 });
  await poller.stop();
});

test('poll cursor remains unchanged on failed delivery and agent process nonzero is failure', async () => {
  const poller = createPollWatermarkSourcePlugin({ initialCursor: 0, poll: async () => ({ items: [1], cursor: 1 }), toInput: () => ({ networkId: 'test', routing: { mode: 'auto' }, kind: 'task', subject: '', body: '', payload: {} }) });
  await poller.start({ async emit() { throw new Error('offline'); }, registerSource() { throw new Error('unused'); } });
  await assert.rejects(poller.pollOnce(), /offline/); assert.equal(poller.getCursor(), 0); await poller.stop();
  const executor = createAgentPtyExecutorPlugin({ commandRunner: async () => ({ stdout: '', exitCode: 1 }) });
  await assert.rejects(executor.execute([envelope()], { networkId: 'test', nodeId: receiver.nodeId, traceId: 't' }, {}), /exited 1/);
});

test('signing-key mismatch is rejected before executor side effects', async t => {
  const kernel = new Kernel(); kernel.ctx.provide('node', node());
  await kernel.use(createGatePlugin({ mode: 'open' })); await kernel.use(createDispatcherPlugin({ privateKeyPem: sender.privateKeyPem })); t.after(() => kernel.stop());
  let count = 0;
  kernel.ctx.services.dispatcher.registerExecutor({ name: 'test', async execute() { count++; return { executor: 'test', executionId: 'x' }; } }, {});
  await assert.rejects(kernel.ctx.bail('transport/inbound', { networkId: 'test', senderNodeId: sender.nodeId, mode: 'unicast', payload: { type: 'message.envelope', envelope: envelope() } }), /key does not match/);
  assert.equal(count, 0);
});

test('dispatcher failed execution is recorded and not automatically replayed', async t => {
  const { dispatcher } = await setup(t); let count = 0;
  dispatcher.registerExecutor({ name: 'failing', async execute() { count++; throw new Error('external outcome unknown'); } }, {});
  const message = envelope({ payload: { executor: 'failing' } });
  await assert.rejects(dispatcher.dispatch(message), /external outcome/);
  assert.equal((await dispatcher.dispatch(message)).stage, 'failed'); assert.equal(count, 1);
  await assert.rejects(dispatcher.dispatch(envelope({ payload: { executor: 'missing' } })), /unavailable/);
});

test('DND opening respects network overrides, concurrent approvals execute once, rules fail closed', async t => {
  const { gate, calls } = await setup(t, { mode: 'dnd' });
  await gate.setNetworkMode('other', 'dnd');
  await gate.evaluate(envelope()); await gate.evaluate(envelope({ networkId: 'other' }));
  await gate.setMode('open'); assert.equal(calls.length, 1);
  await gate.setNetworkMode('other', 'open'); assert.equal(calls.length, 2);
  await gate.setMode('approval_required'); const verdict = await gate.evaluate(envelope());
  assert.equal(verdict.status, 'pending_approval'); if (verdict.status !== 'pending_approval') return;
  const results = await Promise.allSettled([gate.approve(verdict.approvalId), gate.approve(verdict.approvalId)]);
  assert.equal(results.filter(r => r.status === 'fulfilled').length, 1); assert.equal(calls.length, 3);
  gate.registerRule({ name: 'broken', priority: 1, async evaluate() { throw new Error('policy unavailable'); } });
  await assert.rejects(gate.evaluate(envelope()), /policy unavailable/);
});

test('emitter broadcast and multicast paths preserve signed payload and validate receipts', async t => {
  const { emitter, input, origin, sent } = await connected(t);
  const broadcast = await emitter.emit({ ...input, routing: { mode: 'broadcast', topic: 'alerts' } }); assert.deepEqual(broadcast.receipts, []);
  // A custom decision exercises multicast transport even with a single unique destination.
  origin.ctx.services.router.registerJudge({ name: 'multi', priority: 1, async decide() { return { mode: 'multicast', targetNodeIds: [receiver.nodeId], matchedBy: 'multi', reason: 'test' }; } });
  assert.equal((await emitter.emit({ ...input, routing: { mode: 'auto' } })).receipts.length, 1); assert.ok(sent.every(verifyEnvelope));
  const transport = origin.ctx.services.transport; const original = transport.sendUnicast;
  transport.sendUnicast = async (network, target, frame) => {
    const receipt = JSON.parse(Buffer.from(await original(network, target, frame)).toString()); receipt.messageId = 'unrelated'; return Buffer.from(JSON.stringify(receipt));
  };
  await assert.rejects(emitter.emit(input), /Invalid or mismatched/);
});

test('Orca idle inspection includes child workspaces and rejects truncated inventories', async () => {
  let busy = true, truncated = false;
  const executor = createOrcaWaveExecutorPlugin({ cwd: '/workspace', runId: 'run', commandRunner: async request => request.args[0] === 'terminal' ? { stdout: JSON.stringify(request.args[1] === 'list' ? { ok: true, result: { terminals: [{ handle: 'term-test', connected: true, writable: true }] } } : { ok: true }) } : ({ stdout: JSON.stringify({ ok: true, result: { truncated, worktrees: [
    { path: '/workspace', worktreeId: 'parent', childWorktreeIds: ['child'], unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
    { path: '/child', worktreeId: 'child', childWorktreeIds: [], unverifiableTerminalCount: 0, agents: [{ state: busy ? 'working' : 'done' }] },
  ] } }) }) });
  assert.equal((await executor.checkIdle!({})).idle, false); busy = false; assert.equal((await executor.checkIdle!({})).idle, true);
  truncated = true; assert.equal((await executor.checkIdle!({})).idle, false);
});

test('Orca idle inspection trusts real TUI previews and skips terminal wait', async () => {
  let lastLine = '  ⏵⏵ bypass permissions on';
  let waits = 0;
  const executor = createOrcaWaveExecutorPlugin({
    cwd: '/workspace',
    terminalHandle: 'term-test',
    commandRunner: async request => {
      if (request.args[0] === 'worktree') {
        return {
          stdout: JSON.stringify({
            ok: true,
            result: {
              truncated: false,
              worktrees: [
                {
                  path: '/workspace',
                  worktreeId: 'parent',
                  childWorktreeIds: [],
                  unverifiableTerminalCount: 0,
                  agents: [{ state: 'done' }],
                },
              ],
            },
          }),
        };
      }
      if (request.args[1] === 'list') {
        return {
          stdout: JSON.stringify({
            ok: true,
            result: {
              terminals: [
                {
                  handle: 'term-test',
                  connected: true,
                  writable: true,
                  agentIdentity: 'antigravity',
                  preview: `history\n${lastLine}`,
                  lastOutputAt: 1,
                },
              ],
            },
          }),
        };
      }
      if (request.args[1] === 'wait') {
        waits++;
        return { stdout: '{"ok":false,"error":{"code":"timeout"}}' };
      }
      return { stdout: '{"ok":true}' };
    },
  });
  assert.equal((await executor.checkIdle!({})).idle, true);
  assert.equal(waits, 0);
  lastLine = '> unsent draft';
  assert.equal((await executor.checkIdle!({})).idle, false);
  lastLine = '>';
  assert.equal((await executor.checkIdle!({})).idle, true);
});

test('local command executor actually runs a process and captures JSON without shell interpolation', async () => {
  const executor = createAgentPtyExecutorPlugin({ command: process.execPath, args: ['-e', 'let s="";process.stdin.on("data",c=>s+=c);process.stdin.on("end",()=>process.stdout.write(JSON.stringify({input:s})))'] });
  const result = await executor.execute([envelope({ body: '$(touch /do-not-execute) `literal`' })], { networkId: 'test', nodeId: receiver.nodeId, traceId: 't' }, {});
  assert.match(JSON.stringify(result.output), /do-not-execute/);
});

test('unloading emitter stops an active source instead of waiting forever for start', async t => {
  const { emitter, origin } = await connected(t);
  let resolveStart!: () => void; let stopped = false;
  emitter.registerSource({ name: 'long-lived', start() { return new Promise<void>(resolve => { resolveStart = resolve; }); }, stop() { stopped = true; resolveStart(); } });
  await origin.stop(); assert.equal(stopped, true);
});

test('CAS reserves failed claims to avoid replaying an ambiguous external effect', async () => {
  const policy = createDedupCasPolicyPlugin(); let count = 0;
  const message = envelope({ idempotencyKey: 'ambiguous', payload: { claimVersion: 1 } });
  const execute = async () => { count++; throw new Error('timeout after submission'); };
  await assert.rejects(policy.process(message, execute), /timeout/);
  assert.equal((await policy.process(envelope({ ...message, id: 'redelivery' }), execute)).stage, 'rejected');
  assert.equal(count, 1);
});

test('orca-spawn binds project identity only on the receiver and validates parent repository', async t => {
  const root = await mkdtemp(join(tmpdir(), 'dealer-project-roots-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const repo = join(root, 'repo');
  const sibling = join(root, 'sibling');
  const other = join(repo, 'other');
  await mkdir(other, { recursive: true });
  const git = (cwd: string, ...args: string[]) =>
    execFileSync('git', ['-C', cwd, ...args], { stdio: 'ignore' });
  git(repo, 'init', '--quiet');
  git(repo, '-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'commit', '--allow-empty', '-m', 'init');
  git(repo, 'worktree', 'add', '--detach', sibling);
  git(other, 'init', '--quiet');
  const requests: CommandRequest[] = [];
  const executor = createOrcaSpawnExecutorPlugin({
    cwd: '/unrelated/receiver',
    projectRoots: { 'alter-service': repo, virtual: join(root, 'virtual') },
    commandRunner: async request => {
      requests.push(request);
      return { stdout: JSON.stringify({ ok: true, result: { path: join(root, 'spawned') } }) };
    },
  });
  const context = { networkId: 'test', nodeId: receiver.nodeId, traceId: 'trace' };
  const payload = { projectId: ' alter-service ', noParent: true, repoPath: '/sender/repo',
    cwd: '/sender/cwd', parentWorktree: 'path:/sender/parent' };
  await executor.execute([envelope({ payload })], context, {});
  assert.equal(requests[0].cwd, repo);
  assert.ok(requests[0].args.includes('--no-parent'));
  for (const parentWorktree of [`path:${sibling}`, `id:receiver-repo::${sibling}`, `path:${repo}`]) {
    requests.length = 0;
    await executor.execute([envelope({ payload })], context, { parentWorktree });
    assert.equal(requests[0].cwd, repo);
    assert.equal(requests[0].args[requests[0].args.indexOf('--parent-worktree') + 1], parentWorktree);
  }
  for (const parentWorktree of [`path:${other}`, 'active', 'path:', 'id:repo::', `path:${root}/missing`]) {
    requests.length = 0;
    await assert.rejects(executor.execute([envelope({ payload })], context, { parentWorktree }),
      /Parent worktree does not belong/);
    assert.equal(requests.length, 0);
  }
  requests.length = 0;
  await assert.rejects(executor.execute([envelope({ payload: { ...payload, projectId: 'missing' } })], context, {}),
    /Unbound project on receiver: missing/);
  await assert.rejects(executor.execute([envelope({ payload })], context,
    { projectRoots: { 'alter-service': ' ' } }), /Unbound project/);
  assert.equal(requests.length, 0);
  await executor.execute([envelope({ payload: { projectId: 'alter-service' } })], context, {});
  assert.equal(requests[0].args[requests[0].args.indexOf('--parent-worktree') + 1], `path:${repo}`);
  requests.length = 0;
  await executor.execute([envelope({ payload })], context, { projectRoots: { 'alter-service': sibling } });
  assert.equal(requests[0].cwd, sibling);
  requests.length = 0;
  await executor.execute([envelope({ payload: { projectId: 'virtual' } })], context,
    { parentWorktree: `path:${root}/virtual/child` });
  assert.equal(requests[0].cwd, join(root, 'virtual'));
  await assert.rejects(executor.execute([envelope({ payload: { projectId: 'virtual' } })], context,
    { parentWorktree: `path:${root}/virtual-other` }), /Parent worktree does not belong/);
});
