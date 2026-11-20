import { createTestWakeupDrivers } from './helpers/wakeup-drivers.ts';
import { createOrcaWakeupDrivers } from '../packages/index.ts';
import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  DealerNodeRuntime,
  MailboxStore,
  WakeupRedriveService,
  createOrcaExecutorPlugin,
  createOrcaSpawnExecutorPlugin,
  createOrcaWaveExecutorPlugin,
  initProjectScaffold,
  runDealerCli,
} from '../packages/index.ts';
import type {
  CommandRequest,
  CommandRunner,
  ExecutionContext,
  MessageEnvelope,
} from '../packages/index.ts';

function sessionBinding(target) { return target?.driverConfig ?? {}; }

function directory(t: TestContext): string {
  const root = mkdtempSync(join(tmpdir(), 'dealer-thread-affinity-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return root;
}

const origin = {
  nodeId: `ed25519:${'a'.repeat(64)}`,
  member: 'origin',
  nodeName: 'halfbeak',
};
const peer = {
  nodeId: `ed25519:${'b'.repeat(64)}`,
  member: 'peer',
  nodeName: 'peer-node',
};
const context: ExecutionContext = {
  networkId: 'team',
  nodeId: origin.nodeId,
  traceId: 'trace',
};

function envelope(id: string, patch: Partial<MessageEnvelope> = {}): MessageEnvelope {
  return {
    id,
    networkId: 'team',
    traceId: id,
    from: origin,
    routing: { mode: 'unicast' },
    kind: 'collab.request.v4',
    priority: 'normal',
    subject: id,
    body: 'body',
    payload: {},
    attachments: [],
    createdAt: new Date().toISOString(),
    signature: '',
    ...patch,
  };
}

function terminalSends(calls: readonly CommandRequest[]): CommandRequest[] {
  return calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'send');
}

function terminalOf(call: CommandRequest): string {
  return String(call.args[call.args.indexOf('--terminal') + 1]);
}

async function spawnPeers(t: TestContext) {
  const root = directory(t);
  const originWorkspace = join(root, 'tool-registry发布部署方式');
  const childWorkspace = join(root, 'spawned-child');
  for (const workspace of [originWorkspace, childWorkspace]) mkdirSync(workspace);
  const project = join(root, 'alter-service');
  initProjectScaffold(project, { projectId: 'alter-service', defaultNetworkId: 'team' });
  const calls: CommandRequest[] = [];
  const commandRunner: CommandRunner = async request => {
    if (request.command === 'lark-cli') return { stdout: '{"message_id":"notified"}' };
    calls.push(request);
    const reply = (result: unknown) => ({ stdout: JSON.stringify({ ok: true, result }) });
    switch (request.args.slice(0, 2).join(' ')) {
      case 'worktree create': return reply({ path: childWorkspace });
      case 'worktree ps': return reply({
        truncated: false,
        worktrees: [originWorkspace, childWorkspace].map((path, index) => ({
          path, worktreeId: `wt-${index}`, unverifiableTerminalCount: 0, agents: [{ state: 'done' }],
        })),
      });
      case 'terminal create': return reply({ terminal: { handle: 'term_child' } });
      case 'terminal list': return reply({
        terminals: [{
          handle: request.args.includes(`path:${childWorkspace}`) ? 'term_child' : 'term_origin',
          connected: true, writable: true, preview: '>',
        }],
      });
      default: return { stdout: '{"ok":true}' };
    }
  };
  const make = async (member: string) => {
    const runtime = await new DealerNodeRuntime({
      stateDir: join(root, member),
      member, name: member, profile: 'personal-dev',
      transport: { discoveryDir: join(root, 'mesh') },
      patch: {
        gateMode: 'open', defaultExecutor: 'inbox', larkNotify: { enabled: false, target: { userId: 'test-user' } },
        ...(member === 'alice' ? { projects: [{ projectId: 'alter-service', rootPath: project }] } : {}),
      },
      wakeupIdleCheck: async () => ({ idle: true }),
      commandRunner,
    }).start();
    t.after(() => runtime.stop());
    await runtime.mountNetwork({ networkId: 'team', passphrase: 'affinity-test' });
    return runtime;
  };
  const alice = await make('alice');
  const bob = await make('bob');
  const deadline = Date.now() + 5000;
  while (!alice.node.listPeers({ networkId: 'team', member: 'bob' }).length ||
         !bob.node.listPeers({ networkId: 'team', member: 'alice' }).length) {
    assert.ok(Date.now() < deadline, 'peer discovery');
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  const preset = await alice.applyAgentPreset({
    host: 'orca', mode: 'spawn', projectId: 'alter-service', agent: 'antigravity', notifyLark: true,
  });
  assert.equal(preset.binding.executor, 'orca');
  assert.equal(preset.binding.mode, 'spawn');
  const send = async (runtime: DealerNodeRuntime, member: string, ...args: string[]) => {
    const out: string[] = [], err: string[] = [];
    const code = await runDealerCli([
      'send', '--network', 'team', '--to', `member:${member}`, '--topic', 'collab.request.v4', ...args, '--json',
    ], { runtime, env: {}, stdout: line => out.push(line), stderr: line => err.push(line) });
    assert.equal(code, 0, err.join('\n'));
    const result = JSON.parse(out.join('\n'));
    assert.equal(result.receipts[0]?.stage, 'dispatched', JSON.stringify(result.receipts));
    return result;
  };
  return { alice, bob, calls, originWorkspace, send };
}

test('spawn preset delivers structured status updates to the originating Orca worktree', async t => {
  const { alice, bob, calls, originWorkspace, send } = await spawnPeers(t);
  const mailboxId = 'worktree:tool-registry发布部署方式';
  const request = await send(alice, 'bob', '--body', 'Publish the tool registry',
    '--workspace-path', originWorkspace, '--terminal-handle', 'term_origin');
  const ids = [];
  for (const status of ['accepted', 'in_progress', 'done']) {
    const update = await send(bob, 'alice', '--thread', request.messageId, '--body', JSON.stringify({ status }));
    ids.push(update.messageId);
  }
  const followup = await bob.emitter.emit({
    networkId: 'team', routing: { mode: 'unicast', targets: ['member:alice'] },
    kind: 'collab.request.v4', subject: 'Follow-up', body: 'Additional detail', payload: {},
    semantic: { type: 'message', threadId: request.messageId },
  });
  assert.equal(followup.receipts[0].stage, 'dispatched', JSON.stringify(followup.receipts));
  await alice.wakeups.flushMailbox(mailboxId, { force: true });
  assert.equal(calls.filter(call => call.args.slice(0, 2).join(' ') === 'worktree create').length, 0);
  const items = alice.mailbox.list({ mailboxId }).items.toReversed();
  assert.deepEqual(items.map(item => item.envelope.id), [...ids, followup.messageId]);
  assert.deepEqual(items.slice(0, 3).map(item => item.envelope.payload.status), ['accepted', 'in_progress', 'done']);
  assert.deepEqual(items.map(item => item.envelope.semantic?.targetMailbox), [mailboxId, mailboxId, mailboxId, undefined]);
  assert.ok(items.every(item => item.envelope.semantic?.type === 'message'));
  assert.ok(terminalSends(calls).length > 0);
  assert.ok(terminalSends(calls).every(call => terminalOf(call) === 'term_origin'));
  assert.equal(alice.mailbox.getWakeTarget(mailboxId)?.pendingWake, false);
});

test('spawn preset reuses the task mailbox for follow-ups to an inbound root', async t => {
  const { alice, bob, calls, send } = await spawnPeers(t);
  const request = await send(bob, 'alice', '--body', 'New task');
  const mailboxId = `task:${request.messageId}`;
  const lease = alice.mailbox.check({ mailboxId });
  assert.equal(lease.items[0]?.envelope.id, request.messageId);
  alice.mailbox.ack(lease.deliveryId!, mailboxId);
  const before = terminalSends(calls).length;
  const followup = await bob.emitter.emit({
    networkId: 'team', routing: { mode: 'unicast', targets: ['member:alice'] },
    kind: 'collab.request.v4', subject: 'Follow-up', body: 'Additional detail', payload: {},
    semantic: { type: 'message', threadId: request.messageId, targetMailbox: 'default' },
  });
  assert.equal(followup.receipts[0].stage, 'dispatched', JSON.stringify(followup.receipts));
  await alice.wakeups.flushMailbox(mailboxId, { force: true });
  assert.equal(calls.filter(call => call.args.slice(0, 2).join(' ') === 'worktree create').length, 1);
  assert.deepEqual(alice.mailbox.list({ mailboxId }).items.map(item => item.envelope.id), [followup.messageId]);
  assert.deepEqual(terminalSends(calls).slice(before).map(terminalOf), ['term_child']);
});

test('dealer send --thread leaves a root’s payload mailbox hint out of the signed target', async t => {
  const { alice, bob, calls, send } = await spawnPeers(t);
  const hint = 'worktree:sender-hint';
  const request = await send(bob, 'alice', '--body', JSON.stringify({ mailbox: hint }));
  const mailboxId = `task:${request.messageId}`;
  assert.equal(alice.mailbox.list({ mailboxId }).items[0]?.envelope.payload.mailbox, hint);
  assert.equal(bob.mailbox.resolveReplyTarget(request.messageId, 'team')?.targetMailbox, 'default');

  const followup = await send(bob, 'alice', '--thread', request.messageId, '--body', 'Additional context');
  const items = alice.mailbox.list({ mailboxId }).items;
  assert.equal(items.find(item => item.id === followup.messageId)?.envelope.semantic?.targetMailbox, undefined);
  assert.deepEqual(items.map(item => item.id).sort(), [request.messageId, followup.messageId].sort());
  assert.equal(alice.mailbox.list({ mailboxId: hint }).totalCount, 0);
  assert.equal(calls.filter(call => call.args.slice(0, 2).join(' ') === 'worktree create').length, 1);
});

for (const [name, create] of [['orca', createOrcaExecutorPlugin], ['orca-spawn', createOrcaSpawnExecutorPlugin]] as const) {
  test(`standalone ${name} initializes and reuses its configured spawn mailbox`, async t => {
    const root = directory(t);
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const mailboxId = 'requests';
    const calls: CommandRequest[] = [];
    const commandRunner: CommandRunner = async request => {
      calls.push(request);
      const action = request.args.slice(0, 2).join(' ');
      if (action === 'worktree create') return { stdout: JSON.stringify({ ok: true, result: { path: root } }) };
      if (action === 'terminal create') return { stdout: '{"ok":true,"result":{"terminal":{"handle":"term-bound"}}}' };
      return { stdout: '{"ok":true}' };
    };
    const wakeup = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner, idleCheck: async () => ({ idle: true }) }) });
    t.after(() => wakeup.stop());
    const plugin = create({ store, wakeup, commandRunner, resolveTopicBinding: () => ({ mailboxId, mode: 'spawn' }) });
    const first = envelope('first', { semantic: { type: 'message', threadId: 'first', targetMailbox: mailboxId } });
    const second = envelope('second');
    const third = envelope('third', { semantic: { type: 'message', threadId: 'third', targetMailbox: mailboxId } });
    const receipts = await Promise.all([first, second].map(message => plugin.execute([message], context, {})));
    receipts.push(await plugin.execute([third], context, {}));
    for (const receipt of receipts) {
      assert.equal(receipt.executor, name);
      assert.equal(receipt.mailboxId, mailboxId);
    }
    assert.deepEqual(store.list({ mailboxId }).items.map(item => item.id).sort(), ['first', 'second', 'third']);
    assert.equal(store.getWakeTarget(mailboxId)?.driver, 'orca-terminal');
    assert.equal(calls.filter(call => call.args.slice(0, 2).join(' ') === 'worktree create').length, 1);
  });

  test(`standalone ${name} spawn delivers a batch to its owning thread and session mailboxes`, async t => {
    const store = new MailboxStore(join(directory(t), 'mailbox.json'));
    const calls: CommandRequest[] = [];
    const commandRunner: CommandRunner = async request => {
      calls.push(request);
      assert.notDeepEqual(request.args.slice(0, 2), ['worktree', 'create']);
      return { stdout: '{"ok":true}' };
    };
    const wakeup = new WakeupRedriveService({
      store, drivers: createTestWakeupDrivers({ commandRunner, idleCheck: async () => ({ idle: true }) }),
    });
    t.after(() => wakeup.stop());
    const root = envelope('root', { semantic: { type: 'message', threadId: 'root' } });
    store.recordInboundEnvelope(root, 'task:root');
    store.upsertWakeTarget('task:root', { driver: 'orca-terminal', driverConfig: { terminalHandle: 'term_child' } });
    store.upsertWakeTarget('worktree:origin', { driver: 'orca-terminal', driverConfig: { terminalHandle: 'term_origin' } });
    const plugin = create({ store, wakeup, commandRunner, mode: 'spawn' });
    const receipt = await plugin.execute([
      envelope('followup', { semantic: { type: 'message', threadId: root.id } }),
      envelope('reply', { semantic: { type: 'message', threadId: 'reply', targetMailbox: 'worktree:origin' } }),
    ], context, {});
    assert.equal(receipt.executor, name);
    assert.deepEqual(receipt.mailboxIds, ['task:root', 'worktree:origin']);
    assert.deepEqual(store.list({ mailboxId: 'task:root' }).items.map(item => item.envelope.id), ['followup']);
    assert.deepEqual(store.list({ mailboxId: 'worktree:origin' }).items.map(item => item.envelope.id), ['reply']);
    assert.deepEqual(terminalSends(calls).map(terminalOf), ['term_child', 'term_origin']);
  });
}

test('standalone orca-spawn separates a new task from an existing thread in the same batch', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  const commandRunner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'create') {
      return { stdout: JSON.stringify({ ok: true, result: { path: root } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'create') {
      return { stdout: '{"ok":true,"result":{"terminal":{"handle":"term_new"}}}' };
    }
    return { stdout: '{"ok":true}' };
  };
  const first = envelope('existing', { semantic: { type: 'message', threadId: 'existing' } });
  store.recordInboundEnvelope(first, 'task:existing');
  store.upsertWakeTarget('task:existing', { driver: 'orca-terminal', driverConfig: { terminalHandle: 'term_existing' } });
  const plugin = createOrcaSpawnExecutorPlugin({ store, commandRunner, cwd: root, mode: 'spawn' });
  const receipt = await plugin.execute([
    envelope('fresh', { payload: { mailbox: 'sender-hint' } }),
    envelope('followup', { semantic: { type: 'message', threadId: first.id } }),
  ], context, {});
  assert.deepEqual(receipt.mailboxIds, ['task:fresh', 'task:existing']);
  assert.deepEqual(store.list({ mailboxId: 'task:fresh' }).items.map(item => item.envelope.id), ['fresh']);
  assert.deepEqual(store.list({ mailboxId: 'task:existing' }).items.map(item => item.envelope.id), ['followup']);
  assert.equal(store.list({ mailboxId: 'sender-hint' }).totalCount, 0);
  assert.equal(calls.filter(call => call.args.slice(0, 2).join(' ') === 'worktree create').length, 1);
});

test('thread replies wake the originating worktree session instead of the topic inbox', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const threadId = 'thread-affinity-1';
  const originMailbox = 'worktree:halfbeak-2';
  const originWorkspace = join(root, 'halfbeak-2');
  // The originating session's outbound request establishes the thread's local affinity route,
  // and bindCallerWakeTarget persists the session's terminal as its wake target.
  store.recordOutboundEnvelope(
    envelope('origin-request', {
      semantic: { type: 'message', threadId, targetMailbox: 'stream:inbox', replyMailbox: originMailbox },
    }),
    [peer.nodeId],
    originMailbox,
  );
  store.upsertWakeTarget(originMailbox, {
    driver: 'orca-terminal',
    driverConfig: {
      workspacePath: originWorkspace,
      worktreeId: 'wt-halfbeak-2',
      terminalHandle: 'term_origin',
    },
  });

  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    return { stdout: '{"ok":true}' };
  };
  const wakeup = new WakeupRedriveService({
    store,
    drivers: createTestWakeupDrivers({ commandRunner: runner, idleCheck: async () => ({ idle: true }) }),
  });
  t.after(() => wakeup.stop());
  const plugin = createOrcaWaveExecutorPlugin({
    store,
    wakeup,
    resolveTopicBinding: kind =>
      kind === 'collab.request.v4'
        ? {
            executor: 'orca-wave',
            mailboxId: 'stream:inbox',
            workspacePath: join(root, 'inbox'),
            rotateAfterWaves: 1,
          }
        : undefined,
  });

  // A plain thread message without any explicit target must follow the thread affinity route and
  // must not apply the topic binding's workspace/rotation to the session mailbox.
  await plugin.execute(
    [
      envelope('peer-reply', {
        from: peer,
        semantic: { type: 'message', threadId },
        payload: { workspacePath: '/sender/impostor', rotateAfterWaves: 9 },
      }),
    ],
    context,
    {},
  );

  assert.equal(store.list({ mailboxId: originMailbox }).queuedCount, 1);
  assert.equal(store.list({ mailboxId: 'stream:inbox' }).totalCount, 0);
  const preserved = store.getWakeTarget(originMailbox)!;
  assert.equal(preserved.driverConfig?.workspacePath, originWorkspace);
  assert.equal(preserved.driverConfig?.worktreeId, 'wt-halfbeak-2');
  assert.equal(preserved.driverConfig?.terminalHandle, 'term_origin');
  assert.equal(preserved.rotateAfterWaves, undefined);
  assert.equal(store.getWakeTarget('stream:inbox'), undefined);
  assert.equal(terminalSends(calls).length, 1);
  assert.equal(terminalOf(terminalSends(calls)[0]), 'term_origin');
  assert.equal(terminalSends(calls)[0].cwd, originWorkspace);

  // A control reply with an explicit session target keeps the same affinity and wake target.
  calls.length = 0;
  await plugin.execute(
    [
      envelope('peer-control-reply', {
        from: peer,
        semantic: { type: 'reply', threadId, targetMailbox: originMailbox, replyMailbox: 'stream:inbox' },
      }),
    ],
    context,
    {},
  );
  assert.equal(store.list({ mailboxId: originMailbox }).queuedCount, 2);
  assert.equal(store.list({ mailboxId: 'stream:inbox' }).totalCount, 0);
  const stillPreserved = store.getWakeTarget(originMailbox)!;
  assert.equal(stillPreserved.driverConfig?.workspacePath, originWorkspace);
  assert.equal(stillPreserved.driverConfig?.terminalHandle, 'term_origin');
  assert.equal(stillPreserved.rotateAfterWaves, undefined);
  assert.equal(terminalSends(calls).length, 1);
  assert.equal(terminalOf(terminalSends(calls)[0]), 'term_origin');
});

test('a session mailbox with no live wake target infers its Orca worktree', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'ps') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            truncated: false,
            worktrees: [{ displayName: 'halfbeak-2', path: '/orca/halfbeak-2', worktreeId: 'wt-2' }],
          },
        }),
      };
    }
    return { stdout: '{"ok":true}' };
  };
  store.recordOutboundEnvelope(
    envelope('origin-request-2', {
      semantic: { type: 'message', threadId: 'thread-affinity-2', replyMailbox: 'worktree:halfbeak-2' },
    }),
    [peer.nodeId],
    'worktree:halfbeak-2',
  );
  const wakeup = new WakeupRedriveService({ store, drivers: createOrcaWakeupDrivers({ commandRunner: runner }), flushIntervalMs: 100_000 });
  t.after(() => wakeup.stop());
  const plugin = createOrcaWaveExecutorPlugin({
    store, wakeup,
    commandRunner: runner,
    resolveTopicBinding: () => undefined,
  });
  await plugin.execute(
    [envelope('peer-reply-2', { from: peer, semantic: { type: 'message', threadId: 'thread-affinity-2' }, payload: { workspacePath: '/sender/impostor', agent: 'antigravity' } })],
    context,
    {},
  );
  assert.equal(store.list({ mailboxId: 'worktree:halfbeak-2' }).queuedCount, 1);
  const inferred = store.getWakeTarget('worktree:halfbeak-2')!;
  assert.equal(inferred.driver, 'orca-terminal');
  assert.equal(inferred.driverConfig?.workspacePath, '/orca/halfbeak-2');
  assert.equal(inferred.driverConfig?.worktreeId, 'wt-2');
  assert.equal(inferred.rotateAfterWaves, undefined);
  assert.equal(store.getWakeTarget('stream:inbox'), undefined);
});

test('dealer send --thread reuses the thread reply route for its mailboxes', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'node'),
    member: 'bob',
    name: 'bob',
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
    commandRunner: async request =>
      request.args[0] === 'worktree' && request.args[1] === 'ps'
        ? { stdout: JSON.stringify({ ok: true, result: { truncated: false, worktrees: [] } }) }
        : { stdout: '{"ok":true}' },
    wakeupIdleCheck: async () => ({ idle: true }),
    patch: { defaultExecutor: 'inbox', gateMode: 'open' },
  }).start();
  t.after(() => runtime.stop());
  const threadId = 'thread-cli';
  // The peer's reply we received asks us to answer in task:peer; we are handling the thread from
  // worktree:origin. Both routes are recorded on the thread's legs.
  runtime.mailbox.recordInboundEnvelope(
    envelope('peer-reply-3', {
      networkId: 'local',
      from: peer,
      semantic: { type: 'reply', threadId, replyMailbox: 'task:peer' },
    }),
    'worktree:origin',
  );

  const run = async (extra: string[]) => {
    const out: string[] = [];
    const err: string[] = [];
    const code = await runDealerCli(
      ['send', '--network', 'local', '--to', 'member:bob', '--topic', 'collab.request.v4', '--body', 'follow-up', '--thread', threadId, ...extra, '--json'],
      { runtime, stdout: line => out.push(line), stderr: line => err.push(line) },
    );
    assert.equal(code, 0, err.join('\n'));
    return JSON.parse(out.join('\n'));
  };

  await run([]);
  const [delivered] = runtime.mailbox.list({ mailboxId: 'task:peer' }).items;
  assert.ok(delivered, 'the thread reply route must address the peer mailbox');
  assert.deepEqual(delivered.envelope.semantic, {
    type: 'message',
    threadId,
    targetMailbox: 'task:peer',
    replyMailbox: 'worktree:origin',
  });

  // An explicit target always wins over the recorded thread route.
  await run(['--target-mailbox', 'worktree:peer-session']);
  const explicit = runtime.mailbox.list({ mailboxId: 'worktree:peer-session' }).items[0];
  assert.ok(explicit, 'the explicit target mailbox must win');
  assert.equal(explicit.envelope.semantic?.targetMailbox, 'worktree:peer-session');

  // A thread route recorded on another network never applies to this send.
  const foreignThreadId = 'thread-cli-foreign';
  runtime.mailbox.recordInboundEnvelope(
    envelope('peer-reply-foreign', {
      networkId: 'team',
      from: peer,
      semantic: { type: 'reply', threadId: foreignThreadId, replyMailbox: 'task:foreign' },
    }),
    'worktree:origin',
  );
  const foreignOut: string[] = [];
  const foreignErr: string[] = [];
  const foreignCode = await runDealerCli(
    ['send', '--network', 'local', '--to', 'member:bob', '--topic', 'collab.request.v4', '--body', 'foreign', '--thread', foreignThreadId, '--json'],
    { runtime, stdout: line => foreignOut.push(line), stderr: line => foreignErr.push(line) },
  );
  assert.equal(foreignCode, 0, foreignErr.join('\n'));
  assert.equal(runtime.mailbox.list({ mailboxId: 'task:foreign' }).totalCount, 0);
  const foreign = runtime.mailbox.list({ mailboxId: 'default' }).items.at(-1);
  assert.equal(foreign?.envelope.semantic?.targetMailbox, undefined);
});

test('dealer send --thread resolves the route recorded on the sending network', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'node'),
    member: 'bob',
    name: 'bob',
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
    commandRunner: async request =>
      request.args[0] === 'worktree' && request.args[1] === 'ps'
        ? { stdout: JSON.stringify({ ok: true, result: { truncated: false, worktrees: [] } }) }
        : { stdout: '{"ok":true}' },
    wakeupIdleCheck: async () => ({ idle: true }),
    patch: { defaultExecutor: 'inbox', gateMode: 'open' },
  }).start();
  t.after(() => runtime.stop());
  const threadId = 'thread-cross-network';
  // The same thread id accumulated an older local route and a newer team route. `--network local`
  // must follow the local leg instead of the newer leg shadowing it.
  runtime.mailbox.recordInboundEnvelope(
    envelope('peer-reply-local', {
      networkId: 'local',
      from: peer,
      semantic: { type: 'reply', threadId, replyMailbox: 'task:local' },
    }),
    'worktree:origin',
  );
  runtime.mailbox.recordInboundEnvelope(
    envelope('peer-reply-team', {
      networkId: 'team',
      from: peer,
      semantic: { type: 'reply', threadId, replyMailbox: 'task:team' },
    }),
    'worktree:origin',
  );
  // The unscoped lookup follows the newest record; the network-scoped lookup must not.
  assert.equal(runtime.mailbox.resolveReplyTarget(threadId)?.networkId, 'team');
  const localRoute = runtime.mailbox.resolveReplyTarget(threadId, 'local')!;
  assert.equal(localRoute.networkId, 'local');
  assert.equal(localRoute.targetMailbox, 'task:local');

  const out: string[] = [];
  const err: string[] = [];
  const code = await runDealerCli(
    ['send', '--network', 'local', '--to', 'member:bob', '--topic', 'collab.request.v4', '--body', 'follow-up', '--thread', threadId, '--json'],
    { runtime, stdout: line => out.push(line), stderr: line => err.push(line) },
  );
  assert.equal(code, 0, err.join('\n'));
  assert.equal(runtime.mailbox.list({ mailboxId: 'task:team' }).totalCount, 0);
  const [delivered] = runtime.mailbox.list({ mailboxId: 'task:local' }).items;
  assert.ok(delivered, 'the route recorded on the sending network must receive the follow-up');
  assert.deepEqual(delivered.envelope.semantic, {
    type: 'message',
    threadId,
    targetMailbox: 'task:local',
    replyMailbox: 'worktree:origin',
  });
});

test('dealer send --thread follows the newest inbound leg of a thread we originated', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'node'),
    member: 'bob',
    name: 'bob',
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
    commandRunner: async request =>
      request.args[0] === 'worktree' && request.args[1] === 'ps'
        ? { stdout: JSON.stringify({ ok: true, result: { truncated: false, worktrees: [] } }) }
        : { stdout: '{"ok":true}' },
    wakeupIdleCheck: async () => ({ idle: true }),
    patch: { defaultExecutor: 'inbox', gateMode: 'open' },
  }).start();
  t.after(() => runtime.stop());
  const threadId = 'thread-root-route';
  const originMailbox = 'worktree:halfbeak-2';
  // We started the thread without an explicit target mailbox, so its root outbound leg records
  // `default` as the peer reply mailbox.
  runtime.mailbox.recordOutboundEnvelope(
    envelope(threadId, { networkId: 'local', semantic: { type: 'message', threadId } }),
    [peer.nodeId],
    originMailbox,
  );
  const rootLeg = runtime.mailbox.getThread(threadId)!;
  assert.equal(rootLeg.direction, 'outbound');
  assert.equal(rootLeg.peerReplyMailbox, 'default');
  // The peer answers from its own session mailbox, which the inbound leg records on the thread.
  runtime.mailbox.recordInboundEnvelope(
    envelope('peer-reply-root', {
      networkId: 'local',
      from: peer,
      semantic: { type: 'reply', threadId, replyMailbox: 'worktree:peer-review' },
    }),
    originMailbox,
  );

  // A thread reference must not fall back to the root's `default` peer mailbox.
  const route = runtime.mailbox.resolveReplyTarget(threadId)!;
  assert.equal(route.threadId, threadId);
  assert.equal(route.targetMailbox, 'worktree:peer-review');
  assert.equal(route.localMailbox, originMailbox);

  const out: string[] = [];
  const err: string[] = [];
  const code = await runDealerCli(
    ['send', '--network', 'local', '--to', 'member:bob', '--topic', 'collab.request.v4', '--body', 'follow-up', '--thread', threadId, '--json'],
    { runtime, stdout: line => out.push(line), stderr: line => err.push(line) },
  );
  assert.equal(code, 0, err.join('\n'));
  const [delivered] = runtime.mailbox.list({ mailboxId: 'worktree:peer-review' }).items;
  assert.ok(delivered, 'the peer reply mailbox recorded on the newest inbound leg must receive the follow-up');
  assert.deepEqual(delivered.envelope.semantic, {
    type: 'message',
    threadId,
    targetMailbox: 'worktree:peer-review',
    replyMailbox: originMailbox,
  });
});

for (const agent of ['claude', 'codex'] as const) {
  test(`unbound orca-wave thread routing preserves the receiver ${agent} session`, async t => {
    const root = directory(t);
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const mailboxId = 'worktree:origin';
    const identity = { workspacePath: join(root, 'origin'), agent, sessionId: 'original-session', ...{ paneKey: 'original:pane' }, interactiveCommand: 'receiver-command' };
    store.upsertWakeTarget(mailboxId, { driver: 'orca-terminal', driverConfig: { ...identity }});
    store.recordOutboundEnvelope(envelope('request', {
      semantic: { type: 'message', threadId: 'original-thread', replyMailbox: mailboxId },
    }), [peer.nodeId], mailboxId);
    const plugin = createOrcaWaveExecutorPlugin({ store, resolveTopicBinding: () => undefined });
    await plugin.execute([envelope('reply', {
      from: peer, semantic: { type: 'message', threadId: 'original-thread' },
      payload: { workspacePath: '/sender/impostor', agent: 'antigravity' },
    })], context, {});
    const target = store.getWakeTarget(mailboxId)!;
    for (const key of Object.keys(identity) as Array<keyof typeof identity>) assert.deepEqual(target.driverConfig?.[key], identity[key], key);
    assert.equal(store.list({ mailboxId }).queuedCount, 1);
  });

  test(`fixed orca-wave ingestion preserves runtime ${agent} identity without explicit overrides`, async t => {
    const root = directory(t);
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const mailboxId = 'stream:fixed';
    const workspacePath = join(root, 'fixed');
    store.upsertWakeTarget(mailboxId, { driver: 'orca-terminal', driverConfig: {...{ paneKey: 'runtime:pane' }, workspacePath, agent, terminalHandle: 'runtime-tab', sessionId: 'runtime-session', interactiveCommand: `${agent === 'claude' ? 'claude' : 'codex'} --model test`}});
    const binding = { executor: 'orca-wave', mode: 'fixed', mailboxId, workspacePath } as const;
    const plugin = createOrcaWaveExecutorPlugin({ store, resolveTopicBinding: () => binding });
    for (const id of ['first', 'next']) {
      await plugin.execute([envelope(id, { payload: { workspacePath: '/sender/impostor', agent: 'antigravity' } })], context, {});
      const target = store.getWakeTarget(mailboxId)!;
      assert.equal(target.driverConfig?.workspacePath, workspacePath);
      assert.equal(target.driverConfig?.agent, agent);
      assert.equal(target.driverConfig?.sessionId, 'runtime-session');
      assert.equal(sessionBinding(target).paneKey, 'runtime:pane');
      assert.equal(target.driverConfig?.interactiveCommand, `${agent === 'claude' ? 'claude' : 'codex'} --model test`);
    }
    const explicit = createOrcaWaveExecutorPlugin({ store, resolveTopicBinding: () => ({
      ...binding, agent: 'antigravity', sessionId: 'configured-session', interactiveCommand: 'configured-command', resumeCommand: 'configured-command {sessionId}',
    }) });
    await explicit.execute([envelope('override')], context, {});
    const target = store.getWakeTarget(mailboxId)!;
    assert.equal(target.driverConfig?.agent, 'antigravity');
    assert.equal(target.driverConfig?.sessionId, 'configured-session');
    assert.equal(sessionBinding(target).configuredSessionId, 'configured-session');
    assert.equal(target.driverConfig?.terminalHandle, undefined);
    assert.equal(sessionBinding(target).paneKey, undefined);
    assert.equal(target.driverConfig?.interactiveCommand, 'configured-command');
    assert.equal(sessionBinding(target).resumeCommand, 'configured-command {sessionId}');
  });
}

test('partial unbound wave ingestion preserves the established runtime session', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const mailboxId = 'stream:partial';
  const workspacePath = '/receiver/workspace';
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => { calls.push(request); return { stdout: '{"ok":true}' }; };
  const plugin = createOrcaWaveExecutorPlugin({ store, resolveTopicBinding: () => undefined });
  await plugin.execute([envelope('initial', { kind: 'partial', payload: { workspacePath } })], context, {});
  store.upsertWakeTarget(mailboxId, { driverConfig: { terminalHandle: 'term-owner', sessionId: 'session-owner', agent: 'claude',...{ paneKey: 'owner:pane', configuredSessionId: 'retired-config', configuredTerminalHandle: 'original-config-handle' }}});
  const wakeup = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner, idleCheck: async () => ({ idle: true }) })});
  t.after(() => wakeup.stop());
  const next = createOrcaWaveExecutorPlugin({ store, wakeup, resolveTopicBinding: () => undefined });
  await next.execute([envelope('follow-up', { kind: 'partial', payload: {} })], context, {});
  const target = store.getWakeTarget(mailboxId)!;
  assert.equal(target.driverConfig?.workspacePath, workspacePath);
  assert.equal(target.driverConfig?.terminalHandle, 'term-owner');
  assert.equal(sessionBinding(target).paneKey, 'owner:pane');
  assert.equal(target.driverConfig?.sessionId, 'session-owner');
  assert.equal(target.driverConfig?.agent, 'claude');
  assert.equal(sessionBinding(target).configuredSessionId, 'retired-config');
  assert.equal(sessionBinding(target).configuredTerminalHandle, 'original-config-handle');
  assert.equal(terminalSends(calls).length, 1);
  assert.equal(terminalOf(terminalSends(calls)[0]), 'term-owner');
  assert.equal(calls.some(call => call.args[1] === 'create'), false);
  await plugin.execute([envelope('replacement', { kind: 'partial', payload: { workspacePath: '/receiver/replacement' } })], context, {});
  const replaced = store.getWakeTarget(mailboxId)!;
  assert.equal(replaced.driverConfig?.workspacePath, '/receiver/replacement');
  assert.equal(replaced.driverConfig?.sessionId, undefined);
  assert.equal(replaced.driverConfig?.terminalHandle, undefined);
  assert.equal(sessionBinding(replaced).paneKey, undefined);
});
