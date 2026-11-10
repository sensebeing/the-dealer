import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  DealerNodeRuntime,
  MailboxStore,
  WakeupRedriveService,
  createOrcaSpawnExecutorPlugin,
  runDealerCli,
  verifyEnvelope,
} from '../packages/index.ts';
import type {
  CommandRequest,
  CommandRunner,
  ExecutionContext,
  MessageEnvelope,
} from '../packages/index.ts';

function directory(t: TestContext): string {
  const root = mkdtempSync(join(tmpdir(), 'dealer-wakeup-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return root;
}

let counter = 0;
function alert(id: string, patch: Partial<MessageEnvelope> = {}): MessageEnvelope {
  counter += 1;
  return {
    id,
    networkId: 'team',
    traceId: id,
    from: { nodeId: 'ed25519:' + 'a'.repeat(64), member: 'sender', nodeName: 'laptop' },
    routing: { mode: 'unicast' },
    kind: 'alerts',
    priority: 'normal',
    subject: `Alert ${counter}`,
    body: 'alert body',
    payload: {},
    attachments: [],
    createdAt: new Date().toISOString(),
    signature: '',
    ...patch,
  };
}

function textOf(call: CommandRequest): string {
  return String(call.args[call.args.indexOf('--text') + 1]);
}

function bodyOf(call: CommandRequest): string {
  return String(call.args[call.args.indexOf('--body') + 1]);
}

function terminalSends(calls: readonly CommandRequest[]): CommandRequest[] {
  return calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'send');
}

async function cli(runtime: DealerNodeRuntime, ...args: string[]) {
  const out: string[] = [];
  const err: string[] = [];
  const code = await runDealerCli([...args, '--json'], {
    runtime,
    stdout: line => out.push(line),
    stderr: line => err.push(line),
  });
  assert.equal(code, 0, err.join('\n') || out.join('\n'));
  return JSON.parse(out.join('\n'));
}

async function until(check: () => boolean, message: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!check()) {
    assert.ok(Date.now() < deadline, `Timed out: ${message}`);
    await new Promise(resolve => setTimeout(resolve, 25));
  }
}

async function makeNode(
  t: TestContext,
  root: string,
  member: string,
  options: {
    patch?: Record<string, unknown>;
    commandRunner?: CommandRunner;
  } = {},
) {
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, member),
    member,
    name: member,
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
    commandRunner:
      options.commandRunner ??
      (async () => {
        throw new Error('Unexpected external command');
      }),
    patch: { defaultExecutor: 'inbox', ...(options.patch ?? {}) },
  }).start();
  t.after(() => runtime.stop());
  return runtime;
}

test('daemon restart preserves busy waves and delivers exactly one single-line pointer', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    return { stdout: '{"ok":true}' };
  };
  let idle = false;
  const stateDir = join(root, 'state');
  const workspacePath = join(root, 'workspace');
  const make = () =>
    new DealerNodeRuntime({
      stateDir,
      member: 'worker',
      name: 'worker',
      profile: 'dedicated-worker',
      transport: { discoveryDir: join(root, 'mesh') },
      commandRunner: runner,
      wakeupIdleCheck: async () => ({ idle }),
      patch: {
        defaultExecutor: 'orca-wave',
        executorConfig: {
          'orca-wave': { runId: 'run-alerts', workspacePath },
        },
        wave: { flushIntervalMs: 100_000 },
      },
    });
  const first = await make().start();
  const emit = () =>
    first.emitter.emit({
      networkId: 'local',
      routing: { mode: 'unicast', targets: ['member:worker'] },
      kind: 'alerts',
      subject: 'Busy alert',
      body: 'details',
      payload: {
        executor: 'orca-wave',
        streamKey: 'alerts',
        runId: 'run-alerts',
        workspacePath,
      },
    });
  assert.equal((await emit()).receipts[0].stage, 'dispatched');
  assert.equal((await emit()).receipts[0].stage, 'dispatched');
  assert.equal(calls.length, 0, 'a busy workspace must never receive text');
  assert.equal(first.mailbox.list({ mailboxId: 'stream:alerts' }).queuedCount, 2);
  assert.equal(first.mailbox.getWakeTarget('stream:alerts')!.pendingWake, true);
  await first.stop();

  calls.length = 0;
  idle = true;
  const second = await make().start();
  t.after(() => second.stop());
  await second.flushWakeups();
  const orchestration = calls.filter(call => call.args[0] === 'orchestration');
  assert.equal(orchestration.length, 1);
  assert.equal(
    orchestration[0].args[orchestration[0].args.indexOf('--subject') + 1],
    'Dealer wakeup (stream:alerts)',
  );
  const pointer = bodyOf(orchestration[0]);
  assert.match(pointer, /^You have 2 Dealer message\(s\) in stream:alerts/);
  assert.equal(/[\r\n]/.test(pointer), false);
  assert.equal(second.mailbox.getWakeTarget('stream:alerts')!.pendingWake, false);

  const lease = second.mailbox.check({ mailboxId: 'stream:alerts' });
  assert.match(lease.deliveryId!, /^dlv_[0-9a-f]{12}$/);
  assert.equal(lease.items.length, 2);
  assert.equal(new Set(lease.items.map(item => item.leaseId)).size, 1);
});

test('unacked recovery pointer suppresses rotation, and /new follows the ack', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({
    store,
    commandRunner: runner,
    idleCheck: async () => ({ idle: true }),
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());
  const enqueue = (ids: readonly string[]) =>
    store.enqueue(ids.map(id => alert(id)), {
      mailboxId: 'stream:alerts',
      wakeTarget: {
        mode: 'orca-terminal',
        terminalHandle: 'term-alerts',
        workspacePath: root,
        coalesceMs: 0,
        rotateAfterWaves: 1,
        workspaceComment: 'Dealer: alerts',
      },
    });

  enqueue(['a', 'b']);
  await service.flushMailbox('stream:alerts');
  const firstSends = terminalSends(calls);
  assert.equal(firstSends.length, 1);
  assert.match(textOf(firstSends[0]), /^You have 2 Dealer message\(s\) in stream:alerts/);
  assert.equal(calls.some(call => call.args.includes('/new')), false);

  const lease = store.check({ mailboxId: 'stream:alerts' });
  enqueue(['c']);
  await service.flushMailbox('stream:alerts');
  const dual = terminalSends(calls).at(-1)!;
  assert.match(
    textOf(dual),
    new RegExp(`^Unacknowledged Dealer batch ${lease.deliveryId} \\+ 1 newer message\\(s\\) in stream:alerts\\.`),
  );
  assert.equal(calls.some(call => call.args.includes('/new')), false);

  const beforeAck = calls.length;
  store.ack(lease.deliveryId!);
  await service.flushMailbox('stream:alerts');
  await service.flushMailbox('stream:alerts');
  const afterAck = calls.slice(beforeAck);
  const rotated = terminalSends(afterAck);
  assert.equal(rotated.length, 2);
  assert.equal(textOf(rotated[0]), '/new');
  assert.match(textOf(rotated[1]), /^You have 1 Dealer message\(s\) in stream:alerts/);
  assert.equal(/[\r\n]/.test(textOf(rotated[1])), false);
  assert.equal(store.getWakeTarget('stream:alerts')!.completedWaves, 0);
});

test('orca-spawn never injects after a failed idle wait and redrive sends the task preamble', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  let idle = false;
  let waitOk = false;
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'create') {
      return {
        stdout: JSON.stringify({ ok: true, result: { worktree: { path: join(root, 'child') } } }),
      };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'create') {
      return {
        stdout: JSON.stringify({ ok: true, result: { terminal: { handle: 'term-worker' } } }),
      };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'wait') {
      return {
        stdout: JSON.stringify(
          waitOk ? { ok: true } : { ok: false, error: { code: 'timeout' } },
        ),
      };
    }
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({
    store,
    commandRunner: runner,
    idleCheck: async () => ({ idle }),
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());
  const executor = createOrcaSpawnExecutorPlugin({
    commandRunner: runner,
    command: 'codex',
    cwd: join(root, 'workspace'),
    store,
    wakeup: service,
  });

  const timedOut = alert('task-timeout', { payload: { terminalPrompt: 'do the work' } });
  await executor.execute([timedOut], {} as ExecutionContext);
  assert.equal(terminalSends(calls).length, 0, 'must not send on a failed idle wait');
  assert.equal(store.getWakeTarget('task:task-timeout')!.pendingWake, true);
  assert.equal(store.list({ mailboxId: 'task:task-timeout' }).queuedCount, 1);

  idle = true;
  await service.flushMailbox('task:task-timeout');
  const preambleSends = terminalSends(calls);
  assert.equal(preambleSends.length, 1);
  const preamble = textOf(preambleSends[0]);
  assert.match(
    preamble,
    /^Dealer Task dispatched to mailbox task:task-timeout \(Message ID: task-timeout\)\./,
  );
  assert.equal(/[\r\n]/.test(preamble), false);
  assert.ok(preamble.includes('dealer ask --reply-to task-timeout'));
  assert.ok(preamble.includes('dealer settle task-timeout --outcome succeeded'));
  assert.equal(store.getWakeTarget('task:task-timeout')!.pendingWake, false);

  calls.length = 0;
  waitOk = true;
  const ready = alert('task-ready', { payload: { terminalPrompt: 'also do this' } });
  await executor.execute([ready], {} as ExecutionContext);
  const directSends = terminalSends(calls);
  assert.equal(directSends.length, 1);
  assert.match(
    textOf(directSends[0]),
    /^Dealer Task dispatched to mailbox task:task-ready \(Message ID: task-ready\)\./,
  );
  assert.equal(store.getWakeTarget('task:task-ready')!.pendingWake, false);
});

test('single-machine loopback send -> ask -> reply -> settle keeps both thread legs', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'node'),
    member: 'alice',
    name: 'alice',
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
    commandRunner: async request => {
      calls.push(request);
      return { stdout: '{"ok":true}' };
    },
    wakeupIdleCheck: async () => ({ idle: true }),
    patch: { defaultExecutor: 'inbox', gateMode: 'open' },
  }).start();
  t.after(() => runtime.stop());

  const sent = await cli(
    runtime,
    'send',
    '--network',
    'local',
    '--to',
    'member:alice',
    '--topic',
    'triage',
    '--mailbox',
    'task:w1',
    '--reply-mailbox',
    'worktree:coord',
    '--subject',
    'Please fix',
    '--body',
    'Investigate the bug',
  );
  assert.equal(sent.receipts[0].stage, 'dispatched');
  const messageId = String(sent.messageId);
  assert.equal(runtime.mailbox.list({ mailboxId: 'task:w1' }).queuedCount, 1);
  const outboundLeg = runtime.mailbox
    .listThreadRecords(messageId)
    .find(record => record.direction === 'outbound')!;
  assert.equal(outboundLeg.localMailbox, 'worktree:coord');
  assert.equal(outboundLeg.peerReplyMailbox, 'task:w1');

  const checked = await cli(runtime, 'inbox', 'check', '--mailbox', 'task:w1');
  const asked = await cli(
    runtime,
    'ask',
    '--reply-to',
    messageId,
    '--body',
    'Which branch?',
    '--ack',
    String(checked.deliveryId),
  );
  assert.equal(asked.threadId, messageId);
  assert.equal(asked.targetMailbox, 'worktree:coord');
  assert.equal(asked.replyMailbox, 'task:w1');
  assert.equal(runtime.mailbox.list({ mailboxId: 'worktree:coord' }).queuedCount, 1);

  // The inbound leg must never be clobbered by the emitter's outbound leg on loopback.
  const inboundLeg = runtime.mailbox
    .listThreadRecords(messageId)
    .find(record => record.direction === 'inbound')!;
  assert.equal(inboundLeg.localMailbox, 'task:w1');
  assert.equal(inboundLeg.peerReplyMailbox, 'worktree:coord');
  const route = runtime.mailbox.resolveReplyTarget(String(asked.questionId))!;
  assert.equal(route.targetMailbox, 'task:w1');
  assert.equal(route.localMailbox, 'worktree:coord');

  const coordinator = await cli(runtime, 'inbox', 'check', '--mailbox', 'worktree:coord');
  assert.deepEqual(
    coordinator.items.map((item: MessageEnvelope) => item.id),
    [asked.questionId],
  );

  runtime.mailbox.upsertWakeTarget('task:w1', {
    mode: 'orca-terminal',
    terminalHandle: 'term-w1',
    workspacePath: join(root, 'workspace-w1'),
    coalesceMs: 0,
  });
  const replied = await cli(
    runtime,
    'reply',
    String(asked.questionId),
    '--body',
    'Use main',
    '--ack',
    String(coordinator.deliveryId),
  );
  assert.equal(replied.targetMailbox, 'task:w1');
  await runtime.flushWakeups();
  const wakePointer = terminalSends(calls)
    .map(textOf)
    .find(text => text.includes('task:w1'))!;
  assert.match(wakePointer, /^You have 1 Dealer message\(s\) in task:w1/);
  assert.equal(/[\r\n]/.test(wakePointer), false);

  const replyCheck = await cli(runtime, 'inbox', 'check', '--mailbox', 'task:w1');
  assert.deepEqual(
    replyCheck.items.map((item: MessageEnvelope) => item.id),
    [replied.replyId],
  );
  const settled = await cli(
    runtime,
    'settle',
    messageId,
    '--outcome',
    'succeeded',
    '--summary',
    'All done',
    '--ack',
    String(replyCheck.deliveryId),
  );
  assert.equal(settled.outcome, 'succeeded');
  assert.equal(settled.targetMailbox, 'worktree:coord');

  const thread = await cli(runtime, 'thread', 'show', messageId);
  assert.equal(thread.threadId, messageId);
  assert.equal(thread.state, 'settled');
  assert.equal(thread.settlement.outcome, 'succeeded');
  assert.equal(thread.settlement.summary, 'All done');
  assert.ok(
    thread.records.some(
      (record: { direction: string; messageId: string }) =>
        record.direction === 'inbound' && record.messageId === messageId,
    ),
  );
  assert.ok(
    thread.records.some(
      (record: { direction: string; messageId: string }) =>
        record.direction === 'outbound' && record.messageId === messageId,
    ),
  );
  const storedSettlement = runtime.mailbox
    .list({ mailboxId: 'worktree:coord' })
    .items.find(item => item.id === settled.settlementId)!;
  assert.equal(storedSettlement.envelope.semantic?.type, 'settle');
  assert.equal(storedSettlement.envelope.semantic?.outcome, 'succeeded');
});

test('two-node P2P closed loop verifies signatures and keeps unapproved knocks route-free', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    return { stdout: '{"ok":true}' };
  };
  const alice = await makeNode(t, root, 'alice', { commandRunner: runner });
  const bob = await makeNode(t, root, 'bob', {
    commandRunner: runner,
    patch: { gateMode: 'approval_required' },
  });
  await alice.mountNetwork({ networkId: 'team', passphrase: 'team-secret' });
  await bob.mountNetwork({ networkId: 'team', passphrase: 'team-secret' });
  await until(
    () => alice.node.listPeers().length === 1 && bob.node.listPeers().length === 1,
    'peer discovery',
  );

  const sent = await cli(
    alice,
    'send',
    '--network',
    'team',
    '--to',
    'member:bob',
    '--topic',
    'triage',
    '--mailbox',
    'task:bob1',
    '--reply-mailbox',
    'worktree:alice',
    '--subject',
    'Please review',
    '--body',
    'Review the change',
  );
  const messageId = String(sent.messageId);
  assert.equal(sent.receipts[0].stage, 'pending_approval');
  assert.equal(bob.gate.listPendingApprovals().length, 1);
  assert.equal(bob.mailbox.getThread(messageId), undefined);
  assert.equal(bob.mailbox.list({ mailboxId: 'task:bob1' }).totalCount, 0);
  assert.equal(alice.mailbox.getThread(messageId)!.direction, 'outbound');

  await cli(bob, 'gate', 'approve', messageId);
  const inboundLeg = bob.mailbox.getThread(messageId)!;
  assert.equal(inboundLeg.direction, 'inbound');
  assert.equal(inboundLeg.localMailbox, 'task:bob1');
  assert.equal(inboundLeg.peerReplyMailbox, 'worktree:alice');
  const delivered = bob.mailbox.list({ mailboxId: 'task:bob1' }).items[0];
  assert.ok(verifyEnvelope(delivered.envelope));
  assert.equal(delivered.envelope.from.nodeId, alice.transport.localNodeId);

  await cli(bob, 'gate', 'mode', 'open', '--network', 'team');
  const checked = await cli(bob, 'inbox', 'check', '--mailbox', 'task:bob1');
  const asked = await cli(
    bob,
    'ask',
    '--reply-to',
    messageId,
    '--body',
    'Which test?',
    '--ack',
    String(checked.deliveryId),
  );
  assert.equal(asked.targetMailbox, 'worktree:alice');
  assert.equal(asked.replyMailbox, 'task:bob1');
  assert.equal(asked.receipts[0].stage, 'dispatched');
  assert.equal(asked.receipts[0].fromNodeId, alice.transport.localNodeId);

  const aliceInbox = await cli(alice, 'inbox', 'check', '--mailbox', 'worktree:alice');
  assert.deepEqual(
    aliceInbox.items.map((item: MessageEnvelope) => item.id),
    [asked.questionId],
  );
  const questionItem = alice.mailbox
    .list({ mailboxId: 'worktree:alice' })
    .items.find(item => item.id === asked.questionId)!;
  assert.ok(verifyEnvelope(questionItem.envelope));

  const replied = await cli(
    alice,
    'reply',
    String(asked.questionId),
    '--body',
    'The integration test',
    '--ack',
    String(aliceInbox.deliveryId),
  );
  assert.equal(replied.targetMailbox, 'task:bob1');
  assert.equal(replied.receipts[0].stage, 'dispatched');
  assert.equal(replied.receipts[0].fromNodeId, bob.transport.localNodeId);

  const bobInbox = await cli(bob, 'inbox', 'check', '--mailbox', 'task:bob1');
  assert.deepEqual(
    bobInbox.items.map((item: MessageEnvelope) => item.id),
    [replied.replyId],
  );
  const replyItem = bob.mailbox
    .list({ mailboxId: 'task:bob1' })
    .items.find(item => item.id === replied.replyId)!;
  assert.ok(verifyEnvelope(replyItem.envelope));

  const settled = await cli(
    bob,
    'settle',
    messageId,
    '--outcome',
    'succeeded',
    '--summary',
    'All checks passed',
    '--ack',
    String(bobInbox.deliveryId),
  );
  assert.equal(settled.targetMailbox, 'worktree:alice');
  assert.equal(settled.receipts[0].stage, 'dispatched');
  assert.equal(settled.receipts[0].fromNodeId, alice.transport.localNodeId);

  const aliceThread = await cli(alice, 'thread', 'show', messageId);
  assert.equal(aliceThread.state, 'settled');
  assert.equal(aliceThread.settlement.outcome, 'succeeded');
  assert.equal(aliceThread.settlement.summary, 'All checks passed');
  const bobThread = await cli(bob, 'thread', 'show', messageId);
  assert.equal(bobThread.state, 'settled');
  assert.equal(bobThread.settlement.outcome, 'succeeded');
  assert.ok(
    aliceThread.records.some(
      (record: { peerNodeId: string }) => record.peerNodeId === bob.transport.localNodeId,
    ),
  );
  assert.ok(
    bobThread.records.some(
      (record: { peerNodeId: string }) => record.peerNodeId === alice.transport.localNodeId,
    ),
  );
});

test('thread show fails loudly for an unknown thread', async t => {
  const root = directory(t);
  const runtime = await makeNode(t, root, 'solo');
  const out: string[] = [];
  const err: string[] = [];
  const code = await runDealerCli(['thread', 'show', 'missing', '--json'], {
    runtime,
    stdout: line => out.push(line),
    stderr: line => err.push(line),
  });
  assert.equal(code, 1);
  assert.match(JSON.parse(err.join('\n')).error, /Unknown thread: missing/);
});
