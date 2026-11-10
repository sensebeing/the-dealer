import { checkOrcaWorkspaceIdle, isAutopilotComposerPrefill, sendOrcaTerminalText } from '../packages/orca-workspace/index.ts';
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
    new RegExp(`^Unacknowledged Dealer batch ${lease.deliveryId} \\+ 1 newer message\\(s\\) in stream:alerts \\[`),
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

test('/new waits for post-rotation tui-idle before delivering the pointer', async t => {
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
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());
  store.enqueue([alert('rotate-now')], {
    mailboxId: 'stream:rotate',
    wakeTarget: {
      mode: 'orca-run',
      runId: 'run-rotate',
      terminalHandle: 'term-rotate',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      completedWaves: 1,
    },
  });

  const result = await service.flushMailbox('stream:rotate');
  assert.equal(result.woke, true);
  assert.equal(result.rotated, true);
  const newIndex = calls.findIndex(
    call => call.args[0] === 'terminal' && call.args[1] === 'send' && textOf(call) === '/new',
  );
  const waitIndex = calls.findIndex(
    call => call.args[0] === 'terminal' && call.args[1] === 'wait',
  );
  const pointerIndex = calls.findIndex(call => call.args[0] === 'orchestration');
  assert.ok(newIndex >= 0);
  assert.ok(waitIndex > newIndex);
  assert.equal(calls[waitIndex].args[calls[waitIndex].args.indexOf('--for') + 1], 'tui-idle');
  assert.equal(calls[waitIndex].args[calls[waitIndex].args.indexOf('--timeout-ms') + 1], '5000');
  assert.ok(pointerIndex > waitIndex);
  assert.match(bodyOf(calls[pointerIndex]), /^You have 1 Dealer message\(s\) in stream:rotate/);
  assert.equal(store.getWakeTarget('stream:rotate')!.pendingWake, false);
  assert.equal(store.getWakeTarget('stream:rotate')!.completedWaves, 0);
});

test('/new defers the pointer until the rotated terminal reports idle again', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  let waitOk = false;
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'terminal' && request.args[1] === 'wait') {
      return { stdout: JSON.stringify({ ok: waitOk }) };
    }
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({
    store,
    commandRunner: runner,
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());
  store.enqueue([alert('rotate-later')], {
    mailboxId: 'stream:rotate',
    wakeTarget: {
      mode: 'orca-run',
      runId: 'run-rotate',
      terminalHandle: 'term-rotate',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      completedWaves: 1,
    },
  });

  const first = await service.flushMailbox('stream:rotate');
  assert.equal(first.woke, false);
  assert.equal(first.rotated, true);
  assert.equal(first.reason, 'Session rotated; waiting for tui-idle before wakeup');
  assert.equal(calls.some(call => call.args[0] === 'orchestration'), false);
  assert.equal(
    calls.filter(
      call => call.args[0] === 'terminal' && call.args[1] === 'send' && textOf(call) === '/new',
    ).length,
    1,
  );
  assert.equal(store.getWakeTarget('stream:rotate')!.pendingWake, true);
  assert.equal(store.getWakeTarget('stream:rotate')!.completedWaves, 0);

  waitOk = true;
  const before = calls.length;
  const second = await service.flushWakeups();
  assert.equal(second.length, 1);
  assert.equal(second[0].woke, true);
  assert.equal(second[0].rotated, false);
  const followUp = calls.slice(before);
  assert.equal(
    followUp.some(
      call => call.args[0] === 'terminal' && call.args[1] === 'send' && textOf(call) === '/new',
    ),
    false,
  );
  const pointer = followUp.find(call => call.args[0] === 'orchestration')!;
  assert.match(pointer.args[pointer.args.indexOf('--body') + 1], /^You have 1 Dealer message\(s\) in stream:rotate/);
  assert.equal(store.getWakeTarget('stream:rotate')!.pendingWake, false);
  assert.equal(store.getWakeTarget('stream:rotate')!.completedWaves, 0);
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

  const timedOut = alert('task-timeout', { payload: { interactive: true } });
  await executor.execute([timedOut], {} as ExecutionContext);
  assert.equal(terminalSends(calls).length, 0, 'must not send on a failed idle wait');
  assert.equal(store.getWakeTarget('task:task-timeout')!.pendingWake, true);
  assert.equal(store.list({ mailboxId: 'task:task-timeout' }).queuedCount, 1);

  await until(() => store.getWakeTarget('task:task-timeout')!.attemptCount > 0, 'claim release probes busy target');
  idle = true;
  await service.flushMailbox('task:task-timeout', { force: true });
  const preambleSends = terminalSends(calls);
  assert.equal(preambleSends.length, 1);
  const preamble = textOf(preambleSends[0]);
  assert.match(
    preamble,
    /^alert body — Dealer Task dispatched to mailbox task:task-timeout \(Message ID: task-timeout\)\./,
  );
  assert.equal(/[\r\n]/.test(preamble), false);
  assert.ok(preamble.includes('dealer ask --reply-to task-timeout'));
  assert.ok(preamble.includes('dealer settle task-timeout --outcome succeeded'));
  assert.equal(store.getWakeTarget('task:task-timeout')!.pendingWake, false);

  calls.length = 0;
  waitOk = true;
  const ready = alert('task-ready', { payload: { interactive: true } });
  await executor.execute([ready], {} as ExecutionContext);
  const directSends = terminalSends(calls);
  assert.equal(directSends.length, 1);
  assert.match(
    textOf(directSends[0]),
    /^alert body — Dealer Task dispatched to mailbox task:task-ready \(Message ID: task-ready\)\./,
  );
  assert.equal(store.getWakeTarget('task:task-ready')!.pendingWake, false);

  const promptCases = [
    { terminalPrompt: '  Custom\n task\r\n prompt  ', body: 'Body', subject: 'Subject', lead: 'Custom task prompt' },
    { terminalPrompt: ' \n ', body: '  Body\n details  ', subject: 'Subject', lead: 'Body details' },
    { terminalPrompt: 42, body: '', subject: '  Subject\n fallback ', lead: 'Subject fallback' },
    { terminalPrompt: undefined, body: '', subject: '', lead: '' },
  ];
  for (const [index, promptCase] of promptCases.entries()) {
    for (const direct of [false, true]) {
      calls.length = 0;
      waitOk = direct;
      const message = alert(`prompt-${index}-${direct}`, {
        body: promptCase.body,
        subject: promptCase.subject,
        payload: { interactive: true, terminalPrompt: promptCase.terminalPrompt },
      });
      await executor.execute([message], {} as ExecutionContext);
      if (!direct) {
        assert.equal(terminalSends(calls).length, 0);
        await service.flushMailbox(`task:${message.id}`);
      }
      const sends = terminalSends(calls);
      assert.equal(sends.length, 1);
      const text = textOf(sends[0]);
      const prefix = promptCase.lead ? `${promptCase.lead} — ` : '';
      assert.ok(text.startsWith(`${prefix}Dealer Task dispatched to mailbox task:${message.id}`));
      assert.equal(/[\r\n]/.test(text), false);
    }
  }
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

test('session commands honor an explicit --mailbox when acknowledging a lease', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'node'),
    member: 'alice',
    name: 'alice',
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
    commandRunner: async () => ({ stdout: '{"ok":true}' }),
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
    '--body',
    'Investigate',
  );
  const messageId = String(sent.messageId);

  runtime.mailbox.enqueue([alert('ack-ask')], { mailboxId: 'custom:ask' });
  const askLease = runtime.mailbox.check({ mailboxId: 'custom:ask' });
  const asked = await cli(
    runtime,
    'ask',
    '--reply-to',
    messageId,
    '--body',
    'Which branch?',
    '--mailbox',
    'custom:ask',
    '--ack',
    String(askLease.deliveryId),
  );
  assert.equal(asked.targetMailbox, 'worktree:coord');
  assert.equal(asked.replyMailbox, 'custom:ask');
  assert.equal(
    runtime.mailbox
      .list({ mailboxId: 'custom:ask', includeAcked: true })
      .items.find(item => item.id === 'ack-ask')!.status,
    'acked',
  );

  runtime.mailbox.enqueue([alert('ack-reply')], { mailboxId: 'custom:reply' });
  const replyLease = runtime.mailbox.check({ mailboxId: 'custom:reply' });
  const replied = await cli(
    runtime,
    'reply',
    messageId,
    '--body',
    'Use main',
    '--mailbox',
    'custom:reply',
    '--ack',
    String(replyLease.deliveryId),
  );
  assert.equal(replied.targetMailbox, 'worktree:coord');
  assert.equal(
    runtime.mailbox
      .list({ mailboxId: 'custom:reply', includeAcked: true })
      .items.find(item => item.id === 'ack-reply')!.status,
    'acked',
  );
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

test('Autopilot composer recognition preserves human drafts and clears only before pointer delivery', async t => {
  const suggestion = '继续检查剩余任务';
  for (const [preview, prefilled] of [
    [`• ,1（主推）：${suggestion}\n> ${suggestion}`, true],
    [`下一步建议\n> ${suggestion}\nstatus bar`, true],
    [`自动预填\n> ${suggestion}`, true],
    [`• ,1 (主推)\n> ${suggestion}`, true],
    [`previous ${suggestion}\n> ${suggestion}`, true],
    ['history\n> unsent draft', false],
    ['下一步建议\n>', false],
    ['> unsent draft\n自动预填', false],
  ] as const) {
    assert.equal(isAutopilotComposerPrefill(preview), prefilled, preview);
  }
  const root = directory(t);
  const calls: CommandRequest[] = [];
  let preview = `• ,1（主推）：${suggestion}\n> ${suggestion}`;
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'ps') return { stdout: JSON.stringify({ ok: true, result: { worktrees: [
      { path: root, worktreeId: 'root', unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
    ] } }) };
    if (request.args[1] === 'list') return { stdout: JSON.stringify({ ok: true, result: { terminals: [
      { handle: 'term', connected: true, writable: true, preview },
    ] } }) };
    return { stdout: '{"ok":true}' };
  };
  assert.deepEqual(await checkOrcaWorkspaceIdle(runner, 'orca', root), { idle: true, composerPrefilled: true, reason: undefined });
  const childPath = join(root, 'child');
  for (const childPreview of [
    `• ,1（主推）：${suggestion}\n> ${suggestion}`,
    `• ,1（主推）：${suggestion}\n> ${suggestion}\nstatus bar`,
  ]) {
    const childRunner: CommandRunner = async request => {
      if (request.args[0] === 'worktree') return { stdout: JSON.stringify({ ok: true, result: { worktrees: [
        { path: root, worktreeId: 'root', childWorktreeIds: ['child'], unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
        { path: childPath, worktreeId: 'child', unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
      ] } }) };
      assert.equal(request.args[1], 'list');
      return { stdout: JSON.stringify({ ok: true, result: { terminals: [
        { handle: 'term', connected: true, writable: true, preview: request.args.includes(`path:${childPath}`) ? childPreview : '>' },
      ] } }) };
    };
    const result = await checkOrcaWorkspaceIdle(childRunner, 'orca', root);
    assert.equal(result.idle, true);
    assert.equal(Boolean(result.composerPrefilled), false);
  }
  preview = 'history\n> unsent draft';
  assert.equal((await checkOrcaWorkspaceIdle(runner, 'orca', root)).idle, false);
  preview = `• ,1（主推）：${suggestion}\n> ${suggestion}\nstatus bar`;
  const store = new MailboxStore(join(root, 'mailbox.json'));
  store.enqueue([alert('prefill')], { mailboxId: 'default', wakeTarget: { mode: 'orca-terminal', terminalHandle: 'term', workspacePath: root } });
  const wakeup = new WakeupRedriveService({ store, commandRunner: runner });
  t.after(() => wakeup.stop());
  assert.equal((await wakeup.flushMailbox('default', { force: true })).woke, true);
  const sends = terminalSends(calls);
  assert.equal(sends.length, 2);
  assert.equal(textOf(sends[0]), '\x15');
  assert.equal(sends[0].args.includes('--enter'), false);
  assert.match(textOf(sends[1]), /^You have 1 Dealer message\(s\) in default/);
  assert.ok(sends[1].args.includes('--enter'));
  const failedCalls: CommandRequest[] = [];
  await assert.rejects(sendOrcaTerminalText(async call => {
    failedCalls.push(call);
    return { stdout: '{"ok":false}' };
  }, 'orca', 'term', 'pointer', { clearComposer: true }), /clear failed/);
  assert.equal(failedCalls.length, 1);
});

test('inline wakeup previews include bounded latest body, semantic sender and thread on one line', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const calls: CommandRequest[] = [];
  const wakeup = new WakeupRedriveService({ store, commandRunner: async call => {
    calls.push(call);
    return { stdout: '{"ok":true}' };
  } });
  t.after(() => wakeup.stop());
  for (const [id, body, semantic] of [
    ['long', 'x'.repeat(450) + '\r\nend', { type: 'question', threadId: 'thread-1' }],
    ['short', 'hello\r\nworld', { type: 'message', threadId: 'thread-2' }],
    ['empty', ' \n ', undefined],
  ] as const) {
    const claim = wakeup.claim(id);
    store.enqueue([alert(id, { body, semantic })], { mailboxId: id, wakeTarget: { mode: 'orca-run', runId: 'run' } });
    await wakeup.flushMailbox(id);
    claim.dispose();
    const result = await wakeup.flushMailbox(id, { force: true });
    const pointer = result.pointer!;
    assert.match(pointer, new RegExp(`^You have 1 Dealer message\\(s\\) in ${id}`));
    assert.equal(/[\r\n]/.test(pointer), false);
    if (id === 'long') {
      assert.ok(pointer.includes(`Latest (question from sender, thread thread-1): "${'x'.repeat(400)}...".`));
      assert.ok(!pointer.includes('x'.repeat(401)));
    } else if (id === 'short') {
      assert.ok(pointer.includes('Latest (alerts from sender, thread thread-2): "hello world".'));
    } else assert.ok(!pointer.includes('Latest'));
  }
});

test('only newly inserted conversational frames bypass coalescing and never reset retry backoff', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 10_000 });
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  for (const type of ['question', 'reply', 'settle'] as const) {
    store.enqueue([alert(type, { semantic: { type, threadId: 'thread', ...(type === 'settle' ? { outcome: 'succeeded' as const } : {}) } })], {
      mailboxId: type, wakeTarget: { mode: 'orca-run', runId: 'run', coalesceMs: 5000 },
    });
    assert.equal(store.getWakeTarget(type)?.earliestWakeAt, 10_000);
  }
  store.enqueue([alert('normal'), alert('control', { semantic: { type: 'reply', threadId: 'thread' } })], {
    mailboxId: 'mixed', wakeTarget: { mode: 'orca-run', runId: 'run', coalesceMs: 5000 },
  });
  assert.equal(store.getWakeTarget('mixed')?.earliestWakeAt, 15_000);
  store.enqueue([alert('normal'), alert('new-control', { semantic: { type: 'question', threadId: 'thread' } })], { mailboxId: 'mixed' });
  assert.equal(store.getWakeTarget('mixed')?.earliestWakeAt, 10_000, 'duplicates do not affect classification');
  store.upsertWakeTarget('mixed', { attemptCount: 2, earliestWakeAt: 30_000 });
  store.enqueue([alert('retry', { semantic: { type: 'settle', threadId: 'thread', outcome: 'succeeded' } })], { mailboxId: 'mixed' });
  assert.equal(store.getWakeTarget('mixed')?.earliestWakeAt, 30_000);
  const calls: CommandRequest[] = [];
  const wakeup = new WakeupRedriveService({ store, now: () => 10_000, commandRunner: async call => {
    calls.push(call);
    return { stdout: '{"ok":true}' };
  } });
  t.after(() => wakeup.stop());
  assert.equal((await wakeup.flushReady()).filter(result => result.woke).length, 3);
  assert.equal(calls.length, 3);
});

test('outstanding lease exposes a cloned latest queued reply in both check and peek', t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.enqueue([alert('task')], { mailboxId: 'box' });
  const lease = store.check({ mailboxId: 'box' });
  store.enqueue([alert('reply', { body: 'The answer', semantic: { type: 'reply', threadId: 'task' } })], { mailboxId: 'box' });
  for (const peek of [false, true]) {
    const preview = store.check({ mailboxId: 'box', peek });
    assert.equal(preview.deliveryId, lease.deliveryId);
    assert.deepEqual(preview.items.map(item => item.id), ['task']);
    assert.equal(preview.newerQueuedCount, 1);
    assert.equal(preview.latestQueued?.id, 'reply');
    preview.latestQueued!.envelope.body = 'mutated';
    assert.equal(store.check({ mailboxId: 'box', peek: true }).latestQueued!.envelope.body, 'The answer');
  }
});

for (const mode of ['passive', 'orca-run', 'orca-terminal'] as const) {
  test(`${mode} wake delivery redrives newer sequences immediately after its in-flight command`, async t => {
    const store = new MailboxStore(join(directory(t), 'mailbox.json'));
    const mailboxId = 'box';
    store.enqueue([alert('old')], { mailboxId });
    store.check({ mailboxId });
    store.enqueue([alert('first', { semantic: { type: 'reply', threadId: 'old' } })], { mailboxId,
      wakeTarget: { mode, ...(mode === 'passive' ? { hostAgent: 'claude-code', conversationMode: 'spawn' } : {}), runId: 'run', terminalHandle: 'term' } });
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const pointers: string[] = [];
    const service = new WakeupRedriveService({ store, flushIntervalMs: 100_000, idleCheck: async () => ({ idle: true }), commandRunner: async call => {
      if (mode !== 'orca-terminal' || call.args[1] === 'send') {
        pointers.push(mode === 'passive' ? call.args.at(-1)! : mode === 'orca-run' ? bodyOf(call) : textOf(call));
        if (pointers.length === 1) await held;
      }
      return { stdout: '{"ok":true}' };
    } });
    t.after(() => service.stop());
    const first = service.flushMailbox(mailboxId);
    await until(() => pointers.length === 1, 'first command starts');
    store.enqueue([alert('second', { body: 'Second reply', semantic: { type: 'reply', threadId: 'old' } })], { mailboxId });
    release();
    await first;
    await until(() => pointers.length === 2 && !store.getWakeTarget(mailboxId)!.pendingWake, 'new message redrives without timer');
    assert.match(pointers[0], /Latest \(reply from sender, thread old\)/);
    assert.match(pointers[1], /Second reply/);
    if (mode === 'passive') {
      assert.equal(store.getWakeTarget(mailboxId)?.conversationMode, 'fixed');
      assert.equal(store.getWakeTarget(mailboxId)?.sessionId, 'box');
    }
  });
}

test('sequence settlement remains finite when wall clock order differs from arrival order', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 20_000 });
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.enqueue([alert('leased')], { mailboxId: 'box' });
  store.check({ mailboxId: 'box' });
  store.enqueue([alert('later-clock')], { mailboxId: 'box', wakeTarget: { mode: 'passive', hostAgent: 'codex', sessionId: 'existing' } });
  t.mock.timers.setTime(10_000);
  store.enqueue([alert('later-sequence')], { mailboxId: 'box' });
  assert.equal(store.check({ mailboxId: 'box', peek: true }).latestQueued?.id, 'later-clock');
  let calls = 0;
  const wakeup = new WakeupRedriveService({ store, commandRunner: async () => {
    calls++;
    return { stdout: '{}' };
  } });
  t.after(() => wakeup.stop());
  await wakeup.flushMailbox('box', { force: true });
  assert.equal(calls, 1);
  assert.equal(store.getWakeTarget('box')?.pendingWake, false);
});

test('in-flight ordinary arrivals retain enqueue coalescing deadline while replies remain immediate', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 10_000 });
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.enqueue([alert('first')], { mailboxId: 'box', wakeTarget: {
    mode: 'passive', hostAgent: 'claude-code', conversationMode: 'fixed', sessionId: 'session', coalesceMs: 5000,
  } });
  let calls = 0;
  const wakeup = new WakeupRedriveService({ store, commandRunner: async () => {
    calls++;
    if (calls === 1) {
      t.mock.timers.setTime(11_000);
      store.enqueue([alert('ordinary')], { mailboxId: 'box' });
    }
    return { stdout: '{}' };
  } });
  t.after(() => wakeup.stop());
  await wakeup.flushMailbox('box', { force: true });
  assert.equal(calls, 1);
  assert.equal(store.getWakeTarget('box')?.pendingWake, true);
  assert.equal(store.getWakeTarget('box')?.earliestWakeAt, 16_000);
  assert.deepEqual(await wakeup.flushReady(), []);
  t.mock.timers.setTime(16_000);
  await wakeup.flushReady();
  assert.equal(calls, 2);
  assert.equal(store.getWakeTarget('box')?.pendingWake, false);
});

for (const mode of ['passive', 'orca-run', 'orca-terminal'] as const) {
  test(`${mode} uncertain external delivery is fenced against retries, later mail and restart`, async t => {
    const file = join(directory(t), 'mailbox.json');
    const store = new MailboxStore(file);
    store.enqueue([alert('first')], { mailboxId: 'box', wakeTarget: {
      mode, ...(mode === 'passive' ? { hostAgent: 'claude-code' as const, conversationMode: 'spawn' as const } : {}), runId: 'run', terminalHandle: 'term',
    } });
    let effects = 0;
    const wakeup = new WakeupRedriveService({ store, idleCheck: async () => ({ idle: true }), commandRunner: async call => {
      if (mode !== 'orca-terminal' || call.args[1] === 'send') {
        effects++;
        throw new Error('accepted then result lost');
      }
      return { stdout: '{"ok":true}' };
    } });
    t.after(() => wakeup.stop());
    assert.equal((await wakeup.flushMailbox('box')).woke, false);
    assert.equal(store.getWakeTarget('box')?.deliveryInDoubt, true);
    assert.equal(store.getWakeTarget('box')?.pendingWake, false);
    assert.match(store.getWakeTarget('box')!.lastWakeError!, /result lost/);
    await wakeup.stop();
    const restarted = new MailboxStore(file);
    const recovery = new WakeupRedriveService({ store: restarted, commandRunner: async () => {
      effects++;
      return { stdout: '{}' };
    } });
    t.after(() => recovery.stop());
    restarted.enqueue([alert('next')], { mailboxId: 'box' });
    await recovery.flushReady();
    assert.equal((await recovery.flushMailbox('box', { force: true })).woke, false);
    assert.equal(effects, 1);
  });
}

test('a restart with an unfinished external wake remains fenced even without a failure callback', async t => {
  const file = join(directory(t), 'mailbox.json');
  const store = new MailboxStore(file);
  store.enqueue([alert('first')], { mailboxId: 'box', wakeTarget: { mode: 'passive', hostAgent: 'claude-code' } });
  store.beginWakeDelivery('box');
  const restarted = new MailboxStore(file);
  const service = new WakeupRedriveService({ store: restarted, commandRunner: async () => {
    assert.fail('unknown delivery must not be replayed');
  } });
  t.after(() => service.stop());
  assert.equal((await service.flushMailbox('box', { force: true })).woke, false);
  assert.equal(restarted.getWakeTarget('box')?.deliveryInDoubt, true);
});
