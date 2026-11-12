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
  createOrcaWaveExecutorPlugin,
  runDealerCli,
} from '../packages/index.ts';
import type {
  CommandRequest,
  CommandRunner,
  ExecutionContext,
  MessageEnvelope,
} from '../packages/index.ts';

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
    mode: 'orca-terminal',
    workspacePath: originWorkspace,
    worktreeId: 'wt-halfbeak-2',
    terminalHandle: 'term_origin',
  });

  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    return { stdout: '{"ok":true}' };
  };
  const wakeup = new WakeupRedriveService({
    store,
    commandRunner: runner,
    idleCheck: async () => ({ idle: true }),
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
  assert.equal(preserved.workspacePath, originWorkspace);
  assert.equal(preserved.worktreeId, 'wt-halfbeak-2');
  assert.equal(preserved.terminalHandle, 'term_origin');
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
  assert.equal(stillPreserved.workspacePath, originWorkspace);
  assert.equal(stillPreserved.terminalHandle, 'term_origin');
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
  const plugin = createOrcaWaveExecutorPlugin({
    store,
    commandRunner: runner,
    resolveTopicBinding: () => ({
      executor: 'orca-wave',
      mailboxId: 'stream:inbox',
      workspacePath: '/tmp/inbox',
      rotateAfterWaves: 1,
    }),
  });
  await plugin.execute(
    [envelope('peer-reply-2', { from: peer, semantic: { type: 'message', threadId: 'thread-affinity-2' } })],
    context,
    {},
  );
  assert.equal(store.list({ mailboxId: 'worktree:halfbeak-2' }).queuedCount, 1);
  const inferred = store.getWakeTarget('worktree:halfbeak-2')!;
  assert.equal(inferred.mode, 'orca-terminal');
  assert.equal(inferred.workspacePath, '/orca/halfbeak-2');
  assert.equal(inferred.worktreeId, 'wt-2');
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
