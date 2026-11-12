import test from 'node:test';
import assert from 'node:assert/strict';
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  MailboxStore,
  normalizeMailboxId,
  resolveMailboxId,
} from '../packages/index.ts';
import type { MessageEnvelope } from '../packages/index.ts';

function envelope(id: string, patch: Partial<MessageEnvelope> = {}): MessageEnvelope {
  return {
    id,
    networkId: 'team',
    traceId: id,
    from: { nodeId: 'ed25519:' + 'a'.repeat(64), member: 'alice', nodeName: 'laptop' },
    routing: { mode: 'broadcast' },
    kind: 'triage',
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

function workspace(t: { after: (fn: () => void) => void }) {
  const root = mkdtempSync(join(tmpdir(), 'dealer-mailbox-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return { root, path: join(root, 'mailbox.json') };
}

test('single outstanding lease never merges newer mail until acknowledged', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  assert.deepEqual(store.enqueue([envelope('a'), envelope('b')]).inserted, ['a', 'b']);
  const first = store.check();
  assert.match(first.deliveryId!, /^dlv_[0-9a-f]{12}$/);
  assert.equal(first.mailboxId, 'default');
  assert.equal(first.redelivered, false);
  assert.equal(first.newerQueuedCount, 0);
  assert.deepEqual(first.items.map(item => item.id), ['a', 'b']);
  assert.ok(first.items.every(item => item.status === 'leased'));

  const redelivered = store.check();
  assert.equal(redelivered.deliveryId, first.deliveryId);
  assert.equal(redelivered.redelivered, true);
  assert.deepEqual(redelivered.items.map(item => item.id), ['a', 'b']);

  store.enqueue([envelope('c')]);
  const pending = store.check();
  assert.equal(pending.deliveryId, first.deliveryId);
  assert.deepEqual(pending.items.map(item => item.id), ['a', 'b']);
  assert.equal(pending.newerQueuedCount, 1);
  const listed = store.list();
  assert.deepEqual(listed.items.map(item => item.id), ['c', 'b', 'a']);
  assert.equal(listed.queuedCount, 1);
  assert.equal(listed.leasedCount, 2);
  assert.equal(listed.items[0].status, 'queued');

  const next = store.check({ ackLeaseId: first.deliveryId! });
  assert.notEqual(next.deliveryId, first.deliveryId);
  assert.equal(next.redelivered, false);
  assert.deepEqual(next.items.map(item => item.id), ['c']);
  const archived = store.list({ includeAcked: true });
  assert.deepEqual(archived.items.map(item => item.id), ['c', 'b', 'a']);
  assert.equal(archived.ackedCount, 2);
  assert.equal(archived.queuedCount, 0);
  assert.equal(archived.leasedCount, 1);
});

test('ack is idempotent and rejects unknown or mismatched leases', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  store.enqueue([envelope('a')]);
  const lease = store.check();
  const acknowledged = store.ack(lease.deliveryId!);
  assert.equal(acknowledged.alreadyAcknowledged, false);
  assert.deepEqual(acknowledged.itemIds, ['a']);
  assert.equal(acknowledged.mailboxId, 'default');
  assert.equal(store.ack(lease.deliveryId!).alreadyAcknowledged, true);
  assert.throws(
    () => store.ack('dlv_ffffffffffff'),
    /Unknown or mismatched delivery lease/,
  );

  store.enqueue([envelope('x')], { mailboxId: 'task:x' });
  const taskLease = store.check({ mailboxId: 'task:x' });
  assert.throws(
    () => store.ack(taskLease.deliveryId!, 'default'),
    /Unknown or mismatched delivery lease/,
  );
  const inferred = store.ack(taskLease.deliveryId!);
  assert.equal(inferred.mailboxId, 'task:x');
  assert.equal(store.ack(taskLease.deliveryId!, 'task:x').alreadyAcknowledged, true);
});

test('peek never mints a lease or mutates item state', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  store.enqueue([envelope('a')]);
  const peek = store.check({ peek: true });
  assert.equal(peek.deliveryId, null);
  assert.equal(peek.redelivered, false);
  assert.equal(peek.newerQueuedCount, 0);
  assert.deepEqual(peek.items.map(item => item.id), ['a']);
  assert.equal(peek.items[0].status, 'queued');
  const persisted = JSON.parse(readFileSync(path, 'utf8'));
  assert.deepEqual(persisted.mailboxes, {});
  assert.equal(persisted.items[0].status, 'queued');

  const leased = store.check();
  const behind = store.check({ peek: true });
  assert.equal(behind.deliveryId, leased.deliveryId);
  assert.equal(behind.redelivered, true);
  store.enqueue([envelope('b')]);
  const withNewer = store.check({ peek: true });
  assert.deepEqual(withNewer.items.map(item => item.id), ['a']);
  assert.equal(withNewer.newerQueuedCount, 1);
  assert.equal(store.check().deliveryId, leased.deliveryId);
});

test('mailbox resolution is deterministic and logically isolated', t => {
  const { path } = workspace(t);
  const semantic = envelope('one', {
    semantic: {
      type: 'question',
      threadId: 'one',
      targetMailbox: 'stream:alerts',
      replyMailbox: 'worktree:main',
    },
  });
  assert.equal(resolveMailboxId(semantic, 'inbox'), 'stream:alerts');
  const payload = envelope('two', { payload: { mailbox: 'task:msg1' } });
  assert.equal(resolveMailboxId(payload), 'task:msg1');
  const both = envelope('three', {
    payload: { mailbox: 'payload:mailbox' },
    semantic: { type: 'message', threadId: 'three', targetMailbox: 'semantic:mailbox' },
  });
  assert.equal(resolveMailboxId(both), 'semantic:mailbox');
  assert.equal(
    resolveMailboxId(envelope('wave', { kind: 'alerts', payload: { streamKey: 'dev' } }), 'orca-wave'),
    'stream:dev',
  );
  assert.equal(resolveMailboxId(envelope('spawn'), 'orca-spawn'), 'task:spawn');
  assert.equal(resolveMailboxId(envelope('plain'), 'inbox'), 'default');
  assert.throws(
    () => resolveMailboxId(envelope('bad', { payload: { mailbox: 'bad\nid' } })),
    /Invalid payload mailbox/,
  );
  assert.throws(() => normalizeMailboxId('x'.repeat(257)), /Invalid mailbox id/);
  assert.throws(() => normalizeMailboxId(' padded '), /Invalid mailbox id/);

  const store = new MailboxStore(path);
  store.enqueue([envelope('shared')]);
  store.enqueue([envelope('shared')], { mailboxId: 'stream:alerts' });
  assert.equal(store.enqueue([envelope('shared')]).inserted.length, 0);
  assert.equal(store.check().items[0].mailboxId, 'default');
  assert.equal(store.check({ mailboxId: 'stream:alerts' }).items[0].mailboxId, 'stream:alerts');
  assert.equal(store.list({ mailboxId: 'stream:alerts' }).totalCount, 1);
  assert.equal(store.list({ mailboxId: 'default' }).totalCount, 1);
  assert.equal(store.list().totalCount, 2);
});

test('thread affinity outranks topic bindings while explicit session targets outrank both', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  store.recordOutboundEnvelope(
    envelope('origin-request', {
      semantic: { type: 'message', threadId: 'thread-affinity', replyMailbox: 'worktree:origin' },
    }),
    ['ed25519:' + 'b'.repeat(64)],
    'worktree:origin',
  );
  assert.equal(store.resolveThreadLocalMailbox('thread-affinity'), 'worktree:origin');
  assert.equal(store.resolveThreadLocalMailbox('thread-affinity', 'team'), 'worktree:origin');
  assert.equal(store.resolveThreadLocalMailbox('thread-affinity', 'other-network'), undefined);
  assert.equal(store.resolveThreadLocalMailbox('unknown-thread'), undefined);
  assert.equal(store.resolveThreadLocalMailbox(), undefined);
  const binding = { executor: 'orca-wave', mailboxId: 'stream:inbox' };
  // An explicit payload mailbox is a concrete destination and outranks the generic topic binding.
  assert.equal(
    resolveMailboxId(
      envelope('explicit-payload', { payload: { mailbox: 'task:custom' } }),
      'inbox',
      binding,
    ),
    'task:custom',
  );
  assert.equal(
    resolveMailboxId(
      envelope('thread-reply', { semantic: { type: 'message', threadId: 'thread-affinity' } }),
      'orca-wave',
      binding,
      store.resolveThreadLocalMailbox('thread-affinity'),
    ),
    'worktree:origin',
  );
  assert.equal(
    resolveMailboxId(
      envelope('control-reply', { semantic: { type: 'reply', threadId: 'thread-affinity' } }),
      'inbox',
      binding,
      store.resolveThreadLocalMailbox('thread-affinity'),
    ),
    'worktree:origin',
  );
  assert.equal(
    resolveMailboxId(
      envelope('explicit-session', {
        semantic: { type: 'message', threadId: 'thread-affinity', targetMailbox: 'worktree:explicit' },
      }),
      'orca-wave',
      binding,
      'worktree:origin',
    ),
    'worktree:explicit',
  );
  assert.equal(
    resolveMailboxId(
      envelope('new-thread', {
        payload: { streamKey: 'inbox' },
        semantic: { type: 'question', threadId: 'new-thread', targetMailbox: 'default' },
      }),
      'inbox',
      binding,
      undefined,
    ),
    'stream:inbox',
  );
});

test('enqueue deduplicates per mailbox and sanitizes one-line wakeup hints', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  const first = store.enqueue([envelope('a')], { wakeupHint: '  line one\n\n  two   three  ' });
  assert.deepEqual(first, { inserted: ['a'], duplicateIds: [], mailboxIds: ['default'] });
  assert.deepEqual(store.enqueue([envelope('a')]), {
    inserted: [],
    duplicateIds: ['a'],
    mailboxIds: ['default'],
  });
  assert.deepEqual(store.enqueue([envelope('a')], { mailboxId: 'task:a' }).inserted, ['a']);
  assert.equal(
    store.list({ mailboxId: 'default' }).items[0].wakeupHint,
    'line one two three',
  );

  store.enqueue([envelope('long')], { wakeupHint: 'x'.repeat(500) });
  const long = store
    .list({ mailboxId: 'default' })
    .items.find(item => item.id === 'long')!;
  assert.equal(long.wakeupHint!.length, 120);

  const payloadHint = envelope('from-payload', { payload: { wakeupHint: 'payload\nhint' } });
  store.enqueue([payloadHint]);
  assert.equal(
    store
      .list({ mailboxId: 'default' })
      .items.find(item => item.id === 'from-payload')!.wakeupHint,
    'payload hint',
  );

  const subjectHint = envelope('from-subject', { subject: 'subject\nhint' });
  store.enqueue([subjectHint]);
  assert.equal(
    store
      .list({ mailboxId: 'default' })
      .items.find(item => item.id === 'from-subject')!.wakeupHint,
    'subject hint',
  );
});

test('check orders queued items by instant then insertion sequence', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  store.enqueue([envelope('older'), envelope('newer'), envelope('tie')]);
  const saved = JSON.parse(readFileSync(path, 'utf8'));
  saved.items[0].receivedAt = '2026-11-10T09:00:00+08:00';
  saved.items[1].receivedAt = '2026-11-10T02:00:00Z';
  saved.items[2].receivedAt = '2026-11-10T01:00:00Z';
  writeFileSync(path, JSON.stringify(saved));
  assert.deepEqual(
    store.check().items.map(item => item.id),
    ['older', 'tie', 'newer'],
  );
});

test('waitForMail resolves immediately, wakes on enqueue, times out and aborts', async t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  store.enqueue([envelope('ready')]);
  const immediate = await store.waitForMail({ timeoutMs: 1_000 });
  assert.equal(immediate.timedOut, false);
  assert.deepEqual(immediate.items.map(item => item.id), ['ready']);
  store.ack(immediate.deliveryId!);

  const waiter = store.waitForMail({ timeoutMs: 2_000 });
  setTimeout(() => store.enqueue([envelope('wake')]), 25);
  const woken = await waiter;
  assert.equal(woken.timedOut, false);
  assert.deepEqual(woken.items.map(item => item.id), ['wake']);
  store.ack(woken.deliveryId!);

  const empty = new MailboxStore(join(path, '..', 'empty-mailbox.json'));
  const timeout = await empty.waitForMail({ timeoutMs: 30 });
  assert.equal(timeout.timedOut, true);
  assert.equal(timeout.deliveryId, null);
  assert.deepEqual(timeout.items, []);

  const leaseStore = new MailboxStore(join(path, '..', 'abort-mailbox.json'));
  const controller = new AbortController();
  const aborted = leaseStore.waitForMail({ timeoutMs: 5_000, signal: controller.signal });
  setTimeout(() => controller.abort(), 15);
  await assert.rejects(aborted, error => error.name === 'AbortError' && /aborted/.test(error.message));
  leaseStore.enqueue([envelope('after-abort')]);
  assert.equal(leaseStore.check().items[0].id, 'after-abort');
});

test('peek wait reports queued mail without minting a lease', async t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  store.enqueue([envelope('peeked')]);
  const preview = await store.waitForMail({ peek: true, timeoutMs: 500 });
  assert.equal(preview.timedOut, false);
  assert.equal(preview.deliveryId, null);
  assert.deepEqual(preview.items.map(item => item.id), ['peeked']);
  assert.equal(store.list().items[0].status, 'queued');
});

test('wake targets coalesce, survive restarts, clear on lease and count completed waves', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  const created = store.upsertWakeTarget('default', {
    mode: 'orca-run',
    runId: 'run_1',
    coalesceMs: 50,
  });
  assert.equal(created.pendingWake, false);
  const before = Date.now();
  store.enqueue([envelope('a')], {
    wakeTarget: { mode: 'orca-terminal', terminalHandle: 'term_1' },
  });
  const pending = store.listPendingWakeTargets(before + 1_000);
  assert.equal(pending.length, 1);
  assert.equal(pending[0].mode, 'orca-terminal');
  assert.equal(pending[0].runId, 'run_1');
  assert.equal(pending[0].terminalHandle, 'term_1');
  assert.equal(pending[0].pendingWake, true);
  assert.ok(pending[0].earliestWakeAt >= before + 50);
  assert.deepEqual(store.listPendingWakeTargets(before), []);

  const reopened = new MailboxStore(path);
  assert.equal(
    reopened.listPendingWakeTargets(Date.now() + 1_000)[0].terminalHandle,
    'term_1',
  );

  const lease = reopened.check();
  assert.deepEqual(reopened.listPendingWakeTargets(Date.now() + 1_000), []);
  reopened.ack(lease.deliveryId!);
  assert.equal(reopened.upsertWakeTarget('default').completedWaves, 1);
  assert.equal(
    JSON.parse(readFileSync(path, 'utf8')).wakeTargets.default.completedWaves,
    1,
  );
});

test('lowering coalescing releases pending wakes without shortening retry backoff', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  const before = Date.now();
  store.enqueue([envelope('initial')], { wakeTarget: { coalesceMs: 86_400_000 } });
  assert.ok(store.getWakeTarget('default')!.earliestWakeAt >= before + 86_400_000);
  assert.deepEqual(store.listPendingWakeTargets(Date.now()), []);
  store.upsertWakeTarget('default', { coalesceMs: 1000 });
  assert.ok(store.getWakeTarget('default')!.earliestWakeAt <= Date.now() + 1000);
  store.upsertWakeTarget('default', { coalesceMs: 0 });
  assert.equal(store.getWakeTarget('default')!.earliestWakeAt, 0);
  assert.equal(store.listPendingWakeTargets(Date.now()).length, 1);

  store.enqueue([envelope('large-window')], { wakeTarget: { coalesceMs: 86_400_000 } });
  assert.deepEqual(store.listPendingWakeTargets(Date.now()), []);
  store.enqueue([envelope('lower-window')], { wakeTarget: { coalesceMs: 0 } });
  assert.equal(store.listPendingWakeTargets(Date.now()).length, 1);

  const retryAt = Date.now() + 60_000;
  store.upsertWakeTarget('default', { attemptCount: 1, earliestWakeAt: retryAt, coalesceMs: 1000 });
  store.upsertWakeTarget('default', { coalesceMs: 0 });
  assert.equal(store.getWakeTarget('default')!.earliestWakeAt, retryAt);
  store.enqueue([envelope('retry-window')], { wakeTarget: { coalesceMs: 0 } });
  assert.equal(store.getWakeTarget('default')!.earliestWakeAt, retryAt);
  assert.deepEqual(store.listPendingWakeTargets(Date.now()), []);
  store.upsertWakeTarget('default', { attemptCount: 0, coalesceMs: 0, earliestWakeAt: retryAt });
  assert.equal(store.getWakeTarget('default')!.earliestWakeAt, retryAt);
});

test('thread routes reverse-route replies and track settlement on both directions', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  const question = envelope('msg_1', {
    semantic: {
      type: 'question',
      threadId: 'msg_1',
      targetMailbox: 'task:msg_1',
      replyMailbox: 'worktree:main',
    },
  });
  const inbound = store.recordInboundEnvelope(question, 'task:msg_1');
  assert.equal(inbound.direction, 'inbound');
  assert.equal(inbound.localMailbox, 'task:msg_1');
  assert.deepEqual(store.resolveReplyTarget('msg_1'), {
    threadId: 'msg_1',
    messageId: 'msg_1',
    peerNodeId: question.from.nodeId,
    networkId: 'team',
    targetMailbox: 'worktree:main',
    localMailbox: 'task:msg_1',
    traceId: 'msg_1',
    kind: 'triage',
    subject: 'msg_1',
  });

  const reply = envelope('msg_2', {
    semantic: { type: 'reply', threadId: 'msg_1', targetMailbox: 'worktree:main' },
  });
  store.recordInboundEnvelope(reply, 'worktree:main');
  assert.equal(store.getThread('msg_1')!.state, 'replied');
  assert.equal(store.getThread('msg_2')!.direction, 'inbound');

  const settle = envelope('msg_3', {
    semantic: { type: 'settle', threadId: 'msg_1', outcome: 'succeeded' },
    payload: { summary: 'verified and merged' },
  });
  store.recordInboundEnvelope(settle, 'worktree:main');
  const settledThread = store.getThread('msg_1')!;
  assert.equal(settledThread.state, 'settled');
  assert.equal(settledThread.settlement!.outcome, 'succeeded');
  assert.equal(settledThread.settlement!.summary, 'verified and merged');

  const outbound = envelope('out_1', {
    semantic: { type: 'question', threadId: 'out_1', replyMailbox: 'worktree:main' },
  });
  const recorded = store.recordOutboundEnvelope(
    outbound,
    ['ed25519:' + 'b'.repeat(64), 'ed25519:' + 'b'.repeat(64)],
    'stream:main',
  );
  assert.equal(recorded.direction, 'outbound');
  assert.deepEqual(recorded.peerNodeIds, ['ed25519:' + 'b'.repeat(64)]);
  assert.equal(store.getThread('out_1')!.state, 'open');
  store.recordInboundEnvelope(
    envelope('out_2', { semantic: { type: 'reply', threadId: 'out_1' } }),
    'stream:main',
  );
  assert.equal(store.getThread('out_1')!.state, 'replied');
  assert.equal(new MailboxStore(path).getThread('out_1')!.state, 'replied');
  assert.equal(store.getThread('missing'), undefined);
  assert.throws(() => store.getThread(''), /Invalid thread reference/);
});

test('legacy inbox migrates once into the default mailbox without data loss', t => {
  const { root, path } = workspace(t);
  const legacyPath = join(root, 'inbox.json');
  const readAt = '2026-11-10T09:00:00.000Z';
  writeFileSync(
    legacyPath,
    JSON.stringify({
      schema: 'the-dealer-inbox/v1',
      items: [
        {
          id: 'unread',
          networkId: 'team',
          topic: 'triage',
          priority: 'high',
          from: { nodeId: 'ed25519:' + 'a'.repeat(64), member: 'alice', nodeName: 'laptop' },
          subject: 'Needs triage',
          body: 'body',
          payload: { keep: true },
          attachments: [
            { name: 'report', mimeType: 'text/plain', sizeBytes: 4, blake3Hash: 'hash', localPath: '/verified/report' },
          ],
          createdAt: '2026-11-10T08:00:00.000Z',
          receivedAt: '2026-11-10T08:00:01.000Z',
          status: 'unread',
        },
        {
          id: 'read',
          networkId: 'team',
          topic: 'triage',
          priority: 'normal',
          from: { nodeId: 'ed25519:' + 'b'.repeat(64), member: 'bob', nodeName: 'desktop' },
          subject: 'Already read',
          body: 'body',
          payload: {},
          attachments: [],
          createdAt: '2026-11-10T07:00:00.000Z',
          receivedAt: '2026-11-10T07:00:01.000Z',
          status: 'read',
          readAt,
        },
      ],
    }),
  );
  const store = new MailboxStore(path, { legacyInboxPath: legacyPath });
  assert.equal(existsSync(legacyPath), false);
  assert.equal(existsSync(`${legacyPath}.migrated`), true);
  const all = store.list({ includeAcked: true });
  assert.equal(all.totalCount, 2);
  assert.equal(all.ackedCount, 1);
  assert.equal(all.queuedCount, 1);
  const queued = all.items.find(item => item.id === 'unread')!;
  assert.equal(queued.mailboxId, 'default');
  assert.equal(queued.status, 'queued');
  assert.equal(queued.wakeupHint, 'Needs triage');
  assert.deepEqual(queued.envelope.payload, {
    keep: true,
    localAttachments: ['/verified/report'],
  });
  assert.deepEqual(queued.envelope.attachments.map(attachment => attachment.name), ['report']);
  const acked = all.items.find(item => item.id === 'read')!;
  assert.equal(acked.status, 'acked');
  assert.equal(acked.ackedAt, readAt);
  assert.equal(statSync(path).mode & 0o777, 0o600);

  const second = new MailboxStore(path, { legacyInboxPath: legacyPath });
  assert.equal(second.list({ includeAcked: true }).totalCount, 2);
  assert.equal(existsSync(`${legacyPath}.migrated`), true);
});

test('invalid mailbox or legacy files fail loudly and locks are released', t => {
  const { root, path } = workspace(t);
  const store = new MailboxStore(path);
  store.enqueue([envelope('a')]);
  assert.equal(existsSync(`${path}.lock`), false);
  for (const invalid of ['{', '{}', '{"schema":"the-dealer-mailbox/v1","items":[{}]}']) {
    writeFileSync(path, invalid);
    assert.throws(() => store.list(), /Invalid mailbox store/);
    assert.throws(() => store.check(), /Invalid mailbox store/);
  }
  writeFileSync(path, '{"schema":"the-dealer-mailbox/v1","sequence":0,"items":[]}');
  assert.equal(store.list().totalCount, 0);

  const badLegacy = join(root, 'bad-inbox.json');
  writeFileSync(badLegacy, '{');
  assert.throws(
    () => new MailboxStore(join(root, 'fresh.json'), { legacyInboxPath: badLegacy }),
    /Invalid inbox store/,
  );
  assert.equal(existsSync(badLegacy), true);
});
