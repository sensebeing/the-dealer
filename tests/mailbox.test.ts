import { toMailboxTopicBinding } from '../packages/cli/config.ts';
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
  OutboxStore,
  WakeupRedriveService,
  normalizeMailboxId,
  resolveMailboxId,
  resolveBoundMailboxId,
} from '../packages/index.ts';
import type { DeliveryReceipt, MessageEnvelope } from '../packages/index.ts';
import { netKey } from '../packages/mailbox/network-key.ts';

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

const waveSlot = 'stream:alert-wave:1';
const slotSession = 'terminal:term-wave';

test('slot obligations prioritize fenced wakeups and then session replies', t => {
  const store = new MailboxStore(workspace(t).path);
  store.enqueue([envelope('session-reply')], { mailboxId: slotSession });
  store.upsertWakeTarget(waveSlot, { pendingWake: true });
  const inspect = () => store.inspectSlotObligations(waveSlot, { sessionMailboxId: slotSession });

  assert.equal(inspect().immediateBusyReason, 'wakeup_pending');
  store.beginWakeDelivery(waveSlot);
  assert.equal(inspect().immediateBusyReason, 'wakeup_pending');
  store.completeWakeDelivery(waveSlot, 0);
  assert.deepEqual(inspect(), {
    hasPendingWave: false,
    immediateBusyReason: 'session_reply_pending',
  });
  store.ack(store.check({ mailboxId: slotSession }).deliveryId!, slotSession);
  store.beginWakeDelivery(slotSession);
  assert.equal(inspect().immediateBusyReason, 'session_reply_pending');
});

test('slot obligations follow local downstream mail through acknowledgement', t => {
  const store = new MailboxStore(workspace(t).path);
  store.enqueue([envelope('wave')], { mailboxId: waveSlot });
  const releaseMailbox = 'stream:prod-release';
  const release = envelope('release', {
    kind: 'release.prod.v1',
    semantic: { type: 'message', threadId: 'release', replyMailbox: slotSession, targetMailbox: releaseMailbox },
  });
  store.recordOutboundEnvelope(release, [release.from.nodeId], slotSession);
  store.enqueue([release], { mailboxId: releaseMailbox });
  const lease = store.check({ mailboxId: releaseMailbox });
  const inspect = () => store.inspectSlotObligations(waveSlot, { sessionMailboxId: slotSession });

  assert.deepEqual(inspect(), {
    hasPendingWave: true,
    immediateBusyReason: 'local_outbound_pending',
  });
  store.ack(lease.deliveryId!, releaseMailbox);
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
});

test('slot obligations exclude replied, settled and failed outbound messages', t => {
  const { path, root } = workspace(t);
  const store = new MailboxStore(path);
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'blobs'));
  store.enqueue([envelope('wave')], { mailboxId: waveSlot });
  const remote = (id: string) => envelope(id, {
    semantic: { type: 'message', threadId: id, replyMailbox: slotSession },
  });
  const inspect = () => store.inspectSlotObligations(waveSlot, { sessionMailboxId: slotSession, outbox });

  store.recordOutboundEnvelope(remote('replied'), ['remote'], slotSession);
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  store.recordInboundEnvelope(envelope('reply', {
    semantic: { type: 'reply', threadId: 'replied' },
  }), slotSession, { committed: false });
  assert.equal(inspect().immediateBusyReason, undefined);

  store.recordOutboundEnvelope(remote('settled'), ['remote'], slotSession);
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  store.recordInboundEnvelope(envelope('settlement', {
    semantic: { type: 'settle', threadId: 'settled', outcome: 'succeeded' },
  }), slotSession, { committed: false });
  assert.equal(inspect().immediateBusyReason, undefined);

  const failed = remote('failed');
  store.recordOutboundEnvelope(failed, ['remote'], slotSession);
  outbox.enqueue({ envelope: failed, explicitTargets: [], pendingNodeIds: [] });
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  assert.equal(outbox.markFailed(failed.id, 'Local delivery failed')?.status, 'failed');
  assert.equal(inspect().immediateBusyReason, undefined);
});

test('slot obligations exclude the local wave after both thread legs are pinned to its session', t => {
  t.mock.timers.enable({ apis: ['Date'], now: 10_000 });
  const store = new MailboxStore(workspace(t).path);
  const wave = envelope('self-wave', {
    kind: 'wave.alert.v1',
    semantic: { type: 'message', threadId: 'self-wave', replyMailbox: waveSlot, targetMailbox: waveSlot },
  });
  store.enqueue([wave], { mailboxId: waveSlot, wakeTarget: { rotateAfterWaves: 1 } });
  store.recordOutboundEnvelope(wave, [wave.from.nodeId], waveSlot);
  const [item] = store.check({ mailboxId: waveSlot }).items;
  store.completeWakeDelivery(waveSlot, item.sequence, {}, { sessionMailboxId: slotSession });
  assert.deepEqual(store.listThreadRecords(wave.id).map(({ direction, localMailbox, state }) => ({
    direction, localMailbox, state,
  })).sort((a, b) => a.direction.localeCompare(b.direction)), [
    { direction: 'inbound', localMailbox: slotSession, state: 'open' },
    { direction: 'outbound', localMailbox: slotSession, state: 'open' },
  ]);
  assert.deepEqual(store.inspectSlotObligations(waveSlot, { sessionMailboxId: slotSession }), {
    hasPendingWave: true,
  });

  store.recordOutboundEnvelope(envelope('wave-follow-up', {
    replyToId: wave.id,
    semantic: { type: 'question', threadId: wave.id, replyMailbox: slotSession },
  }), ['remote'], slotSession);
  assert.equal(store.inspectSlotObligations(waveSlot, { sessionMailboxId: slotSession }).immediateBusyReason, 'outbound_thread_open');
});

test('an empty slot only waits remotely for questions addressed back to the session', t => {
  const store = new MailboxStore(workspace(t).path);
  store.upsertWakeTarget(waveSlot, { rotateAfterWaves: 1 });
  store.recordOutboundEnvelope(envelope('old-task', {
    semantic: { type: 'message', threadId: 'old-task', replyMailbox: slotSession },
  }), ['remote'], slotSession);
  store.recordOutboundEnvelope(envelope('old-slot-question', {
    semantic: { type: 'question', threadId: 'old-slot-question', replyMailbox: waveSlot },
  }), ['remote'], waveSlot);
  const inspect = () => store.inspectSlotObligations(waveSlot, { sessionMailboxId: slotSession });
  assert.deepEqual(inspect(), { hasPendingWave: false });
  store.recordOutboundEnvelope(envelope('session-question', {
    semantic: { type: 'question', threadId: 'session-question', replyMailbox: slotSession },
  }), ['remote'], slotSession);
  assert.deepEqual(inspect(), { hasPendingWave: false, immediateBusyReason: 'outbound_thread_open' });
});

test('a reply to one question leaves the next question on the same thread open', t => {
  t.mock.timers.enable({ apis: ['Date'], now: 10_000 });
  const store = new MailboxStore(workspace(t).path);
  store.enqueue([envelope('wave')], { mailboxId: waveSlot });
  const ask = (id: string) => store.recordOutboundEnvelope(envelope(id, {
    semantic: { type: 'question', threadId: 'conversation', replyMailbox: slotSession },
  }), ['remote'], slotSession);
  const inspect = () => store.inspectSlotObligations(waveSlot, { sessionMailboxId: slotSession });
  ask('q1');
  t.mock.timers.tick(1);
  store.recordInboundEnvelope(envelope('answer-1', {
    replyToId: 'q1', semantic: { type: 'reply', threadId: 'conversation' },
  }), slotSession, { committed: false });
  assert.equal(inspect().immediateBusyReason, undefined);
  t.mock.timers.tick(1);
  ask('q2');
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  t.mock.timers.tick(1);
  store.recordInboundEnvelope(envelope('root-progress', {
    replyToId: 'conversation', semantic: { type: 'reply', threadId: 'conversation' },
  }), slotSession, { committed: false });
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  store.recordInboundEnvelope(envelope('thread-answer', {
    semantic: { type: 'reply', threadId: 'conversation' },
  }), slotSession, { committed: false });
  assert.equal(inspect().immediateBusyReason, undefined);
  t.mock.timers.tick(1);
  ask('q3');
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
});

test('older unanswered questions survive newer leased mail on fixed and rotating slots', t => {
  t.mock.timers.enable({ apis: ['Date'], now: 10_000 });
  const store = new MailboxStore(workspace(t).path);
  for (const { mailboxId, sessionMailboxId } of [
    { mailboxId: 'stream:fixed', sessionMailboxId: undefined },
    { mailboxId: waveSlot, sessionMailboxId: slotSession },
  ]) {
    if (sessionMailboxId) store.upsertWakeTarget(mailboxId, { rotateAfterWaves: 1 });
    const questionId = `question-${mailboxId}`;
    const replyMailbox = sessionMailboxId ?? mailboxId;
    store.recordOutboundEnvelope(envelope(questionId, {
      semantic: { type: 'question', threadId: questionId, replyMailbox },
    }), ['remote'], replyMailbox);
    t.mock.timers.tick(1);
    store.enqueue([envelope(`new-wave-${mailboxId}`)], { mailboxId });
    store.check({ mailboxId });
    assert.deepEqual(store.inspectSlotObligations(mailboxId, { sessionMailboxId }), {
      hasPendingWave: true, immediateBusyReason: 'outbound_thread_open',
    });
  }
});

test('slot obligations only accept peer replies received by the slot or its session', t => {
  const store = new MailboxStore(workspace(t).path);
  const coordinator = 'stream:coordinator';
  const inspect = () => store.inspectSlotObligations(waveSlot, { sessionMailboxId: slotSession });
  store.enqueue([envelope('root')], { mailboxId: waveSlot });
  store.recordOutboundEnvelope(envelope('child-question', {
    replyToId: 'root', semantic: { type: 'question', threadId: 'root', replyMailbox: slotSession },
  }), ['peer'], slotSession);
  const progress = envelope('progress', {
    replyToId: 'root',
    semantic: { type: 'reply', threadId: 'root', replyMailbox: slotSession, targetMailbox: coordinator },
  });
  store.recordOutboundEnvelope(progress, [progress.from.nodeId], slotSession);
  store.enqueue([progress], { mailboxId: coordinator });
  store.ack(store.check({ mailboxId: coordinator }).deliveryId!, coordinator);
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');

  store.recordInboundEnvelope(envelope('misrouted-answer', {
    replyToId: 'child-question', semantic: { type: 'reply', threadId: 'root' },
  }), coordinator);
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  const selfAnswer = envelope('self-answer', {
    replyToId: 'child-question', semantic: { type: 'reply', threadId: 'root', replyMailbox: slotSession },
  });
  store.recordOutboundEnvelope(selfAnswer, [selfAnswer.from.nodeId], slotSession);
  store.recordInboundEnvelope(selfAnswer, slotSession, { committed: false });
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  store.recordInboundEnvelope(envelope('delivered-answer', {
    replyToId: 'child-question', semantic: { type: 'reply', threadId: 'root' },
  }), slotSession, { committed: false });
  assert.equal(inspect().immediateBusyReason, undefined);
});

test('an answer cannot discharge unacknowledged custody of the downstream message itself', t => {
  const store = new MailboxStore(workspace(t).path);
  const downstream = 'stream:downstream';
  const question = envelope('local-question', {
    semantic: { type: 'question', threadId: 'local-question', replyMailbox: waveSlot, targetMailbox: downstream },
  });
  store.recordOutboundEnvelope(question, [question.from.nodeId], waveSlot);
  store.enqueue([question], { mailboxId: downstream });
  store.recordInboundEnvelope(envelope('early-answer', {
    replyToId: question.id, semantic: { type: 'reply', threadId: question.id },
  }), waveSlot, { committed: false });
  assert.equal(store.inspectSlotObligations(waveSlot).immediateBusyReason, 'local_outbound_pending');
  store.ack(store.check({ mailboxId: downstream }).deliveryId!, downstream);
  assert.equal(store.inspectSlotObligations(waveSlot).immediateBusyReason, undefined);
});

test('delimiter-containing network and message identities stay distinct in mailbox and outbox lookups', t => {
  const { path, root } = workspace(t);
  const store = new MailboxStore(path);
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'blobs'));
  const first = envelope('b\0c', {
    networkId: 'a', semantic: { type: 'question', threadId: 'b\0c', replyMailbox: waveSlot },
  });
  const second = envelope('c', {
    networkId: 'a\0b', semantic: { type: 'question', threadId: 'c', replyMailbox: waveSlot },
  });
  outbox.enqueue({ envelope: first, explicitTargets: [], pendingNodeIds: [] });
  outbox.markFailed(first.id, 'No effect');
  store.recordOutboundEnvelope(second, ['remote'], waveSlot);
  assert.equal(store.inspectSlotObligations(waveSlot, { outbox }).immediateBusyReason, 'outbound_thread_open');
  assert.notEqual(netKey(first.networkId, first.id), netKey(second.networkId, second.id));
  assert.deepEqual([...outbox.readDefinitivelyFailedKeys()], [netKey(first.networkId, first.id)]);
});

test('answering and acknowledging a peer clarification leaves the original question open', t => {
  const store = new MailboxStore(workspace(t).path);
  store.upsertWakeTarget(waveSlot, { rotateAfterWaves: 1 });
  store.recordOutboundEnvelope(envelope('original-question', {
    semantic: { type: 'question', threadId: 'conversation', replyMailbox: slotSession },
  }), ['remote'], slotSession);
  store.enqueue([envelope('clarification', {
    replyToId: 'original-question',
    semantic: { type: 'question', threadId: 'conversation', targetMailbox: slotSession },
  })], { mailboxId: slotSession });
  const lease = store.check({ mailboxId: slotSession });
  store.recordOutboundEnvelope(envelope('clarification-answer', {
    replyToId: 'clarification',
    semantic: { type: 'reply', threadId: 'conversation', replyMailbox: slotSession },
  }), ['remote'], slotSession);
  store.ack(lease.deliveryId!, slotSession);

  const inspect = () => store.inspectSlotObligations(waveSlot, { sessionMailboxId: slotSession });
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  store.recordInboundEnvelope(envelope('original-answer', {
    replyToId: 'original-question',
    semantic: { type: 'reply', threadId: 'conversation' },
  }), slotSession, { committed: false });
  assert.equal(inspect().immediateBusyReason, undefined);
});

test('slot thread custody and answers are scoped to each network', t => {
  const store = new MailboxStore(workspace(t).path);
  store.enqueue([envelope('wave')], { mailboxId: waveSlot });
  const threadId = 'shared-thread';
  store.recordOutboundEnvelope(envelope('team-question', {
    semantic: { type: 'question', threadId, replyMailbox: slotSession },
  }), ['remote'], slotSession);
  store.enqueue([envelope('other-network-local', {
    networkId: 'other', semantic: { type: 'message', threadId },
  })], { mailboxId: 'stream:other' });
  store.recordInboundEnvelope(envelope('other-network-settlement', {
    networkId: 'other', semantic: { type: 'settle', threadId, outcome: 'succeeded' },
  }), slotSession);
  assert.deepEqual(store.inspectSlotObligations(waveSlot, { sessionMailboxId: slotSession }), {
    hasPendingWave: true,
    immediateBusyReason: 'outbound_thread_open',
  });
});

test('slot obligations read outbox once and retain custody with partial or uncertain delivery', t => {
  const { path, root } = workspace(t);
  const store = new MailboxStore(path);
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'blobs'));
  const outboxReads = t.mock.method(outbox as unknown as { read(): unknown }, 'read');
  const inspect = () => store.inspectSlotObligations(waveSlot, { sessionMailboxId: slotSession, outbox });
  store.enqueue([envelope('wave')], { mailboxId: waveSlot });
  assert.equal(inspect().immediateBusyReason, undefined);
  assert.equal(outboxReads.mock.callCount(), 0);

  const request = (id: string, targetMailbox?: string) => envelope(id, {
    semantic: { type: 'question', threadId: id, replyMailbox: slotSession, targetMailbox },
  });
  const partial = request('partial');
  const receipt: DeliveryReceipt = {
    messageId: partial.id, networkId: partial.networkId, fromNodeId: 'delivered',
    stage: 'dispatched', updatedAt: partial.createdAt, signature: 'receipt',
  };
  assert.equal(outbox.enqueue({
    envelope: partial, explicitTargets: [], pendingNodeIds: [],
    deliveredReceipts: { delivered: receipt }, failedNodeIds: ['rejected'],
  }).status, 'failed');
  store.recordOutboundEnvelope(partial, ['delivered', 'rejected'], slotSession);

  const local = request('definitively-failed-but-local', 'stream:downstream');
  outbox.enqueue({ envelope: local, explicitTargets: [], pendingNodeIds: [] });
  outbox.markFailed(local.id, 'No remote delivery');
  store.recordOutboundEnvelope(local, [local.from.nodeId], slotSession);
  store.enqueue([local], { mailboxId: 'stream:downstream' });
  outboxReads.mock.resetCalls();
  assert.equal(inspect().immediateBusyReason, 'local_outbound_pending');
  assert.equal(outboxReads.mock.callCount(), 0);
  store.ack(store.check({ mailboxId: 'stream:downstream' }).deliveryId!, 'stream:downstream');

  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  store.recordInboundEnvelope(envelope('partial-answer', {
    replyToId: partial.id, semantic: { type: 'reply', threadId: partial.id },
  }), slotSession, { committed: false });

  const uncertain = request('uncertain');
  outbox.enqueue({ envelope: uncertain, explicitTargets: [], pendingNodeIds: ['remote'], inDoubtNodeIds: ['remote'] });
  assert.deepEqual(outbox.markFailed(uncertain.id, 'Unknown effect')?.inDoubtNodeIds, ['remote']);
  store.recordOutboundEnvelope(uncertain, ['remote'], slotSession);
  outboxReads.mock.resetCalls();
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  assert.equal(outboxReads.mock.callCount(), 1);

  const failed = request('no-effect');
  outbox.enqueue({ envelope: failed, explicitTargets: [], pendingNodeIds: [] });
  outbox.markFailed(failed.id, 'No effect');
  store.recordOutboundEnvelope(failed, ['remote'], slotSession);
  outboxReads.mock.resetCalls();
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  assert.equal(outboxReads.mock.callCount(), 1);
});

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
  assert.equal(resolveMailboxId(semantic, undefined, undefined, toMailboxTopicBinding({ executor: 'inbox' }).mode), 'stream:alerts');
  const payload = envelope('two', { payload: { mailbox: 'task:msg1' } });
  assert.equal(resolveMailboxId(payload), 'task:msg1');
  const both = envelope('three', {
    payload: { mailbox: 'payload:mailbox' },
    semantic: { type: 'message', threadId: 'three', targetMailbox: 'semantic:mailbox' },
  });
  assert.equal(resolveMailboxId(both), 'semantic:mailbox');
  assert.equal(
    resolveMailboxId(envelope('wave', { kind: 'alerts', payload: { streamKey: 'dev' } }), undefined, undefined, toMailboxTopicBinding({ executor: 'orca-wave' }).mode),
    'stream:dev',
  );
  assert.equal(resolveMailboxId(envelope('spawn'), undefined, undefined, toMailboxTopicBinding({ executor: 'orca-spawn' }).mode), 'task:spawn');
  assert.equal(resolveMailboxId(envelope('plain'), undefined, undefined, toMailboxTopicBinding({ executor: 'inbox' }).mode), 'default');
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
    resolveMailboxId(envelope('explicit-payload', { payload: { mailbox: 'task:custom' } }), toMailboxTopicBinding(binding), undefined, toMailboxTopicBinding({ executor: 'inbox' }).mode),
    'task:custom',
  );
  assert.equal(
    resolveMailboxId(envelope('thread-reply', { semantic: { type: 'message', threadId: 'thread-affinity' } }), toMailboxTopicBinding(binding), store.resolveThreadLocalMailbox('thread-affinity'), toMailboxTopicBinding({ executor: 'orca-wave' }).mode),
    'worktree:origin',
  );
  assert.equal(
    resolveMailboxId(envelope('control-reply', { semantic: { type: 'reply', threadId: 'thread-affinity' } }), toMailboxTopicBinding(binding), store.resolveThreadLocalMailbox('thread-affinity'), toMailboxTopicBinding({ executor: 'inbox' }).mode),
    'worktree:origin',
  );
  assert.equal(
    resolveMailboxId(envelope('explicit-session', {
        semantic: { type: 'message', threadId: 'thread-affinity', targetMailbox: 'worktree:explicit' },
      }), toMailboxTopicBinding(binding), 'worktree:origin', toMailboxTopicBinding({ executor: 'orca-wave' }).mode),
    'worktree:explicit',
  );
  assert.equal(
    resolveMailboxId(envelope('new-thread', {
        payload: { streamKey: 'inbox' },
        semantic: { type: 'question', threadId: 'new-thread', targetMailbox: 'default' },
      }), toMailboxTopicBinding(binding), undefined, toMailboxTopicBinding({ executor: 'inbox' }).mode),
    'stream:inbox',
  );
});

test('enqueue deduplicates per mailbox and sanitizes one-line wakeup hints', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  const first = store.enqueue([envelope('a')], { wakeupHint: '  line one\n\n  two   three  ' });
  assert.deepEqual(first, { inserted: ['a'], duplicateIds: [], mailboxIds: ['default'], envelopeMailboxes: { a: 'default' } });
  assert.deepEqual(store.enqueue([envelope('a')]), {
    inserted: [],
    duplicateIds: ['a'],
    mailboxIds: ['default'],
    envelopeMailboxes: { a: 'default' },
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
  const created = store.upsertWakeTarget('default', { driver: 'orca-run', coalesceMs: 50, driverConfig: { runId: 'run_1' } });
  assert.equal(created.pendingWake, false);
  const before = Date.now();
  store.enqueue([envelope('a')], {
    wakeTarget: { driver: 'orca-terminal', driverConfig: { terminalHandle: 'term_1' } },
  });
  const pending = store.listPendingWakeTargets(before + 1_000);
  assert.equal(pending.length, 1);
  assert.equal(pending[0].driver, 'orca-terminal');
  assert.equal(pending[0].driverConfig?.runId, undefined); // Switching drivers clears incompatible config.
  assert.equal(pending[0].driverConfig?.terminalHandle, 'term_1');
  assert.equal(pending[0].pendingWake, true);
  assert.ok(pending[0].earliestWakeAt >= before + 50);
  assert.deepEqual(store.listPendingWakeTargets(before), []);

  const reopened = new MailboxStore(path);
  assert.equal(
    reopened.listPendingWakeTargets(Date.now() + 1_000)[0].driverConfig?.terminalHandle,
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

test('pruneOutboundThreadRecord removes only orphan outbound legs and recomputes the thread', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  const question = envelope('msg_1', {
    semantic: { type: 'question', threadId: 'msg_1', replyMailbox: 'worktree:main' },
  });
  store.recordInboundEnvelope(question, 'task:msg_1');
  const reply = envelope('msg_2', {
    semantic: { type: 'reply', threadId: 'msg_1', replyMailbox: 'worktree:main' },
  });
  store.recordOutboundEnvelope(reply, ['ed25519:' + 'b'.repeat(64)], 'worktree:main');
  assert.equal(store.getThread('msg_1')!.state, 'replied');

  const pruned = store.pruneOutboundThreadRecord('msg_2')!;
  assert.equal(pruned.threadId, 'msg_1');
  assert.equal(pruned.remainingCount, 1);
  assert.equal(pruned.state, 'open');
  assert.equal(pruned.settlement, undefined);
  assert.equal(pruned.pruned.direction, 'outbound');
  assert.equal(pruned.pruned.messageId, 'msg_2');
  assert.equal(store.listThreadRecords('msg_1').length, 1);
  assert.equal(store.getThread('msg_1')!.state, 'open');
  assert.equal(store.getThread('msg_1')!.settlement, undefined);

  // Pruning an absent outbound leg is a no-op, but an inbound leg is never prunable.
  assert.equal(store.pruneOutboundThreadRecord('msg_2'), undefined);
  assert.throws(
    () => store.pruneOutboundThreadRecord('msg_1'),
    /Cannot prune inbound thread record/,
  );
  assert.throws(() => store.pruneOutboundThreadRecord(''), /Invalid message id/);
  assert.equal(
    store.getThread('msg_1')!.direction,
    'inbound',
    'a refused inbound prune leaves the record intact',
  );
});

test('pruneOutboundThreadRecord clears a stale settlement the removed leg introduced', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  store.recordInboundEnvelope(
    envelope('root', { semantic: { type: 'question', threadId: 'root' } }),
    'default',
  );
  store.recordOutboundEnvelope(
    envelope('done', {
      semantic: { type: 'settle', threadId: 'root', outcome: 'succeeded' },
      payload: { summary: 'settled before the send failed' },
    }),
    ['ed25519:' + 'b'.repeat(64)],
  );
  assert.equal(store.getThread('root')!.state, 'settled');
  assert.equal(store.getThread('root')!.settlement?.outcome, 'succeeded');

  const pruned = store.pruneOutboundThreadRecord('done')!;
  assert.equal(pruned.state, 'open');
  assert.equal(pruned.settlement, undefined);
  assert.equal(store.listThreadRecords('root').length, 1);
  const [remaining] = store.listThreadRecords('root');
  assert.equal(remaining.state, 'open');
  assert.equal(remaining.settlement, undefined);
  assert.equal(store.getThread('root')!.state, 'open');
  assert.equal(store.getThread('root')!.settlement, undefined);
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

  const original = envelope('signed');
  const projected = { ...original, body: 'execution projection' };
  store.enqueue([projected], { signedEnvelopes: { signed: original } });
  original.body = 'changed after enqueue';
  assert.equal(new MailboxStore(path).list().items[0].signedEnvelope?.body, 'body');
  const valid = readFileSync(path, 'utf8');
  for (const signedEnvelope of [{ ...envelope('signed'), payload: null }, envelope('wrong-id')]) {
    const saved = JSON.parse(valid);
    saved.items[0].signedEnvelope = signedEnvelope;
    writeFileSync(path, JSON.stringify(saved));
    assert.throws(() => new MailboxStore(path).list(), /Invalid mailbox store/);
  }
  writeFileSync(path, valid);
  assert.throws(() => store.enqueue([envelope('other')], {
    signedEnvelopes: { other: envelope('mismatched') },
  }), /Invalid mailbox store/);
  assert.equal(readFileSync(path, 'utf8'), valid, 'invalid provenance must not mutate the ledger');
  assert.equal(existsSync(`${path}.lock`), false);

  const badLegacy = join(root, 'bad-inbox.json');
  writeFileSync(badLegacy, '{');
  assert.throws(
    () => new MailboxStore(join(root, 'fresh.json'), { legacyInboxPath: badLegacy }),
    /Invalid inbox store/,
  );
  assert.equal(existsSync(badLegacy), true);
});

test('spawn bindings resolve inbound task identity without changing outbound bound mailboxes', () => {
  // Thread reverse routes must name the same task mailbox the spawn executor commits.
  for (const executor of ['orca-spawn', 'orca', 'codex', 'codex-cli', 'claude-code', 'claude-app', 'chatgpt-app', 'antigravity', 'antigravity-cli', 'antigravity-app', 'agent-pty']) {
    const binding = { executor, mode: 'spawn' as const };
    const message = envelope('bound-spawn');
    assert.equal(resolveMailboxId(message, toMailboxTopicBinding(binding), undefined, toMailboxTopicBinding({ executor: executor }).mode), 'task:bound-spawn', executor);
    assert.equal(resolveBoundMailboxId(message.kind, toMailboxTopicBinding(binding)), 'default', 'outbound binding has no stream reply mailbox');
    assert.equal(resolveMailboxId(message, toMailboxTopicBinding({ ...binding, mailboxId: 'explicit' }), undefined, toMailboxTopicBinding({ executor: executor }).mode), 'explicit');
    assert.equal(resolveMailboxId(message, toMailboxTopicBinding(binding), 'task:existing', toMailboxTopicBinding({ executor: executor }).mode), 'task:existing');
  }
});

test('v1 wake targets upgrade losslessly to strict v2 on the next mutation', async t => {
  const { path } = workspace(t);
  const base = { coalesceMs: 12, completedWaves: 3, pendingWake: true, earliestWakeAt: 1234, attemptCount: 2, wakeupPrefix: 'prefix', lastWakeError: 'unconfirmed' };
  const legacy = {
    passive: { ...base, mailboxId: 'passive', mode: 'passive' },
    residual: { ...base, mailboxId: 'residual', mode: 'passive', workspacePath: '/tmp/ws', interactiveCommand: 'codex' },
    host: { ...base, mailboxId: 'host', mode: 'passive', hostAgent: 'claude-code', sessionId: 's', conversationMode: 'fixed' },
    terminal: { ...base, mailboxId: 'terminal', mode: 'orca-terminal', terminalHandle: 'h', workspacePath: '/workspace', agent: 'codex', rotateAfterWaves: 4, deliveryInDoubt: true },
    run: { ...base, mailboxId: 'run', mode: 'orca-run', runId: 'r', worktreeId: 'w', futureOption: { values: [1, null, true] } },
  };
  const seed = new MailboxStore(path);
  const message = envelope('preserved');
  seed.enqueue([message]);
  seed.recordInboundEnvelope(message, 'default');
  const lease = seed.check();
  const original = JSON.parse(readFileSync(path, 'utf8'));
  writeFileSync(path, JSON.stringify({ ...original, schema: 'the-dealer-mailbox/v1', wakeTargets: legacy }));
  const store = new MailboxStore(path);
  assert.equal(store.getWakeTarget('passive')?.driver, 'passive');
  assert.equal(store.getWakeTarget('passive')?.driverConfig, undefined);
  assert.equal(store.getWakeTarget('host')?.driver, 'agent-host');
  assert.deepEqual(store.getWakeTarget('host')?.driverConfig, { hostAgent: 'claude-code', sessionId: 's', conversationMode: 'fixed' });
  assert.deepEqual(store.getWakeTarget('terminal'), { ...base, mailboxId: 'terminal', driver: 'orca-terminal', rotateAfterWaves: 4, deliveryInDoubt: true, driverConfig: { terminalHandle: 'h', workspacePath: '/workspace', agent: 'codex' } });
  assert.deepEqual(store.getWakeTarget('run')?.driverConfig, { runId: 'r', worktreeId: 'w', futureOption: { values: [1, null, true] } });
  assert.equal(JSON.parse(readFileSync(path, 'utf8')).schema, 'the-dealer-mailbox/v1');
  assert.equal(store.getWakeTarget('residual')?.driver, 'passive');
  assert.deepEqual(store.getWakeTarget('residual')?.driverConfig, { workspacePath: '/tmp/ws', interactiveCommand: 'codex' });
  const wakeups = new WakeupRedriveService({ store, drivers: [{
    name: 'mock-session', deliver: async () => ({ kind: 'noop', reason: 'unused' }),
    resolveSessionTarget: id => id === 'residual' ? { driver: 'mock-session' } : undefined,
  }] });
  t.after(() => wakeups.stop());
  assert.equal((await wakeups.resolveSessionWakeTarget('residual'))?.driver, 'mock-session');
  assert.equal((await wakeups.flushMailbox('residual', { force: true })).woke, true);
  assert.equal(store.getWakeTarget('residual')?.pendingWake, false);
  store.upsertWakeTarget('passive', { pendingWake: false });
  const persisted = JSON.parse(readFileSync(path, 'utf8'));
  assert.equal(persisted.schema, 'the-dealer-mailbox/v2');
  assert.deepEqual(persisted.items, original.items);
  assert.deepEqual(persisted.mailboxes, original.mailboxes);
  assert.deepEqual(persisted.threads, original.threads);
  assert.equal(new MailboxStore(path).check({ peek: true }).deliveryId, lease.deliveryId);
  assert.deepEqual(new MailboxStore(path).getWakeTarget('terminal'), store.getWakeTarget('terminal'));
  assert.deepEqual(new MailboxStore(path).getWakeTarget('host'), store.getWakeTarget('host'));
  persisted.wakeTargets.run.unknown = 'reject';
  writeFileSync(path, JSON.stringify(persisted));
  assert.throws(() => new MailboxStore(path).getWakeTarget('run'), /Invalid mailbox store/);
});

test('opaque driver configuration merges deletions, clears on driver switch, and rejects non-JSON values', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  const input = { nested: { keep: [true, null, 2] }, remove: 'value' };
  store.upsertWakeTarget('box', { driver: 'mock:one', driverConfig: input });
  input.nested.keep.push(false);
  store.upsertWakeTarget('box', { driverConfig: { remove: undefined, added: 'new' } });
  assert.deepEqual(new MailboxStore(path).getWakeTarget('box')?.driverConfig, { nested: { keep: [true, null, 2] }, added: 'new' });
  for (const value of [NaN, Infinity, { nested: undefined }, new Date(), () => {}]) {
    assert.throws(() => store.upsertWakeTarget('box', { driverConfig: { invalid: value } }));
  }
  for (const driver of ['', ' spaced', 'trailing ', 'a'.repeat(65), 'bad/driver']) {
    assert.throws(() => store.upsertWakeTarget('box', { driver }));
  }
  assert.throws(() => store.upsertWakeTarget('box', { staleField: true } as never));
  store.upsertWakeTarget('box', { driver: 'mock:two', driverConfig: { fresh: true } });
  assert.deepEqual(store.getWakeTarget('box')?.driverConfig, { fresh: true });
  store.upsertWakeTarget('box', { driver: 'mock:three' });
  assert.equal(store.getWakeTarget('box')?.driverConfig, undefined);
  store.upsertWakeTarget('box', { driverConfig: { fresh: true } });
  store.upsertWakeTarget('box', { driverConfig: undefined });
  assert.equal(store.getWakeTarget('box')?.driverConfig, undefined);
});

test('generic session pinning atomically binds delivered threads and preserves existing session fences', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  const first = envelope('first', { semantic: { type: 'message', threadId: 'first' } });
  const newer = envelope('newer', { semantic: { type: 'message', threadId: 'newer' } });
  const queued = envelope('queued', { semantic: { type: 'message', threadId: first.id } });
  store.enqueue([first], { mailboxId: 'stream:test', wakeTarget: { driver: 'mock', rotateAfterWaves: 2, wakeupPrefix: 'check --mailbox stream:test', driverConfig: { destination: 'next' } } });
  store.recordInboundEnvelope(first, 'stream:test');
  store.upsertWakeTarget('session:one', { driver: 'mock', completedWaves: 7, pendingWake: true, deliveryInDoubt: true, lastWakeError: 'unknown prior send' });
  const observed = store.check({ mailboxId: 'stream:test', peek: true }).items[0].sequence;
  store.beginWakeDelivery('stream:test');
  store.enqueue([newer], { mailboxId: 'stream:test' });
  store.recordInboundEnvelope(newer, 'stream:test');
  store.recordInboundEnvelope(queued, 'stream:test', { committed: false });
  store.completeWakeDelivery('stream:test', observed, {}, { sessionMailboxId: 'session:one', driverConfig: { destination: 'one' } });
  assert.equal(store.getWakeTarget('stream:test')?.pendingWake, true);
  assert.equal(store.resolveThreadLocalMailbox('first'), 'session:one');
  assert.equal(store.resolveThreadLocalMailbox('newer'), 'stream:test');
  assert.equal(store.getInboundThreadRecord(first.id, first.networkId)?.mailboxCommitted, true);
  assert.equal(store.getInboundThreadRecord(queued.id, queued.networkId)?.localMailbox, 'stream:test');
  assert.equal(store.getInboundThreadRecord(queued.id, queued.networkId)?.mailboxCommitted, false);
  const pinned = new MailboxStore(path).getWakeTarget('session:one')!;
  assert.equal(pinned.rotateAfterWaves, undefined);
  assert.equal(pinned.completedWaves, 7);
  assert.equal(pinned.deliveryInDoubt, true);
  assert.equal(pinned.pendingWake, true);
  assert.equal(pinned.lastWakeError, 'unknown prior send');
  assert.equal(pinned.wakeupPrefix, 'check --mailbox session:one');
  assert.equal(store.pinSessionMailbox('default', 'session:ignored'), 'default');
  assert.equal(store.getWakeTarget('session:ignored'), undefined);
});

test('a proposed spawn reuses a committed thread without changing its wake target', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  const root = envelope('root');
  const followup = envelope('followup', { semantic: { type: 'message', threadId: root.id } });
  const original = store.enqueue([root], { mailboxId: 'task:root', proposedSpawn: true, wakeTarget: { driver: 'owner' } });
  const concurrent = new MailboxStore(path).enqueue([followup], {
    mailboxId: 'task:followup', proposedSpawn: true, wakeTarget: { driver: 'redundant' },
  });
  assert.equal(original.claimedSpawn, true);
  assert.equal(concurrent.claimedSpawn, false);
  assert.deepEqual(concurrent.envelopeMailboxes, { followup: 'task:root' });
  assert.equal(store.getWakeTarget('task:root')?.driver, 'owner');
  assert.equal(store.getWakeTarget('task:followup'), undefined);
  const wave = envelope('wave', { semantic: { type: 'message', threadId: root.id, targetMailbox: 'default' } });
  assert.deepEqual(store.enqueue([wave], {
    mailboxId: 'stream:requests', wakeTarget: { driver: 'wave' },
  }).envelopeMailboxes, { wave: 'task:root' });
  assert.equal(store.getWakeTarget('stream:requests'), undefined);
  assert.equal(store.getWakeTarget('task:root')?.driver, 'owner');
});

test('legacy v2 inbound routes follow the stored delivery despite an unrelated wake target', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  const stale = envelope('stale');
  const history = envelope('history');
  store.enqueue([stale], { mailboxId: 'task:root' });
  store.ack(store.check({ mailboxId: 'task:root' }).deliveryId!, 'task:root');
  store.upsertWakeTarget('stream:requests', { driver: 'local' });
  store.recordInboundEnvelope(stale, 'stream:requests');
  store.recordInboundEnvelope(history, 'history-hint');
  const snapshot = JSON.parse(readFileSync(path, 'utf8'));
  for (const record of Object.values(snapshot.threads) as Array<{ mailboxCommitted?: boolean }>) delete record.mailboxCommitted;
  writeFileSync(path, JSON.stringify(snapshot));

  const reloaded = new MailboxStore(path);
  assert.equal(reloaded.resolveThreadLocalMailbox(stale.id, stale.networkId), 'task:root');
  assert.equal(reloaded.resolveThreadLocalMailbox(history.id, history.networkId), undefined);
  assert.equal(reloaded.getThread(history.id)?.localMailbox, 'history-hint');
  assert.equal(reloaded.getThread(history.id)?.mailboxCommitted, false);
});

for (const early of ['terminal:early', 'session:one']) test(`an upgraded legacy rotating thread retains ${early} through subsequent rotation`, t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  const stream = 'stream:requests';
  const first = envelope('first');
  store.enqueue([first], { mailboxId: stream, wakeTarget: { driver: 'orca-terminal', rotateAfterWaves: 1 } });
  const firstDelivery = store.check({ mailboxId: stream });
  store.completeWakeDelivery(stream, firstDelivery.items[0].sequence, {}, { sessionMailboxId: early });
  store.ack(firstDelivery.deliveryId!, stream);
  const snapshot = JSON.parse(readFileSync(path, 'utf8'));
  delete snapshot.threads['inbound:first'].mailboxCommitted;
  writeFileSync(path, JSON.stringify(snapshot));

  const upgraded = new MailboxStore(path);
  assert.equal(upgraded.resolveThreadLocalMailbox(first.id, first.networkId), early);
  const next = envelope('next');
  upgraded.enqueue([next], { mailboxId: stream });
  const nextDelivery = upgraded.check({ mailboxId: stream });
  upgraded.completeWakeDelivery(stream, nextDelivery.items[0].sequence, {}, { sessionMailboxId: 'terminal:later' });
  assert.equal(upgraded.resolveThreadLocalMailbox(first.id, first.networkId), early);
  assert.equal(upgraded.resolveThreadLocalMailbox(next.id, next.networkId), 'terminal:later');
});

test('reply lookup trusts a stored outbound semantic address over a legacy mailbox hint', t => {
  const { path } = workspace(t);
  const store = new MailboxStore(path);
  const explicit = envelope('explicit', { semantic: { type: 'message', threadId: 'explicit', targetMailbox: 'task:peer' } });
  const hint = envelope('hint', { payload: { mailbox: 'wrong-hint' } });
  store.recordOutboundEnvelope(explicit, [explicit.from.nodeId]);
  store.recordOutboundEnvelope(hint, [hint.from.nodeId]);
  const snapshot = JSON.parse(readFileSync(path, 'utf8'));
  snapshot.threads['outbound:explicit'].peerReplyMailbox = 'wrong-hint';
  snapshot.threads['outbound:hint'].peerReplyMailbox = 'wrong-hint';
  writeFileSync(path, JSON.stringify(snapshot));

  assert.equal(store.resolveReplyTarget(explicit.id, explicit.networkId)?.targetMailbox, 'task:peer');
  assert.equal(store.resolveReplyTarget(hint.id, hint.networkId)?.targetMailbox, 'default');
});
