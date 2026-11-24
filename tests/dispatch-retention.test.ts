import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { generateNodeKeyPair, signEnvelope } from '../packages/core-types/index.ts';
import type { DeliveryReceipt, MessageEnvelope, OutboxItem } from '../packages/core-types/index.ts';
import { writeFileAtomically } from '../packages/fs-atomic/index.ts';
import { openStateLedgers } from '../packages/cli/ledgers.ts';
import { MailboxStore } from '../packages/mailbox/store.ts';

function stores(t: TestContext) {
  const root = mkdtempSync(join(tmpdir(), 'dealer-retention-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return openStateLedgers(root);
}

const keys = generateNodeKeyPair();
const old = '2025-01-01T00:00:00.000Z';

function envelope(id: string, patch: Partial<MessageEnvelope> = {}): MessageEnvelope {
  return {
    id, networkId: 'team', traceId: id, from: { nodeId: keys.nodeId, member: 'alice', nodeName: 'local' },
    routing: { mode: 'broadcast' }, kind: 'wave.alert.v1', priority: 'normal', subject: id, body: '',
    payload: { revision: 1 }, attachments: [], createdAt: old, signature: '', ...patch,
  };
}

function receipt(messageId: string, stage: 'dispatched' | 'queued'): DeliveryReceipt {
  return {
    messageId, networkId: 'team', fromNodeId: keys.nodeId, stage, updatedAt: old,
    signature: '', executionReceipt: { durable: true },
  };
}

function terminalHistory(base: OutboxItem, count: number): OutboxItem[] {
  return Array.from({ length: count }, (_, index) => {
    const id = `delivery-${index}`;
    return {
      ...base, id, envelope: envelope(id), status: 'delivered',
      updatedAt: new Date(Date.parse(old) + index + 1).toISOString(),
      deliveredReceipts: { peer: receipt(id, 'dispatched') },
    };
  });
}

test('ledger snapshots reuse validation and detect offline atomic replacement and deletion', t => {
  const { mailbox, outbox } = stores(t);
  const message = envelope('original');
  mailbox.enqueue([message]);
  outbox.enqueue({ envelope: message, explicitTargets: [], pendingNodeIds: [] });
  const parse = JSON.parse;
  const parsed: string[] = [];
  t.mock.method(JSON, 'parse', (raw: string) => {
    const value = parse(raw);
    if (typeof value?.schema === 'string' && value.schema.startsWith('the-dealer-')) parsed.push(value.schema);
    return value;
  });

  for (const store of [mailbox, outbox]) {
    const list = () => store instanceof MailboxStore ? store.list().items : store.list();
    const before = list();
    before[0].envelope.subject = 'caller mutation';
    const ledgerReads = parsed.length;
    assert.equal(list()[0].envelope.subject, 'original');
    assert.equal(parsed.length, ledgerReads);

    const replacement = parse(readFileSync(store.path, 'utf8'));
    replacement.items[0].envelope.subject = 'offline!';
    writeFileAtomically(store.path, JSON.stringify(replacement));
    assert.equal(list()[0].envelope.subject, 'offline!');
    assert.equal(parsed.length, ledgerReads + 1);

    rmSync(store.path);
    assert.deepEqual(list(), []);
  }
});

test('mailbox acknowledgement caps historical items and one-way threads while preserving active custody', t => {
  const { mailbox } = stores(t);
  const history = Array.from({ length: 205 }, (_, index) => envelope(`wave-${String(index).padStart(3, '0')}`));
  const signed = signEnvelope(history.at(-1)!, keys.privateKeyPem);
  history[history.length - 1] = { ...signed, subject: 'local projection' };
  mailbox.enqueue(history, { mailboxId: 'history', signedEnvelopes: { [signed.id]: signed } });
  const leased = envelope('leased', { semantic: { threadId: history[0].id, type: 'message', targetMailbox: 'active' } });
  mailbox.enqueue([leased], { mailboxId: 'active' });
  // Give the independent active mailbox a lease and keep a second item behind it.
  mailbox.check({ mailboxId: 'active' });
  mailbox.enqueue([envelope('queued')], { mailboxId: 'active' });
  mailbox.recordInboundEnvelope(envelope('collaboration', { kind: 'collab.question' }), 'default');
  const reply = envelope('reply', { replyToId: history[1].id, semantic: { threadId: history[1].id, type: 'reply' } });
  mailbox.recordOutboundEnvelope(reply, ['peer']);
  // Give the bulk historical fixture a deterministic order distinct from the protected records.
  const disk = JSON.parse(readFileSync(mailbox.path, 'utf8'));
  for (const [index, message] of history.entries()) {
    disk.threads[`inbound:${message.id}`].updatedAt = new Date(Date.parse(old) + index).toISOString();
  }
  writeFileAtomically(mailbox.path, JSON.stringify(disk));
  mailbox.ack(mailbox.check({ mailboxId: 'history' }).deliveryId!, 'history');

  const view = mailbox.list({ includeAcked: true });
  assert.equal(view.ackedCount, 200);
  assert.equal(view.leasedCount, 1);
  assert.equal(view.queuedCount, 1);
  assert.ok(!view.items.some(item => item.id === history[4].id));
  assert.deepEqual(view.items.find(item => item.id === signed.id)?.signedEnvelope, signed);
  assert.ok(mailbox.hasConfirmedEnvelope('team', keys.nodeId, signed.id, 1));
  assert.equal(mailbox.getThread(history[0].id)?.threadId, history[0].id);
  assert.equal(mailbox.getThread(history[1].id)?.state, 'replied');
  assert.equal(mailbox.getThread('collaboration')?.state, 'open');
  assert.equal(mailbox.getThread(history[104].id), undefined);
  assert.ok(mailbox.getThread(history[105].id));
});

test('an acknowledged wave question retains its reply route and session after later alerts', t => {
  const { mailbox } = stores(t);
  const question = envelope('old-question', {
    semantic: { type: 'question', threadId: 'old-question', replyMailbox: 'terminal:peer' },
  });
  mailbox.enqueue([question], { mailboxId: 'terminal:existing' });
  mailbox.ack(mailbox.check({ mailboxId: 'terminal:existing' }).deliveryId!, 'terminal:existing');
  mailbox.enqueue(Array.from({ length: 205 }, (_, index) => envelope('new-wave-' + index)));
  const disk = JSON.parse(readFileSync(mailbox.path, 'utf8'));
  disk.threads['inbound:old-question'].updatedAt = old;
  writeFileAtomically(mailbox.path, JSON.stringify(disk));
  mailbox.ack(mailbox.check().deliveryId!);
  assert.equal(mailbox.resolveReplyTarget(question.id)?.targetMailbox, 'terminal:peer');
  assert.equal(mailbox.resolveReplyTarget(question.id)?.localMailbox, 'terminal:existing');
});

test('mailbox trims whole delivered loopback wave groups and keeps their outbox CAS winners across restart', t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse(old) });
  const { mailbox, outbox } = stores(t);
  const offline = new MailboxStore(mailbox.path);
  const question = envelope('protected-question', { semantic: { type: 'question', threadId: 'protected-question' } });
  const replied = envelope('protected-replied');
  const unacked = envelope('protected-unacked');
  for (const message of [question, replied, unacked]) offline.recordOutboundEnvelope(message, [keys.nodeId]);
  offline.recordInboundEnvelope(question, 'default');
  offline.recordInboundEnvelope(envelope('wave-reply', {
    replyToId: replied.id, semantic: { type: 'reply', threadId: replied.id },
  }), 'default');
  offline.enqueue([unacked]);

  const history = Array.from({ length: 102 }, (_, index) => envelope(`loopback-${index}`, {
    idempotencyKey: `wave-key-${index}`,
    ...(index === 0 ? { semantic: { type: 'message', threadId: 'loopback-0' } } : {}),
  }));
  for (const [index, message] of history.entries()) {
    t.mock.timers.tick(1);
    offline.recordOutboundEnvelope(message, [keys.nodeId]);
    if (index !== 1) offline.recordInboundEnvelope(message, 'default');
    outbox.enqueue({ envelope: message, explicitTargets: [], pendingNodeIds: [],
      deliveredReceipts: { [keys.nodeId]: receipt(message.id, 'dispatched') } });
  }
  assert.equal(mailbox.listThreadRecords(history[0].id).length, 2);
  assert.equal(mailbox.listThreadRecords(history[1].id).length, 1);
  const oldest = history[0];
  const winner = outbox.findKeyedObligation(oldest.networkId, oldest.idempotencyKey!, 0);
  assert.equal(winner?.status, 'delivered');
  const outboxBefore = readFileSync(outbox.path, 'utf8');

  mailbox.upsertWakeTarget('stream:alerts', { pendingWake: false });

  // Reconcile without write-time outbox retention so any restored outbound leg stays visible.
  const restarted = new MailboxStore(mailbox.path);
  for (const message of history.slice(0, 2)) assert.equal(restarted.listThreadRecords(message.id).length, 0, message.id);
  restarted.reconcileOutboxThreads(outbox);
  for (const message of history.slice(0, 2)) assert.equal(restarted.listThreadRecords(message.id).length, 0, message.id);
  for (const message of history.slice(2)) {
    assert.deepEqual(restarted.listThreadRecords(message.id).map(record => record.direction).sort(), ['inbound', 'outbound']);
  }
  assert.equal(restarted.listThreadRecords(question.id).length, 2);
  assert.deepEqual(restarted.listThreadRecords(replied.id).map(record => record.state), ['replied', 'replied']);
  assert.equal(restarted.listThreadRecords(unacked.id).length, 2);
  assert.equal(readFileSync(outbox.path, 'utf8'), outboxBefore);
  assert.deepEqual(outbox.findKeyedObligation(oldest.networkId, oldest.idempotencyKey!, 0), winner);
});

test('outbox restores and projects in-flight waves while excluding confirmed one-way history', t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse(old) });
  const { mailbox, outbox } = stores(t);
  const cases: { id: string; patch: Partial<OutboxItem>; projects?: boolean; kind?: string }[] = [
    { id: 'queued', patch: { status: 'queued' } },
    { id: 'pending', patch: { pendingNodeIds: ['other'] } },
    { id: 'in-doubt', patch: { inDoubtNodeIds: ['other'] } },
    { id: 'relay-spooled', patch: { relaySpooledNodeIds: ['other'] } },
    { id: 'uncommitted', patch: { sentCommitted: false } },
    { id: 'active-attempt', patch: { activeAttempt: { token: 'active', expiresAt: '2025-01-02T00:00:00.000Z' } } },
    { id: 'queued-receipt', patch: { deliveredReceipts: { peer: receipt('queued-receipt', 'queued') } } },
    { id: 'confirmed', patch: {}, projects: false },
    { id: 'release', patch: {}, kind: 'release.prod.v1' },
  ];
  const base = outbox.enqueue({ envelope: envelope('base'), explicitTargets: [], pendingNodeIds: [] });
  for (const { id } of cases) mailbox.enqueue([envelope(`active-${id}`)], { mailboxId: `stream:${id}` });
  writeFileAtomically(outbox.path, JSON.stringify({
    schema: 'the-dealer-outbox/v2',
    items: cases.map(({ id, patch, kind }) => ({
      ...base, id, status: 'delivered', deliveredReceipts: { peer: receipt(id, 'dispatched') },
      envelope: envelope(id, { kind: kind ?? base.envelope.kind,
        semantic: { type: 'message', threadId: id, replyMailbox: `stream:${id}` } }),
      ...patch,
    })),
  }));

  t.mock.timers.tick(1);
  mailbox.enqueue(Array.from({ length: 100 }, (_, index) => envelope(`history-${index}`)));
  mailbox.ack(mailbox.check().deliveryId!);
  for (const { id, projects = true } of cases) {
    assert.equal(mailbox.getThread(id), undefined, id);
    assert.equal(mailbox.inspectSlotObligations(`stream:${id}`, { outbox }).immediateBusyReason,
      projects ? 'outbound_thread_open' : undefined, id);
  }

  mailbox.reconcileOutboxThreads(outbox);

  for (const { id, projects = true } of cases) {
    assert.equal(mailbox.getThread(id)?.direction, projects ? 'outbound' : undefined, id);
    assert.equal(mailbox.inspectSlotObligations(`stream:${id}`, { outbox }).immediateBusyReason,
      projects ? 'outbound_thread_open' : undefined, id);
  }
  assert.ok(mailbox.getThread('history-0'));
});

test('a delivered loopback wave keeps its originating slot busy until downstream acknowledgement', t => {
  const { mailbox, outbox } = stores(t);
  const origin = 'stream:origin';
  const downstream = 'stream:downstream';
  const wave = envelope('interrupted-loopback', {
    idempotencyKey: 'interrupted-loopback',
    semantic: { type: 'message', threadId: 'interrupted-loopback', replyMailbox: origin, targetMailbox: downstream },
  });
  const claim = outbox.claimIdempotency({ envelope: wave, explicitTargets: [], plannedTargetNodeIds: [keys.nodeId] });
  mailbox.enqueue([wave], { mailboxId: downstream });
  const delivered = outbox.enqueue({ envelope: wave, explicitTargets: [], pendingNodeIds: [],
    attemptToken: claim.attemptToken, deliveredReceipts: { [keys.nodeId]: receipt(wave.id, 'dispatched') } });
  assert.equal(delivered.status, 'delivered');

  const restarted = new MailboxStore(mailbox.path, { outbox });
  const inspect = () => restarted.inspectSlotObligations(origin, { outbox }).immediateBusyReason;
  assert.deepEqual(restarted.listThreadRecords(wave.id).map(record => record.direction), ['inbound']);
  assert.equal(inspect(), 'local_outbound_pending');

  restarted.reconcileOutboxThreads(outbox);

  assert.deepEqual(restarted.listThreadRecords(wave.id).map(record => record.direction).sort(), ['inbound', 'outbound']);
  const downstreamLease = restarted.check({ mailboxId: downstream });
  assert.equal(inspect(), 'local_outbound_pending');
  restarted.ack(downstreamLease.deliveryId!, downstream);
  assert.equal(inspect(), undefined);
});

test('mailbox without an outbox trims inbound wave history while keeping loopback groups', t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse(old) });
  const { mailbox } = stores(t);
  const offline = new MailboxStore(mailbox.path);
  const loopback = envelope('offline-loopback');
  offline.recordOutboundEnvelope(loopback, [keys.nodeId]);
  offline.recordInboundEnvelope(loopback, 'default');
  offline.recordInboundEnvelope(envelope('old-inbound'), 'default');
  t.mock.timers.tick(1);
  offline.enqueue(Array.from({ length: 100 }, (_, index) => envelope(`inbound-${index}`)));
  offline.ack(offline.check().deliveryId!);

  assert.equal(offline.getThread('old-inbound'), undefined);
  assert.equal(offline.listThreadRecords(loopback.id).length, 2);
  assert.equal(offline.listThreads({ kind: loopback.kind }).length, 101);
});

test('first mailbox write compacts old settled threads as whole routes and preserves active ones', t => {
  const { mailbox } = stores(t);
  mailbox.enqueue([envelope('protected')]);
  mailbox.recordInboundEnvelope(envelope('settle', {
    replyToId: 'protected',
    semantic: { threadId: 'protected', type: 'settle', outcome: 'succeeded' },
  }), 'default');
  const state = JSON.parse(readFileSync(mailbox.path, 'utf8'));
  const root = state.threads['inbound:protected'];
  const settle = state.threads['inbound:settle'];
  for (let index = 0; index < 201; index++) {
    const threadId = `settled-${index}`;
    const updatedAt = new Date(Date.parse(old) + index).toISOString();
    const settleMessageId = `${threadId}-final`;
    state.threads[`inbound:${threadId}`] = { ...root, envelope: undefined, threadId, messageId: threadId, updatedAt };
    state.threads[`outbound:${settleMessageId}`] = {
      ...settle, direction: 'outbound', envelope: undefined, threadId, messageId: settleMessageId, updatedAt,
    };
  }
  writeFileAtomically(mailbox.path, JSON.stringify(state));

  mailbox.upsertWakeTarget('another-slot', { pendingWake: false });

  assert.equal(mailbox.getThread('settled-0'), undefined);
  assert.equal(mailbox.getThread('settled-0-final'), undefined);
  assert.equal(mailbox.listThreadRecords('settled-1').length, 2);
  assert.equal(mailbox.listThreadRecords('protected').length, 2);
  assert.equal(mailbox.listThreads({ state: 'settled' }).length, 201);
});

test('outbox caps terminal history without dropping pending, unconfirmed or unsuperseded work', t => {
  const { outbox } = stores(t);
  const base = outbox.enqueue({ envelope: envelope('base'), explicitTargets: [], pendingNodeIds: [] });
  const item = (id: string, patch: Partial<typeof base> = {}) => ({ ...base, id, envelope: envelope(id), ...patch });
  writeFileAtomically(outbox.path, JSON.stringify({
    schema: 'the-dealer-outbox/v2',
    items: [
      ...terminalHistory(base, 200),
      item('queued', { updatedAt: old, pendingNodeIds: ['peer'] }),
      item('failed', { updatedAt: old, status: 'failed' }),
      item('pending', { updatedAt: old, status: 'delivered', pendingNodeIds: ['other'], deliveredReceipts: { peer: receipt('pending', 'dispatched') } }),
      item('unconfirmed', { updatedAt: old, status: 'delivered', deliveredReceipts: { peer: receipt('unconfirmed', 'queued') } }),
      item('superseded', { updatedAt: old, status: 'failed', supersededById: 'replacement' }),
      item('replacement', { pendingNodeIds: ['peer'] }),
    ],
  }));
  outbox.markNodeDelivered('replacement', 'peer', receipt('replacement', 'dispatched'));

  assert.equal(outbox.get('delivery-0'), undefined);
  assert.equal(outbox.get('superseded'), undefined);
  assert.equal(outbox.get('delivery-1')?.status, 'delivered');
  assert.equal(outbox.get('replacement')?.status, 'delivered');
  for (const id of ['queued', 'failed', 'pending', 'unconfirmed']) assert.ok(outbox.get(id), id);
  assert.equal(outbox.list().length, 204);
});

test('expiring superseded outbox history cannot revive a failed outbound question', t => {
  const { mailbox, outbox } = stores(t);
  const slot = 'stream:alerts';
  const question = envelope('superseded-question', {
    semantic: { type: 'question', threadId: 'superseded-question', replyMailbox: slot },
  });
  mailbox.recordOutboundEnvelope(question, ['peer'], slot);
  const base = outbox.enqueue({ envelope: question, explicitTargets: [], pendingNodeIds: [] });
  writeFileAtomically(outbox.path, JSON.stringify({
    schema: 'the-dealer-outbox/v2',
    items: [...terminalHistory(base, 200), {
      ...base, status: 'failed', supersededById: 'replacement', unattemptedFailure: true, updatedAt: old,
    }],
  }));
  const inspect = () => mailbox.inspectSlotObligations(slot, { outbox }).immediateBusyReason;
  assert.equal(inspect(), undefined);
  outbox.enqueue({ envelope: envelope('replacement'), explicitTargets: [], pendingNodeIds: ['peer'] });

  assert.equal(inspect(), undefined);
  assert.ok(outbox.get(question.id));
  mailbox.pruneOutboundThreadRecord(question.id);
  outbox.markNodeDelivered('replacement', 'peer', receipt('replacement', 'dispatched'));
  assert.equal(outbox.get(question.id), undefined);
});

test('outbox reconciliation cannot reopen a compacted settled question', t => {
  const { mailbox, outbox } = stores(t);
  const slot = 'stream:alerts';
  const question = envelope('settled-question', {
    semantic: { type: 'question', threadId: 'settled-question', replyMailbox: slot },
  });
  mailbox.recordOutboundEnvelope(question, ['peer'], slot);
  const delivered = outbox.enqueue({ envelope: question, explicitTargets: [], pendingNodeIds: [], deliveredReceipts: {
    peer: receipt(question.id, 'dispatched'),
  } });
  mailbox.recordInboundEnvelope(envelope('settle-question', {
    replyToId: question.id, semantic: { type: 'settle', threadId: question.id, outcome: 'succeeded' },
  }), slot);
  const state = JSON.parse(readFileSync(mailbox.path, 'utf8'));
  const settle = state.threads['inbound:settle-question'];
  state.threads['outbound:settled-question'].updatedAt = old;
  settle.updatedAt = old;
  for (let index = 0; index < 200; index++) {
    const threadId = `settled-${index}`;
    state.threads[`inbound:${threadId}`] = {
      ...settle, envelope: undefined, threadId, messageId: threadId,
      updatedAt: new Date(Date.parse(old) + index + 1).toISOString(),
    };
  }
  writeFileAtomically(mailbox.path, JSON.stringify(state));

  const inspect = () => mailbox.inspectSlotObligations(slot, { outbox }).immediateBusyReason;
  assert.equal(inspect(), undefined);
  mailbox.upsertWakeTarget(slot, { pendingWake: false });
  mailbox.reconcileOutboxThreads(outbox);

  assert.equal(inspect(), undefined);
  assert.equal(mailbox.getThread(question.id)?.state, 'settled');

  writeFileAtomically(outbox.path, JSON.stringify({
    schema: 'the-dealer-outbox/v2',
    items: [{ ...delivered, updatedAt: old }, ...terminalHistory(delivered, 200)],
  }));
  outbox.enqueue({ envelope: envelope('next'), explicitTargets: [], pendingNodeIds: ['peer'] });
  assert.equal(outbox.get(question.id), undefined);
  mailbox.upsertWakeTarget(slot, { pendingWake: false });
  mailbox.reconcileOutboxThreads(outbox);
  assert.equal(mailbox.getThread(question.id), undefined);
  assert.equal(inspect(), undefined);
});
