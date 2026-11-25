import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { MailboxStore } from '../packages/mailbox/store.ts';
import {
  WakeupRedriveService,
  SettledWakeLaunchError,
  recordWakeFailure,
} from '../packages/mailbox/wakeup.ts';
import type { WakeupDriver } from '../packages/mailbox/wakeup.ts';
import { CommandNotStartedError } from '../packages/subprocess/index.ts';
import type { MessageEnvelope } from '../packages/core-types/index.ts';

function fixture(t: { after: (fn: () => void) => void }) {
  const root = mkdtempSync(join(tmpdir(), 'dealer-drivers-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const message: MessageEnvelope = {
    id: 'one',
    networkId: 'test',
    traceId: 'one',
    from: { nodeId: 'peer', member: 'peer', nodeName: 'peer' },
    routing: { mode: 'broadcast' },
    kind: 'task',
    priority: 'normal',
    subject: 'one',
    body: 'body',
    payload: {},
    attachments: [],
    createdAt: new Date().toISOString(),
    signature: '',
  };
  store.enqueue([message], { mailboxId: 'box', wakeTarget: { driver: 'mock', pendingWake: true } });
  return store;
}

test('driver registry resolves in order, protects existing routes, and unregisters without dropping pending work', async (t) => {
  const store = fixture(t);
  const seen: string[] = [];
  const first: WakeupDriver = {
    name: 'first',
    deliver: async () => ({ kind: 'noop', reason: 'idle' }),
    resolveSessionTarget: () => {
      seen.push('first');
      return undefined;
    },
  };
  const second: WakeupDriver = {
    name: 'second',
    deliver: first.deliver,
    resolveSessionTarget: () => {
      seen.push('second');
      return { driver: 'second', driverConfig: { key: 'session' } };
    },
  };
  const service = new WakeupRedriveService({
    store,
    drivers: [first, second],
    now: () => 100,
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());
  assert.deepEqual(await service.resolveSessionWakeTarget('new'), {
    driver: 'second',
    driverConfig: { key: 'session' },
  });
  assert.deepEqual(seen, ['first', 'second']);
  assert.equal(await service.resolveSessionWakeTarget('box'), undefined);
  assert.equal(seen.length, 2);
  const missing = await service.flushMailbox('box', { force: true });
  assert.match(missing.reason!, /Unsupported wakeup driver: mock/);
  assert.equal(store.getWakeTarget('box')?.earliestWakeAt, 1100);
  let deliveries = 0;
  const registration = service.registerDriver({
    name: 'mock',
    async deliver({ store, mailboxId, preview }) {
      deliveries++;
      store.beginWakeDelivery(mailboxId);
      store.completeWakeDelivery(mailboxId, preview.items[0].sequence);
      return { kind: 'delivered', pointer: 'pointer' };
    },
  });
  assert.throws(() => service.registerDriver(first), /already registered/);
  assert.equal((await service.flushMailbox('box', { force: true })).pointer, 'pointer');
  assert.equal(deliveries, 1);
  registration.dispose();
  store.upsertWakeTarget('box', { pendingWake: true, earliestWakeAt: 1000 });
  assert.match((await service.flushMailbox('box', { force: true })).reason!, /Unsupported wakeup driver/);
});

test('generic driver outcomes preserve scheduling and settled failure classification', async (t) => {
  const store = fixture(t);
  let outcome: 'defer' | 'noop' | 'disarm' | 'settled' = 'defer';
  const service = new WakeupRedriveService({
    store,
    now: () => 0,
    flushIntervalMs: 100_000,
    drivers: [
      {
        name: 'mock',
        async deliver({ store, mailboxId }) {
          if (outcome === 'settled') {
            recordWakeFailure(store, mailboxId, 'already settled', 0);
            throw new SettledWakeLaunchError('already settled');
          }
          return {
            kind: outcome,
            reason: outcome,
            ...(outcome === 'defer' ? { patch: { driverConfig: { checkpoint: 1 } } } : {}),
          };
        },
      },
    ],
  });
  t.after(() => service.stop());
  await service.flushMailbox('box', { force: true });
  assert.equal(store.getWakeTarget('box')?.attemptCount, 1);
  assert.deepEqual(store.getWakeTarget('box')?.driverConfig, { checkpoint: 1 });
  outcome = 'noop';
  await service.flushMailbox('box', { force: true });
  assert.equal(store.getWakeTarget('box')?.attemptCount, 1);
  outcome = 'settled';
  await service.flushMailbox('box', { force: true });
  assert.equal(store.getWakeTarget('box')?.attemptCount, 2);
  outcome = 'disarm';
  await service.flushMailbox('box', { force: true });
  assert.equal(store.getWakeTarget('box')?.pendingWake, false);
});

test('generic drivers retry proven non-starts and resume pointer fences after matching consumption', async (t) => {
  const store = fixture(t);
  let starts = 0;
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: [
      {
        name: 'mock',
        async deliver({ store, mailboxId, preview }) {
          starts++;
          store.beginWakeDelivery(mailboxId, undefined, { deliveredSequence: preview.items[0].sequence });
          if (starts === 1) throw new CommandNotStartedError('missing binary');
          if (starts === 2) throw new Error('lost receipt');
          store.completeWakeDelivery(mailboxId, preview.items[0].sequence);
          return { kind: 'delivered', pointer: 'pointer' };
        },
      },
    ],
  });
  t.after(() => service.stop());
  await service.flushMailbox('box', { force: true });
  assert.equal(store.getWakeTarget('box')?.deliveryInDoubt, undefined);
  assert.equal(store.getWakeTarget('box')?.pendingWake, true);
  await service.flushMailbox('box', { force: true });
  assert.equal(store.getWakeTarget('box')?.deliveryInDoubt, true);
  await service.flushMailbox('box', { force: true });
  assert.equal(starts, 2);

  const lease = store.check({ mailboxId: 'box' });
  const [original] = lease.items;
  store.enqueue([{ ...original.envelope, id: 'next' }], { mailboxId: 'box' });
  store.ack(lease.deliveryId!, 'box');
  assert.equal((await service.flushMailbox('box')).woke, true);
  assert.equal(starts, 3);
});

test('synchronous driver events cannot reenter an in-flight mailbox delivery', async (t) => {
  const store = fixture(t);
  const initial = store.check({ mailboxId: 'box', peek: true }).items[0].envelope;
  let active = 0;
  let peak = 0;
  let calls = 0;
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: [
      {
        name: 'mock',
        async deliver({ mailboxId, preview }) {
          active++;
          peak = Math.max(peak, active);
          calls++;
          if (calls === 1) store.enqueue([{ ...initial, id: 'second' }], { mailboxId });
          await Promise.resolve();
          store.beginWakeDelivery(mailboxId);
          store.completeWakeDelivery(mailboxId, preview.items.at(-1)!.sequence);
          active--;
          return { kind: 'delivered', pointer: 'pointer' };
        },
      },
    ],
  });
  t.after(() => service.stop());
  await service.flushMailbox('box', { force: true });
  await service.flushMailbox('box', { force: true });
  assert.equal(peak, 1);
  assert.equal(calls, 2);
  assert.equal(store.getWakeTarget('box')?.pendingWake, false);
});
