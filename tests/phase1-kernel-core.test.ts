import test from 'node:test';
import assert from 'node:assert/strict';
import { createPublicKey, verify, createHash, createHmac, createDecipheriv } from 'node:crypto';
import {
  canonicalJson, generateNodeKeyPair, signCanonicalPayload, verifyCanonicalPayload,
  signNodeCard, verifyNodeCard, signEnvelope, verifyEnvelope, signReceipt, verifyReceipt,
  deriveNetworkKeys, sealNetworkFrame, unsealNetworkFrame,
} from '../packages/core-types/index.ts';
import type { NodeCard, MessageEnvelope, DeliveryReceipt } from '../packages/core-types/index.ts';
import { Kernel, composeProfile } from '../packages/kernel/index.ts';
import type { Plugin } from '../packages/kernel/index.ts';

declare module '../packages/kernel/index.ts' {
  interface Services { counter: { value: number }; derived: { value: number } }
  interface Events {
    observe: (value: number) => unknown;
    work: () => unknown;
    flow: (value: number, label: string, next: (value: number) => Promise<number>) => number | Promise<number>;
    gate: () => unknown;
  }
}

const identity = generateNodeKeyPair();
const card: Omit<NodeCard, 'signature'> = {
  nodeId: identity.nodeId, networkId: 'test', revision: 1,
  identity: { member: 'ben', nodeName: 'test-node' }, admissionMode: 'open',
  capabilities: { summary: 'testing', skills: [] },
  runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] }, updatedAt: '2026-11-09T00:00:00Z',
};
const envelope: Omit<MessageEnvelope, 'signature'> = {
  id: 'message-1', networkId: 'test', traceId: 'trace-1', from: { nodeId: identity.nodeId, ...card.identity },
  routing: { mode: 'auto' }, kind: 'task', priority: 'normal', subject: 'test', body: 'hello',
  payload: { signature: 'nested business data' }, attachments: [], createdAt: card.updatedAt,
};
const receipt: Omit<DeliveryReceipt, 'signature'> = {
  messageId: envelope.id, networkId: 'test', fromNodeId: identity.nodeId, stage: 'received', updatedAt: card.updatedAt,
};

test('canonical JSON sorts keys, excludes only the outer signature, and rejects non-JSON data', () => {
  assert.equal(canonicalJson({ z: 1, signature: 'ignored', a: { z: 2, a: 3, signature: 'signed' } }),
    '{"a":{"a":3,"signature":"signed","z":2},"z":1}');
  assert.equal(canonicalJson({ a: undefined, list: [undefined, 1] }), '{"list":[null,1]}');
  assert.throws(() => canonicalJson({ value: NaN }), /finite|JSON/);
  assert.throws(() => canonicalJson({ value: 1n }), /JSON/);
  const cyclic: Record<string, unknown> = {}; cyclic.self = cyclic;
  assert.throws(() => canonicalJson(cyclic), /cycl/i);
});

test('Ed25519 node IDs encode raw public keys and signatures interoperate with node:crypto', () => {
  assert.match(identity.nodeId, /^ed25519:[0-9a-f]{64}$/);
  const raw = createPublicKey(identity.publicKeyPem).export({ format: 'der', type: 'spki' }).subarray(-32);
  assert.equal(identity.nodeId, `ed25519:${raw.toString('hex')}`);
  const sig = signCanonicalPayload({ b: 2, a: 1 }, identity.privateKeyPem);
  assert.ok(verify(null, Buffer.from('{"a":1,"b":2}'), identity.publicKeyPem, Buffer.from(sig, 'base64url')));
  assert.ok(verifyCanonicalPayload({ a: 1, b: 2 }, sig, identity.nodeId));
  assert.equal(verifyCanonicalPayload({ a: 2 }, sig, identity.nodeId), false);
  assert.equal(verifyCanonicalPayload({ a: 1, b: 2 }, sig, generateNodeKeyPair().nodeId), false);
  assert.equal(verifyCanonicalPayload({}, 'bad!', 'ed25519:bad'), false);
});

test('canonical card, envelope and receipt signatures detect tampering and identity substitution', () => {
  const c = signNodeCard(card, identity.privateKeyPem);
  const e = signEnvelope(envelope, identity.privateKeyPem);
  const r = signReceipt(receipt, identity.privateKeyPem);
  assert.ok(verifyNodeCard(c)); assert.ok(verifyEnvelope(e)); assert.ok(verifyReceipt(r));
  assert.equal(verifyNodeCard({ ...c, revision: 2 }), false);
  assert.equal(verifyEnvelope({ ...e, payload: { signature: 'tampered' } }), false);
  assert.equal(verifyReceipt({ ...r, stage: 'settled' }), false);
  assert.equal(verifyNodeCard({ ...c, nodeId: generateNodeKeyPair().nodeId }), false);
  assert.equal('signature' in card, false);
});

test('network derivation matches HKDF-Expand and yields deterministic Ed25519 rendezvous keys', () => {
  const keys = deriveNetworkKeys('test', 'secret');
  assert.deepEqual(keys, deriveNetworkKeys('test', 'secret'));
  const master = createHash('sha256').update('the-dealer:network:v1:test:secret').digest();
  const expected = createHmac('sha256', master).update('the-dealer:swarm-topic:v1').update(Buffer.from([1])).digest('hex');
  assert.equal(keys.swarmTopicId, expected);
  assert.notEqual(keys.swarmTopicId, deriveNetworkKeys('other', 'secret').swarmTopicId);
  assert.notEqual(keys.swarmTopicId, deriveNetworkKeys('test', 'wrong').swarmTopicId);
  assert.ok(verifyCanonicalPayload({}, signCanonicalPayload({}, keys.rendezvousPrivateKeyPem), keys.rendezvousPublicKey));
});

test('sealed frames round trip signatures and reject wrong keys, tampering, and header substitution', () => {
  const keys = deriveNetworkKeys('test', 'secret');
  const payload = { type: 'peer.card', card: signNodeCard(card, identity.privateKeyPem) };
  const frame = sealNetworkFrame(payload, keys, identity.nodeId);
  assert.deepEqual(unsealNetworkFrame(frame, keys), payload);
  assert.notEqual(frame.nonce, sealNetworkFrame(payload, keys, identity.nodeId).nonce);
  assert.throws(() => unsealNetworkFrame(frame, deriveNetworkKeys('test', 'wrong')), /swarmTopicId/);
  assert.throws(() => unsealNetworkFrame(frame, { ...keys, sealedKey: new Uint8Array(32) }), /authenticat/i);
  for (const patch of [
    { ciphertext: Buffer.from('tampered').toString('base64') },
    { senderNodeId: generateNodeKeyPair().nodeId }, { authTag: Buffer.alloc(16).toString('base64') },
    { protocol: 'wrong' }, { nonce: '!' },
  ]) assert.throws(() => unsealNetworkFrame({ ...frame, ...patch } as typeof frame, keys));
});

const provider: Plugin<{ value: number }> = {
  name: 'provider', apply(ctx, config) { ctx.provide('counter', { value: config.value }); },
};

test('injection waits, activates topologically, and tears down dependents before providers restart', async () => {
  const kernel = new Kernel(); const log: string[] = [];
  await kernel.use({ name: 'consumer', inject: ['derived'], apply(ctx) {
    log.push(`consumer:${ctx.services.derived.value}`);
    ctx.on('observe', () => log.push('observed'));
    ctx.effect(() => () => { log.push('consumer:stop'); });
  } });
  await kernel.use({ name: 'middle', inject: ['counter'], apply(ctx) {
    log.push('middle:start'); ctx.provide('derived', { value: ctx.services.counter.value + 1 });
    return () => { log.push('middle:stop'); };
  } });
  assert.deepEqual(log, []);
  await kernel.use(provider, { value: 1 });
  assert.deepEqual(log, ['middle:start', 'consumer:2']);
  await kernel.applyPatch([{ name: 'provider', config: { value: 5 } }]);
  assert.deepEqual(log, ['middle:start', 'consumer:2', 'consumer:stop', 'middle:stop', 'middle:start', 'consumer:6']);
  log.length = 0; kernel.ctx.emit('observe', 0); assert.deepEqual(log, ['observed']);
  await kernel.stop(); log.length = 0; kernel.ctx.emit('observe', 0); assert.deepEqual(log, []);
  assert.throws(() => kernel.ctx.services.counter, /unavailable/);
});

test('all event modes preserve ordering, concurrent awaits, serial results, middleware and bail', async () => {
  const kernel = new Kernel(); const ctx = kernel.ctx; const log: number[] = [];
  ctx.on('observe', n => { log.push(n); });
  const first = ctx.on('observe', n => { log.push(n * 2); }, { prepend: true });
  ctx.emit('observe', 3); assert.deepEqual(log, [6, 3]);
  await first.dispose(); log.length = 0; ctx.emit('observe', 4); assert.deepEqual(log, [4]);
  let release!: () => void;
  const pending = new Promise<void>(resolve => { release = resolve; });
  ctx.on('work', async () => { await pending; log.push(1); return 'first'; });
  ctx.on('work', () => { release(); log.push(2); });
  log.length = 0; await ctx.parallel('work'); assert.deepEqual(log, [2, 1]);
  log.length = 0; assert.equal(await ctx.serial('work'), 'first'); assert.deepEqual(log, [1, 2]);
  ctx.on('flow', async (v, label, next) => { assert.equal(label, 'x'); return (await next(v + 1)) * 2; });
  const short = ctx.on('flow', v => v + 10);
  ctx.on('flow', (_v, _label, _next) => { throw new Error('must short circuit'); });
  assert.equal(await ctx.waterfall('flow', 2, 'x'), 26);
  await short.dispose(); await assert.rejects(ctx.waterfall('flow', 2, 'x'), /short circuit/);
  ctx.on('gate', () => null); ctx.on('gate', () => false);
  ctx.on('gate', () => { throw new Error('must bail'); });
  assert.equal(await ctx.bail('gate'), false);
  await kernel.stop(); assert.equal(await ctx.bail('gate'), undefined);
});

test('profile composition replaces config, disables/enables, and inserts without leaking effects', async () => {
  const log: string[] = [];
  const plugin: Plugin<{ value: number }> = { name: 'effect', apply(ctx, config) {
    log.push(`start:${config.value}`);
    ctx.effect(() => () => { log.push('cleanup:1'); });
    ctx.effect(() => async () => { await Promise.resolve(); log.push('cleanup:2'); });
    return () => { log.push('cleanup:apply'); };
  } };
  const base = [{ plugin, config: { value: 1 } }];
  const profile = composeProfile(base, [[{ name: 'effect', config: { value: 2 } }]]);
  assert.deepEqual(base[0].config, { value: 1 });
  const kernel = new Kernel(profile); await kernel.start();
  await kernel.applyPatch([{ name: 'effect', enabled: false }]);
  assert.deepEqual(log, ['start:2', 'cleanup:apply', 'cleanup:2', 'cleanup:1']);
  await kernel.applyPatch([{ name: 'effect', enabled: true, config: { value: 3 } }, { plugin: provider, config: { value: 7 } }]);
  assert.equal(kernel.ctx.services.counter.value, 7);
  await kernel.stop(); await kernel.stop();
  assert.deepEqual(log.slice(4), ['start:3', 'cleanup:apply', 'cleanup:2', 'cleanup:1']);
});

test('failed activation rolls back services and listeners, and cleanup failures do not leak other effects', async () => {
  const kernel = new Kernel(); let observed = 0; const log: number[] = [];
  await assert.rejects(kernel.use({ name: 'broken', apply(ctx) {
    ctx.provide('counter', { value: 1 }); ctx.on('observe', () => { observed++; });
    ctx.effect(() => () => { log.push(1); });
    throw new Error('activation failed');
  } }), /activation failed/);
  kernel.ctx.emit('observe', 1); assert.equal(observed, 0); assert.deepEqual(log, [1]);
  assert.throws(() => kernel.ctx.services.counter, /unavailable/);
  await kernel.use({ name: 'cleanup', apply(ctx) {
    ctx.effect(() => () => { log.push(2); });
    ctx.effect(() => () => { throw new Error('cleanup failed'); });
    ctx.provide('counter', { value: 2 });
  } });
  await assert.rejects(kernel.stop(), /cleanup/);
  assert.deepEqual(log, [1, 2]); assert.throws(() => kernel.ctx.services.counter, /unavailable/);
});


test('frame plaintext is the inner payload itself, including its signature', () => {
  const keys = deriveNetworkKeys('test', 'secret');
  const signed = signNodeCard(card, identity.privateKeyPem);
  const frame = sealNetworkFrame(signed, keys, identity.nodeId);
  const decipher = createDecipheriv('aes-256-gcm', keys.sealedKey, Buffer.from(frame.nonce, 'base64'));
  decipher.setAAD(Buffer.from(canonicalJson({ protocol: frame.protocol, swarmTopicId: frame.swarmTopicId, senderNodeId: frame.senderNodeId })));
  decipher.setAuthTag(Buffer.from(frame.authTag, 'base64'));
  assert.deepEqual(JSON.parse(Buffer.concat([decipher.update(Buffer.from(frame.ciphertext, 'base64')), decipher.final()]).toString()), signed);
});

test('services provided after activation wake waiting plugins; disposal is idempotent across re-provision', async () => {
  const kernel = new Kernel(); const values: number[] = [];
  await kernel.use({ name: 'late', inject: ['counter'], apply(ctx) { values.push(ctx.services.counter.value); } });
  const old = kernel.ctx.provide('counter', { value: 1 });
  await kernel.whenIdle(); assert.deepEqual(values, [1]);
  await old.dispose();
  const replacement = kernel.ctx.provide('counter', { value: 2 });
  await kernel.whenIdle();
  await old.dispose();
  assert.equal(kernel.ctx.services.counter.value, 2);
  assert.deepEqual(values, [1, 2]);
  await replacement.dispose(); await kernel.stop();
});

test('async cleanup finishes before hot replacement and stale contexts cannot register effects', async () => {
  const kernel = new Kernel(); const log: string[] = [];
  let oldContext: typeof kernel.ctx;
  let release!: () => void;
  const pending = new Promise<void>(resolve => { release = resolve; });
  await kernel.use({ name: 'slow', apply(ctx) {
    oldContext = ctx;
    ctx.provide('counter', { value: 1 });
    return async () => { log.push('stopping'); await pending; log.push('stopped'); };
  } });
  const patch = kernel.applyPatch([{ name: 'slow', plugin: { name: 'slow', apply(ctx) {
    log.push('replacement'); ctx.provide('counter', { value: 2 });
  } } }]);
  await Promise.resolve(); await Promise.resolve();
  assert.deepEqual(log, ['stopping']);
  release(); await patch;
  assert.deepEqual(log, ['stopping', 'stopped', 'replacement']);
  assert.throws(() => oldContext!.provide('derived', { value: 1 }), /disposed/);
  assert.throws(() => oldContext!.on('observe', () => {}), /disposed/);
  await kernel.stop();
});

test('duplicate services and invalid patches leave the existing provider intact', async () => {
  const kernel = new Kernel(); await kernel.use(provider, { value: 1 });
  await assert.rejects(kernel.use({ name: 'duplicate', apply(ctx) { ctx.provide('counter', { value: 2 }); } }), /already provided/);
  await assert.rejects(kernel.applyPatch([{ name: 'provider', enabled: false }, { name: 'missing', enabled: true }]), /Unknown plugin/);
  assert.equal(kernel.ctx.services.counter.value, 1);
  await kernel.stop();
});

test('waterfall delegates transformed values through the chain and rejects multiple next calls', async () => {
  const kernel = new Kernel();
  kernel.ctx.on('flow', async (value, _label, next) => (await next(value + 1)) * 2);
  kernel.ctx.on('flow', (value, _label, next) => next(value + 3));
  assert.equal(await kernel.ctx.waterfall('flow', 2, 'x'), 12);
  const duplicate = kernel.ctx.on('flow', async (value, _label, next) => { await next(value); return next(value); });
  await assert.rejects(kernel.ctx.waterfall('flow', 2, 'x'), /only be called once/);
  await duplicate.dispose(); await kernel.stop();
});

test('parallel invokes every observer despite synchronous failure and serial stops on failure', async () => {
  const kernel = new Kernel(); const seen: string[] = [];
  kernel.ctx.on('work', () => { seen.push('first'); throw new Error('listener failed'); });
  kernel.ctx.on('work', () => { seen.push('second'); });
  await assert.rejects(kernel.ctx.parallel('work'), /listener failed/);
  assert.deepEqual(seen, ['first', 'second']); seen.length = 0;
  await assert.rejects(kernel.ctx.serial('work'), /listener failed/);
  assert.deepEqual(seen, ['first']); await kernel.stop();
});

test('explicit use reactivates a disabled plugin and exposes enabled name reservations', async () => {
  const kernel = new Kernel();
  let active = 0;
  const plugin: Plugin = {
    name: 'reusable',
    apply() {
      active++;
      return () => {
        active--;
      };
    },
  };
  try {
    await kernel.use(plugin);
    assert.deepEqual(kernel.enabledPluginNames(), ['reusable']);
    await kernel.applyPatch([{ name: 'reusable', enabled: false }]);
    assert.equal(active, 0);
    assert.deepEqual(kernel.enabledPluginNames(), []);
    await kernel.use(plugin);
    assert.equal(active, 1);
    assert.deepEqual(kernel.enabledPluginNames(), ['reusable']);
  } finally {
    await kernel.stop();
  }
  assert.equal(active, 0);
});
