import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm, stat, readdir } from 'node:fs/promises';
import os, { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash, randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createConnection, createServer, Socket } from 'node:net';
import { Kernel } from '../packages/kernel/index.ts';
import { generateNodeKeyPair, deriveNetworkKeys, sealNetworkFrame, signNodeCard, DeliveryTimeoutError, LocalDeliveryError } from '../packages/core-types/index.ts';
import type { InboundFrameEvent, NodeCard } from '../packages/core-types/index.ts';
import { createTransportPlugin, P2PTransportService, blake3 } from '../packages/transport/index.ts';
import { createNodePlugin, PeerDirectoryService } from '../packages/node/index.ts';

async function until(check: () => boolean) {
  for (let i = 0; i < 150; i++) { if (check()) return; await new Promise(r => setTimeout(r, 20)); }
  assert.ok(check(), 'condition did not converge');
}
async function mesh(t: test.TestContext, count = 3) {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-test-'));
  const kernels: Kernel[] = [];
  t.after(async () => { await Promise.all(kernels.map(k => k.stop())); await rm(dir, { recursive: true, force: true }); });
  for (let i = 0; i < count; i++) {
    const keyPair = generateNodeKeyPair();
    const k = new Kernel([
      { plugin: createTransportPlugin({ localNodeId: keyPair.nodeId, discoveryDir: dir }) },
      { plugin: createNodePlugin({ keyPair, member: i === 1 ? 'oliver' : 'ben.xu', nodeName: i === 0 ? 'mbp' : `node-${i}`, capabilities: { summary: 'worker', skills: [{ id: 'prod-release', description: 'release', tags: ['ops'] }] }, exchangeIntervalMs: 60 }) },
    ]);
    kernels.push(k); await k.start();
  }
  return { dir, kernels, transports: kernels.map(k => k.ctx.services.transport as P2PTransportService), nodes: kernels.map(k => k.ctx.services.node as PeerDirectoryService) };
}
const encode = (x: unknown) => Buffer.from(JSON.stringify(x));

test('real mesh discovers signed cards and resolves scoped targets', async t => {
  const { transports: ts, nodes: ns } = await mesh(t);
  await Promise.all(ts.map(t => t.mountNetwork({ networkId: 'sensebeing', passphrase: 'secret' })));
  await until(() => ns.every(n => n.listPeers().length === 2));
  assert.equal(ns[0].resolveTarget('member:oliver').length, 1);
  assert.equal(ns[1].resolveTarget('ben.xu@mbp').length, 1);
  assert.equal(ns[0].resolveTarget('skill:prod-release').length, 2);
  assert.equal(ns[0].resolveTarget('sensebeing/member:oliver').length, 1);
  assert.equal(ns[0].resolveTarget(ts[1].localNodeId).length, 1);
  const rev = ns[0].getSelfCard('sensebeing').revision;
  await ns[0].updateRuntimeState({ load: 'busy', activeTasks: 2, availableAgents: ['codex'] });
  await until(() => ns[1].resolveTarget('ben.xu@mbp')[0]?.runtimeState.activeTasks === 2);
  assert.ok(ns[0].getSelfCard('sensebeing').revision > rev);
  const current = ns[0].getSelfCard('sensebeing');
  await ns[0].updateNetworkAdmissionMode('sensebeing', 'closed');
  await until(() => ns[1].resolveTarget('ben.xu@mbp')[0]?.admissionMode === 'closed');
  await ts[0].broadcast('sensebeing', 'peer.card', encode({type:'peer.card',card:current}));
  assert.equal(ns[1].resolveTarget('ben.xu@mbp')[0].admissionMode, 'closed');
});

function card(member: string, nodeName: string, updates: Partial<NodeCard> = {}, keys = generateNodeKeyPair()): NodeCard {
  return signNodeCard({
    nodeId: keys.nodeId,
    networkId: 'team',
    identity: { member, nodeName },
    revision: 1,
    updatedAt: '2026-01-01T00:00:00.000Z',
    capabilities: { summary: '', skills: [{ id: 'release', description: '' }] },
    runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] },
    admissionMode: 'open',
    ...updates,
  }, keys.privateKeyPem);
}

function deliverPeerCard(kernel: Kernel, card: NodeCard): void {
  kernel.ctx.emit('transport/inbound', {
    networkId: card.networkId, senderNodeId: card.nodeId, mode: 'unicast',
    payload: { type: 'peer.card', card },
  });
}

test('peer routing prefers self and reachable teammates while retaining verified cards by node ID', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-peer-routing-'));
  const peersPath = join(dir, 'peers.json');
  const local = generateNodeKeyPair();
  const oldGhost = card('ben', 'ben', { updatedAt: '2025-01-01T00:00:00.000Z' });
  const newGhost = card('ben', 'ben');
  const duplicateSelf = card('ben', 'local');
  const older = card('oliver', 'desktop', { revision: 10 });
  const newer = card('oliver', 'desktop', { updatedAt: '2026-01-02T00:00:00.000Z' });
  const oldest = card('oliver', 'desktop', { updatedAt: '2025-01-01T00:00:00.000Z' });
  const otherMachine = card('oliver', 'laptop');
  const otherNetwork = card('oliver', 'desktop', { networkId: 'other-team' });
  await writeFile(peersPath, JSON.stringify({
    schema: 'the-dealer-peer-directory/v1',
    cards: [oldGhost, newGhost, duplicateSelf, older, newer, oldest, otherMachine, otherNetwork],
  }));
  const kernel = new Kernel([
    { plugin: createTransportPlugin({ localNodeId: local.nodeId, discoveryDir: dir }) },
    { plugin: createNodePlugin({
      keyPair: local, member: 'ben', nodeName: 'local', peersPath,
      capabilities: { summary: '', skills: [{ id: 'release', description: '' }] },
    }) },
  ]);
  t.after(async () => { await kernel.stop(); await rm(dir, { recursive: true, force: true }); });
  await kernel.start();
  const persistedIds = async () => new Set<string>(
    JSON.parse(await readFile(peersPath, 'utf8')).cards.map((entry: NodeCard) => entry.nodeId),
  );
  const verified = [oldGhost, newGhost, duplicateSelf, older, newer, oldest, otherMachine, otherNetwork];
  assert.deepEqual(await persistedIds(), new Set(verified.map(entry => entry.nodeId)));
  const transport: P2PTransportService = kernel.ctx.services.transport;
  const node: PeerDirectoryService = kernel.ctx.services.node;
  await transport.mountNetwork({ networkId: 'team', passphrase: 'test-secret' });
  const self = node.getSelfCard('team');
  for (const selector of ['member:ben', 'team/member:ben', 'ben@local', local.nodeId]) {
    assert.deepEqual(node.resolveTarget(selector, 'team'), [self], selector);
  }
  assert.deepEqual(node.resolveTarget('ben@ben', 'team'), [newGhost]);
  assert.ok(node.resolveTarget('skill:release', 'team').every(entry => entry.nodeId !== local.nodeId));
  assert.deepEqual(node.resolveTarget('member:oliver', 'team'), [newer, otherMachine]);
  const reachable = new Set([otherMachine.nodeId]);
  t.mock.method(transport, 'isPeerReachable', (networkId: string, nodeId: string) => networkId === 'team' && reachable.has(nodeId));
  assert.deepEqual(node.resolveTarget('member:oliver', 'team'), [otherMachine]);
  assert.deepEqual(node.resolveTarget('oliver@desktop', 'team'), [newer]);

  const replacement = card('oliver', 'desktop', { updatedAt: newer.updatedAt, revision: newer.revision + 1 });
  reachable.add(newGhost.nodeId);
  const claimant = card('ben', 'ben', { updatedAt: '2026-01-03T00:00:00.000Z' });
  for (const incoming of [replacement, newer, duplicateSelf, claimant]) {
    deliverPeerCard(kernel, incoming);
  }
  assert.deepEqual(node.resolveTarget('oliver@desktop', 'team'), [replacement]);
  assert.deepEqual(node.resolveTarget('ben@ben', 'team'), [newGhost]);
  const stored = new Set([...verified, claimant, replacement].map(entry => entry.nodeId));
  assert.deepEqual(await persistedIds(), stored);

  reachable.add(claimant.nodeId);
  deliverPeerCard(kernel, claimant);
  assert.deepEqual(node.resolveTarget('ben@ben', 'team'), [claimant]);
  assert.deepEqual(await persistedIds(), stored);
});

test('peer failover restores a reachable fallback after restart and rejects stale or equivocated revisions', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-peer-revisions-'));
  const peersPath = join(dir, 'peers.json');
  const local = generateNodeKeyPair();
  const retired = generateNodeKeyPair();
  const alternative = generateNodeKeyPair();
  const oldProfile = card('oliver', 'laptop', {}, retired);
  const endpointAddr = '127.0.0.1:45678';
  const lastProfile = card('oliver', 'desktop', { revision: 10, endpointAddr }, retired);
  const replacement = card('oliver', 'desktop', { updatedAt: '2026-01-02T00:00:00.000Z' });
  const olderAlternative = card('oliver', 'desktop', { revision: 12 }, alternative);
  const equivocation = card('oliver', 'tablet', { revision: olderAlternative.revision, endpointAddr }, alternative);
  await writeFile(peersPath, JSON.stringify({ schema: 'the-dealer-peer-directory/v1', cards: [oldProfile, lastProfile] }));
  const kernels: Kernel[] = [];
  const reachable = new Set([lastProfile.nodeId]);
  t.after(async () => { await Promise.all(kernels.map(kernel => kernel.stop())); await rm(dir, { recursive: true, force: true }); });
  async function openDirectory() {
    const kernel = new Kernel([
      { plugin: createTransportPlugin({ localNodeId: local.nodeId, discoveryDir: dir }) },
      { plugin: createNodePlugin({ keyPair: local, member: 'ben', nodeName: 'local', peersPath }) },
    ]);
    kernels.push(kernel);
    await kernel.start();
    const transport: P2PTransportService = kernel.ctx.services.transport;
    await transport.mountNetwork({ networkId: 'team', passphrase: 'test-secret' });
    t.mock.method(transport, 'isPeerReachable', (networkId: string, nodeId: string) => networkId === 'team' && reachable.has(nodeId));
    const registerEndpoint = t.mock.method(transport, 'registerPeerEndpoint', () => {});
    return { kernel, node: kernel.ctx.services.node as PeerDirectoryService, registerEndpoint };
  }

  const initial = await openDirectory();
  for (const card of [replacement, olderAlternative]) deliverPeerCard(initial.kernel, card);
  assert.deepEqual(initial.node.resolveTarget('oliver@desktop', 'team'), [lastProfile]);
  const snapshot = JSON.parse(await readFile(peersPath, 'utf8'));
  assert.deepEqual(new Set(snapshot.cards), new Set([lastProfile, replacement, olderAlternative]));
  await initial.kernel.stop();

  const { kernel, node, registerEndpoint } = await openDirectory();
  const assertActivePeer = (active: NodeCard) => {
    assert.deepEqual(node.resolveTarget('oliver@desktop', 'team'), [active]);
    assert.deepEqual(node.resolveTarget('member:oliver', 'team'), [active]);
    assert.deepEqual(node.listPeers({ networkId: 'team' }), [active]);
    assert.deepEqual(node.resolveTarget(lastProfile.nodeId, 'team'), [lastProfile]);
    assert.deepEqual(node.resolveTarget(replacement.nodeId, 'team'), [replacement]);
    assert.deepEqual(node.resolveTarget(replacement.nodeId, 'another-team'), []);
  };
  assertActivePeer(lastProfile);

  const updates: unknown[] = [];
  kernel.ctx.on('node/peer-updated', (card, provenance) => { updates.push({ card, provenance }); });
  const beforeHeartbeat = await readFile(peersPath, 'utf8');
  const beforeStat = await stat(peersPath);
  deliverPeerCard(kernel, lastProfile);
  assert.deepEqual(registerEndpoint.mock.calls.map(call => call.arguments), [['team', lastProfile.nodeId, endpointAddr]]);
  assert.deepEqual(updates, []);

  reachable.delete(lastProfile.nodeId);
  assertActivePeer(replacement);
  for (const replay of [oldProfile, equivocation]) deliverPeerCard(kernel, replay);
  assert.deepEqual(node.resolveTarget('oliver@laptop', 'team'), []);
  assert.deepEqual(node.resolveTarget('oliver@tablet', 'team'), []);
  assert.equal(registerEndpoint.mock.callCount(), 1);
  assert.equal(updates.length, 0);
  assert.equal(await readFile(peersPath, 'utf8'), beforeHeartbeat);
  assert.equal((await stat(peersPath)).ino, beforeStat.ino);
});

test('a peer claiming the local identity retains its revision across restart', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-peer-local-identity-'));
  const peersPath = join(dir, 'peers.json');
  const local = generateNodeKeyPair();
  const remote = generateNodeKeyPair();
  const earlier = card('oliver', 'desktop', { revision: 10 }, remote);
  const localClaim = card('ben', 'local', { revision: 11, endpointAddr: '127.0.0.1:45678' }, remote);
  const kernels: Kernel[] = [];
  t.after(async () => {
    await Promise.all(kernels.map(kernel => kernel.stop()));
    await rm(dir, { recursive: true, force: true });
  });
  for (const restarting of [false, true]) {
    const kernel = new Kernel([
      { plugin: createTransportPlugin({ localNodeId: local.nodeId, discoveryDir: dir }) },
      { plugin: createNodePlugin({ keyPair: local, member: 'ben', nodeName: 'local', peersPath }) },
    ]);
    kernels.push(kernel);
    await kernel.start();
    const transport: P2PTransportService = kernel.ctx.services.transport;
    const node: PeerDirectoryService = kernel.ctx.services.node;
    await transport.mountNetwork({ networkId: 'team', passphrase: 'test-secret' });
    if (!restarting) {
      deliverPeerCard(kernel, earlier);
      assert.deepEqual(node.resolveTarget('oliver@desktop', 'team'), [earlier]);
    }
    const register = t.mock.method(transport, 'registerPeerEndpoint', () => {});
    const updated = t.mock.fn();
    kernel.ctx.on('node/peer-updated', updated);
    if (!restarting) {
      deliverPeerCard(kernel, localClaim);
    }
    assert.deepEqual(node.resolveTarget(remote.nodeId, 'team'), []);
    deliverPeerCard(kernel, earlier);
    assert.deepEqual(node.resolveTarget('oliver@desktop', 'team'), []);
    assert.deepEqual(node.resolveTarget('skill:release', 'team'), []);
    assert.deepEqual(node.resolveTarget(remote.nodeId, 'team'), []);
    assert.deepEqual(node.listPeers({ networkId: 'team' }), []);
    assert.equal(register.mock.callCount(), 0);
    assert.equal(updated.mock.callCount(), 0);
    assert.deepEqual(JSON.parse(await readFile(peersPath, 'utf8')).cards, [localClaim]);
    await kernel.stop();
  }
});

test('selector and self card reads leave the peer store unchanged across a heartbeat', { timeout: 10_000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-peer-reads-'));
  const peersPath = join(dir, 'peers.json');
  const local = generateNodeKeyPair();
  const remote = card('oliver', 'desktop');
  const kernel = new Kernel([
    { plugin: createTransportPlugin({ localNodeId: local.nodeId, discoveryDir: dir }) },
    { plugin: createNodePlugin({ keyPair: local, member: 'ben', nodeName: 'local', peersPath, exchangeIntervalMs: 30 }) },
  ]);
  t.after(async () => { await kernel.stop(); await rm(dir, { recursive: true, force: true }); });
  await kernel.start();
  const transport: P2PTransportService = kernel.ctx.services.transport;
  const node: PeerDirectoryService = kernel.ctx.services.node;
  await transport.mountNetwork({ networkId: 'team', passphrase: 'test-secret' });
  assert.deepEqual(transport.listMountedNetworks(), ['team']);
  deliverPeerCard(kernel, remote);
  const before = await readFile(peersPath, 'utf8');
  const beforeStat = await stat(peersPath);
  assert.deepEqual(node.resolveTarget(remote.nodeId), [remote]);

  await new Promise<void>(resolve => {
    const broadcast = transport.broadcast.bind(transport);
    t.mock.method(transport, 'broadcast', async (...args: Parameters<typeof broadcast>) => {
      const result = await broadcast(...args);
      resolve();
      return result;
    });
  });

  assert.deepEqual(node.resolveTarget(remote.nodeId), [remote]);
  assert.equal(node.resolveTarget('member:ben')[0]?.networkId, 'local');
  const projected = node.getSelfCard('unmounted');
  assert.equal(projected.networkId, 'unmounted');
  assert.deepEqual(node.resolveTarget('unmounted/member:ben'), [projected]);
  for (const selector of ['member:ben', 'ben@local', local.nodeId]) {
    assert.deepEqual(node.resolveTarget(selector, 'unmounted'), [projected], selector);
  }
  const after = await readFile(peersPath, 'utf8');
  const localRevision = JSON.parse(before).selfRevisions.local;
  assert.equal(typeof localRevision, 'number');
  assert.equal(JSON.parse(after).selfRevisions.local, localRevision);
  assert.equal(after, before);
  assert.equal((await stat(peersPath)).ino, beforeStat.ino);
});

test('wrong passphrase, forged cards, replay and other networks are isolated', async t => {
  const { dir, transports: ts, nodes: ns } = await mesh(t);
  await ts[0].mountNetwork({ networkId: 'sensebeing', passphrase: 'secret' });
  await ts[0].mountNetwork({ networkId: 'ben-home', passphrase: 'private', exposedSkills: [] });
  assert.equal(ns[0].getSelfCard('ben-home').capabilities.skills.length, 0);
  await ts[1].mountNetwork({ networkId: 'sensebeing', passphrase: 'secret' });
  await ts[2].mountNetwork({ networkId: 'sensebeing', passphrase: 'wrong' });
  await until(() => ns[0].listPeers().length === 1 && ns[1].listPeers().length === 1);
  assert.equal(ns[2].listPeers().length, 0);
  assert.equal(ns[1].listPeers({ networkId: 'ben-home' }).length, 0);
  const prior = ns[1].getSelfCard('sensebeing');
  await ts[1].broadcast('sensebeing', 'cards', encode({ type: 'peer.card', card: { ...prior, revision: 999, identity: { member: 'evil', nodeName: 'evil' } } }));
  assert.equal(ns[0].listPeers()[0].identity.member, 'oliver');
  ts[2].registerPeerEndpoint('sensebeing', ts[0].localNodeId, ts[0].endpointAddr!);
  await assert.rejects(ts[2].sendUnicast('sensebeing', ts[0].localNodeId, encode({ type: 'peer.card', card: prior })));
  const forged = sealNetworkFrame({ mode: 'unicast', dataBase64: encode({ bad: true }).toString('base64') }, deriveNetworkKeys('sensebeing', 'secret'), ts[1].localNodeId);
  forged.authTag = Buffer.alloc(16).toString('base64');
  await assert.rejects(ts[1].sendUnicast('sensebeing', ts[0].localNodeId, encode(forged)));
  assert.ok((await readdir(dir)).every(x => !x.includes('sensebeing') && !x.includes('ben-home')));
  await ts[0].unmountNetwork('sensebeing');
  assert.equal(ns[0].listPeers({ networkId: 'sensebeing' }).length, 0);
});

test('unicast responses, concurrent multicast and broadcast use framed sockets', async t => {
  const { transports: ts, kernels } = await mesh(t);
  await Promise.all(ts.map(t => t.mountNetwork({ networkId: 'sensebeing', passphrase: 'secret' })));
  await until(() => ts.every(t => t.listPeerEndpoints('sensebeing').size === 2));
  const seen = new Set<string>();
  for (const [i, k] of kernels.entries()) k.ctx.on('transport/inbound', e => {
    if ((e.payload as { ping?: boolean }).ping) { seen.add(`${i}:${e.mode}`); return Buffer.from(`pong-${i}`); }
  });
  assert.equal(Buffer.from(await ts[0].sendUnicast('sensebeing', ts[1].localNodeId, encode({ ping: true }))).toString(), 'pong-1');
  const sealed = sealNetworkFrame({ping:true}, deriveNetworkKeys('sensebeing','secret'), ts[0].localNodeId);
  assert.equal(Buffer.from(await ts[0].sendUnicast('sensebeing', ts[1].localNodeId, encode(sealed))).toString(), 'pong-1');
  const replies = await ts[0].sendMulticast('sensebeing', [ts[1].localNodeId, ts[2].localNodeId, 'missing'], encode({ ping: true }));
  assert.equal(replies.get('missing')?.ok, false);
  assert.equal(replies.get(ts[2].localNodeId)?.ok, true);
  await ts[0].broadcast('sensebeing', 'ping', encode({ ping: true }));
  assert.ok(seen.has('1:broadcast') && seen.has('2:broadcast'));
});

test('BLAKE3 vectors and large binary streaming verify before atomic replacement', async t => {
  assert.equal(blake3(Buffer.alloc(0)), 'af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262');
  assert.equal(blake3(Buffer.from('abc')), '6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85');
  const { dir, transports: ts } = await mesh(t, 2);
  const input = join(dir, 'video.bin'); const dest = join(dir, 'received', 'video.bin');
  const bytes = randomBytes(3 * 1024 * 1024 + 19); await writeFile(input, bytes);
  const blob = await ts[0].shareFile(input);
  assert.equal(blob.blake3Hash, blake3(bytes));
  await ts[1].fetchBlobToPath(blob.ticket, dest);
  assert.deepEqual(await readFile(dest), bytes);
  assert.equal((await stat(dest)).mode & 0o777, 0o600);
  const selfDest = join(dir, 'received', 'self-video.bin');
  await ts[0].fetchBlobToPath(blob.ticket, selfDest);
  assert.deepEqual(await readFile(selfDest), bytes);
  await writeFile(input, Buffer.alloc(bytes.length, 1));
  await assert.rejects(ts[1].fetchBlobToPath(blob.ticket, dest), /hash|digest/i);
  assert.deepEqual(await readFile(dest), bytes);
  await assert.rejects(ts[1].fetchBlobToPath(blob.ticket.replace(`size=${bytes.length}`, 'size=1'), dest));
});


test('discovery crosses OS processes and cleanup removes the registry beacon', async t => {
  const { dir, transports: ts, nodes: ns } = await mesh(t, 1);
  await ts[0].mountNetwork({ networkId: 'sensebeing', passphrase: 'secret' });
  const childCode = `
    import { Kernel } from ${JSON.stringify(new URL('../packages/kernel/index.ts', import.meta.url).href)};
    import { createTransportPlugin } from ${JSON.stringify(new URL('../packages/transport/index.ts', import.meta.url).href)};
    import { createNodePlugin } from ${JSON.stringify(new URL('../packages/node/index.ts', import.meta.url).href)};
    const k = new Kernel([{plugin:createTransportPlugin({discoveryDir:process.env.DISCOVERY_DIR})},
      {plugin:createNodePlugin({member:'child',nodeName:'os-process',exchangeIntervalMs:50})}]);
    await k.start();
    k.ctx.on('transport/inbound', e => {if(e.payload?.ping) return Buffer.from('child-pong');});
    await k.ctx.services.transport.mountNetwork({networkId:'sensebeing',passphrase:'secret'});
    process.send({nodeId:k.ctx.services.transport.localNodeId});
    process.on('message', async () => {await k.stop(); process.disconnect();});
  `;
  const child = spawn(process.execPath, ['--experimental-strip-types', '--input-type=module', '-e', childCode], {
    env: { ...process.env, DISCOVERY_DIR: dir }, stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  let stderr = ''; child.stderr!.on('data', b => { stderr += b; });
  t.after(() => { child.kill(); });
  const ready = await Promise.race([once(child, 'message'), once(child, 'exit').then(() => { throw new Error(stderr); })]);
  const nodeId = ready[0].nodeId;
  await until(() => ns[0].resolveTarget(nodeId).length === 1);
  assert.equal(Buffer.from(await ts[0].sendUnicast('sensebeing', nodeId, encode({ping:true}))).toString(), 'child-pong');
  const exited = once(child, 'exit'); child.send('stop'); await exited;
  const topic = deriveNetworkKeys('sensebeing', 'secret').swarmTopicId;
  assert.equal((await readdir(join(dir, topic))).filter(x => x.endsWith('.json')).length, 1);
});

test('BLAKE3 tree boundary vectors match the upstream reference implementation', () => {
  const vectors: [number,string][] = [
    [1024,'42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7'],
    [1025,'d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444'],
    [2048,'e776b6028c7cd22a4d0ba182a8bf62205d2ef576467e838ed6f2529b85fba24a'],
    [31744,'62b6960e1a44bcc1eb1a611a8d6235b6b4b78f32e7abc4fb4c6cdcce94895c47'],
  ];
  for (const [size, expected] of vectors) assert.equal(blake3(Buffer.from(Array.from({length:size}, (_, i) => i % 251))), expected);
});

test('receiver rejects AEAD tampering on the actual TCP wire', async t => {
  const {transports:ts, kernels} = await mesh(t, 1);
  await ts[0].mountNetwork({networkId:'sensebeing',passphrase:'secret'});
  let delivered = 0; kernels[0].ctx.on('transport/inbound', () => {delivered++;});
  const frame = sealNetworkFrame({mode:'unicast',dataBase64:encode({ping:true}).toString('base64')}, deriveNetworkKeys('sensebeing','secret'), generateNodeKeyPair().nodeId);
  frame.authTag = Buffer.alloc(16).toString('base64');
  const [host, port] = ts[0].endpointAddr!.split(':'); const socket = createConnection({host,port:Number(port)});
  socket.on('error', () => {}); t.after(() => socket.destroy());
  const body = encode({type:'frame',frame}); const header = Buffer.alloc(4); header.writeUInt32BE(body.length);
  const closed = once(socket, 'close'); socket.write(Buffer.concat([header,body])); await closed;
  assert.equal(delivered, 0);
});


test('UDP multicast discovers peers without a shared discovery directory', async t => {
  const root = await mkdtemp(join(tmpdir(), 'dealer-udp-'));
  const a = new P2PTransportService({discoveryDir:join(root,'a')});
  const b = new P2PTransportService({discoveryDir:join(root,'b')});
  t.after(async () => {await Promise.all([a.dispose(),b.dispose()]); await rm(root,{recursive:true,force:true});});
  await a.mountNetwork({networkId:'udp-test',passphrase:'secret'});
  await b.mountNetwork({networkId:'udp-test',passphrase:'secret'});
  await until(() => a.listPeerEndpoints('udp-test').has(b.localNodeId) && b.listPeerEndpoints('udp-test').has(a.localNodeId));
  b.onInboundFrame(() => Buffer.from('via-udp'));
  assert.equal(Buffer.from(await a.sendUnicast('udp-test',b.localNodeId,Buffer.from('bytes'))).toString(),'via-udp');
});

test('mounted same-node unicast stays in-process while sealed wire frames remain authenticated', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-self-'));
  const keyPair = generateNodeKeyPair();
  const kernel = new Kernel([
    { plugin: createTransportPlugin({ localNodeId: keyPair.nodeId, discoveryDir: dir, socketTimeoutMs: 30 }) },
    { plugin: createNodePlugin({ keyPair, member: 'ben.xu', nodeName: 'mbp', exchangeIntervalMs: 60 }) },
  ]);
  t.after(async () => { await kernel.stop(); await rm(dir, { recursive: true, force: true }); });
  await kernel.start();
  const transport = kernel.ctx.services.transport as P2PTransportService;
  await transport.mountNetwork({ networkId: 'self', passphrase: 'secret' });
  const received: InboundFrameEvent[] = [];
  kernel.ctx.on('transport/inbound', async event => {
    if ((event.payload as { type?: string }).type !== 'slow.ping') {
      return;
    }
    received.push(event);
    // Longer than socketTimeoutMs: only an in-process delivery can complete.
    await new Promise(resolve => setTimeout(resolve, 60));
    return Buffer.from('in-process-pong');
  });
  const reply = await transport.sendUnicast('self', transport.localNodeId, encode({ type: 'slow.ping' }));
  assert.equal(Buffer.from(reply).toString(), 'in-process-pong');
  assert.equal(received.length, 1);
  assert.equal(received[0].senderNodeId, transport.localNodeId);
  assert.equal(received[0].mode, 'unicast');

  const keys = deriveNetworkKeys('self', 'secret');
  const sealed = sealNetworkFrame({ mode: 'unicast', dataBase64: encode({ type: 'slow.ping' }).toString('base64') }, keys, transport.localNodeId);
  sealed.authTag = Buffer.alloc(16).toString('base64');
  await assert.rejects(transport.sendUnicast('self', transport.localNodeId, encode(sealed)), /authenticate/);
  const foreign = sealNetworkFrame({ mode: 'unicast', dataBase64: encode({ bad: true }).toString('base64') }, deriveNetworkKeys('other', 'secret'), transport.localNodeId);
  await assert.rejects(transport.sendUnicast('self', transport.localNodeId, encode(foreign)), /Unknown swarm/);
  assert.equal(received.length, 1);

  const offline = new P2PTransportService({ localNodeId: generateNodeKeyPair().nodeId, discoveryDir: join(dir, 'offline') });
  t.after(() => offline.dispose());
  offline.onInboundFrame(() => Buffer.from('offline-pong'));
  assert.equal(Buffer.from(await offline.sendUnicast('self', offline.localNodeId, encode({ type: 'ping' }))).toString(), 'offline-pong');
});

test('cross-node exchange defaults to socketTimeoutMs instead of a hardcoded short timeout', async t => {
  const root = await mkdtemp(join(tmpdir(), 'dealer-timeout-'));
  const options = { discoveryDir: root, socketTimeoutMs: 30 };
  const a = new P2PTransportService(options);
  const b = new P2PTransportService(options);
  t.after(async () => { await Promise.all([a.dispose(), b.dispose()]); await rm(root, { recursive: true, force: true }); });
  await a.mountNetwork({ networkId: 'timeout-test', passphrase: 'secret' });
  await b.mountNetwork({ networkId: 'timeout-test', passphrase: 'secret' });
  a.registerPeerEndpoint('timeout-test', b.localNodeId, b.endpointAddr!);
  b.onInboundFrame(async () => {
    await new Promise(resolve => setTimeout(resolve, 60));
    return Buffer.from('late-pong');
  });
  const startedAt = performance.now();
  await assert.rejects(a.sendUnicast('timeout-test', b.localNodeId, encode({ type: 'slow.ping' })), /Transport timeout/);
  assert.ok(performance.now() - startedAt < 3000);
});

/** A TCP endpoint that accepts connections and never answers: an unreachable, blackholed peer. */
async function blackholeEndpoint(t: test.TestContext): Promise<string> {
  const sockets = new Set<Socket>();
  const server = createServer(socket => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  t.after(() => new Promise<void>(resolve => {
    for (const socket of sockets) socket.destroy();
    server.close(() => resolve());
  }));
  const address = server.address();
  assert.ok(address && typeof address === 'object');
  return `${address.address}:${address.port}`;
}

test('presence gossip to a blackholed peer is bounded by the presence budget, not the RPC timeout', { timeout: 60_000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-presence-'));
  const peerEndpoint = await blackholeEndpoint(t);
  const seedEndpoint = await blackholeEndpoint(t);
  const keyPair = generateNodeKeyPair();
  const network = { networkId: 'presence-net', passphrase: 'secret', bootstrapPeers: [seedEndpoint] };
  const keys = deriveNetworkKeys(network.networkId, network.passphrase);
  // A peer beacon already in the shared registry: its advertised endpoint is dead, and the
  // seed that introduced it can only relay into the same blackhole.
  const stalePeerId = generateNodeKeyPair().nodeId;
  const beacon = sealNetworkFrame(
    { type: 'discovery', endpointAddr: peerEndpoint, expires: Date.now() + 10_000 },
    keys,
    stalePeerId,
  );
  const registry = join(dir, keys.swarmTopicId);
  await mkdir(registry, { recursive: true, mode: 0o700 });
  await writeFile(join(registry, `${createHash('sha256').update(stalePeerId).digest('hex')}.json`), JSON.stringify(beacon));
  const kernel = new Kernel([
    { plugin: createTransportPlugin({ localNodeId: keyPair.nodeId, discoveryDir: dir }) },
    { plugin: createNodePlugin({ keyPair, member: 'ben.xu', nodeName: 'mbp', exchangeIntervalMs: 600_000 }) },
  ]);
  t.after(async () => { await kernel.stop(); await rm(dir, { recursive: true, force: true }); });
  await kernel.start();
  const transport = kernel.ctx.services.transport as P2PTransportService;
  const node = kernel.ctx.services.node as PeerDirectoryService;
  // Bound: the 2s seed probe plus the 4s presence budget. The 90s task-RPC socket timeout
  // must never be inherited by ephemeral presence gossip.
  const mountStarted = performance.now();
  await transport.mountNetwork(network);
  const mountElapsed = performance.now() - mountStarted;
  assert.ok(mountElapsed < 10_000, `mountNetwork took ${Math.round(mountElapsed)}ms for a blackholed peer`);
  const publishStarted = performance.now();
  await node.updateSelfProfile({ nodeName: 'renamed' });
  const publishElapsed = performance.now() - publishStarted;
  assert.ok(publishElapsed < 6_000, `presence publish took ${Math.round(publishElapsed)}ms for a blackholed peer`);
});

test('exchange spends its timeout as one total budget across the direct attempt and fallback seeds', { timeout: 60_000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-budget-'));
  const directEndpoint = await blackholeEndpoint(t);
  const seedOne = await blackholeEndpoint(t);
  const seedTwo = await blackholeEndpoint(t);
  const transport = new P2PTransportService({ discoveryDir: dir, socketTimeoutMs: 1_000 });
  t.after(async () => { await transport.dispose(); await rm(dir, { recursive: true, force: true }); });
  await transport.mountNetwork({ networkId: 'budget-net', passphrase: 'secret', bootstrapPeers: [seedOne, seedTwo] });
  transport.registerPeerEndpoint('budget-net', 'peer-with-dead-endpoint', directEndpoint);
  const started = performance.now();
  await transport.broadcast('budget-net', 'peer.card', encode({ type: 'peer.card' }), { timeoutMs: 300 });
  const elapsed = performance.now() - started;
  // Direct (300ms) + first hanging seed drains the budget; the second seed is skipped.
  assert.ok(elapsed < 700, `broadcast spent ${Math.round(elapsed)}ms of a 300ms exchange budget`);
});

test('exchange splits its remaining budget across fallback seeds so a live relay behind a dead one is reached', { timeout: 60_000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-fallback-'));
  const network = { networkId: 'fallback-net', passphrase: 'secret' };
  const peer = async (name: string) => {
    const transport = new P2PTransportService({
      discoveryDir: join(dir, name),
      multicast: false,
      socketTimeoutMs: 2_000,
    });
    t.after(() => transport.dispose());
    await transport.mountNetwork(network);
    return transport;
  };
  const seed = await peer('seed');
  const target = await peer('target');
  // The live seed can reach the target; the sender's own endpoint for the target is dead.
  seed.registerPeerEndpoint('fallback-net', target.localNodeId, target.endpointAddr!);
  const deadDirect = await blackholeEndpoint(t);
  const deadSeed = await blackholeEndpoint(t);
  const sender = new P2PTransportService({
    discoveryDir: join(dir, 'sender'),
    multicast: false,
    socketTimeoutMs: 2_000,
  });
  t.after(() => sender.dispose());
  await sender.mountNetwork({ ...network, bootstrapPeers: [deadSeed, seed.endpointAddr!] });
  sender.registerPeerEndpoint('fallback-net', target.localNodeId, deadDirect);
  const received: string[] = [];
  target.onInboundFrame(event => {
    received.push(`${event.senderNodeId}:${event.mode}`);
    return Buffer.from('relay-pong');
  });
  const started = performance.now();
  await sender.broadcast('fallback-net', 'presence', encode({ ping: true }), { timeoutMs: 3_000 });
  const elapsed = performance.now() - started;
  // The dead seed must not consume the whole budget: the live seed behind it still delivers,
  // and the whole exchange stays inside the caller's deadline.
  assert.deepEqual(received, [`${sender.localNodeId}:broadcast`]);
  assert.ok(elapsed < 3_500, `fallback relay took ${Math.round(elapsed)}ms of a 3000ms exchange budget`);
});

test('direct frame bounds an unfinished TCP handshake independently of the RPC budget', async t => {
  const transport = new P2PTransportService({ socketTimeoutMs: 90_000 });
  const internal = transport as unknown as {
    directFrame(mount: unknown, target: string, address: string, frame: unknown, timeout: number): Promise<unknown>;
  };
  // Model a kernel SYN blackhole without dialing an external or machine-specific address.
  const connect = t.mock.method(Socket.prototype, 'connect', function (this: Socket) { return this; });
  const started = Date.now();
  await assert.rejects(internal.directFrame({}, 'target', '127.0.0.1:1', {}, 90_000),
    (error: any) => error.code === 'ETIMEDOUT' && !(error instanceof DeliveryTimeoutError));
  assert.ok(Date.now() - started < 5_000, 'TCP establishment must not wait for the 90s RPC deadline');
  await assert.rejects(internal.directFrame({}, 'target', '127.0.0.1:1', {}, 20),
    (error: any) => error.code === 'ETIMEDOUT' && !(error instanceof DeliveryTimeoutError));
  connect.mock.restore();
  const closing = t.mock.method(Socket.prototype, 'connect', function (this: Socket) {
    queueMicrotask(() => this.destroy());
    return this;
  });
  await assert.rejects(internal.directFrame({}, 'target', '127.0.0.1:1', {}, 90_000));
  closing.mock.restore();
  const server = createServer(socket => socket.end());
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = (server.address() as { port: number }).port;
  await new Promise<void>(resolve => server.close(() => resolve()));
  await assert.rejects(internal.directFrame({}, 'target', '127.0.0.1:' + port, {}, 90_000), /ECONNREFUSED/);
});

test('direct frame gives a connected slow executor the full RPC timeout', async t => {
  const transport = new P2PTransportService({ socketTimeoutMs: 4000 });
  const internal = transport as unknown as {
    directFrame(mount: unknown, target: string, address: string, frame: unknown, timeout: number): Promise<unknown>;
  };
  const sockets = new Set<Socket>();
  const server = createServer(socket => {
    sockets.add(socket);
    socket.once('close', () => sockets.delete(socket));
    socket.once('data', () => {
      const timer = setTimeout(() => {
        const body = encode({ frame: { senderNodeId: 'target' } });
        const header = Buffer.alloc(4);
        header.writeUInt32BE(body.length);
        socket.end(Buffer.concat([header, body]));
      }, 1700);
      socket.once('close', () => clearTimeout(timer));
    });
  });
  t.after(() => {
    for (const socket of sockets) socket.destroy();
    server.close();
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const endpoint = '127.0.0.1:' + (server.address() as { port: number }).port;
  assert.deepEqual(await internal.directFrame({}, 'target', endpoint, {}, 4000), { senderNodeId: 'target' });
});


test('oversized direct frames reject before opening a socket', async t => {
  const transport = new P2PTransportService();
  const internal = transport as unknown as {
    directFrame(mount: unknown, target: string, address: string, frame: unknown, timeout: number): Promise<unknown>;
  };
  const connect = t.mock.method(Socket.prototype, 'connect', function (this: Socket) { return this; });
  await assert.rejects(internal.directFrame({}, 'target', '127.0.0.1:1', { ciphertext: 'x'.repeat(8 * 1024 * 1024) }, 100),
    LocalDeliveryError);
  assert.equal(connect.mock.callCount(), 0);
});

test('knownCards gossip excludes unreachable peer cards and includes reachable ones', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-gossip-reachable-'));
  const local = generateNodeKeyPair();
  const kernel = new Kernel([
    { plugin: createTransportPlugin({ localNodeId: local.nodeId, discoveryDir: dir }) },
    { plugin: createNodePlugin({ keyPair: local, member: 'ben', nodeName: 'local', exchangeIntervalMs: 600_000 }) },
  ]);
  t.after(async () => { await kernel.stop(); await rm(dir, { recursive: true, force: true }); });
  await kernel.start();
  const transport = kernel.ctx.services.transport as P2PTransportService;
  const node = kernel.ctx.services.node as PeerDirectoryService;
  await transport.mountNetwork({ networkId: 'team', passphrase: 'secret' });

  const liveRelay = card('relay', 'us-master', { updatedAt: '2024-01-01T00:00:00.000Z' });
  const staleGhost = card('diz', 'old-mac', { updatedAt: '2026-01-02T00:00:00.000Z' });
  deliverPeerCard(kernel, liveRelay);
  deliverPeerCard(kernel, staleGhost);

  t.mock.method(transport, 'isPeerReachable', (networkId: string, nodeId: string) =>
    networkId === 'team' && nodeId === liveRelay.nodeId,
  );
  const broadcasts: Array<{ knownCards?: NodeCard[] }> = [];
  t.mock.method(transport, 'broadcast', async (_networkId: string, _subTopic: string, frame: Uint8Array) => {
    broadcasts.push(JSON.parse(Buffer.from(frame).toString('utf8')));
  });

  await node.updateSelfProfile({ nodeName: 'refreshed' });
  assert.equal(broadcasts.length, 1);
  assert.deepEqual(broadcasts[0].knownCards?.map(entry => entry.nodeId), [liveRelay.nodeId]);
});

test('listPeers onlyAvailable retains offline open+idle peers while isPeerReachable delegates liveness', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-available-reachable-'));
  const local = generateNodeKeyPair();
  const kernel = new Kernel([
    { plugin: createTransportPlugin({ localNodeId: local.nodeId, discoveryDir: dir }) },
    { plugin: createNodePlugin({ keyPair: local, member: 'ben', nodeName: 'local', exchangeIntervalMs: 600_000 }) },
  ]);
  t.after(async () => { await kernel.stop(); await rm(dir, { recursive: true, force: true }); });
  await kernel.start();
  const transport = kernel.ctx.services.transport as P2PTransportService;
  const node = kernel.ctx.services.node as PeerDirectoryService;
  await transport.mountNetwork({ networkId: 'team', passphrase: 'secret' });

  const offlineRelease = card('alice', 'offline', {
    admissionMode: 'open',
    capabilities: { summary: '', skills: [{ id: 'release', description: '' }] },
    runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] },
  });
  const liveTriage = card('bob', 'online', {
    admissionMode: 'open',
    capabilities: { summary: '', skills: [{ id: 'triage', description: '' }] },
    runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] },
  });
  const liveBusy = card('carol', 'busy', {
    admissionMode: 'open',
    capabilities: { summary: '', skills: [{ id: 'triage', description: '' }] },
    runtimeState: { load: 'busy', activeTasks: 2, availableAgents: [] },
  });
  deliverPeerCard(kernel, offlineRelease);
  deliverPeerCard(kernel, liveTriage);
  deliverPeerCard(kernel, liveBusy);

  t.mock.method(transport, 'isPeerReachable', (networkId: string, nodeId: string) =>
    networkId === 'team' && (nodeId === liveTriage.nodeId || nodeId === liveBusy.nodeId),
  );

  assert.deepEqual(
    new Set(node.listPeers({ networkId: 'team', onlyAvailable: true }).map(entry => entry.nodeId)),
    new Set([offlineRelease.nodeId, liveTriage.nodeId]),
  );
  assert.equal(node.isPeerReachable('team', liveTriage.nodeId), true);
  assert.equal(node.isPeerReachable('team', offlineRelease.nodeId), false);
});

test('prunePeers removes targeted or unreachable cards, preserves replay/equivocation floors across restart, and persists atomically', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-prune-peers-'));
  const peersPath = join(dir, 'peers.json');
  const local = generateNodeKeyPair();
  const staleTwoKeys = generateNodeKeyPair();
  const relay = card('relay', 'us-master', { updatedAt: '2024-01-01T00:00:00.000Z' });
  const staleOne = card('ben', 'old-1', { updatedAt: '2026-01-02T00:00:00.000Z' });
  const staleTwo = card('diz', 'old-2', { revision: 5, updatedAt: '2026-01-03T00:00:00.000Z' }, staleTwoKeys);
  const staleTwoRollback = card('diz', 'old-2', { revision: 4, updatedAt: '2026-01-02T00:00:00.000Z' }, staleTwoKeys);
  const staleTwoEquivocation = card('diz', 'evil-2', { revision: 5, updatedAt: '2026-01-05T00:00:00.000Z' }, staleTwoKeys);
  const staleTwoFresh = card('diz', 'reborn-2', { revision: 6, updatedAt: '2026-01-06T00:00:00.000Z' }, staleTwoKeys);
  const otherNetStale = card('loader', 'old-3', { networkId: 'other-team', updatedAt: '2026-01-04T00:00:00.000Z' });
  await writeFile(peersPath, JSON.stringify({
    schema: 'the-dealer-peer-directory/v1',
    cards: [relay, staleOne, staleTwo, otherNetStale],
  }));

  const kernels: Kernel[] = [];
  t.after(async () => {
    await Promise.all(kernels.map(k => k.stop()));
    await rm(dir, { recursive: true, force: true });
  });
  const startNode = async () => {
    const kernel = new Kernel([
      { plugin: createTransportPlugin({ localNodeId: local.nodeId, discoveryDir: dir }) },
      { plugin: createNodePlugin({ keyPair: local, member: 'ben', nodeName: 'local', peersPath, exchangeIntervalMs: 600_000 }) },
    ]);
    kernels.push(kernel);
    await kernel.start();
    const transport = kernel.ctx.services.transport as P2PTransportService;
    const node = kernel.ctx.services.node as PeerDirectoryService;
    await transport.mountNetwork({ networkId: 'team', passphrase: 'secret' });
    t.mock.method(transport, 'isPeerReachable', (networkId: string, nodeId: string) =>
      networkId === 'team' && nodeId === relay.nodeId,
    );
    return { kernel, transport, node };
  };

  const readStore = async () => JSON.parse(await readFile(peersPath, 'utf8')) as {
    cards: NodeCard[];
    prunedPeers?: Array<{ networkId: string; nodeId: string; revision: number; signature: string }>;
  };
  const persistedIds = async () => new Set<string>((await readStore()).cards.map(entry => entry.nodeId));

  const first = await startNode();
  assert.throws(() => first.node.prunePeers({}), /nodeId|unreachable/);
  assert.throws(() => first.node.prunePeers({ networkId: 'team' }), /nodeId|unreachable/);

  assert.deepEqual(first.node.prunePeers({ networkId: 'team', nodeId: staleOne.nodeId }), {
    removedCount: 1,
    removedNodeIds: [staleOne.nodeId],
  });
  assert.deepEqual(await persistedIds(), new Set([relay.nodeId, staleTwo.nodeId, otherNetStale.nodeId]));

  // Combining nodeId + unreachable does not remove a reachable peer and avoids rewriting peers.json.
  const beforeNoopStat = await stat(peersPath);
  assert.deepEqual(first.node.prunePeers({ networkId: 'team', nodeId: relay.nodeId, unreachable: true }), {
    removedCount: 0,
    removedNodeIds: [],
  });
  assert.equal((await stat(peersPath)).ino, beforeNoopStat.ino);

  // Scoped unreachable prune removes staleTwo while keeping reachable old-updatedAt relay and other-team.
  assert.deepEqual(first.node.prunePeers({ networkId: 'team', unreachable: true }), {
    removedCount: 1,
    removedNodeIds: [staleTwo.nodeId],
  });
  assert.deepEqual(first.node.listPeers({ networkId: 'team' }).map(entry => entry.nodeId), [relay.nodeId]);
  assert.deepEqual(first.node.resolveTarget(staleTwo.nodeId, 'team'), []);
  assert.deepEqual(await persistedIds(), new Set([relay.nodeId, otherNetStale.nodeId]));
  assert.ok(
    (await readStore()).prunedPeers?.some(
      entry =>
        entry.networkId === 'team' &&
        entry.nodeId === staleTwo.nodeId &&
        entry.revision === staleTwo.revision &&
        entry.signature === staleTwo.signature,
    ),
  );

  // Re-pruning an already-pruned peer is a no-op.
  assert.deepEqual(first.node.prunePeers({ networkId: 'team', nodeId: staleTwo.nodeId }), {
    removedCount: 0,
    removedNodeIds: [],
  });

  // In-memory replay protection: rollback, same-revision replay, and equivocation are all rejected.
  for (const replay of [staleTwoRollback, staleTwo, staleTwoEquivocation]) {
    deliverPeerCard(first.kernel, replay);
  }
  assert.deepEqual(first.node.listPeers({ networkId: 'team' }).map(entry => entry.nodeId), [relay.nodeId]);
  assert.deepEqual(first.node.resolveTarget(staleTwo.nodeId, 'team'), []);

  // Restart and verify durable replay/equivocation protection survives restart.
  await first.kernel.stop();
  const restarted = await startNode();
  assert.deepEqual(restarted.node.listPeers({ networkId: 'team' }).map(entry => entry.nodeId), [relay.nodeId]);
  for (const replay of [staleTwoRollback, staleTwo, staleTwoEquivocation]) {
    deliverPeerCard(restarted.kernel, replay);
  }
  assert.deepEqual(restarted.node.listPeers({ networkId: 'team' }).map(entry => entry.nodeId), [relay.nodeId]);
  assert.deepEqual(restarted.node.resolveTarget(staleTwo.nodeId, 'team'), []);

  // A strictly newer verified revision from the same key cleanly re-admits the peer and clears its pruned witness.
  deliverPeerCard(restarted.kernel, staleTwoFresh);
  assert.deepEqual(restarted.node.resolveTarget(staleTwo.nodeId, 'team'), [staleTwoFresh]);
  assert.equal(
    (await readStore()).prunedPeers?.some(entry => entry.nodeId === staleTwo.nodeId) ?? false,
    false,
  );

  // Global unreachable prune cleans staleTwoFresh and other-team while preserving reachable relay and self.
  assert.deepEqual(restarted.node.prunePeers({ unreachable: true }), {
    removedCount: 2,
    removedNodeIds: [staleTwoFresh.nodeId, otherNetStale.nodeId].sort(),
  });
  assert.deepEqual(await persistedIds(), new Set([relay.nodeId]));
  assert.equal(restarted.node.getSelfCard('team').nodeId, local.nodeId);

  // Explicit nodeId prune without unreachable removes the specified peer.
  assert.deepEqual(restarted.node.prunePeers({ nodeId: relay.nodeId }), {
    removedCount: 1,
    removedNodeIds: [relay.nodeId],
  });
  assert.deepEqual(await persistedIds(), new Set());
});

test('delayed direct Tailscale beacon upgrades relay-gossiped LAN endpoint without rolling back expiry', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-tailscale-upgrade-'));
  const kernel = new Kernel([
    { plugin: createTransportPlugin({ discoveryDir: dir, multicast: false, discoveryIntervalMs: 60_000 }) },
  ]);
  t.after(async () => { await kernel.stop(); await rm(dir, { recursive: true, force: true }); });
  await kernel.start();
  const transport = kernel.ctx.services.transport as P2PTransportService;
  await transport.mountNetwork({ networkId: 'team', passphrase: 'secret' });
  const keys = deriveNetworkKeys('team', 'secret');
  const peer = generateNodeKeyPair();
  const events: Array<{ networkId: string; nodeId: string; via: string }> = [];
  kernel.ctx.on('transport/peer-reachable', event => events.push(event));

  const internal = transport as unknown as {
    learnWithMeta(frame: unknown, remoteHost?: string, viaSeed?: string): { mount?: { beacons: Map<string, { expires: number }>; relays: Map<string, string> } };
  };
  const gossipExpires = Date.now() + 9_500;
  const gossipFrame = sealNetworkFrame(
    { type: 'discovery', endpointAddr: '10.165.37.126:45874', expires: gossipExpires },
    keys,
    peer.nodeId,
  );
  const first = internal.learnWithMeta(gossipFrame, undefined, '43.110.36.140:8880');
  assert.equal(transport.listPeerEndpoints('team').get(peer.nodeId), '10.165.37.126:45874');
  assert.equal(first.mount?.relays.get(peer.nodeId), '43.110.36.140:8880');
  assert.equal(first.mount?.beacons.get(peer.nodeId)?.expires, gossipExpires);
  assert.deepEqual(events, [{ networkId: 'team', nodeId: peer.nodeId, via: 'relay' }]);

  const directExpires = gossipExpires - 1_000;
  const directFrame = sealNetworkFrame(
    { type: 'discovery', endpointAddr: '10.165.37.126:45874', expires: directExpires },
    keys,
    peer.nodeId,
  );
  internal.learnWithMeta(directFrame, '100.64.0.28');
  assert.equal(transport.listPeerEndpoints('team').get(peer.nodeId), '100.64.0.28:45874');
  assert.equal(first.mount?.relays.has(peer.nodeId), false);
  assert.equal(
    first.mount?.beacons.get(peer.nodeId)?.expires,
    gossipExpires,
    'upgrading the endpoint must not roll back the newer beacon expiry',
  );
  assert.deepEqual(events, [
    { networkId: 'team', nodeId: peer.nodeId, via: 'relay' },
    { networkId: 'team', nodeId: peer.nodeId, via: 'direct' },
  ]);

  // Repeating the same delayed direct observation does not emit a duplicate reachability event.
  internal.learnWithMeta(directFrame, '100.64.0.28');
  assert.equal(events.length, 2);
});

test('announce refreshes wildcard endpointAddr on DHCP IP change and fetchBlobToPath falls back to signed ticket host', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'dealer-dhcp-blob-'));
  let currentLan = '10.165.36.198';
  const iface = (address: string) => ({
    en0: [{ address, netmask: '255.255.255.0', family: 'IPv4' as const, mac: '00:00:00:00:00:00', internal: false, cidr: `${address}/24` }],
  });
  const mockIfaces = t.mock.method(os, 'networkInterfaces', () => iface(currentLan));
  const refreshed = new P2PTransportService({ discoveryDir: join(dir, 'dhcp'), multicast: false, discoveryIntervalMs: 60_000 });
  t.after(async () => {
    mockIfaces.mock.restore();
    await refreshed.dispose();
    await rm(dir, { recursive: true, force: true });
  });
  await refreshed.mountNetwork({ networkId: 'dhcp-net', passphrase: 'secret' });
  const initialEndpoint = refreshed.endpointAddr!;
  const port = initialEndpoint.slice(initialEndpoint.lastIndexOf(':') + 1);
  assert.equal(initialEndpoint, `10.165.36.198:${port}`);

  currentLan = '10.165.37.126';
  const internal = refreshed as unknown as {
    mounts: Map<string, unknown>;
    selfEndpoints: Set<string>;
    announce(mount: unknown): Promise<void>;
  };
  await internal.announce(internal.mounts.get('dhcp-net')!);
  assert.equal(refreshed.endpointAddr, `10.165.37.126:${port}`);
  assert.equal(internal.selfEndpoints.has(initialEndpoint), false);
  assert.equal(internal.selfEndpoints.has(`10.165.37.126:${port}`), true);
  const sampleFile = join(dir, 'sample.txt');
  await writeFile(sampleFile, 'updated-ip-payload');
  const updatedBlob = await refreshed.shareFile(sampleFile);
  assert.ok(updatedBlob.ticket.includes(`@10.165.37.126:${port}/`));
  mockIfaces.mock.restore();

  // Receiver whose peer map still holds an old dead IP falls back to the signed ticket's url.host.
  const sender = new P2PTransportService({
    discoveryDir: join(dir, 'sender'),
    listenHost: '127.0.0.1',
    multicast: false,
    discoveryIntervalMs: 60_000,
  });
  const receiver = new P2PTransportService({
    discoveryDir: join(dir, 'receiver'),
    listenHost: '127.0.0.1',
    multicast: false,
    discoveryIntervalMs: 60_000,
  });
  t.after(async () => {
    await sender.dispose();
    await receiver.dispose();
  });
  await sender.mountNetwork({ networkId: 'blob-net', passphrase: 'secret' });
  await receiver.mountNetwork({ networkId: 'blob-net', passphrase: 'secret' });
  const closedServer = createServer();
  closedServer.listen(0, '127.0.0.1');
  await once(closedServer, 'listening');
  const deadPort = (closedServer.address() as { port: number }).port;
  await new Promise<void>(resolve => closedServer.close(() => resolve()));

  receiver.registerPeerEndpoint('blob-net', sender.localNodeId, `127.0.0.1:${deadPort}`);
  const payloadBytes = randomBytes(4096);
  const srcPath = join(dir, 'attachment.bin');
  const destPath = join(dir, 'fetched.bin');
  await writeFile(srcPath, payloadBytes);
  const shared = await sender.shareFile(srcPath);
  assert.ok(shared.ticket.includes(`@${sender.endpointAddr}/`));

  await receiver.fetchBlobToPath(shared.ticket, destPath);
  const fetched = await readFile(destPath);
  assert.deepEqual(fetched, payloadBytes);
  assert.equal(blake3(fetched), shared.blake3Hash);

  // fetchBlobRelay also bounds an unfinished TCP handshake instead of waiting 90s.
  const blobReceiver = new P2PTransportService({
    discoveryDir: join(dir, 'relay-timeout'),
    listenHost: '127.0.0.1',
    multicast: false,
    socketTimeoutMs: 90_000,
  });
  t.after(() => blobReceiver.dispose());
  await blobReceiver.mountNetwork({ networkId: 'blob-net', passphrase: 'secret' });
  const relayInternal = blobReceiver as unknown as {
    mounts: Map<string, unknown>;
    fetchBlobRelay(relay: { mount: unknown; seed: string }, nodeId: string, hash: string, size: number, temp: string): Promise<void>;
  };
  const hangConnect = t.mock.method(Socket.prototype, 'connect', function (this: Socket) { return this; });
  const started = Date.now();
  await assert.rejects(
    relayInternal.fetchBlobRelay(
      { mount: relayInternal.mounts.get('blob-net')!, seed: '127.0.0.1:1' },
      sender.localNodeId,
      shared.blake3Hash,
      shared.sizeBytes,
      join(dir, 'relay-timeout.part'),
    ),
    (error: any) => error.code === 'ETIMEDOUT',
  );
  hangConnect.mock.restore();
  assert.ok(Date.now() - started < 5_000, 'relay blob TCP connect must not wait for 90s RPC timeout');
});
