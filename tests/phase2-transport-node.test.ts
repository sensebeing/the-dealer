import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm, stat, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash, randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createConnection, createServer } from 'node:net';
import type { Socket } from 'node:net';
import { Kernel } from '../packages/kernel/index.ts';
import { generateNodeKeyPair, deriveNetworkKeys, sealNetworkFrame } from '../packages/core-types/index.ts';
import type { InboundFrameEvent } from '../packages/core-types/index.ts';
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
  // Bound: the 2s seed probe plus the 2s presence budget. The 90s task-RPC socket timeout
  // must never be inherited by ephemeral presence gossip.
  const mountStarted = performance.now();
  await transport.mountNetwork(network);
  const mountElapsed = performance.now() - mountStarted;
  assert.ok(mountElapsed < 8_000, `mountNetwork took ${Math.round(mountElapsed)}ms for a blackholed peer`);
  const publishStarted = performance.now();
  await node.updateSelfProfile({ nodeName: 'renamed' });
  const publishElapsed = performance.now() - publishStarted;
  assert.ok(publishElapsed < 5_000, `presence publish took ${Math.round(publishElapsed)}ms for a blackholed peer`);
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
