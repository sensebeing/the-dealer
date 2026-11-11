import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, rm } from 'node:fs/promises';
import { createServer, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import { Kernel } from '../packages/kernel/index.ts';
import {
  deriveNetworkKeys,
  generateNodeKeyPair,
  sealNetworkFrame,
} from '../packages/core-types/index.ts';
import type { DerivedNetworkKeys, SealedNetworkFrame } from '../packages/core-types/index.ts';
import {
  createTransportPlugin,
  P2PTransportService,
} from '../packages/transport/index.ts';
import { createNodePlugin, PeerDirectoryService } from '../packages/node/index.ts';

const encode = (value: unknown) => Buffer.from(JSON.stringify(value));

async function until(check: () => boolean, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  assert.ok(check(), 'condition did not converge');
}

/** One relay seed plus a factory for peers that can only reach each other through it. */
async function relayMesh(t: TestContext, root: string) {
  // The seed is reached over TCP bootstrap peers only; skipping multicast keeps these
  // tests off the UDP discovery port that other suites bind in parallel.
  const seedKeys = generateNodeKeyPair();
  const seedKernel = new Kernel([
    {
      plugin: createTransportPlugin({
        localNodeId: seedKeys.nodeId,
        discoveryDir: join(root, 'relay'),
        multicast: false,
      }),
    },
    {
      plugin: createNodePlugin({
        keyPair: seedKeys,
        member: 'seed',
        nodeName: 'node-seed',
        capabilities: { summary: 'seed', skills: [] },
        exchangeIntervalMs: 100,
      }),
    },
  ]);
  const kernels: Kernel[] = [seedKernel];
  t.after(async () => {
    await Promise.all(kernels.map(kernel => kernel.stop()));
    await rm(root, { recursive: true, force: true });
  });
  await seedKernel.start();
  const relay = seedKernel.ctx.services.transport as P2PTransportService;
  await relay.mountNetwork({ networkId: 'relay-net', passphrase: 'secret' });
  const seed = relay.endpointAddr!;

  const isolated = async (member: string) => {
    const keyPair = generateNodeKeyPair();
    const kernel = new Kernel([
      {
        plugin: createTransportPlugin({
          localNodeId: keyPair.nodeId,
          discoveryDir: join(root, member),
          multicast: false,
          // TEST-NET-1 is advertised and gossiped but never reachable, so only the
          // relay seed can carry frames between the two peers.
          advertisedHost: '192.0.2.1',
          socketTimeoutMs: 400,
        }),
      },
      {
        plugin: createNodePlugin({
          keyPair,
          member,
          nodeName: `node-${member}`,
          capabilities: { summary: member, skills: [] },
          exchangeIntervalMs: 100,
        }),
      },
    ]);
    kernels.push(kernel);
    await kernel.start();
    const transport = kernel.ctx.services.transport as P2PTransportService;
    await transport.mountNetwork({ networkId: 'relay-net', passphrase: 'secret', bootstrapPeers: [seed] });
    return { kernel, transport, node: kernel.ctx.services.node as PeerDirectoryService };
  };
  return { seed, relay, isolated };
}

test('isolated nodes discover and exchange through a sealed relay seed', async t => {
  const root = await mkdtemp(join(tmpdir(), 'dealer-relay-'));
  const { isolated, relay } = await relayMesh(t, root);

  const alice = await isolated('alice');
  const bob = await isolated('bob');

  // Gossip through the seed is the only way either node can learn the other's endpoint.
  await until(() =>
    alice.transport.listPeerEndpoints('relay-net').get(bob.transport.localNodeId)?.startsWith('192.0.2.1:') === true &&
    bob.transport.listPeerEndpoints('relay-net').get(alice.transport.localNodeId)?.startsWith('192.0.2.1:') === true);

  // NodeCards travel as broadcast frames over the relay.
  await until(() => alice.node.listPeers({ networkId: 'relay-net' }).some(card => card.identity.member === 'bob'));
  await until(() => bob.node.listPeers({ networkId: 'relay-net' }).some(card => card.identity.member === 'alice'));
  // The seed's own NodeCard can only reach either peer down that peer's tunnel to the
  // seed: their advertised TEST-NET-1 endpoints are not reachable from the seed.
  await until(() =>
    alice.node.listPeers({ networkId: 'relay-net' }).some(card => card.nodeId === relay.localNodeId));
  await until(() =>
    bob.node.listPeers({ networkId: 'relay-net' }).some(card => card.nodeId === relay.localNodeId));

  const received: string[] = [];
  bob.kernel.ctx.on('transport/inbound', event => {
    const payload = event.payload as { ping?: boolean };
    if (!payload?.ping) {
      return;
    }
    received.push(`${event.senderNodeId}:${event.mode}`);
    return Buffer.from('relayed-pong');
  });

  const reply = await alice.transport.sendUnicast('relay-net', bob.transport.localNodeId, encode({ ping: true }));
  assert.equal(Buffer.from(reply).toString(), 'relayed-pong');
  assert.ok(received.includes(`${alice.transport.localNodeId}:unicast`));

  await alice.transport.broadcast('relay-net', 'ping', encode({ ping: true }));
  await until(() => received.includes(`${alice.transport.localNodeId}:broadcast`));

  // The ticket keeps Alice's unreachable advertised endpoint; the fetch must fall
  // back to the relay seed that introduced her.
  const input = join(root, 'report.bin');
  const bytes = randomBytes(256 * 1024);
  await writeFile(input, bytes);
  const blob = await alice.transport.shareFile(input);
  assert.ok(blob.ticket.includes('192.0.2.1:'));
  const destination = join(root, 'bob-received', 'report.bin');
  await bob.transport.fetchBlobToPath(blob.ticket, destination);
  assert.deepEqual(await readFile(destination), bytes);

  // The seed is just another peer: Alice's advertised endpoint is unreachable from it
  // as well, so the blob must come back down Alice's own tunnel to the seed.
  const seedCopy = join(root, 'seed-received', 'report.bin');
  await relay.fetchBlobToPath(blob.ticket, seedCopy);
  assert.deepEqual(await readFile(seedCopy), bytes);
});

test('bootstrap tunnel identity deduplicates aliases, ignores self, and replaces stale relay sockets', async t => {
  const root = await mkdtemp(join(tmpdir(), 'dealer-relay-dedup-'));
  const { isolated, relay, seed } = await relayMesh(t, root);
  const alice = await isolated('alice');
  const mount = (alice.transport as any).mounts.get('relay-net');
  mount.config.bootstrapPeers.push(seed.replace('127.0.0.1', 'localhost'), alice.transport.endpointAddr);
  mount.lastSeedProbeAt = Number.NEGATIVE_INFINITY;
  await alice.transport['discover']();
  const outbound = mount.outboundTunnels as Map<string, { socket: Socket; seed: string; acked: boolean }>;
  assert.equal(outbound.size, 1, 'seed aliases share one verified outbound tunnel');
  assert.equal(outbound.has(alice.transport.localNodeId), false, 'self discovery never creates a tunnel');
  const oldSocket = outbound.get(relay.localNodeId)!.socket;
  const relayMount = (relay as any).mounts.get('relay-net');
  relayMount.inboundTunnels.delete(alice.transport.localNodeId);
  mount.lastSeedProbeAt = Number.NEGATIVE_INFINITY;
  await alice.transport['discover']();
  assert.notEqual(outbound.get(relay.localNodeId)!.socket, oldSocket, 'a relay restart replaces the stale acknowledged tunnel');
  let delivered = false;
  alice.kernel.ctx.on('transport/inbound', event => {
    const payload = event.payload as { ping?: string };
    if (payload?.ping === 'after-restart') delivered = true;
  });
  await until(() => relayMount.inboundTunnels.has(alice.transport.localNodeId));
  await relay.sendUnicast('relay-net', alice.transport.localNodeId, encode({ ping: 'after-restart' }));
  assert.equal(delivered, true, 'the replacement tunnel delivers frames to the peer');
});

test('the relay packet budget bounds blobs: the largest legal one crosses the seed tunnel', async t => {
  const root = await mkdtemp(join(tmpdir(), 'dealer-relay-blob-'));
  const { seed } = await relayMesh(t, root);
  // Blob transfers need no node-card exchange, so they run on bare transports and
  // leave the relay tunnel to the attachment.
  const peer = async (member: string) => {
    const transport = new P2PTransportService({
      discoveryDir: join(root, member),
      multicast: false,
      // Advertised and gossiped but never reachable, so the blob must come back
      // through the seed that introduced the sender.
      advertisedHost: '192.0.2.1',
      socketTimeoutMs: 10_000,
    });
    t.after(() => transport.dispose());
    await transport.mountNetwork({ networkId: 'relay-net', passphrase: 'secret', bootstrapPeers: [seed] });
    return transport;
  };
  const alice = await peer('alice');
  const bob = await peer('bob');
  await until(() =>
    bob.listPeerEndpoints('relay-net').get(alice.localNodeId)?.startsWith('192.0.2.1:') === true);

  // The raw size whose base64 form still fits one relay response packet.
  const bytes = Buffer.alloc(Math.floor((8 * 1024 * 1024 - 4096) * 3 / 4), 0x61);
  const input = join(root, 'boundary.bin');
  await writeFile(input, bytes);
  const blob = await alice.shareFile(input);
  assert.ok(blob.ticket.includes('192.0.2.1:'));
  const destination = join(root, 'bob-boundary', 'boundary.bin');
  await bob.fetchBlobToPath(blob.ticket, destination);
  assert.deepEqual(await readFile(destination), bytes);

  // One byte more can never leave the relay tunnel as a single packet, so it must be
  // refused instead of being expanded to base64 and dropped mid-flight.
  const oversized = join(root, 'oversized.bin');
  await writeFile(oversized, Buffer.concat([bytes, Buffer.from([0x61])]));
  const tooBig = await alice.shareFile(oversized);
  await assert.rejects(bob.fetchBlobToPath(tooBig.ticket, join(root, 'bob-oversized', 'oversized.bin')));
});

/** Just enough of the discovery protocol for one peer to probe this seed over TCP. */
async function startSeed(
  t: TestContext,
  keys: DerivedNetworkKeys,
  gossip: (probe: number) => SealedNetworkFrame[],
  relayReply: string,
) {
  const nodeId = generateNodeKeyPair().nodeId;
  const sockets = new Set<Socket>();
  let endpoint = '';
  let probes = 0;
  let settled = 0;
  const server = createServer(socket => {
    sockets.add(socket);
    socket.once('close', () => sockets.delete(socket));
    let buffered = Buffer.alloc(0);
    let servedProbe = false;
    socket.on('error', () => {});
    socket.on('data', chunk => {
      buffered = Buffer.concat([buffered, chunk]);
      while (buffered.length >= 4) {
        const size = buffered.readUInt32BE(0);
        if (buffered.length < size + 4) {
          return;
        }
        const request = JSON.parse(buffered.subarray(4, size + 4).toString());
        buffered = buffered.subarray(size + 4);
        if (request?.type === 'discovery') {
          servedProbe = true;
          probes += 1;
          socket.write(framedPacket({
            type: 'discovery',
            frame: sealNetworkFrame(
              { type: 'discovery', endpointAddr: endpoint, expires: Date.now() + 10_000 },
              keys,
              nodeId,
            ),
            // Each seed re-gossips whatever beacon its cache holds, even a stale one.
            peers: gossip(probes),
          }));
        }
        if (request?.type === 'relay' && typeof request.frame?.nonce === 'string' && request.target) {
          socket.write(framedPacket({
            frame: sealNetworkFrame(
              { requestNonce: request.frame.nonce, dataBase64: Buffer.from(relayReply).toString('base64') },
              keys,
              request.target,
            ),
          }));
        }
      }
    });
    socket.on('close', () => {
      if (servedProbe) {
        settled += 1;
      }
    });
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  endpoint = `127.0.0.1:${typeof address === 'object' && address ? address.port : 0}`;
  t.after(() => {
    for (const socket of sockets) {
      socket.destroy();
    }
    server.close();
  });
  return { endpoint, settled: () => settled };
}

function framedPacket(value: unknown): Buffer {
  const body = Buffer.from(JSON.stringify(value));
  const size = Buffer.alloc(4);
  size.writeUInt32BE(body.length);
  return Buffer.concat([size, body]);
}

test('only a strictly newer gossiped beacon can overwrite a peer endpoint or relay seed', async t => {
  const root = await mkdtemp(join(tmpdir(), 'dealer-beacon-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const keys = deriveNetworkKeys('beacon-net', 'secret');
  const alice = generateNodeKeyPair().nodeId;
  const expires = Date.now() + 20_000;
  const fresher = sealNetworkFrame(
    { type: 'discovery', endpointAddr: '127.0.0.1:1', expires },
    keys,
    alice,
  );
  // Ten seconds older but still inside the accepted discovery window.
  const staler = sealNetworkFrame(
    { type: 'discovery', endpointAddr: '127.0.0.1:2', expires: expires - 10_000 },
    keys,
    alice,
  );
  // The same expiry is the same generation, so it carries no newer endpoint either.
  const sameGeneration = sealNetworkFrame(
    { type: 'discovery', endpointAddr: '127.0.0.1:3', expires },
    keys,
    alice,
  );

  // The seed that introduced Alice gossips her fresher beacon once; the other seed's
  // stale cache keeps replaying the older frame and a same-generation frame afterwards.
  const introducing = await startSeed(t, keys, probe => (probe === 1 ? [fresher] : []), 'from-introducing-seed');
  const stale = await startSeed(
    t,
    keys,
    probe => (probe === 1 ? [staler] : [staler, sameGeneration]),
    'from-stale-seed',
  );

  const transport = new P2PTransportService({
    discoveryDir: join(root, 'bob'),
    multicast: false,
    socketTimeoutMs: 2_000,
  });
  t.after(() => transport.dispose());
  await transport.mountNetwork({
    networkId: 'beacon-net',
    passphrase: 'secret',
    // The stale seed is configured first, so only a relay mapping that still points
    // at the introducing seed can produce its reply.
    bootstrapPeers: [stale.endpoint, introducing.endpoint],
  });

  await until(() => stale.settled() >= 2);
  assert.equal(transport.listPeerEndpoints('beacon-net').get(alice), '127.0.0.1:1');
  // The relay mapping still points at the introducing seed, so that is who carries the frame.
  const reply = await transport.sendUnicast('beacon-net', alice, encode({ ping: true }));
  assert.equal(Buffer.from(reply).toString(), 'from-introducing-seed');
});
