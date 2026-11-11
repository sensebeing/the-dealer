import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import { Kernel } from '../packages/kernel/index.ts';
import { generateNodeKeyPair } from '../packages/core-types/index.ts';
import { createTransportPlugin, P2PTransportService } from '../packages/transport/index.ts';
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

test('isolated nodes discover and exchange through a sealed relay seed', async t => {
  const root = await mkdtemp(join(tmpdir(), 'dealer-relay-'));
  const relay = new P2PTransportService({ discoveryDir: join(root, 'relay') });
  const kernels: Kernel[] = [];
  t.after(async () => {
    await Promise.all(kernels.map(kernel => kernel.stop()));
    await relay.dispose();
    await rm(root, { recursive: true, force: true });
  });
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

  const alice = await isolated('alice');
  const bob = await isolated('bob');

  // Gossip through the seed is the only way either node can learn the other's endpoint.
  await until(() =>
    alice.transport.listPeerEndpoints('relay-net').get(bob.transport.localNodeId)?.startsWith('192.0.2.1:') === true &&
    bob.transport.listPeerEndpoints('relay-net').get(alice.transport.localNodeId)?.startsWith('192.0.2.1:') === true);

  // NodeCards travel as broadcast frames over the relay.
  await until(() => alice.node.listPeers({ networkId: 'relay-net' }).some(card => card.identity.member === 'bob'));
  await until(() => bob.node.listPeers({ networkId: 'relay-net' }).some(card => card.identity.member === 'alice'));

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
});
