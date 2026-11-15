import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { request } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DealerNodeRuntime } from '../packages/cli/runtime.ts';
import { blake3 } from '../packages/transport/index.ts';
import type { TransportPluginOptions } from '../packages/transport/index.ts';
import type { RuntimeConfig } from '../packages/cli/config.ts';
import type { MessageEnvelope } from '../packages/core-types/index.ts';

const NETWORK = 'offline-net';

function control(
  socketPath: string,
  path: string,
  body: unknown,
): Promise<{ status: number; value: any }> {
  return new Promise((resolve, reject) => {
    const req = request(
      { socketPath, path, method: 'POST', headers: { 'content-type': 'application/json' } },
      response => {
        const chunks: Buffer[] = [];
        response.on('data', chunk => chunks.push(Buffer.from(chunk)));
        response.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8');
          resolve({
            status: response.statusCode ?? 0,
            value: text ? JSON.parse(text) : undefined,
          });
        });
      },
    );
    req.on('error', reject);
    req.end(JSON.stringify(body));
  });
}

async function until(check: () => boolean, message: string, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) {
      assert.ok(check(), `Timed out: ${message}`);
    }
    await new Promise(resolve => setTimeout(resolve, 50));
  }
}

function directory(t: TestContext): string {
  const path = mkdtempSync(join(tmpdir(), 'dealer-offline-'));
  t.after(() => rmSync(path, { recursive: true, force: true }));
  return path;
}

interface NodeOptions {
  transport?: Partial<TransportPluginOptions>;
  patch?: Partial<RuntimeConfig>;
  discovery?: string;
}

function runtimeFor(root: string, member: string, options: NodeOptions = {}): DealerNodeRuntime {
  return new DealerNodeRuntime({
    stateDir: join(root, member),
    member,
    name: member,
    profile: 'headless-relay',
    patch: {
      defaultExecutor: 'inbox',
      ...options.patch,
    },
    transport: {
      discoveryDir: join(root, options.discovery ?? `${member}-mesh`),
      multicast: false,
      ...options.transport,
    },
  });
}

test('known peer persistence survives restart and queued mail auto-redrives when the peer returns', async t => {
  const root = directory(t);
  const runtimes: DealerNodeRuntime[] = [];
  t.after(async () => {
    for (const runtime of [...runtimes].reverse()) {
      await runtime.stop().catch(() => {});
    }
  });
  const create = async (member: string) => {
    const runtime = runtimeFor(root, member, { discovery: 'mesh' });
    runtimes.push(runtime);
    await runtime.start();
    return runtime;
  };

  let ben = await create('ben');
  let diz = await create('diz');
  await ben.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  await diz.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  await until(
    () =>
      ben.node.listPeers({ networkId: NETWORK }).some(card => card.identity.member === 'diz') &&
      diz.node.listPeers({ networkId: NETWORK }).some(card => card.identity.member === 'ben'),
    'direct peer discovery',
  );
  const dizNodeId = diz.node.getSelfCard(NETWORK).nodeId;

  await diz.stop();
  await ben.stop();

  // A restarted sender loads the verified peer directory: member selectors resolve even
  // though the peer is not on the network and the transport has no live endpoint.
  ben = await create('ben');
  const peersFile = join(ben.stateDir, 'peers.json');
  assert.ok(existsSync(peersFile), 'verified peer directory is persisted');
  assert.match(readFileSync(peersFile, 'utf8'), new RegExp(dizNodeId));
  assert.equal(ben.node.resolveTarget('member:diz', NETWORK).length, 1);

  const emitted = await ben.emitter.emit({
    networkId: NETWORK,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'collab.request.v1',
    subject: 'Offline delivery',
    body: 'queued while diz is offline',
    payload: {},
  });
  assert.equal(emitted.receipts.length, 1);
  assert.equal(emitted.receipts[0].stage, 'queued');
  assert.equal(emitted.receipts[0].executionReceipt?.queuedOffline, true);
  assert.equal(emitted.receipts[0].executionReceipt?.executor, 'outbox');
  const queued = ben.listOutbox({ status: 'queued' });
  assert.equal(queued.length, 1);
  assert.equal(queued[0].id, emitted.messageId);
  assert.deepEqual(queued[0].pendingNodeIds, [dizNodeId]);

  diz = await create('diz');
  await until(
    () => ben.listOutbox({ status: 'delivered' }).length === 1,
    'peer announcement triggers outbox redrive',
  );
  await until(
    () =>
      diz.mailbox
        .list({ mailboxId: 'default' })
        .items.some(item => item.id === emitted.messageId),
    'queued envelope lands in the returning peer inbox',
  );
  // Redrive is end-to-end confirmed, not merely optimistic.
  assert.equal(ben.listOutbox({ status: 'queued' }).length, 0);
});

test('staggered offline peers converge through a relay seed spool, attachment cache and dedup-cas', async t => {
  const root = directory(t);
  const runtimes: DealerNodeRuntime[] = [];
  t.after(async () => {
    for (const runtime of [...runtimes].reverse()) {
      await runtime.stop().catch(() => {});
    }
  });
  const isolatedTransport = {
    advertisedHost: '192.0.2.1',
    socketTimeoutMs: 10_000,
  } satisfies Partial<TransportPluginOptions>;
  const create = async (member: string, options: NodeOptions = {}) => {
    const runtime = runtimeFor(root, member, options);
    runtimes.push(runtime);
    await runtime.start();
    return runtime;
  };

  const seed = await create('seed');
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const seedEndpoint = seed.transport.endpointAddr!;

  const ben = await create('ben', { transport: isolatedTransport });
  let diz = await create('diz', {
    transport: isolatedTransport,
    // The target owns duplicate suppression: a redrive after the relay drain must be
    // answered with the signed dedup-cas receipt instead of enqueueing a second message.
    patch: { policies: ['dedup-cas'] },
  });
  await ben.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [seedEndpoint],
  });
  await diz.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [seedEndpoint],
  });
  // Isolated discovery directories and TEST-NET-1 endpoints force every card and every
  // frame through the seed; knownCards gossip must teach each peer the other's card.
  await until(
    () =>
      ben.node.resolveTarget('member:diz', NETWORK).length > 0 &&
      diz.node.resolveTarget('member:ben', NETWORK).length > 0,
    'seed card gossip',
    40_000,
  );
  const dizNodeId = diz.node.getSelfCard(NETWORK).nodeId;

  await diz.stop();
  const attachmentPath = join(root, 'payload.txt');
  const attachmentBody = 'spooled attachment survives the sender going offline\n'.repeat(50);
  writeFileSync(attachmentPath, attachmentBody);
  const emitted = await ben.emitter.emit({
    networkId: NETWORK,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'collab.request.v1',
    subject: 'Staggered offline delivery',
    body: 'diz is offline; spool this on the seed',
    payload: {},
    filePaths: [attachmentPath],
  });
  assert.equal(emitted.receipts.length, 1);
  assert.equal(emitted.receipts[0].stage, 'queued');
  assert.equal(emitted.receipts[0].executionReceipt?.queuedOffline, true);
  assert.equal(emitted.receipts[0].executionReceipt?.relaySpooled, true);
  const queued = ben.listOutbox({ status: 'queued' });
  assert.equal(queued.length, 1);
  assert.deepEqual(queued[0].relaySpooledNodeIds, [dizNodeId]);
  const staged = Object.keys(queued[0].stagedFiles);
  assert.deepEqual(staged, [blake3(Buffer.from(attachmentBody))]);
  assert.ok(existsSync(queued[0].stagedFiles[staged[0]]));

  // The sender disappears before the target ever wakes up: only the seed's durable spool
  // and blob cache remain.
  await ben.stop();

  diz = await create('diz', {
    transport: isolatedTransport,
    patch: { policies: ['dedup-cas'] },
  });
  await until(
    () =>
      diz.mailbox
        .list({ mailboxId: 'default' })
        .items.some(item => item.id === emitted.messageId),
    'seed drains its spool to the reconnected target',
    40_000,
  );
  const received = diz.mailbox
    .list({ mailboxId: 'default' })
    .items.find(item => item.id === emitted.messageId)!;
  assert.equal(received.envelope.attachments[0].blake3Hash, staged[0]);
  const localAttachments = received.envelope.payload
    .localAttachments as string[] | undefined;
  assert.ok(localAttachments?.[0], 'the executor fetched the spooled attachment');
  assert.equal(readFileSync(localAttachments[0], 'utf8'), attachmentBody);

  // The original sender returns with a queued obligation; the target's dedup-cas policy
  // answers with a confirmed duplicate receipt and the outbox settles without a duplicate.
  const benAgain = await create('ben', { transport: isolatedTransport });
  await until(
    () => benAgain.listOutbox({ status: 'delivered' }).length === 1,
    'returning sender converges its outbox',
    40_000,
  );
  assert.equal(
    diz.mailbox.list({ mailboxId: 'default', includeAcked: true }).totalCount,
    1,
    'dedup-cas prevents a duplicate inbox entry',
  );
});

test('daemon outbox RPC lists and flushes durable obligations', async t => {
  const root = directory(t);
  const runtime = await runtimeFor(root, 'ben').start();
  t.after(async () => {
    await runtime.stop().catch(() => {});
  });
  await runtime.serveControl(async () => {
    throw new Error('The outbox RPC endpoints must not fall through to the CLI handler');
  });

  const empty = await control(runtime.socketPath, '/v1/outbox/list', {});
  assert.equal(empty.status, 200);
  assert.deepEqual(empty.value, []);
  const flushedEmpty = await control(runtime.socketPath, '/v1/outbox/flush', {});
  assert.equal(flushedEmpty.status, 200);
  assert.equal(flushedEmpty.value.attempted, 0);

  const nodeId = runtime.node.getSelfCard('local').nodeId;
  const envelope: MessageEnvelope = {
    id: 'msg_outbox_rpc',
    networkId: NETWORK,
    traceId: 'trace_outbox_rpc',
    from: { nodeId, member: 'ben', nodeName: 'ben' },
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'collab.request.v1',
    priority: 'normal',
    subject: 'RPC outbox',
    body: 'listed through the control socket',
    payload: {},
    attachments: [],
    createdAt: new Date().toISOString(),
    signature: 'test-signature',
  };
  runtime.outbox.enqueue({
    envelope,
    explicitTargets: ['member:diz'],
    pendingNodeIds: ['a'.repeat(64)],
  });

  const listed = await control(runtime.socketPath, '/v1/outbox/list', {
    networkId: NETWORK,
    status: 'queued',
  });
  assert.equal(listed.status, 200);
  assert.deepEqual(listed.value.map((item: { id: string }) => item.id), [envelope.id]);
  const rejected = await control(runtime.socketPath, '/v1/outbox/list', { status: 'bogus' });
  assert.equal(rejected.status, 400);
  assert.match(rejected.value.error, /Invalid outbox status filter/);
});
