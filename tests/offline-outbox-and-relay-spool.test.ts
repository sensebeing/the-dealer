import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { EventEmitter } from 'node:events';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  truncateSync,
  writeFileSync,
} from 'node:fs';
import { request } from 'node:http';
import { createConnection, createServer, type Socket } from 'node:net';
import { tmpdir, hostname } from 'node:os';
import { dirname, join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { DealerNodeRuntime } from '../packages/cli/runtime.ts';
import { runDealerCli, summarizeNextStep } from '../packages/cli/index.ts';
import { Kernel } from '../packages/kernel/index.ts';
import { createEmitterPlugin } from '../packages/emitter/index.ts';
import { createRouterPlugin } from '../packages/router/index.ts';
import { OutboxRedriveService, OutboxStore } from '../packages/mailbox/outbox.ts';
import { MailboxStore } from '../packages/mailbox/store.ts';
import { createPollWatermarkSourcePlugin } from '../packages/plugins-source/index.ts';
import { P2PTransportService, blake3 } from '../packages/transport/index.ts';
import type { TransportPluginOptions } from '../packages/transport/index.ts';
import type { RuntimeConfig } from '../packages/cli/config.ts';
import {
  DeliveryTimeoutError,
  LocalDeliveryError,
  PeerUnreachableError,
  deriveNetworkKeys,
  generateNodeKeyPair,
  isConfirmedHandoffReceipt,
  isDeliveryTimeoutFailure,
  isLocalDeliveryError,
  isPeerUnreachableError,
  isUnresolvedTargetError,
  sealNetworkFrame,
  signEnvelope,
  signNodeCard,
  signReceipt,
} from '../packages/core-types/index.ts';
import type {
  DeliveryReceipt,
  DeliveryStage,
  EmitMessageInput,
  EmitterService,
  MessageEnvelope,
  NodeCard,
  NodeService,
  TransportService,
} from '../packages/core-types/index.ts';

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
  port?: number;
}

function runtimeFor(root: string, member: string, options: NodeOptions = {}): DealerNodeRuntime {
  return new DealerNodeRuntime({
    stateDir: join(root, member),
    member,
    name: member,
    profile: 'headless-relay',
    ...(options.port !== undefined ? { port: options.port } : {}),
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

  // An explicit selector with no directory entry yet is still a durable obligation. Its
  // first emit has no target, so the recorded outbound route must converge once redrive
  // resolves the member after that peer joins. The ask result must not claim the local
  // outbox signer as the target.
  const askOut: string[] = [];
  const askCode = await runDealerCli(
    [
      'ask',
      '--network',
      NETWORK,
      '--to',
      'member:future',
      '--subject',
      'Future member',
      '--body',
      'queued before the member exists',
      '--json',
    ],
    { runtime: ben, stdout: line => askOut.push(line) },
  );
  assert.equal(askCode, 0);
  const asked = JSON.parse(askOut.join('\n')) as {
    questionId: string;
    targetNodeId: string | null;
  };
  assert.equal(asked.targetNodeId, null);
  const unknownItem = ben
    .listOutbox({ status: 'queued' })
    .find(item => item.id === asked.questionId);
  assert.ok(unknownItem);
  assert.deepEqual(unknownItem.pendingNodeIds, []);
  assert.equal(ben.mailbox.resolveReplyTarget(asked.questionId)?.peerNodeId, '');

  const future = await create('future');
  await future.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const futureNodeId = future.node.getSelfCard(NETWORK).nodeId;
  await until(
    () =>
      ben
        .listOutbox({ status: 'delivered' })
        .some(item => item.id === asked.questionId),
    'redrive resolves and delivers the initially unknown member',
  );
  assert.equal(
    ben.mailbox.resolveReplyTarget(asked.questionId)?.peerNodeId,
    futureNodeId,
    'the outbound thread route converges to the resolved target',
  );
  await until(
    () =>
      future.mailbox
        .list({ mailboxId: 'default' })
        .items.some(item => item.id === asked.questionId),
    'the resolved target receives the queued question',
  );
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
  const seedPort = Number(seedEndpoint.slice(seedEndpoint.lastIndexOf(':') + 1));
  assert.ok(Number.isSafeInteger(seedPort) && seedPort > 0);

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
  // and blob cache remain. Restart the seed on the same endpoint first: the reloaded spool
  // must rehydrate attachment blobs even though the frame entry is drained and removed
  // before the target's executor asks for the blob.
  await ben.stop();
  await seed.stop();
  const restartedSeed = await create('seed', { port: seedPort });
  await restartedSeed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  assert.equal(restartedSeed.transport.endpointAddr, seedEndpoint);

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

test('outbox list and flush use the single CLI control surface', async t => {
  const root = directory(t);
  const runtime = runtimeFor(root, 'ben');
  await runtime.start();
  t.after(async () => {
    await runtime.stop().catch(() => {});
  });
  await runtime.serveControl(async () => {
    throw new Error('The CLI handler is the only outbox control surface');
  });

  const listCli = async (args: string[]) => {
    const lines: string[] = [];
    const code = await runDealerCli(args, { runtime, stdout: line => lines.push(line) });
    return { code, value: lines.length ? JSON.parse(lines.join('\n')) : undefined };
  };
  const empty = await listCli(['outbox', 'list', '--json']);
  assert.equal(empty.code, 0);
  assert.deepEqual(empty.value, []);
  const flushedEmpty = await listCli(['outbox', 'flush', '--json']);
  assert.equal(flushedEmpty.code, 0);
  assert.equal(flushedEmpty.value.attempted, 0);

  const removedRpc = await control(runtime.socketPath, '/v1/outbox/list', {});
  assert.equal(removedRpc.status, 404, 'no duplicate outbox RPC surface remains');

  const nodeId = runtime.node.getSelfCard('local').nodeId;
  const envelope: MessageEnvelope = {
    id: 'msg_outbox_cli',
    networkId: NETWORK,
    traceId: 'trace_outbox_cli',
    from: { nodeId, member: 'ben', nodeName: 'ben' },
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'collab.request.v1',
    priority: 'normal',
    subject: 'CLI outbox',
    body: 'listed through the standard CLI surface',
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

  const listed = await listCli([
    'outbox',
    'list',
    '--network',
    NETWORK,
    '--status',
    'queued',
    '--json',
  ]);
  assert.equal(listed.code, 0);
  assert.deepEqual(listed.value.map((item: { id: string }) => item.id), [envelope.id]);

  // A scoped operator flush must honor --network; without it every queued obligation in
  // the local store would be attempted regardless of the requested network.
  const otherEnvelope: MessageEnvelope = {
    ...envelope,
    id: 'msg_outbox_cli_other',
    traceId: 'trace_outbox_cli_other',
    networkId: 'other-net',
  };
  runtime.outbox.enqueue({
    envelope: otherEnvelope,
    explicitTargets: ['member:other'],
    pendingNodeIds: ['b'.repeat(64)],
  });
  const scopedFlush = await listCli(['outbox', 'flush', '--network', NETWORK, '--json']);
  assert.equal(scopedFlush.code, 0);
  assert.equal(scopedFlush.value.attempted, 1);
  assert.equal(
    scopedFlush.value.stillQueued,
    1,
    'the result count is scoped to the requested network',
  );
  assert.deepEqual(
    scopedFlush.value.items.map((item: { id: string }) => item.id),
    [envelope.id],
  );
  assert.equal(
    runtime.outbox.get(otherEnvelope.id)?.status,
    'queued',
    'an out-of-network obligation is not attempted by a scoped flush',
  );
  const stderr: string[] = [];
  const rejected = await runDealerCli(['outbox', 'list', '--status', 'bogus', '--json'], {
    runtime,
    stderr: line => stderr.push(line),
  });
  assert.notEqual(rejected, 0);
  assert.match(stderr.join('\n'), /--status must be queued, delivered, failed or all/);
});

test('dealer thread prune removes orphan outbound records and guards live outbox obligations', async t => {
  const root = directory(t);
  const stateDir = join(root, 'state');
  mkdirSync(stateDir, { recursive: true });
  const store = new MailboxStore(join(stateDir, 'mailbox.json'));
  const peer = generateNodeKeyPair();
  const sender = generateNodeKeyPair();
  const message = (
    id: string,
    semantic: MessageEnvelope['semantic'],
    from: ReturnType<typeof generateNodeKeyPair>,
    member: string,
  ): MessageEnvelope =>
    signEnvelope(
      {
        id,
        traceId: id,
        networkId: NETWORK,
        from: { nodeId: from.nodeId, member, nodeName: 'node' },
        routing: { mode: 'unicast', targets: [] },
        kind: 'collab.request.v1',
        priority: 'normal',
        subject: id,
        body: id,
        payload: {},
        attachments: [],
        createdAt: new Date().toISOString(),
        ...(semantic ? { semantic } : {}),
      },
      from.privateKeyPem,
    );

  const inbound = message(
    'msg_inbound_prune',
    { type: 'question', threadId: 'msg_inbound_prune' },
    peer,
    'diz',
  );
  store.recordInboundEnvelope(inbound, 'default');
  const orphan = message(
    'msg_orphan_reply',
    { type: 'reply', threadId: 'msg_inbound_prune' },
    sender,
    'ben',
  );
  store.recordOutboundEnvelope(orphan, [peer.nodeId], 'default');
  assert.equal(store.getThread('msg_inbound_prune')!.state, 'replied');

  const run = async (args: string[]) => {
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli([...args, '--json'], {
      stateDir,
      stdout: line => stdout.push(line),
      stderr: line => stderr.push(line),
    });
    return {
      code,
      value: stdout.length ? JSON.parse(stdout.join('\n')) : undefined,
      stderr: stderr.join('\n'),
    };
  };

  const pruned = await run(['thread', 'prune', orphan.id]);
  assert.equal(pruned.code, 0, pruned.stderr);
  assert.equal(pruned.value.ok, true);
  assert.equal(pruned.value.threadId, 'msg_inbound_prune');
  assert.equal(pruned.value.remainingCount, 1);
  assert.equal(pruned.value.state, 'open');
  assert.equal(pruned.value.pruned.messageId, orphan.id);
  assert.equal(store.getThread('msg_inbound_prune')!.state, 'open');
  assert.equal(store.getThread('msg_inbound_prune')!.settlement, undefined);

  const inboundRefusal = await run(['thread', 'prune', inbound.id]);
  assert.equal(inboundRefusal.code, 1);
  assert.match(inboundRefusal.stderr, /Cannot prune inbound thread record/);

  const unknown = await run(['thread', 'prune', 'msg_missing_prune']);
  assert.equal(unknown.code, 1);
  assert.match(unknown.stderr, /Outbound thread record not found/);

  // A queued obligation still owns the message: pruning is refused without --force.
  const outbox = new OutboxStore(join(stateDir, 'outbox.json'), join(stateDir, 'outbox-blobs'));
  const guarded = message(
    'msg_guarded_reply',
    { type: 'reply', threadId: 'msg_inbound_prune' },
    sender,
    'ben',
  );
  store.recordOutboundEnvelope(guarded, [peer.nodeId], 'default');
  outbox.enqueue({
    envelope: guarded,
    explicitTargets: [peer.nodeId],
    pendingNodeIds: [peer.nodeId],
  });
  const guardedRefusal = await run(['thread', 'prune', guarded.id]);
  assert.equal(guardedRefusal.code, 1);
  assert.match(guardedRefusal.stderr, /Refusing to prune outbound thread record/);
  assert.equal(store.getThread(guarded.id)?.direction, 'outbound');

  const forced = await run(['thread', 'prune', guarded.id, '--force']);
  assert.equal(forced.code, 0, forced.stderr);
  assert.equal(
    store.listThreadRecords('msg_inbound_prune').some(record => record.messageId === guarded.id),
    false,
  );

  // A delivered obligation is guarded exactly like a queued one.
  const delivered = message(
    'msg_delivered_reply',
    { type: 'reply', threadId: 'msg_inbound_prune' },
    sender,
    'ben',
  );
  store.recordOutboundEnvelope(delivered, [peer.nodeId], 'default');
  outbox.enqueue({
    envelope: delivered,
    explicitTargets: [peer.nodeId],
    pendingNodeIds: [peer.nodeId],
  });
  outbox.markNodeDelivered(delivered.id, peer.nodeId, receiptFor({
    messageId: delivered.id,
    networkId: NETWORK,
    fromNodeId: peer.nodeId,
  }));
  assert.equal(outbox.get(delivered.id)?.status, 'delivered');
  const deliveredRefusal = await run(['thread', 'prune', delivered.id]);
  assert.equal(deliveredRefusal.code, 1);
  assert.match(deliveredRefusal.stderr, /while outbox status is delivered/);
});

function cardFor(
  keyPair: ReturnType<typeof generateNodeKeyPair>,
  networkId: string,
  member: string,
  revision = 1,
): NodeCard {
  return signNodeCard(
    {
      nodeId: keyPair.nodeId,
      networkId,
      revision,
      identity: { member, nodeName: member },
      admissionMode: 'open',
      capabilities: { summary: member, skills: [] },
      runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] },
      updatedAt: new Date().toISOString(),
    },
    keyPair.privateKeyPem,
  );
}

function envelopeFor(
  sender: ReturnType<typeof generateNodeKeyPair>,
  networkId: string,
  target: string,
): MessageEnvelope {
  return signEnvelope(
    {
      id: randomUUID(),
      traceId: randomUUID(),
      networkId,
      from: { nodeId: sender.nodeId, member: 'ben', nodeName: 'ben' },
      routing: { mode: 'unicast', targets: [target] },
      kind: 'task',
      priority: 'normal',
      subject: 'outbox test',
      body: 'outbox test body',
      payload: {},
      attachments: [],
      createdAt: new Date().toISOString(),
    },
    sender.privateKeyPem,
  );
}

async function emitterHarness(
  t: TestContext,
  options: {
    stage?: DeliveryStage;
    sendError?: Error;
    attemptTimeoutMs?: number;
    extraMembers?: readonly string[];
    plan?: (
      targetNodeId: string,
      callIndex: number,
    ) => { stage?: DeliveryStage; detail?: string; error?: Error; delayMs?: number } | undefined;
  } = {},
) {
  const dir = directory(t);
  const networkId = 'harness-net';
  const sender = generateNodeKeyPair();
  const receiver = generateNodeKeyPair();
  const targetCard = cardFor(receiver, networkId, 'diz');
  const extraTargets = (options.extraMembers ?? []).map(member => {
    const keyPair = generateNodeKeyPair();
    return { member, keyPair, card: cardFor(keyPair, networkId, member) };
  });
  const targetCards = [targetCard, ...extraTargets.map(target => target.card)];
  const receiptKeys = new Map<string, string>([
    [targetCard.nodeId, receiver.privateKeyPem],
    ...extraTargets.map(target => [target.card.nodeId, target.keyPair.privateKeyPem] as const),
  ]);
  const selfCard = cardFor(sender, networkId, 'ben');
  const outbox = new OutboxStore(join(dir, 'outbox.json'), join(dir, 'outbox-blobs'));
  // Mirrors the runtime subscription: `emitter/sent` is the only writer of the outbound
  // thread ledger, so any event emitted by a send that never committed is observable as a
  // polluted record here.
  const mailbox = new MailboxStore(join(dir, 'mailbox.json'));
  const sentEvents: Array<{
    envelope: MessageEnvelope;
    targetNodeIds: readonly string[];
    receipts: readonly DeliveryReceipt[];
  }> = [];
  const sendCalls: Array<{
    targetNodeId: string;
    callIndex: number;
    opts?: {
      timeoutMs?: number;
      spoolIfOffline?: boolean;
      spoolKey?: string;
      spoolBlobHashes?: readonly string[];
    };
    envelope: MessageEnvelope;
  }> = [];
  let mounted = true;
  const transport = {
    localNodeId: sender.nodeId,
    async mountNetwork() {
      throw new Error('unused');
    },
    async unmountNetwork() {},
    listMountedNetworks: () => (mounted ? [networkId] : []),
    registerBlob(_hash: string, filePath: string) {
      if (!existsSync(filePath)) {
        throw Object.assign(new Error(`ENOENT: ${filePath}`), { code: 'ENOENT' });
      }
    },
    async sendUnicast(
      targetNetworkId: string,
      targetNodeId: string,
      frame: Uint8Array,
      opts?: {
        timeoutMs?: number;
        spoolIfOffline?: boolean;
        spoolKey?: string;
        spoolBlobHashes?: readonly string[];
      },
    ) {
      const envelope = (JSON.parse(Buffer.from(frame).toString('utf8')) as {
        envelope: MessageEnvelope;
      }).envelope;
      const callIndex = sendCalls.filter(call => call.targetNodeId === targetNodeId).length;
      sendCalls.push({ targetNodeId, callIndex, ...(opts ? { opts } : {}), envelope });
      const plan = options.plan?.(targetNodeId, callIndex) ?? {};
      if (options.sendError) {
        throw options.sendError;
      }
      if (plan.delayMs !== undefined) {
        await new Promise<void>((resolveDelay, rejectDelay) => {
          const timer = setTimeout(resolveDelay, plan.delayMs);
          const timeout = setTimeout(() => {
            clearTimeout(timer);
            rejectDelay(new PeerUnreachableError(targetNodeId, 'Transport timeout', {
              cause: new DeliveryTimeoutError('Transport timeout'),
            }));
          }, Math.max(1, opts?.timeoutMs ?? 90_000));
        });
      }
      if (plan.error) {
        throw plan.error;
      }
      const receipt = signReceipt(
        {
          messageId: envelope.id,
          networkId: targetNetworkId,
          fromNodeId: targetNodeId,
          stage: plan.stage ?? options.stage ?? 'dispatched',
          ...(plan.detail !== undefined
            ? { detail: plan.detail }
            : options.stage === 'rejected'
              ? { detail: 'gate closed' }
              : {}),
          updatedAt: new Date().toISOString(),
        },
        receiptKeys.get(targetNodeId) ?? receiver.privateKeyPem,
      );
      return Buffer.from(JSON.stringify(receipt));
    },
    async sendMulticast() {
      return new Map();
    },
    async broadcast() {},
    async shareFile(filePath: string) {
      const bytes = readFileSync(filePath);
      const hash = blake3(bytes);
      this.registerBlob(hash, filePath);
      return {
        name: 'attachment',
        mimeType: 'application/octet-stream',
        sizeBytes: bytes.length,
        blake3Hash: hash,
        ticket: `local:${hash}`,
      };
    },
    async fetchBlobToPath() {},
  } as unknown as TransportService;
  const node = {
    getSelfCard: () => selfCard,
    async updateSelfProfile() {},
    listPeers: () => targetCards,
    resolveTarget: (selector: string) => {
      const card = targetCards.find(
        candidate =>
          selector === candidate.nodeId ||
          selector === `member:${candidate.identity.member}`,
      );
      return card ? [card] : [];
    },
  } as unknown as NodeService;
  const kernel = new Kernel();
  kernel.ctx.provide('transport', transport);
  kernel.ctx.provide('node', node);
  await kernel.use(createRouterPlugin());
  await kernel.use(
    createEmitterPlugin({
      privateKeyPem: sender.privateKeyPem,
      outbox,
      ...(options.attemptTimeoutMs !== undefined
        ? { attemptTimeoutMs: options.attemptTimeoutMs }
        : {}),
    }),
  );
  kernel.ctx.on('emitter/sent', event => {
    sentEvents.push(event);
    mailbox.recordOutboundEnvelope(
      event.envelope,
      event.targetNodeIds,
      event.envelope.semantic?.replyMailbox ?? 'default',
    );
  });
  t.after(() => kernel.stop());
  return {
    dir,
    networkId,
    sender,
    receiver,
    targetCard,
    targetCards,
    targetByMember: new Map(
      targetCards.map(card => [card.identity.member, card] as const),
    ),
    outbox,
    mailbox,
    sentEvents,
    emitter: kernel.ctx.services.emitter,
    sendCalls,
    setMounted(value: boolean) {
      mounted = value;
    },
  };
}

test('redrive settles signed rejection or dispatcher failure as terminal failed', async t => {
  for (const stage of ['rejected', 'failed'] as const) {
    const harness = await emitterHarness(t, { stage });
    const envelope = envelopeFor(harness.sender, harness.networkId, harness.targetCard.nodeId);
    const sourcePath = join(harness.dir, `evidence-${stage}.txt`);
    writeFileSync(sourcePath, `evidence for ${stage}`);
    const hash = blake3(Buffer.from(`evidence for ${stage}`));
    const item = harness.outbox.enqueue({
      envelope,
      explicitTargets: [harness.targetCard.nodeId],
      pendingNodeIds: [harness.targetCard.nodeId],
      filePaths: { [hash]: sourcePath },
    });
    const service = new OutboxRedriveService({
      outbox: harness.outbox,
      deliver: (candidate, options) => {
        assert.ok(harness.emitter.redriveOutboxItem, 'emitter owns redrive delivery');
        return harness.emitter.redriveOutboxItem(candidate, options);
      },
    });
    const result = await service.flush({ id: item.id });
    const failed = harness.outbox.get(item.id)!;
    assert.equal(failed.status, 'failed');
    assert.equal(failed.failedReceipts[harness.targetCard.nodeId]?.stage, stage);
    assert.match(failed.lastError ?? '', new RegExp(stage));
    assert.ok(existsSync(failed.stagedFiles[hash]), 'staged evidence is preserved');
    assert.equal(result.stillQueued, 0);
    await service.stop();
  }

  const harness = await emitterHarness(t);
  const envelope = envelopeFor(harness.sender, harness.networkId, harness.targetCard.nodeId);
  const sourcePath = join(harness.dir, 'missing-evidence.txt');
  writeFileSync(sourcePath, 'will be removed');
  const hash = blake3(Buffer.from('will be removed'));
  const item = harness.outbox.enqueue({
    envelope,
    explicitTargets: [harness.targetCard.nodeId],
    pendingNodeIds: [harness.targetCard.nodeId],
    filePaths: { [hash]: sourcePath },
  });
  rmSync(item.stagedFiles[hash]!);
  const service = new OutboxRedriveService({
    outbox: harness.outbox,
    deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options),
  });
  await service.flush({ id: item.id });
  assert.equal(harness.outbox.get(item.id)?.status, 'failed');
  assert.match(harness.outbox.get(item.id)?.lastError ?? '', /Missing staged attachment/);
  await service.stop();
});

test('initial emit keeps a gate rejection out of delivered receipts', async t => {
  let dizOnline = false;
  let rejectTarget = '';
  const harness = await emitterHarness(t, {
    extraMembers: ['aya'],
    plan: target => {
      if (target === rejectTarget) {
        return { stage: 'rejected', detail: 'gate closed' };
      }
      return dizOnline ? {} : { error: new PeerUnreachableError(target) };
    },
  });
  const diz = harness.targetByMember.get('diz')!.nodeId;
  const aya = harness.targetByMember.get('aya')!.nodeId;
  rejectTarget = aya;
  const emitted = await harness.emitter.emit({
    networkId: harness.networkId,
    routing: { mode: 'multicast', targets: ['member:diz', 'member:aya'] },
    kind: 'task',
    subject: 'mixed multicast',
    body: 'one offline target and one rejecting target',
    payload: {},
  });
  assert.ok(emitted.receipts.some(receipt => receipt.stage === 'rejected'));
  assert.ok(
    emitted.receipts.some(
      receipt => receipt.stage === 'queued' && receipt.executionReceipt?.targetNodeId === diz,
    ),
  );
  const queued = harness.outbox.list({ status: 'queued' })[0];
  assert.deepEqual(queued.pendingNodeIds, [diz]);
  assert.deepEqual(queued.deliveredReceipts, {});
  assert.equal(queued.failedReceipts[aya]?.stage, 'rejected');

  dizOnline = true;
  const service = new OutboxRedriveService({
    outbox: harness.outbox,
    deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options),
  });
  await service.flush({ id: queued.id, force: true });
  const settled = harness.outbox.get(queued.id)!;
  assert.equal(settled.status, 'failed', 'a failed target is never reported as delivered');
  assert.equal(settled.deliveredReceipts[diz]?.stage, 'dispatched');
  assert.equal(settled.failedReceipts[aya]?.stage, 'rejected');
  assert.deepEqual(settled.pendingNodeIds, []);
  assert.equal(
    harness.sendCalls.filter(call => call.targetNodeId === aya).length,
    1,
    'the rejected target is not retried',
  );
  await service.stop();
});

test('initial emit enqueues offline siblings before surfacing a deterministic protocol failure', async t => {
  let ayaTarget = '';
  let ayaOnline = false;
  let dizOnline = false;
  const harness = await emitterHarness(t, {
    extraMembers: ['aya'],
    plan: target => {
      if (target === ayaTarget && !ayaOnline) {
        throw new LocalDeliveryError('Invalid or mismatched delivery receipt');
      }
      return dizOnline ? {} : { error: new PeerUnreachableError(target) };
    },
  });
  const diz = harness.targetByMember.get('diz')!.nodeId;
  ayaTarget = harness.targetByMember.get('aya')!.nodeId;
  let failure: unknown;
  try {
    await harness.emitter.emit({
      networkId: harness.networkId,
      routing: { mode: 'multicast', targets: ['member:diz', 'member:aya'] },
      kind: 'task',
      subject: 'partial protocol failure',
      body: 'offline sibling must survive',
      payload: {},
    });
  } catch (error) {
    failure = error;
  }
  assert.ok(isLocalDeliveryError(failure), 'the deterministic error still surfaces');
  const queued = harness.outbox.list({ status: 'queued' })[0];
  assert.ok(queued, 'the offline sibling is durable despite the thrown protocol error');
  assert.deepEqual(queued.pendingNodeIds, [diz]);
  assert.deepEqual(queued.failedNodeIds, [ayaTarget]);
  const receipts = (failure as Error & { receipts?: DeliveryReceipt[] }).receipts ?? [];
  assert.ok(
    receipts.some(
      receipt =>
        receipt.stage === 'queued' &&
        receipt.executionReceipt?.targetNodeId === diz,
    ),
    'the queued sibling receipt is produced before the error is thrown',
  );
  assert.equal(
    harness.sendCalls.filter(call => call.targetNodeId === ayaTarget).length,
    1,
  );

  dizOnline = true;
  const service = new OutboxRedriveService({
    outbox: harness.outbox,
    deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options),
  });
  await service.flush({ id: queued.id, force: true });
  const settled = harness.outbox.get(queued.id)!;
  assert.equal(settled.deliveredReceipts[diz]?.stage, 'dispatched');
  assert.deepEqual(settled.pendingNodeIds, []);
  assert.deepEqual(settled.failedNodeIds, [ayaTarget]);
  assert.equal(
    harness.sendCalls.filter(call => call.targetNodeId === ayaTarget).length,
    1,
    'the terminal local failure is never retried by an ordinary flush',
  );

  // The explicit operator path restores only the unsigned local-failure target.
  const requeued = harness.outbox.requeue(queued.id)!;
  assert.equal(requeued.status, 'queued');
  assert.deepEqual(requeued.pendingNodeIds, [ayaTarget]);
  assert.deepEqual(requeued.failedNodeIds, []);
  ayaOnline = true;
  await service.flush({ id: queued.id, force: true });
  const recovered = harness.outbox.get(queued.id)!;
  assert.equal(recovered.status, 'delivered');
  assert.equal(recovered.deliveredReceipts[ayaTarget]?.stage, 'dispatched');
  assert.equal(
    harness.sendCalls.filter(call => call.targetNodeId === ayaTarget).length,
    2,
    'requeue is the only path that retries a local failure',
  );
  await service.stop();
});

test('uncommitted sends never emit emitter/sent or pollute the outbound thread ledger', async t => {
  // A signed terminal rejection is evidence, not a handoff: nothing entered the outbox, so
  // observers must not create a reverse route for it.
  const rejecting = await emitterHarness(t, { stage: 'rejected' });
  const rejected = await rejecting.emitter.emit({
    networkId: rejecting.networkId,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'task',
    subject: 'terminal rejection',
    body: 'the target refuses this message',
    payload: {},
  });
  assert.equal(rejected.receipts[0]?.stage, 'rejected');
  assert.deepEqual(rejecting.sentEvents, []);
  assert.deepEqual(rejecting.outbox.list({ status: 'all' }), []);
  assert.deepEqual(rejecting.mailbox.listThreads(), []);

  // A deterministic local/protocol failure never reaches the outbox either.
  const failing = await emitterHarness(t, {
    plan: () => {
      throw new LocalDeliveryError('Invalid or mismatched delivery receipt');
    },
  });
  let failure: unknown;
  try {
    await failing.emitter.emit({
      networkId: failing.networkId,
      routing: { mode: 'unicast', targets: ['member:diz'] },
      kind: 'task',
      subject: 'local failure',
      body: 'receipt verification failed',
      payload: {},
    });
  } catch (error) {
    failure = error;
  }
  assert.ok(isLocalDeliveryError(failure));
  assert.deepEqual(failing.sentEvents, []);
  assert.deepEqual(failing.outbox.list({ status: 'all' }), []);
  assert.deepEqual(failing.mailbox.listThreads(), []);

  // An explicit opt-out from offline queueing must not invent a durable obligation either.
  const optOut = await emitterHarness(t, {
    plan: target => ({ error: new PeerUnreachableError(target) }),
  });
  await assert.rejects(
    optOut.emitter.emit({
      networkId: optOut.networkId,
      routing: { mode: 'unicast', targets: ['member:diz'] },
      kind: 'task',
      subject: 'offline without queueing',
      body: 'queueIfOffline:false leaves no trace',
      payload: {},
      queueIfOffline: false,
    }),
    error => isPeerUnreachableError(error),
  );
  assert.deepEqual(optOut.sentEvents, []);
  assert.deepEqual(optOut.outbox.list({ status: 'all' }), []);
  assert.deepEqual(optOut.mailbox.listThreads(), []);
});

test('partial multicast emits emitter/sent only for the targets that actually committed', async t => {
  let ayaTarget = '';
  let dizOnline = false;
  const harness = await emitterHarness(t, {
    extraMembers: ['aya'],
    plan: target => {
      if (target === ayaTarget) {
        throw new LocalDeliveryError('Invalid or mismatched delivery receipt');
      }
      return dizOnline ? {} : { error: new PeerUnreachableError(target) };
    },
  });
  const diz = harness.targetByMember.get('diz')!.nodeId;
  ayaTarget = harness.targetByMember.get('aya')!.nodeId;
  let failure: unknown;
  try {
    await harness.emitter.emit({
      networkId: harness.networkId,
      routing: { mode: 'multicast', targets: ['member:diz', 'member:aya'] },
      kind: 'task',
      subject: 'partial handoff',
      body: 'one durable sibling and one deterministic failure',
      payload: {},
    });
  } catch (error) {
    failure = error;
  }
  assert.ok(isLocalDeliveryError(failure));
  assert.equal(harness.sentEvents.length, 1);
  assert.deepEqual(harness.sentEvents[0].targetNodeIds, [diz]);
  const records = harness.mailbox.listThreadRecords(harness.sentEvents[0].envelope.id);
  assert.equal(records.length, 1);
  assert.equal(records[0].direction, 'outbound');
  assert.deepEqual(records[0].peerNodeIds, [diz]);
  assert.equal(harness.outbox.list({ status: 'queued' }).length, 1);

  // A genuine live handoff still records its target exactly once.
  dizOnline = true;
  const delivered = await harness.emitter.emit({
    networkId: harness.networkId,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'task',
    subject: 'live handoff',
    body: 'dispatched receipt',
    payload: {},
  });
  assert.equal(delivered.receipts[0]?.stage, 'dispatched');
  assert.equal(harness.sentEvents.length, 2);
  assert.deepEqual(harness.sentEvents[1].targetNodeIds, [diz]);
  assert.equal(
    harness.mailbox.listThreadRecords(delivered.messageId)[0].peerNodeIds[0],
    diz,
  );
});

test('initial emit parks a timed-out target in doubt instead of ordinary offline', async t => {
  const harness = await emitterHarness(t, {
    attemptTimeoutMs: 25,
    plan: () => ({ delayMs: 500 }),
  });
  const target = harness.targetCard.nodeId;
  const emitted = await harness.emitter.emit({
    networkId: harness.networkId,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'task',
    subject: 'initial timeout',
    body: 'the request may have reached the target',
    payload: {},
  });
  assert.equal(emitted.receipts[0]?.stage, 'queued');
  const item = harness.outbox.list({ status: 'queued' })[0];
  assert.deepEqual(item.pendingNodeIds, [target]);
  assert.deepEqual(item.inDoubtNodeIds, [target]);
  assert.equal(harness.sendCalls.length, 1);

  const service = new OutboxRedriveService({
    outbox: harness.outbox,
    attemptTimeoutMs: 25,
    deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options),
  });
  await service.flush({ id: item.id });
  assert.equal(harness.sendCalls.length, 1, 'the ordinary timer leaves an in-doubt target parked');
  assert.equal(harness.outbox.get(item.id)?.inDoubtNodeIds.length, 1);
  await service.stop();
});

test('scoped redrive preserves in-doubt flags for untouched targets', async t => {
  const dir = directory(t);
  const outbox = new OutboxStore(join(dir, 'outbox.json'), join(dir, 'outbox-blobs'));
  const sender = generateNodeKeyPair();
  const aya = `ed25519:${'a'.repeat(64)}`;
  const bo = `ed25519:${'b'.repeat(64)}`;
  const item = outbox.enqueue({
    envelope: envelopeFor(sender, 'scope-doubt-net', aya),
    explicitTargets: [aya, bo],
    pendingNodeIds: [aya, bo],
    inDoubtNodeIds: [aya],
  });
  const calls: Array<readonly string[] | undefined> = [];
  const service = new OutboxRedriveService({
    outbox,
    deliver: async (candidate, options) => {
      calls.push(options?.targetNodeIds);
      return {
        deliveredReceipts: {
          [bo]: {
            messageId: candidate.envelope.id,
            networkId: candidate.networkId,
            fromNodeId: bo,
            stage: 'dispatched',
            updatedAt: new Date().toISOString(),
            signature: 'test-signature',
          },
        },
        pendingNodeIds: [],
        inDoubtNodeIds: [],
      };
    },
  });
  await service.flush({ id: item.id, forceMatchingTarget: true, targetNodeId: bo });
  assert.deepEqual(calls, [[bo]]);
  const current = outbox.get(item.id)!;
  assert.equal(current.status, 'queued');
  assert.deepEqual(current.pendingNodeIds, [aya]);
  assert.deepEqual(
    current.inDoubtNodeIds,
    [aya],
    'a scoped attempt may not clear an untouched target doubt flag',
  );
  await service.stop();
});

function threadedEnvelope(
  sender: ReturnType<typeof generateNodeKeyPair>,
  networkId: string,
  target: string,
  id: string,
  threadId: string,
): MessageEnvelope {
  return signEnvelope(
    {
      id,
      traceId: id,
      networkId,
      from: { nodeId: sender.nodeId, member: 'ben', nodeName: 'ben' },
      routing: { mode: 'unicast', targets: [target] },
      kind: 'collab.request.v1',
      priority: 'normal',
      subject: id,
      body: id,
      payload: {},
      attachments: [],
      semantic: { type: 'reply', threadId },
      createdAt: new Date().toISOString(),
    },
    sender.privateKeyPem,
  );
}

test('outbox redrive honours per-thread causal FIFO without blocking other threads', async t => {
  const dir = directory(t);
  const outbox = new OutboxStore(join(dir, 'outbox.json'), join(dir, 'outbox-blobs'));
  const sender = generateNodeKeyPair();
  const target = generateNodeKeyPair().nodeId;
  const queued = (id: string, threadId: string) =>
    outbox.enqueue({
      envelope: threadedEnvelope(sender, 'fifo-net', target, id, threadId),
      explicitTargets: [target],
      pendingNodeIds: [target],
    });
  // Fixed ids keep the oldest-first order deterministic when all enqueues share a millisecond.
  const evt2 = queued('evt-0002', 'thread-a');
  const evt3 = queued('evt-0003', 'thread-a');
  const evt4 = queued('evt-0004', 'thread-a');
  const other = queued('evt-0100', 'thread-b');
  // The earlier causal event is parked behind backoff and in-doubt; the later events on the
  // same thread are already due.
  outbox.recordAttempt(evt2.id, {
    nextAttemptAt: new Date(Date.now() + 60_000).toISOString(),
    inDoubtNodeIds: [target],
  });
  const deliveries: string[] = [];
  const service = new OutboxRedriveService({
    outbox,
    deliver: async (candidate, options) => {
      deliveries.push(candidate.id);
      const targets = options?.targetNodeIds ?? candidate.pendingNodeIds;
      return {
        deliveredReceipts: Object.fromEntries(
          targets.map(nodeId => [
            nodeId,
            {
              messageId: candidate.envelope.id,
              networkId: candidate.networkId,
              fromNodeId: nodeId,
              stage: 'dispatched' as const,
              updatedAt: new Date().toISOString(),
              signature: 'test-signature',
            },
          ]),
        ),
        pendingNodeIds: [],
        inDoubtNodeIds: [],
      };
    },
  });
  t.after(() => service.stop());

  const first = await service.flush();
  assert.deepEqual(
    deliveries,
    [other.id],
    'a later event on a blocked thread may never overtake its undelivered head',
  );
  assert.deepEqual(first.items.map(item => item.id), [evt2.id, evt3.id, evt4.id, other.id]);
  assert.equal(outbox.get(other.id)?.status, 'delivered');
  assert.equal(outbox.get(evt2.id)?.status, 'queued');
  assert.deepEqual(outbox.get(evt3.id)?.pendingNodeIds, [target]);
  assert.deepEqual(outbox.get(evt4.id)?.pendingNodeIds, [target]);

  // A scoped --id without --force still may not jump the thread's undelivered head.
  const scoped = await service.flush({ id: evt3.id });
  assert.equal(scoped.attempted, 0);
  assert.deepEqual(deliveries, [other.id]);

  // Once the head is eligible (peer pulse or explicit operator force), one pass drains the
  // whole thread in exact causal order.
  await service.flush({ force: true });
  assert.deepEqual(deliveries, [other.id, evt2.id, evt3.id, evt4.id]);
  for (const id of [evt2.id, evt3.id, evt4.id]) {
    assert.equal(outbox.get(id)?.status, 'delivered');
  }
});

interface SessionHarness {
  store: MailboxStore;
  leaseId: string;
  inboundId: string;
  emitCalls: EmitMessageInput[];
  runtime: DealerNodeRuntime;
}

/** A minimal session command surface: only the mailbox lease and emitter are exercised. */
function sessionHarness(
  t: TestContext,
  emit: (
    input: EmitMessageInput,
  ) => Promise<{ messageId: string; traceId: string; receipts: DeliveryReceipt[] }>,
): SessionHarness {
  const dir = directory(t);
  const store = new MailboxStore(join(dir, 'mailbox.json'));
  const peer = generateNodeKeyPair();
  const inbound = signEnvelope(
    {
      id: 'msg_inbound_session',
      traceId: 'msg_inbound_session',
      networkId: 'team',
      from: { nodeId: peer.nodeId, member: 'diz', nodeName: 'diz' },
      routing: { mode: 'unicast', targets: [] },
      kind: 'collab.request.v1',
      priority: 'normal',
      subject: 'inbound question',
      body: 'answer me',
      payload: {},
      attachments: [],
      semantic: { type: 'question', threadId: 'msg_inbound_session' },
      createdAt: new Date().toISOString(),
    },
    peer.privateKeyPem,
  );
  store.enqueue([inbound]);
  store.recordInboundEnvelope(inbound, 'default');
  const leaseId = store.check({ mailboxId: 'default' }).deliveryId!;
  const emitCalls: EmitMessageInput[] = [];
  const runtime = {
    mailbox: store,
    emitter: {
      emit: async (input: EmitMessageInput) => {
        emitCalls.push(input);
        return emit(input);
      },
    },
    start: async () => {},
  } as unknown as DealerNodeRuntime;
  return { store, leaseId, inboundId: inbound.id, emitCalls, runtime };
}

function receiptFor(overrides: Partial<DeliveryReceipt>): DeliveryReceipt {
  return {
    messageId: 'msg_outbound_session',
    networkId: 'team',
    fromNodeId: 'ed25519:' + 'c'.repeat(64),
    stage: 'dispatched',
    updatedAt: new Date().toISOString(),
    signature: 'test-signature',
    ...overrides,
  };
}

test('session --ack validates before sending and only burns the lease after commit', async t => {
  const argsFor = (group: 'ask' | 'reply' | 'settle', messageId: string, leaseId: string) =>
    group === 'ask'
      ? ['ask', '--reply-to', messageId, '--body', 'follow-up', '--ack', leaseId, '--json']
      : group === 'reply'
        ? ['reply', messageId, '--body', 'answer', '--ack', leaseId, '--json']
        : ['settle', messageId, '--outcome', 'succeeded', '--summary', 'done', '--ack', leaseId, '--json'];
  const run = async (runtime: DealerNodeRuntime, args: string[]) => {
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(args, {
      runtime,
      stdout: line => stdout.push(line),
      stderr: line => stderr.push(line),
    });
    return { code, stdout, stderr: stderr.join('\n') };
  };
  const inboundStatus = (store: MailboxStore, id: string) =>
    store.list({ includeAcked: true }).items.find(item => item.id === id)?.status;

  for (const group of ['ask', 'reply', 'settle'] as const) {
    // An unknown lease is rejected before the emitter is ever called.
    const invalid = sessionHarness(t, async () => {
      throw new Error('emit must not run for an unknown --ack lease');
    });
    const rejectedAck = await run(
      invalid.runtime,
      argsFor(group, invalid.inboundId, 'dlv_deadbeef0000'),
    );
    assert.equal(rejectedAck.code, 1);
    assert.match(rejectedAck.stderr, /Unknown or mismatched delivery lease/);
    assert.deepEqual(invalid.emitCalls, []);
    invalid.store.assertAckableLease(invalid.leaseId);

    // A throwing emit leaves the caller's inbound lease untouched.
    const throwing = sessionHarness(t, async () => {
      throw new LocalDeliveryError('send failed');
    });
    const thrown = await run(
      throwing.runtime,
      argsFor(group, throwing.inboundId, throwing.leaseId),
    );
    assert.equal(thrown.code, 1);
    assert.match(thrown.stderr, /send failed/);
    throwing.store.assertAckableLease(throwing.leaseId);
    assert.equal(inboundStatus(throwing.store, throwing.inboundId), 'leased');

    // A terminally rejected send is not a committed handoff: the lease stays leased.
    const rejecting = sessionHarness(t, async () => ({
      messageId: 'msg_outbound_session',
      traceId: 'trace_outbound_session',
      receipts: [receiptFor({ stage: 'rejected', detail: 'gate closed' })],
    }));
    const rejected = await run(
      rejecting.runtime,
      argsFor(group, rejecting.inboundId, rejecting.leaseId),
    );
    assert.equal(rejected.code, 0, rejected.stderr);
    assert.equal(rejecting.emitCalls.length, 1);
    rejecting.store.assertAckableLease(rejecting.leaseId);
    assert.equal(inboundStatus(rejecting.store, rejecting.inboundId), 'leased');

    // A live handoff or a durable outbox obligation commits the send and only then acks.
    const queued = sessionHarness(t, async () => ({
      messageId: 'msg_outbound_session',
      traceId: 'trace_outbound_session',
      receipts: [
        receiptFor({
          stage: 'queued',
          executionReceipt: { durable: true, executor: 'outbox' },
        }),
      ],
    }));
    const committed = await run(
      queued.runtime,
      argsFor(group, queued.inboundId, queued.leaseId),
    );
    assert.equal(committed.code, 0, committed.stderr);
    assert.equal(queued.store.ack(queued.leaseId).alreadyAcknowledged, true);
    assert.equal(inboundStatus(queued.store, queued.inboundId), 'acked');
  }
});

test('unmounted network keeps outbox obligations queued through remount', async t => {
  let online = false;
  const harness = await emitterHarness(t, {
    plan: target => (online ? {} : { error: new PeerUnreachableError(target) }),
  });
  const target = harness.targetCard.nodeId;
  harness.setMounted(false);
  const emitted = await harness.emitter.emit({
    networkId: harness.networkId,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'task',
    subject: 'unmounted',
    body: 'queued through a transient unmount',
    payload: {},
  });
  assert.equal(emitted.receipts[0]?.stage, 'queued');
  assert.equal(harness.sendCalls.length, 0);
  const item = harness.outbox.list({ status: 'queued' })[0];
  const service = new OutboxRedriveService({
    outbox: harness.outbox,
    deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options),
  });
  await service.flush({ id: item.id, force: true });
  let current = harness.outbox.get(item.id)!;
  assert.equal(current.status, 'queued', 'unmount is transient, not terminal failure');
  assert.match(current.lastError ?? '', /Network not mounted/);
  assert.equal(harness.sendCalls.length, 0);

  harness.setMounted(true);
  online = true;
  await service.flush({ id: item.id, force: true });
  current = harness.outbox.get(item.id)!;
  assert.equal(current.deliveredReceipts[target]?.stage, 'dispatched');
  assert.deepEqual(current.pendingNodeIds, []);
  await service.stop();
});

test('durable outbox stages declared attachments from attachmentPaths across redrive', async t => {
  let online = false;
  const harness = await emitterHarness(t, {
    plan: target => (online ? {} : { error: new PeerUnreachableError(target) }),
  });
  const target = harness.targetCard.nodeId;
  const bytes = Buffer.from('declared attachment bytes');
  const sourcePath = join(harness.dir, 'declared-attachment.txt');
  writeFileSync(sourcePath, bytes);
  const hash = blake3(bytes);
  const attachment = {
    name: 'declared.txt',
    mimeType: 'text/plain',
    sizeBytes: bytes.length,
    blake3Hash: hash,
    ticket: `local:${hash}`,
  };
  await assert.rejects(
    harness.emitter.emit({
      networkId: harness.networkId,
      routing: { mode: 'unicast', targets: ['member:diz'] },
      kind: 'task',
      subject: 'missing attachment source',
      body: 'must not queue an unservable envelope',
      payload: {},
      attachments: [attachment],
    }),
    /attachmentPaths/,
  );
  assert.equal(harness.outbox.list({ status: 'all' }).length, 0);

  const emitted = await harness.emitter.emit({
    networkId: harness.networkId,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'task',
    subject: 'declared attachment',
    body: 'restart-safe attachment',
    payload: {},
    attachments: [attachment],
    attachmentPaths: { [hash]: sourcePath },
  });
  assert.equal(emitted.receipts[0]?.stage, 'queued');
  const queued = harness.outbox.list({ status: 'queued' })[0];
  assert.ok(queued.stagedFiles[hash], 'the declared attachment is staged durably');
  assert.notEqual(queued.stagedFiles[hash], sourcePath);
  assert.ok(existsSync(queued.stagedFiles[hash]));
  // Simulate the source disappearing while the durable staged copy survives a restart.
  rmSync(sourcePath);

  online = true;
  const service = new OutboxRedriveService({
    outbox: harness.outbox,
    deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options),
  });
  await service.flush({ id: queued.id, force: true });
  const settled = harness.outbox.get(queued.id)!;
  assert.equal(settled.deliveredReceipts[target]?.stage, 'dispatched');
  assert.equal(settled.status, 'delivered');
  assert.equal(existsSync(queued.stagedFiles[hash]), false, 'staged evidence is removed on settlement');
  await service.stop();
});

test('concurrent idempotent emits create one obligation and send one envelope', async t => {
  const harness = await emitterHarness(t, {
    plan: target => ({ error: new PeerUnreachableError(target) }),
  });
  const input = {
    networkId: harness.networkId,
    routing: { mode: 'unicast' as const, targets: ['member:diz'] },
    kind: 'task',
    subject: 'concurrent idempotency',
    body: 'one obligation',
    payload: {},
    idempotencyKey: 'concurrent-key',
  };
  const [first, second] = await Promise.all([
    harness.emitter.emit(input),
    harness.emitter.emit(input),
  ]);
  assert.equal(first.messageId, second.messageId);
  assert.equal(harness.outbox.list({ status: 'all' }).length, 1);
  assert.equal(harness.sendCalls.length, 1, 'only the claim owner performs the initial send');
  assert.deepEqual(
    [...new Set(harness.sendCalls.map(call => call.envelope.id))],
    [first.messageId],
  );
});

test('multicast redrive records terminal failures per target and keeps retrying peers', async t => {
  let boOnline = false;
  let ayaTarget = '';
  let boTarget = '';
  const harness = await emitterHarness(t, {
    extraMembers: ['aya', 'bo'],
    plan: target => {
      if (target === ayaTarget) {
        return { stage: 'rejected', detail: 'gate closed' };
      }
      if (target === boTarget && !boOnline) {
        return { error: new PeerUnreachableError(target) };
      }
      return {};
    },
  });
  const diz = harness.targetByMember.get('diz')!.nodeId;
  ayaTarget = harness.targetByMember.get('aya')!.nodeId;
  boTarget = harness.targetByMember.get('bo')!.nodeId;
  const envelope = envelopeFor(harness.sender, harness.networkId, diz);
  const sourcePath = join(harness.dir, 'multicast-evidence.txt');
  writeFileSync(sourcePath, 'multicast evidence');
  const hash = blake3(Buffer.from('multicast evidence'));
  const item = harness.outbox.enqueue({
    envelope,
    explicitTargets: [diz, ayaTarget, boTarget],
    pendingNodeIds: [diz, ayaTarget, boTarget],
    filePaths: { [hash]: sourcePath },
  });
  const service = new OutboxRedriveService({
    outbox: harness.outbox,
    deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options),
  });
  await service.flush({ id: item.id, force: true });
  let current = harness.outbox.get(item.id)!;
  assert.equal(current.status, 'queued', 'the offline target keeps the obligation live');
  assert.deepEqual(current.pendingNodeIds, [boTarget]);
  assert.equal(current.deliveredReceipts[diz]?.stage, 'dispatched');
  assert.equal(current.failedReceipts[ayaTarget]?.stage, 'rejected');
  assert.ok(existsSync(current.stagedFiles[hash]), 'staged evidence survives a partial failure');
  assert.equal(harness.sendCalls.filter(call => call.targetNodeId === ayaTarget).length, 1);

  boOnline = true;
  await service.flush({ id: item.id, force: true });
  current = harness.outbox.get(item.id)!;
  assert.equal(current.status, 'failed', 'a terminal target decision is never hidden by delivery');
  assert.deepEqual(current.pendingNodeIds, []);
  assert.equal(current.deliveredReceipts[boTarget]?.stage, 'dispatched');
  assert.ok(existsSync(current.stagedFiles[hash]), 'staged evidence is retained for the failure');
  assert.equal(
    harness.sendCalls.filter(call => call.targetNodeId === ayaTarget).length,
    1,
    'later flushes never resend a terminal target',
  );
  assert.throws(
    () => harness.outbox.requeue(item.id),
    /no locally failed targets/,
    'a signed terminal rejection is never operator-requeued as a local failure',
  );
  await service.stop();
});

test('operator requeue restores a global local failure to routing', async t => {
  const dir = directory(t);
  const outbox = new OutboxStore(join(dir, 'outbox.json'), join(dir, 'outbox-blobs'));
  const sender = generateNodeKeyPair();
  const target = generateNodeKeyPair().nodeId;
  const item = outbox.enqueue({
    envelope: envelopeFor(sender, NETWORK, target),
    explicitTargets: [target],
    pendingNodeIds: [target],
  });
  outbox.markFailed(item.id, 'Missing staged attachment evidence');
  const failed = outbox.get(item.id)!;
  assert.equal(failed.status, 'failed');
  assert.deepEqual(
    failed.pendingNodeIds,
    [target],
    'an item-level failure retains the targets it never attempted',
  );
  const requeued = outbox.requeue(item.id)!;
  assert.equal(requeued.status, 'queued');
  assert.deepEqual(requeued.pendingNodeIds, [target], 'requeue restores the pending target');
  assert.equal(requeued.lastError, undefined);
});

test('requeue preserves in-doubt fences through a global local failure', async t => {
  const dir = directory(t);
  const outbox = new OutboxStore(join(dir, 'outbox.json'), join(dir, 'outbox-blobs'));
  const sender = generateNodeKeyPair();
  const target = generateNodeKeyPair().nodeId;
  const item = outbox.enqueue({
    envelope: envelopeFor(sender, NETWORK, target),
    explicitTargets: [target],
    pendingNodeIds: [target],
    inDoubtNodeIds: [target],
  });
  outbox.markFailed(item.id, 'Missing staged attachment evidence');
  const requeued = outbox.requeue(item.id)!;
  assert.equal(requeued.status, 'queued');
  assert.deepEqual(requeued.pendingNodeIds, [target]);
  assert.deepEqual(
    requeued.inDoubtNodeIds,
    [target],
    'requeue never clears a target whose execution outcome is unknown',
  );

  const service = new OutboxRedriveService({
    outbox,
    deliver: async () => {
      throw new Error('an in-doubt target must not be delivered without an explicit force');
    },
  });
  const parked = await service.flush({ id: item.id });
  assert.equal(parked.attempted, 0);
  assert.equal(outbox.get(item.id)?.status, 'queued');
  await service.stop();
});

test('requeue after a global local failure resumes a partially delivered obligation', async t => {
  const harness = await emitterHarness(t, { extraMembers: ['bo'] });
  const diz = harness.targetCard.nodeId;
  const bo = harness.targetByMember.get('bo')!.nodeId;
  const envelope = envelopeFor(harness.sender, harness.networkId, diz);
  const body = Buffer.from('partial delivery evidence');
  const hash = blake3(body);
  const sourcePath = join(harness.dir, 'partial-evidence.txt');
  writeFileSync(sourcePath, body);
  const item = harness.outbox.enqueue({
    envelope,
    explicitTargets: [diz, bo],
    pendingNodeIds: [diz, bo],
    filePaths: { [hash]: sourcePath },
  });
  const service = new OutboxRedriveService({
    outbox: harness.outbox,
    deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options),
  });

  // Deliver only diz; bo stays an untouched pending target.
  await service.flush({
    id: item.id,
    force: true,
    forceMatchingTarget: true,
    targetNodeId: diz,
  });
  let current = harness.outbox.get(item.id)!;
  assert.equal(current.status, 'queued');
  assert.deepEqual(current.pendingNodeIds, [bo]);
  assert.equal(current.deliveredReceipts[diz]?.stage, 'dispatched');

  // Missing staged evidence aborts the attempt with no per-target decision at all.
  rmSync(current.stagedFiles[hash]);
  await service.flush({ id: item.id, force: true });
  current = harness.outbox.get(item.id)!;
  assert.equal(current.status, 'failed');
  assert.deepEqual(
    current.pendingNodeIds,
    [bo],
    'the never-attempted target remains an obligation after the item-level failure',
  );
  assert.ok(current.stagedFiles[hash], 'the staged path is retained for recovery');
  assert.equal(harness.sendCalls.filter(call => call.targetNodeId === bo).length, 0);

  writeFileSync(current.stagedFiles[hash], body);
  const requeued = harness.outbox.requeue(item.id)!;
  assert.equal(requeued.status, 'queued');
  assert.deepEqual(requeued.pendingNodeIds, [bo]);
  assert.ok(existsSync(requeued.stagedFiles[hash]), 'requeue must not delete staged evidence');

  await service.flush({ id: item.id, force: true });
  current = harness.outbox.get(item.id)!;
  assert.equal(current.status, 'delivered');
  assert.deepEqual(current.pendingNodeIds, []);
  assert.equal(harness.sendCalls.filter(call => call.targetNodeId === bo).length, 1);
  assert.equal(
    harness.sendCalls.filter(call => call.targetNodeId === diz).length,
    1,
    'a requeue never resends a target with a confirmed receipt',
  );
  assert.equal(Object.keys(current.stagedFiles).length, 0);
  await service.stop();
});

test('requeue recovers mixed unsigned failures while keeping signed rejections terminal', async t => {
  let ayaTarget = '';
  let boTarget = '';
  let cyTarget = '';
  let cyOnline = false;
  const harness = await emitterHarness(t, {
    extraMembers: ['aya', 'bo', 'cy'],
    plan: (target, callIndex) => {
      if (target === ayaTarget) {
        return { stage: 'rejected', detail: 'gate closed' };
      }
      if (target === boTarget && callIndex === 0) {
        return { error: new LocalDeliveryError('local integrity failure') };
      }
      if (target === cyTarget && !cyOnline) {
        return { error: new PeerUnreachableError(target) };
      }
      return {};
    },
  });
  const diz = harness.targetCard.nodeId;
  ayaTarget = harness.targetByMember.get('aya')!.nodeId;
  boTarget = harness.targetByMember.get('bo')!.nodeId;
  cyTarget = harness.targetByMember.get('cy')!.nodeId;
  const envelope = envelopeFor(harness.sender, harness.networkId, diz);
  const body = Buffer.from('mixed failure evidence');
  const hash = blake3(body);
  const sourcePath = join(harness.dir, 'mixed-evidence.txt');
  writeFileSync(sourcePath, body);
  const item = harness.outbox.enqueue({
    envelope,
    explicitTargets: [diz, ayaTarget, boTarget, cyTarget],
    pendingNodeIds: [diz, ayaTarget, boTarget, cyTarget],
    filePaths: { [hash]: sourcePath },
  });
  const service = new OutboxRedriveService({
    outbox: harness.outbox,
    deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options),
  });

  await service.flush({ id: item.id, force: true });
  let current = harness.outbox.get(item.id)!;
  assert.equal(current.status, 'queued');
  assert.deepEqual(current.pendingNodeIds, [cyTarget]);
  assert.equal(current.failedReceipts[ayaTarget]?.stage, 'rejected');
  assert.deepEqual(current.failedNodeIds.slice().sort(), [ayaTarget, boTarget].sort());

  // The missing evidence is an item-level failure; cy must stay a recoverable obligation.
  rmSync(current.stagedFiles[hash]);
  await service.flush({ id: item.id, force: true });
  current = harness.outbox.get(item.id)!;
  assert.equal(current.status, 'failed');
  assert.deepEqual(current.pendingNodeIds, [cyTarget]);

  writeFileSync(current.stagedFiles[hash], body);
  cyOnline = true;
  const requeued = harness.outbox.requeue(item.id)!;
  assert.equal(requeued.status, 'queued');
  assert.deepEqual(requeued.failedNodeIds, [ayaTarget], 'only the signed rejection stays terminal');
  assert.deepEqual(
    requeued.pendingNodeIds.slice().sort(),
    [boTarget, cyTarget].sort(),
    'the unsigned failure and the retained target resume routing',
  );

  await service.flush({ id: item.id, force: true });
  current = harness.outbox.get(item.id)!;
  assert.equal(current.status, 'failed', 'the signed rejection remains terminal evidence');
  assert.deepEqual(current.pendingNodeIds, []);
  assert.equal(current.deliveredReceipts[boTarget]?.stage, 'dispatched');
  assert.equal(current.deliveredReceipts[cyTarget]?.stage, 'dispatched');
  assert.equal(harness.sendCalls.filter(call => call.targetNodeId === ayaTarget).length, 1);
  assert.equal(harness.sendCalls.filter(call => call.targetNodeId === boTarget).length, 2);
  assert.equal(harness.sendCalls.filter(call => call.targetNodeId === cyTarget).length, 2);
  assert.ok(
    existsSync(current.stagedFiles[hash]),
    'staged evidence survives while a terminal rejection remains',
  );
  await service.stop();
});

test('redrive preserves completed receipts and parks timed-out sends in doubt', async t => {
  let ayaOnline = false;
  let ayaTarget = '';
  const harness = await emitterHarness(t, {
    extraMembers: ['aya'],
    plan: target => {
      if (target === ayaTarget) {
        return ayaOnline ? {} : { delayMs: 200 };
      }
      return {};
    },
  });
  const diz = harness.targetByMember.get('diz')!.nodeId;
  ayaTarget = harness.targetByMember.get('aya')!.nodeId;
  const item = harness.outbox.enqueue({
    envelope: envelopeFor(harness.sender, harness.networkId, diz),
    explicitTargets: [diz, ayaTarget],
    pendingNodeIds: [diz, ayaTarget],
  });
  const pulse = new EventEmitter();
  const service = new OutboxRedriveService({
    outbox: harness.outbox,
    intervalMs: 60_000,
    attemptTimeoutMs: 25,
    events: {
      on: (event, listener) => {
        pulse.on(event, listener);
        return { dispose: () => pulse.off(event, listener) };
      },
    },
    deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options),
  });
  service.start();
  t.after(() => service.stop());
  await service.flush({ id: item.id });
  let current = harness.outbox.get(item.id)!;
  assert.equal(current.deliveredReceipts[diz]?.stage, 'dispatched', 'the completed receipt is durable');
  assert.deepEqual(current.pendingNodeIds, [ayaTarget]);
  assert.deepEqual(current.inDoubtNodeIds, [ayaTarget], 'the timed-out send is in doubt');
  assert.equal(harness.sendCalls.filter(call => call.targetNodeId === diz).length, 1);

  await new Promise(resolveTimeout => setTimeout(resolveTimeout, 80));
  await service.flush({ id: item.id });
  assert.equal(
    harness.sendCalls.filter(call => call.targetNodeId === ayaTarget).length,
    1,
    'the ordinary timer never resends an in-doubt target',
  );
  assert.equal(harness.sendCalls.filter(call => call.targetNodeId === diz).length, 1);

  ayaOnline = true;
  pulse.emit(
    'node/peer-updated',
    harness.targetByMember.get('aya'),
    { direct: true },
  );
  await until(
    () => harness.outbox.get(item.id)?.status === 'delivered',
    'a direct peer pulse resolves and delivers the in-doubt target',
  );
  current = harness.outbox.get(item.id)!;
  assert.equal(current.deliveredReceipts[ayaTarget]?.stage, 'dispatched');
  assert.deepEqual(current.inDoubtNodeIds, []);
  assert.equal(harness.sendCalls.filter(call => call.targetNodeId === diz).length, 1);
});

test('queueIfOffline opt-out leaves no durable copy and idempotent re-emits converge', async t => {
  const harness = await emitterHarness(t, {
    sendError: new PeerUnreachableError(`ed25519:${'a'.repeat(64)}`),
  });
  const input = {
    networkId: harness.networkId,
    routing: { mode: 'unicast' as const, targets: ['member:diz'] },
    kind: 'task',
    subject: 'opt-out',
    body: 'body',
    payload: {},
  };
  await assert.rejects(
    harness.emitter.emit({ ...input, queueIfOffline: false }),
    /Peer unreachable/,
  );
  assert.equal(harness.sendCalls.length, 1);
  assert.equal(harness.sendCalls[0].opts?.spoolIfOffline, undefined);
  assert.equal(harness.outbox.list({ status: 'all' }).length, 0);

  const first = await harness.emitter.emit({ ...input, idempotencyKey: 'poll-item-1' });
  assert.equal(first.receipts[0].stage, 'queued');
  const second = await harness.emitter.emit({ ...input, idempotencyKey: 'poll-item-1' });
  assert.equal(second.messageId, first.messageId);
  assert.equal(harness.outbox.list({ status: 'all' }).length, 1);
  assert.equal(harness.sendCalls.length, 2);
  assert.equal(harness.sendCalls[1].opts?.spoolIfOffline, true);
  assert.equal(harness.sendCalls[1].opts?.spoolKey, first.messageId);
});

test('peer pulse redrive is scoped to the direct target and gossip cannot reset backoff', async t => {
  const dir = directory(t);
  const outbox = new OutboxStore(join(dir, 'outbox.json'), join(dir, 'outbox-blobs'));
  const sender = generateNodeKeyPair();
  const targetA = `ed25519:${'a'.repeat(64)}`;
  const targetB = `ed25519:${'b'.repeat(64)}`;
  const itemA = outbox.enqueue({
    envelope: envelopeFor(sender, 'scope-net', targetA),
    explicitTargets: [targetA],
    pendingNodeIds: [targetA],
  });
  const itemB = outbox.enqueue({
    envelope: envelopeFor(sender, 'scope-net', targetB),
    explicitTargets: [targetB],
    pendingNodeIds: [targetB],
  });
  const future = new Date(Date.now() + 60_000).toISOString();
  const scheduledB = outbox.recordAttempt(itemB.id, { nextAttemptAt: future })!;
  const calls: string[] = [];
  const pulse = new EventEmitter();
  const service = new OutboxRedriveService({
    outbox,
    intervalMs: 60_000,
    events: {
      on: (event, listener) => {
        pulse.on(event, listener);
        return { dispose: () => pulse.off(event, listener) };
      },
    },
    deliver: async item => {
      calls.push(item.id);
      const target = item.pendingNodeIds[0];
      return {
        deliveredReceipts: {
          [target]: {
            messageId: item.envelope.id,
            networkId: item.networkId,
            fromNodeId: target,
            stage: 'dispatched',
            updatedAt: new Date().toISOString(),
            signature: 'test-signature',
          },
        },
        pendingNodeIds: [],
      };
    },
  });
  service.start();
  t.after(() => service.stop());

  pulse.emit(
    'node/peer-updated',
    { networkId: 'scope-net', nodeId: targetA } as NodeCard,
    { direct: true },
  );
  await until(
    () => outbox.get(itemA.id)?.status === 'delivered',
    'the direct target is redriven immediately',
  );
  assert.deepEqual(calls, [itemA.id]);
  assert.equal(outbox.get(itemB.id)?.status, 'queued');
  assert.equal(outbox.get(itemB.id)?.nextAttemptAt, scheduledB.nextAttemptAt);

  pulse.emit(
    'node/peer-updated',
    { networkId: 'scope-net', nodeId: targetB } as NodeCard,
    { direct: false },
  );
  await new Promise(resolve => setTimeout(resolve, 150));
  assert.deepEqual(calls, [itemA.id], 'identity gossip never advances a retry clock');
  assert.equal(outbox.get(itemB.id)?.status, 'queued');
});

test('self card revisions advance across a restart and peers accept them', async t => {
  const root = directory(t);
  const runtimes: DealerNodeRuntime[] = [];
  t.after(async () => {
    for (const runtime of [...runtimes].reverse()) {
      await runtime.stop().catch(() => {});
    }
  });
  const create = async (member: string, port?: number) => {
    const runtime = runtimeFor(root, member, {
      discovery: 'mesh',
      ...(port !== undefined ? { port } : {}),
    });
    runtimes.push(runtime);
    await runtime.start();
    return runtime;
  };

  const ben = await create('ben');
  let diz = await create('diz');
  await ben.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  await diz.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  await until(
    () =>
      ben.node.listPeers({ networkId: NETWORK }).some(card => card.identity.member === 'diz') &&
      diz.node.listPeers({ networkId: NETWORK }).some(card => card.identity.member === 'ben'),
    'initial peer discovery',
  );
  const dizNodeId = diz.node.getSelfCard(NETWORK).nodeId;
  const before = ben.node
    .listPeers({ networkId: NETWORK })
    .find(card => card.nodeId === dizNodeId)!.revision;
  let directUpdates = 0;
  ben.kernel.ctx.on('node/peer-updated', (card, provenance) => {
    if (card.nodeId === dizNodeId && provenance?.direct !== false) {
      directUpdates += 1;
    }
  });

  await diz.stop();
  diz = await create('diz');
  await diz.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  await until(
    () =>
      (ben.node
        .listPeers({ networkId: NETWORK })
        .find(card => card.nodeId === dizNodeId)?.revision ?? 0) > before,
    'restarted self card is strictly newer and accepted',
  );
  assert.ok(directUpdates > 0, 'node/peer-updated fired for the restart');
  const persisted = JSON.parse(readFileSync(join(diz.stateDir, 'peers.json'), 'utf8')) as {
    selfRevisions?: Record<string, number>;
  };
  assert.ok(
    (persisted.selfRevisions?.[NETWORK] ?? 0) >=
      diz.node.getSelfCard(NETWORK).revision,
    'the last self revision is durable',
  );
});

test('poll watermark advances on durable outbox receipts independent of settlement mode', async t => {
  let emits = 0;
  const receipt = signReceipt(
    {
      messageId: 'poll-message',
      networkId: 'poll-net',
      fromNodeId: `ed25519:${'c'.repeat(64)}`,
      stage: 'queued',
      executionReceipt: { executor: 'outbox', durable: true, handoff: 'queued' },
      updatedAt: new Date().toISOString(),
    },
    generateNodeKeyPair().privateKeyPem,
  );
  assert.equal(isConfirmedHandoffReceipt(receipt), true);
  const emitter = {
    async emit() {
      emits += 1;
      return { messageId: 'poll-message', traceId: 'trace', receipts: [receipt] };
    },
    registerSource() {
      throw new Error('unused');
    },
  } as unknown as EmitterService;
  let polls = 0;
  const source = createPollWatermarkSourcePlugin<number, number>({
    initialCursor: -1,
    intervalMs: 1_000_000,
    poll: async () => {
      polls += 1;
      return polls === 1 ? { items: [1], cursor: 1 } : { items: [], cursor: 1 };
    },
    toInput: item => ({
      networkId: 'poll-net',
      routing: { mode: 'unicast', targets: ['member:x'] },
      kind: 'task',
      subject: 'poll',
      body: 'poll body',
      payload: {},
      idempotencyKey: `poll-item-${item}`,
    }),
  });
  await source.start(emitter);
  t.after(() => source.stop());
  await source.pollOnce();
  assert.equal(source.getCursor(), 1, 'durable outbox handoff advanced the cursor');
  await source.pollOnce();
  assert.equal(emits, 1, 'a settled item is not re-emitted');
});

test('unresolved target classification is typed rather than message-text based', () => {
  assert.equal(
    isUnresolvedTargetError(
      Object.assign(new Error('selector resolution wording changed'), {
        code: 'unresolved_target',
      }),
    ),
    true,
  );
  assert.equal(
    isUnresolvedTargetError(new Error('Unresolved explicit target: member:future')),
    false,
  );
});

test('CLI next-step summary uses the shared durable outbox receipt predicate', () => {
  const receipt = (durable: boolean): DeliveryReceipt => ({
    messageId: 'msg_summary',
    networkId: 'summary-net',
    fromNodeId: 'ed25519:' + 'c'.repeat(64),
    stage: 'queued',
    executionReceipt: { executor: 'outbox', durable },
    updatedAt: new Date().toISOString(),
    signature: 'test-signature',
  });
  assert.equal(
    summarizeNextStep('send', [receipt(false)]),
    undefined,
    'a non-durable outbox-stage receipt is not a completed durable handoff',
  );
  assert.match(
    summarizeNextStep('send', [receipt(true)]) ?? '',
    /durably queued in outbox/,
  );
});

test('an unresolved selector queues durably while its network is unmounted', async t => {
  const root = directory(t);
  const runtime = runtimeFor(root, 'ben');
  t.after(async () => {
    await runtime.stop().catch(() => {});
  });
  await runtime.start();
  await runtime.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  await runtime.unmountNetwork(NETWORK);
  const emitted = await runtime.emitter.emit({
    networkId: NETWORK,
    routing: { mode: 'unicast', targets: ['member:not-yet-known'] },
    kind: 'collab.request.v1',
    subject: 'queued while unmounted',
    body: 'identity resolution is directory-owned',
    payload: {},
  });
  assert.equal(emitted.receipts[0]?.stage, 'queued');
  const [item] = runtime.listOutbox({ status: 'queued' });
  assert.deepEqual(item.pendingNodeIds, []);
  assert.deepEqual(item.explicitTargets, ['member:not-yet-known']);
});

test('mailbox and outbox reclaim the shared lock owner after a dead incarnation', t => {
  const dir = directory(t);
  const deadOwner = {
    pid: 2_147_483_646,
    instanceId: 'dead-incarnation',
    host: hostname(),
  };
  const mailboxPath = join(dir, 'mailbox.json');
  mkdirSync(`${mailboxPath}.lock`, { recursive: true });
  writeFileSync(join(`${mailboxPath}.lock`, 'owner.json'), JSON.stringify(deadOwner));
  const mailbox = new MailboxStore(mailboxPath);
  assert.equal(mailbox.upsertWakeTarget('default').mailboxId, 'default');
  assert.equal(existsSync(`${mailboxPath}.lock`), false, 'the dead mailbox lock is reclaimed');

  const outboxPath = join(dir, 'outbox.json');
  mkdirSync(`${outboxPath}.lock`, { recursive: true });
  writeFileSync(join(`${outboxPath}.lock`, 'owner.json'), JSON.stringify(deadOwner));
  const outbox = new OutboxStore(outboxPath, join(dir, 'outbox-blobs'));
  const sender = generateNodeKeyPair();
  const target = generateNodeKeyPair().nodeId;
  const item = outbox.enqueue({
    envelope: envelopeFor(sender, 'lock-net', target),
    explicitTargets: [target],
    pendingNodeIds: [target],
  });
  assert.equal(item.status, 'queued');
  assert.equal(existsSync(`${outboxPath}.lock`), false, 'the dead outbox lock is reclaimed');
});

test('outbox enqueue is durable even when the writer is killed immediately after returning', t => {
  const dir = directory(t);
  const outboxPath = join(dir, 'outbox.json');
  const blobsPath = join(dir, 'outbox-blobs');
  const senderNodeId = `ed25519:${'a'.repeat(64)}`;
  const targetNodeId = `ed25519:${'b'.repeat(64)}`;
  const envelope: MessageEnvelope = {
    id: 'crash-item',
    traceId: 'crash-trace',
    networkId: 'crash-net',
    from: { nodeId: senderNodeId, member: 'ben', nodeName: 'ben' },
    routing: { mode: 'unicast', targets: [targetNodeId] },
    kind: 'task',
    priority: 'normal',
    subject: 'crash',
    body: 'crash body',
    payload: {},
    attachments: [],
    createdAt: new Date().toISOString(),
    signature: 'test-signature',
  };
  const moduleUrl = new URL('../packages/mailbox/outbox.ts', import.meta.url).href;
  const code = [
    `import { OutboxStore } from ${JSON.stringify(moduleUrl)};`,
    `const store = new OutboxStore(${JSON.stringify(outboxPath)}, ${JSON.stringify(blobsPath)});`,
    `store.enqueue(${JSON.stringify({
      envelope,
      explicitTargets: [targetNodeId],
      pendingNodeIds: [targetNodeId],
    })});`,
    `process.kill(process.pid, 'SIGKILL');`,
  ].join('\n');
  const child = spawnSync(
    process.execPath,
    ['--experimental-strip-types', '--input-type=module', '-e', code],
    { encoding: 'utf8', timeout: 30_000 },
  );
  assert.equal(child.signal, 'SIGKILL', child.stderr);
  const store = new OutboxStore(outboxPath, blobsPath);
  assert.deepEqual(
    store.list({ status: 'queued' }).map(item => item.id),
    [envelope.id],
  );
  // The durable state is the renamed final file; no partial temp file may survive.
  assert.deepEqual(
    readdirSync(dirname(outboxPath)).filter(name => name.endsWith('.tmp')),
    [],
  );
});

function framed(value: unknown): Buffer {
  const body = Buffer.from(JSON.stringify(value));
  const size = Buffer.alloc(4);
  size.writeUInt32BE(body.length);
  return Buffer.concat([size, body]);
}

/** Effective durable spool state: compacted base snapshot plus the append-only journal. */
function readRelaySpool(spoolFile: string): {
  schema: string;
  networkId: string;
  entries: Array<Record<string, any>>;
  blobs: Record<string, { size: number; expiresAt: string }>;
} {
  const entries = new Map<string, Record<string, any>>();
  const blobs: Record<string, { size: number; expiresAt: string }> = {};
  let schema = 'the-dealer-relay-spool/v1';
  let networkId = '';
  const keyFor = (entry: Record<string, any>) => `${entry.targetNodeId}\n${entry.id}`;
  if (existsSync(spoolFile)) {
    const base = JSON.parse(readFileSync(spoolFile, 'utf8')) as {
      schema?: string;
      networkId?: string;
      entries?: Array<Record<string, any>>;
      blobs?: Record<string, { size: number; expiresAt: string }>;
    };
    schema = base.schema ?? schema;
    networkId = base.networkId ?? networkId;
    for (const entry of base.entries ?? []) {
      entries.set(keyFor(entry), entry);
    }
    Object.assign(blobs, base.blobs ?? {});
  }
  const journalFile = `${spoolFile}.journal`;
  if (existsSync(journalFile)) {
    for (const line of readFileSync(journalFile, 'utf8').split('\n')) {
      if (!line) continue;
      const record = JSON.parse(line) as Record<string, any>;
      if (record.op === 'upsert') {
        entries.set(keyFor(record.entry), record.entry);
      } else if (record.op === 'remove' || record.op === 'drain') {
        entries.delete(`${record.targetNodeId}\n${record.id}`);
        if (record.op === 'drain' && record.blobs) {
          Object.assign(blobs, record.blobs);
        }
      } else if (record.op === 'cache') {
        Object.assign(blobs, record.blobs ?? {});
      } else if (record.op === 'uncache') {
        for (const hash of record.blobs as string[]) {
          delete blobs[hash];
        }
      }
    }
  }
  return { schema, networkId, entries: [...entries.values()], blobs };
}

/** True when the server sent any framed response; false when it closed without one. */
function tcpReplyReceived(port: number, body: unknown, timeoutMs = 2_000): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const socket: Socket = createConnection({ host: '127.0.0.1', port });
    let received = false;
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error('raw TCP probe timed out'));
    }, timeoutMs);
    const finish = (value: boolean) => {
      clearTimeout(timer);
      socket.destroy();
      resolve(value);
    };
    socket.on('connect', () => socket.write(framed(body)));
    socket.on('data', () => {
      received = true;
      finish(true);
    });
    socket.on('close', () => finish(received));
    socket.on('error', () => finish(received));
  });
}

function tcpRequest(port: number, body: unknown, timeoutMs = 10_000): Promise<any> {  return new Promise((resolve, reject) => {
    const socket: Socket = createConnection({ host: '127.0.0.1', port });
    let buffer = Buffer.alloc(0);
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error('relay spool request timed out'));
    }, timeoutMs);
    socket.on('connect', () => socket.write(framed(body)));
    socket.on('data', chunk => {
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= 4) {
        const size = buffer.readUInt32BE(0);
        if (buffer.length < size + 4) {
          return;
        }
        const value = JSON.parse(buffer.subarray(4, size + 4).toString('utf8'));
        clearTimeout(timer);
        socket.destroy();
        resolve(value);
        return;
      }
    });
    socket.on('error', error => {
      clearTimeout(timer);
      reject(error);
    });
  });
}

/**
 * Open a persistent inbound tunnel to a seed as a peer that deterministically rejects every
 * forwarded frame. Resolves once the seed acknowledges the tunnel.
 */
async function openRejectingTunnel(
  t: TestContext,
  port: number,
  keys: ReturnType<typeof deriveNetworkKeys>,
  targetNodeId: string,
): Promise<void> {
  const socket = createConnection({ host: '127.0.0.1', port });
  t.after(() => socket.destroy());
  let buffer = Buffer.alloc(0);
  let ack: (() => void) | undefined;
  let acknowledged = false;
  const ready = new Promise<void>((resolve, reject) => {
    ack = resolve;
    socket.once('error', reject);
  });
  socket.on('connect', () => {
    socket.write(framed({
      type: 'tunnel',
      frame: sealNetworkFrame(
        {
          type: 'discovery',
          endpointAddr: '127.0.0.1:9',
          expires: Date.now() + 20_000,
        },
        keys,
        targetNodeId,
      ),
    }));
  });
  socket.on('data', chunk => {
    buffer = Buffer.concat([buffer, chunk]);
    while (buffer.length >= 4) {
      const size = buffer.readUInt32BE(0);
      if (buffer.length < size + 4) {
        return;
      }
      const message = JSON.parse(buffer.subarray(4, size + 4).toString('utf8'));
      buffer = buffer.subarray(size + 4);
      if (message?.type === 'tunnel_ack' && !acknowledged) {
        acknowledged = true;
        ack?.();
      }
      if (message?.type === 'relay_deliver' && typeof message.relayId === 'string') {
        socket.write(framed({
          type: 'relay_reply',
          relayId: message.relayId,
          error: 'target policy rejected',
        }));
      }
    }
  });
  await ready;
}

/** A raw TCP relay endpoint that answers every request with one fixed frame body. */
async function rawRelayServer(  t: TestContext,
  responseBody: string | Buffer,
): Promise<string> {
  const response = Buffer.isBuffer(responseBody)
    ? responseBody
    : Buffer.from(responseBody);
  const server = createServer(socket => {
    socket.on('error', () => {});
    socket.once('data', () => {
      const size = Buffer.alloc(4);
      size.writeUInt32BE(response.length);
      socket.end(Buffer.concat([size, response]));
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve());
  });
  t.after(
    () =>
      new Promise<void>(resolve => {
        server.close(() => resolve());
      }),
  );
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('Invalid raw relay address');
  }
  return `127.0.0.1:${address.port}`;
}

/**
 * Replace only the sender's direct TCP hop with a deterministic error. A real ARP failure
 * (EHOSTDOWN) cannot be provoked on every test host, but every downstream decision is
 * identical to the OS raising the same classified connect error.
 */
function failDirectHop(transport: P2PTransportService, error: Error): void {
  (transport as unknown as {
    directFrame: (...args: unknown[]) => Promise<never>;
  }).directFrame = async () => {
    throw error;
  };
}

/**
 * Emulates a pre-76e33d9 seed: it forwards the original `relay` verb with a sealed target
 * reply, but closes the connection for the newer `relay_send` verb it never learned.
 */
async function legacyRelaySeed(
  t: TestContext,
  targetNodeId: string,
): Promise<{ endpoint: string; relayTypes: string[] }> {
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const relayTypes: string[] = [];
  const server = createServer(socket => {
    socket.on('error', () => {});
    let buffer = Buffer.alloc(0);
    socket.on('data', chunk => {
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= 4) {
        const size = buffer.readUInt32BE(0);
        if (buffer.length < size + 4) {
          return;
        }
        const request = JSON.parse(buffer.subarray(4, size + 4).toString('utf8'));
        buffer = buffer.subarray(size + 4);
        if (request.type !== 'relay' && request.type !== 'relay_send') {
          // An un-upgraded seed has no discovery/spool handler on this fake endpoint.
          socket.destroy();
          return;
        }
        relayTypes.push(String(request.type));
        if (request.type !== 'relay') {
          socket.destroy();
          return;
        }
        const reply = sealNetworkFrame(
          {
            requestNonce: request.frame.nonce,
            dataBase64: Buffer.from('legacy-relay-pong').toString('base64'),
          },
          keys,
          targetNodeId,
        );
        socket.end(framed({ ok: true, frame: reply }));
        return;
      }
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve());
  });
  t.after(
    () =>
      new Promise<void>(resolve => {
        server.close(() => resolve());
      }),
  );
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('Invalid legacy relay address');
  }
  return { endpoint: `127.0.0.1:${address.port}`, relayTypes };
}

test('Darwin host-down, interface-down and AggregateError direct failures fall back to a live relay seed', async t => {
  const dir = directory(t);
  const network = { networkId: NETWORK, passphrase: 'secret' };
  const seed = new P2PTransportService({
    discoveryDir: join(dir, 'seed'),
    multicast: false,
    socketTimeoutMs: 5_000,
  });
  t.after(() => seed.dispose());
  await seed.mountNetwork(network);
  const target = new P2PTransportService({
    discoveryDir: join(dir, 'target'),
    multicast: false,
    socketTimeoutMs: 5_000,
  });
  t.after(() => target.dispose());
  await target.mountNetwork(network);
  target.onInboundFrame(() => Buffer.from('relayed-pong'));
  // The live seed knows the target endpoint; the sender learns the target card but its own
  // direct hop is the only thing replaced below.
  seed.registerPeerEndpoint(NETWORK, target.localNodeId, target.endpointAddr!);
  const sender = new P2PTransportService({
    discoveryDir: join(dir, 'sender'),
    multicast: false,
    socketTimeoutMs: 5_000,
  });
  t.after(() => sender.dispose());
  await sender.mountNetwork({ ...network, bootstrapPeers: [seed.endpointAddr!] });
  sender.registerPeerEndpoint(NETWORK, target.localNodeId, '127.0.0.1:9');

  const hostDown = Object.assign(new Error('connect EHOSTDOWN 10.165.37.53:51086'), {
    code: 'EHOSTDOWN',
  });
  const interfaceDown = Object.assign(new Error('connect ENETDOWN'), { code: 'ENETDOWN' });
  const aggregate = new AggregateError(
    [
      Object.assign(new Error('connect EHOSTDOWN'), { code: 'EHOSTDOWN' }),
      Object.assign(new Error('connect ENETUNREACH'), { code: 'ENETUNREACH' }),
    ],
    'connect ENETUNREACH',
  );
  for (const failure of [hostDown, interfaceDown, aggregate]) {
    failDirectHop(sender, failure);
    const reply = await sender.sendUnicast(
      NETWORK,
      target.localNodeId,
      Buffer.from('reachability probe'),
    );
    assert.equal(
      Buffer.from(reply).toString(),
      'relayed-pong',
      `${failure.message} must be classified offline instead of failing the exchange`,
    );
  }

  // An aggregate that mixes a deterministic error with an offline one is not a peer outage:
  // relaying or queueing it would hide a real protocol/local failure.
  const mixed = new AggregateError(
    [
      Object.assign(new Error('connect EHOSTDOWN'), { code: 'EHOSTDOWN' }),
      new LocalDeliveryError('Invalid or mismatched delivery receipt'),
    ],
    'mixed connect failure',
  );
  failDirectHop(sender, mixed);
  await assert.rejects(
    sender.sendUnicast(NETWORK, target.localNodeId, Buffer.from('poisoned probe')),
    error => error === mixed,
  );
});

test('mixed-version seeds keep live sends on the legacy relay verb and fail spool offers fast', async t => {
  const dir = directory(t);
  const target = generateNodeKeyPair().nodeId;
  const legacy = await legacyRelaySeed(t, target);
  const transport = new P2PTransportService({
    discoveryDir: dir,
    multicast: false,
    socketTimeoutMs: 5_000,
  });
  t.after(() => transport.dispose());
  await transport.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [legacy.endpoint],
  });

  // A live non-spooling send must stay byte-compatible with seeds that only know `relay`.
  const live = await transport.sendUnicast(
    NETWORK,
    target,
    Buffer.from('live frame'),
    { timeoutMs: 5_000 },
  );
  assert.equal(Buffer.from(live).toString(), 'legacy-relay-pong');
  assert.deepEqual(legacy.relayTypes, ['relay']);

  let failure: unknown;
  try {
    await transport.sendUnicast(NETWORK, target, Buffer.from('spool offer'), {
      timeoutMs: 5_000,
      spoolIfOffline: true,
      spoolKey: 'legacy-seed-spool',
    });
  } catch (error) {
    failure = error;
  }
  assert.ok(
    isPeerUnreachableError(failure),
    'a legacy seed closing on relay_send is an offline peer, not an in-doubt send',
  );
  assert.equal(
    isDeliveryTimeoutFailure(failure),
    false,
    'the closed socket must not park the target in inDoubtNodeIds',
  );
  assert.deepEqual(legacy.relayTypes, ['relay', 'relay_send']);
});

test('classified Darwin connect failures become a durable queued receipt instead of a failed emit', async t => {
  const root = directory(t);
  const ben = runtimeFor(root, 'ben', { discovery: 'mesh' });
  const diz = runtimeFor(root, 'diz', { discovery: 'mesh' });
  await ben.start();
  await diz.start();
  t.after(async () => {
    await ben.stop().catch(() => {});
    await diz.stop().catch(() => {});
  });
  await ben.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  await diz.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  await until(
    () => ben.node.resolveTarget('member:diz', NETWORK).length === 1,
    'sender learns the target card',
  );
  const dizNodeId = diz.node.getSelfCard(NETWORK).nodeId;

  const failures = [
    Object.assign(new Error('connect EHOSTDOWN 10.165.37.53:51086'), { code: 'EHOSTDOWN' }),
    Object.assign(new Error('connect ENETDOWN'), { code: 'ENETDOWN' }),
    new AggregateError(
      [
        Object.assign(new Error('connect EHOSTDOWN'), { code: 'EHOSTDOWN' }),
        Object.assign(new Error('connect ENETUNREACH'), { code: 'ENETUNREACH' }),
      ],
      'connect ENETUNREACH',
    ),
  ];
  for (const failure of failures) {
    failDirectHop(ben.transport, failure);
    const emitted = await ben.emitter.emit({
      networkId: NETWORK,
      routing: { mode: 'unicast', targets: ['member:diz'] },
      kind: 'collab.request.v4',
      subject: `offline ${failure.message}`,
      body: 'the peer is unreachable, not actively refusing the protocol',
      payload: {},
    });
    assert.equal(emitted.receipts.length, 1);
    assert.equal(emitted.receipts[0].stage, 'queued', failure.message);
    const item = ben
      .listOutbox({ status: 'queued' })
      .find(candidate => candidate.id === emitted.messageId);
    assert.ok(item, `${failure.message} must be durably owned by the outbox`);
    assert.deepEqual(item.pendingNodeIds, [dizNodeId]);
    assert.deepEqual(item.inDoubtNodeIds, []);
  }
});

test('spool-enabled send to a pre-upgrade seed queues locally with no in-doubt target', async t => {
  const root = directory(t);
  const ben = runtimeFor(root, 'ben', { discovery: 'mesh' });
  const diz = runtimeFor(root, 'diz', { discovery: 'mesh' });
  await ben.start();
  await diz.start();
  t.after(async () => {
    await ben.stop().catch(() => {});
    await diz.stop().catch(() => {});
  });
  await diz.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const dizNodeId = diz.node.getSelfCard(NETWORK).nodeId;
  const legacy = await legacyRelaySeed(t, dizNodeId);
  await ben.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [legacy.endpoint],
  });
  await until(
    () => ben.node.resolveTarget('member:diz', NETWORK).length === 1,
    'sender learns the target card',
  );
  failDirectHop(
    ben.transport,
    Object.assign(new Error('connect EHOSTDOWN'), { code: 'EHOSTDOWN' }),
  );

  const emitted = await ben.emitter.emit({
    networkId: NETWORK,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'collab.request.v4',
    subject: 'legacy seed spool',
    body: 'the seed predates relay_send; keep the obligation local',
    payload: {},
  });
  assert.equal(emitted.receipts[0]?.stage, 'queued');
  assert.equal(
    emitted.receipts[0]?.executionReceipt?.relaySpooled,
    false,
    'a legacy seed never confirmed a spool handoff',
  );
  const item = ben
    .listOutbox({ status: 'queued' })
    .find(candidate => candidate.id === emitted.messageId)!;
  assert.deepEqual(item.pendingNodeIds, [dizNodeId]);
  assert.deepEqual(item.inDoubtNodeIds, []);
  assert.deepEqual(item.relaySpooledNodeIds, []);
  assert.deepEqual(legacy.relayTypes, ['relay_send']);
});

test('malformed relay responses surface as local protocol failures, not offline queueing', async t => {
  const root = directory(t);
  const endpoint = await rawRelayServer(t, Buffer.from('not-json'));
  const client = runtimeFor(root, 'client');
  t.after(async () => {
    await client.stop().catch(() => {});
  });
  await client.start();
  await client.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [endpoint],
  });
  const target = generateNodeKeyPair().nodeId;
  let failure: unknown;
  try {
    await client.transport.sendUnicast(
      NETWORK,
      target,
      Buffer.from('malformed response probe'),
      { timeoutMs: 5_000 },
    );
  } catch (error) {
    failure = error;
  }
  assert.ok(isLocalDeliveryError(failure), 'a malformed seed response is a protocol failure');
  assert.match((failure as Error).message, /Malformed relay response/);
});

test('relay seed surfaces deterministic target rejection instead of spooling it', async t => {
  const root = directory(t);
  const targetEndpoint = await rawRelayServer(
    t,
    JSON.stringify({ error: 'target policy rejected' }),
  );
  const seed = runtimeFor(root, 'seed');
  t.after(async () => {
    await seed.stop().catch(() => {});
  });
  await seed.start();
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const endpoint = seed.transport.endpointAddr!;
  const port = Number(endpoint.slice(endpoint.lastIndexOf(':') + 1));
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const sender = generateNodeKeyPair();
  const target = generateNodeKeyPair().nodeId;
  seed.transport.registerPeerEndpoint(NETWORK, target, targetEndpoint);
  const response = await tcpRequest(port, {
    type: 'relay_send',
    networkId: NETWORK,
    spoolIfOffline: true,
    target,
    frame: sealNetworkFrame({ payload: 'poison' }, keys, sender.nodeId),
    spoolKey: 'deterministic-rejection',
  });
  assert.equal(response.error, 'target policy rejected');
  assert.notEqual(response.spooled, true, 'a deterministic rejection is never a spooled handoff');
  const spoolFile = join(root, 'seed', 'relay-spool', `${keys.swarmTopicId}.json`);
  assert.equal(
    readRelaySpool(spoolFile).entries.length,
    0,
    'the seed must not retain a poison frame for offline replay',
  );
});

test('relay seed reports a target tunnel rejection without spooling it', async t => {
  const root = directory(t);
  const seed = runtimeFor(root, 'seed');
  t.after(async () => {
    await seed.stop().catch(() => {});
  });
  await seed.start();
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const endpoint = seed.transport.endpointAddr!;
  const port = Number(endpoint.slice(endpoint.lastIndexOf(':') + 1));
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const target = generateNodeKeyPair().nodeId;
  await openRejectingTunnel(t, port, keys, target);
  const sender = generateNodeKeyPair();
  const response = await tcpRequest(port, {
    type: 'relay_send',
    networkId: NETWORK,
    spoolIfOffline: true,
    target,
    frame: sealNetworkFrame({ payload: 'tunnel poison' }, keys, sender.nodeId),
    spoolKey: 'tunnel-rejection',
  });
  assert.equal(response.error, 'target policy rejected');
  assert.notEqual(response.spooled, true);
  const spoolFile = join(root, 'seed', 'relay-spool', `${keys.swarmTopicId}.json`);
  assert.equal(readRelaySpool(spoolFile).entries.length, 0);
});

test('relay network mismatch responses are local protocol failures', async t => {
  const root = directory(t);
  const endpoint = await rawRelayServer(
    t,
    JSON.stringify({ error: 'Relay network mismatch' }),
  );
  const client = runtimeFor(root, 'client');
  t.after(async () => {
    await client.stop().catch(() => {});
  });
  await client.start();
  await client.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [endpoint],
  });
  let failure: unknown;
  try {
    await client.transport.sendUnicast(
      NETWORK,
      generateNodeKeyPair().nodeId,
      Buffer.from('network mismatch probe'),
      { timeoutMs: 5_000 },
    );
  } catch (error) {
    failure = error;
  }
  assert.ok(isLocalDeliveryError(failure));
  assert.match((failure as Error).message, /Relay network mismatch/);
});

test('relay seed answers a networkId mismatch instead of dropping the request', async t => {
  const root = directory(t);
  const seed = runtimeFor(root, 'seed');
  t.after(async () => {
    await seed.stop().catch(() => {});
  });
  await seed.start();
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const endpoint = seed.transport.endpointAddr!;
  const port = Number(endpoint.slice(endpoint.lastIndexOf(':') + 1));
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const response = await tcpRequest(port, {
    type: 'relay_send',
    networkId: 'other-network',
    target: generateNodeKeyPair().nodeId,
    frame: sealNetworkFrame({ probe: true }, keys, generateNodeKeyPair().nodeId),
    spoolIfOffline: true,
    spoolKey: 'mismatch',
  });
  assert.match(String(response.error), /Relay network mismatch/);
});

test('relay_send packet budget degrades oversized attachment offers to live-only', async t => {
  const root = directory(t);
  const runtimes: DealerNodeRuntime[] = [];
  t.after(async () => {
    for (const runtime of [...runtimes].reverse()) {
      await runtime.stop().catch(() => {});
    }
  });
  const seed = runtimeFor(root, 'seed');
  runtimes.push(seed);
  await seed.start();
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const endpoint = seed.transport.endpointAddr!;
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const client = runtimeFor(root, 'client');
  runtimes.push(client);
  await client.start();
  await client.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [endpoint],
  });

  // A sealed frame plus this attachment would exceed MAX_FRAME once JSON keys and base64
  // expansion are counted, while the raw attachment alone is below the old raw-byte limit.
  const attachmentPath = join(root, 'boundary-attachment.bin');
  writeFileSync(attachmentPath, Buffer.alloc(3 * 1024 * 1024, 7));
  const attachment = await client.transport.shareFile(attachmentPath);
  // The sealed frame is base64-expanded twice (payload then ciphertext), so 3 MiB of
  // plaintext already occupies ~5.6 MiB of the packet. The 3 MiB attachment's base64 adds
  // ~4 MiB, pushing the exact packet past MAX_FRAME while its raw bytes remain well below
  // the former raw attachment budget.
  const frame = Buffer.from('x'.repeat(3 * 1024 * 1024));
  const spoolFile = join(root, 'seed', 'relay-spool', `${keys.swarmTopicId}.json`);
  const entriesBefore = readRelaySpool(spoolFile).entries.length;
  let failure: unknown;
  try {
    await client.transport.sendUnicast(
      NETWORK,
      generateNodeKeyPair().nodeId,
      frame,
      {
        timeoutMs: 15_000,
        spoolIfOffline: true,
        spoolKey: 'packet-budget',
        spoolBlobHashes: [attachment.blake3Hash],
      },
    );
  } catch (error) {
    failure = error;
  }
  assert.ok(failure instanceof Error);
  assert.equal(
    (failure as { code?: string }).code,
    'peer_unreachable',
    `an incomplete offer stays live-only and surfaces as offline, not a frame-limit bug: ${
      (failure as Error).message
    }`,
  );
  assert.doesNotMatch((failure as Error).message, /Frame exceeds limit/);
  assert.equal(
    readRelaySpool(spoolFile).entries.length,
    entriesBefore,
    'no partial relay entry is admitted for a packet that cannot fit',
  );
});

test('relay spool validates targets, isolates senders and collects expired blobs', async t => {
  const root = directory(t);
  const runtimes: DealerNodeRuntime[] = [];
  t.after(async () => {
    for (const runtime of [...runtimes].reverse()) {
      await runtime.stop().catch(() => {});
    }
  });
  const createSeed = async (port?: number) => {
    const runtime = runtimeFor(root, 'seed', port !== undefined ? { port } : {});
    runtimes.push(runtime);
    await runtime.start();
    return runtime;
  };
  let seed = await createSeed();
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const endpoint = seed.transport.endpointAddr!;
  const port = Number(endpoint.slice(endpoint.lastIndexOf(':') + 1));
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const swarmTopicId = keys.swarmTopicId;

  const senderA = generateNodeKeyPair();
  const senderB = generateNodeKeyPair();
  const senderC = generateNodeKeyPair();
  const target = generateNodeKeyPair().nodeId;
  const send = (body: Record<string, unknown>) =>
    tcpRequest(port, {
      type: 'relay_send',
      networkId: NETWORK,
      spoolIfOffline: true,
      ...body,
    });

  let response = await send({
    target: 'member:ben',
    frame: sealNetworkFrame({ payload: 1 }, keys, senderA.nodeId),
    spoolKey: 'key',
  });
  assert.match(String(response.error), /Invalid relay spool target/);

  response = await send({
    target,
    frame: sealNetworkFrame({ payload: 2 }, keys, senderA.nodeId),
    spoolKey: 'same',
  });
  assert.equal(response.spooled, true);
  assert.equal(response.spoolId, `${senderA.nodeId}:same`);

  response = await send({
    target,
    frame: sealNetworkFrame({ payload: 3 }, keys, senderB.nodeId),
    spoolKey: 'same',
  });
  assert.equal(response.spooled, true);
  assert.equal(
    response.spoolId,
    `${senderB.nodeId}:same`,
    'a different sender cannot overwrite another sender spool entry',
  );

  response = await send({
    target,
    frame: sealNetworkFrame({ payload: 4 }, keys, senderC.nodeId),
    spoolKey: 'bad\nkey',
  });
  assert.match(String(response.error), /Invalid relay spool key/);

  const spoolFile = join(root, 'seed', 'relay-spool', `${swarmTopicId}.json`);
  let spool = readRelaySpool(spoolFile);
  assert.equal(spool.entries.length, 2);
  assert.ok(
    spool.entries.every(entry => entry.blobHashes === undefined),
    'entries without attachments persist no attachment metadata',
  );

  const blobTarget = generateNodeKeyPair().nodeId;
  const blobBody = Buffer.from('cached attachment bytes');
  const hash = blake3(blobBody);
  response = await send({
    target: blobTarget,
    frame: sealNetworkFrame({ payload: 5 }, keys, senderC.nodeId),
    spoolKey: 'blob',
    blobs: { [hash]: blobBody.toString('base64') },
  });
  assert.equal(response.spooled, true);
  const blobPath = join(root, 'seed', 'relay-spool', 'blobs', swarmTopicId, hash);
  assert.ok(existsSync(blobPath));
  spool = readRelaySpool(spoolFile);
  assert.equal(
    spool.entries.find(entry => entry.id.endsWith(':blob'))?.blobHashes?.[hash],
    blobBody.length,
  );
  assert.equal(
    await tcpReplyReceived(port, { type: 'blob', hash }),
    false,
    'a spooled attachment is never exposed through the process-global blob cache',
  );

  const client = runtimeFor(root, 'client');
  runtimes.push(client);
  await client.start();
  await client.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [endpoint],
  });
  const unknownTarget = generateNodeKeyPair().nodeId;
  const missingHash = 'f'.repeat(64);
  await assert.rejects(
    client.transport.sendUnicast(
      NETWORK,
      unknownTarget,
      Buffer.from('missing attachment frame'),
      { spoolIfOffline: true, spoolKey: 'missing', spoolBlobHashes: [missingHash] },
    ),
    (error: { code?: string; message?: string }) =>
      error.code === 'local_delivery_failure' &&
      /Missing attachment blob/.test(error.message ?? ''),
  );

  const oversizedPath = join(root, 'oversized.bin');
  writeFileSync(oversizedPath, '');
  truncateSync(oversizedPath, 7 * 1024 * 1024);
  const oversizedHash = 'e'.repeat(64);
  client.transport.registerBlob(oversizedHash, oversizedPath);
  const entriesBefore = readRelaySpool(spoolFile).entries.length;
  await assert.rejects(
    client.transport.sendUnicast(
      NETWORK,
      unknownTarget,
      Buffer.from('oversized attachment frame'),
      {
        spoolIfOffline: true,
        spoolKey: 'oversized',
        spoolBlobHashes: [oversizedHash],
      },
    ),
    (error: { code?: string }) =>
      // Oversized offers stay live-only: the caller may queue locally, but must never
      // mint a spooled handoff or leave a partial relay entry behind.
      error.code === 'peer_unreachable' && error.code !== 'offline_peer_spooled',
  );
  const entriesAfter = readRelaySpool(spoolFile).entries.length;
  assert.equal(entriesAfter, entriesBefore, 'an incomplete attachment offer is never spooled');

  await seed.stop();
  spool = readRelaySpool(spoolFile);
  for (const entry of spool.entries) {
    (entry as { expiresAt: string }).expiresAt = new Date(Date.now() - 1_000).toISOString();
  }
  writeFileSync(spoolFile, JSON.stringify(spool));
  seed = await createSeed(port);
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  assert.equal(existsSync(blobPath), false, 'expired spool blobs are garbage collected');
  const pruned = readRelaySpool(spoolFile);
  assert.equal(pruned.entries.length, 0);
});

test('relay spool mount reclaims orphaned blob files without a snapshot or journal', async t => {
  const root = directory(t);
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const blobDir = join(root, 'seed', 'relay-spool', 'blobs', keys.swarmTopicId);
  mkdirSync(blobDir, { recursive: true });
  const orphan = Buffer.from('orphaned spool attachment');
  const hash = blake3(orphan);
  const blobPath = join(blobDir, hash);
  writeFileSync(blobPath, orphan);
  const seed = runtimeFor(root, 'seed');
  t.after(async () => {
    await seed.stop().catch(() => {});
  });
  await seed.start();
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  assert.equal(existsSync(blobPath), false, 'an unreferenced blob is collected on mount');
});

test('relay spool enforces per-target and global entry bounds when loading', async t => {
  const root = directory(t);
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const spoolDir = join(root, 'seed', 'relay-spool');
  mkdirSync(spoolDir, { recursive: true });
  const sender = generateNodeKeyPair();
  const hotTarget = generateNodeKeyPair().nodeId;
  const otherTargets = Array.from({ length: 4 }, () => generateNodeKeyPair().nodeId);
  const now = Date.now();
  const entries: Array<Record<string, unknown>> = [];
  let sequence = 0;
  const senderFor = (value: number) => `ed25519:${value.toString(16).padStart(64, '0')}`;
  const add = (targetNodeId: string, count: number, offset: number) => {
    for (let index = 0; index < count; index += 1) {
      const sender = senderFor(sequence);
      entries.push({
        id: `${sender}:key-${sequence}`,
        targetNodeId,
        networkId: NETWORK,
        frame: sealNetworkFrame({ sequence }, keys, sender),
        spooledAt: new Date(now - 1_000_000 + offset + index).toISOString(),
        expiresAt: new Date(now + 86_400_000).toISOString(),
      });
      sequence += 1;
    }
  };
  otherTargets.forEach((target, index) => add(target, 200, index * 200));
  add(hotTarget, 300, 900);
  const spoolFile = join(spoolDir, `${keys.swarmTopicId}.json`);
  writeFileSync(
    spoolFile,
    JSON.stringify({
      schema: 'the-dealer-relay-spool/v1',
      networkId: NETWORK,
      entries,
    }),
  );

  const seed = runtimeFor(root, 'seed');
  t.after(async () => {
    await seed.stop().catch(() => {});
  });
  await seed.start();
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const saved = readRelaySpool(spoolFile);
  assert.equal(saved.entries.length, 1024, 'global entry budget is enforced');
  assert.equal(
    saved.entries.filter(entry => entry.targetNodeId === hotTarget).length,
    256,
    'per-target entry budget is enforced',
  );
});

test('relay spool load enforces the per-sender bound for one sender', async t => {
  const root = directory(t);
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const spoolDir = join(root, 'seed', 'relay-spool');
  mkdirSync(spoolDir, { recursive: true });
  const sender = generateNodeKeyPair();
  // Two targets each start at 300 entries: no per-target violation survives pruning at 256,
  // yet the single sender owns 512 entries, so only the 256-entry sender bound can produce
  // the observed eviction. An id shared across targets must not shield either group.
  const targetA = generateNodeKeyPair().nodeId;
  const targetB = generateNodeKeyPair().nodeId;
  const now = Date.now();
  const entries = Array.from({ length: 600 }, (_, index) => ({
    id: `${sender.nodeId}:load-${index}`,
    targetNodeId: index % 2 === 0 ? targetA : targetB,
    networkId: NETWORK,
    frame: sealNetworkFrame({ index }, keys, sender.nodeId),
    spooledAt: new Date(now - 1_000_000 + index).toISOString(),
    expiresAt: new Date(now + 86_400_000).toISOString(),
  }));
  const spoolFile = join(spoolDir, `${keys.swarmTopicId}.json`);
  writeFileSync(
    spoolFile,
    JSON.stringify({ schema: 'the-dealer-relay-spool/v1', networkId: NETWORK, entries }),
  );
  const seed = runtimeFor(root, 'seed');
  t.after(async () => {
    await seed.stop().catch(() => {});
  });
  await seed.start();
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const saved = readRelaySpool(spoolFile);
  assert.equal(saved.entries.length, 256, 'the per-sender load bound is enforced');
  assert.ok(saved.entries.every(entry => entry.frame.senderNodeId === sender.nodeId));
  const perTarget = new Map<string, number>();
  for (const entry of saved.entries) {
    perTarget.set(entry.targetNodeId, (perTarget.get(entry.targetNodeId) ?? 0) + 1);
  }
  assert.deepEqual(
    [...perTarget.keys()].sort(),
    [targetA, targetB].sort(),
    'both target groups survive the sender-level eviction',
  );
  assert.ok(
    [...perTarget.values()].every(count => count <= 256),
    'no single target exceeds its own cap',
  );
});

test('relay spool protects only the unique target/id admission pair', async t => {
  const root = directory(t);
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const spoolDir = join(root, 'seed', 'relay-spool');
  mkdirSync(spoolDir, { recursive: true });
  const sender = generateNodeKeyPair();
  const now = Date.now();
  const sharedKey = 'shared-spool-key';
  // 128 distinct targets all share one sender/id. An id-only protection rule would shield
  // every one of them from every eviction loop and let one admission breach the target cap.
  const entries = Array.from({ length: 128 }, (_, index) => ({
    id: `${sender.nodeId}:${sharedKey}`,
    targetNodeId: `ed25519:${(index + 1).toString(16).padStart(64, '0')}`,
    networkId: NETWORK,
    frame: sealNetworkFrame({ index }, keys, sender.nodeId),
    spooledAt: new Date(now - 1_000_000 + index).toISOString(),
    expiresAt: new Date(now + 86_400_000).toISOString(),
  }));
  const spoolFile = join(spoolDir, `${keys.swarmTopicId}.json`);
  writeFileSync(
    spoolFile,
    JSON.stringify({ schema: 'the-dealer-relay-spool/v1', networkId: NETWORK, entries }),
  );
  const seed = runtimeFor(root, 'seed');
  t.after(async () => {
    await seed.stop().catch(() => {});
  });
  await seed.start();
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const endpoint = seed.transport.endpointAddr!;
  const port = Number(endpoint.slice(endpoint.lastIndexOf(':') + 1));
  const newTarget = generateNodeKeyPair().nodeId;
  const response = await tcpRequest(port, {
    type: 'relay_send',
    networkId: NETWORK,
    spoolIfOffline: true,
    target: newTarget,
    frame: sealNetworkFrame({ payload: 'new' }, keys, sender.nodeId),
    spoolKey: sharedKey,
  });
  assert.equal(response.spooled, true);
  const saved = readRelaySpool(spoolFile);
  assert.equal(saved.entries.length, 128, 'the target cap evicts a same-id old entry');
  assert.ok(
    saved.entries.some(entry => entry.targetNodeId === newTarget),
    'the newly admitted target/id pair is preserved',
  );
});

test('relay spool journal restores entries before the first snapshot compaction', async t => {
  const root = directory(t);
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const spoolDir = join(root, 'seed', 'relay-spool');
  mkdirSync(spoolDir, { recursive: true });
  const sender = generateNodeKeyPair();
  const entry = {
    id: `${sender.nodeId}:journal-only`,
    targetNodeId: generateNodeKeyPair().nodeId,
    networkId: NETWORK,
    frame: sealNetworkFrame({ payload: 'journal-only' }, keys, sender.nodeId),
    frameBytes: 0,
    spooledAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
  };
  entry.frameBytes = Buffer.byteLength(JSON.stringify(entry.frame));
  const spoolFile = join(spoolDir, `${keys.swarmTopicId}.json`);
  writeFileSync(`${spoolFile}.journal`, `${JSON.stringify({ op: 'upsert', entry })}\n`);
  const seed = runtimeFor(root, 'seed');
  t.after(async () => {
    await seed.stop().catch(() => {});
  });
  await seed.start();
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const saved = readRelaySpool(spoolFile);
  assert.equal(saved.entries.length, 1, 'the journal is replayed when no snapshot exists');
  assert.equal(saved.entries[0].id, entry.id);
  assert.ok(existsSync(spoolFile), 'mount compacts the journal into a base snapshot');
});

test('relay spool counts sealed frame bytes against the global budget', async t => {
  const root = directory(t);
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const seed = runtimeFor(root, 'seed');
  t.after(async () => {
    await seed.stop().catch(() => {});
  });
  await seed.start();
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const endpoint = seed.transport.endpointAddr!;
  const port = Number(endpoint.slice(endpoint.lastIndexOf(':') + 1));
  const sender = generateNodeKeyPair();
  const payload = 'x'.repeat(3 * 1024 * 1024);
  for (let index = 0; index < 9; index += 1) {
    const response = await tcpRequest(port, {
      type: 'relay_send',
      networkId: NETWORK,
      spoolIfOffline: true,
      target: generateNodeKeyPair().nodeId,
      frame: sealNetworkFrame({ index, payload }, keys, sender.nodeId),
      spoolKey: `large-${index}`,
    }, 30_000);
    assert.equal(response.spooled, true);
  }
  const spoolFile = join(root, 'seed', 'relay-spool', `${keys.swarmTopicId}.json`);
  const saved = readRelaySpool(spoolFile);
  const frameBytes = saved.entries.reduce(
    (total, entry) => total + Number(entry.frameBytes ?? 0),
    0,
  );
  assert.ok(saved.entries.length < 9, 'large sealed frames are evicted by the byte budget');
  assert.ok(frameBytes <= 32 * 1024 * 1024, 'the global budget includes sealed frame bytes');
});
