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
import { createConnection, createServer, Socket } from 'node:net';
import { tmpdir, hostname, networkInterfaces } from 'node:os';
import { dirname, join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { DealerNodeRuntime } from '../packages/cli/runtime.ts';
import { runDealerCli, summarizeNextStep } from '../packages/cli/index.ts';
import { Kernel } from '../packages/kernel/index.ts';
import { createEmitterPlugin } from '../packages/emitter/index.ts';
import { createDispatcherPlugin } from '../packages/dispatcher/index.ts';
import { createRouterPlugin } from '../packages/router/index.ts';
import { withOwnedLock, writeFileAtomically } from '../packages/fs-atomic/index.ts';
import { OutboxRedriveService, OutboxStore } from '../packages/mailbox/outbox.ts';
import { MailboxStore } from '../packages/mailbox/store.ts';
import { netKey } from '../packages/mailbox/network-key.ts';
import { createNodePlugin } from '../packages/node/index.ts';
import { createPollWatermarkSourcePlugin } from '../packages/plugins-source/index.ts';
import { P2PTransportService, blake3, createTransportPlugin } from '../packages/transport/index.ts';
import type { TransportPluginOptions } from '../packages/transport/index.ts';
import type { RuntimeConfig } from '../packages/cli/config.ts';
import {
  DeliveryTimeoutError,
  LocalDeliveryError,
  OfflinePeerSpooledError,
  PeerUnreachableError,
  deriveNetworkKeys,
  generateNodeKeyPair,
  hasProvenUnattemptedFailure,
  isConfirmedHandoffReceipt,
  isBeforeSendError,
  DEDUP_CAS_AMBIGUOUS_DETAIL,
  DEDUP_CAS_DUPLICATE_DETAIL,
  isDeliveryTimeoutFailure,
  isLocalDeliveryError,
  isPeerUnreachableError,
  isUnresolvedTargetError,
  markBeforeSendError,
  sealNetworkFrame,
  signEnvelope,
  signNodeCard,
  signReceipt,
  unsealNetworkFrame,
} from '../packages/core-types/index.ts';
import { isRetryablePreEffectReceipt, isUnattemptedOutboxItem } from '../packages/index.ts';
import type {
  DeliveryReceipt,
  DeliveryStage,
  EmitMessageInput,
  EmitterService,
  MessageEnvelope,
  NodeCard,
  NodeService,
  OutboxItem,
  PeerReachableEvent,
  SealedNetworkFrame,
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
    /** Replace only the wire hop: an emitter-level test over a real transport instance. */
    send?: (
      targetNodeId: string,
      frame: Uint8Array,
      opts?: {
        timeoutMs?: number;
        spoolIfOffline?: boolean;
        spoolKey?: string;
        spoolBlobHashes?: readonly string[];
      },
    ) => Promise<Uint8Array>;
    plan?: (
      targetNodeId: string,
      callIndex: number,
    ) => { stage?: DeliveryStage; detail?: string; error?: Error; delayMs?: number; executionReceipt?: Record<string, unknown> } | undefined;
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
      if (options.send) {
        return options.send(targetNodeId, frame, opts);
      }
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
          ...(plan.executionReceipt ? { executionReceipt: plan.executionReceipt } : {}),
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
      { createdAt: event.createdAt },
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
    kernel,
    sendCalls,
    setMounted(value: boolean) {
      mounted = value;
    },
  };
}

test('redrive settles signed rejection or dispatcher failure as terminal failed', async t => {
  for (const stage of ['rejected', 'failed', 'dispatching'] as const) {
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
      return dizOnline ? {} : { error: markBeforeSendError(new PeerUnreachableError(target)) };
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
      return dizOnline ? {} : { error: markBeforeSendError(new PeerUnreachableError(target)) };
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
  // The signed rejection proves the recipient did not attempt execution.
  const rejecting = await emitterHarness(t, {
    plan: () => ({ stage: 'rejected', executionReceipt: { handoff: 'unattempted' } }),
  });
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

  // The outbound framing check failed before transport could reach any peer.
  const failing = await emitterHarness(t, {
    plan: () => {
      throw markBeforeSendError(new LocalDeliveryError('Frame exceeds limit'));
    },
  });
  let failure: unknown;
  try {
    await failing.emitter.emit({
      networkId: failing.networkId,
      routing: { mode: 'unicast', targets: ['member:diz'] },
      kind: 'task',
      subject: 'local failure',
      body: 'framing rejected before send',
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
    plan: target => ({ error: markBeforeSendError(new PeerUnreachableError(target)) }),
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
        throw markBeforeSendError(new LocalDeliveryError('Frame exceeds limit'));
      }
      return dizOnline ? {} : { error: markBeforeSendError(new PeerUnreachableError(target)) };
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

test('scoped --id flush delivers its head even when a later event shares the thread', async t => {
  const dir = directory(t);
  const outbox = new OutboxStore(join(dir, 'outbox.json'), join(dir, 'outbox-blobs'));
  const sender = generateNodeKeyPair();
  const target = generateNodeKeyPair().nodeId;
  const queued = (id: string) =>
    outbox.enqueue({
      envelope: threadedEnvelope(sender, 'scoped-net', target, id, 'thread-scoped'),
      explicitTargets: [target],
      pendingNodeIds: [target],
    });
  const head = queued('evt-0002');
  const later = queued('evt-0003');
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

  const result = await service.flush({ id: head.id });
  assert.equal(result.attempted, 1, 'the scoped head itself must be attempted');
  assert.deepEqual(deliveries, [head.id]);
  assert.equal(outbox.get(head.id)?.status, 'delivered');
  assert.equal(
    outbox.get(later.id)?.status,
    'queued',
    'a later event on the same thread must not be delivered by a scoped --id flush',
  );
});

test('peer pulse resolves the matching head without letting a later matching event overtake a skipped sibling', async t => {
  const dir = directory(t);
  const outbox = new OutboxStore(join(dir, 'outbox.json'), join(dir, 'outbox-blobs'));
  const sender = generateNodeKeyPair();
  const peerP = generateNodeKeyPair().nodeId;
  const peerQ = generateNodeKeyPair().nodeId;
  const queued = (id: string, peer: string) =>
    outbox.enqueue({
      envelope: threadedEnvelope(sender, 'pulse-net', peer, id, 'thread-pulse'),
      explicitTargets: [peer],
      pendingNodeIds: [peer],
    });
  const head = queued('evt-0002', peerP);
  const sibling = queued('evt-0003', peerQ);
  const trailing = queued('evt-0004', peerP);
  outbox.recordAttempt(head.id, {
    nextAttemptAt: new Date(Date.now() + 60_000).toISOString(),
    inDoubtNodeIds: [peerP],
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

  const result = await service.flush({
    force: true,
    forceMatchingTarget: true,
    targetNodeId: peerP,
  });
  assert.equal(result.attempted, 1, 'only the matching in-doubt head is attempted');
  assert.deepEqual(deliveries, [head.id]);
  const resolved = outbox.get(head.id)!;
  assert.equal(resolved.status, 'delivered', 'the peer pulse resolves the in-doubt head');
  assert.deepEqual(resolved.inDoubtNodeIds, []);
  assert.equal(outbox.get(sibling.id)?.status, 'queued');
  assert.equal(
    outbox.get(trailing.id)?.status,
    'queued',
    'a later peer-matching event may not overtake the skipped sibling on its thread',
  );
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

    for (const stage of ['rejected', 'dispatching'] as const) {
      // A terminally rejected send is not a committed handoff: the lease stays leased.
      const rejecting = sessionHarness(t, async () => ({
        messageId: 'msg_outbound_session',
        traceId: 'trace_outbound_session',
        receipts: [receiptFor({ stage, detail: 'execution not committed' })],
      }));
      const rejected = await run(
        rejecting.runtime,
        argsFor(group, rejecting.inboundId, rejecting.leaseId),
      );
      assert.equal(rejected.code, 0, rejected.stderr);
      assert.equal(rejecting.emitCalls.length, 1);
      rejecting.store.assertAckableLease(rejecting.leaseId);
      assert.equal(inboundStatus(rejecting.store, rejecting.inboundId), 'leased');
      assert.equal(
        JSON.parse(rejected.stdout.join('')).acked,
        false,
        'a terminal rejection must report that the lease was preserved',
      );

    }

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
    assert.equal(
      JSON.parse(committed.stdout.join('')).acked,
      true,
      'a committed send must report that the lease was burned',
    );
    assert.equal(queued.store.ack(queued.leaseId).alreadyAcknowledged, true);
    assert.equal(inboundStatus(queued.store, queued.inboundId), 'acked');
  }
});

test('unmounted network keeps outbox obligations queued through remount', async t => {
  let online = false;
  const harness = await emitterHarness(t, {
    plan: target => (online ? {} : { error: markBeforeSendError(new PeerUnreachableError(target)) }),
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

test('staged attachments survive a failed outbox settlement save and are removed after retry', t => {
  const root = directory(t);
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'blobs'));
  const sender = generateNodeKeyPair();
  const peer = generateNodeKeyPair();
  const source = join(root, 'source.txt');
  const body = Buffer.from('recoverable attachment');
  const hash = blake3(body);
  writeFileSync(source, body);
  for (const mutation of ['enqueue', 'recordAttempt', 'markNodeDelivered'] as const) {
    const envelope = signEnvelope({
      ...envelopeFor(sender, NETWORK, peer.nodeId),
      attachments: [{ name: 'source.txt', mimeType: 'text/plain', sizeBytes: body.length, blake3Hash: hash, ticket: `local:${hash}` }],
    }, sender.privateKeyPem);
    const input = { envelope, explicitTargets: [peer.nodeId], pendingNodeIds: [peer.nodeId] };
    const item = outbox.enqueue({ ...input, filePaths: { [hash]: source } });
    const staged = item.stagedFiles[hash];
    const receipt = signReceipt({
      messageId: envelope.id, networkId: envelope.networkId, fromNodeId: peer.nodeId,
      stage: 'dispatched', updatedAt: envelope.createdAt,
    }, peer.privateKeyPem);
    const deliveredReceipts = { [peer.nodeId]: receipt };
    const settle = () => {
      if (mutation === 'enqueue') return outbox.enqueue({ ...input, deliveredReceipts });
      if (mutation === 'recordAttempt') return outbox.recordAttempt(item.id, { deliveredReceipts, nextAttemptAt: item.nextAttemptAt });
      return outbox.markNodeDelivered(item.id, peer.nodeId, receipt);
    };
    const save = t.mock.method(outbox as unknown as { save(state: unknown): void }, 'save', () => {
      throw new Error('outbox save failed');
    });
    try {
      assert.throws(settle, /outbox save failed/);
      assert.equal(outbox.get(item.id)?.status, 'queued');
      assert.equal(outbox.get(item.id)?.stagedFiles[hash], staged);
      assert.deepEqual(readFileSync(staged), body);
    } finally {
      save.mock.restore();
    }
    assert.equal(settle()?.status, 'delivered');
    assert.deepEqual(outbox.get(item.id)?.stagedFiles, {});
    assert.equal(existsSync(staged), false);
  }
});

test('durable outbox stages declared attachments from attachmentPaths across redrive', async t => {
  let online = false;
  const harness = await emitterHarness(t, {
    plan: target => (online ? {} : { error: markBeforeSendError(new PeerUnreachableError(target)) }),
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
    plan: target => ({ error: markBeforeSendError(new PeerUnreachableError(target)) }),
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
        return { error: markBeforeSendError(new PeerUnreachableError(target)) };
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
        return { error: markBeforeSendError(new PeerUnreachableError(target)) };
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
    sendError: markBeforeSendError(new PeerUnreachableError(`ed25519:${'a'.repeat(64)}`)),
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

test('an unchanged direct peer card refreshes the endpoint while preserving outbox backoff', async t => {
  const dir = directory(t);
  const local = generateNodeKeyPair();
  const peer = generateNodeKeyPair();
  const kernel = new Kernel([
    { plugin: createTransportPlugin({ localNodeId: local.nodeId, discoveryDir: dir, multicast: false }) },
    { plugin: createNodePlugin({ keyPair: local, member: 'ben', nodeName: 'local' }) },
  ]);
  t.after(() => kernel.stop());
  await kernel.start();
  const transport: P2PTransportService = kernel.ctx.services.transport;
  await transport.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const endpointAddr = '127.0.0.1:45678';
  const card = {
    nodeId: peer.nodeId, networkId: NETWORK, identity: { member: 'oliver', nodeName: 'laptop' },
    capabilities: { summary: '', skills: [] },
    runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] },
    admissionMode: 'open', endpointAddr, updatedAt: new Date().toISOString(),
  } as const;
  const first = signNodeCard({ ...card, revision: 1 }, peer.privateKeyPem);
  const deliverCard = (card: NodeCard) => kernel.ctx.emit('transport/inbound', {
    networkId: NETWORK, senderNodeId: peer.nodeId, mode: 'unicast', payload: { type: 'peer.card', card },
  });
  deliverCard(first);

  const outbox = new OutboxStore(join(dir, 'outbox.json'), join(dir, 'outbox-blobs'));
  const item = outbox.enqueue({
    envelope: envelopeFor(local, NETWORK, peer.nodeId),
    explicitTargets: [peer.nodeId],
    pendingNodeIds: [peer.nodeId],
  });
  const nextAttemptAt = new Date(Date.now() + 60_000).toISOString();
  outbox.recordAttempt(item.id, { nextAttemptAt });
  const service = new OutboxRedriveService({
    outbox,
    intervalMs: 60_000,
    events: { on: (event, listener) => kernel.ctx.on(event, listener) },
    deliver: async candidate => ({
      deliveredReceipts: {
        [peer.nodeId]: {
          messageId: candidate.envelope.id, networkId: NETWORK, fromNodeId: peer.nodeId,
          stage: 'dispatched', updatedAt: new Date().toISOString(), signature: 'test-signature',
        },
      },
      pendingNodeIds: [],
    }),
  });
  service.start();
  t.after(() => service.stop());

  transport.registerPeerEndpoint(NETWORK, peer.nodeId, '127.0.0.1:45679');
  deliverCard(first);
  assert.equal(transport.listPeerEndpoints(NETWORK).get(peer.nodeId), endpointAddr);
  await service.flush(); // The regular flush follows any event-driven flush in the same queue.
  assert.equal(outbox.get(item.id)?.status, 'queued');
  assert.equal(outbox.get(item.id)?.nextAttemptAt, nextAttemptAt);

  deliverCard(signNodeCard({ ...card, revision: 2 }, peer.privateKeyPem));
  await service.flush();
  assert.equal(outbox.get(item.id)?.status, 'delivered');
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

/** Read the sender's live view of one peer's sealed discovery beacon expiry. */
function beaconExpiry(
  transport: P2PTransportService,
  networkId: string,
  nodeId: string,
): number {
  const mounts = (transport as unknown as {
    mounts: Map<string, { beacons: Map<string, { expires: number }> }>;
  }).mounts;
  return mounts.get(networkId)?.beacons.get(nodeId)?.expires ?? 0;
}

/** Decode a sealed relay frame far enough to tell a user envelope from presence gossip. */
function forwardedMessageEnvelope(
  frame: SealedNetworkFrame,
  keys: ReturnType<typeof deriveNetworkKeys>,
): { id: string; networkId: string } | undefined {
  try {
    const forwarded = unsealNetworkFrame<{ dataBase64?: unknown }>(frame, keys);
    if (typeof forwarded.dataBase64 !== 'string') {
      return undefined;
    }
    const payload = JSON.parse(Buffer.from(forwarded.dataBase64, 'base64').toString('utf8'));
    return payload?.type === 'message.envelope' &&
      typeof payload.envelope?.id === 'string' &&
      typeof payload.envelope?.networkId === 'string'
      ? { id: payload.envelope.id, networkId: payload.envelope.networkId }
      : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Emulates a pre-76e33d9 seed: it forwards the original `relay` verb with a sealed target
 * reply, but closes the connection for the newer `relay_send` verb it never learned. When
 * the target's signing key is supplied, a forwarded message envelope is answered exactly
 * as the live target would: a signed `dispatched` receipt bound to that envelope. Presence
 * heartbeats also relay through the seed, so `envelopeRelayTypes` records only user sends.
 */
async function legacyRelaySeed(
  t: TestContext,
  targetNodeId: string,
  targetPrivateKeyPem?: string,
): Promise<{ endpoint: string; relayTypes: string[]; envelopeRelayTypes: string[] }> {
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const relayTypes: string[] = [];
  const envelopeRelayTypes: string[] = [];
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
        const envelope = forwardedMessageEnvelope(request.frame, keys);
        if (envelope !== undefined) {
          envelopeRelayTypes.push(String(request.type));
        }
        if (request.type !== 'relay') {
          socket.destroy();
          return;
        }
        let dataBase64 = Buffer.from('legacy-relay-pong').toString('base64');
        if (targetPrivateKeyPem !== undefined && envelope !== undefined) {
          dataBase64 = Buffer.from(
            JSON.stringify(
              signReceipt(
                {
                  messageId: envelope.id,
                  networkId: envelope.networkId,
                  fromNodeId: targetNodeId,
                  stage: 'dispatched',
                  updatedAt: new Date().toISOString(),
                },
                targetPrivateKeyPem,
              ),
            ),
          ).toString('base64');
        }
        const reply = sealNetworkFrame(
          {
            requestNonce: request.frame.nonce,
            dataBase64,
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
  return {
    endpoint: `127.0.0.1:${address.port}`,
    relayTypes,
    envelopeRelayTypes,
  };
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
    'a legacy seed closing on relay_send is classified as unreachable',
  );
  assert.equal(
    isDeliveryTimeoutFailure(failure),
    false,
    'the socket closed before a timeout',
  );
  assert.equal(isBeforeSendError(failure), false, 'the relay request was already written');
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

test('spool offer to a pre-upgrade seed retries the legacy verb while the target beacon is fresh', async t => {
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
  const legacy = await legacyRelaySeed(t, dizNodeId, diz.node.privateKeyPem);
  await ben.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [legacy.endpoint],
  });
  await until(
    () => ben.node.resolveTarget('member:diz', NETWORK).length === 1,
    'sender learns the target card',
  );
  await until(
    () => beaconExpiry(ben.transport, NETWORK, dizNodeId) >= Date.now(),
    'sender learns a fresh target beacon',
  );
  failDirectHop(
    ben.transport,
    Object.assign(new Error('connect EHOSTDOWN'), { code: 'EHOSTDOWN' }),
  );

  // The legacy seed drops the `relay_send` offer, but the fresh beacon proves the NAT-bound
  // target is online. Retrying the original `relay` verb must deliver live, not queue.
  const emitted = await ben.emitter.emit({
    networkId: NETWORK,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'collab.request.v4',
    subject: 'legacy seed live fallback',
    body: 'a fresh beacon means the peer is reachable behind the old seed',
    payload: {},
  });
  assert.equal(emitted.receipts.length, 1);
  assert.equal(emitted.receipts[0]?.stage, 'dispatched');
  assert.equal(emitted.receipts[0]?.fromNodeId, dizNodeId);
  assert.deepEqual(legacy.envelopeRelayTypes, ['relay_send', 'relay']);
  assert.equal(
    ben.listOutbox({ status: 'queued' }).some(item => item.id === emitted.messageId),
    false,
    'a live fallback delivery must not leave a queued obligation',
  );
});

test('spool-enabled send to a pre-upgrade seed queues locally once the target beacon expires', async t => {
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
  const legacy = await legacyRelaySeed(t, dizNodeId, diz.node.privateKeyPem);
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
  // The peer is only genuinely offline once its last beacon has expired; until then the
  // sender must trust the beacon and retry the legacy verb instead of queueing.
  await diz.stop();
  await until(
    () => beaconExpiry(ben.transport, NETWORK, dizNodeId) < Date.now(),
    'the target beacon expires after the peer stops',
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
  assert.deepEqual(item.inDoubtNodeIds, [dizNodeId]);
  assert.deepEqual(item.relaySpooledNodeIds, []);
  assert.deepEqual(legacy.envelopeRelayTypes, ['relay_send']);
});

test('a lost relay spool acknowledgment preserves uncertainty through later direct rejection', async t => {
  const root = directory(t);
  const network = { networkId: NETWORK, passphrase: 'secret' };
  const start = async (member: string, options: NodeOptions = {}) => {
    const runtime = runtimeFor(root, member, { discovery: 'mesh', ...options });
    await runtime.start();
    t.after(() => runtime.stop());
    return runtime;
  };
  const seed = await start('seed', { discovery: 'isolated' });
  await seed.mountNetwork(network);
  const port = portOf(seed.transport.endpointAddr!);
  let spooledId: string | undefined;
  const proxy = createServer(socket => {
    socket.on('error', () => {});
    let buffer = Buffer.alloc(0);
    const readRequest = (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (buffer.length < 4 || buffer.length < 4 + buffer.readUInt32BE(0)) return;
      socket.off('data', readRequest);
      const request = JSON.parse(buffer.subarray(4, 4 + buffer.readUInt32BE(0)).toString('utf8'));
      if (request.type !== 'relay_send') {
        socket.destroy();
        return;
      }
      void tcpRequest(port, request).then(response => {
        if (response.spooled === true) spooledId = response.spoolId;
      }).then(() => socket.end(), error => socket.destroy(error));
    };
    socket.on('data', readRequest);
  });
  await new Promise<void>((resolve, reject) => {
    proxy.once('error', reject);
    proxy.listen(0, '127.0.0.1', resolve);
  });
  t.after(() => new Promise<void>(resolve => proxy.close(() => resolve())));
  const { port: proxyPort } = proxy.address() as { port: number };
  const alice = await start('alice');
  const bob = await start('bob');
  const carol = await start('carol');
  await alice.mountNetwork({ ...network, bootstrapPeers: [`127.0.0.1:${proxyPort}`] });
  await bob.mountNetwork(network);
  await carol.mountNetwork(network);
  const target = bob.node.getSelfCard(NETWORK).nodeId;
  await until(() => ['bob', 'carol'].every(member => alice.node.resolveTarget(`member:${member}`, NETWORK).length === 1),
    'sender learns both destinations');
  await bob.stop();
  const request: EmitMessageInput = {
    networkId: NETWORK, routing: { mode: 'unicast', targets: ['member:bob'] },
    kind: 'question', subject: 'Relay acknowledgment lost', body: 'Original', payload: {}, idempotencyKey: 'lost-ack',
    semantic: { type: 'question', replyMailbox: 'terminal:lost-ack' },
  };
  const first = await alice.emitter.emit(request);
  assert.ok(spooledId, 'the real seed stored a copy before the proxy closed the response');
  assert.deepEqual(alice.outbox.get(first.messageId)?.relaySpooledNodeIds, []);
  assert.deepEqual(alice.outbox.get(first.messageId)?.inDoubtNodeIds, [target]);
  await seed.stop();

  const returning = await start('bob', { patch: { gateMode: 'closed' } });
  await returning.mountNetwork(network);
  await until(() => alice.node.resolveTarget('member:bob', NETWORK)[0]?.endpointAddr === returning.transport.endpointAddr,
    'sender learns the returning target endpoint');
  await alice.outboxRedrive.flush({ id: first.messageId, force: true });
  // The reachability event can settle the rejection before the manual flush runs.
  const failed = alice.outbox.get(first.messageId);
  assert.ok(failed);
  assert.equal(isRetryablePreEffectReceipt(failed.failedReceipts[target]), true);
  assert.deepEqual(failed.inDoubtNodeIds, [target]);
  assert.equal(isUnattemptedOutboxItem(failed), false);
  const changed = { ...request, routing: { mode: 'unicast' as const, targets: ['member:carol'] }, body: 'Revised' };
  assert.equal((await alice.emitter.emit(changed)).messageId, first.messageId);
  assert.equal(carol.mailbox.list().totalCount, 0);

  await returning.gate.setMode('open');
  const restarted = await start('seed');
  await restarted.mountNetwork(network);
  await until(() => returning.mailbox.list().totalCount === 1, 'the restarted seed delivers its persisted copy');
  assert.equal((await alice.emitter.emit(request)).receipts[0]?.stage, 'dispatched');
  assert.deepEqual(alice.outbox.get(first.messageId)?.inDoubtNodeIds, []);
  assert.deepEqual(returning.mailbox.list().items.map(item => item.id), [first.messageId]);
  assert.equal((await alice.emitter.emit(changed)).messageId, first.messageId);
  assert.equal(carol.mailbox.list().totalCount, 0);
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

test('unauthenticated self-relay responses fail locally without blacklisting or queueing', async t => {
  const root = directory(t);
  const endpoint = await rawRelayServer(t, JSON.stringify({ selfRelay: true }));
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
  const harness = await emitterHarness(t, {
    send: (targetNodeId, frame, opts) =>
      client.transport.sendUnicast(NETWORK, targetNodeId, frame, opts),
  });
  let failure: unknown;
  try {
    await harness.emitter.emit({
      networkId: harness.networkId,
      routing: { mode: 'unicast', targets: ['member:diz'] },
      kind: 'collab.request.v1',
      subject: 'spoofed self relay',
      body: 'must never become self-blacklist or offline state',
      payload: {},
    });
  } catch (error) {
    failure = error;
  }
  assert.ok(
    isLocalDeliveryError(failure),
    'an unauthenticated selfRelay response is a deterministic protocol failure',
  );
  assert.match((failure as Error).message, /Unauthenticated self-relay response/);
  assert.equal(
    (client.transport as unknown as { selfEndpoints: Set<string> }).selfEndpoints.has(endpoint),
    false,
    'a spoofed self relay never blacklists the seed',
  );
  assert.equal(
    harness.outbox.list({ status: 'queued' }).length,
    0,
    'a deterministic protocol failure is not an offline obligation',
  );
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

// ---------------------------------------------------------------------------
// Reachability attribution, sleep/wake redrive and self-seed exclusion.
// ---------------------------------------------------------------------------

function portOf(endpoint: string): number {
  return Number(endpoint.slice(endpoint.lastIndexOf(':') + 1));
}

/** One authenticated sealed discovery beacon, the only liveness evidence transport trusts. */
function sealedDiscovery(
  keys: ReturnType<typeof deriveNetworkKeys>,
  senderNodeId: string,
  endpointAddr: string,
  ttlMs = 10_000,
): SealedNetworkFrame {
  return sealNetworkFrame(
    { type: 'discovery', endpointAddr, expires: Date.now() + ttlMs },
    keys,
    senderNodeId,
  );
}

/** Deliver one sealed beacon to a listener exactly as a waking peer's announce would. */
async function sendDiscoveryBeacon(
  port: number,
  frame: SealedNetworkFrame,
  peers: readonly SealedNetworkFrame[] = [],
): Promise<any> {
  return tcpRequest(port, { type: 'discovery', frame, peers: [...peers] });
}

/**
 * Open the peer's real persistent tunnel and answer every forwarded frame with a sealed
 * reply bound to its nonce. `received` records the decoded application payloads in order.
 */
async function openAcceptingTunnel(
  t: TestContext,
  port: number,
  keys: ReturnType<typeof deriveNetworkKeys>,
  targetNodeId: string,
  received: string[],
): Promise<void> {
  const socket = createConnection({ host: '127.0.0.1', port });
  t.after(() => socket.destroy());
  let buffer = Buffer.alloc(0);
  let acknowledge: (() => void) | undefined;
  let acknowledged = false;
  const ready = new Promise<void>((resolve, reject) => {
    acknowledge = resolve;
    socket.once('error', reject);
  });
  socket.on('connect', () => {
    socket.write(framed({
      type: 'tunnel',
      frame: sealNetworkFrame(
        { type: 'discovery', endpointAddr: '127.0.0.1:9', expires: Date.now() + 20_000 },
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
        acknowledge?.();
        continue;
      }
      if (message?.type === 'relay_deliver' && typeof message.relayId === 'string' && message.frame) {
        const inner = unsealNetworkFrame<{ dataBase64?: string }>(message.frame, keys);
        received.push(Buffer.from(inner.dataBase64 ?? '', 'base64').toString('utf8'));
        socket.write(framed({
          type: 'relay_reply',
          relayId: message.relayId,
          frame: sealNetworkFrame(
            {
              requestNonce: message.frame.nonce,
              dataBase64: Buffer.from('delivered').toString('base64'),
            },
            keys,
            targetNodeId,
          ),
        }));
      }
    }
  });
  await ready;
}

/** A TCP forwarder back to this node's own listener: a hairpinned alias of self. */
async function hairpinForwarder(
  t: TestContext,
  targetPort: number,
  delayMs = 0,
): Promise<string> {
  const server = createServer(client => {
    // Hold the request until the delay elapses so a race test can guarantee a real seed
    // completes its probe first; `pipe` resumes the paused socket with buffered bytes.
    client.pause();
    client.on('error', () => {});
    const forward = () => {
      const upstream = createConnection({ host: '127.0.0.1', port: targetPort });
      client.pipe(upstream);
      upstream.pipe(client);
      client.on('error', () => upstream.destroy());
      upstream.on('error', () => client.destroy());
    };
    if (delayMs > 0) {
      setTimeout(forward, delayMs);
    } else {
      forward();
    }
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
    throw new Error('Invalid hairpin address');
  }
  return `127.0.0.1:${address.port}`;
}

function mountOf(
  transport: P2PTransportService,
  networkId: string,
): {
  outboundTunnels: Map<string, { seed: string }>;
  inboundTunnels: Map<string, unknown>;
  relays: Map<string, string>;
  peers: Map<string, string>;
} {
  const mounts = (transport as unknown as {
    mounts: Map<string, {
      outboundTunnels: Map<string, { seed: string }>;
      inboundTunnels: Map<string, unknown>;
      relays: Map<string, string>;
      peers: Map<string, string>;
    }>;
  }).mounts;
  const mount = mounts.get(networkId);
  if (!mount) {
    throw new Error(`Network not mounted: ${networkId}`);
  }
  return mount;
}

test('a sleeping peer behind a zombie relay tunnel is spooled as offline and its causal thread drains on wake', async t => {
  const root = directory(t);
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const seed = new P2PTransportService({
    discoveryDir: join(root, 'seed-discovery'),
    spoolDir: join(root, 'seed-spool'),
    multicast: false,
    socketTimeoutMs: 5_000,
  });
  t.after(() => seed.dispose());
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const seedPort = portOf(seed.endpointAddr!);
  const spoolFile = join(root, 'seed-spool', `${keys.swarmTopicId}.json`);

  const sender = new P2PTransportService({
    discoveryDir: join(root, 'sender-discovery'),
    multicast: false,
    socketTimeoutMs: 5_000,
  });
  t.after(() => sender.dispose());
  await sender.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [seed.endpointAddr!],
  });
  const harness = await emitterHarness(t, {
    send: (targetNodeId, frame, opts) =>
      sender.sendUnicast(NETWORK, targetNodeId, frame, opts),
  });
  const target = harness.targetCard.nodeId;

  // The peer closed its lid after opening a tunnel: the TCP socket stays half-open with an
  // already expired discovery beacon, so no relay_deliver will ever be answered.
  const zombie = createConnection({ host: '127.0.0.1', port: seedPort });
  t.after(() => zombie.destroy());
  await new Promise<void>((resolve, reject) => {
    zombie.once('error', reject);
    let buffer = Buffer.alloc(0);
    zombie.on('connect', () => {
      zombie.write(framed({
        type: 'tunnel',
        frame: sealNetworkFrame(
          { type: 'discovery', endpointAddr: '127.0.0.1:9', expires: Date.now() + 150 },
          keys,
          target,
        ),
      }));
    });
    zombie.on('data', chunk => {
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= 4) {
        const size = buffer.readUInt32BE(0);
        if (buffer.length < size + 4) {
          return;
        }
        const message = JSON.parse(buffer.subarray(4, size + 4).toString('utf8'));
        buffer = buffer.subarray(size + 4);
        if (message?.type === 'tunnel_ack') {
          resolve();
          return;
        }
      }
    });
  });
  await new Promise(resolveStorage => setTimeout(resolveStorage, 250));
  // The tunnel socket is still half-open, but its beacon expired: a stale socket alone is
  // never live reachability evidence.
  assert.equal(seed.isPeerReachable(NETWORK, target), false);

  const threadId = 'thread-a1';
  const emittedIds: string[] = [];
  const startedAt = Date.now();
  for (const label of ['EVT-0002', 'EVT-0003', 'EVT-0004']) {
    const emitted = await harness.emitter.emit({
      networkId: harness.networkId,
      routing: { mode: 'unicast', targets: ['member:diz'] },
      kind: 'collab.request.v1',
      subject: label,
      body: label,
      payload: {},
      semantic: { type: 'reply', threadId },
    });
    assert.equal(emitted.receipts[0]?.stage, 'queued');
    emittedIds.push(emitted.messageId);
  }
  // The relay seed's dead tunnel was evicted instead of hanging the sender, and an offline
  // target behind it is durable spooling evidence, never a possibly-executed in-doubt send.
  assert.ok(
    Date.now() - startedAt < 5_000,
    'the zombie tunnel is bounded by the relay failover window',
  );
  const queued = harness.outbox.list({ status: 'queued' });
  assert.equal(queued.length, 3);
  for (const item of queued) {
    assert.deepEqual(item.pendingNodeIds, [harness.targetCard.nodeId]);
    assert.deepEqual(item.inDoubtNodeIds, []);
    assert.deepEqual(item.relaySpooledNodeIds, [harness.targetCard.nodeId]);
    assert.equal(item.deliveredReceipts[harness.targetCard.nodeId], undefined);
  }
  const spooled = readRelaySpool(spoolFile);
  assert.deepEqual(
    spooled.entries.map(entry => entry.id),
    emittedIds.map(id => `${sender.localNodeId}:${id}`),
    'the causal thread is spooled in order',
  );

  // The peer wakes and reconnects its own tunnel: the seed drains the whole thread in FIFO
  // order, which is only possible because the dead zombie tunnel was evicted above.
  const delivered: string[] = [];
  await openAcceptingTunnel(t, seedPort, keys, target, delivered);
  await until(
    () => readRelaySpool(spoolFile).entries.length === 0,
    'the reconnected peer drains the whole spooled thread',
    10_000,
  );
  assert.deepEqual(
    delivered.map(body => (JSON.parse(body) as { envelope?: { id?: string } }).envelope?.id),
    emittedIds,
  );
});

test('ordinary redrive leaves an unreachable in-doubt thread head and its successors untouched', async t => {
  const root = directory(t);
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'outbox-blobs'));
  const sender = generateNodeKeyPair();
  const target = generateNodeKeyPair().nodeId;
  const ids = ['parked-head', 'queued-successor'];
  for (const id of ids) {
    outbox.enqueue({
      envelope: threadedEnvelope(sender, NETWORK, target, id, 'parked-thread'),
      explicitTargets: [target], pendingNodeIds: [target],
    });
  }
  outbox.recordAttempt(ids[0], {
    nextAttemptAt: new Date(Date.now() - 1_000).toISOString(), inDoubtNodeIds: [target],
  });
  const saved = t.mock.method(outbox as unknown as { save(state: unknown): void }, 'save');
  const deliver = t.mock.fn(async () => ({}));
  const service = new OutboxRedriveService({ outbox, isPeerReachable: () => false, deliver });
  const before = readFileSync(outbox.path, 'utf8');

  for (let count = 0; count < 2; count += 1) {
    const result = await service.flush();
    assert.equal(result.attempted, 0);
    assert.equal(result.stillQueued, 2);
    assert.deepEqual(result.items.map(item => item.id), ids);
  }
  assert.equal(deliver.mock.callCount(), 0);
  assert.equal(saved.mock.callCount(), 0);
  assert.equal(readFileSync(outbox.path, 'utf8'), before);
});

test('an in-doubt send reconciles on its normal schedule once transport proves the peer live without executing twice', async t => {
  const root = directory(t);
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const kernel = new Kernel();
  t.after(() => kernel.stop());
  const transport = new P2PTransportService(
    { discoveryDir: join(root, 'mesh'), multicast: false },
    kernel.ctx,
  );
  t.after(() => transport.dispose());
  await transport.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const port = portOf(transport.endpointAddr!);
  const targetKeyPair = generateNodeKeyPair();
  const target = targetKeyPair.nodeId;
  const sender = generateNodeKeyPair();
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'outbox-blobs'));
  const item = outbox.enqueue({
    envelope: threadedEnvelope(sender, NETWORK, target, 'EVT-A2', 'thread-a2'),
    explicitTargets: [target],
    pendingNodeIds: [target],
  });

  // The receiver's DeliveryLedger/dedup-cas: executing the same signed envelope id twice
  // is impossible, so a redrive answers from the ledger instead of running the handler.
  const executions: string[] = [];
  const ledger = new Map<string, DeliveryReceipt>();
  let dropReply = true;
  const deliver = async (candidate: OutboxItem) => {
    let receipt = ledger.get(candidate.envelope.id);
    if (!receipt) {
      executions.push(candidate.envelope.id);
      receipt = signReceipt(
        {
          messageId: candidate.envelope.id,
          networkId: candidate.networkId,
          fromNodeId: target,
          stage: 'dispatched',
          updatedAt: new Date().toISOString(),
        },
        targetKeyPair.privateKeyPem,
      );
      ledger.set(candidate.envelope.id, receipt);
    }
    if (dropReply) {
      // The target ran, but its reply packet was lost: the sender cannot know that.
      dropReply = false;
      return { pendingNodeIds: [target], inDoubtNodeIds: [target], lastError: 'Transport timeout' };
    }
    return { deliveredReceipts: { [target]: receipt }, pendingNodeIds: [], inDoubtNodeIds: [] };
  };
  const service = new OutboxRedriveService({
    outbox,
    deliver,
    isPeerReachable: (networkId, nodeId) => transport.isPeerReachable(networkId, nodeId),
  });

  await service.flush();
  let current = outbox.get(item.id)!;
  assert.deepEqual(current.inDoubtNodeIds, [target], 'a lost reply after execution is in doubt');
  assert.deepEqual(executions, [item.id]);

  // Due on the ordinary schedule, but transport holds no live evidence for the target.
  outbox.recordAttempt(item.id, { nextAttemptAt: new Date().toISOString() });
  await service.flush();
  assert.deepEqual(executions, [item.id], 'without live evidence the in-doubt target stays parked');
  assert.deepEqual(outbox.get(item.id)!.inDoubtNodeIds, [target]);

  // The peer never restarted (`NodeCard.revision` unchanged): one sealed beacon is the
  // only proof of liveness, and the parked obligation reconciles on its normal backoff.
  await sendDiscoveryBeacon(port, sealedDiscovery(keys, target, '127.0.0.1:9'));
  assert.equal(transport.isPeerReachable(NETWORK, target), true);
  outbox.recordAttempt(item.id, { nextAttemptAt: new Date().toISOString() });
  await service.flush();
  current = outbox.get(item.id)!;
  assert.equal(current.status, 'delivered');
  assert.deepEqual(current.inDoubtNodeIds, []);
  assert.deepEqual(executions, [item.id], 'the receiver ledger answers the duplicate');
  assert.equal(ledger.size, 1);
  await service.stop();
});

test('a sleep/wake beacon pulse with an unchanged identity drains a parked causal thread within 2s', async t => {
  const root = directory(t);
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const kernel = new Kernel();
  t.after(() => kernel.stop());
  const transport = new P2PTransportService(
    { discoveryDir: join(root, 'mesh'), multicast: false },
    kernel.ctx,
  );
  t.after(() => transport.dispose());
  await transport.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  const port = portOf(transport.endpointAddr!);
  const targetKeyPair = generateNodeKeyPair();
  const target = targetKeyPair.nodeId;
  const sender = generateNodeKeyPair();
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'outbox-blobs'));
  const threadId = 'thread-b1';
  const ids = ['EVT-0002', 'EVT-0003', 'EVT-0004'];
  const items = ids.map(id =>
    outbox.enqueue({
      envelope: threadedEnvelope(sender, NETWORK, target, id, threadId),
      explicitTargets: [target],
      pendingNodeIds: [target],
    }),
  );
  // The head is parked in doubt with 60s of backoff: only real reachability may move it,
  // and no forced operator flush is involved anywhere in this test.
  outbox.recordAttempt(items[0].id, {
    nextAttemptAt: new Date(Date.now() + 60_000).toISOString(),
    inDoubtNodeIds: [target],
  });

  const reachability: PeerReachableEvent[] = [];
  const identityPulses: string[] = [];
  kernel.ctx.on('transport/peer-reachable', event => reachability.push(event));
  kernel.ctx.on('node/peer-updated', card => identityPulses.push(card.nodeId));
  const deliveries: string[] = [];
  const service = new OutboxRedriveService({
    outbox,
    intervalMs: 60_000,
    events: {
      on: (event, listener) => kernel.ctx.on(event, listener),
    },
    deliver: async (candidate, options) => {
      const targets = options?.targetNodeIds ?? candidate.pendingNodeIds;
      deliveries.push(candidate.envelope.id);
      return {
        deliveredReceipts: Object.fromEntries(
          targets.map(nodeId => [
            nodeId,
            {
              messageId: candidate.envelope.id,
              networkId: candidate.networkId,
              fromNodeId: nodeId,
              stage: 'dispatched',
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
  service.start();
  t.after(() => service.stop());

  const startedAt = Date.now();
  await sendDiscoveryBeacon(port, sealedDiscovery(keys, target, '127.0.0.1:9'));
  await until(
    () => outbox.list({ status: 'delivered' }).length === 3,
    'the reachability pulse drains the whole causal thread',
    5_000,
  );
  assert.ok(Date.now() - startedAt < 2_000, 'the wake pulse redrives without waiting out backoff');
  assert.deepEqual(deliveries, ids, 'the causal thread is drained in order');
  assert.deepEqual(reachability, [{ networkId: NETWORK, nodeId: target, via: 'direct' }]);
  assert.deepEqual(identityPulses, [], 'identity cards never advance a retry clock');
  assert.equal(transport.isPeerReachable(NETWORK, target), true);

  // A steady-state refresh of the same beacon is not a new reachability transition.
  await sendDiscoveryBeacon(port, sealedDiscovery(keys, target, '127.0.0.1:9', 9_000));
  assert.equal(reachability.length, 1, 'only live transitions pulse; refreshes stay silent');
});

test('a relay seed gossips a waking peer beacon so a cross-subnet obligation redrives without an identity change', async t => {
  const root = directory(t);
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const seedKernel = new Kernel();
  t.after(() => seedKernel.stop());
  const seed = new P2PTransportService(
    { discoveryDir: join(root, 'seed'), multicast: false },
    seedKernel.ctx,
  );
  t.after(() => seed.dispose());
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });

  const kernel = new Kernel();
  t.after(() => kernel.stop());
  const transport = new P2PTransportService(
    { discoveryDir: join(root, 'ben'), multicast: false },
    kernel.ctx,
  );
  t.after(() => transport.dispose());
  await transport.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [seed.endpointAddr!],
  });
  await until(
    () => transport.isPeerReachable(NETWORK, seed.localNodeId),
    'the sender learns the relay seed beacon',
    5_000,
  );

  const targetKeyPair = generateNodeKeyPair();
  const target = targetKeyPair.nodeId;
  const sender = generateNodeKeyPair();
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'outbox-blobs'));
  const threadId = 'thread-b2';
  const ids = ['EVT-0002', 'EVT-0003'];
  const items = ids.map(id =>
    outbox.enqueue({
      envelope: threadedEnvelope(sender, NETWORK, target, id, threadId),
      explicitTargets: [target],
      pendingNodeIds: [target],
    }),
  );
  outbox.recordAttempt(items[0].id, {
    nextAttemptAt: new Date(Date.now() + 60_000).toISOString(),
  });

  const reachability: PeerReachableEvent[] = [];
  kernel.ctx.on('transport/peer-reachable', event => reachability.push(event));
  const deliveries: string[] = [];
  const service = new OutboxRedriveService({
    outbox,
    intervalMs: 60_000,
    events: {
      on: (event, listener) => kernel.ctx.on(event, listener),
    },
    deliver: async (candidate, options) => {
      const targets = options?.targetNodeIds ?? candidate.pendingNodeIds;
      deliveries.push(candidate.envelope.id);
      return {
        deliveredReceipts: Object.fromEntries(
          targets.map(nodeId => [
            nodeId,
            {
              messageId: candidate.envelope.id,
              networkId: candidate.networkId,
              fromNodeId: nodeId,
              stage: 'dispatched',
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
  service.start();
  t.after(() => service.stop());

  // The peer is on an isolated subnet: only the seed can ever see its beacon.
  await sendDiscoveryBeacon(portOf(seed.endpointAddr!), sealedDiscovery(keys, target, '192.0.2.44:9'));
  await until(
    () => reachability.some(event => event.nodeId === target),
    'the seed gossips the waking peer beacon',
    8_000,
  );
  assert.deepEqual(
    reachability.filter(event => event.nodeId === target),
    [{ networkId: NETWORK, nodeId: target, via: 'relay' }],
  );
  await until(
    () => outbox.list({ status: 'delivered' }).length === 2,
    'the gossiped relay pulse redrives the cross-subnet thread',
    5_000,
  );
  assert.deepEqual(deliveries, ids);
  assert.equal(transport.isPeerReachable(NETWORK, target), true);
});

test('steady-state seed probes never fabricate reachability or force-flush parked obligations', async t => {
  const lanAddress = Object.values(networkInterfaces())
    .flatMap(addresses => addresses ?? [])
    .find(address => address.family === 'IPv4' && !address.internal)?.address;
  if (!lanAddress) {
    t.skip('no non-loopback IPv4 interface is available to exercise a direct seed probe');
    return;
  }
  const root = directory(t);
  const seedKernel = new Kernel();
  t.after(() => seedKernel.stop());
  const seed = new P2PTransportService(
    {
      discoveryDir: join(root, 'seed'),
      multicast: false,
      advertisedHost: lanAddress,
    },
    seedKernel.ctx,
  );
  t.after(() => seed.dispose());
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });

  const kernel = new Kernel();
  t.after(() => kernel.stop());
  const transport = new P2PTransportService(
    { discoveryDir: join(root, 'client'), multicast: false, discoveryIntervalMs: 50 },
    kernel.ctx,
  );
  t.after(() => transport.dispose());
  await transport.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [seed.endpointAddr!],
  });
  await until(
    () => transport.isPeerReachable(NETWORK, seed.localNodeId),
    'client learns the non-loopback seed beacon',
    5_000,
  );

  const sender = generateNodeKeyPair();
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'outbox-blobs'));
  // An unresolved selector can only be moved by identity-directory resolution, and the
  // parked in-doubt item can only be moved by an explicit force: a steady-state seed
  // refresh must do neither.
  const unresolved = outbox.enqueue({
    envelope: envelopeFor(sender, NETWORK, 'member:unseen'),
    explicitTargets: ['member:unseen'],
    pendingNodeIds: [],
    unresolvedRoute: true,
  });
  const parked = outbox.enqueue({
    envelope: threadedEnvelope(sender, NETWORK, seed.localNodeId, 'EVT-PARKED', 'thread-parked'),
    explicitTargets: [seed.localNodeId],
    pendingNodeIds: [seed.localNodeId],
  });
  outbox.recordAttempt(parked.id, {
    nextAttemptAt: new Date(Date.now() + 60_000).toISOString(),
    inDoubtNodeIds: [seed.localNodeId],
  });
  const attemptsBefore = {
    unresolved: outbox.get(unresolved.id)!.attempts,
    parked: outbox.get(parked.id)!.attempts,
  };
  const outboxBefore = readFileSync(join(root, 'outbox.json'), 'utf8');

  const reachability: PeerReachableEvent[] = [];
  kernel.ctx.on('transport/peer-reachable', event => reachability.push(event));
  const deliveries: string[] = [];
  const service = new OutboxRedriveService({
    outbox,
    intervalMs: 60_000,
    events: {
      on: (event, listener) => kernel.ctx.on(event, listener),
    },
    deliver: async candidate => {
      deliveries.push(candidate.envelope.id);
      return {
        pendingNodeIds: candidate.pendingNodeIds,
        inDoubtNodeIds: candidate.inDoubtNodeIds,
      };
    },
  });
  service.start();
  t.after(() => service.stop());

  // At least two full seed-probe cycles (rate-limited to 2s) with nothing changing.
  await new Promise(resolveProbe => setTimeout(resolveProbe, 4_600));
  assert.equal(
    transport.isPeerReachable(NETWORK, seed.localNodeId),
    true,
    'every probe cycle actually reached the non-loopback seed',
  );
  assert.deepEqual(reachability, [], 'a steady-state seed refresh is not a reachability transition');
  assert.equal(
    mountOf(transport, NETWORK).relays.has(seed.localNodeId),
    false,
    'a seed is never mapped as its own relay',
  );
  assert.deepEqual(deliveries, [], 'no parked obligation is force-flushed by a probe');
  assert.equal(outbox.get(unresolved.id)!.attempts, attemptsBefore.unresolved);
  assert.equal(outbox.get(parked.id)!.attempts, attemptsBefore.parked);
  assert.equal(
    readFileSync(join(root, 'outbox.json'), 'utf8'),
    outboxBefore,
    'steady-state probes never rewrite outbox.json',
  );
});

test('a reachability pulse never reroutes unresolved selectors while an identity pulse does', async t => {
  const harness = await emitterHarness(t, { extraMembers: ['unseen'] });
  const unseen = harness.targetByMember.get('unseen')!;
  const item = harness.outbox.enqueue({
    envelope: envelopeFor(harness.sender, harness.networkId, 'member:unseen'),
    explicitTargets: ['member:unseen'],
    pendingNodeIds: [],
    unresolvedRoute: true,
  });
  const parked = harness.outbox.recordAttempt(item.id, {
    nextAttemptAt: new Date(Date.now() + 60_000).toISOString(),
  })!;
  assert.ok(harness.emitter.redriveOutboxItem, 'emitter owns redrive delivery');
  const service = new OutboxRedriveService({
    outbox: harness.outbox,
    intervalMs: 60_000,
    events: {
      on: (event, listener) => harness.kernel.ctx.on(event, listener),
    },
    deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options),
  });
  service.start();
  t.after(() => service.stop());

  // Transport liveness for an unrelated peer cannot resolve `member:unseen`.
  harness.kernel.ctx.emit('transport/peer-reachable', {
    networkId: harness.networkId,
    nodeId: generateNodeKeyPair().nodeId,
    via: 'direct',
  });
  await new Promise(resolvePulse => setTimeout(resolvePulse, 100));
  assert.equal(harness.outbox.get(item.id)!.attempts, parked.attempts);
  assert.equal(harness.outbox.get(item.id)!.status, 'queued');
  assert.deepEqual(harness.sendCalls, [], 'the unresolved selector is not force-routed');

  // The identity directory learning the member is what resolves and delivers it.
  harness.kernel.ctx.emit('node/peer-updated', unseen, { direct: true });
  await until(
    () => harness.outbox.get(item.id)!.status === 'delivered',
    'the identity pulse resolves the unresolved selector',
    5_000,
  );
  assert.deepEqual(
    harness.sendCalls.map(call => call.targetNodeId),
    [unseen.nodeId],
    'identity resolution routes the unresolved selector to the learned member',
  );
  assert.deepEqual(
    Object.keys(harness.outbox.get(item.id)!.deliveredReceipts),
    [unseen.nodeId],
  );
});

test('bootstrapPeers with self aliases and a hairpin forwarder never self-tunnel or self-spool', async t => {
  const root = directory(t);
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const kernel = new Kernel();
  t.after(() => kernel.stop());
  const transport = new P2PTransportService(
    {
      discoveryDir: join(root, 'mesh'),
      spoolDir: join(root, 'spool'),
      multicast: false,
    },
    kernel.ctx,
  );
  t.after(() => transport.dispose());
  // Start the listener on a throwaway network first so the self aliases are known.
  await transport.mountNetwork({ networkId: 'warmup-net', passphrase: 'secret' });
  const port = portOf(transport.endpointAddr!);
  const lanAddress = Object.values(networkInterfaces())
    .flatMap(addresses => addresses ?? [])
    .find(address => address.family === 'IPv4' && !address.internal)?.address;
  const hairpin = await hairpinForwarder(t, port);
  const bootstrapPeers = [
    transport.endpointAddr!,
    `127.0.0.1:${port}`,
    ...(lanAddress ? [`${lanAddress}:${port}`] : []),
    hairpin,
  ];
  await transport.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers,
  });
  // Two full seed-probe rounds: every self alias must stay filtered on every round.
  await new Promise(resolveProbe => setTimeout(resolveProbe, 2_600));

  const mount = mountOf(transport, NETWORK);
  assert.equal(mount.outboundTunnels.size, 0, 'no outbound tunnel is opened to self');
  assert.equal(mount.inboundTunnels.size, 0, 'no inbound tunnel is registered from self');
  assert.equal(mount.relays.has(transport.localNodeId), false, 'self is never a relay seed');
  assert.equal(mount.peers.has(transport.localNodeId), false, 'self is never a peer endpoint');
  const selfEndpoints = (transport as unknown as { selfEndpoints: Set<string> }).selfEndpoints;
  assert.equal(selfEndpoints.has(transport.endpointAddr!), true);
  assert.equal(selfEndpoints.has(hairpin), true, 'the hairpinned forwarder is proven to be self');

  // The target is offline and every configured "seed" is self: queue locally, never claim
  // a durable spool handoff on this node itself.
  let probeSelfRelay = true;
  const harness = await emitterHarness(t, {
    send: (targetNodeId, frame, opts) => {
      if (probeSelfRelay) {
        // Force this send to confirm the hairpin via the relay response itself.
        probeSelfRelay = false;
        selfEndpoints.delete(hairpin);
      }
      return transport.sendUnicast(NETWORK, targetNodeId, frame, opts);
    },
  });
  const selfOnly = await harness.emitter.emit({
    networkId: harness.networkId,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'collab.request.v1',
    subject: 'self-only seed list',
    body: 'must stay a local obligation',
    payload: {},
  });
  assert.equal(selfOnly.receipts[0]?.stage, 'queued');
  assert.equal(selfOnly.receipts[0]?.executionReceipt?.relaySpooled, false);
  const selfOnlyItem = harness.outbox.get(selfOnly.messageId)!;
  assert.deepEqual(selfOnlyItem.inDoubtNodeIds, []);
  assert.deepEqual(selfOnlyItem.relaySpooledNodeIds, []);
  assert.equal(selfEndpoints.has(hairpin), true, 'the relay response proved that no external hop began');
  const spoolFile = join(root, 'spool', `${keys.swarmTopicId}.json`);
  assert.equal(
    existsSync(spoolFile) ? readRelaySpool(spoolFile).entries.length : 0,
    0,
    'no self relay-spool entry is ever written',
  );

  // A real seed configured behind the self aliases must still be reached: the static
  // filter skips self, and the runtime hairpin proof removes the forwarder mid-probe.
  const realSeed = new P2PTransportService({
    discoveryDir: join(root, 'seed-discovery'),
    spoolDir: join(root, 'seed-spool'),
    multicast: false,
    socketTimeoutMs: 5_000,
  });
  t.after(() => realSeed.dispose());
  await realSeed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  await transport.unmountNetwork(NETWORK);
  await transport.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [...bootstrapPeers, realSeed.endpointAddr!],
  });
  const throughRealSeed = await harness.emitter.emit({
    networkId: harness.networkId,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'collab.request.v1',
    subject: 'real seed behind self',
    body: 'must spool on the real seed',
    payload: {},
  });
  assert.equal(throughRealSeed.receipts[0]?.stage, 'queued');
  assert.equal(throughRealSeed.receipts[0]?.executionReceipt?.relaySpooled, true);
  const throughItem = harness.outbox.get(throughRealSeed.messageId)!;
  assert.deepEqual(throughItem.relaySpooledNodeIds, [harness.targetCard.nodeId]);
  assert.deepEqual(throughItem.inDoubtNodeIds, []);
  assert.deepEqual(
    readRelaySpool(join(root, 'seed-spool', `${keys.swarmTopicId}.json`)).entries.map(
      entry => entry.id,
    ),
    [`${transport.localNodeId}:${throughRealSeed.messageId}`],
    'the frame is spooled on the real seed only',
  );
  assert.equal(
    existsSync(spoolFile) ? readRelaySpool(spoolFile).entries.length : 0,
    0,
    'the hairpinned self endpoint never stores its own frame',
  );

  // An alias that only appears after mount (a stale relay mapping) cannot be filtered
  // statically. The wire response proves it is self, and the send still falls through to
  // the real seed instead of reporting a self-spooled handoff.
  const lateHairpin = await hairpinForwarder(t, port);
  mountOf(transport, NETWORK).relays.set(harness.targetCard.nodeId, lateHairpin);
  const throughLateAlias = await harness.emitter.emit({
    networkId: harness.networkId,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'collab.request.v1',
    subject: 'late hairpin alias',
    body: 'must be rejected by the wire protocol',
    payload: {},
  });
  assert.equal(throughLateAlias.receipts[0]?.stage, 'queued');
  assert.equal(throughLateAlias.receipts[0]?.executionReceipt?.relaySpooled, true);
  assert.ok(
    (transport as unknown as { selfEndpoints: Set<string> }).selfEndpoints.has(lateHairpin),
    'the wire response proves the alias is this node',
  );
  assert.ok(
    readRelaySpool(join(root, 'seed-spool', `${keys.swarmTopicId}.json`)).entries.some(
      entry => entry.id === `${transport.localNodeId}:${throughLateAlias.messageId}`,
    ),
    'the send advances past the self alias and spools on the real seed',
  );
  assert.equal(
    existsSync(spoolFile) ? readRelaySpool(spoolFile).entries.length : 0,
    0,
    'the late self alias never stores its own frame',
  );
});

test('a hairpinned self probe still proves itself when a real seed wins the probe round', async t => {
  const root = directory(t);
  const kernel = new Kernel();
  t.after(() => kernel.stop());
  const transport = new P2PTransportService(
    { discoveryDir: join(root, 'client'), multicast: false },
    kernel.ctx,
  );
  t.after(() => transport.dispose());
  // Warm up on a throwaway network so the forwarder can point back at this listener.
  await transport.mountNetwork({ networkId: 'warmup-net', passphrase: 'secret' });
  const hairpin = await hairpinForwarder(t, portOf(transport.endpointAddr!), 250);
  const realSeed = new P2PTransportService({
    discoveryDir: join(root, 'seed'),
    multicast: false,
  });
  t.after(() => realSeed.dispose());
  await realSeed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });

  const reachability: PeerReachableEvent[] = [];
  kernel.ctx.on('transport/peer-reachable', event => reachability.push(event));
  // The real seed answers within milliseconds while the hairpin holds its request for
  // 250ms: a per-seed nonce deletion would let the fast probe invalidate the slow one.
  await transport.mountNetwork({
    networkId: NETWORK,
    passphrase: 'secret',
    bootstrapPeers: [realSeed.endpointAddr!, hairpin],
  });
  assert.equal(
    (transport as unknown as { selfEndpoints: Set<string> }).selfEndpoints.has(hairpin),
    true,
    'the hairpinned forwarder is still proven to be this node',
  );
  assert.equal(transport.isPeerReachable(NETWORK, realSeed.localNodeId), true);
  assert.equal(
    mountOf(transport, NETWORK).relays.has(transport.localNodeId),
    false,
    'self is never mapped as a relay seed',
  );
  assert.equal(
    reachability.filter(event => event.nodeId === transport.localNodeId).length,
    0,
    'the self probe never becomes a peer reachability pulse',
  );
});

test('a mounted transport purges pre-existing self-spooled snapshot and journal entries', async t => {
  const root = directory(t);
  const keys = deriveNetworkKeys(NETWORK, 'secret');
  const selfId = generateNodeKeyPair().nodeId;
  const remoteSender = generateNodeKeyPair().nodeId;
  const remoteTarget = generateNodeKeyPair().nodeId;
  const spoolDir = join(root, 'spool');
  mkdirSync(spoolDir, { recursive: true });
  const spoolFile = join(spoolDir, `${keys.swarmTopicId}.json`);
  const now = Date.now();
  const expiry = new Date(now + 86_400_000).toISOString();
  const selfAsSender = {
    id: `${selfId}:self-spool`,
    targetNodeId: remoteTarget,
    networkId: NETWORK,
    frame: sealNetworkFrame({ payload: 'self-spooled' }, keys, selfId),
    spooledAt: new Date(now - 2_000).toISOString(),
    expiresAt: expiry,
  };
  const selfAsTarget = {
    id: `${remoteSender}:incoming-self`,
    targetNodeId: selfId,
    networkId: NETWORK,
    frame: sealNetworkFrame({ payload: 'incoming-self' }, keys, remoteSender),
    spooledAt: new Date(now - 1_000).toISOString(),
    expiresAt: expiry,
  };
  const legitimate = {
    id: `${remoteSender}:legitimate`,
    targetNodeId: remoteTarget,
    networkId: NETWORK,
    frame: sealNetworkFrame({ payload: 'legitimate' }, keys, remoteSender),
    spooledAt: new Date(now - 500).toISOString(),
    expiresAt: expiry,
  };
  writeFileSync(
    spoolFile,
    JSON.stringify({
      schema: 'the-dealer-relay-spool/v1',
      networkId: NETWORK,
      entries: [selfAsSender, selfAsTarget],
    }),
  );
  writeFileSync(
    `${spoolFile}.journal`,
    `${JSON.stringify({ op: 'upsert', entry: selfAsSender })}\n` +
      `${JSON.stringify({ op: 'upsert', entry: legitimate })}\n`,
  );

  const transport = new P2PTransportService({
    localNodeId: selfId,
    discoveryDir: join(root, 'mesh'),
    spoolDir,
    multicast: false,
  });
  t.after(() => transport.dispose());
  await transport.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });

  const saved = readRelaySpool(spoolFile);
  assert.deepEqual(
    saved.entries.map(entry => entry.id),
    [legitimate.id],
    'only the entry with two real remote endpoints survives',
  );
  assert.equal(
    existsSync(`${spoolFile}.journal`),
    false,
    'the dropped self entries are compacted into one clean snapshot',
  );
});

test('prepare keys claim one sender obligation for resolved and unresolved targets', async t => {
  for (const selector of ['member:diz', 'member:unknown']) {
    const harness = await emitterHarness(t, {
      plan: target => ({ error: markBeforeSendError(new PeerUnreachableError(target)) }),
    });
    harness.kernel.ctx.on('emitter/prepare', envelope => ({ ...envelope, idempotencyKey: 'topic-request' }));
    const input: EmitMessageInput = {
      networkId: harness.networkId, routing: { mode: 'unicast', targets: [selector] },
      kind: 'task', subject: 'prepare key', body: 'one request', payload: {},
    };
    const [first, second] = await Promise.all([harness.emitter.emit(input), harness.emitter.emit(input)]);
    assert.equal(first.messageId, second.messageId);
    assert.equal(harness.outbox.list({ status: 'all' }).length, 1);
    assert.equal(harness.sendCalls.length, selector === 'member:diz' ? 1 : 0);
    assert.equal(harness.outbox.list({ status: 'all' })[0].envelope.idempotencyKey, 'topic-request');
    await assert.rejects(harness.emitter.emit({ ...input, idempotencyKey: 'caller-key' }),
      /Prepare hook cannot change caller idempotencyKey/);
  }
});

test('prepare rejects blank injected keys and deletion of caller keys', async t => {
  for (const key of ['', '   ', undefined]) {
    const harness = await emitterHarness(t);
    harness.kernel.ctx.on('emitter/prepare', envelope => ({ ...envelope, idempotencyKey: key }));
    const input: EmitMessageInput = {
      networkId: harness.networkId, routing: { mode: 'unicast', targets: ['member:diz'] },
      kind: 'task', subject: 'invalid key', body: 'no send', payload: {},
    };
    await assert.rejects(harness.emitter.emit({ ...input, idempotencyKey: 'caller-key' }),
      /Prepare hook cannot change caller idempotencyKey/);
    if (key !== undefined) await assert.rejects(harness.emitter.emit(input), /non-empty string/);
    for (const callerKey of ['', '   ']) {
      await assert.rejects(harness.emitter.emit({ ...input, idempotencyKey: callerKey }),
        /idempotencyKey must be a non-empty string/);
    }
    assert.equal(harness.sendCalls.length, 0);
  }
});

test('send CLI forwards idempotency keys and rejects blank keys', async t => {
  const runtime = runtimeFor(directory(t), 'ben');
  await runtime.start();
  t.after(() => runtime.stop());
  const args = ['send', '--network', 'local', '--to', 'member:missing', '--topic', 'request',
    '--body', 'retry me', '--json', '--idempotency-key'];
  const send = async (key: string) => {
    const lines: string[] = [];
    const errors: string[] = [];
    const code = await runDealerCli([...args, key], {
      runtime, stdout: line => lines.push(line), stderr: line => errors.push(line),
    });
    return { code, lines, errors };
  };
  const first = await send('request-key');
  const second = await send('request-key');
  assert.equal(first.code, 0);
  assert.equal(second.code, 0);
  assert.equal(JSON.parse(first.lines.join('\n')).messageId, JSON.parse(second.lines.join('\n')).messageId);
  assert.equal(runtime.outbox.list({ status: 'all' })[0].envelope.idempotencyKey, 'request-key');
  for (const key of ['', '   ']) {
    const invalid = await send(key);
    assert.equal(invalid.code, 1);
    assert.match(invalid.errors.join('\n'), /--idempotency-key/);
  }
  assert.equal(runtime.outbox.list({ status: 'all' }).length, 1);
});

test('initial emit and redrive launch live siblings before the first target times out', async t => {
  let stalled: string;
  let offline = false;
  const harness = await emitterHarness(t, {
    extraMembers: ['aya'], attemptTimeoutMs: 100,
    plan: target => offline ? { error: markBeforeSendError(new PeerUnreachableError(target)) }
      : target === stalled ? { delayMs: 1000 } : {},
  });
  const targets = harness.targetCards.map(card => card.nodeId).sort();
  stalled = targets[0];
  const live = targets[1];
  const input: EmitMessageInput = {
    networkId: harness.networkId, routing: { mode: 'multicast', targets },
    kind: 'task', subject: 'live sibling', body: 'must start concurrently', payload: {},
  };
  const emitted = await harness.emitter.emit(input);
  assert.equal(harness.sendCalls.length, 2);
  assert.ok(harness.sendCalls[1].opts!.timeoutMs! > 50, 'live sibling receives its own send budget');
  assert.equal(emitted.receipts.find(receipt => receipt.fromNodeId === live)?.stage, 'dispatched');
  const initial = harness.outbox.get(emitted.messageId)!;
  assert.deepEqual(initial.pendingNodeIds, [stalled]);
  assert.deepEqual(initial.inDoubtNodeIds, [stalled]);

  offline = true;
  const queued = await harness.emitter.emit({ ...input, subject: 'redrive sibling' });
  offline = false;
  const before = harness.sendCalls.length;
  const claim = harness.outbox.beginAttempt(queued.messageId)!;
  const outcome = await harness.emitter.redriveOutboxItem!(claim.item, { timeoutMs: 100, attemptToken: claim.attemptToken })
    .finally(() => harness.outbox.releaseAttempt(claim.item.id, claim.attemptToken));
  assert.equal(harness.sendCalls.length - before, 2);
  assert.ok(harness.sendCalls[before + 1].opts!.timeoutMs! > 50);
  assert.equal(outcome.deliveredReceipts?.[live]?.stage, 'dispatched');
  assert.deepEqual(outcome.pendingNodeIds, [stalled]);
  assert.deepEqual(outcome.inDoubtNodeIds, [stalled]);
});


for (const hop of ['direct', 'relay']) test(`${hop} pre-connect handshake timeout stays retryable through ordinary outbox redrive`, async t => {
  const harness = await emitterHarness(t, { attemptTimeoutMs: 30 });
  const transport = new P2PTransportService({ discoveryDir: join(harness.dir, 'isolated'), multicast: false });
  await transport.mountNetwork({ networkId: harness.networkId, passphrase: 'offline-test-only',
    ...(hop === 'relay' ? { bootstrapPeers: ['127.0.0.1:1'] } : {}) });
  t.after(() => transport.dispose());
  if (hop === 'direct') transport.registerPeerEndpoint(harness.networkId, harness.targetCard.nodeId, '127.0.0.1:1');
  const connect = t.mock.method(Socket.prototype, 'connect', function (this: Socket) { return this; });
  const send = t.mock.method(harness.kernel.ctx.services.transport, 'sendUnicast',
    (...args: Parameters<TransportService['sendUnicast']>) => transport.sendUnicast(...args));
  const emitted = await harness.emitter.emit({
    networkId: harness.networkId, routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'task', subject: 'connect timeout', body: 'no bytes sent', payload: {},
  });
  assert.ok(connect.mock.callCount() > 0);
  connect.mock.restore();
  send.mock.restore();
  const item = harness.outbox.get(emitted.messageId)!;
  assert.deepEqual(item.pendingNodeIds, [harness.targetCard.nodeId]);
  assert.deepEqual(item.inDoubtNodeIds, []);
  const redrive = new OutboxRedriveService({ outbox: harness.outbox,
    deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options) });
  await redrive.flush({ id: item.id });
  assert.equal(harness.outbox.get(item.id)?.status, 'delivered');
  await redrive.stop();
});

test('terminal keyed delivery replays its signed receipt before auto-routing can shift peers', async t => {
  const harness = await emitterHarness(t, { extraMembers: ['aya'] });
  let selected = harness.targetCard.nodeId;
  let routes = 0;
  harness.kernel.ctx.services.router.registerJudge({ name: 'availability', priority: 1,
    async decide() { routes++; return { mode: 'unicast', targetNodeIds: [selected], matchedBy: 'judge', reason: 'available' }; } });
  const input: EmitMessageInput = { networkId: harness.networkId, routing: { mode: 'auto' },
    kind: 'task', subject: 'keyed auto', body: 'execute once', payload: {}, idempotencyKey: 'stable-auto' };
  const first = await harness.emitter.emit(input);
  const confirmed = harness.outbox.list({ status: 'delivered' })[0];
  const history = Array.from({ length: 201 }, () => {
    const envelope = envelopeFor(harness.sender, harness.networkId, 'member:diz');
    const receipt = signReceipt({ ...first.receipts[0], messageId: envelope.id }, harness.receiver.privateKeyPem);
    return {
      ...confirmed, id: envelope.id, envelope, updatedAt: '2099-01-01T00:00:00.000Z',
      deliveredReceipts: { [receipt.fromNodeId]: receipt },
    };
  });
  writeFileAtomically(harness.outbox.path, JSON.stringify({
    schema: 'the-dealer-outbox/v2', items: [confirmed, ...history.slice(0, 200)],
  }));
  const { envelope, explicitTargets } = history[200];
  const receipt = history[200].deliveredReceipts[harness.targetCard.nodeId];
  harness.outbox.enqueue({ envelope, explicitTargets, pendingNodeIds: [harness.targetCard.nodeId] });
  harness.outbox.markNodeDelivered(envelope.id, harness.targetCard.nodeId, receipt);
  selected = harness.targetByMember.get('aya')!.nodeId;
  const second = await harness.emitter.emit(input);
  assert.equal(routes, 1);
  assert.equal(harness.sendCalls.length, 1);
  assert.deepEqual(second, first);
  assert.equal(harness.outbox.list({ status: 'delivered' }).length, 201);
  assert.equal(second.receipts[0].stage, 'dispatched');
  assert.equal(second.receipts[0].fromNodeId, harness.targetCard.nodeId);
});

test('outbox replacement requires positive pre-effect evidence for every failed target', () => {
  const unattempted = { stage: 'failed', executionReceipt: { handoff: 'unattempted' } };
  const item = { status: 'failed', failedNodeIds: ['a'], failedReceipts: { a: unattempted } };
  assert.equal(isUnattemptedOutboxItem(item), true);
  assert.equal(isUnattemptedOutboxItem({ ...item, failedNodeIds: ['a', 'local'], unattemptedNodeIds: ['local'] }), true);
  assert.equal(isUnattemptedOutboxItem({ ...item, failedNodeIds: [], failedReceipts: {}, unattemptedFailure: true }), true);
  const boundItem = {
    ...item, envelope: { signature: 'original' },
    failedReceipts: { a: { stage: 'failed', executionReceipt: { handoff: 'unattempted', envelopeSignature: 'other' } } },
  };
  assert.equal(isUnattemptedOutboxItem(boundItem), false);
  assert.equal(isUnattemptedOutboxItem({ ...boundItem, envelope: { signature: 'other' } }), true);
  const unannotated = { stage: 'rejected', detail: 'custom' };
  assert.equal(isRetryablePreEffectReceipt(unannotated), false);
  for (const blocked of [
    { ...item, failedNodeIds: ['a', 'unsigned'] },
    { ...item, pendingNodeIds: ['a'] },
    { ...item, inDoubtNodeIds: ['a'] },
    { ...item, supersededById: 'replacement' },
    { ...item, deliveredReceipts: { b: { stage: 'dispatched' } } },
    { ...item, failedReceipts: { a: { stage: 'failed' } } },
    { ...item, failedReceipts: { a: { stage: 'failed', executionReceipt: { handoff: 'unknown' } } } },
    { ...item, failedReceipts: { a: { stage: 'dispatching' } } },
    { ...item, failedReceipts: { a: unannotated } },
    { ...item, failedReceipts: { a: { stage: 'rejected', detail: DEDUP_CAS_AMBIGUOUS_DETAIL } } },
  ]) assert.equal(isUnattemptedOutboxItem(blocked), false);
  for (const handoff of ['settled', 'queued', 'dispatched', 'unknown']) {
    const receipt = { stage: 'rejected', executionReceipt: { handoff, durable: false } };
    for (const detail of ['Gate closed', DEDUP_CAS_DUPLICATE_DETAIL]) {
      const failure = { ...receipt, detail };
      assert.equal(isRetryablePreEffectReceipt(failure), false);
      assert.equal(isUnattemptedOutboxItem({ ...item, failedReceipts: { a: failure } }), false);
    }
  }
});

test('a receipt bound to a different envelope cannot authorize outbox replacement', async t => {
  const harness = await emitterHarness(t, { plan: () => ({
    stage: 'rejected', executionReceipt: { handoff: 'unattempted', envelopeSignature: 'other-envelope' },
  }) });
  const input: EmitMessageInput = {
    networkId: harness.networkId, routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'wave', subject: 'wave', body: 'slot 2', payload: {}, idempotencyKey: 'receipt-binding',
  };
  await assert.rejects(harness.emitter.emit(input), /Invalid or mismatched delivery receipt/);
  const [original] = harness.outbox.list();
  assert.equal(isUnattemptedOutboxItem(original), false);
  await assert.rejects(harness.emitter.emit({ ...input, body: 'slot 1' }), /Invalid or mismatched delivery receipt/);
  assert.deepEqual(harness.sendCalls.map(call => call.envelope.id), [original.id]);
  assert.deepEqual(harness.outbox.list().map(item => item.id), [original.id]);
});

for (const handoff of ['unattempted', 'unknown'] as const) {
  test(`keyed waves retarget only with ${handoff} failure evidence`, async t => {
    let ready = false;
    const harness = await emitterHarness(t, {
      plan: () => ready ? {} : { stage: 'failed', executionReceipt: { handoff } },
    });
    const file = join(harness.dir, 'wave.txt');
    writeFileSync(file, 'wave evidence');
    const input: EmitMessageInput = { networkId: harness.networkId,
      routing: { mode: 'unicast', targets: ['member:diz'] },
      kind: 'wave', subject: 'wave', body: 'slot 2', payload: { streamKey: 'alerts:2', revision: 2 },
      idempotencyKey: 'same-wave', semantic: { type: 'message' }, filePaths: [file] };
    const first = await harness.emitter.emit(input);
    const before = harness.outbox.get(first.messageId)!;
    assert.equal(before.status, 'failed');
    if (handoff === 'unattempted') {
      assert.equal((await harness.emitter.emit(input)).messageId, first.messageId);
      assert.equal(harness.sendCalls.length, 2);
    }

    ready = true;
    const retry = await harness.emitter.emit({ ...input, body: 'slot 1', payload: { revision: 2, streamKey: 'alerts:1' } });
    if (handoff === 'unattempted') {
      assert.notEqual(retry.messageId, first.messageId);
      assert.equal(retry.receipts[0].stage, 'dispatched');
      assert.equal(harness.sendCalls.at(-1)?.envelope.payload.streamKey, 'alerts:1');
      assert.equal(harness.sendCalls.at(-1)?.envelope.body, 'slot 1');
      assert.ok(Object.values(before.stagedFiles).every(path => !existsSync(path)));
    } else {
      assert.deepEqual(retry, first);
      assert.equal(harness.sendCalls.length, 1);
      assert.ok(Object.values(before.stagedFiles).every(path => existsSync(path)));
    }
    assert.deepEqual(harness.outbox.list().filter(item => !item.supersededById).map(item => item.id), [retry.messageId]);
    if (handoff === 'unattempted') assert.equal(harness.outbox.get(first.messageId)?.supersededById, retry.messageId);
  });
}

test('online-only keyed replacements retain approval or uncertain custody without offline queueing', async t => {
  for (const outcome of ['approval', 'volatile', 'offline'] as const) {
    const harness = await emitterHarness(t, {
      extraMembers: ['carol'],
      plan: target => {
        if (target === harness.targetCard.nodeId) {
          return { stage: 'rejected', executionReceipt: { handoff: 'unattempted' } };
        }
        if (outcome === 'approval') return { stage: 'pending_approval' };
        if (outcome === 'volatile') return { stage: 'queued', executionReceipt: { durable: false } };
        return { error: new PeerUnreachableError(target, 'Connection closed after write') };
      },
    });
    const input: EmitMessageInput = {
      networkId: harness.networkId, routing: { mode: 'unicast', targets: ['member:diz'] },
      kind: 'wave', subject: 'wave', body: 'original', payload: {}, idempotencyKey: 'online-replacement',
    };
    const original = await harness.emitter.emit(input);
    const emitReplacement = () => harness.emitter.emit({
      ...input, routing: { mode: 'unicast', targets: ['member:carol'] }, body: 'replacement', queueIfOffline: false,
    });
    if (outcome === 'offline') {
      await assert.rejects(emitReplacement(), isPeerUnreachableError);
    } else {
      const result = await emitReplacement();
      assert.deepEqual(result.receipts.map(receipt => receipt.stage), [outcome === 'approval' ? 'pending_approval' : 'queued']);
    }
    const carol = harness.sendCalls[1].targetNodeId;
    const active = harness.outbox.get(harness.outbox.get(original.messageId)!.supersededById!)!;
    assert.equal(harness.sendCalls[1].opts?.spoolIfOffline, undefined);
    assert.equal(isUnattemptedOutboxItem(active), false);
    if (outcome === 'approval') {
      assert.equal(active.status, 'queued');
      assert.deepEqual(active.pendingNodeIds, [carol]);
      assert.deepEqual(active.approvalPendingNodeIds, [carol]);
    } else {
      assert.equal(active.status, 'failed');
      assert.deepEqual(active.pendingNodeIds, []);
      assert.deepEqual(active.inDoubtNodeIds, [carol]);
    }
  }
});

test('a gate rejection after a timed-out send retains uncertainty and forbids replacement', async t => {
  const harness = await emitterHarness(t, {
    plan: (target, index) => index === 0
      ? { error: new PeerUnreachableError(target, 'Transport timeout', { cause: new DeliveryTimeoutError('Transport timeout') }) }
      : { stage: 'rejected', detail: 'Gate closed', executionReceipt: { handoff: 'unattempted' } },
  });
  const first = await harness.emitter.emit({
    networkId: harness.networkId, routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'wave', subject: 'wave', body: 'slot 2', payload: {}, idempotencyKey: 'timed-out-wave',
  });
  const target = harness.targetCard.nodeId;
  assert.deepEqual(harness.outbox.get(first.messageId)?.inDoubtNodeIds, [target]);
  const service = new OutboxRedriveService({ outbox: harness.outbox, deliver: harness.emitter.redriveOutboxItem! });
  assert.equal((await service.flush({ force: true })).attempted, 1);
  const failed = harness.outbox.get(first.messageId)!;
  assert.equal(failed.status, 'failed');
  assert.equal(failed.failedReceipts[target].stage, 'rejected');
  assert.deepEqual(failed.inDoubtNodeIds, [target]);
  assert.equal(isUnattemptedOutboxItem(failed), false);
  const replacement = harness.outbox.replaceUnattemptedIdempotency({
    envelope: signEnvelope({ ...failed.envelope, id: randomUUID(), body: 'slot 1' }, harness.sender.privateKeyPem),
    explicitTargets: failed.explicitTargets,
  });
  assert.equal(replacement?.claimed, false);
  assert.deepEqual(harness.outbox.list().map(item => item.id), [first.messageId]);
});

test('a direct pre-effect rejection leaves the disconnected relay copy in custody until execution', async t => {
  let receiver: 'offline' | 'closed' | 'queued' | 'open' = 'offline';
  let executions = 0;
  const harness = await emitterHarness(t, {
    extraMembers: ['carol'],
    plan: target => {
      if (receiver === 'offline') return { error: new OfflinePeerSpooledError(target, 'seed', 'spooled-copy') };
      if (receiver === 'closed') return { stage: 'rejected', executionReceipt: { handoff: 'unattempted' } };
      if (receiver === 'queued') return { stage: 'queued', executionReceipt: { durable: true } };
      executions++;
      return { stage: 'dispatched' };
    },
  });
  const session = 'terminal:relay-copy';
  const input: EmitMessageInput = {
    networkId: harness.networkId, routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'wave', subject: 'wave', body: 'original', payload: {}, idempotencyKey: 'spooled-obligation',
    semantic: { type: 'question', replyMailbox: session },
  };
  const first = await harness.emitter.emit(input);
  const target = harness.targetCard.nodeId;
  assert.deepEqual(harness.outbox.get(first.messageId)?.relaySpooledNodeIds, [target]);
  receiver = 'closed';
  const redrive = new OutboxRedriveService({ outbox: harness.outbox, deliver: harness.emitter.redriveOutboxItem! });
  const failed = (await redrive.flush({ force: true })).items[0];
  assert.equal(isRetryablePreEffectReceipt(failed.failedReceipts[target]), true);
  assert.deepEqual(failed.relaySpooledNodeIds, [target]);
  assert.equal(isUnattemptedOutboxItem(failed), false);
  assert.equal(harness.mailbox.inspectSlotObligations('stream:relay', {
    sessionMailboxId: session, outbox: harness.outbox,
  }).immediateBusyReason, 'outbound_thread_open');

  const changed = { ...input, routing: { mode: 'unicast' as const, targets: ['member:carol'] }, body: 'other node' };
  assert.equal((await harness.emitter.emit(changed)).messageId, first.messageId);
  assert.deepEqual(harness.outbox.get(first.messageId)?.relaySpooledNodeIds, [target]);
  receiver = 'queued';
  assert.equal((await harness.emitter.emit(input)).receipts[0].stage, 'queued');
  assert.deepEqual(harness.outbox.get(first.messageId)?.relaySpooledNodeIds, [target]);
  receiver = 'open';
  assert.equal((await harness.emitter.emit(input)).receipts[0].stage, 'dispatched');
  assert.deepEqual(harness.outbox.get(first.messageId)?.relaySpooledNodeIds, []);
  assert.equal((await harness.emitter.emit(changed)).messageId, first.messageId);
  assert.equal(executions, 1);
  assert.ok(harness.sendCalls.every(call => call.targetNodeId === target && call.envelope.id === first.messageId));
});

test('later pre-effect rejection cannot overwrite earlier unknown outbox evidence', async t => {
  const harness = await emitterHarness(t);
  const target = harness.targetCard.nodeId;
  const original = envelopeFor(harness.sender, harness.networkId, 'member:diz');
  const unknown = signReceipt({
    messageId: original.id, networkId: original.networkId, fromNodeId: target,
    stage: 'failed', executionReceipt: { handoff: 'unknown' }, updatedAt: new Date().toISOString(),
  }, harness.receiver.privateKeyPem);
  const rejection = signReceipt({
    ...unknown, stage: 'rejected', executionReceipt: { handoff: 'unattempted' },
  }, harness.receiver.privateKeyPem);
  const base = { envelope: original, explicitTargets: ['member:diz'], pendingNodeIds: [] };
  harness.outbox.enqueue({ ...base, failedReceipts: { [target]: unknown } });
  const failed = harness.outbox.markNodeFailed(original.id, target, 'Gate closed', rejection)!;
  assert.deepEqual(failed.failedReceipts[target], unknown);
  const reconciled = harness.outbox.enqueue({
    ...base, reconcileNodeIds: [target], failedReceipts: { [target]: rejection },
  });
  assert.deepEqual(reconciled.failedReceipts[target], unknown);
  assert.equal(isUnattemptedOutboxItem(reconciled), false);
});

test('kind-only changes replace a proven-unattempted keyed envelope', async t => {
  const harness = await emitterHarness(t, {
    plan: (_target, index) => index === 0 ? { stage: 'failed', executionReceipt: { handoff: 'unattempted' } } : {},
  });
  const input: EmitMessageInput = {
    networkId: harness.networkId, routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'initial-wave', subject: 'wave', body: 'same slot', payload: {}, idempotencyKey: 'kind-correction',
  };
  const first = await harness.emitter.emit(input);
  const corrected = await harness.emitter.emit({ ...input, kind: 'corrected-wave' });
  assert.notEqual(corrected.messageId, first.messageId);
  assert.equal(harness.sendCalls.at(-1)?.envelope.kind, 'corrected-wave');
});

test('forced redrive skips a queued snapshot superseded while an earlier send waits', async t => {
  const harness = await emitterHarness(t);
  harness.setMounted(false);
  const input: EmitMessageInput = {
    networkId: harness.networkId, routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'wave', subject: 'wave', body: 'slot 2', payload: {}, idempotencyKey: 'snapshot-wave',
  };
  // Order these independent obligations without relying on clock precision or UUID ordering.
  const head = signEnvelope({
    ...envelopeFor(harness.sender, harness.networkId, 'member:diz'), id: '!head',
  }, harness.sender.privateKeyPem);
  harness.outbox.enqueue({ envelope: head, explicitTargets: [], pendingNodeIds: [harness.targetCard.nodeId] });
  const original = await harness.emitter.emit(input);
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const attemptedIds: string[] = [];
  const service = new OutboxRedriveService({ outbox: harness.outbox, deliver: async item => {
    attemptedIds.push(item.id);
    if (item.id === head.id) {
      entered.resolve();
      await release.promise;
    }
    return {};
  } });
  const flush = service.flush({ force: true });
  const result = await Promise.race([
    entered.promise.then(() => undefined),
    flush.then(() => { throw new Error('Flush finished without reaching the first obligation'); }),
  ]).then(async () => {
    try {
      const before = harness.outbox.get(original.messageId)!;
      const rejection = signReceipt({
        messageId: before.id, networkId: before.networkId, fromNodeId: harness.targetCard.nodeId,
        stage: 'failed', executionReceipt: { handoff: 'unattempted' }, updatedAt: new Date().toISOString(),
      }, harness.receiver.privateKeyPem);
      assert.equal(harness.outbox.markNodeFailed(before.id, rejection.fromNodeId, 'Pre-effect failure', rejection)?.status, 'failed');
      harness.setMounted(true);
      return await harness.emitter.emit({ ...input, body: 'slot 1' });
    } finally {
      release.resolve();
    }
  });
  assert.equal((await flush).attempted, 1);
  assert.deepEqual(attemptedIds, [head.id]);
  assert.deepEqual(harness.sendCalls.map(call => call.envelope.id), [result.messageId]);
  assert.equal(harness.outbox.get(original.messageId)?.supersededById, result.messageId);
  assert.deepEqual(harness.outbox.list().filter(item => item.envelope.idempotencyKey === input.idempotencyKey &&
    !item.supersededById).map(item => item.id), [result.messageId]);
});

test('concurrent keyed emit joins an active forced redrive and can replace after pre-effect settlement', async t => {
  const harness = await emitterHarness(t, {
    plan: (_target, index) => index === 0 ? { stage: 'failed', executionReceipt: { handoff: 'unattempted' } } : {},
  });
  harness.setMounted(false);
  const input: EmitMessageInput = {
    networkId: harness.networkId, routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'wave', subject: 'wave', body: 'slot 2', payload: {}, idempotencyKey: 'forced-wave',
  };
  const first = await harness.emitter.emit(input);
  harness.setMounted(true);
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const service = new OutboxRedriveService({ outbox: harness.outbox, deliver: async (item, options) => {
    entered.resolve();
    await release.promise;
    return harness.emitter.redriveOutboxItem!(item, options);
  } });
  const flush = service.flush({ force: true });
  const changed = { ...input, body: 'slot 1' };
  try {
    await entered.promise;
    assert.ok(harness.outbox.get(first.messageId)?.activeAttempt?.token);
    assert.equal((await harness.emitter.emit(changed)).messageId, first.messageId);
    assert.equal(harness.sendCalls.length, 0);
    assert.equal(harness.outbox.list().length, 1);
  } finally {
    release.resolve();
  }
  assert.equal((await flush).items[0].status, 'failed');
  assert.equal(harness.outbox.get(first.messageId)?.activeAttempt, undefined);
  const next = await harness.emitter.emit(changed);
  assert.notEqual(next.messageId, first.messageId);
  assert.deepEqual(harness.sendCalls.map(call => call.envelope.id), [first.messageId, next.messageId]);
});

test('an expired outbox attempt cannot settle or release a newer lease', async t => {
  const harness = await emitterHarness(t);
  const envelope = envelopeFor(harness.sender, harness.networkId, 'member:diz');
  const target = harness.targetCard.nodeId;
  harness.outbox.enqueue({ envelope, explicitTargets: [], pendingNodeIds: [target], inDoubtNodeIds: [target] });
  const now = Date.now();
  const first = harness.outbox.beginAttempt(envelope.id, { now, leaseMs: 10 })!;
  const second = harness.outbox.beginAttempt(envelope.id, { now: now + 10 })!;
  assert.notEqual(first.attemptToken, second.attemptToken);
  assert.equal(harness.outbox.markFailed(envelope.id, 'stale failure', first.attemptToken), undefined);
  assert.throws(() => harness.outbox.enqueue({
    envelope, explicitTargets: [], pendingNodeIds: [], failedNodeIds: [target], attemptToken: first.attemptToken,
  }), /Outbox attempt no longer owns/);
  harness.outbox.releaseAttempt(envelope.id, first.attemptToken);
  assert.equal(harness.outbox.get(envelope.id)?.activeAttempt?.token, second.attemptToken);

  const delivered = signReceipt({
    messageId: envelope.id, networkId: envelope.networkId, fromNodeId: target,
    stage: 'dispatched', updatedAt: new Date().toISOString(),
  }, harness.receiver.privateKeyPem);
  const settled = harness.outbox.markNodeDelivered(envelope.id, target, delivered, second.attemptToken);
  assert.equal(settled?.status, 'delivered');
  assert.equal(settled?.activeAttempt, undefined);
  assert.deepEqual(settled?.inDoubtNodeIds, []);
});

test('outbox redrive requires a live token and uses only the leased row', async t => {
  const harness = await emitterHarness(t, { extraMembers: ['carol'],
    plan: () => ({ stage: 'rejected', executionReceipt: { handoff: 'unattempted' } }) });
  const bob = harness.targetCard.nodeId;
  const carol = harness.targetByMember.get('carol')!.nodeId;
  const envelope = signEnvelope({ ...envelopeFor(harness.sender, harness.networkId, bob),
    idempotencyKey: 'lease-only' }, harness.sender.privateKeyPem);
  const cached = harness.outbox.enqueue({ envelope, explicitTargets: [bob], pendingNodeIds: [bob] });
  const redrive = harness.emitter.redriveOutboxItem!;
  const stoppedBeforeSend = (error: unknown) => error instanceof LocalDeliveryError && isBeforeSendError(error);
  await assert.rejects(redrive(cached), stoppedBeforeSend);
  const expired = harness.outbox.beginAttempt(envelope.id, { now: 0, leaseMs: 1 })!;
  await assert.rejects(redrive(cached, { attemptToken: expired.attemptToken }), stoppedBeforeSend);
  assert.equal(harness.sendCalls.length, 0);

  const claimed = harness.outbox.beginAttempt(envelope.id)!;
  try {
    await assert.rejects(redrive(cached, { attemptToken: 'different-lease' }), stoppedBeforeSend);
    await assert.rejects(redrive(cached, { attemptToken: claimed.attemptToken, targetNodeIds: [carol] }), stoppedBeforeSend);
    assert.equal(harness.sendCalls.length, 0);
    const stale = { ...cached, pendingNodeIds: [carol], envelope: { ...envelope, body: 'stale body' } };
    const outcome = await redrive(stale, { attemptToken: claimed.attemptToken });
    assert.deepEqual(harness.sendCalls.map(call => [call.targetNodeId, call.envelope.body]), [[bob, envelope.body]]);
    harness.outbox.recordAttempt(envelope.id, { pendingNodeIds: [], nextAttemptAt: envelope.createdAt,
      clearFenceNodeIds: outcome.clearFenceNodeIds, failures: outcome.failures,
    }, claimed.attemptToken);
  } finally {
    harness.outbox.releaseAttempt(envelope.id, claimed.attemptToken);
  }

  const replacement = signEnvelope({ ...envelope, id: randomUUID(), routing: { mode: 'unicast', targets: [carol] } },
    harness.sender.privateKeyPem);
  const reserved = harness.outbox.replaceUnattemptedIdempotency({ envelope: replacement,
    explicitTargets: [carol], plannedTargetNodeIds: [carol], expectedId: envelope.id });
  assert.equal(reserved?.claimed, true);
  assert.equal(harness.outbox.get(envelope.id)?.supersededById, replacement.id);
  const sentBefore = harness.sendCalls.length;
  await assert.rejects(redrive(cached), stoppedBeforeSend);
  await assert.rejects(redrive(cached, { attemptToken: claimed.attemptToken }), stoppedBeforeSend);
  if (reserved?.claimed) {
    await assert.rejects(redrive(cached, { attemptToken: reserved.attemptToken }), stoppedBeforeSend);
    harness.outbox.releaseAttempt(replacement.id, reserved.attemptToken);
  }
  assert.equal(harness.sendCalls.length, sentBefore);
});

function legacyOutboxItem(envelope: MessageEnvelope, explicitTargets: string[], lastError?: string): OutboxItem {
  return {
    id: envelope.id, networkId: envelope.networkId, envelope, explicitTargets, pendingNodeIds: [],
    deliveredReceipts: {}, failedReceipts: {}, failedNodeIds: [], inDoubtNodeIds: [], relaySpooledNodeIds: [], stagedFiles: {},
    status: 'queued', attempts: 0, createdAt: envelope.createdAt, updatedAt: envelope.createdAt,
    nextAttemptAt: envelope.createdAt, lastError,
  };
}

test('legacy v1 non-concrete claims stay parked even with an earlier unresolved selector error', async t => {
  const harness = await emitterHarness(t, { extraMembers: ['carol'] });
  const { outbox, kernel, mailbox, sender, receiver, networkId } = harness;
  const bob = new Kernel();
  t.after(() => bob.stop());
  bob.ctx.provide('node', { getSelfCard: () => harness.targetCard });
  const ledgerPath = join(harness.dir, 'bob-deliveries.json');
  await bob.use(createDispatcherPlugin({ privateKeyPem: receiver.privateKeyPem, ledgerPath }));
  const executions: string[] = [];
  bob.ctx.services.dispatcher.registerExecutor({ name: 'inbox', async execute([message]) {
    executions.push(message.id);
    return { executor: 'capture', executionId: message.id };
  } }, {});
  const items: OutboxItem[] = [];
  for (const selectors of [[], ['skill:incident'], ['member:diz']]) {
    const key = selectors[0] ?? 'auto';
    const envelope = signEnvelope({ ...envelopeFor(sender, networkId, harness.targetCard.nodeId),
      idempotencyKey: key, routing: { mode: 'auto', targets: selectors },
      semantic: { type: 'question', threadId: key, replyMailbox: `terminal:v1-${items.length}` },
    }, sender.privateKeyPem);
    assert.equal((await bob.ctx.services.dispatcher.dispatch(envelope)).stage, 'dispatched');
    items.push(legacyOutboxItem(envelope, selectors,
      key === 'member:diz' ? `Unresolved explicit target: ${key}` : undefined));
  }
  assert.ok(existsSync(ledgerPath));
  assert.deepEqual(executions, items.map(item => item.id));
  const legacy = JSON.stringify({ schema: 'the-dealer-outbox/v1', items });
  writeFileSync(outbox.path, legacy);
  const carol = harness.targetByMember.get('carol')!.nodeId;
  const router = kernel.ctx.services.router;
  router.registerJudge({ name: 'new-recipient', priority: 1,
    async decide() { return { mode: 'unicast', targetNodeIds: [carol], matchedBy: 'judge', reason: 'now available' }; },
  });
  assert.deepEqual((await router.route(items[0].envelope)).targetNodeIds, [carol]);
  const route = t.mock.method(router, 'route');
  const redrive = new OutboxRedriveService({ outbox, deliver: harness.emitter.redriveOutboxItem!, intervalMs: 60_000,
    events: { on: (event, listener) => kernel.ctx.on(event, listener) } });
  redrive.start();
  t.after(() => redrive.stop());
  assert.equal((await redrive.flush({ force: true })).attempted, 0);
  kernel.ctx.emit('node/peer-updated', harness.targetByMember.get('carol')!, { direct: true });
  assert.equal((await redrive.flush()).attempted, 0);
  assert.equal(readFileSync(outbox.path, 'utf8'), legacy);
  for (const original of items) {
    const parked = outbox.get(original.id)!;
    assert.equal(parked.status, 'failed');
    assert.match(parked.lastError!, /Interrupted v1 outbox claim/);
    if (original.lastError) assert.ok(parked.lastError!.includes(original.lastError));
    assert.equal(parked.unresolvedRoute, undefined);
    assert.equal(isUnattemptedOutboxItem(parked), false);
    assert.equal(hasProvenUnattemptedFailure(parked), false);
    assert.equal(outbox.readDefinitivelyFailedKeys().has(netKey(networkId, parked.id)), false);
    assert.equal(mailbox.inspectSlotObligations('stream:legacy', {
      sessionMailboxId: original.envelope.semantic?.replyMailbox, outbox,
    }).immediateBusyReason, 'outbound_thread_open');
    await assert.rejects(harness.emitter.emit({ ...original.envelope, body: 'different intent',
      routing: { mode: 'unicast', targets: [carol] },
    }), /Interrupted v1 outbox claim/);
    assert.equal(outbox.findKeyedObligation(networkId, original.envelope.idempotencyKey!, 0)?.id, original.id);
  }
  assert.equal(route.mock.callCount(), 0);
  assert.deepEqual(harness.sendCalls, []);
  const recovered = outbox.requeue(items[2].id);
  assert.equal(recovered?.status, 'queued');
  assert.equal(recovered?.unresolvedRoute, true);
});

test('a v1 concrete claim pins the original target and upgrades on mutation', async t => {
  const harness = await emitterHarness(t, { extraMembers: ['carol'] });
  const { outbox, sender, networkId } = harness;
  const bob = harness.targetCard.nodeId;
  const concrete = `node:${bob}`;
  const pinned = legacyOutboxItem(envelopeFor(sender, networkId, concrete), [concrete, bob]);
  writeFileSync(outbox.path, JSON.stringify({ schema: 'the-dealer-outbox/v1', items: [{ ...pinned, unresolvedRoute: true }] }));
  let reachable = false;
  const redrive = new OutboxRedriveService({ outbox, deliver: harness.emitter.redriveOutboxItem!,
    isPeerReachable: () => reachable });
  t.after(() => redrive.stop());
  assert.deepEqual(outbox.get(pinned.id)?.pendingNodeIds, [bob]);
  assert.deepEqual(outbox.get(pinned.id)?.inDoubtNodeIds, [bob]);
  assert.equal(outbox.get(pinned.id)?.unresolvedRoute, undefined);
  assert.equal((await redrive.flush({ id: pinned.id })).attempted, 0);
  assert.deepEqual(harness.sendCalls, []);
  assert.equal(JSON.parse(readFileSync(outbox.path, 'utf8')).schema, 'the-dealer-outbox/v1');
  reachable = true;
  await redrive.flush({ id: pinned.id });
  assert.deepEqual(harness.sendCalls.map(call => [call.envelope.id, call.targetNodeId]), [[pinned.id, bob]]);
  assert.equal(outbox.get(pinned.id)?.status, 'delivered');
  assert.deepEqual(outbox.get(pinned.id)?.inDoubtNodeIds, []);
  assert.equal(JSON.parse(readFileSync(outbox.path, 'utf8')).schema, 'the-dealer-outbox/v2');
});

test('legacy outbox reads project obligations under an existing lock and explicit upgrade writes once', t => {
  const root = directory(t);
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'outbox-blobs'));
  const mailbox = new MailboxStore(join(root, 'mailbox.json'));
  const sender = generateNodeKeyPair();
  const session = 'terminal:locked-v1';
  const envelope = signEnvelope({ ...envelopeFor(sender, NETWORK, 'member:diz'), idempotencyKey: 'locked-v1',
    semantic: { type: 'question', threadId: 'locked-v1', replyMailbox: session },
  }, sender.privateKeyPem);
  const legacy = JSON.stringify({ schema: 'the-dealer-outbox/v1', items: [legacyOutboxItem(envelope, ['member:diz'])] });
  assert.equal(outbox.upgradeLegacyStore(), false, 'a missing store needs no upgrade');
  assert.equal(existsSync(outbox.path), false);
  writeFileSync(outbox.path, legacy);
  withOwnedLock(outbox.lockDir, 'Test outbox', () => {
    const ownerPath = join(outbox.lockDir, 'owner.json');
    const owner = readFileSync(ownerPath, 'utf8');
    assert.equal(outbox.get(envelope.id)?.status, 'failed');
    assert.deepEqual(outbox.list({ status: 'all' }).map(item => item.id), [envelope.id]);
    assert.equal(outbox.findKeyedObligation(NETWORK, 'locked-v1', 0)?.id, envelope.id);
    assert.equal(outbox.readDefinitivelyFailedKeys().size, 0);
    assert.equal(mailbox.inspectSlotObligations('stream:legacy', { sessionMailboxId: session, outbox }).immediateBusyReason,
      'outbound_thread_open');
    assert.equal(readFileSync(outbox.path, 'utf8'), legacy);
    assert.equal(readFileSync(ownerPath, 'utf8'), owner);
  });
  assert.equal(existsSync(outbox.lockDir), false);
  assert.equal(outbox.upgradeLegacyStore(), true);
  const upgraded = readFileSync(outbox.path, 'utf8');
  assert.equal(JSON.parse(upgraded).schema, 'the-dealer-outbox/v2');
  withOwnedLock(outbox.lockDir, 'Test outbox', () => assert.equal(outbox.upgradeLegacyStore(), false));
  assert.equal(readFileSync(outbox.path, 'utf8'), upgraded);
});

test('redrive resumes a v2 claim reserved before routing and fences the recipient before send', async t => {
  let atSend: OutboxItem | undefined;
  const harness = await emitterHarness(t, { plan: () => {
    atSend = harness.outbox.get(envelope.id);
    return {};
  } });
  const { outbox, sender, networkId, emitter, sendCalls, targetCard } = harness;
  const original = envelopeFor(sender, networkId, 'member:diz');
  const envelope = signEnvelope({ ...original, idempotencyKey: original.id }, sender.privateKeyPem);
  const claim = outbox.claimIdempotency({ envelope, explicitTargets: ['member:diz'] });
  assert.ok(claim.claimed);
  assert.equal(claim.item.unresolvedRoute, true);
  assert.deepEqual(claim.item.pendingNodeIds, []);
  assert.deepEqual(claim.item.inDoubtNodeIds, []);
  outbox.releaseAttempt(envelope.id, claim.attemptToken);

  const redrive = new OutboxRedriveService({ outbox, deliver: emitter.redriveOutboxItem! });
  t.after(() => redrive.stop());
  assert.equal((await redrive.flush({ id: envelope.id, force: true })).delivered, 1);
  assert.deepEqual(sendCalls.map(call => [call.envelope.id, call.targetNodeId]), [[envelope.id, targetCard.nodeId]]);
  assert.ok(atSend);
  assert.equal(atSend.unresolvedRoute, undefined);
  assert.deepEqual(atSend.pendingNodeIds, [targetCard.nodeId]);
  assert.deepEqual(atSend.inDoubtNodeIds, [targetCard.nodeId]);
  assert.deepEqual(outbox.get(envelope.id)?.inDoubtNodeIds, []);
});

test('a v2 claim with an explicitly empty route stays parked until operator requeue', async t => {
  const { outbox, sender, networkId, emitter, sendCalls, targetCard } = await emitterHarness(t);
  const original = envelopeFor(sender, networkId, 'member:diz');
  const envelope = signEnvelope({ ...original, idempotencyKey: original.id }, sender.privateKeyPem);
  const { item } = outbox.claimIdempotency({ envelope,
    explicitTargets: ['member:diz'], plannedTargetNodeIds: [], leaseMs: 1 });
  assert.equal(item.unresolvedRoute, undefined);
  const redrive = new OutboxRedriveService({ outbox, deliver: emitter.redriveOutboxItem! });
  t.after(() => redrive.stop());
  await until(() => Date.now() >= Date.parse(item.activeAttempt!.expiresAt), 'the interrupted attempt expires');
  await redrive.flush({ force: true });
  assert.deepEqual(sendCalls, []);
  assert.match(outbox.get(item.id)!.lastError!, /no persisted targets; operator reconciliation required/);
  assert.equal(isUnattemptedOutboxItem(outbox.get(item.id)), false);

  assert.equal(outbox.requeue(item.id)?.unresolvedRoute, true);
  await redrive.flush({ force: true });
  assert.deepEqual(sendCalls.map(call => call.targetNodeId), [targetCard.nodeId]);
  assert.equal(outbox.get(item.id)?.status, 'delivered');
  assert.equal(outbox.get(item.id)?.unresolvedRoute, undefined);
});

test('interrupted claims and prepared sends pin keyed delivery to the original recipient', async t => {
  let closed = true;
  const harness = await emitterHarness(t, { extraMembers: ['carol'],
    plan: () => closed ? { stage: 'rejected', executionReceipt: { handoff: 'unattempted' } } : {},
  });
  const target = harness.targetCard.nodeId;
  const carol = harness.targetByMember.get('carol')!.nodeId;
  let routes = 0;
  harness.kernel.ctx.services.router.registerJudge({ name: 'availability', priority: 1,
    async decide() { routes++; return { mode: 'unicast', targetNodeIds: [carol], matchedBy: 'judge', reason: 'available' }; },
  });
  const recovered = new OutboxStore(join(harness.dir, 'outbox.json'), join(harness.dir, 'outbox-blobs'));
  const redrive = new OutboxRedriveService({
    outbox: recovered, deliver: (item, options) => harness.emitter.redriveOutboxItem!(item, options),
  });
  t.mock.timers.enable({ apis: ['Date'], now: Date.now() });
  for (const interruptedAt of ['claim', 'prepare']) {
    const envelope = signEnvelope({
      ...envelopeFor(harness.sender, harness.networkId, 'member:diz'), routing: { mode: 'auto' },
      idempotencyKey: interruptedAt,
    }, harness.sender.privateKeyPem);
    const claim = harness.outbox.claimIdempotency({ envelope, explicitTargets: [], leaseMs: 1,
      ...(interruptedAt === 'claim' ? { plannedTargetNodeIds: [target] } : {}),
    });
    assert.equal(claim.claimed, true);
    if (interruptedAt === 'prepare') {
      t.mock.timers.tick(1);
      const lease = harness.outbox.beginAttempt(envelope.id, { leaseMs: 1 })!;
      assert.ok(harness.outbox.prepareSend(envelope.id, lease.attemptToken, [target]));
    }
    const persisted = recovered.get(envelope.id)!;
    assert.deepEqual(persisted.pendingNodeIds, [target]);
    assert.deepEqual(persisted.inDoubtNodeIds, [target]);
    t.mock.timers.tick(1);
    closed = true;
    const [failed] = (await redrive.flush({ id: envelope.id, force: true })).items;
    assert.deepEqual(failed.inDoubtNodeIds, [target]);
    assert.equal(isRetryablePreEffectReceipt(failed.failedReceipts[target]), true);
    assert.equal(isUnattemptedOutboxItem(failed), false);
    const changed: EmitMessageInput = { ...envelope, body: 'retargeted intent', routing: { mode: 'auto' } };
    assert.equal((await harness.emitter.emit(changed)).messageId, envelope.id);
    closed = false;
    assert.equal((await harness.emitter.emit(changed)).receipts[0].stage, 'dispatched');
    assert.deepEqual(recovered.get(envelope.id)?.inDoubtNodeIds, []);
    assert.deepEqual(harness.sendCalls.filter(call => call.envelope.id === envelope.id).map(call => call.targetNodeId),
      [target, target, target]);
  }
  assert.equal(routes, 0);
});

test('redrive, replay and reconciliation restore interrupted outbound threads with their claim timestamps', async t => {
  for (const path of ['redrive', 'replay', 'reconcile'] as const) {
    const harness = await emitterHarness(t, {
      plan: () => path === 'reconcile' ? { stage: 'rejected', executionReceipt: { handoff: 'unattempted' } } : {},
    });
    const session = `terminal:recovered-${path}`;
    const target = harness.targetCard.nodeId;
    const request = signEnvelope({
      ...envelopeFor(harness.sender, harness.networkId, 'member:diz'),
      idempotencyKey: path, semantic: { type: 'question', threadId: path, replyMailbox: session },
      createdAt: '2025-01-01T00:00:00.000Z',
    }, harness.sender.privateKeyPem);
    const { item } = harness.outbox.claimIdempotency({
      envelope: request, explicitTargets: ['member:diz'], plannedTargetNodeIds: [target], leaseMs: 1,
    });
    assert.equal(harness.mailbox.getThread(request.id), undefined);
    const inspect = () => harness.mailbox.inspectSlotObligations('stream:test', { sessionMailboxId: session, outbox: harness.outbox });
    assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
    if (path === 'redrive') {
      const redrive = new OutboxRedriveService({
        outbox: harness.outbox, deliver: (candidate, options) => harness.emitter.redriveOutboxItem!(candidate, options),
        onTargetsResolved: (envelope, peers, createdAt) => harness.mailbox.recordOutboundEnvelope(envelope, peers, session, { createdAt }),
      });
      await until(() => Date.now() >= Date.parse(item.activeAttempt!.expiresAt), 'the interrupted attempt expires');
      assert.equal((await redrive.flush({ force: true })).delivered, 1);
      assert.equal(harness.sendCalls.length, 1);
    } else if (path === 'replay') {
      assert.equal((await harness.emitter.emit(request)).messageId, request.id);
      assert.equal((await harness.emitter.emit(request)).messageId, request.id);
      assert.equal(harness.sentEvents.length, 1);
      assert.deepEqual(harness.sentEvents[0].targetNodeIds, [target]);
      assert.equal(harness.sendCalls.length, 0);
    } else {
      const receipt = signReceipt({
        messageId: request.id, networkId: request.networkId, fromNodeId: target,
        stage: 'rejected', updatedAt: new Date().toISOString(),
        executionReceipt: { executor: 'gate', executionId: request.id, handoff: 'unattempted' },
      }, harness.receiver.privateKeyPem);
      harness.outbox.markNodeFailed(request.id, target, 'Interrupted response', receipt, item.activeAttempt?.token);
      const retried = await harness.emitter.emit(request);
      assert.equal(retried.messageId, request.id);
      assert.equal(retried.receipts[0].stage, 'rejected');
      assert.equal(harness.sendCalls.length, 1);
      assert.deepEqual(harness.outbox.get(request.id)?.inDoubtNodeIds, [target]);
      assert.deepEqual(harness.sentEvents.map(event => event.targetNodeIds), [[target]]);
    }
    assert.equal(harness.outbox.get(request.id)?.sentCommitted, true);
    const restored = harness.mailbox.getThread(request.id)!;
    assert.equal(restored.createdAt, item.createdAt);
    assert.deepEqual(restored.peerNodeIds, [target]);
    const reply = signEnvelope({
      ...envelopeFor(harness.receiver, harness.networkId, harness.sender.nodeId),
      replyToId: request.id, semantic: { type: 'reply', threadId: path },
    }, harness.receiver.privateKeyPem);
    harness.mailbox.enqueue([reply]);
    assert.equal(harness.mailbox.list({ mailboxId: session }).items[0]?.id, reply.id);
    harness.mailbox.ack(harness.mailbox.check({ mailboxId: session }).deliveryId!, session);
    assert.equal(inspect().immediateBusyReason, undefined);
  }
});

test('runtime startup upgrades a legacy claim and restores its reply route before redrive', async t => {
  const root = directory(t);
  const stateDir = join(root, 'ben');
  mkdirSync(stateDir);
  const outbox = new OutboxStore(join(stateDir, 'outbox.json'), join(stateDir, 'outbox-blobs'));
  const sender = generateNodeKeyPair();
  const peer = generateNodeKeyPair();
  const session = 'terminal:startup-recovery';
  const request = signEnvelope({
    ...envelopeFor(sender, NETWORK, peer.nodeId), idempotencyKey: 'startup-recovery',
    semantic: { type: 'question', threadId: 'startup-recovery', replyMailbox: session },
  }, sender.privateKeyPem);
  const item = legacyOutboxItem(request, [peer.nodeId]);
  writeFileSync(outbox.path, JSON.stringify({ schema: 'the-dealer-outbox/v1', items: [item] }));
  assert.equal(new MailboxStore(join(stateDir, 'mailbox.json')).getThread(request.id), undefined);
  const runtime = runtimeFor(root, 'ben');
  await runtime.start();
  t.after(() => runtime.stop());
  assert.equal(JSON.parse(readFileSync(outbox.path, 'utf8')).schema, 'the-dealer-outbox/v2');
  const restored = new MailboxStore(join(stateDir, 'mailbox.json')).getThread(request.id)!;
  assert.equal(restored.createdAt, item.createdAt);
  assert.equal(restored.localMailbox, session);
  assert.deepEqual(restored.peerNodeIds, [peer.nodeId]);
  assert.deepEqual(runtime.outbox.get(request.id)?.inDoubtNodeIds, [peer.nodeId]);
  const reply = signEnvelope({
    ...envelopeFor(peer, NETWORK, sender.nodeId),
    replyToId: request.id, semantic: { type: 'reply', threadId: 'startup-recovery' },
  }, peer.privateKeyPem);
  runtime.mailbox.enqueue([reply]);
  assert.equal(runtime.mailbox.list({ mailboxId: session }).items[0]?.id, reply.id);
  runtime.mailbox.ack(runtime.mailbox.check({ mailboxId: session }).deliveryId!, session);
  assert.equal(runtime.mailbox.inspectSlotObligations('stream:test', {
    sessionMailboxId: session, outbox: runtime.outbox,
  }).immediateBusyReason, undefined);
});

test('TCP frame limits distinguish local non-start from an uncertain reply', async t => {
  const root = directory(t);
  const sender = runtimeFor(root, 'ben', { discovery: 'mesh' });
  const receiver = runtimeFor(root, 'diz', { discovery: 'mesh' });
  const alternate = runtimeFor(root, 'carol', { discovery: 'mesh' });
  for (const runtime of [sender, receiver, alternate]) {
    await runtime.start();
    t.after(() => runtime.stop());
    await runtime.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  }
  await until(() => sender.node.listPeers({ networkId: NETWORK }).length === 2, 'direct peers');
  const recipient = receiver.node.getSelfCard(NETWORK).nodeId;
  let sockets = 0;
  const server = createServer(socket => {
    sockets++;
    socket.on('error', () => {});
    socket.once('data', () => {
      const header = Buffer.alloc(4);
      header.writeUInt32BE(8 * 1024 * 1024 + 1);
      socket.end(header);
    });
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const endpoint = server.address();
  assert.ok(endpoint && typeof endpoint !== 'string');
  const useReplyFixture = () => sender.transport.registerPeerEndpoint(NETWORK, recipient, `127.0.0.1:${endpoint.port}`);
  useReplyFixture();
  const send = t.mock.method(sender.transport, 'sendUnicast');
  const request: EmitMessageInput = {
    networkId: NETWORK, routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'question', subject: 'frame limit', body: 'x'.repeat(8 * 1024 * 1024), payload: {},
    idempotencyKey: 'frame-limit', semantic: { type: 'question', replyMailbox: 'terminal:frame-limit' },
  };
  const busyReason = (sessionMailboxId: string) =>
    sender.mailbox.inspectSlotObligations('stream:frame-limit', { sessionMailboxId, outbox: sender.outbox }).immediateBusyReason;

  await assert.rejects(sender.emitter.emit(request), /Frame exceeds limit/);
  const [failed] = sender.outbox.list();
  assert.deepEqual(failed.inDoubtNodeIds, []);
  assert.deepEqual(failed.unattemptedNodeIds, [recipient]);
  assert.equal(isUnattemptedOutboxItem(failed), true);
  assert.equal(busyReason('terminal:frame-limit'), undefined);
  await assert.rejects(sender.emitter.emit(request), /Frame exceeds limit/);
  assert.equal(send.mock.callCount(), 1);
  assert.equal(sockets, 0);

  sender.transport.registerPeerEndpoint(NETWORK, recipient, receiver.transport.endpointAddr!);
  const corrected = await sender.emitter.emit({ ...request, body: 'small enough' });
  assert.equal(corrected.receipts[0]?.stage, 'dispatched');
  assert.equal(sender.outbox.get(failed.id)?.supersededById, corrected.messageId);

  useReplyFixture();
  const uncertain: EmitMessageInput = {
    ...request, body: 'small request, oversized reply', idempotencyKey: 'reply-limit',
    semantic: { type: 'question', replyMailbox: 'terminal:reply-limit' },
  };
  await assert.rejects(sender.emitter.emit(uncertain), /Frame exceeds limit/);
  assert.equal(sockets, 1);
  const unclear = sender.outbox.list().find(item => item.envelope.idempotencyKey === uncertain.idempotencyKey)!;
  assert.deepEqual(unclear.inDoubtNodeIds, [recipient]);
  assert.equal(isUnattemptedOutboxItem(unclear), false);
  assert.equal(busyReason('terminal:reply-limit'), 'outbound_thread_open');
  await assert.rejects(sender.emitter.emit({
    ...uncertain, routing: { mode: 'unicast', targets: ['member:carol'] }, body: 'changed target',
  }), /Frame exceeds limit/);
  assert.equal(send.mock.callCount(), 3);
  assert.equal(alternate.mailbox.list().totalCount, 0);
});

test('unkeyed questions keep unresolved send effects fenced without redrive', async t => {
  for (const stage of ['failed', 'dispatching', 'rejected', 'unattempted', 'malformed'] as const) {
    const harness = await emitterHarness(t, {
      plan: () => stage === 'malformed' ? { error: new LocalDeliveryError('Invalid or mismatched delivery receipt') }
        : stage === 'unattempted' ? { stage: 'rejected', executionReceipt: { handoff: 'unattempted' } }
        : { stage, ...(stage === 'failed' ? { executionReceipt: { executor: 'test', executionId: 'failure', handoff: 'unknown' } } : {}) },
    });
    const session = 'terminal:unkeyed-question';
    const request: EmitMessageInput = {
      ...envelopeFor(harness.sender, harness.networkId, 'member:diz'),
      semantic: { type: 'question', replyMailbox: session },
    };
    if (stage === 'malformed') await assert.rejects(harness.emitter.emit(request), LocalDeliveryError);
    else await harness.emitter.emit(request);
    const obligations = harness.mailbox.inspectSlotObligations('stream:unkeyed', {
      sessionMailboxId: session, outbox: harness.outbox,
    });
    const [item] = harness.outbox.list();
    if (stage === 'unattempted') {
      assert.equal(item, undefined);
      assert.equal(obligations.immediateBusyReason, undefined);
      assert.deepEqual(harness.mailbox.listThreads(), []);
    } else {
      assert.equal(item?.status, 'failed');
      assert.equal(item.envelope.idempotencyKey, undefined);
      assert.equal(obligations.immediateBusyReason, 'outbound_thread_open');
      assert.deepEqual(harness.mailbox.getThread(item.id)?.peerNodeIds, [harness.targetCard.nodeId]);
      const service = new OutboxRedriveService({ outbox: harness.outbox, deliver: harness.emitter.redriveOutboxItem! });
      assert.equal((await service.flush({ force: true })).attempted, 0);
      assert.equal(harness.sendCalls.length, 1);
    }
  }
});

test('a redrive non-start records local evidence without clearing earlier custody or uncertainty', async t => {
  for (const prior of ['offline', 'approval', 'timeout'] as const) {
    const harness = await emitterHarness(t, {
      plan: (target, index) => {
        if (index > 0) return { error: markBeforeSendError(new LocalDeliveryError('Frame exceeds limit')) };
        if (prior === 'approval') return { stage: 'pending_approval' };
        return { error: prior === 'timeout'
          ? new PeerUnreachableError(target, 'Transport timeout', { cause: new DeliveryTimeoutError('Transport timeout') })
          : markBeforeSendError(new PeerUnreachableError(target)) };
      },
    });
    const original = await harness.emitter.emit({
      ...envelopeFor(harness.sender, harness.networkId, 'member:diz'), idempotencyKey: 'redrive-non-start',
    });
    const service = new OutboxRedriveService({ outbox: harness.outbox, deliver: harness.emitter.redriveOutboxItem! });
    assert.equal((await service.flush({ force: true })).attempted, 1);
    const item = harness.outbox.get(original.messageId)!;
    const target = harness.targetCard.nodeId;
    assert.deepEqual(item.inDoubtNodeIds, prior === 'timeout' ? [target] : []);
    assert.deepEqual(item.unattemptedNodeIds, prior === 'offline' ? [target] : []);
    assert.equal(isUnattemptedOutboxItem(item), prior === 'offline');
    assert.equal(item.status, prior === 'approval' ? 'queued' : 'failed');
    if (prior === 'approval') assert.deepEqual(item.approvalPendingNodeIds, [target]);
  }
});

test('a malformed receipt after a keyed send leaves the recipient in doubt', async t => {
  const harness = await emitterHarness(t, { async send() { return Buffer.from('malformed receipt'); } });
  await assert.rejects(harness.emitter.emit({
    ...envelopeFor(harness.sender, harness.networkId, 'member:diz'), idempotencyKey: 'malformed-receipt',
  }), LocalDeliveryError);
  const [item] = harness.outbox.list();
  assert.equal(harness.sendCalls.length, 1);
  assert.deepEqual(item.inDoubtNodeIds, [harness.targetCard.nodeId]);
  assert.equal(harness.outbox.readDefinitivelyFailedKeys().size, 0);
});

test('keyed sends, exact retries and redrive persist a fresh fence before transport', async t => {
  const harness = await emitterHarness(t, {
    plan: (target, index) => {
      const [item] = harness.outbox.list();
      assert.deepEqual(item.pendingNodeIds, [target]);
      assert.deepEqual(item.inDoubtNodeIds, [target]);
      assert.deepEqual(item.activeAttempt?.fencedNodeIds, [target]);
      if (index === 0) return { stage: 'failed', executionReceipt: { handoff: 'unattempted' } };
      if (index === 1) return { error: markBeforeSendError(new PeerUnreachableError(target, 'Peer offline')) };
      return { stage: 'dispatched' };
    },
  });
  const request = { ...envelopeFor(harness.sender, harness.networkId, 'member:diz'), idempotencyKey: 'fence-before-wire' };
  const first = await harness.emitter.emit(request);
  assert.equal(isUnattemptedOutboxItem(harness.outbox.get(first.messageId)), true);
  assert.equal((await harness.emitter.emit(request)).messageId, first.messageId);
  assert.deepEqual(harness.outbox.get(first.messageId)?.inDoubtNodeIds, []);
  const redrive = new OutboxRedriveService({
    outbox: harness.outbox, deliver: (item, options) => harness.emitter.redriveOutboxItem!(item, options),
  });
  assert.equal((await redrive.flush({ force: true })).delivered, 1);
  assert.equal(harness.sendCalls.length, 3);
  assert.deepEqual(harness.outbox.get(first.messageId)?.inDoubtNodeIds, []);
});

test('only resolved failed sends release their outbound thread after redrive', async t => {
  for (const [stage, detail, handoff, definitivelyFailed] of [
    ['failed', 'Uncertain executor', 'unknown', false],
    ['failed', 'Legacy failed receipt', undefined, false],
    ['dispatching', 'Dispatch was interrupted', undefined, false],
    ['rejected', DEDUP_CAS_AMBIGUOUS_DETAIL, undefined, false],
    ['rejected', 'dnd_handoff_parked', 'unknown', false],
    ['failed', 'Executor never started', 'unattempted', true],
    ['rejected', 'Policy declined before dispatch', 'rejected', true],
  ] as const) {
    const harness = await emitterHarness(t, {
      plan: (target, index) => index === 0 ? { error: markBeforeSendError(new PeerUnreachableError(target, 'Peer offline')) }
        : { stage, detail, ...(handoff ? { executionReceipt: { handoff } } : {}) },
    });
    const session = 'terminal:failed-wave';
    const original = await harness.emitter.emit({
      ...envelopeFor(harness.sender, harness.networkId, 'member:diz'), idempotencyKey: 'failed-wave',
      semantic: { type: 'question', replyMailbox: session },
    });
    const redrive = new OutboxRedriveService({
      outbox: harness.outbox, deliver: (item, options) => harness.emitter.redriveOutboxItem!(item, options),
    });
    const [failed] = (await redrive.flush({ id: original.messageId, force: true })).items;
    assert.deepEqual(failed.inDoubtNodeIds, [], detail);
    assert.equal(harness.outbox.readDefinitivelyFailedKeys().size, definitivelyFailed ? 1 : 0, detail);
    assert.equal(harness.mailbox.inspectSlotObligations('stream:failures', {
      sessionMailboxId: session, outbox: harness.outbox,
    }).immediateBusyReason, definitivelyFailed ? undefined : 'outbound_thread_open', detail);
  }
});

test('superseded offline requests retain failed evidence and stop blocking the slot', async t => {
  const harness = await emitterHarness(t, {
    plan: (_target, index) => index === 0 ? { stage: 'failed', executionReceipt: { handoff: 'unattempted' } } : {},
  });
  const slot = 'stream:alerts:2';
  const session = 'terminal:alerts-2';
  const inspect = () => harness.mailbox.inspectSlotObligations(slot, { sessionMailboxId: session, outbox: harness.outbox });
  const input: EmitMessageInput = {
    networkId: harness.networkId, routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'wave', subject: 'wave', body: 'original', payload: {}, idempotencyKey: 'offline-request',
    semantic: { type: 'question', replyMailbox: session },
  };
  harness.setMounted(false);
  const first = await harness.emitter.emit(input);
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  harness.setMounted(true);
  const service = new OutboxRedriveService({ outbox: harness.outbox, deliver: harness.emitter.redriveOutboxItem! });
  assert.equal((await service.flush({ force: true })).items[0].status, 'failed');
  const replacement = await harness.emitter.emit({ ...input, body: 'replacement' });
  assert.notEqual(replacement.messageId, first.messageId);
  assert.equal(inspect().immediateBusyReason, 'outbound_thread_open');
  harness.mailbox.recordInboundEnvelope(signEnvelope({
    ...envelopeFor(harness.receiver, harness.networkId, harness.sender.nodeId),
    replyToId: replacement.messageId, semantic: { type: 'reply', threadId: replacement.messageId },
  }, harness.receiver.privateKeyPem), session, { committed: false });
  assert.deepEqual(inspect(), { hasPendingWave: false });
  assert.equal(harness.outbox.get(first.messageId)?.supersededById, replacement.messageId);
});

test('a retry of the original keyed envelope reserves its outbox obligation against replacement', async t => {
  const retryStarted = Promise.withResolvers<void>();
  const harness = await emitterHarness(t, { plan: (_target, index) => {
    if (index === 0) return { stage: 'failed', executionReceipt: { handoff: 'unattempted' } };
    retryStarted.resolve();
    return { delayMs: 100 };
  } });
  const input: EmitMessageInput = { networkId: harness.networkId,
    routing: { mode: 'unicast', targets: ['member:diz'] },
    kind: 'wave', subject: 'wave', body: 'slot 2', payload: {}, idempotencyKey: 'racing-wave' };
  const first = await harness.emitter.emit(input);
  const otherStore = new OutboxStore(harness.outbox.path, harness.outbox.blobDir);
  const before = otherStore.get(first.messageId)!;
  const retry = harness.emitter.emit(input);
  await retryStarted.promise;
  const replacement = otherStore.replaceUnattemptedIdempotency({
    expectedId: before.id,
    envelope: signEnvelope({ ...before.envelope, id: randomUUID(), body: 'slot 1' }, harness.sender.privateKeyPem),
    explicitTargets: before.explicitTargets,
  });
  assert.equal(replacement?.claimed, false);
  assert.equal(replacement?.item.id, before.id);
  assert.equal(replacement?.item.status, 'queued');
  assert.equal((await retry).messageId, before.id);
  assert.deepEqual(otherStore.list().map(item => item.id), [before.id]);
});

test('keyed gate rejections reconcile while unsigned local failures require explicit requeue', async t => {
  for (const localFailure of [false, true]) {
    let recovered = false;
    const harness = await emitterHarness(t, {
      plan: () => recovered ? {} : localFailure
        ? { error: new LocalDeliveryError('invalid wire reply') }
        : { stage: 'rejected', detail: 'Gate closed', executionReceipt: { handoff: 'unattempted' } },
    });
    const input: EmitMessageInput = { networkId: harness.networkId,
      routing: { mode: 'unicast', targets: ['member:diz'] },
      kind: 'task', subject: 'retry key', body: 'retry after recovery', payload: {}, idempotencyKey: 'terminal-key' };
    for (let attempt = 0; attempt < 2; attempt++) {
      if (localFailure) await assert.rejects(harness.emitter.emit(input), /invalid wire reply/);
      else assert.equal((await harness.emitter.emit(input)).receipts[0].detail, 'Gate closed');
    }
    assert.equal(harness.sendCalls.length, localFailure ? 1 : 2);
    const [original] = harness.outbox.list({ status: 'failed' });
    assert.ok(original);
    recovered = true;
    if (localFailure) {
      harness.outbox.requeue(original.id);
      const service = new OutboxRedriveService({ outbox: harness.outbox, deliver: harness.emitter.redriveOutboxItem! });
      assert.equal((await service.flush({ force: true })).delivered, 1);
    }
    const delivered = await harness.emitter.emit(input);
    assert.equal(delivered.messageId, original.id);
    assert.equal(delivered.receipts[0].stage, 'dispatched');
    assert.deepEqual(await harness.emitter.emit(input), delivered);
    assert.equal(harness.sendCalls.length, localFailure ? 2 : 3, 'lookup reuses settled execution');
  }
});

test('pre-send routing failures leave no keyed claim or staged files', async t => {
  const harness = await emitterHarness(t);
  let available = false;
  const route = t.mock.method(harness.kernel.ctx.services.router, 'route', async () => {
    if (!available) throw new Error('No route matched envelope');
    return { mode: 'unicast' as const, targetNodeIds: [harness.targetCard.nodeId], matchedBy: 'test', reason: 'available' };
  });
  const file = join(harness.dir, 'attachment.txt');
  writeFileSync(file, 'claim attachment');
  const input: EmitMessageInput = { networkId: harness.networkId, routing: { mode: 'auto' },
    kind: 'task', subject: 'retry route', body: 'safe release', payload: {}, idempotencyKey: 'route-key', filePaths: [file] };
  await assert.rejects(harness.emitter.emit(input), /No route matched envelope/);
  assert.equal(harness.outbox.list({ status: 'all' }).length, 0);
  assert.equal(existsSync(join(harness.dir, 'outbox-blobs')), false);
  assert.equal(harness.sendCalls.length, 0);
  available = true;
  const sent = await harness.emitter.emit(input);
  assert.equal(sent.receipts[0].stage, 'dispatched');
  assert.equal(route.mock.callCount(), 2);
  assert.equal(harness.sendCalls.length, 1);
});


test('keyed claims replay the highest version and dispatch only strictly newer work', async t => {
  const harness = await emitterHarness(t);
  const router = t.mock.method(harness.kernel.ctx.services.router, 'route');
  const input: EmitMessageInput = { networkId: harness.networkId,
    routing: { mode: 'unicast', targets: ['member:diz'] }, kind: 'task',
    subject: 'versioned claim', body: 'entity work', payload: {}, idempotencyKey: 'entity' };
  const first = await harness.emitter.emit({ ...input, payload: { claimVersion: 1, revision: 99 } });
  for (const payload of [{ claimVersion: 1 }, { revision: 1 }, { revision: 0 }, {}]) {
    assert.deepEqual(await harness.emitter.emit({ ...input, payload }), first);
  }
  assert.equal(router.mock.callCount(), 1);
  assert.equal(harness.sendCalls.length, 1);
  const second = await harness.emitter.emit({ ...input, payload: { revision: 2 } });
  assert.notEqual(second.messageId, first.messageId);
  assert.equal(second.receipts[0].stage, 'dispatched');
  for (const payload of [{ claimVersion: 1 }, { claimVersion: 2 }, { revision: 2 }, {}]) {
    assert.deepEqual(await harness.emitter.emit({ ...input, payload }), second);
  }
  assert.equal(router.mock.callCount(), 2);
  assert.equal(harness.sendCalls.length, 2);
  const third = await harness.emitter.emit({ ...input, payload: { claimVersion: 3 } });
  assert.notEqual(third.messageId, second.messageId);
  assert.equal(third.receipts[0].stage, 'dispatched');
  assert.equal(harness.outbox.list({ status: 'delivered' }).length, 3);
  for (const claimVersion of ['4', -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
    await assert.rejects(harness.emitter.emit({ ...input, payload: { claimVersion } }), /Invalid claim version/);
  }
  assert.equal(router.mock.callCount(), 3);
  assert.equal(harness.sendCalls.length, 3);
});

test('outbox retains prepare-normalized targets for unresolved and resolved offline sends', async t => {
  for (const target of ['member:missing', 'member:diz']) {
    const harness = await emitterHarness(t, { plan: id => ({ error: markBeforeSendError(new PeerUnreachableError(id)) }) });
    harness.kernel.ctx.on('emitter/prepare', envelope => ({ ...envelope,
      routing: { ...envelope.routing, targets: [target] } }));
    const sent = await harness.emitter.emit({ networkId: harness.networkId,
      routing: { mode: 'unicast', targets: ['area:runtime'] }, kind: 'task',
      subject: 'normalized route', body: 'offline work', payload: {}, idempotencyKey: 'normalized' });
    const queued = harness.outbox.get(sent.messageId)!;
    assert.equal(queued.status, 'queued');
    assert.deepEqual(queued.explicitTargets, [target]);
    assert.deepEqual(queued.envelope.routing.targets, [target]);
  }
});


test('concurrent keyed routing failures never publish a temporary durable obligation', async t => {
  const harness = await emitterHarness(t);
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  let entered!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  let available = false;
  const route = t.mock.method(harness.kernel.ctx.services.router, 'route', async () => {
    entered();
    await blocked;
    if (!available) throw new Error('No route matched envelope');
    return { mode: 'unicast' as const, targetNodeIds: [harness.targetCard.nodeId], matchedBy: 'test', reason: 'ready' };
  });
  const input: EmitMessageInput = { networkId: harness.networkId, routing: { mode: 'auto' },
    kind: 'task', subject: 'concurrent routes', body: 'no phantom queue', payload: {}, idempotencyKey: 'race' };
  const results = Promise.allSettled([harness.emitter.emit(input), harness.emitter.emit(input)]);
  await started;
  assert.deepEqual(harness.outbox.list({ status: 'all' }), []);
  release();
  for (const result of await results) {
    assert.equal(result.status, 'rejected');
    if (result.status === 'rejected') assert.match(result.reason.message, /No route matched envelope/);
  }
  assert.equal(route.mock.callCount(), 2);
  assert.deepEqual(harness.sentEvents, []);
  assert.deepEqual(harness.outbox.list({ status: 'all' }), []);
  available = true;
  assert.equal((await harness.emitter.emit(input)).receipts[0].stage, 'dispatched');
});

test('keyed DND retries retain the original target and staged attachment online and offline', async t => {
  let offline = false;
  let settled = false;
  const harness = await emitterHarness(t, { extraMembers: ['aya'], plan: target => offline
    ? { error: markBeforeSendError(new PeerUnreachableError(target)) } : settled ? {}
      : { stage: 'queued', executionReceipt: { executor: 'gate-dnd', durable: true, handoff: 'queued' } } });
  let selected = harness.targetCard.nodeId;
  const route = t.mock.method(harness.kernel.ctx.services.router, 'route', async () => ({
    mode: 'unicast' as const, targetNodeIds: [selected], matchedBy: 'test', reason: 'available',
  }));
  const file = join(harness.dir, 'dnd.txt');
  writeFileSync(file, 'durable attachment');
  const input: EmitMessageInput = { networkId: harness.networkId, routing: { mode: 'auto' },
    kind: 'task', subject: 'dnd identity', body: 'preserve target', payload: {},
    idempotencyKey: 'dnd-key', filePaths: [file] };
  const first = await harness.emitter.emit(input);
  const staged = harness.outbox.get(first.messageId)!.stagedFiles;
  assert.equal(Object.keys(staged).length, 1);
  assert.equal(harness.outbox.get(first.messageId)?.status, 'delivered');
  assert.equal(harness.outbox.beginAttempt(first.messageId), undefined);
  selected = harness.targetByMember.get('aya')!.nodeId;
  const online = await harness.emitter.emit(input);
  assert.equal(online.messageId, first.messageId);
  assert.equal(online.receipts[0].executionReceipt?.executor, 'gate-dnd');
  offline = true;
  const missing = await harness.emitter.emit(input);
  assert.equal(missing.messageId, first.messageId);
  assert.deepEqual(missing.receipts, online.receipts, 'offline cannot downgrade prior durable custody');
  assert.ok(Object.values(staged).every(path => existsSync(path)));
  offline = false;
  settled = true;
  const delivered = await harness.emitter.emit(input);
  assert.equal(delivered.messageId, first.messageId);
  assert.equal(delivered.receipts[0].stage, 'dispatched');
  assert.equal(harness.outbox.beginAttempt(first.messageId, { reconcile: true }), undefined);
  assert.ok(Object.values(staged).every(path => !existsSync(path)));
  assert.equal(route.mock.callCount(), 1);
  assert.ok(harness.sendCalls.every(call => call.targetNodeId === harness.targetCard.nodeId));
  assert.ok(harness.sendCalls.every(call => call.envelope.id === first.messageId));
});

test('keyed mixed multicast recovery retries only the original failed sibling', async t => {
  for (const localFailure of [false, true]) {
    let recovered = false;
    const harness = await emitterHarness(t, { extraMembers: ['aya', 'bo'], plan: target =>
      target === harness.targetByMember.get('aya')!.nodeId && !recovered
        ? localFailure ? { error: new LocalDeliveryError('local protocol failure') }
          : { stage: 'rejected', detail: 'Gate closed', executionReceipt: { handoff: 'unattempted' } }
        : {} });
    const diz = harness.targetCard.nodeId;
    const aya = harness.targetByMember.get('aya')!.nodeId;
    const bo = harness.targetByMember.get('bo')!.nodeId;
    let selected = [diz, aya];
    const route = t.mock.method(harness.kernel.ctx.services.router, 'route', async () => ({
      mode: 'multicast' as const, targetNodeIds: selected, matchedBy: 'test', reason: 'available',
    }));
    const input: EmitMessageInput = { networkId: harness.networkId, routing: { mode: 'auto' },
      kind: 'task', subject: 'mixed obligation', body: 'pin siblings', payload: {}, idempotencyKey: 'mixed' };
    if (localFailure) await assert.rejects(harness.emitter.emit(input), /local protocol failure/);
    else await harness.emitter.emit(input);
    const first = harness.outbox.list({ status: 'failed' })[0];
    selected = [bo];
    if (localFailure) await assert.rejects(harness.emitter.emit(input), (error: any) => {
      assert.deepEqual(error.receipts, [first.deliveredReceipts[diz]]);
      return /local protocol failure/.test(error.message);
    });
    else {
      const retry = await harness.emitter.emit(input);
      assert.equal(retry.messageId, first.id);
      assert.deepEqual(retry.receipts.find(r => r.fromNodeId === diz), first.deliveredReceipts[diz]);
      assert.equal(retry.receipts.find(r => r.fromNodeId === aya)?.stage, 'rejected');
    }
    recovered = true;
    if (localFailure) {
      assert.equal(harness.sendCalls.filter(c => c.targetNodeId === aya).length, 1);
      harness.outbox.requeue(first.id);
      const service = new OutboxRedriveService({ outbox: harness.outbox, deliver: harness.emitter.redriveOutboxItem! });
      assert.equal((await service.flush({ force: true })).delivered, 1);
    }
    const recoveredResult = await harness.emitter.emit(input);
    assert.equal(recoveredResult.messageId, first.id);
    const item = harness.outbox.get(first.id)!;
    assert.equal(item.status, 'delivered');
    assert.deepEqual(new Set(harness.mailbox.listThreadRecords(first.id)[0].peerNodeIds), new Set([diz, aya]));
    assert.deepEqual(item.failedNodeIds, []);
    assert.deepEqual(item.failedReceipts, {});
    assert.equal(harness.outbox.list({ status: 'all' }).length, 1);
    assert.equal(harness.sendCalls.filter(c => c.targetNodeId === diz).length, 1);
    assert.equal(harness.sendCalls.filter(c => c.targetNodeId === aya).length, localFailure ? 2 : 3);
    assert.equal(harness.sendCalls.filter(c => c.targetNodeId === bo).length, 0);
    assert.equal(route.mock.callCount(), 1);
    assert.ok(harness.sendCalls.every(c => c.envelope.id === first.id));
  }
});


test('signed interrupted dispatch evidence never becomes a durable outbox queue receipt', async t => {
  const harness = await emitterHarness(t, { stage: 'dispatching' });
  const input: EmitMessageInput = { networkId: harness.networkId,
    routing: { mode: 'unicast', targets: ['member:diz'] }, kind: 'task', subject: 'interrupted',
    body: 'unknown execution is fenced', payload: {}, idempotencyKey: 'interrupted-key' };
  const first = await harness.emitter.emit(input);
  const second = await harness.emitter.emit(input);
  assert.equal(second.messageId, first.messageId);
  for (const result of [first, second]) {
    assert.equal(result.receipts.length, 1);
    assert.equal(result.receipts[0].stage, 'dispatching');
  }
  const item = harness.outbox.get(first.messageId)!;
  assert.equal(item.status, 'failed');
  assert.equal(item.failedReceipts[harness.targetCard.nodeId].stage, 'dispatching');
  assert.deepEqual(item.pendingNodeIds, []);
  assert.deepEqual(item.deliveredReceipts, {});
});


test('reconciliation returns a live pending receipt and retains the complete outbound route', async t => {
  let pending = false;
  const harness = await emitterHarness(t, { extraMembers: ['aya'], plan: target =>
    target === harness.targetByMember.get('aya')!.nodeId
      ? pending ? { stage: 'pending_approval' }
        : { stage: 'rejected', executionReceipt: { handoff: 'unattempted' } }
      : {} });
  const input: EmitMessageInput = { networkId: harness.networkId,
    routing: { mode: 'multicast', targets: ['member:diz', 'member:aya'] }, kind: 'task',
    subject: 'pending recovery', body: 'preserve observed evidence', payload: {}, idempotencyKey: 'pending-route' };
  const first = await harness.emitter.emit(input);
  pending = true;
  const second = await harness.emitter.emit(input);
  const aya = harness.targetByMember.get('aya')!.nodeId;
  assert.equal(second.messageId, first.messageId);
  assert.equal(second.receipts.find(r => r.fromNodeId === aya)?.stage, 'pending_approval');
  assert.ok(second.receipts.some(r => r.executionReceipt?.executor === 'outbox'));
  assert.deepEqual(new Set(harness.mailbox.listThreadRecords(first.messageId)[0].peerNodeIds),
    new Set([harness.targetCard.nodeId, aya]));
  assert.deepEqual(harness.sentEvents.at(-1)!.receipts, second.receipts);
});
