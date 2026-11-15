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
  rmSync,
  truncateSync,
  writeFileSync,
} from 'node:fs';
import { request } from 'node:http';
import { createConnection, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { DealerNodeRuntime } from '../packages/cli/runtime.ts';
import { runDealerCli } from '../packages/cli/index.ts';
import { Kernel } from '../packages/kernel/index.ts';
import { createEmitterPlugin } from '../packages/emitter/index.ts';
import { createRouterPlugin } from '../packages/router/index.ts';
import { OutboxRedriveService, OutboxStore } from '../packages/mailbox/outbox.ts';
import { createPollWatermarkSourcePlugin } from '../packages/plugins-source/index.ts';
import { blake3 } from '../packages/transport/index.ts';
import type { TransportPluginOptions } from '../packages/transport/index.ts';
import type { RuntimeConfig } from '../packages/cli/config.ts';
import {
  PeerUnreachableError,
  deriveNetworkKeys,
  generateNodeKeyPair,
  isConfirmedHandoffReceipt,
  isUnresolvedTargetError,
  sealNetworkFrame,
  signEnvelope,
  signNodeCard,
  signReceipt,
} from '../packages/core-types/index.ts';
import type {
  DeliveryReceipt,
  DeliveryStage,
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
  const stderr: string[] = [];
  const rejected = await runDealerCli(['outbox', 'list', '--status', 'bogus', '--json'], {
    runtime,
    stderr: line => stderr.push(line),
  });
  assert.notEqual(rejected, 0);
  assert.match(stderr.join('\n'), /--status must be queued, delivered, failed or all/);
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
  options: { stage?: DeliveryStage; sendError?: Error } = {},
) {
  const dir = directory(t);
  const networkId = 'harness-net';
  const sender = generateNodeKeyPair();
  const receiver = generateNodeKeyPair();
  const targetCard = cardFor(receiver, networkId, 'diz');
  const selfCard = cardFor(sender, networkId, 'ben');
  const outbox = new OutboxStore(join(dir, 'outbox.json'), join(dir, 'outbox-blobs'));
  const sendCalls: Array<{
    targetNodeId: string;
    opts?: {
      timeoutMs?: number;
      spoolIfOffline?: boolean;
      spoolKey?: string;
      spoolBlobHashes?: readonly string[];
    };
    envelope: MessageEnvelope;
  }> = [];
  const transport = {
    localNodeId: sender.nodeId,
    async mountNetwork() {
      throw new Error('unused');
    },
    async unmountNetwork() {},
    listMountedNetworks: () => [networkId],
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
      sendCalls.push({ targetNodeId, ...(opts ? { opts } : {}), envelope });
      if (options.sendError) {
        throw options.sendError;
      }
      const receipt = signReceipt(
        {
          messageId: envelope.id,
          networkId: targetNetworkId,
          fromNodeId: targetNodeId,
          stage: options.stage ?? 'dispatched',
          ...(options.stage === 'rejected' ? { detail: 'gate closed' } : {}),
          updatedAt: new Date().toISOString(),
        },
        receiver.privateKeyPem,
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
    listPeers: () => [targetCard],
    resolveTarget: (selector: string) =>
      selector === targetCard.nodeId || selector === 'member:diz' ? [targetCard] : [],
  } as unknown as NodeService;
  const kernel = new Kernel();
  kernel.ctx.provide('transport', transport);
  kernel.ctx.provide('node', node);
  await kernel.use(createRouterPlugin());
  await kernel.use(
    createEmitterPlugin({ privateKeyPem: sender.privateKeyPem, outbox }),
  );
  t.after(() => kernel.stop());
  return {
    dir,
    networkId,
    sender,
    receiver,
    targetCard,
    outbox,
    emitter: kernel.ctx.services.emitter,
    sendCalls,
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
    assert.equal(failed.failedReceipt?.receipt.stage, stage);
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
  assert.equal(existsSync(`${outboxPath}.tmp`), false);
});

function framed(value: unknown): Buffer {
  const body = Buffer.from(JSON.stringify(value));
  const size = Buffer.alloc(4);
  size.writeUInt32BE(body.length);
  return Buffer.concat([size, body]);
}

function tcpRequest(port: number, body: unknown, timeoutMs = 10_000): Promise<any> {
  return new Promise((resolve, reject) => {
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
  let spool = JSON.parse(readFileSync(spoolFile, 'utf8')) as {
    entries: Array<{ id: string; blobs?: unknown; blobHashes?: Record<string, number> }>;
  };
  assert.equal(spool.entries.length, 2);
  assert.ok(spool.entries.every(entry => entry.blobs === undefined));

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
  spool = JSON.parse(readFileSync(spoolFile, 'utf8'));
  assert.equal(
    spool.entries.find(entry => entry.id.endsWith(':blob'))?.blobHashes?.[hash],
    blobBody.length,
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
  const entriesBefore = (
    JSON.parse(readFileSync(spoolFile, 'utf8')) as { entries: unknown[] }
  ).entries.length;
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
  const entriesAfter = (
    JSON.parse(readFileSync(spoolFile, 'utf8')) as { entries: unknown[] }
  ).entries.length;
  assert.equal(entriesAfter, entriesBefore, 'an incomplete attachment offer is never spooled');

  await seed.stop();
  spool = JSON.parse(readFileSync(spoolFile, 'utf8'));
  for (const entry of spool.entries) {
    (entry as { expiresAt: string }).expiresAt = new Date(Date.now() - 1_000).toISOString();
  }
  writeFileSync(spoolFile, JSON.stringify(spool));
  seed = await createSeed(port);
  await seed.mountNetwork({ networkId: NETWORK, passphrase: 'secret' });
  assert.equal(existsSync(blobPath), false, 'expired spool blobs are garbage collected');
  const pruned = JSON.parse(readFileSync(spoolFile, 'utf8')) as { entries: unknown[] };
  assert.equal(pruned.entries.length, 0);
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
  const add = (targetNodeId: string, count: number, offset: number) => {
    for (let index = 0; index < count; index += 1) {
      entries.push({
        id: `${sender.nodeId}:key-${sequence}`,
        targetNodeId,
        networkId: NETWORK,
        frame: sealNetworkFrame({ sequence }, keys, sender.nodeId),
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
  const saved = JSON.parse(readFileSync(spoolFile, 'utf8')) as {
    entries: Array<{ targetNodeId: string }>;
  };
  assert.equal(saved.entries.length, 1024, 'global entry budget is enforced');
  assert.equal(
    saved.entries.filter(entry => entry.targetNodeId === hotTarget).length,
    256,
    'per-target entry budget is enforced',
  );
});
