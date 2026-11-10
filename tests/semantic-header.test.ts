import test from 'node:test';
import assert from 'node:assert/strict';
import { Kernel } from '../packages/kernel/index.ts';
import {
  canonicalJson,
  canonicalizeEnvelopeForSigning,
  generateNodeKeyPair,
  resolveSemanticHeader,
  signCanonicalPayload,
  signEnvelope,
  signNodeCard,
  signReceipt,
  validateSemanticHeader,
  verifyEnvelope,
} from '../packages/index.ts';
import type {
  MessageEnvelope,
  NodeCard,
  NodeService,
  SemanticHeader,
  TransportService,
} from '../packages/index.ts';
import { createRouterPlugin } from '../packages/router/index.ts';
import { createEmitterPlugin } from '../packages/emitter/index.ts';

const sender = generateNodeKeyPair();
const receiver = generateNodeKeyPair();

function base(): Omit<MessageEnvelope, 'signature'> {
  return {
    id: 'message-1',
    traceId: 'trace-1',
    networkId: 'test',
    from: { nodeId: sender.nodeId, member: 'alice', nodeName: 'laptop' },
    routing: { mode: 'unicast', targets: [receiver.nodeId] },
    kind: 'task',
    priority: 'normal',
    subject: 'Task',
    body: 'Do it',
    payload: {},
    attachments: [],
    createdAt: '2026-11-10T00:00:00.000Z',
  };
}

function peerCard(): NodeCard {
  return signNodeCard(
    {
      nodeId: receiver.nodeId,
      networkId: 'test',
      revision: 1,
      identity: { member: 'bob', nodeName: 'worker' },
      admissionMode: 'open',
      capabilities: { summary: 'Worker', skills: [] },
      runtimeState: { load: 'idle', activeTasks: 0, availableAgents: ['codex'] },
      updatedAt: '2026-11-10T00:00:00.000Z',
    },
    receiver.privateKeyPem,
  );
}

test('semantic headers are covered by the Ed25519 envelope signature', () => {
  const semantic: SemanticHeader = {
    type: 'question',
    threadId: 'thread-1',
    targetMailbox: 'task:message-1',
    replyMailbox: 'worktree:main',
  };
  const signed = signEnvelope({ ...base(), semantic }, sender.privateKeyPem);
  assert.ok(verifyEnvelope(signed));
  assert.match(canonicalizeEnvelopeForSigning(signed), /"semantic":/);

  for (const tampered of [
    { ...signed, semantic: { ...semantic, threadId: 'thread-2' } },
    { ...signed, semantic: { ...semantic, replyMailbox: 'worktree:evil' } },
    { ...signed, semantic: { ...semantic, targetMailbox: 'task:other' } },
  ]) {
    assert.equal(verifyEnvelope(tampered), false);
  }

  const settle = signEnvelope(
    {
      ...base(),
      id: 'settle-1',
      semantic: { type: 'settle', threadId: 'thread-1', outcome: 'succeeded' },
    },
    sender.privateKeyPem,
  );
  assert.ok(verifyEnvelope(settle));
  assert.equal(
    verifyEnvelope({ ...settle, semantic: { ...settle.semantic!, outcome: 'failed' } }),
    false,
  );
  assert.equal(
    verifyEnvelope({ ...settle, semantic: { ...settle.semantic!, outcome: undefined } }),
    false,
  );
  assert.equal(
    verifyEnvelope({ ...signed, semantic: { ...semantic, threadId: '' } }),
    false,
  );
});

test('envelopes without a semantic header sign byte-for-byte as v1', () => {
  const payload = base();
  const signed = signEnvelope(payload, sender.privateKeyPem);
  assert.equal(canonicalizeEnvelopeForSigning(signed), canonicalJson(signed));
  assert.equal(signed.signature, signCanonicalPayload(payload, sender.privateKeyPem));
  assert.ok(verifyEnvelope(signed));

  const withUndefined = signEnvelope({ ...payload, semantic: undefined }, sender.privateKeyPem);
  assert.equal(withUndefined.signature, signed.signature);
  assert.equal(canonicalizeEnvelopeForSigning(withUndefined), canonicalJson(withUndefined));
});

test('semantic validation enforces types, mailboxes and settlement outcomes', () => {
  assert.throws(
    () => validateSemanticHeader({ type: 'bogus', threadId: 'x' }),
    /Invalid semantic message type/,
  );
  assert.throws(
    () => validateSemanticHeader({ type: 'message', threadId: '' }),
    /threadId must be a non-empty string/,
  );
  assert.throws(
    () => validateSemanticHeader({ type: 'message', threadId: 'x', targetMailbox: 'bad\nbox' }),
    /Invalid semantic targetMailbox/,
  );
  assert.throws(
    () => validateSemanticHeader({ type: 'message', threadId: 'x', replyMailbox: ' ' }),
    /Invalid semantic replyMailbox/,
  );
  assert.throws(
    () => validateSemanticHeader({ type: 'message', threadId: 'x', outcome: 'succeeded' }),
    /only valid for settle/,
  );
  assert.throws(
    () => validateSemanticHeader({ type: 'settle', threadId: 'x' }),
    /Settlement outcome required/,
  );
  assert.throws(
    () => validateSemanticHeader({ type: 'settle', threadId: 'x', outcome: 'unknown' }),
    /Settlement outcome required/,
  );
  assert.deepEqual(resolveSemanticHeader({ type: 'message' }, 'generated-id'), {
    type: 'message',
    threadId: 'generated-id',
  });
  assert.deepEqual(
    resolveSemanticHeader({ type: 'reply', threadId: 'root', replyMailbox: 'a' }, 'ignored'),
    { type: 'reply', threadId: 'root', replyMailbox: 'a' },
  );
});

test('emitter normalizes semantic input, rejects invalid headers and emits sent events', async t => {
  const kernel = new Kernel();
  const card = peerCard();
  const sent: MessageEnvelope[] = [];
  const events: { envelope: MessageEnvelope; targetNodeIds: readonly string[] }[] = [];
  const node: NodeService = {
    getSelfCard: networkId =>
      signNodeCard(
        {
          nodeId: sender.nodeId,
          networkId,
          revision: 1,
          identity: { member: 'alice', nodeName: 'laptop' },
          admissionMode: 'open',
          capabilities: { summary: 'Sender', skills: [] },
          runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] },
          updatedAt: '2026-11-10T00:00:00.000Z',
        },
        sender.privateKeyPem,
      ),
    async updateSelfProfile() {},
    listPeers: () => [card],
    resolveTarget: selector => (selector === card.nodeId ? [card] : []),
  };
  const transport: TransportService = {
    localNodeId: sender.nodeId,
    async mountNetwork() {
      throw new Error('unused');
    },
    async unmountNetwork() {},
    listMountedNetworks: () => ['test'],
    async sendUnicast(networkId, targetNodeId, frame) {
      const parsed = JSON.parse(Buffer.from(frame).toString('utf8')) as {
        envelope: MessageEnvelope;
      };
      sent.push(parsed.envelope);
      return Buffer.from(
        JSON.stringify(
          signReceipt(
            {
              messageId: parsed.envelope.id,
              networkId,
              fromNodeId: targetNodeId,
              stage: 'dispatched',
              updatedAt: new Date().toISOString(),
            },
            receiver.privateKeyPem,
          ),
        ),
      );
    },
    async sendMulticast() {
      return new Map();
    },
    async broadcast() {},
    async shareFile() {
      throw new Error('unused');
    },
    async fetchBlobToPath() {},
  };
  kernel.ctx.provide('node', node);
  kernel.ctx.provide('transport', transport);
  await kernel.use(createRouterPlugin());
  await kernel.use(createEmitterPlugin({ privateKeyPem: sender.privateKeyPem }));
  t.after(() => kernel.stop());
  const emitter = kernel.ctx.services.emitter;
  kernel.ctx.on('emitter/sent', event => {
    events.push(event);
  });

  const input = {
    networkId: 'test',
    routing: { mode: 'unicast' as const, targets: [receiver.nodeId] },
    kind: 'task',
    subject: 'Task',
    body: 'Do it',
    payload: {},
  };
  const result = await emitter.emit({
    ...input,
    semantic: { type: 'question', replyMailbox: 'worktree:main' },
  });
  assert.equal(result.receipts.length, 1);
  assert.equal(result.receipts[0].stage, 'dispatched');
  assert.equal(sent.length, 1);
  assert.deepEqual(sent[0].semantic, {
    type: 'question',
    threadId: sent[0].id,
    replyMailbox: 'worktree:main',
  });
  assert.ok(verifyEnvelope(sent[0]));
  assert.equal(events.length, 1);
  assert.equal(events[0].envelope.id, sent[0].id);
  assert.deepEqual(events[0].targetNodeIds, [receiver.nodeId]);

  await assert.rejects(
    emitter.emit({
      ...input,
      semantic: { type: 'message', outcome: 'succeeded' },
    }),
    /only valid for settle/,
  );
  assert.equal(sent.length, 1);

  await assert.rejects(
    emitter.emit({
      ...input,
      semantic: { type: 'message', threadId: 'x', replyMailbox: 'bad\nmailbox' },
    }),
    /Invalid semantic replyMailbox/,
  );
  assert.equal(sent.length, 1);
});

test('prepare hooks cannot inject an invalid semantic header', async t => {
  const kernel = new Kernel();
  const card = peerCard();
  const node: NodeService = {
    getSelfCard: networkId =>
      signNodeCard(
        {
          nodeId: sender.nodeId,
          networkId,
          revision: 1,
          identity: { member: 'alice', nodeName: 'laptop' },
          admissionMode: 'open',
          capabilities: { summary: 'Sender', skills: [] },
          runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] },
          updatedAt: '2026-11-10T00:00:00.000Z',
        },
        sender.privateKeyPem,
      ),
    async updateSelfProfile() {},
    listPeers: () => [card],
    resolveTarget: selector => (selector === card.nodeId ? [card] : []),
  };
  const transport: TransportService = {
    localNodeId: sender.nodeId,
    async mountNetwork() {
      throw new Error('unused');
    },
    async unmountNetwork() {},
    listMountedNetworks: () => ['test'],
    async sendUnicast(networkId, targetNodeId, frame) {
      const envelope = JSON.parse(Buffer.from(frame).toString('utf8')).envelope;
      return Buffer.from(
        JSON.stringify(
          signReceipt(
            {
              messageId: envelope.id,
              networkId,
              fromNodeId: targetNodeId,
              stage: 'dispatched',
              updatedAt: new Date().toISOString(),
            },
            receiver.privateKeyPem,
          ),
        ),
      );
    },
    async sendMulticast() {
      return new Map();
    },
    async broadcast() {},
    async shareFile() {
      throw new Error('unused');
    },
    async fetchBlobToPath() {},
  };
  kernel.ctx.provide('node', node);
  kernel.ctx.provide('transport', transport);
  await kernel.use(createRouterPlugin());
  await kernel.use(createEmitterPlugin({ privateKeyPem: sender.privateKeyPem }));
  t.after(() => kernel.stop());
  kernel.ctx.on('emitter/prepare', (message, next) =>
    next({
      ...message,
      semantic: { type: 'settle' } as unknown as SemanticHeader,
    }),
  );
  await assert.rejects(
    kernel.ctx.services.emitter.emit({
      networkId: 'test',
      routing: { mode: 'unicast', targets: [receiver.nodeId] },
      kind: 'task',
      subject: 'Task',
      body: 'Do it',
      payload: {},
      semantic: { type: 'message' },
    }),
    /Settlement outcome required/,
  );
});

test('prepare hooks may inject a message semantic without a threadId', async t => {
  const kernel = new Kernel();
  const card = peerCard();
  const sent: MessageEnvelope[] = [];
  const node: NodeService = {
    getSelfCard: networkId =>
      signNodeCard(
        {
          nodeId: sender.nodeId,
          networkId,
          revision: 1,
          identity: { member: 'alice', nodeName: 'laptop' },
          admissionMode: 'open',
          capabilities: { summary: 'Sender', skills: [] },
          runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] },
          updatedAt: '2026-11-10T00:00:00.000Z',
        },
        sender.privateKeyPem,
      ),
    async updateSelfProfile() {},
    listPeers: () => [card],
    resolveTarget: selector => (selector === card.nodeId ? [card] : []),
  };
  const transport: TransportService = {
    localNodeId: sender.nodeId,
    async mountNetwork() {
      throw new Error('unused');
    },
    async unmountNetwork() {},
    listMountedNetworks: () => ['test'],
    async sendUnicast(networkId, targetNodeId, frame) {
      const envelope = JSON.parse(Buffer.from(frame).toString('utf8')).envelope as MessageEnvelope;
      sent.push(envelope);
      return Buffer.from(
        JSON.stringify(
          signReceipt(
            {
              messageId: envelope.id,
              networkId,
              fromNodeId: targetNodeId,
              stage: 'dispatched',
              updatedAt: new Date().toISOString(),
            },
            receiver.privateKeyPem,
          ),
        ),
      );
    },
    async sendMulticast() {
      return new Map();
    },
    async broadcast() {},
    async shareFile() {
      throw new Error('unused');
    },
    async fetchBlobToPath() {},
  };
  kernel.ctx.provide('node', node);
  kernel.ctx.provide('transport', transport);
  await kernel.use(createRouterPlugin());
  await kernel.use(createEmitterPlugin({ privateKeyPem: sender.privateKeyPem }));
  t.after(() => kernel.stop());
  kernel.ctx.on('emitter/prepare', (message, next) =>
    next({
      ...message,
      semantic: {
        type: 'message',
        targetMailbox: 'task:w1',
        replyMailbox: 'worktree:main',
      } as unknown as SemanticHeader,
    }),
  );

  const input = {
    networkId: 'test',
    routing: { mode: 'unicast' as const, targets: [receiver.nodeId] },
    kind: 'task',
    subject: 'Task',
    body: 'Do it',
    payload: {},
  };
  const result = await kernel.ctx.services.emitter.emit(input);
  assert.equal(sent.length, 1);
  assert.equal(result.messageId, sent[0].id);
  assert.deepEqual(sent[0].semantic, {
    type: 'message',
    threadId: sent[0].id,
    targetMailbox: 'task:w1',
    replyMailbox: 'worktree:main',
  });
  assert.ok(verifyEnvelope(sent[0]));

  kernel.ctx.on('emitter/prepare', (message, next) =>
    next({ ...message, id: 'forged-message-id' }),
  );
  await assert.rejects(
    kernel.ctx.services.emitter.emit(input),
    /Prepare hook cannot change message id/,
  );
  assert.equal(sent.length, 1);
});
