import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { request } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  DealerNodeRuntime,
  MailboxStore,
  createInboxExecutorPlugin,
  initProjectScaffold,
  rpc,
  runDealerCli,
} from '../packages/index.ts';
import type { ExecutionContext, MessageEnvelope } from '../packages/index.ts';

function envelope(
  id: string,
  networkId = 'team',
  member = 'alice',
  kind = 'triage',
): MessageEnvelope {
  return {
    id,
    networkId,
    traceId: id,
    from: { nodeId: member, member, nodeName: member },
    routing: { mode: 'broadcast' },
    kind,
    priority: 'normal',
    subject: id,
    body: 'body',
    payload: {},
    attachments: [],
    createdAt: new Date().toISOString(),
    signature: '',
  };
}

async function cli(state: string | DealerNodeRuntime, ...args: string[]) {
  const out: string[] = [];
  const err: string[] = [];
  const code = await runDealerCli(['inbox', ...args, '--json'], {
    ...(typeof state === 'string' ? { stateDir: state } : { runtime: state }),
    stdout: line => out.push(line),
    stderr: line => err.push(line),
  });
  return { code, value: JSON.parse((code ? err : out).join('\n')) as any };
}

function rawControl(path: string, socketPath: string, argv: string[]): Promise<Response> {
  return new Promise((resolve, reject) => {
    const req = request(
      { socketPath, path, method: 'POST', headers: { 'content-type': 'application/json' } },
      res => {
        const chunks: Buffer[] = [];
        res.on('data', chunk => chunks.push(Buffer.from(chunk)));
        res.on('end', () => resolve(new Response(Buffer.concat(chunks), { status: res.statusCode })));
      },
    );
    req.on('error', reject);
    req.end(JSON.stringify(argv));
  });
}

test('inbox CLI supports live, control socket and offline check/ack/list', async t => {
  const root = mkdtempSync(join(tmpdir(), 'inbox-cli-'));
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'state'),
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
  }).start();
  t.after(async () => {
    await runtime.stop();
    rmSync(root, { recursive: true, force: true });
  });
  await runtime.serveControl(async (argv, context) => {
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(argv, {
      runtime,
      stdout: line => stdout.push(line),
      stderr: line => stderr.push(line),
      signal: context.signal,
    });
    return { code, stdout, stderr };
  });

  runtime.mailbox.enqueue([envelope('live-a'), envelope('live-b', 'team', 'bob', 'other')]);
  const first = await cli(runtime, 'check');
  assert.equal(first.code, 0);
  assert.match(first.value.deliveryId, /^dlv_[0-9a-f]{12}$/);
  assert.equal(first.value.redelivered, false);
  assert.equal(first.value.mailboxId, 'default');
  assert.deepEqual(first.value.items.map((item: any) => item.id), ['live-a', 'live-b']);
  const redelivered = await cli(runtime, 'check');
  assert.equal(redelivered.value.deliveryId, first.value.deliveryId);
  assert.equal(redelivered.value.redelivered, true);

  const peeked = await cli(runtime, 'check', '--peek');
  assert.equal(peeked.value.deliveryId, first.value.deliveryId);
  assert.equal(peeked.value.redelivered, true);

  const listed = await cli(runtime, 'list');
  assert.equal(listed.value.leasedCount, 2);
  assert.equal(listed.value.queuedCount, 0);
  const aliased = await cli(runtime);
  assert.equal(aliased.value.totalCount, 2);

  const acked = await cli(runtime, 'check', '--ack', String(first.value.deliveryId));
  assert.equal(acked.value.deliveryId, null);
  assert.equal(acked.value.redelivered, false);
  const archived = await cli(runtime, 'list', '--all');
  assert.equal(archived.value.ackedCount, 2);
  const repeated = await cli(runtime, 'ack', String(first.value.deliveryId));
  assert.equal(repeated.value.alreadyAcknowledged, true);
  const unknown = await cli(runtime, 'ack', 'dlv_ffffffffffff');
  assert.equal(unknown.code, 1);
  assert.match(unknown.value.error, /Unknown or mismatched delivery lease/);

  runtime.mailbox.enqueue([envelope('streamed')], { mailboxId: 'stream:alerts' });
  const isolated = await cli(runtime, 'check', '--mailbox', 'stream:alerts');
  assert.equal(isolated.value.mailboxId, 'stream:alerts');
  assert.deepEqual(isolated.value.items.map((item: any) => item.id), ['streamed']);
  assert.equal((await cli(runtime, 'list')).value.totalCount, 3);
  const defaultOnly = await cli(runtime, 'list', '--mailbox', 'default');
  assert.equal(defaultOnly.value.queuedCount, 0);
  assert.equal(defaultOnly.value.leasedCount, 0);
  assert.equal(defaultOnly.value.ackedCount, 2);

  // Control socket path for an ordinary (non-waiting) command.
  runtime.mailbox.enqueue([envelope('socket-a')]);
  const socketCheck = await cli(runtime.stateDir, 'check');
  assert.equal(socketCheck.code, 0);
  assert.deepEqual(socketCheck.value.items.map((item: any) => item.id), ['socket-a']);
  assert.equal((await cli(runtime.stateDir, 'check')).value.deliveryId, socketCheck.value.deliveryId);
  assert.equal(
    (await cli(runtime.stateDir, 'check', '--ack', String(socketCheck.value.deliveryId))).code,
    0,
  );

  // Offline direct MailboxStore path.
  runtime.persist();
  await runtime.stop();
  const mailboxPath = join(runtime.stateDir, 'mailbox.json');
  assert.equal(statSync(mailboxPath).mode & 0o777, 0o600);
  assert.equal((await cli(runtime.stateDir, 'list', '--all')).value.ackedCount, 3);
  const offlineStore = new MailboxStore(mailboxPath);
  offlineStore.enqueue([envelope('offline-a')]);
  const offlineCheck = await cli(runtime.stateDir, 'check');
  assert.equal(offlineCheck.code, 0);
  assert.deepEqual(offlineCheck.value.items.map((item: any) => item.id), ['offline-a']);
  const offlineAck = await cli(
    runtime.stateDir,
    'ack',
    String(offlineCheck.value.deliveryId),
  );
  assert.equal(offlineAck.code, 0);
  assert.equal(offlineAck.value.alreadyAcknowledged, false);

  for (const args of [
    ['read', 'live-a'],
    ['show', 'live-a'],
    ['list', '--wait'],
    ['list', '--peek'],
    ['check', '--timeout-ms', '0'],
    ['check', '--timeout-ms', '10'],
    ['ack'],
    ['ack', 'dlv_ffffffffffff', '--all'],
    ['bogus'],
  ]) {
    assert.equal((await cli(runtime.stateDir, ...args)).code, 1, args.join(' '));
  }
  assert.equal(existsSync(join(runtime.stateDir, 'runtime.lock')), false);
  assert.equal(existsSync(join(runtime.stateDir, 'control.sock')), false);
});

test('offline CLI and a live runtime observe the same durable lease', async t => {
  const root = mkdtempSync(join(tmpdir(), 'inbox-shared-'));
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'state'),
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
  }).start();
  t.after(async () => {
    await runtime.stop();
    rmSync(root, { recursive: true, force: true });
  });
  await runtime.mailbox.enqueue([envelope('shared')]);
  assert.equal(existsSync(join(runtime.stateDir, 'control.sock')), false);
  const offline = await cli(runtime.stateDir, 'check');
  assert.equal(offline.code, 0);
  const leaseId = String(offline.value.deliveryId);
  assert.equal(runtime.mailbox.check().deliveryId, leaseId);
  assert.equal(runtime.mailbox.check().redelivered, true);
  await runtime.stop();
  const acked = await cli(runtime.stateDir, 'ack', leaseId);
  assert.equal(acked.code, 0);
  assert.equal(acked.value.alreadyAcknowledged, false);
  assert.equal(new MailboxStore(join(runtime.stateDir, 'mailbox.json')).list({ includeAcked: true }).ackedCount, 1);
});

test('inbox executor routes logical mailboxes, deduplicates and preserves attachments', async t => {
  const root = mkdtempSync(join(tmpdir(), 'inbox-plugin-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const path = join(root, 'mailbox.json');
  const store = new MailboxStore(path);
  const plugin = createInboxExecutorPlugin({ store });
  assert.throws(() => createInboxExecutorPlugin(), /requires store or inboxPath/);
  const first = envelope('shared');
  first.payload = { content: { keep: true }, localAttachments: ['/verified/file'] };
  first.attachments = [
    { name: 'file', mimeType: 'text/plain', sizeBytes: 4, blake3Hash: 'hash', ticket: 'ticket' },
    { name: 'absent', mimeType: 'text/plain', sizeBytes: 0, blake3Hash: 'hash2', ticket: 'ticket2' },
  ];
  const result = (await plugin.execute([first, first], {} as ExecutionContext)) as {
    count: number;
    inserted: string[];
  };
  assert.equal(result.count, 2);
  assert.deepEqual(result.inserted, ['shared']);
  const [item] = store.list().items;
  assert.deepEqual(item.envelope.payload, {
    content: { keep: true },
    localAttachments: ['/verified/file'],
  });
  assert.deepEqual(item.envelope.attachments.map(attachment => attachment.name), [
    'file',
    'absent',
  ]);
  assert.equal(Object.hasOwn(item.envelope.attachments[0], 'localPath'), false);

  const routed = envelope('routed');
  routed.payload = { mailbox: 'stream:alerts' };
  await plugin.execute([routed], {} as ExecutionContext);
  assert.equal(store.list({ mailboxId: 'stream:alerts' }).totalCount, 1);
  assert.equal(store.list({ mailboxId: 'default' }).totalCount, 1);
  await plugin.execute([first], {} as ExecutionContext);
  assert.equal(store.list({ mailboxId: 'default' }).totalCount, 1);
  assert.equal(statSync(path).mode & 0o777, 0o600);
});

test('topic defaults, P2P delivery and thread reverse routes converge end to end', async t => {
  const root = mkdtempSync(join(tmpdir(), 'inbox-routing-'));
  const make = (member: string, receiver = false) =>
    new DealerNodeRuntime({
      stateDir: join(root, member),
      member,
      name: member,
      profile: 'personal-dev',
      transport: { discoveryDir: join(root, 'mesh') },
      patch: {
        gateMode: 'open',
        executorConfig: { 'orca-wave': { runId: 'test' } },
      },
      commandRunner: async () => {
        throw new Error('Unexpected external executor');
      },
      executorOverrides: receiver
        ? [{ name: 'notify', async execute() { return { executor: 'notify' }; } }]
        : [],
    });
  const alice = await make('alice').start();
  const bob = await make('bob', true).start();
  t.after(async () => {
    await alice.stop();
    await bob.stop();
    rmSync(root, { recursive: true, force: true });
  });
  const project = join(root, 'project');
  initProjectScaffold(project, { projectId: 'one', defaultNetworkId: 'team' });
  const manifestPath = join(project, '.dealer', 'project.json');
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  manifest.topics = {
    triage: { description: 'Triage', defaultExecutor: 'inbox' },
    wave: { description: 'Wave', defaultExecutor: 'orca-wave' },
  };
  writeFileSync(manifestPath, JSON.stringify(manifest));
  await bob.linkProject(project);
  assert.equal(bob.resolveExecutorName({ kind: 'triage', payload: { executor: ' ' } }), 'inbox');
  assert.equal(bob.resolveExecutorName({ kind: 'other' }), 'orca-spawn');
  const conflicting = join(root, 'conflict');
  initProjectScaffold(conflicting, { projectId: 'two', defaultNetworkId: 'team' });
  writeFileSync(
    join(conflicting, '.dealer', 'project.json'),
    JSON.stringify({
      ...manifest,
      projectId: 'two',
      topics: { triage: { description: 'Triage', defaultExecutor: 'notify' } },
    }),
  );
  await assert.rejects(bob.linkProject(conflicting), /Conflicting defaultExecutor for topic: triage/);
  assert.equal(bob.config.projects.length, 1);
  for (const runtime of [alice, bob]) {
    await runtime.mountNetwork({ networkId: 'team', passphrase: 'secret' });
  }
  const deadline = Date.now() + 5000;
  while (!alice.node.listPeers().length || !bob.node.listPeers().length) {
    assert.ok(Date.now() < deadline, 'peer discovery');
    await new Promise(resolve => setTimeout(resolve, 25));
  }

  const file = join(root, 'attachment.txt');
  writeFileSync(file, 'verified inbox attachment');
  const sent = await alice.emitter.emit({
    networkId: 'team',
    routing: { mode: 'unicast', targets: ['member:bob'] },
    kind: 'triage',
    subject: 'triage',
    body: '{}',
    payload: {},
    semantic: {
      type: 'message',
      targetMailbox: 'task:one',
      replyMailbox: 'worktree:main',
    },
    filePaths: [file],
  });
  assert.equal(sent.receipts[0].stage, 'dispatched');
  const [item] = bob.mailbox.list({ mailboxId: 'task:one' }).items;
  assert.equal(item.envelope.semantic?.replyMailbox, 'worktree:main');
  assert.equal(
    readFileSync(item.envelope.payload.localAttachments[0], 'utf8'),
    'verified inbox attachment',
  );
  assert.equal(Object.hasOwn(item.envelope.payload, 'localAttachments'), true);
  assert.equal(statSync(join(bob.stateDir, 'mailbox.json')).mode & 0o777, 0o600);
  assert.equal(bob.mailbox.list({ mailboxId: 'task:one' }).totalCount, 1);
  assert.equal(bob.mailbox.list({ mailboxId: 'default' }).totalCount, 0);

  const inboundRoute = bob.mailbox.getThread(sent.messageId)!;
  assert.equal(inboundRoute.direction, 'inbound');
  assert.equal(inboundRoute.localMailbox, 'task:one');
  assert.deepEqual(
    bob.mailbox.resolveReplyTarget(sent.messageId),
    {
      threadId: sent.messageId,
      messageId: sent.messageId,
      peerNodeId: alice.transport.localNodeId,
      networkId: 'team',
      targetMailbox: 'worktree:main',
      localMailbox: 'task:one',
      traceId: sent.traceId,
      kind: 'triage',
      subject: 'triage',
    },
  );
  const outboundRoute = alice.mailbox.getThread(sent.messageId)!;
  assert.equal(outboundRoute.direction, 'outbound');
  assert.deepEqual(outboundRoute.peerNodeIds, [bob.transport.localNodeId]);

  const override = await alice.emitter.emit({
    networkId: 'team',
    routing: { mode: 'unicast', targets: ['member:bob'] },
    kind: 'triage',
    subject: 'override',
    body: '',
    payload: { executor: 'notify' },
  });
  assert.equal(override.receipts[0].stage, 'dispatched');
  assert.equal(bob.mailbox.list({ mailboxId: 'task:one' }).totalCount, 1);
  const queued = await alice.emitter.emit({
    networkId: 'team',
    routing: { mode: 'unicast', targets: ['member:bob'] },
    kind: 'wave',
    subject: 'wave',
    body: '',
    payload: {},
  });
  // Durable wave ingestion commits before the busy/idle decision, so dispatch always succeeds.
  assert.equal(queued.receipts[0].stage, 'dispatched');
  assert.equal(bob.mailbox.list({ mailboxId: 'stream:wave' }).totalCount, 1);
});

test('streaming control keeps long waits alive and aborts cleanly on disconnect', async t => {
  const root = mkdtempSync(join(tmpdir(), 'inbox-stream-'));
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'state'),
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
  }).start();
  t.after(async () => {
    await runtime.stop();
    rmSync(root, { recursive: true, force: true });
  });
  await runtime.serveControl(async (argv, context) => {
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(argv, {
      runtime,
      stdout: line => stdout.push(line),
      stderr: line => stderr.push(line),
      signal: context.signal,
    });
    return { code, stdout, stderr };
  }, { keepaliveMs: 40 });

  const waiting = rpc(
    runtime.socketPath,
    ['inbox', 'check', '--wait', '--timeout-ms', '5000', '--json'],
    { inactivityMs: 250 },
  );
  // Several keepalives (40ms cadence) span more than one 250ms inactivity window.
  await new Promise(resolve => setTimeout(resolve, 600));
  runtime.mailbox.enqueue([envelope('wake-live')]);
  const result = await waiting;
  assert.equal(result.code, 0);
  const payload = JSON.parse(result.stdout[0]);
  assert.equal(payload.timedOut, false);
  assert.deepEqual(payload.items.map((item: any) => item.id), ['wake-live']);
  assert.equal(payload.redelivered, false);

  const direct = await rawControl('/cli', runtime.socketPath, ['inbox', 'list', '--json']);
  assert.equal(direct.status, 200);
  const listed = await direct.json();
  assert.equal(listed.code, 0);
  assert.equal(JSON.parse(listed.stdout[0]).leasedCount, 1);

  const abandoned = new Promise<void>((resolve, reject) => {
    const req = request(
      {
        socketPath: runtime.socketPath,
        path: '/cli-stream',
        method: 'POST',
        headers: { 'content-type': 'application/json' },
      },
      res => {
        res.resume();
        res.on('end', () => resolve());
      },
    );
    req.on('error', error => {
      if ((error as NodeJS.ErrnoException).code === 'ECONNRESET') {
        resolve();
      } else {
        reject(error);
      }
    });
    req.on('close', () => resolve());
    req.end(
      JSON.stringify([
        'inbox',
        'check',
        '--mailbox',
        'abandoned',
        '--wait',
        '--timeout-ms',
        '30000',
        '--json',
      ]),
    );
    setTimeout(() => req.destroy(), 60);
  });
  await abandoned;
  await new Promise(resolve => setTimeout(resolve, 50));
  const after = await rpc(runtime.socketPath, ['inbox', 'list', '--json'], {
    inactivityMs: 2_000,
  });
  assert.equal(after.code, 0);
  assert.equal(JSON.parse(after.stdout[0]).totalCount, 1);
});


test('dealer help exposes structured usage', async () => {
  const out: string[] = [];
  const code = await runDealerCli(['--help'], { stateDir: join(tmpdir(), 'dealer-help-missing'), stdout: line => out.push(line) });
  assert.equal(code, 0);
  const value = JSON.parse(out.join('\n'));
  assert.equal(value.ok, true);
  assert.match(value.commands.inbox, /inbox/);
});
