import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DealerNodeRuntime, InboxStore, createInboxExecutorPlugin, initProjectScaffold, runDealerCli } from '../packages/index.ts';
import type { MessageEnvelope, ExecutionContext } from '../packages/index.ts';

function envelope(id: string, networkId = 'team', member = 'alice', kind = 'triage'): MessageEnvelope {
  return { id, networkId, traceId: id, from: { nodeId: member, member, nodeName: member },
    routing: { mode: 'broadcast' }, kind, priority: 'normal', subject: id, body: 'body', payload: {},
    attachments: [], createdAt: new Date().toISOString(), signature: '' };
}

async function cli(state: string | DealerNodeRuntime, ...args: string[]) {
  const out: string[] = [];
  const err: string[] = [];
  const code = await runDealerCli(['inbox', ...args, '--json'], {
    ...(typeof state === 'string' ? { stateDir: state } : { runtime: state }),
    stdout: line => out.push(line), stderr: line => err.push(line),
  });
  return { code, value: JSON.parse((code ? err : out).join('\n')) };
}

test('inbox snapshots deduplicate composite identity, serialize mutations and preserve read state', async t => {
  const root = mkdtempSync(join(tmpdir(), 'inbox-store-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const path = join(root, 'inbox.json');
  const store = new InboxStore(path);
  assert.throws(() => createInboxExecutorPlugin(), /requires store or inboxPath/);
  await Promise.all(Array.from({ length: 20 }, (_, i) => store.deliver([envelope(String(i))])));
  assert.equal((await store.list()).totalCount, 20);
  const read = await store.show('0', { markRead: true });
  assert.deepEqual(await store.deliver([envelope('0')]), { inserted: [], duplicateIds: ['0'] });
  assert.deepEqual(await store.show('0'), read);
  assert.deepEqual((await store.markRead({ id: '0' })).updated, []);
  assert.equal(statSync(path).mode & 0o777, 0o600);
  const second = new InboxStore(path);
  await second.deliver([envelope('other')]);
  await store.markRead({ id: 'other' });
  assert.equal((await second.show('other')).status, 'read');
  await store.deliver([envelope('0', 'other-network')]);
  await assert.rejects(store.show('0'), /Ambiguous inbox message/);
  await assert.rejects(store.markRead({ id: '0' }), /Ambiguous inbox message/);
  assert.equal((await store.show('0', { networkId: 'other-network' })).status, 'unread');
  await assert.rejects(store.show('missing'), /Inbox message not found/);
  await assert.rejects(store.markRead({}), /Specify either/);
  const scoped = await store.list({ networkId: 'team', status: 'read', limit: 1 });
  assert.equal(scoped.totalCount, 21);
  assert.equal(scoped.unreadCount, 19);
  assert.equal(scoped.items.length, 1);
  await store.markRead({ all: true, networkId: 'team' });
  assert.equal((await store.list()).unreadCount, 1);
  for (const invalid of ['{', '{}', '{"schema":"the-dealer-inbox/v1","items":[{}]}']) {
    writeFileSync(path, invalid);
    await assert.rejects(store.list(), /Invalid inbox store/);
  }
  writeFileSync(path, '{"schema":"the-dealer-inbox/v1","items":[]}');
  assert.equal((await store.list()).totalCount, 0);
});

test('inbox ordering compares instants across offsets with deterministic ties', async t => {
  const root = mkdtempSync(join(tmpdir(), 'inbox-order-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const path = join(root, 'inbox.json');
  const store = new InboxStore(path);
  await store.deliver([envelope('older'), envelope('newer'), envelope('tie')]);
  const saved = JSON.parse(readFileSync(path, 'utf8'));
  saved.items[0].receivedAt = '2026-11-10T09:00:00+08:00';
  saved.items[1].receivedAt = '2026-11-10T02:00:00Z';
  saved.items[2].receivedAt = '2026-11-10T10:00:00+08:00';
  writeFileSync(path, JSON.stringify(saved));
  assert.deepEqual((await store.list()).items.map(item => item.id), ['tie', 'newer', 'older']);
});

test('offline inbox mutations respect an embedded runtime ownership lock', async t => {
  const root = mkdtempSync(join(tmpdir(), 'inbox-owned-'));
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'state'), profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') } }).start();
  t.after(async () => {
    await runtime.stop();
    rmSync(root, { recursive: true, force: true });
  });
  await runtime.inbox.deliver([envelope('owned')]);
  const lockDir = join(runtime.stateDir, 'runtime.lock');
  const owner = readFileSync(join(lockDir, 'owner.json'), 'utf8');
  assert.equal(existsSync(join(runtime.stateDir, 'control.sock')), false);
  assert.equal((await cli(runtime.stateDir, 'list')).code, 0);
  assert.equal((await cli(runtime.stateDir, 'show', 'owned')).value.status, 'unread');
  for (const args of [['read', 'owned'], ['ack', 'owned'], ['show', 'owned', '--mark-read']]) {
    const result = await cli(runtime.stateDir, ...args);
    assert.equal(result.code, 1);
    assert.match(result.value.error, /State directory already owned/);
    assert.equal(readFileSync(join(lockDir, 'owner.json'), 'utf8'), owner);
  }
  assert.equal((await runtime.inbox.show('owned')).status, 'unread');
  await runtime.stop();
  assert.equal((await cli(runtime.stateDir, 'read', 'missing')).code, 1);
  assert.equal(existsSync(lockDir), false);
  assert.equal((await cli(runtime.stateDir, 'read', 'owned')).code, 0);
  assert.equal(existsSync(lockDir), false);
});

test('inbox CLI supports live, control socket and offline queries and acknowledgements', async t => {
  const root = mkdtempSync(join(tmpdir(), 'inbox-cli-'));
  const inboxPath = join(root, 'custom', 'mail.json');
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'state'), profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') }, patch: { executorConfig: { inbox: { inboxPath } } } }).start();
  t.after(async () => {
    await runtime.stop();
    rmSync(root, { recursive: true, force: true });
  });
  await runtime.serveControl(async args => {
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(args, { runtime, stdout: s => stdout.push(s), stderr: s => stderr.push(s) });
    return { code, stdout, stderr };
  });
  for (const mode of ['live', 'socket', 'offline']) {
    if (mode === 'offline') {
      runtime.persist();
      await runtime.stop();
    }
    const state = mode === 'live' ? runtime : runtime.stateDir;
    const store = new InboxStore(inboxPath);
    await store.deliver([envelope(`${mode}-a`), envelope(`${mode}-b`, 'team', 'bob', 'other')]);
    const listed = await cli(state, 'list');
    assert.equal(listed.code, 0);
    assert.equal(listed.value.status, 'unread');
    assert.equal((await cli(state, 'show', `${mode}-a`, '--mark-read')).value.status, 'read');
    assert.deepEqual((await cli(state, 'read', `${mode}-a`)).value.updated, []);
    assert.equal((await cli(state, 'list', '--status', 'read', '--topic', 'triage', '--from', 'alice', '--limit', '1')).value.items.length, 1);
    assert.equal((await cli(state, 'list', '--all', '--network', 'team')).value.items.length, ['live', 'socket', 'offline'].indexOf(mode) * 3 + 2);
    assert.deepEqual((await cli(state, 'ack', `${mode}-b`)).value.updated, [`${mode}-b`]);
    await store.deliver([envelope(`${mode}-c`)]);
    assert.deepEqual((await cli(state, 'ack', '--all', '--topic', 'triage', '--from', 'alice')).value.updated, [`${mode}-c`]);
    // Acknowledge without removing any history.
    assert.equal((await cli(state, 'read', '--all')).value.unreadCount, 0);
    for (const args of [['list', '--all', '--status', 'read'], ['list', '--status', 'bad'],
      ['list', '--limit', '0'], ['list', '--limit', '1.5'], ['read'], ['read', 'id', '--all'],
      ['read', 'id', '--topic', 'triage'], ['show']]) {
      assert.equal((await cli(state, ...args)).code, 1, args.join(' '));
    }
  }
  assert.equal(existsSync(join(runtime.stateDir, 'runtime.lock')), false);
  assert.equal(existsSync(join(runtime.stateDir, 'control.sock')), false);
});

test('topic defaults converge with wave policy and deliver verified attachments to inbox', async t => {
  const root = mkdtempSync(join(tmpdir(), 'inbox-routing-'));
  const make = (member: string, receiver = false) => new DealerNodeRuntime({
    stateDir: join(root, member), member, name: member, profile: 'personal-dev',
    transport: { discoveryDir: join(root, 'mesh') },
    patch: { gateMode: 'open', policies: ['wave-coalescer'],
      executorConfig: { 'orca-wave': { runId: 'test' } } },
    commandRunner: async () => { throw new Error('Unexpected external executor'); },
    executorOverrides: receiver ? [{ name: 'notify', async execute() { return { executor: 'notify' }; } }] : [],
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
  manifest.topics = { triage: { description: 'Triage', defaultExecutor: 'inbox' }, wave: { description: 'Wave', defaultExecutor: 'orca-wave' } };
  writeFileSync(manifestPath, JSON.stringify(manifest));
  await bob.linkProject(project);
  assert.equal(bob.resolveExecutorName({ kind: 'triage', payload: { executor: ' ' } }), 'inbox');
  assert.equal(bob.resolveExecutorName({ kind: 'other' }), 'orca-spawn');
  const conflicting = join(root, 'conflict');
  initProjectScaffold(conflicting, { projectId: 'two', defaultNetworkId: 'team' });
  writeFileSync(join(conflicting, '.dealer', 'project.json'), JSON.stringify({ ...manifest, projectId: 'two', topics: { triage: { description: 'Triage', defaultExecutor: 'notify' } } }));
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
  const sent = await runDealerCli(['send', '--network', 'team', '--to', 'member:bob', '--topic', 'triage', '--body', '{}', '--attach', file], {
    runtime: alice, stdout: () => {}, stderr: line => assert.fail(line),
  });
  assert.equal(sent, 0);
  const [item] = (await bob.inbox.list()).items;
  assert.equal(readFileSync(item.attachments[0].localPath, 'utf8'), 'verified inbox attachment');
  assert.equal(Object.hasOwn(item.payload, 'localAttachments'), false);
  assert.equal(statSync(join(bob.stateDir, 'inbox.json')).mode & 0o777, 0o600);
  const override = await alice.emitter.emit({ networkId: 'team', routing: { mode: 'unicast', targets: ['member:bob'] },
    kind: 'triage', subject: 'override', body: '', payload: { executor: 'notify' } });
  assert.equal(override.receipts[0].stage, 'dispatched');
  assert.equal((await bob.inbox.list()).totalCount, 1);
  const queued = await alice.emitter.emit({ networkId: 'team', routing: { mode: 'unicast', targets: ['member:bob'] },
    kind: 'wave', subject: 'wave', body: '', payload: {} });
  assert.equal(queued.receipts[0].stage, 'queued');
});


test('inbox plugin preserves canonical attachments, batch deduplication and stable ordering', async t => {
  const root = mkdtempSync(join(tmpdir(), 'inbox-plugin-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const path = join(root, 'inbox.json');
  const store = new InboxStore(path);
  const plugin = createInboxExecutorPlugin({ store });
  const first = envelope('shared');
  first.payload = { content: { keep: true }, localAttachments: ['/verified/file'] };
  first.attachments = [{ name: 'file', mimeType: 'text/plain', sizeBytes: 4, blake3Hash: 'hash', ticket: 'ticket' },
    { name: 'absent', mimeType: 'text/plain', sizeBytes: 0, blake3Hash: 'hash2', ticket: 'ticket2' }];
  const result = await plugin.execute([first, first], {} as ExecutionContext);
  assert.equal(result.count, 2);
  assert.deepEqual(result.inserted, ['shared']);
  const read = await store.show('shared', { markRead: true });
  assert.deepEqual(read.payload, { content: { keep: true } });
  assert.deepEqual(read.attachments.map(a => a.localPath), ['/verified/file', '']);
  assert.equal(Object.hasOwn(read.attachments[0], 'ticket'), false);
  await plugin.execute([first], {} as ExecutionContext);
  assert.deepEqual(await store.show('shared'), read);
  first.payload.content = 'mutated';
  assert.deepEqual((await store.show('shared')).payload, { content: { keep: true } });
  await store.deliver([envelope('shared', 'team', 'bob'), envelope('third')]);
  assert.equal((await store.list({ status: 'all' })).totalCount, 3);
  await assert.rejects(store.show('shared', { networkId: 'team' }), /Ambiguous inbox message/);
  const saved = JSON.parse(readFileSync(path, 'utf8'));
  for (const item of saved.items) {
    item.receivedAt = '2026-01-01T00:00:00.000Z';
  }
  writeFileSync(path, JSON.stringify(saved));
  assert.deepEqual((await store.list({ status: 'all' })).items.map(item => item.from.member + ':' + item.id),
    ['alice:third', 'bob:shared', 'alice:shared']);
  assert.deepEqual(await store.markRead({ all: true, from: 'bob' }), { updated: ['shared'], unreadCount: 0 });
  await Promise.all([store.deliver([envelope('last')]), store.markRead({ all: true })]);
  assert.equal((await store.list()).unreadCount, 0);
});
