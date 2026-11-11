import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync, statSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  DealerNodeRuntime, MailboxStore, LarkMailboxNotifier, WakeupRedriveService, createLarkMirrorPlugin,
  signEnvelope, generateNodeKeyPair, verifyEnvelope, runDealerCli,
  ensureOrcaWorkspace, checkOrcaWorkspaceIdle, resolveOrSpawnOrcaWorkspaceTerminal, defaultAntigravityLaunchCommand,
  validateTopicBindings, validateLarkNotify, initProjectScaffold,
} from '../packages/index.ts';
import type { MessageEnvelope, CommandRequest, CommandRunner } from '../packages/index.ts';

function directory(t: TestContext): string {
  const path = mkdtempSync(join(tmpdir(), 'dealer-requests-'));
  t.after(() => rmSync(path, { recursive: true, force: true }));
  return path;
}
const key = generateNodeKeyPair();
function envelope(id: string, patch: Partial<MessageEnvelope> = {}): MessageEnvelope {
  return signEnvelope({
    id, traceId: id, networkId: 'local', from: { nodeId: key.nodeId, member: 'alice', nodeName: 'laptop' },
    routing: { mode: 'unicast' }, kind: 'request', priority: 'normal', subject: id, body: 'Details',
    payload: {}, attachments: [], createdAt: new Date().toISOString(), ...patch,
  }, key.privateKeyPem);
}
async function cli(runtime: DealerNodeRuntime, ...args: string[]) {
  const out: string[] = [];
  const err: string[] = [];
  assert.equal(await runDealerCli([...args, '--json'], { runtime, stdout: line => out.push(line), stderr: line => err.push(line) }), 0, err.join('\n'));
  return JSON.parse(out.join('\n'));
}
function argument(call: CommandRequest, flag: string): string {
  return call.args[call.args.indexOf(flag) + 1];
}

test('thread envelopes survive settlement and reload; summaries and sequence scans clone history', t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const first = envelope('root');
  const reply = envelope('reply', { semantic: { type: 'reply', threadId: 'root' } });
  const settled = envelope('settled', { semantic: { type: 'settle', threadId: 'root', outcome: 'succeeded' } });
  store.recordInboundEnvelope(first, 'default');
  store.recordOutboundEnvelope(reply, [key.nodeId]);
  store.recordInboundEnvelope(settled, 'default');
  store.enqueue([first, reply, settled]);
  const reloaded = new MailboxStore(store.path);
  const records = reloaded.listThreadRecords('root');
  for (const record of records) {
    assert.ok(verifyEnvelope(record.envelope!));
    assert.equal(record.envelope!.id, record.messageId);
    assert.equal(record.state, 'settled');
  }
  const summary = reloaded.listThreads({ kind: 'request', state: 'settled', limit: 1 })[0];
  assert.equal(summary.rootMessageId, 'root');
  assert.equal(summary.hopCount, 3);
  summary.rootEnvelope!.body = 'mutated';
  assert.equal(reloaded.listThreads()[0].rootEnvelope!.body, 'Details');
  const items = reloaded.listItemsAfterSequence(0);
  assert.deepEqual(items.map(item => item.sequence), [1, 2]);
  assert.throws(() => reloaded.listThreads({ state: 'bad' as 'open' }));
  assert.throws(() => reloaded.listItemsAfterSequence(-2));
  const old = JSON.parse(readFileSync(store.path, 'utf8'));
  for (const record of Object.values(old.threads) as any[]) delete record.envelope;
  writeFileSync(store.path, JSON.stringify(old));
  assert.equal(new MailboxStore(store.path).listThreads()[0].rootEnvelope, undefined);
});

test('topic binding routes signed messages and control frames consistently with CLI threads', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({ stateDir: root, member: 'alice', profile: 'personal-dev',
    patch: { gateMode: 'open', topicBindings: { request: { executor: 'orca-wave', streamKey: 'requests', workspacePath: '/fixed', terminalHandle: 'term_fixed', coalesceMs: 60000 } } },
    wakeupIdleCheck: async () => ({ idle: false }),
  }).start();
  t.after(() => runtime.stop());
  assert.equal(runtime.mailbox.getWakeTarget('stream:requests')?.workspacePath, '/fixed');
  await cli(runtime, 'send', '--network', 'local', '--to', `member:alice`, '--topic', 'request', '--body', 'first', '--thread', 'thread-main');
  for (const type of ['question', 'reply', 'settle'] as const) {
    await runtime.emitter.emit({ networkId: 'local', routing: { mode: 'unicast', targets: ['member:alice'] }, kind: 'request', subject: type, body: type, payload: {},
      semantic: { type, threadId: 'thread-main', targetMailbox: 'default', ...(type === 'settle' ? { outcome: 'succeeded' as const } : {}) } });
  }
  const items = runtime.mailbox.listItemsAfterSequence(-1);
  assert.equal(items.length, 4);
  for (const item of items) {
    assert.equal(item.mailboxId, 'stream:requests');
    assert.ok(verifyEnvelope(item.envelope));
    assert.equal(item.envelope.semantic?.replyMailbox, 'stream:requests');
    assert.equal(item.envelope.payload.executor, undefined);
  }
  const shown = await cli(runtime, 'thread', 'show', 'thread-main');
  assert.equal(shown.records.length, 8);
  assert.ok(shown.records.every((record: any) => record.envelope && record.localMailbox === 'stream:requests'));
  const listed = await cli(runtime, 'thread', 'list', '--topic', 'request', '--state', 'settled', '--limit', '1');
  assert.equal(listed[0].threadId, 'thread-main');
  await runtime.emitter.emit({ networkId: 'local', routing: { mode: 'unicast', targets: ['member:alice'] }, kind: 'request', subject: 'explicit', body: 'explicit', payload: {}, semantic: { type: 'reply', targetMailbox: 'chosen', replyMailbox: 'caller' } });
  const explicit = runtime.mailbox.listItemsAfterSequence(3)[0];
  assert.equal(explicit.mailboxId, 'chosen');
  assert.equal(explicit.envelope.semantic?.replyMailbox, 'caller');
});

test('linked bindings override defaults, validate conflicts and refresh on unlink', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'state'), profile: 'personal-dev', patch: { topicBindings: { request: { executor: 'inbox' } } } }).start();
  t.after(() => runtime.stop());
  const project = join(root, 'project');
  initProjectScaffold(project, { projectId: 'example', defaultNetworkId: 'local' });
  const patch = join(root, 'patch.json');
  writeFileSync(patch, JSON.stringify({ topicBindings: { request: { executor: 'orca-wave', streamKey: 'linked', workspacePath: '/fixed', autoCreateWorkspace: true } } }));
  await runtime.linkProject(project, patch);
  assert.equal(runtime.resolveExecutorName({ kind: 'request' }), 'orca-wave');
  assert.equal(runtime.mailbox.getWakeTarget('stream:linked')?.repoRoot, project);
  const updated = JSON.parse(readFileSync(patch, 'utf8'));
  updated.topicBindings.request.coalesceMs = 5000;
  writeFileSync(patch, JSON.stringify(updated));
  await runtime.linkProject(project, patch);
  assert.equal(runtime.mailbox.getWakeTarget('stream:linked')?.coalesceMs, 5000);
  delete updated.topicBindings.request.coalesceMs;
  writeFileSync(patch, JSON.stringify(updated));
  await runtime.linkProject(project, patch);
  assert.equal(runtime.mailbox.getWakeTarget('stream:linked')?.coalesceMs, 0);
  const other = join(root, 'other');
  initProjectScaffold(other, { projectId: 'other', defaultNetworkId: 'local' });
  await assert.rejects(runtime.linkProject(other, patch), /Conflicting topicBindings/);
  await runtime.unlinkProject('example');
  assert.equal(runtime.resolveExecutorName({ kind: 'request' }), 'inbox');
  assert.throws(() => validateTopicBindings({ request: { executor: 'unknown' } }, ['inbox']));
  assert.throws(() => validateTopicBindings({ request: { executor: 'inbox', autoCreateWorkspace: 'yes' } }, ['inbox']));
  assert.throws(() => validateLarkNotify({ enabled: true, target: { chatId: 'a', userId: 'b' } }));
  assert.throws(() => validateLarkNotify({ enabled: true, includeKinds: [3] }));
  for (const option of ['notifyGateApprovals', 'mirrorOutboundThreads']) {
    assert.throws(() => validateLarkNotify({ enabled: true, [option]: 'yes' }));
    assert.doesNotThrow(() => validateLarkNotify({ enabled: true, [option]: false }));
  }
});

test('fixed workspaces bootstrap once and orphaned terminals neither block nor win resolution', async t => {
  const root = directory(t);
  const path = join(root, 'fixed');
  const calls: CommandRequest[] = [];
  let created = false;
  let orphanConnected = false;
  let orphanPreview = "";
  let orphanIdentity: string | undefined;
  let onlyOrphan = false;
  let agentCount = 1;
  const runner: CommandRunner = async call => {
    calls.push(call);
    if (call.args[0] === 'worktree' && call.args[1] === 'list') return { stdout: JSON.stringify({ ok: true, result: { worktrees: created ? [{ path }] : [] } }) };
    if (call.args[0] === 'worktree' && call.args[1] === 'create') {
      created = true;
      return { stdout: JSON.stringify({ ok: true, result: { worktree: { path } } }) };
    }
    if (call.args[1] === 'ps') return { stdout: JSON.stringify({ ok: true, result: { worktrees: [{ path, worktreeId: 'fixed', agents: Array.from({ length: agentCount }, () => ({ state: 'done' })), unverifiableTerminalCount: 0 }] } }) };
    if (call.args[0] === 'terminal' && call.args[1] === 'create') {
      return { stdout: JSON.stringify({ ok: true, result: { handle: 'spawned' } }) };
    }
    const terminals = [{ handle: 'orphan', orphaned: true, connected: orphanConnected, writable: orphanConnected, preview: orphanPreview, ...(orphanIdentity ? { agentIdentity: orphanIdentity } : {}), lastOutputAt: 100 }];
    if (!onlyOrphan) terminals.push({ handle: 'live', connected: true, writable: true, agentIdentity: 'antigravity', preview: '>', lastOutputAt: 1 });
    return { stdout: JSON.stringify({ ok: true, result: { terminals } }) };
  };
  const settings = join(root, 'settings.json');
  assert.deepEqual(await ensureOrcaWorkspace(runner, 'orca', { workspacePath: path, repoRoot: root, agySettingsPath: settings }), { workspacePath: path, created: true });
  assert.ok(calls[1].args.includes('--no-parent'));
  assert.deepEqual(JSON.parse(readFileSync(settings, 'utf8')).trustedWorkspaces, [path]);
  assert.equal((await ensureOrcaWorkspace(runner, 'orca', { workspacePath: path })).created, false);
  assert.equal((await checkOrcaWorkspaceIdle(runner, 'orca', path)).idle, true);
  assert.deepEqual(await resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', path), { handle: 'live', spawned: false });
  agentCount = 2;
  assert.equal((await checkOrcaWorkspaceIdle(runner, 'orca', path)).idle, false);
  orphanConnected = true;
  assert.equal((await checkOrcaWorkspaceIdle(runner, 'orca', path)).idle, true);
  orphanPreview = '> draft text';
  assert.equal((await checkOrcaWorkspaceIdle(runner, 'orca', path)).idle, false);
  orphanPreview = '';
  assert.deepEqual(await resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', path), { handle: 'live', spawned: false });
  onlyOrphan = true;
  orphanIdentity = 'antigravity';
  assert.deepEqual(await resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', path), { handle: 'orphan', spawned: false });
  orphanIdentity = undefined;
  assert.deepEqual(await resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', path), { handle: 'spawned', spawned: true });
  const missing: CommandRunner = async () => ({ stdout: JSON.stringify({ ok: true, result: { worktrees: [] } }) });
  await assert.rejects(ensureOrcaWorkspace(missing, 'orca', { workspacePath: path }), /repoRoot/);
});

test('mock Orca runner never mutates default Antigravity settings during workspace ensure', async t => {
  const root = directory(t);
  const path = join(root, 'mock-workspace');
  const settings = join(homedir(), '.gemini/antigravity-cli/settings.json');
  const before = existsSync(settings) ? readFileSync(settings) : undefined;
  const runner: CommandRunner = async call => {
    if (call.args[0] === 'worktree' && call.args[1] === 'create') {
      return { stdout: JSON.stringify({ ok: true, result: { worktree: { path } } }) };
    }
    return { stdout: JSON.stringify({ ok: true, result: { worktrees: [] } }) };
  };
  assert.deepEqual(await ensureOrcaWorkspace(runner, 'orca', { workspacePath: path, repoRoot: root }), { workspacePath: path, created: true });
  if (before) assert.deepEqual(readFileSync(settings), before);
});

test('spawned terminals wait for idle before a prefixed pointer is injected', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  let idleCalls = 0;
  const calls: CommandRequest[] = [];
  let spawned = false;
  const runner: CommandRunner = async call => {
    calls.push(call);
    if (call.args[1] === 'list') return { stdout: JSON.stringify({ ok: true, result: { terminals: spawned ? [{ handle: 'new', connected: true, writable: true }] : [] } }) };
    if (call.args[0] === 'terminal' && call.args[1] === 'create') {
      assert.deepEqual(call.args, ['terminal', 'create', '--worktree', `path:${call.cwd}`, '--command', defaultAntigravityLaunchCommand(), '--json']);
      spawned = true;
      return { stdout: JSON.stringify({ ok: true, result: { handle: 'new' } }) };
    }
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({ store, commandRunner: runner, idleCheck: async () => ({ idle: ++idleCalls !== 2 }), flushIntervalMs: 60000 });
  t.after(() => service.stop());
  store.enqueue([envelope('spawn')], { mailboxId: 'stream:fixed', wakeTarget: { mode: 'orca-terminal', workspacePath: '/fixed', wakeupPrefix: 'Read guide\nthen handle' } });
  const first = await service.flushMailbox('stream:fixed');
  assert.match(first.reason!, /Terminal spawned/);
  assert.equal(calls.some(call => call.args[1] === 'send'), false);
  await service.flushMailbox('stream:fixed', { force: true });
  const send = calls.find(call => call.args[1] === 'send')!;
  assert.match(argument(send, '--text'), /^Read guide then handle You have/);
  assert.equal(argument(send, '--text').includes('\n'), false);
});

test('Lark auth, personalized markdown, root threading, deleted root fallback and restart cursor', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  let deleted = false;
  const runner: CommandRunner = async call => {
    calls.push(call);
    if (call.args.includes('status')) return { stdout: JSON.stringify({ identities: { user: { openId: 'user' }, bot: { available: true } } }) };
    if (deleted && call.args.includes('+messages-reply')) {
      deleted = false;
      throw new Error('root message deleted');
    }
    return { stdout: JSON.stringify({ data: { message_id: `msg-${calls.length}` } }), stderr: '[lark-cli] [WARN] warning' };
  };
  const options = { store, stateDir: root, config: { enabled: true, profile: 'work' }, localNodeId: 'local', localMember: 'bob', commandRunner: runner };
  const notifier = new LarkMailboxNotifier(options);
  notifier.start();
  store.enqueue([envelope('root', { payload: { notification: { markdown: 'fallback', recipientMarkdown: { bob: 'Personal message' } } } })]);
  await notifier.flush();
  const send = calls.find(call => call.args.includes('+messages-send'))!;
  assert.equal(argument(send, '--markdown'), 'Personal message');
  assert.equal(argument(send, '--as'), 'bot');
  assert.equal(argument(send, '--user-id'), 'user');
  assert.equal(argument(send, '--idempotency-key').length, 44);
  store.enqueue([envelope('reply', { semantic: { type: 'reply', threadId: 'root' } })]);
  await notifier.flush();
  assert.ok(calls.at(-1)!.args.includes('--reply-in-thread'));
  assert.equal(argument(calls.at(-1)!, '--markdown'), '**[reply] alice**\n\nDetails');
  deleted = true;
  store.enqueue([envelope('next', { semantic: { type: 'question', threadId: 'root' } })]);
  await notifier.flush();
  assert.ok(calls.at(-1)!.args.includes('+messages-send'));
  assert.match(argument(calls.at(-1)!, '--idempotency-key'), /^dlr-r-/);
  assert.equal(argument(calls.at(-1)!, '--markdown'), '**[request] next**\n- Type: `question`\n- From: `alice` (`laptop`)\n- Mailbox: `default` | Priority: `normal`\n- Thread: `root`\n\nDetails');
  await notifier.stop();
  const before = calls.length;
  const restarted = new LarkMailboxNotifier(options);
  restarted.start();
  await restarted.flush();
  assert.equal(calls.length, before);
  await restarted.stop();
  assert.equal(statSync(join(root, 'lark-notify.json')).mode & 0o777, 0o600);
});

test('Lark filtering advances cursor and failed delivery fences later items until retry', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  let fail = true;
  let errors = 0;
  const runner: CommandRunner = async call => {
    calls.push(call);
    if (fail) throw new Error('network unavailable');
    return { stdout: '{"message_id":"delivered"}' };
  };
  store.enqueue([envelope('excluded', { kind: 'excluded' }), envelope('first'), envelope('second')]);
  const options = { store, stateDir: root, localNodeId: 'local', localMember: 'bob', commandRunner: runner,
    config: { enabled: true, target: { chatId: 'chat' }, excludeKinds: ['excluded'], retryBaseMs: 60000 }, onError: () => { errors++; } };
  const notifier = new LarkMailboxNotifier(options);
  notifier.start();
  await notifier.flush();
  assert.equal(errors, 1);
  assert.equal(calls.length, 1);
  assert.equal(JSON.parse(readFileSync(join(root, 'lark-notify.json'), 'utf8')).cursorSequence, 0);
  const idempotency = argument(calls[0], '--idempotency-key');
  await notifier.stop();
  fail = false;
  const restarted = new LarkMailboxNotifier(options);
  restarted.start();
  await restarted.flush();
  assert.equal(argument(calls[1], '--idempotency-key'), idempotency);
  assert.equal(calls.length, 3);
  assert.equal(JSON.parse(readFileSync(join(root, 'lark-notify.json'), 'utf8')).cursorSequence, 2);
  await restarted.stop();
});

test('runtime notifier refreshes linked mailboxes and broadcast preserves explicit thread ids', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'state'), member: 'alice', profile: 'personal-dev',
    commandRunner: async call => {
      calls.push(call);
      return { stdout: '{"message_id":"notified"}' };
    },
    patch: { gateMode: 'open', larkNotify: { enabled: true, target: { userId: 'bob' } } }, wakeupIdleCheck: async () => ({ idle: false }),
  }).start();
  t.after(() => runtime.stop());
  const project = join(root, 'project');
  initProjectScaffold(project, { projectId: 'p', defaultNetworkId: 'local' });
  const patch = join(root, 'binding.json');
  writeFileSync(patch, JSON.stringify({ topicBindings: { request: { executor: 'orca-wave', streamKey: 'personal', runId: 'run_personal' } } }));
  await runtime.linkProject(project, patch);
  assert.equal(runtime.mailbox.getWakeTarget('stream:personal')?.mode, 'orca-run');
  await cli(runtime, 'send', '--to', 'member:alice', '--network', 'local', '--topic', 'request', '--body', 'broadcast', '--thread', 'broadcast-thread');
  await runtime.larkNotifier!.flush();
  assert.equal(calls.length, 1);
  assert.match(argument(calls[0], '--markdown'), /broadcast-thread/);
  await runtime.transport.mountNetwork({ networkId: 'local', passphrase: 'test-secret' });
  await cli(runtime, 'broadcast', '--network', 'local', '--topic', 'request', '--body', 'broadcast', '--thread', 'actual-broadcast');
  assert.equal(runtime.mailbox.listThreadRecords('actual-broadcast')[0].envelope?.semantic?.threadId, 'actual-broadcast');
  await runtime.unlinkProject('p');
  runtime.mailbox.enqueue([envelope('after-unlink')], { mailboxId: 'stream:personal' });
  await runtime.larkNotifier!.flush();
  assert.equal(calls.length, 1);
});

test('control frames cannot override inbox using an explicit spawn executor', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({ stateDir: root, member: 'alice', profile: 'personal-dev',
    patch: { gateMode: 'open', topicBindings: { request: { executor: 'orca-wave', streamKey: 'fixed' } } }, wakeupIdleCheck: async () => ({ idle: false }),
    commandRunner: async () => { throw new Error('No external execution allowed'); },
  }).start();
  t.after(() => runtime.stop());
  await runtime.emitter.emit({ networkId: 'local', routing: { mode: 'unicast', targets: ['member:alice'] }, kind: 'request', subject: 'Question', body: 'Help',
    payload: { executor: 'orca-spawn' }, semantic: { type: 'question', targetMailbox: 'default' } });
  assert.equal(runtime.mailbox.listItemsAfterSequence(-1)[0].mailboxId, 'stream:fixed');
  assert.ok(verifyEnvelope(runtime.mailbox.listItemsAfterSequence(-1)[0].envelope));
});

test('native bootstrap stores canonical path and blocks newborn TUI until authoritative idle', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const path = join(root, 'canonical');
  const calls: CommandRequest[] = [];
  let spawned = false;
  let ready = false;
  const runner: CommandRunner = async call => {
    calls.push(call);
    if (call.args[0] === 'worktree' && call.args[1] === 'list') return { stdout: JSON.stringify({ ok: true, result: { worktrees: [] } }) };
    if (call.args[0] === 'worktree' && call.args[1] === 'create') return { stdout: JSON.stringify({ ok: true, result: { path } }) };
    if (call.args[1] === 'ps') return { stdout: JSON.stringify({ ok: true, result: { worktrees: [{ path, worktreeId: 'fixed', agents: [], unverifiableTerminalCount: 0 }] } }) };
    if (call.args[1] === 'list') return { stdout: JSON.stringify({ ok: true, result: { terminals: spawned ? [{ handle: 'term', connected: true, writable: true }] : [] } }) };
    if (call.args[0] === 'terminal' && call.args[1] === 'create') {
      assert.deepEqual(call.args, ['terminal', 'create', '--worktree', `path:${call.cwd}`, '--command', defaultAntigravityLaunchCommand(), '--json']);
      spawned = true;
      return { stdout: '{"ok":true,"result":{"handle":"term"}}' };
    }
    if (call.args[1] === 'wait') return { stdout: JSON.stringify({ ok: ready }) };
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({ store, commandRunner: runner, flushIntervalMs: 60000 });
  t.after(() => service.stop());
  store.enqueue([envelope('newborn')], { wakeTarget: { mode: 'orca-terminal', workspacePath: join(root, 'desired'), repoRoot: root, autoCreateWorkspace: true } });
  await service.flushMailbox('default');
  assert.equal(store.getWakeTarget('default')?.workspacePath, path);
  assert.ok(calls.some(call => call.args.includes('5000')));
  assert.equal(calls.some(call => call.args[1] === 'send'), false);
  ready = true;
  await service.flushMailbox('default', { force: true });
  assert.equal(calls.filter(call => call.args[1] === 'send').length, 1);
});

test('Lark filtering honors explicit empty mailboxes, kinds, self and notification overrides', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  store.enqueue([envelope('self'), envelope('skip-kind', { kind: 'other' })]);
  store.enqueue([envelope('private')], { mailboxId: 'private' });
  store.enqueue([envelope('allowed', { from: { nodeId: 'peer', member: 'peer', nodeName: 'other' }, payload: { notification: { markdown: 'Shared override' } } })], { mailboxId: 'bound' });
  const calls: CommandRequest[] = [];
  const notifier = new LarkMailboxNotifier({ store, stateDir: root, defaultBoundMailboxes: ['bound'], localNodeId: key.nodeId, localMember: 'bob',
    config: { enabled: true, skipSelfMessages: true, includeKinds: ['request'], excludeMailboxes: ['private'] }, commandRunner: async call => {
      calls.push(call);
      return { stdout: call.args.includes('status') ? '{"identities":{"user":{"openId":"user"},"bot":{"available":false}}}' : '{"result":{"message_id":"root"}}' };
    },
  });
  notifier.start();
  await notifier.flush();
  assert.equal(calls.length, 2);
  assert.equal(argument(calls[1], '--as'), 'user');
  assert.equal(argument(calls[1], '--markdown'), 'Shared override');
  await notifier.stop();
  const none = new LarkMailboxNotifier({ store, stateDir: join(root, 'none'), localNodeId: 'local', localMember: 'bob', config: { enabled: true, mailboxes: [] },
    commandRunner: async () => { throw new Error('Must be filtered'); },
  });
  none.start();
  await none.flush();
  await none.stop();
  assert.equal(JSON.parse(readFileSync(join(root, 'none/lark-notify.json'), 'utf8')).cursorSequence, 3);
});

test('Lark retry timer preserves idempotency and stops without advancing a failed cursor', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  store.enqueue([envelope('retry')]);
  const keys: string[] = [];
  let resolveDelivered!: () => void;
  const delivered = new Promise<void>(resolve => { resolveDelivered = resolve; });
  const notifier = new LarkMailboxNotifier({ store, stateDir: root, localNodeId: 'local', localMember: 'bob', config: { enabled: true, target: { chatId: 'chat' }, retryBaseMs: 5 },
    commandRunner: async call => {
      keys.push(argument(call, '--idempotency-key'));
      if (keys.length === 1) throw new Error('network unavailable');
      resolveDelivered();
      return { stdout: '{"data":{"messageId":"retry-ok"}}' };
    },
  });
  t.after(() => notifier.stop());
  notifier.start();
  let timer: ReturnType<typeof setTimeout>;
  await Promise.race([delivered, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error('Retry never arrived')), 2000); })]);
  clearTimeout(timer!);
  await notifier.flush();
  assert.equal(keys.length, 2);
  assert.equal(keys[0], keys[1]);
  assert.equal(JSON.parse(readFileSync(join(root, 'lark-notify.json'), 'utf8')).cursorSequence, 0);
});

for (const type of ['question', 'reply', 'settle'] as const) {
  test(`Lark ${type} without a prior local root retains full thread context`, async t => {
    const root = directory(t);
    const store = new MailboxStore(join(root, 'mailbox.json'));
    store.enqueue([envelope(`first-${type}`, { semantic: {
      type, threadId: 'outbound-thread', ...(type === 'settle' ? { outcome: 'succeeded' as const } : {}),
    } })]);
    const calls: CommandRequest[] = [];
    const notifier = new LarkMailboxNotifier({
      store, stateDir: root, localNodeId: 'local', localMember: 'bob',
      config: { enabled: true, target: { chatId: 'chat' } },
      commandRunner: async call => {
        calls.push(call);
        return { stdout: '{"message_id":"new-root"}' };
      },
    });
    t.after(() => notifier.stop());
    notifier.start();
    await notifier.flush();
    assert.equal(calls.length, 1);
    assert.ok(calls[0].args.includes('+messages-send'));
    assert.equal(argument(calls[0], '--markdown'),
      `**[request] first-${type}**\n- Type: \`${type}\`\n- From: \`alice\` (\`laptop\`)\n- Mailbox: \`default\` | Priority: \`normal\`\n- Thread: \`outbound-thread\`\n\nDetails`);
  });
}

test('bare-shell bootstrap defers task text until the next authoritative idle tick', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  let detected = false;
  const runner: CommandRunner = async call => {
    calls.push(call);
    if (call.args[1] === 'ps') return { stdout: JSON.stringify({ ok: true, result: { worktrees: [{
      path: root, worktreeId: 'fixed', agents: detected ? [{ state: 'done' }] : [], unverifiableTerminalCount: 0,
    }] } }) };
    if (call.args[1] === 'list') return { stdout: JSON.stringify({ ok: true, result: { terminals: [{
      handle: 'shell-1', connected: true, writable: true, preview: detected ? '>' : 'user@mac %',
      ...(detected ? { agentIdentity: 'antigravity' } : {}),
    }] } }) };
    assert.equal(call.args[1], 'send');
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({ store, commandRunner: runner, flushIntervalMs: 60000 });
  t.after(() => service.stop());
  store.enqueue([envelope('shell-bootstrap')], { wakeTarget: { mode: 'orca-terminal', workspacePath: root } });
  const first = await service.flushMailbox('default');
  assert.equal(first.woke, false);
  assert.match(first.reason!, /Terminal spawned; waiting for tui-idle/);
  const sends = () => calls.filter(call => call.args[1] === 'send');
  assert.equal(sends().length, 1);
  assert.equal(argument(sends()[0], '--terminal'), 'shell-1');
  assert.equal(argument(sends()[0], '--text'), defaultAntigravityLaunchCommand());
  assert.equal(store.getWakeTarget('default')?.pendingWake, true);
  detected = true;
  const second = await service.flushMailbox('default', { force: true });
  assert.equal(second.woke, true);
  assert.equal(sends().length, 2);
  assert.equal(argument(sends()[1], '--terminal'), 'shell-1');
  assert.match(argument(sends()[1], '--text'), /^You have 1 Dealer message/);
});

test('temporary workspace bootstrap does not modify default Antigravity settings', async t => {
  const root = directory(t);
  const path = join(root, 'not-created-by-mock');
  const settings = join(homedir(), '.gemini/antigravity-cli/settings.json');
  const before = existsSync(settings) ? readFileSync(settings) : undefined;
  const runner: CommandRunner = async call => ({ stdout: JSON.stringify({ ok: true, result:
    call.args[1] === 'list' ? { worktrees: [] } : { path },
  }) });
  assert.equal((await ensureOrcaWorkspace(runner, 'orca', { workspacePath: path, repoRoot: root })).created, true);
  const after = existsSync(settings) ? readFileSync(settings) : undefined;
  assert.deepEqual(after, before);
});

test('lark-mirror turns gate knocks into roots and approval delivery into thread replies', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runtime = await new DealerNodeRuntime({ stateDir: root, member: 'bob', profile: 'personal-dev',
    patch: { gateMode: 'approval_required', topicBindings: { request: { executor: 'inbox' } }, larkNotify: { enabled: true, target: { chatId: 'copilot' } } },
    commandRunner: async call => {
      calls.push(call);
      return { stdout: JSON.stringify({ message_id: `lark-${calls.length}` }) };
    },
  }).start();
  t.after(() => runtime.stop());
  const request = envelope('knock', { semantic: { type: 'message', threadId: 'approval-thread' } });
  const verdict = await runtime.gate.evaluate(request);
  assert.equal(verdict.status, 'pending_approval');
  if (verdict.status !== 'pending_approval') throw new Error('Expected pending approval');
  await runtime.larkNotifier!.flush();
  assert.equal(calls.length, 1);
  assert.match(argument(calls[0], '--markdown'), /\[gate · pending_approval\]/);
  assert.ok(argument(calls[0], '--markdown').includes(`dealer gate approve ${verdict.approvalId} --trust-sender --json`));
  await runtime.gate.evaluate(request);
  await runtime.larkNotifier!.flush();
  assert.equal(calls.length, 1, 'repeated gate evaluation emits no duplicate knock');
  await runtime.gate.approve(verdict.approvalId);
  await runtime.larkNotifier!.flush();
  assert.equal(calls.length, 2);
  assert.equal(argument(calls[1], '--message-id'), 'lark-1');
  assert.ok(calls[1].args.includes('--reply-in-thread'));
  const state = JSON.parse(readFileSync(join(root, 'lark-notify.json'), 'utf8'));
  assert.ok(state.deliveredItems[`approval:${verdict.approvalId}`]);
  assert.equal(state.threadRoots['approval-thread'].messageId, 'lark-1');
});

test('lark-mirror mirrors outbound thread events and deduplicates local loopback arrivals', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runtime = await new DealerNodeRuntime({ stateDir: root, member: 'bob', profile: 'personal-dev',
    patch: { gateMode: 'open', larkNotify: { enabled: true, target: { chatId: 'copilot' } } },
    commandRunner: async call => {
      calls.push(call);
      return { stdout: JSON.stringify({ message_id: `lark-${calls.length}` }) };
    },
  }).start();
  t.after(() => runtime.stop());
  runtime.mailbox.enqueue([envelope('remote-root')]);
  await runtime.larkNotifier!.flush();
  for (const type of ['reply', 'question', 'settle'] as const) {
    const outgoing = envelope(`out-${type}`, { from: { nodeId: runtime.transport.localNodeId, member: 'bob', nodeName: 'desktop' },
      semantic: { type, threadId: 'remote-root', ...(type === 'settle' ? { outcome: 'succeeded' as const } : {}) } });
    runtime.kernel.ctx.emit('emitter/sent', { envelope: outgoing, targetNodeIds: [key.nodeId], receipts: [] });
    await runtime.larkNotifier!.flush();
    assert.equal(argument(calls.at(-1)!, '--message-id'), 'lark-1');
    assert.equal(argument(calls.at(-1)!, '--markdown'), `**[${type} · outbound] bob**\n\nDetails`);
    runtime.kernel.ctx.emit('emitter/sent', { envelope: outgoing, targetNodeIds: [key.nodeId], receipts: [] });
    await runtime.larkNotifier!.flush();
  }
  assert.equal(calls.length, 4);
  await runtime.emitter.emit({ networkId: 'local', routing: { mode: 'unicast', targets: ['member:bob'] }, kind: 'request',
    subject: 'loopback', body: 'local reply', payload: {}, semantic: { type: 'reply', threadId: 'remote-root' } });
  await runtime.larkNotifier!.flush();
  assert.equal(calls.length, 5, 'local mailbox delivery is mirrored exactly once');
  assert.equal(argument(calls[4], '--markdown'), '**[reply] bob**\n\nlocal reply');
  const unrooted = envelope('unrooted', { from: { nodeId: runtime.transport.localNodeId, member: 'bob', nodeName: 'desktop' } });
  runtime.kernel.ctx.emit('emitter/sent', { envelope: unrooted, targetNodeIds: [key.nodeId], receipts: [] });
  await runtime.larkNotifier!.flush();
  assert.equal(calls.length, 5, 'outbound messages never create unsolicited roots');
});

test('smart Lark defaults omit compound and project background bindings but permit explicit opt-in', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'state'), member: 'bob', profile: 'personal-dev',
    patch: { gateMode: 'open', larkNotify: { enabled: true, target: { chatId: 'copilot' } } },
    commandRunner: async call => { calls.push(call); return { stdout: '{"message_id":"root"}' }; },
  }).start();
  t.after(() => runtime.stop());
  const project = join(root, 'project');
  const manifest = initProjectScaffold(project, { projectId: 'background', defaultNetworkId: 'local' });
  writeFileSync(join(project, '.dealer/project.json'), JSON.stringify({ ...manifest, topics: {
    wave: { description: 'Background wave', defaultExecutor: 'orca-wave' },
    spawn: { description: 'Background spawn', defaultExecutor: 'orca-spawn' },
    pty: { description: 'Background PTY', defaultExecutor: 'agent-pty' },
    inbox: { description: 'Inbox', defaultExecutor: 'inbox' },
  } }));
  const patch = join(root, 'binding.json');
  const bindings = Object.fromEntries(['wave:stream', 'wave', 'spawn', 'pty', 'inbox', 'request'].map(kind =>
    [kind, { executor: 'inbox', mailboxId: `bound-${kind.replace(':', '-')}` }]));
  writeFileSync(patch, JSON.stringify({ topicBindings: bindings }));
  await runtime.linkProject(project, patch);
  for (const kind of Object.keys(bindings)) runtime.mailbox.enqueue([envelope(`default-${kind}`, { kind })], { mailboxId: bindings[kind].mailboxId });
  await runtime.larkNotifier!.flush();
  assert.equal(calls.length, 2, 'only inbox and top-level request defaults notify');
  writeFileSync(patch, JSON.stringify({ topicBindings: Object.fromEntries(Object.entries(bindings).map(([kind, binding]) =>
    [kind, { ...binding, notifyLark: kind !== 'request' }])) }));
  await runtime.linkProject(project, patch);
  for (const kind of Object.keys(bindings)) runtime.mailbox.enqueue([envelope(`explicit-${kind}`, { kind })], { mailboxId: bindings[kind].mailboxId });
  await runtime.larkNotifier!.flush();
  assert.equal(calls.length, 7, 'explicit true opts background mailboxes in; false opts request out');
});

test('lark-mirror seeds existing approvals and retries failed persistence with the same idempotency key', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'node'), member: 'bob', profile: 'personal-dev',
    patch: { gateMode: 'approval_required' },
  }).start();
  t.after(() => runtime.stop());
  const verdict = await runtime.gate.evaluate(envelope('seeded-knock'));
  assert.equal(verdict.status, 'pending_approval');
  if (verdict.status !== 'pending_approval') throw new Error('Expected approval');
  const blocked = join(root, 'blocked');
  writeFileSync(blocked, 'prevent atomic state write');
  const calls: CommandRequest[] = [];
  let resolveRetry!: () => void;
  const retried = new Promise<void>(resolve => { resolveRetry = resolve; });
  const options = { store: runtime.mailbox, stateDir: blocked, localNodeId: runtime.transport.localNodeId, localMember: 'bob',
    config: { enabled: true, target: { chatId: 'copilot' }, retryBaseMs: 5 },
    commandRunner: async (call: CommandRequest) => {
      calls.push(call);
      if (calls.length === 2) resolveRetry();
      return { stdout: '{"message_id":"seeded-root"}' };
    },
    onError: () => rmSync(blocked),
  };
  const notifier = new LarkMailboxNotifier(options);
  await runtime.kernel.use(createLarkMirrorPlugin({ ...options, notifier }));
  let timer: ReturnType<typeof setTimeout>;
  try {
    await Promise.race([retried, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error('Approval retry never arrived')), 2000);
    })]);
  } finally {
    clearTimeout(timer!);
  }
  await notifier.flush();
  assert.equal(calls.length, 2);
  assert.equal(argument(calls[0], '--idempotency-key'), argument(calls[1], '--idempotency-key'));
  const state = JSON.parse(readFileSync(join(blocked, 'lark-notify.json'), 'utf8'));
  assert.equal(state.schema, 'the-dealer-lark-notify/v1');
  assert.ok(state.deliveredItems[`approval:${verdict.approvalId}`]);
  await runtime.stop();
  notifier.notifyApproval({ id: 'after-stop', envelope: envelope('after-stop'), reason: 'test', createdAt: new Date().toISOString() });
  await notifier.flush();
  assert.equal(calls.length, 2, 'Kernel shutdown stops notifier delivery');
});

test('Lark mirror flags and envelope filters suppress gate and outbound events independently of mailbox selection', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  const options = { store, stateDir: root, localNodeId: 'local', localMember: 'bob',
    config: { enabled: true, target: { chatId: 'copilot' }, mailboxes: [], includeKinds: ['request'], excludeKinds: ['blocked'], skipSelfMessages: true },
    commandRunner: async (call: CommandRequest) => { calls.push(call); return { stdout: '{"message_id":"root"}' }; },
  };
  const notifier = new LarkMailboxNotifier(options);
  t.after(() => notifier.stop());
  notifier.start();
  for (const [id, message] of [
    ['self', envelope('self', { from: { nodeId: 'local', member: 'bob', nodeName: 'local' } })],
    ['excluded', envelope('excluded', { kind: 'blocked' })],
    ['not-included', envelope('not-included', { kind: 'other' })],
    ['allowed', envelope('allowed')],
  ] as const) notifier.notifyApproval({ id, envelope: message, reason: 'test', createdAt: new Date().toISOString() });
  await notifier.flush();
  assert.equal(calls.length, 1, 'mailboxes: [] does not suppress allowed gate knocks');
  const outgoing = envelope('outgoing', { from: { nodeId: 'local', member: 'bob', nodeName: 'local' }, semantic: { type: 'reply', threadId: 'allowed' } });
  notifier.notifyOutbound(outgoing);
  notifier.notifyOutbound(envelope('remote', { semantic: { type: 'reply', threadId: 'allowed' } }));
  notifier.notifyOutbound({ ...outgoing, id: 'excluded-out', kind: 'blocked' });
  await notifier.flush();
  assert.equal(calls.length, 2, 'outbound local thread replies ignore skipSelfMessages but honor origin and kinds');
  await notifier.stop();
  const disabled = new LarkMailboxNotifier({ ...options, config: { ...options.config, notifyGateApprovals: false, mirrorOutboundThreads: false } });
  t.after(() => disabled.stop());
  disabled.start();
  disabled.notifyApproval({ id: 'disabled', envelope: envelope('disabled'), reason: 'test', createdAt: new Date().toISOString() });
  disabled.notifyOutbound({ ...outgoing, id: 'disabled-out' });
  await disabled.flush();
  assert.equal(calls.length, 2);
});
