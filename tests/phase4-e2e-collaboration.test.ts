import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, realpathSync, writeFileSync, readFileSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve as resolvePath } from 'node:path';
import { spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import {
  DealerNodeRuntime,
  DEDUP_CAS_DUPLICATE_DETAIL,
  DEDUP_CAS_AMBIGUOUS_DETAIL,
  generateNodeKeyPair,
  signEnvelope,
  isDedupCasDuplicateReceipt,
  isConfirmedHandoffReceipt,
  runDealerCli,
  getBuiltInProfile,
  readPatch,
  validateConfig,
  decodeNetworkInvite,
  encodeNetworkInvite,
  createLocalAgentRouterPlugin,
  createPollWatermarkSourcePlugin,
  blake3,
  readProjectManifest,
  initProjectScaffold,
} from '../packages/index.ts';
import type { DealerRuntimeOptions, CommandRequest, EmitMessageInput } from '../packages/index.ts';

async function until(check: () => boolean, message = 'condition'): Promise<void> {
  const end = Date.now() + 5000;
  while (!check()) {
    if (Date.now() > end) {
      throw new Error(`Timed out: ${message}`);
    }
    await new Promise(resolve => setTimeout(resolve, 25));
  }
}

function directory(t: TestContext) {
  const path = mkdtempSync(join(tmpdir(), 'dealer-e2e-'));
  t.after(() => rmSync(path, { recursive: true, force: true }));
  return path;
}

async function node(t: TestContext, root: string, member: string, options: DealerRuntimeOptions = {}) {
  const runtime = new DealerNodeRuntime({
    stateDir: join(root, member),
    member,
    name: member,
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
    ...options,
  });
  await runtime.start();
  t.after(() => runtime.stop());
  return runtime;
}

async function cli(runtime: DealerNodeRuntime, ...args: string[]) {
  const out: string[] = [];
  const err: string[] = [];
  assert.equal(
    await runDealerCli(['--json', ...args], {
      runtime,
      stdout: line => out.push(line),
      stderr: line => err.push(line),
    }),
    0,
    err.join('\n'),
  );
  return JSON.parse(out.join('\n'));
}

const input = (to: string, payload = {}): EmitMessageInput => ({
  networkId: 'sensebeing',
  routing: { mode: 'unicast', targets: [`member:${to}`] },
  kind: 'collab.request.v1',
  subject: 'Please investigate',
  body: 'details',
  payload,
});

async function connect(...nodes: DealerNodeRuntime[]) {
  for (const runtime of nodes) {
    await cli(runtime, 'network', 'mount', 'sensebeing', '--passphrase', 'team-secret');
  }
  await until(() => nodes.every(n => n.node.listPeers().length === nodes.length - 1), 'peer discovery');
}

test('Phase 4: cross-member P2P attachment, approval and trusted sender dispatch', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const alice = await node(t, root, 'alice');
  const bob = await node(t, root, 'bob', {
    profile: 'personal-dev',
    patch: { gateMode: 'approval_required' },
    commandRunner: async request => {
      calls.push(request);
      return {
        stdout: JSON.stringify({
          ok: true,
          result:
            request.args[0] === 'worktree'
              ? { path: join(root, 'spawned') }
              : { terminal: 'fake-external-terminal' },
        }),
      };
    },
  });
  await connect(alice, bob);
  const path = join(root, 'spec.txt');
  const content = 'real shared spec\n'.repeat(5000);
  writeFileSync(path, content);
  const sent = await cli(
    alice,
    'send',
    '--network',
    'sensebeing',
    '--to',
    'member:bob',
    '--topic',
    'collab.request.v1',
    '--body',
    '{"area":"runtime"}',
    '--attach',
    path,
  );
  assert.equal(sent.receipts[0].stage, 'pending_approval');
  assert.equal(calls.length, 0);
  const pending = await cli(bob, 'gate', 'list');
  assert.equal(pending.length, 1);
  assert.equal(pending[0].envelope.attachments[0].blake3Hash, blake3(Buffer.from(content)));
  await cli(bob, 'gate', 'approve', sent.messageId, '--trust-sender');
  assert.equal(calls.length, 2);
  assert.equal((await cli(bob, 'gate', 'list')).length, 0);
  const localPath = join(bob.stateDir, 'attachments', sent.messageId, '0-spec.txt');
  assert.equal(readFileSync(localPath, 'utf8'), content);
  assert.match(calls[1].args.join(' '), /localAttachments/);
  assert.equal((await alice.emitter.emit(input('bob'))).receipts[0].stage, 'dispatched');
  assert.equal(calls.length, 4);
});

test('Phase 4: local agent judge routes using discovered live NodeCards', async t => {
  const root = directory(t);
  const alice = await node(t, root, 'alice');
  const bob = await node(t, root, 'bob', { skills: ['ios-crash'] });
  const carol = await node(t, root, 'carol', { skills: ['backend'] });
  await connect(alice, bob, carol);
  await carol.node.updateRuntimeState({ load: 'busy', activeTasks: 1 });
  await until(() => alice.node.listPeers().find(c => c.identity.member === 'carol')?.runtimeState.load === 'busy');
  let judgments = 0;
  alice.kernel.getService('router').registerJudge(
    createLocalAgentRouterPlugin({
      judgeRunner: async prompt => {
        judgments++;
        const data = JSON.parse(prompt.slice(prompt.indexOf('\n') + 1));
        assert.equal(JSON.parse(data.task.body).category, 'ios-crash');
        const candidate = data.candidates.find(
          (c: any) => c.load === 'idle' && c.skills.some((s: any) => s.id === 'ios-crash'),
        );
        assert.equal(candidate.member, 'bob');
        return JSON.stringify({
          mode: 'unicast',
          targetNodeIds: [candidate.nodeId],
          reason: 'idle iOS specialist',
        });
      },
    }),
  );
  const result = await cli(
    alice,
    'send',
    '--network',
    'sensebeing',
    '--auto-route',
    '--topic',
    'triage',
    '--body',
    '{"category":"ios-crash"}',
  );
  assert.equal(judgments, 1);
  assert.equal(result.receipts[0].fromNodeId, bob.transport.localNodeId);
  assert.equal(result.receipts[0].stage, 'dispatched');
  assert.match(readFileSync(join(bob.stateDir, 'notifications.jsonl'), 'utf8'), /ios-crash/);
});

test('Phase 4: polling, CAS dedup, durable busy waves and run-managed delivery after ack', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  let idle = false;
  const runner = async (request: CommandRequest) => {
    calls.push(request);
    return { stdout: '{"ok":true}' };
  };
  const alice = await node(t, root, 'alice');
  const bob = await node(t, root, 'bob', {
    profile: 'dedicated-worker',
    commandRunner: runner,
    wakeupIdleCheck: async () => ({ idle }),
    patch: {
      defaultExecutor: 'orca-wave',
      executorConfig: { 'orca-wave': { runId: 'test-run', terminalHandle: 'test-terminal' } },
      wave: { rotateAfterWaves: 1, flushIntervalMs: 100000 },
    },
  });
  await connect(alice, bob);
  let cursor = 0;
  const source = createPollWatermarkSourcePlugin({
    initialCursor: 0,
    settlementMode: 'queued-or-settled',
    poll: async current => ({
      items: [{ id: `alert-${current}`, revision: 1 }],
      cursor: current + 1,
    }),
    toInput: item => ({
      ...input('bob', { executor: 'orca-wave', streamKey: 'alerts', revision: item.revision }),
      idempotencyKey: item.id,
    }),
    saveCursor: async value => {
      cursor = value;
    },
  });
  const registration = alice.emitter.registerSource(source);
  await source.pollOnce();
  await source.pollOnce();
  assert.equal(cursor, 2);
  assert.equal(calls.length, 0, 'busy workspace must not receive text');
  const duplicate = await alice.emitter.emit({
    ...input('bob', { executor: 'orca-wave', streamKey: 'alerts', revision: 1 }),
    idempotencyKey: 'alert-0',
  });
  assert.equal(duplicate.receipts[0].stage, 'dispatched', 'sender replays its durable handoff');
  // Bypass sender obligations explicitly to exercise the receiver CAS boundary too.
  const wireDuplicate = await alice.emitter.emit({
    ...input('bob', { executor: 'orca-wave', streamKey: 'alerts', revision: 1 }),
    idempotencyKey: 'alert-0', queueIfOffline: false,
  });
  assert.equal(wireDuplicate.receipts[0].stage, 'rejected');
  assert.equal(bob.mailbox.list({ mailboxId: 'stream:alerts' }).queuedCount, 2);

  idle = true;
  await bob.flushWakeups();
  const orchestration = calls.filter(c => c.args[0] === 'orchestration');
  assert.equal(orchestration.length, 1);
  const pointer = String(orchestration[0].args[orchestration[0].args.indexOf('--body') + 1]);
  assert.match(pointer, /^You have 2 Dealer message\(s\) in stream:alerts/);
  assert.equal(/[\r\n]/.test(pointer), false);
  assert.equal(
    orchestration[0].args[orchestration[0].args.indexOf('--subject') + 1],
    'Dealer wakeup (stream:alerts)',
  );

  const lease = bob.mailbox.check({ mailboxId: 'stream:alerts' });
  assert.equal(lease.items.length, 2);
  bob.mailbox.ack(lease.deliveryId!);
  await source.pollOnce();
  assert.equal(cursor, 3);
  // An Orca Run owns its coordinator session: rotateAfterWaves never resets or spawns a terminal.
  const sends = calls.filter(
    c => c.args[0] === 'orchestration' && c.args.includes('--body'),
  );
  assert.equal(sends.length, 2);
  assert.match(String(sends[1].args[sends[1].args.indexOf('--body') + 1]), /^You have 1 Dealer message/);
  assert.equal(calls.some(c => c.args[0] === 'terminal'), false);
  assert.equal(calls.some(c => c.args.includes('/new')), false);
  await registration.dispose();
});

test('Phase 4: single-machine zero-config self loopback routes without network mount', async t => {
  const root = directory(t);
  const solo = await node(t, root, 'solo', {
    skills: ['local-dev'],
    patch: { gateMode: 'approval_required' },
  });
  assert.deepEqual(solo.transport.listMountedNetworks(), []);
  assert.equal(solo.gate.getMode(), 'approval_required');
  assert.equal(solo.node.listPeers().length, 0);
  const member = await solo.emitter.emit({
    networkId: 'local',
    routing: { mode: 'unicast', targets: ['member:solo'] },
    kind: 'collab.request.v1',
    subject: 'Self member delivery',
    body: 'Run the local sweep',
    payload: { summary: 'local' },
  });
  assert.equal(member.receipts.length, 1);
  assert.equal(member.receipts[0].stage, 'dispatched');
  assert.equal(member.receipts[0].fromNodeId, solo.transport.localNodeId);
  const skill = await solo.emitter.emit({
    networkId: 'local',
    routing: { mode: 'auto', targets: ['skill:local-dev'] },
    kind: 'alert.event',
    subject: 'Local skill delivery',
    body: 'Handle the local alert',
    payload: { area: 'local-dev' },
  });
  assert.equal(skill.receipts[0].stage, 'dispatched');
  assert.equal(skill.receipts[0].fromNodeId, solo.transport.localNodeId);
  assert.equal(solo.gate.listPendingApprovals().length, 0);
  assert.equal(
    solo.node.resolveTarget('member:solo', 'local')[0].nodeId,
    solo.transport.localNodeId,
  );
  const inbox = readFileSync(join(solo.stateDir, 'notifications.jsonl'), 'utf8');
  assert.match(inbox, /Self member delivery/);
  assert.match(inbox, /Local skill delivery/);
});

test('Phase 4: wave streams resolve distinct workspace terminals with single-line pointers', async t => {
  const root = directory(t);
  const workspaceA = join(root, 'workspace-a');
  const workspaceB = join(root, 'workspace-b');
  mkdirSync(workspaceA, { recursive: true });
  mkdirSync(workspaceB, { recursive: true });
  const calls: CommandRequest[] = [];
  const createdByWorkspace = new Map<string, string[]>([
    [workspaceA, []],
    [workspaceB, []],
  ]);
  let createdCount = 0;
  const workspaceOf = (request: CommandRequest): string =>
    request.args.includes(`path:${workspaceA}`) ? workspaceA : workspaceB;
  const runner = async (request: CommandRequest) => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'ps') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            truncated: false,
            worktrees: [
              {
                path: request.cwd,
                worktreeId: `worktree-${request.cwd}`,
                childWorktreeIds: [],
                unverifiableTerminalCount: 0,
                agents: [{ state: 'done' }],
              },
            ],
          },
        }),
      };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'list') {
      const workspace = workspaceOf(request);
      const created = createdByWorkspace.get(workspace)!;
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            terminals: [
              {
                handle: workspace === workspaceA ? 'term-a' : 'term-b',
                connected: true,
                writable: true,
                agentIdentity: 'antigravity',
                lastOutputAt: 10,
                preview: 'Idle',
              },
              ...created.map((handle, index) => ({
                handle,
                connected: true,
                writable: true,
                agentIdentity: 'antigravity',
                lastOutputAt: 20 + index,
                preview: '',
              })),
            ],
          },
        }),
      };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'create') {
      createdCount += 1;
      const handle = `term-created-${createdCount}`;
      createdByWorkspace.get(workspaceOf(request))!.push(handle);
      return { stdout: JSON.stringify({ ok: true, result: { terminal: { handle } } }) };
    }
    return { stdout: '{"ok":true}' };
  };
  const worker = await node(t, root, 'worker', {
    profile: 'dedicated-worker',
    commandRunner: runner,
    patch: {
      defaultExecutor: 'orca-wave',
      executorConfig: { 'orca-wave': {} },
      wave: { flushIntervalMs: 100000 },
    },
  });
  const streamInput = (
    workspacePath: string,
    prompt: string,
    revision: number,
    workspaceComment?: string,
  ): EmitMessageInput => ({
    networkId: 'local',
    routing: { mode: 'unicast', targets: ['member:worker'] },
    kind: 'alert.event',
    subject: 'wave item',
    body: 'details',
    payload: {
      executor: 'orca-wave',
      streamKey: workspacePath,
      workspacePath,
      prompt,
      rotateAfterWaves: 1,
      revision,
      ...(workspaceComment ? { workspaceComment } : {}),
    },
  });
  await worker.emitter.emit(streamInput(workspaceA, 'A first\nline', 1));
  await worker.emitter.emit(streamInput(workspaceA, 'A second', 2));
  await worker.emitter.emit(streamInput(workspaceB, 'B first', 1, 'Wave B in progress'));

  const sends = calls.filter(c => c.args[0] === 'terminal' && c.args[1] === 'send');
  const textOf = (call: CommandRequest) => call.args[call.args.indexOf('--text') + 1];
  const handleOf = (call: CommandRequest) => call.args[call.args.indexOf('--terminal') + 1];
  // `rotateAfterWaves: 1` gives each workspace's first wave its own session tab; the second
  // message joins that same wave's tab instead of opening another one.
  const aSends = sends.filter(c => handleOf(c) === 'term-created-1');
  const bSends = sends.filter(c => handleOf(c) === 'term-created-2');
  assert.equal(aSends.length, 2);
  assert.ok(aSends.every(c => !/[\r\n]/.test(textOf(c))));
  assert.ok(aSends.some(c => /^You have 1 Dealer message\(s\) in stream:/.test(textOf(c))));
  assert.ok(aSends.some(c => /^You have 2 Dealer message\(s\) in stream:/.test(textOf(c))));
  assert.equal(bSends.length, 1);
  assert.match(textOf(bSends[0]), /^You have 1 Dealer message\(s\) in stream:/);
  assert.equal(
    calls.filter(c => c.args[0] === 'terminal' && c.args[1] === 'create').length,
    2,
    'one independent session tab per workspace wave',
  );
  assert.equal(sends.some(c => handleOf(c) === 'term-a' || handleOf(c) === 'term-b'), false);
  assert.equal(sends.some(c => textOf(c) === '/new'), false);
  assert.equal(sends.some(c => textOf(c) === '\x15'), false);
  assert.ok(
    calls.some(
      c =>
        c.args[0] === 'terminal' &&
        c.args[1] === 'list' &&
        c.args.includes(`path:${workspaceA}`),
    ),
  );
  assert.ok(
    calls.some(
      c =>
        c.args[0] === 'terminal' &&
        c.args[1] === 'list' &&
        c.args.includes(`path:${workspaceB}`),
    ),
  );
  assert.ok(calls.some(c => c.args[0] === 'worktree' && c.args[1] === 'ps' && c.cwd === workspaceA));
  assert.ok(calls.some(c => c.args[0] === 'worktree' && c.args[1] === 'ps' && c.cwd === workspaceB));
  assert.ok(
    calls.some(
      c =>
        c.args[0] === 'worktree' &&
        c.args[1] === 'set' &&
        c.args.includes('Wave B in progress'),
    ),
  );
});

test('Phase 4: CLI commands, gate states and invite URI round trip', async t => {
  const root = directory(t);
  const alice = await node(t, root, 'alice');
  const bob = await node(t, root, 'bob');
  await connect(alice, bob);
  assert.equal((await cli(alice, 'node', 'status')).running, true);
  assert.equal((await cli(alice, 'node', 'card', '--network', 'sensebeing')).identity.member, 'alice');
  const networks = await cli(alice, 'network', 'list');
  assert.equal(networks.length, 1);
  assert.equal(networks[0].passphrase, undefined);
  const invite = await cli(alice, 'network', 'invite', 'sensebeing', '--ttl', '1h');
  assert.equal(decodeNetworkInvite(invite.uri).passphrase, 'team-secret');
  assert.throws(() => decodeNetworkInvite('dealer-net://v1/team?passphrase=x&expires=1'), /expired/);
  const extra = await node(t, root, 'extra');
  await cli(extra, 'network', 'mount', invite.uri);
  await cli(extra, 'network', 'unmount', 'sensebeing');
  assert.equal((await cli(alice, 'peers', 'list', '--network', 'sensebeing', '--status', 'idle')).length >= 1, true);
  assert.equal(
    (await cli(alice, 'peers', 'resolve', 'member:bob', '--network', 'sensebeing'))[0].nodeId,
    bob.transport.localNodeId,
  );
  await cli(bob, 'gate', 'mode', 'approval_required', '--network', 'sensebeing');
  const held = await alice.emitter.emit(input('bob'));
  await cli(bob, 'gate', 'reject', held.messageId, '--reason', 'declined');
  assert.equal(bob.gate.listRejections().at(-1)?.reason, 'declined');
  const accepted = await alice.emitter.emit(input('bob'));
  await cli(bob, 'gate', 'approve', accepted.messageId);
  await cli(bob, 'gate', 'mode', 'dnd', '--network', 'sensebeing');
  assert.equal((await alice.emitter.emit(input('bob'))).receipts[0].stage, 'queued');
  await cli(bob, 'gate', 'drain');
  await cli(bob, 'gate', 'mode', 'closed', '--network', 'sensebeing');
  assert.equal((await alice.emitter.emit(input('bob'))).receipts[0].stage, 'rejected');
  await cli(bob, 'gate', 'mode', 'open', '--network', 'sensebeing');
  await cli(alice, 'broadcast', '--network', 'sensebeing', '--topic', 'announcement', '--body', 'hello team');
  assert.match(readFileSync(join(bob.stateDir, 'notifications.jsonl'), 'utf8'), /hello team/);
  const errors: string[] = [];
  assert.equal(await runDealerCli(['gate', 'mode', 'invalid'], { runtime: bob, stderr: line => errors.push(line) }), 1);
  assert.match(errors[0], /Invalid gate/);
  assert.equal(
    await runDealerCli(['send', '--network', 'sensebeing', '--topic', 'x', '--body', 'x'], {
      runtime: alice,
      stderr: () => {},
    }),
    1,
  );
  assert.equal(await runDealerCli(['--unknown'], { runtime: alice, stderr: () => {} }), 1);
});

test('Phase 4: identity/config persistence, exclusive state ownership and private control RPC', async t => {
  const root = directory(t);
  const runtime = await node(t, root, 'alice');
  const id = runtime.transport.localNodeId;
  await cli(runtime, 'network', 'mount', 'saved', '--passphrase', 'private', '--gate', 'dnd');
  await assert.rejects(new DealerNodeRuntime({ stateDir: runtime.stateDir }).start(), /already owned/);
  await runtime.serveControl(async args => {
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(args, {
      runtime,
      stdout: line => stdout.push(line),
      stderr: line => stderr.push(line),
    });
    return { code, stdout, stderr };
  });
  const output: string[] = [];
  assert.equal(
    await runDealerCli(['node', 'status', '--json'], {
      stateDir: runtime.stateDir,
      stdout: line => output.push(line),
    }),
    0,
  );
  assert.equal(JSON.parse(output[0]).nodeId, id);
  assert.equal(statSync(runtime.socketPath).mode & 0o777, 0o600);
  assert.equal(statSync(join(runtime.stateDir, 'identity.json')).mode & 0o777, 0o600);
  await runtime.stop();
  const restored = new DealerNodeRuntime({
    stateDir: runtime.stateDir,
    transport: { discoveryDir: join(root, 'mesh') },
  });
  await restored.start();
  t.after(() => restored.stop());
  assert.equal(restored.transport.localNodeId, id);
  assert.equal(restored.gate.getNetworkMode('saved'), 'dnd');
  assert.equal(restored.node.getSelfCard('saved').admissionMode, 'dnd');
  assert.equal(getBuiltInProfile('personal-dev').gateMode, 'open');
  assert.throws(() => getBuiltInProfile('nope'), /Unknown/);
});

test('Phase 4: actual dealer node up launches dealerd and CLI talks over socket', async t => {
  const root = directory(t);
  const stateDir = join(root, 'daemon');
  const run = (args: string[], cwd = process.cwd()) =>
    new Promise<{ code: number | null; output: string; errors: string }>((resolve, reject) => {
      const child = spawn(process.execPath, ['--experimental-strip-types', resolvePath('bin/dealer.js'), ...args], {
        cwd,
        env: { ...process.env, DEALER_STATE_DIR: stateDir },
      });
      let output = '';
      let errors = '';
      child.stdout.on('data', b => {
        output += b;
      });
      child.stderr.on('data', b => {
        errors += b;
      });
      child.once('error', reject);
      child.once('exit', code => resolve({ code, output, errors }));
    });
  const boot = await run(['node', 'up', '--profile', 'headless-relay', '--member', 'daemon-user', '--json']);
  assert.equal(boot.code, 0, boot.errors);
  assert.equal(JSON.parse(boot.output).running, true);
  const owner = JSON.parse(readFileSync(join(stateDir, 'runtime.lock', 'owner.json'), 'utf8'));
  t.after(async () => {
    process.kill(owner.pid, 'SIGTERM');
    await until(() => {
      try {
        process.kill(owner.pid, 0);
        return false;
      } catch {
        return true;
      }
    }, 'daemon shutdown');
  });
  const status = await run(['node', 'status', '--json']);
  assert.equal(status.code, 0, status.errors);
  assert.equal(JSON.parse(status.output).member, 'daemon-user');
  assert.equal((await run(['network', 'mount', 'daemon-test', '--passphrase', 'secret', '--json'])).code, 0);
  const list = await run(['network', 'list', '--json']);
  assert.equal(JSON.parse(list.output)[0].networkId, 'daemon-test');
  const setupProbe = await run(['setup', 'probe', '--root', root, '--json'], root);
  assert.equal(setupProbe.code, 0, setupProbe.errors);
  assert.equal(JSON.parse(setupProbe.output).cwd, root);
  assert.equal(JSON.parse(setupProbe.output).node.controlSocketPresent, true);
  const setupInvite = await run(['setup', 'invite-prompt', '--network', 'daemon-test', '--json']);
  assert.equal(setupInvite.code, 0, setupInvite.errors);
  assert.match(JSON.parse(setupInvite.output).teammatePrompt, /dealer network mount/);
  const setupBinding = await run(['setup', 'binding-preset', '--host', 'codex', '--mode', 'fixed', '--json']);
  assert.equal(setupBinding.code, 0, setupBinding.errors);
  assert.equal(JSON.parse(setupBinding.output).binding.executor, 'codex');
  const projectRoot = join(root, 'daemon-project');
  mkdirSync(projectRoot);
  const scaffold = await run(['project', 'init', '--id', 'daemon-project', '--network', 'daemon-test', '--json'], projectRoot);
  assert.equal(scaffold.code, 0, scaffold.errors);
  const manifest = JSON.parse(scaffold.output);
  manifest.plugins = [{ name: 'daemon-project-plugin', module: '.dealer/plugins/live.ts' }];
  writeFileSync(join(projectRoot, '.dealer/project.json'), JSON.stringify(manifest));
  writeFileSync(join(projectRoot, '.dealer/plugins/live.ts'), `
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
export default (config: any, context: any) => ({
  apply() {
    const marker = join(context.projectRoot, 'execution.txt');
    writeFileSync(marker, config.message);
    return () => writeFileSync(marker, 'unlinked');
  },
});
`);
  writeFileSync(join(projectRoot, 'overlay.json'), JSON.stringify({ pluginConfig: { 'daemon-project-plugin': { message: 'live' } } }));
  const link = await run(['project', 'link', '--user-patch', 'overlay.json', '--json'], projectRoot);
  assert.equal(link.code, 0, link.errors);
  assert.equal(readFileSync(join(projectRoot, 'execution.txt'), 'utf8'), 'live');
  const projectStatus = await run(['project', 'status', '--project', 'daemon-project', '--json']);
  assert.equal(realpathSync(JSON.parse(projectStatus.output).projects[0].rootPath), realpathSync(projectRoot));
  assert.equal((await run(['project', 'unlink', 'daemon-project', '--json'])).code, 0);
  assert.equal(readFileSync(join(projectRoot, 'execution.txt'), 'utf8'), 'unlinked');
});

test('Phase 4: declarative YAML patch and live route/source registrations', async t => {
  const root = directory(t);
  const patchPath = join(root, 'patch.yaml');
  writeFileSync(
    patchPath,
    'gateMode: dnd\nskills: ["ios", "runtime"]\nexecutorConfig:\n  notify:\n    inboxPath: "/tmp/example-unused"\n',
  );
  const runtime = await node(t, root, 'configured', {
    patch: { ...readPatch(patchPath), executorConfig: {} },
  });
  assert.throws(() => validateConfig({ ...runtime.config, executorConfig: { orcaSpawn: {} } }), /Unknown executor/);
  for (const invalid of [null, [], 'invalid', 42]) {
    assert.throws(() => validateConfig({ ...runtime.config, executorConfig: invalid as any }), /Invalid executorConfig/);
    assert.throws(() => validateConfig({ ...runtime.config, executorConfig: { 'orca-spawn': invalid as any } }), /Invalid executorConfig/);
  }
  assert.doesNotThrow(() => validateConfig({ ...runtime.config, executorConfig: {
    'orca-spawn': {}, 'orca-wave': {}, 'agent-pty': {}, notify: {},
  } }));
  assert.equal(runtime.gate.getMode(), 'dnd');
  assert.deepEqual(runtime.config.skills, ['ios', 'runtime']);
  await cli(runtime, 'route', 'add', '--type', 'local-agent', '--command', 'codex');
  await cli(runtime, 'route', 'add', '--type', 'llm-api', '--endpoint', 'http://127.0.0.1:1', '--model', 'test');
  await cli(runtime, 'source', 'add', '--type', 'webhook', '--port', '0', '--token', 'secret');
  await cli(runtime, 'source', 'add', '--type', 'poll-watermark', '--url', 'http://127.0.0.1:1', '--interval-ms', '100000');
  assert.equal(runtime.config.routes.length, 2);
  assert.equal(runtime.config.sources.length, 2);
  assert.equal(await runDealerCli(['source', 'add', '--type', 'invalid'], { runtime, stderr: () => {} }), 1);
});

test('Stage 1: project plugins, user overlays, restart and unlink', async t => {
  const root = directory(t);
  const runtime = await node(t, root, 'project-user');
  const projectRoot = join(root, 'project');
  const manifest = await cli(runtime, 'project', 'init', '--root', projectRoot, '--id', 'demo', '--network', 'team', '--skills', 'shared');
  assert.equal(manifest.schema, 'the-dealer-project/v1');
  const module = '.dealer/plugins/custom-project-plugin.ts';
  writeFileSync(join(projectRoot, module), `
export async function createPlugin(config: Record<string, unknown>, context: any) {
  const executor = context.api.createNotifyExecutorPlugin({});
  return {
    apply(ctx: any, applied: any) {
      ctx.on('emitter/prepare', (input: any, next: any) => next({
        ...input,
        body: String(applied.message),
        projectRoot: context.projectRoot,
        patchSkill: context.userPatch.skills.at(-1),
        factoryExecutor: executor.name,
      }));
    },
  };
}
`);
  manifest.plugins = [{ name: 'project-example', module, config: { message: 'base' } }];
  manifest.topics = { triage: { description: 'Triage requests', defaultExecutor: 'notify' } };
  writeFileSync(join(projectRoot, '.dealer/project.json'), JSON.stringify(manifest));
  const overlay = join(runtime.stateDir, 'projects', 'demo.patch.json');
  mkdirSync(join(runtime.stateDir, 'projects'), { recursive: true });
  writeFileSync(overlay, JSON.stringify({ skills: ['shared', 'personal'], pluginConfig: { 'project-example': { message: 'overlay' } } }));
  const linked = await cli(runtime, 'project', 'link', projectRoot);
  assert.deepEqual(linked.loadedPlugins, ['project-example']);
  assert.equal(linked.userPatchPath, overlay);
  assert.deepEqual(runtime.config.skills, ['shared', 'personal']);
  const prepared = await runtime.kernel.ctx.waterfall('emitter/prepare', { body: 'original' } as any);
  assert.equal(prepared.body, 'overlay');
  assert.equal(prepared.projectRoot, projectRoot);
  assert.equal(prepared.patchSkill, 'personal');
  assert.equal(prepared.factoryExecutor, 'notify');
  assert.equal((await cli(runtime, 'project', 'list')).length, 1);
  const status = await cli(runtime, 'project', 'status', '--project', 'demo');
  assert.equal(status.projects[0].manifest.defaultNetworkId, 'team');
  assert.equal(status.projects[0].manifest.topics.triage.defaultExecutor, 'notify');
  await cli(runtime, 'project', 'link', projectRoot);
  assert.equal(runtime.config.projects.length, 1);
  mkdirSync(join(runtime.stateDir, 'plugins'), { recursive: true });
  writeFileSync(join(runtime.stateDir, 'plugins/user.ts'), `
export default (_config: any, context: any) => {
  const factories = [
    'writeAtomicCursorFile', 'readJsonCursorFile', 'everyReceiptSettled',
    'parseYaml', 'parseMarkdownFrontmatter', 'parseTeamRegistry', 'resolveTeamRegistryMembers', 'createTeamRegistryRouterPlugin',
    'createWebhookSourcePlugin', 'createPollWatermarkSourcePlugin', 'createScriptSourcePlugin',
    'createDedupCasPolicyPlugin', 'isConfirmedHandoffReceipt', 'isDedupCasDuplicateReceipt', 'isDurableQueuedReceipt', 'createOrcaSpawnExecutorPlugin',
    'createOrcaWaveExecutorPlugin', 'createAgentPtyExecutorPlugin', 'createNotifyExecutorPlugin',
    'createLocalAgentRouterPlugin', 'createLlmApiRouterPlugin', 'createInboxExecutorPlugin',
  ];
  if (factories.some(name => typeof context.api[name] !== 'function')) {
    throw new Error('Missing built-in factory');
  }
  const policy = context.api.createDedupCasPolicyPlugin();
  return {
    name: 'user-example',
    apply(ctx: any) {
      ctx.on('emitter/prepare', async (input: any, next: any) => {
        const envelope = {
          id: 'api-contract',
          networkId: 'team',
          from: { nodeId: 'api-contract-sender' },
          payload: { claimVersion: 1 },
        };
        const dispatch = async () => ({ stage: 'dispatched' });
        const first = await policy.process(envelope, dispatch);
        const duplicate = await policy.process(envelope, dispatch);
        return next({
          ...input,
          userPlugin: true,
          factoryPolicy: policy.name,
          duplicateDetail: context.api.DEDUP_CAS_DUPLICATE_DETAIL,
          ambiguousDetail: context.api.DEDUP_CAS_AMBIGUOUS_DETAIL,
          duplicateReceiptRecognized: context.api.isDedupCasDuplicateReceipt(duplicate),
          dispatchedReceiptRecognized: context.api.isDedupCasDuplicateReceipt(first),
        });
      });
    },
  };
};
`);
  await cli(runtime, 'plugin', 'add', '--name', 'user-example', '--module', 'plugins/user.ts');
  await runtime.stop();
  const restored = new DealerNodeRuntime({ stateDir: runtime.stateDir, transport: { discoveryDir: join(root, 'mesh') } });
  await restored.start();
  t.after(() => restored.stop());
  const afterRestart = await restored.kernel.ctx.waterfall('emitter/prepare', { body: 'original' } as any);
  assert.equal(afterRestart.body, 'overlay');
  assert.equal(afterRestart.userPlugin, true);
  assert.equal(afterRestart.factoryPolicy, 'dedup-cas');
  assert.equal(afterRestart.duplicateDetail, DEDUP_CAS_DUPLICATE_DETAIL);
  assert.equal(afterRestart.ambiguousDetail, DEDUP_CAS_AMBIGUOUS_DETAIL);
  assert.equal(afterRestart.duplicateReceiptRecognized, false);
  assert.equal(afterRestart.dispatchedReceiptRecognized, false);
  assert.equal((await cli(restored, 'project', 'status')).userPlugins[0].name, 'user-example');
  await cli(restored, 'project', 'unlink', 'demo');
  assert.deepEqual((await cli(restored, 'project', 'list')), []);
  const afterUnlink = await restored.kernel.ctx.waterfall('emitter/prepare', { body: 'original' } as any);
  assert.equal(afterUnlink.body, 'original');
  assert.equal(afterUnlink.userPlugin, true);
  assert.deepEqual(JSON.parse(readFileSync(join(restored.stateDir, 'config.json'), 'utf8')).projects, []);
});


test('Stage 1: invalid manifests, collisions and failed activation preserve the live project', async t => {
  const root = directory(t);
  const runtime = await node(t, root, 'validation');
  const project = join(root, 'project');
  assert.throws(() => initProjectScaffold(project, { projectId: '../escape', defaultNetworkId: 'team' }), /Invalid project/);
  const manifest = initProjectScaffold(project, { projectId: 'valid', defaultNetworkId: 'team' });
  assert.throws(() => initProjectScaffold(project, { projectId: 'valid', defaultNetworkId: 'team' }), /EEXIST/);
  const manifestPath = join(project, '.dealer/project.json');
  writeFileSync(manifestPath, JSON.stringify({ ...manifest, plugins: [{ name: 'bad', module: 42 }] }));
  assert.throws(() => readProjectManifest(project), /Invalid plugin/);
  writeFileSync(join(project, '.dealer/plugins/good.ts'), `
export default () => ({
  name: 'good',
  apply(ctx: any) {
    ctx.on('emitter/prepare', (input: any, next: any) => next({ ...input, good: true }));
  },
});
`);
  const ref = { name: 'good', module: '.dealer/plugins/good.ts' };
  writeFileSync(manifestPath, JSON.stringify({ ...manifest, plugins: [ref] }));
  await runtime.linkProject(project);
  const before = readFileSync(join(runtime.stateDir, 'config.json'), 'utf8');
  writeFileSync(join(project, '.dealer/plugins/bad.ts'), `
export default {
  name: 'bad',
  apply() {
    throw new Error('activation failed');
  },
};
`);
  writeFileSync(manifestPath, JSON.stringify({ ...manifest, plugins: [ref, { name: 'bad', module: '.dealer/plugins/bad.ts' }] }));
  await assert.rejects(runtime.linkProject(project), /activation failed/);
  assert.equal(readFileSync(join(runtime.stateDir, 'config.json'), 'utf8'), before);
  assert.equal((await runtime.kernel.ctx.waterfall('emitter/prepare', {} as any)).good, true);
  assert.deepEqual(runtime.projectStatus().projects[0].loadedPlugins, ['good']);
  await assert.rejects(runtime.installUserPlugin({ name: 'different-ref', module: join(project, ref.module) }), /already owned/);
  writeFileSync(join(project, '.dealer/plugins/core.ts'), `
export default {
  name: 'transport',
  apply() {},
};
`);
  await assert.rejects(runtime.installUserPlugin({ name: 'core', module: join(project, '.dealer/plugins/core.ts') }), /already owned/);
  assert.ok(runtime.transport.localNodeId);
  await runtime.unlinkProject('valid');
  writeFileSync(manifestPath, JSON.stringify({ ...manifest, plugins: [ref] }));
  await runtime.linkProject(project);
  assert.equal((await runtime.kernel.ctx.waterfall('emitter/prepare', {} as any)).good, true);
});

test('Phase 4: runtime binds linked receiver projects into orca-spawn', async t => {
  const root = directory(t);
  const projectRoot = join(root, 'linked-project');
  mkdirSync(projectRoot);
  initProjectScaffold(projectRoot, { projectId: 'alter-service', defaultNetworkId: 'sensebeing' });
  const calls: CommandRequest[] = [];
  const alice = await node(t, root, 'alice');
  const bob = await node(t, root, 'bob', {
    profile: 'personal-dev',
    patch: {
      gateMode: 'open',
      projects: [{ projectId: 'alter-service', rootPath: projectRoot }],
      executorConfig: { 'orca-spawn': { cwd: '/unrelated/receiver' } },
    },
    commandRunner: async request => {
      calls.push(request);
      return { stdout: JSON.stringify({ ok: true, result: { path: join(root, 'spawned') } }) };
    },
  });
  await connect(alice, bob);
  const result = await alice.emitter.emit(input('bob', {
    projectId: 'alter-service', noParent: true, repoPath: '/sender/repo',
    cwd: '/sender/cwd', parentWorktree: 'path:/sender/parent',
  }));
  assert.equal(result.receipts[0].stage, 'dispatched');
  assert.equal(calls[0].cwd, projectRoot);
  assert.ok(calls[0].args.includes('--no-parent'));
});

test('Phase 4: DND durable receipt survives runtime restart and drains after user admission rules load', async t => {
  const root = directory(t);
  const alice = await node(t, root, 'dnd-alice');
  const bob = await node(t, root, 'dnd-bob', { patch: { gateMode: 'dnd', defaultExecutor: 'inbox' } });
  await connect(alice, bob);
  const allowed = await alice.emitter.emit({ ...input('dnd-bob'), subject: 'allowed' });
  const denied = await alice.emitter.emit({ ...input('dnd-bob'), subject: 'deny after restart' });
  assert.equal(allowed.receipts[0].stage, 'queued');
  assert.equal(allowed.receipts[0].executionReceipt?.durable, true);
  assert.equal(denied.receipts[0].stage, 'queued');
  assert.equal(bob.mailbox.list({ mailboxId: 'default' }).queuedCount, 0);
  mkdirSync(join(bob.stateDir, 'plugins'), { recursive: true });
  writeFileSync(join(bob.stateDir, 'plugins/admission.ts'), `
export default {
  name: 'restart-admission',
  apply(ctx: any) {
    ctx.services.gate.registerRule({
      name: 'restart-deny', priority: 100,
      async evaluate(envelope: any) {
        return envelope.subject === 'deny after restart'
          ? { status: 'reject', code: 'denied', reason: 'Current admission rule' }
          : null;
      },
    });
  },
};
`);
  await cli(bob, 'plugin', 'add', '--name', 'restart-admission', '--module', 'plugins/admission.ts');
  const stateDir = bob.stateDir;
  await bob.stop();
  const restarted = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'mesh') }, patch: { gateMode: 'open' } });
  await restarted.start();
  t.after(() => restarted.stop());
  assert.equal(restarted.mailbox.list({ mailboxId: 'default' }).queuedCount, 1);
  assert.equal(restarted.gate.listRejections()[0]?.reason, 'Current admission rule');
  assert.deepEqual(JSON.parse(readFileSync(join(stateDir, 'gate-dnd.json'), 'utf8')), { queued: {} });
  await restarted.gate.drainDnd();
  assert.equal(restarted.mailbox.list({ mailboxId: 'default' }).queuedCount, 1);
});

test('Phase 4: failed restored DND delivery stays ledger-fenced without preventing node startup', async t => {
  const root = directory(t);
  const alice = await node(t, root, 'fail-alice');
  const bob = await node(t, root, 'fail-bob', { patch: { gateMode: 'dnd', defaultExecutor: 'inbox' } });
  await connect(alice, bob);
  await alice.emitter.emit({ ...input('fail-bob'), subject: 'failure' });
  await alice.emitter.emit({ ...input('fail-bob'), subject: 'success' });
  const stateDir = bob.stateDir;
  await bob.stop();
  const calls: string[] = [];
  const options: DealerRuntimeOptions = {
    stateDir, transport: { discoveryDir: join(root, 'mesh') }, patch: { gateMode: 'open' },
    executorOverrides: [{ name: 'inbox', async execute(batch) {
      calls.push(batch[0].subject);
      if (batch[0].subject === 'failure') throw new Error('external outcome unknown');
      return { executor: 'inbox', executionId: batch[0].id };
    } }],
  };
  const restarted = new DealerNodeRuntime(options);
  await restarted.start();
  assert.match(restarted.lastError!, /DND drain failed/);
  assert.deepEqual(calls, ['failure', 'success']);
  assert.equal(Object.keys(JSON.parse(readFileSync(join(stateDir, 'gate-dnd.json'), 'utf8')).queued).length, 1);
  await restarted.stop();
  const again = new DealerNodeRuntime(options);
  await again.start();
  t.after(() => again.stop());
  assert.equal(again.lastError, undefined);
  assert.equal(again.gate.listDndQueue()[0].status, 'parked');
  assert.equal(again.gate.listDndQueue()[0].attempts, 1);
  assert.deepEqual(calls, ['failure', 'success'], 'unknown external effects are never replayed');
});

test('Phase 4: both DND crash windows park the signed obligation across restart and newer revision recovers', async t => {
  for (const crash of ['watermark', 'ledger'] as const) {
    const root = directory(t);
    const runtime = await node(t, root, crash, {
      patch: { gateMode: 'dnd', defaultExecutor: 'inbox', policies: ['dedup-cas'] },
    });
    await cli(runtime, 'network', 'mount', 'sensebeing', '--passphrase', 'team-secret');
    const keys = generateNodeKeyPair();
    const message = signEnvelope({
      id: 'crashed', traceId: 'crash', networkId: 'sensebeing',
      from: { nodeId: keys.nodeId, member: 'sender', nodeName: 'sender' },
      routing: { mode: 'unicast', targets: [runtime.node.getSelfCard('sensebeing').nodeId] },
      kind: 'task', subject: 'recover me', body: 'full obligation', attachments: [],
      priority: 'normal', createdAt: new Date().toISOString(), payload: { revision: 1 },
      idempotencyKey: 'crash-entity',
    }, keys.privateKeyPem);
    await runtime.gate.evaluate(message);
    const stateDir = runtime.stateDir;
    await runtime.stop();
    if (crash === 'watermark') {
      const key = JSON.stringify([message.networkId, keys.nodeId, message.idempotencyKey]);
      writeFileSync(join(stateDir, 'dedup-watermarks.json'), JSON.stringify({ [key]: { revision: 1, status: 'unknown' } }));
    } else {
      const key = JSON.stringify([message.networkId, keys.nodeId, message.id]);
      writeFileSync(join(stateDir, 'deliveries.json'), JSON.stringify({ [key]: { stage: 'dispatching' } }));
    }
    const restarted = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'mesh') }, patch: { gateMode: 'open' } });
    await restarted.start();
    t.after(() => restarted.stop());
    assert.match(restarted.lastError!, /DND drain failed/);
    assert.equal(restarted.mailbox.list().totalCount, 0);
    const [parked] = restarted.gate.listDndQueue();
    assert.deepEqual(parked.envelope, message);
    assert.equal(parked.status, 'parked');
    assert.equal(parked.attempts, 1);
    assert.deepEqual(JSON.parse(readFileSync(join(stateDir, 'gate-dnd.json'), 'utf8')).queued[parked.id].envelope, message);
    await restarted.gate.drainDnd();
    assert.equal(restarted.gate.listDndQueue()[0].attempts, 1);
    await restarted.gate.setMode('dnd');
    const newer = signEnvelope({ ...message, id: 'recovery', payload: { revision: 2 } }, keys.privateKeyPem);
    await restarted.gate.evaluate(newer);
    await restarted.gate.setMode('open');
    assert.equal(restarted.gate.listDndQueue().length, 0);
    assert.deepEqual(restarted.mailbox.list().items[0].signedEnvelope, newer);
    assert.deepEqual(restarted.mailbox.list().items[0].envelope.payload.localAttachments, []);
    await restarted.stop();
  }
});

test('Phase 4: legacy numeric watermark upgrade uses durable evidence, persists status and fences unknown execution', async t => {
  for (const evidence of ['mailbox', 'materialized-mailbox', 'materialized-tampered-mailbox', 'dispatched', 'settled', 'failed', 'dispatching', 'missing', 'queued-receipt', 'dnd', 'parked', 'tampered-mailbox'] as const) {
    const root = directory(t);
    const runtime = await node(t, root, evidence, {
      patch: { gateMode: 'dnd', defaultExecutor: 'inbox', policies: ['dedup-cas'] },
    });
    await cli(runtime, 'network', 'mount', 'sensebeing', '--passphrase', 'team-secret');
    const keys = generateNodeKeyPair();
    const message = signEnvelope({
      id: 'legacy', traceId: 'legacy', networkId: 'sensebeing',
      from: { nodeId: keys.nodeId, member: 'sender', nodeName: 'sender' },
      routing: { mode: 'unicast', targets: [runtime.node.getSelfCard('sensebeing').nodeId] },
      kind: 'task', subject: 'legacy', body: 'legacy obligation',
      attachments: evidence.startsWith('materialized')
        ? [{ name: 'claim.txt', mimeType: 'text/plain', sizeBytes: 4, blake3Hash: 'signed-hash', ticket: 'signed-ticket' }] : [],
      priority: 'normal', createdAt: new Date().toISOString(), payload: { claimVersion: 2 },
    }, keys.privateKeyPem);
    const key = JSON.stringify([message.networkId, keys.nodeId, message.id]);
    if (evidence === 'mailbox' || evidence === 'tampered-mailbox') {
      runtime.mailbox.enqueue([evidence === 'mailbox' ? message : { ...message, body: 'tampered' }]);
    }
    if (evidence.startsWith('materialized')) {
      runtime.mailbox.enqueue([{ ...message,
        payload: { ...message.payload, localAttachments: [join(root, 'claim.txt')] },
      }], { signedEnvelopes: { [message.id]: evidence === 'materialized-tampered-mailbox'
        ? { ...message, body: 'tampered' } : message } });
    }
    if (evidence === 'dnd' || evidence === 'parked') await runtime.gate.evaluate(message);
    const stateDir = runtime.stateDir;
    await runtime.stop();
    writeFileSync(join(stateDir, 'dedup-watermarks.json'), JSON.stringify({ [key]: 2 }));
    if (['dispatched', 'settled', 'failed', 'dispatching'].includes(evidence)) {
      writeFileSync(join(stateDir, 'deliveries.json'), JSON.stringify({ [key]: { stage: evidence } }));
    }
    if (evidence === 'queued-receipt') {
      writeFileSync(join(stateDir, 'deliveries.json'), JSON.stringify({ [key]: {
        stage: 'queued', executionReceipt: { executor: 'inbox', executionId: 'queued', durable: true },
      } }));
    }
    if (evidence === 'parked') {
      const path = join(stateDir, 'gate-dnd.json');
      const saved = JSON.parse(readFileSync(path, 'utf8'));
      saved.queued[key].status = 'parked';
      writeFileSync(path, JSON.stringify(saved));
    }
    const options = { stateDir, transport: { discoveryDir: join(root, 'mesh') } };
    const restarted = new DealerNodeRuntime(options);
    await restarted.start();
    const status = ['mailbox', 'materialized-mailbox', 'dispatched', 'settled'].includes(evidence) ? 'settled' : 'unknown';
    const path = join(stateDir, 'dedup-watermarks.json');
    assert.deepEqual(JSON.parse(readFileSync(path, 'utf8'))[key], { revision: 2, status }, evidence);
    await restarted.stop();
    const again = new DealerNodeRuntime(options);
    await again.start();
    t.after(() => again.stop());
    const retry = signEnvelope({ ...message, id: 'retry', idempotencyKey: message.id }, keys.privateKeyPem);
    const receipt = await again.kernel.ctx.services.dispatcher.dispatch(retry);
    assert.equal(receipt.executionReceipt?.handoff, status, evidence);
    assert.equal(isDedupCasDuplicateReceipt(receipt), status === 'settled', evidence);
    assert.equal(isDedupCasDuplicateReceipt(receipt, { allowQueued: true }), status !== 'unknown', evidence);
    if (status === 'unknown') {
      const newer = signEnvelope({ ...message, id: 'newer', idempotencyKey: message.id, attachments: [], payload: { claimVersion: 3 } }, keys.privateKeyPem);
      assert.equal((await again.kernel.ctx.services.dispatcher.dispatch(newer)).stage, 'dispatched', evidence);
      assert.equal(again.mailbox.hasConfirmedEnvelope(message.networkId, keys.nodeId, message.id, 3), true);
    }
    await again.stop();
  }
});


test('Phase 4: legacy numeric reservation cannot self-confirm its pending DND obligation after restart', async t => {
  const root = directory(t);
  const alice = await node(t, root, 'legacy-alice');
  const bob = await node(t, root, 'legacy-bob', {
    patch: { gateMode: 'dnd', defaultExecutor: 'inbox', policies: ['dedup-cas'] },
  });
  await connect(alice, bob);
  // A prior gate-dnd handoff must not hide the receiver's ambiguous recovery state.
  const request = { ...input('legacy-bob', { revision: 2 }), idempotencyKey: 'legacy-entity' };
  assert.equal((await alice.emitter.emit(request)).receipts[0].stage, 'queued');
  const message = bob.gate.listDndQueue()[0].envelope;
  const key = JSON.stringify([message.networkId, message.from.nodeId, message.idempotencyKey]);
  const stateDir = bob.stateDir;
  assert.equal(bob.mailbox.list().totalCount, 0);
  await bob.stop();
  writeFileSync(join(stateDir, 'dedup-watermarks.json'), JSON.stringify({ [key]: 2 }));
  const restarted = new DealerNodeRuntime({
    stateDir, transport: { discoveryDir: join(root, 'mesh') }, patch: { gateMode: 'open' },
  });
  await restarted.start();
  t.after(() => restarted.stop());
  assert.match(restarted.lastError!, /DND drain failed/);
  assert.equal(restarted.mailbox.list().totalCount, 0);
  const [parked] = restarted.gate.listDndQueue();
  assert.deepEqual(parked.envelope, message);
  assert.equal(parked.status, 'parked');
  assert.ok(parked.lastError);
  const { id, ...persisted } = parked;
  assert.deepEqual(JSON.parse(readFileSync(join(stateDir, 'gate-dnd.json'), 'utf8')).queued[id], persisted);
  assert.deepEqual(JSON.parse(readFileSync(join(stateDir, 'dedup-watermarks.json'), 'utf8'))[key], {
    revision: 2, status: 'unknown',
  });
  const retry = await alice.emitter.emit(request);
  assert.equal(isConfirmedHandoffReceipt(retry.receipts[0], { allowQueued: true }), false);
  assert.deepEqual(restarted.gate.listDndQueue(), [parked]);
  assert.equal(restarted.mailbox.list().totalCount, 0);
  await restarted.gate.setMode('dnd');
  for (const revision of [2, 1]) {
    const rejected = await alice.emitter.emit({ ...request, payload: { revision } });
    assert.equal(rejected.receipts[0].stage, 'rejected');
    assert.equal(isConfirmedHandoffReceipt(rejected.receipts[0], { allowQueued: true }), false);
    assert.deepEqual(restarted.gate.listDndQueue(), [parked]);
    assert.deepEqual(JSON.parse(readFileSync(join(stateDir, 'gate-dnd.json'), 'utf8')).queued[id], persisted);
  }
  const recovered = await alice.emitter.emit({ ...request, payload: { revision: 3 } });
  assert.equal(recovered.receipts[0].stage, 'queued');
  const [queued] = restarted.gate.listDndQueue();
  assert.equal(queued.status, 'queued');
  assert.equal(queued.attempts, 0);
  assert.equal(queued.lastError, undefined);
  await restarted.gate.setMode('open');
  assert.deepEqual(restarted.gate.listDndQueue(), []);
  assert.deepEqual(JSON.parse(readFileSync(join(stateDir, 'gate-dnd.json'), 'utf8')), { queued: {} });
  assert.equal(restarted.mailbox.list().totalCount, 1);
  assert.deepEqual(restarted.mailbox.list().items[0].signedEnvelope, queued.envelope);
  assert.deepEqual(restarted.mailbox.list().items[0].envelope.payload.localAttachments, []);
});

test('external project executors and policies share configuration, attachments and lifecycle with builtins', async t => {
  const root = directory(t);
  const project = join(root, 'project');
  mkdirSync(join(project, '.dealer', 'plugins'), { recursive: true });
  writeFileSync(join(project, '.dealer', 'project.json'), JSON.stringify({
    schema: 'the-dealer-project/v1', projectId: 'custom', defaultNetworkId: 'sensebeing', skills: [],
    plugins: [{ name: 'custom-plugin', module: '.dealer/plugins/custom-exec.ts' }],
  }));
  writeFileSync(join(project, '.dealer', 'plugins', 'custom-exec.ts'), `
    import { readFileSync, writeFileSync } from 'node:fs';
    import { join } from 'node:path';
    export function createPlugin(config, context) {
      return { name: 'custom-plugin', inject: ['dispatcher'], apply(ctx) {
        let policyCalls = 0;
        const policy = ctx.services.dispatcher.registerPolicy({ name: 'custom-policy',
          async process(envelope, next) { policyCalls++; return next([envelope]); }
        });
        const executor = ctx.services.dispatcher.registerExecutor({ name: 'custom-exec',
          async execute(batch, execution, options) {
            const envelope = batch[0];
            writeFileSync(join(context.stateDir, 'custom-result.json'), JSON.stringify({
              policyCalls, marker: options.marker,
              attachments: envelope.payload.localAttachments.map(path => readFileSync(path, 'utf8')),
              originalAttachment: envelope.attachments[0]?.name,
              localAttachments: envelope.payload.localAttachments,
            }));
            return { executor: 'custom-exec', executionId: envelope.id, mailboxId: 'custom-receipt' };
          }
        }, { marker: 'plugin-default' });
        ctx.effect(() => () => executor.dispose());
        ctx.effect(() => () => policy.dispose());
      } };
    }
  `);
  const alice = await node(t, root, 'custom-alice');
  const bob = await node(t, root, 'custom-bob', { patch: {
    projects: [{ projectId: 'custom', rootPath: project }],
    executors: ['custom-exec', 'inbox'], defaultExecutor: 'custom-exec', policies: ['custom-policy'],
    executorConfig: { 'custom-exec': { marker: 'receiver-config' } },
    topicBindings: { custom: { executor: 'custom-exec', mailboxId: 'configured-mailbox', metadata: { team: 'tools' }, assignee: 'alice' } },
  } });
  await connect(alice, bob);
  const auxiliaryModule = join(root, 'auxiliary.ts');
  writeFileSync(auxiliaryModule, `export default { name: 'auxiliary', apply() {} };`);
  await bob.installUserPlugin({ name: 'auxiliary', module: auxiliaryModule });
  const beforeUnlink = structuredClone(bob.config);
  const beforeStatus = bob.projectStatus();
  const savedConfig = readFileSync(join(bob.stateDir, 'config.json'), 'utf8');
  await assert.rejects(bob.unlinkProject('custom'), /Unknown executor: custom-exec/);
  assert.deepEqual(bob.config, beforeUnlink);
  assert.deepEqual(bob.projectStatus(), beforeStatus);
  assert.equal(readFileSync(join(bob.stateDir, 'config.json'), 'utf8'), savedConfig);
  assert.equal(bob.kernel.getService('dispatcher').hasExecutor('custom-exec'), true);
  assert.equal(bob.kernel.getService('dispatcher').hasPolicy('custom-policy'), true);
  const attachment = join(root, 'custom.txt');
  writeFileSync(attachment, 'external attachment contents');
  for (const topic of ['custom', 'default-fallback']) {
    const sent = await cli(alice, 'send', '--network', 'sensebeing', '--to', 'member:custom-bob', '--topic', topic, '--body', 'custom', '--attach', attachment);
    assert.equal(sent.receipts[0].stage, 'dispatched', JSON.stringify(sent.receipts));
    assert.deepEqual(JSON.parse(readFileSync(join(bob.stateDir, 'custom-result.json'), 'utf8')), {
      policyCalls: topic === 'custom' ? 1 : 2, marker: 'receiver-config', attachments: ['external attachment contents'], originalAttachment: 'custom.txt',
      localAttachments: [join(bob.stateDir, 'attachments', sent.messageId, '0-custom.txt')],
    });
    assert.equal(bob.mailbox.listThreadRecords(sent.messageId)[0].localMailbox, 'custom-receipt');
  }
  const forgedSecretPath = join(root, 'receiver-secret.txt');
  writeFileSync(forgedSecretPath, 'must never reach an executor');
  const forged = await alice.emitter.emit(input('custom-bob', { localAttachments: [forgedSecretPath] }));
  assert.equal(forged.receipts[0].stage, 'dispatched');
  const sanitized = JSON.parse(readFileSync(join(bob.stateDir, 'custom-result.json'), 'utf8'));
  assert.deepEqual(sanitized.localAttachments, []);
  assert.deepEqual(sanitized.attachments, []);
  assert.equal(JSON.stringify(sanitized).includes(forgedSecretPath), false);
  const inboxForged = await alice.emitter.emit(input('custom-bob', {
    executor: 'inbox', localAttachments: [forgedSecretPath],
  }));
  assert.equal(inboxForged.receipts[0].stage, 'dispatched');
  const persisted = bob.mailbox.listItemsAfterSequence(-1).find(item => item.id === inboxForged.messageId)!;
  assert.ok(persisted);
  assert.deepEqual(persisted.envelope.payload.localAttachments, []);
  const savedMailbox = JSON.parse(readFileSync(join(bob.stateDir, 'mailbox.json'), 'utf8'));
  assert.deepEqual(savedMailbox.items.find(item => item.id === inboxForged.messageId).envelope.payload.localAttachments, []);
  const dispatcher = bob.kernel.getService('dispatcher');
  assert.equal(dispatcher.hasExecutor('custom-exec'), true);
  assert.equal(dispatcher.hasPolicy('custom-policy'), true);
  await bob.stop();
  assert.equal(dispatcher.hasExecutor('custom-exec'), false);
  assert.equal(dispatcher.hasPolicy('custom-policy'), false);
});

test('external configuration defers names until plugin loading but rejects unregistered executors and policies', async t => {
  const root = directory(t);
  const override = await node(t, root, 'override', {
    patch: { executors: ['custom-override'], defaultExecutor: 'custom-override', executorConfig: { 'custom-override': {} } },
    executorOverrides: [{ name: 'custom-override', async execute(batch) {
      return { executor: 'custom-override', executionId: batch[0].id };
    } }],
  });
  assert.equal(override.kernel.getService('dispatcher').hasExecutor('custom-override'), true);
  const module = join(root, 'empty-plugin.ts');
  writeFileSync(module, `export default { name: 'empty-plugin', apply() {} };`);
  for (const [name, patch, error] of [
    ['executor', { executors: ['missing-exec', 'inbox'], defaultExecutor: 'missing-exec' }, /Unknown executor: missing-exec/],
    ['policy', { policies: ['missing-policy'] }, /Unknown policy: missing-policy/],
    ['identifier', { executors: ['bad name'], defaultExecutor: 'bad name' }, /Unknown executor/],
  ] as const) {
    const stateDir = join(root, name);
    const runtime = new DealerNodeRuntime({ stateDir, profile: 'headless-relay', patch: {
      userPlugins: [{ name: 'empty-plugin', module }], ...patch,
    } });
    await assert.rejects(runtime.start(), error);
    assert.equal(runtime.kernel.hasService('dispatcher'), false, 'failed start unwinds services');
    assert.equal(statSync(stateDir).isDirectory(), true);
  }
});


test('startup script dispatch waits for external executors and policies before mounting traffic', async t => {
  const root = directory(t);
  const plugin = join(root, 'startup-plugin.ts');
  writeFileSync(plugin, `
    import { appendFileSync } from 'node:fs';
    import { join } from 'node:path';
    export function createPlugin(config, context) {
      return { name: 'startup-plugin', inject: ['dispatcher'], apply(ctx) {
        const policy = ctx.services.dispatcher.registerPolicy({ name: 'custom-policy',
          async process(envelope, next) {
            appendFileSync(join(context.stateDir, 'startup-events.jsonl'), JSON.stringify(['policy', envelope.subject]) + '\\n');
            return next([envelope]);
          }
        });
        const executor = ctx.services.dispatcher.registerExecutor({ name: 'custom-exec',
          async execute(batch) {
            appendFileSync(join(context.stateDir, 'startup-events.jsonl'), JSON.stringify(['executor', batch[0].subject]) + '\\n');
            return { executor: 'custom-exec', executionId: batch[0].id };
          }
        }, {});
        ctx.effect(() => () => executor.dispose());
        ctx.effect(() => () => policy.dispose());
      } };
    }
  `);
  const runtime = new DealerNodeRuntime({ stateDir: join(root, 'state'), member: 'startup', profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
    patch: {
      networks: [{ networkId: 'sensebeing', passphrase: 'startup-secret' }],
      userPlugins: [{ name: 'startup-plugin', module: plugin }],
      executors: ['custom-exec', 'inbox'], defaultExecutor: 'custom-exec', policies: ['custom-policy'],
      sources: [{ type: 'script', config: { command: 'startup-source' } }],
    },
    commandRunner: async request => {
      assert.equal(request.command, 'startup-source');
      assert.equal(runtime.kernel.getService('dispatcher').hasExecutor('custom-exec'), true);
      assert.equal(runtime.kernel.getService('dispatcher').hasPolicy('custom-policy'), true);
      return { stdout: JSON.stringify({ ...input('startup'), subject: 'first startup message' }) + '\n' };
    },
  });
  t.after(() => runtime.stop());
  await runtime.start();
  const events = readFileSync(join(runtime.stateDir, 'startup-events.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.deepEqual(events, [['policy', 'first startup message'], ['executor', 'first startup message']]);
});


test('a sender-signed empty attachment array remains authentic across mailbox and runtime restart', async t => {
  const root = directory(t);
  const runtime = await node(t, root, 'signed-empty', {
    patch: { defaultExecutor: 'inbox', policies: ['dedup-cas'] },
  });
  const keys = generateNodeKeyPair();
  const message = signEnvelope({
    id: 'signed-empty', traceId: 'signed-empty', networkId: 'local',
    from: { nodeId: keys.nodeId, member: 'sender', nodeName: 'sender' },
    routing: { mode: 'unicast' }, kind: 'claim', subject: 'signed empty paths', body: 'original',
    payload: { claimVersion: 2, localAttachments: [] }, attachments: [],
    priority: 'normal', createdAt: new Date().toISOString(),
  }, keys.privateKeyPem);
  const receipt = await runtime.kernel.getService('dispatcher').dispatch(message);
  assert.equal(receipt.stage, 'dispatched');
  const item = runtime.mailbox.list().items[0];
  assert.deepEqual(item.signedEnvelope, message);
  assert.deepEqual(item.envelope.payload.localAttachments, []);
  assert.equal(runtime.mailbox.hasConfirmedEnvelope('local', keys.nodeId, message.id, 2), true);
  const stateDir = runtime.stateDir;
  await runtime.stop();
  const key = JSON.stringify(['local', keys.nodeId, message.id]);
  writeFileSync(join(stateDir, 'dedup-watermarks.json'), JSON.stringify({ [key]: 2 }));
  const restored = await new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'mesh') } }).start();
  t.after(() => restored.stop());
  assert.deepEqual(restored.mailbox.list().items[0].signedEnvelope, message);
  assert.equal(restored.mailbox.hasConfirmedEnvelope('local', keys.nodeId, message.id, 2), true);
  assert.deepEqual(JSON.parse(readFileSync(join(stateDir, 'dedup-watermarks.json'), 'utf8'))[key], { revision: 2, status: 'settled' });
});

test('external sources wait for later configured policies at startup and never run on failed readiness', async t => {
  for (const missingPolicy of [false, true]) {
    const root = directory(t);
    const module = join(root, 'ordered-plugins.ts');
    writeFileSync(module, `
      export const events = [];
      export function createPlugin(config) {
        if (config.source) return {
          name: 'early-source', inject: ['emitter'], apply(ctx) {
            const registration = ctx.services.emitter.registerSource({
              name: 'early-source',
              async start(emitter) {
                events.push('source');
                await emitter.emit({ networkId: 'local', routing: { mode: 'unicast', targets: ['member:guarded'] },
                  kind: 'early', subject: 'first', body: 'first' });
              },
              stop() { events.push('source-stop'); },
            });
            ctx.effect(() => () => registration.dispose());
          },
        };
        return { name: 'later-policy-executor', inject: ['dispatcher'], apply(ctx) {
          if (config.candidateSource) {
            const source = ctx.services.emitter.registerSource({ name: 'candidate-source',
              start() { events.push('candidate-source'); }, stop() { events.push('candidate-stop'); },
            });
            ctx.effect(() => () => source.dispose());
          }
          if (!config.missingPolicy) {
            const registration = ctx.services.dispatcher.registerPolicy({ name: 'custom-policy',
              async process(envelope, next) { events.push('policy'); return next([envelope]); },
            });
            ctx.effect(() => () => registration.dispose());
          }
          const registration = ctx.services.dispatcher.registerExecutor({ name: 'custom-exec',
            async execute(batch) { events.push('executor'); return { executor: 'custom-exec', executionId: batch[0].id }; },
          }, {});
          ctx.effect(() => () => registration.dispose());
        } };
      }
    `);
    const { events } = await import(pathToFileURL(module).href);
    const runtime = new DealerNodeRuntime({ stateDir: join(root, 'state'), member: 'guarded', profile: 'headless-relay',
      transport: { discoveryDir: join(root, 'mesh') },
      patch: { executors: ['custom-exec', 'inbox'], defaultExecutor: 'custom-exec', policies: ['custom-policy'],
        userPlugins: [
          { name: 'early-source', module, config: { source: true } },
          { name: 'later-policy-executor', module, config: { missingPolicy } },
        ],
      },
    });
    t.after(() => runtime.stop());
    if (missingPolicy) {
      await assert.rejects(runtime.start(), /Unknown policy: custom-policy/);
      assert.deepEqual(events, [], 'startup rejection must not start the early source or execute its message');
      assert.equal(runtime.kernel.hasService('dispatcher'), false);
    } else {
      await runtime.start();
      await until(() => events.includes('executor'));
      assert.deepEqual(events, ['source', 'policy', 'executor']);
      await assert.rejects(runtime.installUserPlugin({ name: 'later-policy-executor', module,
        config: { missingPolicy: true, candidateSource: true },
      }), /Unknown policy: custom-policy/);
      assert.deepEqual(events, ['source', 'policy', 'executor'], 'rollback disposes an unstarted source without starting it');
      await runtime.removeUserPlugin('early-source');
      await runtime.installUserPlugin({ name: 'early-source', module, config: { source: true } });
      await until(() => events.filter(event => event === 'executor').length === 2);
      assert.deepEqual(events, ['source', 'policy', 'executor', 'source-stop', 'source', 'policy', 'executor'],
        'live source installation activates its protected first emission');
      await runtime.stop();
    }
  }
});

test('dispatch fails before execution during policy replacement and rejected removal without poisoning the ledger', async t => {
  for (const operation of ['replace', 'remove']) {
    const root = directory(t);
    const module = join(root, 'policy-plugin.ts');
    writeFileSync(module, `
      export const events = [];
      let executionPause;
      export function pauseExecution() {
        let entered, release;
        const enteredPromise = new Promise(resolve => { entered = resolve; });
        const released = new Promise(resolve => { release = resolve; });
        executionPause = { entered, released };
        return { entered: enteredPromise, release };
      }
      let pause;
      export function pauseRemoval() {
        let entered, release;
        const enteredPromise = new Promise(resolve => { entered = resolve; });
        const released = new Promise(resolve => { release = resolve; });
        pause = { entered, released };
        return { entered: enteredPromise, release };
      }
      export function createPlugin(config) {
        return { name: 'policy-plugin', inject: ['dispatcher'], apply(ctx) {
          const registration = ctx.services.dispatcher.registerPolicy({ name: 'custom-policy',
            async process(envelope, next) {
              events.push(config.version);
              const blocked = executionPause;
              executionPause = undefined;
              if (blocked) { blocked.entered(); await blocked.released; }
              return next([envelope]);
            },
          });
          ctx.effect(() => async () => {
            registration.dispose();
            const blocked = pause;
            pause = undefined;
            if (blocked) { blocked.entered(); await blocked.released; }
          });
        } };
      }
    `);
    const fixture = await import(pathToFileURL(module).href);
    const runtime = await node(t, root, operation, {
      patch: { executors: ['custom-exec', 'inbox'], defaultExecutor: 'custom-exec', policies: ['custom-policy'],
        userPlugins: [{ name: 'policy-plugin', module, config: { version: 'v1' } }],
      },
      executorOverrides: [{ name: 'custom-exec', async execute(batch) {
        fixture.events.push('executor');
        return { executor: 'custom-exec', executionId: batch[0].id };
      } }],
    });
    const pause = fixture.pauseRemoval();
    t.after(() => pause.release());
    const transition = operation === 'replace'
      ? runtime.installUserPlugin({ name: 'policy-plugin', module, config: { version: 'v2' } })
      : runtime.removeUserPlugin('policy-plugin');
    const settledTransition = operation === 'remove'
      ? assert.rejects(transition, /Unknown policy: custom-policy/) : transition;
    await pause.entered;
    const dispatcher = runtime.kernel.getService('dispatcher');
    assert.equal(dispatcher.hasPolicy('custom-policy'), false);
    const keys = generateNodeKeyPair();
    const message = signEnvelope({ id: operation, traceId: operation, networkId: 'local',
      from: { nodeId: keys.nodeId, member: 'sender', nodeName: 'sender' }, routing: { mode: 'unicast' },
      kind: 'protected', subject: operation, body: operation, payload: {}, attachments: [],
      priority: 'normal', createdAt: new Date().toISOString(),
    }, keys.privateKeyPem);
    await assert.rejects(dispatcher.dispatch(message), error =>
      error instanceof Error && /Policy unavailable: custom-policy/.test(error.message) && error['beforeExternalEffect'] === true);
    assert.deepEqual(fixture.events, []);
    pause.release();
    await settledTransition;
    assert.equal((await dispatcher.dispatch(message)).stage, 'dispatched');
    assert.deepEqual(fixture.events, [operation === 'replace' ? 'v2' : 'v1', 'executor']);
    assert.equal(dispatcher.hasPolicy('custom-policy'), true);

    const retryable = signEnvelope({ ...message, id: 'required-policy-retry' }, keys.privateKeyPem);
    runtime.config.policies.push('missing-policy');
    await assert.rejects(dispatcher.dispatch(retryable), error =>
      error instanceof Error && /Policy unavailable: missing-policy/.test(error.message) && error['beforeExternalEffect'] === true);
    assert.equal(fixture.events.length, 2, 'no policy or executor runs before all required policies are ready');
    runtime.config.policies.pop();
    assert.equal((await dispatcher.dispatch(retryable)).stage, 'dispatched', 'readiness failure leaves no terminal ledger receipt');
    const duringPolicy = signEnvelope({ ...message, id: 'async-policy-removal' }, keys.privateKeyPem);
    const executionPause = fixture.pauseExecution();
    const running = dispatcher.dispatch(duringPolicy);
    const rejected = assert.rejects(running, error => error instanceof Error &&
      /Policy unavailable: custom-policy/.test(error.message) && error['beforeExternalEffect'] === true);
    await executionPause.entered;
    const removalPause = fixture.pauseRemoval();
    const removing = assert.rejects(runtime.removeUserPlugin('policy-plugin'), /Unknown policy/);
    await removalPause.entered;
    const executions = fixture.events.filter(event => event === 'executor').length;
    executionPause.release();
    await rejected;
    assert.equal(fixture.events.filter(event => event === 'executor').length, executions);
    removalPause.release();
    await removing;
    assert.equal((await dispatcher.dispatch(duringPolicy)).stage, 'dispatched');
    await runtime.stop();
  }
});

test('poll settlement completes during policy replacement or rejected removal without waiting on its own teardown', { timeout: 15000 }, async t => {
  for (const operation of ['replace', 'remove']) {
    const root = directory(t);
    const module = join(root, 'poll-policy.ts');
    writeFileSync(module, `
      import { createPollWatermarkSourcePlugin } from ${JSON.stringify(pathToFileURL(resolvePath('packages/plugins-source/index.ts')).href)};
      export const events = [];
      export let poll;
      let enter, release;
      export const entered = new Promise(resolve => { enter = resolve; });
      const resume = new Promise(resolve => { release = resolve; });
      export { release };
      export function createPlugin(config) {
        return { name: 'poll-policy', inject: ['emitter', 'dispatcher'], apply(ctx) {
          const policy = ctx.services.dispatcher.registerPolicy({ name: 'custom-policy',
            process(envelope, next) { events.push(config.version); return next([envelope]); },
          });
          const source = createPollWatermarkSourcePlugin({ initialCursor: 0,
            poll: async cursor => ({ items: [cursor], cursor: cursor + 1 }),
            toInput: () => ({ networkId: 'local', kind: 'poll', subject: 'poll', body: 'poll', routing: { mode: 'unicast', targets: ['member:poller'] } }),
            saveCursor: async () => {},
          });
          poll = () => source.pollOnce();
          const registration = ctx.services.emitter.registerSource({ name: 'poll',
            start(emitter) { return source.start({ ...emitter, async emit(input) { enter(); await resume; return emitter.emit(input); } }); },
            stop() { events.push('stop'); return source.stop(); },
          });
          ctx.effect(() => () => registration.dispose());
          ctx.effect(() => () => { policy.dispose(); events.push('policy-removed'); });
        } };
      }
    `);
    const fixture = await import(pathToFileURL(module).href);
    const runtime = await node(t, root, 'poller', { patch: {
      executors: ['custom-exec', 'inbox'], defaultExecutor: 'custom-exec', policies: ['custom-policy'],
      userPlugins: [{ name: 'poll-policy', module, config: { version: 'v1' } }],
    }, executorOverrides: [{ name: 'custom-exec', async execute(batch) {
      fixture.events.push('executor'); return { executor: 'custom-exec', executionId: batch[0].id };
    } }] });
    const polling = fixture.poll();
    const rejectedPoll = assert.rejects(polling);
    await fixture.entered;
    const changing = operation === 'replace'
      ? runtime.installUserPlugin({ name: 'poll-policy', module, config: { version: 'v2' } })
      : runtime.removeUserPlugin('poll-policy');
    const completed = operation === 'remove' ? assert.rejects(changing, /Unknown policy/) : changing;
    await until(() => fixture.events.includes('stop'));
    assert.equal(runtime.kernel.getService('dispatcher').hasPolicy('custom-policy'), false);
    fixture.release();
    await rejectedPoll;
    await completed;
    assert.equal(fixture.events.includes('executor'), false);
    await fixture.poll();
    assert.deepEqual(fixture.events.slice(-2), [operation === 'replace' ? 'v2' : 'v1', 'executor']);
    await runtime.stop();
  }
});

test('rollback restores a policy and its source while discarding candidate sources', { timeout: 15000 }, async t => {
  const root = directory(t);
  const module = join(root, 'combined-plugin.ts');
  writeFileSync(module, `
    export const events = [];
    export let emit;
    export function createPlugin(config) {
      return { name: 'combined', inject: ['dispatcher', 'emitter'], apply(ctx) {
        if (!config.broken) {
          const policy = ctx.services.dispatcher.registerPolicy({ name: 'custom-policy',
            process(envelope, next) { events.push(config.version + '-policy'); return next([envelope]); },
          });
          ctx.effect(() => () => policy.dispose());
        }
        const registration = ctx.services.emitter.registerSource({ name: 'combined-source',
          start(emitter) {
            events.push(config.version + '-start');
            emit = () => emitter.emit({ networkId: 'local', kind: 'restored', subject: 'restored', body: 'restored',
              routing: { mode: 'unicast', targets: ['member:restored'] } });
          },
          stop() { events.push(config.version + '-stop'); emit = undefined; },
        });
        ctx.effect(() => () => registration.dispose());
      } };
    }
  `);
  const fixture = await import(pathToFileURL(module).href);
  const runtime = await node(t, root, 'restored', { patch: {
    executors: ['custom-exec', 'inbox'], defaultExecutor: 'custom-exec', policies: ['custom-policy'],
    userPlugins: [{ name: 'combined', module, config: { version: 'v1' } }],
  }, executorOverrides: [{ name: 'custom-exec', async execute(batch) {
    fixture.events.push('executor'); return { executor: 'custom-exec', executionId: batch[0].id };
  } }] });
  for (const change of [
    () => runtime.installUserPlugin({ name: 'combined', module, config: { version: 'v2', broken: true } }),
    () => runtime.removeUserPlugin('combined'),
  ]) {
    await assert.rejects(change(), /Unknown policy: custom-policy/);
    assert.equal(runtime.kernel.getService('dispatcher').hasPolicy('custom-policy'), true);
    await fixture.emit();
    assert.deepEqual(fixture.events.slice(-3), ['v1-start', 'v1-policy', 'executor']);
    assert.equal(fixture.events.some(event => event.startsWith('v2')), false);
  }
  await runtime.stop();
});

test('long-lived source start promises do not block startup or unrelated source installation', { timeout: 10000 }, async t => {
  const root = directory(t);
  const module = join(root, 'lifetime-plugin.ts');
  writeFileSync(module, `
    export const events = [];
    export function createPlugin(config) {
      return { name: config.name, inject: ['emitter'], apply(ctx) {
        let stopped;
        const registration = ctx.services.emitter.registerSource({ name: config.name,
          start() { events.push(config.name + '-start'); return new Promise(resolve => { stopped = resolve; }); },
          stop() { events.push(config.name + '-stop'); stopped(); },
        });
        ctx.effect(() => () => registration.dispose());
      } };
    }
  `);
  const fixture = await import(pathToFileURL(module).href);
  const runtime = await node(t, root, 'lifetime', { patch: {
    userPlugins: [{ name: 'first', module, config: { name: 'first' } }],
  } });
  await runtime.installUserPlugin({ name: 'second', module, config: { name: 'second' } });
  const registration = await runtime.installSource({ type: 'webhook', config: { port: 0 } });
  assert.deepEqual(fixture.events, ['first-start', 'second-start']);
  await registration.dispose();
  await runtime.stop();
  assert.deepEqual(fixture.events, ['first-start', 'second-start', 'second-stop', 'first-stop']);
});

test('source add waits for held replacement activation and does not persist a failed startup', { timeout: 10000 }, async t => {
  const root = directory(t);
  const module = join(root, 'held-replacement.ts');
  writeFileSync(module, `
    let enter, release;
    export const entered = new Promise(resolve => { enter = resolve; });
    const resumed = new Promise(resolve => { release = resolve; });
    export { release };
    export function createPlugin(config) {
      return { name: 'held-replacement', apply(ctx) {
        if (config.pause) ctx.effect(() => async () => { enter(); await resumed; });
      } };
    }
  `);
  const fixture = await import(pathToFileURL(module).href);
  let sourceSignal: AbortSignal | undefined;
  const runtime = await node(t, root, 'held-add', {
    commandRunner: async request => {
      sourceSignal = request.signal;
      throw new Error('Source startup failed');
    },
    patch: { userPlugins: [{ name: 'held-replacement', module, config: { pause: true } }] },
  });
  t.after(() => fixture.release());
  const replacement = runtime.installUserPlugin({ name: 'held-replacement', module, config: {} });
  await fixture.entered;
  let settled = false;
  const errors: string[] = [];
  const adding = runDealerCli(['source', 'add', '--type', 'script', '--command', 'failing-source'], {
    runtime, stderr: line => errors.push(line), stdout: () => {},
  }).then(code => { settled = true; return code; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(settled, false);
  assert.equal(sourceSignal, undefined, 'held source must not execute');
  assert.deepEqual(runtime.config.sources, []);
  assert.deepEqual(JSON.parse(readFileSync(join(runtime.stateDir, 'config.json'), 'utf8')).sources, []);
  fixture.release();
  await replacement;
  assert.equal(await adding, 1);
  assert.match(errors.join('\n'), /Source startup failed/);
  assert.equal(sourceSignal?.aborted, true, 'failed registration is disposed');
  assert.deepEqual(runtime.config.sources, []);
  assert.deepEqual(JSON.parse(readFileSync(join(runtime.stateDir, 'config.json'), 'utf8')).sources, []);
  await runtime.stop();
});

test('held source cancellation rejects only its registration and never reports a source execution error', { timeout: 10000 }, async t => {
  for (const cancellation of ['release', 'dispose', 'stop']) {
    const root = directory(t);
    let runs = 0;
    const runtime = await node(t, root, `cancel-${cancellation}`, {
      commandRunner: async () => { runs++; return { stdout: '' }; },
    });
    const emitter = runtime.emitter;
    const hold = emitter.holdSources!();
    if (cancellation === 'dispose') {
      const registration = emitter.registerSource({ name: 'cancelled',
        start() { runs++; }, stop() { assert.fail('never-started source must not be stopped'); },
      });
      const rejected = assert.rejects(registration.started!, /Source cancelled before start/);
      await registration.dispose();
      await rejected;
      hold.release();
    } else {
      let settled = false;
      const errors: string[] = [];
      const adding = runDealerCli(['source', 'add', '--type', 'script', '--command', 'cancelled-source'], {
        runtime, stderr: line => errors.push(line), stdout: () => {},
      }).then(code => { settled = true; return code; });
      await new Promise(resolve => setImmediate(resolve));
      assert.equal(settled, false);
      if (cancellation === 'release') hold.release({ start: false });
      else await runtime.stop();
      assert.equal(await adding, 1);
      assert.match(errors.join('\n'), /Source cancelled before start/);
      hold.release();
    }
    assert.equal(runs, 0);
    assert.deepEqual(runtime.config.sources, []);
    assert.deepEqual(JSON.parse(readFileSync(join(runtime.stateDir, 'config.json'), 'utf8')).sources, []);
    assert.equal(runtime.lastError, undefined, 'pre-start cancellation is not an execution error');
    await runtime.stop();
  }
});
