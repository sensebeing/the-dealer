import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, realpathSync, writeFileSync, readFileSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve as resolvePath } from 'node:path';
import { spawn } from 'node:child_process';
import {
  DealerNodeRuntime,
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

test('Phase 4: polling, CAS dedup, durable busy waves and rotation after ack', async t => {
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
  assert.equal(duplicate.receipts[0].stage, 'rejected');
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
  const rotation = calls.findIndex(c => c.args.includes('/new'));
  assert.ok(rotation >= 0);
  const sends = calls.filter(
    c => c.args[0] === 'orchestration' && c.args.includes('--body'),
  );
  assert.equal(sends.length, 2);
  assert.ok(rotation < calls.indexOf(sends[1]));
  assert.match(String(sends[1].args[sends[1].args.indexOf('--body') + 1]), /^You have 1 Dealer message/);
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
  const calls: CommandRequest[] = [];
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
      const handle = request.args.includes(`path:${workspaceA}`) ? 'term-a' : 'term-b';
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            terminals: [
              {
                handle,
                connected: true,
                writable: true,
                agentIdentity: 'antigravity',
                lastOutputAt: 10,
                preview: 'Idle',
              },
            ],
          },
        }),
      };
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
  const aSends = sends.filter(c => handleOf(c) === 'term-a');
  const bSends = sends.filter(c => handleOf(c) === 'term-b');
  assert.equal(aSends.length, 2);
  assert.ok(aSends.every(c => !/[\r\n]/.test(textOf(c))));
  assert.ok(aSends.some(c => /^You have 1 Dealer message\(s\) in stream:/.test(textOf(c))));
  assert.ok(aSends.some(c => /^You have 2 Dealer message\(s\) in stream:/.test(textOf(c))));
  assert.equal(bSends.length, 1);
  assert.match(textOf(bSends[0]), /^You have 1 Dealer message\(s\) in stream:/);
  assert.equal(sends.some(c => textOf(c) === '/new'), false);
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
  assert.equal(getBuiltInProfile('personal-dev').gateMode, 'approval_required');
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
    'createWebhookSourcePlugin', 'createPollWatermarkSourcePlugin', 'createScriptSourcePlugin',
    'createDedupCasPolicyPlugin', 'isDedupCasDuplicateReceipt', 'createOrcaSpawnExecutorPlugin',
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
  assert.equal(afterRestart.duplicateReceiptRecognized, true);
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
