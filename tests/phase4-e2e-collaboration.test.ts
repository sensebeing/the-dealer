import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, readFileSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import {
  DealerNodeRuntime,
  runDealerCli,
  getBuiltInProfile,
  readPatch,
  decodeNetworkInvite,
  encodeNetworkInvite,
  createLocalAgentRouterPlugin,
  createPollWatermarkSourcePlugin,
  createOrcaWaveExecutorPlugin,
  blake3,
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

test('Phase 4: polling, CAS dedup, busy wave coalescing and session rotation', async t => {
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
    patch: {
      defaultExecutor: 'orca-wave',
      executorConfig: { 'orca-wave': { runId: 'test-run', terminalHandle: 'test-terminal' } },
      wave: { rotateAfterWaves: 1, flushIntervalMs: 100000 },
    },
    executorOverrides: [
      createOrcaWaveExecutorPlugin({
        runId: 'test-run',
        terminalHandle: 'test-terminal',
        idleCheck: async () => ({ idle }),
        commandRunner: runner,
      }),
    ],
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
  assert.equal(calls.length, 0);
  const duplicate = await alice.emitter.emit({
    ...input('bob', { executor: 'orca-wave', streamKey: 'alerts', revision: 1 }),
    idempotencyKey: 'alert-0',
  });
  assert.equal(duplicate.receipts[0].stage, 'rejected');
  const key = JSON.stringify(['sensebeing', 'alerts', 'orca-wave']);
  assert.equal(bob.wave!.pendingCount(key), 2);
  idle = true;
  await bob.flushWaves();
  assert.equal(bob.wave!.pendingCount(key), 0);
  assert.equal(calls.filter(c => c.args[0] === 'orchestration').length, 1);
  assert.match(calls[0].args.join(' '), /Dealer wave \(2\)/);
  await source.pollOnce();
  assert.equal(cursor, 3);
  const rotation = calls.findIndex(c => c.args.includes('/new'));
  assert.ok(rotation >= 0);
  assert.equal(calls.filter(c => c.args[0] === 'orchestration').length, 2);
  await registration.dispose();
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
  const run = (args: string[]) =>
    new Promise<{ code: number | null; output: string; errors: string }>((resolve, reject) => {
      const child = spawn(process.execPath, ['--experimental-strip-types', 'bin/dealer.js', ...args], {
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
