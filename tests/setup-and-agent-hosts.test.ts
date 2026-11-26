import { toMailboxTopicBinding } from '../packages/cli/config.ts';
import { readAgentHostDriverConfig } from '../packages/plugins-agent-host/index.ts';
import { createTestWakeupDrivers } from './helpers/wakeup-drivers.ts';
import { createFakeCodexAppServer } from './helpers/fake-codex-app-server.ts';
import { planOrcaWorkspaceTerminal } from '../packages/plugins-orca/workspace.ts';
import { createDedupCasPolicyPlugin, DEDUP_CAS_DUPLICATE_DETAIL } from '../packages/plugins-policy/index.ts';
import { runCommand, CommandNotStartedError } from '../packages/subprocess/index.ts';
import {
  CODEX_AGENT_PROFILE_ERROR,
  DEFAULT_RPC_TIMEOUT_MS,
  buildAgentHostInvocation,
  buildCodexThreadName,
  deliverAgentHostWakeTarget,
  isAgentHostRotationDue,
  resolveAgentHostContinuation,
  resolveAgentHostSessionLink,
  resolveAgentSessionId,
  runCodexAppServerTurn,
} from '../packages/plugins-agent-host/index.ts';
import { WakeupRedriveService, createInboxExecutorPlugin } from '../packages/index.ts';
import test from 'node:test';
import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import assert from 'node:assert/strict';
import { cpSync, mkdtempSync, mkdirSync, renameSync, rmSync, writeFileSync, existsSync, readFileSync, chmodSync } from 'node:fs';
import { tmpdir, homedir, networkInterfaces } from 'node:os';
import type { NetworkInterfaceInfo } from 'node:os';
import { createSocket } from 'node:dgram';
import { createServer } from 'node:net';
import { P2PTransportService, defaultAdvertisedHost, isTailscaleIpv4Host } from '../packages/transport/index.ts';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  AGENT_HOST_CATALOG, buildAgentBindingPreset, probeDealerSetup, runDealerCli, stopDaemonIfRunning,
  decodeNetworkInvite, DealerNodeRuntime, MailboxStore, createAgentPtyExecutorPlugin,
  createOrcaSpawnExecutorPlugin, createOrcaWaveExecutorPlugin,
  createOrcaExecutorPlugin, createCodexCliExecutorPlugin, createAntigravityCliExecutorPlugin,
  createClaudeAppExecutorPlugin, createChatgptAppExecutorPlugin, createAntigravityAppExecutorPlugin,
  createCodexExecutorPlugin, createClaudeCodeExecutorPlugin, createAntigravityExecutorPlugin,
  BUILTIN_EXECUTORS, getBuiltInProfile, readProjectManifest, buildOnboardingPrompts, resolveAgentBindingPreset,
  defaultAgentLaunchCommand, resolveOrSpawnOrcaWorkspaceTerminal, validateTopicBindings,
  Kernel, createTransportPlugin, createNodePlugin, generateNodeKeyPair, deriveNetworkKeys,
  sealNetworkFrame, unsealNetworkFrame, signEnvelope, signNodeCard,
} from '../packages/index.ts';
import type { WakeTargetState, CommandRequest, CommandRunner, MessageEnvelope, StdioProcessSpawner, SupportedOrcaAgent } from '../packages/index.ts';

const context = { networkId: 'test', nodeId: `ed25519:${'a'.repeat(64)}`, traceId: 'test' };
function message(id = 'test', payload = {}): MessageEnvelope {
  return { id, networkId: 'test', traceId: id, from: { nodeId: context.nodeId, member: 'alice', nodeName: 'laptop' },
    routing: { mode: 'unicast' }, kind: 'collab.request.v4', priority: 'normal', subject: 'Task', body: 'Perform task\nthen report',
    payload, attachments: [], createdAt: new Date().toISOString(), signature: '' };
}
function directory(t: { after: (fn: () => void) => void }) {
  const dir = mkdtempSync(join(tmpdir(), 'dealer-hosts-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}
async function eventually(check: () => void | Promise<void>, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      await check();
      return;
    } catch (error) {
      if (Date.now() > deadline) throw error;
    }
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}
async function freeUdpPorts(count: number): Promise<number[]> {
  const sockets: ReturnType<typeof createSocket>[] = [];
  try {
    for (let index = 0; index < count; index++) {
      const socket = createSocket('udp4');
      socket.bind(0, '127.0.0.1');
      await once(socket, 'listening');
      sockets.push(socket);
    }
    return sockets.map(socket => (socket.address() as { port: number }).port);
  } finally {
    for (const socket of sockets) socket.close();
  }
}
async function freeTcpPort(t: { after: (fn: () => void) => void }): Promise<number> {
  const server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => server.close());
  return (server.address() as { port: number }).port;
}
async function cli(args: string[], stateDir: string) {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const code = await runDealerCli([...args, '--json'], { stateDir, stdout: line => stdout.push(line), stderr: line => stderr.push(line) });
  assert.equal(code, 0, stderr.join('\n'));
  return JSON.parse(stdout.join(''));
}

test('setup probe is read-only, detects CLI/project and redacts network secrets', async t => {
  const root = directory(t);
  mkdirSync(join(root, '.dealer'));
  writeFileSync(join(root, '.dealer/project.json'), JSON.stringify({ projectId: 'demo' }));
  writeFileSync(join(root, 'config.json'), JSON.stringify({ member: 'alice', networks: [{ networkId: 'team', passphrase: 'secret' }] }));
  const result = probeDealerSetup({ stateDir: root, cwd: root, env: { PATH: '/fake' }, pathLookup: command => command === 'orca' ? '/fake/orca' : undefined });
  assert.equal(result.recommendedHost, 'orca');
  assert.equal(result.project.projectId, 'demo');
  assert.equal(result.clis.codex, null);
  assert.ok(!JSON.stringify(result).includes('secret'));
  assert.equal((await cli(['setup', 'probe', '--root', root], root)).cwd, root);
  assert.equal(existsSync(join(root, 'private-key.pem')), false);
});

test('project onboarding validates fields and probe normalizes defaults without exposing Lark targets', t => {
  const root = directory(t);
  mkdirSync(join(root, '.dealer'));
  const path = join(root, '.dealer/project.json');
  const manifest = { schema: 'the-dealer-project/v1', projectId: 'demo', defaultNetworkId: 'team' };
  const probe = () => probeDealerSetup({ cwd: root, stateDir: root, env: { PATH: '' } });
  const onboarding = { networkTopology: 'fixed', networkId: 'invited', requireLarkCli: true,
    notifyLark: true, defaultTopic: 'review', defaultConversationMode: 'spawn', teamRegistryPath: 'team.json' };
  writeFileSync(path, JSON.stringify({ ...manifest, onboarding }));
  assert.deepEqual(readProjectManifest(root).onboarding, onboarding);
  writeFileSync(join(root, 'config.json'), JSON.stringify({ larkNotify: { enabled: true, command: '/custom/lark-cli', userId: 'private-target' } }));
  assert.deepEqual(probe().onboarding, { ...onboarding, hasNetworkPassphrase: false });
  const secretPreset = { ...onboarding, passphrase: 'onboarding-secret', bootstrapPeers: ['127.0.0.1:45678'] };
  writeFileSync(path, JSON.stringify({ ...manifest, onboarding: secretPreset }));
  assert.deepEqual(readProjectManifest(root).onboarding, secretPreset);
  assert.deepEqual(probe().onboarding, { ...onboarding, hasNetworkPassphrase: true, bootstrapPeers: secretPreset.bootstrapPeers });
  assert.equal(Object.hasOwn(probe().project.onboarding, 'passphrase'), false);
  assert.ok(!JSON.stringify(probe()).includes(secretPreset.passphrase));
  assert.deepEqual(probe().node.larkNotify, { enabled: true, command: '/custom/lark-cli' });
  assert.ok(!JSON.stringify(probe()).includes('private-target'));
  for (const invalid of [null, [], 'fixed', { networkTopology: 'other' }, { requireLarkCli: 'true' },
    { passphrase: '' }, { passphrase: ' ' }, { passphrase: 1 }, { bootstrapPeers: 'seed' },
    { bootstrapPeers: [''] }, { bootstrapPeers: [1] }, { notifyLark: 1 }, { networkId: '' }, { defaultTopic: ' ' }, { teamRegistryPath: '' }, { defaultConversationMode: 'other' }]) {
    writeFileSync(path, JSON.stringify({ ...manifest, onboarding: invalid }));
    assert.throws(() => readProjectManifest(root), /onboarding/);
  }
  for (const defaultNetworkId of ['team', 'local']) {
    writeFileSync(path, JSON.stringify({ ...manifest, defaultNetworkId }));
    assert.deepEqual(probe().onboarding, { networkTopology: defaultNetworkId === 'local' ? 'selectable' : 'fixed',
      networkId: defaultNetworkId, hasNetworkPassphrase: false, requireLarkCli: false, notifyLark: false,
      defaultTopic: 'collab.request.v4', defaultConversationMode: 'fixed' });
  }
  rmSync(path);
  rmSync(join(root, 'config.json'));
  assert.equal(probe().onboarding, null);
  assert.equal(probe().node.larkNotify, null);
});

test('runtime start reclaims a lock left by a dead owner but refuses a live owner', async t => {
  const root = directory(t);
  const stateDir = join(root, 'state');
  const lockDir = join(stateDir, 'runtime.lock');
  mkdirSync(lockDir, { recursive: true, mode: 0o700 });
  writeFileSync(join(lockDir, 'owner.json'), JSON.stringify({ pid: 999999 }));
  const runtime = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await runtime.start();
  const owner = JSON.parse(readFileSync(join(lockDir, 'owner.json'), 'utf8'));
  assert.deepEqual(Object.keys(owner).sort(), ['host', 'instanceId', 'pid']);
  assert.equal(owner.pid, process.pid);
  assert.equal(existsSync(`${lockDir}.reclaim`), false, 'the reclaim gate is released');
  await runtime.stop();
  assert.equal(existsSync(lockDir), false, 'a dead owner lock is reclaimed and released');
  // A live foreign process keeps its lock; a same-PID file with no active in-process
  // owner is a previous incarnation (container PID reuse) and is covered separately.
  mkdirSync(lockDir, { recursive: true, mode: 0o700 });
  writeFileSync(join(lockDir, 'owner.json'), JSON.stringify({ pid: process.ppid }));
  const blocked = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await assert.rejects(blocked.start(), /State directory already owned/);
  assert.equal(existsSync(join(lockDir, 'owner.json')), true, 'a live owner keeps its lock');
});

test('runtime stop leaves a lock that no longer records this process', async t => {
  const root = directory(t);
  const stateDir = join(root, 'state');
  const lockDir = join(stateDir, 'runtime.lock');
  const runtime = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await runtime.start();
  writeFileSync(join(lockDir, 'owner.json'), JSON.stringify({ pid: process.ppid }));
  await runtime.stop();
  assert.equal(existsSync(join(lockDir, 'owner.json')), true, 'a foreign owner lock survives stop');
});

test('runtime start recovers a reclaim gate left by a dead reclaimer', async t => {
  const root = directory(t);
  const stateDir = join(root, 'state');
  const lockDir = join(stateDir, 'runtime.lock');
  const reclaimDir = `${lockDir}.reclaim`;
  const liveOwnerFile = `owner.${process.ppid}.json`;
  const seedOwner = (dir: string, ownerFile: string, pid: number) => {
    mkdirSync(dir, { recursive: true, mode: 0o700 });
    writeFileSync(join(dir, ownerFile), JSON.stringify({ pid }));
  };
  // A reclaimer killed between claiming the gate and releasing it leaves the dead
  // lock and its gate behind; the next start must recover both.
  seedOwner(lockDir, 'owner.json', 999999);
  seedOwner(reclaimDir, 'owner.json', 999999);
  seedOwner(reclaimDir, 'owner.999998.json', 999998);
  const runtime = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await runtime.start();
  const owner = JSON.parse(readFileSync(join(lockDir, 'owner.json'), 'utf8'));
  assert.deepEqual(Object.keys(owner).sort(), ['host', 'instanceId', 'pid']);
  assert.equal(owner.pid, process.pid);
  assert.equal(existsSync(reclaimDir), false, 'the dead reclaim gate is reclaimed');
  await runtime.stop();
  assert.equal(existsSync(lockDir), false, 'the reclaimed lock is released');

  // A gate that also records a live owner keeps every entry: a contender must never
  // unlink a file it cannot prove dead, nor the dead files next to it.
  seedOwner(lockDir, 'owner.json', 999999);
  seedOwner(reclaimDir, 'owner.999999.json', 999999);
  seedOwner(reclaimDir, liveOwnerFile, process.ppid);
  const blocked = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await assert.rejects(blocked.start(), /State directory already owned/);
  assert.equal(existsSync(join(reclaimDir, liveOwnerFile)), true, 'a live reclaim gate survives');
  assert.equal(existsSync(join(reclaimDir, 'owner.999999.json')), true, 'a refused contender unlinks nothing');
  assert.equal(existsSync(join(lockDir, 'owner.json')), true, 'a blocked start leaves the lock untouched');
  rmSync(lockDir, { recursive: true, force: true });
  rmSync(reclaimDir, { recursive: true, force: true });

  // A dead gate left with no lock is cleaned on the fast path instead of blocking
  // every later reclamation.
  seedOwner(reclaimDir, 'owner.999999.json', 999999);
  const fast = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await fast.start();
  assert.equal(existsSync(reclaimDir), false, 'a dead reclaim gate is removed on the fast path');
  await fast.stop();
  assert.equal(existsSync(lockDir), false, 'the fast-path lock is released');
});

test('project link mounts its onboarding network once and preserves an existing mount', async t => {
  const root = directory(t);
  mkdirSync(join(root, '.dealer'));
  const path = join(root, '.dealer/project.json');
  const manifest = { schema: 'the-dealer-project/v1', projectId: 'demo', defaultNetworkId: 'default-team',
    onboarding: { networkId: 'fixed-team', passphrase: 'project-secret', bootstrapPeers: ['127.0.0.1:45678'] } };
  writeFileSync(path, JSON.stringify(manifest));
  const runtime = new DealerNodeRuntime({ stateDir: join(root, 'state'), transport: { discoveryDir: join(root, 'discovery') } });
  await runtime.start();
  t.after(() => runtime.stop());
  await runtime.linkProject(root);
  assert.ok(runtime.transport.listMountedNetworks().includes('fixed-team'));
  assert.ok(!runtime.transport.listMountedNetworks().includes('default-team'));
  assert.deepEqual(runtime.config.networks.find(n => n.networkId === 'fixed-team'), {
    networkId: 'fixed-team', passphrase: 'project-secret', bootstrapPeers: ['127.0.0.1:45678'],
  });
  await runtime.linkProject(root);
  assert.equal(runtime.config.networks.filter(n => n.networkId === 'fixed-team').length, 1);
  writeFileSync(path, JSON.stringify({ ...manifest, onboarding: { ...manifest.onboarding, passphrase: 'changed-secret' } }));
  await runtime.linkProject(root);
  assert.equal(runtime.config.networks.find(n => n.networkId === 'fixed-team')?.passphrase, 'project-secret');
  writeFileSync(path, JSON.stringify({ ...manifest, onboarding: { passphrase: 'default-secret' } }));
  await runtime.linkProject(root);
  assert.equal(runtime.config.networks.find(n => n.networkId === 'default-team')?.passphrase, 'default-secret');
  for (const action of ['list', 'status']) {
    const stdout: string[] = [];
    assert.equal(await runDealerCli(['project', action, '--json'], { runtime, stdout: line => stdout.push(line) }), 0);
    const output = JSON.parse(stdout.join(''));
    const projects = action === 'list' ? output : output.projects;
    assert.equal(Object.hasOwn(projects[0].manifest.onboarding, 'passphrase'), false);
    assert.ok(!stdout.join('').includes('default-secret'));
  }
  await runtime.mountNetwork({ networkId: 'fixed-team', passphrase: 'project-secret', admissionMode: 'closed', role: 'worker' });
  assert.equal(runtime.gate.getNetworkMode('fixed-team'), 'closed');
  assert.equal(runtime.node.getSelfCard('fixed-team').admissionMode, 'closed');
  await runtime.setGateMode('dnd', 'fixed-team');
  const replacement = { networkId: 'fixed-team', passphrase: 'override-secret', bootstrapPeers: ['127.0.0.1:45679'],
    admissionMode: 'dnd', role: 'worker' };
  await runtime.mountNetwork({ networkId: replacement.networkId, passphrase: replacement.passphrase, bootstrapPeers: replacement.bootstrapPeers });
  assert.equal(runtime.gate.getNetworkMode('fixed-team'), 'dnd');
  assert.equal(runtime.node.getSelfCard('fixed-team').admissionMode, 'dnd');
  assert.deepEqual(JSON.parse(readFileSync(join(runtime.stateDir, 'config.json'), 'utf8')).networks.find(n => n.networkId === 'fixed-team'), replacement);
  assert.deepEqual(runtime.transport.getNetworkConfig('fixed-team'), replacement);
  assert.deepEqual(runtime.config.networks.find(n => n.networkId === 'fixed-team'), replacement);
  const mount = runtime.transport.mountNetwork.bind(runtime.transport);
  const stub = t.mock.method(runtime.transport, 'mountNetwork', async network => {
    if (network.passphrase === 'rejected-secret') throw new Error('mount rejected');
    return mount(network);
  });
  await assert.rejects(runtime.mountNetwork({ networkId: 'fixed-team', passphrase: 'rejected-secret' }), /mount rejected/);
  stub.mock.restore();
  assert.deepEqual(runtime.transport.getNetworkConfig('fixed-team'), replacement);
  assert.deepEqual(runtime.config.networks.find(n => n.networkId === 'fixed-team'), replacement);
  assert.equal(await runDealerCli(['network', 'unmount', 'default-team'], { runtime, stdout: () => {} }), 0);
  await runtime.stop();
  const restarted = new DealerNodeRuntime({ stateDir: runtime.stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await restarted.start();
  t.after(() => restarted.stop());
  assert.equal(restarted.projectStatus().projects.length, 1);
  assert.equal(restarted.transport.getNetworkConfig('default-team'), undefined);
  assert.equal(restarted.config.networks.some(n => n.networkId === 'default-team'), false);
  assert.deepEqual(restarted.transport.getNetworkConfig('fixed-team'), replacement);
  assert.deepEqual(restarted.config.networks.find(n => n.networkId === 'fixed-team'), replacement);
  assert.equal(restarted.gate.getNetworkMode('fixed-team'), 'dnd');
  assert.equal(restarted.node.getSelfCard('fixed-team').admissionMode, 'dnd');
  await restarted.linkProject(root);
  assert.equal(restarted.transport.getNetworkConfig('default-team')?.passphrase, 'default-secret');
});

test('network mount resolves project credentials before daemon forwarding and respects explicit flags', async t => {
  const root = directory(t);
  mkdirSync(join(root, '.dealer'));
  writeFileSync(join(root, '.dealer/project.json'), JSON.stringify({ schema: 'the-dealer-project/v1',
    projectId: 'demo', defaultNetworkId: 'fixed-team',
    onboarding: { passphrase: 'project-secret', bootstrapPeers: ['127.0.0.1:45678'] } }));
  const stateDir = join(root, 'state');
  const runtime = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await runtime.start();
  t.after(() => runtime.stop());
  const forwarded: string[][] = [];
  await runtime.serveControl(async argv => {
    forwarded.push(argv);
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(argv, { runtime, stdout: line => stdout.push(line), stderr: line => stderr.push(line) });
    return { code, stdout, stderr };
  });
  assert.deepEqual(await cli(['network', 'mount', 'fixed-team', '--root', root], stateDir), { mounted: 'fixed-team' });
  assert.ok(forwarded[0].includes('project-secret'));
  assert.ok(forwarded[0].includes('127.0.0.1:45678'));
  assert.deepEqual(runtime.config.networks.find(n => n.networkId === 'fixed-team'), {
    networkId: 'fixed-team', passphrase: 'project-secret', bootstrapPeers: ['127.0.0.1:45678'],
  });
  await cli(['network', 'mount', 'fixed-team', '--root', root, '--seed', '127.0.0.1:45679'], stateDir);
  assert.deepEqual(runtime.config.networks.find(n => n.networkId === 'fixed-team')?.bootstrapPeers, ['127.0.0.1:45679']);
  await cli(['network', 'mount', 'fixed-team', '--root', root, '--passphrase', 'explicit-secret'], stateDir);
  assert.equal(runtime.config.networks.find(n => n.networkId === 'fixed-team')?.passphrase, 'explicit-secret');
  assert.equal(runtime.config.networks.find(n => n.networkId === 'fixed-team')?.bootstrapPeers, undefined);
  const stderr: string[] = [];
  assert.equal(await runDealerCli(['network', 'mount', 'unrelated', '--root', root], {
    stateDir, stdout: () => {}, stderr: line => stderr.push(line),
  }), 1);
  assert.match(stderr.join(''), /passphrase/);
  assert.ok(!runtime.transport.listMountedNetworks().includes('unrelated'));
});

test('network mount pins caller cwd and relative root when its project has no matching preset', async t => {
  const root = directory(t);
  const clientRoot = join(root, 'client');
  const daemonRoot = join(root, 'daemon');
  for (const path of [clientRoot, join(clientRoot, 'relative'), daemonRoot, join(daemonRoot, 'relative')]) {
    mkdirSync(join(path, '.dealer'), { recursive: true });
    writeFileSync(join(path, '.dealer/project.json'), JSON.stringify({ schema: 'the-dealer-project/v1',
      projectId: 'demo', defaultNetworkId: path.startsWith(clientRoot) ? 'other-team' : 'daemon-team',
      onboarding: { passphrase: 'daemon-secret' } }));
  }
  const stateDir = join(root, 'state');
  const runtime = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await runtime.start();
  t.after(() => runtime.stop());
  const forwarded: string[][] = [];
  await runtime.serveControl(async argv => {
    forwarded.push(argv);
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(argv, { runtime, stdout: line => stdout.push(line), stderr: line => stderr.push(line) });
    return { code, stdout, stderr };
  });
  const originalCwd = process.cwd();
  const bin = new URL('../bin/dealer.js', import.meta.url).pathname;
  try {
    process.chdir(daemonRoot);
    for (const flags of [[], ['--root', 'relative']]) {
      await assert.rejects(promisify(execFile)(process.execPath, [bin, 'network', 'mount', 'daemon-team', ...flags, '--json'], {
        cwd: clientRoot, env: { ...process.env, DEALER_STATE_DIR: stateDir },
      }), error => {
        assert.match(error.stderr, /passphrase/);
        return true;
      });
      const argv = forwarded.at(-1)!;
      const resolvedRoot = argv[argv.indexOf('--root') + 1];
      assert.ok(resolvedRoot.endsWith(flags.length ? '/client/relative' : '/client'));
      assert.ok(resolvedRoot.startsWith('/'));
      assert.equal(argv.includes('--passphrase'), false);
      assert.equal(runtime.transport.getNetworkConfig('daemon-team'), undefined);
    }
  } finally {
    process.chdir(originalCwd);
  }
});

test('preset enables live Lark delivery and inherits only the selected project default', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runtime = new DealerNodeRuntime({ stateDir: join(root, 'state'), member: 'solo',
    transport: { discoveryDir: join(root, 'discovery') },
    patch: { larkNotify: { enabled: false, target: { userId: 'receiver' } } },
    commandRunner: async call => { calls.push(call); return { stdout: '{"data":{"message_id":"lark-1"}}' }; },
  });
  await runtime.start();
  t.after(() => runtime.stop());
  for (const id of ['one', 'two']) {
    const projectRoot = join(root, id);
    mkdirSync(join(projectRoot, '.dealer'), { recursive: true });
    writeFileSync(join(projectRoot, '.dealer/project.json'), JSON.stringify({ schema: 'the-dealer-project/v1',
      projectId: id, defaultNetworkId: 'local', onboarding: { notifyLark: true } }));
    await runtime.linkProject(projectRoot);
    if (id === 'one') {
      const disabled = await runtime.applyAgentPreset({ host: 'codex', conversationMode: 'inbox-only', notifyLark: false });
      assert.equal(disabled.binding.notifyLark, false);
      assert.equal(runtime.config.larkNotify?.enabled, false);
      const inherited = await runtime.applyAgentPreset({ host: 'codex', conversationMode: 'inbox-only' });
      assert.equal(inherited.binding.notifyLark, true);
      assert.equal(runtime.config.larkNotify?.enabled, true);
      assert.ok(runtime.larkNotifier);
    }
  }
  assert.equal((await runtime.applyAgentPreset({ host: 'codex', conversationMode: 'inbox-only', topic: 'ambiguous' })).binding.notifyLark, undefined);
  assert.equal((await runtime.applyAgentPreset({ host: 'codex', conversationMode: 'inbox-only', topic: 'selected', projectId: 'two' })).binding.notifyLark, true);
  const notifier = runtime.larkNotifier;
  await runtime.applyAgentPreset({ host: 'codex', conversationMode: 'inbox-only', topic: 'explicit', notifyLark: true });
  assert.notEqual(runtime.larkNotifier, notifier, 'repeated activation replaces the notifier through kernel lifecycle');
  runtime.mailbox.enqueue([message('live')], { mailboxId: 'stream:explicit' });
  await runtime.larkNotifier!.flush();
  assert.ok(calls.some(call => call.command === 'lark-cli' && call.args.includes('+messages-send')));
  const config = JSON.parse(readFileSync(join(runtime.stateDir, 'config.json'), 'utf8'));
  assert.equal(config.larkNotify.enabled, true);
  assert.equal(config.larkNotify.target.userId, 'receiver');
  assert.equal(config.topicBindings.explicit.notifyLark, true);
});

test('CLI notify-lark activates an existing daemon and survives restart', async t => {
  const root = directory(t);
  const options = { stateDir: join(root, 'state'), transport: { discoveryDir: join(root, 'discovery') },
    commandRunner: async () => ({ stdout: '{}' }) };
  const runtime = new DealerNodeRuntime(options);
  await runtime.start();
  t.after(() => runtime.stop());
  assert.equal(runtime.larkNotifier, undefined);
  const stdout: string[] = [];
  const stderr: string[] = [];
  assert.equal(await runDealerCli(['setup', 'apply-preset', '--host', 'codex', '--mode', 'inbox-only', '--notify-lark', '--json'],
    { runtime, stdout: line => stdout.push(line), stderr: line => stderr.push(line) }), 0, stderr.join(''));
  assert.equal(JSON.parse(stdout.join('')).binding.notifyLark, true);
  assert.equal(runtime.config.larkNotify?.enabled, true);
  assert.ok(runtime.larkNotifier);
  await runtime.stop();
  const restarted = new DealerNodeRuntime(options);
  await restarted.start();
  t.after(() => restarted.stop());
  assert.equal(restarted.config.larkNotify?.enabled, true);
  assert.ok(restarted.larkNotifier);
});

test('preset preview and application agree for explicit and project-default Lark activation', async t => {
  for (const scenario of ['explicit', 'sole', 'selected', 'cwd'] as const) {
    const root = directory(t);
    const stateDir = join(root, 'state');
    const runtime = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') },
      patch: { larkNotify: { enabled: false, target: { userId: 'verified-user' } } },
      commandRunner: async () => ({ stdout: '{}' }),
    });
    await runtime.start();
    t.after(() => runtime.stop());
    const manifest = { schema: 'the-dealer-project/v1', projectId: 'demo', defaultNetworkId: 'team',
      onboarding: { networkTopology: 'selectable', notifyLark: true } };
    if (scenario !== 'explicit') {
      mkdirSync(join(root, '.dealer'));
      writeFileSync(join(root, '.dealer/project.json'), JSON.stringify(manifest));
      if (scenario !== 'cwd') await runtime.linkProject(root);
    }
    await runtime.serveControl(async argv => {
      const stdout: string[] = [];
      const stderr: string[] = [];
      const code = await runDealerCli(argv, { runtime, stdout: line => stdout.push(line), stderr: line => stderr.push(line) });
      return { code, stdout, stderr };
    });
    if (scenario === 'sole' || scenario === 'selected') {
      writeFileSync(join(root, '.dealer/project.json'), JSON.stringify({ ...manifest, onboarding: { notifyLark: false } }));
    }
    const flags = scenario === 'explicit' ? ['--notify-lark'] : scenario === 'selected' ? ['--project', 'demo'] : scenario === 'cwd' ? ['--root', root] : [];
    const preview = await cli(['setup', 'binding-preset', '--host', 'codex', '--mode', 'inbox-only', ...flags], stateDir);
    const applied = await cli(['setup', 'apply-preset', '--host', 'codex', '--mode', 'inbox-only', ...flags], stateDir);
    for (const key of ['binding', 'executorConfig', 'larkNotify']) assert.deepEqual(preview[key], applied[key], scenario + ': ' + key);
    assert.equal(preview.larkNotify.enabled, true);
    assert.equal(preview.larkNotify.target.userId, 'verified-user');
    if (scenario === 'cwd') {
      const local = resolveAgentBindingPreset({ host: 'codex', conversationMode: 'inbox-only' }, { cwd: root, larkNotify: runtime.config.larkNotify });
      assert.deepEqual(local.binding, applied.binding);
      assert.deepEqual(local.larkNotify, applied.larkNotify);
      assert.equal(probeDealerSetup({ cwd: root, stateDir, env: {} }).onboarding.networkTopology, 'selectable');
    }
  }
});

test('executable discovery prefers executable PATH entries and falls back to executable local bin', async t => {
  const root = directory(t);
  const pathBin = join(root, 'path-bin');
  const localBin = join(root, '.local/bin');
  mkdirSync(pathBin);
  mkdirSync(localBin, { recursive: true });
  const pathCommand = join(pathBin, 'lark-cli');
  const localCommand = join(localBin, 'lark-cli');
  for (const file of [pathCommand, localCommand]) { writeFileSync(file, '#!/bin/sh\nexit 0\n'); chmodSync(file, 0o755); }
  const module = new URL('../packages/subprocess/index.ts', import.meta.url).href;
  const setup = new URL('../packages/cli/setup.ts', import.meta.url).href;
  const inspect = async () => {
    const result = await promisify(execFile)(process.execPath, ['--experimental-strip-types', '--input-type=module', '-e',
      `import { findExecutable } from ${JSON.stringify(module)};
       import { probeDealerSetup } from ${JSON.stringify(setup)};
       console.log(JSON.stringify({ resolved: findExecutable('lark-cli') ?? null,
         probe: probeDealerSetup().clis['lark-cli'], isolated: probeDealerSetup({ env: { PATH: '' } }).clis['lark-cli'] }));`],
      { cwd: root, env: { ...process.env, HOME: root, PATH: pathBin, DEALER_STATE_DIR: join(root, 'state') } });
    return JSON.parse(result.stdout);
  };
  assert.deepEqual(await inspect(), { resolved: pathCommand, probe: pathCommand, isolated: null });
  chmodSync(pathCommand, 0o644);
  assert.deepEqual(await inspect(), { resolved: localCommand, probe: localCommand, isolated: null });
  chmodSync(localCommand, 0o644);
  assert.deepEqual(await inspect(), { resolved: null, probe: null, isolated: null });
});

test('setup invite prompts reuse configured network, encode seeds, and do not mutate state', async t => {
  const root = directory(t);
  const config = JSON.stringify({ member: 'alice', networks: [{ networkId: 'team', passphrase: 'shared', bootstrapPeers: ['localhost:4000'] }] });
  writeFileSync(join(root, 'config.json'), config);
  for (const args of [[], ['--passphrase', 'override']]) {
    const result = await cli(['setup', 'invite-prompt', '--network', 'team', '--peer-member', 'bob', ...args], root);
    assert.equal(decodeNetworkInvite(result.inviteUri).passphrase, args.length ? 'override' : 'shared');
    assert.deepEqual(decodeNetworkInvite(result.inviteUri).bootstrapPeers, ['localhost:4000']);
    assert.match(result.crossMachinePrompt, /dealer network mount/);
    assert.match(result.crossMachinePrompt, /--member 'alice'/);
    assert.match(result.teammatePrompt, /--member 'bob'/);
    assert.match(result.teammatePrompt, /collab.request.v4/);
    for (const prompt of [result.crossMachinePrompt, result.teammatePrompt]) {
      assert.match(prompt, /https:\/\/github.com\/sensebeing\/the-dealer.git/);
      assert.ok(!prompt.includes('npm install -g git+https://github.com/sensebeing/the-dealer'));
      assert.ok(!prompt.includes('npm --prefix ~/the-dealer link'));
      assert.ok(prompt.includes('~/.local/bin/dealer'));
      assert.ok(prompt.includes('cd ~/the-dealer && npm link'));
      assert.ok(prompt.includes("npx skills add sensebeing/the-dealer --global --agent '*'"));
      assert.match(prompt, /~\/\.agents\/skills\//);
      assert.match(prompt, /~\/\.claude\/skills\//);
      assert.match(prompt, /the-dealer-setup\/SKILL.md/);
      assert.match(prompt, /dealer setup probe --json/);
      assert.match(prompt, /Never overwrite ~\/\.dealer\/identity.json/);
    }
    assert.match(result.teammatePrompt, /Ask for the teammate’s member handle/);
    assert.match(result.teammatePrompt, /dealer setup binding-preset.*--notify-lark/);
    assert.match(result.teammatePrompt, /dealer setup apply-preset.*--notify-lark/);
    for (const prompt of [result.crossMachinePrompt, result.teammatePrompt]) {
      assert.ok(prompt.includes('Network topology is fixed to team; do not ask to choose local or another network topology.'));
      for (const command of ['command -v lark-cli', '~/.local/bin/lark-cli', 'npx @larksuite/cli@latest install',
        'lark-cli config init --new', 'lark-cli auth login --recommend',
        'LARKSUITE_CLI_NO_UPDATE_NOTIFIER=1 LARKSUITE_CLI_NO_SKILLS_NOTIFIER=1 lark-cli auth status --json --verify']) assert.ok(prompt.includes(command));
    }
    const projectPrompt = buildOnboardingPrompts({ networkId: 'team', inviteUri: result.inviteUri, projectId: 'demo' }).teammatePrompt;
    assert.match(projectPrompt, /binding-preset.*--notify-lark --project 'demo'/);
    assert.match(projectPrompt, /apply-preset.*--notify-lark --project 'demo'/);
  }
});

for (const entry of AGENT_HOST_CATALOG) {
  for (const mode of entry.modes) {
    test(`setup preset ${entry.host} ${mode} is valid through CLI`, async t => {
      const root = directory(t);
      const result = await cli(['setup', 'binding-preset', '--host', entry.host, '--mode', mode, '--target-bot', 'review', '--workspace-path', root], root);
      validateTopicBindings({ [result.topic]: result.binding }, [result.binding.executor]);
      assert.equal(result.binding.mode, mode);
      if (mode === 'inbox-only') assert.equal(result.binding.executor, 'inbox');
      else if (entry.host === 'orca' || entry.host === 'antigravity') assert.equal(result.binding.executor, 'orca');
      else if (entry.host === 'chatgpt-dot') assert.equal(result.binding.executor, 'chatgpt-app');
      else if (entry.host === 'grok-bot') assert.equal(result.binding.executor, 'agent-pty');
      else assert.equal(result.binding.executor, entry.host === 'claude' ? 'claude-code' : entry.host);
    });
  }
  if (entry.host === 'antigravity-cli') {
    for (const mode of ['wave', 'fixed'] as const) {
      test(`setup preset ${entry.host} ${mode} rejects unsupported mode`, () => {
        assert.throws(() => buildAgentBindingPreset({ host: entry.host, mode }), /Invalid conversation mode/);
        assert.throws(() => validateTopicBindings({ task: { executor: entry.host, mode } }, [entry.host]), /Invalid topic binding/);
      });
    }
  }
}

for (const agent of ['codex', 'claude-code', 'chatgpt-dot', 'grok-bot', 'antigravity'] as const) {
  for (const conversationMode of ['spawn', 'fixed'] as const) {
    test(`agent-pty ${agent} ${conversationMode} persists before invoking and uses host argv`, async t => {
      const root = directory(t);
      const store = new MailboxStore(join(root, 'mailbox.json'));
      const mailboxId = conversationMode === 'spawn' ? 'task:test' : 'stream:collab.request.v4';
      const calls: CommandRequest[] = [];
      const fakeCodex = createFakeCodexAppServer({
        nextThreadId: () => 'session',
        onTurnStart: () => {
          assert.equal(store.getWakeTarget(mailboxId)?.driver, 'agent-host');
        },
      });
      const commandRunner: CommandRunner = async request => {
        assert.equal(store.getWakeTarget(mailboxId)?.driver, 'agent-host');
        calls.push(request);
        return { stdout: '{"accepted":true}' };
      };
      const plugin = createAgentPtyExecutorPlugin({
        store,
        commandRunner,
        stdioSpawner: fakeCodex.spawner,
        resolveTopicBinding: () => ({
          executor: 'agent-pty',
          agent,
          conversationMode,
          ...(conversationMode === 'fixed' ? { mailboxId } : {}),
          ...(agent === 'codex' ? {} : { agentProfile: 'review' }),
          targetBot: 'bot',
          sessionId: 'session',
          workspacePath: root,
        }),
      });
      await plugin.execute([message()], context, {});
      if (agent === 'codex') {
        assert.equal(calls.length, 0);
        assert.equal(fakeCodex.sessions.length, 1);
        const session = fakeCodex.sessions[0];
        const prompt = session.promptText!;
        assert.equal(session.request.command, 'codex');
        assert.deepEqual(session.request.args, ['app-server', '--stdio']);
        assert.equal(session.request.cwd, root);
        assert.equal(session.threadMethod, conversationMode === 'spawn' ? 'thread/start' : 'thread/resume');
        assert.equal(session.threadParams?.cwd, root);
        assert.equal(session.threadParams?.approvalPolicy, 'never');
        assert.equal(session.threadParams?.sandbox, 'danger-full-access');
        if (conversationMode === 'fixed') {
          assert.equal(session.threadParams?.threadId, 'session');
          assert.equal(session.threadParams?.excludeTurns, true);
          assert.match(prompt, /Latest .*Perform task then report/);
        } else {
          assert.ok(prompt.startsWith('Perform task then report — Dealer Task dispatched to mailbox task:test'));
          assert.ok(!prompt.includes('Payload:'));
        }
        assert.match(prompt, new RegExp(mailboxId));
        assert.ok(!prompt.includes('\n'));
        assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.conversationMode, 'fixed');
        assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'session');
        return;
      }
      const call = calls[0];
      const args = call.args;
      const prompt = args.at(-1)!;
      assert.match(prompt, new RegExp(mailboxId));
      assert.ok(!prompt.includes('\n'));
      if (conversationMode === 'spawn') {
        assert.ok(prompt.startsWith('Perform task then report — Dealer Task dispatched to mailbox task:test'));
        assert.ok(!prompt.includes('Payload:'));
      }
      assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.conversationMode, agent === 'antigravity' ? undefined : 'fixed');
      assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, agent === 'antigravity' ? undefined : 'session');
      if (conversationMode === 'fixed') assert.match(prompt, /Latest .*Perform task then report/);
      assert.equal(call.cwd, root);
      if (agent === 'claude-code') {
        assert.equal(call.command, 'claude');
        assert.deepEqual(
          args.slice(0, 4),
          conversationMode === 'spawn'
            ? ['--bg', '--name', 'session', '--dangerously-skip-permissions']
            : ['--bg', '--resume', 'session', '--dangerously-skip-permissions'],
        );
      } else if (agent === 'antigravity') {
        assert.equal(call.command, 'agy');
        assert.deepEqual(args.slice(0, 2), ['run', '--dangerously-skip-permissions']);
      } else {
        assert.equal(call.command, agent === 'chatgpt-dot' ? 'dot' : 'grok-bot');
        assert.equal(args[0], agent === 'chatgpt-dot' ? 'send' : 'dispatch');
        assert.equal(args[args.indexOf('--session-mode') + 1], conversationMode);
        assert.equal(args[args.indexOf(agent === 'chatgpt-dot' ? '--agent' : '--bot') + 1], 'bot');
      }
    });
  }
}

test('agent-pty preserves legacy invocation and rejects invalid Grok configuration before launch', async () => {
  const calls: CommandRequest[] = [];
  const commandRunner: CommandRunner = async request => {
    calls.push(request);
    return { stdout: '{}' };
  };
  await createAgentPtyExecutorPlugin({ commandRunner }).execute([message()], context, {});
  assert.deepEqual(calls[0].args, ['exec', '-']);
  assert.match(calls[0].input!, /Subject: Task/);
  await assert.rejects(createAgentPtyExecutorPlugin({ commandRunner, agent: 'grok-bot' }).execute([message()], context, {}), /requires targetBot/);
  assert.equal(calls.length, 1);
  assert.throws(() => buildAgentBindingPreset({ host: 'grok-bot' }), /requires targetBot/);
});

for (const agent of ['antigravity', 'claude', 'codex'] as SupportedOrcaAgent[]) {
  test(`Orca spawn and wave persist ${agent} for restart`, async t => {
    const root = directory(t);
    const settings = join(root, 'settings.json');
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const calls: CommandRequest[] = [];
    const runner: CommandRunner = async request => {
      calls.push(request);
      if (request.args[0] === 'worktree' && request.args[1] === 'create') return { stdout: JSON.stringify({ ok: true, result: { path: root } }) };
      if (request.args[0] === 'terminal' && request.args[1] === 'create') return { stdout: '{"ok":true,"result":{"handle":"term-test"}}' };
      return { stdout: '{"ok":true}' };
    };
    await createOrcaSpawnExecutorPlugin({ store, commandRunner: runner, agySettingsPath: settings }).execute([message('spawn', { interactive: true, agent })], context, {});
    const create = calls.find(call => call.args[0] === 'terminal' && call.args[1] === 'create')!;
    assert.equal(create.args[create.args.indexOf('--command') + 1], defaultAgentLaunchCommand(agent));
    assert.equal(existsSync(settings), agent === 'antigravity');
    assert.equal(new MailboxStore(join(root, 'mailbox.json')).getWakeTarget('task:spawn')?.driverConfig?.agent, agent);
    await createOrcaWaveExecutorPlugin({ store, agent, cwd: root, interactiveCommand: 'custom-agent' }).execute([message('wave')], context, {});
    const wake = new MailboxStore(join(root, 'mailbox.json')).getWakeTarget('stream:collab.request.v4');
    assert.equal(wake?.driverConfig?.agent, agent);
    assert.equal(wake?.driverConfig?.interactiveCommand, 'custom-agent');
  });
}

test('Orca terminal resolution reuses the configured agent, boots shells and creates with the chosen command', async () => {
  let terminals: object[] = [
    { handle: 'agy', connected: true, agentIdentity: 'antigravity', lastOutputAt: 9 },
    { handle: 'claude', connected: true, agentIdentity: 'claude-code', lastOutputAt: 1 },
  ];
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    return { stdout: JSON.stringify({ ok: true, result: request.args[1] === 'list' ? { terminals } : { handle: 'created' } }) };
  };
  assert.equal((await resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', '/repo', undefined, { agent: 'claude' })).handle, 'claude');
  assert.deepEqual(await resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', '/repo', undefined, { agent: 'codex' }), {
    handle: 'created', spawned: true,
  });
  terminals = [{ handle: 'shell', connected: true, preview: '$' }];
  assert.equal((await resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', '/repo', undefined, { agent: 'codex' })).shellBootstrapped, true);
  assert.ok(calls.at(-1)!.args.includes(defaultAgentLaunchCommand('codex')));
  terminals = [];
  assert.equal((await resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', '/repo', undefined, { agent: 'claude', interactiveCommand: 'custom' })).handle, 'created');
  assert.ok(calls.at(-1)!.args.includes('custom'));
});

test('orca-wave ignores inbound scheduling and prefers receiver binding or config over payload', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const binding = {
    executor: 'orca-wave', workspacePath: '/receiver/binding', runId: 'bound-run',
    terminalHandle: 'bound-terminal', worktreeId: 'bound-worktree', rotateAfterWaves: 3,
    coalesceMs: 60_000, interactiveCommand: 'bound-command', agent: 'claude' as const,
  };
  const bound = createOrcaWaveExecutorPlugin({
    store, agent: 'codex', cwd: '/receiver/options', runId: 'option-run',
    terminalHandle: 'option-terminal', interactiveCommand: 'option-command',
    wave: { minAgeMs: 5_000, rotateAfterWaves: 2 }, resolveTopicBinding: () => binding,
  });
  await bound.execute([message('bound', {
    minAgeMs: -1, interactiveCommand: 'sender-command', workspacePath: '/sender/workspace',
    cwd: '/sender/cwd', runId: 'sender-run', terminalHandle: 'sender-terminal',
    worktreeId: 'sender-worktree', rotateAfterWaves: 1, agent: 'codex',
  })], context, {});
  const boundTarget = store.getWakeTarget('stream:collab.request.v4')!;
  assert.equal(boundTarget.coalesceMs, 60_000);
  assert.equal(boundTarget.driverConfig?.interactiveCommand, 'bound-command');
  assert.equal(boundTarget.driverConfig?.workspacePath, '/receiver/binding');
  assert.equal(boundTarget.driverConfig?.runId, 'bound-run');
  assert.equal(boundTarget.driverConfig?.terminalHandle, 'bound-terminal');
  assert.equal(boundTarget.driverConfig?.worktreeId, 'bound-worktree');
  assert.equal(boundTarget.rotateAfterWaves, 3);
  assert.equal(boundTarget.driverConfig?.agent, 'claude');

  const configured = createOrcaWaveExecutorPlugin({
    store, cwd: '/receiver/options', runId: 'option-run', terminalHandle: 'option-terminal',
    worktreeId: 'option-worktree', interactiveCommand: 'option-command',
    wave: { minAgeMs: 5_000 }, resolveTopicBinding: () => ({ executor: 'orca-wave', streamKey: 'configured' }),
  });
  await configured.execute([message('configured', {
    minAgeMs: 99_999, interactiveCommand: 'sender-command', workspacePath: '/sender/workspace',
    cwd: '/sender/cwd', runId: 'sender-run', terminalHandle: 'sender-terminal', worktreeId: 'sender-worktree',
  })], context, {
    cwd: '/receiver/config', runId: 'config-run', terminalHandle: 'config-terminal',
    worktreeId: 'config-worktree', interactiveCommand: 'config-command', wave: { minAgeMs: 2_500 },
  });
  const configuredTarget = store.getWakeTarget('stream:configured')!;
  assert.equal(configuredTarget.coalesceMs, 2_500);
  assert.equal(configuredTarget.driverConfig?.interactiveCommand, 'config-command');
  assert.equal(configuredTarget.driverConfig?.workspacePath, '/receiver/config');
  assert.equal(configuredTarget.driverConfig?.runId, 'config-run');
  assert.equal(configuredTarget.driverConfig?.terminalHandle, 'config-terminal');
  assert.equal(configuredTarget.driverConfig?.worktreeId, 'config-worktree');
});

test('orca-wave defaults fixed workspaces to one fresh session tab per wave', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const execute = (streamKey: string, options: Parameters<typeof createOrcaWaveExecutorPlugin>[0]) =>
    createOrcaWaveExecutorPlugin(options).execute(
      [message(streamKey, { streamKey })],
      context,
      {},
    );

  await execute('default', { store, cwd: '/receiver/default' });
  const defaulted = store.getWakeTarget('stream:default')!;
  assert.equal(defaulted.rotateAfterWaves, 1);
  assert.equal(defaulted.driverConfig?.workspacePath, '/receiver/default');
  assert.equal(defaulted.driverConfig?.terminalHandle, undefined);

  // An inbound sender can never pin a tab or stretch the rotation window of a receiver-owned
  // workspace, even when the receiver only configured `cwd`.
  await createOrcaWaveExecutorPlugin({ store, cwd: '/receiver/default' }).execute(
    [message('default-inbound', {
      streamKey: 'default', terminalHandle: 'sender-tab', rotateAfterWaves: 99,
    })],
    context,
    {},
  );
  const inbound = store.getWakeTarget('stream:default')!;
  assert.equal(inbound.driverConfig?.workspacePath, '/receiver/default');
  assert.equal(inbound.driverConfig?.terminalHandle, undefined, 'payload terminalHandle must not rebind the receiver');
  assert.equal(inbound.rotateAfterWaves, 1, 'payload rotateAfterWaves must not override the receiver default');

  await execute('explicit', { store, cwd: '/receiver/explicit', wave: { rotateAfterWaves: 3 } });
  assert.equal(store.getWakeTarget('stream:explicit')!.rotateAfterWaves, 3);

  await execute('pinned', { store, cwd: '/receiver/pinned', terminalHandle: 'pinned-tab' });
  assert.equal(store.getWakeTarget('stream:pinned')!.rotateAfterWaves, undefined);
  assert.equal(store.getWakeTarget('stream:pinned')!.driverConfig?.terminalHandle, 'pinned-tab');

  await execute('run', { store, cwd: '/receiver/run', runId: 'run-owner' });
  assert.equal(store.getWakeTarget('stream:run')!.rotateAfterWaves, undefined);
  assert.equal(store.getWakeTarget('stream:run')!.driver, 'orca-run');
});

test('managed fixed presets default to one session tab per wave and allow an override', () => {
  const wavePreset = buildAgentBindingPreset({ host: 'orca', mode: 'wave' });
  assert.equal(wavePreset.binding.autoCreateWorkspace, true);
  assert.equal(wavePreset.binding.rotateAfterWaves, 1);
  for (const host of ['orca', 'antigravity'] as const) {
    const preset = buildAgentBindingPreset({ host, conversationMode: 'fixed' });
    assert.equal(preset.binding.autoCreateWorkspace, true);
    assert.equal(preset.binding.rotateAfterWaves, 1);
  }
  assert.equal(
    buildAgentBindingPreset({ host: 'orca', conversationMode: 'fixed', rotateAfterWaves: 4 }).binding.rotateAfterWaves,
    4,
  );
  assert.equal(buildAgentBindingPreset({ host: 'orca', conversationMode: 'spawn' }).binding.rotateAfterWaves, undefined);
  assert.equal(buildAgentBindingPreset({ host: 'codex', conversationMode: 'fixed' }).binding.rotateAfterWaves, undefined);
});

test('preferred terminal handle reuses a connected orphan over a fresher live terminal', async () => {
  let terminals: object[] = [
    { handle: 'bound', orphaned: true, connected: true, writable: true, lastOutputAt: 1 },
    { handle: 'live', connected: true, writable: true, agentIdentity: 'antigravity', lastOutputAt: 9 },
  ];
  const runner: CommandRunner = async () => ({ stdout: JSON.stringify({ ok: true, result: { terminals } }) });
  const resolve = (preferredHandle: string) =>
    resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', '/repo', undefined, { preferredHandle });
  assert.deepEqual(await resolve('bound'), { handle: 'bound', spawned: false });
  terminals = [
    { handle: 'bound', orphaned: true, connected: false, writable: true },
    { handle: 'live', connected: true, writable: true, agentIdentity: 'antigravity' },
  ];
  assert.deepEqual(await resolve('bound'), { handle: 'live', spawned: false });
  terminals = [
    { handle: 'bound', connected: true, writable: false },
    { handle: 'live', connected: true, writable: true, agentIdentity: 'antigravity' },
  ];
  assert.deepEqual(await resolve('bound'), { handle: 'live', spawned: false });
  assert.equal((await resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', '/repo', undefined, {})).handle, 'live');
});

test('an unusable bound Orca terminal cannot launch a replacement or take a sibling tab for an exclusive session', async () => {
  const bound = { handle: 'bound', connected: false, writable: true, tabId: 'tab', leafId: 'leaf' };
  const sibling = { handle: 'sibling', connected: true, writable: true, agentIdentity: 'antigravity' };
  let terminals = [bound, sibling];
  const runner: CommandRunner = async request => {
    assert.deepEqual(request.args.slice(0, 2), ['terminal', 'list']);
    return { stdout: JSON.stringify({ ok: true, result: { terminals } }) };
  };
  for (const { options, hasSibling } of [
    { options: { preferredHandle: 'bound', sessionId: 'session' }, hasSibling: true },
    { options: { preferredHandle: 'bound', dedicatedTab: true }, hasSibling: true },
    { options: { preferredPaneKey: 'tab:leaf' }, hasSibling: true },
    { options: { preferredHandle: 'bound' }, hasSibling: false },
  ]) {
    terminals = hasSibling ? [bound, sibling] : [bound];
    assert.deepEqual(await planOrcaWorkspaceTerminal(runner, 'orca', '/repo', undefined, options), {
      kind: 'unusable', handle: 'bound',
    });
    await assert.rejects(
      resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', '/repo', undefined, options),
      { message: 'Terminal is disconnected or not writable' },
    );
  }
});

test('runtime uses a generated fixed binding, persists mailbox and supports setup while running', async t => {
  const root = directory(t);
  const fakeCodex = createFakeCodexAppServer();
  const preset = buildAgentBindingPreset({ host: 'codex', conversationMode: 'fixed', workspacePath: root, sessionId: 'review' });
  const runtime = new DealerNodeRuntime({
    stateDir: join(root, 'state'), member: 'solo',
    transport: { discoveryDir: join(root, 'discovery') },
    patch: { topicBindings: { [preset.topic]: preset.binding } },
    stdioSpawner: fakeCodex.spawner,
  });
  await runtime.start();
  t.after(() => runtime.stop());
  assert.equal(runtime.config.profile, 'personal-dev');
  assert.equal(runtime.config.gateMode, 'open');
  const result = await runtime.emitter.emit({ networkId: 'local', kind: preset.topic,
    routing: { mode: 'unicast', targets: ['member:solo'] }, subject: 'Review', body: 'Check this change', payload: {} });
  assert.equal(result.receipts[0].stage, 'dispatched', JSON.stringify(result.receipts));
  await eventually(() => {
    assert.equal(fakeCodex.sessions.length, 1);
    assert.equal(fakeCodex.sessions[0].stdinEnded, true);
  });
  assert.deepEqual(fakeCodex.sessions[0].request.args, ['app-server', '--stdio']);
  assert.equal(fakeCodex.sessions[0].request.cwd, root);
  assert.equal(fakeCodex.sessions[0].threadMethod, 'thread/resume');
  assert.equal(fakeCodex.sessions[0].threadId, 'review');
  assert.equal(runtime.mailbox.getWakeTarget(`stream:${preset.topic}`)?.driver, 'agent-host');
  assert.equal((await cli(['setup', 'probe', '--root', root], runtime.stateDir)).node.configured, true);
  const invite = await cli(['setup', 'invite-prompt', '--network', 'new-team'], runtime.stateDir);
  assert.equal(decodeNetworkInvite(invite.inviteUri).networkId, 'new-team');
  assert.equal((await cli(['setup', 'binding-preset', '--host', 'claude-code', '--mode', 'fixed'], runtime.stateDir)).binding.agent, 'claude-code');
});

for (const scenario of [
  { name: 'saved personal-dev', profile: 'personal-dev', override: false, expected: true },
  { name: 'explicit executor override', profile: 'personal-dev', override: true, expected: false },
  { name: 'another saved profile', profile: 'headless-relay', override: false, expected: false },
]) {
  test(`runtime agent-pty migration respects ${scenario.name}`, async t => {
    const root = directory(t);
    const executors = scenario.name === 'saved personal-dev' ? ['orca-spawn', 'orca-wave', 'notify', 'inbox', 'agent-pty', 'codex', 'claude-code', 'antigravity'] : ['notify', 'inbox'];
    writeFileSync(join(root, 'config.json'), JSON.stringify({
      profile: scenario.profile, executors, defaultExecutor: 'inbox', gateMode: 'approval_required',
    }));
    const runtime = new DealerNodeRuntime({
      stateDir: root,
      transport: { discoveryDir: join(root, 'discovery') },
      ...(scenario.override ? { patch: { executors } } : {}),
    });
    await runtime.start();
    t.after(() => runtime.stop());
    for (const executor of ['agent-pty', 'codex', 'claude-code', 'antigravity', 'orca', 'codex-cli', 'antigravity-cli', 'claude-app', 'chatgpt-app', 'antigravity-app']) {
      assert.equal(runtime.config.executors.includes(executor), scenario.expected);
    }
    assert.equal(runtime.config.gateMode, 'approval_required');
  });
}

test('failed host execution confirms durable custody without claiming a successful wake', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const fakeCodex = createFakeCodexAppServer({
    onTurnStart: () => ({ status: 'failed', error: { message: 'host rejected command' } }),
  });
  const plugin = createAgentPtyExecutorPlugin({
    store,
    agent: 'codex',
    conversationMode: 'spawn',
    stdioSpawner: fakeCodex.spawner,
  });
  const receipt = await plugin.execute([message('failed')], context, {});
  assert.equal(receipt.woke, false);
  assert.match(String(receipt.reason), /host rejected command/);
  assert.equal(new MailboxStore(join(root, 'mailbox.json')).getWakeTarget('task:failed')?.driver, 'agent-host');
  const failed = store.getWakeTarget('task:failed')!;
  assert.equal(failed.pendingWake, false);
  assert.equal(failed.deliveryInDoubt, true);
  assert.match(failed.lastWakeError!, /host rejected command/);
});

test('wake target host options merge, clear and validate after reload', t => {
  const root = directory(t);
  const file = join(root, 'mailbox.json');
  const store = new MailboxStore(file);
  store.upsertWakeTarget('default', { driverConfig: { agent: 'codex', interactiveCommand: 'custom', hostAgent: 'claude-code', conversationMode: 'fixed', agentProfile: 'review', targetBot: 'bot', sessionId: 'session' } });
  store.upsertWakeTarget('default', { driverConfig: { workspacePath: root } });
  assert.equal(new MailboxStore(file).getWakeTarget('default')?.driverConfig?.agent, 'codex');
  const restored = new MailboxStore(file).getWakeTarget('default')!;
  assert.equal(restored.driverConfig?.hostAgent, 'claude-code');
  assert.equal(restored.driverConfig?.conversationMode, 'fixed');
  assert.equal(restored.driverConfig?.agentProfile, 'review');
  assert.equal(restored.driverConfig?.targetBot, 'bot');
  assert.equal(restored.driverConfig?.sessionId, 'session');
  for (const patch of [{ hostAgent: 'invalid' }, { conversationMode: 'invalid' }, { agentProfile: 1 }, { targetBot: false }, { sessionId: [] }]) {
    assert.throws(() => readAgentHostDriverConfig({ driverConfig: patch }));
  }
  store.upsertWakeTarget('default', { driverConfig: { agent: undefined, interactiveCommand: undefined, hostAgent: undefined, conversationMode: undefined, agentProfile: undefined, targetBot: undefined, sessionId: undefined } });
  const cleared = new MailboxStore(file).getWakeTarget('default')!;
  for (const field of ['hostAgent', 'conversationMode', 'agentProfile', 'targetBot', 'sessionId'] as const) assert.equal(cleared.driverConfig?.[field], undefined);
  assert.equal(new MailboxStore(file).getWakeTarget('default')?.driverConfig?.interactiveCommand, undefined);
  assert.throws(() => validateTopicBindings({ task: { executor: 'agent-pty', conversationMode: 'inbox-only' } }, ['agent-pty']), /Invalid topic binding/);
  assert.throws(() => validateTopicBindings({ task: { executor: 'orca-wave', agent: 'grok-bot' } }, ['orca-wave']), /Invalid topic binding/);
});


test('TCP seeds discover both directions without shared registry or multicast and reject foreign keys', async t => {
  const root = directory(t);
  const seed = new P2PTransportService({ discoveryDir: join(root, 'seed'), multicast: false });
  const peer = new P2PTransportService({ discoveryDir: join(root, 'peer'), multicast: false, listenHost: '127.0.0.1' });
  const foreign = new P2PTransportService({ discoveryDir: join(root, 'foreign'), multicast: false, listenHost: '127.0.0.1' });
  t.after(async () => { await Promise.all([seed.dispose(), peer.dispose(), foreign.dispose()]); });
  await seed.mountNetwork({ networkId: 'team', passphrase: 'shared' });
  await peer.mountNetwork({ networkId: 'team', passphrase: 'shared', bootstrapPeers: ['127.0.0.1:1', seed.endpointAddr!] });
  assert.ok(seed.endpointAddr!.startsWith(`${defaultAdvertisedHost()}:`));
  assert.equal(peer.listPeerEndpoints('team').get(seed.localNodeId), seed.endpointAddr);
  assert.equal(seed.listPeerEndpoints('team').get(peer.localNodeId), peer.endpointAddr);
  seed.onInboundFrame(() => Buffer.from('seed reply'));
  peer.onInboundFrame(() => Buffer.from('peer reply'));
  assert.equal(Buffer.from(await peer.sendUnicast('team', seed.localNodeId, Buffer.from('ping'))).toString(), 'seed reply');
  assert.equal(Buffer.from(await seed.sendUnicast('team', peer.localNodeId, Buffer.from('ping'))).toString(), 'peer reply');
  await foreign.mountNetwork({ networkId: 'team', passphrase: 'wrong', bootstrapPeers: [seed.endpointAddr!] });
  assert.equal(foreign.listPeerEndpoints('team').size, 0);
  assert.equal(seed.listPeerEndpoints('team').has(foreign.localNodeId), false);
});

test('advertised host prefers overrides, explicit listeners, then the primary LAN IPv4', () => {
  const primary = defaultAdvertisedHost();
  assert.equal(defaultAdvertisedHost(), primary);
  assert.equal(defaultAdvertisedHost({ listenHost: '0.0.0.0' }), primary);
  assert.equal(defaultAdvertisedHost({ listenHost: '::' }), primary);
  assert.equal(defaultAdvertisedHost({ listenHost: '127.0.0.1' }), '127.0.0.1');
  assert.equal(defaultAdvertisedHost({ listenHost: '127.0.0.1', advertisedHost: 'reachable.example' }), 'reachable.example');
});

test('project init publishes relay seeds and node startup advertises the override host', { timeout: 15_000 }, async t => {
  const root = directory(t);
  const stateDir = join(root, 'state');
  const projectRoot = join(root, 'project');
  const manifest = await cli(['project', 'init', '--root', projectRoot, '--id', 'team', '--network', 'team',
    '--bootstrap-peers', 'relay.example:45874, other.example:45874'], stateDir);
  assert.deepEqual(manifest.onboarding?.bootstrapPeers, ['relay.example:45874', 'other.example:45874']);
  assert.deepEqual(readProjectManifest(projectRoot).onboarding?.bootstrapPeers, ['relay.example:45874', 'other.example:45874']);
  assert.equal((await cli(['project', 'init', '--root', join(root, 'plain'), '--id', 'plain'], stateDir)).onboarding, undefined);

  process.env.DEALER_ADVERTISED_HOST = '198.51.100.9';
  try {
    const runtime = await new DealerNodeRuntime({
      stateDir: join(root, 'env-state'),
      transport: { discoveryDir: join(root, 'env-mesh'), multicast: false },
    }).start();
    t.after(() => runtime.stop());
    await runtime.transport.mountNetwork({ networkId: 'team', passphrase: 'shared' });
    assert.ok(String(runtime.status().endpoint).startsWith('198.51.100.9:'));
  } finally {
    delete process.env.DEALER_ADVERTISED_HOST;
  }

  const daemonState = join(root, 'daemon');
  mkdirSync(daemonState, { recursive: true });
  writeFileSync(join(daemonState, 'config.json'), JSON.stringify({ networks: [{ networkId: 'team', passphrase: 'shared' }] }));
  const daemon = spawn(process.execPath, ['--no-warnings', '--experimental-strip-types',
    fileURLToPath(new URL('../bin/dealerd.js', import.meta.url)),
    '--profile', 'headless-relay', '--member', 'relay', '--advertised-host', '192.0.2.7',
  ], { env: { ...process.env, DEALER_STATE_DIR: daemonState }, stdio: ['ignore', 'pipe', 'pipe'] });
  const exited = once(daemon, 'exit');
  const lines = createInterface({ input: daemon.stdout });
  let errors = '';
  daemon.stderr.on('data', chunk => { errors += chunk; });
  t.after(async () => {
    daemon.kill('SIGTERM');
    await exited;
    lines.close();
  });
  const ready = await Promise.race([
    once(lines, 'line').then(([line]) => JSON.parse(line)),
    exited.then(() => { throw new Error(`Daemon exited before ready: ${errors}`); }),
  ]);
  assert.ok(String(ready.endpoint).startsWith('192.0.2.7:'), String(ready.endpoint));
});

test('invite auto-seeds from live daemon endpoint, preserves explicit seeds and excludes loopback', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({ stateDir: join(root, 'state'),
    transport: { discoveryDir: join(root, 'mesh'), multicast: false, advertisedHost: '192.0.2.42' },
  }).start();
  t.after(() => runtime.stop());
  await runtime.transport.mountNetwork({ networkId: 'team', passphrase: 'shared' });
  await runtime.serveControl(async argv => {
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(argv, { runtime, stdout: line => stdout.push(line), stderr: line => stderr.push(line) });
    return { code, stdout, stderr };
  });
  const generated = await cli(['setup', 'invite-prompt', '--network', 'team'], runtime.stateDir);
  assert.deepEqual(decodeNetworkInvite(generated.inviteUri).bootstrapPeers, [runtime.status().endpoint]);
  const explicit = await cli(['setup', 'invite-prompt', '--network', 'team', '--seed', 'other.example:4567'], runtime.stateDir);
  assert.deepEqual(decodeNetworkInvite(explicit.inviteUri).bootstrapPeers, ['other.example:4567']);
  runtime.transport.endpointAddr = '127.0.0.1:4567';
  const loopback = await cli(['setup', 'invite-prompt', '--network', 'team'], runtime.stateDir);
  assert.equal(decodeNetworkInvite(loopback.inviteUri).bootstrapPeers?.length ?? 0, 0);
});

test('offline invite uses configured port with LAN address but never invents an ephemeral port', async t => {
  const root = directory(t);
  const primary = defaultAdvertisedHost();
  writeFileSync(join(root, 'config.json'), JSON.stringify({ port: 4567 }));
  const configured = await cli(['setup', 'invite-prompt', '--network', 'team'], root);
  assert.deepEqual(decodeNetworkInvite(configured.inviteUri).bootstrapPeers ?? [], primary === '127.0.0.1' ? [] : [`${primary}:4567`]);
  writeFileSync(join(root, 'config.json'), JSON.stringify({ port: 0 }));
  const ephemeral = await cli(['setup', 'invite-prompt', '--network', 'team'], root);
  assert.equal(decodeNetworkInvite(ephemeral.inviteUri).bootstrapPeers?.length ?? 0, 0);
});


test('advertised host ranks physical private LAN addresses and excludes TUN and link-local IPv4', () => {
  const ipv4 = (address: string, internal = false): NetworkInterfaceInfo => ({
    address, family: 'IPv4', internal, netmask: '255.255.0.0', mac: '00:00:00:00:00:00', cidr: null,
  });
  const unusable = { lo0: [ipv4('127.0.0.1', true)], utun0: [ipv4('198.18.0.1')],
    utun1: [ipv4('198.19.255.1')], en0: [ipv4('169.254.1.2')] };
  assert.equal(defaultAdvertisedHost({}, unusable), '127.0.0.1');
  for (const name of ['en0', 'eth0', 'wlan0']) {
    for (const address of ['10.1.2.3', '192.168.1.2', '172.16.0.2', '172.31.255.2']) {
      assert.equal(defaultAdvertisedHost({}, { ...unusable, vpn0: [ipv4('10.8.0.1')],
        public0: [ipv4('203.0.113.2')], [name]: [ipv4(address)] }), address);
    }
  }
  for (const address of ['172.15.0.1', '172.32.0.1', '192.169.1.2']) {
    assert.equal(defaultAdvertisedHost({}, { other0: [ipv4('203.0.113.2')], en0: [ipv4(address)] }), '203.0.113.2');
  }
  assert.equal(defaultAdvertisedHost({}, { ...unusable, other0: [ipv4('203.0.113.2')] }), '203.0.113.2');
  assert.equal(defaultAdvertisedHost({}, {}), '127.0.0.1');
});

test('TCP seed probes are bounded across 50ms ticks for discovered and unavailable seeds', async t => {
  const root = directory(t);
  const seed = new P2PTransportService({ discoveryDir: join(root, 'seed'), multicast: false, listenHost: '127.0.0.1' });
  const peer = new P2PTransportService({ discoveryDir: join(root, 'peer'), multicast: false, listenHost: '127.0.0.1' });
  t.after(async () => { await peer.dispose(); await seed.dispose(); });
  await seed.mountNetwork({ networkId: 'team', passphrase: 'shared' });
  clearInterval(seed['timer']);
  const attempts = t.mock.fn(peer['track']);
  peer['track'] = attempts;
  let connections = 0;
  seed['server']!.on('connection', () => { connections++; });
  // Tunnels are fire-and-forget, so the server's accept may land after the awaited probe.
  const settledConnections = async (expected: number) => {
    for (let i = 0; i < 200 && connections < expected; i++) {
      await new Promise(resolve => setTimeout(resolve, 5));
    }
    return connections;
  };
  t.mock.timers.enable({ apis: ['Date'], now: 10_000 });
  // The same seed with a foreign key remains undiscovered, exercising failed probes too.
  await peer.mountNetwork({ networkId: 'foreign', passphrase: 'wrong', bootstrapPeers: [seed.endpointAddr!, '127.0.0.1:1'] });
  await peer.mountNetwork({ networkId: 'team', passphrase: 'shared', bootstrapPeers: [seed.endpointAddr!] });
  clearInterval(peer['timer']);
  assert.equal(attempts.mock.callCount(), 4, 'foreign and unavailable seeds do not open wasted tunnels');
  assert.equal(await settledConnections(3), 3, 'only the verified team seed gets a persistent tunnel');
  assert.equal(peer.listPeerEndpoints('team').get(seed.localNodeId), seed.endpointAddr);
  assert.equal(peer.listPeerEndpoints('foreign').size, 0);
  for (let tick = 1; tick < 40; tick++) {
    t.mock.timers.tick(50);
    await peer['discover']();
  }
  assert.equal(attempts.mock.callCount(), 4, 'no TCP churn during fast discovery ticks');
  assert.equal(connections, 3, 'no TCP churn during fast discovery ticks');
  t.mock.timers.tick(50);
  await peer['discover']();
  assert.equal(attempts.mock.callCount(), 7, 'known, failed and rejected-tunnel seeds retry at the bounded interval');
  assert.equal(await settledConnections(5), 5, 'only verified seeds open tunnels on retry');
});

test('Tailscale unicast UDP discovery answers once and keeps the routed host through peer cards', async t => {
  const tailscaleAddress = Object.values(networkInterfaces()).flat()
    .find(info => info?.family === 'IPv4' && !info.internal && isTailscaleIpv4Host(info.address))?.address;
  if (!tailscaleAddress) {
    t.skip('No local Tailscale IPv4 interface (100.64.0.0/10)');
    return;
  }
  const root = directory(t);
  const networkId = 'tailscale-discovery';
  const passphrase = 'tailscale-secret';
  const keys = deriveNetworkKeys(networkId, passphrase);
  const local = generateNodeKeyPair();
  const remote = generateNodeKeyPair();
  const [udpPort, remotePort] = await freeUdpPorts(2);
  const peerPort = await freeTcpPort(t);
  const advertisedLanAddress = `10.165.36.130:${peerPort}`;
  const kernel = new Kernel([
    { plugin: createTransportPlugin({ localNodeId: local.nodeId, discoveryDir: join(root, 'mesh'),
      mDnsPort: udpPort, tailscalePeers: () => [tailscaleAddress] }) },
    { plugin: createNodePlugin({ keyPair: local, member: 'alice', nodeName: 'alice-mbp', exchangeIntervalMs: 60_000 }) },
  ]);
  t.after(() => kernel.stop());
  await kernel.start();
  const transport = kernel.ctx.services.transport as P2PTransportService;
  // A raw UDP peer stands in for a Tailscale host: it receives the announce over the
  // routed 100.64/10 address and replies with a beacon advertising its LAN address.
  const announcements: { type: string; endpointAddr: string; reply?: boolean }[] = [];
  const remoteSocket = createSocket('udp4');
  t.after(() => remoteSocket.close());
  remoteSocket.on('message', (bytes, rinfo) => {
    if (rinfo.address !== tailscaleAddress) {
      return; // Ignore the same-host LAN broadcast copy; this test exercises the routed unicast path.
    }
    announcements.push(unsealNetworkFrame(JSON.parse(bytes.toString()), keys));
    const reply = sealNetworkFrame(
      { type: 'discovery', endpointAddr: advertisedLanAddress, expires: Date.now() + 10_000, reply: true },
      keys,
      remote.nodeId,
    );
    remoteSocket.send(Buffer.from(JSON.stringify(reply)), rinfo.port, rinfo.address);
  });
  await new Promise<void>(resolve => remoteSocket.bind(remotePort, () => resolve()));
  await transport.mountNetwork({ networkId, passphrase, mDnsPort: remotePort });
  const routedEndpoint = `${tailscaleAddress}:${peerPort}`;
  await eventually(() => assert.equal(transport.listPeerEndpoints(networkId).get(remote.nodeId), routedEndpoint));
  clearInterval(transport['timer']);
  await new Promise(resolve => setTimeout(resolve, 200));
  assert.equal(announcements.length, 1, 'one announce, one reply, no discovery ping-pong');
  assert.equal(announcements[0].type, 'discovery');
  assert.equal(announcements[0].reply, undefined);
  const card = signNodeCard({
    nodeId: remote.nodeId, networkId, revision: 1,
    identity: { member: 'ben', nodeName: 'ben-mac-mini' },
    admissionMode: 'open', capabilities: { summary: 'remote', skills: [] },
    runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] },
    endpointAddr: advertisedLanAddress, updatedAt: new Date().toISOString(),
  }, remote.privateKeyPem);
  kernel.ctx.emit('transport/inbound', {
    networkId, senderNodeId: remote.nodeId, mode: 'unicast', payload: { type: 'peer.card', card },
  });
  assert.equal(transport.listPeerEndpoints(networkId).get(remote.nodeId), routedEndpoint,
    'a card advertising the LAN address must not replace the routed Tailscale endpoint');
  for (const endpoint of ['43.110.36.140:8880', '10.relay.example:8880', '192.168.relay.example:8880', '172.16.relay.example:8880']) {
    transport.registerPeerEndpoint(networkId, remote.nodeId, routedEndpoint);
    assert.equal(transport['reachableEndpoint'](endpoint, tailscaleAddress), endpoint,
      'public IPv4 and DNS relay endpoints must not be rewritten to a Tailscale interface');
    transport.registerPeerEndpoint(networkId, remote.nodeId, endpoint);
    assert.equal(transport.listPeerEndpoints(networkId).get(remote.nodeId), endpoint,
      'public IPv4 and DNS relay endpoints can replace a Tailscale endpoint');
  }
});

test('project linking merges newly published onboarding bootstrap peers into a mounted network', async t => {
  const root = directory(t);
  mkdirSync(join(root, '.dealer'));
  const path = join(root, '.dealer/project.json');
  const manifest = { schema: 'the-dealer-project/v1', projectId: 'merge', defaultNetworkId: 'merge-team',
    onboarding: { passphrase: 'merge-secret', bootstrapPeers: ['127.0.0.1:45671'] } };
  writeFileSync(path, JSON.stringify(manifest));
  const runtime = new DealerNodeRuntime({ stateDir: join(root, 'state'), transport: { discoveryDir: join(root, 'discovery') } });
  await runtime.start();
  t.after(() => runtime.stop());
  await runtime.linkProject(root);
  assert.deepEqual(runtime.transport.getNetworkConfig('merge-team')?.bootstrapPeers, ['127.0.0.1:45671']);
  writeFileSync(path, JSON.stringify({ ...manifest,
    onboarding: { ...manifest.onboarding, bootstrapPeers: ['127.0.0.1:45671', '127.0.0.1:45672'] } }));
  await runtime.linkProject(root);
  assert.deepEqual(runtime.transport.getNetworkConfig('merge-team')?.bootstrapPeers, ['127.0.0.1:45671', '127.0.0.1:45672']);
  assert.deepEqual(runtime.config.networks.find(network => network.networkId === 'merge-team')?.bootstrapPeers,
    ['127.0.0.1:45671', '127.0.0.1:45672']);
  await runtime.linkProject(root);
  assert.deepEqual(runtime.transport.getNetworkConfig('merge-team')?.bootstrapPeers, ['127.0.0.1:45671', '127.0.0.1:45672']);
  await runtime.stop();
  writeFileSync(path, JSON.stringify({ ...manifest, onboarding: { ...manifest.onboarding,
    bootstrapPeers: ['127.0.0.1:45671', '127.0.0.1:45672', '127.0.0.1:45673'] } }));
  const restored = new DealerNodeRuntime({ stateDir: runtime.stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await restored.start();
  t.after(() => restored.stop());
  assert.deepEqual(restored.transport.getNetworkConfig('merge-team')?.bootstrapPeers,
    ['127.0.0.1:45671', '127.0.0.1:45672', '127.0.0.1:45673']);
  await restored.mountNetwork({ networkId: 'merge-team', passphrase: 'offline-test-only' });
  await restored.linkProject(root);
  assert.deepEqual(restored.transport.getNetworkConfig('merge-team')?.bootstrapPeers ?? [], []);
  assert.equal(restored.transport.getNetworkConfig('merge-team')?.passphrase, 'offline-test-only');
});


for (const [name, factory, command] of [
  ['codex', createCodexExecutorPlugin, 'codex'],
  ['claude-code', createClaudeCodeExecutorPlugin, 'claude'],
  ['antigravity', createAntigravityExecutorPlugin, 'agy'],
] as const) {
  for (const mode of ['spawn', 'fixed'] as const) {
    test(`first-class ${name} ${mode} uses durable host invocation and isolates trust`, async t => {
      const root = directory(t);
      const settings = join(root, 'agy-settings.json');
      const store = new MailboxStore(join(root, 'mailbox.json'));
      const calls: CommandRequest[] = [];
      const mailboxId = mode === 'spawn' ? 'task:test' : 'stream:review';
      const fakeCodex = createFakeCodexAppServer({
        nextThreadId: () => 'session',
        onTurnStart: (_params, ctx) => {
          assert.equal(store.getWakeTarget(mailboxId)?.driver, 'agent-host');
          ctx.sendStderr('diagnostic');
        },
      });
      const options = {
        store, conversationMode: mode,
        resolveTopicBinding: () => ({ executor: name, ...(mode === 'fixed' ? { mailboxId } : {}), workspacePath: root, sessionId: 'session' }),
        stdioSpawner: fakeCodex.spawner,
        commandRunner: async (request: CommandRequest) => {
          assert.equal(store.getWakeTarget(mailboxId)?.driver, 'agent-host');
          calls.push(request);
          return { stdout: '{"accepted":true}', stderr: 'diagnostic' };
        },
      };
      const hostSettings = join(homedir(), '.gemini/antigravity-cli/settings.json');
      const beforeSettings = existsSync(hostSettings) ? readFileSync(hostSettings, 'utf8') : undefined;
      const plugin = factory(options);
      assert.equal(plugin.name, name);
      const result = await plugin.execute([message('test', { agent: 'grok-bot' })], context, {});
      assert.equal(result.executor, name);
      assert.ok(result.executionId);
      assert.equal(result.stderr, 'diagnostic');
      assert.equal(existsSync(settings), false);
      assert.equal(existsSync(hostSettings) ? readFileSync(hostSettings, 'utf8') : undefined, beforeSettings);
      if (name === 'codex') {
        assert.deepEqual(result.output, { threadId: 'session' });
        assert.equal(fakeCodex.sessions.length, 1);
        assert.equal(fakeCodex.sessions[0].request.command, command);
        assert.deepEqual(fakeCodex.sessions[0].request.args, ['app-server', '--stdio']);
        assert.equal(fakeCodex.sessions[0].request.cwd, root);
        assert.equal(fakeCodex.sessions[0].threadMethod, mode === 'spawn' ? 'thread/start' : 'thread/resume');
        assert.match(fakeCodex.sessions[0].promptText!, new RegExp(mailboxId));
        return;
      }
      assert.deepEqual(result.output, { accepted: true });
      assert.equal(calls[0].command, command);
      assert.equal(calls[0].cwd, root);
      assert.match(calls[0].args.at(-1)!, new RegExp(mailboxId));
      if (name === 'claude-code') {
        assert.deepEqual(
          calls[0].args.slice(0, 4),
          mode === 'spawn'
            ? ['--bg', '--name', 'session', '--dangerously-skip-permissions']
            : ['--bg', '--resume', 'session', '--dangerously-skip-permissions'],
        );
      }
      if (name === 'antigravity') {
        // Invalid invocation configuration must fail before trusting the workspace: a
        // pre-effect error cannot leave a settings mutation behind.
        await assert.rejects(
          createAntigravityExecutorPlugin({ ...options, agySettingsPath: settings })
            .execute([message('invalid-invocation', { mode: 'wave' })], context, {}),
          (error: Error & { beforeExternalEffect?: boolean }) =>
            error.beforeExternalEffect === true && /Standalone Antigravity CLI/.test(error.message),
        );
        assert.equal(existsSync(settings), false);
        await createAntigravityExecutorPlugin({ ...options, agySettingsPath: settings,
          commandRunner: async request => {
            assert.ok(JSON.parse(readFileSync(settings, 'utf8')).trustedWorkspaces.includes(root));
            return options.commandRunner(request);
          },
        }).execute([message()], context, {});
      }
    });
  }
}

test('named executors default to spawn and preserve conversation-mode precedence', async () => {
  const calls: CommandRequest[] = [];
  const fakeCodex = createFakeCodexAppServer();
  const commandRunner: CommandRunner = async request => { calls.push(request); return { stdout: 'plain output' }; };
  const codexResult = await createCodexExecutorPlugin({ stdioSpawner: fakeCodex.spawner }).execute([message()], context, {});
  assert.deepEqual(codexResult.output, { threadId: 'codex-thread-1' });
  assert.equal(fakeCodex.sessions.at(-1)!.threadMethod, 'thread/start');
  assert.match(fakeCodex.sessions.at(-1)!.promptText!, /Task/);
  for (const factory of [createClaudeCodeExecutorPlugin, createAntigravityExecutorPlugin]) {
    const result = await factory({ commandRunner }).execute([message()], context, {});
    assert.equal(result.output, 'plain output');
    assert.match(calls.at(-1)!.args.at(-1)!, /Task/);
  }
  for (const source of ['options', 'config', 'binding', 'payload']) {
    const result = await createCodexExecutorPlugin({ stdioSpawner: fakeCodex.spawner, sessionId: 'existing',
      conversationMode: source === 'options' ? 'fixed' : 'spawn',
      resolveTopicBinding: () => ({ executor: 'codex', ...(source === 'binding' ? { conversationMode: 'fixed' as const } : {}) }),
    }).execute([message('test', source === 'payload' ? { conversationMode: 'fixed' } : {})], context,
      source === 'config' ? { conversationMode: 'fixed' } : {});
    assert.equal(result.executor, 'codex');
    assert.equal(fakeCodex.sessions.at(-1)!.threadMethod, 'thread/resume');
    assert.equal(fakeCodex.sessions.at(-1)!.threadId, 'existing');
  }
  await assert.rejects(createCodexExecutorPlugin({ stdioSpawner: fakeCodex.spawner }).execute([message('bad', { conversationMode: 'inbox-only' })], context, {}), /Invalid agent conversationMode/);
  const failingCodex = createFakeCodexAppServer({
    onTurnStart: () => ({ status: 'failed', error: { message: 'turn failed' } }),
  });
  await assert.rejects(createCodexExecutorPlugin({ stdioSpawner: failingCodex.spawner }).execute([message()], context, {}), /turn failed/);
});

test('orca executor resolves spawn, wave and fixed modes per batch', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    return {
      stdout: request.args.includes('worktree') && request.args.includes('create')
        ? JSON.stringify({ path: root })
        : request.args.includes('terminal') && request.args.includes('create')
          ? JSON.stringify({ handle: 'term-spawn' })
          : '{}',
    };
  };
  const plugin = createOrcaExecutorPlugin({
    store,
    cwd: root,
    commandRunner: runner,
    resolveTopicBinding: (_kind, streamKey) => streamKey === 'wave' ? { executor: 'orca', mode: 'wave', mailboxId: 'stream:wave', workspacePath: root, rotateAfterWaves: 1 } : undefined,
  });
  for (const [id, payload] of [['spawn', { mode: 'spawn', noParent: true }], ['wave', { mode: 'wave', streamKey: 'wave' }], ['fixed', { mode: 'fixed', streamKey: 'wave' }]] as const) {
    const receipt = await plugin.execute([message(id, payload)], context, {});
    assert.equal(receipt.executor, 'orca');
    if (id === 'spawn') {
      assert.ok(calls.some(call => call.args.includes('worktree')));
      assert.ok(calls.some(call => call.args.includes('create')));
      assert.equal(store.check({ mailboxId: 'task:spawn', peek: true }).items.length, 1);
    } else if (id === 'wave') {
      assert.equal(store.getWakeTarget('stream:wave')?.rotateAfterWaves, 1);
    } else {
      assert.equal(store.getWakeTarget('stream:wave')?.rotateAfterWaves, undefined);
    }
  }
});

test('new host executor factories use host invocation mode', async t => {
  const fakeCodex = createFakeCodexAppServer({ nextThreadId: () => 'wave-thread' });
  await createCodexCliExecutorPlugin({ stdioSpawner: fakeCodex.spawner }).execute([message()], context, {});
  assert.equal(fakeCodex.sessions.at(-1)!.request.command, 'codex');
  assert.deepEqual(fakeCodex.sessions.at(-1)!.request.args, ['app-server', '--stdio']);
  assert.equal(fakeCodex.sessions.at(-1)!.threadMethod, 'thread/start');
  const cases = [
    [createAntigravityCliExecutorPlugin, 'agy', ['run', '--dangerously-skip-permissions']],
    [createClaudeAppExecutorPlugin, 'claude-app', ['send', '--session-mode', 'spawn']],
    [createChatgptAppExecutorPlugin, 'dot', ['send', '--session-mode', 'spawn']],
    [createAntigravityAppExecutorPlugin, 'antigravity-app', ['send', '--session-mode', 'spawn']],
  ] as const;
  for (const [factory, command, prefix] of cases) {
    const calls: CommandRequest[] = [];
    const commandRunner: CommandRunner = async request => { calls.push(request); return { stdout: '{}' }; };
    await factory({ commandRunner }).execute([message()], context, {});
    assert.equal(calls.at(-1)!.command, command);
    assert.deepEqual(calls.at(-1)!.args.slice(0, prefix.length), prefix);
  }
  for (const mode of ['wave', 'fixed'] as const) {
    await assert.rejects(createAntigravityCliExecutorPlugin({ commandRunner: async () => ({ stdout: '{}' }) }).execute([message(`agy-${mode}`, { mode })], context, {}), /Standalone Antigravity CLI only supports spawn mode/);
  }
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  await createCodexCliExecutorPlugin({ store, resolveTopicBinding: () => ({ executor: 'codex-cli', mode: 'wave', mailboxId: 'default' }), stdioSpawner: fakeCodex.spawner })
    .execute([message('wave-msg', { mode: 'wave' })], context, {});
  assert.deepEqual(fakeCodex.sessions.at(-1)!.request.args, ['app-server', '--stdio']);
  assert.equal(fakeCodex.sessions.at(-1)!.threadMethod, 'thread/start');
  assert.equal(store.getWakeTarget('default')?.driverConfig?.conversationMode, 'fixed');
  assert.equal(store.getWakeTarget('default')?.driverConfig?.sessionId, 'wave-thread');
  const claudeCalls: CommandRequest[] = [];
  const claudeStore = new MailboxStore(join(root, 'claude-mailbox.json'));
  await createClaudeAppExecutorPlugin({ store: claudeStore, resolveTopicBinding: () => ({ executor: 'claude-app', mode: 'wave', mailboxId: 'default' }), commandRunner: async request => { claudeCalls.push(request); return { stdout: '{}' }; } })
    .execute([message('claude-wave', { mode: 'wave' })], context, {});
  assert.deepEqual(claudeCalls.at(-1)!.args.slice(0, 3), ['send', '--session-mode', 'wave']);
  assert.equal(claudeStore.getWakeTarget('default')?.driverConfig?.conversationMode, 'wave');
  const runtime = await new DealerNodeRuntime({ stateDir: directory(t), gateMode: 'approval_required' }).start();
  assert.equal(runtime.config.gateMode, 'approval_required');
  await runtime.stop();
});

for (const [host, profileName, defaultExecutor, executors] of [
  ['orca', 'orca-dev', 'orca', ['orca', 'orca-spawn', 'orca-wave', 'notify', 'inbox']],
  ['codex', 'codex-dev', 'codex', ['codex-cli', 'codex', 'agent-pty', 'notify', 'inbox']],
  ['claude-code', 'claude-dev', 'claude-code', ['claude-code', 'agent-pty', 'notify', 'inbox']],
  ['antigravity', 'antigravity-dev', 'antigravity', ['antigravity', 'antigravity-cli', 'antigravity-app', 'orca', 'orca-spawn', 'orca-wave', 'agent-pty', 'notify', 'inbox']],
] as const) {
  test(`${profileName} and ${host} patch validate`, () => {
    const profile = getBuiltInProfile(profileName);
    assert.equal(profile.defaultExecutor, defaultExecutor);
    assert.equal(profile.gateMode, 'open');
    assert.deepEqual(profile.policies, ['dedup-cas']);
    assert.deepEqual(profile.executors, executors);
    const patch = JSON.parse(readFileSync(new URL(`../examples/user-patches/${host}.patch.json`, import.meta.url), 'utf8'));
    validateTopicBindings(patch.topicBindings, BUILTIN_EXECUTORS);
    validateTopicBindings(patch.topicBindings, profile.executors);
    assert.equal(patch.defaultExecutor, defaultExecutor);
  });
  for (const live of [false, true]) {
    test(`apply-preset ${host} ${live ? 'RPC' : 'offline'} preserves or sets defaults across modes`, async t => {
      const root = directory(t);
      writeFileSync(join(root, 'config.json'), JSON.stringify({ profile: 'headless-relay', executors: ['notify', 'inbox'], defaultExecutor: 'notify' }));
      let runtime: DealerNodeRuntime | undefined;
      if (live) {
        runtime = await new DealerNodeRuntime({ stateDir: root, transport: { discoveryDir: join(root, 'discovery') }, commandRunner: async () => ({ stdout: '{}' }) }).start();
        t.after(() => runtime!.stop());
        await runtime.serveControl(async argv => {
          const stdout: string[] = [];
          const stderr: string[] = [];
          const code = await runDealerCli(argv, { runtime, stdout: line => stdout.push(line), stderr: line => stderr.push(line) });
          return { code, stdout, stderr };
        });
      }
      let previousDefault = 'notify';
      for (const mode of ['spawn', 'fixed', 'inbox-only'] as const) {
        for (const setDefault of [false, true]) {
          const result = await cli(['setup', 'apply-preset', '--host', host, '--mode', mode,
            '--topic', 'review', ...(host === 'codex' ? [] : ['--agent-profile', 'reviewer']), '--session-id', 'review-session', '--notify-lark',
            ...(setDefault ? ['--set-default'] : [])], root);
          const expected = setDefault ? mode === 'inbox-only' ? 'inbox' : defaultExecutor : previousDefault;
          assert.equal(result.defaultExecutor, expected);
          assert.equal(result.applied, 'review');
          assert.equal(result.binding.notifyLark, true);
          const saved = JSON.parse(readFileSync(join(root, 'config.json'), 'utf8'));
          assert.equal(saved.defaultExecutor, expected);
          assert.deepEqual(saved.topicBindings.review, result.binding);
          assert.ok(saved.executors.includes(result.binding.executor));
          assert.ok(saved.executors.includes(expected));
          if (runtime) assert.equal(runtime.config.defaultExecutor, expected);
          previousDefault = expected;
        }
      }
    });
  }
}

test('live applied preset enables dispatch immediately and changes unbound default without restart', async t => {
  const root = directory(t);
  const fakeCodex = createFakeCodexAppServer();
  const runtime = await new DealerNodeRuntime({ stateDir: root, profile: 'headless-relay', member: 'solo',
    transport: { discoveryDir: join(root, 'discovery') }, stdioSpawner: fakeCodex.spawner,
  }).start();
  t.after(() => runtime.stop());
  await runtime.serveControl(async argv => {
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(argv, { runtime, stdout: line => stdout.push(line), stderr: line => stderr.push(line) });
    return { code, stdout, stderr };
  });
  await cli(['setup', 'apply-preset', '--host', 'codex', '--mode', 'fixed', '--topic', 'review', '--session-id', 'review-session', '--set-default'], root);
  const modulePath = join(root, 'host-api.mjs');
  writeFileSync(modulePath, `export default (_config, { api }) => {
    for (const [factory, name] of [['createCodexExecutorPlugin', 'codex'], ['createClaudeCodeExecutorPlugin', 'claude-code'], ['createAntigravityExecutorPlugin', 'antigravity']]) {
      if (api[factory]().name !== name) throw new Error('Missing named executor API: ' + factory);
    }
    return { name: 'host-api', apply() {} };
  };`);
  await runtime.installUserPlugin({ name: 'host-api', module: modulePath });
  for (const kind of ['review', 'unbound']) {
    const result = await runtime.emitter.emit({ networkId: 'local', kind, routing: { mode: 'unicast', targets: ['member:solo'] }, subject: 'Review', body: 'Check change', payload: {} });
    assert.equal(result.receipts[0].stage, 'dispatched', JSON.stringify(result.receipts));
  }
  await eventually(() => {
    assert.equal(fakeCodex.sessions.length, 2);
    assert.equal(fakeCodex.sessions[0].stdinEnded, true);
    assert.equal(fakeCodex.sessions[1].stdinEnded, true);
  });
  assert.equal(fakeCodex.sessions[0].threadMethod, 'thread/resume');
  assert.equal(fakeCodex.sessions[0].threadId, 'review-session');
  assert.equal(fakeCodex.sessions[1].threadMethod, 'thread/start');
  assert.equal(runtime.mailbox.getWakeTarget('stream:review')?.driver, 'agent-host');
  await runtime.applyAgentPreset({ host: 'claude', conversationMode: 'inbox-only', topic: 'manual', setDefault: true });
  assert.equal(runtime.config.defaultExecutor, 'inbox');
});

test('host-bound passive mailboxes redrive inbox conversation frames after initial execution and restart', async t => {
  for (const [hostAgent, factory, command] of [
    ['codex', createCodexExecutorPlugin, 'codex'],
    ['claude-code', createClaudeCodeExecutorPlugin, 'claude'],
    ['codex', createAgentPtyExecutorPlugin, 'codex'],
  ] as const) {
    const root = directory(t);
    const file = join(root, 'mailbox.json');
    let store = new MailboxStore(file);
    const calls: CommandRequest[] = [];
    let wakeup: WakeupRedriveService;
    let fail = false;
    const fakeCodex = createFakeCodexAppServer({
      onTurnStart: () => (fail ? { status: 'failed', error: { message: 'turn rejected' } } : undefined),
    });
    const commandRunner: CommandRunner = async call => {
      calls.push(call);
      return { stdout: '{}', exitCode: fail ? 2 : 0 };
    };
    wakeup = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner, stdioSpawner: fakeCodex.spawner }) });
    t.after(() => wakeup.stop());
    const plugin = factory({ store, wakeup, commandRunner, stdioSpawner: fakeCodex.spawner, agent: hostAgent, conversationMode: 'fixed',
      cwd: root, sessionId: 'existing-session', ...(hostAgent === 'codex' ? {} : { agentProfile: 'review' }), targetBot: 'bot',
      resolveTopicBinding: () => ({ mailboxId: 'stream:session', executor: hostAgent }) });
    const execReceipt = await plugin.execute([message('initial')], context, {});
    assert.equal(execReceipt.woke, false);
    assert.equal(execReceipt.reason, 'Queued for background wakeup');
    await wakeup.flushMailbox('stream:session');
    await eventually(() => {
      const target = store.getWakeTarget('stream:session');
      assert.equal(target?.pendingWake, false);
      assert.equal(target?.deliveryInDoubt, undefined);
    });
    assert.equal(hostAgent === 'codex' ? fakeCodex.sessions.length : calls.length, 1);
    assert.equal(store.getWakeTarget('stream:session')?.driverConfig?.hostAgent, hostAgent);
    const initial = store.check({ mailboxId: 'stream:session' });
    store.ack(initial.deliveryId!, 'stream:session');
    await wakeup.stop();
    store = new MailboxStore(file);
    wakeup = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner, stdioSpawner: fakeCodex.spawner }) });
    const inbox = createInboxExecutorPlugin({ store, wakeup });
    store.upsertWakeTarget('stream:session', { coalesceMs: 60_000 });
    for (const type of ['reply', 'question', 'settle'] as const) {
      const envelope = { ...message(type), semantic: { type, threadId: 'conversation', targetMailbox: 'stream:session', ...(type === 'settle' ? { outcome: 'succeeded' as const } : {}) } };
      await inbox.execute([envelope], context, {});
      const result = await wakeup.flushMailbox('stream:session');
      assert.equal(result.woke, true);
      assert.equal(result.driver, 'agent-host');
      assert.match(result.pointer!, new RegExp(`Latest \\(${type} from alice, thread conversation\\)`));
      if (hostAgent === 'codex') {
        const session = fakeCodex.sessions.at(-1)!;
        assert.equal(session.request.command, command);
        assert.deepEqual(session.request.args, ['app-server', '--stdio']);
        assert.equal(session.request.cwd, root);
        assert.equal(session.threadMethod, 'thread/resume');
        assert.equal(session.threadId, 'existing-session');
        assert.equal(session.promptText, result.pointer!);
      } else {
        const call = calls.at(-1)!;
        assert.equal(call.command, command);
        assert.equal(call.cwd, root);
        assert.ok(call.args.includes(result.pointer!));
        assert.deepEqual(call.args.slice(0, 3), ['--bg', '--resume', 'existing-session']);
      }
      assert.equal(store.getWakeTarget('stream:session')?.pendingWake, false);
      const lease = store.check({ mailboxId: 'stream:session' });
      store.ack(lease.deliveryId!, 'stream:session');
    }
    assert.equal(hostAgent === 'codex' ? fakeCodex.sessions.length : calls.length, 4);
    fail = true;
    await inbox.execute([{ ...message('failure'), semantic: { type: 'reply', threadId: 'conversation', targetMailbox: 'stream:session' } }], context, {});
    assert.equal((await wakeup.flushMailbox('stream:session')).woke, false);
    const failed = store.getWakeTarget('stream:session')!;
    assert.equal(failed.pendingWake, false);
    assert.equal(failed.deliveryInDoubt, true);
    assert.match(failed.lastWakeError!, hostAgent === 'codex' ? /turn rejected/ : /exited 2/);
    await wakeup.stop();
  }
});

for (const agent of ['codex', 'claude-code'] as const) {
  test(`${agent} spawn transitions to fixed and resumes a leased task with reply preview`, async t => {
    const store = new MailboxStore(join(directory(t), 'mailbox.json'));
    const calls: CommandRequest[] = [];
    const fakeCodex = createFakeCodexAppServer({ nextThreadId: () => 'real-thread-42' });
    const runner: CommandRunner = async call => {
      calls.push(call);
      return { stdout: '{}' };
    };
    const wakeup = new WakeupRedriveService({
      store,
      flushIntervalMs: 100_000,
      drivers: createTestWakeupDrivers({ commandRunner: runner, stdioSpawner: fakeCodex.spawner }),
    });
    t.after(() => wakeup.stop());
    await createAgentPtyExecutorPlugin({ store, wakeup, agent, conversationMode: 'spawn', commandRunner: runner, stdioSpawner: fakeCodex.spawner })
      .execute([message('msg-1')], context, {});
    const mailboxId = 'task:msg-1';
    await wakeup.flushMailbox(mailboxId);
    await eventually(() => {
      const target = store.getWakeTarget(mailboxId);
      assert.equal(target?.pendingWake, false);
      assert.equal(target?.deliveryInDoubt, undefined);
    });
    if (agent === 'codex') {
      assert.equal(fakeCodex.sessions[0].threadMethod, 'thread/start');
    } else {
      assert.deepEqual(calls[0].args.slice(0, 3), ['--bg', '--name', 'task_x3a_msg_x2d_1']);
    }
    assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.conversationMode, 'fixed');
    assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, agent === 'codex' ? 'real-thread-42' : 'task_x3a_msg_x2d_1');
    store.check({ mailboxId });
    const reply = { ...message('reply', { mailboxId, wakeupHint: 'Answer ready' }),
      body: 'Use the smaller design', semantic: { type: 'reply' as const, threadId: 'msg-1', targetMailbox: mailboxId } };
    await createInboxExecutorPlugin({ store }).execute([reply], context, {});
    await wakeup.flushMailbox(mailboxId);
    if (agent === 'codex') {
      assert.equal(fakeCodex.sessions.length, 2);
      assert.equal(fakeCodex.sessions[1].threadMethod, 'thread/resume');
      assert.equal(fakeCodex.sessions[1].threadId, 'real-thread-42');
      assert.match(fakeCodex.sessions[1].promptText!, /\[Answer ready\].*Latest \(reply from alice, thread msg-1\): "Use the smaller design"/);
    } else {
      assert.equal(calls.length, 2);
      assert.deepEqual(calls[1].args.slice(0, 3), ['--bg', '--resume', 'task_x3a_msg_x2d_1']);
      assert.match(calls[1].args.at(-1)!, /\[Answer ready\].*Latest \(reply from alice, thread msg-1\): "Use the smaller design"/);
    }
    assert.equal(store.getWakeTarget(mailboxId)?.pendingWake, false);
  });
}

test('agent executor preserves an in-flight arrival and redrives immediately once wave 1 completes', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  let entered!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  let redriven!: () => void;
  const second = new Promise<void>(resolve => { redriven = resolve; });
  const fakeCodex = createFakeCodexAppServer({
    nextThreadId: () => 'real-race',
    onTurnStart: async (_params, ctx) => {
      if (ctx.session.index === 1) {
        entered();
        await held;
      } else {
        redriven();
      }
    },
  });
  const wakeup = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ stdioSpawner: fakeCodex.spawner }),
  });
  t.after(() => wakeup.stop());
  const receipt = await createCodexExecutorPlugin({ store, wakeup, stdioSpawner: fakeCodex.spawner }).execute([message('race')], context, {});
  assert.equal(receipt.woke, false);
  assert.equal(receipt.reason, 'Queued for background wakeup');
  await started;
  const mailboxId = 'task:race';
  store.check({ mailboxId });
  store.enqueue([{ ...message('new'), body: 'New instruction', semantic: { type: 'reply', threadId: 'race' } }], { mailboxId });
  release();
  await Promise.race([second, new Promise((_, reject) => { const timer = setTimeout(() => reject(new Error('No immediate redrive')), 2000); t.after(() => clearTimeout(timer)); })]);
  await wakeup.flushMailbox(mailboxId);
  await eventually(() => {
    const target = store.getWakeTarget(mailboxId);
    assert.equal(target?.pendingWake, false);
    assert.equal(target?.deliveryInDoubt, undefined);
  });
  assert.equal(fakeCodex.sessions.length, 2);
  assert.equal(fakeCodex.sessions[1].threadMethod, 'thread/resume');
  assert.equal(fakeCodex.sessions[1].threadId, 'real-race');
  assert.match(fakeCodex.sessions[1].promptText!, /New instruction/);
  assert.equal(store.getWakeTarget(mailboxId)?.pendingWake, false);
});

test('ambiguous host launch remains parked across restart and later arrivals', async t => {
  const file = join(directory(t), 'mailbox.json');
  const store = new MailboxStore(file);
  const receipt = await createClaudeCodeExecutorPlugin({ store, commandRunner: async () => {
    throw new Error('accepted then connection lost');
  } }).execute([message('recover')], context, {});
  assert.equal(receipt.woke, false);
  assert.match(String(receipt.reason), /connection lost/);
  assert.equal(store.getWakeTarget('task:recover')?.deliveryInDoubt, true);
  assert.equal(store.getWakeTarget('task:recover')?.pendingWake, false);
  const restarted = new MailboxStore(file);
  const calls: CommandRequest[] = [];
  const wakeup = new WakeupRedriveService({
    store: restarted,
    drivers: createTestWakeupDrivers({
      commandRunner: async (call) => {
        calls.push(call);
        return { stdout: '{}' };
      },
    }),
  });
  t.after(() => wakeup.stop());
  restarted.enqueue([message('late')], { mailboxId: 'task:recover' });
  assert.equal((await wakeup.flushMailbox('task:recover', { force: true })).woke, false);
  await wakeup.flushReady();
  assert.equal(calls.length, 0);
  assert.equal(restarted.getWakeTarget('task:recover')?.deliveryInDoubt, true);
  // A later attempt is refused before the boundary and keeps the original unknown-outcome reason.
  assert.throws(() => restarted.beginWakeDelivery('task:recover'), /requires reconciliation/);
  assert.match(restarted.getWakeTarget('task:recover')!.lastWakeError!, /connection lost/);
});


test('host session keys preserve explicit identities and encode Claude mailbox keys without collisions', () => {
  for (const sessionId of [undefined, '   ', '  explicit._-key  ', 'key:with/slash']) {
    for (const agent of ['codex', 'claude-code', 'chatgpt-dot', 'grok-bot'] as const) {
      const expected = sessionId?.trim() ? sessionId : agent === 'claude-code' ? 'task_x3a_msg_x2d_1' : agent === 'codex' ? undefined : 'task:msg-1';
      assert.equal(resolveAgentSessionId('task:msg-1', sessionId, agent), expected);
      for (const conversationMode of ['spawn', 'fixed'] as const) {
        const options = { agent, conversationMode, mailboxId: 'task:msg-1', sessionId, promptText: 'hello', targetBot: 'bot' };
        if (agent === 'codex' && conversationMode === 'fixed' && !expected) {
          assert.throws(() => buildAgentHostInvocation(options), /explicit sessionId/);
          continue;
        }
        const { args } = buildAgentHostInvocation(options);
        if (agent === 'codex') {
          assert.deepEqual(args, ['app-server', '--stdio']);
          continue;
        }
        const flag = agent === 'claude-code' ? (conversationMode === 'spawn' ? '--name' : '--resume') : '--session-key';
        assert.equal(args[args.indexOf(flag) + 1], expected);
      }
    }
  }
  const mailboxes = ['team/review', 'team:review', 'team-review', 'team_x3a_review', '团队/review', 'team.review'];
  assert.equal(new Set(mailboxes.map(id => resolveAgentSessionId(id))).size, mailboxes.length);
});

test('antigravity without an addressable continuation retains mail without automatic session wakeups', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  let calls = 0;
  const runner: CommandRunner = async () => { calls++; return { stdout: '{"accepted":true}' }; };
  const wakeup = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner }) });
  t.after(() => wakeup.stop());
  await createAgentPtyExecutorPlugin({ agent: 'antigravity', conversationMode: 'spawn', sessionId: 'unbound-existing', store, wakeup, commandRunner: runner }).execute([message('manual')], context, {});
  await wakeup.flushMailbox('task:manual');
  await eventually(() => {
    const t0 = store.getWakeTarget('task:manual');
    assert.equal(t0?.pendingWake, false);
    assert.equal(t0?.deliveryInDoubt, undefined);
  });
  const target = store.getWakeTarget('task:manual')!;
  assert.equal(target.driverConfig?.hostAgent, undefined);
  assert.equal(target.driverConfig?.conversationMode, undefined);
  assert.equal(target.driverConfig?.sessionId, undefined);
  store.enqueue([message('reply')], { mailboxId: 'task:manual' });
  await wakeup.flushMailbox('task:manual');
  assert.equal(calls, 1);
  assert.equal(store.check({ mailboxId: 'task:manual' }).items.length, 2);
});

test('aborted host launch retains its unknown-effect fence without replay', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const controller = new AbortController();
  let calls = 0;
  const runner: CommandRunner = async () => {
    calls++;
    controller.abort();
    throw new DOMException('accepted before abort', 'AbortError');
  };
  const receipt = await createClaudeCodeExecutorPlugin({ store, commandRunner: runner })
    .execute([message('aborted')], { ...context, signal: controller.signal }, {});
  assert.equal(receipt.woke, false);
  assert.match(String(receipt.reason), /accepted before abort/);
  const wakeup = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner }) });
  t.after(() => wakeup.stop());
  await wakeup.flushMailbox('task:aborted', { force: true });
  assert.equal(calls, 1);
  assert.equal(store.getWakeTarget('task:aborted')?.deliveryInDoubt, true);
});

test('command boundary distinguishes absent executables, synchronous validation and pre-abort from post-spawn failures', async t => {
  const root = directory(t);
  await assert.rejects(runCommand({ command: join(root, 'missing'), args: [] }), CommandNotStartedError);
  await assert.rejects(runCommand({ command: '', args: [] }), CommandNotStartedError);
  const before = new AbortController();
  before.abort('before invocation');
  await assert.rejects(runCommand({ command: process.execPath, args: [], signal: before.signal }), CommandNotStartedError);
  await assert.rejects(runCommand({ command: process.execPath, args: ['-e', 'process.exit(2)'] }), error =>
    error instanceof Error && !(error instanceof CommandNotStartedError) && /exited 2/.test(error.message));
  const marker = join(root, 'started');
  const after = new AbortController();
  t.after(() => after.abort());
  const execution = runCommand({ command: process.execPath, args: ['-e',
    'require("node:fs").writeFileSync(process.argv[1], "started"); setInterval(() => {}, 1000)', marker], signal: after.signal });
  const outcome = assert.rejects(execution, error => error instanceof Error && !(error instanceof CommandNotStartedError));
  const deadline = Date.now() + 3000;
  while (!existsSync(marker)) {
    assert.ok(Date.now() < deadline, 'child starts before abort');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  after.abort();
  await outcome;
});

test('initial host ENOENT is retryable and recovery binds the actual Codex receipt', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const policy = createDedupCasPolicyPlugin();
  const input = { ...message('pre-effect', { revision: 1 }), idempotencyKey: 'pre-effect' };
  const executor = createCodexExecutorPlugin({ store, command: join(root, 'missing'), sessionId: 'not-created' });
  const receipt = await policy.process(input, async batch => ({
    stage: 'dispatched', executionReceipt: await executor.execute(batch, context, {}),
  }));
  assert.equal(receipt.executionReceipt?.woke, false);
  assert.match(String(receipt.executionReceipt?.reason), /Command did not start/);
  const duplicate = await policy.process({ ...input, id: 'next-poll' }, async () => {
    assert.fail('A pending durable task must not create a second task on the next poll');
  });
  assert.equal(duplicate.detail, DEDUP_CAS_DUPLICATE_DETAIL);
  const target = store.getWakeTarget('task:pre-effect')!;
  assert.equal(target.deliveryInDoubt, undefined);
  assert.equal(target.pendingWake, true);
  assert.equal(target.attemptCount, 1);
  assert.equal(target.driverConfig?.conversationMode, 'spawn');
  assert.ok(target.earliestWakeAt > Date.now());
  const fakeCodex = createFakeCodexAppServer({ nextThreadId: () => 'host-returned-identity' });
  const service = new WakeupRedriveService({
    store,
    drivers: createTestWakeupDrivers({
      stdioSpawner: fakeCodex.spawner,
    }),
  });
  t.after(() => service.stop());
  await service.flushMailbox('task:pre-effect', { force: true });
  assert.deepEqual(fakeCodex.sessions[0].request.args, ['app-server', '--stdio']);
  assert.equal(fakeCodex.sessions[0].threadMethod, 'thread/start');
  assert.equal(store.getWakeTarget('task:pre-effect')?.driverConfig?.sessionId, 'host-returned-identity');
  assert.equal(store.getWakeTarget('task:pre-effect')?.driverConfig?.conversationMode, 'fixed');
  assert.equal(store.getWakeTarget('task:pre-effect')?.pendingWake, false);
});

test('plugin and agent manifests match package metadata', () => {
  const read = (path: string) => readFileSync(new URL('../' + path, import.meta.url), 'utf8');
  const metadata = JSON.parse(read('package.json'));
  const claude = JSON.parse(read('.claude-plugin/plugin.json'));
  const cursor = JSON.parse(read('.cursor-plugin/plugin.json'));
  const marketplace = JSON.parse(read('.claude-plugin/marketplace.json'));
  assert.equal(metadata.name, 'the-dealer');
  assert.equal(marketplace.name, metadata.name);
  for (const manifest of [claude, cursor, marketplace.plugins[0]]) {
    for (const key of ['name', 'version', 'description', 'license', 'keywords']) {
      assert.deepEqual(manifest[key], metadata[key], key);
    }
  }
  const skills = ['the-dealer', 'the-dealer-setup', 'the-dealer-extend'];
  for (const manifest of [claude, cursor]) {
    assert.deepEqual(manifest.skills, skills.map(skill => './skills/' + skill));
  }
  for (const skill of skills) {
    assert.ok(read('skills/' + skill + '/SKILL.md').trim());
    const agent = read('skills/' + skill + '/agents/openai.yaml');
    for (const field of ['display_name', 'short_description', 'default_prompt']) {
      assert.match(agent, new RegExp('^  ' + field + ': \\S.+$', 'm'));
    }
    assert.ok(agent.includes('$' + skill));
  }
});

test('installed CLI and daemon run from node_modules with clean JSON output', { timeout: 15_000 }, async t => {
  const root = directory(t);
  const installed = join(root, 'node_modules', 'the-dealer');
  mkdirSync(installed, { recursive: true });
  for (const path of ['bin', 'packages', 'package.json']) {
    cpSync(new URL('../' + path, import.meta.url), join(installed, path), { recursive: true });
  }
  const stateDir = join(root, 'state');
  const run = (args: string[]) => promisify(execFile)(join(installed, 'bin/dealer.js'), [...args, '--json'], {
    cwd: root, env: { ...process.env, DEALER_STATE_DIR: stateDir },
  });
  const probe = await run(['setup', 'probe', '--root', root]);
  assert.equal(probe.stderr, '');
  assert.ok(JSON.parse(probe.stdout));

  const daemon = spawn(join(installed, 'bin/dealerd.js'), [
    '--profile', 'headless-relay', '--member', 'installed-user',
  ], { cwd: root, env: { ...process.env, DEALER_STATE_DIR: stateDir }, stdio: ['ignore', 'pipe', 'pipe'] });
  const exited = once(daemon, 'exit');
  const lines = createInterface({ input: daemon.stdout });
  let errors = '';
  daemon.stderr.on('data', chunk => { errors += chunk; });
  t.after(async () => {
    daemon.kill('SIGTERM');
    await exited;
    lines.close();
  });
  const ready = await Promise.race([
    once(lines, 'line').then(([line]) => JSON.parse(line)),
    exited.then(() => { throw new Error('Daemon exited before ready: ' + errors); }),
  ]);
  assert.equal(ready.running, true);
  const status = await run(['node', 'status']);
  assert.equal(status.stderr, '');
  assert.equal(JSON.parse(status.stdout).member, 'installed-user');
  daemon.kill('SIGTERM');
  const [code] = await exited;
  assert.equal(code, 0, errors);
  assert.equal(errors, '');
});

for (const executor of ['codex-cli', 'agent-pty', 'orca-spawn'] as const) {
  test(`mode-less topic binding preserves executor behavior and thread addressing: ${executor}`, async t => {
    const root = directory(t);
    const calls: CommandRequest[] = [];
    const fakeCodex = createFakeCodexAppServer();
    const executorConfig = executor === 'codex-cli' ? { conversationMode: 'fixed', sessionId: 's1' }
      : executor === 'agent-pty' ? { command: 'raw-tool', args: ['--pipe'] }
      : { cwd: root, command: 'codex', noParent: true };
    const runtime = await new DealerNodeRuntime({
      stateDir: join(root, 'state'), member: 'solo',
      transport: { discoveryDir: join(root, 'discovery') },
      patch: { topicBindings: { review: { executor } }, executorConfig: { [executor]: executorConfig } },
      stdioSpawner: fakeCodex.spawner,
      commandRunner: async request => {
        calls.push(request);
        if (request.args[0] === 'worktree' && request.args[1] === 'create')
          return { stdout: JSON.stringify({ ok: true, result: { worktree: { path: root } } }) };
        if (request.args[0] === 'terminal' && request.args[1] === 'create')
          return { stdout: JSON.stringify({ ok: true, result: { terminal: { handle: 'worker' } } }) };
        return { stdout: '{}' };
      },
    }).start();
    t.after(() => runtime.stop());
    const result = await runtime.emitter.emit({ networkId: 'local', kind: 'review',
      routing: { mode: 'unicast', targets: ['member:solo'] }, subject: 'Review', body: 'Check this change', payload: {} });
    assert.equal(result.receipts[0].stage, 'dispatched', JSON.stringify(result.receipts));
    const thread = runtime.mailbox.listThreads()[0];
    assert.ok(thread);
    const inbound = runtime.mailbox.listThreadRecords(thread.threadId).find(record => record.direction === 'inbound');
    assert.ok(inbound);
    assert.equal(inbound.localMailbox, executor === 'orca-spawn' ? `task:${inbound.messageId}` : 'default');
    assert.equal(runtime.resolveTopicBinding('review')?.binding.mode, undefined);
    if (executor === 'codex-cli') {
      await eventually(() => {
    assert.equal(fakeCodex.sessions.length, 1);
    assert.equal(fakeCodex.sessions[0].stdinEnded, true);
  });
      assert.deepEqual(fakeCodex.sessions[0].request.args, ['app-server', '--stdio']);
      assert.equal(fakeCodex.sessions[0].threadMethod, 'thread/resume');
      assert.equal(fakeCodex.sessions[0].threadId, 's1');
      assert.equal(runtime.mailbox.getWakeTarget('default')?.driver, 'agent-host');
    } else if (executor === 'agent-pty') {
      assert.equal(calls.length, 1);
      assert.equal(calls[0].command, 'raw-tool');
      assert.deepEqual(calls[0].args, ['--pipe']);
      assert.match(String(calls[0].input), /Check this change/);
    } else {
      assert.deepEqual(calls.map(call => call.args.slice(0, 2).join(' ')), ['worktree create', 'terminal create']);
      const args = calls[1].args;
      assert.match(args[args.indexOf('--command') + 1], /'codex' 'exec'/);
    }
  });
}

test('payload-selected host owns inbound task addressing and thread replies resume its session', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'state'), member: 'solo',
    transport: { discoveryDir: join(root, 'discovery') },
    patch: { topicBindings: { review: { executor: 'agent-pty' } } },
    commandRunner: async request => { calls.push(request); return { stdout: '{}' }; },
  }).start();
  t.after(() => runtime.stop());
  const initial = await runtime.emitter.emit({ networkId: 'local', kind: 'review',
    routing: { mode: 'unicast', targets: ['member:solo'] }, subject: 'Review', body: 'Check this change',
    payload: { agent: 'claude-code' } });
  assert.equal(initial.receipts[0].stage, 'dispatched');
  const mailboxId = `task:${initial.messageId}`;
  assert.equal(initial.receipts[0].executionReceipt?.mailboxId, mailboxId);
  const inbound = runtime.mailbox.listThreadRecords(initial.messageId).find(record => record.direction === 'inbound');
  assert.equal(inbound?.localMailbox, mailboxId);
  await runtime.wakeups.flushMailbox(mailboxId);
  await eventually(() => {
    const target = runtime.mailbox.getWakeTarget(mailboxId);
    assert.equal(target?.pendingWake, false);
    assert.equal(target?.deliveryInDoubt, undefined);
  });
  assert.equal(calls.length, 1);
  const sessionId = runtime.mailbox.getWakeTarget(mailboxId)?.driverConfig?.sessionId;
  assert.equal(typeof sessionId, 'string');
  assert.deepEqual(calls[0].args.slice(0, 3), ['--bg', '--name', sessionId]);
  const lease = runtime.mailbox.check({ mailboxId });
  runtime.mailbox.ack(lease.deliveryId!, mailboxId);
  const reply = await runtime.emitter.emit({ networkId: 'local', kind: 'review',
    routing: { mode: 'unicast', targets: ['member:solo'] }, subject: 'Reply', body: 'Use the smaller design',
    payload: {}, semantic: { type: 'reply', threadId: initial.messageId } });
  assert.equal(reply.receipts[0].stage, 'dispatched');
  assert.equal(reply.receipts[0].executionReceipt?.mailboxId, mailboxId);
  assert.deepEqual(reply.receipts[0].executionReceipt?.mailboxIds, [mailboxId]);
  await runtime.wakeups.flushMailbox(mailboxId, { force: true });
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[1].args.slice(0, 3), ['--bg', '--resume', sessionId]);
  assert.match(calls[1].args.at(-1)!, /Use the smaller design/);
  assert.equal(runtime.mailbox.listThreadRecords(initial.messageId).find(record => record.direction === 'inbound' && record.messageId === reply.messageId)?.localMailbox, mailboxId);
  assert.equal(runtime.mailbox.list({ mailboxId: 'default' }).queuedCount, 0);
  // Spawn execution owns its task mailbox even when a sender supplied a competing mailbox hint.
  const competing = await runtime.emitter.emit({ networkId: 'local', kind: 'review',
    routing: { mode: 'unicast', targets: ['member:solo'] }, subject: 'Another task', body: 'New work',
    payload: { agent: 'claude-code', mailbox: 'sender-hint' } });
  assert.equal(competing.receipts[0].stage, 'dispatched');
  assert.equal(competing.receipts[0].executionReceipt?.mailboxId, `task:${competing.messageId}`);
  assert.equal(runtime.mailbox.listThreadRecords(competing.messageId).find(record => record.direction === 'inbound')?.localMailbox, `task:${competing.messageId}`);

});

for (const [name, create] of [
  ['codex', createCodexExecutorPlugin],
  ['claude-code', createClaudeCodeExecutorPlugin],
  ['antigravity', createAntigravityExecutorPlugin],
  ['agent-pty', createAgentPtyExecutorPlugin],
] as const) {
  test(`standalone ${name} spawn preserves existing thread and explicit session mailboxes`, async t => {
    const store = new MailboxStore(join(directory(t), 'mailbox.json'));
    const calls: CommandRequest[] = [];
    const fakeCodex = createFakeCodexAppServer({ nextThreadId: () => 'task-session' });
    const commandRunner: CommandRunner = async request => {
      calls.push(request);
      return { stdout: '{"ok":true}' };
    };
    const wakeup = new WakeupRedriveService({
      store, drivers: createTestWakeupDrivers({ commandRunner, stdioSpawner: fakeCodex.spawner, idleCheck: async () => ({ idle: true }) }),
    });
    t.after(() => wakeup.stop());
    const plugin = create({ store, wakeup, commandRunner, stdioSpawner: fakeCodex.spawner, mode: 'spawn', sessionId: 'task-session',
      ...(name === 'agent-pty' ? { agent: 'claude-code' } : {}) });
    const root = {
      ...message('new-task', { mailbox: 'sender-hint' }),
      semantic: { type: 'message', threadId: 'new-task', targetMailbox: 'default' } as const,
    };
    const taskMailbox = 'task:new-task';
    assert.equal((await plugin.execute([root], context, {})).mailboxId, taskMailbox);
    await wakeup.flushMailbox(taskMailbox);
    await eventually(() => {
      const target = store.getWakeTarget(taskMailbox);
      assert.equal(target?.pendingWake, false);
      assert.equal(target?.deliveryInDoubt, undefined);
    });
    assert.equal(store.list({ mailboxId: 'sender-hint' }).totalCount, 0);
    store.recordInboundEnvelope(root, taskMailbox);
    store.ack(store.check({ mailboxId: taskMailbox }).deliveryId!, taskMailbox);
    const taskTarget = store.getWakeTarget(taskMailbox)!;
    const worktreeMailbox = 'worktree:origin';
    const followup = { ...message('followup'), semantic: { type: 'message' as const, threadId: root.id } };
    assert.equal((await plugin.execute([followup], context, {})).mailboxId, taskMailbox);
    await wakeup.flushMailbox(taskMailbox, { force: true });
    assert.deepEqual(store.list({ mailboxId: taskMailbox }).items.map(item => item.envelope.id), ['followup']);
    assert.equal(store.getWakeTarget('task:followup'), undefined);
    if (name === 'antigravity') {
      // The standalone CLI has no continuation and leaves the task mailbox passive.
      assert.equal(taskTarget.driver, 'passive');
      assert.equal(calls.length, 1);
    } else if (name === 'codex') {
      assert.equal(fakeCodex.sessions.length, 2);
      assert.equal(fakeCodex.sessions[1].threadMethod, 'thread/resume');
      assert.equal(fakeCodex.sessions[1].threadId, 'task-session');
    } else {
      assert.equal(calls.length, 2);
      assert.ok(calls[1].args.includes('--resume'));
      assert.ok(calls[1].args.includes('task-session'));
    }
    store.upsertWakeTarget(worktreeMailbox, { driver: 'orca-terminal', driverConfig: { terminalHandle: 'term_origin' } });
    const reply = { ...message('reply'), semantic: { type: 'message' as const, threadId: root.id, targetMailbox: worktreeMailbox } };
    assert.equal((await plugin.execute([reply], context, {})).mailboxId, worktreeMailbox);
    await wakeup.flushMailbox(worktreeMailbox, { force: true });
    assert.deepEqual(store.list({ mailboxId: worktreeMailbox }).items.map(item => item.envelope.id), ['reply']);
    assert.equal(store.getWakeTarget('task:reply'), undefined);
    const last = calls.at(-1)!;
    assert.deepEqual(last.args.slice(0, 2), ['terminal', 'send']);
    assert.equal(last.args[last.args.indexOf('--terminal') + 1], 'term_origin');
  });
}

for (const [name, create] of [
  ['claude-code', createClaudeCodeExecutorPlugin],
  ['agent-pty', createAgentPtyExecutorPlugin],
] as const) {
  for (const freshFirst of [true, false]) {
    test(`standalone ${name} routes a mixed batch with ${freshFirst ? 'the fresh task' : 'the follow-up'} first`, async t => {
      const store = new MailboxStore(join(directory(t), 'mailbox.json'));
      const calls: CommandRequest[] = [];
      const commandRunner: CommandRunner = async request => {
        calls.push(request);
        return { stdout: '{}' };
      };
      const wakeup = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner }) });
      t.after(() => wakeup.stop());
      store.recordInboundEnvelope(message('existing'), 'task:existing');
      store.upsertWakeTarget('task:existing', {
        driver: 'agent-host',
        driverConfig: { hostAgent: 'claude-code', conversationMode: 'fixed', sessionId: 'existing-session' },
      });
      const plugin = create({
        store, wakeup, commandRunner, agent: 'claude-code',
        resolveTopicBinding: (_kind, streamKey) => streamKey === 'new'
          ? { mode: 'spawn', sessionId: 'new-session' }
          : { mode: 'fixed', mailboxId: 'stream:topic', sessionId: 'topic-session' },
      });
      const key = generateNodeKeyPair();
      const freshMessage = message('fresh', { streamKey: 'new', mailbox: 'sender-hint' });
      const fresh = signEnvelope({
        ...freshMessage,
        from: { ...freshMessage.from, nodeId: key.nodeId },
      }, key.privateKeyPem);
      const followup = signEnvelope({
        ...message('followup'),
        from: fresh.from,
        semantic: { type: 'message', threadId: 'existing' },
      }, key.privateKeyPem);
      const batch = freshFirst ? [fresh, followup] : [followup, fresh];
      const receipt = await plugin.execute(batch.map(envelope => ({
        ...envelope, payload: { ...envelope.payload, localAttachments: [] },
      })), { ...context, signedEnvelopes: { fresh, followup } }, {});
      await wakeup.flushMailbox('task:fresh', { force: true });
      await wakeup.flushMailbox('task:existing', { force: true });
      assert.deepEqual(store.list({ mailboxId: 'task:fresh' }).items.map(item => item.signedEnvelope), [fresh]);
      assert.deepEqual(store.list({ mailboxId: 'task:existing' }).items.map(item => item.signedEnvelope), [followup]);
      assert.deepEqual(store.getThread(fresh.id)?.envelope, fresh);
      assert.deepEqual(store.getThread(followup.id)?.envelope, followup);
      assert.equal(receipt.executor, name);
      assert.ok(Array.isArray(receipt.mailboxIds));
      assert.deepEqual(receipt.mailboxIds.toSorted(), ['task:existing', 'task:fresh']);
      assert.ok(Array.isArray(receipt.inserted));
      assert.deepEqual(receipt.inserted.toSorted(), ['followup', 'fresh']);
      assert.equal(store.list({ mailboxId: 'sender-hint' }).totalCount, 0);
      assert.equal(store.list({ mailboxId: 'stream:topic' }).totalCount, 0);
      assert.deepEqual(calls.map(call => call.args.slice(0, 3).join(' ')).sort(), [
        '--bg --name new-session', '--bg --resume existing-session',
      ]);
    });
  }

  test(`standalone ${name} validates the entire batch before handing off messages`, async t => {
    const store = new MailboxStore(join(directory(t), 'mailbox.json'));
    const plugin = create({
      store, agent: 'claude-code', mode: 'spawn',
      commandRunner: async () => assert.fail('An invalid batch must never invoke the host'),
    });
    await assert.rejects(
      plugin.execute([message('fresh'), message('invalid', { mode: 'broken' })], context, {}),
      { message: 'Invalid agent conversationMode', beforeExternalEffect: true },
    );
    assert.equal(store.list().totalCount, 0);
  });
}

for (const [name, create] of [
  ['claude-code', createClaudeCodeExecutorPlugin],
  ['agent-pty', createAgentPtyExecutorPlugin],
  ['orca-spawn', createOrcaSpawnExecutorPlugin],
] as const) {
  for (const rootHasSemantic of [true, false]) {
    const rootType = rootHasSemantic ? 'with an explicit thread' : 'without a semantic header';
    test(`standalone ${name} spawns once per new thread in a mixed batch ${rootType}`, async t => {
      const root = directory(t);
      const store = new MailboxStore(join(root, 'mailbox.json'));
      const calls: CommandRequest[] = [];
      const commandRunner: CommandRunner = async request => {
        calls.push(request);
        if (request.args[0] === 'worktree' && request.args[1] === 'create') {
          return { stdout: JSON.stringify({ ok: true, result: { path: root } }) };
        }
        if (request.args[0] === 'terminal' && request.args[1] === 'create') {
          return { stdout: '{"ok":true,"result":{"terminal":{"handle":"term-new"}}}' };
        }
        return { stdout: '{"ok":true}' };
      };
      const plugin = create({ store, commandRunner, mode: 'spawn', cwd: root, agent: 'claude-code' });
      const newRoot = message('new-root');
      if (rootHasSemantic) newRoot.semantic = { type: 'message', threadId: newRoot.id };
      const followup = { ...message('followup', { executor: ' ' }), semantic: { type: 'message', threadId: newRoot.id } as const };
      const forced = { ...followup, id: 'forced', payload: { executor: name } };
      const unrelated = message('unrelated', { mailbox: 'sender-hint' });
      const receipt = await plugin.execute([followup, unrelated, newRoot, forced], context, {});
      const sharedTask = store.list({ mailboxId: 'task:new-root' }).items;
      assert.deepEqual(sharedTask.sort((a, b) => a.sequence - b.sequence).map(item => item.envelope.id), ['new-root', 'followup']);
      assert.deepEqual(store.list({ mailboxId: 'task:unrelated' }).items.map(item => item.envelope.id), ['unrelated']);
      assert.deepEqual(store.list({ mailboxId: 'task:forced' }).items.map(item => item.envelope.id), ['forced']);
      assert.equal(store.list({ mailboxId: 'sender-hint' }).totalCount, 0);
      assert.equal(calls.filter(call => name === 'orca-spawn'
        ? call.args.slice(0, 2).join(' ') === 'worktree create'
        : call.args.slice(0, 2).join(' ') === '--bg --name').length, 3);
      assert.deepEqual(receipt.envelopeMailboxes, {
        'new-root': 'task:new-root', followup: 'task:new-root', unrelated: 'task:unrelated', forced: 'task:forced',
      });
    });
  }
}

test('runtime records authoritative task mailboxes for every dispatched batch member', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'state'), member: 'solo',
    transport: { discoveryDir: join(root, 'discovery') },
    patch: { topicBindings: { review: { executor: 'agent-pty', agent: 'claude-code', mode: 'spawn' } } },
    commandRunner: async request => { calls.push(request); return { stdout: '{}' }; },
  }).start();
  t.after(() => runtime.stop());
  let pending: MessageEnvelope | undefined;
  runtime.kernel.getService('dispatcher').registerPolicy({
    name: 'batch-two',
    async process(envelope, next) {
      if (pending) return next([pending, envelope]);
      pending = envelope;
      return { stage: 'queued' };
    },
  });
  const send = (hint: string) => runtime.emitter.emit({
    networkId: 'local', kind: 'review', routing: { mode: 'unicast', targets: ['member:solo'] },
    subject: 'Review', body: 'Review this change', payload: { mailbox: hint },
  });
  const first = await send('sender-hint-one');
  assert.equal(first.receipts[0].stage, 'queued');
  const second = await send('sender-hint-two');
  assert.equal(second.receipts[0].stage, 'dispatched', JSON.stringify(second.receipts));
  for (const { messageId } of [first, second]) {
    assert.deepEqual(runtime.mailbox.list({ mailboxId: `task:${messageId}` }).items.map(item => item.envelope.id), [messageId]);
    const inbound = runtime.mailbox.listThreadRecords(messageId).find(record => record.direction === 'inbound');
    assert.equal(inbound?.localMailbox, `task:${messageId}`);
  }
  await eventually(() => assert.equal(calls.filter(call => call.args.slice(0, 2).join(' ') === '--bg --name').length, 2));
});

test('a policy-queued spawn root gains thread ownership at enqueue, before its host launch finishes', { timeout: 10000 }, async t => {
  const root = directory(t);
  const launchStarted = Promise.withResolvers<void>();
  const releaseLaunch = Promise.withResolvers<void>();
  let launches = 0;
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'state'), member: 'solo', transport: { discoveryDir: join(root, 'discovery') },
    patch: { topicBindings: { review: { executor: 'agent-pty', agent: 'claude-code', mode: 'spawn' } } },
    commandRunner: async request => {
      if (request.args[0] === '--bg' && request.args[1] === '--name' && ++launches === 1) {
        launchStarted.resolve();
        await releaseLaunch.promise;
      }
      return { stdout: '{}' };
    },
  }).start();
  t.after(() => runtime.stop());
  const dispatcher = runtime.kernel.getService('dispatcher');
  let pending: MessageEnvelope | undefined;
  dispatcher.registerPolicy({ name: 'hold-root', async process(envelope, next) {
    if (envelope.id === 'queued-root') {
      pending = envelope;
      return { stage: 'queued' };
    }
    const batch = pending ? [pending, envelope] : [envelope];
    pending = undefined;
    return next(batch);
  } });
  const newRoot = { ...message('queued-root', { mailbox: 'sender-hint' }), kind: 'review', networkId: 'local' };
  const followup = { ...newRoot, id: 'batch-followup', semantic: { type: 'message', threadId: newRoot.id } as const };
  const mailboxId = 'task:queued-root';
  try {
    assert.equal((await dispatcher.dispatch(newRoot)).stage, 'queued');
    assert.equal(runtime.mailbox.getThread(newRoot.id)?.localMailbox, 'sender-hint');
    assert.equal(runtime.mailbox.resolveThreadLocalMailbox(newRoot.id, 'local'), undefined);
    assert.equal(runtime.mailbox.resolveReplyTarget(newRoot.id, 'local')?.localMailbox, 'default');
    assert.equal((await dispatcher.dispatch(followup)).stage, 'dispatched');
    await launchStarted.promise;
    const reloaded = new MailboxStore(runtime.mailbox.path);
    assert.equal(reloaded.resolveThreadLocalMailbox(newRoot.id, 'local'), mailboxId);
    assert.equal(reloaded.getThread(newRoot.id)?.mailboxCommitted, true);
    assert.equal((await dispatcher.dispatch({ ...followup, id: 'concurrent-followup' })).executionReceipt?.executor, 'inbox');
    releaseLaunch.resolve();
    await eventually(() => assert.equal(runtime.mailbox.getWakeTarget(mailboxId)?.pendingWake, false));
    assert.equal(launches, 1);
    assert.equal(runtime.mailbox.list({ mailboxId: 'sender-hint' }).totalCount, 0);
    assert.deepEqual(runtime.mailbox.list({ mailboxId }).items.map(item => item.id).sort(), ['batch-followup', 'concurrent-followup', 'queued-root']);
  } finally {
    releaseLaunch.resolve();
  }
});

test('a later Orca batch failure preserves committed receipts and leaves unattempted members retryable', async t => {
  const root = directory(t);
  const launched: string[] = [];
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'state'), member: 'solo', transport: { discoveryDir: join(root, 'discovery') },
    patch: { topicBindings: { review: { executor: 'orca', mode: 'spawn' } } },
    commandRunner: async request => {
      if (request.args[0] === 'worktree' && request.args[1] === 'create') {
        const name = request.args[request.args.indexOf('--name') + 1];
        launched.push(name);
        if (name === 'failing') throw new Error('worktree creation failed');
        return { stdout: JSON.stringify({ ok: true, result: { path: root } }) };
      }
      if (request.args[0] === 'terminal' && request.args[1] === 'create') {
        return { stdout: '{"ok":true,"result":{"terminal":{"handle":"term-batch"}}}' };
      }
      return { stdout: '{"ok":true}' };
    },
  }).start();
  t.after(() => runtime.stop());
  const dispatcher = runtime.kernel.getService('dispatcher');
  const make = (id: string): MessageEnvelope => ({ ...message(id, { worktreeName: id }), kind: 'review', networkId: 'local' });
  const first = make('completed');
  const failed = make('failing');
  const last = make('unattempted');
  const queuedLast = make('previously-queued');
  const queued: MessageEnvelope[] = [];
  let drained = false;
  const published: string[] = [];
  runtime.kernel.ctx.on('dispatcher/inbound', event => {
    if (event.receipt.stage === 'dispatched') published.push(event.envelope.id);
  });
  dispatcher.registerPolicy({ name: 'fail-middle-batch', async process(envelope, next) {
    if (envelope.id !== last.id && !drained) {
      queued.push(envelope);
      return { stage: 'queued' };
    }
    drained = true;
    return next([...queued.splice(0), envelope]);
  } });
  await dispatcher.dispatch(first);
  await dispatcher.dispatch(failed);
  await dispatcher.dispatch(queuedLast);
  await assert.rejects(dispatcher.dispatch(last), (error: any) => error.beforeExternalEffect === true);
  assert.deepEqual(launched, [first.id, failed.id]);
  assert.deepEqual(published, [first.id]);
  assert.equal((await dispatcher.dispatch(first)).executionReceipt?.envelopeMailboxes?.[first.id], 'task:completed');
  assert.equal((await dispatcher.dispatch(failed)).stage, 'failed');
  assert.equal(runtime.mailbox.list({ mailboxId: 'task:completed' }).totalCount, 1);
  assert.equal((await dispatcher.dispatch(last)).executionReceipt?.envelopeMailboxes?.[last.id], 'task:unattempted');
  assert.equal((await dispatcher.dispatch(queuedLast)).executionReceipt?.envelopeMailboxes?.[queuedLast.id], 'task:previously-queued');
  assert.deepEqual(launched, [first.id, failed.id, last.id, queuedLast.id]);
});

test('a mixed raw and host receipt keeps the raw thread unbound', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'state'), member: 'solo', transport: { discoveryDir: join(root, 'discovery') },
    patch: { topicBindings: { review: { executor: 'agent-pty' } } },
    commandRunner: async () => ({ stdout: '{}' }),
  }).start();
  t.after(() => runtime.stop());
  const dispatcher = runtime.kernel.getService('dispatcher');
  const raw = { ...message('raw-thread', { mailbox: 'hint' }), networkId: 'local', kind: 'review' };
  const hosted = { ...message('host-thread', { agent: 'claude-code' }), networkId: 'local', kind: 'review' };
  dispatcher.registerPolicy({ name: 'mix-host-and-raw', async process(envelope, next) {
    return envelope.id === hosted.id ? next([raw, envelope]) : { stage: 'queued' };
  } });
  await dispatcher.dispatch(raw);
  const { executionReceipt: receipt } = await dispatcher.dispatch(hosted);
  assert.deepEqual(receipt?.envelopeMailboxes, { 'host-thread': 'task:host-thread' });
  assert.equal(receipt?.mailboxId, undefined);
  assert.equal(runtime.mailbox.getThread(raw.id)?.localMailbox, 'default');
  assert.equal(runtime.mailbox.resolveThreadLocalMailbox(raw.id, 'local'), undefined);
  assert.equal(runtime.mailbox.resolveThreadLocalMailbox(hosted.id, 'local'), 'task:host-thread');
});

test('mixed interactive and noninteractive Orca spawns report only the committed mailbox', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const plugin = createOrcaSpawnExecutorPlugin({ store, cwd: root, commandRunner: async ({ args }) => ({
    stdout: JSON.stringify({ ok: true, result: args[0] === 'worktree' ? { path: root } : { terminal: { handle: 'term-mixed' } } }),
  }) });
  const receipt = await plugin.execute([message('persistent', { interactive: true }), message('one-off')], context, {});
  assert.deepEqual(receipt.envelopeMailboxes, { persistent: 'task:persistent' });
  assert.equal(receipt.mailboxId, undefined);
  assert.equal(store.resolveThreadLocalMailbox('one-off', 'test'), undefined);
});

for (const targetMailbox of [undefined, 'requests']) {
  test(`agent host launches in the configured spawn mailbox when the semantic target is ${targetMailbox ?? 'omitted'}`, async t => {
    const store = new MailboxStore(join(directory(t), 'mailbox.json'));
    const calls: CommandRequest[] = [];
    const plugin = createClaudeCodeExecutorPlugin({ store, resolveTopicBinding: () => ({ mode: 'spawn', mailboxId: 'requests' }),
      commandRunner: async request => { calls.push(request); return { stdout: '{}' }; },
    });
    const root = { ...message('root'), semantic: { type: 'message', threadId: 'root', ...(targetMailbox ? { targetMailbox } : {}) } as const };
    const receipt = await plugin.execute([root], context, {});
    assert.equal(receipt.mailboxId, 'requests');
    assert.equal(store.getWakeTarget('requests')?.driver, 'agent-host');
    assert.equal(calls.length, 1);
  });
}

for (const [name, create, rootPayload, followupPayload] of [
  ['orca', createOrcaExecutorPlugin, { mode: 'spawn' }, { mode: 'wave' }],
  ['agent-pty', createAgentPtyExecutorPlugin, { agent: 'claude-code' }, {}],
] as const) {
  test(`${name} batches root and follow-up before selecting delivery mode`, async t => {
    const cwd = directory(t);
    const store = new MailboxStore(join(cwd, 'mailbox.json'));
    const calls: CommandRequest[] = [];
    const plugin = create({ store, cwd, commandRunner: async request => {
      calls.push(request);
      return { stdout: JSON.stringify({ ok: true, result: request.args[0] === 'worktree' ? { path: cwd } : { terminal: { handle: 'term-group' } } }) };
    } });
    const root = message('root', rootPayload);
    const followup = { ...message('followup', followupPayload), semantic: { type: 'message', threadId: root.id } as const };
    const receipt = await plugin.execute([followup, root], context, {});
    assert.deepEqual(receipt.envelopeMailboxes, { root: 'task:root', followup: 'task:root' });
    assert.deepEqual(store.list({ mailboxId: 'task:root' }).items.map(item => item.id), ['followup', 'root']);
    assert.equal(calls.filter(call => name === 'orca' ? call.args.slice(0, 2).join(' ') === 'worktree create' : call.args[0] === '--bg').length, 1);
  });
}

test('simultaneous spawn, wave and inbox deliveries keep the first root’s worktree', { timeout: 8000 }, async t => {
  const cwd = directory(t);
  const store = new MailboxStore(join(cwd, 'mailbox.json'));
  const beganCreate = Promise.withResolvers<void>();
  const finishCreate = Promise.withResolvers<void>();
  let creates = 0;
  const plugin = createOrcaExecutorPlugin({ store, cwd, mode: 'spawn', commandRunner: async ({ args }) => {
    if (args[0] === 'worktree' && args[1] === 'create') {
      if (++creates === 1) {
        beganCreate.resolve();
        await finishCreate.promise;
      }
      return { stdout: JSON.stringify({ ok: true, result: { path: cwd } }) };
    }
    return { stdout: '{"ok":true,"result":{"terminal":{"handle":"term-concurrent"}}}' };
  } });
  const root = message('root');
  const followup = { ...message('followup'), semantic: { type: 'message', threadId: root.id } as const };
  const wave = { ...message('wave-followup'), semantic: followup.semantic };
  const inbox = { ...message('inbox-followup'), semantic: followup.semantic };
  const initial = plugin.execute([root], context, {});
  const concurrent = plugin.execute([followup], context, {});
  const concurrentWave = createOrcaWaveExecutorPlugin({ store }).execute([wave], context, {});
  const concurrentInbox = createInboxExecutorPlugin({ store }).execute([inbox], context, {});
  try {
    await beganCreate.promise;
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(creates, 1);
    assert.equal(store.resolveThreadLocalMailbox(root.id, root.networkId), undefined);
  } finally {
    finishCreate.resolve();
    await Promise.all([initial, concurrent, concurrentWave, concurrentInbox]);
  }
  assert.equal((await concurrent).mailboxId, 'task:root');
  assert.equal((await concurrentWave).mailboxId, 'task:root');
  assert.equal((await concurrentInbox).mailboxId, 'task:root');
  assert.equal(creates, 1);
  assert.equal(store.list({ mailboxId: 'task:root' }).totalCount, 4);
});

test('runtime reserves a spawn thread before fetching the root’s attachment', { timeout: 8000 }, async t => {
  const root = directory(t);
  const attachmentStarted = Promise.withResolvers<void>();
  const releaseAttachment = Promise.withResolvers<void>();
  let creates = 0;
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'state'), member: 'solo', transport: { discoveryDir: join(root, 'discovery') },
    patch: { topicBindings: { review: { executor: 'orca', mode: 'spawn' } } },
    commandRunner: async ({ args }) => {
      if (args.slice(0, 2).join(' ') === 'worktree create') {
        creates++;
        return { stdout: JSON.stringify({ ok: true, result: { path: root } }) };
      }
      return { stdout: '{"ok":true,"result":{"terminal":{"handle":"term-attachment"}}}' };
    },
  }).start();
  t.after(() => runtime.stop());
  runtime.transport.fetchBlobToPath = async (_ticket, path) => {
    attachmentStarted.resolve();
    await releaseAttachment.promise;
    writeFileSync(path, 'content');
  };
  const rootEnvelope: MessageEnvelope = {
    ...message('attachment-root'), networkId: 'local', kind: 'review',
    attachments: [{ name: 'context.txt', mimeType: 'text/plain', sizeBytes: 7, blake3Hash: 'a'.repeat(64), ticket: 'test-ticket' }],
  };
  const followup: MessageEnvelope = {
    ...rootEnvelope, id: 'attachment-followup', attachments: [],
    semantic: { type: 'message', threadId: rootEnvelope.id },
  };
  const dispatcher = runtime.kernel.getService('dispatcher');
  const initial = dispatcher.dispatch(rootEnvelope);
  await attachmentStarted.promise;
  const concurrent = dispatcher.dispatch(followup);
  try {
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(creates, 0);
    assert.equal(runtime.mailbox.resolveThreadLocalMailbox(rootEnvelope.id, rootEnvelope.networkId), undefined);
  } finally {
    releaseAttachment.resolve();
    await Promise.all([initial, concurrent]);
  }
  const mailboxId = 'task:attachment-root';
  assert.equal(creates, 1);
  assert.equal((await initial).executionReceipt?.mailboxId, mailboxId);
  assert.equal((await concurrent).executionReceipt?.mailboxId, mailboxId);
  assert.deepEqual(runtime.mailbox.list({ mailboxId }).items.map(item => item.id).sort(), [followup.id, rootEnvelope.id]);
});

test('runtime selects inbox after waiting for an explicitly selected Orca root', { timeout: 8000 }, async t => {
  const root = directory(t);
  const createStarted = Promise.withResolvers<void>();
  const releaseCreate = Promise.withResolvers<void>();
  const calls: CommandRequest[] = [];
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'state'), member: 'solo', transport: { discoveryDir: join(root, 'discovery') },
    patch: { defaultExecutor: 'notify' },
    commandRunner: async request => {
      calls.push(request);
      if (request.args.slice(0, 2).join(' ') === 'worktree create') {
        createStarted.resolve();
        await releaseCreate.promise;
        return { stdout: JSON.stringify({ ok: true, result: { path: root } }) };
      }
      return { stdout: '{"ok":true,"result":{"terminal":{"handle":"term-root-owner"}}}' };
    },
  }).start();
  t.after(() => runtime.stop());
  const initial: MessageEnvelope = { ...message('explicit-root', { executor: 'orca', mode: 'spawn' }), networkId: 'local', kind: 'review' };
  const followup: MessageEnvelope = {
    ...initial, id: 'unforced-followup', payload: {}, semantic: { type: 'message', threadId: initial.id },
  };
  const dispatcher = runtime.kernel.getService('dispatcher');
  const rootDelivery = dispatcher.dispatch(initial);
  await createStarted.promise;
  assert.equal(runtime.resolveExecutorName(followup), 'notify');
  const followupDelivery = dispatcher.dispatch(followup);
  try {
    await new Promise(resolve => setImmediate(resolve));
  } finally {
    releaseCreate.resolve();
  }
  const [first, second] = await Promise.all([rootDelivery, followupDelivery]);
  assert.equal(first.executionReceipt?.executor, 'orca');
  assert.equal(second.executionReceipt?.executor, 'inbox');
  assert.equal(second.executionReceipt?.mailboxId, 'task:explicit-root');
  assert.deepEqual(runtime.mailbox.list({ mailboxId: 'task:explicit-root' }).items.map(item => item.id), [followup.id, initial.id]);
  assert.ok(calls.every(call => call.command !== 'osascript'));
});

test('runtime reports a dedup watermark write failure without replaying a confirmed delivery', async t => {
  const root = directory(t);
  const stateDir = join(root, 'state');
  const watermarksPath = join(stateDir, 'dedup-watermarks.json');
  const pendingWatermarksPath = join(stateDir, 'pending-watermarks.json');
  let calls = 0;
  const runtime = await new DealerNodeRuntime({
    stateDir, member: 'solo', transport: { discoveryDir: join(root, 'discovery') },
    patch: { topicBindings: { review: { executor: 'agent-pty' } } },
    commandRunner: async () => {
      calls++;
      renameSync(watermarksPath, pendingWatermarksPath);
      mkdirSync(watermarksPath);
      return { stdout: '{}' };
    },
  }).start();
  t.after(() => runtime.stop());
  const envelope = { ...message('watermark-failure', { revision: 1 }), kind: 'review', networkId: 'local', idempotencyKey: 'review-1' };
  const dispatcher = runtime.kernel.getService('dispatcher');
  const receipt = await dispatcher.dispatch(envelope);
  assert.equal(receipt.stage, 'dispatched');
  assert.deepEqual(await dispatcher.dispatch(envelope), receipt);
  assert.equal(calls, 1);
  assert.ok(runtime.lastError?.includes(envelope.networkId));
  assert.ok(runtime.lastError?.includes(envelope.from.nodeId));
  assert.ok(runtime.lastError?.includes(envelope.id));
  assert.match(runtime.lastError ?? '', /EISDIR|ENOTDIR/);
  assert.match(readFileSync(pendingWatermarksPath, 'utf8'), /"status":"unknown"/);
});

test('concurrent host preparations commit to one task before launching', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const calls: CommandRequest[] = [];
  const plugin = createClaudeCodeExecutorPlugin({ store, commandRunner: async request => {
    calls.push(request);
    return { stdout: '{}' };
  } });
  const root = message('root');
  const followup = { ...message('followup'), semantic: { type: 'message', threadId: root.id } as const };
  const receipts = await Promise.all([plugin.execute([root], context, {}), plugin.execute([followup], context, {})]);
  assert.deepEqual(receipts.map(receipt => receipt.mailboxId), ['task:root', 'task:root']);
  assert.equal(calls.length, 1);
  assert.equal(store.list({ mailboxId: 'task:root' }).totalCount, 2);
});

for (const reverse of [false, true]) {
  test(`unified Orca routes spawn, wave, fixed and inbox-only in ${reverse ? 'reverse' : 'forward'} batch order`, async t => {
    const root = directory(t);
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const bindings = {
      spawn: { mode: 'spawn', workspacePath: root },
      wave: { mode: 'wave', workspacePath: root, rotateAfterWaves: 2 },
      fixed: { mode: 'fixed', mailboxId: 'stream:fixed', terminalHandle: 'term-fixed' },
      inbox: { mode: 'inbox-only', mailboxId: 'review-inbox' },
    } as const;
    let creates = 0;
    const plugin = createOrcaExecutorPlugin({ store, resolveTopicBinding: kind => bindings[kind as keyof typeof bindings],
      commandRunner: async ({ args }) => {
        if (args[0] === 'worktree' && args[1] === 'create') creates += 1;
        return { stdout: JSON.stringify({ ok: true, result: args[0] === 'worktree' ? { path: root } : { terminal: { handle: 'term-spawn' } } }) };
      },
    });
    const batch = Object.keys(bindings).map(kind => ({ ...message(kind), kind }));
    const receipt = await plugin.execute(reverse ? batch.reverse() : batch, context, {});
    assert.deepEqual(receipt.envelopeMailboxes, { spawn: 'task:spawn', wave: 'stream:wave', fixed: 'stream:fixed', inbox: 'review-inbox' });
    assert.equal(receipt.mailboxId, undefined);
    assert.equal(creates, 1);
    assert.equal(store.getWakeTarget('stream:wave')?.rotateAfterWaves, 2);
    assert.equal(store.getWakeTarget('stream:fixed')?.driverConfig?.terminalHandle, 'term-fixed');
    assert.equal(store.getWakeTarget('stream:fixed')?.rotateAfterWaves, undefined);
    assert.equal(store.list({ mailboxId: 'review-inbox' }).totalCount, 1);
  });
}

test('mailbox addressing fallback accepts valid payload hints and ignores malformed hints', () => {
  const binding = { executor: 'agent-pty' };
  assert.equal(toMailboxTopicBinding(binding, {}, { agent: 'claude-code' }).mode, 'spawn');
  assert.equal(toMailboxTopicBinding(binding, {}, { agent: 'claude-code', conversationMode: 'wave' }).mode, 'wave');
  assert.equal(toMailboxTopicBinding(binding, {}, { agent: {}, mode: 'invalid' }).mode, 'inbox-only');
  assert.equal(toMailboxTopicBinding(binding, {}, { mode: 'wave', conversationMode: 'fixed' }).mode, 'inbox-only');
});

test('queued thread addressing uses payload hints without validating execution prematurely', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'state'), member: 'solo',
    transport: { discoveryDir: join(root, 'discovery') },
    patch: { topicBindings: { review: { executor: 'agent-pty' } } },
    commandRunner: async () => { assert.fail('queued messages must not execute'); },
  }).start();
  t.after(() => runtime.stop());
  runtime.kernel.getService('dispatcher').registerPolicy({ name: 'hold-payload-validation', async process() { return { stage: 'queued' }; } });
  for (const [payload, expected] of [
    [{ agent: 'claude-code' }, 'task'],
    [{ agent: 'claude-code', mode: 'wave' }, 'stream:review'],
    [{ agent: {}, mode: 'invalid' }, 'default'],
  ] as const) {
    const result = await runtime.emitter.emit({ networkId: 'local', kind: 'review',
      routing: { mode: 'unicast', targets: ['member:solo'] }, subject: 'Queued', body: 'Wait', payload });
    assert.equal(result.receipts[0].stage, 'queued');
    const inbound = runtime.mailbox.listThreadRecords(result.messageId).find(record => record.direction === 'inbound');
    assert.equal(inbound?.localMailbox, expected === 'task' ? `task:${result.messageId}` : expected);
  }
});

test('dealer peers prune works through runDealerCli including live daemon RPC', async t => {
  const root = directory(t);
  const stateDir = join(root, 'state');
  const runtime = new DealerNodeRuntime({
    stateDir,
    member: 'ben',
    name: 'local',
    transport: { discoveryDir: join(root, 'discovery') },
  });
  await runtime.start();
  t.after(() => runtime.stop());
  await runtime.mountNetwork({ networkId: 'team', passphrase: 'secret' });
  await runtime.serveControl(async argv => {
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(argv, { runtime, stdout: line => stdout.push(line), stderr: line => stderr.push(line) });
    return { code, stdout, stderr };
  });

  const makeCard = (member: string, nodeName: string, updatedAt: string) => {
    const keys = generateNodeKeyPair();
    return signNodeCard({
      nodeId: keys.nodeId,
      networkId: 'team',
      revision: 1,
      identity: { member, nodeName },
      admissionMode: 'open',
      capabilities: { summary: '', skills: [] },
      runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] },
      updatedAt,
    }, keys.privateKeyPem);
  };
  const liveRelay = makeCard('relay', 'us-master', '2024-01-01T00:00:00.000Z');
  const staleGhost = makeCard('diz', 'old-mac', '2026-01-02T00:00:00.000Z');
  for (const peerCard of [liveRelay, staleGhost]) {
    runtime.kernel.ctx.emit('transport/inbound', {
      networkId: 'team',
      senderNodeId: peerCard.nodeId,
      mode: 'unicast',
      payload: { type: 'peer.card', card: peerCard },
    });
  }
  t.mock.method(runtime.transport, 'isPeerReachable', (networkId: string, nodeId: string) =>
    networkId === 'team' && nodeId === liveRelay.nodeId,
  );

  // Reject unscoped prune via live daemon RPC.
  const errLines: string[] = [];
  assert.equal(await runDealerCli(['peers', 'prune', '--network', 'team', '--json'], {
    stateDir,
    stdout: () => {},
    stderr: line => errLines.push(line),
  }), 1);
  assert.match(errLines.join(''), /nodeId|unreachable/);

  // Prune unreachable peers via live daemon RPC.
  const pruned = await cli(['peers', 'prune', '--network', 'team', '--unreachable'], stateDir);
  assert.deepEqual(pruned, { removedCount: 1, removedNodeIds: [staleGhost.nodeId] });
  const remaining = await cli(['peers', 'list', '--network', 'team'], stateDir);
  assert.deepEqual(remaining.map((entry: { nodeId: string }) => entry.nodeId), [liveRelay.nodeId]);
  const persisted = JSON.parse(readFileSync(join(stateDir, 'peers.json'), 'utf8'));
  assert.deepEqual(persisted.cards.map((entry: { nodeId: string }) => entry.nodeId), [liveRelay.nodeId]);

  // Direct in-process prune by explicit --node.
  const directOut: string[] = [];
  assert.equal(await runDealerCli(['peers', 'prune', '--network', 'team', '--node', liveRelay.nodeId, '--json'], {
    runtime,
    stdout: line => directOut.push(line),
  }), 0);
  assert.deepEqual(JSON.parse(directOut.join('')), { removedCount: 1, removedNodeIds: [liveRelay.nodeId] });
});

test('mixed-capability routing prefers reachable eligible peers without breaking offline skill outbox queuing', async t => {
  const root = directory(t);
  const runtime = new DealerNodeRuntime({
    stateDir: join(root, 'state'),
    member: 'ben',
    name: 'local',
    transport: { discoveryDir: join(root, 'discovery'), multicast: false },
  });
  await runtime.start();
  t.after(() => runtime.stop());
  await runtime.mountNetwork({ networkId: 'team', passphrase: 'secret' });

  const makeSkillCard = (member: string, nodeName: string, skillId: string) => {
    const keys = generateNodeKeyPair();
    return signNodeCard({
      nodeId: keys.nodeId,
      networkId: 'team',
      revision: 1,
      identity: { member, nodeName },
      admissionMode: 'open',
      capabilities: { summary: skillId, skills: [{ id: skillId, description: skillId }] },
      runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] },
      updatedAt: '2026-01-02T00:00:00.000Z',
    }, keys.privateKeyPem);
  };
  const offlineRelease = makeSkillCard('alice', 'release-box', 'release');
  const liveTriage = makeSkillCard('bob', 'live-triage', 'triage');
  const staleTriage = makeSkillCard('ghost', 'stale-triage', 'triage');
  for (const peerCard of [offlineRelease, liveTriage, staleTriage]) {
    runtime.kernel.ctx.emit('transport/inbound', {
      networkId: 'team',
      senderNodeId: peerCard.nodeId,
      mode: 'unicast',
      payload: { type: 'peer.card', card: peerCard },
    });
  }

  const reachable = new Set<string>([liveTriage.nodeId]);
  t.mock.method(runtime.transport, 'isPeerReachable', (networkId: string, nodeId: string) =>
    networkId === 'team' && reachable.has(nodeId),
  );

  // 1. Offline specialist for skill:release must still route and durably queue in outbox even when unrelated liveTriage is online.
  const emitted = await runtime.emitter.emit({
    networkId: 'team',
    routing: { mode: 'auto', targets: ['skill:release'] },
    kind: 'collab.request.v1',
    subject: 'Ship release',
    body: 'Queue for offline release specialist',
    payload: {},
  });
  assert.equal(emitted.receipts.length, 1);
  assert.equal(emitted.receipts[0].stage, 'queued');
  assert.equal(emitted.receipts[0].executionReceipt?.executor, 'outbox');
  const queued = runtime.listOutbox({ networkId: 'team', status: 'queued' });
  assert.equal(queued.length, 1);
  assert.deepEqual(queued[0].pendingNodeIds, [offlineRelease.nodeId]);

  // 2. When multiple peers match skill:triage, reachable liveTriage is chosen over staleTriage without multicast fan-out.
  const triageEnvelope = signEnvelope({
    id: 'msg-triage-1',
    traceId: 'trace-triage-1',
    networkId: 'team',
    from: { nodeId: runtime.transport.localNodeId, member: 'ben', nodeName: 'local' },
    routing: { mode: 'auto', targets: ['skill:triage'] },
    kind: 'collab.request.v1',
    priority: 'normal',
    subject: 'Triage',
    body: 'Prefer live triage peer',
    payload: {},
    attachments: [],
    createdAt: new Date().toISOString(),
  }, runtime.node.privateKeyPem);
  const liveDecision = await runtime.kernel.ctx.services.router.route(triageEnvelope);
  assert.equal(liveDecision.mode, 'unicast');
  assert.deepEqual(liveDecision.targetNodeIds, [liveTriage.nodeId]);

  // 3. When all matching triage peers are offline, fallback preserves all eligible offline matches.
  reachable.clear();
  const offlineDecision = await runtime.kernel.ctx.services.router.route(triageEnvelope);
  assert.equal(offlineDecision.mode, 'multicast');
  assert.deepEqual(new Set(offlineDecision.targetNodeIds), new Set([liveTriage.nodeId, staleTriage.nodeId]));
});

test('codex wave starts via app-server thread/start and resumes via thread/resume', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const fakeCodex = createFakeCodexAppServer({
    nextThreadId: () => '019d-app-server-thread',
  });
  const plugin = createCodexExecutorPlugin({
    store,
    stdioSpawner: fakeCodex.spawner,
    resolveTopicBinding: () => ({
      executor: 'codex',
      mode: 'wave',
      mailboxId: 'stream:codex-inbox',
      workspacePath: root,
    }),
  });

  // Wave 1: starts fresh via app-server thread/start directly in workspacePath and captures thread.id.
  await plugin.execute([message('w1')], context, {});
  assert.equal(fakeCodex.sessions.length, 1);
  assert.deepEqual(fakeCodex.sessions[0].request.args, ['app-server', '--stdio']);
  assert.equal(fakeCodex.sessions[0].request.cwd, root);
  assert.equal(fakeCodex.sessions[0].threadMethod, 'thread/start');
  assert.deepEqual(fakeCodex.sessions[0].threadParams, {
    cwd: root,
    approvalPolicy: 'never',
    sandbox: 'danger-full-access',
    threadSource: 'user',
  });
  assert.equal(fakeCodex.sessions[0].threadName, 'Dealer · codex-inbox');
  assert.equal(fakeCodex.sessions[0].stdinEnded, true);
  const afterWave1 = store.getWakeTarget('stream:codex-inbox')!;
  assert.equal(afterWave1.driverConfig?.sessionId, '019d-app-server-thread');
  assert.equal(typeof afterWave1.driverConfig?.sessionStartedAt, 'number');
  const lease1 = store.check({ mailboxId: 'stream:codex-inbox' });
  store.ack(lease1.deliveryId!, 'stream:codex-inbox');

  // Wave 2: omits sessionId in binding so persisted thread.id is preserved and resumed via thread/resume.
  await plugin.execute([message('w2')], context, {});
  assert.equal(fakeCodex.sessions.length, 2);
  assert.equal(fakeCodex.sessions[1].threadMethod, 'thread/resume');
  assert.deepEqual(fakeCodex.sessions[1].threadParams, {
    threadId: '019d-app-server-thread',
    cwd: root,
    approvalPolicy: 'never',
    sandbox: 'danger-full-access',
    excludeTurns: true,
  });
  assert.equal(store.getWakeTarget('stream:codex-inbox')?.driverConfig?.sessionId, '019d-app-server-thread');

  // Continuation parser handles both single JSON objects and JSONL with threadId/thread_id/sessionId/session_id.
  assert.equal(
    resolveAgentHostContinuation('codex', 'stream:codex-inbox', undefined, '{"session_id":"sid-1"}', 'wave').sessionId,
    'sid-1',
  );
  assert.equal(
    resolveAgentHostContinuation('codex', 'stream:codex-inbox', undefined, '{"type":"init"}\n{"threadId":"tid-2"}', 'spawn').sessionId,
    'tid-2',
  );
});

test('agent-host rotateAfterWaves rotates codex wave after acked threshold and never rotates mid-lease', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const fakeCodex = createFakeCodexAppServer({
    nextThreadId: count => `thread-${count}`,
  });
  const plugin = createCodexExecutorPlugin({
    store,
    stdioSpawner: fakeCodex.spawner,
    resolveTopicBinding: () => ({
      executor: 'codex',
      mode: 'wave',
      mailboxId: 'stream:rot-waves',
      workspacePath: root,
      wakeupPrefix: 'Check inbox carefully',
      coalesceMs: 1_500,
      rotateAfterWaves: 2,
    }),
  });

  // Wave 1: starts thread-1 and propagates wakeTarget metadata.
  await plugin.execute([message('rw1')], context, {});
  const t1 = store.getWakeTarget('stream:rot-waves')!;
  assert.equal(t1.wakeupPrefix, 'Check inbox carefully');
  assert.equal(t1.coalesceMs, 1_500);
  assert.equal(t1.rotateAfterWaves, 2);
  assert.equal(t1.driverConfig?.sessionId, 'thread-1');
  assert.equal(t1.completedWaves, 0);
  const startedAt1 = t1.driverConfig?.sessionStartedAt as number;
  assert.equal(typeof startedAt1, 'number');
  store.ack(store.check({ mailboxId: 'stream:rot-waves' }).deliveryId!, 'stream:rot-waves');
  assert.equal(store.getWakeTarget('stream:rot-waves')?.completedWaves, 1);

  // Wave 2: completedWaves (1) < rotateAfterWaves (2), so reuses thread-1 and preserves sessionStartedAt.
  await plugin.execute([message('rw2')], context, {});
  assert.equal(fakeCodex.sessions[1].threadMethod, 'thread/resume');
  assert.equal(fakeCodex.sessions[1].threadId, 'thread-1');
  assert.equal(store.getWakeTarget('stream:rot-waves')?.driverConfig?.sessionStartedAt, startedAt1);

  // Lease wave 2, artificially raise completedWaves >= rotateAfterWaves while lease is outstanding:
  // mid-lease redrive MUST NOT rotate!
  const lease2 = store.check({ mailboxId: 'stream:rot-waves' });
  store.upsertWakeTarget('stream:rot-waves', { completedWaves: 2 });
  store.enqueue([message('rw2-followup')], { mailboxId: 'stream:rot-waves' });
  await deliverAgentHostWakeTarget(store, runCommand, 'stream:rot-waves', { now: startedAt1 + 10, stdioSpawner: fakeCodex.spawner });
  assert.equal(fakeCodex.sessions[2].threadMethod, 'thread/resume');
  assert.equal(fakeCodex.sessions[2].threadId, 'thread-1');
  assert.equal(store.getWakeTarget('stream:rot-waves')?.driverConfig?.sessionId, 'thread-1');

  // Ack lease 2 -> completedWaves becomes 3 (>= 2) and no outstanding lease remains.
  store.ack(lease2.deliveryId!, 'stream:rot-waves');

  // Wave 3: rotates to a fresh thread-2 via thread/start, resets completedWaves to 0, refreshes sessionStartedAt.
  const rotateNow = startedAt1 + 5_000;
  await deliverAgentHostWakeTarget(store, runCommand, 'stream:rot-waves', { now: rotateNow, stdioSpawner: fakeCodex.spawner });
  assert.equal(fakeCodex.sessions[3].threadMethod, 'thread/start');
  const rotated = store.getWakeTarget('stream:rot-waves')!;
  assert.equal(rotated.driverConfig?.sessionId, 'thread-2');
  assert.equal(rotated.driverConfig?.sessionStartedAt, rotateNow);
  assert.equal(rotated.completedWaves, 0);
});

test('agent-host rotateAfterMs rotates codex and claude-code waves when session age expires', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const fakeCodex = createFakeCodexAppServer({
    nextThreadId: count => `age-thread-${count}`,
  });
  const plugin = createCodexExecutorPlugin({
    store,
    stdioSpawner: fakeCodex.spawner,
    resolveTopicBinding: () => ({
      executor: 'codex',
      mode: 'wave',
      mailboxId: 'stream:rot-age',
      workspacePath: root,
      rotateAfterMs: 60_000,
    }),
  });

  await plugin.execute([message('age-1')], context, {});
  const t1 = store.getWakeTarget('stream:rot-age')!;
  assert.equal(t1.driverConfig?.rotateAfterMs, 60_000);
  assert.equal(t1.driverConfig?.sessionId, 'age-thread-1');
  const startAt = t1.driverConfig?.sessionStartedAt as number;
  store.ack(store.check({ mailboxId: 'stream:rot-age' }).deliveryId!, 'stream:rot-age');

  // Before rotateAfterMs elapses: still reuses age-thread-1.
  store.enqueue([message('age-2')], { mailboxId: 'stream:rot-age' });
  await deliverAgentHostWakeTarget(store, runCommand, 'stream:rot-age', { now: startAt + 30_000, stdioSpawner: fakeCodex.spawner });
  assert.equal(fakeCodex.sessions[1].threadMethod, 'thread/resume');
  assert.equal(fakeCodex.sessions[1].threadId, 'age-thread-1');
  assert.equal(store.getWakeTarget('stream:rot-age')?.driverConfig?.sessionStartedAt, startAt);

  // While lease is open past rotateAfterMs: still does not rotate mid-lease.
  const lease2 = store.check({ mailboxId: 'stream:rot-age' });
  store.enqueue([message('age-2b')], { mailboxId: 'stream:rot-age' });
  await deliverAgentHostWakeTarget(store, runCommand, 'stream:rot-age', { now: startAt + 120_000, stdioSpawner: fakeCodex.spawner });
  assert.equal(fakeCodex.sessions[2].threadMethod, 'thread/resume');
  assert.equal(fakeCodex.sessions[2].threadId, 'age-thread-1');
  store.ack(lease2.deliveryId!, 'stream:rot-age');

  // After ack and past rotateAfterMs: rotates to age-thread-2 and resets completedWaves & sessionStartedAt.
  const rotateAt = startAt + 125_000;
  await deliverAgentHostWakeTarget(store, runCommand, 'stream:rot-age', { now: rotateAt, stdioSpawner: fakeCodex.spawner });
  assert.equal(fakeCodex.sessions[3].threadMethod, 'thread/start');
  const rotated = store.getWakeTarget('stream:rot-age')!;
  assert.equal(rotated.driverConfig?.sessionId, 'age-thread-2');
  assert.equal(rotated.driverConfig?.rotateAfterMs, 60_000);
  assert.equal(rotated.driverConfig?.sessionStartedAt, rotateAt);
  assert.equal(rotated.completedWaves, 0);

  // Claude Code wave rotation generates a fresh non-colliding session name and resumes it on subsequent waves.
  const claudeCalls: CommandRequest[] = [];
  const claudeRunner: CommandRunner = async request => {
    claudeCalls.push(request);
    return { stdout: '{}' };
  };
  const claudePlugin = createClaudeCodeExecutorPlugin({
    store,
    commandRunner: claudeRunner,
    resolveTopicBinding: () => ({
      executor: 'claude-code',
      mode: 'wave',
      mailboxId: 'stream:claude-rot',
      workspacePath: root,
      rotateAfterMs: 10_000,
    }),
  });
  await claudePlugin.execute([message('c1')], context, {});
  assert.deepEqual(claudeCalls[0].args.slice(0, 4), ['--bg', '--name', 'stream_x3a_claude_x2d_rot', '--dangerously-skip-permissions']);
  const cStart = store.getWakeTarget('stream:claude-rot')!.driverConfig!.sessionStartedAt as number;
  store.ack(store.check({ mailboxId: 'stream:claude-rot' }).deliveryId!, 'stream:claude-rot');

  store.enqueue([message('c2')], { mailboxId: 'stream:claude-rot' });
  const cRotateAt = cStart + 20_000;
  await deliverAgentHostWakeTarget(store, claudeRunner, 'stream:claude-rot', { now: cRotateAt });
  const expectedFreshName = `stream_x3a_claude_x2d_rot_r${cRotateAt}`;
  assert.deepEqual(claudeCalls[1].args.slice(0, 4), ['--bg', '--name', expectedFreshName, '--dangerously-skip-permissions']);
  assert.equal(store.getWakeTarget('stream:claude-rot')?.driverConfig?.sessionId, expectedFreshName);
  store.ack(store.check({ mailboxId: 'stream:claude-rot' }).deliveryId!, 'stream:claude-rot');

  await claudePlugin.execute([message('c3')], context, {});
  assert.deepEqual(claudeCalls[2].args.slice(0, 4), ['--bg', '--resume', expectedFreshName, '--dangerously-skip-permissions']);
});

test('validateTopicBindings validates positive integer rotateAfterMs', () => {
  assert.doesNotThrow(() =>
    validateTopicBindings(
      { task: { executor: 'codex', mode: 'wave', rotateAfterWaves: 5, rotateAfterMs: 3_600_000 } },
      ['codex'],
    ),
  );
  for (const invalid of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1, '60000']) {
    assert.throws(
      () => validateTopicBindings({ task: { executor: 'codex', mode: 'wave', rotateAfterMs: invalid as unknown as number } }, ['codex']),
      /Invalid topic binding: task/,
    );
  }
});

test('agent-host rotation preserves mid-execution acks committed before host command exits', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const mailboxId = 'stream:sync-ack-rot';
  const fakeCodex = createFakeCodexAppServer({
    nextThreadId: count => `sync-thread-${count}`,
    onTurnStart: () => {
      const lease = store.check({ mailboxId });
      assert.ok(lease.deliveryId, 'expected an active lease during synchronous execution');
      store.ack(lease.deliveryId!, mailboxId);
    },
  });
  const plugin = createCodexExecutorPlugin({
    store,
    stdioSpawner: fakeCodex.spawner,
    resolveTopicBinding: () => ({
      executor: 'codex',
      mode: 'wave',
      mailboxId,
      workspacePath: root,
      rotateAfterWaves: 1,
    }),
  });

  // Wave 1: starts sync-thread-1 and acks synchronously -> completedWaves = 1.
  await plugin.execute([message('sa-1')], context, {});
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'sync-thread-1');
  assert.equal(store.getWakeTarget(mailboxId)?.completedWaves, 1);

  // Wave 2: rotates to sync-thread-2 and acks synchronously -> completedWaves must stay 1, not be wiped to 0.
  await plugin.execute([message('sa-2')], context, {});
  assert.equal(fakeCodex.sessions[1].threadMethod, 'thread/start');
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'sync-thread-2');
  assert.equal(store.getWakeTarget(mailboxId)?.completedWaves, 1);

  // Wave 3: must rotate again to sync-thread-3 instead of reusing sync-thread-2 for a second wave.
  await plugin.execute([message('sa-3')], context, {});
  assert.equal(fakeCodex.sessions[2].threadMethod, 'thread/start');
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'sync-thread-3');
  assert.equal(store.getWakeTarget(mailboxId)?.completedWaves, 1);
});

test('agent-host ordinary enqueues preserve rotated sessionId over configuredSessionId until intentionally rebound', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const mailboxId = 'stream:explicit-seed-rot';
  let configuredSession = 'seed-A';
  const fakeCodex = createFakeCodexAppServer({
    nextThreadId: () => 'rotated-B',
  });
  const plugin = createCodexExecutorPlugin({
    store,
    stdioSpawner: fakeCodex.spawner,
    resolveTopicBinding: () => ({
      executor: 'codex',
      mode: 'wave',
      mailboxId,
      workspacePath: root,
      sessionId: configuredSession,
      rotateAfterWaves: 2,
    }),
  });

  // Waves 1 & 2 reuse configured seed-A.
  await plugin.execute([message('es-1')], context, {});
  assert.equal(fakeCodex.sessions[0].threadMethod, 'thread/resume');
  assert.equal(fakeCodex.sessions[0].threadId, 'seed-A');
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.configuredSessionId, 'seed-A');
  store.ack(store.check({ mailboxId }).deliveryId!, mailboxId);

  await plugin.execute([message('es-2')], context, {});
  assert.equal(fakeCodex.sessions[1].threadMethod, 'thread/resume');
  assert.equal(fakeCodex.sessions[1].threadId, 'seed-A');
  store.ack(store.check({ mailboxId }).deliveryId!, mailboxId);

  // Wave 3 rotates to rotated-B.
  await plugin.execute([message('es-3')], context, {});
  assert.equal(fakeCodex.sessions[2].threadMethod, 'thread/start');
  const afterRotate = store.getWakeTarget(mailboxId)!;
  assert.equal(afterRotate.driverConfig?.sessionId, 'rotated-B');
  assert.equal(afterRotate.driverConfig?.configuredSessionId, 'seed-A');
  const rotatedStartedAt = afterRotate.driverConfig?.sessionStartedAt;
  assert.equal(typeof rotatedStartedAt, 'number');
  store.ack(store.check({ mailboxId }).deliveryId!, mailboxId);

  // Wave 4 ordinary enqueue with unchanged binding (sessionId: 'seed-A') MUST resume rotated-B, not retired seed-A.
  await plugin.execute([message('es-4')], context, {});
  assert.equal(fakeCodex.sessions[3].threadMethod, 'thread/resume');
  assert.equal(fakeCodex.sessions[3].threadId, 'rotated-B');
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'rotated-B');
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionStartedAt, rotatedStartedAt);
  store.ack(store.check({ mailboxId }).deliveryId!, mailboxId);

  // Intentional receiver rebinding to rebound-C advances bindingRevision, switches sessionId, and resets completedWaves.
  const revisionBeforeRebind = store.getWakeTarget(mailboxId)!.bindingRevision!;
  configuredSession = 'rebound-C';
  await plugin.execute([message('es-5')], context, {});
  assert.equal(fakeCodex.sessions[4].threadMethod, 'thread/resume');
  assert.equal(fakeCodex.sessions[4].threadId, 'rebound-C');
  const afterRebind = store.getWakeTarget(mailboxId)!;
  assert.ok(afterRebind.bindingRevision! > revisionBeforeRebind);
  assert.equal(afterRebind.driverConfig?.sessionId, 'rebound-C');
  assert.equal(afterRebind.driverConfig?.configuredSessionId, 'rebound-C');
  assert.equal(afterRebind.completedWaves, 0);
});

test('agent-host rotation atomically refuses concurrent lease acquisition across two ledger connections', async t => {
  const root = directory(t);
  const dbPath = join(root, 'mailbox.json');
  const storeA = new MailboxStore(dbPath);
  const storeB = new MailboxStore(dbPath);
  const mailboxId = 'stream:lease-race';
  const fakeCodex = createFakeCodexAppServer({
    nextThreadId: count => `race-thread-${count}`,
  });
  const plugin = createCodexExecutorPlugin({
    store: storeA,
    stdioSpawner: fakeCodex.spawner,
    resolveTopicBinding: () => ({
      executor: 'codex',
      mode: 'wave',
      mailboxId,
      workspacePath: root,
      rotateAfterWaves: 1,
    }),
  });

  // Wave 1 completes and is acknowledged -> completedWaves = 1, eligible to rotate on next wave.
  await plugin.execute([message('lr-1')], context, {});
  assert.equal(storeA.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'race-thread-1');
  storeB.ack(storeB.check({ mailboxId }).deliveryId!, mailboxId);

  // Enqueue wave 2 on storeA, then simulate an independent CLI process (storeB) acquiring a lease
  // between storeA's read-only peek and tryBeginWakeDelivery.
  storeA.enqueue([message('lr-2')], { mailboxId });
  let racedLeaseId: string | undefined;
  const originalCheck = storeA.check.bind(storeA);
  storeA.check = ((options) => {
    const preview = originalCheck(options);
    if (options?.peek && !racedLeaseId) {
      racedLeaseId = storeB.check({ mailboxId }).deliveryId ?? undefined;
    }
    return preview;
  }) as typeof storeA.check;

  await assert.rejects(
    deliverAgentHostWakeTarget(storeA, runCommand, mailboxId, { now: Date.now(), stdioSpawner: fakeCodex.spawner }),
    /Cannot rotate session while delivery lease .* is outstanding/,
  );
  storeA.check = originalCheck;
  assert.ok(racedLeaseId, 'second ledger connection should have acquired a lease');
  assert.equal(fakeCodex.sessions.length, 1, 'host command must not launch when rotation fence detects an outstanding lease');
  const deferred = storeA.getWakeTarget(mailboxId)!;
  assert.equal(deferred.deliveryInDoubt, undefined, 'refused rotation must defer without fencing');
  assert.equal(deferred.pendingWake, true);
  assert.equal(deferred.driverConfig?.sessionId, 'race-thread-1');

  // Direct structured check on tryBeginWakeDelivery while lease is still held.
  const directCheck = storeA.tryBeginWakeDelivery(mailboxId, deferred.bindingRevision, {
    requireNoOutstandingLease: true,
  });
  assert.equal(directCheck.kind, 'outstanding_lease');
  if (directCheck.kind === 'outstanding_lease') {
    assert.equal(directCheck.leaseId, racedLeaseId);
  }

  // Once storeB acknowledges the concurrent lease and wave 3 is queued, rotation proceeds cleanly.
  storeB.ack(racedLeaseId!, mailboxId);
  storeA.enqueue([message('lr-3')], { mailboxId });
  await deliverAgentHostWakeTarget(storeA, runCommand, mailboxId, { now: Date.now(), stdioSpawner: fakeCodex.spawner });
  assert.equal(fakeCodex.sessions.length, 2);
  assert.equal(fakeCodex.sessions[1].threadMethod, 'thread/start');
  assert.equal(storeA.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'race-thread-2');
  assert.equal(storeA.getWakeTarget(mailboxId)?.completedWaves, 0);
});

test('concurrent rebinding deliveries sharing a mailbox compute reset inside enqueue without erasing first ack', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const mailboxId = 'stream:concurrent-rebind';
  let configuredSession = 'seed-A';
  const fakeCodex = createFakeCodexAppServer({
    onTurnStart: () => {
      const lease = store.check({ mailboxId });
      if (lease.deliveryId) {
        store.ack(lease.deliveryId, mailboxId);
      }
    },
  });
  const plugin = createCodexExecutorPlugin({
    store,
    stdioSpawner: fakeCodex.spawner,
    resolveTopicBinding: () => ({
      executor: 'codex',
      mode: 'wave',
      mailboxId,
      workspacePath: root,
      sessionId: configuredSession,
      rotateAfterWaves: 2,
    }),
  });

  // Seed mailbox on seed-A.
  await plugin.execute([message('cr-1')], context, {});
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'seed-A');
  assert.equal(store.getWakeTarget(mailboxId)?.completedWaves, 1);

  // Rebind to seed-B and simulate two independent threads entering the same mailbox:
  // Thread 2 prepares while the mailbox still holds seed-A, then Thread 1 enqueues, runs,
  // and acknowledges its wave (completedWaves = 1 on seed-B) before Thread 2 commits its enqueue.
  configuredSession = 'seed-B';
  const originalEnqueue = store.enqueue.bind(store);
  let capturedWakeTargetCb: ((current: WakeTargetState | undefined) => Partial<WakeTargetState>) | undefined;
  const probeStore = Object.create(store) as MailboxStore;
  probeStore.enqueue = ((batch, options) => {
    if (typeof options?.wakeTarget === 'function' && !capturedWakeTargetCb) {
      capturedWakeTargetCb = options.wakeTarget;
      return { inserted: [], duplicateIds: [], mailboxIds: [], envelopeMailboxes: {} };
    }
    return originalEnqueue(batch, options);
  }) as typeof store.enqueue;

  await createCodexExecutorPlugin({
    store: probeStore,
    stdioSpawner: fakeCodex.spawner,
    resolveTopicBinding: () => ({
      executor: 'codex',
      mode: 'wave',
      mailboxId,
      workspacePath: root,
      sessionId: 'seed-B',
      rotateAfterWaves: 2,
    }),
  }).execute([message('cr-2b')], context, {});
  assert.ok(capturedWakeTargetCb, 'enqueue should pass a transactional wakeTarget callback');

  // Thread 1 completes rebind to seed-B and acks synchronously (completedWaves = 1).
  await plugin.execute([message('cr-2a')], context, {});
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'seed-B');
  assert.equal(store.getWakeTarget(mailboxId)?.completedWaves, 1);

  // Thread 2 now commits its previously prepared enqueue against the updated locked row:
  // rebind is evaluated inside enqueue's write tx, so completedWaves MUST stay 1.
  originalEnqueue([message('cr-2b')], { mailboxId, wakeTarget: capturedWakeTargetCb });
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'seed-B');
  assert.equal(store.getWakeTarget(mailboxId)?.completedWaves, 1);
});

test('switching canonical agent host after rotation rebinds sessionId and resets rotation metadata', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const mailboxId = 'stream:host-switch-rot';
  const calls: CommandRequest[] = [];
  const fakeCodex = createFakeCodexAppServer({
    nextThreadId: () => 'codex-rotated-B',
  });
  const runner: CommandRunner = async request => {
    calls.push(request);
    return { stdout: '{}' };
  };
  const codexPlugin = createCodexExecutorPlugin({
    store,
    stdioSpawner: fakeCodex.spawner,
    resolveTopicBinding: () => ({
      executor: 'codex',
      mode: 'wave',
      mailboxId,
      workspacePath: root,
      sessionId: 'shared-A',
      rotateAfterWaves: 1,
    }),
  });

  // Codex runs wave 1 on shared-A, acks, then rotates wave 2 to codex-rotated-B.
  await codexPlugin.execute([message('hs-1')], context, {});
  store.ack(store.check({ mailboxId }).deliveryId!, mailboxId);
  await codexPlugin.execute([message('hs-2')], context, {});
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'codex-rotated-B');
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.configuredSessionId, 'shared-A');
  store.ack(store.check({ mailboxId }).deliveryId!, mailboxId);
  assert.equal(store.getWakeTarget(mailboxId)?.completedWaves, 1);

  // Switch the same mailbox from codex to claude-code while keeping explicit sessionId: 'shared-A'.
  // Because the canonical host changed, Claude MUST NOT inherit codex-rotated-B; it must rebind to shared-A
  // and reset completedWaves / sessionStartedAt.
  const claudePlugin = createClaudeCodeExecutorPlugin({
    store,
    commandRunner: runner,
    resolveTopicBinding: () => ({
      executor: 'claude-code',
      mode: 'fixed',
      mailboxId,
      workspacePath: root,
      sessionId: 'shared-A',
      rotateAfterWaves: 2,
    }),
  });
  await claudePlugin.execute([message('hs-3')], context, {});
  const claudeCall = calls.at(-1)!;
  assert.equal(claudeCall.command, 'claude');
  assert.deepEqual(claudeCall.args.slice(0, 4), ['--bg', '--resume', 'shared-A', '--dangerously-skip-permissions']);
  const afterSwitch = store.getWakeTarget(mailboxId)!;
  assert.equal(afterSwitch.driverConfig?.hostAgent, 'claude-code');
  assert.equal(afterSwitch.driverConfig?.sessionId, 'shared-A');
  assert.equal(afterSwitch.driverConfig?.configuredSessionId, 'shared-A');
  assert.equal(afterSwitch.completedWaves, 0);
  store.ack(store.check({ mailboxId }).deliveryId!, mailboxId);

  // Switching hosts without an explicit sessionId also clears the previous host's live sessionId.
  const codexNoSessionPlugin = createCodexExecutorPlugin({
    store,
    stdioSpawner: fakeCodex.spawner,
    resolveTopicBinding: () => ({
      executor: 'codex',
      mode: 'wave',
      mailboxId,
      workspacePath: root,
      rotateAfterWaves: 2,
    }),
  });
  await codexNoSessionPlugin.execute([message('hs-4')], context, {});
  const backToCodex = fakeCodex.sessions.at(-1)!;
  assert.equal(backToCodex.request.command, 'codex');
  assert.equal(backToCodex.threadMethod, 'thread/start', 'host switch without explicit sessionId must start fresh instead of resuming Claude session');
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'codex-rotated-B');
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.configuredSessionId, undefined);
  assert.equal(store.getWakeTarget(mailboxId)?.completedWaves, 0);
});

test('codex app-server executes full JSON-RPC handshake for spawn, wave, and fixed modes', async t => {
  const root = directory(t);
  const fakeCodex = createFakeCodexAppServer({
    nextThreadId: count => `desktop-thread-${count}`,
  });

  // 1. spawn -> initialize -> initialized -> thread/start -> turn/start -> turn/completed
  const spawnResult = await runCodexAppServerTurn({
    cwd: root,
    mode: 'spawn',
    promptText: 'Start task',
    stdioSpawner: fakeCodex.spawner,
  });
  assert.equal(spawnResult.threadId, 'desktop-thread-1');
  assert.equal(spawnResult.status, 'completed');
  assert.deepEqual(JSON.parse(spawnResult.stdout), { threadId: 'desktop-thread-1' });
  const s1 = fakeCodex.sessions[0];
  assert.deepEqual(s1.request, { command: 'codex', args: ['app-server', '--stdio'], cwd: root });
  assert.deepEqual(
    s1.requests.map(req => [req.id ?? null, req.method]),
    [
      [1, 'initialize'],
      [null, 'initialized'],
      [2, 'thread/start'],
      [3, 'turn/start'],
    ],
  );
  assert.deepEqual(s1.requests[0].params, { clientInfo: { name: 'the-dealer', version: '1.0.0' } });
  assert.deepEqual(s1.requests[2].params, { cwd: root, approvalPolicy: 'never', sandbox: 'danger-full-access', threadSource: 'user' });
  assert.deepEqual(s1.requests[3].params, {
    threadId: 'desktop-thread-1',
    input: [{ type: 'text', text: 'Start task' }],
  });
  assert.equal(s1.stdinEnded, true);

  // 2. wave with existing sessionId -> thread/resume
  const waveResume = await runCodexAppServerTurn({
    cwd: root,
    mode: 'wave',
    sessionId: 'desktop-thread-1',
    promptText: 'Continue wave',
    stdioSpawner: fakeCodex.spawner,
  });
  assert.equal(waveResume.threadId, 'desktop-thread-1');
  assert.equal(fakeCodex.sessions[1].threadMethod, 'thread/resume');
  assert.deepEqual(fakeCodex.sessions[1].threadParams, {
    threadId: 'desktop-thread-1',
    cwd: root,
    approvalPolicy: 'never',
    sandbox: 'danger-full-access',
    excludeTurns: true,
  });

  // 3. fixed -> thread/resume
  const fixedResult = await runCodexAppServerTurn({
    cwd: root,
    mode: 'fixed',
    sessionId: 'fixed-desktop-thread',
    promptText: 'Fixed prompt',
    stdioSpawner: fakeCodex.spawner,
  });
  assert.equal(fixedResult.threadId, 'fixed-desktop-thread');
  assert.equal(fakeCodex.sessions[2].threadMethod, 'thread/resume');
  assert.deepEqual(fakeCodex.sessions[2].threadParams, {
    threadId: 'fixed-desktop-thread',
    cwd: root,
    approvalPolicy: 'never',
    sandbox: 'danger-full-access',
    excludeTurns: true,
  });
});

test('codex app-server pre-boundary failures throw CommandNotStartedError and defer without fencing deliveryInDoubt', async t => {
  const root = directory(t);
  const scenarios: Array<{ name: string; run: () => ReturnType<typeof createFakeCodexAppServer>; abortBefore?: boolean; rpcTimeoutMs?: number }> = [
    {
      name: 'synchronous spawn throw',
      run: () => createFakeCodexAppServer({ spawnThrow: () => Object.assign(new Error('spawn ENOENT'), { code: 'ENOENT' }) }),
    },
    {
      name: 'async spawn error',
      run: () => createFakeCodexAppServer({ spawnError: () => Object.assign(new Error('spawn EACCES'), { code: 'EACCES' }) }),
    },
    {
      name: 'early exit before initialize',
      run: () => createFakeCodexAppServer({
        onMessage: (msg, ctx) => {
          if (msg.method === 'initialize') {
            ctx.exit(1, null);
            return true;
          }
        },
      }),
    },
    {
      name: 'initialize RPC error',
      run: () => createFakeCodexAppServer({
        onMessage: (msg, ctx) => {
          if (msg.method === 'initialize') {
            ctx.send({ jsonrpc: '2.0', id: msg.id, error: { code: -32000, message: 'init refused' } });
            return true;
          }
        },
      }),
    },
    {
      name: 'thread/start RPC error',
      run: () => createFakeCodexAppServer({
        onMessage: (msg, ctx) => {
          if (msg.method === 'thread/start') {
            ctx.send({ jsonrpc: '2.0', id: msg.id, error: { code: -32001, message: 'thread start refused' } });
            return true;
          }
        },
      }),
    },
    {
      name: 'thread/start missing thread.id',
      run: () => createFakeCodexAppServer({
        onMessage: (msg, ctx) => {
          if (msg.method === 'thread/start') {
            ctx.send({ jsonrpc: '2.0', id: msg.id, result: { thread: {} } });
            return true;
          }
        },
      }),
    },
    {
      name: 'turn/start explicit RPC rejection',
      run: () => createFakeCodexAppServer({
        onMessage: (msg, ctx) => {
          if (msg.method === 'turn/start') {
            ctx.send({ jsonrpc: '2.0', id: msg.id, error: { code: -32002, message: 'turn rejected before start' } });
            return true;
          }
        },
      }),
    },
    {
      name: 'handshake timeout',
      rpcTimeoutMs: 40,
      run: () => createFakeCodexAppServer({
        onMessage: msg => (msg.method === 'initialize' ? true : undefined),
      }),
    },
    {
      name: 'abort before turn/start',
      abortBefore: true,
      run: () => createFakeCodexAppServer(),
    },
  ];

  for (const [index, scenario] of scenarios.entries()) {
    const fake = scenario.run();
    const controller = new AbortController();
    if (scenario.abortBefore) controller.abort('cancelled pre-turn');
    await assert.rejects(
      runCodexAppServerTurn({
        cwd: root,
        mode: 'spawn',
        promptText: 'pre-boundary check',
        stdioSpawner: fake.spawner,
        signal: controller.signal,
        ...(scenario.rpcTimeoutMs ? { rpcTimeoutMs: scenario.rpcTimeoutMs } : {}),
      }),
      error => error instanceof CommandNotStartedError,
      `direct pre-boundary failure must throw CommandNotStartedError: ${scenario.name}`,
    );

    // Verify deliverAgentHostWakeTarget defers retryably WITHOUT fencing deliveryInDoubt.
    const store = new MailboxStore(join(root, `pre-${index}.json`));
    const mailboxId = `task:pre-${index}`;
    store.enqueue([message(`pre-${index}`)], {
      mailboxId,
      wakeTarget: { driver: 'agent-host', driverConfig: { hostAgent: 'codex', conversationMode: 'spawn', workspacePath: root } },
    });
    const wakeFake = scenario.run();
    const wakeAbort = new AbortController();
    if (scenario.abortBefore) wakeAbort.abort('cancelled pre-turn');
    if (!scenario.rpcTimeoutMs) {
      await assert.rejects(
        deliverAgentHostWakeTarget(store, runCommand, mailboxId, {
          signal: wakeAbort.signal,
          stdioSpawner: wakeFake.spawner,
        }),
        CommandNotStartedError,
      );
      const target = store.getWakeTarget(mailboxId)!;
      assert.equal(target.deliveryInDoubt, undefined, `${scenario.name} must not fence deliveryInDoubt`);
      assert.equal(target.pendingWake, true, `${scenario.name} must keep pendingWake retryable`);
      assert.equal(target.attemptCount, 1);
    }
  }
});

test('codex app-server post-boundary failures park deliveryInDoubt and terminate child cleanly', async t => {
  const root = directory(t);

  // 1. Mid-turn abort sends SIGTERM and throws regular Error (not CommandNotStartedError).
  {
    const started = Promise.withResolvers<void>();
    const fake = createFakeCodexAppServer({
      onTurnStart: async () => {
        started.resolve();
        return { suppressCompletion: true };
      },
    });
    const controller = new AbortController();
    const store = new MailboxStore(join(root, 'post-abort.json'));
    const mailboxId = 'task:post-abort';
    store.enqueue([message('post-abort')], {
      mailboxId,
      wakeTarget: { driver: 'agent-host', driverConfig: { hostAgent: 'codex', conversationMode: 'spawn', workspacePath: root } },
    });
    const delivery = deliverAgentHostWakeTarget(store, runCommand, mailboxId, {
      signal: controller.signal,
      stdioSpawner: fake.spawner,
    });
    await started.promise;
    controller.abort('daemon stopping');
    await assert.rejects(delivery, error => error instanceof Error && !(error instanceof CommandNotStartedError));
    assert.ok(fake.sessions[0].killedSignals.includes('SIGTERM'));
    const target = store.getWakeTarget(mailboxId)!;
    assert.equal(target.deliveryInDoubt, true);
    assert.equal(target.pendingWake, false);
  }

  // 2. Unexpected child exit after turn/start parks deliveryInDoubt.
  {
    const fake = createFakeCodexAppServer({
      onTurnStart: (_params, ctx) => {
        queueMicrotask(() => ctx.exit(137, null));
        return { suppressCompletion: true };
      },
    });
    const store = new MailboxStore(join(root, 'post-exit.json'));
    const mailboxId = 'task:post-exit';
    store.enqueue([message('post-exit')], {
      mailboxId,
      wakeTarget: { driver: 'agent-host', driverConfig: { hostAgent: 'codex', conversationMode: 'spawn', workspacePath: root } },
    });
    await assert.rejects(
      deliverAgentHostWakeTarget(store, runCommand, mailboxId, { stdioSpawner: fake.spawner }),
      error => error instanceof Error && !(error instanceof CommandNotStartedError) && /exited unexpectedly/.test(error.message),
    );
    assert.equal(store.getWakeTarget(mailboxId)?.deliveryInDoubt, true);
  }

  // 3. turn/completed with status 'failed' parks deliveryInDoubt and escalates SIGTERM -> SIGKILL when child refuses to exit.
  {
    const fake = createFakeCodexAppServer({
      ignoreStdinEnd: true,
      ignoreSigterm: true,
      onTurnStart: () => ({ status: 'failed', error: { message: 'sandbox fault' } }),
    });
    await assert.rejects(
      runCodexAppServerTurn({
        cwd: root,
        mode: 'spawn',
        promptText: 'will fail',
        stdioSpawner: fake.spawner,
        closeGraceMs: 30,
        killGraceMs: 40,
      }),
      error => error instanceof Error && !(error instanceof CommandNotStartedError) && /failed.*sandbox fault/.test(error.message),
    );
    assert.ok(fake.sessions[0].killedSignals.includes('SIGTERM'));
    assert.ok(fake.sessions[0].killedSignals.includes('SIGKILL'));
  }
});

test('codex app-server replies -32601 to server requests, discards streaming deltas, and bounds stderr to 16 KiB', async t => {
  const root = directory(t);
  const fake = createFakeCodexAppServer({
    onTurnStart: async (_params, ctx) => {
      // Flood stderr beyond 16 KiB with a known sentinel at the end.
      ctx.sendStderr('A'.repeat(24 * 1024));
      ctx.sendStderr('TAIL_SENTINEL');
      // Emit streaming deltas (must be discarded immediately) and an unexpected server request.
      for (let i = 0; i < 50; i++) {
        ctx.send({ jsonrpc: '2.0', method: 'item/agentMessage/delta', params: { delta: 'x'.repeat(256) } });
      }
      ctx.send({
        jsonrpc: '2.0',
        id: 99,
        method: 'item/commandExecution/requestApproval',
        params: { command: 'rm -rf /' },
      });
      await eventually(() => assert.equal(ctx.session.clientResponses.length, 1));
    },
  });

  const result = await runCodexAppServerTurn({
    cwd: root,
    mode: 'spawn',
    promptText: 'handle server request',
    stdioSpawner: fake.spawner,
  });
  assert.equal(result.status, 'completed');
  assert.deepEqual(fake.sessions[0].clientResponses[0], {
    jsonrpc: '2.0',
    id: 99,
    error: {
      code: -32601,
      message: 'Non-interactive Dealer host does not accept server requests',
    },
  });
  assert.ok(Buffer.byteLength(result.stderr, 'utf8') <= 16 * 1024);
  assert.ok(result.stderr.endsWith('TAIL_SENTINEL'));
});

test('codex and codex-cli reject agentProfile before crossing external boundaries', async t => {
  const root = directory(t);
  for (const host of ['codex', 'codex-cli'] as const) {
    const entry = AGENT_HOST_CATALOG.find(item => item.host === host)!;
    assert.deepEqual(entry.selectors, ['sessionId']);
    assert.throws(
      () => buildAgentHostInvocation({ agent: host, conversationMode: 'spawn', mailboxId: 'task:1', promptText: 'hi', agentProfile: 'fast' }),
      { message: CODEX_AGENT_PROFILE_ERROR },
    );
    assert.throws(
      () => buildAgentBindingPreset({ host, mode: 'wave', agentProfile: 'fast' }),
      { message: CODEX_AGENT_PROFILE_ERROR },
    );
    assert.throws(
      () => validateTopicBindings({ review: { executor: host, mode: 'wave', agentProfile: 'fast' } }, [host]),
      /Invalid topic binding: review/,
    );
  }
  assert.throws(
    () => validateTopicBindings({ review: { executor: 'agent-pty', agent: 'codex', mode: 'wave', agentProfile: 'fast' } }, ['agent-pty']),
    /Invalid topic binding: review/,
  );

  let spawned = 0;
  const fake = createFakeCodexAppServer({
    spawnThrow: () => {
      spawned += 1;
      return new Error('must not spawn');
    },
  });
  const store = new MailboxStore(join(root, 'mailbox.json'));
  await assert.rejects(
    createCodexExecutorPlugin({ store, stdioSpawner: fake.spawner, agentProfile: 'fast' }).execute([message('p1')], context, {}),
    (error: Error & { beforeExternalEffect?: boolean }) =>
      error.beforeExternalEffect === true && error.message === CODEX_AGENT_PROFILE_ERROR,
  );
  assert.equal(spawned, 0);
  assert.equal(store.list().totalCount, 0);

  // Persisted legacy wakeTarget with agentProfile defers without fencing deliveryInDoubt.
  store.enqueue([message('p2')], {
    mailboxId: 'task:p2',
    wakeTarget: { driver: 'agent-host', driverConfig: { hostAgent: 'codex', conversationMode: 'spawn', agentProfile: 'fast' } },
  });
  await assert.rejects(
    deliverAgentHostWakeTarget(store, runCommand, 'task:p2', { stdioSpawner: fake.spawner }),
    { message: CODEX_AGENT_PROFILE_ERROR },
  );
  assert.equal(spawned, 0);
  assert.equal(store.getWakeTarget('task:p2')?.deliveryInDoubt, undefined);
  assert.equal(store.getWakeTarget('task:p2')?.pendingWake, true);
});

test('async wakeup handoff returns immediately, honors wave-1 coalesceMs, and isolates slow turns from inbound RPC abort', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 10_000 });
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const turnStarted = Promise.withResolvers<void>();
  const releaseTurn = Promise.withResolvers<void>();
  const fakeCodex = createFakeCodexAppServer({
    nextThreadId: () => 'slow-thread-1',
    onTurnStart: async () => {
      turnStarted.resolve();
      await releaseTurn.promise;
    },
  });
  const wakeup = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ stdioSpawner: fakeCodex.spawner }),
  });
  t.after(async () => {
    releaseTurn.resolve();
    await wakeup.stop();
  });

  const plugin = createCodexExecutorPlugin({
    store,
    wakeup,
    stdioSpawner: fakeCodex.spawner,
    resolveTopicBinding: () => ({
      executor: 'codex',
      mode: 'wave',
      mailboxId: 'stream:slow-codex',
      workspacePath: root,
      coalesceMs: 5_000,
    }),
  });

  const rpcAbort = new AbortController();
  const receipt = await plugin.execute([message('slow-1')], { ...context, signal: rpcAbort.signal }, {});
  assert.equal(receipt.woke, false);
  assert.equal(receipt.reason, 'Queued for background wakeup');
  assert.equal(fakeCodex.sessions.length, 0, 'wave 1 must honor coalesceMs and not bypass WakeupRedriveService');

  // Enqueue a second message inside the coalesce window and abort the original RPC signal.
  const receipt2 = await plugin.execute([message('slow-2')], { ...context, signal: rpcAbort.signal }, {});
  assert.equal(receipt2.woke, false);
  rpcAbort.abort('30s inbound RPC timeout');
  assert.equal(fakeCodex.sessions.length, 0, 'still inside coalesceMs window');

  // Advance clock past coalesceMs and trigger background flush; turn starts despite aborted RPC signal.
  t.mock.timers.setTime(16_000);
  const flushPromise = wakeup.flushReady();
  await turnStarted.promise;
  assert.equal(fakeCodex.sessions.length, 1);
  assert.match(fakeCodex.sessions[0].promptText!, /2 Dealer message\(s\)/);
  assert.equal(fakeCodex.sessions[0].killedSignals.length, 0, 'inbound RPC abort must not kill background Codex turn');

  releaseTurn.resolve();
  await flushPromise;
  const settled = store.getWakeTarget('stream:slow-codex')!;
  assert.equal(settled.pendingWake, false);
  assert.equal(settled.deliveryInDoubt, undefined);
  assert.equal(settled.driverConfig?.sessionId, 'slow-thread-1');

  // Stopping WakeupRedriveService aborts an in-flight background Codex turn and parks deliveryInDoubt.
  const stopTurnStarted = Promise.withResolvers<void>();
  const stopFake = createFakeCodexAppServer({
    onTurnStart: async () => {
      stopTurnStarted.resolve();
      return { suppressCompletion: true };
    },
  });
  const stopStore = new MailboxStore(join(root, 'stop-mailbox.json'));
  const stopWakeup = new WakeupRedriveService({
    store: stopStore,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ stdioSpawner: stopFake.spawner }),
  });
  await createCodexExecutorPlugin({
    store: stopStore,
    wakeup: stopWakeup,
    stdioSpawner: stopFake.spawner,
    cwd: root,
  }).execute([message('stop-mid-turn')], context, {});
  await stopTurnStarted.promise;
  await stopWakeup.stop();
  assert.ok(stopFake.sessions[0].killedSignals.includes('SIGTERM'));
  assert.equal(stopStore.getWakeTarget('task:stop-mid-turn')?.deliveryInDoubt, true);

  // Restart redrive for Antigravity also trusts workspacePath inside deliverAgentHostWakeTarget.
  const agySettings = join(root, 'redrive-agy-settings.json');
  store.enqueue([message('agy-redrive')], {
    mailboxId: 'task:agy-redrive',
    wakeTarget: {
      driver: 'agent-host',
      driverConfig: { hostAgent: 'antigravity', conversationMode: 'spawn', workspacePath: root },
    },
  });
  await deliverAgentHostWakeTarget(store, async () => ({ stdout: '{}' }), 'task:agy-redrive', {
    agySettingsPath: agySettings,
  });
  assert.ok(JSON.parse(readFileSync(agySettings, 'utf8')).trustedWorkspaces.includes(root));
});

test('codex app-server survives exit-before-stdout-drain, ignores stray handshake turn/completed, and preserves pre-write turn/start boundary', async t => {
  const root = directory(t);

  // 1. Exit-before-stdout-drain race: child emits 'exit' before turn/completed drains from stdout.
  {
    const fake = createFakeCodexAppServer({
      nextThreadId: () => 'drain-thread-1',
      onTurnStart: (_params, ctx) => {
        // Emit process exit BEFORE turn/completed reaches stdout and before stdout closes.
        ctx.child.emit('exit', 0, null);
        setTimeout(() => {
          ctx.send({
            jsonrpc: '2.0',
            method: 'turn/completed',
            params: { threadId: 'drain-thread-1', turn: { id: ctx.session.turnId, status: 'completed' } },
          });
          ctx.child.stopKeepAlive();
          ctx.child.stdout.end();
          ctx.child.stderr.end();
          ctx.child.emit('close', 0, null);
        }, 15);
        return { suppressCompletion: true };
      },
    });
    const result = await runCodexAppServerTurn({
      cwd: root,
      mode: 'spawn',
      promptText: 'exit before drain',
      stdioSpawner: fake.spawner,
    });
    assert.equal(result.status, 'completed');
    assert.equal(result.threadId, 'drain-thread-1');
  }

  // 2. Stray turn/completed during thread/resume handshake is ignored; waits for the real new turn.
  {
    let newTurnStarted = false;
    const fake = createFakeCodexAppServer({
      onMessage: (msg, ctx) => {
        if (msg.method === 'thread/resume') {
          ctx.session.threadMethod = 'thread/resume';
          ctx.session.threadParams = msg.params;
          ctx.session.threadId = String(msg.params?.threadId ?? '');
          // Emit a stray failed turn/completed notification before thread/resume completes.
          ctx.send({
            jsonrpc: '2.0',
            method: 'turn/completed',
            params: { turn: { id: 'stale-turn-0', status: 'failed', error: { message: 'stale turn' } } },
          });
          ctx.send({ jsonrpc: '2.0', id: msg.id, result: { thread: { id: ctx.session.threadId } } });
          return true;
        }
      },
      onTurnStart: async () => {
        newTurnStarted = true;
        await new Promise(resolve => setTimeout(resolve, 15));
      },
    });
    const result = await runCodexAppServerTurn({
      cwd: root,
      mode: 'fixed',
      sessionId: 'resumed-thread',
      promptText: 'ignore stray completion',
      stdioSpawner: fake.spawner,
    });
    assert.equal(newTurnStarted, true, 'must still start and wait for the new turn');
    assert.equal(result.status, 'completed');
    assert.equal(result.threadId, 'resumed-thread');
  }

  // 3a. Stdin closed right before turn/start is written -> CommandNotStartedError (defers without fencing).
  {
    const store = new MailboxStore(join(root, 'closed-stdin.json'));
    const mailboxId = 'task:closed-stdin';
    store.enqueue([message('closed-stdin')], {
      mailboxId,
      wakeTarget: { driver: 'agent-host', driverConfig: { hostAgent: 'codex', conversationMode: 'spawn', workspacePath: root } },
    });
    const fake = createFakeCodexAppServer({
      ignoreStdinEnd: true,
      onMessage: (msg, ctx) => {
        if (msg.method === 'thread/start') {
          ctx.child.stdin.end();
          ctx.send({ jsonrpc: '2.0', id: msg.id, result: { thread: { id: 'pre-write-thread' } } });
          return true;
        }
      },
    });
    await assert.rejects(
      deliverAgentHostWakeTarget(store, runCommand, mailboxId, { stdioSpawner: fake.spawner }),
      CommandNotStartedError,
    );
    const target = store.getWakeTarget(mailboxId)!;
    assert.equal(target.deliveryInDoubt, undefined, 'closed stdin before turn/start write must not fence deliveryInDoubt');
    assert.equal(target.pendingWake, true);
  }

  // 3b. Signal aborted right before turn/start is written -> CommandNotStartedError (defers without fencing).
  {
    const store = new MailboxStore(join(root, 'pre-write-abort.json'));
    const mailboxId = 'task:pre-write-abort';
    store.enqueue([message('pre-write-abort')], {
      mailboxId,
      wakeTarget: { driver: 'agent-host', driverConfig: { hostAgent: 'codex', conversationMode: 'spawn', workspacePath: root } },
    });
    const controller = new AbortController();
    const fake = createFakeCodexAppServer({
      onMessage: (msg, ctx) => {
        if (msg.method === 'thread/start') {
          ctx.send({ jsonrpc: '2.0', id: msg.id, result: { thread: { id: 'abort-before-turn' } } });
          controller.abort('aborted before turn/start write');
          return true;
        }
      },
    });
    await assert.rejects(
      deliverAgentHostWakeTarget(store, runCommand, mailboxId, {
        signal: controller.signal,
        stdioSpawner: fake.spawner,
      }),
      CommandNotStartedError,
    );
    const target = store.getWakeTarget(mailboxId)!;
    assert.equal(target.deliveryInDoubt, undefined, 'abort before turn/start write must not fence deliveryInDoubt');
    assert.equal(target.pendingWake, true);
  }

  // 4. Wakeup-backed antigravity executor delegates workspace trust to deliverAgentHostWakeTarget:
  // duplicate/redirected deliveries do not mutate settings, and malformed settings preserve the
  // committed receipt while deferring retryably on the wake target.
  {
    const store = new MailboxStore(join(root, 'trust-guard.json'));
    const agySettings = join(root, 'trust-guard-settings.json');
    const wakeup = new WakeupRedriveService({
      store,
      flushIntervalMs: 100_000,
      drivers: createTestWakeupDrivers({
        agySettingsPath: agySettings,
        commandRunner: async () => ({ stdout: '{}' }),
      }),
    });
    t.after(() => wakeup.stop());

    // Pre-seed duplicate message in task:dup and an existing thread mailbox for redirected spawn.
    const dup = message('dup');
    store.enqueue([dup], { mailboxId: 'task:dup' });
    const existingRoot = { ...message('existing-root'), semantic: { type: 'message' as const, threadId: 'shared-thread' } };
    store.enqueue([existingRoot], { mailboxId: 'task:existing-root' });

    const plugin = createAntigravityExecutorPlugin({
      store,
      wakeup,
      agySettingsPath: agySettings,
      cwd: root,
      commandRunner: async () => ({ stdout: '{}' }),
    });

    // Duplicate delivery: no wake target created -> must not touch settings.
    await plugin.execute([dup], context, {});
    await wakeup.waitUntilIdle();
    assert.equal(existsSync(agySettings), false, 'duplicate delivery must not mutate Antigravity settings');

    // Redirected spawn (claimedSpawn === false inside host deliver()): must not touch settings.
    const racedStore = Object.create(store) as MailboxStore;
    const origResolve = store.resolveThreadLocalMailbox.bind(store);
    racedStore.resolveThreadLocalMailbox = ((threadId, networkId, opts) =>
      threadId === 'shared-thread' ? undefined : origResolve(threadId, networkId, opts)) as typeof store.resolveThreadLocalMailbox;
    const racedPlugin = createAntigravityExecutorPlugin({
      store: racedStore,
      wakeup,
      agySettingsPath: agySettings,
      cwd: root,
      commandRunner: async () => ({ stdout: '{}' }),
    });
    const redirected = { ...message('redirected-child'), semantic: { type: 'message' as const, threadId: 'shared-thread' } };
    const redirectedReceipt = await racedPlugin.execute([redirected], context, {});
    assert.equal(redirectedReceipt.mailboxId, 'task:existing-root');
    assert.equal(redirectedReceipt.claimedSpawn, false);
    await wakeup.waitUntilIdle();
    assert.equal(existsSync(agySettings), false, 'redirected spawn must not mutate Antigravity settings');

    // Malformed settings preserve the committed execution receipt and defer retryably on the wake target.
    writeFileSync(agySettings, '{malformed-json');
    const committedReceipt = await plugin.execute([message('malformed-settings-agy')], context, {});
    assert.equal(committedReceipt.mailboxId, 'task:malformed-settings-agy');
    assert.equal(committedReceipt.woke, false);
    assert.equal(committedReceipt.reason, 'Queued for background wakeup');
    await wakeup.waitUntilIdle();
    const deferredTarget = store.getWakeTarget('task:malformed-settings-agy')!;
    assert.equal(deferredTarget.pendingWake, true);
    assert.equal(deferredTarget.deliveryInDoubt, undefined);
    assert.ok(deferredTarget.lastWakeError);

    // Repairing settings allows retry to trust workspacePath and complete cleanly.
    rmSync(agySettings, { force: true });
    const flushed = await wakeup.flushMailbox('task:malformed-settings-agy', { force: true });
    assert.equal(flushed.woke, true);
    assert.equal(existsSync(agySettings), true);
    assert.ok(JSON.parse(readFileSync(agySettings, 'utf8')).trustedWorkspaces.includes(root));
  }
});

test('codex app-server correlates turn/completed strictly by turnId+threadId and completes bounded teardown on failure', async t => {
  const root = directory(t);

  // 1. Uncorrelated notifications (missing turn.id, wrong turn.id, wrong threadId) during turn-sent
  //    and turn-running are ignored; an early matching turn/completed arriving before turn/start
  //    response is buffered and correlated once turn/start returns.
  {
    const fake = createFakeCodexAppServer({
      nextThreadId: () => 'corr-thread-1',
      onMessage: (msg, ctx) => {
        if (msg.method === 'turn/start') {
          // Emit uncorrelated / malformed notifications BEFORE the turn/start response.
          ctx.send({ jsonrpc: '2.0', method: 'turn/completed', params: { threadId: 'corr-thread-1', turn: { status: 'failed' } } });
          ctx.send({ jsonrpc: '2.0', method: 'turn/completed', params: { threadId: 'other-thread', turn: { id: 'real-turn-1', status: 'failed' } } });
          ctx.send({ jsonrpc: '2.0', method: 'turn/completed', params: { threadId: 'corr-thread-1', turn: { id: 'stale-turn-0', status: 'failed' } } });
          // Emit the real matching turn/completed BEFORE the turn/start response!
          ctx.send({ jsonrpc: '2.0', method: 'turn/completed', params: { threadId: 'corr-thread-1', turn: { id: 'real-turn-1', status: 'completed' } } });
          ctx.send({ jsonrpc: '2.0', id: msg.id, result: { turn: { id: 'real-turn-1', status: 'inProgress' } } });
          return true;
        }
      },
    });
    const result = await runCodexAppServerTurn({
      cwd: root,
      mode: 'spawn',
      promptText: 'early correlated completion',
      stdioSpawner: fake.spawner,
    });
    assert.equal(result.status, 'completed');
    assert.equal(result.threadId, 'corr-thread-1');
    assert.equal(result.turnId, 'real-turn-1');
  }

  // 2. turn/start response missing turn.id rejects with a regular Error (preserves deliveryInDoubt fence).
  {
    const fake = createFakeCodexAppServer({
      nextThreadId: () => 'missing-turn-id-thread',
      onMessage: (msg, ctx) => {
        if (msg.method === 'turn/start') {
          ctx.send({ jsonrpc: '2.0', id: msg.id, result: { turn: {} } });
          return true;
        }
      },
    });
    const store = new MailboxStore(join(root, 'missing-turn-id.json'));
    const mailboxId = 'task:missing-turn-id';
    store.enqueue([message('missing-turn-id')], {
      mailboxId,
      wakeTarget: { driver: 'agent-host', driverConfig: { hostAgent: 'codex', conversationMode: 'spawn', workspacePath: root } },
    });
    await assert.rejects(
      deliverAgentHostWakeTarget(store, runCommand, mailboxId, { stdioSpawner: fake.spawner }),
      error => error instanceof Error && !(error instanceof CommandNotStartedError) && /turn\.id/.test(error.message),
    );
    assert.equal(store.getWakeTarget(mailboxId)?.deliveryInDoubt, true);
  }

  // 3. Failure teardown awaits actual child exit (including SIGKILL escalation) and absorbs late stdin EPIPE.
  {
    const turnStarted = Promise.withResolvers<void>();
    let capturedChild: { exited: boolean; stdin: NodeJS.WritableStream } | undefined;
    const fake = createFakeCodexAppServer({
      ignoreStdinEnd: true,
      ignoreSigterm: true,
      onTurnStart: (_params, ctx) => {
        capturedChild = ctx.child;
        turnStarted.resolve();
        return { suppressCompletion: true };
      },
    });
    const controller = new AbortController();
    const runPromise = runCodexAppServerTurn({
      cwd: root,
      mode: 'spawn',
      promptText: 'must await sigkill teardown',
      stdioSpawner: fake.spawner,
      signal: controller.signal,
      killGraceMs: 40,
    });
    await turnStarted.promise;
    controller.abort('abort-with-sigterm-ignored');
    await assert.rejects(runPromise, error => error instanceof Error && !(error instanceof CommandNotStartedError));
    assert.equal(capturedChild?.exited, true, 'runCodexAppServerTurn must not return before child terminates');
    assert.ok(fake.sessions[0].killedSignals.includes('SIGTERM'));
    assert.ok(fake.sessions[0].killedSignals.includes('SIGKILL'));
    // Emitting a late EPIPE on stdin after teardown must not produce an uncaught exception.
    capturedChild?.stdin.emit('error', Object.assign(new Error('write EPIPE'), { code: 'EPIPE' }));
  }
});

test('codex app-server defaults to 180s RPC timeout and uses single-SIGTERM teardown with post-SIGKILL grace', async t => {
  assert.equal(DEFAULT_RPC_TIMEOUT_MS, 180_000);
  const root = directory(t);
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const tickAsync = async (ms: number) => {
    t.mock.timers.tick(ms);
    await new Promise(resolve => setImmediate(resolve));
  };

  // 1. Slow cold startup (>30s thread/start and turn/start) completes cleanly under default 180s RPC timeout.
  {
    const threadStartSeen = Promise.withResolvers<() => void>();
    const turnStartSeen = Promise.withResolvers<() => void>();
    const fake = createFakeCodexAppServer({
      onMessage: (msg, ctx) => {
        if (msg.method === 'thread/start') {
          threadStartSeen.resolve(() => {
            ctx.session.threadMethod = 'thread/start';
            ctx.session.threadParams = msg.params;
            ctx.session.threadId = 'slow-cold-thread';
            ctx.send({ jsonrpc: '2.0', id: msg.id, result: { thread: { id: 'slow-cold-thread' } } });
          });
          return true;
        }
        if (msg.method === 'turn/start') {
          turnStartSeen.resolve(() => {
            ctx.session.turnId = 'slow-cold-turn';
            ctx.send({ jsonrpc: '2.0', id: msg.id, result: { turn: { id: 'slow-cold-turn', status: 'inProgress' } } });
            ctx.send({
              jsonrpc: '2.0',
              method: 'turn/completed',
              params: { threadId: 'slow-cold-thread', turn: { id: 'slow-cold-turn', status: 'completed' } },
            });
          });
          return true;
        }
      },
    });

    const runPromise = runCodexAppServerTurn({
      cwd: root,
      mode: 'spawn',
      promptText: 'slow cold startup',
      stdioSpawner: fake.spawner,
    });
    const finishThreadStart = await threadStartSeen.promise;
    await tickAsync(45_000);
    assert.deepEqual(fake.sessions[0].killedSignals, [], '45s thread/start must not trip default RPC timeout');
    finishThreadStart();

    const finishTurnStart = await turnStartSeen.promise;
    await tickAsync(60_000);
    assert.deepEqual(fake.sessions[0].killedSignals, [], '60s turn/start must not trip default RPC timeout');
    finishTurnStart();

    const result = await runPromise;
    assert.equal(result.status, 'completed');
    assert.equal(result.threadId, 'slow-cold-thread');
    assert.deepEqual(fake.sessions[0].killedSignals, []);
  }

  // 2. Wedged turn/start times out at 180s, sends a single SIGTERM, escalates to SIGKILL after 2s,
  //    and reaps cleanly when SIGKILL exit arrives 500ms later (within 1000ms default post-kill grace).
  {
    const turnStartSeen = Promise.withResolvers<void>();
    let capturedChild: { finish: (code?: number | null, signal?: NodeJS.Signals | null) => void; stopKeepAlive: () => void } | undefined;
    const fake = createFakeCodexAppServer({
      ignoreStdinEnd: true,
      ignoreSigterm: true,
      onMessage: (msg, ctx) => {
        if (msg.method === 'turn/start') {
          capturedChild = ctx.child;
          turnStartSeen.resolve();
          return true;
        }
      },
    });
    let reapTimer: ReturnType<typeof setTimeout> | undefined;
    const delayedKillSpawner: StdioProcessSpawner = request => {
      const child = fake.spawner(request);
      const origKill = child.kill.bind(child);
      child.kill = signal => {
        if (signal === 'SIGKILL') {
          fake.sessions.at(-1)!.killedSignals.push('SIGKILL');
          reapTimer = setTimeout(() => {
            capturedChild?.finish(null, 'SIGKILL');
          }, 500);
          return true;
        }
        return origKill(signal);
      };
      return child;
    };
    t.after(() => {
      if (reapTimer) clearTimeout(reapTimer);
      capturedChild?.stopKeepAlive();
    });

    let settledError: unknown;
    const runPromise = runCodexAppServerTurn({
      cwd: root,
      mode: 'spawn',
      promptText: 'wedged turn start',
      stdioSpawner: delayedKillSpawner,
    }).catch(error => {
      settledError = error;
    });

    await turnStartSeen.promise;
    await tickAsync(179_999);
    assert.equal(settledError, undefined);
    assert.deepEqual(fake.sessions[0].killedSignals, []);

    // Hit 180_000ms RPC timeout: must send exactly ONE SIGTERM via finally -> teardownChild('terminate').
    await tickAsync(1);
    assert.deepEqual(fake.sessions[0].killedSignals, ['SIGTERM']);

    // Hit 2_000ms killGraceMs: escalates to SIGKILL without duplicate SIGTERM.
    await tickAsync(2_000);
    assert.deepEqual(fake.sessions[0].killedSignals, ['SIGTERM', 'SIGKILL']);

    // Child exits 500ms after SIGKILL (past old 100ms window, inside new 1000ms postKillGraceMs).
    await tickAsync(500);
    await runPromise;
    assert.ok(settledError instanceof Error && !(settledError instanceof CommandNotStartedError));
    assert.match(settledError.message, /turn\/start timed out after 180000ms/);
    assert.doesNotMatch(settledError.message, /failed to terminate within bounded teardown/);
    assert.deepEqual(fake.sessions[0].killedSignals, ['SIGTERM', 'SIGKILL']);
  }

  // 3. Unkillable children report accurate bounded teardown windows for terminate (killGraceMs + postKillGraceMs)
  //    and graceful (closeGraceMs + killGraceMs + postKillGraceMs) with a single SIGTERM.
  {
    const makeUnkillableSpawner = (fake: ReturnType<typeof createFakeCodexAppServer>): StdioProcessSpawner => {
      return request => {
        const child = fake.spawner(request);
        const origKill = child.kill.bind(child);
        t.after(() => origKill('SIGKILL'));
        child.kill = signal => {
          fake.sessions.at(-1)!.killedSignals.push(signal);
          return true;
        };
        return child;
      };
    };

    // 3a. Terminate teardown on RPC timeout: killGraceMs=40 -> postKillGraceMs=100 -> 140ms.
    const initSeen = Promise.withResolvers<void>();
    const timeoutFake = createFakeCodexAppServer({
      ignoreStdinEnd: true,
      ignoreSigterm: true,
      onMessage: msg => {
        if (msg.method === 'initialize') {
          initSeen.resolve();
          return true;
        }
      },
    });
    let timeoutError: unknown;
    const timeoutRun = runCodexAppServerTurn({
      cwd: root,
      mode: 'spawn',
      promptText: 'unkillable on timeout',
      stdioSpawner: makeUnkillableSpawner(timeoutFake),
      rpcTimeoutMs: 40,
      killGraceMs: 40,
    }).catch(error => {
      timeoutError = error;
    });
    await initSeen.promise;
    await tickAsync(40);
    assert.deepEqual(timeoutFake.sessions[0].killedSignals, ['SIGTERM']);
    await tickAsync(40);
    assert.deepEqual(timeoutFake.sessions[0].killedSignals, ['SIGTERM', 'SIGKILL']);
    await tickAsync(100);
    await timeoutRun;
    assert.ok(timeoutError instanceof CommandNotStartedError);
    assert.match(timeoutError.message, /initialize timed out after 40ms/);
    assert.match(timeoutError.message, /failed to terminate within bounded teardown \(140ms\)/);
    assert.deepEqual(timeoutFake.sessions[0].killedSignals, ['SIGTERM', 'SIGKILL']);

    // 3b. Graceful teardown after turn/completed: closeGraceMs=30, killGraceMs=40 -> postKillGraceMs=100 -> 170ms.
    const gracefulFake = createFakeCodexAppServer({
      ignoreStdinEnd: true,
      ignoreSigterm: true,
    });
    let gracefulError: unknown;
    const gracefulRun = runCodexAppServerTurn({
      cwd: root,
      mode: 'spawn',
      promptText: 'unkillable on graceful close',
      stdioSpawner: makeUnkillableSpawner(gracefulFake),
      closeGraceMs: 30,
      killGraceMs: 40,
    }).catch(error => {
      gracefulError = error;
    });
    await new Promise(resolve => setImmediate(resolve));
    await tickAsync(30);
    assert.deepEqual(gracefulFake.sessions[0].killedSignals, ['SIGTERM']);
    await tickAsync(40);
    assert.deepEqual(gracefulFake.sessions[0].killedSignals, ['SIGTERM', 'SIGKILL']);
    await tickAsync(100);
    await gracefulRun;
    assert.ok(gracefulError instanceof Error && !(gracefulError instanceof CommandNotStartedError));
    assert.match(gracefulError.message, /failed to terminate within bounded teardown \(170ms\)/);
    assert.deepEqual(gracefulFake.sessions[0].killedSignals, ['SIGTERM', 'SIGKILL']);
  }
});

test('runtime idle/drain waits for active background Codex turns and offline CLI settles accepted turns before exit', async t => {
  const root = directory(t);

  // 1. isIdleForRestart() and drainForStop() account for WakeupRedriveService in-flight turns.
  {
    const turnStarted = Promise.withResolvers<void>();
    const releaseTurn = Promise.withResolvers<void>();
    const fakeCodex = createFakeCodexAppServer({
      nextThreadId: () => 'drain-codex-1',
      onTurnStart: async () => {
        turnStarted.resolve();
        await releaseTurn.promise;
      },
    });
    const runtime = await new DealerNodeRuntime({
      stateDir: join(root, 'drain-state'),
      member: 'solo',
      transport: { discoveryDir: join(root, 'drain-discovery') },
      patch: { topicBindings: { review: { executor: 'codex', mode: 'wave', workspacePath: root } } },
      stdioSpawner: fakeCodex.spawner,
    }).start();
    t.after(async () => {
      releaseTurn.resolve();
      await runtime.stop();
    });

    await runtime.emitter.emit({
      networkId: 'local',
      kind: 'review',
      routing: { mode: 'unicast', targets: ['member:solo'] },
      subject: 'Long Codex turn',
      body: 'Take your time',
      payload: {},
    });
    await turnStarted.promise;
    assert.equal(runtime.wakeups.activeCount(), 1);
    assert.equal(runtime.isIdleForRestart(), false, 'running background Codex turn must block restart');
    assert.equal(await runtime.drainForStop({ drainTimeoutMs: 40 }), false, 'drainForStop must defer while Codex turn runs');
    assert.equal(runtime.isStoppingOrDraining(), false);
    assert.equal(fakeCodex.sessions[0].killedSignals.length, 0, 'deferred drain must not abort running turn');

    // Awaiting drainForStop with sufficient timeout wakes as soon as the turn finishes.
    const drainPromise = runtime.drainForStop({ drainTimeoutMs: 5_000 });
    releaseTurn.resolve();
    assert.equal(await drainPromise, true, 'drainForStop should succeed once background turn finishes');
    assert.equal(runtime.mailbox.getWakeTarget('stream:review')?.deliveryInDoubt, undefined);
    assert.equal(runtime.mailbox.getWakeTarget('stream:review')?.driverConfig?.sessionId, 'drain-codex-1');
  }

  // 2. Offline runDealerCli local send waits for its accepted background Codex turn before owned.stop().
  {
    const stateDir = join(root, 'cli-state');
    mkdirSync(stateDir, { recursive: true });
    const fakeBin = join(root, 'fake-codex.mjs');
    writeFileSync(
      fakeBin,
      [
        `#!${process.execPath}`,
        "import { createInterface } from 'node:readline';",
        "const rl = createInterface({ input: process.stdin });",
        "let threadId = 'cli-codex-thread-1';",
        "rl.on('line', line => {",
        "  if (!line.trim()) return;",
        "  const msg = JSON.parse(line);",
        "  if (msg.method === 'initialize') process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }) + '\\n');",
        "  else if (msg.method === 'thread/start') process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { thread: { id: threadId } } }) + '\\n');",
        "  else if (msg.method === 'thread/name/set') process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }) + '\\n');",
        "  else if (msg.method === 'turn/start') {",
        "    process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { turn: { id: 'cli-turn-1', status: 'inProgress' } } }) + '\\n');",
        "    setTimeout(() => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', method: 'turn/completed', params: { threadId, turn: { id: 'cli-turn-1', status: 'completed' } } }) + '\\n'), 80);",
        "  }",
        "});",
      ].join('\n'),
    );
    chmodSync(fakeBin, 0o755);
    writeFileSync(
      join(stateDir, 'config.json'),
      JSON.stringify({
        profile: 'codex-dev',
        member: 'solo',
        name: 'local',
        topicBindings: { review: { executor: 'codex', mode: 'wave', workspacePath: root } },
        executorConfig: { codex: { command: fakeBin } },
      }),
    );
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(
      ['send', '--network', 'local', '--to', 'member:solo', '--topic', 'review', '--subject', 'Offline', '--body', 'Run turn', '--json'],
      { stateDir, stdout: line => stdout.push(line), stderr: line => stderr.push(line) },
    );
    assert.equal(code, 0, stderr.join('\n'));
    const store = new MailboxStore(join(stateDir, 'mailbox.json'));
    const target = store.getWakeTarget('stream:review')!;
    assert.equal(target.deliveryInDoubt, undefined, 'offline CLI teardown must not abort accepted turn into deliveryInDoubt');
    assert.equal(target.pendingWake, false);
    assert.equal(target.driverConfig?.sessionId, 'cli-codex-thread-1');

    // 3. Offline runDealerCli local send with coalesceMs > 0 starts a background daemon so pending
    //    coalesced mailbox work survives CLI exit and completes automatically.
    const coalescedStateDir = join(root, 'cli-coalesced-state');
    mkdirSync(coalescedStateDir, { recursive: true });
    writeFileSync(
      join(coalescedStateDir, 'config.json'),
      JSON.stringify({
        profile: 'codex-dev',
        member: 'solo',
        name: 'local',
        wave: { flushIntervalMs: 100 },
        topicBindings: { review: { executor: 'codex', mode: 'wave', workspacePath: root, coalesceMs: 250 } },
        executorConfig: { codex: { command: fakeBin } },
      }),
    );
    try {
      const cStdout: string[] = [];
      const cStderr: string[] = [];
      const cCode = await runDealerCli(
        ['send', '--network', 'local', '--to', 'member:solo', '--topic', 'review', '--subject', 'Coalesced', '--body', 'Run after coalesce', '--json'],
        { stateDir: coalescedStateDir, stdout: line => cStdout.push(line), stderr: line => cStderr.push(line) },
      );
      assert.equal(cCode, 0, cStderr.join('\n'));
      await eventually(() => {
        const coalescedStore = new MailboxStore(join(coalescedStateDir, 'mailbox.json'), { readonly: true });
        try {
          const coalescedTarget = coalescedStore.getWakeTarget('stream:review');
          assert.equal(coalescedTarget?.deliveryInDoubt, undefined);
          assert.equal(coalescedTarget?.pendingWake, false);
          assert.equal(coalescedTarget?.driverConfig?.sessionId, 'cli-codex-thread-1');
        } finally {
          coalescedStore.close();
        }
      }, 10_000);
    } finally {
      await stopDaemonIfRunning(coalescedStateDir, { force: true }).catch(() => {});
    }

    // 4. drainForStop() quiesces wake scheduling at commit so a coalesced mailbox maturing during
    //    teardown cannot launch a new Codex turn that wakeups.stop() would abort into deliveryInDoubt.
    {
      const fakeCodex = createFakeCodexAppServer({ nextThreadId: () => 'should-not-launch' });
      const runtime = await new DealerNodeRuntime({
        stateDir: join(root, 'quiesce-drain-state'),
        member: 'solo',
        transport: { discoveryDir: join(root, 'quiesce-drain-discovery') },
        patch: {
          wave: { flushIntervalMs: 20 },
          topicBindings: { review: { executor: 'codex', mode: 'wave', workspacePath: root, coalesceMs: 60 } },
        },
        stdioSpawner: fakeCodex.spawner,
      }).start();
      t.after(() => runtime.stop());

      await runtime.emitter.emit({
        networkId: 'local',
        kind: 'review',
        routing: { mode: 'unicast', targets: ['member:solo'] },
        subject: 'Coalesced before drain',
        body: 'Must stay queued across graceful drain',
        payload: {},
      });
      assert.equal(runtime.wakeups.activeCount(), 0);
      assert.equal(await runtime.drainForStop({ drainTimeoutMs: 500 }), true);

      // Simulate slow teardown (e.g. outboxRedrive.stop()) lasting past coalesceMs and flush ticks.
      await new Promise(resolve => setTimeout(resolve, 140));
      runtime.wakeups.notifyMailbox('stream:review');
      await runtime.wakeups.flushReady();
      assert.equal(fakeCodex.sessions.length, 0, 'quiesced drain commit must not launch newly matured turns');
      assert.equal(runtime.wakeups.activeCount(), 0);

      await runtime.stop();
      const postStopStore = new MailboxStore(join(root, 'quiesce-drain-state', 'mailbox.json'), { readonly: true });
      try {
        const targetAfterStop = postStopStore.getWakeTarget('stream:review');
        assert.equal(targetAfterStop?.pendingWake, true);
        assert.equal(targetAfterStop?.deliveryInDoubt, undefined);
      } finally {
        postStopStore.close();
      }
    }

    // 5. WakeupRedriveService.quiesce() lets already-in-flight turns finish during waitUntilIdle()
    //    while preventing newly matured mailboxes from launching during settlement.
    {
      const turnStarted = Promise.withResolvers<void>();
      const releaseTurn = Promise.withResolvers<void>();
      const fakeCodex = createFakeCodexAppServer({
        nextThreadId: () => 'in-flight-quiesce-thread',
        onTurnStart: async () => {
          turnStarted.resolve();
          await releaseTurn.promise;
        },
      });
      const store = new MailboxStore(join(root, 'quiesce-settle.json'));
      t.after(() => store.close());
      const wakeup = new WakeupRedriveService({
        store,
        flushIntervalMs: 20,
        drivers: createTestWakeupDrivers({ stdioSpawner: fakeCodex.spawner }),
      });
      t.after(() => wakeup.stop());

      store.enqueue([message('in-flight-msg')], {
        mailboxId: 'stream:active',
        wakeTarget: {
          driver: 'agent-host',
          coalesceMs: 0,
          driverConfig: { hostAgent: 'codex', conversationMode: 'wave', workspacePath: root },
        },
      });
      await turnStarted.promise;

      store.enqueue([message('coalesced-msg')], {
        mailboxId: 'stream:later',
        wakeTarget: {
          driver: 'agent-host',
          coalesceMs: 40,
          driverConfig: { hostAgent: 'codex', conversationMode: 'wave', workspacePath: root },
        },
      });

      wakeup.quiesce();
      await new Promise(resolve => setTimeout(resolve, 90));
      assert.equal(fakeCodex.sessions.length, 1, 'quiesced service must not start newly matured mailbox');

      releaseTurn.resolve();
      await wakeup.waitUntilIdle();
      assert.equal(fakeCodex.sessions.length, 1);
      assert.equal(store.getWakeTarget('stream:active')?.pendingWake, false);
      assert.equal(store.getWakeTarget('stream:active')?.deliveryInDoubt, undefined);
      assert.equal(store.getWakeTarget('stream:active')?.driverConfig?.sessionId, 'in-flight-quiesce-thread');
      assert.equal(store.getWakeTarget('stream:later')?.pendingWake, true);
      assert.equal(store.getWakeTarget('stream:later')?.deliveryInDoubt, undefined);
    }

    // 6. Offline gate drain AND partial-commit error exits preserve pending mailbox continuation
    //    by handing authoritative pending wake obligations off to the background daemon.
    for (const mode of ['clean-gate-drain', 'partial-commit-error'] as const) {
      const gateStateDir = join(root, `cli-${mode}`);
      mkdirSync(gateStateDir, { recursive: true });
      const baseConfig = {
        profile: 'codex-dev',
        member: 'solo',
        name: 'local',
        gateMode: 'dnd',
        wave: { flushIntervalMs: 100 },
        topicBindings: { review: { executor: 'codex', mode: 'wave', workspacePath: root, coalesceMs: 250 } },
        executorConfig: { codex: { command: fakeBin } },
      };
      writeFileSync(join(gateStateDir, 'config.json'), JSON.stringify(baseConfig));

      // Seed genuine signed DND entries via an in-process runtime in DND mode.
      const seedRuntime = await new DealerNodeRuntime({
        stateDir: gateStateDir,
        transport: { discoveryDir: join(root, `cli-${mode}-discovery`) },
      }).start();
      await seedRuntime.emitter.emit({
        networkId: 'local',
        kind: 'review',
        routing: { mode: 'unicast', targets: ['member:solo'] },
        subject: `DND ${mode}`,
        body: 'Queued under DND then drained offline',
        payload: {},
      });
      if (mode === 'partial-commit-error') {
        // Seed a second valid signed DND entry whose dispatch fails on unsupported Codex
        // agentProfile after the first entry has already committed coalesced mailbox work.
        await seedRuntime.emitter.emit({
          networkId: 'local',
          kind: 'review',
          routing: { mode: 'unicast', targets: ['member:solo'] },
          subject: 'DND failing item',
          body: 'Triggers AggregateError after partial commit',
          payload: { agentProfile: 'unsupported-profile' },
        });
      }
      assert.equal(seedRuntime.gate.listDndQueue().length, mode === 'partial-commit-error' ? 2 : 1);
      await seedRuntime.stop();

      // Switch persisted gateMode to open so offline `gate drain` releases DND custody into the mailbox.
      writeFileSync(join(gateStateDir, 'config.json'), JSON.stringify({ ...baseConfig, gateMode: 'open' }));

      try {
        const gStdout: string[] = [];
        const gStderr: string[] = [];
        const gCode = await runDealerCli(['gate', 'drain', '--json'], {
          stateDir: gateStateDir,
          stdout: line => gStdout.push(line),
          stderr: line => gStderr.push(line),
        });
        assert.equal(
          gCode,
          mode === 'clean-gate-drain' ? 0 : 1,
          `unexpected exit code for ${mode}: ${gStderr.join('\n')}`,
        );
        await eventually(() => {
          const drainedStore = new MailboxStore(join(gateStateDir, 'mailbox.json'), { readonly: true });
          try {
            const drainedTarget = drainedStore.getWakeTarget('stream:review');
            assert.equal(drainedTarget?.deliveryInDoubt, undefined);
            assert.equal(drainedTarget?.pendingWake, false);
            assert.equal(drainedTarget?.driverConfig?.sessionId, 'cli-codex-thread-1');
          } finally {
            drainedStore.close();
          }
        }, 10_000);
      } finally {
        await stopDaemonIfRunning(gateStateDir, { force: true }).catch(() => {});
      }
    }

    // 7. Mail committed during runtime teardown (while awaiting outboxRedrive.stop() or kernel
    //    producer shutdown) is captured by the post-producer continuation snapshot and handed off
    //    to the background daemon even for non-sending CLI commands with an initially empty mailbox.
    {
      // 7a. In-process runtime: signed inbound message arriving during outboxRedrive.stop().
      const fakeCodex = createFakeCodexAppServer({ nextThreadId: () => 'should-not-launch-during-stop' });
      const runtime = await new DealerNodeRuntime({
        stateDir: join(root, 'teardown-race-runtime'),
        member: 'solo',
        transport: { discoveryDir: join(root, 'teardown-race-discovery') },
        patch: {
          topicBindings: { review: { executor: 'codex', mode: 'wave', workspacePath: root, coalesceMs: 0 } },
        },
        stdioSpawner: fakeCodex.spawner,
      }).start();
      assert.equal(runtime.mailbox.listPendingWakeTargets(Number.MAX_SAFE_INTEGER).length, 0);

      const sender = generateNodeKeyPair();
      const lateEnvelope = signEnvelope(
        {
          id: 'late-teardown-inbound',
          networkId: 'local',
          traceId: 'late-teardown-inbound',
          from: { nodeId: sender.nodeId, member: 'peer', nodeName: 'remote' },
          routing: { mode: 'unicast', targets: ['member:solo'] },
          kind: 'review',
          priority: 'normal',
          subject: 'Arrived during outbox stop',
          body: 'Must survive teardown for daemon handoff',
          payload: {},
          attachments: [],
          createdAt: new Date().toISOString(),
        },
        sender.privateKeyPem,
      );
      const origOutboxStop = runtime.outboxRedrive.stop.bind(runtime.outboxRedrive);
      runtime.outboxRedrive.stop = async () => {
        // Dispatch while finishStop() is awaiting outboxRedrive.stop() and transport/dispatcher
        // are still live, after wakeups.quiesce() has already fenced new launches.
        await runtime.kernel.ctx.services.dispatcher.dispatch(lateEnvelope);
        await origOutboxStop();
      };
      await runtime.stop();
      assert.equal(fakeCodex.sessions.length, 0, 'quiesced stop must not launch late inbound mail mid-teardown');
      assert.equal(runtime.hasPendingWakeupsAfterStop(), true, 'post-producer snapshot must observe late inbound wake');
      const postStopStore = new MailboxStore(join(root, 'teardown-race-runtime', 'mailbox.json'), { readonly: true });
      try {
        assert.equal(postStopStore.getWakeTarget('stream:review')?.pendingWake, true);
        assert.equal(postStopStore.getWakeTarget('stream:review')?.deliveryInDoubt, undefined);
      } finally {
        postStopStore.close();
      }

      // 7b. End-to-end offline CLI: non-sending command (`peers list`) whose teardown accepts late
      //     inbound mail during owned.stop() still hands off to a background daemon.
      const cliLateStateDir = join(root, 'cli-late-teardown-state');
      mkdirSync(cliLateStateDir, { recursive: true });
      const markerPath = join(cliLateStateDir, 'late-dispatched.flag');
      const pluginPath = join(root, 'late-teardown-plugin.mjs');
      const coreTypesUrl = new URL('../packages/core-types/index.ts', import.meta.url).href;
      writeFileSync(
        pluginPath,
        [
          "import { existsSync, writeFileSync } from 'node:fs';",
          `import { generateNodeKeyPair, signEnvelope } from ${JSON.stringify(coreTypesUrl)};`,
          'export function createPlugin() {',
          '  return {',
          "    name: 'test-late-teardown',",
          "    inject: ['dispatcher'],",
          '    apply(ctx) {',
          '      ctx.effect(() => async () => {',
          `        if (existsSync(${JSON.stringify(markerPath)})) return;`,
          `        writeFileSync(${JSON.stringify(markerPath)}, '1');`,
          '        const sender = generateNodeKeyPair();',
          '        const env = signEnvelope({',
          "          id: 'cli-late-teardown-msg',",
          "          networkId: 'local',",
          "          traceId: 'cli-late-teardown-msg',",
          "          from: { nodeId: sender.nodeId, member: 'peer', nodeName: 'remote' },",
          "          routing: { mode: 'unicast', targets: ['member:solo'] },",
          "          kind: 'review',",
          "          priority: 'normal',",
          "          subject: 'Arrived during CLI stop',",
          "          body: 'Must trigger daemon handoff after stop',",
          '          payload: {},',
          '          attachments: [],',
          '          createdAt: new Date().toISOString(),',
          '        }, sender.privateKeyPem);',
          '        await ctx.services.dispatcher.dispatch(env);',
          '      });',
          '    },',
          '  };',
          '}',
        ].join('\n'),
      );
      writeFileSync(
        join(cliLateStateDir, 'config.json'),
        JSON.stringify({
          profile: 'codex-dev',
          member: 'solo',
          name: 'local',
          wave: { flushIntervalMs: 100 },
          topicBindings: { review: { executor: 'codex', mode: 'wave', workspacePath: root, coalesceMs: 0 } },
          executorConfig: { codex: { command: fakeBin } },
          userPlugins: [{ name: 'test-late-teardown', module: pluginPath }],
        }),
      );
      try {
        const lStdout: string[] = [];
        const lStderr: string[] = [];
        const lCode = await runDealerCli(['peers', 'list', '--json'], {
          stateDir: cliLateStateDir,
          stdout: line => lStdout.push(line),
          stderr: line => lStderr.push(line),
        });
        assert.equal(lCode, 0, lStderr.join('\n'));
        await eventually(() => {
          const lateStore = new MailboxStore(join(cliLateStateDir, 'mailbox.json'), { readonly: true });
          try {
            const lateTarget = lateStore.getWakeTarget('stream:review');
            assert.equal(lateTarget?.deliveryInDoubt, undefined);
            assert.equal(lateTarget?.pendingWake, false);
            assert.equal(lateTarget?.driverConfig?.sessionId, 'cli-codex-thread-1');
          } finally {
            lateStore.close();
          }
        }, 10_000);
      } finally {
        await stopDaemonIfRunning(cliLateStateDir, { force: true }).catch(() => {});
      }
    }
  }
});

test('codex threadSource user, thread/name/set naming, resume preservation, and -32601 compatibility', async t => {
  const root = directory(t);

  // 1. Pure buildCodexThreadName contract: spawn subject + whitespace collapse + 80-char cap + fallback; wave label + rotating MM-DD.
  const longSubject = `Investigate   critical\nmultiline   incident   ${'x'.repeat(80)}`;
  const capped = buildCodexThreadName('task:1', 'spawn', { items: [{ envelope: { subject: longSubject } }] });
  assert.equal(capped.includes('\n'), false);
  assert.equal(Array.from(capped).length, 80);
  assert.ok(capped.endsWith('…'));
  assert.ok(capped.startsWith('Investigate critical multiline incident '));
  assert.equal(
    buildCodexThreadName('task:empty', 'spawn', { items: [{ envelope: { subject: '   \n\t  ' } }] }),
    'Dealer · task:empty',
  );
  assert.equal(
    buildCodexThreadName('stream:collab.request.v4', 'wave', { items: [] }, { rotates: false }),
    'Dealer · collab.request.v4',
  );
  const sampleDate = new Date(2026, 4, 7, 15, 30, 0).getTime(); // local 05-07
  assert.equal(
    buildCodexThreadName('stream:alerts', 'wave', { items: [] }, { now: sampleDate, rotates: true }),
    'Dealer · alerts (05-07)',
  );

  // 2. Fresh spawn and rotating wave pass threadSource:'user' + sequential IDs + thread/name/set; resume never calls thread/name/set.
  {
    const store = new MailboxStore(join(root, 'naming.json'));
    t.after(() => store.close());
    const fakeCodex = createFakeCodexAppServer({
      nextThreadId: count => `named-thread-${count}`,
    });

    // Spawn mode via executor: uses message subject.
    const spawnPlugin = createCodexExecutorPlugin({
      store,
      stdioSpawner: fakeCodex.spawner,
      resolveTopicBinding: () => ({
        executor: 'codex',
        mode: 'spawn',
        workspacePath: root,
      }),
    });
    await spawnPlugin.execute([
      { ...message('spawn-named'), subject: '  Fix   billing\nreconciliation  ' },
    ], context, {});
    const sSpawn = fakeCodex.sessions[0];
    assert.deepEqual(
      sSpawn.requests.map(req => [req.id ?? null, req.method]),
      [
        [1, 'initialize'],
        [null, 'initialized'],
        [2, 'thread/start'],
        [3, 'thread/name/set'],
        [4, 'turn/start'],
      ],
    );
    assert.equal(sSpawn.threadParams?.threadSource, 'user');
    assert.equal(sSpawn.threadName, 'Fix billing reconciliation');

    // Stateless (!store) spawn also sets threadSource:'user' and threadName from subject.
    const statelessPlugin = createCodexExecutorPlugin({
      stdioSpawner: fakeCodex.spawner,
      mode: 'spawn',
      cwd: root,
    });
    await statelessPlugin.execute([
      { ...message('stateless-named'), subject: 'Stateless spawn title' },
    ], context, {});
    const sStateless = fakeCodex.sessions[1];
    assert.equal(sStateless.threadMethod, 'thread/start');
    assert.equal(sStateless.threadParams?.threadSource, 'user');
    assert.equal(sStateless.threadName, 'Stateless spawn title');

    // Wave mode with rotation: Wave 1 sets Dealer · <stream> (MM-DD), Wave 2 resumes without calling thread/name/set,
    // Wave 3 rotates and calls thread/name/set on the new thread.
    const waveNow1 = new Date(2026, 8, 3, 10, 0, 0).getTime(); // local 09-03
    const waveNow3 = new Date(2026, 8, 9, 10, 0, 0).getTime(); // local 09-09
    store.enqueue([message('w1')], {
      mailboxId: 'stream:ops-feed',
      wakeTarget: {
        driver: 'agent-host',
        rotateAfterWaves: 1,
        driverConfig: { hostAgent: 'codex', conversationMode: 'wave', workspacePath: root },
      },
    });
    await deliverAgentHostWakeTarget(store, runCommand, 'stream:ops-feed', { now: waveNow1, stdioSpawner: fakeCodex.spawner });
    const sWave1 = fakeCodex.sessions[2];
    assert.equal(sWave1.threadMethod, 'thread/start');
    assert.equal(sWave1.threadParams?.threadSource, 'user');
    assert.equal(sWave1.threadName, 'Dealer · ops-feed (09-03)');

    // Follow-up while lease is outstanding -> resumes existing thread without calling thread/name/set!
    const lease1 = store.check({ mailboxId: 'stream:ops-feed' });
    store.enqueue([message('w1-followup')], { mailboxId: 'stream:ops-feed' });
    await deliverAgentHostWakeTarget(store, runCommand, 'stream:ops-feed', { now: waveNow1 + 500, stdioSpawner: fakeCodex.spawner });
    const sResume = fakeCodex.sessions[3];
    assert.equal(sResume.threadMethod, 'thread/resume');
    assert.equal(sResume.threadName, undefined);
    assert.equal(sResume.requests.some(req => req.method === 'thread/name/set'), false);

    // Even direct runCodexAppServerTurn with threadName on thread/resume must NOT call thread/name/set.
    await runCodexAppServerTurn({
      cwd: root,
      mode: 'fixed',
      sessionId: 'named-thread-3',
      threadName: 'Should Never Overwrite',
      promptText: 'keep title',
      stdioSpawner: fakeCodex.spawner,
    });
    const sFixed = fakeCodex.sessions[4];
    assert.equal(sFixed.threadMethod, 'thread/resume');
    assert.equal(sFixed.requests.some(req => req.method === 'thread/name/set'), false);

    // Ack wave 1 -> completedWaves === 1 === rotateAfterWaves -> rotates to fresh thread with new MM-DD title.
    store.ack(lease1.deliveryId!, 'stream:ops-feed');
    store.enqueue([message('w2')], { mailboxId: 'stream:ops-feed' });
    await deliverAgentHostWakeTarget(store, runCommand, 'stream:ops-feed', { now: waveNow3, stdioSpawner: fakeCodex.spawner });
    const sRotated = fakeCodex.sessions[5];
    assert.equal(sRotated.threadMethod, 'thread/start');
    assert.equal(sRotated.threadParams?.threadSource, 'user');
    assert.equal(sRotated.threadName, 'Dealer · ops-feed (09-09)');
  }

  // 3. Older server returning -32601 to thread/name/set degrades cleanly and completes turn.
  {
    const fakeCompat = createFakeCodexAppServer({
      nextThreadId: () => 'legacy-codex-thread',
      onMessage: (msg, ctx) => {
        if (msg.method === 'thread/name/set') {
          ctx.send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: 'Method not found' } });
          return true;
        }
      },
    });
    const result = await runCodexAppServerTurn({
      cwd: root,
      mode: 'spawn',
      threadName: 'Legacy Fallback Title',
      promptText: 'Proceed anyway',
      stdioSpawner: fakeCompat.spawner,
    });
    assert.equal(result.status, 'completed');
    assert.equal(result.threadId, 'legacy-codex-thread');
    assert.match(result.stderr, /-32601/);
    assert.deepEqual(
      fakeCompat.sessions[0].requests.map(req => [req.id ?? null, req.method]),
      [
        [1, 'initialize'],
        [null, 'initialized'],
        [2, 'thread/start'],
        [3, 'thread/name/set'],
        [4, 'turn/start'],
      ],
    );
  }

  // 4. Non--32601 failure on thread/name/set fails cleanly before turn/start with CommandNotStartedError and does not fence deliveryInDoubt.
  {
    const store = new MailboxStore(join(root, 'name-fail.json'));
    t.after(() => store.close());
    const fakeFail = createFakeCodexAppServer({
      onMessage: (msg, ctx) => {
        if (msg.method === 'thread/name/set') {
          ctx.send({ jsonrpc: '2.0', id: msg.id, error: { code: -32602, message: 'Invalid params' } });
          return true;
        }
      },
    });
    store.enqueue([message('fail-name')], {
      mailboxId: 'stream:name-fail',
      wakeTarget: {
        driver: 'agent-host',
        driverConfig: { hostAgent: 'codex', conversationMode: 'wave', workspacePath: root },
      },
    });
    await assert.rejects(
      deliverAgentHostWakeTarget(store, runCommand, 'stream:name-fail', { stdioSpawner: fakeFail.spawner }),
      error => error instanceof CommandNotStartedError && /thread\/name\/set failed.*-32602/.test(error.message),
    );
    assert.equal(fakeFail.sessions[0].requests.some(req => req.method === 'turn/start'), false);
    assert.equal(store.getWakeTarget('stream:name-fail')?.deliveryInDoubt, undefined);
    assert.equal(store.getWakeTarget('stream:name-fail')?.pendingWake, true);
  }
});

test('resolveAgentHostSessionLink and isAgentHostRotationDue enforce host, pending, and rotation boundaries', () => {
  const baseTarget = (overrides: Partial<WakeTargetState> = {}): WakeTargetState => ({
    mailboxId: 'stream:review',
    driver: 'agent-host',
    coalesceMs: 0,
    completedWaves: 0,
    pendingWake: false,
    earliestWakeAt: 0,
    attemptCount: 0,
    driverConfig: { hostAgent: 'codex', conversationMode: 'wave', sessionId: 'thread-abc/1', sessionStartedAt: 1_000 },
    ...overrides,
  });

  // Non-Codex / non-agent-host / undefined -> undefined.
  assert.equal(resolveAgentHostSessionLink(undefined), undefined);
  assert.equal(resolveAgentHostSessionLink(baseTarget({ driver: 'orca-terminal', driverConfig: { workspacePath: '/tmp' } })), undefined);
  assert.equal(resolveAgentHostSessionLink(baseTarget({ driver: 'passive', driverConfig: undefined })), undefined);
  assert.equal(resolveAgentHostSessionLink(baseTarget({ driverConfig: { hostAgent: 'claude-code', sessionId: 'c1' } })), undefined);
  assert.equal(resolveAgentHostSessionLink(baseTarget({ driverConfig: { hostAgent: 'antigravity', sessionId: 'a1' } })), undefined);

  // Codex / codex-cli with active session and no rotation due -> ready with encoded deep link.
  assert.deepEqual(resolveAgentHostSessionLink(baseTarget()), {
    state: 'ready',
    label: 'ChatGPT App',
    url: 'codex://threads/thread-abc%2F1',
  });
  assert.deepEqual(
    resolveAgentHostSessionLink(baseTarget({ driverConfig: { hostAgent: 'codex-cli', conversationMode: 'fixed', sessionId: 'tid-42' } })),
    { state: 'ready', label: 'ChatGPT App', url: 'codex://threads/tid-42' },
  );

  // Missing or whitespace sessionId -> pending.
  assert.deepEqual(
    resolveAgentHostSessionLink(baseTarget({ driverConfig: { hostAgent: 'codex', conversationMode: 'wave' } })),
    { state: 'pending' },
  );
  assert.deepEqual(
    resolveAgentHostSessionLink(baseTarget({ driverConfig: { hostAgent: 'codex', conversationMode: 'spawn', sessionId: '  ' } })),
    { state: 'pending' },
  );

  // Wave rotation due with no outstanding lease -> pending; with outstanding lease -> ready (must not rotate mid-lease).
  const waveRotating = baseTarget({ rotateAfterWaves: 2, completedWaves: 2 });
  assert.equal(isAgentHostRotationDue(waveRotating, { hasOutstandingLease: false }), true);
  assert.equal(isAgentHostRotationDue(waveRotating, { hasOutstandingLease: true }), false);
  assert.deepEqual(resolveAgentHostSessionLink(waveRotating, { hasOutstandingLease: false }), { state: 'pending' });
  assert.deepEqual(resolveAgentHostSessionLink(waveRotating, { hasOutstandingLease: true }), {
    state: 'ready',
    label: 'ChatGPT App',
    url: 'codex://threads/thread-abc%2F1',
  });

  // Age rotation due (rotateAfterMs) with no outstanding lease -> pending; before expiry or with lease -> ready.
  const ageRotating = baseTarget({
    driverConfig: {
      hostAgent: 'codex',
      conversationMode: 'wave',
      sessionId: 'age-1',
      rotateAfterMs: 60_000,
      sessionStartedAt: 10_000,
    },
  });
  assert.equal(isAgentHostRotationDue(ageRotating, { now: 69_999, hasOutstandingLease: false }), false);
  assert.equal(isAgentHostRotationDue(ageRotating, { now: 70_000, hasOutstandingLease: false }), true);
  assert.equal(isAgentHostRotationDue(ageRotating, { now: 70_000, hasOutstandingLease: true }), false);
  assert.deepEqual(resolveAgentHostSessionLink(ageRotating, { now: 69_999, hasOutstandingLease: false }), {
    state: 'ready',
    label: 'ChatGPT App',
    url: 'codex://threads/age-1',
  });
  assert.deepEqual(resolveAgentHostSessionLink(ageRotating, { now: 70_000, hasOutstandingLease: false }), {
    state: 'pending',
  });
  assert.deepEqual(resolveAgentHostSessionLink(ageRotating, { now: 70_000, hasOutstandingLease: true }), {
    state: 'ready',
    label: 'ChatGPT App',
    url: 'codex://threads/age-1',
  });
});
