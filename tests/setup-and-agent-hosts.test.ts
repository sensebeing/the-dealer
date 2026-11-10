import { runCommand, CommandNotStartedError } from '../packages/orca-workspace/index.ts';
import { buildAgentHostInvocation, resolveAgentSessionId } from '../packages/agent-host/index.ts';
import { WakeupRedriveService, createInboxExecutorPlugin } from '../packages/index.ts';
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync, existsSync, readFileSync } from 'node:fs';
import { tmpdir, homedir } from 'node:os';
import type { NetworkInterfaceInfo } from 'node:os';
import { P2PTransportService, defaultAdvertisedHost } from '../packages/transport/index.ts';
import { join } from 'node:path';
import {
  AGENT_HOST_CATALOG, buildAgentBindingPreset, probeDealerSetup, runDealerCli,
  decodeNetworkInvite, DealerNodeRuntime, MailboxStore, createAgentPtyExecutorPlugin,
  createOrcaSpawnExecutorPlugin, createOrcaWaveExecutorPlugin,
  createCodexExecutorPlugin, createClaudeCodeExecutorPlugin, createAntigravityExecutorPlugin,
  BUILTIN_EXECUTORS, getBuiltInProfile,
  defaultAgentLaunchCommand, resolveOrSpawnOrcaWorkspaceTerminal, validateTopicBindings,
} from '../packages/index.ts';
import type { WakeTargetState, CommandRequest, CommandRunner, MessageEnvelope, SupportedOrcaAgent } from '../packages/index.ts';

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
      assert.match(prompt, /npm install && npm link/);
      assert.match(prompt, /~\/\.agents\/skills\//);
      assert.match(prompt, /~\/\.claude\/skills\//);
      assert.match(prompt, /the-dealer-setup\/SKILL.md/);
      assert.match(prompt, /dealer setup probe --json/);
      assert.match(prompt, /Never overwrite ~\/\.dealer\/identity.json/);
    }
    assert.match(result.teammatePrompt, /Ask for the teammate’s member handle/);
    assert.match(result.teammatePrompt, /dealer setup binding-preset/);
  }
});

for (const { host } of AGENT_HOST_CATALOG) {
  for (const mode of ['spawn', 'fixed', 'inbox-only'] as const) {
    test(`setup preset ${host} ${mode} is valid through CLI`, async t => {
      const root = directory(t);
      const result = await cli(['setup', 'binding-preset', '--host', host, '--mode', mode, '--target-bot', 'review', '--workspace-path', root], root);
      validateTopicBindings({ [result.topic]: result.binding }, [result.binding.executor]);
      assert.equal(result.binding.conversationMode, mode);
      if (mode === 'inbox-only') assert.equal(result.binding.executor, 'inbox');
      else if (host === 'orca' || host === 'antigravity') assert.equal(result.binding.executor, mode === 'spawn' ? 'orca-spawn' : 'orca-wave');
      else assert.equal(result.binding.executor, 'agent-pty');
    });
  }
}

for (const agent of ['codex', 'claude-code', 'chatgpt-dot', 'grok-bot', 'antigravity'] as const) {
  for (const conversationMode of ['spawn', 'fixed'] as const) {
    test(`agent-pty ${agent} ${conversationMode} persists before invoking and uses host argv`, async t => {
      const root = directory(t);
      const store = new MailboxStore(join(root, 'mailbox.json'));
      const mailboxId = conversationMode === 'spawn' ? 'task:test' : 'stream:collab.request.v4';
      const calls: CommandRequest[] = [];
      const commandRunner: CommandRunner = async request => {
        assert.equal(store.getWakeTarget(mailboxId)?.mode, 'passive');
        calls.push(request);
        return { stdout: agent === 'codex' ? '{"threadId":"session"}' : '{"accepted":true}' };
      };
      const plugin = createAgentPtyExecutorPlugin({ store, commandRunner, resolveTopicBinding: () => ({ executor: 'agent-pty', agent, conversationMode,
        mailboxId: 'stream:collab.request.v4', agentProfile: 'review', targetBot: 'bot', sessionId: 'session', workspacePath: root }) });
      await plugin.execute([message()], context, {});
      const call = calls[0];
      const args = call.args;
      const prompt = args.at(-1)!;
      assert.match(prompt, new RegExp(mailboxId));
      assert.ok(!prompt.includes('\n'));
      if (conversationMode === 'spawn') {
        assert.ok(prompt.startsWith('Perform task then report — Dealer Task dispatched to mailbox task:test'));
        assert.ok(!prompt.includes('Payload:'));
      }
      assert.equal(store.getWakeTarget(mailboxId)?.conversationMode, agent === 'antigravity' ? undefined : 'fixed');
      assert.equal(store.getWakeTarget(mailboxId)?.sessionId, agent === 'antigravity' ? undefined : 'session');
      if (conversationMode === 'fixed') assert.match(prompt, /Latest .*Perform task then report/);
      assert.equal(call.cwd, root);
      if (agent === 'codex') {
        assert.equal(call.command, 'codex');
        assert.deepEqual(args.slice(0, conversationMode === 'spawn' ? 2 : 4), conversationMode === 'spawn' ? ['exec', '--worktree'] : ['queue', '--thread', 'session', '--resume']);
        assert.ok(args.includes('-p'));
      } else if (agent === 'claude-code') {
        assert.equal(call.command, 'claude');
        assert.deepEqual(args.slice(0, 3), conversationMode === 'spawn' ? ['--bg', '--name', 'session'] : ['--bg', '--resume', 'session']);
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
    assert.equal(new MailboxStore(join(root, 'mailbox.json')).getWakeTarget('task:spawn')?.agent, agent);
    await createOrcaWaveExecutorPlugin({ store, agent, cwd: root, interactiveCommand: 'custom-agent' }).execute([message('wave')], context, {});
    const wake = new MailboxStore(join(root, 'mailbox.json')).getWakeTarget('stream:collab.request.v4');
    assert.equal(wake?.agent, agent);
    assert.equal(wake?.interactiveCommand, 'custom-agent');
  });
}

test('Orca terminal resolution prefers target host then other agents, boots shells and creates with chosen command', async () => {
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
  assert.equal((await resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', '/repo', undefined, { agent: 'codex' })).handle, 'agy');
  terminals = [{ handle: 'shell', connected: true, preview: '$' }];
  assert.equal((await resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', '/repo', undefined, { agent: 'codex' })).shellBootstrapped, true);
  assert.ok(calls.at(-1)!.args.includes(defaultAgentLaunchCommand('codex')));
  terminals = [];
  assert.equal((await resolveOrSpawnOrcaWorkspaceTerminal(runner, 'orca', '/repo', undefined, { agent: 'claude', interactiveCommand: 'custom' })).handle, 'created');
  assert.ok(calls.at(-1)!.args.includes('custom'));
});

test('runtime uses a generated fixed binding, persists mailbox and supports setup while running', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const preset = buildAgentBindingPreset({ host: 'codex', conversationMode: 'fixed', workspacePath: root, sessionId: 'review' });
  const runtime = new DealerNodeRuntime({
    stateDir: join(root, 'state'), member: 'solo',
    transport: { discoveryDir: join(root, 'discovery') },
    patch: { topicBindings: { [preset.topic]: preset.binding } },
    commandRunner: async request => {
      calls.push(request);
      return { stdout: '{}' };
    },
  });
  await runtime.start();
  t.after(() => runtime.stop());
  assert.equal(runtime.config.profile, 'personal-dev');
  assert.equal(runtime.config.gateMode, 'approval_required');
  const result = await runtime.emitter.emit({ networkId: 'local', kind: preset.topic,
    routing: { mode: 'unicast', targets: ['member:solo'] }, subject: 'Review', body: 'Check this change', payload: {} });
  assert.equal(result.receipts[0].stage, 'dispatched', JSON.stringify(result.receipts));
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].args.slice(0, 4), ['queue', '--thread', 'review', '--resume']);
  assert.equal(runtime.mailbox.getWakeTarget(`stream:${preset.topic}`)?.mode, 'passive');
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
    const executors = ['notify', 'inbox'];
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
    for (const executor of ['agent-pty', 'codex', 'claude-code', 'antigravity']) {
      assert.equal(runtime.config.executors.includes(executor), scenario.expected);
    }
    assert.equal(runtime.config.gateMode, 'approval_required');
  });
}

test('failed host execution leaves durable mail and does not report success', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const plugin = createAgentPtyExecutorPlugin({ store, agent: 'codex', conversationMode: 'spawn',
    commandRunner: async () => ({ stdout: '', stderr: 'host rejected command', exitCode: 2 }) });
  await assert.rejects(plugin.execute([message('failed')], context, {}), /exited 2/);
  assert.equal(new MailboxStore(join(root, 'mailbox.json')).getWakeTarget('task:failed')?.mode, 'passive');
  assert.equal(store.getWakeTarget('task:failed')?.pendingWake, false);
  assert.equal(store.getWakeTarget('task:failed')?.deliveryInDoubt, true);
});

test('wake target host options merge, clear and validate after reload', t => {
  const root = directory(t);
  const file = join(root, 'mailbox.json');
  const store = new MailboxStore(file);
  store.upsertWakeTarget('default', { agent: 'codex', interactiveCommand: 'custom', hostAgent: 'claude-code', conversationMode: 'fixed', agentProfile: 'review', targetBot: 'bot', sessionId: 'session' });
  store.upsertWakeTarget('default', { workspacePath: root });
  assert.equal(new MailboxStore(file).getWakeTarget('default')?.agent, 'codex');
  const restored = new MailboxStore(file).getWakeTarget('default')!;
  assert.equal(restored.hostAgent, 'claude-code');
  assert.equal(restored.conversationMode, 'fixed');
  assert.equal(restored.agentProfile, 'review');
  assert.equal(restored.targetBot, 'bot');
  assert.equal(restored.sessionId, 'session');
  for (const patch of [{ hostAgent: 'invalid' }, { conversationMode: 'invalid' }, { agentProfile: 1 }, { targetBot: false }, { sessionId: [] }]) {
    assert.throws(() => store.upsertWakeTarget('default', patch as Partial<WakeTargetState>));
  }
  store.upsertWakeTarget('default', { agent: undefined, interactiveCommand: undefined, hostAgent: undefined, conversationMode: undefined, agentProfile: undefined, targetBot: undefined, sessionId: undefined });
  const cleared = new MailboxStore(file).getWakeTarget('default')!;
  for (const field of ['hostAgent', 'conversationMode', 'agentProfile', 'targetBot', 'sessionId'] as const) assert.equal(cleared[field], undefined);
  assert.equal(new MailboxStore(file).getWakeTarget('default')?.interactiveCommand, undefined);
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
  t.mock.timers.enable({ apis: ['Date'], now: 10_000 });
  // The same seed with a foreign key remains undiscovered, exercising failed probes too.
  await peer.mountNetwork({ networkId: 'foreign', passphrase: 'wrong', bootstrapPeers: [seed.endpointAddr!, '127.0.0.1:1'] });
  await peer.mountNetwork({ networkId: 'team', passphrase: 'shared', bootstrapPeers: [seed.endpointAddr!] });
  clearInterval(peer['timer']);
  assert.equal(connections, 2, 'one initial probe per mount');
  assert.equal(attempts.mock.callCount(), 3, 'unreachable seed also probed once');
  assert.equal(peer.listPeerEndpoints('team').get(seed.localNodeId), seed.endpointAddr);
  assert.equal(peer.listPeerEndpoints('foreign').size, 0);
  for (let tick = 1; tick < 40; tick++) {
    t.mock.timers.tick(50);
    await peer['discover']();
  }
  assert.equal(connections, 2, 'no TCP churn during fast discovery ticks');
  assert.equal(attempts.mock.callCount(), 3, 'unreachable seed retries are also bounded');
  t.mock.timers.tick(50);
  await peer['discover']();
  assert.equal(connections, 4, 'both known and failed seeds are retried at the bounded interval');
  assert.equal(attempts.mock.callCount(), 6, 'unreachable seed is retried at the bounded interval');
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
      const options = {
        store, conversationMode: mode,
        resolveTopicBinding: () => ({ executor: name, mailboxId: 'stream:review', workspacePath: root, sessionId: 'session' }),
        commandRunner: async (request: CommandRequest) => {
          assert.equal(store.getWakeTarget(mailboxId)?.mode, 'passive');
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
      assert.deepEqual(result.output, { accepted: true });
      assert.equal(result.stderr, 'diagnostic');
      assert.equal(calls[0].command, command);
      assert.equal(calls[0].cwd, root);
      assert.match(calls[0].args.at(-1)!, new RegExp(mailboxId));
      assert.equal(existsSync(settings), false);
      assert.equal(existsSync(hostSettings) ? readFileSync(hostSettings, 'utf8') : undefined, beforeSettings);
      if (name === 'codex') assert.equal(calls[0].args[0], mode === 'spawn' ? 'exec' : 'queue');
      if (name === 'claude-code') assert.equal(calls[0].args[1], mode === 'spawn' ? '--name' : '--resume');
      if (name === 'antigravity') {
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
  const commandRunner: CommandRunner = async request => { calls.push(request); return { stdout: 'plain output' }; };
  for (const factory of [createCodexExecutorPlugin, createClaudeCodeExecutorPlugin, createAntigravityExecutorPlugin]) {
    const result = await factory({ commandRunner }).execute([message()], context, {});
    assert.equal(result.output, 'plain output');
    assert.match(calls.at(-1)!.args.at(-1)!, /Task/);
  }
  for (const source of ['options', 'config', 'binding', 'payload']) {
    const result = await createCodexExecutorPlugin({ commandRunner, sessionId: 'existing',
      conversationMode: source === 'options' ? 'fixed' : 'spawn',
      resolveTopicBinding: () => ({ executor: 'codex', ...(source === 'binding' ? { conversationMode: 'fixed' as const } : {}) }),
    }).execute([message('test', source === 'payload' ? { conversationMode: 'fixed' } : {})], context,
      source === 'config' ? { conversationMode: 'fixed' } : {});
    assert.equal(result.executor, 'codex');
    assert.equal(calls.at(-1)!.args[0], 'queue');
  }
  await assert.rejects(createCodexExecutorPlugin({ commandRunner }).execute([message('bad', { conversationMode: 'inbox-only' })], context, {}), /Invalid agent conversationMode/);
  await assert.rejects(createCodexExecutorPlugin({ commandRunner: async () => ({ stdout: '', exitCode: 2 }) }).execute([message()], context, {}), /exited 2/);
});

for (const [host, profileName, defaultExecutor, executors] of [
  ['orca', 'orca-dev', 'orca-spawn', ['orca-spawn', 'orca-wave', 'notify', 'inbox']],
  ['codex', 'codex-dev', 'codex', ['codex', 'agent-pty', 'notify', 'inbox']],
  ['claude-code', 'claude-dev', 'claude-code', ['claude-code', 'agent-pty', 'notify', 'inbox']],
  ['antigravity', 'antigravity-dev', 'antigravity', ['antigravity', 'orca-spawn', 'orca-wave', 'agent-pty', 'notify', 'inbox']],
] as const) {
  test(`${profileName} and ${host} patch validate`, () => {
    const profile = getBuiltInProfile(profileName);
    assert.equal(profile.defaultExecutor, defaultExecutor);
    assert.equal(profile.gateMode, 'approval_required');
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
            '--topic', 'review', '--agent-profile', 'reviewer', '--session-id', 'review-session', '--notify-lark',
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
  const calls: CommandRequest[] = [];
  const runtime = await new DealerNodeRuntime({ stateDir: root, profile: 'headless-relay', member: 'solo',
    transport: { discoveryDir: join(root, 'discovery') }, commandRunner: async request => { calls.push(request); return { stdout: '{}' }; },
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
  assert.deepEqual(calls[0].args.slice(0, 4), ['queue', '--thread', 'review-session', '--resume']);
  assert.deepEqual(calls[1].args.slice(0, 2), ['exec', '--worktree']);
  assert.equal(runtime.mailbox.getWakeTarget('stream:review')?.mode, 'passive');
  runtime.applyAgentPreset({ host: 'claude', conversationMode: 'inbox-only', topic: 'manual', setDefault: true });
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
    const commandRunner: CommandRunner = async call => {
      calls.push(call);
      if (calls.length === 1) {
        assert.equal((await wakeup.flushMailbox('stream:session')).woke, false, 'initial execution must hold its claim');
      }
      return { stdout: '{}', exitCode: fail ? 2 : 0 };
    };
    wakeup = new WakeupRedriveService({ store, commandRunner });
    t.after(() => wakeup.stop());
    const plugin = factory({ store, wakeup, commandRunner, agent: hostAgent, conversationMode: 'fixed',
      cwd: root, sessionId: 'existing-session', agentProfile: 'review', targetBot: 'bot',
      resolveTopicBinding: () => ({ mailboxId: 'stream:session', executor: hostAgent }) });
    await plugin.execute([message('initial')], context, {});
    assert.equal(calls.length, 1);
    assert.equal(store.getWakeTarget('stream:session')?.pendingWake, false);
    assert.equal(store.getWakeTarget('stream:session')?.hostAgent, hostAgent);
    const initial = store.check({ mailboxId: 'stream:session' });
    store.ack(initial.deliveryId!, 'stream:session');
    await wakeup.stop();
    store = new MailboxStore(file);
    wakeup = new WakeupRedriveService({ store, commandRunner });
    const inbox = createInboxExecutorPlugin({ store, wakeup });
    store.upsertWakeTarget('stream:session', { coalesceMs: 60_000 });
    for (const type of ['reply', 'question', 'settle'] as const) {
      const envelope = { ...message(type), semantic: { type, threadId: 'conversation', targetMailbox: 'stream:session', ...(type === 'settle' ? { outcome: 'succeeded' as const } : {}) } };
      await inbox.execute([envelope], context, {});
      const result = await wakeup.flushMailbox('stream:session');
      assert.equal(result.woke, true);
      assert.equal(result.reason, 'Agent host wakeup delivered');
      assert.match(result.pointer!, new RegExp(`Latest \\(${type} from alice, thread conversation\\)`));
      const call = calls.at(-1)!;
      assert.equal(call.command, command);
      assert.equal(call.cwd, root);
      assert.ok(call.args.includes(result.pointer!));
      if (hostAgent === 'codex') assert.deepEqual(call.args.slice(0, 4), ['queue', '--thread', 'existing-session', '--resume']);
      if (hostAgent === 'claude-code') assert.deepEqual(call.args.slice(0, 3), ['--bg', '--resume', 'existing-session']);
      assert.equal(store.getWakeTarget('stream:session')?.pendingWake, false);
      const lease = store.check({ mailboxId: 'stream:session' });
      store.ack(lease.deliveryId!, 'stream:session');
    }
    assert.equal(calls.length, 4);
    fail = true;
    await inbox.execute([{ ...message('failure'), semantic: { type: 'reply', threadId: 'conversation', targetMailbox: 'stream:session' } }], context, {});
    assert.equal((await wakeup.flushMailbox('stream:session')).woke, false);
    assert.equal(store.getWakeTarget('stream:session')?.pendingWake, false);
    assert.equal(store.getWakeTarget('stream:session')?.deliveryInDoubt, true);
    assert.match(store.getWakeTarget('stream:session')!.lastWakeError!, /exited 2/);
    await wakeup.stop();
  }
});

for (const agent of ['codex', 'claude-code'] as const) {
  test(`${agent} spawn transitions to fixed and resumes a leased task with reply preview`, async t => {
    const store = new MailboxStore(join(directory(t), 'mailbox.json'));
    const calls: CommandRequest[] = [];
    const runner: CommandRunner = async call => {
      calls.push(call);
      return { stdout: agent === 'codex' ? '{"threadId":"real-thread-42"}' : '{}' };
    };
    const wakeup = new WakeupRedriveService({ store, commandRunner: runner, flushIntervalMs: 100_000 });
    t.after(() => wakeup.stop());
    await createAgentPtyExecutorPlugin({ store, wakeup, agent, conversationMode: 'spawn', commandRunner: runner })
      .execute([message('msg-1')], context, {});
    const mailboxId = 'task:msg-1';
    assert.deepEqual(calls[0].args.slice(0, agent === 'codex' ? 2 : 3),
      agent === 'codex' ? ['exec', '--worktree'] : ['--bg', '--name', 'task_x3a_msg_x2d_1']);
    assert.equal(store.getWakeTarget(mailboxId)?.conversationMode, 'fixed');
    assert.equal(store.getWakeTarget(mailboxId)?.sessionId, agent === 'codex' ? 'real-thread-42' : 'task_x3a_msg_x2d_1');
    store.check({ mailboxId });
    const reply = { ...message('reply', { mailboxId, wakeupHint: 'Answer ready' }),
      body: 'Use the smaller design', semantic: { type: 'reply' as const, threadId: 'msg-1', targetMailbox: mailboxId } };
    await createInboxExecutorPlugin({ store }).execute([reply], context, {});
    await wakeup.flushMailbox(mailboxId);
    assert.equal(calls.length, 2);
    assert.deepEqual(calls[1].args.slice(0, agent === 'codex' ? 4 : 3),
      agent === 'codex' ? ['queue', '--thread', 'real-thread-42', '--resume'] : ['--bg', '--resume', 'task_x3a_msg_x2d_1']);
    assert.match(calls[1].args.at(-1)!, /\[Answer ready\].*Latest \(reply from alice, thread msg-1\): "Use the smaller design"/);
    assert.equal(store.getWakeTarget(mailboxId)?.pendingWake, false);
  });
}

test('agent executor preserves an in-flight arrival and releases its claim for immediate fixed redrive', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  let entered!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  let redriven!: () => void;
  const second = new Promise<void>(resolve => { redriven = resolve; });
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async call => {
    calls.push(call);
    if (calls.length === 1) { entered(); await held; }
    else redriven();
    return { stdout: '{"threadId":"real-race"}' };
  };
  const wakeup = new WakeupRedriveService({ store, commandRunner: runner, flushIntervalMs: 100_000 });
  t.after(() => wakeup.stop());
  const executing = createCodexExecutorPlugin({ store, wakeup, commandRunner: runner }).execute([message('race')], context, {});
  await started;
  const mailboxId = 'task:race';
  store.check({ mailboxId });
  store.enqueue([{ ...message('new'), body: 'New instruction', semantic: { type: 'reply', threadId: 'race' } }], { mailboxId });
  release();
  await executing;
  await Promise.race([second, new Promise((_, reject) => { const timer = setTimeout(() => reject(new Error('No immediate redrive')), 2000); t.after(() => clearTimeout(timer)); })]);
  await wakeup.flushMailbox(mailboxId);
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[1].args.slice(0, 4), ['queue', '--thread', 'real-race', '--resume']);
  assert.match(calls[1].args.at(-1)!, /New instruction/);
  assert.equal(store.getWakeTarget(mailboxId)?.pendingWake, false);
});

test('ambiguous host launch remains parked across restart and later arrivals', async t => {
  const file = join(directory(t), 'mailbox.json');
  const store = new MailboxStore(file);
  await assert.rejects(createClaudeCodeExecutorPlugin({ store, commandRunner: async () => {
    throw new Error('accepted then connection lost');
  } }).execute([message('recover')], context, {}), /connection lost/);
  assert.equal(store.getWakeTarget('task:recover')?.deliveryInDoubt, true);
  assert.equal(store.getWakeTarget('task:recover')?.pendingWake, false);
  const restarted = new MailboxStore(file);
  const calls: CommandRequest[] = [];
  const wakeup = new WakeupRedriveService({ store: restarted, commandRunner: async call => {
    calls.push(call);
    return { stdout: '{}' };
  } });
  t.after(() => wakeup.stop());
  restarted.enqueue([message('late')], { mailboxId: 'task:recover' });
  assert.equal((await wakeup.flushMailbox('task:recover', { force: true })).woke, false);
  await wakeup.flushReady();
  assert.equal(calls.length, 0);
  assert.equal(restarted.getWakeTarget('task:recover')?.deliveryInDoubt, true);
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
        if (agent === 'codex' && conversationMode === 'spawn') continue;
        const flag = agent === 'codex' ? '--thread' : agent === 'claude-code' ? (conversationMode === 'spawn' ? '--name' : '--resume') : '--session-key';
        assert.equal(args[args.indexOf(flag) + 1], expected);
      }
    }
  }
  const mailboxes = ['team/review', 'team:review', 'team-review', 'team_x3a_review', '团队/review', 'team.review'];
  assert.equal(new Set(mailboxes.map(id => resolveAgentSessionId(id))).size, mailboxes.length);
});

for (const agent of ['codex', 'antigravity'] as const) {
  test(`${agent} without an addressable continuation retains mail without automatic session wakeups`, async t => {
    const store = new MailboxStore(join(directory(t), 'mailbox.json'));
    let calls = 0;
    const runner: CommandRunner = async () => { calls++; return { stdout: '{"accepted":true}' }; };
    const wakeup = new WakeupRedriveService({ store, commandRunner: runner });
    t.after(() => wakeup.stop());
    await createAgentPtyExecutorPlugin({ agent, conversationMode: 'spawn', sessionId: 'unbound-existing', store, wakeup, commandRunner: runner }).execute([message('manual')], context, {});
    const target = store.getWakeTarget('task:manual')!;
    assert.equal(target.hostAgent, undefined);
    assert.equal(target.conversationMode, undefined);
    assert.equal(target.sessionId, undefined);
    store.enqueue([message('reply')], { mailboxId: 'task:manual' });
    await wakeup.flushMailbox('task:manual');
    assert.equal(calls, 1);
    assert.equal(store.check({ mailboxId: 'task:manual' }).items.length, 2);
  });
}

test('aborted host launch retains its unknown-effect fence without claim-release replay', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const controller = new AbortController();
  let calls = 0;
  const runner: CommandRunner = async () => {
    calls++;
    controller.abort();
    throw new DOMException('accepted before abort', 'AbortError');
  };
  const wakeup = new WakeupRedriveService({ store, commandRunner: runner });
  t.after(() => wakeup.stop());
  await assert.rejects(createClaudeCodeExecutorPlugin({ store, wakeup, commandRunner: runner })
    .execute([message('aborted')], { ...context, signal: controller.signal }, {}), /accepted before abort/);
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
  await assert.rejects(createCodexExecutorPlugin({ store, command: join(root, 'missing'), sessionId: 'not-created' })
    .execute([message('pre-effect')], context, {}), CommandNotStartedError);
  const target = store.getWakeTarget('task:pre-effect')!;
  assert.equal(target.deliveryInDoubt, undefined);
  assert.equal(target.pendingWake, true);
  assert.equal(target.attemptCount, 1);
  assert.equal(target.conversationMode, 'spawn');
  assert.ok(target.earliestWakeAt > Date.now());
  const calls: CommandRequest[] = [];
  const service = new WakeupRedriveService({ store, commandRunner: async call => {
    calls.push(call);
    return { stdout: '{"sessionId":"host-returned-identity"}' };
  } });
  t.after(() => service.stop());
  await service.flushMailbox('task:pre-effect', { force: true });
  assert.deepEqual(calls[0].args.slice(0, 2), ['exec', '--worktree']);
  assert.equal(store.getWakeTarget('task:pre-effect')?.sessionId, 'host-returned-identity');
  assert.equal(store.getWakeTarget('task:pre-effect')?.conversationMode, 'fixed');
  assert.equal(store.getWakeTarget('task:pre-effect')?.pendingWake, false);
});
