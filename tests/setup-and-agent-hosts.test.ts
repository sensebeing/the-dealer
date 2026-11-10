import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  AGENT_HOST_CATALOG, buildAgentBindingPreset, probeDealerSetup, runDealerCli,
  decodeNetworkInvite, DealerNodeRuntime, MailboxStore, createAgentPtyExecutorPlugin,
  createOrcaSpawnExecutorPlugin, createOrcaWaveExecutorPlugin,
  defaultAgentLaunchCommand, resolveOrSpawnOrcaWorkspaceTerminal, validateTopicBindings,
} from '../packages/index.ts';
import type { CommandRequest, CommandRunner, MessageEnvelope, SupportedOrcaAgent } from '../packages/index.ts';

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
        return { stdout: '{"accepted":true}' };
      };
      const plugin = createAgentPtyExecutorPlugin({ store, commandRunner, resolveTopicBinding: () => ({ executor: 'agent-pty', agent, conversationMode,
        mailboxId: 'stream:collab.request.v4', agentProfile: 'review', targetBot: 'bot', sessionId: 'session', workspacePath: root }) });
      await plugin.execute([message()], context, {});
      const call = calls[0];
      const args = call.args;
      const prompt = args.at(-1)!;
      assert.match(prompt, new RegExp(mailboxId));
      assert.ok(!prompt.includes('\n'));
      assert.equal(call.cwd, root);
      if (agent === 'codex') {
        assert.equal(call.command, 'codex');
        assert.deepEqual(args.slice(0, conversationMode === 'spawn' ? 2 : 4), conversationMode === 'spawn' ? ['exec', '--worktree'] : ['queue', '--thread', 'session', '--resume']);
        assert.ok(args.includes('-p'));
      } else if (agent === 'claude-code') {
        assert.equal(call.command, 'claude');
        assert.deepEqual(args.slice(0, 3), conversationMode === 'spawn' ? ['--bg', '--name', 'task-test'] : ['--bg', '--resume', 'session']);
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
    stateDir: join(root, 'state'), member: 'solo', profile: 'dedicated-worker',
    transport: { discoveryDir: join(root, 'discovery') },
    patch: { topicBindings: { [preset.topic]: preset.binding } },
    commandRunner: async request => {
      calls.push(request);
      return { stdout: '{}' };
    },
  });
  await runtime.start();
  t.after(() => runtime.stop());
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

test('failed host execution leaves durable mail and does not report success', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const plugin = createAgentPtyExecutorPlugin({ store, agent: 'codex', conversationMode: 'spawn',
    commandRunner: async () => ({ stdout: '', stderr: 'host rejected command', exitCode: 2 }) });
  await assert.rejects(plugin.execute([message('failed')], context, {}), /exited 2/);
  assert.equal(new MailboxStore(join(root, 'mailbox.json')).getWakeTarget('task:failed')?.mode, 'passive');
});

test('wake target host options merge, clear and validate after reload', t => {
  const root = directory(t);
  const file = join(root, 'mailbox.json');
  const store = new MailboxStore(file);
  store.upsertWakeTarget('default', { agent: 'codex', interactiveCommand: 'custom' });
  store.upsertWakeTarget('default', { workspacePath: root });
  assert.equal(new MailboxStore(file).getWakeTarget('default')?.agent, 'codex');
  store.upsertWakeTarget('default', { agent: undefined, interactiveCommand: undefined });
  assert.equal(new MailboxStore(file).getWakeTarget('default')?.interactiveCommand, undefined);
  assert.throws(() => validateTopicBindings({ task: { executor: 'agent-pty', conversationMode: 'inbox-only' } }, ['agent-pty']), /Invalid topic binding/);
  assert.throws(() => validateTopicBindings({ task: { executor: 'orca-wave', agent: 'grok-bot' } }, ['orca-wave']), /Invalid topic binding/);
});
