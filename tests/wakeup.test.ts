import { Kernel } from '../packages/kernel/index.ts';
import { createDispatcherPlugin } from '../packages/dispatcher/index.ts';
import { createDedupCasPolicyPlugin, DEDUP_CAS_DUPLICATE_DETAIL } from '../packages/plugins-policy/index.ts';
import type { NodeService } from '../packages/core-types/index.ts';
import { buildOrcaAgentLaunchCommand, spawnOrcaWorkspaceTerminal, OrcaTerminalLaunchRejectedError, checkOrcaWorkspaceIdle, defaultAgentLaunchCommand, findOrcaWorktreeByName, isAntigravityModePlaceholder, isAutopilotComposerPrefill, sendOrcaTerminalText } from '../packages/orca-workspace/index.ts';
import { CommandNotStartedError, runCommand } from '../packages/subprocess/index.ts';
import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  DealerNodeRuntime,
  MailboxStore,
  WakeupRedriveService,
  createOrcaSpawnExecutorPlugin,
  runDealerCli,
  verifyEnvelope,
  settleWakeDeliveryFailure,
  launchFencedOrcaWorkspaceTerminal,
} from '../packages/index.ts';
import type {
  CommandRequest,
  CommandRunner,
  ExecutionContext,
  MessageEnvelope,
} from '../packages/index.ts';

function directory(t: TestContext): string {
  const root = mkdtempSync(join(tmpdir(), 'dealer-wakeup-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return root;
}

let counter = 0;
function alert(id: string, patch: Partial<MessageEnvelope> = {}): MessageEnvelope {
  counter += 1;
  return {
    id,
    networkId: 'team',
    traceId: id,
    from: { nodeId: 'ed25519:' + 'a'.repeat(64), member: 'sender', nodeName: 'laptop' },
    routing: { mode: 'unicast' },
    kind: 'alerts',
    priority: 'normal',
    subject: `Alert ${counter}`,
    body: 'alert body',
    payload: {},
    attachments: [],
    createdAt: new Date().toISOString(),
    signature: '',
    ...patch,
  };
}

function textOf(call: CommandRequest): string {
  return String(call.args[call.args.indexOf('--text') + 1]);
}

function bodyOf(call: CommandRequest): string {
  return String(call.args[call.args.indexOf('--body') + 1]);
}

function terminalSends(calls: readonly CommandRequest[]): CommandRequest[] {
  return calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'send');
}

function sendsTo(calls: readonly CommandRequest[], handle: string): CommandRequest[] {
  return terminalSends(calls).filter(
    call => String(call.args[call.args.indexOf('--terminal') + 1]) === handle,
  );
}

interface FakeTerminalRow {
  orphaned?: boolean;
  recordedPaneKey?: string;
  tabId?: string;
  leafId?: string;
  handle: string;
  connected: boolean;
  writable: boolean;
  agentIdentity?: string;
  preview?: string;
}

/** Minimal Orca inventory: one fixed workspace whose `terminal create` adds a live tab. */
function fakeOrcaWorkspace(workspacePath: string, initial: FakeTerminalRow[] = []) {
  const state = {
    calls: [] as CommandRequest[],
    terminals: [...initial],
    waitOk: true,
    created: 0,
    agents: initial.length ? [{ state: 'done' } as { state: string; paneKey?: string }] : [],
  };
  const runner: CommandRunner = async request => {
    state.calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'ps') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            truncated: false,
            worktrees: [{
              path: workspacePath,
              worktreeId: 'fake-worktree',
              childWorktreeIds: [],
              unverifiableTerminalCount: 0,
              agents: state.agents,
            }],
          },
        }),
      };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'list') {
      return { stdout: JSON.stringify({ ok: true, result: { terminals: state.terminals } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'create') {
      state.created += 1;
      const handle = `term-new-${state.created}`;
      state.terminals.push({ handle, connected: true, writable: true, agentIdentity: 'antigravity', preview: '', tabId: handle, leafId: 'root' });
      state.agents.push({ state: 'done', paneKey: `${handle}:root` });
      return { stdout: JSON.stringify({ ok: true, result: { terminal: { handle } } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'wait') {
      return { stdout: JSON.stringify({ ok: state.waitOk }) };
    }
    return { stdout: '{"ok":true}' };
  };
  return { runner, state };
}

async function cli(runtime: DealerNodeRuntime, ...args: string[]) {
  const out: string[] = [];
  const err: string[] = [];
  const code = await runDealerCli([...args, '--json'], {
    runtime,
    stdout: line => out.push(line),
    stderr: line => err.push(line),
  });
  assert.equal(code, 0, err.join('\n') || out.join('\n'));
  return JSON.parse(out.join('\n'));
}

async function until(check: () => boolean, message: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!check()) {
    assert.ok(Date.now() < deadline, `Timed out: ${message}`);
    await new Promise(resolve => setTimeout(resolve, 25));
  }
}

async function makeNode(
  t: TestContext,
  root: string,
  member: string,
  options: {
    patch?: Record<string, unknown>;
    commandRunner?: CommandRunner;
  } = {},
) {
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, member),
    member,
    name: member,
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
    commandRunner:
      options.commandRunner ??
      (async () => {
        throw new Error('Unexpected external command');
      }),
    patch: { defaultExecutor: 'inbox', ...(options.patch ?? {}) },
  }).start();
  t.after(() => runtime.stop());
  return runtime;
}

test('daemon restart preserves busy waves and delivers exactly one single-line pointer', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    return { stdout: '{"ok":true}' };
  };
  let idle = false;
  const stateDir = join(root, 'state');
  const workspacePath = join(root, 'workspace');
  const make = () =>
    new DealerNodeRuntime({
      stateDir,
      member: 'worker',
      name: 'worker',
      profile: 'dedicated-worker',
      transport: { discoveryDir: join(root, 'mesh') },
      commandRunner: runner,
      wakeupIdleCheck: async () => ({ idle }),
      patch: {
        defaultExecutor: 'orca-wave',
        executorConfig: {
          'orca-wave': { runId: 'run-alerts', workspacePath },
        },
        wave: { flushIntervalMs: 100_000 },
      },
    });
  const first = await make().start();
  const emit = () =>
    first.emitter.emit({
      networkId: 'local',
      routing: { mode: 'unicast', targets: ['member:worker'] },
      kind: 'alerts',
      subject: 'Busy alert',
      body: 'details',
      payload: {
        executor: 'orca-wave',
        streamKey: 'alerts',
        runId: 'run-alerts',
        workspacePath,
      },
    });
  assert.equal((await emit()).receipts[0].stage, 'dispatched');
  assert.equal((await emit()).receipts[0].stage, 'dispatched');
  assert.equal(calls.length, 0, 'a busy workspace must never receive text');
  assert.equal(first.mailbox.list({ mailboxId: 'stream:alerts' }).queuedCount, 2);
  assert.equal(first.mailbox.getWakeTarget('stream:alerts')!.pendingWake, true);
  await first.stop();

  calls.length = 0;
  idle = true;
  const second = await make().start();
  t.after(() => second.stop());
  await second.flushWakeups();
  const orchestration = calls.filter(call => call.args[0] === 'orchestration');
  assert.equal(orchestration.length, 1);
  assert.equal(
    orchestration[0].args[orchestration[0].args.indexOf('--subject') + 1],
    'Dealer wakeup (stream:alerts)',
  );
  const pointer = bodyOf(orchestration[0]);
  assert.match(pointer, /^You have 2 Dealer message\(s\) in stream:alerts/);
  assert.equal(/[\r\n]/.test(pointer), false);
  assert.equal(second.mailbox.getWakeTarget('stream:alerts')!.pendingWake, false);

  const lease = second.mailbox.check({ mailboxId: 'stream:alerts' });
  assert.match(lease.deliveryId!, /^dlv_[0-9a-f]{12}$/);
  assert.equal(lease.items.length, 2);
  assert.equal(new Set(lease.items.map(item => item.leaseId)).size, 1);
});

test('unacked recovery pointer suppresses rotation, and the acknowledged wave opens a fresh session tab', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'workspace');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-alerts', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const service = new WakeupRedriveService({
    store,
    commandRunner: runner,
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());
  const enqueue = (ids: readonly string[]) =>
    store.enqueue(ids.map(id => alert(id)), {
      mailboxId: 'stream:alerts',
      wakeTarget: {
        mode: 'orca-terminal',
        terminalHandle: 'term-alerts',
        workspacePath,
        coalesceMs: 0,
        rotateAfterWaves: 1,
        workspaceComment: 'Dealer: alerts',
      },
    });

  enqueue(['a', 'b']);
  const delivered = await service.flushMailbox('stream:alerts');
  const firstSends = sendsTo(state.calls, 'term-alerts');
  assert.equal(firstSends.length, 1, JSON.stringify(delivered));
  assert.match(textOf(firstSends[0]), /^You have 2 Dealer message\(s\) in stream:alerts/);
  assert.equal(terminalSends(state.calls).some(call => textOf(call) === '/new'), false);

  const lease = store.check({ mailboxId: 'stream:alerts' });
  enqueue(['c']);
  await service.flushMailbox('stream:alerts');
  const dual = sendsTo(state.calls, 'term-alerts').at(-1)!;
  assert.match(
    textOf(dual),
    new RegExp(`^Unacknowledged Dealer batch ${lease.deliveryId} \\+ 1 newer message\\(s\\) in stream:alerts \\[`),
  );
  assert.equal(terminalSends(state.calls).some(call => textOf(call) === '/new'), false);
  assert.equal(state.created, 0, 'an unacknowledged batch never rotates');

  store.ack(lease.deliveryId!);
  await service.flushMailbox('stream:alerts');
  assert.equal(state.created, 1, 'the acknowledged wave opens exactly one new session tab');
  const created = state.calls.find(call => call.args[0] === 'terminal' && call.args[1] === 'create')!;
  assert.deepEqual(created.args, [
    'terminal', 'create', '--worktree', `path:${workspacePath}`,
    '--command', defaultAgentLaunchCommand(), '--json',
  ]);
  const rotated = sendsTo(state.calls, 'term-new-1');
  assert.equal(rotated.length, 1);
  assert.match(textOf(rotated[0]), /^You have 1 Dealer message\(s\) in stream:alerts/);
  assert.equal(/[\r\n]/.test(textOf(rotated[0])), false);
  assert.equal(store.getWakeTarget('stream:alerts')!.terminalHandle, 'term-new-1');
  assert.equal(store.getWakeTarget('stream:alerts')!.completedWaves, 0);
  // The retained tab keeps its history: it is never reset, cleared or written to again.
  assert.equal(sendsTo(state.calls, 'term-alerts').length, 2);
  assert.equal(terminalSends(state.calls).some(call => textOf(call) === '/new'), false);
  assert.equal(terminalSends(state.calls).some(call => textOf(call) === '\x15'), false);
});

test('rotation persists the new session tab before tui-idle and the next tick reuses it', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'fixed');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-rotate', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const service = new WakeupRedriveService({
    store,
    commandRunner: runner,
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());
  store.enqueue([alert('rotate-now')], {
    mailboxId: 'stream:rotate',
    wakeTarget: {
      mode: 'orca-terminal',
      workspacePath,
      terminalHandle: 'term-rotate',
      sessionId: 'retired-session', paneKey: 'retired:pane',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      completedWaves: 1,
    },
  });

  state.waitOk = false;
  const first = await service.flushMailbox('stream:rotate');
  assert.equal(first.woke, false);
  assert.equal(first.rotated, true);
  assert.equal(first.reason, 'Session rotated; waiting for tui-idle before wakeup');
  assert.equal(state.created, 1);
  assert.equal(store.getWakeTarget('stream:rotate')!.terminalHandle, 'term-new-1');
  assert.equal(store.getWakeTarget('stream:rotate')!.completedWaves, 0);
  assert.equal(store.getWakeTarget('stream:rotate')!.sessionId, undefined);
  assert.equal(store.getWakeTarget('stream:rotate')!.paneKey, undefined);
  const create = state.calls.find(call => call.args[1] === 'create')!;
  assert.equal(create.args[create.args.indexOf('--command') + 1], 'agy --dangerously-skip-permissions');
  assert.equal(store.getWakeTarget('stream:rotate')!.pendingWake, true);
  assert.equal(terminalSends(state.calls).length, 0, 'no pointer is written before the TUI is idle');

  state.waitOk = true;
  const second = await service.flushMailbox('stream:rotate', { force: true });
  assert.equal(second.woke, true);
  assert.equal(second.rotated, false);
  assert.equal(state.created, 1, 'the persisted tab is reused instead of creating another');
  const pointer = terminalSends(state.calls).at(-1)!;
  assert.equal(pointer.args[pointer.args.indexOf('--terminal') + 1], 'term-new-1');
  assert.match(textOf(pointer), /^You have 1 Dealer message\(s\) in stream:rotate/);
  assert.equal(store.getWakeTarget('stream:rotate')!.pendingWake, false);
  assert.equal(store.getWakeTarget('stream:rotate')!.completedWaves, 0);
  assert.equal(terminalSends(state.calls).some(call => textOf(call) === '/new'), false);
});

for (const retainedComposer of [
  '> unsent draft',
  '• ,1（主推）：继续检查剩余任务\n> 继续检查剩余任务',
]) {
test(`the per-wave default opens a session tab and never hijacks a retained composer ${JSON.stringify(retainedComposer)}`, async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'wave');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-human', connected: true, writable: true, agentIdentity: 'antigravity', preview: retainedComposer },
  ]);
  const service = new WakeupRedriveService({
    store,
    commandRunner: runner,
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());
  const enqueue = (ids: readonly string[]) =>
    store.enqueue(ids.map(id => alert(id)), {
      mailboxId: 'stream:wave',
      wakeTarget: { mode: 'orca-terminal', workspacePath, coalesceMs: 0, rotateAfterWaves: 1 },
    });

  enqueue(['a']);
  const first = await service.flushMailbox('stream:wave');
  assert.equal(first.woke, true, first.reason);
  assert.equal(state.created, 1);
  assert.equal(sendsTo(state.calls, 'term-human').length, 0, 'a human tab is never hijacked');
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
  assert.equal(terminalSends(state.calls).some(call => textOf(call) === '\x15'), false, 'a fresh tab needs no composer clear');
  assert.equal(store.getWakeTarget('stream:wave')!.terminalHandle, 'term-new-1');

  // Messages merged before the acknowledgement keep using this wave's own tab.
  enqueue(['b']);
  await service.flushMailbox('stream:wave', { force: true });
  assert.equal(state.created, 1, 'the merged message does not open another tab');
  const merged = sendsTo(state.calls, 'term-new-1').at(-1)!;
  assert.match(textOf(merged), /^You have 2 Dealer message\(s\) in stream:wave/);

  // A tab the user closed mid-wave is replaced instead of falling back to their other tabs.
  state.terminals = state.terminals.filter(terminal => terminal.handle !== 'term-new-1');
  enqueue(['c']);
  await service.flushMailbox('stream:wave', { force: true });
  assert.equal(state.created, 2);
  assert.equal(sendsTo(state.calls, 'term-human').length, 0);
  assert.equal(store.getWakeTarget('stream:wave')!.terminalHandle, 'term-new-2');
  assert.ok(sendsTo(state.calls, 'term-new-2').length >= 1);
  assert.equal(terminalSends(state.calls).some(call => textOf(call) === '/new'), false);
});
}

test('an explicit rotateAfterWaves above one reuses the bound tab until the threshold', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'alerts');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-alerts', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const service = new WakeupRedriveService({
    store,
    commandRunner: runner,
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());
  const enqueue = (id: string) =>
    store.enqueue([alert(id)], {
      mailboxId: 'stream:alerts',
      wakeTarget: {
        mode: 'orca-terminal', terminalHandle: 'term-alerts', workspacePath,
        coalesceMs: 0, rotateAfterWaves: 2,
      },
    });
  const ack = () => {
    const lease = store.check({ mailboxId: 'stream:alerts' });
    store.ack(lease.deliveryId!);
  };

  enqueue('one');
  await service.flushMailbox('stream:alerts');
  ack();
  assert.equal(store.getWakeTarget('stream:alerts')!.completedWaves, 1);
  enqueue('two');
  await service.flushMailbox('stream:alerts');
  assert.equal(state.created, 0, 'waves below the threshold reuse the bound tab');
  assert.equal(sendsTo(state.calls, 'term-alerts').length, 2);
  ack();
  assert.equal(store.getWakeTarget('stream:alerts')!.completedWaves, 2);
  enqueue('three');
  await service.flushMailbox('stream:alerts');
  assert.equal(state.created, 1);
  assert.equal(store.getWakeTarget('stream:alerts')!.terminalHandle, 'term-new-1');
  assert.equal(store.getWakeTarget('stream:alerts')!.completedWaves, 0);
  assert.match(textOf(sendsTo(state.calls, 'term-new-1')[0]), /^You have 1 Dealer message\(s\) in stream:alerts/);
  assert.equal(terminalSends(state.calls).some(call => textOf(call) === '/new'), false);
});

for (const rotateAfterWaves of [1, 2] as const) {
  test(`rotation opens a new session tab despite an unsent draft in the retained tab (rotateAfterWaves: ${rotateAfterWaves})`, async t => {
    const root = directory(t);
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const workspacePath = join(root, 'retained');
    const { runner, state } = fakeOrcaWorkspace(workspacePath, [
      { handle: 'term-retained', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
    ]);
    const service = new WakeupRedriveService({
      store,
      commandRunner: runner,
      flushIntervalMs: 100_000,
    });
    t.after(() => service.stop());
    const enqueue = (id: string) =>
      store.enqueue([alert(id)], {
        mailboxId: 'stream:retained',
        wakeTarget: {
          mode: 'orca-terminal', terminalHandle: 'term-retained', workspacePath,
          coalesceMs: 0, rotateAfterWaves,
        },
      });
    const ack = () => {
      const lease = store.check({ mailboxId: 'stream:retained' });
      store.ack(lease.deliveryId!);
    };

    // Fill the bound tab's rotation window; every wave is acknowledged before the next arrives.
    for (let wave = 1; wave <= rotateAfterWaves; wave += 1) {
      enqueue(`wave-${wave}`);
      const delivered = await service.flushMailbox('stream:retained');
      assert.equal(delivered.woke, true, delivered.reason);
      ack();
    }
    assert.equal(store.getWakeTarget('stream:retained')!.completedWaves, rotateAfterWaves);
    const retainedSends = sendsTo(state.calls, 'term-retained').length;
    assert.equal(retainedSends, rotateAfterWaves);

    // A human leaves an unsent draft in the retained tab after acknowledging the wave.
    state.terminals = state.terminals.map(terminal =>
      terminal.handle === 'term-retained' ? { ...terminal, preview: '> unsent draft' } : terminal);

    enqueue('after-ack');
    const rotated = await service.flushMailbox('stream:retained');
    assert.equal(rotated.woke, true, rotated.reason);
    assert.equal(rotated.rotated, true);
    assert.equal(state.created, 1, 'the retained draft must not block rotation');
    assert.equal(
      sendsTo(state.calls, 'term-retained').length,
      retainedSends,
      'the retained tab is never written to again',
    );
    assert.equal(store.getWakeTarget('stream:retained')!.terminalHandle, 'term-new-1');
    assert.equal(store.getWakeTarget('stream:retained')!.completedWaves, 0);
    const fresh = sendsTo(state.calls, 'term-new-1');
    assert.equal(fresh.length, 1);
    assert.match(textOf(fresh[0]), /^You have 1 Dealer message\(s\) in stream:retained/);
    assert.equal(terminalSends(state.calls).some(call => textOf(call) === '/new'), false);
  });
}

test('orca-spawn never injects after a failed idle wait and redrive sends the task preamble', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  let idle = false;
  let waitOk = false;
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'create') {
      return {
        stdout: JSON.stringify({ ok: true, result: { worktree: { path: join(root, 'child') } } }),
      };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'create') {
      return {
        stdout: JSON.stringify({ ok: true, result: { terminal: { handle: 'term-worker' } } }),
      };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'wait') {
      return {
        stdout: JSON.stringify(
          waitOk ? { ok: true } : { ok: false, error: { code: 'timeout' } },
        ),
      };
    }
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({
    store,
    commandRunner: runner,
    idleCheck: async () => ({ idle }),
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());
  const executor = createOrcaSpawnExecutorPlugin({
    commandRunner: runner,
    command: 'codex',
    cwd: join(root, 'workspace'),
    store,
    wakeup: service,
  });

  const timedOut = alert('task-timeout', { payload: { interactive: true } });
  await executor.execute([timedOut], {} as ExecutionContext);
  assert.equal(terminalSends(calls).length, 0, 'must not send on a failed idle wait');
  assert.equal(store.getWakeTarget('task:task-timeout')!.pendingWake, true);
  assert.equal(store.list({ mailboxId: 'task:task-timeout' }).queuedCount, 1);

  await until(() => store.getWakeTarget('task:task-timeout')!.attemptCount > 0, 'claim release probes busy target');
  idle = true;
  await service.flushMailbox('task:task-timeout', { force: true });
  const preambleSends = terminalSends(calls);
  assert.equal(preambleSends.length, 1);
  const preamble = textOf(preambleSends[0]);
  assert.match(
    preamble,
    /^alert body — Dealer Task dispatched to mailbox task:task-timeout \(Message ID: task-timeout\)\./,
  );
  assert.equal(/[\r\n]/.test(preamble), false);
  assert.ok(preamble.includes('dealer ask --reply-to task-timeout'));
  assert.ok(preamble.includes('dealer settle task-timeout --outcome succeeded'));
  assert.equal(store.getWakeTarget('task:task-timeout')!.pendingWake, false);

  calls.length = 0;
  waitOk = true;
  const ready = alert('task-ready', { payload: { interactive: true } });
  await executor.execute([ready], {} as ExecutionContext);
  const directSends = terminalSends(calls);
  assert.equal(directSends.length, 1);
  assert.match(
    textOf(directSends[0]),
    /^alert body — Dealer Task dispatched to mailbox task:task-ready \(Message ID: task-ready\)\./,
  );
  assert.equal(store.getWakeTarget('task:task-ready')!.pendingWake, false);

  const promptCases = [
    { terminalPrompt: '  Custom\n task\r\n prompt  ', body: 'Body', subject: 'Subject', lead: 'Custom task prompt' },
    { terminalPrompt: ' \n ', body: '  Body\n details  ', subject: 'Subject', lead: 'Body details' },
    { terminalPrompt: 42, body: '', subject: '  Subject\n fallback ', lead: 'Subject fallback' },
    { terminalPrompt: undefined, body: '', subject: '', lead: '' },
  ];
  for (const [index, promptCase] of promptCases.entries()) {
    for (const direct of [false, true]) {
      calls.length = 0;
      waitOk = direct;
      const message = alert(`prompt-${index}-${direct}`, {
        body: promptCase.body,
        subject: promptCase.subject,
        payload: { interactive: true, terminalPrompt: promptCase.terminalPrompt },
      });
      await executor.execute([message], {} as ExecutionContext);
      if (!direct) {
        assert.equal(terminalSends(calls).length, 0);
        await service.flushMailbox(`task:${message.id}`);
      }
      const sends = terminalSends(calls);
      assert.equal(sends.length, 1);
      const text = textOf(sends[0]);
      const prefix = promptCase.lead ? `${promptCase.lead} — ` : '';
      assert.ok(text.startsWith(`${prefix}Dealer Task dispatched to mailbox task:${message.id}`));
      assert.equal(/[\r\n]/.test(text), false);
    }
  }
});

test('single-machine loopback send -> ask -> reply -> settle keeps both thread legs', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'node'),
    member: 'alice',
    name: 'alice',
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
    commandRunner: async request => {
      calls.push(request);
      if (request.args[0] === 'worktree' && request.args[1] === 'ps') return { stdout: '{"ok":true,"result":{"worktrees":[]}}' };
      return { stdout: '{"ok":true}' };
    },
    wakeupIdleCheck: async () => ({ idle: true }),
    patch: { defaultExecutor: 'inbox', gateMode: 'open' },
  }).start();
  t.after(() => runtime.stop());

  const sent = await cli(
    runtime,
    'send',
    '--network',
    'local',
    '--to',
    'member:alice',
    '--topic',
    'triage',
    '--mailbox',
    'task:w1',
    '--reply-mailbox',
    'worktree:coord',
    '--subject',
    'Please fix',
    '--body',
    'Investigate the bug',
  );
  assert.equal(sent.receipts[0].stage, 'dispatched');
  const messageId = String(sent.messageId);
  assert.equal(runtime.mailbox.list({ mailboxId: 'task:w1' }).queuedCount, 1);
  const outboundLeg = runtime.mailbox
    .listThreadRecords(messageId)
    .find(record => record.direction === 'outbound')!;
  assert.equal(outboundLeg.localMailbox, 'worktree:coord');
  assert.equal(outboundLeg.peerReplyMailbox, 'task:w1');

  const checked = await cli(runtime, 'inbox', 'check', '--mailbox', 'task:w1');
  const asked = await cli(
    runtime,
    'ask',
    '--reply-to',
    messageId,
    '--body',
    'Which branch?',
    '--ack',
    String(checked.deliveryId),
  );
  assert.equal(asked.threadId, messageId);
  assert.equal(asked.targetMailbox, 'worktree:coord');
  assert.equal(asked.replyMailbox, 'task:w1');
  assert.equal(runtime.mailbox.list({ mailboxId: 'worktree:coord' }).queuedCount, 1);

  // The inbound leg must never be clobbered by the emitter's outbound leg on loopback.
  const inboundLeg = runtime.mailbox
    .listThreadRecords(messageId)
    .find(record => record.direction === 'inbound')!;
  assert.equal(inboundLeg.localMailbox, 'task:w1');
  assert.equal(inboundLeg.peerReplyMailbox, 'worktree:coord');
  const route = runtime.mailbox.resolveReplyTarget(String(asked.questionId))!;
  assert.equal(route.targetMailbox, 'task:w1');
  assert.equal(route.localMailbox, 'worktree:coord');

  const coordinator = await cli(runtime, 'inbox', 'check', '--mailbox', 'worktree:coord');
  assert.deepEqual(
    coordinator.items.map((item: MessageEnvelope) => item.id),
    [asked.questionId],
  );

  runtime.mailbox.upsertWakeTarget('task:w1', {
    mode: 'orca-terminal',
    terminalHandle: 'term-w1',
    workspacePath: join(root, 'workspace-w1'),
    coalesceMs: 0,
  });
  const replied = await cli(
    runtime,
    'reply',
    String(asked.questionId),
    '--body',
    'Use main',
    '--ack',
    String(coordinator.deliveryId),
  );
  assert.equal(replied.targetMailbox, 'task:w1');
  await runtime.flushWakeups();
  const wakePointer = terminalSends(calls)
    .map(textOf)
    .find(text => text.includes('task:w1'))!;
  assert.match(wakePointer, /^You have 1 Dealer message\(s\) in task:w1/);
  assert.equal(/[\r\n]/.test(wakePointer), false);

  const replyCheck = await cli(runtime, 'inbox', 'check', '--mailbox', 'task:w1');
  assert.deepEqual(
    replyCheck.items.map((item: MessageEnvelope) => item.id),
    [replied.replyId],
  );
  const settled = await cli(
    runtime,
    'settle',
    messageId,
    '--outcome',
    'succeeded',
    '--summary',
    'All done',
    '--ack',
    String(replyCheck.deliveryId),
  );
  assert.equal(settled.outcome, 'succeeded');
  assert.equal(settled.targetMailbox, 'worktree:coord');

  const thread = await cli(runtime, 'thread', 'show', messageId);
  assert.equal(thread.threadId, messageId);
  assert.equal(thread.state, 'settled');
  assert.equal(thread.settlement.outcome, 'succeeded');
  assert.equal(thread.settlement.summary, 'All done');
  assert.ok(
    thread.records.some(
      (record: { direction: string; messageId: string }) =>
        record.direction === 'inbound' && record.messageId === messageId,
    ),
  );
  assert.ok(
    thread.records.some(
      (record: { direction: string; messageId: string }) =>
        record.direction === 'outbound' && record.messageId === messageId,
    ),
  );
  const storedSettlement = runtime.mailbox
    .list({ mailboxId: 'worktree:coord' })
    .items.find(item => item.id === settled.settlementId)!;
  assert.equal(storedSettlement.envelope.semantic?.type, 'settle');
  assert.equal(storedSettlement.envelope.semantic?.outcome, 'succeeded');
});

test('session commands honor an explicit --mailbox when acknowledging a lease', async t => {
  const root = directory(t);
  const runtime = await new DealerNodeRuntime({
    stateDir: join(root, 'node'),
    member: 'alice',
    name: 'alice',
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh') },
    commandRunner: async request => request.args[0] === 'worktree' && request.args[1] === 'ps'
      ? { stdout: '{"ok":true,"result":{"worktrees":[]}}' }
      : { stdout: '{"ok":true}' },
    wakeupIdleCheck: async () => ({ idle: true }),
    patch: { defaultExecutor: 'inbox', gateMode: 'open' },
  }).start();
  t.after(() => runtime.stop());

  const sent = await cli(
    runtime,
    'send',
    '--network',
    'local',
    '--to',
    'member:alice',
    '--topic',
    'triage',
    '--mailbox',
    'task:w1',
    '--reply-mailbox',
    'worktree:coord',
    '--body',
    'Investigate',
  );
  const messageId = String(sent.messageId);

  runtime.mailbox.enqueue([alert('ack-ask')], { mailboxId: 'custom:ask' });
  const askLease = runtime.mailbox.check({ mailboxId: 'custom:ask' });
  const asked = await cli(
    runtime,
    'ask',
    '--reply-to',
    messageId,
    '--body',
    'Which branch?',
    '--mailbox',
    'custom:ask',
    '--ack',
    String(askLease.deliveryId),
  );
  assert.equal(asked.targetMailbox, 'worktree:coord');
  assert.equal(asked.replyMailbox, 'custom:ask');
  assert.equal(
    runtime.mailbox
      .list({ mailboxId: 'custom:ask', includeAcked: true })
      .items.find(item => item.id === 'ack-ask')!.status,
    'acked',
  );

  runtime.mailbox.enqueue([alert('ack-reply')], { mailboxId: 'custom:reply' });
  const replyLease = runtime.mailbox.check({ mailboxId: 'custom:reply' });
  const replied = await cli(
    runtime,
    'reply',
    messageId,
    '--body',
    'Use main',
    '--mailbox',
    'custom:reply',
    '--ack',
    String(replyLease.deliveryId),
  );
  assert.equal(replied.targetMailbox, 'worktree:coord');
  assert.equal(
    runtime.mailbox
      .list({ mailboxId: 'custom:reply', includeAcked: true })
      .items.find(item => item.id === 'ack-reply')!.status,
    'acked',
  );
});

test('two-node P2P closed loop verifies signatures and keeps unapproved knocks route-free', async t => {
  const root = directory(t);
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'ps') return { stdout: '{"ok":true,"result":{"worktrees":[]}}' };
    return { stdout: '{"ok":true}' };
  };
  const alice = await makeNode(t, root, 'alice', { commandRunner: runner });
  const bob = await makeNode(t, root, 'bob', {
    commandRunner: runner,
    patch: { gateMode: 'approval_required' },
  });
  await alice.mountNetwork({ networkId: 'team', passphrase: 'team-secret' });
  await bob.mountNetwork({ networkId: 'team', passphrase: 'team-secret' });
  await until(
    () => alice.node.listPeers().length === 1 && bob.node.listPeers().length === 1,
    'peer discovery',
  );

  const sent = await cli(
    alice,
    'send',
    '--network',
    'team',
    '--to',
    'member:bob',
    '--topic',
    'triage',
    '--mailbox',
    'task:bob1',
    '--reply-mailbox',
    'worktree:alice',
    '--subject',
    'Please review',
    '--body',
    'Review the change',
  );
  const messageId = String(sent.messageId);
  assert.equal(sent.receipts[0].stage, 'pending_approval');
  assert.equal(bob.gate.listPendingApprovals().length, 1);
  assert.equal(bob.mailbox.getThread(messageId), undefined);
  assert.equal(bob.mailbox.list({ mailboxId: 'task:bob1' }).totalCount, 0);
  assert.equal(alice.mailbox.getThread(messageId)!.direction, 'outbound');

  await cli(bob, 'gate', 'approve', messageId);
  const inboundLeg = bob.mailbox.getThread(messageId)!;
  assert.equal(inboundLeg.direction, 'inbound');
  assert.equal(inboundLeg.localMailbox, 'task:bob1');
  assert.equal(inboundLeg.peerReplyMailbox, 'worktree:alice');
  const delivered = bob.mailbox.list({ mailboxId: 'task:bob1' }).items[0];
  assert.ok(verifyEnvelope(delivered.envelope));
  assert.equal(delivered.envelope.from.nodeId, alice.transport.localNodeId);

  await cli(bob, 'gate', 'mode', 'open', '--network', 'team');
  const checked = await cli(bob, 'inbox', 'check', '--mailbox', 'task:bob1');
  const asked = await cli(
    bob,
    'ask',
    '--reply-to',
    messageId,
    '--body',
    'Which test?',
    '--ack',
    String(checked.deliveryId),
  );
  assert.equal(asked.targetMailbox, 'worktree:alice');
  assert.equal(asked.replyMailbox, 'task:bob1');
  assert.equal(asked.receipts[0].stage, 'dispatched');
  assert.equal(asked.receipts[0].fromNodeId, alice.transport.localNodeId);

  const aliceInbox = await cli(alice, 'inbox', 'check', '--mailbox', 'worktree:alice');
  assert.deepEqual(
    aliceInbox.items.map((item: MessageEnvelope) => item.id),
    [asked.questionId],
  );
  const questionItem = alice.mailbox
    .list({ mailboxId: 'worktree:alice' })
    .items.find(item => item.id === asked.questionId)!;
  assert.ok(verifyEnvelope(questionItem.envelope));

  const replied = await cli(
    alice,
    'reply',
    String(asked.questionId),
    '--body',
    'The integration test',
    '--ack',
    String(aliceInbox.deliveryId),
  );
  assert.equal(replied.targetMailbox, 'task:bob1');
  assert.equal(replied.receipts[0].stage, 'dispatched');
  assert.equal(replied.receipts[0].fromNodeId, bob.transport.localNodeId);

  const bobInbox = await cli(bob, 'inbox', 'check', '--mailbox', 'task:bob1');
  assert.deepEqual(
    bobInbox.items.map((item: MessageEnvelope) => item.id),
    [replied.replyId],
  );
  const replyItem = bob.mailbox
    .list({ mailboxId: 'task:bob1' })
    .items.find(item => item.id === replied.replyId)!;
  assert.ok(verifyEnvelope(replyItem.envelope));

  const settled = await cli(
    bob,
    'settle',
    messageId,
    '--outcome',
    'succeeded',
    '--summary',
    'All checks passed',
    '--ack',
    String(bobInbox.deliveryId),
  );
  assert.equal(settled.targetMailbox, 'worktree:alice');
  assert.equal(settled.receipts[0].stage, 'dispatched');
  assert.equal(settled.receipts[0].fromNodeId, alice.transport.localNodeId);

  const aliceThread = await cli(alice, 'thread', 'show', messageId);
  assert.equal(aliceThread.state, 'settled');
  assert.equal(aliceThread.settlement.outcome, 'succeeded');
  assert.equal(aliceThread.settlement.summary, 'All checks passed');
  const bobThread = await cli(bob, 'thread', 'show', messageId);
  assert.equal(bobThread.state, 'settled');
  assert.equal(bobThread.settlement.outcome, 'succeeded');
  assert.ok(
    aliceThread.records.some(
      (record: { peerNodeId: string }) => record.peerNodeId === bob.transport.localNodeId,
    ),
  );
  assert.ok(
    bobThread.records.some(
      (record: { peerNodeId: string }) => record.peerNodeId === alice.transport.localNodeId,
    ),
  );
});

test('Orca caller bindings provide zero-mailbox P2P sessions and control RPC forwarding', async t => {
  const root = directory(t);
  const aliceWtPath = join(root, 'alice-wt');
  const bobWtPath = join(root, 'bob-wt');
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'ps') {
      return { stdout: JSON.stringify({ ok: true, result: { worktrees: [
        { path: aliceWtPath, worktreeId: `repo::${aliceWtPath}`, displayName: 'alice-wt', isArchived: false, unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
        { path: bobWtPath, worktreeId: `repo::${bobWtPath}`, displayName: 'bob-wt', isArchived: false, unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
      ] } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'list') {
      const path = request.args[request.args.indexOf('--worktree') + 1]?.replace(/^path:/, '');
      const handle = path === bobWtPath ? 'term-bob' : 'term-alice';
      return { stdout: JSON.stringify({ ok: true, result: { terminals: [{ handle, connected: true, writable: true, preview: '' }] } }) };
    }
    return { stdout: JSON.stringify({ ok: true }) };
  };
  const alice = await makeNode(t, root, 'alice', { commandRunner: runner });
  const bob = await makeNode(t, root, 'bob', { commandRunner: runner });
  await alice.mountNetwork({ networkId: 'team', passphrase: 'team-secret' });
  await bob.mountNetwork({ networkId: 'team', passphrase: 'team-secret' });
  await until(() => alice.node.listPeers().length === 1 && bob.node.listPeers().length === 1, 'peer discovery');
  for (const runtime of [alice, bob]) {
    await runtime.serveControl(async (argv, context) => {
      const stdout: string[] = [];
      const stderr: string[] = [];
      const code = await runDealerCli(argv, { runtime, signal: context.signal, stdout: line => stdout.push(line), stderr: line => stderr.push(line) });
      return { code, stdout, stderr };
    });
  }
  const aliceEnv = { ANTIGRAVITY_CONVERSATION_ID: 'alice-session', ORCA_PANE_KEY: 'alice:pane', ORCA_TERMINAL_HANDLE: 'term-alice', ORCA_WORKTREE_ID: `repo::${aliceWtPath}` };
  const bobEnv = { CLAUDE_CODE_SESSION_ID: 'bob-session', ORCA_PANE_KEY: 'bob:pane', ORCA_TERMINAL_HANDLE: 'term-bob', ORCA_WORKTREE_ID: `repo::${bobWtPath}` };
  const askOut: string[] = [];
  await runDealerCli(['ask', '--network', 'team', '--to', 'member:bob', '--target-mailbox', 'worktree:bob-wt', '--body', 'Please review'], { stateDir: alice.stateDir, env: aliceEnv, stdout: line => askOut.push(line) });
  const asked = JSON.parse(askOut.join('\n'));
  assert.equal(asked.replyMailbox, 'worktree:alice-wt');
  assert.equal(alice.mailbox.getWakeTarget('worktree:alice-wt')?.mode, 'orca-terminal');
  assert.equal(alice.mailbox.getWakeTarget('worktree:alice-wt')?.terminalHandle, 'term-alice');
  assert.equal(alice.mailbox.getWakeTarget('worktree:alice-wt')?.workspacePath, aliceWtPath);
  assert.equal(alice.mailbox.getWakeTarget('worktree:alice-wt')?.sessionId, 'alice-session');
  assert.equal(alice.mailbox.getWakeTarget('worktree:alice-wt')?.paneKey, 'alice:pane');
  await until(() => bob.mailbox.list({ mailboxId: 'worktree:bob-wt' }).totalCount > 0, 'Bob inbound mailbox');
  await until(() => calls.some(call => call.args[0] === 'terminal' && call.args[1] === 'send' && call.args.includes('term-bob')), 'Bob terminal wake');
  const checkOut: string[] = [];
  await runDealerCli(['inbox', 'check'], { stateDir: bob.stateDir, env: bobEnv, stdout: line => checkOut.push(line) });
  const checked = JSON.parse(checkOut.join('\n'));
  assert.equal(checked.mailboxId, 'worktree:bob-wt');
  assert.equal(checked.items.length, 1);
  const replyOut: string[] = [];
  await runDealerCli(['reply', asked.questionId, '--body', 'Approved', '--ack', checked.deliveryId], { stateDir: bob.stateDir, env: bobEnv, stdout: line => replyOut.push(line) });
  const replied = JSON.parse(replyOut.join('\n'));
  assert.equal(replied.replyMailbox, 'worktree:bob-wt');
  assert.equal(replied.targetMailbox, 'worktree:alice-wt');
  assert.equal(bob.mailbox.getWakeTarget('worktree:bob-wt')?.mode, 'orca-terminal');
  assert.equal(bob.mailbox.getWakeTarget('worktree:bob-wt')?.terminalHandle, 'term-bob');
  assert.equal(bob.mailbox.getWakeTarget('worktree:bob-wt')?.workspacePath, bobWtPath);
  assert.equal(bob.mailbox.getWakeTarget('worktree:bob-wt')?.sessionId, 'bob-session');
  assert.equal(bob.mailbox.getWakeTarget('worktree:bob-wt')?.agent, 'claude');
  assert.equal(bob.mailbox.getWakeTarget('worktree:bob-wt')?.paneKey, 'bob:pane');
  await until(() => alice.mailbox.list({ mailboxId: 'worktree:alice-wt' }).totalCount > 0, 'Alice reply mailbox');
  await until(() => calls.some(call => call.args[0] === 'terminal' && call.args[1] === 'send' && call.args.includes('term-alice')), 'Alice terminal wake');
  const aliceCheckOut: string[] = [];
  await runDealerCli(['inbox', 'check'], { stateDir: alice.stateDir, env: aliceEnv, stdout: line => aliceCheckOut.push(line) });
  const aliceCheck = JSON.parse(aliceCheckOut.join('\n'));
  assert.equal(aliceCheck.mailboxId, 'worktree:alice-wt');
  assert.equal(aliceCheck.items.length, 1);
  const followUpOut: string[] = [];
  await runDealerCli(['ask', '--reply-to', replied.replyId, '--body', 'Follow-up', '--ack', aliceCheck.deliveryId], { stateDir: alice.stateDir, env: aliceEnv, stdout: line => followUpOut.push(line) });
  await until(() => bob.mailbox.list({ mailboxId: 'worktree:bob-wt' }).totalCount > 0, 'Bob follow-up mailbox');
  await until(() => calls.some(call => call.args[0] === 'terminal' && call.args[1] === 'send' && call.args.includes('term-bob')), 'Bob terminal wake');
  const bobCheckOut: string[] = [];
  await runDealerCli(['inbox', 'check'], { stateDir: bob.stateDir, env: bobEnv, stdout: line => bobCheckOut.push(line) });
  const bobCheck = JSON.parse(bobCheckOut.join('\n'));
  assert.equal(bobCheck.mailboxId, 'worktree:bob-wt');
  assert.equal(bobCheck.items.length, 1);
  const settleOut: string[] = [];
  await runDealerCli(['settle', asked.questionId, '--outcome', 'succeeded', '--summary', 'Done', '--ack', bobCheck.deliveryId], { stateDir: bob.stateDir, env: bobEnv, stdout: line => settleOut.push(line) });
  await until(() => calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'send' && call.args.includes('term-alice')).length >= 2, 'Alice terminal settle wake');

  alice.mailbox.enqueue([alert('rpc-mail')], { mailboxId: 'worktree:alice-wt' });
  await runDealerCli(['inbox', 'check', '--json'], { stateDir: alice.stateDir, env: { ORCA_TERMINAL_HANDLE: 'term-rpc', ORCA_WORKTREE_ID: `repo::${aliceWtPath}` }, stdout: () => {} });
  assert.equal(alice.mailbox.getWakeTarget('worktree:alice-wt')?.terminalHandle, 'term-rpc');

  await assert.rejects(() => findOrcaWorktreeByName(async () => ({ stdout: '{' }), 'orca', 'alice-wt'), /Unexpected end|JSON/);
  await assert.rejects(() => findOrcaWorktreeByName(async () => ({ stdout: '{"ok":false}' }), 'orca', 'alice-wt'), /Orca worktree ps failed/);
  await assert.rejects(() => findOrcaWorktreeByName(async () => ({ stdout: '{"ok":true}' }), 'orca', 'alice-wt'), /Incomplete Orca workspace inventory/);
});

test('dealer send binds an Orca caller mailbox when the topic is locally bound', async t => {
  const root = directory(t);
  const callerPath = join(root, 'caller-wt');
  const prodPath = join(root, 'prod-release');
  const calls: CommandRequest[] = [];
  const runtime = await makeNode(t, root, 'prod', {
    commandRunner: async request => {
      calls.push(request);
      if (request.args[0] === 'worktree' && request.args[1] === 'ps') {
        return { stdout: JSON.stringify({ ok: true, result: { worktrees: [
          { path: callerPath, worktreeId: `repo::${callerPath}`, displayName: 'caller-wt', isArchived: false, unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
          { path: prodPath, worktreeId: `repo::${prodPath}`, displayName: 'prod-release', isArchived: false, unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
        ] } }) };
      }
      if (request.args[0] === 'terminal' && request.args[1] === 'list') {
        const path = request.args[request.args.indexOf('--worktree') + 1]?.replace(/^path:/, '');
        return { stdout: JSON.stringify({ ok: true, result: { terminals: [{ handle: path === callerPath ? 'term-caller' : 'term-prod', connected: true, writable: true, preview: '' }] } }) };
      }
      return { stdout: JSON.stringify({ ok: true }) };
    },
    patch: {
      gateMode: 'open',
      executors: ['orca-wave', 'inbox'],
      topicBindings: {
        'release.prod.v1': {
          executor: 'orca-wave', streamKey: 'prod-release', workspacePath: prodPath, terminalHandle: 'term-prod',
        },
      },
    },
  });
  const sendOut: string[] = [];
  await runDealerCli(['send', '--network', 'local', '--to', 'member:prod', '--topic', 'release.prod.v1', '--body', '{}', '--json'], {
    runtime,
    env: { ORCA_TERMINAL_HANDLE: 'term-caller', ORCA_WORKTREE_ID: `repo::${callerPath}` },
    stdout: line => sendOut.push(line),
    stderr: line => assert.fail(line),
  });
  const sent = JSON.parse(sendOut.join('\n')) as { messageId: string };
  const inbound = runtime.mailbox.listItemsAfterSequence(-1).find(item => item.envelope?.id === sent.messageId);
  assert.equal(inbound?.mailboxId, 'stream:prod-release');
  assert.equal(inbound?.envelope?.semantic?.replyMailbox, 'worktree:caller-wt');
  assert.equal(runtime.mailbox.getWakeTarget('worktree:caller-wt')?.terminalHandle, 'term-caller');

  const checkedOut: string[] = [];
  await runDealerCli(['inbox', 'check', '--mailbox', 'stream:prod-release', '--json'], {
    runtime, stdout: line => checkedOut.push(line), stderr: line => assert.fail(line),
  });
  const checked = JSON.parse(checkedOut.join('\n')) as { items: Array<{ id: string }>; deliveryId: string };
  const replyOut: string[] = [];
  await runDealerCli(['reply', checked.items[0].id, '--body', 'released', '--ack', checked.deliveryId, '--json'], {
    runtime,
    env: { ORCA_TERMINAL_HANDLE: 'term-prod', ORCA_WORKTREE_ID: `repo::${prodPath}` },
    stdout: line => replyOut.push(line),
    stderr: line => assert.fail(line),
  });
  assert.equal(JSON.parse(replyOut.join('\n')).targetMailbox, 'worktree:caller-wt');
  await until(() => runtime.mailbox.list({ mailboxId: 'worktree:caller-wt' }).totalCount > 0, 'caller reply mailbox');
  await until(() => calls.some(call => call.args[0] === 'terminal' && call.args[1] === 'send' && call.args.includes('term-caller')), 'caller terminal wake');

  const boundSendOut: string[] = [];
  await runDealerCli(['send', '--network', 'local', '--to', 'member:prod', '--topic', 'release.prod.v1', '--body', '{}', '--json'], {
    runtime,
    env: { ORCA_TERMINAL_HANDLE: 'term-prod', ORCA_WORKTREE_ID: `repo::${prodPath}` },
    stdout: line => boundSendOut.push(line),
    stderr: line => assert.fail(line),
  });
  const boundSent = JSON.parse(boundSendOut.join('\n')) as { messageId: string };
  const boundInbound = runtime.mailbox.listItemsAfterSequence(-1).find(item => item.envelope?.id === boundSent.messageId);
  assert.equal(boundInbound?.envelope?.semantic?.replyMailbox, 'stream:prod-release');
  assert.equal(runtime.mailbox.getWakeTarget('worktree:prod-release'), undefined);
});

test('thread show fails loudly for an unknown thread', async t => {
  const root = directory(t);
  const runtime = await makeNode(t, root, 'solo');
  const out: string[] = [];
  const err: string[] = [];
  const code = await runDealerCli(['thread', 'show', 'missing', '--json'], {
    runtime,
    stdout: line => out.push(line),
    stderr: line => err.push(line),
  });
  assert.equal(code, 1);
  assert.match(JSON.parse(err.join('\n')).error, /Unknown thread: missing/);
});

test('Autopilot composer recognition preserves human drafts and clears only before pointer delivery', async t => {
  const suggestion = '继续检查剩余任务';
  for (const [preview, prefilled] of [
    [`• ,1（主推）：${suggestion}\n> ${suggestion}`, true],
    [`下一步建议\n> ${suggestion}\nstatus bar`, true],
    [`自动预填\n> ${suggestion}`, true],
    [`• ,1 (主推)\n> ${suggestion}`, true],
    [`previous ${suggestion}\n> ${suggestion}`, true],
    ['history\n> unsent draft', false],
    ['下一步建议\n>', false],
    ['> unsent draft\n自动预填', false],
  ] as const) {
    assert.equal(isAutopilotComposerPrefill(preview), prefilled, preview);
  }
  const root = directory(t);
  const calls: CommandRequest[] = [];
  let preview = `• ,1（主推）：${suggestion}\n> ${suggestion}`;
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'ps') return { stdout: JSON.stringify({ ok: true, result: { worktrees: [
      { path: root, worktreeId: 'root', unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
    ] } }) };
    if (request.args[1] === 'list') return { stdout: JSON.stringify({ ok: true, result: { terminals: [
      { handle: 'term', connected: true, writable: true, preview },
    ] } }) };
    return { stdout: '{"ok":true}' };
  };
  assert.deepEqual(await checkOrcaWorkspaceIdle(runner, 'orca', root), { idle: true, composerPrefilled: true, reason: undefined });
  const childPath = join(root, 'child');
  for (const childPreview of [
    `• ,1（主推）：${suggestion}\n> ${suggestion}`,
    `• ,1（主推）：${suggestion}\n> ${suggestion}\nstatus bar`,
  ]) {
    const childRunner: CommandRunner = async request => {
      if (request.args[0] === 'worktree') return { stdout: JSON.stringify({ ok: true, result: { worktrees: [
        { path: root, worktreeId: 'root', childWorktreeIds: ['child'], unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
        { path: childPath, worktreeId: 'child', unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
      ] } }) };
      assert.equal(request.args[1], 'list');
      return { stdout: JSON.stringify({ ok: true, result: { terminals: [
        { handle: 'term', connected: true, writable: true, preview: request.args.includes(`path:${childPath}`) ? childPreview : '>' },
      ] } }) };
    };
    const result = await checkOrcaWorkspaceIdle(childRunner, 'orca', root);
    assert.equal(result.idle, true);
    assert.equal(Boolean(result.composerPrefilled), false);
  }
  preview = 'history\n> unsent draft';
  assert.equal((await checkOrcaWorkspaceIdle(runner, 'orca', root)).idle, false);
  preview = `• ,1（主推）：${suggestion}\n> ${suggestion}\nstatus bar`;
  const store = new MailboxStore(join(root, 'mailbox.json'));
  store.enqueue([alert('prefill')], { mailboxId: 'default', wakeTarget: { mode: 'orca-terminal', terminalHandle: 'term', workspacePath: root } });
  const wakeup = new WakeupRedriveService({ store, commandRunner: runner });
  t.after(() => wakeup.stop());
  assert.equal((await wakeup.flushMailbox('default', { force: true })).woke, true);
  const sends = terminalSends(calls);
  assert.equal(sends.length, 2);
  assert.equal(textOf(sends[0]), '\x15');
  assert.equal(sends[0].args.includes('--enter'), false);
  assert.match(textOf(sends[1]), /^You have 1 Dealer message\(s\) in default/);
  assert.ok(sends[1].args.includes('--enter'));
  const failedCalls: CommandRequest[] = [];
  await assert.rejects(sendOrcaTerminalText(async call => {
    failedCalls.push(call);
    return { stdout: '{"ok":false}' };
  }, 'orca', 'term', 'pointer', { clearComposer: true }), /clear failed/);
  assert.equal(failedCalls.length, 1);
});

test('workspace idle inspection scopes composer checks to the terminal the wakeup will write', async t => {
  const root = directory(t);
  let terminals = [
    { handle: 'human', connected: true, writable: true, agentIdentity: 'antigravity', preview: 'history\n> unsent draft' },
    { handle: 'target', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ];
  const runner: CommandRunner = async request => {
    if (request.args[0] === 'worktree' && request.args[1] === 'ps') {
      return { stdout: JSON.stringify({ ok: true, result: { truncated: false, worktrees: [
        { path: root, worktreeId: 'root', childWorktreeIds: [], unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
      ] } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'list') {
      return { stdout: JSON.stringify({ ok: true, result: { terminals } }) };
    }
    return { stdout: '{"ok":true}' };
  };
  // Legacy callers inspect every reachable composer in the root workspace.
  assert.equal((await checkOrcaWorkspaceIdle(runner, 'orca', root)).idle, false);
  // A bound target ignores drafts in other retained conversation tabs.
  assert.deepEqual(
    await checkOrcaWorkspaceIdle(runner, 'orca', root, undefined, { targetTerminalHandle: 'target' }),
    { idle: true, reason: undefined },
  );
  // A fresh session tab has its own empty composer, so no retained tab can block it.
  assert.deepEqual(
    await checkOrcaWorkspaceIdle(runner, 'orca', root, undefined, { targetTerminalHandle: null }),
    { idle: true, reason: undefined },
  );
  // A closed bound handle only inspects the terminals a rebind could reuse.
  assert.equal(
    (await checkOrcaWorkspaceIdle(runner, 'orca', root, undefined, { targetTerminalHandle: 'closed' })).idle,
    false,
  );

  const suggestion = '继续检查剩余任务';
  terminals = [
    { handle: 'human', connected: true, writable: true, agentIdentity: 'antigravity', preview: `• ,1（主推）：${suggestion}\n> ${suggestion}` },
    ...terminals.slice(1),
  ];
  assert.deepEqual(
    await checkOrcaWorkspaceIdle(runner, 'orca', root),
    { idle: true, composerPrefilled: true, reason: undefined },
  );
  assert.deepEqual(
    await checkOrcaWorkspaceIdle(runner, 'orca', root, undefined, { targetTerminalHandle: null }),
    { idle: true, reason: undefined },
  );
  assert.deepEqual(
    await checkOrcaWorkspaceIdle(runner, 'orca', root, undefined, { targetTerminalHandle: 'target' }),
    { idle: true, reason: undefined },
  );
});

test('only exact Antigravity mode placeholders relax the composer draft guard', () => {
  const placeholders = [
    '> Auto mode: edits & safe commands auto-approved (shift+tab to cycle)',
    '> Plan mode: research & plan only (shift+tab to cycle)',
    '> Accept-edits mode: file edits auto-approved (shift+tab to cycle)',
    '> Best-of-N mode: fork into parallel arms and pick a winner (shift+tab to cycle)',
  ];
  for (const line of placeholders) {
    assert.equal(isAntigravityModePlaceholder(line, 'antigravity'), true, line);
  }
  assert.equal(
    isAntigravityModePlaceholder('> Auto mode: edits & safe commands auto-approved', 'antigravity'),
    false,
    'a changed or truncated hint is an unsent draft, not a placeholder',
  );
  assert.equal(isAntigravityModePlaceholder('> unsent draft', 'antigravity'), false);
  assert.equal(
    isAntigravityModePlaceholder(placeholders[0], 'claude'),
    false,
    'another agent never inherits the Antigravity placeholder vocabulary',
  );
  assert.equal(isAntigravityModePlaceholder(placeholders[0], undefined), false);
});

test('a TUI mode placeholder on an empty composer never wedges a bound wave delivery', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'agy-placeholder');
  const placeholder = '> Auto mode: edits & safe commands auto-approved (shift+tab to cycle)';
  const calls: CommandRequest[] = [];
  const waits: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'ps') {
      return { stdout: JSON.stringify({ ok: true, result: { truncated: false, worktrees: [
        { path: workspacePath, worktreeId: 'agy', childWorktreeIds: [], unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
      ] } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'list') {
      return { stdout: JSON.stringify({ ok: true, result: { terminals: [
        { handle: 'term-agy', connected: true, writable: true, agentIdentity: 'antigravity', preview: `history\n${placeholder}` },
      ] } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'wait') {
      waits.push(request);
      return { stdout: '{"ok":true}' };
    }
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({ store, commandRunner: runner, flushIntervalMs: 100_000 });
  t.after(() => service.stop());
  store.enqueue([alert('placeholder')], {
    mailboxId: 'stream:placeholder',
    wakeTarget: {
      mode: 'orca-terminal',
      terminalHandle: 'term-agy',
      workspacePath,
      coalesceMs: 0,
      rotateAfterWaves: 10,
      completedWaves: 1,
    },
  });

  const delivered = await service.flushMailbox('stream:placeholder');
  assert.equal(delivered.woke, true, delivered.reason);
  // The exact placeholder and Orca's readiness verdict must both agree before delivery.
  assert.deepEqual(waits.map(call => call.args), [[
    'terminal', 'wait', '--terminal', 'term-agy', '--for', 'tui-idle', '--timeout-ms', '1', '--json',
  ]]);
  const sends = sendsTo(calls, 'term-agy');
  assert.equal(sends.length, 1);
  assert.match(textOf(sends[0]), /^You have 1 Dealer message\(s\) in stream:placeholder/);
  assert.equal(terminalSends(calls).some(call => textOf(call) === '\x15'), false, 'an empty composer is never cleared');
  assert.equal(store.getWakeTarget('stream:placeholder')!.completedWaves, 1);
});

test('an unsent human draft stays busy even when tui-idle reports a title-idle agent', async t => {
  for (const agentIdentity of ['antigravity', 'claude'] as const) {
    const root = directory(t);
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const workspacePath = join(root, `${agentIdentity}-draft`);
    const calls: CommandRequest[] = [];
    const runner: CommandRunner = async request => {
      calls.push(request);
      if (request.args[0] === 'worktree' && request.args[1] === 'ps') {
        return { stdout: JSON.stringify({ ok: true, result: { truncated: false, worktrees: [
          { path: workspacePath, worktreeId: 'draft', childWorktreeIds: [], unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
        ] } }) };
      }
      if (request.args[0] === 'terminal' && request.args[1] === 'list') {
        return { stdout: JSON.stringify({ ok: true, result: { terminals: [
          { handle: 'term-draft', connected: true, writable: true, agentIdentity, preview: 'history\n> unsent draft' },
        ] } }) };
      }
      if (request.args[0] === 'terminal' && request.args[1] === 'wait') {
        // `tui-idle` is a readiness verdict, not a composer-draft check: title-idle agents report
        // ready even while a human draft sits in the composer.
        return { stdout: '{"ok":true}' };
      }
      return { stdout: '{"ok":true}' };
    };
    assert.deepEqual(
      await checkOrcaWorkspaceIdle(runner, 'orca', workspacePath),
      { idle: false, reason: 'Workspace or descendant is busy or unverifiable' },
    );
    assert.equal(
      calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'wait').length,
      0,
      'an unsent draft is never resolved by a readiness probe',
    );
    const service = new WakeupRedriveService({ store, commandRunner: runner, flushIntervalMs: 100_000 });
    t.after(() => service.stop());
    store.enqueue([alert(`draft-${agentIdentity}`)], {
      mailboxId: 'stream:draft',
      wakeTarget: {
        mode: 'orca-terminal', terminalHandle: 'term-draft', workspacePath, coalesceMs: 0,
      },
    });

    const deferred = await service.flushMailbox('stream:draft');
    assert.equal(deferred.woke, false);
    assert.equal(deferred.reason, 'Workspace or descendant is busy or unverifiable');
    assert.equal(terminalSends(calls).length, 0, 'nothing is injected while the draft is present');
    assert.equal(store.getWakeTarget('stream:draft')!.pendingWake, true);
  }
});

test('fresh-tab and bound-tab plans ignore disconnected or non-writable retained tabs', async t => {
  const root = directory(t);
  const { runner, state } = fakeOrcaWorkspace(root, [
    { handle: 'target', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
    { handle: 'dead-human', connected: false, writable: true, agentIdentity: 'antigravity', preview: '> unsent draft' },
    { handle: 'readonly-human', connected: true, writable: false, agentIdentity: 'antigravity', preview: '> unsent draft' },
  ]);
  // A fresh session tab receives the pointer, so no retained tab can gate the wakeup.
  assert.deepEqual(
    await checkOrcaWorkspaceIdle(runner, 'orca', root, undefined, { targetTerminalHandle: null }),
    { idle: true, reason: undefined },
  );
  // A bound target gates only itself; unrelated dead or non-writable tabs are irrelevant.
  assert.deepEqual(
    await checkOrcaWorkspaceIdle(runner, 'orca', root, undefined, { targetTerminalHandle: 'target' }),
    { idle: true, reason: undefined },
  );
  // Legacy inspection still rejects the disconnected retained tab.
  assert.deepEqual(
    await checkOrcaWorkspaceIdle(runner, 'orca', root),
    { idle: false, reason: 'Workspace or descendant is busy or unverifiable' },
  );
  // The terminal that actually receives the pointer must still be connected and writable.
  state.terminals = [
    { handle: 'target', connected: true, writable: false, agentIdentity: 'antigravity', preview: '' },
    { handle: 'live-human', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ];
  assert.deepEqual(
    await checkOrcaWorkspaceIdle(runner, 'orca', root, undefined, { targetTerminalHandle: 'target' }),
    { idle: false, reason: 'Workspace or descendant is busy or unverifiable' },
  );
});

test('fresh-tab delivery is not blocked by a disconnected retained tab', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'fresh');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-live', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
    { handle: 'term-dead', connected: false, writable: true, agentIdentity: 'antigravity', preview: '> unsent draft' },
  ]);
  const service = new WakeupRedriveService({
    store,
    commandRunner: runner,
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());
  store.enqueue([alert('fresh-dead')], {
    mailboxId: 'stream:fresh-dead',
    wakeTarget: { mode: 'orca-terminal', workspacePath, coalesceMs: 0, rotateAfterWaves: 1 },
  });

  const delivered = await service.flushMailbox('stream:fresh-dead');
  assert.equal(delivered.woke, true, delivered.reason);
  assert.equal(state.created, 1);
  assert.equal(sendsTo(state.calls, 'term-dead').length, 0);
  const pointer = sendsTo(state.calls, 'term-new-1')[0];
  assert.match(textOf(pointer), /^You have 1 Dealer message\(s\) in stream:fresh-dead/);
});

test('rotation is not blocked by a non-writable retained tab', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'rotate');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-bound', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
    { handle: 'term-live', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const service = new WakeupRedriveService({
    store,
    commandRunner: runner,
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());
  const enqueue = (id: string) =>
    store.enqueue([alert(id)], {
      mailboxId: 'stream:rotate-readonly',
      wakeTarget: {
        mode: 'orca-terminal', terminalHandle: 'term-bound', workspacePath,
        coalesceMs: 0, rotateAfterWaves: 1,
      },
    });

  enqueue('wave-one');
  assert.equal((await service.flushMailbox('stream:rotate-readonly')).woke, true);
  const lease = store.check({ mailboxId: 'stream:rotate-readonly' });
  store.ack(lease.deliveryId!);
  assert.equal(sendsTo(state.calls, 'term-bound').length, 1);

  state.terminals = state.terminals.map(terminal =>
    terminal.handle === 'term-bound' ? { ...terminal, writable: false } : terminal);
  enqueue('wave-two');
  const rotated = await service.flushMailbox('stream:rotate-readonly');
  assert.equal(rotated.woke, true, rotated.reason);
  assert.equal(rotated.rotated, true);
  assert.equal(state.created, 1);
  assert.equal(
    sendsTo(state.calls, 'term-bound').length,
    1,
    'the retained tab is never written to again',
  );
  const pointer = sendsTo(state.calls, 'term-new-1')[0];
  assert.match(textOf(pointer), /^You have 1 Dealer message\(s\) in stream:rotate-readonly/);
});

test('refreshTopicBindings keeps a runtime-resolved terminal handle across restarts', async t => {
  const root = directory(t);
  const stateDir = join(root, 'state');
  const workspacePath = join(root, 'workspace');
  const patch = {
    defaultExecutor: 'orca-wave',
    topicBindings: {
      'collab.request.v4': {
        executor: 'orca-wave',
        mailboxId: 'stream:collab.request.v4',
        workspacePath,
      },
    },
    wave: { flushIntervalMs: 100_000 },
  };
  const make = () =>
    new DealerNodeRuntime({
      stateDir,
      member: 'worker',
      name: 'worker',
      profile: 'dedicated-worker',
      transport: { discoveryDir: join(root, 'mesh') },
      commandRunner: async () => {
        throw new Error('Unexpected external command');
      },
      patch,
    });
  const first = await make().start();
  t.after(() => first.stop());
  const target = first.mailbox.getWakeTarget('stream:collab.request.v4')!;
  assert.equal(target.rotateAfterWaves, 1);
  first.mailbox.upsertWakeTarget('stream:collab.request.v4', { terminalHandle: 'runtime-tab' });
  await first.stop();

  const second = await make().start();
  t.after(() => second.stop());
  assert.equal(
    second.mailbox.getWakeTarget('stream:collab.request.v4')!.terminalHandle,
    'runtime-tab',
    'a restart must not erase the live tab resolved at runtime',
  );
  assert.equal(second.mailbox.getWakeTarget('stream:collab.request.v4')!.rotateAfterWaves, 1);

  // Refreshing the same binding keeps the runtime handle; switching workspaces clears it.
  await second.applyAgentPreset({
    host: 'orca', conversationMode: 'fixed', topic: 'collab.request.v4', workspacePath,
  });
  assert.equal(second.mailbox.getWakeTarget('stream:collab.request.v4')!.terminalHandle, 'runtime-tab');
  await second.applyAgentPreset({
    host: 'orca', conversationMode: 'fixed', topic: 'collab.request.v4', workspacePath: join(root, 'other'),
  });
  assert.equal(second.mailbox.getWakeTarget('stream:collab.request.v4')!.terminalHandle, undefined);

  // Reconfiguring the binding to an explicitly pinned tab or an Orca Run clears the stale
  // rotation default; any preset application re-reads every binding.
  second.config.topicBindings = {
    ...second.config.topicBindings,
    'collab.request.v4': {
      executor: 'orca-wave', mailboxId: 'stream:collab.request.v4',
      workspacePath, terminalHandle: 'explicit-tab',
    },
  };
  await second.applyAgentPreset({
    host: 'codex', conversationMode: 'inbox-only', topic: 'reconfiguration.trigger',
  });
  const pinned = second.mailbox.getWakeTarget('stream:collab.request.v4')!;
  assert.equal(pinned.terminalHandle, 'explicit-tab');
  assert.equal(pinned.rotateAfterWaves, undefined, 'an explicitly pinned tab never rotates');

  second.config.topicBindings = {
    ...second.config.topicBindings,
    'collab.request.v4': {
      executor: 'orca-wave', mailboxId: 'stream:collab.request.v4',
      workspacePath, runId: 'run-owner',
    },
  };
  await second.applyAgentPreset({
    host: 'codex', conversationMode: 'inbox-only', topic: 'reconfiguration.trigger',
  });
  const runBound = second.mailbox.getWakeTarget('stream:collab.request.v4')!;
  assert.equal(runBound.mode, 'orca-run');
  assert.equal(runBound.terminalHandle, undefined, 'an Orca Run clears the stale pinned tab');
  assert.equal(runBound.rotateAfterWaves, undefined, 'an Orca Run owns its coordinator session');
});

test('inline wakeup previews include bounded latest body, semantic sender and thread on one line', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const calls: CommandRequest[] = [];
  const wakeup = new WakeupRedriveService({ store, commandRunner: async call => {
    calls.push(call);
    return { stdout: '{"ok":true}' };
  } });
  t.after(() => wakeup.stop());
  for (const [id, body, semantic] of [
    ['long', 'x'.repeat(450) + '\r\nend', { type: 'question', threadId: 'thread-1' }],
    ['short', 'hello\r\nworld', { type: 'message', threadId: 'thread-2' }],
    ['empty', ' \n ', undefined],
  ] as const) {
    const claim = wakeup.claim(id);
    store.enqueue([alert(id, { body, semantic })], { mailboxId: id, wakeTarget: { mode: 'orca-run', runId: 'run' } });
    await wakeup.flushMailbox(id);
    claim.dispose();
    const result = await wakeup.flushMailbox(id, { force: true });
    const pointer = result.pointer!;
    assert.match(pointer, new RegExp(`^You have 1 Dealer message\\(s\\) in ${id}`));
    assert.equal(/[\r\n]/.test(pointer), false);
    if (id === 'long') {
      assert.ok(pointer.includes(`Latest (question from sender, thread thread-1): "${'x'.repeat(400)}...".`));
      assert.ok(!pointer.includes('x'.repeat(401)));
    } else if (id === 'short') {
      assert.ok(pointer.includes('Latest (alerts from sender, thread thread-2): "hello world".'));
    } else assert.ok(!pointer.includes('Latest'));
  }
});

test('only newly inserted conversational frames bypass coalescing and never reset retry backoff', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 10_000 });
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  for (const type of ['question', 'reply', 'settle'] as const) {
    store.enqueue([alert(type, { semantic: { type, threadId: 'thread', ...(type === 'settle' ? { outcome: 'succeeded' as const } : {}) } })], {
      mailboxId: type, wakeTarget: { mode: 'orca-run', runId: 'run', coalesceMs: 5000 },
    });
    assert.equal(store.getWakeTarget(type)?.earliestWakeAt, 10_000);
  }
  store.enqueue([alert('normal'), alert('control', { semantic: { type: 'reply', threadId: 'thread' } })], {
    mailboxId: 'mixed', wakeTarget: { mode: 'orca-run', runId: 'run', coalesceMs: 5000 },
  });
  assert.equal(store.getWakeTarget('mixed')?.earliestWakeAt, 15_000);
  store.enqueue([alert('normal'), alert('new-control', { semantic: { type: 'question', threadId: 'thread' } })], { mailboxId: 'mixed' });
  assert.equal(store.getWakeTarget('mixed')?.earliestWakeAt, 10_000, 'duplicates do not affect classification');
  store.upsertWakeTarget('mixed', { attemptCount: 2, earliestWakeAt: 30_000 });
  store.enqueue([alert('retry', { semantic: { type: 'settle', threadId: 'thread', outcome: 'succeeded' } })], { mailboxId: 'mixed' });
  assert.equal(store.getWakeTarget('mixed')?.earliestWakeAt, 30_000);
  const calls: CommandRequest[] = [];
  const wakeup = new WakeupRedriveService({ store, now: () => 10_000, commandRunner: async call => {
    calls.push(call);
    return { stdout: '{"ok":true}' };
  } });
  t.after(() => wakeup.stop());
  assert.equal((await wakeup.flushReady()).filter(result => result.woke).length, 3);
  assert.equal(calls.length, 3);
});

test('outstanding lease exposes a cloned latest queued reply in both check and peek', t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.enqueue([alert('task')], { mailboxId: 'box' });
  const lease = store.check({ mailboxId: 'box' });
  store.enqueue([alert('reply', { body: 'The answer', semantic: { type: 'reply', threadId: 'task' } })], { mailboxId: 'box' });
  for (const peek of [false, true]) {
    const preview = store.check({ mailboxId: 'box', peek });
    assert.equal(preview.deliveryId, lease.deliveryId);
    assert.deepEqual(preview.items.map(item => item.id), ['task']);
    assert.equal(preview.newerQueuedCount, 1);
    assert.equal(preview.latestQueued?.id, 'reply');
    preview.latestQueued!.envelope.body = 'mutated';
    assert.equal(store.check({ mailboxId: 'box', peek: true }).latestQueued!.envelope.body, 'The answer');
  }
});

for (const mode of ['passive', 'orca-run', 'orca-terminal'] as const) {
  test(`${mode} wake delivery redrives newer sequences immediately after its in-flight command`, async t => {
    const store = new MailboxStore(join(directory(t), 'mailbox.json'));
    const mailboxId = 'box';
    store.enqueue([alert('old')], { mailboxId });
    store.check({ mailboxId });
    store.enqueue([alert('first', { semantic: { type: 'reply', threadId: 'old' } })], { mailboxId,
      wakeTarget: { mode, ...(mode === 'passive' ? { hostAgent: 'claude-code', conversationMode: 'spawn' } : {}), runId: 'run', terminalHandle: 'term' } });
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const pointers: string[] = [];
    const service = new WakeupRedriveService({ store, flushIntervalMs: 100_000, idleCheck: async () => ({ idle: true }), commandRunner: async call => {
      if (mode !== 'orca-terminal' || call.args[1] === 'send') {
        pointers.push(mode === 'passive' ? call.args.at(-1)! : mode === 'orca-run' ? bodyOf(call) : textOf(call));
        if (pointers.length === 1) await held;
      }
      return { stdout: '{"ok":true}' };
    } });
    t.after(() => service.stop());
    const first = service.flushMailbox(mailboxId);
    await until(() => pointers.length === 1, 'first command starts');
    store.enqueue([alert('second', { body: 'Second reply', semantic: { type: 'reply', threadId: 'old' } })], { mailboxId });
    release();
    await first;
    await until(() => pointers.length === 2 && !store.getWakeTarget(mailboxId)!.pendingWake, 'new message redrives without timer');
    assert.match(pointers[0], /Latest \(reply from sender, thread old\)/);
    assert.match(pointers[1], /Second reply/);
    if (mode === 'passive') {
      assert.equal(store.getWakeTarget(mailboxId)?.conversationMode, 'fixed');
      assert.equal(store.getWakeTarget(mailboxId)?.sessionId, 'box');
    }
  });
}

test('sequence settlement remains finite when wall clock order differs from arrival order', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 20_000 });
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.enqueue([alert('leased')], { mailboxId: 'box' });
  store.check({ mailboxId: 'box' });
  store.enqueue([alert('later-clock')], { mailboxId: 'box', wakeTarget: { mode: 'passive', hostAgent: 'codex', sessionId: 'existing' } });
  t.mock.timers.setTime(10_000);
  store.enqueue([alert('later-sequence')], { mailboxId: 'box' });
  assert.equal(store.check({ mailboxId: 'box', peek: true }).latestQueued?.id, 'later-clock');
  let calls = 0;
  const wakeup = new WakeupRedriveService({ store, commandRunner: async () => {
    calls++;
    return { stdout: '{}' };
  } });
  t.after(() => wakeup.stop());
  await wakeup.flushMailbox('box', { force: true });
  assert.equal(calls, 1);
  assert.equal(store.getWakeTarget('box')?.pendingWake, false);
});

test('in-flight ordinary arrivals retain enqueue coalescing deadline while replies remain immediate', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 10_000 });
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.enqueue([alert('first')], { mailboxId: 'box', wakeTarget: {
    mode: 'passive', hostAgent: 'claude-code', conversationMode: 'fixed', sessionId: 'session', coalesceMs: 5000,
  } });
  let calls = 0;
  const wakeup = new WakeupRedriveService({ store, commandRunner: async () => {
    calls++;
    if (calls === 1) {
      t.mock.timers.setTime(11_000);
      store.enqueue([alert('ordinary')], { mailboxId: 'box' });
    }
    return { stdout: '{}' };
  } });
  t.after(() => wakeup.stop());
  await wakeup.flushMailbox('box', { force: true });
  assert.equal(calls, 1);
  assert.equal(store.getWakeTarget('box')?.pendingWake, true);
  assert.equal(store.getWakeTarget('box')?.earliestWakeAt, 16_000);
  assert.deepEqual(await wakeup.flushReady(), []);
  t.mock.timers.setTime(16_000);
  await wakeup.flushReady();
  assert.equal(calls, 2);
  assert.equal(store.getWakeTarget('box')?.pendingWake, false);
});

test('spawned terminal handles persist across deferral and completion', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  let idleChecks = 0;
  let created = 0;
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'terminal' && request.args[1] === 'list') {
      return { stdout: JSON.stringify({ ok: true, result: { terminals: [] } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'create') {
      created += 1;
      return { stdout: JSON.stringify({ ok: true, result: { handle: `term-${created}` } }) };
    }
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({
    store, commandRunner: runner,
    idleCheck: async () => ({ idle: ++idleChecks !== 2 }),
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());

  store.enqueue([alert('deferred')], { mailboxId: 'stream:deferred', wakeTarget: {
    mode: 'orca-terminal', workspacePath: '/fixed',
  } });
  const deferred = await service.flushMailbox('stream:deferred');
  assert.equal(deferred.woke, false);
  assert.match(deferred.reason!, /Terminal spawned/);
  assert.equal(store.getWakeTarget('stream:deferred')!.terminalHandle, 'term-1');
  const delivered = await service.flushMailbox('stream:deferred', { force: true });
  assert.equal(delivered.woke, true);
  assert.equal(store.getWakeTarget('stream:deferred')!.terminalHandle, 'term-1');
  assert.equal(store.getWakeTarget('stream:deferred')!.pendingWake, false);
  const send = calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'send').at(-1)!;
  assert.equal(send.args[send.args.indexOf('--terminal') + 1], 'term-1');
  assert.equal(
    calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'create').length,
    1,
    'the deferred terminal is reused instead of respawned',
  );

  idleChecks = 10;
  store.enqueue([alert('direct')], { mailboxId: 'stream:direct', wakeTarget: {
    mode: 'orca-terminal', workspacePath: '/fixed-direct',
  } });
  const direct = await service.flushMailbox('stream:direct');
  assert.equal(direct.woke, true);
  assert.equal(store.getWakeTarget('stream:direct')!.terminalHandle, 'term-2');
  assert.equal(store.getWakeTarget('stream:direct')!.pendingWake, false);
});

test('a closed persisted terminal is rebound to the live one before the pointer is sent', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'fixed');
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'ps') {
      return { stdout: JSON.stringify({ ok: true, result: { worktrees: [
        { path: workspacePath, worktreeId: 'fixed', unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
      ] } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'list') {
      return { stdout: JSON.stringify({ ok: true, result: { terminals: [
        { handle: 'term-live', connected: true, writable: true, agentIdentity: 'antigravity' },
      ] } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'send') {
      const handle = String(request.args[request.args.indexOf('--terminal') + 1]);
      return { stdout: JSON.stringify({ ok: handle !== 'term-dead' }) };
    }
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({ store, commandRunner: runner, flushIntervalMs: 100_000 });
  t.after(() => service.stop());
  store.enqueue([alert('heal')], { mailboxId: 'box', wakeTarget: {
    mode: 'orca-terminal', workspacePath, terminalHandle: 'term-dead',
  } });

  // Resolution runs before the external send, so the closed handle is replaced on the first
  // attempt instead of failing into the backoff ledger.
  const healed = await service.flushMailbox('box');
  assert.equal(healed.woke, true, healed.reason);
  const sends = terminalSends(calls);
  assert.equal(sends.length, 1);
  assert.equal(sends[0].args[sends[0].args.indexOf('--terminal') + 1], 'term-live');
  assert.match(textOf(sends[0]), /^You have 1 Dealer message\(s\) in box/);
  assert.equal(
    calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'list').length,
    2,
    'idle proof plus pre-send terminal resolution',
  );
  assert.equal(
    calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'create').length,
    0,
    'the live terminal is reused instead of spawning another',
  );
  const target = store.getWakeTarget('box')!;
  assert.equal(target.terminalHandle, 'term-live');
  assert.equal(target.pendingWake, false);
  assert.equal(target.deliveryInDoubt, undefined);
});

test('runCommand keeps strict cwd semantics while only the read-only workspace probe narrows a gone cwd', async t => {
  const root = directory(t);
  const missing = join(root, 'removed-worktree');

  // The shared runner still proves the process boundary: requesting a cwd that is gone is a
  // CommandNotStartedError, never a silent fallback to the daemon's own cwd.
  await assert.rejects(
    runCommand({
      command: process.execPath,
      args: ['-e', 'process.stdout.write(process.cwd())'],
      cwd: missing,
    }),
    { name: 'CommandNotStartedError' },
  );

  // Only the read-only global inventory probe narrows the cwd: it runs from the daemon cwd and
  // reports the removed workspace before anything is spawned inside the gone directory.
  const fakeOrca = join(root, 'fake-orca');
  writeFileSync(fakeOrca, [
    `#!${process.execPath}`,
    `if (process.cwd() !== ${JSON.stringify(process.cwd())}) process.exit(3);`,
    `if (process.argv.slice(2).join(' ') !== 'worktree ps --agent-wait --limit 10000 --json') process.exit(4);`,
    `process.stdout.write(JSON.stringify({ ok: true, result: { worktrees: [] } }));`,
    '',
  ].join('\n'), { mode: 0o755 });
  assert.deepEqual(
    await checkOrcaWorkspaceIdle(runCommand, fakeOrca, missing),
    { idle: false, reason: 'Workspace not found' },
  );
});

test('a removed non-auto-created workspace disarms wake retries but keeps the queued mail', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const gonePath = join(root, 'removed-worktree');
  const livePath = join(root, 'live-workspace');
  mkdirSync(livePath);
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'ps') {
      return { stdout: JSON.stringify({ ok: true, result: { worktrees: [] } }) };
    }
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({
    store, commandRunner: runner, flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());

  store.enqueue([alert('gone-reply', { semantic: { type: 'reply', threadId: 'gone-task' } })], {
    mailboxId: 'worktree:gone',
    wakeTarget: { mode: 'orca-terminal', terminalHandle: 'term-gone', workspacePath: gonePath, coalesceMs: 0 },
  });
  const results = await service.flushWakeups();
  assert.equal(results.length, 1);
  assert.equal(results[0].woke, false);
  assert.equal(results[0].reason, 'Workspace not found');
  const disarmed = store.getWakeTarget('worktree:gone')!;
  assert.equal(disarmed.pendingWake, false);
  assert.equal(disarmed.attemptCount, 0);
  assert.equal(disarmed.earliestWakeAt, 0);
  assert.equal(disarmed.lastWakeError, undefined);
  assert.equal(store.list({ mailboxId: 'worktree:gone' }).queuedCount, 1);
  assert.equal(terminalSends(calls).length, 0);
  const probes = calls.length;
  assert.deepEqual(await service.flushWakeups(), []);
  assert.equal(calls.length, probes, 'a disarmed target is never probed again');

  // An unspawned task must still disarm on a removed workspace instead of creating a tab.
  store.enqueue([alert('gone-spawn')], {
    mailboxId: 'task:gone-spawn',
    wakeTarget: { mode: 'orca-terminal', workspacePath: gonePath, coalesceMs: 0 },
  });
  assert.equal((await service.flushMailbox('task:gone-spawn')).reason, 'Workspace not found');
  assert.equal(store.getWakeTarget('task:gone-spawn')!.pendingWake, false);
  assert.equal(calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'create').length, 0);

  // A workspace whose directory still exists keeps its durable retry ledger even while Orca's
  // inventory is incomplete; only a path that is truly gone is disarmed.
  store.enqueue([alert('live-reply', { semantic: { type: 'reply', threadId: 'live-task' } })], {
    mailboxId: 'worktree:live',
    wakeTarget: { mode: 'orca-terminal', terminalHandle: 'term-live', workspacePath: livePath, coalesceMs: 0 },
  });
  const deferred = await service.flushMailbox('worktree:live', { force: true });
  assert.equal(deferred.woke, false);
  assert.equal(store.getWakeTarget('worktree:live')!.pendingWake, true);
  assert.equal(store.getWakeTarget('worktree:live')!.attemptCount, 1);
  assert.match(store.getWakeTarget('worktree:live')!.lastWakeError!, /Workspace not found/);
});

test('a persisted unknown-effect fence survives reload and blocks delivery', async t => {
  const root = directory(t);
  const path = join(root, 'mailbox.json');
  new MailboxStore(path).enqueue([alert('legacy')], { mailboxId: 'box', wakeTarget: {
    mode: 'orca-run', runId: 'run-legacy',
  } });
  // Reproduce the persisted shape of a send whose external outcome was never confirmed.
  const raw = JSON.parse(readFileSync(path, 'utf8'));
  raw.wakeTargets.box = {
    ...raw.wakeTargets.box, deliveryInDoubt: true, pendingWake: false, earliestWakeAt: 0,
  };
  writeFileSync(path, JSON.stringify(raw));

  const store = new MailboxStore(path);
  const loaded = store.getWakeTarget('box')!;
  assert.equal(loaded.deliveryInDoubt, true);
  assert.equal(loaded.pendingWake, false);

  const calls: CommandRequest[] = [];
  const service = new WakeupRedriveService({ store, flushIntervalMs: 100_000, commandRunner: async request => {
    calls.push(request);
    return { stdout: '{"ok":true}' };
  } });
  t.after(() => service.stop());
  assert.deepEqual(await service.flushReady(), []);
  const forced = await service.flushMailbox('box', { force: true });
  assert.equal(forced.woke, false);
  assert.match(forced.reason!, /reconciliation/);
  assert.equal(calls.length, 0, 'an unknown external outcome must never be replayed');
  assert.equal(store.getWakeTarget('box')!.deliveryInDoubt, true);
});

for (const mode of ['passive', 'orca-run', 'orca-terminal'] as const) {
  test(`${mode} uncertain external delivery is fenced against retries, later mail and restart`, async t => {
    const file = join(directory(t), 'mailbox.json');
    const store = new MailboxStore(file);
    store.enqueue([alert('first')], { mailboxId: 'box', wakeTarget: {
      mode, ...(mode === 'passive' ? { hostAgent: 'claude-code' as const, conversationMode: 'spawn' as const } : {}), runId: 'run', terminalHandle: 'term',
    } });
    let effects = 0;
    const wakeup = new WakeupRedriveService({ store, idleCheck: async () => ({ idle: true }), commandRunner: async call => {
      if (mode !== 'orca-terminal' || call.args[1] === 'send') {
        effects++;
        throw new Error('accepted then result lost');
      }
      return { stdout: '{"ok":true}' };
    } });
    t.after(() => wakeup.stop());
    assert.equal((await wakeup.flushMailbox('box')).woke, false);
    assert.equal(store.getWakeTarget('box')?.deliveryInDoubt, true);
    assert.equal(store.getWakeTarget('box')?.pendingWake, false);
    assert.match(store.getWakeTarget('box')!.lastWakeError!, /result lost/);
    assert.equal(effects, 1);
    await wakeup.stop();
    const restarted = new MailboxStore(file);
    const recovery = new WakeupRedriveService({ store: restarted, idleCheck: async () => ({ idle: true }), commandRunner: async () => {
      effects++;
      return { stdout: '{"ok":true}' };
    } });
    t.after(() => recovery.stop());
    restarted.enqueue([alert('next')], { mailboxId: 'box' });
    assert.deepEqual(await recovery.flushReady(), []);
    assert.equal((await recovery.flushMailbox('box', { force: true })).woke, false);
    assert.equal(effects, 1);
    assert.equal(restarted.getWakeTarget('box')?.deliveryInDoubt, true);
  });
}

test('a restart with an unfinished external wake remains fenced even without a failure callback', async t => {
  const file = join(directory(t), 'mailbox.json');
  const store = new MailboxStore(file);
  store.enqueue([alert('first')], { mailboxId: 'box', wakeTarget: { mode: 'passive', hostAgent: 'claude-code' } });
  store.beginWakeDelivery('box');
  const restarted = new MailboxStore(file);
  const service = new WakeupRedriveService({ store: restarted, commandRunner: async () => {
    assert.fail('unknown delivery must not be replayed');
  } });
  t.after(() => service.stop());
  assert.equal((await service.flushMailbox('box', { force: true })).woke, false);
  assert.equal(restarted.getWakeTarget('box')?.deliveryInDoubt, true);
});

for (const mode of ['passive', 'orca-run', 'orca-terminal'] as const) {
  test(`${mode} pre-effect command failure backs off and resumes safely after repair`, async t => {
    const root = directory(t);
    const store = new MailboxStore(join(root, 'mailbox.json'));
    store.enqueue([alert('first')], { mailboxId: 'box', wakeTarget: {
      mode, ...(mode === 'passive' ? { hostAgent: 'claude-code' as const } : {}), runId: 'run', terminalHandle: 'term',
      interactiveCommand: join(root, 'missing'), orcaCommand: join(root, 'missing'),
    } });
    const failing = new WakeupRedriveService({ store, idleCheck: async () => ({ idle: true }) });
    t.after(() => failing.stop());
    assert.equal((await failing.flushMailbox('box')).woke, false);
    const failed = store.getWakeTarget('box')!;
    assert.equal(failed.deliveryInDoubt, undefined);
    assert.equal(failed.pendingWake, true);
    assert.equal(failed.attemptCount, 1);
    assert.ok(failed.earliestWakeAt > Date.now());
    assert.match(failed.lastWakeError!, /did not start|ENOENT/);
    await failing.stop();
    const repaired = new WakeupRedriveService({ store, idleCheck: async () => ({ idle: true }), commandRunner: async () => ({ stdout: '{"ok":true}' }) });
    t.after(() => repaired.stop());
    assert.equal((await repaired.flushMailbox('box', { force: true })).woke, true);
    assert.equal(store.getWakeTarget('box')?.pendingWake, false);
  });
}

test('local Orca target validation does not create an unknown-effect fence', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  let effects = 0;
  const service = new WakeupRedriveService({ store, idleCheck: async () => ({ idle: true }), commandRunner: async () => {
    effects++;
    return { stdout: '{"ok":true}' };
  } });
  t.after(() => service.stop());
  const claim = service.claim('box');
  store.enqueue([alert('first')], { mailboxId: 'box', wakeTarget: { mode: 'orca-run' } });
  claim.dispose();
  assert.equal((await service.flushMailbox('box')).woke, false);
  assert.equal(effects, 0);
  assert.equal(store.getWakeTarget('box')?.deliveryInDoubt, undefined);
  assert.equal(store.getWakeTarget('box')?.pendingWake, true);
  assert.equal(store.getWakeTarget('box')?.attemptCount, 1);
  store.upsertWakeTarget('box', { runId: 'repaired' });
  assert.equal((await service.flushMailbox('box', { force: true })).woke, true);
  assert.equal(effects, 1);
});


test('Orca sender retry preserves delivery fences', async t => {
  const calls: CommandRequest[] = [];
  let attempt = 0;
  const runner: CommandRunner = async request => {
    calls.push(request);
    attempt += 1;
    if (attempt === 1) throw new Error('no_active_sender_terminal');
    return { stdout: '{"ok":true}' };
  };
  await sendOrcaTerminalText(runner, 'orca', 'term-1', 'hello');
  assert.equal(calls.length, 2);
  assert.ok(calls[1]?.args.includes('--allow-self'));
  assert.equal(calls[1]?.env?.ORCA_TERMINAL_ID, 'term-1');
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.upsertWakeTarget('box', { mode: 'orca-terminal', terminalHandle: 'term-1' });
  settleWakeDeliveryFailure(store, 'box', new Error('no_active_sender_terminal'));
  assert.equal(store.getWakeTarget('box')?.deliveryInDoubt, true);
});

test('rotating inbound delivery preserves terminal thread affinity across call order', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.upsertWakeTarget('stream:inbox', { mode: 'orca-terminal', rotateAfterWaves: 1, terminalHandle: 'term-1', coalesceMs: 0 });
  const inbound = alert('thread-a', { semantic: { type: 'message', threadId: 'thread-a', replyMailbox: 'default' } });
  store.enqueue([inbound], { mailboxId: 'stream:inbox' });
  store.completeWakeDelivery('stream:inbox', 0, { terminalHandle: 'term-1' });
  store.recordInboundEnvelope(inbound, 'stream:inbox');
  assert.equal(store.resolveThreadLocalMailbox('thread-a'), 'terminal:term-1');
  assert.equal(store.getWakeTarget('terminal:term-1')?.rotateAfterWaves, undefined);
});

test('pinning a rotating session preserves existing terminal wake fences', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.upsertWakeTarget('stream:inbox', { mode: 'orca-terminal', rotateAfterWaves: 1, terminalHandle: 'term-1' });
  store.upsertWakeTarget('terminal:term-1', { mode: 'orca-terminal', terminalHandle: 'term-1', pendingWake: true });
  const notifications: string[] = [];
  const subscription = store.onWakeTarget(mailbox => notifications.push(mailbox));
  t.after(() => subscription.dispose());
  assert.equal(store.pinRotatingSessionMailbox('stream:inbox', 'term-1'), 'terminal:term-1');
  const target = store.getWakeTarget('terminal:term-1');
  assert.equal(target?.pendingWake, true);
  assert.ok(notifications.includes('terminal:term-1'));
  store.upsertWakeTarget('stream:inbox', { deliveryInDoubt: true, lastWakeError: 'fenced' });
  assert.equal(store.pinRotatingSessionMailbox('stream:inbox', 'term-2'), 'terminal:term-2');
  assert.equal(store.getWakeTarget('terminal:term-2')?.deliveryInDoubt, true);
});

test('durable spawn survives the account gate and redrives into one dedicated agent tab', async t => {
  // A created worktree is already an external effect: terminal launch failure must settle CAS
  // against a durable task, and recovery must never bootstrap agy inside Terminal 1.
  const root = directory(t);
  const workspacePath = join(root, 'child');
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const watermarksPath = join(root, 'dedup.json');
  const { runner: inventory, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'bare-shell', connected: true, writable: true, preview: 'user@host %' },
  ]);
  let accountReady = false;
  let launches = 0;
  const accountError = 'The native Antigravity account changed. Select the account again in Accounts before launching agy.';
  const runner: CommandRunner = async request => {
    if (request.args[0] === 'worktree' && request.args[1] === 'create') {
      state.calls.push(request);
      return { stdout: JSON.stringify({ ok: true, result: { worktree: { path: workspacePath } } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'create') {
      launches += 1;
      assert.equal(store.list({ mailboxId: 'task:account-gate' }).queuedCount, 1);
      assert.equal(request.args[request.args.indexOf('--command') + 1], 'agy --dangerously-skip-permissions');
      if (!accountReady) {
        state.calls.push(request);
        return { exitCode: 1, stdout: JSON.stringify({ ok: false, error: { code: 'runtime_error', message: accountError } }) };
      }
    }
    return inventory(request);
  };
  const kernel = new Kernel();
  kernel.ctx.provide('node', { getSelfCard: () => ({ nodeId: 'receiver' }) } as NodeService);
  await kernel.use(createDispatcherPlugin());
  t.after(() => kernel.stop());
  const dispatcher = kernel.ctx.services.dispatcher;
  dispatcher.registerPolicy(createDedupCasPolicyPlugin({ watermarksPath }));
  dispatcher.registerExecutor(createOrcaSpawnExecutorPlugin({
    store, commandRunner: runner, cwd: root, interactive: true, noParent: true,
  }), {});
  const message = alert('account-gate', {
    idempotencyKey: 'feedback:account', payload: { revision: 2, workspaceComment: 'Investigate account feedback' },
  });
  assert.equal((await dispatcher.dispatch(message)).stage, 'dispatched');
  assert.ok(Object.values(JSON.parse(readFileSync(watermarksPath, 'utf8'))).some((entry: any) => entry.status === 'settled'));
  const duplicate = await dispatcher.dispatch({ ...message, id: 'next-poll' });
  assert.equal(duplicate.detail, DEDUP_CAS_DUPLICATE_DETAIL);
  const initial = store.getWakeTarget('task:account-gate')!;
  assert.equal(initial.pendingWake, true);
  assert.equal(initial.attemptCount, 1);
  assert.equal(initial.deliveryInDoubt, undefined);
  assert.match(initial.lastWakeError!, /native Antigravity account changed/);
  assert.ok(state.calls.some(call => call.args[0] === 'worktree' && call.args[1] === 'set'));
  assert.equal(terminalSends(state.calls).length, 0);

  // Reload the durable owner to prove recovery does not depend on the executor's memory.
  const recovered = new MailboxStore(store.path);
  const service = new WakeupRedriveService({ store: recovered, commandRunner: runner, flushIntervalMs: 100_000 });
  t.after(() => service.stop());
  assert.equal((await service.flushMailbox('task:account-gate', { force: true })).woke, false);
  assert.equal(recovered.getWakeTarget('task:account-gate')!.attemptCount, 2);
  assert.equal(recovered.getWakeTarget('task:account-gate')!.deliveryInDoubt, undefined);
  accountReady = true;
  state.waitOk = false;
  assert.equal((await service.flushMailbox('task:account-gate', { force: true })).woke, false);
  assert.equal(recovered.getWakeTarget('task:account-gate')!.terminalHandle, 'term-new-1');
  assert.equal(state.created, 1);
  assert.equal(terminalSends(state.calls).length, 0);
  assert.equal(launches, 3, 'two failed launches and the first dedicated tab');

  // If that tab exits before delivery, recovery must replace it through Orca's account-aware
  // terminal creation path, never bootstrap the agent in the surviving bare login shell.
  state.terminals = state.terminals.filter(terminal => terminal.handle !== 'term-new-1');
  assert.equal((await service.flushMailbox('task:account-gate', { force: true })).woke, false);
  assert.equal(recovered.getWakeTarget('task:account-gate')!.terminalHandle, 'term-new-2');
  assert.equal(state.created, 2);
  assert.equal(terminalSends(state.calls).length, 0);
  state.waitOk = true;
  assert.equal((await service.flushMailbox('task:account-gate', { force: true })).woke, true);
  assert.equal(launches, 4, 'two failed launches, original tab and replacement; idle deferrals create no tabs');
  assert.equal(state.created, 2);
  assert.equal(sendsTo(state.calls, 'bare-shell').length, 0);
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 0);
  const sends = sendsTo(state.calls, 'term-new-2');
  assert.equal(sends.length, 1);
  assert.match(textOf(sends[0]), /Dealer Task dispatched to mailbox task:account-gate/);
  assert.equal(recovered.getWakeTarget('task:account-gate')!.pendingWake, false);
  assert.ok(state.calls.filter(call => call.args[0] === 'worktree' && call.args[1] === 'set').length >= 2);
});

test('spawn keeps durable custody through launch, comment and pointer failures', async t => {
  // Preparation failures remain retryable; an attempted pointer remains fenced. Neither
  // may turn the already-committed task into a failed dispatcher/CAS handoff.
  const root = directory(t);
  for (const failure of ['non-start', 'throw', 'rejected', 'rejected-exit1', 'thrown-rejection', 'signal-rejection', 'malformed', 'no-handle', 'comment', 'send']) {
    const store = new MailboxStore(join(root, `${failure}.json`));
    const calls: CommandRequest[] = [];
    const runner: CommandRunner = async request => {
      calls.push(request);
      const operation = request.args.slice(0, 2).join(' ');
      if (operation === 'worktree create') return { stdout: JSON.stringify({ ok: true, result: { path: join(root, failure) } }) };
      if (operation === 'terminal create') {
        assert.equal(new MailboxStore(store.path).getWakeTarget(`task:${failure}`)!.deliveryInDoubt, true,
          'launch uncertainty is durable before the command starts');
        if (failure === 'non-start') throw new CommandNotStartedError('ENOENT');
        if (failure === 'throw') throw new Error('connection lost');
        if (failure === 'rejected-exit1') return { stdout: '{"ok":false}', exitCode: 1 };
        if (failure === 'thrown-rejection' || failure === 'signal-rejection') {
          throw Object.assign(new Error('Orca refused or was interrupted'), { stdout: '{"ok":false}', exitCode: failure === 'signal-rejection' ? null : 1 });
        }
        if (failure === 'malformed') return { stdout: 'broken JSON' };
        if (failure === 'rejected') return { stdout: '{"ok":false}' };
        if (failure === 'no-handle') return { stdout: '{"ok":true}' };
        return { stdout: '{"ok":true,"result":{"handle":"task-tab"}}' };
      }
      if (operation === 'worktree set' && failure === 'comment') throw new Error('comment unavailable');
      if (operation === 'terminal send' && failure === 'send') throw new Error('send outcome unknown');
      return { stdout: '{"ok":true}' };
    };
    const receipt = await createOrcaSpawnExecutorPlugin({ store, commandRunner: runner, cwd: root, interactive: true })
      .execute([alert(failure, { payload: { workspaceComment: 'Task summary' } })], {} as ExecutionContext);
    const target = store.getWakeTarget(`task:${failure}`)!;
    assert.equal(store.list({ mailboxId: `task:${failure}` }).queuedCount, 1);
    if (failure === 'comment') {
      assert.equal(receipt.woke, true);
      assert.match(String(receipt.warning), /comment unavailable/);
      assert.equal(target.pendingWake, false);
    } else {
      assert.equal(receipt.woke, false);
      const retryable = ['non-start', 'rejected', 'rejected-exit1', 'thrown-rejection'].includes(failure);
      assert.equal(target.pendingWake, retryable, failure);
      assert.ok(receipt.reason);
      if (retryable) {
        assert.equal(target.deliveryInDoubt, undefined);
        assert.equal(target.attemptCount, 1);
        assert.equal(terminalSends(calls).length, 0);
      } else {
        assert.equal(target.deliveryInDoubt, true, 'unknown launch/pointer effects must never be automatically replayed');
        const before = calls.length;
        const service = new WakeupRedriveService({ store: new MailboxStore(store.path), commandRunner: runner });
        t.after(() => service.stop());
        await service.flushMailbox(`task:${failure}`, { force: true });
        assert.equal(calls.length, before, 'restart and forced redrive preserve the launch fence');
      }
    }
  }
});

test('task tab disappearing after idle proof is replaced without borrowing another terminal', async t => {
  // The dedicated-task policy must survive the final resolution race, not just initial planning.
  const root = directory(t);
  const workspacePath = join(root, 'workspace');
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const { runner: inventory, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'task-old', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
    { handle: 'bare-shell', connected: true, writable: true, preview: 'user@host %' },
    { handle: 'other-agent', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  let lists = 0;
  const runner: CommandRunner = async request => {
    if (request.args[0] === 'terminal' && request.args[1] === 'list') {
      lists++;
      // First inventory plans reuse; second proves the target idle; third is deliver's final check.
      if (lists === 3) state.terminals = state.terminals.filter(row => row.handle !== 'task-old');
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'create') {
      assert.equal(new MailboxStore(store.path).getWakeTarget('task:race')!.deliveryInDoubt, true);
      assert.ok(request.args.includes('--command'));
    }
    return inventory(request);
  };
  store.enqueue([alert('race')], { mailboxId: 'task:race', wakeTarget: {
    mode: 'orca-terminal', workspacePath, terminalHandle: 'task-old', agent: 'antigravity', coalesceMs: 0,
  } });
  const service = new WakeupRedriveService({ store, commandRunner: runner });
  t.after(() => service.stop());
  state.waitOk = false;
  const deferred = await service.flushMailbox('task:race', { force: true });
  assert.equal(deferred.woke, false);
  assert.equal(state.created, 1);
  assert.equal(store.getWakeTarget('task:race')!.terminalHandle, 'term-new-1');
  assert.equal(terminalSends(state.calls).length, 0);
  state.waitOk = true;
  assert.equal((await service.flushMailbox('task:race', { force: true })).woke, true);
  assert.equal(state.created, 1);
  assert.equal(sendsTo(state.calls, 'bare-shell').length, 0);
  assert.equal(sendsTo(state.calls, 'other-agent').length, 0);
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
});

test('Orca launch classification preserves structured refusal from a real nonzero subprocess', async () => {
  // runCommand rejects nonzero exits: stdout must survive that boundary for explicit refusal.
  const refusalRunner: CommandRunner = request => runCommand({
    ...request, command: process.execPath, cwd: undefined,
    args: ['-e', 'console.log(JSON.stringify({ok:false})); console.error("account gate"); process.exit(1)'],
  });
  await assert.rejects(spawnOrcaWorkspaceTerminal(refusalRunner, 'orca', '/unused'),
    error => error instanceof OrcaTerminalLaunchRejectedError && /account gate/.test(error.message));
  await assert.rejects(runCommand({ command: '/definitely-missing-dealer-test', args: [] }),
    error => error instanceof CommandNotStartedError && !('beforeExternalEffect' in error));
});

test('launch crash and handle persistence failure stay fenced across restart', async t => {
  // An absent handle after launch is not evidence that no tab was created.
  const root = directory(t);
  for (const failure of ['crash', 'persist']) {
    const store = new MailboxStore(join(root, `${failure}.json`));
    const mailboxId = `task:${failure}`;
    store.enqueue([alert(failure)], { mailboxId, wakeTarget: {
      mode: 'orca-terminal', workspacePath: root, coalesceMs: 0,
    } });
    if (failure === 'crash') {
      store.beginWakeDelivery(mailboxId); // Simulate a process exit during terminal creation.
    } else {
      const upsert = store.upsertWakeTarget.bind(store);
      t.mock.method(store, 'upsertWakeTarget', (id, patch) => {
        if (patch.terminalHandle) throw new Error('handle persistence failed');
        return upsert(id, patch);
      });
      await assert.rejects(launchFencedOrcaWorkspaceTerminal(store, async () => ({
        stdout: '{"ok":true,"result":{"handle":"created-before-disk-error"}}',
      }), mailboxId, { command: 'orca', workspacePath: root }), /persistence failed/);
    }
    const restarted = new MailboxStore(store.path);
    assert.equal(restarted.getWakeTarget(mailboxId)!.deliveryInDoubt, true);
    assert.equal(restarted.getWakeTarget(mailboxId)!.pendingWake, false);
    let calls = 0;
    const service = new WakeupRedriveService({ store: restarted, commandRunner: async () => {
      calls++;
      throw new Error('Must not retry an unknown launch');
    } });
    t.after(() => service.stop());
    await service.flushMailbox(mailboxId, { force: true });
    assert.equal(calls, 0);
  }
});

test('redrive fences both dedicated tab creation and non-task shell bootstrap once', async t => {
  // Shared launch settlement distinguishes non-start from unknown effects without double backoff.
  const root = directory(t);
  for (const task of [false, true]) {
    for (const nonStart of [false, true]) {
      const mailboxId = `${task ? 'task' : 'stream'}:launch-${nonStart}`;
      const store = new MailboxStore(join(root, `${task}-${nonStart}.json`));
      const { runner: inventory, state } = fakeOrcaWorkspace(root, [
        { handle: 'shell', connected: true, writable: true, preview: 'user@host %' },
      ]);
      const runner: CommandRunner = async request => {
        if (request.args[0] === 'terminal' && ['create', 'send'].includes(request.args[1])) {
          state.calls.push(request);
          assert.equal(new MailboxStore(store.path).getWakeTarget(mailboxId)!.deliveryInDoubt, true);
          throw nonStart ? new CommandNotStartedError('ENOENT') : new Error('connection lost during launch');
        }
        return inventory(request);
      };
      store.enqueue([alert(mailboxId)], { mailboxId, wakeTarget: {
        mode: 'orca-terminal', workspacePath: root, coalesceMs: 0,
      } });
      const service = new WakeupRedriveService({ store, commandRunner: runner, idleCheck: async () => ({ idle: true }) });
      t.after(() => service.stop());
      assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, false);
      const target = store.getWakeTarget(mailboxId)!;
      assert.equal(target.pendingWake, nonStart);
      assert.equal(target.deliveryInDoubt, nonStart ? undefined : true);
      assert.equal(target.attemptCount, 1, 'the shared helper settles once, including parked outcomes');
      assert.equal(state.calls.filter(call => call.args[0] === 'terminal' && call.args[1] === (task ? 'create' : 'send')).length, 1);
    }
  }
});

for (const [agent, envKey, resumeCommand] of [
  ['antigravity', 'ANTIGRAVITY_CONVERSATION_ID', "agy --dangerously-skip-permissions --conversation 'original-session'"],
  ['claude', 'CLAUDE_CODE_SESSION_ID', "claude --dangerously-skip-permissions --resume 'original-session'"],
  ['codex', 'CODEX_THREAD_ID', "codex --dangerously-bypass-approvals-and-sandbox resume 'original-session'"],
] as const) {
  test(`RPC caller capture and sleeping conversation resume: ${agent}`, async t => {
    const root = directory(t);
    const workspacePath = join(root, 'sleeping');
    const { runner, state } = fakeOrcaWorkspace(workspacePath);
    const runtime = await makeNode(t, root, 'self', { commandRunner: runner });
    await runtime.serveControl(async (argv, context) => {
      const stdout: string[] = [];
      const stderr: string[] = [];
      const code = await runDealerCli(argv, { runtime, signal: context.signal, stdout: line => stdout.push(line), stderr: line => stderr.push(line) });
      return { code, stdout, stderr };
    });
    const env = { ORCA_TERMINAL_HANDLE: 'term-origin', ORCA_WORKTREE_ID: `repo::${workspacePath}`, ORCA_PANE_KEY: 'tab:leaf', [envKey]: 'original-session' };
    const invoke = async (args: string[], callerEnv = env) => {
      const out: string[] = [];
      const code = await runDealerCli([...args, '--json'], { stateDir: runtime.stateDir, env: callerEnv, stdout: line => out.push(line), stderr: line => assert.fail(line) });
      assert.equal(code, 0);
      return JSON.parse(out.join('\n'));
    };
    const sent = await invoke(['send', '--network', 'local', '--to', 'member:self', '--topic', 'test.resume', '--body', 'hello']);
    const mailboxId = 'worktree:sleeping';
    const bound = runtime.mailbox.getWakeTarget(mailboxId)!;
    assert.equal(bound.agent, agent);
    assert.equal(bound.sessionId, 'original-session');
    assert.equal(bound.paneKey, 'tab:leaf');
    for (const args of [['inbox', 'list'], ['inbox', 'check', '--peek']]) {
      await invoke(args, { ...env, ORCA_TERMINAL_HANDLE: 'term-foreign', [envKey]: 'foreign-session' });
      assert.deepEqual(runtime.mailbox.getWakeTarget(mailboxId), bound);
    }
    await invoke(['reply', sent.messageId, '--body', 'reply'], {} as typeof env);
    await until(() => state.calls.some(call => call.args[1] === 'send' && call.args.includes('term-new-1')), 'resumed conversation pointer');
    const create = state.calls.find(call => call.args[1] === 'create')!;
    assert.equal(create.args[create.args.indexOf('--command') + 1], resumeCommand);
    assert.ok(!create.args.includes('--focus'));
    assert.ok(state.calls.findIndex(call => call.args[1] === 'wait') < state.calls.findIndex(call => call.args[1] === 'send'));
    const resumed = runtime.mailbox.getWakeTarget(mailboxId)!;
    assert.equal(resumed.terminalHandle, 'term-new-1');
    assert.equal(resumed.sessionId, 'original-session');
    assert.equal(resumed.paneKey, undefined);
    await invoke(['inbox', 'check'], { ...env, ORCA_TERMINAL_HANDLE: 'term-new-1', [envKey]: '' });
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.sessionId, 'original-session');
    await invoke(['inbox', 'check'], { ...env, ORCA_TERMINAL_HANDLE: 'term-restored-shell', [envKey]: '' });
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.sessionId, 'original-session');
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.terminalHandle, 'term-restored-shell');
    await invoke(['inbox', 'check'], { ...env, ORCA_TERMINAL_HANDLE: 'term-shell', ORCA_PANE_KEY: '', [envKey]: '' });
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.sessionId, undefined);
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.paneKey, undefined);
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.agent, undefined);
    await runtime.mountNetwork({ networkId: 'capture', passphrase: 'capture-test-secret' });
    await invoke(['broadcast', '--network', 'capture', '--topic', 'test.capture', '--body', 'hello']);
    const broadcastTarget = runtime.mailbox.getWakeTarget(mailboxId)!;
    assert.equal(broadcastTarget.terminalHandle, 'term-origin');
    assert.equal(broadcastTarget.agent, agent);
    assert.equal(broadcastTarget.sessionId, 'original-session');
    assert.equal(broadcastTarget.paneKey, 'tab:leaf');
  });
}

for (const mailboxId of ['worktree:bound', 'task:bound']) {
  for (const restored of [undefined, 'recorded', 'coordinates'] as const) {
    test(`conversation isolation with busy sibling (${mailboxId}): ${restored ?? 'resume'}`, async t => {
      const root = directory(t);
      const workspacePath = join(root, 'bound');
      const store = new MailboxStore(join(root, 'mailbox.json'));
      const { runner, state } = fakeOrcaWorkspace(workspacePath, [
        { handle: 'term-sibling', connected: true, writable: true, agentIdentity: 'antigravity', preview: '> human draft' },
      ]);
      if (restored) state.terminals.push({ handle: 'term-restored', connected: true, writable: true, agentIdentity: 'antigravity', preview: '', orphaned: true,
        ...(restored === 'recorded' ? { recordedPaneKey: 'tab:leaf' } : { tabId: 'tab', leafId: 'leaf' }),
      });
      state.agents = [{ state: 'working', paneKey: 'sibling:pane' }, ...(restored ? [{ state: 'done', paneKey: 'tab:leaf' }] : [])];
      const service = new WakeupRedriveService({ store, commandRunner: runner });
      t.after(() => service.stop());
      store.enqueue([alert('conversation-mail')], { mailboxId, wakeTarget: {
        mode: 'orca-terminal', workspacePath, terminalHandle: 'term-origin', sessionId: 'original', paneKey: 'tab:leaf', coalesceMs: 0,
      } });
      const result = await service.flushMailbox(mailboxId, { force: true });
      assert.equal(result.woke, true);
      assert.equal(state.created, restored ? 0 : 1);
      assert.equal(sendsTo(state.calls, 'term-sibling').length, 0);
      assert.equal(sendsTo(state.calls, restored ? 'term-restored' : 'term-new-1').length, 1);
    });
  }

}

test('Orca resume commands quote conversation IDs and reject control characters before launch', async () => {
  assert.equal(buildOrcaAgentLaunchCommand({ agent: 'claude', sessionId: "a'b;$(touch nope)" }),
    "claude --dangerously-skip-permissions --resume 'a'\\''b;$(touch nope)'");
  assert.equal(buildOrcaAgentLaunchCommand({ sessionId: 'session', interactiveCommand: 'custom-agent --flag' }), 'custom-agent --flag');
  assert.equal(buildOrcaAgentLaunchCommand({ sessionId: 'session', interactiveCommand: 'agy --dangerously-skip-permissions' }), "agy --dangerously-skip-permissions --conversation 'session'");
  let calls = 0;
  for (const sessionId of ['bad\nvalue', 'bad\rvalue', 'bad\0value']) {
    await assert.rejects(spawnOrcaWorkspaceTerminal(async () => { calls++; return { stdout: '' }; }, 'orca', '/workspace', undefined, { sessionId }), /Invalid Orca conversation ID/);
  }
  assert.equal(calls, 0);
});

test('rotating mailbox pins the active conversation identity durably', t => {
  const file = join(directory(t), 'mailbox.json');
  const store = new MailboxStore(file);
  store.upsertWakeTarget('stream:wave', {
    mode: 'orca-terminal', workspacePath: '/workspace', terminalHandle: 'term-original',
    agent: 'codex', sessionId: 'original-session', paneKey: 'tab:leaf', rotateAfterWaves: 1,
  });
  assert.equal(store.pinRotatingSessionMailbox('stream:wave', 'term-original'), 'terminal:term-original');
  const pinned = new MailboxStore(file).getWakeTarget('terminal:term-original')!;
  assert.equal(pinned.agent, 'codex');
  assert.equal(pinned.sessionId, 'original-session');
  assert.equal(pinned.paneKey, 'tab:leaf');
  assert.equal(pinned.rotateAfterWaves, undefined);
});

for (const busyOwner of ['restored', 'child'] as const) {
  test(`restored orphan pane gates its ${busyOwner} agent by recorded identity`, async t => {
    const root = directory(t);
    const workspacePath = join(root, 'restored');
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const { runner, state } = fakeOrcaWorkspace(workspacePath, [{
      handle: 'term-restored', connected: true, writable: true, orphaned: true,
      tabId: 'pty:5afdf9f6', leafId: 'pty:5afdf9f6', recordedPaneKey: 'tab:leaf',
      agentIdentity: 'antigravity', preview: '',
    }]);
    let busy = true;
    const service = new WakeupRedriveService({ store, commandRunner: async request => {
      state.agents = [{ state: busy && busyOwner === 'restored' ? 'working' : 'done', paneKey: 'tab:leaf' }];
      const output = await runner(request);
      if (busyOwner === 'child' && request.args[0] === 'worktree' && request.args[1] === 'ps') {
        const inventory = JSON.parse(output.stdout);
        inventory.result.worktrees[0].childWorktreeIds = ['child'];
        inventory.result.worktrees.push({ path: join(workspacePath, 'child'), worktreeId: 'child',
          unverifiableTerminalCount: 0, agents: [{ state: busy ? 'working' : 'done', parentPaneKey: 'tab:leaf' }],
        });
        return { stdout: JSON.stringify(inventory) };
      }
      return output;
    } });
    t.after(() => service.stop());
    store.enqueue([alert('restored-busy')], { mailboxId: 'worktree:restored', wakeTarget: {
      mode: 'orca-terminal', workspacePath, terminalHandle: 'term-old', paneKey: 'tab:leaf', sessionId: 'original', coalesceMs: 0,
    } });
    assert.equal((await service.flushMailbox('worktree:restored', { force: true })).woke, false);
    assert.equal(terminalSends(state.calls).length, 0);
    busy = false;
    assert.equal((await service.flushMailbox('worktree:restored', { force: true })).woke, true);
    assert.equal(state.created, 0);
    assert.equal(sendsTo(state.calls, 'term-restored').length, 1);
  });
}

for (const siblingAgent of ['antigravity', undefined]) {
  test(`dedicated task cannot rebind to ${siblingAgent ?? 'shell'} after its pane exits during selection`, async t => {
    const root = directory(t);
    const workspacePath = join(root, 'task');
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const { runner, state } = fakeOrcaWorkspace(workspacePath, [
      { handle: 'term-task', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
      { handle: 'term-sibling', connected: true, writable: true, agentIdentity: siblingAgent, preview: '' },
    ]);
    let lists = 0;
    const service = new WakeupRedriveService({ store, commandRunner: async request => {
      // Initial liveness, authoritative idle, and replacement check see the task tab;
      // it exits just before the final read-only selection inventory.
      if (request.args[0] === 'terminal' && request.args[1] === 'list' && ++lists === 4) {
        state.terminals = state.terminals.filter(terminal => terminal.handle !== 'term-task');
      }
      return runner(request);
    } });
    t.after(() => service.stop());
    store.enqueue([alert('task-race')], { mailboxId: 'task:isolated', wakeTarget: {
      mode: 'orca-terminal', workspacePath, terminalHandle: 'term-task', coalesceMs: 0,
    } });
    const result = await service.flushMailbox('task:isolated', { force: true });
    assert.equal(result.woke, true);
    assert.equal(state.created, 1);
    assert.equal(sendsTo(state.calls, 'term-sibling').length, 0);
    assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
  });
}

test('caller identity transitions clear omitted fields only when identity changes', async t => {
  const root = directory(t);
  const runtime = await makeNode(t, root, 'identity');
  const workspacePath = join(root, 'identity');
  const mailboxId = 'worktree:identity';
  const cases = [
    { name: 'new session without pane', env: { CODEX_THREAD_ID: 'new-session' }, expected: { agent: 'codex', sessionId: 'new-session', paneKey: undefined } },
    { name: 'new pane without session', env: { ORCA_PANE_KEY: 'new:pane' }, expected: { agent: undefined, sessionId: undefined, paneKey: 'new:pane' } },
    { name: 'restored pane subshell', env: { ORCA_TERMINAL_HANDLE: 'term-restored', ORCA_PANE_KEY: 'old:pane' }, expected: { agent: 'codex', sessionId: 'old-session', paneKey: 'old:pane' } },
    { name: 'same session new handle', env: { ORCA_TERMINAL_HANDLE: 'term-restored', CODEX_THREAD_ID: 'old-session' }, expected: { agent: 'codex', sessionId: 'old-session', paneKey: 'old:pane' } },
    { name: 'bare shell new handle', env: { ORCA_TERMINAL_HANDLE: 'term-shell' }, expected: { agent: undefined, sessionId: undefined, paneKey: undefined } },
  ];
  for (const { name, env, expected } of cases) {
    runtime.mailbox.upsertWakeTarget(mailboxId, {
      mode: 'orca-terminal', workspacePath, terminalHandle: 'term-original',
      agent: 'codex', sessionId: 'old-session', paneKey: 'old:pane',
    });
    const code = await runDealerCli(['inbox', 'check', '--json'], {
      runtime, env: { ORCA_WORKTREE_ID: `repo::${workspacePath}`, ORCA_TERMINAL_HANDLE: 'term-original', ...env },
      stdout: () => {}, stderr: line => assert.fail(line),
    });
    assert.equal(code, 0, name);
    const actual = runtime.mailbox.getWakeTarget(mailboxId)!;
    assert.deepEqual({ agent: actual.agent, sessionId: actual.sessionId, paneKey: actual.paneKey }, expected, name);
  }
});
