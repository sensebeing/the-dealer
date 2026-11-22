import { createTestWakeupDrivers } from './helpers/wakeup-drivers.ts';
import { pinOrcaTerminalSessionMailbox, reconcileCallerOrcaWakeTarget } from '../packages/plugins-orca/index.ts';
import { Kernel } from '../packages/kernel/index.ts';
import { createDispatcherPlugin } from '../packages/dispatcher/index.ts';
import { createDedupCasPolicyPlugin, DEDUP_CAS_DUPLICATE_DETAIL } from '../packages/plugins-policy/index.ts';
import type { NodeService } from '../packages/core-types/index.ts';
import { buildOrcaAgentLaunchCommand, spawnOrcaWorkspaceTerminal, OrcaTerminalLaunchRejectedError, checkOrcaWorkspaceIdle, defaultAgentLaunchCommand, findOrcaWorktreeByName, isAntigravityModePlaceholder, isOrcaComposerEmpty, sendOrcaTerminalText } from '../packages/plugins-orca/index.ts';
import { CommandNotStartedError, runCommand } from '../packages/subprocess/index.ts';
import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
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
  createOrcaWaveExecutorPlugin,
  resolveConfiguredOrcaWakeTargetPatch, resolveCallerOrcaWakeTargetPatch,
  resolveBoundMailboxId,
  deliverSpawnedOrcaTerminalWakeup, deliverAgentHostWakeTarget,
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
  handle: string;
  connected: boolean;
  writable: boolean;
  agentIdentity?: string;
  preview?: string;
  tabId?: string;
  leafId?: string;
}

interface FakeOrcaAgent {
  state: string;
  paneKey?: string;
  parentPaneKey?: string | null;
}

interface FakeOrcaChild {
  path: string;
  worktreeId: string;
  childWorktreeIds?: string[];
  unverifiableTerminalCount: number;
  agents?: FakeOrcaAgent[];
  terminals?: FakeTerminalRow[];
  terminalsTruncated?: boolean;
  lineage?: { createdByTerminalHandle?: string };
}

/** Minimal Orca inventory: one fixed workspace whose `terminal create` adds a live tab. */
function fakeOrcaWorkspace(workspacePath: string, initial: FakeTerminalRow[] = []) {
  const state = {
    calls: [] as CommandRequest[],
    terminals: [...initial],
    waitOk: true,
    created: 0,
    agents: undefined as FakeOrcaAgent[] | undefined,
    children: [] as FakeOrcaChild[],
    childWorktreeIds: undefined as string[] | undefined,
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
              childWorktreeIds: state.childWorktreeIds ?? state.children.map(child => child.worktreeId),
              unverifiableTerminalCount: 0,
              agents: state.agents ?? state.terminals.filter(t => t.connected).map(t => ({ state: 'done', ...(t.tabId && t.leafId ? { paneKey: `${t.tabId}:${t.leafId}` } : {}) })),
            }, ...state.children.map(({ path, worktreeId, childWorktreeIds, unverifiableTerminalCount, agents }) =>
              ({ path, worktreeId, childWorktreeIds, unverifiableTerminalCount, agents }))],
          },
        }),
      };
    }
    if (request.args[0] === 'worktree' && request.args[1] === 'list') {
      const worktrees = state.children.map(({ worktreeId, lineage }) => ({ id: worktreeId, lineage }));
      return { stdout: JSON.stringify({ ok: true, result: { worktrees } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'list') {
      const child = state.children.find(item => request.args.includes(`path:${item.path}`));
      return { stdout: JSON.stringify({ ok: true, result: {
        terminals: child ? child.terminals ?? [] : state.terminals, truncated: child?.terminalsTruncated,
      } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'create') {
      state.created += 1;
      const handle = `term-new-${state.created}`;
      state.agents?.push({ state: 'done', paneKey: `${handle}:root` });
      state.terminals.push({ handle, connected: true, writable: true, agentIdentity: 'antigravity', preview: '', tabId: handle, leafId: 'root' });
      return { stdout: JSON.stringify({ ok: true, result: { terminal: { handle } } }) };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'close') {
      const index = request.args.indexOf('--terminal');
      const handle = index >= 0 ? request.args[index + 1] : undefined;
      if (handle) state.terminals = state.terminals.filter(terminal => terminal.handle !== handle);
      return { stdout: '{"ok":true}' };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'wait') {
      return { stdout: JSON.stringify({ ok: state.waitOk }) };
    }
    return { stdout: '{"ok":true}' };
  };
  return { runner, state };
}

const probeMailbox = 'stream:alert-wave:1';
const probeTerminal = {
  handle: 'term-wave',
  connected: true,
  writable: true,
  agentIdentity: 'antigravity',
  tabId: 'wave',
  leafId: 'root',
  preview: '',
};

function slotProbeFixture(t: TestContext, options: { wave?: boolean; paneKey?: string } = {}) {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const orca = fakeOrcaWorkspace(root, [{ ...probeTerminal }]);
  if (options.wave) {
    store.enqueue([alert('wave')], { mailboxId: probeMailbox });
    store.check({ mailboxId: probeMailbox });
  }
  store.upsertWakeTarget(probeMailbox, {
    driver: 'orca-terminal',
    rotateAfterWaves: 1,
    driverConfig: { workspacePath: root, terminalHandle: probeTerminal.handle, paneKey: options.paneKey },
  });
  return { root, store, ...orca, ...startSlotProbeService(t, store, orca.runner) };
}

function startSlotProbeService(t: TestContext, store: MailboxStore, commandRunner: CommandRunner) {
  const errors: unknown[] = [];
  const service = new WakeupRedriveService({
    store, drivers: createTestWakeupDrivers({ commandRunner }),
    flushIntervalMs: 100_000, onError: error => errors.push(error),
  });
  t.after(() => service.stop());
  return { service, errors };
}

test('slot probe uses agent state, the composer draft and TUI-idle for a leased wave', async t => {
  const { service, state, errors } = slotProbeFixture(t, { wave: true, paneKey: 'wave:root' });
  state.agents = [{ state: 'working', paneKey: 'wave:root' }];
  assert.equal((await service.probeSlot(probeMailbox)).busy, true);
  state.agents[0].state = 'done';
  state.terminals[0].preview = '> approval draft';
  assert.equal((await service.probeSlot(probeMailbox)).busy, true);
  state.terminals[0].preview = '';
  state.waitOk = false;
  assert.equal((await service.probeSlot(probeMailbox)).busy, true);
  state.waitOk = true;
  assert.deepEqual(await service.probeSlot(probeMailbox), {
    busy: false, ready: false, reason: 'quiescent_leased_wave',
  });
  assert.deepEqual(errors, []);
});

test('slot probe follows a closed terminal’s pane to its active child worktree', async t => {
  const { root, service, state, errors } = slotProbeFixture(t, { wave: true, paneKey: 'wave:root' });
  state.terminals = [];
  state.children = [{
    path: join(root, 'child'), worktreeId: 'child', unverifiableTerminalCount: 0,
    agents: [{ state: 'working', parentPaneKey: 'wave:root' }],
  }];
  assert.equal((await service.probeSlot(probeMailbox)).busy, true);
  state.children[0].agents = [];
  assert.deepEqual(await service.probeSlot(probeMailbox), {
    busy: false, ready: false, reason: 'terminal_gone',
  });
  assert.deepEqual(errors, []);
});

test('a vanished pane still gates workspace and Orca run readiness on its own work', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const sibling = { ...probeTerminal, handle: 'term-sibling', tabId: 'sibling', preview: '> human draft' };
  const { runner, state } = fakeOrcaWorkspace(root, [sibling]);
  const paneKey = 'vanished:root';
  const terminalHandle = 'term-vanished';
  const ownChild: FakeOrcaChild = {
    path: join(root, 'own-child'), worktreeId: 'own-child', unverifiableTerminalCount: 0,
    agents: [{ state: 'working', parentPaneKey: paneKey }],
    terminals: [{ ...probeTerminal, handle: 'term-child' }],
    lineage: { createdByTerminalHandle: terminalHandle },
  };
  state.children = [ownChild, {
    path: join(root, 'sibling-child'), worktreeId: 'sibling-child', unverifiableTerminalCount: 0,
    agents: [{ state: 'working', parentPaneKey: 'sibling:root' }],
  }];
  state.agents = [{ state: 'working', paneKey: 'sibling:root' }];
  store.upsertWakeTarget(probeMailbox, {
    driver: 'orca-run', driverConfig: { workspacePath: root, paneKey, terminalHandle, runId: 'run-test' },
  });
  const { service, errors } = startSlotProbeService(t, store, request =>
    request.args[0] === 'orchestration' && request.args[1] === 'worker-list'
      ? Promise.resolve({ stdout: '{"ok":true,"result":{"workers":[]}}' })
      : runner(request),
  );
  const workspaceIdle = () => checkOrcaWorkspaceIdle(runner, 'orca', root, undefined, {
    targetPaneKey: paneKey, targetTerminalHandle: terminalHandle,
  });
  const assertBusy = async () => {
    assert.equal((await workspaceIdle()).idle, false);
    assert.equal((await service.probeSlot(probeMailbox)).busy, true);
  };

  await assertBusy();
  ownChild.agents = [{ state: 'working', parentPaneKey: null }];
  await assertBusy();
  ownChild.agents = [{ state: 'done', parentPaneKey: null }];
  state.agents.push({ state: 'working', paneKey });
  await assertBusy();
  state.agents.pop();
  state.agents.push({ state: 'working' });
  await assertBusy();
  state.agents[1].state = 'done';
  assert.equal((await workspaceIdle()).idle, true);
  const slot = await service.probeSlot(probeMailbox);
  assert.equal(slot.busy, false, slot.reason);
  assert.equal(slot.ready, true, slot.reason);
  assert.deepEqual(errors, []);
});

test('slot probe leaves an active sibling child alone when the leased terminal is gone without a pane', async t => {
  const { root, store, service, state } = slotProbeFixture(t, { wave: true });
  const sibling = { ...probeTerminal, handle: 'term-sibling', tabId: 'sibling' };
  state.terminals = [sibling];
  state.children = [{
    path: join(root, 'sibling-child'), worktreeId: 'sibling-child', unverifiableTerminalCount: 0,
    agents: [{ state: 'working', parentPaneKey: 'sibling:root' }],
  }];
  store.upsertWakeTarget('stream:alert-wave:2', {
    driver: 'orca-terminal', driverConfig: { workspacePath: root, terminalHandle: sibling.handle },
  });
  state.agents = [{ state: 'working', paneKey: 'sibling:root' }];
  assert.deepEqual(await service.probeSlot(probeMailbox), {
    busy: false, ready: false, reason: 'terminal_gone',
  });
  assert.equal(state.calls.some(call => call.args.includes(`path:${state.children[0].path}`)), false);
  state.agents = [{ state: 'working', paneKey: 'unmatched:root' }];
  assert.equal((await service.probeSlot(probeMailbox)).busy, true);
  state.agents = [{ state: 'working' }];
  assert.equal((await service.probeSlot(probeMailbox)).busy, true);
  state.agents = [];
  state.children[0].agents = [{ state: 'working' }];
  assert.equal((await service.probeSlot(probeMailbox)).busy, true);
});

function worktreeListCalls(calls: readonly CommandRequest[]): number {
  return calls.filter(call => call.args[0] === 'worktree' && call.args[1] === 'list').length;
}

for (const mode of ['retiring', 'bound'] as const) {
  test(`an unmatched explicit parent pane blocks a vanished ${mode} terminal despite sibling lineage`, async t => {
    const root = directory(t);
    const sibling = { ...probeTerminal, handle: 'term-sibling', tabId: 'sibling' };
    const { runner, state } = fakeOrcaWorkspace(root, [sibling]);
    const child: FakeOrcaChild = {
      path: join(root, 'child'), worktreeId: 'child', unverifiableTerminalCount: 0,
      agents: [{ state: 'working', parentPaneKey: '%vanished-retiring-pane' }],
      lineage: { createdByTerminalHandle: sibling.handle },
    };
    state.children = [child];
    const options = mode === 'retiring'
      ? { targetTerminalHandle: null, retiringTerminalHandle: probeTerminal.handle }
      : { targetTerminalHandle: probeTerminal.handle, excludedHandles: new Set([sibling.handle]) };
    const idle = async () => (await checkOrcaWorkspaceIdle(runner, 'orca', root, undefined, options)).idle;

    assert.equal(await idle(), false);
    child.agents!.push({ state: 'working', parentPaneKey: null });
    assert.equal(await idle(), false, 'missing pane records cannot override an unmatched explicit parent');
    child.agents = [{ state: 'working', parentPaneKey: ' sibling:root ' }];
    assert.equal(await idle(), true, 'a known sibling pane positively attributes the child');
    assert.equal(worktreeListCalls(state.calls), 0);
  });
}

test('sibling exclusion requires positive attribution throughout the subtree', async t => {
  const root = directory(t);
  const sibling = { ...probeTerminal, handle: 'term-sibling', tabId: 'sibling' };
  const { runner, state } = fakeOrcaWorkspace(root, [probeTerminal, sibling]);
  const child: FakeOrcaChild = {
    path: join(root, 'child'), worktreeId: 'child', childWorktreeIds: ['grandchild'],
    unverifiableTerminalCount: 0, agents: [], lineage: { createdByTerminalHandle: sibling.handle },
  };
  const grandchild: FakeOrcaChild = {
    path: join(child.path, 'grandchild'), worktreeId: 'grandchild', unverifiableTerminalCount: 0,
    agents: [{ state: 'working', parentPaneKey: ' wave:root ' }],
    lineage: { createdByTerminalHandle: sibling.handle },
  };
  state.childWorktreeIds = [child.worktreeId];
  state.children = [child, grandchild];
  const idle = async () => (await checkOrcaWorkspaceIdle(runner, 'orca', root, undefined, {
    targetTerminalHandle: probeTerminal.handle,
  })).idle;

  assert.equal(await idle(), false, 'a grandchild with the target parent pane stays in scope');
  child.agents = [{ state: 'working', parentPaneKey: 'sibling:root' }];
  assert.equal(await idle(), false, 'an explicit sibling parent cannot hide its descendant');
  child.agents = [];
  grandchild.agents = [{ state: 'working', parentPaneKey: null }];
  grandchild.lineage = { createdByTerminalHandle: probeTerminal.handle };
  assert.equal(await idle(), false, 'a grandchild created by the target terminal stays in scope');

  grandchild.lineage.createdByTerminalHandle = sibling.handle;
  state.calls.length = 0;
  assert.equal(await idle(), true);
  assert.equal(worktreeListCalls(state.calls), 1, 'both levels use the same lineage inventory');

  grandchild.unverifiableTerminalCount = 1;
  assert.equal(await idle(), false, 'an unverifiable grandchild stays in scope');
  grandchild.unverifiableTerminalCount = 0;
  grandchild.childWorktreeIds = [child.worktreeId];
  assert.equal(await idle(), false, 'a cycle cannot establish sibling ownership');
  state.children.pop();
  assert.equal(await idle(), false, 'a missing grandchild cannot establish sibling ownership');
});

test('sibling attribution follows a descendant creator terminal during slot probes and rotation', async t => {
  for (const byPane of [false, true]) {
    const { root, store, service, state, runner } = slotProbeFixture(t, { paneKey: 'wave:root' });
    const sibling = { ...probeTerminal, handle: 'term-sibling', tabId: 'sibling' };
    const childTerminal = { ...probeTerminal, handle: 'term-c1', tabId: 'c1' };
    state.terminals.push(sibling);
    const child: FakeOrcaChild = {
      path: join(root, 'c1'), worktreeId: 'c1', childWorktreeIds: ['c2'], unverifiableTerminalCount: 0,
      agents: byPane ? [{ state: 'working', parentPaneKey: 'sibling:root' }] : [],
      terminals: [childTerminal],
      ...(byPane ? {} : { lineage: { createdByTerminalHandle: sibling.handle } }),
    };
    const grandchild: FakeOrcaChild = {
      path: join(child.path, 'c2'), worktreeId: 'c2', unverifiableTerminalCount: 0,
      agents: [{ state: 'working', parentPaneKey: null }], lineage: { createdByTerminalHandle: childTerminal.handle },
    };
    state.childWorktreeIds = [child.worktreeId];
    state.children = [child, grandchild];

    if (!byPane) {
      child.lineage!.createdByTerminalHandle = probeTerminal.handle;
      assert.equal((await service.probeSlot(probeMailbox)).busy, true, 'an owned creator cannot pass through its terminals');
      child.lineage!.createdByTerminalHandle = sibling.handle;
    }
    grandchild.agents![0].parentPaneKey = 'wave:root';
    assert.equal((await service.probeSlot(probeMailbox)).busy, true, 'explicit ownership wins over transitive lineage');
    grandchild.agents![0].parentPaneKey = null;
    child.terminals = [];
    assert.equal((await service.probeSlot(probeMailbox)).busy, true, 'the creator terminal must be in the inventory');
    child.terminals = [childTerminal];
    child.terminalsTruncated = true;
    const failed = await service.probeSlot(probeMailbox);
    assert.equal(failed.busy, true);
    assert.match(failed.reason!, /Incomplete Orca terminal inventory/);
    await assert.rejects(checkOrcaWorkspaceIdle(runner, 'orca', root, undefined, {
      targetTerminalHandle: probeTerminal.handle, throwOnProbeError: true,
    }), /Incomplete Orca terminal inventory/);
    child.terminalsTruncated = false;

    state.calls.length = 0;
    assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
    assert.equal(worktreeListCalls(state.calls), 1);
    assert.equal(state.calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'list' &&
      call.args.includes(`path:${child.path}`)).length, 1);

    store.upsertWakeTarget('stream:alert-wave:2', {
      driver: 'orca-terminal', rotateAfterWaves: 1, driverConfig: { workspacePath: root, terminalHandle: sibling.handle },
    });
    store.upsertWakeTarget(probeMailbox, { completedWaves: 1 });
    store.enqueue([alert('next-wave')], { mailboxId: probeMailbox });
    const wake = await service.flushMailbox(probeMailbox, { force: true });
    assert.equal(wake.woke, true, wake.reason);
    assert.equal(wake.rotated, true);
    assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
    assert.equal(sendsTo(state.calls, sibling.handle).length, 0);
  }
});

test('durable lineage lets an idle slot rotate and wake while sibling follow-ups are working', async t => {
  const { root, store, service, state } = slotProbeFixture(t);
  const sibling = { ...probeTerminal, handle: 'term-sibling', tabId: 'sibling' };
  state.terminals.push(sibling);
  state.agents = [{ state: 'done', paneKey: 'wave:root' }, { state: 'working', paneKey: 'sibling:root' }];
  state.children = ['follow-up-a', 'follow-up-b'].map(worktreeId => ({
    path: join(root, worktreeId), worktreeId, unverifiableTerminalCount: 0,
    agents: [{ state: 'working', parentPaneKey: null }],
    lineage: { createdByTerminalHandle: ` ${sibling.handle} ` },
  }));

  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
  assert.equal(worktreeListCalls(state.calls), 1, 'one lookup covers both children');
  store.upsertWakeTarget('stream:alert-wave:2', {
    driver: 'orca-terminal', rotateAfterWaves: 1, driverConfig: { workspacePath: root, terminalHandle: sibling.handle },
  });
  store.upsertWakeTarget(probeMailbox, { completedWaves: 1 });

  state.children[0].agents!.push({ state: 'done', parentPaneKey: 'wave:root' });
  assert.equal((await service.probeSlot(probeMailbox)).busy, true, 'an explicit parent pane keeps the child in scope');
  state.children[0].agents!.pop();
  state.children[0].unverifiableTerminalCount = 1;
  assert.equal((await service.probeSlot(probeMailbox)).busy, true, 'unverifiable child terminals keep the child in scope');
  state.children[0].unverifiableTerminalCount = 0;

  const siblingAgents = state.children[0].agents;
  state.children[0].agents = undefined;
  assert.deepEqual(await service.probeSlot(probeMailbox), {
    busy: true, ready: false, reason: 'Workspace or descendant is busy or unverifiable',
  });
  state.children[0].agents = siblingAgents;

  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
  state.terminals.pop();
  state.children[0].unverifiableTerminalCount = 1;
  assert.equal((await service.probeSlot(probeMailbox)).busy, true, 'a sibling claim cannot exclude an unverifiable child with one live tab');
  state.children[0].unverifiableTerminalCount = 0;
  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true }, 'a sibling claim survives a missing terminal');
  state.terminals.push(sibling);
  state.children[0].agents = undefined;
  store.enqueue([alert('next-wave')], { mailboxId: probeMailbox });
  const blockedWake = await service.flushMailbox(probeMailbox, { force: true });
  assert.equal(blockedWake.woke, false);
  assert.equal(state.created, 0);
  assert.deepEqual(state.terminals.map(terminal => terminal.handle), [probeTerminal.handle, sibling.handle]);
  state.children[0].agents = siblingAgents;
  state.calls.length = 0;
  const wake = await service.flushMailbox(probeMailbox, { force: true });
  assert.equal(wake.woke, true, wake.reason);
  assert.equal(wake.rotated, true);
  assert.equal(worktreeListCalls(state.calls), 2, 'rotation and the fresh tab each prove readiness');
  assert.deepEqual(state.terminals.map(terminal => terminal.handle), [sibling.handle, 'term-new-1']);
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
  assert.equal(sendsTo(state.calls, sibling.handle).length, 0);
});

test('durable lineage keeps a slot busy on its own follow-up after the root terminal closes', async t => {
  const { root, store, service, state } = slotProbeFixture(t, { wave: true });
  const sibling = { ...probeTerminal, handle: 'term-sibling', tabId: 'sibling' };
  state.terminals.push(sibling);
  state.children = [{
    path: join(root, 'follow-up'), worktreeId: 'follow-up', unverifiableTerminalCount: 0,
    agents: [{ state: 'working' }], lineage: { createdByTerminalHandle: probeTerminal.handle },
  }];
  const busy = { busy: true, ready: false, reason: 'Workspace or descendant is busy or unverifiable' };
  assert.deepEqual(await service.probeSlot(probeMailbox), busy);
  state.terminals = [sibling];
  assert.deepEqual(await service.probeSlot(probeMailbox), busy);

  store.ack(store.check({ mailboxId: probeMailbox }).deliveryId!, probeMailbox);
  store.upsertWakeTarget(probeMailbox, { completedWaves: 1 });
  store.enqueue([alert('next-wave')], { mailboxId: probeMailbox });
  for (const terminals of [[probeTerminal, sibling], [sibling]]) {
    state.terminals = terminals;
    const wake = await service.flushMailbox(probeMailbox, { force: true });
    assert.equal(wake.woke, false);
    assert.equal(wake.reason, busy.reason);
  }
  assert.equal(state.created, 0);
  assert.equal(state.calls.some(call => call.args[1] === 'close'), false);
  assert.equal(terminalSends(state.calls).length, 0);
});

test('slot probes consult invalid lineage inventory only for a busy subtree with a possible sibling owner', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const { runner, state } = fakeOrcaWorkspace(root, [probeTerminal]);
  const child: FakeOrcaChild = {
    path: join(root, 'child'), worktreeId: 'child', unverifiableTerminalCount: 0, agents: [],
    terminals: [{ ...probeTerminal, handle: 'term-child' }],
  };
  state.children = [child];
  const sibling = { ...probeTerminal, handle: 'term-sibling', tabId: 'sibling' };
  let lineageResponse: unknown;
  const commandRunner: CommandRunner = async request => {
    if (request.args[0] !== 'worktree' || request.args[1] !== 'list') return runner(request);
    state.calls.push(request);
    return { stdout: JSON.stringify(lineageResponse) };
  };
  const { service, errors } = startSlotProbeService(t, store, commandRunner);
  const options = { targetTerminalHandle: probeTerminal.handle };
  store.upsertWakeTarget(probeMailbox, {
    driver: 'orca-terminal', driverConfig: { workspacePath: root, terminalHandle: probeTerminal.handle },
  });
  for (const response of [
    { ok: false }, { ok: true, result: { truncated: true, worktrees: [] } }, { ok: true, result: {} },
  ]) {
    lineageResponse = response;
    state.calls.length = 0;
    for (const terminals of [[probeTerminal], [probeTerminal, sibling]]) {
      state.terminals = terminals;
      child.agents = [];
      assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
    }
    state.terminals = [probeTerminal];
    child.agents = [{ state: 'working' }];
    assert.equal((await service.probeSlot(probeMailbox)).busy, true);
    assert.equal(worktreeListCalls(state.calls), 0);

    state.terminals.push(sibling);
    const failed = await service.probeSlot(probeMailbox);
    assert.equal(failed.busy, true);
    assert.equal(failed.ready, false);
    assert.match(failed.reason!, /Orca worktree list/);
    assert.equal(String(errors.at(-1)), failed.reason);
    assert.equal((await checkOrcaWorkspaceIdle(commandRunner, 'orca', root, undefined, options)).idle, false);
    assert.equal(worktreeListCalls(state.calls), 2);
  }
  assert.equal(errors.length, 3);
});

test('the real Orca command runner only requires lineage inventory when it can exclude busy work', async t => {
  const root = directory(t);
  const fakeOrca = join(root, 'fake-orca');
  const fixture = join(root, 'inventory.json');
  const calls = join(root, 'lineage.log');
  const childPath = join(root, 'child');
  const sibling = { ...probeTerminal, handle: 'term-sibling', tabId: 'sibling' };
  writeFileSync(fakeOrca, [
    `#!${process.execPath}`,
    'const { readFileSync, appendFileSync } = require("node:fs");',
    'const command = process.argv.slice(2, 4).join(" ");',
    `const { agents, terminals, lineage } = JSON.parse(readFileSync(${JSON.stringify(fixture)}, "utf8"));`,
    `const childPath = ${JSON.stringify(childPath)};`,
    'const idle = { agents: [], unverifiableTerminalCount: 0 };',
    `const worktrees = [
      { ...idle, path: ${JSON.stringify(root)}, worktreeId: 'root', childWorktreeIds: ['child'] },
      { ...idle, path: childPath, worktreeId: 'child', agents },
    ];`,
    'const child = process.argv.includes("path:" + childPath);',
    'const result = command === "worktree ps" ? { worktrees } : command === "terminal list" ? { terminals: child ? [] : terminals } : {};',
    `if (command === "worktree list") appendFileSync(${JSON.stringify(calls)}, "list\\n");`,
    'process.stdout.write(JSON.stringify(command === "worktree list" ? lineage : { ok: true, result }));',
    '',
  ].join('\n'), { mode: 0o755 });
  const check = (throwOnProbeError = true) => checkOrcaWorkspaceIdle(runCommand, fakeOrca, root, undefined, {
    targetTerminalHandle: probeTerminal.handle, throwOnProbeError,
  });
  for (const lineage of [
    { ok: false }, { ok: true, result: { truncated: true, worktrees: [] } }, { ok: true, result: {} },
  ]) {
    writeFileSync(calls, '');
    for (const terminals of [[probeTerminal], [probeTerminal, sibling]]) {
      writeFileSync(fixture, JSON.stringify({ agents: [], terminals, lineage }));
      assert.equal((await check()).idle, true);
    }
    const agents = [{ state: 'working' }];
    writeFileSync(fixture, JSON.stringify({ agents, terminals: [probeTerminal], lineage }));
    assert.equal((await check()).idle, false);
    assert.equal(readFileSync(calls, 'utf8'), '');

    writeFileSync(fixture, JSON.stringify({ agents, terminals: [probeTerminal, sibling], lineage }));
    assert.equal((await check(false)).idle, false);
    await assert.rejects(check(), /Orca worktree list/);
    assert.equal(readFileSync(calls, 'utf8'), 'list\nlist\n');
  }
});

test('slot idle checks skip lineage inventory when there are no children or panes already attribute them', async t => {
  const { root, runner, service, state } = slotProbeFixture(t);
  state.terminals.push({ ...probeTerminal, handle: 'term-sibling', tabId: 'sibling' });
  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
  state.children = [{
    path: join(root, 'sibling-child'), worktreeId: 'sibling-child', unverifiableTerminalCount: 0,
    agents: [{ state: 'working', parentPaneKey: 'sibling:root' }],
  }];
  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
  assert.equal(state.calls.some(call => call.args.includes(`path:${state.children[0].path}`)), false);
  state.children[0].agents = [{ state: 'working', parentPaneKey: 'wave:root' }];
  assert.equal((await service.probeSlot(probeMailbox)).busy, true);
  state.children[0].agents = [{ state: 'working', parentPaneKey: null }];
  assert.equal((await checkOrcaWorkspaceIdle(runner, 'orca', root)).idle, false);
  assert.equal(worktreeListCalls(state.calls), 0);
});

test('a leased slot stays busy when its bound terminal is present but unusable', async t => {
  const { service, state } = slotProbeFixture(t, { wave: true });
  state.terminals[0].writable = false;
  assert.deepEqual(await service.probeSlot(probeMailbox), {
    busy: true, ready: false, reason: 'Terminal is disconnected or not writable',
  });
  state.terminals[0].writable = true;
  state.terminals[0].connected = false;
  assert.equal((await service.probeSlot(probeMailbox)).busy, true);
  state.terminals = [];
  assert.deepEqual(await service.probeSlot(probeMailbox), {
    busy: false, ready: false, reason: 'terminal_gone',
  });
});

test('an outbound question holds its slot across terminal unwritability and disappearance', async t => {
  const { store, service, state } = slotProbeFixture(t, { wave: true });
  const sessionMailbox = `terminal:${probeTerminal.handle}`;
  store.recordOutboundEnvelope(alert('question', {
    semantic: { type: 'question', threadId: 'question', replyMailbox: sessionMailbox },
  }), ['remote'], sessionMailbox);
  const waiting = {
    busy: true, ready: false, reason: 'outbound_thread_open',
  };
  assert.deepEqual(await service.probeSlot(probeMailbox), waiting);
  state.terminals[0].writable = false;
  assert.deepEqual(await service.probeSlot(probeMailbox), waiting);
  state.terminals = [];
  assert.deepEqual(await service.probeSlot(probeMailbox), waiting);
  store.ack(store.check({ mailboxId: probeMailbox }).deliveryId!, probeMailbox);
  assert.deepEqual(await service.probeSlot(probeMailbox), waiting);
  assert.deepEqual(state.calls, []);

  store.recordInboundEnvelope(alert('answer', {
    replyToId: 'question', semantic: { type: 'reply', threadId: 'question' },
  }), sessionMailbox, { committed: false });
  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
});

test('a settled or pruned outbound question releases its slot after the terminal is gone', async t => {
  const { store, service, state } = slotProbeFixture(t);
  const sessionMailbox = `terminal:${probeTerminal.handle}`;
  state.terminals = [];
  const ask = (id: string) => store.recordOutboundEnvelope(alert(id, {
    semantic: { type: 'question', threadId: id, replyMailbox: sessionMailbox },
  }), ['remote'], sessionMailbox);
  ask('question-to-settle');
  assert.equal((await service.probeSlot(probeMailbox)).reason, 'outbound_thread_open');
  store.recordInboundEnvelope(alert('settlement', {
    semantic: { type: 'settle', threadId: 'question-to-settle', outcome: 'succeeded' },
  }), sessionMailbox, { committed: false });
  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });

  ask('question-to-prune');
  assert.equal((await service.probeSlot(probeMailbox)).reason, 'outbound_thread_open');
  assert.ok(store.pruneOutboundThreadRecord('question-to-prune'));
  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
});

test('fixed workspace, direct terminal and Orca run slots stay busy on unanswered post-ack questions', async t => {
  const { root, store, service } = slotProbeFixture(t);
  const cases = [
    {
      mailboxId: 'stream:fixed',
      driver: 'orca-terminal',
      driverConfig: { workspacePath: root, terminalHandle: probeTerminal.handle },
    },
    {
      mailboxId: 'terminal:term-direct',
      driver: 'orca-terminal',
      driverConfig: { terminalHandle: 'term-direct' },
    },
    { mailboxId: 'stream:run', driver: 'orca-run', driverConfig: { runId: 'run-worker' } },
  ];
  // The fixed stream owns this terminal for the case below.
  store.upsertWakeTarget(probeMailbox, { driverConfig: { terminalHandle: undefined } });
  for (const { mailboxId, driver, driverConfig } of cases) {
    store.enqueue([alert(`wave-${mailboxId}`)], { mailboxId });
    const lease = store.check({ mailboxId });
    store.upsertWakeTarget(mailboxId, { driver, driverConfig });
    const questionId = `question-${mailboxId}`;
    store.recordOutboundEnvelope(alert(questionId, {
      semantic: { type: 'question', threadId: questionId, replyMailbox: mailboxId },
    }), ['remote'], mailboxId);
    store.ack(lease.deliveryId!, mailboxId);
    assert.deepEqual(await service.probeSlot(mailboxId), {
      busy: true, ready: false, reason: 'outbound_thread_open',
    }, mailboxId);
  }
});

test('an Orca run is busy with a leased wave and an outbound question even without a workspace', async t => {
  const { store, service, state } = slotProbeFixture(t, { wave: true });
  store.upsertWakeTarget(probeMailbox, {
    driver: 'orca-run', rotateAfterWaves: undefined, driverConfig: { runId: 'run-worker' },
  });
  store.recordOutboundEnvelope(alert('run-question', {
    semantic: { type: 'question', threadId: 'run-question', replyMailbox: probeMailbox },
  }), ['remote'], probeMailbox);
  assert.deepEqual(await service.probeSlot(probeMailbox), {
    busy: true, ready: false, reason: 'outbound_thread_open',
  });
  assert.deepEqual(state.calls, []);
});

test('Orca run readiness requires inactive workers and an observed idle coordinator', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const runId = 'run-probe';
  let workers = [{ state: 'ready', outcome: 'in_progress' }];
  let activeWorkerOnNextPage = true;
  let coordinatorHandle: string | undefined = probeTerminal.handle;
  let waitOk = true;
  const { service, errors } = startSlotProbeService(t, store, async request => {
    const [surface, action] = request.args;
    if (surface === 'orchestration' && action === 'worker-list') {
      const base = ['orchestration', 'worker-list', '--run', runId];
      if (activeWorkerOnNextPage && !request.args.includes('--cursor')) {
        assert.deepEqual(request.args, [...base, '--json']);
        return { stdout: '{"ok":true,"result":{"workers":[{"state":"succeeded"}],"nextCursor":"next"}}' };
      }
      assert.deepEqual(request.args, [...base, ...(activeWorkerOnNextPage ? ['--cursor', 'next'] : []), '--json']);
      return { stdout: JSON.stringify({ ok: true, result: { workers } }) };
    }
    if (surface === 'orchestration' && action === 'run-show') {
      assert.deepEqual(request.args, ['orchestration', 'run-show', '--id', runId, '--full', '--json']);
      return { stdout: JSON.stringify({ ok: true, result: { run: { coordinator_handle: coordinatorHandle } } }) };
    }
    assert.equal(surface, 'terminal');
    assert.ok(request.args.includes(probeTerminal.handle));
    if (action === 'show') return { stdout: JSON.stringify({ ok: true, result: { terminal: probeTerminal } }) };
    assert.equal(action, 'wait');
    return { stdout: JSON.stringify({ ok: waitOk }) };
  });
  store.upsertWakeTarget(probeMailbox, { driver: 'orca-run' });
  assert.deepEqual(await service.probeSlot(probeMailbox), {
    busy: true, ready: false, reason: 'orca-run target requires runId',
  });
  store.upsertWakeTarget(probeMailbox, { driverConfig: { runId } });
  const claim = service.claim(probeMailbox);
  store.enqueue([alert('run-wave')], { mailboxId: probeMailbox });
  const lease = store.check({ mailboxId: probeMailbox });
  store.completeWakeDelivery(probeMailbox, lease.items[0].sequence);
  claim.dispose();
  assert.deepEqual(await service.probeSlot(probeMailbox), {
    busy: true, ready: false, reason: 'Orca run has active workers',
  });
  store.ack(lease.deliveryId!, probeMailbox);
  assert.equal((await service.probeSlot(probeMailbox)).reason, 'Orca run has active workers');

  workers = [{ state: 'succeeded', outcome: 'succeeded' }];
  activeWorkerOnNextPage = false;
  waitOk = false;
  assert.match((await service.probeSlot(probeMailbox)).reason!, /Terminal is busy/);
  coordinatorHandle = undefined;
  assert.deepEqual(await service.probeSlot(probeMailbox), {
    busy: true, ready: false, reason: 'Orca run coordinator activity is unobservable',
  });
  coordinatorHandle = probeTerminal.handle;
  waitOk = true;
  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
  assert.deepEqual(errors, []);
});

test('an empty auto-create slot checks workspace provisioning without creating it', async t => {
  const repoRoot = directory(t);
  const workspacePath = join(repoRoot, 'missing-workspace');
  const store = new MailboxStore(join(repoRoot, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  const { service, errors } = startSlotProbeService(t, store, async request => {
    calls.push(request);
    assert.equal(request.args[0], 'worktree');
    assert.ok(['ps', 'list'].includes(request.args[1]));
    return { stdout: '{"ok":true,"result":{"worktrees":[]}}' };
  });
  store.upsertWakeTarget(probeMailbox, {
    driver: 'orca-terminal', driverConfig: { workspacePath, autoCreateWorkspace: true, repoRoot },
  });
  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
  assert.deepEqual(calls.map(call => call.args[1]), ['ps', 'list']);
  assert.equal(calls[1].cwd, repoRoot);
  assert.equal(existsSync(workspacePath), false);
  assert.deepEqual(errors, []);

  store.upsertWakeTarget(probeMailbox, { driverConfig: { repoRoot: undefined } });
  const failed = await service.probeSlot(probeMailbox);
  assert.equal(failed.busy, true);
  assert.equal(failed.ready, false);
  assert.match(failed.reason!, /requires a receiver-local repoRoot/);
  assert.equal(errors.length, 1);
});

test('terminal wait timeouts are busy observations and server failures retain their diagnostics', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const terminalMailbox = `terminal:${probeTerminal.handle}`;
  store.upsertWakeTarget(terminalMailbox, {
    driver: 'orca-terminal', driverConfig: { terminalHandle: probeTerminal.handle },
  });
  let waitResult: Awaited<ReturnType<CommandRunner>> = {
    stdout: '{"ok":false,"error":{"code":"timeout","message":"timeout"}}', exitCode: 1,
  };
  let rejectWait = false;
  const { service, errors } = startSlotProbeService(t, store, async request => {
    if (request.args[1] === 'show') {
      return { stdout: JSON.stringify({ ok: true, result: { terminal: probeTerminal } }) };
    }
    assert.equal(request.args[1], 'wait');
    if (rejectWait) throw Object.assign(new Error(waitResult.stderr || waitResult.stdout), waitResult);
    return waitResult;
  });
  for (const rejectsOnExit of [false, true]) {
    rejectWait = rejectsOnExit;
    const timeout = await service.probeSlot(terminalMailbox);
    assert.equal(timeout.busy, true);
    assert.equal(timeout.ready, false);
    assert.match(timeout.reason!, /Terminal is busy/);
    assert.deepEqual(errors, []);
  }

  const failures = [
    { result: { stdout: '{"ok":false,"error":{"code":"runtime_unavailable","message":"Orca daemon unreachable"}}', exitCode: 1 }, diagnostic: 'Orca daemon unreachable' },
    { result: { stdout: '', stderr: 'connection refused', exitCode: 1 }, diagnostic: 'connection refused' },
  ];
  rejectWait = false;
  for (const { result, diagnostic } of failures) {
    waitResult = result;
    const failed = await service.probeSlot(terminalMailbox);
    assert.equal(failed.busy, true);
    assert.equal(failed.ready, false);
    assert.ok(failed.reason?.includes(diagnostic), failed.reason);
    assert.ok(String(errors.at(-1)).includes(diagnostic));
  }
  assert.equal(errors.length, failures.length);
});

test('an empty rotating slot is ready when its retired terminal is idle or gone', async t => {
  const { store, service, state, errors } = slotProbeFixture(t);
  const retired = { ...probeTerminal, handle: 'term-retired', tabId: 'retired' };
  state.terminals.push(retired);
  state.agents = [{ state: 'working', paneKey: 'retired:root' }];
  store.upsertWakeTarget(probeMailbox, {
    completedWaves: 1, driverConfig: { retiredTerminalHandle: retired.handle },
  });
  assert.equal((await service.probeSlot(probeMailbox)).ready, false);
  state.agents[0].state = 'done';
  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
  state.terminals.pop();
  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
  assert.equal(store.getWakeTarget(probeMailbox)?.driverConfig?.retiredTerminalHandle, retired.handle);
  assert.deepEqual(errors, []);
});

test('slot probe answers from the ledger without invoking Orca when there is no host work to inspect', async t => {
  const { store, service, state } = slotProbeFixture(t, { wave: true });
  assert.deepEqual(await service.probeSlot('stream:alert-wave:2'), { busy: false, ready: true });
  const claim = service.claim(probeMailbox);
  assert.deepEqual(await service.probeSlot(probeMailbox), {
    busy: true, ready: false, reason: 'wakeup_in_flight',
  });
  claim.dispose();
  store.beginWakeDelivery(probeMailbox);
  assert.equal((await service.probeSlot(probeMailbox)).reason, 'wakeup_pending');
  const [wave] = store.check({ mailboxId: probeMailbox }).items;
  store.completeWakeDelivery(probeMailbox, wave.sequence);
  const replyMailbox = `terminal:${probeTerminal.handle}`;
  store.enqueue([alert('session-reply')], { mailboxId: replyMailbox });
  assert.equal((await service.probeSlot(probeMailbox)).reason, 'session_reply_pending');
  store.ack(store.check({ mailboxId: replyMailbox }).deliveryId!, replyMailbox);
  const targetMailbox = 'stream:prod-release';
  const release = alert('release', {
    kind: 'release.prod.v1', semantic: { type: 'message', threadId: 'release', replyMailbox, targetMailbox },
  });
  store.recordOutboundEnvelope(release, [release.from.nodeId], replyMailbox);
  store.enqueue([release], { mailboxId: targetMailbox });
  assert.equal((await service.probeSlot(probeMailbox)).reason, 'local_outbound_pending');
  assert.deepEqual(state.calls, []);
});

test('slot probe is busy after wave acknowledgement until queued and leased downstream mail is acknowledged', async t => {
  for (const replyMailbox of [`terminal:${probeTerminal.handle}`, probeMailbox]) {
    await t.test(replyMailbox, async t => {
      const { store, service, state } = slotProbeFixture(t, { wave: true });
      const targetMailbox = 'stream:prod-release';
      const release = alert('release', {
        kind: 'release.prod.v1', semantic: { type: 'message', threadId: 'release', replyMailbox, targetMailbox },
      });
      store.recordOutboundEnvelope(release, [release.from.nodeId], replyMailbox);
      store.enqueue([release], { mailboxId: targetMailbox });
      store.ack(store.check({ mailboxId: probeMailbox }).deliveryId!, probeMailbox);

      const busy = { busy: true, ready: false, reason: 'local_outbound_pending' };
      assert.deepEqual(await service.probeSlot(probeMailbox), busy);
      const downstreamLease = store.check({ mailboxId: targetMailbox });
      assert.deepEqual(await service.probeSlot(probeMailbox), busy);
      assert.deepEqual(state.calls, []);
      store.ack(downstreamLease.deliveryId!, targetMailbox);
      assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
    });
  }
});

test('slot probe stays busy on a settled thread while another downstream message is queued or leased', async t => {
  const { store, service, state } = slotProbeFixture(t, { wave: true });
  const replyMailbox = `terminal:${probeTerminal.handle}`;
  const targetMailbox = 'stream:prod-release';
  const release = alert('release', {
    kind: 'release.prod.v1', semantic: { type: 'message', threadId: 'release', replyMailbox, targetMailbox },
  });
  store.recordOutboundEnvelope(release, [release.from.nodeId], replyMailbox);
  store.enqueue([alert('follow-up', {
    semantic: { type: 'message', threadId: release.id, targetMailbox },
  })], { mailboxId: targetMailbox });
  store.recordInboundEnvelope(alert('settlement', {
    semantic: { type: 'settle', threadId: release.id, outcome: 'succeeded' },
  }), replyMailbox, { committed: false });

  const busy = { busy: true, ready: false, reason: 'local_outbound_pending' };
  assert.deepEqual(await service.probeSlot(probeMailbox), busy);
  const downstreamLease = store.check({ mailboxId: targetMailbox });
  assert.deepEqual(await service.probeSlot(probeMailbox), busy);
  store.ack(store.check({ mailboxId: probeMailbox }).deliveryId!, probeMailbox);
  assert.deepEqual(await service.probeSlot(probeMailbox), busy);
  assert.deepEqual(state.calls, []);
  store.ack(downstreamLease.deliveryId!, targetMailbox);
  assert.deepEqual(await service.probeSlot(probeMailbox), { busy: false, ready: true });
});

test('slot probe reports failed Orca reads and fails closed', async t => {
  const workspacePath = directory(t);
  const store = new MailboxStore(join(workspacePath, 'mailbox.json'));
  const reports: unknown[] = [];
  let response: Awaited<ReturnType<CommandRunner>> = { stdout: '', stderr: 'Orca unavailable', exitCode: 1 };
  const service = new WakeupRedriveService({
    store,
    drivers: createTestWakeupDrivers({ commandRunner: async () => response }),
    onError: error => reports.push(error),
  });
  t.after(() => service.stop());

  store.upsertWakeTarget(probeMailbox, { driver: 'orca-run', driverConfig: { workspacePath } });
  const failed = await service.probeSlot(probeMailbox);
  assert.equal(failed.busy, true);
  assert.equal(failed.ready, false);
  assert.match(failed.reason!, /Orca unavailable/);

  response = { stdout: '{"ok":true,"result":{"truncated":true,"worktrees":[]}}' };
  assert.match((await service.probeSlot(probeMailbox)).reason!, /Incomplete workspace inventory/);

  store.upsertWakeTarget(probeMailbox, {
    driver: 'orca-terminal', driverConfig: { terminalHandle: probeTerminal.handle },
  });
  response = { stdout: 'not JSON' };
  const unreadable = await service.probeSlot(probeMailbox);
  assert.equal(unreadable.busy, true);
  assert.equal(unreadable.ready, false);
  assert.equal(reports.length, 3);
  assert.ok(reports[2] instanceof SyntaxError);
});

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
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  const enqueue = (ids: readonly string[]) =>
    store.enqueue(ids.map(id => alert(id)), {
      mailboxId: 'stream:alerts',
      wakeTarget: {
        driver: 'orca-terminal',
        coalesceMs: 0,
        rotateAfterWaves: 1,
        driverConfig: { terminalHandle: 'term-alerts', workspacePath, workspaceComment: 'Dealer: alerts' },
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
  assert.equal(store.getWakeTarget('stream:alerts')!.driverConfig?.terminalHandle, 'term-new-1');
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
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('rotate-now')], {
    mailboxId: 'stream:rotate',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      completedWaves: 1,
      driverConfig: { workspacePath, terminalHandle: 'term-rotate' },
    },
  });

  state.waitOk = false;
  const first = await service.flushMailbox('stream:rotate');
  assert.equal(first.woke, false);
  assert.equal(first.rotated, true);
  assert.equal(first.reason, 'Session rotated; waiting for tui-idle before wakeup');
  assert.equal(state.created, 1);
  assert.equal(store.getWakeTarget('stream:rotate')!.driverConfig?.terminalHandle, 'term-new-1');
  assert.equal(store.getWakeTarget('stream:rotate')!.completedWaves, 0);
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
  // One slot keeps at most one live terminal: the retired tab is closed after the launch commits.
  const close = state.calls.find(call => call.args[0] === 'terminal' && call.args[1] === 'close')!;
  assert.deepEqual(close.args, ['terminal', 'close', '--terminal', 'term-rotate', '--json']);
  assert.equal(close.args.includes('--tab'), false, 'the whole retired tab is closed, not reused');
  assert.deepEqual(state.terminals.map(terminal => terminal.handle), ['term-new-1']);
});

test('rotation waits for the retiring slot agent before it opens the replacement tab', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'slot-busy');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-slot', tabId: 'slot-1', leafId: 'root', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
    { handle: 'term-other', tabId: 'slot-2', leafId: 'root', connected: true, writable: true, agentIdentity: 'antigravity', preview: '> human draft' },
  ]);
  state.agents = [{ state: 'working', paneKey: 'slot-1:root' }, { state: 'done', paneKey: 'slot-2:root' }];
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('busy-slot')], {
    mailboxId: 'stream:slot-busy',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      completedWaves: 1,
      driverConfig: { workspacePath, terminalHandle: 'term-slot', paneKey: 'slot-1:root' },
    },
  });

  const deferred = await service.flushMailbox('stream:slot-busy');
  assert.equal(deferred.woke, false);
  assert.equal(deferred.reason, 'Workspace or descendant is busy or unverifiable');
  assert.equal(state.created, 0, 'a busy slot never opens a second live terminal');
  assert.equal(state.calls.some(call => call.args[1] === 'close'), false, 'the busy tab is not retired');
  assert.equal(terminalSends(state.calls).length, 0);

  state.agents = [{ state: 'done', paneKey: 'slot-1:root' }, { state: 'done', paneKey: 'slot-2:root' }];
  const rotated = await service.flushMailbox('stream:slot-busy', { force: true });
  assert.equal(rotated.woke, true, rotated.reason);
  assert.equal(rotated.rotated, true);
  assert.equal(state.created, 1);
  const close = state.calls.find(call => call.args[0] === 'terminal' && call.args[1] === 'close')!;
  assert.deepEqual(close.args, ['terminal', 'close', '--terminal', 'term-slot', '--json']);
  assert.equal(state.terminals.some(terminal => terminal.handle === 'term-slot'), false);
  assert.equal(state.terminals.some(terminal => terminal.handle === 'term-other'), true, 'other slots keep their tabs');
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
  assert.equal(sendsTo(state.calls, 'term-other').length, 0, 'a human tab is never hijacked');
});

test('rotation waits for a child worktree owned by the retiring slot pane', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'slot-child');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-slot', tabId: 'slot-1', leafId: 'root', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const child = {
    worktreeId: 'child-slot',
    path: join(workspacePath, 'child'),
    unverifiableTerminalCount: 0,
    agents: [{ state: 'working', parentPaneKey: 'slot-1:root' }] as FakeOrcaAgent[],
    terminals: [{ handle: 'term-child', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' }],
  };
  state.children = [child];
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('child-busy')], {
    mailboxId: 'stream:slot-child',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      completedWaves: 1,
      driverConfig: { workspacePath, terminalHandle: 'term-slot', paneKey: 'slot-1:root' },
    },
  });

  const deferred = await service.flushMailbox('stream:slot-child');
  assert.equal(deferred.woke, false);
  assert.equal(deferred.reason, 'Workspace or descendant is busy or unverifiable');
  assert.equal(state.created, 0, 'a working child worktree keeps the slot on its one terminal');
  assert.equal(state.calls.some(call => call.args[1] === 'close'), false, 'the busy slot is not retired');

  child.agents = [{ state: 'done', parentPaneKey: 'slot-1:root' }];
  const rotated = await service.flushMailbox('stream:slot-child', { force: true });
  assert.equal(rotated.woke, true, rotated.reason);
  assert.equal(rotated.rotated, true);
  assert.equal(state.created, 1);
  const close = state.calls.find(call => call.args[0] === 'terminal' && call.args[1] === 'close')!;
  assert.deepEqual(close.args, ['terminal', 'close', '--terminal', 'term-slot', '--json']);
  assert.equal(state.terminals.some(terminal => terminal.handle === 'term-slot'), false);
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
});

test('a failed retiring-terminal close stays durable, surfaces a warning, and retries on the next wakeup', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'close-fail');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-old', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  let closeOk = false;
  const errors: unknown[] = [];
  const commandRunner: CommandRunner = async request => {
    if (request.args[0] === 'terminal' && request.args[1] === 'close' && !closeOk) {
      state.calls.push(request);
      // A transient CLI failure (not a deterministic refusal): the tab is still listed.
      return { stdout: '', exitCode: 1 };
    }
    return runner(request);
  };
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    onError: error => errors.push(error),
    drivers: createTestWakeupDrivers({ commandRunner }),
  });
  t.after(() => service.stop());
  const enqueue = (id: string) =>
    store.enqueue([alert(id)], {
      mailboxId: 'stream:close-fail',
      wakeTarget: {
        driver: 'orca-terminal',
        coalesceMs: 0,
        rotateAfterWaves: 1,
        completedWaves: 1,
        driverConfig: { workspacePath, terminalHandle: 'term-old' },
      },
    });

  enqueue('close-fails');
  const rotated = await service.flushMailbox('stream:close-fail');
  assert.equal(rotated.woke, true, rotated.reason);
  assert.equal(rotated.rotated, true);
  assert.match(String(rotated.warning), /Retired terminal close failed \(term-old\): Orca terminal close failed/);
  assert.equal(errors.length, 1, 'the failed close is reported, never swallowed');
  assert.equal(
    store.getWakeTarget('stream:close-fail')!.driverConfig?.retiredTerminalHandle,
    'term-old',
    'an unconfirmed close stays durable for retry',
  );
  assert.equal(state.terminals.some(terminal => terminal.handle === 'term-old'), true);
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);

  closeOk = true;
  enqueue('close-retries');
  const retried = await service.flushMailbox('stream:close-fail', { force: true });
  assert.equal(retried.woke, true, retried.reason);
  assert.equal(retried.warning, undefined);
  assert.equal(store.getWakeTarget('stream:close-fail')!.driverConfig?.retiredTerminalHandle, undefined);
  assert.equal(state.terminals.some(terminal => terminal.handle === 'term-old'), false, 'the retry closes the retired tab');
  assert.equal(errors.length, 1, 'a successful retry reports nothing new');
});

test('rotation fails closed when the retiring terminal cannot be attributed to one pane', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'unattributed');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-slot', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
    { handle: 'term-other', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  state.agents = [{ state: 'working' }, { state: 'done' }];
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('unattributed')], {
    mailboxId: 'stream:unattributed',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      completedWaves: 1,
      driverConfig: { workspacePath, terminalHandle: 'term-slot' },
    },
  });

  const deferred = await service.flushMailbox('stream:unattributed');
  assert.equal(deferred.woke, false);
  assert.equal(deferred.reason, 'Workspace or descendant is busy or unverifiable');
  assert.equal(state.created, 0, 'a busy unattributed slot never gains a second live terminal');
  assert.equal(state.calls.some(call => call.args[1] === 'close'), false);

  state.agents = [{ state: 'done' }, { state: 'done' }];
  const rotated = await service.flushMailbox('stream:unattributed', { force: true });
  assert.equal(rotated.woke, true, rotated.reason);
  assert.equal(rotated.rotated, true);
  assert.equal(state.created, 1);
  assert.equal(state.terminals.some(terminal => terminal.handle === 'term-slot'), false);
});

test('a terminal awaiting retirement close is excluded from sibling selection', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'reserve');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-retiring', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  // A previous rotation's close failed, so the tab is still reserved while its owner retries.
  store.upsertWakeTarget('stream:retiring-owner', {
    driver: 'orca-terminal',
    rotateAfterWaves: 1,
    driverConfig: { workspacePath, terminalHandle: 'term-current', retiredTerminalHandle: 'term-retiring' },
  });
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('sibling')], {
    mailboxId: 'stream:sibling',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      driverConfig: { workspacePath, terminalHandle: 'term-retiring' },
    },
  });

  const sibling = await service.flushMailbox('stream:sibling', { force: true });
  assert.equal(sibling.woke, true, sibling.reason);
  assert.equal(state.created, 1, 'the sibling opens its own tab instead of the retiring one');
  assert.equal(store.getWakeTarget('stream:sibling')!.driverConfig?.terminalHandle, 'term-new-1');
  assert.equal(sendsTo(state.calls, 'term-retiring').length, 0, 'an awaiting-retirement tab is never selected');
  assert.equal(
    store.getWakeTarget('stream:retiring-owner')!.driverConfig?.retiredTerminalHandle,
    'term-retiring',
    'the reservation belongs to its owner until the close is confirmed',
  );
});

test('a retired terminal claimed by another mailbox before the close boundary is left alone', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'claim-race');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-old', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const commandRunner: CommandRunner = async request => {
    if (request.args[0] === 'terminal' && request.args[1] === 'create') {
      // A sibling binds the retiring tab after the idle proof but before the close boundary.
      store.upsertWakeTarget('stream:sibling-claim', {
        driver: 'orca-terminal',
        driverConfig: { workspacePath, terminalHandle: 'term-old' },
      });
    }
    return runner(request);
  };
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('claim-race')], {
    mailboxId: 'stream:claim-race',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      completedWaves: 1,
      driverConfig: { workspacePath, terminalHandle: 'term-old' },
    },
  });

  const rotated = await service.flushMailbox('stream:claim-race');
  assert.equal(rotated.woke, true, rotated.reason);
  assert.equal(rotated.rotated, true);
  assert.equal(
    state.calls.some(call => call.args[0] === 'terminal' && call.args[1] === 'close'),
    false,
    'a tab another mailbox now actively owns is never closed',
  );
  assert.equal(store.getWakeTarget('stream:claim-race')!.driverConfig?.retiredTerminalHandle, undefined);
  assert.equal(state.terminals.some(terminal => terminal.handle === 'term-old'), true, 'the claimed tab stays live');
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
});

test('an unconfirmed retiring-terminal close defers the next rotation instead of overwriting it', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'retry-before-rotate');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-1', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  let closeOk = false;
  const commandRunner: CommandRunner = async request => {
    if (request.args[0] === 'terminal' && request.args[1] === 'close' && !closeOk) {
      state.calls.push(request);
      // A transient CLI failure (not a deterministic refusal): the tab is still listed.
      return { stdout: '', exitCode: 1 };
    }
    return runner(request);
  };
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner }),
  });
  t.after(() => service.stop());
  const mailboxId = 'stream:retry-before-rotate';
  const ack = () => {
    const lease = store.check({ mailboxId });
    store.ack(lease.deliveryId!);
  };

  store.enqueue([alert('wave-one')], {
    mailboxId,
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      completedWaves: 1,
      driverConfig: { workspacePath, terminalHandle: 'term-1' },
    },
  });
  const first = await service.flushMailbox(mailboxId);
  assert.equal(first.woke, true, first.reason);
  assert.equal(first.rotated, true);
  assert.equal(state.created, 1);
  assert.match(String(first.warning), /Retired terminal close failed \(term-1\)/);
  assert.equal(store.getWakeTarget(mailboxId)!.driverConfig?.retiredTerminalHandle, 'term-1');

  ack();
  store.enqueue([alert('wave-two')], { mailboxId });
  const deferred = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(deferred.woke, false);
  assert.match(String(deferred.reason), /Retired terminal close failed \(term-1\)/);
  assert.equal(deferred.rotated, undefined);
  assert.equal(state.created, 1, 'the next rotation waits for the earlier retired tab to close');
  assert.equal(
    store.getWakeTarget(mailboxId)!.driverConfig?.retiredTerminalHandle,
    'term-1',
    'a pending retirement is never overwritten by the next rotation',
  );
  assert.equal(store.getWakeTarget(mailboxId)!.driverConfig?.terminalHandle, 'term-new-1');

  closeOk = true;
  const rotated = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(rotated.woke, true, rotated.reason);
  assert.equal(rotated.rotated, true);
  assert.equal(state.created, 2, 'the deferred rotation finally opens its replacement');
  const closeCalls = state.calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'close');
  assert.deepEqual(
    closeCalls.map(call => call.args[call.args.indexOf('--terminal') + 1]),
    ['term-1', 'term-1', 'term-1', 'term-new-1'],
    'the retry fails twice, then closes term-1 before the launch and term-new-1 after it',
  );
  assert.equal(store.getWakeTarget(mailboxId)!.driverConfig?.terminalHandle, 'term-new-2');
  assert.equal(store.getWakeTarget(mailboxId)!.driverConfig?.retiredTerminalHandle, undefined);
  assert.deepEqual(state.terminals.map(terminal => terminal.handle), ['term-new-2']);
  assert.equal(sendsTo(state.calls, 'term-new-2').length, 1);
});

test('a missing retiring terminal still waits for its busy child worktree before rotating', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'slot-gone');
  // The retiring tab is already absent and has no recorded pane, so no pane owns its children.
  const { runner, state } = fakeOrcaWorkspace(workspacePath);
  const child = {
    worktreeId: 'child-gone',
    path: join(workspacePath, 'child'),
    unverifiableTerminalCount: 0,
    agents: [{ state: 'working', parentPaneKey: 'slot-gone:root' }] as FakeOrcaAgent[],
    terminals: [{ handle: 'term-child', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' }],
  };
  state.children = [child];
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('missing-retiring')], {
    mailboxId: 'stream:missing-retiring',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      completedWaves: 1,
      driverConfig: { workspacePath, terminalHandle: 'term-gone' },
    },
  });

  const deferred = await service.flushMailbox('stream:missing-retiring');
  assert.equal(deferred.woke, false);
  assert.equal(deferred.reason, 'Workspace or descendant is busy or unverifiable');
  assert.equal(state.created, 0, 'an unattributed child worktree fails closed instead of being guessed idle');

  child.agents = [{ state: 'done', parentPaneKey: 'slot-gone:root' }];
  const rotated = await service.flushMailbox('stream:missing-retiring', { force: true });
  assert.equal(rotated.woke, true, rotated.reason);
  assert.equal(rotated.rotated, true);
  assert.equal(state.created, 1);
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
});

test('a pending retirement is still closed after the slot is reconfigured to a fixed tab', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'retire-fixed');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-1', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  let closeOk = false;
  const commandRunner: CommandRunner = async request => {
    if (request.args[0] === 'terminal' && request.args[1] === 'close' && !closeOk) {
      state.calls.push(request);
      return { stdout: '', exitCode: 1 };
    }
    return runner(request);
  };
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner }),
  });
  t.after(() => service.stop());
  const mailboxId = 'stream:retire-fixed';
  const ack = () => {
    const lease = store.check({ mailboxId });
    store.ack(lease.deliveryId!);
  };

  store.enqueue([alert('wave-one')], {
    mailboxId,
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      completedWaves: 1,
      driverConfig: { workspacePath, terminalHandle: 'term-1' },
    },
  });
  const first = await service.flushMailbox(mailboxId);
  assert.equal(first.woke, true, first.reason);
  assert.equal(first.rotated, true);
  assert.match(String(first.warning), /Retired terminal close failed \(term-1\)/);
  assert.equal(store.getWakeTarget(mailboxId)!.driverConfig?.retiredTerminalHandle, 'term-1');

  // The slot is reconfigured as a fixed tab: rotation is off, but the durable retirement remains
  // its own obligation and must not be replaced by the fixed binding.
  store.upsertWakeTarget(mailboxId, { rotateAfterWaves: undefined });
  assert.equal(store.getWakeTarget(mailboxId)!.rotateAfterWaves, undefined);
  ack();
  closeOk = true;
  store.enqueue([alert('wave-two')], { mailboxId });
  const reconciled = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(reconciled.woke, true, reconciled.reason);
  assert.equal(reconciled.warning, undefined);
  assert.equal(state.created, 1, 'a fixed slot opens no replacement tab');
  assert.equal(
    state.terminals.some(terminal => terminal.handle === 'term-1'),
    false,
    'the pending retirement is reconciled even without rotateAfterWaves',
  );
  assert.equal(store.getWakeTarget(mailboxId)!.driverConfig?.retiredTerminalHandle, undefined);
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 2);
});

test('a retired terminal already absent from the inventory settles without a warning or defer', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'retire-absent');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-live', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const commandRunner: CommandRunner = async request => {
    if (request.args[0] === 'terminal' && request.args[1] === 'close') {
      state.calls.push(request);
      // Orca cannot close a tab that is already gone; the inventory below confirms it.
      return { stdout: '{"ok":false,"error":{"code":"terminal_not_found"}}', exitCode: 1 };
    }
    return runner(request);
  };
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('absent')], {
    mailboxId: 'stream:absent',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      driverConfig: { workspacePath, terminalHandle: 'term-live', retiredTerminalHandle: 'term-gone' },
    },
  });

  const delivered = await service.flushMailbox('stream:absent');
  assert.equal(delivered.woke, true, delivered.reason);
  assert.equal(delivered.warning, undefined, 'an already-absent tab settles the retirement silently');
  assert.equal(
    store.getWakeTarget('stream:absent')!.driverConfig?.retiredTerminalHandle,
    undefined,
    'the retirement is satisfied by the absence proof',
  );
  assert.equal(sendsTo(state.calls, 'term-live').length, 1);
});

test('a deterministic close refusal parks the wake target instead of retrying forever', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'retire-pinned');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-live', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
    { handle: 'term-old', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const commandRunner: CommandRunner = async request => {
    if (request.args[0] === 'terminal' && request.args[1] === 'close') {
      state.calls.push(request);
      // `runCommand` rejects on a non-zero exit and attaches the captured JSON output.
      throw Object.assign(new Error('exit 1'), {
        exitCode: 1,
        stdout: '{"ok":false,"error":{"code":"terminal_tab_pinned","message":"Tab is pinned"}}',
        stderr: '',
      });
    }
    return runner(request);
  };
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('pinned')], {
    mailboxId: 'stream:pinned',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      driverConfig: { workspacePath, terminalHandle: 'term-live', retiredTerminalHandle: 'term-old' },
    },
  });

  const result = await service.flushMailbox('stream:pinned');
  assert.equal(result.woke, false);
  assert.match(String(result.reason), /Orca refused to close terminal term-old \(terminal_tab_pinned\)/);
  const parked = store.getWakeTarget('stream:pinned')!;
  assert.equal(parked.deliveryInDoubt, true, 'an unresolvable refusal is parked, never retried forever');
  assert.equal(parked.pendingWake, false);
  assert.match(String(parked.lastWakeError), /terminal_tab_pinned/);
  assert.equal(sendsTo(state.calls, 'term-live').length, 0, 'a parked target never writes a pointer');
  assert.equal(state.terminals.some(terminal => terminal.handle === 'term-old'), true);
});

test('a truncated terminal inventory never proves the retired tab absent', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'retire-truncated');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-live', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
    { handle: 'term-old', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  let closeAttempted = false;
  const commandRunner: CommandRunner = async request => {
    if (request.args[0] === 'terminal' && request.args[1] === 'close') {
      state.calls.push(request);
      closeAttempted = true;
      // A transient failure, so absence has to be verified from the inventory.
      return { stdout: '', exitCode: 1 };
    }
    if (closeAttempted && request.args[0] === 'terminal' && request.args[1] === 'list') {
      state.calls.push(request);
      // A truncated page cannot show whether the retired handle still exists.
      return { stdout: JSON.stringify({ ok: true, result: { truncated: true, terminals: [] } }) };
    }
    return runner(request);
  };
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('truncated')], {
    mailboxId: 'stream:truncated',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      driverConfig: { workspacePath, terminalHandle: 'term-live', retiredTerminalHandle: 'term-old' },
    },
  });

  const delivered = await service.flushMailbox('stream:truncated');
  assert.equal(delivered.woke, true, delivered.reason);
  assert.match(String(delivered.warning), /Retired terminal close failed \(term-old\)/);
  assert.equal(
    store.getWakeTarget('stream:truncated')!.driverConfig?.retiredTerminalHandle,
    'term-old',
    'a truncated inventory never clears the durable retirement',
  );
  assert.equal(sendsTo(state.calls, 'term-live').length, 1);
});

test('rotation fails closed on a working agent that has no pane key', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'slot-unattributed-agent');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-slot', tabId: 'slot-1', leafId: 'root', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
    { handle: 'term-other', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  // Two live terminals and a persisted retiring pane: an agent without a paneKey still cannot be
  // proven to belong to a sibling, so it must keep the rotation deferred while it works.
  state.agents = [{ state: 'working' }];
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('unattributed-agent')], {
    mailboxId: 'stream:unattributed-agent',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      completedWaves: 1,
      driverConfig: { workspacePath, terminalHandle: 'term-slot', paneKey: 'slot-1:root' },
    },
  });

  const deferred = await service.flushMailbox('stream:unattributed-agent');
  assert.equal(deferred.woke, false);
  assert.equal(deferred.reason, 'Workspace or descendant is busy or unverifiable');
  assert.equal(state.created, 0, 'an unattributable working agent keeps the slot on one terminal');
  assert.equal(state.calls.some(call => call.args[1] === 'close'), false);

  state.agents = [{ state: 'done', paneKey: 'slot-1:root' }];
  const rotated = await service.flushMailbox('stream:unattributed-agent', { force: true });
  assert.equal(rotated.woke, true, rotated.reason);
  assert.equal(rotated.rotated, true);
  assert.equal(state.created, 1);
  assert.equal(state.terminals.some(terminal => terminal.handle === 'term-slot'), false);
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
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
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  const enqueue = (ids: readonly string[]) =>
    store.enqueue(ids.map(id => alert(id)), {
      mailboxId: 'stream:wave',
      wakeTarget: {
        driver: 'orca-terminal',
        coalesceMs: 0,
        rotateAfterWaves: 1,
        driverConfig: { workspacePath },
      },
    });

  enqueue(['a']);
  const first = await service.flushMailbox('stream:wave');
  assert.equal(first.woke, true, first.reason);
  assert.equal(state.created, 1);
  assert.equal(sendsTo(state.calls, 'term-human').length, 0, 'a human tab is never hijacked');
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
  assert.equal(terminalSends(state.calls).some(call => textOf(call) === '\x15'), false, 'a fresh tab needs no composer clear');
  assert.equal(store.getWakeTarget('stream:wave')!.driverConfig?.terminalHandle, 'term-new-1');

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
  assert.equal(store.getWakeTarget('stream:wave')!.driverConfig?.terminalHandle, 'term-new-2');
  assert.ok(sendsTo(state.calls, 'term-new-2').length >= 1);
  assert.equal(terminalSends(state.calls).some(call => textOf(call) === '/new'), false);
});
}

test('slotted alerts inherit a base topic binding and route to independent mailboxes', async t => {
  const root = directory(t);
  const workspacePath = join(root, 'alerts');
  const binding = { executor: 'orca-wave', streamKey: 'tools-alert', workspacePath, rotateAfterWaves: 4 };
  const { runner } = fakeOrcaWorkspace(workspacePath);
  const runtime = await makeNode(t, root, 'alerts', {
    commandRunner: runner,
    patch: { executors: ['inbox', 'orca-wave'], topicBindings: { 'wave.alert.v1:tools-alert': binding } },
  });
  const topic = 'wave.alert.v1';
  for (const streamKey of ['tools-alert', 'tools-alert:2', 'tools-alert:3']) {
    const mailboxId = `stream:${streamKey}`;
    assert.equal(resolveBoundMailboxId(`${topic}:${streamKey}`, binding), mailboxId);
    assert.deepEqual(runtime.resolveTopicBinding(`${topic}:${streamKey}`)?.binding, { ...binding, streamKey });
    const receipt = await runtime.emitter.emit({
      networkId: 'local',
      routing: { mode: 'unicast', targets: ['member:alerts'] },
      kind: topic,
      subject: streamKey,
      body: 'Alert details',
      payload: { streamKey },
    });
    assert.equal(receipt.receipts[0].stage, 'dispatched');
    assert.equal(runtime.mailbox.list({ mailboxId }).queuedCount, 1);
    const target = runtime.mailbox.getWakeTarget(mailboxId);
    assert.equal(target?.driverConfig?.workspacePath, workspacePath);
    assert.equal(target?.rotateAfterWaves, 4);
  }

  const updatedWorkspace = join(root, 'updated-alerts');
  const explicitWorkspace = join(root, 'explicit-slot');
  runtime.config.topicBindings = {
    [`${topic}:tools-alert:3`]: { ...binding, streamKey: 'tools-alert:3', workspacePath: explicitWorkspace, rotateAfterWaves: 9 },
    [`${topic}:tools-alert`]: { ...binding, workspacePath: updatedWorkspace, rotateAfterWaves: 6 },
  };
  await runtime.applyAgentPreset({ host: 'codex', mode: 'inbox-only', topic: 'reconfiguration.trigger' });

  const inherited = runtime.mailbox.getWakeTarget('stream:tools-alert:2');
  assert.equal(inherited?.driverConfig?.workspacePath, updatedWorkspace);
  assert.equal(inherited?.rotateAfterWaves, 6);
  assert.equal(runtime.mailbox.list({ mailboxId: 'stream:tools-alert:2' }).queuedCount, 1);
  const explicitSlot = runtime.mailbox.getWakeTarget('stream:tools-alert:3');
  assert.equal(explicitSlot?.driverConfig?.workspacePath, explicitWorkspace);
  assert.equal(explicitSlot?.rotateAfterWaves, 9);
});

test('concurrent alert slot wakeups open independent tabs in one workspace', async t => {
  const workspacePath = directory(t);
  const store = new MailboxStore(join(workspacePath, 'mailbox.json'));
  const { runner, state } = fakeOrcaWorkspace(workspacePath);
  const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner }) });
  t.after(() => service.stop());
  const mailboxes = ['stream:tools-alert', 'stream:tools-alert:2', 'stream:tools-alert:3'];
  for (const [index, mailboxId] of mailboxes.entries()) {
    store.enqueue([alert(`slot-${index}`)], {
      mailboxId,
      wakeTarget: { driver: 'orca-terminal', driverConfig: { workspacePath } },
    });
  }
  const results = await Promise.all(mailboxes.map(mailboxId => service.flushMailbox(mailboxId, { force: true })));
  const handles = mailboxes.map(mailboxId => store.getWakeTarget(mailboxId)?.driverConfig?.terminalHandle as string);
  assert.equal(state.created, 3);
  assert.equal(new Set(handles).size, 3);
  for (const [index, result] of results.entries()) {
    assert.equal(result.woke, true, result.reason);
    const sends = sendsTo(state.calls, handles[index]);
    assert.equal(sends.length, 1);
    assert.ok(textOf(sends[0]).includes(`dealer inbox check --mailbox ${mailboxes[index]} --json`));
  }
});

test('a base alert already selecting a tab cannot take a newly started slot tab', async t => {
  const workspacePath = directory(t);
  const store = new MailboxStore(join(workspacePath, 'mailbox.json'));
  const { runner, state } = fakeOrcaWorkspace(workspacePath);
  const selecting = Promise.withResolvers<void>();
  const releaseSelection = Promise.withResolvers<void>();
  let inventories = 0;
  const commandRunner: CommandRunner = async request => {
    if (request.args[0] === 'terminal' && request.args[1] === 'list' && ++inventories === 2) {
      selecting.resolve();
      await releaseSelection.promise;
    }
    return runner(request);
  };
  const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner }) });
  t.after(() => service.stop());
  const base = 'stream:tools-alert';
  const slot = 'stream:tools-alert:2';
  const enqueue = (mailboxId: string) => store.enqueue([alert(mailboxId)], {
    mailboxId, wakeTarget: { driver: 'orca-terminal', driverConfig: { workspacePath } },
  });

  enqueue(base);
  const baseWake = service.flushMailbox(base, { force: true });
  await Promise.race([selecting.promise, baseWake]);
  try {
    enqueue(slot);
    const slotWake = await service.flushMailbox(slot, { force: true });
    assert.equal(slotWake.woke, true, slotWake.reason);
  } finally {
    releaseSelection.resolve();
  }
  await baseWake;
  if (store.getWakeTarget(base)?.pendingWake) {
    const retry = await service.flushMailbox(base, { force: true });
    assert.equal(retry.woke, true, retry.reason);
  }
  assert.equal(store.getWakeTarget(slot)?.driverConfig?.terminalHandle, 'term-new-1');
  assert.equal(store.getWakeTarget(base)?.driverConfig?.terminalHandle, 'term-new-2');
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
});

test('alert slot idle checks follow the selected tab and its attributed children', async t => {
  const workspacePath = directory(t);
  const store = new MailboxStore(join(workspacePath, 'mailbox.json'));
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-1', tabId: 'slot-1', leafId: 'root', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
    { handle: 'term-2', tabId: 'slot-2', leafId: 'root', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  state.agents = [{ state: 'working', paneKey: 'slot-1:root' }, { state: 'done', paneKey: 'slot-2:root' }];
  const child: (typeof state.children)[number] = { worktreeId: 'child', path: join(workspacePath, 'child'), unverifiableTerminalCount: 0 };
  state.children = [child];
  store.upsertWakeTarget('stream:tools-alert', { driver: 'orca-terminal', driverConfig: { workspacePath, terminalHandle: 'term-1' } });
  const mailboxId = 'stream:tools-alert:2';
  store.upsertWakeTarget(mailboxId, { driver: 'orca-terminal', driverConfig: { workspacePath, terminalHandle: 'term-2' } });
  const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner }) });
  t.after(() => service.stop());

  for (const [index, scenario] of [
    { agents: [], idle: true },
    { agents: [], unverifiableTerminalCount: 1, idle: false },
    { agents: [{ state: 'working', parentPaneKey: 'slot-1:root' }], idle: true },
    { agents: [{ state: 'working', parentPaneKey: 'slot-2:root' }], idle: false },
    { agents: [{ state: 'working' }], idle: false },
    { agents: undefined, idle: false },
  ].entries()) {
    child.agents = scenario.agents;
    child.unverifiableTerminalCount = scenario.unverifiableTerminalCount ?? 0;
    store.enqueue([alert(`child-${index}`)], { mailboxId });
    const result = await service.flushMailbox(mailboxId, { force: true });
    assert.equal(result.woke, scenario.idle, result.reason);
  }
  assert.equal(sendsTo(state.calls, 'term-2').length, 2);
  assert.equal(sendsTo(state.calls, 'term-1').length, 0);
});

test('a target tab requires complete inventory and checks claimed terminals in its child worktree', async t => {
  const workspacePath = directory(t);
  const store = new MailboxStore(join(workspacePath, 'mailbox.json'));
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-1', tabId: 'slot-1', leafId: 'root', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
    { handle: 'term-2', tabId: 'slot-2', leafId: 'root', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const terminal = { handle: 'term-child', connected: true, writable: true, agentIdentity: 'antigravity', preview: '> unsent draft' };
  const childPath = join(workspacePath, 'child');
  state.children = [{
    worktreeId: 'child', path: childPath, unverifiableTerminalCount: 0, terminals: [terminal],
    agents: [{ state: 'done', parentPaneKey: 'slot-2:root' }],
  }];
  store.upsertWakeTarget('task:child', {
    driver: 'orca-terminal', driverConfig: { workspacePath: childPath, terminalHandle: terminal.handle },
  });
  let childMissing = true;
  const commandRunner: CommandRunner = async request => {
    const output = await runner(request);
    if (!childMissing || request.args[0] !== 'worktree' || request.args[1] !== 'ps') return output;
    const inventory = JSON.parse(output.stdout);
    inventory.result.worktrees.length = 1;
    return { stdout: JSON.stringify(inventory) };
  };
  const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner }) });
  t.after(() => service.stop());
  const mailboxId = 'stream:tools-alert:2';
  store.enqueue([alert('child-ready')], {
    mailboxId, wakeTarget: { driver: 'orca-terminal', driverConfig: { workspacePath, terminalHandle: 'term-2' } },
  });
  assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, false, 'missing child inventory blocks');
  childMissing = false;
  assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, false, 'the claimed child composer still gates its parent');
  assert.equal(sendsTo(state.calls, 'term-2').length, 0);
  terminal.preview = '';
  assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, true);
});

test('a stale alert slot binding never writes into a sibling slot tab', async t => {
  const workspacePath = directory(t);
  const store = new MailboxStore(join(workspacePath, 'mailbox.json'));
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-1', tabId: 'slot-1', leafId: 'root', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const driverConfig = { workspacePath, terminalHandle: 'term-1', paneKey: 'slot-1:root' };
  store.upsertWakeTarget('stream:tools-alert', { driver: 'orca-terminal', driverConfig });
  const mailboxId = 'stream:tools-alert:2';
  store.enqueue([alert('stale-slot')], { mailboxId, wakeTarget: { driver: 'orca-terminal', driverConfig } });
  const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner }) });
  t.after(() => service.stop());

  const result = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(result.woke, true, result.reason);
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.terminalHandle, 'term-new-1');
  assert.equal(sendsTo(state.calls, 'term-1').length, 0);
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
});

test('a fixed workspace rebind skips claimed tabs and waits for the available tab’s child', async t => {
  const workspacePath = directory(t);
  const store = new MailboxStore(join(workspacePath, 'mailbox.json'));
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'other-stream', connected: true, writable: true, agentIdentity: 'antigravity', preview: '> unsent draft' },
    { handle: 'available', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const child: FakeOrcaChild = {
    path: join(workspacePath, 'follow-up'), worktreeId: 'follow-up', unverifiableTerminalCount: 0,
    agents: [{ state: 'working', parentPaneKey: null }],
    terminals: [{ ...probeTerminal, handle: 'term-child' }],
    lineage: { createdByTerminalHandle: 'available' },
  };
  state.children = [child];
  store.upsertWakeTarget('stream:other', { driver: 'orca-terminal', driverConfig: { workspacePath, terminalHandle: 'other-stream' } });
  const mailboxId = 'stream:tools-alert';
  store.enqueue([alert('closed-fixed')], {
    mailboxId, wakeTarget: { driver: 'orca-terminal', driverConfig: { workspacePath, terminalHandle: 'closed' } },
  });
  const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner }) });
  t.after(() => service.stop());

  const deferred = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(deferred.woke, false);
  assert.equal(deferred.reason, 'Workspace or descendant is busy or unverifiable');
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.terminalHandle, 'closed');
  assert.equal(terminalSends(state.calls).length, 0);

  child.agents = [{ state: 'done', parentPaneKey: null }];
  const result = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(result.woke, true, result.reason);
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.terminalHandle, 'available');
  assert.equal(sendsTo(state.calls, 'other-stream').length, 0);
  assert.equal(sendsTo(state.calls, 'available').length, 1);
  assert.equal(state.created, 0);
});

test('concurrent alert slots cannot both reclaim the same recorded pane', async t => {
  const workspacePath = directory(t);
  const store = new MailboxStore(join(workspacePath, 'mailbox.json'));
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'restored-tab', tabId: 'restored', leafId: 'root', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const mailboxes = ['stream:tools-alert:2', 'stream:tools-alert:3'];
  for (const mailboxId of mailboxes) {
    store.enqueue([alert(mailboxId)], {
      mailboxId, wakeTarget: { driver: 'orca-terminal', driverConfig: { workspacePath, paneKey: 'restored:root' } },
    });
  }
  const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner }) });
  t.after(() => service.stop());

  const results = await Promise.all(mailboxes.map(mailboxId => service.flushMailbox(mailboxId, { force: true })));
  for (const result of results.filter(result => !result.woke)) {
    const retry = await service.flushMailbox(result.mailboxId, { force: true });
    assert.equal(retry.woke, true, retry.reason);
  }
  const handles = mailboxes.map(mailboxId => store.getWakeTarget(mailboxId)?.driverConfig?.terminalHandle).sort();
  assert.deepEqual(handles, ['restored-tab', 'term-new-1']);
  assert.equal(sendsTo(state.calls, 'restored-tab').length, 1);
  assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
});

test('an explicit inbox check cannot rebind a slot to a sibling slot terminal', t => {
  const workspacePath = directory(t);
  const store = new MailboxStore(join(workspacePath, 'mailbox.json'));
  store.upsertWakeTarget('stream:tools-alert', {
    driver: 'orca-terminal',
    driverConfig: { workspacePath, terminalHandle: 'term-1' },
  });
  const mailboxId = 'stream:tools-alert:2';
  const slot2 = store.upsertWakeTarget(mailboxId, {
    driver: 'orca-terminal',
    driverConfig: { workspacePath, terminalHandle: 'term-2', paneKey: 'slot-2:root' },
  });
  assert.equal(reconcileCallerOrcaWakeTarget(store, mailboxId, {
    mailboxId: 'stream:tools-alert', workspacePath, terminalHandle: 'term-1', paneKey: 'slot-2:root',
  }, { requireBoundOwner: true }), undefined);
  assert.deepEqual(store.getWakeTarget(mailboxId), slot2);
});

test('an explicit rotateAfterWaves above one reuses the bound tab until the threshold', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const workspacePath = join(root, 'alerts');
  const { runner, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'term-alerts', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  const enqueue = (id: string) =>
    store.enqueue([alert(id)], {
      mailboxId: 'stream:alerts',
      wakeTarget: {
        driver: 'orca-terminal',
        coalesceMs: 0,
        rotateAfterWaves: 2,
        driverConfig: { terminalHandle: 'term-alerts', workspacePath },
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
  assert.equal(store.getWakeTarget('stream:alerts')!.driverConfig?.terminalHandle, 'term-new-1');
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
      flushIntervalMs: 100_000,
      drivers: createTestWakeupDrivers({ commandRunner: runner }),
    });
    t.after(() => service.stop());
    const enqueue = (id: string) =>
      store.enqueue([alert(id)], {
        mailboxId: 'stream:retained',
        wakeTarget: {
          driver: 'orca-terminal',
          coalesceMs: 0,
          rotateAfterWaves,
          driverConfig: { terminalHandle: 'term-retained', workspacePath },
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
    assert.equal(store.getWakeTarget('stream:retained')!.driverConfig?.terminalHandle, 'term-new-1');
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
  let terminalCount = 0;
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[0] === 'worktree' && request.args[1] === 'create') {
      return {
        stdout: JSON.stringify({ ok: true, result: { worktree: { path: join(root, 'child') } } }),
      };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'create') {
      terminalCount += 1;
      return {
        stdout: JSON.stringify({ ok: true, result: { terminal: { handle: `term-worker-${terminalCount}` } } }),
      };
    }
    if (request.args[0] === 'terminal' && request.args[1] === 'wait') {
      return {
        stdout: JSON.stringify(
          waitOk ? { ok: true } : { ok: false, error: { code: 'timeout' } },
        ),
      };
    }
    if (request.args[1] === 'ps') return { stdout: JSON.stringify({ ok: true, result: { worktrees: [{ path: join(root, 'child'), worktreeId: 'child', unverifiableTerminalCount: 0, agents: [{ state: 'done' }] }] } }) };
    if (request.args[1] === 'list') return { stdout: JSON.stringify({ ok: true, result: { terminals: [{ handle: `term-worker-${terminalCount}`, connected: true, writable: true, preview: '>' }] } }) };
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner, idleCheck: async () => ({ idle }) }),
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
  await service.flushMailbox('task:task-timeout');
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
      const claim = direct ? undefined : service.claim(`task:${message.id}`);
      try {
        await executor.execute([message], {} as ExecutionContext);
        if (!direct) assert.equal(terminalSends(calls).length, 0);
      } finally {
        claim?.dispose();
      }
      if (!direct) {
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
    driver: 'orca-terminal',
    coalesceMs: 0,
    driverConfig: { terminalHandle: 'term-w1', workspacePath: join(root, 'workspace-w1') },
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
  assert.ok(verifyEnvelope(delivered.signedEnvelope!));
  assert.deepEqual(delivered.envelope.payload.localAttachments, []);
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
  assert.ok(verifyEnvelope(questionItem.signedEnvelope!));
  assert.deepEqual(questionItem.envelope.payload.localAttachments, []);

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
  assert.ok(verifyEnvelope(replyItem.signedEnvelope!));
  assert.deepEqual(replyItem.envelope.payload.localAttachments, []);

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
  let aliceHandle = 'term-alice';
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
      const terminal = path === bobWtPath
        ? { handle: 'term-bob' }
        : { handle: aliceHandle, tabId: 'alice', leafId: 'root' };
      return { stdout: JSON.stringify({ ok: true, result: { terminals: [{ ...terminal, connected: true, writable: true, preview: '' }] } }) };
    }
    return { stdout: JSON.stringify({ ok: true }) };
  };
  const alice = await makeNode(t, root, 'alice', { commandRunner: runner });
  const bob = await makeNode(t, root, 'bob', { commandRunner: runner });
  await alice.mountNetwork({ networkId: 'team', passphrase: 'team-secret' });
  await bob.mountNetwork({ networkId: 'team', passphrase: 'team-secret' });
  await until(() => alice.node.listPeers().length === 1 && bob.node.listPeers().length === 1, 'peer discovery');
  const aliceEnv = { ORCA_TERMINAL_HANDLE: 'term-alice', ORCA_WORKTREE_ID: `repo::${aliceWtPath}`, ORCA_PANE_KEY: 'alice:root' };
  const bobEnv = { ORCA_TERMINAL_HANDLE: 'term-bob', ORCA_WORKTREE_ID: `repo::${bobWtPath}` };
  const taskMailbox = 'task:alice-agent';
  alice.mailbox.upsertWakeTarget(taskMailbox, {
    driver: 'orca-terminal', driverConfig: { workspacePath: aliceWtPath, terminalHandle: 'term-alice' },
  });
  const askOut: string[] = [];
  await runDealerCli(['ask', '--network', 'team', '--to', 'member:bob', '--target-mailbox', 'worktree:bob-wt', '--body', 'Please review'], { runtime: alice, env: aliceEnv, stdout: line => askOut.push(line) });
  const asked = JSON.parse(askOut.join('\n'));
  assert.equal(asked.replyMailbox, 'worktree:alice-wt');
  assert.equal(alice.mailbox.getWakeTarget('worktree:alice-wt')?.driver, 'orca-terminal');
  assert.equal(alice.mailbox.getWakeTarget('worktree:alice-wt')?.driverConfig?.terminalHandle, 'term-alice');
  assert.equal(alice.mailbox.getWakeTarget('worktree:alice-wt')?.driverConfig?.workspacePath, aliceWtPath);
  await until(() => bob.mailbox.list({ mailboxId: 'worktree:bob-wt' }).totalCount > 0, 'Bob inbound mailbox');
  await until(() => calls.some(call => call.args[0] === 'terminal' && call.args[1] === 'send' && call.args.includes('term-bob')), 'Bob terminal wake');
  const checkOut: string[] = [];
  await runDealerCli(['inbox', 'check'], { runtime: bob, env: bobEnv, stdout: line => checkOut.push(line) });
  const checked = JSON.parse(checkOut.join('\n'));
  assert.equal(checked.mailboxId, 'worktree:bob-wt');
  assert.equal(checked.items.length, 1);
  const replyOut: string[] = [];
  await runDealerCli(['reply', asked.questionId, '--body', 'Approved', '--ack', checked.deliveryId], { runtime: bob, env: bobEnv, stdout: line => replyOut.push(line) });
  const replied = JSON.parse(replyOut.join('\n'));
  assert.equal(replied.replyMailbox, 'worktree:bob-wt');
  assert.equal(replied.targetMailbox, 'worktree:alice-wt');
  assert.equal(bob.mailbox.getWakeTarget('worktree:bob-wt')?.driver, 'orca-terminal');
  assert.equal(bob.mailbox.getWakeTarget('worktree:bob-wt')?.driverConfig?.terminalHandle, 'term-bob');
  assert.equal(bob.mailbox.getWakeTarget('worktree:bob-wt')?.driverConfig?.workspacePath, bobWtPath);
  await until(() => alice.mailbox.list({ mailboxId: 'worktree:alice-wt' }).totalCount > 0, 'Alice reply mailbox');
  await until(() => calls.some(call => call.args[0] === 'terminal' && call.args[1] === 'send' && call.args.includes('term-alice')), 'Alice terminal wake');
  const aliceCheckOut: string[] = [];
  await runDealerCli(['inbox', 'check'], { runtime: alice, env: aliceEnv, stdout: line => aliceCheckOut.push(line) });
  const aliceCheck = JSON.parse(aliceCheckOut.join('\n'));
  assert.equal(aliceCheck.mailboxId, 'worktree:alice-wt');
  assert.equal(aliceCheck.items.length, 1);
  const followUpOut: string[] = [];
  await runDealerCli(['ask', '--reply-to', replied.replyId, '--body', 'Follow-up', '--ack', aliceCheck.deliveryId], { runtime: alice, env: aliceEnv, stdout: line => followUpOut.push(line) });
  await until(() => bob.mailbox.list({ mailboxId: 'worktree:bob-wt' }).totalCount > 0, 'Bob follow-up mailbox');
  await until(() => calls.some(call => call.args[0] === 'terminal' && call.args[1] === 'send' && call.args.includes('term-bob')), 'Bob terminal wake');
  const bobCheckOut: string[] = [];
  await runDealerCli(['inbox', 'check'], { runtime: bob, env: bobEnv, stdout: line => bobCheckOut.push(line) });
  const bobCheck = JSON.parse(bobCheckOut.join('\n'));
  assert.equal(bobCheck.mailboxId, 'worktree:bob-wt');
  assert.equal(bobCheck.items.length, 1);
  const settleOut: string[] = [];
  await runDealerCli(['settle', asked.questionId, '--outcome', 'succeeded', '--summary', 'Done', '--ack', bobCheck.deliveryId], { runtime: bob, env: bobEnv, stdout: line => settleOut.push(line) });
  await until(() => calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'send' && call.args.includes('term-alice')).length >= 2, 'Alice terminal settle wake');

  alice.mailbox.enqueue([alert('task-follow-up')], { mailboxId: taskMailbox });
  await until(() => sendsTo(calls, 'term-alice').some(call => textOf(call).includes(`--mailbox ${taskMailbox} --json`)), 'Alice task wake');
  const taskCheckOut: string[] = [];
  await runDealerCli(['inbox', 'check', '--mailbox', taskMailbox, '--json'], {
    runtime: alice,
    env: aliceEnv,
    stdout: line => taskCheckOut.push(line),
  });
  assert.equal(JSON.parse(taskCheckOut.join('\n')).items.length, 1);
  assert.equal(alice.mailbox.getWakeTarget(taskMailbox)?.driverConfig?.paneKey, 'alice:root');

  aliceHandle = 'term-alice-restored';
  alice.mailbox.enqueue([alert('restored-task-mail')], { mailboxId: taskMailbox });
  await until(() => sendsTo(calls, aliceHandle).some(call => textOf(call).includes(`--mailbox ${taskMailbox} --json`)), 'task wakes on the restored pane');
  assert.equal(alice.mailbox.getWakeTarget(taskMailbox)?.driverConfig?.terminalHandle, aliceHandle);
  assert.equal(alice.mailbox.getWakeTarget('worktree:alice-wt')?.driverConfig?.terminalHandle, 'term-alice');
  alice.mailbox.enqueue([alert('rpc-mail')], { mailboxId: 'worktree:alice-wt' });
  await until(() => sendsTo(calls, aliceHandle).some(call => textOf(call).includes('--mailbox worktree:alice-wt --json')), 'worktree alias wakes on the restored pane');
  assert.equal(alice.mailbox.getWakeTarget('worktree:alice-wt')?.driverConfig?.terminalHandle, aliceHandle);
  await alice.serveControl(async (argv, context) => {
    const stdout: string[] = [];
    const stderr: string[] = [];
    const code = await runDealerCli(argv, { runtime: alice, signal: context.signal, stdout: line => stdout.push(line), stderr: line => stderr.push(line) });
    return { code, stdout, stderr };
  });
  await runDealerCli(['inbox', 'check', '--json'], { stateDir: alice.stateDir, env: { ORCA_TERMINAL_HANDLE: 'term-rpc', ORCA_WORKTREE_ID: `repo::${aliceWtPath}` }, stdout: () => {} });
  assert.equal(alice.mailbox.getWakeTarget('worktree:alice-wt')?.driverConfig?.terminalHandle, 'term-rpc');

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
  assert.equal(runtime.mailbox.getWakeTarget('worktree:caller-wt')?.driverConfig?.terminalHandle, 'term-caller');

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

test('slotted Orca replies wake the inferred tab and preserve explicit mailbox overrides', async t => {
  const root = directory(t);
  const workspacePath = join(root, 'alert-processor-prod');
  const worktreeMailbox = 'worktree:alert-processor-prod';
  const { runner, state } = fakeOrcaWorkspace(workspacePath, ['term-1', 'term-2'].map(handle => ({
    handle, connected: true, writable: true, agentIdentity: 'codex', preview: '',
  })));
  const alerts = await makeNode(t, root, 'alerts', { commandRunner: runner });
  const prod = await makeNode(t, root, 'prod', { patch: { gateMode: 'open' } });
  await alerts.mountNetwork({ networkId: 'team', passphrase: 'team-secret' });
  await prod.mountNetwork({ networkId: 'team', passphrase: 'team-secret' });
  await until(() => alerts.node.listPeers().length === 1 && prod.node.listPeers().length === 1, 'peer discovery');

  alerts.mailbox.upsertWakeTarget(worktreeMailbox, {
    driver: 'orca-terminal', driverConfig: { workspacePath, terminalHandle: 'term-1' },
  });
  for (const [mailboxId, terminalHandle] of [
    ['stream:alert-processor-prod', 'term-1'],
    ['stream:alert-processor-prod:2', 'term-2'],
  ]) {
    alerts.mailbox.upsertWakeTarget(mailboxId, {
      driver: 'orca-terminal', rotateAfterWaves: 1, coalesceMs: 0,
      driverConfig: { workspacePath, terminalHandle, worktreeId: 'fake-worktree', agent: 'codex' },
    });
  }

  const callerArgs = ['--terminal-handle', 'term-2', '--workspace-path', workspacePath];
  const peerArgs = ['--network', 'team', '--to', 'member:prod', '--topic', 'release.prod.v1', ...callerArgs];
  const sent = await cli(alerts, 'send', ...peerArgs, '--body', '{}');
  assert.equal(sent.receipts[0].stage, 'dispatched');
  const inbound = prod.mailbox.listItemsAfterSequence(-1).find(item => item.id === sent.messageId);
  assert.equal(inbound?.envelope?.semantic?.replyMailbox, 'terminal:term-2');
  const pinned = alerts.mailbox.getWakeTarget('terminal:term-2');
  assert.equal(pinned?.driverConfig?.workspacePath, workspacePath);
  assert.equal(pinned?.driverConfig?.worktreeId, 'fake-worktree');
  assert.equal(pinned?.driverConfig?.agent, 'codex');
  assert.equal(pinned?.rotateAfterWaves, undefined);

  const replied = await cli(prod, 'reply', sent.messageId, '--body', 'Released');
  assert.equal(replied.targetMailbox, 'terminal:term-2');
  assert.equal(replied.receipts[0].stage, 'dispatched');
  await until(() => sendsTo(state.calls, 'term-2').some(call => textOf(call).includes('--mailbox terminal:term-2 --json')), 'second tab reply wake');
  assert.equal(alerts.mailbox.list({ mailboxId: 'terminal:term-2' }).items[0]?.id, replied.replyId);
  assert.equal(alerts.mailbox.getWakeTarget(worktreeMailbox)?.driverConfig?.terminalHandle, 'term-1');
  assert.equal(sendsTo(state.calls, 'term-1').length, 0);

  for (const explicitMailbox of ['custom:coord', worktreeMailbox]) {
    const explicitSend = await cli(alerts, 'send', ...peerArgs, '--body', '{}', '--reply-mailbox', explicitMailbox);
    const explicitInbound = prod.mailbox.listItemsAfterSequence(-1).find(item => item.id === explicitSend.messageId);
    assert.equal(explicitInbound?.envelope?.semantic?.replyMailbox, explicitMailbox);
  }
  const asked = await cli(alerts, 'ask', ...peerArgs, '--body', 'Release status?', '--mailbox', 'custom:ask');
  assert.equal(asked.replyMailbox, 'custom:ask');
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

test('composer emptiness ignores only exact mode placeholders and status chrome', () => {
  for (const preview of ['', 'history', '>', '>   \n────────\nstatus bar']) assert.equal(isOrcaComposerEmpty(preview), true);
  assert.equal(isOrcaComposerEmpty('> Auto mode: edits & safe commands auto-approved (shift+tab to cycle)', 'antigravity'), true);
  assert.equal(isOrcaComposerEmpty('> Auto mode: edits & safe commands auto-approved (shift+tab to cycle)', 'codex'), false);
  assert.equal(isOrcaComposerEmpty('>\nsecond line'), false);
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
  assert.equal((await checkOrcaWorkspaceIdle(runner, 'orca', root)).idle, false);
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
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('placeholder')], {
    mailboxId: 'stream:placeholder',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 10,
      completedWaves: 1,
      driverConfig: { terminalHandle: 'term-agy', workspacePath },
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
    const service = new WakeupRedriveService({
      store,
      flushIntervalMs: 100_000,
      drivers: createTestWakeupDrivers({ commandRunner: runner }),
    });
    t.after(() => service.stop());
    store.enqueue([alert(`draft-${agentIdentity}`)], {
      mailboxId: 'stream:draft',
      wakeTarget: {
        driver: 'orca-terminal',
        coalesceMs: 0,
        driverConfig: { terminalHandle: 'term-draft', workspacePath },
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
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('fresh-dead')], {
    mailboxId: 'stream:fresh-dead',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      driverConfig: { workspacePath },
    },
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
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  const enqueue = (id: string) =>
    store.enqueue([alert(id)], {
      mailboxId: 'stream:rotate-readonly',
      wakeTarget: {
        driver: 'orca-terminal',
        coalesceMs: 0,
        rotateAfterWaves: 1,
        driverConfig: { terminalHandle: 'term-bound', workspacePath },
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
  first.mailbox.upsertWakeTarget('stream:collab.request.v4', { driverConfig: {...{ paneKey: 'runtime:pane' }, terminalHandle: 'runtime-tab', agent: 'claude', sessionId: 'runtime-session'}});
  await first.stop();

  const second = await make().start();
  t.after(() => second.stop());
  assert.equal(
    second.mailbox.getWakeTarget('stream:collab.request.v4')!.driverConfig?.terminalHandle,
    'runtime-tab',
    'a restart must not erase the live tab resolved at runtime',
  );
  assert.equal(second.mailbox.getWakeTarget('stream:collab.request.v4')!.rotateAfterWaves, 1);
  assert.equal(second.mailbox.getWakeTarget('stream:collab.request.v4')!.driverConfig?.agent, 'claude');
  assert.equal(second.mailbox.getWakeTarget('stream:collab.request.v4')!.driverConfig?.sessionId, 'runtime-session');
  assert.equal(sessionBinding(second.mailbox.getWakeTarget('stream:collab.request.v4')).paneKey, 'runtime:pane');

  // Refreshing the same binding keeps the runtime handle; switching workspaces clears it.
  await second.applyAgentPreset({
    host: 'orca', agent: 'claude', conversationMode: 'fixed', topic: 'collab.request.v4', workspacePath,
  });
  assert.equal(second.mailbox.getWakeTarget('stream:collab.request.v4')!.driverConfig?.terminalHandle, 'runtime-tab');
  await second.applyAgentPreset({
    host: 'orca', conversationMode: 'fixed', topic: 'collab.request.v4', workspacePath: join(root, 'other'),
  });
  assert.equal(second.mailbox.getWakeTarget('stream:collab.request.v4')!.driverConfig?.terminalHandle, undefined);

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
  assert.equal(pinned.driverConfig?.terminalHandle, 'explicit-tab');
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
  assert.equal(runBound.driver, 'orca-run');
  assert.equal(runBound.driverConfig?.terminalHandle, undefined, 'an Orca Run clears the stale pinned tab');
  assert.equal(runBound.rotateAfterWaves, undefined, 'an Orca Run owns its coordinator session');

  for (const change of [
    { workspacePath: join(root, 'changed') },
    { terminalHandle: 'different-tab' },
    { runId: 'different-run' },
    { terminalHandle: 'runtime-tab' },
    { sessionId: 'configured-session' },
  ]) {
    second.mailbox.upsertWakeTarget('stream:collab.request.v4', {
      driver: 'orca-terminal', driverConfig: { workspacePath, terminalHandle: 'runtime-tab',...{ paneKey: 'runtime:pane' }, agent: 'codex', sessionId: 'runtime-session'}});
    second.config.topicBindings = {
      'collab.request.v4': { executor: 'orca-wave', mailboxId: 'stream:collab.request.v4', workspacePath, ...change },
    };
    await second.applyAgentPreset({ host: 'codex', conversationMode: 'inbox-only', topic: 'reconfiguration.trigger' });
    const refreshed = second.mailbox.getWakeTarget('stream:collab.request.v4')!;
    const preserved = change.terminalHandle === 'runtime-tab' || change.sessionId !== undefined;
    assert.equal(refreshed.driverConfig?.sessionId, change.sessionId ?? (preserved ? 'runtime-session' : undefined));
    assert.equal(sessionBinding(refreshed).paneKey, change.terminalHandle === 'runtime-tab' ? 'runtime:pane' : undefined);
    assert.equal(refreshed.driverConfig?.agent, preserved ? 'codex' : undefined);
  }

});

test('inline wakeup previews include bounded latest body, semantic sender and thread on one line', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const calls: CommandRequest[] = [];
  const wakeup = new WakeupRedriveService({
    store,
    drivers: createTestWakeupDrivers({
      commandRunner: async (call) => {
        calls.push(call);
        return { stdout: '{"ok":true}' };
      },
    }),
  });
  t.after(() => wakeup.stop());
  for (const [id, body, semantic] of [
    ['long', 'x'.repeat(450) + '\r\nend', { type: 'question', threadId: 'thread-1' }],
    ['short', 'hello\r\nworld', { type: 'message', threadId: 'thread-2' }],
    ['empty', ' \n ', undefined],
  ] as const) {
    const claim = wakeup.claim(id);
    store.enqueue([alert(id, { body, semantic })], { mailboxId: id, wakeTarget: { driver: 'orca-run', driverConfig: { runId: 'run' } } });
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
    store.enqueue([alert(type, { semantic: { type, threadId: type, ...(type === 'settle' ? { outcome: 'succeeded' as const } : {}) } })], {
      mailboxId: type,
      wakeTarget: { driver: 'orca-run', coalesceMs: 5000, driverConfig: { runId: 'run' } },
    });
    assert.equal(store.getWakeTarget(type)?.earliestWakeAt, 10_000);
  }
  store.enqueue([alert('normal'), alert('control', { semantic: { type: 'reply', threadId: 'mixed' } })], {
    mailboxId: 'mixed',
    wakeTarget: { driver: 'orca-run', coalesceMs: 5000, driverConfig: { runId: 'run' } },
  });
  assert.equal(store.getWakeTarget('mixed')?.earliestWakeAt, 15_000);
  store.enqueue([alert('normal'), alert('new-control', { semantic: { type: 'question', threadId: 'mixed' } })], { mailboxId: 'mixed' });
  assert.equal(store.getWakeTarget('mixed')?.earliestWakeAt, 10_000, 'duplicates do not affect classification');
  store.upsertWakeTarget('mixed', { attemptCount: 2, earliestWakeAt: 30_000 });
  store.enqueue([alert('retry', { semantic: { type: 'settle', threadId: 'mixed', outcome: 'succeeded' } })], { mailboxId: 'mixed' });
  assert.equal(store.getWakeTarget('mixed')?.earliestWakeAt, 30_000);
  const calls: CommandRequest[] = [];
  const wakeup = new WakeupRedriveService({
    store,
    now: () => 10_000,
    drivers: createTestWakeupDrivers({
      commandRunner: async (call) => {
        calls.push(call);
        return { stdout: '{"ok":true}' };
      },
    }),
  });
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
    store.enqueue([alert('first', { semantic: { type: 'reply', threadId: 'old' } })], {
      mailboxId,
      wakeTarget: {
        driver: mode === 'passive' ? 'agent-host' : mode,
        driverConfig: {
          ...(mode === 'passive' ? { hostAgent: 'claude-code', conversationMode: 'spawn' } : {}),
          runId: 'run',
          terminalHandle: 'term',
        },
      },
    });
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const pointers: string[] = [];
    const service = new WakeupRedriveService({
      store,
      flushIntervalMs: 100_000,
      drivers: createTestWakeupDrivers({
        idleCheck: async () => ({ idle: true }),
        commandRunner: async (call) => {
          if (mode !== 'orca-terminal' || call.args[1] === 'send') {
            pointers.push(
              mode === 'passive' ? call.args.at(-1)! : mode === 'orca-run' ? bodyOf(call) : textOf(call),
            );
            if (pointers.length === 1) await held;
          }
          return { stdout: '{"ok":true}' };
        },
      }),
    });
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
      assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.conversationMode, 'fixed');
      assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.sessionId, 'box');
    }
  });
}

test('sequence settlement remains finite when wall clock order differs from arrival order', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 20_000 });
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.enqueue([alert('leased')], { mailboxId: 'box' });
  store.check({ mailboxId: 'box' });
  store.enqueue([alert('later-clock')], {
    mailboxId: 'box',
    wakeTarget: { driver: 'agent-host', driverConfig: { hostAgent: 'codex', sessionId: 'existing' } },
  });
  t.mock.timers.setTime(10_000);
  store.enqueue([alert('later-sequence')], { mailboxId: 'box' });
  assert.equal(store.check({ mailboxId: 'box', peek: true }).latestQueued?.id, 'later-clock');
  let calls = 0;
  const wakeup = new WakeupRedriveService({
    store,
    drivers: createTestWakeupDrivers({
      commandRunner: async () => {
        calls++;
        return { stdout: '{}' };
      },
    }),
  });
  t.after(() => wakeup.stop());
  await wakeup.flushMailbox('box', { force: true });
  assert.equal(calls, 1);
  assert.equal(store.getWakeTarget('box')?.pendingWake, false);
});

test('in-flight ordinary arrivals retain enqueue coalescing deadline while replies remain immediate', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 10_000 });
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.enqueue([alert('first')], {
    mailboxId: 'box',
    wakeTarget: {
      driver: 'agent-host',
      coalesceMs: 5000,
      driverConfig: { hostAgent: 'claude-code', conversationMode: 'fixed', sessionId: 'session' },
    },
  });
  let calls = 0;
  const wakeup = new WakeupRedriveService({
    store,
    drivers: createTestWakeupDrivers({
      commandRunner: async () => {
        calls++;
        if (calls === 1) {
          t.mock.timers.setTime(11_000);
          store.enqueue([alert('ordinary')], { mailboxId: 'box' });
        }
        return { stdout: '{}' };
      },
    }),
  });
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
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({
      commandRunner: runner,
      idleCheck: async () => ({ idle: ++idleChecks !== 2 }),
    }),
  });
  t.after(() => service.stop());

  store.enqueue([alert('deferred')], {
    mailboxId: 'stream:deferred',
    wakeTarget: { driver: 'orca-terminal', driverConfig: { workspacePath: '/fixed' } },
  });
  const deferred = await service.flushMailbox('stream:deferred');
  assert.equal(deferred.woke, false);
  assert.match(deferred.reason!, /Terminal spawned/);
  assert.equal(store.getWakeTarget('stream:deferred')!.driverConfig?.terminalHandle, 'term-1');
  const delivered = await service.flushMailbox('stream:deferred', { force: true });
  assert.equal(delivered.woke, true);
  assert.equal(store.getWakeTarget('stream:deferred')!.driverConfig?.terminalHandle, 'term-1');
  assert.equal(store.getWakeTarget('stream:deferred')!.pendingWake, false);
  const send = calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'send').at(-1)!;
  assert.equal(send.args[send.args.indexOf('--terminal') + 1], 'term-1');
  assert.equal(
    calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'create').length,
    1,
    'the deferred terminal is reused instead of respawned',
  );

  idleChecks = 10;
  store.enqueue([alert('direct')], {
    mailboxId: 'stream:direct',
    wakeTarget: { driver: 'orca-terminal', driverConfig: { workspacePath: '/fixed-direct' } },
  });
  const direct = await service.flushMailbox('stream:direct');
  assert.equal(direct.woke, true);
  assert.equal(store.getWakeTarget('stream:direct')!.driverConfig?.terminalHandle, 'term-2');
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
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  store.enqueue([alert('heal')], {
    mailboxId: 'box',
    wakeTarget: { driver: 'orca-terminal', driverConfig: { workspacePath, terminalHandle: 'term-dead' } },
  });

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
  assert.equal(target.driverConfig?.terminalHandle, 'term-live');
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
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());

  store.enqueue([alert('gone-reply', { semantic: { type: 'reply', threadId: 'gone-task' } })], {
    mailboxId: 'worktree:gone',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      driverConfig: { terminalHandle: 'term-gone', workspacePath: gonePath },
    },
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
    wakeTarget: { driver: 'orca-terminal', coalesceMs: 0, driverConfig: { workspacePath: gonePath } },
  });
  assert.equal((await service.flushMailbox('task:gone-spawn')).reason, 'Workspace not found');
  assert.equal(store.getWakeTarget('task:gone-spawn')!.pendingWake, false);
  assert.equal(calls.filter(call => call.args[0] === 'terminal' && call.args[1] === 'create').length, 0);

  // A workspace whose directory still exists keeps its durable retry ledger even while Orca's
  // inventory is incomplete; only a path that is truly gone is disarmed.
  store.enqueue([alert('live-reply', { semantic: { type: 'reply', threadId: 'live-task' } })], {
    mailboxId: 'worktree:live',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      driverConfig: { terminalHandle: 'term-live', workspacePath: livePath },
    },
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
  new MailboxStore(path).enqueue([alert('legacy')], { mailboxId: 'box', wakeTarget: { driver: 'orca-run', driverConfig: { runId: 'run-legacy' } } });
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
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({
      commandRunner: async (request) => {
        calls.push(request);
        return { stdout: '{"ok":true}' };
      },
    }),
  });
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
    store.enqueue([alert('first')], {
      mailboxId: 'box',
      wakeTarget: {
        driver: mode === 'passive' ? 'agent-host' : mode,
        driverConfig: {
          ...(mode === 'passive'
            ? { hostAgent: 'claude-code' as const, conversationMode: 'spawn' as const }
            : {}),
          runId: 'run',
          terminalHandle: 'term',
        },
      },
    });
    let effects = 0;
    const wakeup = new WakeupRedriveService({
      store,
      drivers: createTestWakeupDrivers({
        idleCheck: async () => ({ idle: true }),
        commandRunner: async (call) => {
          if (mode !== 'orca-terminal' || call.args[1] === 'send') {
            effects++;
            throw new Error('accepted then result lost');
          }
          return { stdout: '{"ok":true}' };
        },
      }),
    });
    t.after(() => wakeup.stop());
    assert.equal((await wakeup.flushMailbox('box')).woke, false);
    assert.equal(store.getWakeTarget('box')?.deliveryInDoubt, true);
    assert.equal(store.getWakeTarget('box')?.pendingWake, false);
    assert.match(store.getWakeTarget('box')!.lastWakeError!, /result lost/);
    assert.equal(effects, 1);
    await wakeup.stop();
    const restarted = new MailboxStore(file);
    const recovery = new WakeupRedriveService({
      store: restarted,
      drivers: createTestWakeupDrivers({
        idleCheck: async () => ({ idle: true }),
        commandRunner: async () => {
          effects++;
          return { stdout: '{"ok":true}' };
        },
      }),
    });
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
  store.enqueue([alert('first')], {
    mailboxId: 'box',
    wakeTarget: { driver: 'agent-host', driverConfig: { hostAgent: 'claude-code' } },
  });
  store.beginWakeDelivery('box');
  const restarted = new MailboxStore(file);
  const service = new WakeupRedriveService({
    store: restarted,
    drivers: createTestWakeupDrivers({
      commandRunner: async () => {
        assert.fail('unknown delivery must not be replayed');
      },
    }),
  });
  t.after(() => service.stop());
  assert.equal((await service.flushMailbox('box', { force: true })).woke, false);
  assert.equal(restarted.getWakeTarget('box')?.deliveryInDoubt, true);
});

for (const mode of ['passive', 'orca-run', 'orca-terminal'] as const) {
  test(`${mode} pre-effect command failure backs off and resumes safely after repair`, async t => {
    const root = directory(t);
    const store = new MailboxStore(join(root, 'mailbox.json'));
    store.enqueue([alert('first')], {
      mailboxId: 'box',
      wakeTarget: {
        driver: mode === 'passive' ? 'agent-host' : mode,
        driverConfig: {
          ...(mode === 'passive' ? { hostAgent: 'claude-code' as const } : {}),
          runId: 'run',
          terminalHandle: 'term',
          interactiveCommand: join(root, 'missing'),
          orcaCommand: join(root, 'missing'),
        },
      },
    });
    const failing = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ idleCheck: async () => ({ idle: true }) }) });
    t.after(() => failing.stop());
    assert.equal((await failing.flushMailbox('box')).woke, false);
    const failed = store.getWakeTarget('box')!;
    assert.equal(failed.deliveryInDoubt, undefined);
    assert.equal(failed.pendingWake, true);
    assert.equal(failed.attemptCount, 1);
    assert.ok(failed.earliestWakeAt > Date.now());
    assert.match(failed.lastWakeError!, /did not start|ENOENT/);
    await failing.stop();
    const repaired = new WakeupRedriveService({
      store,
      drivers: createTestWakeupDrivers({
        idleCheck: async () => ({ idle: true }),
        commandRunner: async () => ({ stdout: '{"ok":true}' }),
      }),
    });
    t.after(() => repaired.stop());
    assert.equal((await repaired.flushMailbox('box', { force: true })).woke, true);
    assert.equal(store.getWakeTarget('box')?.pendingWake, false);
  });
}

test('local Orca target validation does not create an unknown-effect fence', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  let effects = 0;
  const service = new WakeupRedriveService({
    store,
    drivers: createTestWakeupDrivers({
      idleCheck: async () => ({ idle: true }),
      commandRunner: async () => {
        effects++;
        return { stdout: '{"ok":true}' };
      },
    }),
  });
  t.after(() => service.stop());
  const claim = service.claim('box');
  store.enqueue([alert('first')], { mailboxId: 'box', wakeTarget: { driver: 'orca-run' } });
  claim.dispose();
  assert.equal((await service.flushMailbox('box')).woke, false);
  assert.equal(effects, 0);
  assert.equal(store.getWakeTarget('box')?.deliveryInDoubt, undefined);
  assert.equal(store.getWakeTarget('box')?.pendingWake, true);
  assert.equal(store.getWakeTarget('box')?.attemptCount, 1);
  store.upsertWakeTarget('box', { driverConfig: { runId: 'repaired' } });
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
  store.upsertWakeTarget('box', { driver: 'orca-terminal', driverConfig: { terminalHandle: 'term-1' } });
  settleWakeDeliveryFailure(store, 'box', new Error('no_active_sender_terminal'));
  assert.equal(store.getWakeTarget('box')?.deliveryInDoubt, true);
});

test('rotating inbound delivery preserves terminal thread affinity across call order', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.upsertWakeTarget('stream:inbox', {
    driver: 'orca-terminal',
    rotateAfterWaves: 1,
    coalesceMs: 0,
    driverConfig: { terminalHandle: 'term-1' },
  });
  const inbound = alert('thread-a', { semantic: { type: 'message', threadId: 'thread-a', replyMailbox: 'default' } });
  store.enqueue([inbound], { mailboxId: 'stream:inbox' });
  store.completeWakeDelivery('stream:inbox', 0, { driverConfig: { terminalHandle: 'term-1' } });
  store.recordInboundEnvelope(inbound, pinOrcaTerminalSessionMailbox(store, 'stream:inbox', 'term-1'));
  assert.equal(store.resolveThreadLocalMailbox('thread-a'), 'terminal:term-1');
  assert.equal(store.getWakeTarget('terminal:term-1')?.rotateAfterWaves, undefined);
});

test('pinning a rotating session preserves existing terminal wake fences', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  store.upsertWakeTarget('stream:inbox', { driver: 'orca-terminal', rotateAfterWaves: 1, driverConfig: { terminalHandle: 'term-1' } });
  store.upsertWakeTarget('terminal:term-1', { driver: 'orca-terminal', pendingWake: true, driverConfig: { terminalHandle: 'term-1' } });
  const notifications: string[] = [];
  const subscription = store.onWakeTarget(mailbox => notifications.push(mailbox));
  t.after(() => subscription.dispose());
  assert.equal(pinOrcaTerminalSessionMailbox(store, 'stream:inbox', 'term-1'), 'terminal:term-1');
  const target = store.getWakeTarget('terminal:term-1');
  assert.equal(target?.pendingWake, true);
  assert.ok(notifications.includes('terminal:term-1'));
  store.upsertWakeTarget('stream:inbox', { deliveryInDoubt: true, lastWakeError: 'fenced' });
  assert.equal(pinOrcaTerminalSessionMailbox(store, 'stream:inbox', 'term-2'), 'terminal:term-2');
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
  const service = new WakeupRedriveService({
    store: recovered,
    flushIntervalMs: 100_000,
    drivers: createTestWakeupDrivers({ commandRunner: runner }),
  });
  t.after(() => service.stop());
  assert.equal((await service.flushMailbox('task:account-gate', { force: true })).woke, false);
  assert.equal(recovered.getWakeTarget('task:account-gate')!.attemptCount, 2);
  assert.equal(recovered.getWakeTarget('task:account-gate')!.deliveryInDoubt, undefined);
  accountReady = true;
  state.waitOk = false;
  assert.equal((await service.flushMailbox('task:account-gate', { force: true })).woke, false);
  assert.equal(recovered.getWakeTarget('task:account-gate')!.driverConfig?.terminalHandle, 'term-new-1');
  assert.equal(state.created, 1);
  assert.equal(terminalSends(state.calls).length, 0);
  assert.equal(launches, 3, 'two failed launches and the first dedicated tab');

  // If that tab exits before delivery, recovery must replace it through Orca's account-aware
  // terminal creation path, never bootstrap the agent in the surviving bare login shell.
  state.terminals = state.terminals.filter(terminal => terminal.handle !== 'term-new-1');
  assert.equal((await service.flushMailbox('task:account-gate', { force: true })).woke, false);
  assert.equal(recovered.getWakeTarget('task:account-gate')!.driverConfig?.terminalHandle, 'term-new-2');
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

test('a named Orca spawn mailbox recovers its own terminal after restart', async t => {
  const root = directory(t);
  const workspacePath = join(root, 'child');
  const mailboxId = 'named-spawn';
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const { runner: inventory, state } = fakeOrcaWorkspace(workspacePath, [
    { handle: 'shell', connected: true, writable: true, preview: 'user@host %' },
    { handle: 'sibling-agent', connected: true, writable: true, agentIdentity: 'antigravity', preview: '' },
  ]);
  const runner: CommandRunner = async request => {
    if (request.args[0] === 'worktree' && request.args[1] === 'create') {
      state.calls.push(request);
      return { stdout: JSON.stringify({ ok: true, result: { worktree: { path: workspacePath } } }) };
    }
    return inventory(request);
  };
  const plugin = createOrcaSpawnExecutorPlugin({
    store, commandRunner: runner, cwd: root, noParent: true,
    resolveTopicBinding: () => ({ mailboxId, mode: 'spawn' }),
  });
  state.waitOk = false;
  const receipt = await plugin.execute([alert('named-spawn')], { networkId: 'team', nodeId: 'receiver', traceId: 'spawn' }, {});
  assert.equal(receipt.mailboxId, mailboxId);
  assert.equal(receipt.woke, false);
  assert.equal(state.created, 1);
  assert.equal(terminalSends(state.calls).length, 0);

  state.terminals = state.terminals.filter(terminal => terminal.handle !== 'term-new-1');
  state.waitOk = true;
  const recovered = new MailboxStore(store.path);
  const wakeup = new WakeupRedriveService({ store: recovered, drivers: createTestWakeupDrivers({ commandRunner: runner }) });
  t.after(() => wakeup.stop());
  assert.equal((await wakeup.flushMailbox(mailboxId, { force: true })).woke, true);
  assert.equal(state.calls.filter(call => call.args[0] === 'worktree' && call.args[1] === 'create').length, 1);
  assert.equal(state.created, 2);
  assert.equal(terminalSends(state.calls).length, 1);
  assert.match(textOf(sendsTo(state.calls, 'term-new-2')[0]), /dealer inbox check --mailbox named-spawn --json/);
  assert.equal(recovered.getWakeTarget(mailboxId)?.driverConfig?.dedicatedSpawn, true);
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
      if (operation === 'worktree ps') return { stdout: JSON.stringify({ ok: true, result: { worktrees: [{ path: join(root, failure), worktreeId: failure, unverifiableTerminalCount: 0, agents: [{ state: 'done' }] }] } }) };
      if (operation === 'terminal list') return { stdout: JSON.stringify({ ok: true, result: { terminals: [{ handle: 'task-tab', connected: true, writable: true, preview: '>' }] } }) };
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
        const service = new WakeupRedriveService({
          store: new MailboxStore(store.path),
          drivers: createTestWakeupDrivers({ commandRunner: runner }),
        });
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
    if (request.args[1] === 'wait' && request.args.includes('task-old')) return { stdout: '{"ok":true}' };
    return inventory(request);
  };
  store.enqueue([alert('race')], {
    mailboxId: 'task:race',
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      driverConfig: { workspacePath, terminalHandle: 'task-old', agent: 'antigravity' },
    },
  });
  const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner }) });
  t.after(() => service.stop());
  state.waitOk = false;
  const deferred = await service.flushMailbox('task:race', { force: true });
  assert.equal(deferred.woke, false);
  assert.equal(state.created, 1);
  assert.equal(store.getWakeTarget('task:race')!.driverConfig?.terminalHandle, 'term-new-1');
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
    store.enqueue([alert(failure)], {
      mailboxId,
      wakeTarget: { driver: 'orca-terminal', coalesceMs: 0, driverConfig: { workspacePath: root } },
    });
    if (failure === 'crash') {
      store.beginWakeDelivery(mailboxId); // Simulate a process exit during terminal creation.
    } else {
      t.mock.method(store, 'commitWakeLaunch', () => { throw new Error('handle persistence failed'); });
      await assert.rejects(launchFencedOrcaWorkspaceTerminal(store, async () => ({
        stdout: '{"ok":true,"result":{"handle":"created-before-disk-error"}}',
      }), mailboxId, { command: 'orca', workspacePath: root }), /persistence failed/);
    }
    const restarted = new MailboxStore(store.path);
    assert.equal(restarted.getWakeTarget(mailboxId)!.deliveryInDoubt, true);
    assert.equal(restarted.getWakeTarget(mailboxId)!.pendingWake, false);
    let calls = 0;
    const service = new WakeupRedriveService({
      store: restarted,
      drivers: createTestWakeupDrivers({
        commandRunner: async () => {
          calls++;
          throw new Error('Must not retry an unknown launch');
        },
      }),
    });
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
      store.enqueue([alert(mailboxId)], {
        mailboxId,
        wakeTarget: { driver: 'orca-terminal', coalesceMs: 0, driverConfig: { workspacePath: root } },
      });
      const service = new WakeupRedriveService({
        store,
        drivers: createTestWakeupDrivers({ commandRunner: runner, idleCheck: async () => ({ idle: true }) }),
      });
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

for (const preview of [
  '> human draft',
  '• ,1（主推）：原始建议\n> 原始建议',
  '下一步建议\n• 原始建议\n> 用户改写的草稿',
  '• ,1（主推）：原始建议\n> 原始建议\n第二行草稿',
  '> unsent draft\n────────\nstatus bar',
]) {
  test(`composer drafts block both workspace and terminal-only wakes: ${JSON.stringify(preview)}`, async t => {
    assert.equal(isOrcaComposerEmpty(preview), false);
    for (const scoped of [false, true]) {
      const root = directory(t);
      const store = new MailboxStore(join(root, 'mailbox.json'));
      const fake = fakeOrcaWorkspace(root, [{ handle: 'draft', connected: true, writable: true, agentIdentity: 'codex', preview }]);
      const runner: CommandRunner = async request => request.args[1] === 'show'
        ? { stdout: JSON.stringify({ ok: true, result: { terminal: fake.state.terminals[0] } }) }
        : fake.runner(request);
      const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner }), flushIntervalMs: 100_000 });
      t.after(() => service.stop());
      store.enqueue([alert(`draft-${scoped}`)], { mailboxId: 'terminal:draft', wakeTarget: {
        driver: 'orca-terminal', driverConfig: { terminalHandle: 'draft', ...(scoped ? { workspacePath: root } : {}) },
      } });
      const result = await service.flushMailbox('terminal:draft', { force: true });
      assert.equal(result.woke, false);
      assert.equal(store.getWakeTarget('terminal:draft')?.pendingWake, true);
      assert.equal(terminalSends(fake.state.calls).length, 0);
      fake.state.terminals[0].preview = '>\n────────\nstatus bar';
      assert.equal((await service.flushMailbox('terminal:draft', { force: true })).woke, true);
      assert.equal(terminalSends(fake.state.calls).length, 1);
      assert.equal(store.getWakeTarget('terminal:draft')?.pendingWake, false);
    }
  });
}

test('terminal session inference enriches workspace and terminal probes fail closed', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const calls: CommandRequest[] = [];
  let terminal = { connected: true, writable: true, preview: '>', worktreePath: root };
  let failedShow = false;
  const runner: CommandRunner = async request => {
    calls.push(request);
    if (request.args[1] === 'show') {
      if (failedShow) throw new Error('show unavailable');
      return { stdout: JSON.stringify({ ok: true, result: { terminal } }) };
    }
    return { stdout: '{"ok":true}' };
  };
  const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner }) });
  t.after(() => service.stop());
  assert.equal((await service.resolveSessionWakeTarget('terminal:target'))?.driverConfig?.workspacePath, root);
  store.enqueue([alert('disconnected')], { mailboxId: 'terminal:target', wakeTarget: {
    driver: 'orca-terminal', driverConfig: { terminalHandle: 'target' },
  } });
  for (const condition of ['disconnected', 'unwritable', 'show-failed']) {
    terminal = { ...terminal, connected: condition !== 'disconnected', writable: condition !== 'unwritable' };
    failedShow = condition === 'show-failed';
    assert.equal((await service.flushMailbox('terminal:target', { force: true })).woke, false);
    assert.equal(store.getWakeTarget('terminal:target')?.pendingWake, true);
  }
  assert.equal(terminalSends(calls).length, 0);
});

function sessionBinding(target) { return target?.driverConfig ?? {}; }
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
    assert.equal(bound.driverConfig?.agent, agent);
    assert.equal(bound.driverConfig?.sessionId, 'original-session');
    assert.equal(sessionBinding(bound).paneKey, 'tab:leaf');
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
    assert.equal(resumed.driverConfig?.terminalHandle, 'term-new-1');
    assert.equal(resumed.driverConfig?.sessionId, 'original-session');
    assert.equal(sessionBinding(resumed).paneKey, undefined);
    await invoke(['inbox', 'check'], { ...env, ORCA_TERMINAL_HANDLE: 'term-new-1', [envKey]: '' });
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.driverConfig?.sessionId, 'original-session');
    await invoke(['inbox', 'check'], { ...env, ORCA_TERMINAL_HANDLE: 'term-restored-shell', [envKey]: '' });
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.driverConfig?.sessionId, 'original-session');
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.driverConfig?.terminalHandle, 'term-restored-shell');
    await invoke(['inbox', 'check'], { ...env, ORCA_TERMINAL_HANDLE: 'term-shell', ORCA_PANE_KEY: '', [envKey]: '' });
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.driverConfig?.sessionId, undefined);
    assert.equal(sessionBinding(runtime.mailbox.getWakeTarget(mailboxId)).paneKey, undefined);
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.driverConfig?.agent, undefined);
    await runtime.mountNetwork({ networkId: 'capture', passphrase: 'capture-test-secret' });
    await invoke(['broadcast', '--network', 'capture', '--topic', 'test.capture', '--body', 'hello']);
    const broadcastTarget = runtime.mailbox.getWakeTarget(mailboxId)!;
    assert.equal(broadcastTarget.driverConfig?.terminalHandle, 'term-origin');
    assert.equal(broadcastTarget.driverConfig?.agent, agent);
    assert.equal(broadcastTarget.driverConfig?.sessionId, 'original-session');
    assert.equal(sessionBinding(broadcastTarget).paneKey, 'tab:leaf');
    await runtime.stop();
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
      const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner })});
      t.after(() => service.stop());
      store.enqueue([alert('conversation-mail')], { mailboxId, wakeTarget: { driver: 'orca-terminal', coalesceMs: 0, driverConfig: {...{ paneKey: 'tab:leaf' }, workspacePath, terminalHandle: 'term-origin', sessionId: 'original'}} });
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
  assert.throws(() => buildOrcaAgentLaunchCommand({ sessionId: 'session', interactiveCommand: 'custom-agent --flag' }), OrcaTerminalLaunchRejectedError);
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
    driver: 'orca-terminal', rotateAfterWaves: 1, driverConfig: { workspacePath: '/workspace', terminalHandle: 'term-original',...{ retiredSessionId: 'prior-session', configuredSessionId: 'original-session', configuredTerminalHandle: 'term-original', retiredTerminalHandle: 'term-retired', paneKey: 'tab:leaf' }, agent: 'codex', sessionId: 'original-session'}});
  assert.equal(pinOrcaTerminalSessionMailbox(store, 'stream:wave', 'term-original'), 'terminal:term-original');
  const pinned = new MailboxStore(file).getWakeTarget('terminal:term-original')!;
  assert.equal(pinned.driverConfig?.agent, 'codex');
  assert.equal(pinned.driverConfig?.sessionId, 'original-session');
  assert.equal(sessionBinding(pinned).paneKey, 'tab:leaf');
  assert.equal(pinned.rotateAfterWaves, undefined);
  assert.equal(sessionBinding(pinned).configuredSessionId, undefined);
  assert.equal(sessionBinding(pinned).configuredTerminalHandle, undefined);
  assert.equal(sessionBinding(pinned).retiredSessionId, undefined);
  assert.equal(sessionBinding(pinned).retiredTerminalHandle, undefined);
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
    const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: async request => {
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
    } })});
    t.after(() => service.stop());
    store.enqueue([alert('restored-busy')], { mailboxId: 'worktree:restored', wakeTarget: { driver: 'orca-terminal', coalesceMs: 0, driverConfig: {...{ paneKey: 'tab:leaf' }, workspacePath, terminalHandle: 'term-old', sessionId: 'original'}} });
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
    const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: async request => {
      // Initial liveness, authoritative idle, and replacement check see the task tab;
      // it exits just before the final read-only selection inventory.
      if (request.args[0] === 'terminal' && request.args[1] === 'list' && ++lists === 4) {
        state.terminals = state.terminals.filter(terminal => terminal.handle !== 'term-task');
      }
      return runner(request);
    } })});
    t.after(() => service.stop());
    store.enqueue([alert('task-race')], { mailboxId: 'task:isolated', wakeTarget: {
      driver: 'orca-terminal', coalesceMs: 0, driverConfig: { workspacePath, terminalHandle: 'term-task'}} });
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
      driver: 'orca-terminal', driverConfig: { workspacePath, terminalHandle: 'term-original',...{ paneKey: 'old:pane' }, agent: 'codex', sessionId: 'old-session'}});
    const code = await runDealerCli(['inbox', 'check', '--json'], {
      runtime, env: { ORCA_WORKTREE_ID: `repo::${workspacePath}`, ORCA_TERMINAL_HANDLE: 'term-original', ...env },
      stdout: () => {}, stderr: line => assert.fail(line),
    });
    assert.equal(code, 0, name);
    const actual = runtime.mailbox.getWakeTarget(mailboxId)!;
    assert.deepEqual({ agent: actual.driverConfig?.agent, sessionId: actual.driverConfig?.sessionId, paneKey: sessionBinding(actual).paneKey }, expected, name);
  }
});

for (const source of ['refresh', 'wave'] as const) {
  test(`${source} replaces configured sessions and never resurrects a rotated session during recovery`, async t => {
    const root = directory(t);
    const workspacePath = join(root, 'configured');
    const mailboxId = 'stream:configured';
    const { runner, state } = fakeOrcaWorkspace(workspacePath, [
      { handle: 'term-A', connected: true, writable: true, agentIdentity: 'claude', preview: '', tabId: 'pane', leafId: 'A' },
    ]);
    const binding = { executor: 'orca-wave', mailboxId, workspacePath, agent: 'claude' as const, sessionId: 'B', rotateAfterWaves: 1 };
    const options = { stateDir: join(root, 'state'), member: 'worker', name: 'worker', profile: 'headless-relay',
      transport: { discoveryDir: join(root, 'mesh') }, commandRunner: runner,
      patch: { defaultExecutor: 'inbox', executors: ['orca-wave', 'inbox'], topicBindings: { configured: binding }, wave: { flushIntervalMs: 100_000 } },
    };
    let runtime = await new DealerNodeRuntime(options).start();
    t.after(() => runtime.stop());
    await runtime.wakeups.stop();
    runtime.mailbox.upsertWakeTarget(mailboxId, { driver: 'orca-terminal', completedWaves: 1, rotateAfterWaves: 1, driverConfig: { workspacePath,...{ paneKey: 'pane:A', configuredSessionId: undefined }, agent: 'claude', sessionId: 'A', terminalHandle: 'term-A'}});
    const refresh = () => runtime.applyAgentPreset({ host: 'codex', conversationMode: 'inbox-only', topic: 'refresh.trigger' });
    const ingest = async (id: string) => {
      if (source === 'refresh') {
        await refresh();
        runtime.mailbox.enqueue([alert(id)], { mailboxId });
      } else {
        const plugin = createOrcaWaveExecutorPlugin({ store: runtime.mailbox, resolveTopicBinding: () => binding, commandRunner: runner });
        await plugin.execute([alert(id)], { networkId: 'team', nodeId: 'receiver', traceId: id }, {});
      }
    };
    let service = new WakeupRedriveService({ store: runtime.mailbox, drivers: createTestWakeupDrivers({ commandRunner: runner })});
    t.after(() => service.stop());
    const initialClaim = service.claim(mailboxId);
    try {
      await ingest('wave-one');
      assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.driverConfig?.terminalHandle, undefined);
      assert.equal(sessionBinding(runtime.mailbox.getWakeTarget(mailboxId)).paneKey, undefined);
      assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.completedWaves, 0);
    } finally {
      initialClaim.dispose();
    }
    assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, true);
    assert.equal(sendsTo(state.calls, 'term-A').length, 0);
    const firstCreate = state.calls.find(call => call.args[1] === 'create')!;
    assert.equal(firstCreate.args[firstCreate.args.indexOf('--command') + 1], "claude --dangerously-skip-permissions --resume 'B'");
    const firstLease = runtime.mailbox.check({ mailboxId });
    runtime.mailbox.ack(firstLease.deliveryId!);
    await ingest('wave-two');
    assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, true);
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.driverConfig?.sessionId, undefined);
    assert.equal(sessionBinding(runtime.mailbox.getWakeTarget(mailboxId)).configuredSessionId, 'B');
    runtime.mailbox.check({ mailboxId });
    await service.stop();
    await runtime.stop();
    runtime = await new DealerNodeRuntime(options).start();
    await runtime.wakeups.stop();
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.driverConfig?.sessionId, undefined, 'restart must not restore retired B');
    assert.equal(sessionBinding(runtime.mailbox.getWakeTarget(mailboxId)).configuredSessionId, 'B');
    service = new WakeupRedriveService({ store: runtime.mailbox, drivers: createTestWakeupDrivers({ commandRunner: runner })});
    state.terminals = state.terminals.filter(terminal => terminal.handle === 'term-A');
    await ingest('recovery-mail');
    assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.driverConfig?.sessionId, undefined, 'ingestion must not restore retired B');
    assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, true);
    const creates = state.calls.filter(call => call.args[1] === 'create');
    assert.equal(creates.length, 3);
    for (const create of creates.slice(1)) assert.equal(create.args[create.args.indexOf('--command') + 1], 'claude --dangerously-skip-permissions');
    assert.equal(sendsTo(state.calls, 'term-A').length, 0);
  });
}

for (const disappearsLate of [false, true]) {
  for (const agent of ['antigravity', undefined]) {
    test(`pane-only isolation from ${agent ?? 'shell'} when pane disappears ${disappearsLate ? 'during selection' : 'before flush'}`, async t => {
      const root = directory(t);
      const workspacePath = join(root, 'pane-only');
      const store = new MailboxStore(join(root, 'mailbox.json'));
      const { runner, state } = fakeOrcaWorkspace(workspacePath, [
        { handle: 'term-sibling', connected: true, writable: true, agentIdentity: agent, preview: '' },
        ...(disappearsLate ? [{ handle: 'term-bound', connected: true, writable: true, agentIdentity: 'antigravity', preview: '', tabId: 'gone', leafId: 'pane' }] : []),
      ]);
      let lists = 0;
      const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: async request => {
        if (request.args[0] === 'terminal' && request.args[1] === 'list' && ++lists === 3 && disappearsLate) {
          state.terminals = state.terminals.filter(terminal => terminal.handle !== 'term-bound');
        }
        return runner(request);
      } })});
      t.after(() => service.stop());
      store.enqueue([alert('pane-only')], { mailboxId: 'worktree:pane-only', wakeTarget: { driver: 'orca-terminal', coalesceMs: 0, driverConfig: {...{ paneKey: 'gone:pane' }, workspacePath}} });
      assert.equal((await service.flushMailbox('worktree:pane-only', { force: true })).woke, true);
      assert.equal(state.created, 1);
      assert.equal(sendsTo(state.calls, 'term-sibling').length, 0);
      assert.equal(sessionBinding(store.getWakeTarget('worktree:pane-only')).paneKey, undefined);
    });
  }
}

for (const [agent, command, suffix] of [
  ['claude', 'claude --dangerously-skip-permissions --model opus', " --resume 'A'"],
  ['antigravity', 'agy --dangerously-skip-permissions --model gemini-2.5-pro', " --conversation 'A'"],
  ['codex', 'codex --dangerously-bypass-approvals-and-sandbox --model gpt-5', " resume 'A'"],
  ['claude', 'custom-wrapper --session {sessionId}', 'template'],
  ['claude', 'custom-wrapper --flag', 'reject'],
  ['claude', 'wrapper {sessionId}\nother', 'reject'],
  ['claude', 'claude --dangerously-skip-permissions --model opus # worker', 'reject'],
  ['claude', 'custom-wrapper --session {sessionId} # comment', 'reject'],
  ['claude', 'custom-wrapper --session {sessionId}; other', 'reject'],
] as const) {
  test(`custom continuation through wakeup: ${command}`, async t => {
    const root = directory(t);
    const workspacePath = join(root, 'custom');
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const { runner, state } = fakeOrcaWorkspace(workspacePath);
    const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner })});
    t.after(() => service.stop());
    store.enqueue([alert('custom')], { mailboxId: 'worktree:custom', wakeTarget: {
      driver: 'orca-terminal', coalesceMs: 0, driverConfig: { workspacePath, agent, sessionId: 'A',
      interactiveCommand: command.includes('{sessionId}') ? 'custom-wrapper' : command,...{ resumeCommand: command.includes('{sessionId}') ? command : undefined }}} });
    const result = await service.flushMailbox('worktree:custom', { force: true });
    const target = store.getWakeTarget('worktree:custom')!;
    if (suffix === 'reject') {
      assert.equal(result.woke, false);
      assert.equal(state.created, 0);
      assert.equal(terminalSends(state.calls).length, 0);
      assert.equal(store.list({ mailboxId: 'worktree:custom' }).queuedCount, 1);
      assert.equal(target.pendingWake, true);
      assert.equal(target.deliveryInDoubt, undefined);
      assert.match(target.lastWakeError!, /Unsupported Orca session continuation command/);
    } else {
      assert.equal(result.woke, true);
      const create = state.calls.find(call => call.args[1] === 'create')!;
      assert.equal(create.args[create.args.indexOf('--command') + 1], suffix === 'template' ? "custom-wrapper --session 'A'" : command + suffix);
      assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
      assert.equal(target.driverConfig?.sessionId, 'A');
    }
  });
}

test('session transition owner retires configuration and invalidates incompatible caller identities', t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const existing = store.upsertWakeTarget('stream:identity', {
    driver: 'orca-terminal', driverConfig: { workspacePath: '/workspace', agent: 'claude', terminalHandle: 'term-A',...{ paneKey: 'pane:A', configuredSessionId: 'A' }, sessionId: 'A'}});
  const input = { workspacePath: '/workspace', hasTopicBinding: true, authoritativeConfig: true };
  const removed = resolveConfiguredOrcaWakeTargetPatch(existing, input);
  assert.equal(sessionBinding(removed).configuredSessionId, undefined);
  assert.equal(removed.driverConfig?.terminalHandle, undefined);
  assert.equal(removed.driverConfig?.sessionId, undefined);
  const rotated = { ...existing, driverConfig: { ...existing.driverConfig, terminalHandle: 'term-fresh', sessionId: undefined, paneKey: undefined } };
  const removeAfterRotation = resolveConfiguredOrcaWakeTargetPatch(rotated, input);
  assert.equal(removeAfterRotation.driverConfig?.terminalHandle, 'term-fresh');
  assert.equal(sessionBinding(removeAfterRotation).configuredSessionId, undefined);
  const changedTarget = resolveConfiguredOrcaWakeTargetPatch(rotated, { ...input, workspacePath: '/other', configuredSessionId: 'A' });
  assert.equal(changedTarget.driverConfig?.terminalHandle, undefined);
  assert.equal(changedTarget.driverConfig?.sessionId, 'A');
  assert.equal(changedTarget.driverConfig?.agent, undefined);
  const unchanged = resolveConfiguredOrcaWakeTargetPatch(rotated, { ...input, configuredSessionId: 'A' });
  assert.equal(unchanged.driverConfig?.sessionId, undefined);
  assert.equal(unchanged.driverConfig?.terminalHandle, 'term-fresh');
  const newlyConfigured = resolveConfiguredOrcaWakeTargetPatch({ ...existing, driverConfig: { ...existing.driverConfig, configuredSessionId: undefined } }, { ...input, configuredSessionId: 'A' });
  assert.equal(newlyConfigured.driverConfig?.terminalHandle, 'term-A');
  assert.equal(sessionBinding(newlyConfigured).paneKey, 'pane:A');
  for (const caller of [{ sessionId: 'B' }, { workspacePath: '/other' }, { agent: 'codex' as const }]) {
    const patch = resolveCallerOrcaWakeTargetPatch(existing, caller);
    assert.ok(Object.hasOwn(patch.driverConfig!, 'terminalHandle'));
    assert.equal(patch.driverConfig?.terminalHandle, undefined);
    assert.equal(sessionBinding(patch).paneKey, undefined);
  }
  const run = resolveConfiguredOrcaWakeTargetPatch(existing, { ...input, runId: 'run' });
  assert.equal(run.driver, 'orca-run');
  assert.equal(run.driverConfig?.sessionId, undefined);
  assert.equal(sessionBinding(run).configuredSessionId, undefined);
});

test('custom continuation rejects ambiguous syntax, quoted placeholders and conflicting host modes', () => {
  for (const interactiveCommand of [
    "wrapper '{sessionId}'", 'wrapper "{sessionId}"', 'wrapper --session={sessionId}',
    'wrapper \\{sessionId}', 'wrapper {sessionId}suffix', 'wrapper {sessionId} "unclosed',
    'claude --resume old', 'claude --resume=old', 'claude -r old', 'claude --session-id old',
    'claude --print', 'claude -p', 'other --flag', 'claude; echo bad', 'claude | cat',
    'claude $(bad)', 'claude "$(bad)"', 'claude > /tmp/output', 'claude `bad`', 'claude "unterminated',
  ]) {
    assert.throws(() => buildOrcaAgentLaunchCommand({ agent: 'claude', sessionId: 'A', interactiveCommand }), OrcaTerminalLaunchRejectedError, interactiveCommand);
  }
  for (const [agent, interactiveCommand] of [
    ['antigravity', 'agy run'], ['antigravity', 'agy --conversation=A'],
    ['codex', 'codex exec'], ['codex', 'codex resume A'], ['codex', 'codex --'], ['codex', 'codex mcp'],
  ] as const) assert.throws(() => buildOrcaAgentLaunchCommand({ agent, interactiveCommand, sessionId: 'A' }), OrcaTerminalLaunchRejectedError);
  for (const [agent, flags] of [
    ['antigravity', ['-c', '--continue', '-p', '--print', '--prompt', '--']],
    ['claude', ['-c', '--continue', '--bg', '--background', '--']],
  ] as const) {
    for (const flag of flags) assert.throws(() => buildOrcaAgentLaunchCommand({
      agent, sessionId: 'A', interactiveCommand: `${agent === 'antigravity' ? 'agy' : agent} ${flag}`,
    }), OrcaTerminalLaunchRejectedError);
  }
  assert.throws(() => buildOrcaAgentLaunchCommand({ interactiveCommand: 'wrapper {sessionId}' }), /fresh-session command/);
  assert.equal(buildOrcaAgentLaunchCommand({ agent: 'claude', interactiveCommand: '"/opt/claude" --model "opus"', sessionId: 'A' }), "/opt/claude --model opus --resume 'A'");
  assert.equal(buildOrcaAgentLaunchCommand({ interactiveCommand: 'wrapper', resumeCommand: 'wrapper {sessionId} {sessionId}', sessionId: "a'b" }), "wrapper 'a'\\''b' 'a'\\''b'");
  assert.equal(buildOrcaAgentLaunchCommand({ agent: 'claude', interactiveCommand: 'claude --model \"\"', sessionId: 'A' }), "claude --model '' --resume 'A'");
});

for (const source of ['refresh', 'wave'] as const) {
  test(`${source} commits mail before rejecting an unsupported configured continuation`, async t => {
    const root = directory(t);
    const stateDir = join(root, 'state');
    const workspacePath = join(root, 'workspace');
    const mailboxId = 'stream:unsupported';
    const { runner, state } = fakeOrcaWorkspace(workspacePath);
    new MailboxStore(join(stateDir, 'mailbox.json')).upsertWakeTarget(mailboxId, { driver: 'orca-terminal', driverConfig: {...{ configuredSessionId: 'A' }, workspacePath, sessionId: 'A', agent: 'claude',
      interactiveCommand: 'claude --dangerously-skip-permissions'}});
    const binding = { executor: 'orca-wave', mailboxId, workspacePath, sessionId: 'A', agent: 'claude',
      interactiveCommand: 'custom-wrapper --flag',
    };
    const runtime = await new DealerNodeRuntime({
      stateDir, member: 'worker', name: 'worker', profile: 'headless-relay',
      transport: { discoveryDir: join(root, 'mesh') }, commandRunner: runner,
      patch: { defaultExecutor: 'inbox', executors: ['orca-wave', 'inbox'], topicBindings: {
        unsupported: { ...binding, interactiveCommand: source === 'refresh' ? binding.interactiveCommand : 'claude --dangerously-skip-permissions' },
      } },
    }).start();
    t.after(() => runtime.stop());
    await runtime.wakeups.stop();
    const service = new WakeupRedriveService({ store: runtime.mailbox, drivers: createTestWakeupDrivers({ commandRunner: runner })});
    t.after(() => service.stop());
    if (source === 'wave') {
      const plugin = createOrcaWaveExecutorPlugin({ store: runtime.mailbox, resolveTopicBinding: () => binding, commandRunner: runner });
      await plugin.execute([alert('unsupported')], { networkId: 'team', nodeId: 'receiver', traceId: 'unsupported' }, {});
    } else runtime.mailbox.enqueue([alert('unsupported')], { mailboxId });
    assert.equal(runtime.mailbox.list({ mailboxId }).queuedCount, 1);
    assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, false);
    const target = runtime.mailbox.getWakeTarget(mailboxId)!;
    assert.equal(target.pendingWake, true);
    assert.equal(target.deliveryInDoubt, undefined);
    assert.match(target.lastWakeError!, /Unsupported Orca session continuation command/);
    assert.equal(runtime.mailbox.list({ mailboxId }).queuedCount, 1);
    assert.equal(state.created, 0);
    assert.equal(terminalSends(state.calls).length, 0);
  });
}

for (const source of ['refresh', 'wave'] as const) {
  for (const initial of ['active', 'retired', 'established'] as const) {
    test(`${source} handles ${initial} session provenance`, async t => {
      const baseline = initial === 'retired';
      const established = initial === 'established';
      const root = directory(t);
      const stateDir = join(root, 'state');
      const workspacePath = join(root, 'workspace');
      const mailboxId = 'stream:provenance';
      const originalHandle = baseline ? 'term-rotated' : established ? 'term-existing' : 'term-A';
      const { runner, state } = fakeOrcaWorkspace(workspacePath, [
        { handle: originalHandle, connected: true, writable: true, agentIdentity: 'claude', preview: '' },
      ]);
      new MailboxStore(join(stateDir, 'mailbox.json')).upsertWakeTarget(mailboxId, {
        driver: 'orca-terminal',
        ...(baseline || established ? { rotateAfterWaves: 1, completedWaves: 0 } : {}),
        driverConfig: { workspacePath, terminalHandle: originalHandle, agent: 'claude',
          ...(baseline ? { retiredSessionId: 'B' } : established ? {} : {
            paneKey: 'pane:A', configuredSessionId: 'A', configuredTerminalHandle: 'term-A', sessionId: 'A',
          }),
        },
      });
      const binding = { executor: 'orca-wave', mailboxId, workspacePath, agent: 'claude', sessionId: 'B',
        ...(baseline || established ? { rotateAfterWaves: 1 } : { terminalHandle: 'term-A' }),
      };
      const runtime = await new DealerNodeRuntime({ stateDir, member: 'worker', name: 'worker', profile: 'headless-relay',
        transport: { discoveryDir: join(root, 'mesh') }, commandRunner: runner,
        patch: { defaultExecutor: 'inbox', executors: ['orca-wave', 'inbox'], topicBindings: source === 'refresh' ? { provenance: binding } : {} },
      }).start();
      t.after(() => runtime.stop());
      await runtime.wakeups.stop();
      const service = new WakeupRedriveService({ store: runtime.mailbox, drivers: createTestWakeupDrivers({ commandRunner: runner })});
      t.after(() => service.stop());
      const ingest = async (id: string) => {
        if (source === 'refresh') {
          await runtime.applyAgentPreset({ host: 'codex', conversationMode: 'inbox-only', topic: 'refresh.trigger' });
          runtime.mailbox.enqueue([alert(id)], { mailboxId });
        } else {
          const plugin = createOrcaWaveExecutorPlugin({ store: runtime.mailbox, commandRunner: runner, resolveTopicBinding: () => binding });
          await plugin.execute([alert(id)], { networkId: 'team', nodeId: 'receiver', traceId: id }, {});
        }
      };
      const initialClaim = service.claim(mailboxId);
      try {
        await ingest('first');
        assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.driverConfig?.terminalHandle, baseline ? 'term-rotated' : undefined);
        assert.equal(runtime.mailbox.getWakeTarget(mailboxId)!.driverConfig?.sessionId, baseline ? undefined : 'B');
      } finally {
        initialClaim.dispose();
      }
      assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, true);
      assert.equal(state.created, baseline ? 0 : 1);
      if (baseline) {
        assert.equal(sendsTo(state.calls, 'term-rotated').length, 1);
        state.terminals = [];
      } else assert.equal(sendsTo(state.calls, originalHandle).length, 0);
      await ingest('second');
      assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, true);
      assert.equal(state.created, 1);
      const create = state.calls.find(call => call.args[1] === 'create')!;
      assert.equal(create.args[create.args.indexOf('--command') + 1], baseline
        ? 'claude --dangerously-skip-permissions' : "claude --dangerously-skip-permissions --resume 'B'");
      const target = runtime.mailbox.getWakeTarget(mailboxId)!;
      assert.equal(target.driverConfig?.terminalHandle, 'term-new-1');
      assert.equal(sessionBinding(target).configuredSessionId, 'B');
      if (!baseline) {
        assert.equal(sessionBinding(target).configuredTerminalHandle, established ? undefined : 'term-A');
        assert.equal(sendsTo(state.calls, originalHandle).length, 0);
        assert.equal(sendsTo(state.calls, 'term-new-1').length, 2);
      } else {
        const changed = resolveConfiguredOrcaWakeTargetPatch(target, { workspacePath, configuredSessionId: 'C', hasTopicBinding: true, authoritativeConfig: true });
        assert.equal(changed.driverConfig?.sessionId, 'C');
        assert.equal(changed.driverConfig?.terminalHandle, undefined);
      }
    });
  }
}

test('wrapper continuation rotates and recovers with its receiver-owned fresh command', async t => {
  const root = directory(t);
  const workspacePath = join(root, 'wrapper');
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const { runner, state } = fakeOrcaWorkspace(workspacePath);
  const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner })});
  t.after(() => service.stop());
  const mailboxId = 'stream:wrapper';
  store.enqueue([alert('wrapper-one')], { mailboxId, wakeTarget: {
    driver: 'orca-terminal', rotateAfterWaves: 1, coalesceMs: 0, driverConfig: { workspacePath, agent: 'claude', sessionId: 'S1',...{ resumeCommand: 'custom-wrapper --auto --session {sessionId}' }, interactiveCommand: 'custom-wrapper --auto'}} });
  assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, true);
  const lease = store.check({ mailboxId });
  store.ack(lease.deliveryId!);
  store.enqueue([alert('wrapper-two')], { mailboxId });
  const second = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(second.woke, true);
  assert.equal(second.rotated, true);
  assert.equal(store.getWakeTarget(mailboxId)!.driverConfig?.sessionId, undefined);
  assert.equal(sessionBinding(store.getWakeTarget(mailboxId)).retiredSessionId, 'S1');
  state.terminals = [];
  store.enqueue([alert('wrapper-three')], { mailboxId });
  assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, true);
  const commands = state.calls.filter(call => call.args[1] === 'create').map(call => call.args[call.args.indexOf('--command') + 1]);
  assert.deepEqual(commands, ["custom-wrapper --auto --session 'S1'", 'custom-wrapper --auto', 'custom-wrapper --auto']);
  assert.equal(terminalSends(state.calls).length, 3);
});

for (const variant of ['host-change', 'wrapper-host-change', 'same-host'] as const) {
  test(`caller binding atomically reconciles launch config: ${variant}`, async t => {
    const root = directory(t);
    const workspacePath = join(root, 'caller');
    const { runner, state } = fakeOrcaWorkspace(workspacePath);
    const runtime = await makeNode(t, root, 'caller', { commandRunner: runner });
    await runtime.wakeups.stop();
    const mailboxId = 'worktree:caller';
    const command = variant === 'wrapper-host-change' ? 'claude-wrapper' : 'claude --dangerously-skip-permissions --model opus';
    runtime.mailbox.upsertWakeTarget(mailboxId, {
      driver: 'orca-terminal',
      completedWaves: 1, rotateAfterWaves: 1,
       driverConfig: { workspacePath, terminalHandle: 'term-original', agent: 'claude', sessionId: 'claude-1', interactiveCommand: command}});
    const code = await runDealerCli(['inbox', 'check', '--json'], { runtime,
      env: { ORCA_WORKTREE_ID: `repo::${workspacePath}`, ORCA_TERMINAL_HANDLE: 'term-original',
        ...(variant === 'same-host' ? { CLAUDE_CODE_SESSION_ID: 'claude-2' } : { CODEX_THREAD_ID: 'codex-next' }),
      }, stdout: () => {}, stderr: line => assert.fail(line),
    });
    assert.equal(code, 0);
    const target = runtime.mailbox.getWakeTarget(mailboxId)!;
    assert.equal(target.completedWaves, 0);
    assert.equal(target.driverConfig?.interactiveCommand, variant === 'same-host' ? command : undefined);
    assert.equal(sessionBinding(target).resumeCommand, undefined);
    const service = new WakeupRedriveService({ store: runtime.mailbox, drivers: createTestWakeupDrivers({ commandRunner: runner })});
    t.after(() => service.stop());
    runtime.mailbox.enqueue([alert('host-recovery')], { mailboxId });
    assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, true);
    const create = state.calls.find(call => call.args[1] === 'create')!;
    assert.equal(create.args[create.args.indexOf('--command') + 1], variant === 'same-host'
      ? "claude --dangerously-skip-permissions --model opus --resume 'claude-2'"
      : "codex --dangerously-bypass-approvals-and-sandbox resume 'codex-next'");
    assert.equal(sendsTo(state.calls, 'term-new-1').length, 1);
  });
}

test('continuation templates require a matching fresh-session launcher before any launch', () => {
  for (const sessionId of [undefined, 'A']) {
    assert.throws(() => buildOrcaAgentLaunchCommand({ interactiveCommand: 'wrapper {sessionId}', sessionId }), /fresh-session command/);
    assert.throws(() => buildOrcaAgentLaunchCommand({ resumeCommand: 'wrapper --session {sessionId}', sessionId }), /matching fresh-session/);
    assert.throws(() => buildOrcaAgentLaunchCommand({ interactiveCommand: 'other-wrapper', resumeCommand: 'wrapper --session {sessionId}', sessionId }), /matching fresh-session/);
    assert.throws(() => buildOrcaAgentLaunchCommand({ agent: 'claude', interactiveCommand: 'codex', resumeCommand: 'claude --resume {sessionId}', sessionId }), /does not match target agent/);
    assert.throws(() => buildOrcaAgentLaunchCommand({ resumeCommand: 'wrapper --flag', sessionId }), /standalone/);
  }
});

test('known-host fresh commands reject embedded continuation in every launch mode', () => {
  for (const [agent, interactiveCommand, resumeCommand] of [
    ['claude', 'claude --resume old-session', 'claude --resume {sessionId}'],
    ['antigravity', 'agy --conversation old-session', 'agy --conversation {sessionId}'],
    ['codex', 'codex resume old-session', 'codex resume {sessionId}'],
    ['claude', 'claude -p hi', 'claude --resume {sessionId}'],
  ] as const) {
    for (const sessionId of [undefined, 'A']) {
      for (const template of [undefined, resumeCommand]) {
        assert.throws(() => buildOrcaAgentLaunchCommand({
          agent, interactiveCommand, sessionId, resumeCommand: template,
        }), OrcaTerminalLaunchRejectedError);
      }
    }
  }
});

for (const change of ['configuration', 'caller'] as const) {
  test(`in-flight ${change === 'configuration' ? 'rotation' : 'recovery'} cannot overwrite a newer ${change} binding`, async t => {
    const root = directory(t);
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const mailboxId = 'stream:cas';
    const { runner, state } = fakeOrcaWorkspace(root);
    store.enqueue([alert('cas')], { mailboxId, wakeTarget: {
      driver: 'orca-terminal', coalesceMs: 0,
      ...(change === 'configuration' ? { rotateAfterWaves: 1, completedWaves: 1 } : {}), driverConfig: { workspacePath: root, agent: 'claude', sessionId: 'A',
      terminalHandle: 'term-A',...{ paneKey: 'pane:A', configuredSessionId: 'A' }}} });
    let entered!: () => void;
    let release!: () => void;
    const creating = new Promise<void>(resolve => { entered = resolve; });
    const held = new Promise<void>(resolve => { release = resolve; });
    let intercepted = false;
    const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: async request => {
      if (!intercepted && request.args[0] === 'terminal' && request.args[1] === 'create') {
        intercepted = true;
        entered();
        await held;
      }
      return runner(request);
    } })});
    t.after(() => service.stop());
    const pending = service.flushMailbox(mailboxId, { force: true });
    await creating;
    const current = store.getWakeTarget(mailboxId)!;
    if (change === 'configuration') {
      store.upsertWakeTarget(mailboxId, resolveConfiguredOrcaWakeTargetPatch(current, {
        mailboxId, configuredAgent: 'claude',
        hasTopicBinding: true, authoritativeConfig: true, workspacePath: root, configuredSessionId: 'B'}));
    } else {
      state.terminals.push({ handle: 'term-caller', connected: true, writable: true, agentIdentity: 'claude', preview: '' });
      store.upsertWakeTarget(mailboxId, resolveCallerOrcaWakeTargetPatch(current, {
        terminalHandle: 'term-caller', sessionId: 'caller-session', agent: 'claude'}));
    }
    release();
    const stale = await pending;
    assert.equal(stale.reason, 'Wake launch superseded by a newer binding');
    const rebound = store.getWakeTarget(mailboxId)!;
    assert.equal(rebound.driverConfig?.sessionId, change === 'configuration' ? 'B' : 'caller-session');
    assert.equal(sessionBinding(rebound).retiredSessionId, undefined);
    assert.notEqual(rebound.driverConfig?.terminalHandle, 'term-new-1');
    assert.equal(sendsTo(state.calls, 'term-new-1').length, 0);
    assert.equal(rebound.deliveryInDoubt, undefined);
    assert.equal(rebound.earliestWakeAt, 0);
    const destination = change === 'configuration' ? 'term-new-2' : 'term-caller';
    await until(() => sendsTo(state.calls, destination).length === 1, 'superseding binding redrives immediately');
    assert.equal(sendsTo(state.calls, destination).length, 1);
    assert.equal(sendsTo(state.calls, 'term-new-1').length, 0);
    if (change === 'configuration') {
      const commands = state.calls.filter(call => call.args[1] === 'create');
      assert.equal(commands[1].args[commands[1].args.indexOf('--command') + 1], "claude --dangerously-skip-permissions --resume 'B'");
    }
  });
}

test('generic adapter state drives binding CAS without retry metadata or key-order churn', t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const driverConfig = { pane: 'one', session: 'two' };
  const initial = store.upsertWakeTarget('stream:revision', { driverConfig: {...driverConfig}});
  driverConfig.pane = 'mutated';
  assert.equal(store.getWakeTarget('stream:revision')!.driverConfig!.pane, 'one');
  const unchanged = store.upsertWakeTarget('stream:revision', { attemptCount: 4, driverConfig: {...{ session: 'two', pane: 'one' }}});
  assert.equal(unchanged.bindingRevision, initial.bindingRevision);
  const changed = store.upsertWakeTarget('stream:revision', { driverConfig: {...{ pane: 'new' }}});
  assert.ok(changed.bindingRevision! > initial.bindingRevision!);
  assert.equal(store.beginWakeDelivery('stream:revision', initial.bindingRevision), undefined);
  let built = false;
  const result = store.commitWakeLaunch('stream:revision', initial.bindingRevision!, () => { built = true; return {}; });
  assert.equal(built, false);
  assert.equal(result.committed, false);
  assert.deepEqual(result.target.driverConfig, { pane: 'new', session: 'two' });
  assert.equal(result.target.deliveryInDoubt, undefined);
});

for (const path of ['fresh-busy', 'redrive-send', 'spawned-busy', 'spawned-send'] as const) {
  test(`post-await ${path} settlement preserves a newer caller binding`, async t => {
    const root = directory(t);
    const mailboxId = 'stream:post-await';
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const fresh = path === 'fresh-busy';
    const busy = path.endsWith('busy');
    const spawned = path.startsWith('spawned');
    const { runner, state } = fakeOrcaWorkspace(root, fresh ? [] : [{
      handle: 'term-A', connected: true, writable: true, agentIdentity: 'claude', preview: '',
    }]);
    store.enqueue([alert('post-await')], { mailboxId, wakeTarget: {
      driver: 'orca-terminal', coalesceMs: 0, driverConfig: { workspacePath: root, terminalHandle: 'term-A',
      agent: 'claude', sessionId: 'A'}} });
    let entered!: () => void;
    let release!: () => void;
    const suspended = new Promise<void>(resolve => { entered = resolve; });
    const held = new Promise<void>(resolve => { release = resolve; });
    let intercepted = false;
    const controlledRunner: CommandRunner = async request => {
      if (!intercepted && request.args[0] === 'terminal' && request.args[1] === (busy ? 'wait' : 'send')) {
        intercepted = true;
        entered();
        await held;
        if (busy) return { stdout: '{"ok":false}' };
      }
      return runner(request);
    };
    const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: controlledRunner })});
    t.after(() => service.stop());
    const pending = spawned
      ? deliverSpawnedOrcaTerminalWakeup(store, controlledRunner, mailboxId)
      : service.flushMailbox(mailboxId, { force: true });
    await suspended;
    // Hold automatic redrive only while inspecting the atomic settlement result.
    const claim = service.claim(mailboxId);
    state.terminals.push({ handle: 'term-B', connected: true, writable: true, agentIdentity: 'claude', preview: '' });
    store.upsertWakeTarget(mailboxId, resolveCallerOrcaWakeTargetPatch(store.getWakeTarget(mailboxId), {
      terminalHandle: 'term-B', sessionId: 'B', paneKey: 'pane-B', agent: 'claude'}));
    release();
    const result = await pending;
    if (spawned) assert.equal((result as { sent: boolean }).sent, false);
    else assert.equal((result as { reason?: string }).reason, 'Wake launch superseded by a newer binding');
    const current = store.getWakeTarget(mailboxId)!;
    assert.equal(current.driverConfig?.terminalHandle, 'term-B');
    assert.equal(current.driverConfig?.sessionId, 'B');
    assert.equal(sessionBinding(current).paneKey, 'pane-B');
    assert.equal(current.pendingWake, true);
    assert.equal(current.attemptCount, 0);
    assert.equal(current.earliestWakeAt, 0);
    if (busy) assert.equal(terminalSends(state.calls).length, 0);
    claim.dispose();
    service.notifyMailbox(mailboxId);
    await until(() => sendsTo(state.calls, 'term-B').length === 1, 'new binding receives the pending pointer');
    assert.equal(store.getWakeTarget(mailboxId)!.driverConfig?.terminalHandle, 'term-B');
    assert.equal(sendsTo(state.calls, 'term-new-1').length, 0);
  });
}

test('passive host completion cannot install continuation over a newer binding', async t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const mailboxId = 'stream:passive-cas';
  store.enqueue([alert('passive-cas')], { mailboxId, wakeTarget: {
    driver: 'agent-host', coalesceMs: 0, driverConfig: { hostAgent: 'claude', conversationMode: 'fixed', sessionId: 'A'}} });
  let entered!: () => void;
  let release!: () => void;
  const suspended = new Promise<void>(resolve => { entered = resolve; });
  const held = new Promise<void>(resolve => { release = resolve; });
  const delivery = deliverAgentHostWakeTarget(store, async () => {
    entered();
    await held;
    return { stdout: '{}' };
  }, mailboxId);
  await suspended;
  store.upsertWakeTarget(mailboxId, { driverConfig: { sessionId: 'B' }});
  release();
  await assert.rejects(delivery, /superseded by a newer binding/);
  const target = store.getWakeTarget(mailboxId)!;
  assert.equal(target.driverConfig?.sessionId, 'B');
  assert.equal(target.pendingWake, true);
  assert.equal(target.deliveryInDoubt, undefined);
  assert.equal(target.attemptCount, 0);
});

test('stale workspace discovery and missing-workspace disarm preserve the replacement target', t => {
  const store = new MailboxStore(join(directory(t), 'mailbox.json'));
  const mailboxId = 'stream:workspace-cas';
  const original = store.upsertWakeTarget(mailboxId, {
    driver: 'orca-terminal', driverConfig: { workspacePath: '/old', terminalHandle: 'term-A', sessionId: 'A'}});
  const replacement = store.upsertWakeTarget(mailboxId, { pendingWake: true, driverConfig: {
    workspacePath: '/new', terminalHandle: 'term-B', sessionId: 'B'}});
  const discovered = store.updateWakeDriverConfigIfCurrent(mailboxId, original.bindingRevision!, { workspacePath: '/discovered-old' });
  assert.equal(discovered.superseded, true);
  assert.deepEqual(discovered.target, replacement);
  const disarmed = store.disarmWakeTargetIfCurrent(mailboxId, original.bindingRevision!);
  assert.equal(disarmed.superseded, true);
  assert.equal(disarmed.target.pendingWake, true);
  assert.equal(disarmed.target.driverConfig?.workspacePath, '/new');
  assert.equal(disarmed.target.driverConfig?.terminalHandle, 'term-B');
  const deferred = store.deferWakeAttempt(mailboxId, original.bindingRevision!, 'old discovery failed');
  assert.equal(deferred.superseded, true);
  assert.equal(deferred.target.attemptCount, 0);
  assert.equal(deferred.target.lastWakeError, undefined);
});

test('rotating wave pinning preserves session-owned identity and clears parent provenance', async t => {
  const root = directory(t);
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const mailboxId = 'stream:pinned-identity';
  const sessionMailbox = 'terminal:term-1';
  const { runner, state } = fakeOrcaWorkspace(root, [{
    handle: 'term-1', connected: true, writable: true, agentIdentity: 'claude', preview: '',
  }]);
  const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner }) });
  t.after(() => service.stop());
  store.enqueue([alert('pinned-first')], { mailboxId, wakeTarget: {
    driver: 'orca-terminal', rotateAfterWaves: 2, coalesceMs: 0,
    driverConfig: { workspacePath: root, terminalHandle: 'term-1', agent: 'claude',
      sessionId: 'parent-session', paneKey: 'parent:pane', configuredSessionId: 'parent-session',
      retiredTerminalHandle: 'term-retired' },
  } });
  assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, true);
  store.upsertWakeTarget(sessionMailbox, {
    driverConfig: { sessionId: 'conv-1', paneKey: 'pane-1', interactiveCommand: 'claude --model opus',
      configuredSessionId: 'obsolete', configuredTerminalHandle: 'obsolete-handle', retiredSessionId: 'obsolete',
      retiredTerminalHandle: 'obsolete-retirement' },
  });
  const assertPinnedIdentity = () => {
    const config = store.getWakeTarget(sessionMailbox)!.driverConfig!;
    assert.equal(config.sessionId, 'conv-1');
    assert.equal(config.paneKey, 'pane-1');
    assert.equal(config.interactiveCommand, 'claude --model opus');
    for (const key of ['configuredSessionId', 'configuredTerminalHandle', 'retiredSessionId', 'retiredTerminalHandle']) {
      assert.equal(Object.hasOwn(config, key), false, key);
    }
  };
  pinOrcaTerminalSessionMailbox(store, mailboxId, 'term-1');
  assertPinnedIdentity();
  store.ack(store.check({ mailboxId }).deliveryId!);
  // Exercise the completion path independently from explicit pinning.
  store.upsertWakeTarget(sessionMailbox, { driverConfig: {
    configuredSessionId: 'obsolete-again', retiredSessionId: 'obsolete-again',
  } });
  store.enqueue([alert('pinned-second')], { mailboxId });
  assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, true);
  assert.equal(sendsTo(state.calls, 'term-1').length, 2);
  assertPinnedIdentity();
  store.ack(store.check({ mailboxId }).deliveryId!);
  store.enqueue([alert('pinned-third')], { mailboxId });
  assert.equal((await service.flushMailbox(mailboxId, { force: true })).rotated, true);
  const newHandle = store.getWakeTarget(mailboxId)!.driverConfig!.terminalHandle as string;
  assert.notEqual(newHandle, 'term-1');
  store.upsertWakeTarget(mailboxId, resolveCallerOrcaWakeTargetPatch(store.getWakeTarget(mailboxId), {
    terminalHandle: newHandle, sessionId: 'conv-2', paneKey: 'pane-2', agent: 'claude',
  }));
  // A late command in the old tab cannot inherit the new wave's conversation.
  pinOrcaTerminalSessionMailbox(store, mailboxId, 'term-1');
  assertPinnedIdentity();
  pinOrcaTerminalSessionMailbox(store, mailboxId, 'term-unrecorded');
  const unrecorded = store.getWakeTarget('terminal:term-unrecorded')!.driverConfig!;
  assert.equal(unrecorded.sessionId, undefined);
  assert.equal(unrecorded.paneKey, undefined);
  assert.equal(unrecorded.workspacePath, root);
  assert.equal(unrecorded.terminalHandle, 'term-unrecorded');
});

for (const mailboxId of ['worktree:standalone', 'terminal:term-standalone']) {
  test(`standalone wave executor resolves ${mailboxId} without a redrive service`, async t => {
    const root = directory(t);
    const workspacePath = join(root, 'standalone');
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const calls: CommandRequest[] = [];
    const runner: CommandRunner = async request => {
      calls.push(request);
      if (request.args[0] === 'worktree') return { stdout: JSON.stringify({
        ok: true, result: { truncated: false, worktrees: [{ path: workspacePath, worktreeId: 'receiver-worktree' }] },
      }) };
      return { stdout: JSON.stringify({ ok: true, result: { terminal: { worktreePath: workspacePath } } }) };
    };
    const executor = createOrcaWaveExecutorPlugin({ store, commandRunner: runner });
    for (const initial of ['missing', 'passive', 'bound'] as const) {
      if (initial === 'passive') store.upsertWakeTarget(mailboxId, { driver: 'passive' });
      if (initial === 'bound') store.upsertWakeTarget(mailboxId, { driverConfig: { sessionId: 'preserved' } });
      const beforeCalls = calls.length;
      await executor.execute([alert(`standalone-${initial}`, {
        semantic: { type: 'message', threadId: `standalone-${initial}`, targetMailbox: mailboxId },
        payload: { workspacePath: '/sender/impostor' },
      })], { networkId: 'team', nodeId: 'receiver', traceId: 'standalone' }, {});
      const target = store.getWakeTarget(mailboxId)!;
      assert.equal(target.driver, 'orca-terminal');
      assert.equal(target.driverConfig!.workspacePath, workspacePath);
      if (initial === 'bound') {
        assert.equal(target.driverConfig!.sessionId, 'preserved');
        assert.equal(calls.length, beforeCalls);
      } else assert.ok(calls.length > beforeCalls);
    }
    assert.equal(store.list({ mailboxId }).queuedCount, 3);
  });
}

for (const kind of ['task', 'stream'] as const) {
  test(`generated ${kind} inbox commands capture only their owning worker and resume after sleep`, async t => {
    const root = directory(t);
    const workspacePath = join(root, 'worker');
    const fake = fakeOrcaWorkspace(workspacePath);
    const runner: CommandRunner = async request => request.args[0] === 'worktree' && request.args[1] === 'create'
      ? { stdout: JSON.stringify({ ok: true, result: { worktree: { path: workspacePath } } }) }
      : fake.runner(request);
    const runtime = await makeNode(t, root, 'explicit', { commandRunner: runner });
    await runtime.wakeups.stop();
    const store = runtime.mailbox;
    const service = new WakeupRedriveService({ store, drivers: createTestWakeupDrivers({ commandRunner: runner }) });
    t.after(() => service.stop());
    const mailboxId = `${kind}:generated-worker`;
    const binding = { executor: 'orca-wave', mailboxId, workspacePath, rotateAfterWaves: 1 };
    const wave = createOrcaWaveExecutorPlugin({ store, commandRunner: runner, resolveTopicBinding: () => binding });
    const first = alert('generated-worker', { semantic: { type: 'message', threadId: 'thread-wave-1' } });
    store.recordInboundEnvelope(first, mailboxId);
    if (kind === 'task') {
      await createOrcaSpawnExecutorPlugin({ store, commandRunner: runner, command: 'codex', cwd: workspacePath, interactive: true }).execute([first], {} as ExecutionContext);
    } else {
      await wave.execute([first], {} as ExecutionContext);
      assert.equal((await service.flushMailbox(mailboxId, { force: true })).woke, true);
    }
    const promptCommand = (id: string) => {
      const text = terminalSends(fake.state.calls).map(textOf).reverse().find(text => text.includes(`dealer inbox check --mailbox ${id} --json`));
      assert.ok(text, `missing generated command for ${id}`);
      return text.match(/`(dealer inbox check --mailbox [^`]+)`/)![1].split(' ').slice(1);
    };
    const claim = service.claim(mailboxId);
    const command = promptCommand(mailboxId);
    const handle = store.getWakeTarget(mailboxId)!.driverConfig!.terminalHandle as string;
    const env = { ORCA_WORKTREE_ID: `repo::${workspacePath}`, ORCA_TERMINAL_HANDLE: handle,
      ORCA_PANE_KEY: 'worker:pane', [kind === 'task' ? 'CODEX_THREAD_ID' : 'CLAUDE_CODE_SESSION_ID']: 'worker-session' };
    const invoke = async (args: string[], caller = env, expected = 0) => {
      const out: string[] = [], err: string[] = [];
      const code = await runDealerCli(args, { runtime, env: caller, stdout: line => out.push(line), stderr: line => err.push(line) });
      assert.equal(code, expected, err.join('\n'));
      return code === 0 ? JSON.parse(out.join('\n')) : undefined;
    };
    const before = store.getWakeTarget(mailboxId);
    const foreign = { ...env, ORCA_TERMINAL_HANDLE: 'term-foreign', ORCA_PANE_KEY: 'foreign:pane' };
    await invoke(['inbox', 'list', '--mailbox', mailboxId, '--json']);
    await invoke([...command, '--peek']);
    await invoke([...command, '--timeout-ms', '1'], env, 1);
    assert.deepEqual(store.getWakeTarget(mailboxId), before);
    const foreignLease = await invoke(command, foreign);
    assert.deepEqual(store.getWakeTarget(mailboxId), before);
    await invoke(['inbox', 'ack', foreignLease.deliveryId, '--json'], foreign);
    assert.deepEqual(store.getWakeTarget(mailboxId)!.driverConfig, before!.driverConfig);
    assert.equal(store.getWakeTarget('terminal:term-foreign'), undefined);
    assert.equal(store.getWakeTarget('worktree:worker'), undefined);
    // Requeue after the foreign lease was legitimately acknowledged; identity was not trusted.
    store.enqueue([alert('worker-consume')], { mailboxId });
    const lease = await invoke(command);
    assert.equal(store.getWakeTarget(mailboxId)!.driverConfig!.sessionId, 'worker-session');
    await invoke(['inbox', 'ack', lease.deliveryId, ...(kind === 'stream' ? ['--mailbox', mailboxId] : []), '--json']);
    const pinned = `terminal:${handle}`;
    if (kind === 'stream') {
      assert.equal(store.getWakeTarget(pinned)!.driverConfig!.sessionId, 'worker-session');
      assert.equal(store.getWakeTarget(pinned)!.driverConfig!.paneKey, 'worker:pane');
      await wave.execute([alert('wave-two', { semantic: { type: 'message', threadId: 'thread-wave-2' } })], {} as ExecutionContext);
      claim.dispose();
      const rotatedResult = await service.flushMailbox(mailboxId, { force: true });
      assert.equal(rotatedResult.rotated, true, JSON.stringify(rotatedResult));
      const create = fake.state.calls.filter(c => c.args[1] === 'create').at(-1)!;
      assert.equal(create.args[create.args.indexOf('--command') + 1], 'claude --dangerously-skip-permissions');
    } else {
      await invoke(command, { ...env, ORCA_TERMINAL_HANDLE: 'term-restored' });
      assert.equal(store.getWakeTarget(mailboxId)!.driverConfig!.terminalHandle, 'term-restored');
    }
    fake.state.terminals = [];
    const recoveryMailbox = kind === 'stream' ? pinned : mailboxId;
    if (kind === 'stream') {
      await wave.execute([alert('old-thread-followup', { semantic: { type: 'message', threadId: 'thread-wave-1' } })], {} as ExecutionContext);
      assert.equal(store.list({ mailboxId: pinned }).queuedCount, 1);
    } else store.enqueue([alert('task-followup')], { mailboxId });
    claim.dispose();
    const recoveryResult = await service.flushMailbox(recoveryMailbox, { force: true });
    assert.equal(recoveryResult.woke, true, JSON.stringify(recoveryResult));
    const create = fake.state.calls.filter(c => c.args[1] === 'create').at(-1)!;
    assert.equal(create.args[create.args.indexOf('--command') + 1], kind === 'task'
      ? "codex --dangerously-bypass-approvals-and-sandbox resume 'worker-session'"
      : "claude --dangerously-skip-permissions --resume 'worker-session'");
    const resumedHandle = store.getWakeTarget(recoveryMailbox)!.driverConfig!.terminalHandle as string;
    await invoke(promptCommand(recoveryMailbox), { ...env, ORCA_TERMINAL_HANDLE: resumedHandle, ORCA_PANE_KEY: 'resumed:pane' });
    assert.equal(store.getWakeTarget(recoveryMailbox)!.driverConfig!.paneKey, 'resumed:pane');
    await runtime.stop();
  });
}
