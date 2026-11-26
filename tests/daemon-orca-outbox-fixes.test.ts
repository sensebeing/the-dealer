import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { EventEmitter } from 'node:events';
import {
  DeliveryTimeoutError,
  PeerUnreachableError,
  generateNodeKeyPair,
  hasProvenUnattemptedFailure,
  isUnattemptedOutboxItem,
  markBeforeSendError,
  signEnvelope,
  signNodeCard,
  signReceipt,
} from '../packages/core-types/index.ts';
import type {
  MessageEnvelope,
  PeerReachableEvent,
  TransportService,
} from '../packages/core-types/index.ts';
import { Kernel } from '../packages/kernel/index.ts';
import { createNodePlugin } from '../packages/node/index.ts';
import { createRouterPlugin } from '../packages/router/index.ts';
import { createEmitterPlugin } from '../packages/emitter/index.ts';
import {
  MailboxStore,
  OutboxRedriveService,
  OutboxStore,
  WakeupRedriveService,
  attemptDelayMs,
} from '../packages/mailbox/index.ts';
import {
  OrcaWorktreeBindingMismatchError,
  OrcaWorktreeSelectorAmbiguousError,
  buildOrcaWorktreeSelector,
  checkOrcaWorkspaceIdle,
  createOrcaSpawnExecutorPlugin,
  createOrcaWakeupDrivers,
  createOrcaWaveExecutorPlugin,
  inspectOrcaWorkspaceTerminal,
  resolveCallerOrcaWakeTargetPatch,
  setOrcaWorkspaceComment,
  spawnOrcaWorkspaceTerminal,
  syncOrcaTopicWakeTarget,
} from '../packages/plugins-orca/index.ts';
import { runCommand } from '../packages/subprocess/index.ts';
import type { CommandRequest, CommandRunner } from '../packages/subprocess/index.ts';
import {
  DealerNodeRuntime,
  liveDaemonOwner,
  runDealerCli,
  stopDaemonIfRunning,
} from '../packages/cli/index.ts';

const dealerBin = fileURLToPath(new URL('../bin/dealer.js', import.meta.url));
const dealerdBin = fileURLToPath(new URL('../bin/dealerd.js', import.meta.url));

function makeDir(t: TestContext, prefix = 'dealer-fixes-'): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

async function until(check: () => boolean, label: string, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for ${label}`);
    }
    await new Promise(resolve => setTimeout(resolve, 25));
  }
}

function runDealerBin(
  args: string[],
  stateDir: string,
  env: NodeJS.ProcessEnv = {},
): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--no-warnings', '--experimental-strip-types', dealerBin, ...args], {
      env: { ...process.env, DEALER_STATE_DIR: stateDir, DEALER_AUTO_UPDATE: '0', ...env },
    });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.once('error', reject);
    child.once('exit', code => resolve({ code, stdout, stderr }));
  });
}

function makeSignedEnvelope(
  sender: ReturnType<typeof generateNodeKeyPair>,
  id: string,
  overrides: Partial<MessageEnvelope> = {},
): MessageEnvelope {
  return signEnvelope(
    {
      id,
      traceId: id,
      networkId: 'team',
      from: { nodeId: sender.nodeId, member: 'alice', nodeName: 'alice-mac' },
      routing: { mode: 'unicast', targets: ['member:bob'] },
      kind: 'collab.request.v1',
      priority: 'normal',
      subject: `Subject ${id}`,
      body: `Body ${id}`,
      payload: {},
      attachments: [],
      createdAt: new Date().toISOString(),
      ...overrides,
    },
    sender.privateKeyPem,
  );
}

// ---------------------------------------------------------------------------
// 1. Detached Daemon Persistent Log & Pipe-Free Startup
// ---------------------------------------------------------------------------

test('detached launchDaemon logs to dealerd.log, survives stderr writes after CLI exits, and reports early-exit tail', async t => {
  const root = mkdtempSync(join(tmpdir(), 'dealer-daemon-log-'));
  const stateDir = join(root, 'state');
  mkdirSync(stateDir, { recursive: true });
  t.after(async () => {
    await stopDaemonIfRunning(stateDir, { force: true }).catch(() => {});
    rmSync(root, { recursive: true, force: true });
  });

  // Pre-seed log file with an older line to prove early-exit tail only reads from startOffset.
  const logPath = join(stateDir, 'logs', 'dealerd.log');
  mkdirSync(join(stateDir, 'logs'), { recursive: true });
  writeFileSync(logPath, 'PRIOR_DAEMON_LOG_SENTINEL\n');

  // Early exit must surface the bounded error tail written after startOffset.
  const badBoot = await runDealerBin(['node', 'up', '--profile', 'no-such-profile', '--json'], stateDir);
  assert.notEqual(badBoot.code, 0);
  assert.match(badBoot.stderr, /Daemon exited 1:/);
  assert.match(badBoot.stderr, /Unknown profile/i);
  assert.doesNotMatch(badBoot.stderr, /PRIOR_DAEMON_LOG_SENTINEL/);

  // Launch a healthy detached daemon via CLI parent process (which exits right after readiness).
  const boot = await runDealerBin(
    ['node', 'up', '--profile', 'headless-relay', '--member', 'pipe-survivor', '--json'],
    stateDir,
  );
  assert.equal(boot.code, 0, boot.stderr || boot.stdout);
  const bootedStatus = JSON.parse(boot.stdout.trim());
  assert.equal(bootedStatus.running, true);
  assert.equal(bootedStatus.member, 'pipe-survivor');
  const owner = liveDaemonOwner(stateDir);
  assert.ok(owner, 'detached daemon owns runtime.lock');
  assert.ok(existsSync(logPath), 'dealerd.log created');

  // Install a tiny user plugin that writes repeatedly to stderr inside the detached daemon.
  const noisyPluginPath = join(root, 'noisy-plugin.mjs');
  writeFileSync(
    noisyPluginPath,
    `export default function createPlugin() {
  return {
    name: 'noisy-stderr',
    apply() {
      for (let i = 0; i < 20; i += 1) {
        console.error('DETACHED_STDERR_BURST_' + i);
      }
    },
  };
}\n`,
  );
  const added = await runDealerBin(['plugin', 'add', '--name', 'noisy-stderr', '--module', noisyPluginPath, '--json'], stateDir);
  assert.equal(added.code, 0, added.stderr || added.stdout);

  // Daemon must still be alive with the exact same PID and its stderr burst persisted to dealerd.log.
  const statusAfter = await runDealerBin(['node', 'status', '--json'], stateDir);
  assert.equal(statusAfter.code, 0, statusAfter.stderr || statusAfter.stdout);
  assert.equal(JSON.parse(statusAfter.stdout.trim()).running, true);
  assert.equal(liveDaemonOwner(stateDir)?.pid, owner.pid);
  const logContents = readFileSync(logPath, 'utf8');
  assert.match(logContents, /DETACHED_STDERR_BURST_0/);
  assert.match(logContents, /DETACHED_STDERR_BURST_19/);
});

test('bin/dealerd.js ignores EPIPE when externally piped stdout/stderr reader closes', async t => {
  const root = mkdtempSync(join(tmpdir(), 'dealer-epipe-'));
  const stateDir = join(root, 'state');
  mkdirSync(stateDir, { recursive: true });
  let childPid: number | undefined;
  t.after(async () => {
    await stopDaemonIfRunning(stateDir, { force: true }).catch(() => {});
    if (childPid !== undefined) {
      try { process.kill(childPid, 'SIGTERM'); } catch { /* exited */ }
    }
    rmSync(root, { recursive: true, force: true });
  });

  const child = spawn(
    process.execPath,
    ['--no-warnings', '--experimental-strip-types', dealerdBin, '--profile', 'headless-relay', '--member', 'epipe-node'],
    {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, DEALER_STATE_DIR: stateDir, DEALER_AUTO_UPDATE: '0' },
    },
  );
  childPid = child.pid;
  let stdout = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', chunk => { stdout += chunk; });
  await until(() => stdout.includes('\n'), 'dealerd startup status');
  assert.equal(JSON.parse(stdout.split('\n')[0]).running, true);

  // Close both read ends of the pipes so subsequent writes hit EPIPE.
  child.stdout.destroy();
  child.stderr.destroy();

  const noisyPluginPath = join(root, 'epipe-plugin.mjs');
  writeFileSync(
    noisyPluginPath,
    `export default function createPlugin() {
  return {
    name: 'epipe-trigger',
    apply() {
      for (let i = 0; i < 10; i += 1) {
        process.stderr.write('EPIPE_TEST_LINE_' + i + '\\n');
        process.stdout.write('EPIPE_OUT_LINE_' + i + '\\n');
      }
    },
  };
}\n`,
  );
  const added = await runDealerBin(['plugin', 'add', '--name', 'epipe-trigger', '--module', noisyPluginPath, '--json'], stateDir);
  assert.equal(added.code, 0, added.stderr || added.stdout);
  const status = await runDealerBin(['node', 'status', '--json'], stateDir);
  assert.equal(status.code, 0, status.stderr || status.stdout);
  assert.equal(JSON.parse(status.stdout.trim()).running, true);
  assert.equal(child.exitCode, null);
});

// ---------------------------------------------------------------------------
// 2. Orca Single-Owner Worktree Selector & Deterministic Ambiguity Disarm
// ---------------------------------------------------------------------------

test('buildOrcaWorktreeSelector and Orca operations use id:<worktreeId> across list, create, set, and spawn', async t => {
  assert.equal(buildOrcaWorktreeSelector('/repo/a'), 'path:/repo/a');
  assert.equal(buildOrcaWorktreeSelector('/repo/a', '  '), 'path:/repo/a');
  assert.equal(buildOrcaWorktreeSelector('/repo/a', 'wt-1'), 'id:wt-1');
  assert.equal(buildOrcaWorktreeSelector('/repo/a', 'id:wt-1'), 'id:wt-1');
  assert.equal(buildOrcaWorktreeSelector('/repo/a', 'folder:f-1'), 'folder:f-1');
  assert.equal(buildOrcaWorktreeSelector('/repo/a', 'identity:x'), 'identity:x');

  const root = makeDir(t, 'dealer-orca-selector-');
  const sharedPath = join(root, 'shared-repo');
  mkdirSync(sharedPath, { recursive: true });
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    const cmd = `${request.args[0]} ${request.args[1]}`;
    if (cmd === 'worktree ps') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            truncated: false,
            worktrees: [
              { path: sharedPath, worktreeId: 'wt-busy', unverifiableTerminalCount: 0, agents: [{ state: 'working' }] },
              { path: sharedPath, worktreeId: 'wt-idle', unverifiableTerminalCount: 0, agents: [{ state: 'done' }] },
            ],
          },
        }),
      };
    }
    if (cmd === 'terminal list') {
      const selector = request.args[request.args.indexOf('--worktree') + 1];
      const handle = selector === 'id:wt-idle' ? 'term-idle' : 'term-busy';
      return {
        stdout: JSON.stringify({
          ok: true,
          result: { terminals: [{ handle, connected: true, writable: true, preview: '' }] },
        }),
      };
    }
    if (cmd === 'terminal create') {
      return { stdout: JSON.stringify({ ok: true, result: { terminal: { handle: 'term-created' } } }) };
    }
    if (cmd === 'worktree set' || cmd === 'terminal wait') {
      return { stdout: JSON.stringify({ ok: true }) };
    }
    return { stdout: JSON.stringify({ ok: true }) };
  };

  // Disambiguation with explicit worktreeId succeeds and targets id:<worktreeId>.
  const idleResult = await checkOrcaWorkspaceIdle(runner, 'orca', sharedPath, undefined, { worktreeId: 'wt-idle' });
  assert.equal(idleResult.idle, true);
  assert.ok(
    calls.some(c => c.args[0] === 'terminal' && c.args[1] === 'list' && c.args.includes('id:wt-idle')),
    'terminal list used id:wt-idle',
  );

  const busyResult = await checkOrcaWorkspaceIdle(runner, 'orca', sharedPath, undefined, { worktreeId: 'wt-busy' });
  assert.equal(busyResult.idle, false);
  assert.ok(
    calls.some(c => c.args[0] === 'terminal' && c.args[1] === 'list' && c.args.includes('id:wt-busy')),
    'terminal list used id:wt-busy',
  );

  // Without worktreeId, shared path immediately throws bounded OrcaWorktreeSelectorAmbiguousError.
  await assert.rejects(
    () => checkOrcaWorkspaceIdle(runner, 'orca', sharedPath),
    error => {
      assert.ok(error instanceof OrcaWorktreeSelectorAmbiguousError);
      assert.ok(error.message.length <= 200);
      assert.ok(!error.message.includes('\n'));
      return true;
    },
  );

  await spawnOrcaWorkspaceTerminal(runner, 'orca', sharedPath, undefined, { worktreeId: 'wt-idle' });
  assert.ok(
    calls.some(c => c.args[0] === 'terminal' && c.args[1] === 'create' && c.args.includes('id:wt-idle')),
    'terminal create used id:wt-idle',
  );

  await setOrcaWorkspaceComment(runner, 'orca', sharedPath, 'hello', { worktreeId: 'wt-idle' });
  assert.ok(
    calls.some(c => c.args[0] === 'worktree' && c.args[1] === 'set' && c.args.includes('id:wt-idle')),
    'worktree set used id:wt-idle',
  );

  // orca-spawn persists worktreeId returned by worktree create and uses id:<worktreeId>.
  const store = new MailboxStore(join(root, 'mailbox.json'));
  t.after(() => store.close());
  const spawnCalls: CommandRequest[] = [];
  const spawnPath = join(root, 'spawned-wt');
  mkdirSync(spawnPath, { recursive: true });
  const spawnRunner: CommandRunner = async request => {
    spawnCalls.push(request);
    const cmd = `${request.args[0]} ${request.args[1]}`;
    if (cmd === 'worktree create') {
      return { stdout: JSON.stringify({ ok: true, result: { worktree: { path: spawnPath, worktreeId: 'wt-spawn-42' } } }) };
    }
    if (cmd === 'terminal create') {
      return { stdout: JSON.stringify({ ok: true, result: { terminal: { handle: 'term-spawn-42' } } }) };
    }
    if (cmd === 'worktree ps') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: { worktrees: [{ path: spawnPath, worktreeId: 'wt-spawn-42', unverifiableTerminalCount: 0, agents: [{ state: 'done' }] }] },
        }),
      };
    }
    if (cmd === 'terminal list') {
      return { stdout: JSON.stringify({ ok: true, result: { terminals: [{ handle: 'term-spawn-42', connected: true, writable: true, preview: '> ', agentIdentity: 'antigravity' }] } }) };
    }
    return { stdout: JSON.stringify({ ok: true }) };
  };
  const sender = generateNodeKeyPair();
  const executor = createOrcaSpawnExecutorPlugin({
    commandRunner: spawnRunner,
    store,
    cwd: root,
    noParent: true,
    interactive: true,
  });
  const env = makeSignedEnvelope(sender, 'spawn-wt-id', { payload: { workspaceComment: 'spawned note' } });
  await executor.execute([env], { networkId: 'team', localNodeId: sender.nodeId }, {});
  const target = store.getWakeTarget('task:spawn-wt-id');
  assert.equal(target?.driverConfig?.worktreeId, 'wt-spawn-42');
  assert.ok(spawnCalls.some(c => c.args[0] === 'terminal' && c.args[1] === 'create' && c.args.includes('id:wt-spawn-42')));
  assert.ok(spawnCalls.some(c => c.args[0] === 'worktree' && c.args[1] === 'set' && c.args.includes('id:wt-spawn-42')));
  assert.ok(spawnCalls.some(c => c.args[0] === 'terminal' && c.args[1] === 'list' && c.args.includes('id:wt-spawn-42')));
});

test('Orca advisory probe cache isolates distinct worktreeIds sharing one path', async t => {
  const root = makeDir(t, 'dealer-orca-cache-');
  const fakeOrca = join(root, 'fake-orca');
  writeFileSync(
    fakeOrca,
    `#!${process.execPath}
const idx = process.argv.indexOf('--worktree');
const sel = idx >= 0 ? process.argv[idx + 1] : '';
const handle = sel === 'id:wt-a' ? 'term-a' : 'term-b';
process.stdout.write(JSON.stringify({ ok: true, result: { terminals: [{ handle, connected: true, writable: true }] } }));
`,
    { mode: 0o755 },
  );
  const presenceA = await inspectOrcaWorkspaceTerminal(runCommand, fakeOrca, root, 'term-a', undefined, undefined, true, 'wt-a');
  const presenceB = await inspectOrcaWorkspaceTerminal(runCommand, fakeOrca, root, 'term-b', undefined, undefined, true, 'wt-b');
  const crossCheck = await inspectOrcaWorkspaceTerminal(runCommand, fakeOrca, root, 'term-b', undefined, undefined, true, 'wt-a');
  assert.equal(presenceA, 'live');
  assert.equal(presenceB, 'live');
  assert.equal(crossCheck, 'absent');
});

test('Orca selector_ambiguous disarms wake target immediately without deliveryInDoubt or retry loop', async t => {
  const root = makeDir(t, 'dealer-orca-disarm-');
  const sharedPath = join(root, 'shared-path');
  mkdirSync(sharedPath, { recursive: true });
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const sender = generateNodeKeyPair();
  let listCalls = 0;
  const ambiguousJson = JSON.stringify({
    ok: false,
    error: {
      code: 'selector_ambiguous',
      message: 'Multiple worktrees match path',
      candidates: [{ id: 'wt-1', path: sharedPath }, { id: 'wt-2', path: sharedPath }],
    },
  });
  const runner: CommandRunner = async request => {
    const cmd = `${request.args[0]} ${request.args[1]}`;
    if (cmd === 'terminal list') {
      listCalls += 1;
      throw Object.assign(new Error(`orca exited 1: ${ambiguousJson}`), {
        stdout: ambiguousJson,
        stderr: '',
        exitCode: 1,
      });
    }
    if (cmd === 'worktree ps') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            truncated: false,
            worktrees: [
              { path: sharedPath, worktreeId: 'wt-1', unverifiableTerminalCount: 0, agents: [] },
              { path: sharedPath, worktreeId: 'wt-2', unverifiableTerminalCount: 0, agents: [] },
            ],
          },
        }),
      };
    }
    return { stdout: JSON.stringify({ ok: true }) };
  };
  const transitions: Array<{ state: string; reason?: string }> = [];
  const service = new WakeupRedriveService({
    store,
    drivers: [...createOrcaWakeupDrivers({ commandRunner: runner })],
    flushIntervalMs: 100_000,
    onTransition: event => transitions.push({ state: event.state, reason: event.reason }),
  });
  t.after(async () => {
    await service.stop();
    store.close();
  });

  const mailboxId = 'stream:ambiguous';
  store.upsertWakeTarget(mailboxId, {
    driver: 'orca-terminal',
    coalesceMs: 0,
    driverConfig: { workspacePath: sharedPath, terminalHandle: 'term-x' },
  });

  // probeSlot executes OrcaWakeDelivery.probe() and reports busy/not-ready with concise bounded message (no JSON dump).
  const probe = await service.probeSlot(mailboxId);
  assert.equal(probe.busy, true);
  assert.equal(probe.ready, false);
  assert.ok(probe.reason && probe.reason.length <= 200);
  assert.match(probe.reason!, /ambiguous/i);
  assert.doesNotMatch(probe.reason!, /"candidates"/);

  store.enqueue([makeSignedEnvelope(sender, 'ambig-1')], { mailboxId });

  // First flush disarms immediately without setting deliveryInDoubt; mail stays queued.
  const first = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(first.woke, false);
  assert.ok(first.reason && first.reason.length <= 200);
  assert.match(first.reason!, /ambiguous/i);
  assert.doesNotMatch(first.reason!, /"candidates"/);
  const afterFirst = store.getWakeTarget(mailboxId);
  assert.equal(afterFirst?.pendingWake, false);
  assert.notEqual(afterFirst?.deliveryInDoubt, true);
  assert.equal(store.list({ mailboxId }).queuedCount, 1);
  assert.ok(transitions.some(t => t.state === 'disarmed' && /ambiguous/i.test(t.reason ?? '')));

  // Second flush does NOT retry Orca (disarmed, no infinite loop).
  const callsAfterFirst = listCalls;
  await service.flushMailbox(mailboxId);
  assert.equal(listCalls, callsAfterFirst);
});

test('Orca terminal create selector_ambiguous clears launch fence and disarms without deliveryInDoubt', async t => {
  const root = makeDir(t, 'dealer-orca-create-ambig-');
  const workspacePath = join(root, 'ws');
  mkdirSync(workspacePath, { recursive: true });
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const sender = generateNodeKeyPair();
  const ambiguousJson = JSON.stringify({
    ok: false,
    error: { code: 'selector_ambiguous', message: 'Multiple worktrees match path' },
  });
  const runner: CommandRunner = async request => {
    const cmd = `${request.args[0]} ${request.args[1]}`;
    if (cmd === 'worktree ps') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: { worktrees: [{ path: workspacePath, worktreeId: 'wt-single', unverifiableTerminalCount: 0, agents: [] }] },
        }),
      };
    }
    if (cmd === 'terminal list') {
      return { stdout: JSON.stringify({ ok: true, result: { terminals: [] } }) };
    }
    if (cmd === 'terminal create') {
      throw Object.assign(new Error(`orca exited 1: ${ambiguousJson}`), {
        stdout: ambiguousJson,
        stderr: '',
        exitCode: 1,
      });
    }
    return { stdout: JSON.stringify({ ok: true }) };
  };
  const service = new WakeupRedriveService({
    store,
    drivers: [...createOrcaWakeupDrivers({ commandRunner: runner })],
    flushIntervalMs: 100_000,
  });
  t.after(async () => {
    await service.stop();
    store.close();
  });

  const mailboxId = 'stream:create-ambig';
  store.enqueue([makeSignedEnvelope(sender, 'ambig-create-1')], {
    mailboxId,
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      rotateAfterWaves: 1,
      driverConfig: { workspacePath },
    },
  });

  const result = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(result.woke, false);
  assert.match(result.reason ?? '', /ambiguous/i);
  const target = store.getWakeTarget(mailboxId);
  assert.equal(target?.pendingWake, false);
  assert.notEqual(target?.deliveryInDoubt, true);
});

test('Orca autoCreateWorkspace persists worktreeId from ensureOrcaWorkspace and uses id:<worktreeId>', async t => {
  const root = makeDir(t, 'dealer-orca-autocreate-');
  const repoRoot = join(root, 'repo');
  const workspacePath = join(root, 'standing-wt');
  mkdirSync(repoRoot, { recursive: true });
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const sender = generateNodeKeyPair();
  let created = false;
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    const cmd = `${request.args[0]} ${request.args[1]}`;
    if (cmd === 'worktree ps') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            truncated: false,
            worktrees: created
              ? [{ path: workspacePath, worktreeId: 'wt-auto-99', unverifiableTerminalCount: 0, agents: [] }]
              : [],
          },
        }),
      };
    }
    if (cmd === 'worktree list') {
      return { stdout: JSON.stringify({ ok: true, result: { worktrees: [] } }) };
    }
    if (cmd === 'worktree create') {
      created = true;
      mkdirSync(workspacePath, { recursive: true });
      return { stdout: JSON.stringify({ ok: true, result: { worktree: { path: workspacePath, worktreeId: 'wt-auto-99' } } }) };
    }
    if (cmd === 'terminal list') {
      return { stdout: JSON.stringify({ ok: true, result: { terminals: [{ handle: 'term-auto', connected: true, writable: true, preview: '> ', agentIdentity: 'antigravity' }] } }) };
    }
    if (cmd === 'terminal create') {
      return { stdout: JSON.stringify({ ok: true, result: { terminal: { handle: 'term-auto' } } }) };
    }
    return { stdout: JSON.stringify({ ok: true }) };
  };
  const service = new WakeupRedriveService({
    store,
    drivers: [...createOrcaWakeupDrivers({ commandRunner: runner })],
    flushIntervalMs: 100_000,
  });
  t.after(async () => {
    await service.stop();
    store.close();
  });

  const mailboxId = 'stream:auto-ws';
  store.enqueue([makeSignedEnvelope(sender, 'auto-ws-1')], {
    mailboxId,
    wakeTarget: {
      driver: 'orca-terminal',
      coalesceMs: 0,
      driverConfig: { workspacePath, repoRoot, autoCreateWorkspace: true },
    },
  });
  const result = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(result.woke, true, result.reason);
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.worktreeId, 'wt-auto-99');
  assert.ok(calls.some(c => c.args[0] === 'worktree' && c.args[1] === 'set' && c.args.includes('id:wt-auto-99')));
});

// ---------------------------------------------------------------------------
// 3. Outbox Backoff Convergence, `dealer outbox drop`, and Timeout Wording
// ---------------------------------------------------------------------------

test('outbox exponential backoff reaches 30m cap while transport/peer-reachable still wakes immediately', async t => {
  assert.equal(attemptDelayMs(1), 4_000);
  assert.equal(attemptDelayMs(5), 64_000);
  assert.equal(attemptDelayMs(9), 1_024_000);
  assert.equal(attemptDelayMs(10), 1_800_000);
  assert.equal(attemptDelayMs(50), 1_800_000);

  const root = makeDir(t, 'dealer-outbox-backoff-');
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'blobs'));
  const sender = generateNodeKeyPair();
  const receiver = generateNodeKeyPair();
  const env = makeSignedEnvelope(sender, 'backoff-wake-1');
  outbox.enqueue({ envelope: env, pendingNodeIds: [receiver.nodeId] });

  // Push nextAttemptAt 30 minutes into the future (simulating capped backoff after 10+ attempts).
  const thirtyMinAhead = new Date(Date.now() + 1_800_000).toISOString();
  outbox.recordAttempt(env.id, { pendingNodeIds: [receiver.nodeId], nextAttemptAt: thirtyMinAhead });
  assert.equal(outbox.get(env.id)?.status, 'queued');

  const bus = new EventEmitter();
  let deliveredCount = 0;
  const redrive = new OutboxRedriveService({
    outbox,
    intervalMs: 60_000,
    events: {
      on(event, listener) {
        bus.on(event, listener);
        return { dispose: () => { bus.off(event, listener); } };
      },
    },
    deliver: async () => {
      deliveredCount += 1;
      const receipt = signReceipt(
        {
          messageId: env.id,
          networkId: env.networkId,
          fromNodeId: receiver.nodeId,
          stage: 'dispatched',
          updatedAt: new Date().toISOString(),
          executionReceipt: { executor: 'inbox', status: 'accepted', handoff: 'queued', durable: true },
        },
        receiver.privateKeyPem,
      );
      return { deliveredReceipts: { [receiver.nodeId]: receipt }, pendingNodeIds: [] };
    },
  });
  redrive.start();
  t.after(() => redrive.stop());

  // Regular scheduled flush skips backed-off item.
  const skipped = await redrive.flush();
  assert.equal(skipped.attempted, 0);
  assert.equal(deliveredCount, 0);

  // transport/peer-reachable bypasses the 30m backoff immediately.
  const reachable: PeerReachableEvent = { networkId: env.networkId, nodeId: receiver.nodeId, via: 'tunnel' };
  bus.emit('transport/peer-reachable', reachable);
  await until(() => outbox.get(env.id)?.status === 'delivered', 'peer-reachable immediate outbox wake');
  assert.equal(deliveredCount, 1);
});

test('dealer outbox drop settles partially-delivered items, cleans staged files, fails undelivered items, skips active attempts, and validates filters', async t => {
  const root = makeDir(t, 'dealer-outbox-drop-');
  const runtime = new DealerNodeRuntime({
    stateDir: join(root, 'state'),
    member: 'operator',
    name: 'operator',
    profile: 'headless-relay',
    transport: { discoveryDir: join(root, 'mesh'), multicast: false },
  });
  await runtime.start();
  t.after(() => runtime.stop());

  const sender = generateNodeKeyPair();
  const livePeer = generateNodeKeyPair();
  const ghostPeer = generateNodeKeyPair();
  const activePeer = generateNodeKeyPair();

  // Empty filter rejected via CLI.
  const errLines: string[] = [];
  const emptyCode = await runDealerCli(['outbox', 'drop', '--json'], {
    runtime,
    stdout: () => {},
    stderr: line => errLines.push(line),
  });
  assert.equal(emptyCode, 1);
  assert.match(errLines.join('\n'), /outbox drop requires --id or --node/);

  // 1. Partially delivered obligation with staged attachment file + clean offline ghost peer (never sent).
  const stagedSource = join(root, 'attach.txt');
  writeFileSync(stagedSource, 'staged attachment body');
  const blobHash = 'a'.repeat(64);
  const partialEnv = makeSignedEnvelope(sender, 'msg-partial');
  const liveReceipt = signReceipt(
    {
      messageId: partialEnv.id,
      networkId: partialEnv.networkId,
      fromNodeId: livePeer.nodeId,
      stage: 'dispatched',
      updatedAt: new Date().toISOString(),
      executionReceipt: { executor: 'inbox', status: 'accepted', handoff: 'queued', durable: true },
    },
    livePeer.privateKeyPem,
  );
  const partialQueued = runtime.outbox.enqueue({
    envelope: partialEnv,
    pendingNodeIds: [ghostPeer.nodeId],
    deliveredReceipts: { [livePeer.nodeId]: liveReceipt },
    filePaths: { [blobHash]: stagedSource },
    lastError: 'Peer unreachable',
  });
  const stagedCopyPath = partialQueued.stagedFiles[blobHash];
  assert.ok(stagedCopyPath && existsSync(stagedCopyPath), 'staged attachment exists before drop');

  // 2. Undelivered obligation waiting only on ghostPeer.
  const deadOnlyEnv = makeSignedEnvelope(sender, 'msg-dead-only');
  runtime.outbox.enqueue({
    envelope: deadOnlyEnv,
    pendingNodeIds: [ghostPeer.nodeId],
    lastError: 'Peer unreachable',
  });

  // 3. Active-attempt obligation waiting on ghostPeer — must be skipped!
  const activeEnv = makeSignedEnvelope(sender, 'msg-active');
  runtime.outbox.enqueue({
    envelope: activeEnv,
    pendingNodeIds: [ghostPeer.nodeId, activePeer.nodeId],
  });
  const claimed = runtime.outbox.beginAttempt(activeEnv.id, { leaseMs: 60_000 });
  assert.ok(claimed);

  // Drop ghostPeer via CLI.
  const outLines: string[] = [];
  const dropCode = await runDealerCli(
    ['outbox', 'drop', '--node', ghostPeer.nodeId, '--reason', 'ghost peer retired', '--json'],
    { runtime, stdout: line => outLines.push(line), stderr: line => assert.fail(line) },
  );
  assert.equal(dropCode, 0);
  const dropped = JSON.parse(outLines.join('\n'));
  assert.equal(dropped.updatedCount, 2);
  assert.equal(dropped.settledDeliveredCount, 1);
  assert.equal(dropped.markedFailedCount, 1);
  assert.deepEqual(dropped.skippedActiveIds, [activeEnv.id]);

  // Verify partial item settled to delivered, kept deliveredReceipts, did not add ghost to failedNodeIds, cleaned staged files.
  const partialAfter = runtime.outbox.get(partialEnv.id);
  assert.equal(partialAfter?.status, 'delivered');
  assert.deepEqual(partialAfter?.pendingNodeIds, []);
  assert.deepEqual(partialAfter?.inDoubtNodeIds, []);
  assert.deepEqual(partialAfter?.failedNodeIds, []);
  assert.ok(partialAfter?.deliveredReceipts[livePeer.nodeId]);
  assert.deepEqual(partialAfter?.stagedFiles, {});
  assert.equal(existsSync(stagedCopyPath), false, 'staged attachment cleaned on delivered settlement');

  // Verify undelivered item marked failed with operator reason.
  const deadAfter = runtime.outbox.get(deadOnlyEnv.id);
  assert.equal(deadAfter?.status, 'failed');
  assert.equal(deadAfter?.lastError, 'ghost peer retired');
  assert.deepEqual(deadAfter?.pendingNodeIds, []);
  assert.deepEqual(deadAfter?.failedNodeIds, []);

  // Verify active-attempt item remained untouched.
  const activeAfter = runtime.outbox.get(activeEnv.id);
  assert.equal(activeAfter?.status, 'queued');
  assert.deepEqual(activeAfter?.pendingNodeIds, [ghostPeer.nodeId, activePeer.nodeId]);

  // Release active attempt, then drop by --id without --node.
  runtime.outbox.releaseAttempt(activeEnv.id, claimed.attemptToken);
  const idOut: string[] = [];
  const idCode = await runDealerCli(
    ['outbox', 'drop', '--id', activeEnv.id, '--reason', 'cancelled task', '--json'],
    { runtime, stdout: line => idOut.push(line), stderr: line => assert.fail(line) },
  );
  assert.equal(idCode, 0);
  const idDropped = JSON.parse(idOut.join('\n'));
  assert.equal(idDropped.updatedCount, 1);
  assert.equal(idDropped.markedFailedCount, 1);
  assert.equal(runtime.outbox.get(activeEnv.id)?.status, 'failed');
  assert.equal(runtime.outbox.get(activeEnv.id)?.lastError, 'cancelled task');
  assert.deepEqual(runtime.outbox.get(activeEnv.id)?.pendingNodeIds, []);
});

test('emitter timeout queued receipt reports pending reconciliation wording instead of offline peer', async t => {
  const root = makeDir(t, 'dealer-emitter-timeout-');
  const sender = generateNodeKeyPair();
  const target = generateNodeKeyPair();
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'blobs'));
  let sendMode: 'timeout' | 'offline' = 'timeout';

  const kernel = new Kernel();
  t.after(() => kernel.stop());

  const transport: TransportService = {
    localNodeId: sender.nodeId,
    endpointAddr: '127.0.0.1:0',
    async mountNetwork() { return { networkId: 'team', dispose() {} }; },
    async unmountNetwork() {},
    listMountedNetworks: () => ['team'],
    getNetworkConfig: () => undefined,
    isPeerReachable: () => true,
    async sendUnicast() {
      if (sendMode === 'timeout') {
        throw new DeliveryTimeoutError('Unicast response timed out');
      }
      throw markBeforeSendError(new PeerUnreachableError('Peer offline'));
    },
    async broadcast() {},
    async shareFile() { throw new Error('unused'); },
    async fetchBlobToPath() { throw new Error('unused'); },
  };
  await kernel.use({
    name: 'mock-transport',
    apply(ctx) {
      ctx.provide('transport', transport);
    },
  });
  await kernel.use(createNodePlugin({
    keyPair: sender,
    member: 'alice',
    nodeName: 'alice-mac',
    admissionMode: 'open',
    peersPath: join(root, 'peers.json'),
  }));
  await kernel.use(createRouterPlugin());
  await kernel.use(createEmitterPlugin({ outbox }));

  const targetCard = signNodeCard(
    {
      nodeId: target.nodeId,
      networkId: 'team',
      revision: 1,
      identity: { member: 'bob', nodeName: 'bob-mac' },
      admissionMode: 'open',
      capabilities: { summary: 'bob', skills: [] },
      runtimeState: { load: 'idle', activeTasks: 0, availableAgents: [] },
      updatedAt: new Date().toISOString(),
    },
    target.privateKeyPem,
  );
  kernel.ctx.emit('transport/inbound', {
    networkId: 'team',
    senderNodeId: target.nodeId,
    mode: 'broadcast',
    subTopic: 'presence',
    payload: { type: 'peer.card', card: targetCard },
  });

  const emitter = kernel.getService('emitter');
  sendMode = 'timeout';
  const timeoutEmit = await emitter.emit({
    networkId: 'team',
    routing: { mode: 'unicast', targets: [target.nodeId] },
    kind: 'collab.request.v1',
    subject: 'Timeout test',
    body: 'body',
    payload: {},
  });
  assert.equal(timeoutEmit.receipts.length, 1);
  assert.equal(
    timeoutEmit.receipts[0].detail,
    `Queued in local outbox pending reconciliation after delivery timeout for peer ${target.nodeId}`,
  );

  sendMode = 'offline';
  const offlineEmit = await emitter.emit({
    networkId: 'team',
    routing: { mode: 'unicast', targets: [target.nodeId] },
    kind: 'collab.request.v1',
    subject: 'Offline test',
    body: 'body',
    payload: {},
  });
  assert.equal(offlineEmit.receipts.length, 1);
  assert.equal(
    offlineEmit.receipts[0].detail,
    `Queued in local outbox for offline peer ${target.nodeId}`,
  );
});

// ---------------------------------------------------------------------------
// 4. Regression tests for Codex Architecture Review findings
// ---------------------------------------------------------------------------

test('outbox drop preserves uncertainty and remote custody so cancellation cannot authorize replacement or fabricate delivery', t => {
  const root = makeDir(t, 'dealer-outbox-drop-evidence-');
  const outbox = new OutboxStore(join(root, 'outbox.json'), join(root, 'blobs'));
  const sender = generateNodeKeyPair();
  const peerA = generateNodeKeyPair();
  const peerB = generateNodeKeyPair();
  const peerC = generateNodeKeyPair();

  // Case 1: Multicast where A has a proven pre-effect failure and B timed out (inDoubt).
  const mixedTimeoutEnv = makeSignedEnvelope(sender, 'msg-mixed-timeout', { idempotencyKey: 'key-mixed-timeout' });
  outbox.enqueue({
    envelope: mixedTimeoutEnv,
    pendingNodeIds: [peerB.nodeId],
    inDoubtNodeIds: [peerB.nodeId],
    failedNodeIds: [peerA.nodeId],
    unattemptedNodeIds: [peerA.nodeId],
  });
  const droppedTimeout = outbox.drop({ nodeId: peerB.nodeId, reason: 'cancel retry B' });
  assert.equal(droppedTimeout.updatedCount, 1);
  assert.equal(droppedTimeout.markedFailedCount, 1);
  const afterTimeout = outbox.get(mixedTimeoutEnv.id)!;
  assert.equal(afterTimeout.status, 'failed');
  assert.deepEqual(afterTimeout.pendingNodeIds, []);
  assert.deepEqual(afterTimeout.inDoubtNodeIds, [peerB.nodeId]);
  assert.equal(hasProvenUnattemptedFailure(afterTimeout), false);
  assert.equal(isUnattemptedOutboxItem(afterTimeout), false);
  // Same-version idempotency replacement MUST be refused while B's outcome is unknown.
  const replacementEnv = makeSignedEnvelope(sender, 'msg-mixed-timeout-replacement', {
    idempotencyKey: 'key-mixed-timeout',
    body: 'retargeted body',
  });
  const replaced = outbox.replaceUnattemptedIdempotency({
    envelope: replacementEnv,
    expectedId: mixedTimeoutEnv.id,
    plannedTargetNodeIds: [peerC.nodeId],
  });
  assert.equal(replaced?.claimed, false);

  // Case 2: Multicast where A has a proven pre-effect failure and C awaits remote approval.
  const mixedApprovalEnv = makeSignedEnvelope(sender, 'msg-mixed-approval', { idempotencyKey: 'key-mixed-approval' });
  outbox.enqueue({
    envelope: mixedApprovalEnv,
    pendingNodeIds: [peerC.nodeId],
    approvalPendingNodeIds: [peerC.nodeId],
    failedNodeIds: [peerA.nodeId],
    unattemptedNodeIds: [peerA.nodeId],
  });
  outbox.drop({ id: mixedApprovalEnv.id, reason: 'cancel approval retry' });
  const afterApproval = outbox.get(mixedApprovalEnv.id)!;
  assert.equal(afterApproval.status, 'failed');
  assert.deepEqual(afterApproval.pendingNodeIds, []);
  assert.deepEqual(afterApproval.inDoubtNodeIds, [peerC.nodeId]);
  assert.equal(hasProvenUnattemptedFailure(afterApproval), false);
  assert.equal(isUnattemptedOutboxItem(afterApproval), false);

  // Case 3: Live peer A delivered, peer B timed out (inDoubt) or was relay-spooled, with staged file.
  const sourceFile = join(root, 'evidence.txt');
  writeFileSync(sourceFile, 'must remain as evidence');
  const blobHash = 'b'.repeat(64);
  const partialInDoubtEnv = makeSignedEnvelope(sender, 'msg-partial-indoubt');
  const confirmedA = signReceipt(
    {
      messageId: partialInDoubtEnv.id,
      networkId: partialInDoubtEnv.networkId,
      fromNodeId: peerA.nodeId,
      stage: 'dispatched',
      updatedAt: new Date().toISOString(),
      executionReceipt: { executor: 'inbox', status: 'accepted', handoff: 'queued', durable: true },
    },
    peerA.privateKeyPem,
  );
  const queuedPartial = outbox.enqueue({
    envelope: partialInDoubtEnv,
    pendingNodeIds: [peerB.nodeId],
    inDoubtNodeIds: [peerB.nodeId],
    relaySpooledNodeIds: [peerB.nodeId],
    deliveredReceipts: { [peerA.nodeId]: confirmedA },
    filePaths: { [blobHash]: sourceFile },
  });
  const stagedCopy = queuedPartial.stagedFiles[blobHash];
  assert.ok(stagedCopy && existsSync(stagedCopy));

  const droppedPartial = outbox.drop({ nodeId: peerB.nodeId, reason: 'stop retrying timed-out peer' });
  assert.equal(droppedPartial.updatedCount, 1);
  assert.equal(droppedPartial.settledDeliveredCount, 0);
  assert.equal(droppedPartial.markedFailedCount, 1);
  const afterPartial = outbox.get(partialInDoubtEnv.id)!;
  assert.equal(afterPartial.status, 'failed');
  assert.deepEqual(afterPartial.pendingNodeIds, []);
  assert.deepEqual(afterPartial.inDoubtNodeIds, [peerB.nodeId]);
  assert.deepEqual(afterPartial.relaySpooledNodeIds, [peerB.nodeId]);
  assert.ok(afterPartial.deliveredReceipts[peerA.nodeId]);
  assert.equal(existsSync(stagedCopy), true, 'staged evidence preserved when uncertainty/custody remains');
});

test('Orca identity transitions clear stale worktreeId on rebind without replacement ID and reject mismatched ID/path before probing or launching', async t => {
  const root = makeDir(t, 'dealer-orca-rebind-');
  const pathA = join(root, 'ws-a');
  const pathB = join(root, 'ws-b');
  mkdirSync(pathA, { recursive: true });
  mkdirSync(pathB, { recursive: true });
  const store = new MailboxStore(join(root, 'mailbox.json'));
  t.after(() => store.close());

  // 1. Caller transition changing workspacePath without replacement worktreeId clears stale worktreeId.
  const callerMailbox = 'worktree:caller-rebind';
  store.upsertWakeTarget(callerMailbox, {
    driver: 'orca-terminal',
    driverConfig: { workspacePath: pathA, worktreeId: 'wt-a', terminalHandle: 'term-a' },
  });
  const callerPatch = resolveCallerOrcaWakeTargetPatch(store.getWakeTarget(callerMailbox), {
    workspacePath: pathB,
    terminalHandle: 'term-b',
  });
  const callerUpdated = store.upsertWakeTarget(callerMailbox, callerPatch);
  assert.equal(callerUpdated.driverConfig?.workspacePath, pathB);
  assert.equal(callerUpdated.driverConfig?.worktreeId, undefined);
  assert.equal(callerUpdated.driverConfig?.terminalHandle, 'term-b');

  // 2. Authoritative config sync changing workspacePath without replacement worktreeId clears stale worktreeId.
  const configMailbox = 'stream:config-rebind';
  store.upsertWakeTarget(configMailbox, {
    driver: 'orca-terminal',
    driverConfig: { workspacePath: pathA, worktreeId: 'wt-a', terminalHandle: 'term-a' },
  });
  syncOrcaTopicWakeTarget({
    mailboxId: configMailbox,
    store,
    binding: { executor: 'orca-wave', workspacePath: pathB },
  });
  assert.equal(store.getWakeTarget(configMailbox)?.driverConfig?.workspacePath, pathB);
  assert.equal(store.getWakeTarget(configMailbox)?.driverConfig?.worktreeId, undefined);
  assert.equal(store.getWakeTarget(configMailbox)?.driverConfig?.terminalHandle, undefined);

  // 3. Wave ingestion path changing workspacePath without replacement worktreeId clears stale worktreeId.
  const waveMailbox = 'stream:wave-rebind';
  store.upsertWakeTarget(waveMailbox, {
    driver: 'orca-terminal',
    driverConfig: { workspacePath: pathA, worktreeId: 'wt-a', terminalHandle: 'term-a' },
  });
  const sender = generateNodeKeyPair();
  const waveExecutor = createOrcaWaveExecutorPlugin({
    store,
    resolveTopicBinding: () => ({ executor: 'orca-wave', mailboxId: waveMailbox, workspacePath: pathB }),
  });
  await waveExecutor.execute(
    [makeSignedEnvelope(sender, 'wave-rebind-1')],
    { networkId: 'team', nodeId: sender.nodeId, traceId: 'trace-rebind' },
    {},
  );
  assert.equal(store.getWakeTarget(waveMailbox)?.driverConfig?.workspacePath, pathB);
  assert.equal(store.getWakeTarget(waveMailbox)?.driverConfig?.worktreeId, undefined);

  // 4. An inconsistent persisted (workspacePath, worktreeId) binding is rejected BEFORE terminal probing or launching.
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    const cmd = `${request.args[0]} ${request.args[1]}`;
    if (cmd === 'worktree ps') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            truncated: false,
            worktrees: [
              { path: pathA, worktreeId: 'wt-a', unverifiableTerminalCount: 0, agents: [] },
              { path: pathB, worktreeId: 'wt-b', unverifiableTerminalCount: 0, agents: [] },
            ],
          },
        }),
      };
    }
    return { stdout: JSON.stringify({ ok: true, result: { terminals: [] } }) };
  };
  await assert.rejects(
    () => checkOrcaWorkspaceIdle(runner, 'orca', pathB, undefined, { worktreeId: 'wt-a' }),
    OrcaWorktreeBindingMismatchError,
  );
  assert.equal(calls.some(c => c.args[0] === 'terminal'), false, 'no terminal commands after mismatch');

  const brokenMailbox = 'stream:broken-binding';
  store.upsertWakeTarget(brokenMailbox, {
    driver: 'orca-terminal',
    coalesceMs: 0,
    driverConfig: { workspacePath: pathB, worktreeId: 'wt-a', terminalHandle: 'term-a' },
  });
  const service = new WakeupRedriveService({
    store,
    drivers: [...createOrcaWakeupDrivers({ commandRunner: runner })],
    flushIntervalMs: 100_000,
  });
  t.after(() => service.stop());

  calls.length = 0;
  const probe = await service.probeSlot(brokenMailbox);
  assert.equal(probe.busy, true);
  assert.equal(probe.ready, false);
  assert.match(probe.reason ?? '', /wt-a/);
  assert.equal(calls.some(c => c.args[0] === 'terminal'), false, 'probe rejected mismatch before terminal list');

  store.enqueue([makeSignedEnvelope(sender, 'broken-1')], { mailboxId: brokenMailbox });
  calls.length = 0;
  const flushed = await service.flushMailbox(brokenMailbox, { force: true });
  assert.equal(flushed.woke, false);
  assert.match(flushed.reason ?? '', /wt-a/);
  assert.equal(store.getWakeTarget(brokenMailbox)?.pendingWake, false);
  assert.notEqual(store.getWakeTarget(brokenMailbox)?.deliveryInDoubt, true);
  assert.equal(calls.some(c => c.args[0] === 'terminal'), false, 'flush disarmed mismatch before terminal list/create');
});

test('Orca workspace recovery rejects duplicate canonical-path matches with OrcaWorktreeSelectorAmbiguousError when persisted worktreeId disappears', async t => {
  const root = makeDir(t, 'dealer-orca-recover-ambig-');
  const repoRoot = join(root, 'repo');
  const sharedPath = join(root, 'shared-ws');
  mkdirSync(repoRoot, { recursive: true });
  mkdirSync(sharedPath, { recursive: true });
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const sender = generateNodeKeyPair();
  const calls: CommandRequest[] = [];
  const runner: CommandRunner = async request => {
    calls.push(request);
    const cmd = `${request.args[0]} ${request.args[1]}`;
    if (cmd === 'worktree ps') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            truncated: false,
            worktrees: [
              { path: sharedPath, worktreeId: 'wt-1', unverifiableTerminalCount: 0, agents: [] },
              { path: sharedPath, worktreeId: 'wt-2', unverifiableTerminalCount: 0, agents: [] },
            ],
          },
        }),
      };
    }
    if (cmd === 'worktree list') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            worktrees: [
              { id: 'wt-1', path: sharedPath },
              { id: 'wt-2', path: sharedPath },
            ],
          },
        }),
      };
    }
    return { stdout: JSON.stringify({ ok: true }) };
  };
  const service = new WakeupRedriveService({
    store,
    drivers: [...createOrcaWakeupDrivers({ commandRunner: runner })],
    flushIntervalMs: 100_000,
  });
  t.after(async () => {
    await service.stop();
    store.close();
  });

  const mailboxId = 'stream:recover-ambig';
  store.upsertWakeTarget(mailboxId, {
    driver: 'orca-terminal',
    coalesceMs: 0,
    driverConfig: {
      workspacePath: sharedPath,
      worktreeId: 'wt-gone',
      repoRoot,
      autoCreateWorkspace: true,
    },
  });

  const probe = await service.probeSlot(mailboxId);
  assert.equal(probe.busy, true);
  assert.equal(probe.ready, false);
  assert.match(probe.reason ?? '', /ambiguous/i);

  store.enqueue([makeSignedEnvelope(sender, 'recover-ambig-1')], { mailboxId });
  calls.length = 0;
  const flushed = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(flushed.woke, false);
  assert.match(flushed.reason ?? '', /ambiguous/i);
  assert.equal(store.getWakeTarget(mailboxId)?.driverConfig?.worktreeId, 'wt-gone', 'did not persist arbitrary sibling ID');
  assert.equal(store.getWakeTarget(mailboxId)?.pendingWake, false);
  assert.notEqual(store.getWakeTarget(mailboxId)?.deliveryInDoubt, true);
  assert.equal(calls.some(c => c.args[0] === 'terminal' && c.args[1] === 'send'), false);
});

test('standing worktree Git sync blocks when an idle selected worktreeId shares its checkout path with an active same-path sibling workspace', async t => {
  const root = makeDir(t, 'dealer-orca-sync-sibling-');
  const upstream = join(root, 'upstream');
  const standing = join(root, 'standing');
  const git = (cwd: string, ...args: string[]) =>
    execFileSync('git', ['-C', cwd, '-c', 'core.fsmonitor=false', ...args], { encoding: 'utf8' }).trim();
  mkdirSync(upstream, { recursive: true });
  git(upstream, 'init', '--quiet', '--initial-branch=main');
  git(upstream, 'config', 'user.email', 'test@example.com');
  git(upstream, 'config', 'user.name', 'Test');
  writeFileSync(join(upstream, 'README.md'), 'v1\n');
  git(upstream, 'add', 'README.md');
  git(upstream, 'commit', '--quiet', '-m', 'v1');
  execFileSync('git', ['clone', '--quiet', upstream, standing]);
  const initialSha = git(standing, 'rev-parse', 'HEAD');

  // Advance upstream by one commit.
  writeFileSync(join(upstream, 'README.md'), 'v2\n');
  git(upstream, 'add', 'README.md');
  git(upstream, 'commit', '--quiet', '-m', 'v2');
  const upstreamSha = git(upstream, 'rev-parse', 'HEAD');
  assert.notEqual(initialSha, upstreamSha);

  let siblingActive = true;
  let created = 0;
  const selectedTerminals = [
    { handle: 'term-selected', connected: true, writable: true, agentIdentity: 'antigravity', preview: '> ', tabId: 'sel', leafId: 'root' },
  ];
  const runner: CommandRunner = async request => {
    if (request.command === 'git') {
      return runCommand(request);
    }
    const cmd = `${request.args[0]} ${request.args[1]}`;
    if (cmd === 'worktree ps') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            truncated: false,
            worktrees: [
              {
                path: standing,
                worktreeId: 'wt-selected',
                childWorktreeIds: [],
                unverifiableTerminalCount: 0,
                agents: [{ state: 'done', paneKey: 'sel:root' }],
              },
              {
                path: standing,
                worktreeId: 'wt-manual-sibling',
                childWorktreeIds: [],
                unverifiableTerminalCount: 0,
                agents: siblingActive ? [{ state: 'working', hasLivePty: true }] : [],
              },
            ],
          },
        }),
      };
    }
    if (cmd === 'terminal list') {
      const sel = request.args[request.args.indexOf('--worktree') + 1];
      if (sel === 'id:wt-manual-sibling') {
        return {
          stdout: JSON.stringify({
            ok: true,
            result: {
              terminals: siblingActive
                ? [{ handle: 'term-manual', connected: true, writable: true, preview: '' }]
                : [],
            },
          }),
        };
      }
      return { stdout: JSON.stringify({ ok: true, result: { terminals: selectedTerminals } }) };
    }
    if (cmd === 'terminal create') {
      created += 1;
      const handle = `term-rot-${created}`;
      selectedTerminals.push({ handle, connected: true, writable: true, agentIdentity: 'antigravity', preview: '> ', tabId: handle, leafId: 'root' });
      return { stdout: JSON.stringify({ ok: true, result: { terminal: { handle } } }) };
    }
    if (cmd === 'terminal close') {
      const h = request.args[request.args.indexOf('--terminal') + 1];
      const idx = selectedTerminals.findIndex(item => item.handle === h);
      if (idx >= 0) selectedTerminals.splice(idx, 1);
      return { stdout: JSON.stringify({ ok: true }) };
    }
    return { stdout: JSON.stringify({ ok: true }) };
  };

  const store = new MailboxStore(join(root, 'mailbox.json'));
  const service = new WakeupRedriveService({
    store,
    drivers: [...createOrcaWakeupDrivers({ commandRunner: runner })],
    flushIntervalMs: 100_000,
  });
  t.after(async () => {
    await service.stop();
    store.close();
  });
  const sender = generateNodeKeyPair();
  const mailboxId = 'stream:standing-sync';
  store.upsertWakeTarget(mailboxId, {
    driver: 'orca-terminal',
    coalesceMs: 0,
    rotateAfterWaves: 1,
    completedWaves: 1,
    driverConfig: {
      workspacePath: standing,
      worktreeId: 'wt-selected',
      terminalHandle: 'term-selected',
      agent: 'antigravity',
    },
  });

  // 1. Active same-path sibling blocks Git sync while terminal delivery on wt-selected still proceeds.
  store.enqueue([makeSignedEnvelope(sender, 'sync-wave-1')], { mailboxId });
  const first = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(first.woke, true, first.reason);
  assert.equal(git(standing, 'rev-parse', 'HEAD'), initialSha, 'Git sync skipped while same-path sibling is active');

  // 2. Once the same-path sibling is quiescent with no open terminals, the next rotation fast-forwards HEAD.
  siblingActive = false;
  const lease = store.check({ mailboxId });
  assert.ok(lease.deliveryId);
  store.ack(lease.deliveryId, mailboxId);
  store.enqueue([makeSignedEnvelope(sender, 'sync-wave-2')], { mailboxId });
  const second = await service.flushMailbox(mailboxId, { force: true });
  assert.equal(second.woke, true, second.reason);
  assert.equal(git(standing, 'rev-parse', 'HEAD'), upstreamSha, 'Git sync succeeds once same-path sibling is quiescent');
});
