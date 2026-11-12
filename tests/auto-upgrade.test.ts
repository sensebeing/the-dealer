import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { computeRuntimeFingerprint, readDaemonFingerprint, selfUpdate } from '../packages/cli/auto-update.ts';
import { DealerNodeRuntime, ensureDaemonRunning, restartDaemon, stopDaemonIfRunning } from '../packages/cli/index.ts';

const dealerBin = fileURLToPath(new URL('../bin/dealer.js', import.meta.url));

function isolatedState() {
  const root = mkdtempSync(join(tmpdir(), 'dealer-auto-upgrade-'));
  const stateDir = join(root, 'state');
  mkdirSync(stateDir, { recursive: true });
  return { root, stateDir };
}

function git(cwd: string, args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
}

async function until<T>(check: () => T | false | undefined, description: string, timeoutMs = 5_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = check();
    if (value) {
      return value;
    }
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for ${description}`);
    }
    await new Promise(resolve => setTimeout(resolve, 25));
  }
}

async function runDealer(
  args: string[],
  stateDir: string,
  env: NodeJS.ProcessEnv = {},
): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--no-warnings', '--experimental-strip-types', dealerBin, ...args], {
      env: { ...process.env, DEALER_STATE_DIR: stateDir, ...env },
    });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', chunk => {
      stdout += chunk;
    });
    child.stderr.on('data', chunk => {
      stderr += chunk;
    });
    child.once('error', reject);
    child.once('exit', code => resolve({ code, stdout, stderr }));
  });
}

async function dealer(args: string[], stateDir: string, env: NodeJS.ProcessEnv = {}): Promise<any> {
  const result = await runDealer([...args, '--json'], stateDir, env);
  assert.equal(result.code, 0, result.stderr || result.stdout);
  assert.doesNotMatch(result.stderr, /EADDRINUSE/);
  return JSON.parse(result.stdout.trim().split('\n')[0] ?? '{}');
}

async function bootDaemon(stateDir: string, env: NodeJS.ProcessEnv = {}) {
  const status = await dealer(['node', 'up', '--profile', 'headless-relay', '--member', 'test-node'], stateDir, env);
  assert.equal(status.running, true);
  return status;
}

function readDaemonOwner(stateDir: string): { pid: number; instanceId: string } {
  return JSON.parse(readFileSync(join(stateDir, 'runtime.lock', 'owner.json'), 'utf8'));
}

interface KeepAliveSupervisor {
  armed: Promise<void>;
  /** Resolves once the replacement has claimed the lock, before it starts serving. */
  locked: Promise<{ pid: number; instanceId: string }>;
  respawned: Promise<{ pid: number; instanceId: string }>;
  kill(): Promise<void>;
}

/**
 * A launchd-style supervisor: once armed, it starts a warm replacement daemon the instant the
 * supervised daemon releases the state-directory lock. See the fixture for why it must stay warm.
 * `serveDelayMs` keeps the replacement between `runtime.start()` and `serveControl()`, which is
 * where a real cold start loads plugins and mounts networks.
 */
function startKeepAliveSupervisor(
  stateDir: string,
  options: { serveDelayMs?: number } = {},
): KeepAliveSupervisor {
  const script = fileURLToPath(new URL('./fixtures/keepalive-supervisor.mjs', import.meta.url));
  const repoRoot = fileURLToPath(new URL('../', import.meta.url));
  const args = ['--no-warnings', '--experimental-strip-types', script, repoRoot, stateDir];
  if (options.serveDelayMs !== undefined) {
    args.push(String(options.serveDelayMs));
  }
  const child = spawn(process.execPath, args, {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, DEALER_STATE_DIR: stateDir },
  });
  let output = '';
  let errors = '';
  let armedSettled = false;
  let lockedSettled = false;
  let respawnSettled = false;
  let resolveArmed!: () => void;
  let rejectArmed!: (error: Error) => void;
  let resolveLocked!: (owner: { pid: number; instanceId: string }) => void;
  let rejectLocked!: (error: Error) => void;
  let resolveRespawned!: (owner: { pid: number; instanceId: string }) => void;
  let rejectRespawned!: (error: Error) => void;
  const armed = new Promise<void>((resolve, reject) => {
    resolveArmed = resolve;
    rejectArmed = reject;
  });
  const locked = new Promise<{ pid: number; instanceId: string }>((resolve, reject) => {
    resolveLocked = resolve;
    rejectLocked = reject;
  });
  const respawned = new Promise<{ pid: number; instanceId: string }>((resolve, reject) => {
    resolveRespawned = resolve;
    rejectRespawned = reject;
  });
  // A failed attempt may end before either signal is awaited; keep the rejection handled so
  // cleanup cannot surface as an unhandled rejection.
  void armed.catch(() => {});
  void locked.catch(() => {});
  void respawned.catch(() => {});
  child.stdout!.setEncoding('utf8');
  child.stderr!.setEncoding('utf8');
  child.stdout!.on('data', chunk => {
    output += chunk;
    for (let newline = output.indexOf('\n'); newline !== -1; newline = output.indexOf('\n')) {
      const line = output.slice(0, newline).trim();
      output = output.slice(newline + 1);
      if (!line) continue;
      if (line === 'armed') {
        armedSettled = true;
        resolveArmed();
        continue;
      }
      const lockedMatch = /^locked (\d+) (\S+)$/.exec(line);
      if (lockedMatch) {
        lockedSettled = true;
        resolveLocked({ pid: Number(lockedMatch[1]), instanceId: lockedMatch[2] });
        continue;
      }
      const match = /^respawned (\d+) (\S+)$/.exec(line);
      if (match) {
        respawnSettled = true;
        resolveRespawned({ pid: Number(match[1]), instanceId: match[2] });
      }
    }
  });
  child.stderr!.on('data', chunk => {
    errors += chunk;
  });
  child.once('error', error => {
    rejectArmed(error);
    rejectLocked(error);
    rejectRespawned(error);
  });
  child.once('exit', code => {
    const error = new Error(`KeepAlive supervisor exited ${code}: ${errors.trim()}`);
    if (!armedSettled) rejectArmed(error);
    if (!lockedSettled) rejectLocked(error);
    if (!respawnSettled) rejectRespawned(error);
  });
  return {
    armed,
    locked,
    respawned,
    async kill() {
      if (child.exitCode !== null || child.signalCode !== null) {
        return;
      }
      const exited = new Promise<void>(resolve => child.once('exit', () => resolve()));
      child.kill('SIGTERM');
      const timer = setTimeout(() => child.kill('SIGKILL'), 5_000);
      await exited;
      clearTimeout(timer);
    },
  };
}

function editManifest(projectRoot: string, description: string): void {
  const path = join(projectRoot, '.dealer', 'project.json');
  const manifest = JSON.parse(readFileSync(path, 'utf8'));
  writeFileSync(path, `${JSON.stringify({ ...manifest, description }, null, 2)}\n`);
}

test('idle-gated daemon watcher restarts dealerd when a linked project .dealer manifest changes', { timeout: 90_000 }, async t => {
  const { root, stateDir } = isolatedState();
  t.after(async () => {
    await stopDaemonIfRunning(stateDir);
    rmSync(root, { recursive: true, force: true });
  });
  await bootDaemon(stateDir);
  const projectRoot = join(root, 'proj');
  mkdirSync(projectRoot, { recursive: true });
  assert.equal((await dealer(['project', 'init', '--root', projectRoot, '--id', 'demo'], stateDir)).projectId, 'demo');
  assert.equal((await dealer(['project', 'link', projectRoot], stateDir)).projectId, 'demo');
  const status1 = await dealer(['node', 'status'], stateDir);
  const before = await until(() => {
    const record = readDaemonFingerprint(stateDir);
    return record && record.fingerprint === computeRuntimeFingerprint(stateDir) ? record : undefined;
  }, 'daemon fingerprint to settle after project link');
  assert.equal(status1.runtimeFingerprint, before.fingerprint);
  editManifest(projectRoot, 'edited on disk after the project was linked');
  // No lifecycle command runs here: the daemon's own fs.watch + idle gate must restart it.
  const after = await until(() => {
    const record = readDaemonFingerprint(stateDir);
    return record && record.pid !== before.pid && record.fingerprint !== before.fingerprint ? record : undefined;
  }, 'idle-gated daemon restart after the manifest edit');
  assert.equal(after.fingerprint, computeRuntimeFingerprint(stateDir));
  const status2 = await dealer(['node', 'status'], stateDir);
  assert.equal(status2.runtimeFingerprint, after.fingerprint);
  const linked = await dealer(['project', 'status', '--root', projectRoot], stateDir);
  assert.equal(linked.projects[0].projectId, 'demo');
  assert.equal(linked.projects[0].manifest.description, 'edited on disk after the project was linked');
});

test('passive fs.watch reload and gracefulStop drain in-flight dispatches before stopping', { timeout: 90_000 }, async t => {
  const drain = isolatedState();
  const runtime = new DealerNodeRuntime({
    stateDir: drain.stateDir,
    profile: 'headless-relay',
    member: 'drain-node',
    name: 'drain-node',
  });
  t.after(async () => {
    await runtime.stop().catch(() => {});
    await stopDaemonIfRunning(drain.stateDir);
    rmSync(drain.root, { recursive: true, force: true });
  });
  await runtime.start();
  const dispatcher = (runtime.kernel.ctx.services as any).dispatcher;
  let release!: () => void;
  const gate = new Promise<void>(resolve => {
    release = resolve;
  });
  dispatcher.registerExecutor({
    name: 'slow-test',
    async execute() {
      await gate;
      return { executor: 'slow-test', executionId: 'slow-test:receipt' };
    },
  }, {});
  const settled = dispatcher.dispatch({
    id: 'slow-dispatch',
    networkId: 'drain-network',
    traceId: 'slow-dispatch',
    from: { nodeId: `ed25519:${'a'.repeat(64)}`, member: 'tester', nodeName: 'test-node' },
    routing: { mode: 'unicast' },
    kind: 'collab.request.v1',
    priority: 'normal',
    subject: 'slow dispatch',
    body: 'wait for release',
    payload: { executor: 'slow-test' },
    attachments: [],
    createdAt: new Date().toISOString(),
    signature: '',
  });
  await until(() => dispatcher.activeCount() === 1, 'slow dispatch to become active');
  assert.equal(runtime.isIdleForRestart(), false);
  await assert.rejects(() => runtime.gracefulStop({ drainTimeoutMs: 60 }), /Daemon drain timed out/);
  release();
  const receipt = await settled;
  assert.equal(receipt.stage, 'dispatched');
  await runtime.gracefulStop({ drainTimeoutMs: 1_000 });

  const passive = isolatedState();
  t.after(async () => {
    await stopDaemonIfRunning(passive.stateDir);
    rmSync(passive.root, { recursive: true, force: true });
  });
  await bootDaemon(passive.stateDir);
  const projectRoot = join(passive.root, 'proj');
  mkdirSync(projectRoot, { recursive: true });
  assert.equal((await dealer(['project', 'init', '--root', projectRoot, '--id', 'passive-demo'], passive.stateDir)).projectId, 'passive-demo');
  assert.equal((await dealer(['project', 'link', projectRoot], passive.stateDir)).projectId, 'passive-demo');
  const before = await until(() => {
    const record = readDaemonFingerprint(passive.stateDir);
    return record && record.fingerprint === computeRuntimeFingerprint(passive.stateDir) ? record : undefined;
  }, 'daemon fingerprint to settle after project link');
  editManifest(projectRoot, 'edited on disk without running any mutating CLI command');
  // Read only the fingerprint file here: `node status` would trigger the active pre-RPC
  // restart and mask whether the passive fs.watch reload actually fired.
  const after = await until(() => {
    const record = readDaemonFingerprint(passive.stateDir);
    return record && record.pid !== before.pid && record.fingerprint !== before.fingerprint ? record : undefined;
  }, 'passive fs.watch daemon reload');
  assert.notEqual(after.pid, before.pid);
  assert.equal(after.fingerprint, computeRuntimeFingerprint(passive.stateDir));
});

test('dealer self-update fast-forwards clean main, syncs skill symlinks, skips dirty/branch checkouts, and restarts stale dealerd', { timeout: 120_000 }, async t => {
  const { root, stateDir } = isolatedState();
  t.after(async () => {
    await stopDaemonIfRunning(stateDir);
    rmSync(root, { recursive: true, force: true });
  });
  const origin = join(root, 'origin.git');
  execFileSync('git', ['init', '--bare', '--initial-branch=main', origin], { stdio: ['ignore', 'pipe', 'pipe'] });
  const checkout = join(root, 'checkout');
  mkdirSync(join(checkout, 'bin'), { recursive: true });
  mkdirSync(join(checkout, 'packages'), { recursive: true });
  for (const skill of ['the-dealer', 'the-dealer-setup', 'the-dealer-extend']) {
    mkdirSync(join(checkout, 'skills', skill), { recursive: true });
    writeFileSync(join(checkout, 'skills', skill, 'SKILL.md'), `# ${skill}\n`);
  }
  writeFileSync(join(checkout, 'package.json'), `${JSON.stringify({ name: 'the-dealer', version: '0.1.0' }, null, 2)}\n`);
  writeFileSync(join(checkout, 'bin', 'dealer.js'), '#!/usr/bin/env node\n');
  writeFileSync(join(checkout, 'packages', 'index.ts'), 'export {};\n');
  git(checkout, ['init', '--initial-branch=main']);
  git(checkout, ['config', 'user.email', 'dealer-test@example.com']);
  git(checkout, ['config', 'user.name', 'Dealer Test']);
  git(checkout, ['add', '-A']);
  git(checkout, ['commit', '-m', 'initial']);
  git(checkout, ['remote', 'add', 'origin', origin]);
  git(checkout, ['push', '-u', 'origin', 'main']);
  const writer = join(root, 'upstream-writer');
  execFileSync('git', ['clone', origin, writer], { stdio: ['ignore', 'pipe', 'pipe'] });
  git(writer, ['config', 'user.email', 'dealer-test@example.com']);
  git(writer, ['config', 'user.name', 'Dealer Test']);
  writeFileSync(join(writer, 'UPSTREAM.md'), 'new upstream change\n');
  git(writer, ['add', '-A']);
  git(writer, ['commit', '-m', 'upstream change']);
  git(writer, ['push', 'origin', 'main']);
  const upstreamHead = git(writer, ['rev-parse', 'HEAD']);
  const home = join(root, 'home');
  mkdirSync(join(home, '.agents'), { recursive: true });
  mkdirSync(join(home, '.claude'), { recursive: true });
  const gemini = join(root, 'gemini');
  mkdirSync(gemini, { recursive: true });
  await bootDaemon(stateDir);
  const report = await dealer(['self-update', '--checkout-dir', checkout, '--home-dir', home, '--gemini-dir', gemini], stateDir);
  assert.equal(report.sourceStatus, 'updated');
  assert.equal(report.headSha, upstreamHead);
  assert.equal(report.restartedDaemon, true);
  for (const link of [
    join(home, '.agents', 'skills', 'the-dealer'),
    join(home, '.claude', 'skills', 'the-dealer'),
    join(gemini, 'config', 'skills', 'the-dealer'),
  ]) {
    assert.equal(lstatSync(link).isSymbolicLink(), true, link);
    assert.equal(realpathSync(link), realpathSync(join(checkout, 'skills', 'the-dealer')));
  }
  const updateOptions = { stateDir, checkoutDir: checkout, homeDir: home, geminiDir: gemini };
  const lockDir = join(stateDir, 'auto-update.lock');
  const daemonOwner = JSON.parse(readFileSync(join(stateDir, 'runtime.lock', 'owner.json'), 'utf8'));
  mkdirSync(lockDir);
  writeFileSync(join(lockDir, 'owner.json'), JSON.stringify(daemonOwner));
  assert.equal(selfUpdate(updateOptions).sourceStatus, 'skipped_locked');
  rmSync(lockDir, { recursive: true, force: true });
  mkdirSync(lockDir);
  const corrupt = selfUpdate(updateOptions);
  assert.equal(corrupt.status, 'failed');
  assert.equal(corrupt.sourceStatus, 'corrupt_lock');
  assert.match(String(corrupt.error), /owner record/);
  rmSync(lockDir, { recursive: true, force: true });
  mkdirSync(lockDir);
  const deadOwner = { pid: 999999, instanceId: 'dead-incarnation', host: 'test-host' };
  writeFileSync(join(lockDir, 'owner.json'), JSON.stringify(deadOwner));
  assert.equal(selfUpdate(updateOptions).sourceStatus, 'up_to_date');
  assert.equal(existsSync(lockDir), false);
  const reclaimDir = join(stateDir, 'auto-update.lock.reclaim');
  mkdirSync(lockDir);
  writeFileSync(join(lockDir, 'owner.json'), JSON.stringify(deadOwner));
  mkdirSync(reclaimDir);
  writeFileSync(join(reclaimDir, 'owner.json'), JSON.stringify(deadOwner));
  assert.equal(selfUpdate(updateOptions).sourceStatus, 'up_to_date');
  assert.equal(existsSync(lockDir), false);
  assert.equal(existsSync(reclaimDir), false);
  mkdirSync(lockDir);
  writeFileSync(join(lockDir, 'owner.json'), JSON.stringify(deadOwner));
  mkdirSync(reclaimDir);
  const corruptGate = selfUpdate(updateOptions);
  assert.equal(corruptGate.status, 'failed');
  assert.equal(corruptGate.sourceStatus, 'corrupt_lock');
  assert.match(String(corruptGate.error), /reclaim gate/);
  rmSync(lockDir, { recursive: true, force: true });
  rmSync(reclaimDir, { recursive: true, force: true });
  writeFileSync(join(checkout, 'untracked.txt'), 'dirty checkout\n');
  const dirty = selfUpdate(updateOptions);
  assert.equal(dirty.sourceStatus, 'skipped_dirty_checkout');
  rmSync(join(checkout, 'untracked.txt'), { force: true });
  git(checkout, ['checkout', '-b', 'feat']);
  const offMain = selfUpdate(updateOptions);
  assert.equal(offMain.sourceStatus, 'skipped_non_main_branch');
});

test('dealer node restart tolerates concurrent ensureDaemonRunning and node down stops cleanly', { timeout: 90_000 }, async t => {
  const { root, stateDir } = isolatedState();
  t.after(async () => {
    await stopDaemonIfRunning(stateDir);
    rmSync(root, { recursive: true, force: true });
  });
  await bootDaemon(stateDir);
  const [restarted, ensured] = await Promise.all([restartDaemon(stateDir), ensureDaemonRunning(stateDir)]);
  assert.ok(restarted);
  assert.ok(ensured);
  const owner = JSON.parse(readFileSync(join(stateDir, 'runtime.lock', 'owner.json'), 'utf8'));
  const record = readDaemonFingerprint(stateDir);
  assert.ok(record);
  assert.equal(record.pid, owner.pid);
  assert.equal(record.instanceId, owner.instanceId);
  assert.doesNotThrow(() => process.kill(owner.pid, 0));
  const down = await dealer(['node', 'down'], stateDir);
  assert.deepEqual({ running: down.running, stopped: down.stopped }, { running: false, stopped: true });
  const offline = await dealer(['node', 'status'], stateDir);
  assert.equal(offline.running, false);
  assert.equal(existsSync(join(stateDir, 'control.sock')), false);
  // Restarting while offline must launch a daemon, not report a deferred drain.
  const offlineRestart = await dealer(['node', 'restart'], stateDir);
  assert.equal(offlineRestart.running, true);
  assert.equal(existsSync(join(stateDir, 'control.sock')), true);

  // A killed daemon leaves control.sock behind; down must prove staleness instead of
  // reporting a live daemon from filesystem residue. Reuse the instance restarted above.
  const killed = JSON.parse(readFileSync(join(stateDir, 'runtime.lock', 'owner.json'), 'utf8'));
  process.kill(killed.pid, 'SIGKILL');
  await until(() => {
    try {
      process.kill(killed.pid, 0);
      return false;
    } catch {
      return true;
    }
  }, 'killed daemon to exit');
  assert.equal(existsSync(join(stateDir, 'control.sock')), true);
  const recovered = await dealer(['node', 'down'], stateDir);
  assert.deepEqual({ running: recovered.running, stopped: recovered.stopped }, { running: false, stopped: true });
  assert.equal(existsSync(join(stateDir, 'control.sock')), false);
});

test('node down binds shutdown to the captured incarnation and never stops a live replacement', { timeout: 90_000 }, async t => {
  const { root, stateDir } = isolatedState();
  let witness: ReturnType<typeof spawn> | undefined;
  let serving: { pid: number; instanceId: string; fingerprint: string } | undefined;
  t.after(async () => {
    if (witness?.pid) {
      try {
        process.kill(witness.pid, 'SIGKILL');
      } catch { /* exited */ }
    }
    if (serving) {
      try {
        process.kill(serving.pid, 'SIGTERM');
      } catch { /* exited */ }
      await until(() => {
        try {
          process.kill(serving!.pid, 0);
          return false;
        } catch {
          return true;
        }
      }, 'serving daemon to exit');
    }
    await stopDaemonIfRunning(stateDir, { force: true });
    rmSync(root, { recursive: true, force: true });
  });
  await bootDaemon(stateDir);
  serving = readDaemonFingerprint(stateDir)!;
  assert.ok(serving);
  // Deterministic handoff barrier: the lock still records an earlier incarnation (its process
  // is alive) while the replacement already serves the socket. Shutdown must be bound to the
  // recorded target, so the replacement rejects it and keeps serving.
  witness = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1_000)'], { stdio: 'ignore' });
  assert.ok(witness.pid);
  const lockDir = join(stateDir, 'runtime.lock');
  const stagedOwner = join(lockDir, 'owner.json.next');
  writeFileSync(stagedOwner, JSON.stringify({ pid: witness.pid, instanceId: 'prior-incarnation', host: 'supervisor-host' }));
  renameSync(stagedOwner, join(lockDir, 'owner.json'));

  const startedAt = Date.now();
  const result = await stopDaemonIfRunning(stateDir);
  const elapsed = Date.now() - startedAt;
  assert.deepEqual(
    { stopped: result.stopped, running: result.running, respawned: result.respawned },
    { stopped: true, running: true, respawned: true },
  );
  assert.ok(elapsed < 10_000, `node down waited ${elapsed}ms instead of witnessing the replacement`);
  const status = await dealer(['node', 'status'], stateDir);
  assert.equal(status.running, true);
  assert.equal(readDaemonFingerprint(stateDir)?.instanceId, serving.instanceId);

  // Restart takes the same rejected-shutdown path and confirms the surviving replacement.
  const restarted = await restartDaemon(stateDir) as { running?: boolean; deferred?: boolean; runtimeFingerprint?: string };
  assert.equal(restarted.deferred, undefined);
  assert.equal(restarted.running, true);
  assert.equal(restarted.runtimeFingerprint, serving.fingerprint);
  assert.equal(readDaemonFingerprint(stateDir)?.instanceId, serving.instanceId);
});

test('node restart confirms a KeepAlive replacement instead of reporting a false deferral', { timeout: 90_000 }, async t => {
  const { root, stateDir } = isolatedState();
  let supervisor: KeepAliveSupervisor | undefined;
  t.after(async () => {
    await supervisor?.kill();
    await stopDaemonIfRunning(stateDir, { force: true });
    rmSync(root, { recursive: true, force: true });
  });
  await bootDaemon(stateDir);
  const target = readDaemonOwner(stateDir);
  supervisor = startKeepAliveSupervisor(stateDir);
  await supervisor.armed;
  const startedAt = Date.now();
  const restarted = await restartDaemon(stateDir) as { running?: boolean; deferred?: boolean };
  const elapsed = Date.now() - startedAt;
  // The fixture's report is a real barrier: it has claimed the lock and finished serving.
  const replacement = await supervisor.respawned;
  assert.equal(restarted.deferred, undefined);
  assert.equal(restarted.running, true);
  assert.ok(elapsed < 10_000, `node restart waited ${elapsed}ms instead of confirming the replacement`);
  assert.notEqual(replacement.instanceId, target.instanceId);
  // The replacement survived the restart's shutdown phase: the live owner is still that exact
  // incarnation, not a daemon launched after terminating it.
  assert.equal(readDaemonOwner(stateDir).instanceId, replacement.instanceId);
  const status = await dealer(['node', 'status'], stateDir);
  assert.equal(status.running, true);
});

test('node restart waits for a lock-owning replacement that is still cold-starting', { timeout: 90_000 }, async t => {
  const { root, stateDir } = isolatedState();
  let supervisor: KeepAliveSupervisor | undefined;
  t.after(async () => {
    await supervisor?.kill();
    await stopDaemonIfRunning(stateDir, { force: true });
    rmSync(root, { recursive: true, force: true });
  });
  await bootDaemon(stateDir);
  const target = readDaemonOwner(stateDir);
  // The replacement holds the lock immediately but serves the control socket only after the
  // cold-start work a real daemon performs (plugin loading, network mounting). 3.5s exceeds the
  // legacy hard-coded 3s post-spawn budget that made restart fail on a duplicate launch.
  supervisor = startKeepAliveSupervisor(stateDir, { serveDelayMs: 3_500 });
  await supervisor.armed;
  const startedAt = Date.now();
  const restarted = await restartDaemon(stateDir) as {
    running?: boolean;
    deferred?: boolean;
    runtimeFingerprint?: string;
  };
  const elapsed = Date.now() - startedAt;
  const locked = await supervisor.locked;
  const replacement = await supervisor.respawned;
  assert.equal(locked.instanceId, replacement.instanceId);
  assert.notEqual(replacement.instanceId, target.instanceId);
  assert.equal(restarted.deferred, undefined);
  assert.equal(restarted.running, true);
  // The restart reports the daemon that finished the cold start, and no duplicate launcher
  // survived: the recorded owner and fingerprint both belong to that exact incarnation.
  assert.equal(readDaemonOwner(stateDir).instanceId, replacement.instanceId);
  assert.equal(readDaemonFingerprint(stateDir)?.instanceId, replacement.instanceId);
  assert.equal(restarted.runtimeFingerprint, readDaemonFingerprint(stateDir)?.fingerprint);
  assert.ok(elapsed >= 3_500, `node restart returned after ${elapsed}ms without waiting for the cold start`);
});
