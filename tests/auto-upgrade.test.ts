import test from 'node:test';
import assert from 'node:assert/strict';
import childProcess, { execFileSync, spawn } from 'node:child_process';
import { cpSync, existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, readlinkSync, realpathSync, renameSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { syncBuiltinESMExports } from 'node:module';
import { fileURLToPath } from 'node:url';
import { computeRuntimeFingerprint, isSupervisedDaemonEnv, liveDaemonOwner, readDaemonFingerprint, selfUpdate, maybeTriggerBackgroundAutoUpdate } from '../packages/cli/auto-update.ts';
import type { SelfUpdateReport } from '../packages/cli/auto-update.ts';
import { DealerNodeRuntime, ensureDaemonRunning, restartDaemon, rpc, runDealerCli, runDealerDaemon, stopDaemonIfRunning } from '../packages/cli/index.ts';
import { withOwnedLock } from '../packages/fs-atomic/index.ts';
import { holdOwnedDirectory, processOwnerRecord, releaseOwnedDirectory } from '../packages/process-lock/index.ts';

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
  bin = dealerBin,
): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--no-warnings', '--experimental-strip-types', bin, ...args], {
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

interface DaemonChild {
  child: ReturnType<typeof spawn>;
  /** Final exit code and the stderr the daemon produced; resolves on process close. */
  exited: Promise<{ code: number | null; stderr: string }>;
  status: Record<string, any>;
}

const dealerDaemonBin = fileURLToPath(new URL('../bin/dealerd.js', import.meta.url));

/** Start a daemon directly, never through `launchDaemon`, so its custody marker is ours. */
async function bootDirectDaemon(stateDir: string, env: NodeJS.ProcessEnv = {}): Promise<DaemonChild> {
  const child = spawn(
    process.execPath,
    ['--no-warnings', '--experimental-strip-types', dealerDaemonBin, '--profile', 'headless-relay', '--member', 'test-node'],
    {
      env: { ...process.env, DEALER_STATE_DIR: stateDir, DEALER_AUTO_UPDATE: '0', ...env },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  let stdout = '';
  let stderr = '';
  child.stdout!.setEncoding('utf8');
  child.stderr!.setEncoding('utf8');
  child.stdout!.on('data', chunk => { stdout += chunk; });
  child.stderr!.on('data', chunk => { stderr += chunk; });
  const exited = new Promise<{ code: number | null; stderr: string }>(resolve => {
    child.once('close', code => resolve({ code, stderr }));
  });
  await until(() => stdout.includes('\n'), 'daemon to report status');
  const status = JSON.parse(stdout.split('\n')[0]);
  assert.equal(status.running, true);
  return { child, exited, status };
}

/** Start a daemon the way a system supervisor does: `launchd` custody, never `launchDaemon`. */
function bootSupervisedDaemon(stateDir: string): Promise<DaemonChild> {
  return bootDirectDaemon(stateDir, { DEALER_DETACHED_LAUNCH: '0', XPC_SERVICE_NAME: 'ai.sensebeing.the-dealer' });
}

async function stopDaemonChild(daemon: DaemonChild | undefined): Promise<void> {
  if (!daemon || daemon.child.exitCode !== null || daemon.child.signalCode !== null) {
    return;
  }
  daemon.child.kill('SIGTERM');
  await daemon.exited;
}

function readDaemonOwner(stateDir: string): { pid: number; instanceId: string } {
  return JSON.parse(readFileSync(join(stateDir, 'runtime.lock', 'owner.json'), 'utf8'));
}

interface KeepAliveSupervisor {
  pid: number | undefined;
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
 * where a real cold start loads plugins and mounts networks. `lockDelayMs` leaves an ownerless
 * gap before the replacement claims the lock, as a freshly spawned process does.
 */
function startKeepAliveSupervisor(
  stateDir: string,
  options: { serveDelayMs?: number; lockDelayMs?: number } = {},
): KeepAliveSupervisor {
  const script = fileURLToPath(new URL('./fixtures/keepalive-supervisor.mjs', import.meta.url));
  const repoRoot = fileURLToPath(new URL('../', import.meta.url));
  const args = ['--no-warnings', '--experimental-strip-types', script, repoRoot, stateDir];
  if (options.serveDelayMs !== undefined || options.lockDelayMs !== undefined) {
    args.push(String(options.serveDelayMs ?? 0), String(options.lockDelayMs ?? 0));
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
    pid: child.pid,
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
  copyDealerCode(checkout);
  for (const skill of ['the-dealer', 'the-dealer-setup', 'the-dealer-extend']) {
    mkdirSync(join(checkout, 'skills', skill), { recursive: true });
    writeFileSync(join(checkout, 'skills', skill, 'SKILL.md'), `# ${skill}\n`);
  }
  writeFileSync(join(checkout, 'package.json'), `${JSON.stringify({ name: 'the-dealer', type: 'module', version: '1.0.0-test' }, null, 2)}\n`);
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
  writeFileSync(join(stateDir, 'auto-update.json'), JSON.stringify({ ...report, error: 'old HTTP2 failure' }));
  const recovered = selfUpdate(updateOptions);
  assert.equal(recovered.sourceStatus, 'up_to_date');
  assert.equal(Object.hasOwn(recovered, 'error'), false);
  assert.equal(Object.hasOwn(JSON.parse(readFileSync(join(stateDir, 'auto-update.json'), 'utf8')), 'error'), false);

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
  assert.equal(dirty.sourceStatus, 'up_to_date');
  assert.equal(dirty.stashedLocalChanges, true);
  rmSync(join(checkout, 'untracked.txt'), { force: true });
  git(checkout, ['checkout', '-b', 'feat']);
  const offMain = selfUpdate(updateOptions);
  assert.equal(offMain.sourceStatus, 'skipped_non_main_branch');
});

function copyDealerCode(checkout: string): void {
  for (const path of ['bin', 'packages', 'package.json']) {
    cpSync(fileURLToPath(new URL(`../${path}`, import.meta.url)), join(checkout, path), { recursive: true });
  }
}

function runnableLinkedCheckout(t: test.TestContext) {
  const { root, stateDir, checkout } = updateCheckoutFixture({ after: cleanup => t.after(async () => {
    try {
      await stopDaemonIfRunning(stateDir, { force: true });
    } finally {
      cleanup();
    }
  }) }, 'the-dealer');
  copyDealerCode(checkout);
  writeFileSync(join(checkout, 'package.json'), JSON.stringify({ name: 'the-dealer', type: 'module', version: '1.0.0-test' }));
  git(checkout, ['add', '-A']);
  git(checkout, ['commit', '-m', 'add runnable dealer']);
  git(checkout, ['push']);
  const head = git(checkout, ['rev-parse', 'HEAD']);
  const worktree = join(root, 'linked worktree');
  git(checkout, ['worktree', 'add', '-b', 'topic', worktree]);
  return { root, stateDir, checkout, head, worktree };
}

test('up-to-date self-update from a linked worktree heals main links and restarts the live stale daemon', { timeout: 90_000 }, async t => {
  const { root, stateDir, checkout, head, worktree } = runnableLinkedCheckout(t);
  const localBin = join(root, '.local', 'bin');
  mkdirSync(localBin, { recursive: true });
  symlinkSync(join(worktree, 'bin', 'dealer.js'), join(localBin, 'dealer'));
  symlinkSync(join(root, 'stale-daemon'), join(localBin, 'dealerd'));

  await bootDaemon(stateDir, { DEALER_AUTO_RESTART: '0' });
  const before = readDaemonFingerprint(stateDir)!;
  const expected = computeRuntimeFingerprint(stateDir, checkout);
  assert.notEqual(before.fingerprint, expected);
  // The file claims the daemon is current, but its live status proves otherwise.
  writeFileSync(join(stateDir, 'runtime-fingerprint.json'), JSON.stringify({ ...before, fingerprint: expected }));
  const result = await runDealer(['self-update', '--home-dir', root, '--json'], stateDir, {}, join(worktree, 'bin', 'dealer.js'));
  assert.equal(result.code, 0, result.stderr);
  const report = JSON.parse(result.stdout);
  assert.equal(report.sourceStatus, 'up_to_date');
  assert.equal(report.headSha, head);
  assert.equal(report.restartedDaemon, true);
  assert.equal(report.runtimeFingerprint, expected);
  assert.ok(Date.parse(report.lastUpdatedAt) >= Date.parse(report.lastCheckedAt));
  assert.deepEqual(report.syncedBinaries, [join(localBin, 'dealer'), join(localBin, 'dealerd')]);
  for (const bin of report.syncedBinaries) {
    assert.equal(readlinkSync(bin), join(checkout, 'bin', `${bin.endsWith('dealerd') ? 'dealerd' : 'dealer'}.js`));
  }
  assert.deepEqual(JSON.parse(readFileSync(join(stateDir, 'auto-update.json'), 'utf8')), report);
  const log = readFileSync(join(stateDir, 'auto-update.log'), 'utf8').trim().split('\n');
  assert.deepEqual(JSON.parse(log.at(-1)!), report);
  assert.notEqual(readDaemonFingerprint(stateDir)?.instanceId, before.instanceId);
  assert.equal((await dealer(['node', 'status'], stateDir)).runtimeFingerprint, expected);

  rmSync(join(localBin, 'dealer'));
  writeFileSync(join(localBin, 'dealer'), 'keep my wrapper');
  rmSync(join(localBin, 'dealerd'));
  const current = await runDealer(['self-update', '--home-dir', root, '--json'], stateDir, {}, join(worktree, 'bin', 'dealer.js'));
  assert.equal(current.code, 0, current.stderr);
  assert.equal(JSON.parse(current.stdout).restartedDaemon, false);
  assert.deepEqual(JSON.parse(current.stdout).syncedBinaries, [join(localBin, 'dealerd')]);
  assert.equal(readFileSync(join(localBin, 'dealer'), 'utf8'), 'keep my wrapper');
  assert.equal(readlinkSync(join(localBin, 'dealerd')), join(checkout, 'bin', 'dealerd.js'));
});

test('self-update preserves links and the serving daemon when the canonical checkout is conflicted', { timeout: 90_000 }, async t => {
  const { root, stateDir, checkout, head, worktree } = runnableLinkedCheckout(t);
  const localBin = join(root, '.local', 'bin');
  mkdirSync(localBin, { recursive: true });
  const worktreeDealer = join(worktree, 'bin', 'dealer.js');
  const worktreeDaemon = join(worktree, 'bin', 'dealerd.js');
  symlinkSync(worktreeDealer, join(localBin, 'dealer'));
  symlinkSync(worktreeDaemon, join(localBin, 'dealerd'));
  mkdirSync(join(root, '.agents'));
  mkdirSync(join(checkout, 'skills', 'the-dealer'), { recursive: true });
  writeFileSync(join(checkout, '.git', 'MERGE_HEAD'), `${head}\n`);

  await bootDaemon(stateDir, { DEALER_AUTO_RESTART: '0' });
  const before = readDaemonFingerprint(stateDir)!;
  assert.notEqual(before.fingerprint, computeRuntimeFingerprint(stateDir, checkout));

  const result = await runDealer(['self-update', '--home-dir', root, '--json'], stateDir, {}, worktreeDealer);
  assert.equal(result.code, 0, result.stderr);
  const report = JSON.parse(result.stdout);
  assert.equal(report.status, 'failed');
  assert.equal(report.sourceStatus, 'conflicted_checkout');
  assert.deepEqual(report.syncedBinaries, []);
  assert.deepEqual(report.syncedSkills, []);
  assert.equal(report.restartedDaemon, false);
  assert.equal(readlinkSync(join(localBin, 'dealer')), worktreeDealer);
  assert.equal(readlinkSync(join(localBin, 'dealerd')), worktreeDaemon);
  assert.equal((await dealer(['node', 'status'], stateDir)).runtimeFingerprint, before.fingerprint);
  assert.equal(readDaemonFingerprint(stateDir)?.instanceId, before.instanceId);
});

test('self-update leaves an offline node stopped when its control socket is stale', { timeout: 30_000 }, async t => {
  const { root, stateDir, checkout } = updateCheckoutFixture({ after: cleanup => t.after(async () => {
    try {
      await stopDaemonIfRunning(stateDir, { force: true });
    } finally {
      cleanup();
    }
  }) });
  copyDealerCode(checkout);
  git(checkout, ['add', '-A']);
  git(checkout, ['commit', '-m', 'add runnable dealer']);
  git(checkout, ['push']);
  const socket = join(stateDir, 'control.sock');
  const listener = spawn(process.execPath, ['-e', "require('node:net').createServer().listen(process.argv[1], () => console.log('ready'))", socket], {
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  const exited = once(listener, 'exit');
  await Promise.race([once(listener.stdout, 'data'), exited.then(() => assert.fail('listener exited before binding'))]);
  listener.kill('SIGKILL');
  await exited;
  assert.ok(existsSync(socket));
  assert.ok(!existsSync(join(stateDir, 'runtime.lock')));
  await assert.rejects(rpc(socket, ['node', 'status', '--json']), { code: 'ECONNREFUSED' });

  const report = await dealer(['self-update', '--checkout-dir', checkout, '--home-dir', root], stateDir);
  assert.equal(report.sourceStatus, 'up_to_date');
  assert.equal(report.restartedDaemon, false);
  assert.equal(readDaemonFingerprint(stateDir), undefined);
  assert.ok(!existsSync(join(stateDir, 'runtime.lock')));
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

/** A project plugin that registers a never-finishing executor and keeps one dispatch active. */
const busyDispatchPlugin = `
export default function createPlugin() {
  return {
    name: 'busy-dispatch',
    inject: ['dispatcher'],
    apply(ctx) {
      const dispatcher = ctx.services.dispatcher;
      ctx.effect(() => {
        const registration = dispatcher.registerExecutor({
          name: 'busy-test',
          execute(_batch, execution) {
            return new Promise((_resolve, reject) => {
              if (execution.signal.aborted) {
                reject(new Error('dispatch aborted'));
                return;
              }
              execution.signal.addEventListener('abort', () => reject(new Error('dispatch aborted')), { once: true });
            });
          },
        }, {});
        return () => registration.dispose();
      });
      void dispatcher.dispatch({
        id: 'busy-dispatch-envelope',
        networkId: 'local',
        traceId: 'busy-dispatch-envelope',
        from: { nodeId: 'ed25519:' + 'a'.repeat(64), member: 'tester', nodeName: 'busy' },
        routing: { mode: 'unicast' },
        kind: 'busy.test.v1',
        priority: 'normal',
        subject: 'busy',
        body: 'keep the dispatcher busy',
        payload: { executor: 'busy-test' },
        attachments: [],
        createdAt: new Date().toISOString(),
        signature: '',
      }).catch(() => {});
    },
  };
}
`;

/** A project plugin whose kernel cleanup outlives the legacy 2s shutdown assumption. */
const slowStopPlugin = `
export default function createPlugin() {
  return {
    name: 'slow-stop',
    apply(ctx) {
      ctx.effect(() => async () => {
        await new Promise(resolve => setTimeout(resolve, 2500));
      });
    },
  };
}
`;

async function linkProjectPlugin(
  stateDir: string,
  projectRoot: string,
  projectId: string,
  plugin: { name: string; source: string },
): Promise<void> {
  mkdirSync(projectRoot, { recursive: true });
  assert.equal((await dealer(['project', 'init', '--root', projectRoot, '--id', projectId], stateDir)).projectId, projectId);
  writeFileSync(join(projectRoot, '.dealer', 'plugins', `${plugin.name}.ts`), plugin.source);
  const manifestPath = join(projectRoot, '.dealer', 'project.json');
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  writeFileSync(manifestPath, `${JSON.stringify({
    ...manifest,
    plugins: [{ name: plugin.name, module: `./.dealer/plugins/${plugin.name}.ts`, config: {} }],
  }, null, 2)}\n`);
  assert.equal((await dealer(['project', 'link', projectRoot], stateDir)).projectId, projectId);
}

test('a slow stop tears down watchers and idle callbacks before the dispatcher unloads', { timeout: 120_000 }, async t => {
  const { root, stateDir } = isolatedState();
  const savedEnv = {
    stateDir: process.env.DEALER_STATE_DIR,
    detached: process.env.DEALER_DETACHED_LAUNCH,
    autoUpdate: process.env.DEALER_AUTO_UPDATE,
    autoRestart: process.env.DEALER_AUTO_RESTART,
  };
  process.env.DEALER_STATE_DIR = stateDir;
  process.env.DEALER_DETACHED_LAUNCH = '1';
  process.env.DEALER_AUTO_UPDATE = '0';
  delete process.env.DEALER_AUTO_RESTART;
  // Observe self-restart spawns without letting the test spawn a competing daemon.
  const restartSpawns: string[][] = [];
  const originalSpawn = childProcess.spawn;
  const mocked = t.mock.method(childProcess, 'spawn', (command: string, args: string[], options: childProcess.SpawnOptions) => {
    if (args?.[3] === 'node' && args?.[4] === 'restart') {
      restartSpawns.push(args);
      return { once() {}, unref() {} };
    }
    return originalSpawn(command, args, options);
  });
  syncBuiltinESMExports();
  let runtime: DealerNodeRuntime | undefined;
  t.after(async () => {
    await runtime?.stop().catch(() => {});
    mocked.mock.restore();
    syncBuiltinESMExports();
    for (const [key, value] of [
      ['DEALER_STATE_DIR', savedEnv.stateDir],
      ['DEALER_DETACHED_LAUNCH', savedEnv.detached],
      ['DEALER_AUTO_UPDATE', savedEnv.autoUpdate],
      ['DEALER_AUTO_RESTART', savedEnv.autoRestart],
    ] as const) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    await stopDaemonIfRunning(stateDir, { force: true }).catch(() => {});
    rmSync(root, { recursive: true, force: true });
  });
  runtime = await runDealerDaemon(['--profile', 'headless-relay', '--member', 'test-node']);
  assert.equal(runtime.supervised, false);
  const projectRoot = join(root, 'slow-project');
  mkdirSync(projectRoot, { recursive: true });
  assert.equal((await dealer(['project', 'init', '--root', projectRoot, '--id', 'slow'], stateDir)).projectId, 'slow');
  writeFileSync(join(projectRoot, '.dealer', 'plugins', 'slow-stop.ts'), slowStopPlugin);
  const manifestPath = join(projectRoot, '.dealer', 'project.json');
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  writeFileSync(manifestPath, `${JSON.stringify({
    ...manifest,
    plugins: [{ name: 'slow-stop', module: './.dealer/plugins/slow-stop.ts', config: {} }],
  }, null, 2)}\n`);
  await runtime.linkProject(projectRoot);
  await until(() => {
    const record = readDaemonFingerprint(stateDir);
    return record && record.fingerprint === computeRuntimeFingerprint(stateDir) ? record : undefined;
  }, 'daemon fingerprint to settle after linking the slow-stop project');
  // Capture the idle hook the daemon registered. A wake that arrives after teardown must reach
  // it only through the guarded path, never by touching an unloaded kernel service.
  const lateControlIdle = runtime.onControlIdle;
  assert.equal(typeof lateControlIdle, 'function');
  let stoppingListenerRuns = 0;
  runtime.onStopping(() => { stoppingListenerRuns++; });
  // Arm the 400ms watch debounce and commit the stop before it fires; the slow plugin keeps the
  // kernel and dispatcher alive long enough that a leftover timer would call into them.
  editManifest(projectRoot, 'edited while the daemon is about to stop');
  await new Promise(resolve => setTimeout(resolve, 150));
  const stopping = runtime.stop();
  await new Promise(resolve => setTimeout(resolve, 700));
  assert.deepEqual(restartSpawns, []);
  await stopping;
  assert.equal(stoppingListenerRuns, 1);
  assert.equal(runtime.isIdleForRestart(), false);
  assert.doesNotThrow(() => lateControlIdle());
  assert.equal(runtime.onControlIdle, undefined);
  assert.equal(runtime.onConfigPersisted, undefined);
  // A listener registered after the commit runs immediately, exactly once.
  let lateListenerRuns = 0;
  runtime.onStopping(() => { lateListenerRuns++; });
  assert.equal(lateListenerRuns, 1);
  await new Promise(resolve => setTimeout(resolve, 100));
  assert.equal(lateListenerRuns, 1);
});

test('a supervised daemon drains in process for its supervisor instead of spawning a competing restart', { timeout: 60_000 }, async t => {
  const { root, stateDir } = isolatedState();
  const savedEnv = {
    stateDir: process.env.DEALER_STATE_DIR,
    detached: process.env.DEALER_DETACHED_LAUNCH,
    supervised: process.env.DEALER_SUPERVISED,
    autoUpdate: process.env.DEALER_AUTO_UPDATE,
  };
  process.env.DEALER_STATE_DIR = stateDir;
  delete process.env.DEALER_DETACHED_LAUNCH;
  process.env.DEALER_SUPERVISED = '1';
  process.env.DEALER_AUTO_UPDATE = '0';
  const restartSpawns: string[][] = [];
  const originalSpawn = childProcess.spawn;
  const mocked = t.mock.method(childProcess, 'spawn', (command: string, args: string[], options: childProcess.SpawnOptions) => {
    if (args?.[3] === 'node' && args?.[4] === 'restart') {
      restartSpawns.push(args);
      return { once() {}, unref() {} };
    }
    return originalSpawn(command, args, options);
  });
  syncBuiltinESMExports();
  // Capture the supervised exit instead of letting it end the test process.
  const realExit = process.exit;
  let exitCode: number | undefined;
  process.exit = ((code?: number) => {
    exitCode = code ?? 0;
    return undefined as never;
  }) as typeof process.exit;
  let runtime: DealerNodeRuntime | undefined;
  t.after(async () => {
    process.exit = realExit;
    await runtime?.stop().catch(() => {});
    mocked.mock.restore();
    syncBuiltinESMExports();
    for (const [key, value] of [
      ['DEALER_STATE_DIR', savedEnv.stateDir],
      ['DEALER_DETACHED_LAUNCH', savedEnv.detached],
      ['DEALER_SUPERVISED', savedEnv.supervised],
      ['DEALER_AUTO_UPDATE', savedEnv.autoUpdate],
    ] as const) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    await stopDaemonIfRunning(stateDir, { force: true }).catch(() => {});
    rmSync(root, { recursive: true, force: true });
  });
  runtime = await runDealerDaemon(['--profile', 'headless-relay', '--member', 'test-node']);
  assert.equal(runtime.supervised, true);
  const projectRoot = join(root, 'reload-project');
  mkdirSync(projectRoot, { recursive: true });
  assert.equal((await dealer(['project', 'init', '--root', projectRoot, '--id', 'reload'], stateDir)).projectId, 'reload');
  await runtime.linkProject(projectRoot);
  await until(() => {
    const record = readDaemonFingerprint(stateDir);
    return record && record.fingerprint === computeRuntimeFingerprint(stateDir) ? record : undefined;
  }, 'daemon fingerprint to settle after linking the reload project');
  editManifest(projectRoot, 'edited to trigger the supervised reload');
  await until(() => exitCode !== undefined, 'supervised daemon to drain and exit for its supervisor', 10_000);
  assert.equal(exitCode, 0);
  assert.deepEqual(restartSpawns, []);
  assert.equal(runtime.isStoppingOrDraining(), true);
  assert.equal(runtime.isIdleForRestart(), false);
});

test('an external node down during a slow stop neither crashes the daemon nor leaves a duplicate restart', { timeout: 120_000 }, async t => {
  const { root, stateDir } = isolatedState();
  let daemon: DaemonChild | undefined;
  t.after(async () => {
    await stopDaemonChild(daemon);
    await stopDaemonIfRunning(stateDir, { force: true }).catch(() => {});
    rmSync(root, { recursive: true, force: true });
  });
  daemon = await bootDirectDaemon(stateDir, { DEALER_DETACHED_LAUNCH: '1' });
  assert.equal(daemon.status.supervised, false);
  await linkProjectPlugin(stateDir, join(root, 'slow-project'), 'slow', { name: 'slow-stop', source: slowStopPlugin });
  await until(() => {
    const record = readDaemonFingerprint(stateDir);
    return record && record.fingerprint === computeRuntimeFingerprint(stateDir) ? record : undefined;
  }, 'daemon fingerprint to settle after linking the slow-stop project');
  const projectRoot = join(root, 'slow-project');
  editManifest(projectRoot, 'edited just before the external shutdown');
  await new Promise(resolve => setTimeout(resolve, 150));
  const down = await rpc(join(stateDir, 'control.sock'), ['node', 'down', '--json'], { inactivityMs: 20_000 });
  const answer = JSON.parse(down.stdout.join(''));
  assert.deepEqual(
    { ok: answer.ok, stopping: answer.stopping, supervised: answer.supervised },
    { ok: true, stopping: true, supervised: false },
  );
  const exit = await daemon.exited;
  assert.equal(exit.code, 0, exit.stderr);
  assert.doesNotMatch(exit.stderr, /Service unavailable/);
  // A restart spawned by the settling control request would claim the freed lock right after
  // the slow stop ends; none may exist once that window has passed.
  await new Promise(resolve => setTimeout(resolve, 2_000));
  assert.equal(liveDaemonOwner(stateDir), undefined);
  assert.equal(existsSync(join(stateDir, 'control.sock')), false);
});

test('a deferred detached restart re-arms the pending reload when idle precedes its exit', { timeout: 60_000 }, async t => {
  const { root, stateDir } = isolatedState();
  const savedEnv = {
    stateDir: process.env.DEALER_STATE_DIR,
    detached: process.env.DEALER_DETACHED_LAUNCH,
    autoUpdate: process.env.DEALER_AUTO_UPDATE,
    autoRestart: process.env.DEALER_AUTO_RESTART,
  };
  process.env.DEALER_STATE_DIR = stateDir;
  process.env.DEALER_DETACHED_LAUNCH = '1';
  process.env.DEALER_AUTO_UPDATE = '0';
  delete process.env.DEALER_AUTO_RESTART;
  const restartSpawns: string[][] = [];
  let restartExit: (() => void) | undefined;
  const originalSpawn = childProcess.spawn;
  const mocked = t.mock.method(childProcess, 'spawn', (command: string, args: string[], options: childProcess.SpawnOptions) => {
    if (args?.[3] === 'node' && args?.[4] === 'restart') {
      restartSpawns.push(args);
      return {
        once(event: string, listener: () => void) {
          if (event === 'exit') restartExit = listener;
        },
        unref() {},
      };
    }
    return originalSpawn(command, args, options);
  });
  syncBuiltinESMExports();
  let runtime: DealerNodeRuntime | undefined;
  t.after(async () => {
    await runtime?.stop().catch(() => {});
    mocked.mock.restore();
    syncBuiltinESMExports();
    for (const [key, value] of [
      ['DEALER_STATE_DIR', savedEnv.stateDir],
      ['DEALER_DETACHED_LAUNCH', savedEnv.detached],
      ['DEALER_AUTO_UPDATE', savedEnv.autoUpdate],
      ['DEALER_AUTO_RESTART', savedEnv.autoRestart],
    ] as const) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    await stopDaemonIfRunning(stateDir, { force: true }).catch(() => {});
    rmSync(root, { recursive: true, force: true });
  });
  runtime = await runDealerDaemon(['--profile', 'headless-relay', '--member', 'test-node']);
  const projectRoot = join(root, 'reload-project');
  mkdirSync(projectRoot, { recursive: true });
  assert.equal((await dealer(['project', 'init', '--root', projectRoot, '--id', 'reload'], stateDir)).projectId, 'reload');
  await runtime.linkProject(projectRoot);
  await until(() => {
    const record = readDaemonFingerprint(stateDir);
    return record && record.fingerprint === computeRuntimeFingerprint(stateDir) ? record : undefined;
  }, 'daemon fingerprint to settle after linking the reload project');
  editManifest(projectRoot, 'edited to arm the pending reload');
  await until(() => restartSpawns.length === 1, 'self-restart to spawn for the pending reload');
  // The final dispatch finishes while the restart child is still running, so its idle event is
  // swallowed by the restart flag; only the child's exit can re-arm the reload.
  const dispatcher = (runtime.kernel.ctx.services as any).dispatcher;
  let release!: () => void;
  const holding = new Promise<void>(resolve => {
    release = resolve;
  });
  dispatcher.registerExecutor({
    name: 'deferred-restart-test',
    async execute() {
      await holding;
      return { executor: 'deferred-restart-test', executionId: 'deferred-restart-test:receipt' };
    },
  }, {});
  const settled = dispatcher.dispatch({
    id: 'deferred-restart-dispatch',
    networkId: 'local',
    traceId: 'deferred-restart-dispatch',
    from: { nodeId: `ed25519:${'a'.repeat(64)}`, member: 'tester', nodeName: 'test-node' },
    routing: { mode: 'unicast' },
    kind: 'collab.request.v1',
    priority: 'normal',
    subject: 'deferred restart dispatch',
    body: 'hold until the restart child is about to exit',
    payload: { executor: 'deferred-restart-test' },
    attachments: [],
    createdAt: new Date().toISOString(),
    signature: '',
  });
  await until(() => dispatcher.activeCount() === 1, 'dispatch to become active');
  release();
  await settled;
  await until(() => dispatcher.activeCount() === 0, 'dispatch to settle');
  // Drain every watcher debounce the dispatch's state-directory writes armed: a late one must
  // not spawn the second restart and hide whether the child's exit actually re-armed the reload.
  await new Promise(resolve => setTimeout(resolve, 600));
  assert.equal(restartSpawns.length, 1);
  assert.equal(typeof restartExit, 'function');
  restartExit!();
  await until(() => restartSpawns.length === 2, 'reload to be re-armed after the deferred restart exited');
});

test('supervisor custody is decided from the process environment and frozen on the runtime', async t => {
  assert.equal(isSupervisedDaemonEnv({ XPC_SERVICE_NAME: 'ai.sensebeing.the-dealer' }), true);
  assert.equal(isSupervisedDaemonEnv({ XPC_SERVICE_NAME: '  ai.sensebeing.the-dealer  ' }), true);
  assert.equal(isSupervisedDaemonEnv({ INVOCATION_ID: 'systemd-invocation' }), true);
  assert.equal(isSupervisedDaemonEnv({ DEALER_SUPERVISED: '1' }), true);
  assert.equal(isSupervisedDaemonEnv({ XPC_SERVICE_NAME: 'application.com.orca' }), false);
  assert.equal(isSupervisedDaemonEnv({ XPC_SERVICE_NAME: '0' }), false);
  assert.equal(isSupervisedDaemonEnv({ XPC_SERVICE_NAME: '   ' }), false);
  assert.equal(isSupervisedDaemonEnv({ INVOCATION_ID: '   ' }), false);
  assert.equal(isSupervisedDaemonEnv({ DEALER_SUPERVISED: '0', INVOCATION_ID: 'systemd-invocation' }), false);
  assert.equal(isSupervisedDaemonEnv({ DEALER_DETACHED_LAUNCH: '1', XPC_SERVICE_NAME: 'ai.sensebeing.the-dealer' }), false);
  assert.equal(isSupervisedDaemonEnv({}), false);

  const { root, stateDir } = isolatedState();
  const runtime = new DealerNodeRuntime({ stateDir, profile: 'headless-relay', member: 'test-node', supervised: true });
  const savedXpc = process.env.XPC_SERVICE_NAME;
  t.after(async () => {
    if (savedXpc === undefined) delete process.env.XPC_SERVICE_NAME;
    else process.env.XPC_SERVICE_NAME = savedXpc;
    await runtime.stop().catch(() => {});
    rmSync(root, { recursive: true, force: true });
  });
  process.env.XPC_SERVICE_NAME = 'ai.sensebeing.the-dealer';
  const probed = new DealerNodeRuntime({ stateDir: join(root, 'probe') });
  assert.equal(probed.supervised, true);
  assert.equal(new DealerNodeRuntime({ stateDir, supervised: false }).supervised, false);
  await runtime.start();
  assert.equal(runtime.status().supervised, true);
  await runtime.serveControl(async () => ({ code: 0, stdout: [], stderr: [] }));
  assert.equal(readDaemonFingerprint(stateDir)?.supervised, true);
});

test('restartDaemon waits through a supervised cold respawn gap instead of stealing the supervisor lock', { timeout: 120_000 }, async t => {
  const { root, stateDir } = isolatedState();
  let daemon: DaemonChild | undefined;
  let supervisor: KeepAliveSupervisor | undefined;
  t.after(async () => {
    await supervisor?.kill();
    await stopDaemonChild(daemon);
    await stopDaemonIfRunning(stateDir, { force: true }).catch(() => {});
    rmSync(root, { recursive: true, force: true });
  });
  daemon = await bootSupervisedDaemon(stateDir);
  assert.equal(daemon.status.supervised, true);
  const target = readDaemonOwner(stateDir);
  // The replacement is a freshly spawned process: it owns the lock only after the gap. The gap
  // is wide enough that a detached launcher would win it, so only an actual wait can succeed.
  supervisor = startKeepAliveSupervisor(stateDir, { lockDelayMs: 1_000 });
  await supervisor.armed;
  const began = Date.now();
  const restarted = await restartDaemon(stateDir, { supervisorWaitMs: 8_000 }) as {
    running?: boolean;
    deferred?: boolean;
    runtimeFingerprint?: string;
  };
  const elapsed = Date.now() - began;
  const replacement = await supervisor.respawned;
  assert.equal(restarted.deferred, undefined);
  assert.equal(restarted.running, true);
  assert.notEqual(replacement.instanceId, target.instanceId);
  // The supervisor's replacement owns the state directory: no detached launcher won the gap.
  assert.equal(readDaemonOwner(stateDir).pid, supervisor.pid);
  assert.equal(readDaemonOwner(stateDir).instanceId, replacement.instanceId);
  assert.equal(restarted.runtimeFingerprint, readDaemonFingerprint(stateDir)?.fingerprint);
  assert.ok(elapsed >= 1_000, `restart returned after ${elapsed}ms without waiting through the respawn gap`);
  assert.equal((await dealer(['node', 'status'], stateDir)).running, true);
});

test('restartDaemon falls back to a detached launch when no supervisor respawns in the wait window', { timeout: 120_000 }, async t => {
  const { root, stateDir } = isolatedState();
  let daemon: DaemonChild | undefined;
  t.after(async () => {
    await stopDaemonChild(daemon);
    await stopDaemonIfRunning(stateDir, { force: true }).catch(() => {});
    rmSync(root, { recursive: true, force: true });
  });
  daemon = await bootSupervisedDaemon(stateDir);
  const target = readDaemonOwner(stateDir);
  const began = Date.now();
  const restarted = await restartDaemon(stateDir, { supervisorWaitMs: 400 }) as { running?: boolean; deferred?: boolean };
  const elapsed = Date.now() - began;
  assert.equal(restarted.deferred, undefined);
  assert.equal(restarted.running, true);
  assert.ok(elapsed >= 400, `restart returned after ${elapsed}ms without honoring the supervisor wait window`);
  const owner = readDaemonOwner(stateDir);
  assert.notEqual(owner.instanceId, target.instanceId);
  assert.notEqual(owner.pid, daemon.child.pid);
  // The fallback is a detached CLI launch, so the replacement must not claim custody itself.
  assert.equal(readDaemonFingerprint(stateDir)?.supervised, false);
  assert.equal((await dealer(['node', 'status'], stateDir)).running, true);
});

test('self-update waits through a draining 503 predecessor and still records a completed update', { timeout: 60_000 }, async t => {
  const { root, stateDir, checkout } = updateCheckoutFixture(t);
  // The predecessor still owns the lock (and therefore stays "live") while it drains, answering
  // every control request with 503 until it exits.
  const lock = join(stateDir, 'runtime.lock');
  mkdirSync(lock);
  writeFileSync(join(lock, 'owner.json'), JSON.stringify(processOwnerRecord()));
  const ownedStateDir = realpathSync(stateDir);
  holdOwnedDirectory(ownedStateDir);
  t.after(() => releaseOwnedDirectory(ownedStateDir));
  const expectedFingerprint = computeRuntimeFingerprint(stateDir, checkout);
  let probes = 0;
  const server = createServer((request, response) => {
    request.resume();
    request.on('end', () => {
      probes++;
      // Every probe until the first is the draining predecessor; the rest see its replacement.
      if (probes === 1) {
        response.writeHead(503, { 'content-type': 'application/json' }).end(JSON.stringify({ error: 'Daemon is draining' }));
        return;
      }
      const status = { running: true, runtimeFingerprint: expectedFingerprint };
      response.writeHead(200, { 'content-type': 'application/x-ndjson' });
      response.end(`${JSON.stringify({ type: 'result', result: { code: 0, stdout: [JSON.stringify(status)], stderr: [] } })}\n`);
    });
  });
  server.listen(join(stateDir, 'control.sock'));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  await once(server, 'listening');
  const result = await runDealer(['self-update', '--checkout-dir', checkout, '--home-dir', root, '--json'], stateDir);
  assert.equal(result.code, 0, result.stderr);
  const report = JSON.parse(result.stdout);
  assert.equal(report.status, 'complete');
  assert.equal(report.sourceStatus, 'up_to_date');
  assert.equal(report.restartedDaemon, false);
  assert.equal(Object.hasOwn(report, 'error'), false);
  assert.ok(probes >= 2, `daemon was probed ${probes} times without observing the draining predecessor`);
  const persisted = JSON.parse(readFileSync(join(stateDir, 'auto-update.json'), 'utf8'));
  assert.equal(persisted.status, 'complete');
  assert.equal(Object.hasOwn(persisted, 'error'), false);
});

test('self-update still surfaces a non-draining control status as a failure', async t => {
  const { root, stateDir, checkout } = updateCheckoutFixture(t);
  const error = 'bad control request';
  const server = createServer((request, response) => {
    request.resume();
    request.on('end', () => response.writeHead(400).end(error));
  });
  server.listen(join(stateDir, 'control.sock'));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  await once(server, 'listening');
  const result = await runDealer(['self-update', '--checkout-dir', checkout, '--home-dir', root, '--json'], stateDir);
  assert.equal(result.code, 1);
  assert.deepEqual(JSON.parse(result.stderr), { error });
  const report = JSON.parse(readFileSync(join(stateDir, 'auto-update.json'), 'utf8'));
  assert.equal(report.status, 'failed');
  assert.equal(report.error, error);
});

test('self-update verifies a supervised replacement across an ownerless respawn gap', { timeout: 60_000 }, async t => {
  const { root, stateDir, checkout } = updateCheckoutFixture(t);
  const lock = join(stateDir, 'runtime.lock');
  mkdirSync(lock);
  const draining = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1_000)'], { stdio: 'ignore' });
  const replacement = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1_000)'], { stdio: 'ignore' });
  t.after(() => {
    for (const witness of [draining.pid, replacement.pid]) {
      try {
        process.kill(witness!, 'SIGKILL');
      } catch { /* exited */ }
    }
  });
  const expectedFingerprint = computeRuntimeFingerprint(stateDir, checkout);
  writeFileSync(join(lock, 'owner.json'), JSON.stringify({ pid: draining.pid, instanceId: 'draining-incarnation', host: 'supervisor-host' }));
  writeFileSync(join(stateDir, 'runtime-fingerprint.json'), JSON.stringify({
    pid: draining.pid,
    instanceId: 'draining-incarnation',
    fingerprint: 'previous-code',
    startedAt: new Date().toISOString(),
    supervised: true,
  }));
  let serving = false;
  let readyProbes = 0;
  let firstRequestAt = 0;
  const server = createServer((request, response) => {
    request.resume();
    request.on('end', () => {
      if (!firstRequestAt) {
        firstRequestAt = Date.now();
        // The witnessed supervised owner exits early; its cold replacement claims the lock only
        // after an ownerless gap that outlives the first startup budget.
        setTimeout(() => {
          try {
            process.kill(draining.pid!, 'SIGKILL');
          } catch { /* exited */ }
        }, 200);
        setTimeout(() => {
          serving = true;
          const staged = join(lock, 'owner.json.next');
          writeFileSync(staged, JSON.stringify({ pid: replacement.pid, instanceId: 'replacement-incarnation', host: 'supervisor-host' }));
          renameSync(staged, join(lock, 'owner.json'));
          writeFileSync(join(stateDir, 'runtime-fingerprint.json'), JSON.stringify({
            pid: replacement.pid,
            instanceId: 'replacement-incarnation',
            fingerprint: expectedFingerprint,
            startedAt: new Date().toISOString(),
            supervised: true,
          }));
        }, 1_400);
      }
      if (!serving) {
        response.writeHead(503, { 'content-type': 'application/json' }).end(JSON.stringify({ error: 'Daemon is draining' }));
        return;
      }
      readyProbes++;
      const status = { running: true, runtimeFingerprint: expectedFingerprint };
      response.writeHead(200, { 'content-type': 'application/x-ndjson' });
      response.end(`${JSON.stringify({ type: 'result', result: { code: 0, stdout: [JSON.stringify(status)], stderr: [] } })}\n`);
    });
  });
  server.listen(join(stateDir, 'control.sock'));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  await once(server, 'listening');
  const stdout: string[] = [];
  const stderr: string[] = [];
  const began = Date.now();
  const code = await runDealerCli(['self-update', '--checkout-dir', checkout, '--home-dir', root, '--json'], {
    stateDir,
    daemonStartupTimeoutMs: 400,
    daemonSupervisorWaitMs: 8_000,
    stdout: line => stdout.push(line),
    stderr: line => stderr.push(line),
  });
  const elapsed = Date.now() - began;
  assert.equal(code, 0, stderr.join('\n'));
  const report = JSON.parse(stdout.join(''));
  assert.equal(report.status, 'complete');
  assert.equal(report.sourceStatus, 'up_to_date');
  assert.equal(report.restartedDaemon, false);
  assert.equal(Object.hasOwn(report, 'error'), false);
  assert.ok(readyProbes >= 1, 'self-update reported success without verifying the replacement');
  assert.ok(elapsed >= 1_000, `self-update returned after ${elapsed}ms without waiting for the respawn gap`);
});

test('self-update reports a bounded failure when a supervised replacement never becomes ready', { timeout: 60_000 }, async t => {
  const { root, stateDir, checkout } = updateCheckoutFixture(t);
  const lock = join(stateDir, 'runtime.lock');
  mkdirSync(lock);
  const draining = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1_000)'], { stdio: 'ignore' });
  t.after(() => {
    try {
      process.kill(draining.pid!, 'SIGKILL');
    } catch { /* exited */ }
  });
  writeFileSync(join(lock, 'owner.json'), JSON.stringify({ pid: draining.pid, instanceId: 'draining-incarnation', host: 'supervisor-host' }));
  writeFileSync(join(stateDir, 'runtime-fingerprint.json'), JSON.stringify({
    pid: draining.pid,
    instanceId: 'draining-incarnation',
    fingerprint: 'previous-code',
    startedAt: new Date().toISOString(),
    supervised: true,
  }));
  let firstRequestAt = 0;
  const server = createServer((request, response) => {
    request.resume();
    request.on('end', () => {
      if (!firstRequestAt) {
        firstRequestAt = Date.now();
        setTimeout(() => {
          try {
            process.kill(draining.pid!, 'SIGKILL');
          } catch { /* exited */ }
        }, 100);
      }
      response.writeHead(503, { 'content-type': 'application/json' }).end(JSON.stringify({ error: 'Daemon is draining' }));
    });
  });
  server.listen(join(stateDir, 'control.sock'));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  await once(server, 'listening');
  const stdout: string[] = [];
  const stderr: string[] = [];
  const began = Date.now();
  const code = await runDealerCli(['self-update', '--checkout-dir', checkout, '--home-dir', root, '--json'], {
    stateDir,
    daemonStartupTimeoutMs: 200,
    daemonSupervisorWaitMs: 300,
    stdout: line => stdout.push(line),
    stderr: line => stderr.push(line),
  });
  const elapsed = Date.now() - began;
  assert.equal(code, 1);
  const error = `Timed out waiting for a supervisor replacement in ${stateDir}`;
  assert.deepEqual(stdout, []);
  assert.deepEqual(stderr.map(message => JSON.parse(message)), [{ error }]);
  assert.ok(elapsed >= 300, `self-update failed after ${elapsed}ms without honoring the supervisor wait budget`);
  const report = JSON.parse(readFileSync(join(stateDir, 'auto-update.json'), 'utf8'));
  assert.equal(report.status, 'failed');
  assert.equal(report.sourceStatus, 'up_to_date');
  assert.equal(report.error, error);
  assert.equal(report.restartedDaemon, false);
});

test('node restart reports the daemon\'s drain deferral and --force ends that verified incarnation', { timeout: 180_000 }, async t => {
  const { root, stateDir } = isolatedState();
  t.after(async () => {
    await stopDaemonIfRunning(stateDir, { force: true }).catch(() => {});
    rmSync(root, { recursive: true, force: true });
  });
  await bootDaemon(stateDir);
  await linkProjectPlugin(stateDir, join(root, 'busy-project'), 'busy', { name: 'busy-dispatch', source: busyDispatchPlugin });
  const serving = readDaemonOwner(stateDir);

  const began = Date.now();
  const restarted = await restartDaemon(stateDir) as {
    running?: boolean;
    stopped?: boolean;
    restarted?: boolean;
    deferred?: boolean;
  };
  const elapsed = Date.now() - began;
  // The daemon itself decided its drain could not quiesce; restart must report that decision
  // instead of guessing from a timeout after the fact.
  assert.deepEqual(
    {
      running: restarted.running,
      stopped: restarted.stopped,
      restarted: restarted.restarted,
      deferred: restarted.deferred,
    },
    { running: true, stopped: false, restarted: false, deferred: true },
  );
  assert.ok(elapsed >= 14_000, `restart returned after ${elapsed}ms without waiting for the drain budget`);
  assert.ok(elapsed < 60_000, `restart waited ${elapsed}ms after the daemon reported its decision`);
  // The deferred daemon kept serving as the same incarnation.
  assert.equal(readDaemonOwner(stateDir).instanceId, serving.instanceId);
  const status = await dealer(['node', 'status'], stateDir);
  assert.equal(status.running, true);

  const forced = await stopDaemonIfRunning(stateDir, { force: true });
  assert.deepEqual({ stopped: forced.stopped, running: forced.running }, { stopped: true, running: false });
  await until(() => !existsSync(join(stateDir, 'control.sock')), 'forced shutdown to remove the control socket');
});

test('a completed drain waits out a slow stop hook without reporting a false deferral', { timeout: 120_000 }, async t => {
  const { root, stateDir } = isolatedState();
  t.after(async () => {
    await stopDaemonIfRunning(stateDir, { force: true }).catch(() => {});
    rmSync(root, { recursive: true, force: true });
  });
  await bootDaemon(stateDir);
  await linkProjectPlugin(stateDir, join(root, 'slow-stop-project'), 'slow-stop', { name: 'slow-stop', source: slowStopPlugin });
  const before = readDaemonOwner(stateDir);

  const began = Date.now();
  const restarted = await restartDaemon(stateDir) as { running?: boolean; deferred?: boolean };
  const elapsed = Date.now() - began;
  assert.equal(restarted.deferred, undefined);
  assert.equal(restarted.running, true);
  assert.ok(elapsed >= 2_500, `restart returned after ${elapsed}ms without waiting for the slow stop hook`);
  assert.notEqual(readDaemonOwner(stateDir).instanceId, before.instanceId);
  const status = await dealer(['node', 'status'], stateDir);
  assert.equal(status.running, true);
});

function captureBackgroundUpdates(t: test.TestContext): string[] {
  const previous = { enabled: process.env.DEALER_AUTO_UPDATE, disabled: process.env.DEALER_DISABLE_AUTO_UPDATE };
  process.env.DEALER_AUTO_UPDATE = '1';
  delete process.env.DEALER_DISABLE_AUTO_UPDATE;
  t.after(() => {
    for (const [key, value] of [['DEALER_AUTO_UPDATE', previous.enabled], ['DEALER_DISABLE_AUTO_UPDATE', previous.disabled]] as const) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    mocked.mock.restore();
    syncBuiltinESMExports();
  });
  const calls: string[] = [];
  const original = childProcess.spawn;
  const mocked = t.mock.method(childProcess, 'spawn', (command: string, args: string[], options: childProcess.SpawnOptions) => {
    if (args?.[3] === 'self-update') {
      calls.push(options.env!.DEALER_STATE_DIR!);
      return { on() {}, unref() {} };
    }
    return original(command, args, options);
  });
  syncBuiltinESMExports();
  return calls;
}

test('background auto-update retries errors after fifteen minutes and successes after six hours', t => {
  const calls = captureBackgroundUpdates(t);
  for (const [sourceStatus, minutes, expected] of [
    ['error', 14, 0], ['error', 16, 1], ['up_to_date', 16, 1],
    ['up_to_date', 359, 1], ['up_to_date', 361, 2],
  ] as const) {
    const { root, stateDir } = isolatedState();
    t.after(() => rmSync(root, { recursive: true, force: true }));
    writeFileSync(join(stateDir, 'auto-update.json'), JSON.stringify({
      sourceStatus, lastCheckedAt: new Date(Date.now() - minutes * 60_000).toISOString(),
      runtimeFingerprint: computeRuntimeFingerprint(stateDir, root),
    }));
    maybeTriggerBackgroundAutoUpdate({ stateDir, checkoutDir: root });
    assert.equal(calls.length, expected, sourceStatus + ':' + minutes);
  }
});

test('background auto-update bypasses cooldown for disk or live daemon drift with a thirty-second spawn guard', t => {
  const { root, stateDir } = isolatedState();
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const calls = captureBackgroundUpdates(t);
  const checkoutDir = join(root, 'checkout');
  mkdirSync(checkoutDir);
  const now = Date.now();
  let elapsed = 0;
  t.mock.method(Date, 'now', () => now + elapsed);
  const current = computeRuntimeFingerprint(stateDir, checkoutDir);
  const writeStatus = (runtimeFingerprint: string, seconds: number) => writeFileSync(join(stateDir, 'auto-update.json'), JSON.stringify({
    sourceStatus: 'up_to_date', lastCheckedAt: new Date(now - seconds * 1_000).toISOString(), runtimeFingerprint,
  }));
  const check = () => maybeTriggerBackgroundAutoUpdate({ stateDir, checkoutDir });
  writeStatus('old-disk', 20);
  check();
  assert.deepEqual(calls, []);
  writeStatus(current, 60);
  const owner = processOwnerRecord();
  writeFileSync(join(stateDir, 'runtime-fingerprint.json'), JSON.stringify({ ...owner, fingerprint: 'old-daemon' }));
  check();
  assert.deepEqual(calls, []);
  writeFileSync(join(stateDir, 'control.sock'), '');
  const lockDir = join(stateDir, 'runtime.lock');
  mkdirSync(lockDir);
  writeFileSync(join(lockDir, 'owner.json'), JSON.stringify(owner));
  check();
  assert.deepEqual(calls, []);
  holdOwnedDirectory(stateDir);
  t.after(() => releaseOwnedDirectory(stateDir));
  writeFileSync(join(stateDir, 'runtime-fingerprint.json'), JSON.stringify({ ...owner, instanceId: 'previous', fingerprint: 'old-daemon' }));
  check();
  assert.deepEqual(calls, []);
  writeFileSync(join(stateDir, 'runtime-fingerprint.json'), JSON.stringify({ ...owner, fingerprint: 'old-daemon' }));
  check();
  check();
  assert.deepEqual(calls, [stateDir]);
  elapsed = 30_000;
  rmSync(join(stateDir, 'control.sock'));
  writeStatus('old-disk', 60);
  check();
  assert.deepEqual(calls, [stateDir, stateDir]);
});

test('offline CLI commands and daemon startup trigger background auto-update', async t => {
  const calls = captureBackgroundUpdates(t);
  for (const args of [['inbox', 'check'], ['node', 'status'], ['setup', 'probe'], ['project', 'init']]) {
    const { root, stateDir } = isolatedState();
    t.after(() => rmSync(root, { recursive: true, force: true }));
    const result = await runDealerCli([...args, '--root', join(root, 'project'), '--json'], {
      stateDir, stdout: () => {}, stderr: message => assert.fail(message),
    });
    assert.equal(result, 0, args.join(' '));
    assert.equal(calls.at(-1), stateDir, args.join(' '));
  }
  const { root, stateDir } = isolatedState();
  const previous = process.env.DEALER_STATE_DIR;
  process.env.DEALER_STATE_DIR = stateDir;
  let runtime: DealerNodeRuntime | undefined;
  t.after(async () => {
    if (previous === undefined) delete process.env.DEALER_STATE_DIR;
    else process.env.DEALER_STATE_DIR = previous;
    await runtime?.stop();
    rmSync(root, { recursive: true, force: true });
  });
  runtime = await runDealerDaemon(['--profile', 'headless-relay', '--member', 'test-node']);
  assert.equal(calls.length, 5);
  assert.equal(calls.at(-1), stateDir);
});

function updateCheckoutFixture(t: { after: (fn: () => void) => void }, checkoutName = 'checkout') {
  const { root, stateDir } = isolatedState();
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const origin = join(root, 'origin.git');
  git(root, ['init', '--bare', '--initial-branch=main', origin]);
  const checkout = join(root, checkoutName);
  git(root, ['clone', origin, checkout]);
  for (const [key, value] of [['user.name', 'Test'], ['user.email', 'test@example.com']]) git(checkout, ['config', key, value]);
  writeFileSync(join(checkout, 'tracked.txt'), 'original\n');
  git(checkout, ['add', '.']);
  git(checkout, ['commit', '-m', 'initial']);
  git(checkout, ['push', '-u', 'origin', 'main']);
  const beforeSha = git(checkout, ['rev-parse', 'HEAD']);
  const writer = join(root, 'writer');
  git(root, ['clone', origin, writer]);
  for (const [key, value] of [['user.name', 'Test'], ['user.email', 'test@example.com']]) git(writer, ['config', key, value]);
  const update = () => selfUpdate({ stateDir, checkoutDir: checkout, homeDir: join(root, 'home'), geminiDir: join(root, 'gemini') });
  return { root, stateDir, checkout, writer, beforeSha, update };
}

test('overlapping self-updates keep the same-process lock through daemon reconciliation', async t => {
  const { root, stateDir, checkout } = updateCheckoutFixture(t);
  const options = { stateDir, checkoutDir: checkout, homeDir: root };
  const lock = join(stateDir, 'auto-update.lock');
  let finishReconciliation!: (restarted: boolean) => void;
  const pending = new Promise<boolean>(resolve => { finishReconciliation = resolve; });
  let reconcileCalls = 0;
  const reconcile = () => ++reconcileCalls === 1 ? pending : Promise.resolve(false);

  const first = selfUpdate(options, reconcile);
  try {
    assert.equal(reconcileCalls, 1);
    const owner = readFileSync(join(lock, 'owner.json'), 'utf8');
    const second = await selfUpdate(options, reconcile);
    assert.equal(second.status, 'skipped_locked');
    assert.equal(second.sourceStatus, 'skipped_locked');
    assert.equal(reconcileCalls, 1);
    assert.equal(readFileSync(join(lock, 'owner.json'), 'utf8'), owner);
  } finally {
    finishReconciliation(true);
    await first;
  }
  assert.equal((await first).restartedDaemon, true);
  assert.equal(existsSync(lock), false);
});

function startUpdateWorker(
  t: test.TestContext,
  options: { stateDir: string; checkoutDir: string; homeDir: string },
  setup = '',
  reconcile = '',
) {
  const moduleUrl = new URL('../packages/cli/auto-update.ts', import.meta.url).href;
  const child = spawn(process.execPath, ['--no-warnings', '--experimental-strip-types', '--input-type=module', '--eval', `
    import fs from 'node:fs';
    import cp from 'node:child_process';
    import { join } from 'node:path';
    import { syncBuiltinESMExports } from 'node:module';
    import { selfUpdate } from ${JSON.stringify(moduleUrl)};
    const options = JSON.parse(process.argv[1]);
    const mark = name => fs.writeFileSync(join(options.homeDir, name), '');
    const pause = (ready, resume) => {
      mark(ready);
      const sleeper = new Int32Array(new SharedArrayBuffer(4));
      const deadline = Date.now() + 15_000;
      while (!fs.existsSync(join(options.homeDir, resume))) {
        if (Date.now() >= deadline) throw new Error('Timed out at ' + ready);
        Atomics.wait(sleeper, 0, 0, 10);
      }
    };
    ${setup}
    syncBuiltinESMExports();
    const report = await selfUpdate(options, async () => { ${reconcile}; return false; });
    console.log(JSON.stringify(report));
  `, JSON.stringify(options)], { stdio: ['ignore', 'pipe', 'pipe'] });
  const completed = new Promise<{ code: number | null; stdout: string; stderr: string }>(resolve => {
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', data => { stdout += data; });
    child.stderr.on('data', data => { stderr += data; });
    child.once('close', code => resolve({ code, stdout, stderr }));
  });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill();
    await completed;
  });
  return {
    pid: child.pid,
    completed,
    async report() {
      const result = await completed;
      assert.equal(result.code, 0, result.stderr);
      return JSON.parse(result.stdout) as SelfUpdateReport;
    },
  };
}

test('a delayed reclaim contender cannot remove a replacement gate or take its lock', { timeout: 30_000 }, async t => {
  const { root, stateDir, checkout } = updateCheckoutFixture(t);
  const options = { stateDir, checkoutDir: checkout, homeDir: root };
  const lock = join(stateDir, 'auto-update.lock');
  const gate = `${lock}.reclaim`;
  const deadGatePid = 999998;
  mkdirSync(lock);
  mkdirSync(gate);
  writeFileSync(join(lock, 'owner.json'), JSON.stringify({ pid: 999999 }));
  writeFileSync(join(gate, 'owner.json'), JSON.stringify({ pid: deadGatePid }));
  const signal = (name: string) => writeFileSync(join(root, name), '');
  const seen = (name: string) => existsSync(join(root, name));
  const delayed = startUpdateWorker(t, options, `
    const kill = process.kill;
    process.kill = (pid, signal) => {
      try {
        return kill(pid, signal);
      } catch (error) {
        if (pid === ${deadGatePid}) pause('read-dead-gate', 'resume-delayed');
        throw error;
      }
    };
  `);
  await until(() => seen('read-dead-gate'), 'contender to read the dead gate');
  const winner = startUpdateWorker(t, options, `
    const lock = join(options.stateDir, 'auto-update.lock');
    const rename = fs.renameSync;
    fs.renameSync = (source, destination) => {
      if (destination === lock) pause('replacing-lock', 'publish-lock');
      rename(source, destination);
      if (destination === lock + '.reclaim') pause('claimed-gate', 'reclaim-lock');
    };
  `, "pause('reconciling', 'finish-reconciliation')");
  await until(() => seen('claimed-gate'), 'winner to publish the replacement gate');
  const files = readdirSync(gate);
  assert.equal(files.length, 1);
  const ownerPath = join(gate, files[0]);
  assert.equal(JSON.parse(readFileSync(ownerPath, 'utf8')).pid, winner.pid);
  signal('resume-delayed');
  assert.equal((await delayed.report()).status, 'skipped_locked');
  assert.equal(JSON.parse(readFileSync(ownerPath, 'utf8')).pid, winner.pid);

  signal('reclaim-lock');
  await until(() => seen('replacing-lock'), 'winner to stage the replacement lock');
  assert.equal(existsSync(lock), false);
  assert.equal(JSON.parse(readFileSync(ownerPath, 'utf8')).pid, winner.pid);
  assert.equal((await startUpdateWorker(t, options).report()).status, 'skipped_locked');
  signal('publish-lock');
  await until(() => seen('reconciling'), 'winner to hold the update lock during reconciliation');
  assert.equal(JSON.parse(readFileSync(join(lock, 'owner.json'), 'utf8')).pid, winner.pid);
  assert.equal((await startUpdateWorker(t, options).report()).status, 'skipped_locked');
  signal('finish-reconciliation');
  assert.equal((await winner.report()).status, 'complete');
  assert.equal(existsSync(lock), false);
  assert.equal(existsSync(gate), false);
});

for (const point of ['gate-release', 'stale-gate', 'stale-lock'] as const) {
  test(`update locking recovers after interruption at ${point}`, async t => {
    const { root, stateDir, checkout } = updateCheckoutFixture(t);
    const lock = join(stateDir, 'auto-update.lock');
    const gate = `${lock}.reclaim`;
    const deadOwner = JSON.stringify({ pid: 999999, instanceId: 'dead-owner' });
    mkdirSync(lock);
    writeFileSync(join(lock, 'owner.json'), deadOwner);
    if (point === 'stale-gate') {
      mkdirSync(gate);
      writeFileSync(join(gate, 'owner.999999.dead-owner.json'), deadOwner);
      const ticket = `${gate}.evict.${statSync(gate, { bigint: true }).ino}`;
      mkdirSync(ticket);
      writeFileSync(join(ticket, 'owner.json'), deadOwner);
    }
    // Use a child so the interrupted lock owner is actually dead during recovery.
    const worker = startUpdateWorker(t, { stateDir, checkoutDir: checkout, homeDir: root }, `
      const lock = join(options.stateDir, 'auto-update.lock');
      const gate = lock + '.reclaim';
      const point = ${JSON.stringify(point)};
      const rename = fs.renameSync;
      fs.renameSync = (source, destination) => {
        rename(source, destination);
        if ((point === 'gate-release' && source === gate && destination.startsWith(gate + '.release.')) ||
            (point === 'stale-gate' && source === gate && destination.startsWith(gate + '.stale.')) ||
            (point === 'stale-lock' && source === lock && destination.startsWith(lock + '.stale.'))) {
          process.exit(73);
        }
      };
      const rmdir = fs.rmdirSync;
      fs.rmdirSync = path => {
        if (path === gate && point !== 'stale-lock') process.exit(73);
        rmdir(path);
      };
    `);
    const result = await worker.completed;
    assert.equal(result.code, 73, result.stderr || result.stdout);
    assert.equal(existsSync(gate), point === 'stale-lock');
    if (point === 'stale-lock') {
      assert.equal(existsSync(lock), false);
      assert.equal(JSON.parse(readFileSync(join(gate, 'owner.json'), 'utf8')).pid, worker.pid);
    }
    mkdirSync(`${gate}.stage.999999.dead-owner.abcdefghijklmnop.abc123`);
    const liveStageName = 'auto-update.lock.stage.live';
    const liveStage = join(stateDir, liveStageName);
    mkdirSync(liveStage);
    writeFileSync(join(liveStage, 'owner.json'), JSON.stringify({ pid: process.ppid }));
    withOwnedLock(lock, 'Recovered update', () => {
      assert.equal(JSON.parse(readFileSync(join(lock, 'owner.json'), 'utf8')).pid, process.pid);
    });
    assert.deepEqual(readdirSync(stateDir).filter(name => name.startsWith('auto-update.lock')), [liveStageName]);
    assert.equal(selfUpdate({ stateDir, checkoutDir: checkout, homeDir: root }).sourceStatus, 'up_to_date');
  });
}

test('self-updates from separate state directories serialize mutations of a shared checkout and home', { timeout: 30_000 }, async t => {
  const { root, stateDir, checkout, writer } = updateCheckoutFixture(t, 'the-dealer');
  const bin = join(root, '.local', 'bin');
  mkdirSync(bin, { recursive: true });
  const secondState = join(root, 'second-state');
  const alias = join(root, 'checkout-alias');
  symlinkSync(checkout, alias, 'dir');
  writeFileSync(join(checkout, 'tracked.txt'), 'staged edit\n');
  git(checkout, ['add', 'tracked.txt']);
  writeFileSync(join(checkout, 'tracked.txt'), 'working edit\n');
  writeFileSync(join(checkout, 'untracked.txt'), 'untracked edit\n');
  const before = git(checkout, ['status', '--porcelain']);
  writeFileSync(join(writer, 'upstream.txt'), 'upstream\n');
  git(writer, ['add', 'upstream.txt']);
  git(writer, ['commit', '-m', 'upstream']);
  git(writer, ['push']);

  const first = startUpdateWorker(t, { stateDir, checkoutDir: checkout, homeDir: root }, `
    const exec = cp.execFileSync;
    cp.execFileSync = (file, args, options) => {
      if (file === 'git' && args[0] === 'pull') pause('first-stashed', 'allow-first-pull');
      return exec(file, args, options);
    };
  `);
  await until(() => existsSync(join(root, 'first-stashed')), 'first updater to stage its temporary stash');
  assert.match(git(checkout, ['stash', 'list']), /dealer-self-update/);
  const second = startUpdateWorker(t, { stateDir: secondState, checkoutDir: alias, homeDir: root }, `
    const wait = Atomics.wait;
    Atomics.wait = (...args) => {
      mark('second-waiting');
      return wait(...args);
    };
    const exec = cp.execFileSync;
    cp.execFileSync = (file, args, options) => {
      if (file === 'git' && args[0] === 'status') mark('second-entered-checkout');
      return exec(file, args, options);
    };
  `);
  const waiting = join(root, 'second-waiting');
  const entered = join(root, 'second-entered-checkout');
  await until(() => existsSync(waiting) || existsSync(entered), 'second updater to contend for the checkout');
  assert.equal(existsSync(waiting), true);
  assert.equal(existsSync(entered), false);
  writeFileSync(join(root, 'allow-first-pull'), '');

  assert.equal((await first.report()).sourceStatus, 'updated');
  assert.equal((await second.report()).sourceStatus, 'up_to_date');
  assert.equal(git(checkout, ['stash', 'list']), '');
  assert.equal(git(checkout, ['status', '--porcelain']), before);
  assert.equal(git(checkout, ['show', ':tracked.txt']), 'staged edit');
  assert.equal(readFileSync(join(checkout, 'tracked.txt'), 'utf8'), 'working edit\n');
  assert.equal(readFileSync(join(checkout, 'untracked.txt'), 'utf8'), 'untracked edit\n');
  for (const name of ['dealer', 'dealerd']) {
    assert.equal(readlinkSync(join(bin, name)), join(alias, 'bin', `${name}.js`));
  }
});

test('self-update leaves binary links and daemon reconciliation alone when stash restoration fails', async t => {
  const { root, stateDir, checkout, writer } = updateCheckoutFixture(t);
  const bin = join(root, '.local', 'bin');
  mkdirSync(bin, { recursive: true });
  const link = join(bin, 'dealerd');
  const original = join(root, 'serving', 'bin', 'dealerd.js');
  symlinkSync(original, link);

  writeFileSync(join(checkout, 'tracked.txt'), 'local edits\n');
  writeFileSync(join(writer, 'upstream.txt'), 'upstream\n');
  git(writer, ['add', 'upstream.txt']);
  git(writer, ['commit', '-m', 'upstream']);
  git(writer, ['push']);
  const hooks = join(root, 'hooks');
  mkdirSync(hooks);
  git(checkout, ['config', 'core.hooksPath', hooks]);
  writeFileSync(join(hooks, 'post-merge'), '#!/bin/sh\ngit stash drop\n', { mode: 0o755 });

  const reconcile = t.mock.fn(async () => false);
  const report = await selfUpdate({ stateDir, checkoutDir: checkout, homeDir: root }, reconcile);
  assert.equal(report.status, 'failed');
  assert.equal(report.sourceStatus, 'error');
  assert.match(report.error ?? '', /Temporary update stash is no longer the newest stash/);
  assert.deepEqual(report.syncedBinaries, []);
  assert.equal(readlinkSync(link), original);
  assert.equal(existsSync(join(bin, 'dealer')), false);
  assert.equal(reconcile.mock.callCount(), 0);
});

test('self-update reports a daemon probe failure to the CLI and persists it', async t => {
  const { root, stateDir, checkout } = updateCheckoutFixture(t);
  const lock = join(stateDir, 'auto-update.lock');
  const error = 'Daemon status probe failed';
  let lockHeldDuringProbe = false;
  const server = createServer((request, response) => {
    request.resume();
    lockHeldDuringProbe = existsSync(join(lock, 'owner.json'));
    response.writeHead(500).end(error);
  });
  server.listen(join(stateDir, 'control.sock'));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  await once(server, 'listening');

  const result = await runDealer(['self-update', '--checkout-dir', checkout, '--home-dir', root, '--json'], stateDir);
  assert.equal(result.code, 1);
  assert.deepEqual(JSON.parse(result.stderr), { error });
  assert.equal(lockHeldDuringProbe, true);
  const report = JSON.parse(readFileSync(join(stateDir, 'auto-update.json'), 'utf8'));
  assert.equal(report.status, 'failed');
  assert.equal(report.sourceStatus, 'up_to_date');
  assert.equal(report.error, error);
  assert.equal(report.restartedDaemon, false);
  const log = readFileSync(join(stateDir, 'auto-update.log'), 'utf8').trim().split('\n');
  assert.deepEqual(JSON.parse(log.at(-1)!), report);
  assert.equal(existsSync(lock), false);
});

test('self-update persists a readiness timeout for an unready live daemon owner', async t => {
  const { root, stateDir, checkout } = updateCheckoutFixture(t);
  const lock = join(stateDir, 'runtime.lock');
  mkdirSync(lock);
  writeFileSync(join(lock, 'owner.json'), JSON.stringify(processOwnerRecord()));
  const ownedStateDir = realpathSync(stateDir);
  holdOwnedDirectory(ownedStateDir);
  t.after(() => releaseOwnedDirectory(ownedStateDir));
  const stdout: string[] = [];
  const stderr: string[] = [];
  const code = await runDealerCli(['self-update', '--checkout-dir', checkout, '--home-dir', root, '--json'], {
    stateDir,
    daemonStartupTimeoutMs: 40,
    stdout: message => stdout.push(message),
    stderr: message => stderr.push(message),
  });
  assert.equal(code, 1);
  const error = `Timed out waiting for daemon in ${stateDir} to become ready`;
  assert.deepEqual(stdout, []);
  assert.deepEqual(stderr.map(message => JSON.parse(message)), [{ error }]);
  const report = JSON.parse(readFileSync(join(stateDir, 'auto-update.json'), 'utf8'));
  assert.equal(report.status, 'failed');
  assert.equal(report.sourceStatus, 'up_to_date');
  assert.equal(report.error, error);
  assert.equal(report.restartedDaemon, false);
  const log = readFileSync(join(stateDir, 'auto-update.log'), 'utf8').trim().split('\n');
  assert.deepEqual(JSON.parse(log.at(-1)!), report);
  assert.equal(readDaemonOwner(stateDir).pid, process.pid);
  assert.equal(existsSync(join(stateDir, 'auto-update.lock')), false);
});

for (const scenario of ['clean-restore', 'conflict', 'pull-failed'] as const) {
  test(`dirty self-update preserves tracked, staged and untracked files: ${scenario}`, t => {
    const { root, checkout, writer, beforeSha, update } = updateCheckoutFixture(t);
    writeFileSync(join(checkout, 'tracked.txt'), 'local staged\n');
    git(checkout, ['add', 'tracked.txt']);
    writeFileSync(join(checkout, 'tracked.txt'), 'local working tree\n');
    writeFileSync(join(checkout, 'untracked.txt'), 'local untracked\n');
    const beforeStatus = git(checkout, ['status', '--porcelain']);
    const beforeIndex = git(checkout, ['diff', '--cached']);
    writeFileSync(join(writer, scenario === 'conflict' ? 'tracked.txt' : 'upstream.txt'), 'upstream\n');
    git(writer, ['add', '.']);
    git(writer, ['commit', '-m', 'upstream']);
    git(writer, ['push']);
    const upstreamSha = git(writer, ['rev-parse', 'HEAD']);
    if (scenario === 'pull-failed') git(checkout, ['remote', 'set-url', 'origin', join(root, 'missing.git')]);
    const report = update();
    assert.equal(report.sourceStatus, scenario === 'clean-restore' ? 'updated' : scenario === 'conflict' ? 'stash_conflict_rolled_back' : 'error');
    assert.equal(report.status, scenario === 'clean-restore' ? 'updated' : 'failed');
    assert.equal(git(checkout, ['rev-parse', 'HEAD']), scenario === 'clean-restore' ? upstreamSha : beforeSha);
    assert.equal(readFileSync(join(checkout, 'tracked.txt'), 'utf8'), 'local working tree\n');
    assert.equal(git(checkout, ['diff', '--cached']), beforeIndex);
    assert.equal(git(checkout, ['status', '--porcelain']), beforeStatus);
    assert.equal(readFileSync(join(checkout, 'untracked.txt'), 'utf8'), 'local untracked\n');
    assert.equal(git(checkout, ['stash', 'list']), '');
    assert.equal(git(checkout, ['ls-files', '--unmerged']), '');
    if (scenario === 'clean-restore') assert.equal(report.stashedLocalChanges, true);
    else assert.equal(git(checkout, ['status', '--porcelain']), beforeStatus);
    if (scenario === 'conflict') {
      assert.equal(report.headSha, beforeSha);
      assert.equal(report.error, 'Local changes conflicted with upstream update; rolled back cleanly to previous HEAD');
      assert.equal(existsSync(join(checkout, 'upstream.txt')), false);
    }
  });
}

test('self-update preserves dirty feature branches and refuses existing merge/rebase state', t => {
  const { checkout, beforeSha, update } = updateCheckoutFixture(t);
  git(checkout, ['checkout', '-b', 'feature']);
  writeFileSync(join(checkout, 'tracked.txt'), 'do not touch\n');
  assert.equal(update().sourceStatus, 'skipped_non_main_branch');
  git(checkout, ['checkout', 'main']);
  for (const marker of ['MERGE_HEAD', 'rebase-merge', 'rebase-apply']) {
    const file = join(checkout, '.git', marker);
    if (marker === 'MERGE_HEAD') writeFileSync(file, beforeSha + '\n');
    else mkdirSync(file);
    const report = update();
    assert.equal(report.sourceStatus, 'conflicted_checkout');
    assert.equal(report.status, 'failed');
    assert.equal(report.error, 'Checkout has unresolved merge or rebase state');
    assert.equal(readFileSync(join(checkout, 'tracked.txt'), 'utf8'), 'do not touch\n');
    assert.equal(git(checkout, ['stash', 'list']), '');
    rmSync(file, { recursive: true });
  }
});
