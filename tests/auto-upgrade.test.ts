import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
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

function editManifest(projectRoot: string, description: string): void {
  const path = join(projectRoot, '.dealer', 'project.json');
  const manifest = JSON.parse(readFileSync(path, 'utf8'));
  writeFileSync(path, `${JSON.stringify({ ...manifest, description }, null, 2)}\n`);
}

test('active pre-RPC check automatically restarts dealerd when linked project .dealer manifest changes', { timeout: 90_000 }, async t => {
  const { root, stateDir } = isolatedState();
  t.after(async () => {
    await stopDaemonIfRunning(stateDir);
    rmSync(root, { recursive: true, force: true });
  });
  // The passive fs.watch reload is exercised by the next test; disable it here so the
  // assertion below isolates the active pre-RPC restart path.
  await bootDaemon(stateDir, { DEALER_AUTO_RESTART: '0' });
  const projectRoot = join(root, 'proj');
  mkdirSync(projectRoot, { recursive: true });
  assert.equal((await dealer(['project', 'init', '--root', projectRoot, '--id', 'demo'], stateDir)).projectId, 'demo');
  assert.equal((await dealer(['project', 'link', projectRoot], stateDir)).projectId, 'demo');
  const status1 = await dealer(['node', 'status'], stateDir);
  const before = readDaemonFingerprint(stateDir);
  assert.ok(before);
  assert.equal(status1.runtimeFingerprint, before.fingerprint);
  editManifest(projectRoot, 'edited on disk after the project was linked');
  const linked = await dealer(['project', 'status', '--root', projectRoot], stateDir);
  assert.equal(linked.projects[0].projectId, 'demo');
  const status2 = await dealer(['node', 'status'], stateDir);
  const after = readDaemonFingerprint(stateDir);
  assert.ok(after);
  assert.notEqual(after.pid, before.pid);
  assert.notEqual(status2.runtimeFingerprint, status1.runtimeFingerprint);
  assert.equal(status2.runtimeFingerprint, after.fingerprint);
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
  writeFileSync(join(checkout, 'untracked.txt'), 'dirty checkout\n');
  const dirty = selfUpdate({ stateDir, checkoutDir: checkout, homeDir: home, geminiDir: gemini });
  assert.equal(dirty.sourceStatus, 'skipped_dirty_checkout');
  rmSync(join(checkout, 'untracked.txt'), { force: true });
  git(checkout, ['checkout', '-b', 'feat']);
  const offMain = selfUpdate({ stateDir, checkoutDir: checkout, homeDir: home, geminiDir: gemini });
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
  assert.equal(down.stopped, true);
  const offline = await dealer(['node', 'status'], stateDir);
  assert.equal(offline.running, false);
  assert.equal(existsSync(join(stateDir, 'control.sock')), false);
});
