import test from 'node:test';
import type { TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  MailboxStore,
  WakeupRedriveService,
  createOrcaWakeupDrivers,
  tryFastForwardCleanWorktree,
  runCommand,
} from '../packages/index.ts';
import type { CommandRequest, CommandRunner, MessageEnvelope } from '../packages/index.ts';

function tempDir(t: TestContext): string {
  const root = mkdtempSync(join(tmpdir(), 'dealer-standing-ff-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return root;
}

function runGit(cwd: string, ...args: string[]): string {
  return execFileSync('git', ['-C', cwd, '-c', 'core.fsmonitor=false', ...args], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  }).trim();
}

function commitFile(cwd: string, file: string, content: string, message: string): string {
  writeFileSync(join(cwd, file), content);
  runGit(cwd, 'add', file);
  runGit(
    cwd,
    '-c', 'user.name=Dealer Test',
    '-c', 'user.email=test@example.com',
    '-c', 'commit.gpgsign=false',
    'commit', '--quiet', '-m', message,
  );
  return runGit(cwd, 'rev-parse', 'HEAD');
}

function createLinkedStandingWorktree(t: TestContext, defaultBranch = 'main') {
  const root = tempDir(t);
  const bareOrigin = join(root, 'origin.git');
  const seed = join(root, 'seed');
  const primary = join(root, 'primary');
  const worktreePath = join(root, 'standing-wave');

  runGit(root, 'init', '--bare', '--quiet', `--initial-branch=${defaultBranch}`, bareOrigin);
  runGit(root, 'init', '--quiet', `--initial-branch=${defaultBranch}`, seed);
  runGit(seed, 'remote', 'add', 'origin', bareOrigin);
  const baseHead = commitFile(seed, 'README.md', 'v1\n', 'base');
  runGit(seed, 'push', '--quiet', '-u', 'origin', defaultBranch);

  runGit(root, 'clone', '--quiet', bareOrigin, primary);
  runGit(primary, 'worktree', 'add', '--quiet', '-b', 'standing/wave', worktreePath);

  const pushOriginCommit = (file: string, content: string, message: string): string => {
    const sha = commitFile(seed, file, content, message);
    runGit(seed, 'push', '--quiet', 'origin', defaultBranch);
    return sha;
  };

  return { root, bareOrigin, seed, primary, worktreePath, baseHead, pushOriginCommit };
}

let msgCounter = 0;
function makeAlert(id: string): MessageEnvelope {
  msgCounter += 1;
  return {
    id,
    networkId: 'team',
    traceId: id,
    from: { nodeId: 'ed25519:' + 'a'.repeat(64), member: 'sender', nodeName: 'laptop' },
    routing: { mode: 'unicast' },
    kind: 'alerts',
    priority: 'normal',
    subject: `Alert ${msgCounter}`,
    body: 'alert body',
    payload: {},
    attachments: [],
    createdAt: new Date().toISOString(),
    signature: '',
  };
}

interface FakeTerminal {
  handle: string;
  connected: boolean;
  writable: boolean;
  agentIdentity?: string;
  preview?: string;
  tabId?: string;
  leafId?: string;
}

function createFakeOrcaEnv(workspacePath: string, initialTerminals: FakeTerminal[] = []) {
  const terminals = [...initialTerminals];
  const events: string[] = [];
  let created = 0;

  const runner: CommandRunner = async (request: CommandRequest) => {
    if (request.command === 'git') {
      events.push(`git:${request.args[4]}`);
      return runCommand(request);
    }
    const [group, action] = request.args;
    if (group === 'worktree' && action === 'ps') {
      return {
        stdout: JSON.stringify({
          ok: true,
          result: {
            truncated: false,
            worktrees: [{
              path: workspacePath,
              worktreeId: 'wt-standing',
              childWorktreeIds: [],
              unverifiableTerminalCount: 0,
              agents: terminals.filter(t => t.connected).map(t => ({
                state: 'done',
                ...(t.tabId && t.leafId ? { paneKey: `${t.tabId}:${t.leafId}` } : {}),
              })),
            }],
          },
        }),
      };
    }
    if (group === 'terminal' && action === 'list') {
      return { stdout: JSON.stringify({ ok: true, result: { terminals, truncated: false } }) };
    }
    if (group === 'terminal' && action === 'create') {
      created += 1;
      const handle = `term-new-${created}`;
      events.push(`orca:create:${handle}`);
      terminals.push({
        handle,
        connected: true,
        writable: true,
        agentIdentity: 'antigravity',
        preview: '> ',
        tabId: handle,
        leafId: 'root',
      });
      return { stdout: JSON.stringify({ ok: true, result: { terminal: { handle } } }) };
    }
    if (group === 'terminal' && action === 'close') {
      const idx = request.args.indexOf('--terminal');
      const handle = idx >= 0 ? request.args[idx + 1] : undefined;
      if (handle) {
        const pos = terminals.findIndex(t => t.handle === handle);
        if (pos >= 0) terminals.splice(pos, 1);
      }
      events.push(`orca:close:${handle ?? ''}`);
      return { stdout: '{"ok":true}' };
    }
    if (group === 'terminal' && action === 'send') {
      const idx = request.args.indexOf('--terminal');
      const handle = idx >= 0 ? request.args[idx + 1] : '';
      events.push(`orca:send:${handle}`);
      return { stdout: '{"ok":true}' };
    }
    if (group === 'terminal' && action === 'wait') {
      return { stdout: '{"ok":true}' };
    }
    return { stdout: '{"ok":true}' };
  };

  return { runner, terminals, events };
}

test('tryFastForwardCleanWorktree fast-forwards a clean behind linked worktree and reports up_to_date when current', async t => {
  const { worktreePath, pushOriginCommit } = createLinkedStandingWorktree(t);
  const beforeShort = runGit(worktreePath, 'rev-parse', '--short', 'HEAD');
  const newHead = pushOriginCommit('README.md', 'v2\n', 'advance main');

  const updated = await tryFastForwardCleanWorktree(runCommand, worktreePath, async () => true);
  assert.equal(updated.status, 'updated');
  assert.equal(updated.branch, 'standing/wave');
  assert.equal(updated.beforeSha, beforeShort);
  assert.equal(updated.afterSha, runGit(worktreePath, 'rev-parse', '--short', 'HEAD'));
  assert.notEqual(updated.beforeSha, updated.afterSha);
  assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), newHead);
  assert.equal(readFileSync(join(worktreePath, 'README.md'), 'utf8'), 'v2\n');

  const upToDate = await tryFastForwardCleanWorktree(runCommand, worktreePath, async () => true);
  assert.deepEqual(upToDate, {
    status: 'up_to_date',
    branch: 'standing/wave',
    headSha: updated.afterSha,
  });
});

test('tryFastForwardCleanWorktree skips dirty, untracked, ahead, detached HEAD, not_syncable, and mid-fetch TOCTOU without mutating', async t => {
  await t.test('dirty tracked file', async st => {
    const { worktreePath, baseHead, pushOriginCommit } = createLinkedStandingWorktree(st);
    pushOriginCommit('README.md', 'v2\n', 'advance');
    writeFileSync(join(worktreePath, 'README.md'), 'local dirty edit\n');

    const res = await tryFastForwardCleanWorktree(runCommand, worktreePath, async () => true);
    assert.deepEqual(res, { status: 'skipped', reason: 'dirty' });
    assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), baseHead);
    assert.equal(readFileSync(join(worktreePath, 'README.md'), 'utf8'), 'local dirty edit\n');
  });

  await t.test('untracked file', async st => {
    const { worktreePath, baseHead, pushOriginCommit } = createLinkedStandingWorktree(st);
    pushOriginCommit('README.md', 'v2\n', 'advance');
    writeFileSync(join(worktreePath, 'scratch.txt'), 'keep me\n');

    const res = await tryFastForwardCleanWorktree(runCommand, worktreePath, async () => true);
    assert.deepEqual(res, { status: 'skipped', reason: 'dirty' });
    assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), baseHead);
    assert.equal(readFileSync(join(worktreePath, 'scratch.txt'), 'utf8'), 'keep me\n');
  });

  await t.test('local unmerged commit (ahead)', async st => {
    const { worktreePath, pushOriginCommit } = createLinkedStandingWorktree(st);
    pushOriginCommit('README.md', 'v2\n', 'advance');
    const localHead = commitFile(worktreePath, 'local.txt', 'mine\n', 'local work');

    const res = await tryFastForwardCleanWorktree(runCommand, worktreePath, async () => true);
    assert.deepEqual(res, { status: 'skipped', reason: 'ahead' });
    assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), localHead);
  });

  await t.test('detached HEAD', async st => {
    const { worktreePath, baseHead, pushOriginCommit } = createLinkedStandingWorktree(st);
    pushOriginCommit('README.md', 'v2\n', 'advance');
    runGit(worktreePath, 'checkout', '--quiet', '--detach', 'HEAD');

    const res = await tryFastForwardCleanWorktree(runCommand, worktreePath, async () => true);
    assert.deepEqual(res, { status: 'skipped', reason: 'detached_head' });
    assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), baseHead);
  });

  await t.test('canMerge false initially', async st => {
    const { worktreePath, baseHead, pushOriginCommit } = createLinkedStandingWorktree(st);
    pushOriginCommit('README.md', 'v2\n', 'advance');

    const res = await tryFastForwardCleanWorktree(runCommand, worktreePath, async () => false);
    assert.deepEqual(res, { status: 'skipped', reason: 'not_syncable' });
    assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), baseHead);
  });

  await t.test('becomes dirty mid-fetch (TOCTOU)', async st => {
    const { worktreePath, baseHead, pushOriginCommit } = createLinkedStandingWorktree(st);
    pushOriginCommit('README.md', 'v2\n', 'advance');
    const runner: CommandRunner = async request => {
      const result = await runCommand(request);
      if (request.command === 'git' && request.args[4] === 'fetch') {
        writeFileSync(join(worktreePath, 'mid-fetch.txt'), 'appeared\n');
      }
      return result;
    };

    const res = await tryFastForwardCleanWorktree(runner, worktreePath, async () => true);
    assert.deepEqual(res, { status: 'skipped', reason: 'dirty' });
    assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), baseHead);
    assert.equal(readFileSync(join(worktreePath, 'mid-fetch.txt'), 'utf8'), 'appeared\n');
  });
});

test('tryFastForwardCleanWorktree resolves non-main remote default branch via origin/HEAD', async t => {
  const { worktreePath, pushOriginCommit } = createLinkedStandingWorktree(t, 'master');
  const beforeShort = runGit(worktreePath, 'rev-parse', '--short', 'HEAD');
  const newMasterHead = pushOriginCommit('README.md', 'master-v2\n', 'advance master');

  const res = await tryFastForwardCleanWorktree(runCommand, worktreePath, async () => true);
  assert.equal(res.status, 'updated');
  assert.equal(res.branch, 'standing/wave');
  assert.equal(res.beforeSha, beforeShort);
  assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), newMasterHead);
  assert.equal(readFileSync(join(worktreePath, 'README.md'), 'utf8'), 'master-v2\n');
});

test('dormant worktree:* and stream:*:2 sibling bindings on the same workspacePath do not block fast-forward', async t => {
  const { root, worktreePath, pushOriginCommit } = createLinkedStandingWorktree(t);
  const newHead = pushOriginCommit('README.md', 'v2\n', 'advance');
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const fake = createFakeOrcaEnv(worktreePath);
  const errors: unknown[] = [];
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    onError: error => errors.push(error),
    drivers: createOrcaWakeupDrivers({ commandRunner: fake.runner }),
  });
  t.after(async () => { await service.stop(); store.close(); });

  store.upsertWakeTarget('worktree:standing-wave', {
    driver: 'orca-terminal',
    pendingWake: false,
    driverConfig: { workspacePath: worktreePath, terminalHandle: 'term-old-wt' },
  });
  store.upsertWakeTarget('stream:alerts:2', {
    driver: 'orca-terminal',
    pendingWake: false,
    driverConfig: { workspacePath: worktreePath, terminalHandle: 'term-old-slot2' },
  });
  store.upsertWakeTarget('stream:alerts', {
    driver: 'orca-terminal',
    rotateAfterWaves: 1,
    completedWaves: 1,
    driverConfig: { workspacePath: worktreePath },
  });
  store.enqueue([makeAlert('dormant-siblings')], { mailboxId: 'stream:alerts' });

  const result = await service.flushMailbox('stream:alerts', { force: true });
  assert.equal(result.woke, true, JSON.stringify(result));
  assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), newHead);
  assert.deepEqual(errors, []);
});

for (const activeKind of ['pendingWake', 'deliveryInDoubt', 'retiredTerminalHandle'] as const) {
  test(`active sibling (${activeKind}) on the same workspacePath blocks fast-forward without blocking wake`, async t => {
    const { root, worktreePath, baseHead, pushOriginCommit } = createLinkedStandingWorktree(t);
    pushOriginCommit('README.md', 'v2\n', 'advance');
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const fake = createFakeOrcaEnv(worktreePath);
    const errors: unknown[] = [];
    const service = new WakeupRedriveService({
      store,
      flushIntervalMs: 100_000,
      onError: error => errors.push(error),
      drivers: createOrcaWakeupDrivers({ commandRunner: fake.runner }),
    });
    t.after(async () => { await service.stop(); store.close(); });

    if (activeKind === 'pendingWake') {
      store.enqueue([makeAlert('sibling-pending')], {
        mailboxId: 'stream:alerts:2',
        wakeTarget: {
          driver: 'orca-terminal',
          coalesceMs: 60_000,
          driverConfig: { workspacePath: worktreePath, terminalHandle: 'term-sibling' },
        },
      });
    } else {
      store.upsertWakeTarget('stream:alerts:2', {
        driver: 'orca-terminal',
        deliveryInDoubt: activeKind === 'deliveryInDoubt' ? true : undefined,
        driverConfig: {
          workspacePath: worktreePath,
          terminalHandle: 'term-sibling',
          ...(activeKind === 'retiredTerminalHandle' ? { retiredTerminalHandle: 'term-retiring' } : {}),
        },
      });
    }
    store.upsertWakeTarget('stream:alerts', {
      driver: 'orca-terminal',
      rotateAfterWaves: 1,
      completedWaves: 1,
      driverConfig: { workspacePath: worktreePath },
    });
    store.enqueue([makeAlert(`active-${activeKind}`)], { mailboxId: 'stream:alerts' });

    const result = await service.flushMailbox('stream:alerts', { force: true });
    assert.equal(result.woke, true, JSON.stringify(result));
    assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), baseHead);
    assert.equal(fake.events.some(event => event.startsWith('git:')), false);
    assert.deepEqual(errors, []);
  });
}

test('idle reuse path fast-forwards on a fresh wave before sending pointer, honors rebound handles, and skips mid-wave retries', async t => {
  await t.test('fresh wave reusing a live idle tab fast-forwards before pointer send', async st => {
    const { root, worktreePath, pushOriginCommit } = createLinkedStandingWorktree(st);
    const newHead = pushOriginCommit('README.md', 'v2\n', 'advance');
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const fake = createFakeOrcaEnv(worktreePath, [{
      handle: 'term-rebound',
      connected: true,
      writable: true,
      agentIdentity: 'antigravity',
      preview: '> ',
      tabId: 'pane-standing',
      leafId: 'leaf-1',
    }]);
    const errors: unknown[] = [];
    const service = new WakeupRedriveService({
      store,
      flushIntervalMs: 100_000,
      onError: error => errors.push(error),
      drivers: createOrcaWakeupDrivers({ commandRunner: fake.runner }),
    });
    st.after(async () => { await service.stop(); store.close(); });

    store.upsertWakeTarget('stream:reuse', {
      driver: 'orca-terminal',
      rotateAfterWaves: 10,
      completedWaves: 2,
      driverConfig: {
        workspacePath: worktreePath,
        terminalHandle: 'term-stale-handle',
        paneKey: 'pane-standing:leaf-1',
        agent: 'antigravity',
      },
    });
    store.enqueue([makeAlert('reuse-fresh')], { mailboxId: 'stream:reuse' });

    const result = await service.flushMailbox('stream:reuse', { force: true });
    assert.equal(result.woke, true, JSON.stringify(result));
    assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), newHead);
    const mergeIdx = fake.events.indexOf('git:merge');
    const sendIdx = fake.events.indexOf('orca:send:term-rebound');
    assert.ok(mergeIdx >= 0, `expected git:merge in ${JSON.stringify(fake.events)}`);
    assert.ok(sendIdx > mergeIdx, `expected send after merge in ${JSON.stringify(fake.events)}`);
    assert.equal(store.getWakeTarget('stream:reuse')!.completedWaves, 2);
    assert.deepEqual(errors, []);
  });

  await t.test('mid-wave retry with an active deliveryId lease skips fast-forward', async st => {
    const { root, worktreePath, baseHead, pushOriginCommit } = createLinkedStandingWorktree(st);
    pushOriginCommit('README.md', 'v2\n', 'advance');
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const fake = createFakeOrcaEnv(worktreePath, [{
      handle: 'term-live',
      connected: true,
      writable: true,
      agentIdentity: 'antigravity',
      preview: '> ',
      tabId: 'tab-live',
      leafId: 'root',
    }]);
    const errors: unknown[] = [];
    const service = new WakeupRedriveService({
      store,
      flushIntervalMs: 100_000,
      onError: error => errors.push(error),
      drivers: createOrcaWakeupDrivers({ commandRunner: fake.runner }),
    });
    st.after(async () => { await service.stop(); store.close(); });

    store.upsertWakeTarget('stream:reuse-leased', {
      driver: 'orca-terminal',
      rotateAfterWaves: 10,
      completedWaves: 2,
      driverConfig: {
        workspacePath: worktreePath,
        terminalHandle: 'term-live',
        agent: 'antigravity',
      },
    });
    store.enqueue([makeAlert('wave-1')], { mailboxId: 'stream:reuse-leased' });
    const leased = store.check({ mailboxId: 'stream:reuse-leased' });
    assert.ok(leased.deliveryId, 'expected active wave lease');
    store.enqueue([makeAlert('wave-1-followup')], { mailboxId: 'stream:reuse-leased' });

    const result = await service.flushMailbox('stream:reuse-leased', { force: true });
    assert.equal(result.woke, true, JSON.stringify(result));
    assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), baseHead);
    assert.equal(fake.events.some(event => event.startsWith('git:')), false);
    assert.deepEqual(errors, []);
  });

  await t.test('reuse path spawning a replacement terminal syncs before launch', async st => {
    const { root, worktreePath, pushOriginCommit } = createLinkedStandingWorktree(st);
    const newHead = pushOriginCommit('README.md', 'v2\n', 'advance');
    const store = new MailboxStore(join(root, 'mailbox.json'));
    const fake = createFakeOrcaEnv(worktreePath, []);
    const errors: unknown[] = [];
    const service = new WakeupRedriveService({
      store,
      flushIntervalMs: 100_000,
      onError: error => errors.push(error),
      drivers: createOrcaWakeupDrivers({ commandRunner: fake.runner }),
    });
    st.after(async () => { await service.stop(); store.close(); });

    store.upsertWakeTarget('stream:reuse-spawn', {
      driver: 'orca-terminal',
      rotateAfterWaves: 10,
      completedWaves: 2,
      driverConfig: {
        workspacePath: worktreePath,
        terminalHandle: 'term-vanished',
        agent: 'antigravity',
      },
    });
    store.enqueue([makeAlert('reuse-replace')], { mailboxId: 'stream:reuse-spawn' });

    const result = await service.flushMailbox('stream:reuse-spawn', { force: true });
    assert.equal(result.woke, true, JSON.stringify(result));
    assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), newHead);
    const mergeIdx = fake.events.indexOf('git:merge');
    const createIdx = fake.events.indexOf('orca:create:term-new-1');
    assert.ok(mergeIdx >= 0, `expected git:merge in ${JSON.stringify(fake.events)}`);
    assert.ok(createIdx > mergeIdx, `expected terminal create after merge in ${JSON.stringify(fake.events)}`);
    assert.deepEqual(errors, []);
  });
});

test('git fetch failure reports via reportError and does not block wake delivery', async t => {
  const { root, worktreePath, baseHead } = createLinkedStandingWorktree(t);
  runGit(worktreePath, 'remote', 'set-url', 'origin', join(root, 'missing-origin.git'));
  const store = new MailboxStore(join(root, 'mailbox.json'));
  const fake = createFakeOrcaEnv(worktreePath, [{
    handle: 'term-live',
    connected: true,
    writable: true,
    agentIdentity: 'antigravity',
    preview: '> ',
    tabId: 'tab-live',
    leafId: 'root',
  }]);
  const errors: unknown[] = [];
  const service = new WakeupRedriveService({
    store,
    flushIntervalMs: 100_000,
    onError: error => errors.push(error),
    drivers: createOrcaWakeupDrivers({ commandRunner: fake.runner }),
  });
  t.after(async () => { await service.stop(); store.close(); });

  store.upsertWakeTarget('stream:fetch-err', {
    driver: 'orca-terminal',
    rotateAfterWaves: 10,
    completedWaves: 1,
    driverConfig: {
      workspacePath: worktreePath,
      terminalHandle: 'term-live',
      agent: 'antigravity',
    },
  });
  store.enqueue([makeAlert('fetch-fail')], { mailboxId: 'stream:fetch-err' });

  const result = await service.flushMailbox('stream:fetch-err', { force: true });
  assert.equal(result.woke, true, JSON.stringify(result));
  assert.equal(runGit(worktreePath, 'rev-parse', 'HEAD'), baseHead);
  assert.equal(errors.length, 1);
  assert.match(String(errors[0]), /git exited/);
});
