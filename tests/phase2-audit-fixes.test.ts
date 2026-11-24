import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { hostname, tmpdir } from 'node:os';
import { join } from 'node:path';
import { DealerNodeRuntime, MailboxStore } from '../packages/index.ts';
import type { MessageEnvelope } from '../packages/index.ts';

function directory(t: { after: (fn: () => void) => void }) {
  const dir = mkdtempSync(join(tmpdir(), 'dealer-audit-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

function envelope(id: string): MessageEnvelope {
  return {
    id,
    networkId: 'team',
    traceId: id,
    from: { nodeId: 'ed25519:' + 'a'.repeat(64), member: 'alice', nodeName: 'laptop' },
    routing: { mode: 'broadcast' },
    kind: 'triage',
    priority: 'normal',
    subject: id,
    body: 'body',
    payload: {},
    attachments: [],
    createdAt: new Date().toISOString(),
    signature: '',
  };
}

/** A previous incarnation of this very container: same PID, same host, older token. */
function previousIncarnation(): string {
  return JSON.stringify({ pid: process.pid, instanceId: 'previous-incarnation', host: hostname() });
}

test('a container restart reclaims the same-PID lock its previous incarnation left', async t => {
  const root = directory(t);
  const stateDir = join(root, 'state');
  const lockDir = join(stateDir, 'runtime.lock');
  // A container restarted after SIGKILL hands the daemon the same PID (1, or 7 under
  // --init), so the previous incarnation's owner file names the process that boots now.
  mkdirSync(lockDir, { recursive: true, mode: 0o700 });
  writeFileSync(join(lockDir, 'owner.json'), previousIncarnation());

  const runtime = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  t.after(() => runtime.stop());
  await runtime.start();
  const owner = JSON.parse(readFileSync(join(lockDir, 'owner.json'), 'utf8'));
  assert.deepEqual(Object.keys(owner).sort(), ['host', 'instanceId', 'pid']);
  assert.equal(owner.pid, process.pid);

  // While this process really owns the state directory, a second start stays refused.
  const blocked = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await assert.rejects(blocked.start(), /State directory already owned/);

  await runtime.stop();
  assert.equal(existsSync(lockDir), false, 'the reclaimed lock is released by its owner');
});

test('a same-PID lock recorded by another container is never reclaimed', async t => {
  const root = directory(t);
  const stateDir = join(root, 'state');
  const lockDir = join(stateDir, 'runtime.lock');
  mkdirSync(lockDir, { recursive: true, mode: 0o700 });
  // Another container can hold the same PID number against the same volume; the host
  // is what separates it from a predecessor of this container, so its lock survives.
  writeFileSync(
    join(lockDir, 'owner.json'),
    JSON.stringify({ pid: process.pid, instanceId: 'other-container', host: 'another-container-host' }),
  );
  const blocked = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await assert.rejects(blocked.start(), /State directory already owned/);
  assert.equal(existsSync(join(lockDir, 'owner.json')), true, 'a live foreign container keeps its lock');
});

test('mailbox recovers a committed batch after its writer is killed', t => {
  const root = directory(t);
  const path = join(root, 'mailbox.json');
  const moduleUrl = new URL('../packages/mailbox/store.ts', import.meta.url).href;
  const script = `import { MailboxStore } from ${JSON.stringify(moduleUrl)};
    const store = new MailboxStore(${JSON.stringify(path)});
    store.enqueue([${JSON.stringify(envelope('before-crash'))}]);
    process.kill(process.pid, 'SIGKILL');`;
  const writer = spawnSync(process.execPath, ['--experimental-strip-types', '--input-type=module', '-e', script]);
  assert.equal(writer.signal, 'SIGKILL', writer.stderr.toString());
  const store = new MailboxStore(path);
  t.after(() => store.close());
  const batch = store.check();
  assert.deepEqual(batch.items.map(item => item.id), ['before-crash']);
  assert.deepEqual(store.ack(batch.deliveryId!).itemIds, ['before-crash']);
  assert.deepEqual(store.enqueue([envelope('after-restart')]).inserted, ['after-restart']);
});
