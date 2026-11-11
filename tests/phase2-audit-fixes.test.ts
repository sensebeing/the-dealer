import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DealerNodeRuntime } from '../packages/index.ts';

function directory(t: { after: (fn: () => void) => void }) {
  const dir = mkdtempSync(join(tmpdir(), 'dealer-audit-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

test('a container restart reclaims the same-PID lock its previous incarnation left', async t => {
  const root = directory(t);
  const stateDir = join(root, 'state');
  const lockDir = join(stateDir, 'runtime.lock');
  // A container restarted after SIGKILL hands the daemon the same PID (1, or 7 under
  // --init), so the previous incarnation's owner file names the process that boots now.
  mkdirSync(lockDir, { recursive: true, mode: 0o700 });
  writeFileSync(join(lockDir, 'owner.json'), JSON.stringify({ pid: process.pid }));

  const runtime = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  t.after(() => runtime.stop());
  await runtime.start();
  assert.deepEqual(JSON.parse(readFileSync(join(lockDir, 'owner.json'), 'utf8')), { pid: process.pid });

  // While this process really owns the state directory, a second start stays refused.
  const blocked = new DealerNodeRuntime({ stateDir, transport: { discoveryDir: join(root, 'discovery') } });
  await assert.rejects(blocked.start(), /State directory already owned/);

  await runtime.stop();
  assert.equal(existsSync(lockDir), false, 'the reclaimed lock is released by its owner');
});
