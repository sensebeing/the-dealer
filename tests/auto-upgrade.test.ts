import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { computeRuntimeFingerprint, selfUpdate } from '../packages/cli/auto-update.ts';
import { runDealerCli, stopDaemonIfRunning } from '../packages/cli/index.ts';

function isolated() {
  const root = mkdtempSync(join(tmpdir(), 'dealer-auto-upgrade-'));
  const stateDir = join(root, 'state');
  mkdirSync(stateDir, { recursive: true });
  return { root, stateDir };
}

test('runtime fingerprint changes for linked project files', t => {
  const { root, stateDir } = isolated();
  t.after(() => { void stopDaemonIfRunning(stateDir); });
  const project = join(root, 'project');
  mkdirSync(join(project, '.dealer'), { recursive: true });
  writeFileSync(join(project, '.dealer', 'project.json'), JSON.stringify({ projectId: 'p' }));
  writeFileSync(join(stateDir, 'config.json'), JSON.stringify({ projects: [{ projectId: 'p', rootPath: project }] }));
  const before = computeRuntimeFingerprint(stateDir);
  writeFileSync(join(project, '.dealer', 'project.json'), JSON.stringify({ projectId: 'p', changed: true }));
  assert.notEqual(computeRuntimeFingerprint(stateDir), before);
});

test('self update reports missing isolated checkout safely', t => {
  const { root, stateDir } = isolated();
  t.after(() => { void stopDaemonIfRunning(stateDir); });
  const report = selfUpdate({ stateDir, checkoutDir: join(root, 'missing'), homeDir: join(root, 'home'), geminiDir: join(root, 'gemini') });
  assert.equal(report.sourceStatus, 'missing_checkout');
});

test('node down is safe when daemon is absent', async t => {
  const { stateDir } = isolated();
  t.after(() => { void stopDaemonIfRunning(stateDir); });
  let output = '';
  const code = await runDealerCli(['node', 'down', '--json'], { stateDir, stdout: line => { output = line; } });
  assert.equal(code, 0);
  assert.equal(JSON.parse(output).running, false);
});

test('isolated state directories keep tests independent', t => {
  const first = isolated();
  const second = isolated();
  t.after(() => { void stopDaemonIfRunning(first.stateDir); void stopDaemonIfRunning(second.stateDir); });
  assert.notEqual(first.stateDir, second.stateDir);
});
