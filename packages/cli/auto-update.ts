import { spawn, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { canonicalJson } from '../core-types/index.ts';
import { isDeadProcessOwner, processOwnerRecord } from '../process-lock/index.ts';

export type FingerprintRecord = { pid: number; instanceId: string; fingerprint: string; startedAt: string };
function sha256Hex(value: string | Buffer): string {
  return createHash('sha256').update(value).digest('hex');
}
const dealerRootFor = (checkoutDir?: string) => resolve(checkoutDir ?? fileURLToPath(new URL('../..', import.meta.url)));
function fileList(root: string, predicate: (name: string) => boolean, output: string[] = []): string[] {
  if (!existsSync(root)) return output;
  let entries: string[];
  try { entries = readdirSync(root).sort(); } catch { return output; }
  for (const name of entries) {
    if (name === '.git' || name === 'node_modules' || name === 'dist') continue;
    const path = join(root, name);
    let stat;
    try { stat = lstatSync(path); } catch { continue; }
    if (stat.isDirectory()) fileList(path, predicate, output);
    else if (predicate(name)) output.push(path);
  }
  return output;
}
function addEntry(entries: Array<[string, string]>, logical: string, path: string): void {
  let value = '<missing>';
  if (existsSync(path)) {
    try { value = sha256Hex(readFileSync(path)); } catch { value = '<unreadable>'; }
  }
  entries.push([logical, value]);
}
export function computeRuntimeFingerprint(stateDir: string, checkoutDir?: string): string {
  const root = dealerRootFor(checkoutDir); const entries: Array<[string, string]> = [];
  addEntry(entries, 'dealer/package.json', join(root, 'package.json'));
  for (const folder of ['bin', 'packages']) for (const path of fileList(join(root, folder), name => /\.(ts|js|json)$/.test(name))) addEntry(entries, `dealer/${relative(root, path)}`, path);
  const configPath = join(stateDir, 'config.json'); addEntry(entries, 'state/config.json', configPath);
  let config: any = {};
  try { config = JSON.parse(readFileSync(configPath, 'utf8')); } catch { /* marker above */ }
  for (const project of Array.isArray(config.projects) ? config.projects : []) {
    const id = String(project.projectId ?? ''); const dealer = typeof project.rootPath === 'string' ? join(project.rootPath, '.dealer') : '';
    addEntry(entries, `project/${id}/project.json`, join(dealer, 'project.json'));
    for (const path of fileList(dealer, name => /\.(ts|js|json)$/.test(name))) {
      if (relative(dealer, path).split('/').includes('tests')) continue;
      addEntry(entries, `project/${id}/${relative(dealer, path)}`, path);
    }
    addEntry(entries, `project/${id}/userPatch`, project.userPatchPath ?? join(stateDir, 'projects', `${id}.patch.json`));
  }
  for (const plugin of Array.isArray(config.userPlugins) ? config.userPlugins : []) {
    const modulePath = typeof plugin === 'string' ? plugin : plugin?.module;
    if (typeof modulePath === 'string') addEntry(entries, `userPlugin/${modulePath}`, resolve(modulePath));
  }
  entries.sort((a, b) => a[0].localeCompare(b[0]));
  return sha256Hex(Buffer.from(canonicalJson(entries)));
}
function atomicJson(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  const temporary = `${path}.${process.pid}.tmp`;
  writeFileSync(temporary, JSON.stringify(value), { mode: 0o600 });
  renameSync(temporary, path);
}
export function writeDaemonFingerprint(stateDir: string, value: FingerprintRecord): void { atomicJson(join(stateDir, 'runtime-fingerprint.json'), value); }
export function readDaemonFingerprint(stateDir: string): FingerprintRecord | undefined { try { return JSON.parse(readFileSync(join(stateDir, 'runtime-fingerprint.json'), 'utf8')); } catch { return undefined; } }
export function removeDaemonFingerprint(stateDir: string, instanceId: string): void { const path = join(stateDir, 'runtime-fingerprint.json'); if (readDaemonFingerprint(stateDir)?.instanceId === instanceId) rmSync(path, { force: true }); }
function acquireUpdateLock(stateDir: string): string | undefined {
  const lock = join(stateDir, 'auto-update.lock'); mkdirSync(stateDir, { recursive: true, mode: 0o700 });
  try { mkdirSync(lock, { mode: 0o700 }); atomicJson(join(lock, 'owner.json'), processOwnerRecord()); return lock; }
  catch {
    try { const owner = JSON.parse(readFileSync(join(lock, 'owner.json'), 'utf8')); if (isDeadProcessOwner(owner, stateDir)) { rmSync(lock, { recursive: true, force: true }); mkdirSync(lock, { mode: 0o700 }); atomicJson(join(lock, 'owner.json'), processOwnerRecord()); return lock; } }
    catch { /* leave lock */ }
    return undefined;
  }
}
export interface SelfUpdateOptions { stateDir: string; checkoutDir?: string; homeDir?: string; geminiDir?: string; }
export function readAutoUpdateStatus(stateDir: string): any { try { return JSON.parse(readFileSync(join(stateDir, 'auto-update.json'), 'utf8')); } catch { return undefined; } }
function syncSkills(root: string, options: SelfUpdateOptions): string[] {
  const home = options.homeDir ?? process.env.HOME ?? '';
  const locations = [[join(home, '.agents'), join(home, '.agents', 'skills')], [join(home, '.claude'), join(home, '.claude', 'skills')], [options.geminiDir ?? join(home, '.gemini'), join(options.geminiDir ?? join(home, '.gemini'), 'config', 'skills')]] as const;
  const synced: string[] = [];
  for (const [parent, destination] of locations) {
    if (!existsSync(parent)) continue;
    mkdirSync(destination, { recursive: true, mode: 0o700 });
    for (const skill of ['the-dealer', 'the-dealer-setup', 'the-dealer-extend']) {
      const target = join(root, 'skills', skill); const link = join(destination, skill);
      if (!existsSync(target)) continue;
      try { if (existsSync(link) || lstatSync(link).isSymbolicLink()) { if (!lstatSync(link).isSymbolicLink()) continue; rmSync(link, { recursive: true, force: true }); } symlinkSync(target, link, 'dir'); synced.push(link); } catch { /* continue */ }
    }
  }
  return synced;
}
export function selfUpdate(options: SelfUpdateOptions): any {
  const checkedAt = new Date().toISOString(); const lock = acquireUpdateLock(options.stateDir);
  if (!lock) return { status: 'skipped_locked', sourceStatus: 'skipped_locked', lastCheckedAt: checkedAt, syncedSkills: [], restartedDaemon: false };
  const previous = readAutoUpdateStatus(options.stateDir) ?? {}; let status = 'skipped'; let sourceStatus = 'missing_checkout'; let headSha: string | undefined; let error: string | undefined; let syncedSkills: string[] = [];
  try {
    const root = options.checkoutDir ?? (existsSync(join(dealerRootFor(), '.git')) ? dealerRootFor() : join(options.homeDir ?? process.env.HOME ?? '', 'the-dealer'));
    if (existsSync(join(root, '.git'))) {
      const git = (args: string[]) => execFileSync('git', args, { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim(); const dirty = git(['status', '--porcelain']); const branch = git(['branch', '--show-current']); headSha = git(['rev-parse', 'HEAD']);
      if (dirty) sourceStatus = 'skipped_dirty_checkout'; else if (branch !== 'main') sourceStatus = 'skipped_non_main_branch'; else { git(['pull', '--ff-only']); const nextSha = git(['rev-parse', 'HEAD']); sourceStatus = nextSha === headSha ? 'up_to_date' : 'updated'; status = sourceStatus === 'updated' ? 'updated' : 'complete'; headSha = nextSha; }
      syncedSkills = syncSkills(root, options);
    }
  } catch (caught) { sourceStatus = 'error'; error = String(caught); }
  const report = { ...previous, lastCheckedAt: checkedAt, ...(status === 'updated' ? { lastUpdatedAt: new Date().toISOString() } : {}), status, sourceStatus, ...(headSha ? { headSha } : {}), syncedSkills, restartedDaemon: false, ...(error ? { error } : {}) };
  atomicJson(join(options.stateDir, 'auto-update.json'), report); writeFileSync(join(options.stateDir, 'auto-update.log'), `${JSON.stringify(report)}\n`, { flag: 'a', mode: 0o600 }); rmSync(lock, { recursive: true, force: true }); return report;
}
export function maybeTriggerBackgroundAutoUpdate(options: SelfUpdateOptions): void {
  if (process.env.DEALER_DISABLE_AUTO_UPDATE === '1' || process.env.DEALER_AUTO_UPDATE === '0') return;
  if (process.env.NODE_TEST_CONTEXT && process.env.DEALER_AUTO_UPDATE !== '1') return;
  const state = readAutoUpdateStatus(options.stateDir); const last = state?.lastCheckedAt ? Date.parse(state.lastCheckedAt) : 0; const now = Date.now();
  const mismatch = existsSync(join(options.stateDir, 'control.sock')) && readDaemonFingerprint(options.stateDir)?.fingerprint !== computeRuntimeFingerprint(options.stateDir, options.checkoutDir);
  if ((!mismatch && last && now - last < 6 * 3600_000) || (mismatch && last && now - last < 30_000)) return;
  const bin = fileURLToPath(new URL('../../bin/dealer.js', import.meta.url)); const child = spawn(process.execPath, ['--no-warnings', '--experimental-strip-types', bin, 'self-update', '--quiet', '--json'], { detached: true, stdio: 'ignore', env: { ...process.env, DEALER_STATE_DIR: options.stateDir } }); child.unref();
}
