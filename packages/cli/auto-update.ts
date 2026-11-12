import { createHash } from 'node:crypto';
import { execFileSync, spawn } from 'node:child_process';
import { existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, rmSync, symlinkSync, writeFileSync, renameSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { canonicalJson } from '../core-types/index.ts';
import { isDeadProcessOwner, processOwnerRecord } from '../process-lock/index.ts';

type FingerprintRecord = { pid: number; instanceId: string; fingerprint: string; startedAt: string };
const digest = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const dealerRootFor = (checkoutDir?: string) => resolve(checkoutDir ?? fileURLToPath(new URL('../..', import.meta.url)));

function files(root: string, predicate: (name: string) => boolean, out: string[] = [], prefix = ''): string[] {
  if (!existsSync(root)) return out;
  let entries: string[] = [];
  try { entries = readdirSync(root).sort(); } catch { return out; }
  for (const name of entries) {
    if (name === '.git' || name === 'node_modules' || name === 'dist') continue;
    const path = join(root, name); const logical = prefix ? join(prefix, name) : name;
    let stat; try { stat = lstatSync(path); } catch { continue; }
    if (stat.isDirectory()) files(path, predicate, out, logical);
    else if (predicate(name)) out.push(path);
  }
  return out;
}

function addEntry(entries: Array<[string, string]>, logical: string, path: string): void {
  if (!existsSync(path)) { entries.push([logical, '<missing>']); return; }
  try { entries.push([logical, digest(readFileSync(path))]); } catch { entries.push([logical, '<unreadable>']); }
}

export function computeRuntimeFingerprint(stateDir: string, checkoutDir?: string): string {
  const root = dealerRootFor(checkoutDir); const entries: Array<[string, string]> = [];
  addEntry(entries, 'dealer/package.json', join(root, 'package.json'));
  for (const base of ['bin', 'packages']) for (const path of files(join(root, base), n => /\.(?:ts|js|json)$/.test(n))) addEntry(entries, `dealer/${relative(root, path)}`, path);
  const configPath = join(stateDir, 'config.json'); addEntry(entries, 'state/config.json', configPath);
  let config: any = {}; try { config = JSON.parse(readFileSync(configPath, 'utf8')); } catch {}
  for (const project of Array.isArray(config.projects) ? config.projects : []) {
    const projectRoot = typeof project.rootPath === 'string' ? project.rootPath : '';
    const dealer = projectRoot ? join(projectRoot, '.dealer') : '';
    addEntry(entries, `project/${project.projectId ?? ''}/project.json`, join(dealer, 'project.json'));
    for (const path of files(dealer, n => /\.(?:ts|js|json)$/.test(n))) {
      if (relative(dealer, path).split('/').includes('tests')) continue;
      addEntry(entries, `project/${project.projectId ?? ''}/${relative(dealer, path)}`, path);
    }
    const patch = project.userPatchPath ?? join(stateDir, 'projects', `${project.projectId}.patch.json`);
    if (patch) addEntry(entries, `project/${project.projectId ?? ''}/userPatch`, patch);
  }
  for (const plugin of Array.isArray(config.userPlugins) ? config.userPlugins : []) {
    const modulePath = typeof plugin === 'string' ? plugin : plugin.module;
    if (typeof modulePath === 'string') addEntry(entries, `userPlugin/${modulePath}`, resolve(modulePath));
  }
  entries.sort((a, b) => a[0].localeCompare(b[0]));
  return digest(Buffer.from(canonicalJson(entries)));
}

const atomicJson = (path: string, value: unknown) => { mkdirSync(dirname(path), { recursive: true, mode: 0o700 }); const tmp = `${path}.${process.pid}.tmp`; writeFileSync(tmp, JSON.stringify(value)); renameSync(tmp, path); };
export function writeDaemonFingerprint(stateDir: string, value: FingerprintRecord): void { atomicJson(join(stateDir, 'runtime-fingerprint.json'), value); }
export function readDaemonFingerprint(stateDir: string): FingerprintRecord | undefined { try { return JSON.parse(readFileSync(join(stateDir, 'runtime-fingerprint.json'), 'utf8')); } catch { return undefined; } }
export function removeDaemonFingerprint(stateDir: string, instanceId: string): void { const path = join(stateDir, 'runtime-fingerprint.json'); const current = readDaemonFingerprint(stateDir); if (current?.instanceId === instanceId) rmSync(path, { force: true }); }

function acquire(stateDir: string): string | undefined {
  const dir = join(stateDir, 'auto-update.lock'); mkdirSync(stateDir, { recursive: true, mode: 0o700 });
  try { mkdirSync(dir, { mode: 0o700 }); atomicJson(join(dir, 'owner.json'), processOwnerRecord()); return dir; } catch {
    try { const owner = JSON.parse(readFileSync(join(dir, 'owner.json'), 'utf8')); if (isDeadProcessOwner(owner, stateDir)) { rmSync(dir, { recursive: true, force: true }); mkdirSync(dir); atomicJson(join(dir, 'owner.json'), processOwnerRecord()); return dir; } } catch {}
    return undefined;
  }
}

export interface SelfUpdateOptions { stateDir: string; checkoutDir?: string; homeDir?: string; geminiDir?: string; }
export function readAutoUpdateStatus(stateDir: string): any { try { return JSON.parse(readFileSync(join(stateDir, 'auto-update.json'), 'utf8')); } catch { return undefined; } }
export function selfUpdate(options: SelfUpdateOptions): any {
  const started = new Date().toISOString(); const lock = acquire(options.stateDir);
  if (!lock) return { status: 'skipped_locked', sourceStatus: 'skipped_locked', lastCheckedAt: started, syncedSkills: [], restartedDaemon: false };
  let sourceStatus = 'missing_checkout'; let status = 'skipped'; let headSha: string | undefined; let error: string | undefined; const syncedSkills: string[] = [];
  try {
    const root = options.checkoutDir ?? (existsSync(join(dealerRootFor(), '.git')) ? dealerRootFor() : join(options.homeDir ?? process.env.HOME ?? '', 'the-dealer'));
    if (existsSync(join(root, '.git'))) {
      const run = (args: string[]) => execFileSync('git', args, { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
      const dirty = run(['status', '--porcelain']); const branch = run(['branch', '--show-current']); headSha = run(['rev-parse', 'HEAD']);
      if (dirty) sourceStatus = 'skipped_dirty_checkout'; else if (branch !== 'main') sourceStatus = 'skipped_non_main_branch'; else { run(['pull', '--ff-only']); const next = run(['rev-parse', 'HEAD']); sourceStatus = next === headSha ? 'up_to_date' : 'updated'; headSha = next; status = sourceStatus === 'updated' ? 'updated' : 'complete'; }
      const homes = [options.homeDir ?? process.env.HOME ?? '', join(options.homeDir ?? process.env.HOME ?? '', '.claude'), options.geminiDir ?? join(options.homeDir ?? process.env.HOME ?? '', '.gemini')];
      for (const base of homes) { const parent = base.endsWith('.gemini') ? join(base, 'config', 'skills') : join(base, 'skills'); if (!existsSync(dirname(parent))) continue; mkdirSync(parent, { recursive: true }); for (const skill of ['the-dealer', 'the-dealer-setup', 'the-dealer-extend']) { const target = join(root, 'skills', skill); const link = join(parent, skill); if (!existsSync(target)) continue; try { if (existsSync(link) || lstatSync(link).isSymbolicLink()) { if (!lstatSync(link).isSymbolicLink()) continue; rmSync(link, { recursive: true, force: true }); } symlinkSync(target, link, 'dir'); syncedSkills.push(link); } catch {} } }
    }
  } catch (e) { error = String(e); sourceStatus = 'error'; }
  const report = { lastCheckedAt: started, ...(status === 'updated' ? { lastUpdatedAt: new Date().toISOString() } : {}), status, sourceStatus, headSha, syncedSkills, restartedDaemon: false, ...(error ? { error } : {}) };
  atomicJson(join(options.stateDir, 'auto-update.json'), report); writeFileSync(join(options.stateDir, 'auto-update.log'), `${JSON.stringify(report)}\n`, { flag: 'a', mode: 0o600 }); rmSync(lock, { recursive: true, force: true }); return report;
}

export function maybeTriggerBackgroundAutoUpdate(options: SelfUpdateOptions): void {
  if (process.env.DEALER_DISABLE_AUTO_UPDATE === '1' || (process.env.DEALER_AUTO_UPDATE === '0') || (process.env.NODE_TEST_CONTEXT && process.env.DEALER_AUTO_UPDATE !== '1')) return;
  const last = readAutoUpdateStatus(options.stateDir)?.lastCheckedAt; if (last && Date.now() - Date.parse(last) < 6 * 3600_000) return;
  const bin = fileURLToPath(new URL('../../bin/dealer.js', import.meta.url)); const child = spawn(process.execPath, ['--no-warnings', '--experimental-strip-types', bin, 'self-update', '--quiet', '--json'], { detached: true, stdio: 'ignore', env: { ...process.env, DEALER_STATE_DIR: options.stateDir } }); child.unref();
}
