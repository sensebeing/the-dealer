import { spawn, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { canonicalJson } from '../core-types/index.ts';
import { isDeadProcessOwner, processOwnerRecord, readProcessOwner } from '../process-lock/index.ts';

export type FingerprintRecord = {
  pid: number;
  instanceId: string;
  fingerprint: string;
  startedAt: string;
};

function sha256Hex(value: string | Buffer): string {
  return createHash('sha256').update(value).digest('hex');
}

const dealerRootFor = (checkoutDir?: string) =>
  resolve(checkoutDir ?? fileURLToPath(new URL('../..', import.meta.url)));

function fileList(root: string, predicate: (name: string) => boolean, output: string[] = []): string[] {
  if (!existsSync(root)) {
    return output;
  }
  let entries: string[];
  try {
    entries = readdirSync(root).sort();
  } catch {
    return output;
  }
  for (const name of entries) {
    if (name === '.git' || name === 'node_modules' || name === 'dist') {
      continue;
    }
    const path = join(root, name);
    let stat;
    try {
      stat = lstatSync(path);
    } catch {
      continue;
    }
    if (stat.isDirectory()) {
      fileList(path, predicate, output);
    } else if (predicate(name)) {
      output.push(path);
    }
  }
  return output;
}

function addEntry(entries: Array<[string, string]>, logical: string, path: string): void {
  let value = '<missing>';
  if (existsSync(path)) {
    try {
      value = sha256Hex(readFileSync(path));
    } catch {
      value = '<unreadable>';
    }
  }
  entries.push([logical, value]);
}

export type FingerprintEntries = Array<[string, string]>;

/** Entries for the code this process actually loaded; the daemon freezes them at startup. */
export function dealerCodeFingerprintEntries(checkoutDir?: string): FingerprintEntries {
  const root = dealerRootFor(checkoutDir);
  const entries: FingerprintEntries = [];
  addEntry(entries, 'dealer/package.json', join(root, 'package.json'));
  for (const folder of ['bin', 'packages']) {
    for (const path of fileList(join(root, folder), name => /\.(ts|js|json)$/.test(name))) {
      addEntry(entries, `dealer/${relative(root, path)}`, path);
    }
  }
  return entries;
}

function manifestPlugins(manifest: unknown): Array<{ name: string; module: string }> {
  if (!manifest || typeof manifest !== 'object' || Array.isArray(manifest)) {
    return [];
  }
  const plugins = (manifest as { plugins?: unknown }).plugins;
  if (!Array.isArray(plugins)) {
    return [];
  }
  const result: Array<{ name: string; module: string }> = [];
  for (const plugin of plugins) {
    if (!plugin || typeof plugin !== 'object' || Array.isArray(plugin)) {
      continue;
    }
    const { name, module } = plugin as { name?: unknown; module?: unknown };
    if (typeof name === 'string' && typeof module === 'string') {
      result.push({ name, module });
    }
  }
  return result;
}

/** Entries for one project exactly as the runtime loads it: manifest, project code, patch and plugin modules. */
export function projectFingerprintEntries(
  projectId: string,
  projectRoot: string,
  manifest: unknown,
  stateDir: string,
  userPatchPath?: string,
): FingerprintEntries {
  const dealer = join(projectRoot, '.dealer');
  const entries: FingerprintEntries = [];
  addEntry(entries, `project/${projectId}/project.json`, join(dealer, 'project.json'));
  for (const path of fileList(dealer, name => /\.(ts|js|json)$/.test(name))) {
    const logical = relative(dealer, path);
    if (logical === 'project.json' || logical.split('/').includes('tests')) {
      continue;
    }
    addEntry(entries, `project/${projectId}/${logical}`, path);
  }
  addEntry(entries, `project/${projectId}/userPatch`, userPatchPath ?? join(stateDir, 'projects', `${projectId}.patch.json`));
  for (const plugin of manifestPlugins(manifest)) {
    addEntry(entries, `project/${projectId}/plugin/${plugin.name}`, resolve(projectRoot, plugin.module));
  }
  return entries;
}

/** Entries for user plugins, resolved against stateDir exactly like the runtime loader. */
export function userPluginFingerprintEntries(stateDir: string, userPlugins: unknown): FingerprintEntries {
  const entries: FingerprintEntries = [];
  for (const plugin of Array.isArray(userPlugins) ? userPlugins : []) {
    const modulePath = typeof plugin === 'string' ? plugin : (plugin as { module?: unknown } | null)?.module;
    if (typeof modulePath !== 'string') {
      continue;
    }
    const name = plugin && typeof plugin === 'object' && typeof (plugin as { name?: unknown }).name === 'string'
      ? String((plugin as { name: string }).name)
      : modulePath;
    addEntry(entries, `userPlugin/${name}`, resolve(stateDir, modulePath));
  }
  return entries;
}

export function composeRuntimeFingerprint(sections: readonly FingerprintEntries[]): string {
  const entries = sections.flat();
  entries.sort((a, b) => a[0].localeCompare(b[0]));
  return sha256Hex(Buffer.from(canonicalJson(entries)));
}

/**
 * Disk view of the runtime fingerprint. Reads config.json only to discover which projects
 * and user plugins are configured; mutable config fields never participate in the hash.
 */
export function computeRuntimeFingerprint(stateDir: string, checkoutDir?: string): string {
  const sections: FingerprintEntries[] = [dealerCodeFingerprintEntries(checkoutDir)];
  const configPath = join(stateDir, 'config.json');
  let config: any = {};
  try {
    config = JSON.parse(readFileSync(configPath, 'utf8'));
  } catch {
    // Discovery-only read: an unreadable config contributes no configured projects or plugins.
  }
  for (const project of Array.isArray(config.projects) ? config.projects : []) {
    const id = String(project.projectId ?? '');
    const rootPath = typeof project.rootPath === 'string' ? project.rootPath : '';
    let manifest: unknown;
    try {
      manifest = JSON.parse(readFileSync(join(rootPath, '.dealer', 'project.json'), 'utf8'));
    } catch {
      manifest = undefined;
    }
    sections.push(projectFingerprintEntries(id, rootPath, manifest, stateDir, project.userPatchPath));
  }
  sections.push(userPluginFingerprintEntries(stateDir, config.userPlugins));
  return composeRuntimeFingerprint(sections);
}

function atomicJson(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  const temporary = `${path}.${process.pid}.tmp`;
  writeFileSync(temporary, JSON.stringify(value), { mode: 0o600 });
  renameSync(temporary, path);
}

export function writeDaemonFingerprint(stateDir: string, value: FingerprintRecord): void {
  atomicJson(join(stateDir, 'runtime-fingerprint.json'), value);
}

export function readDaemonFingerprint(stateDir: string): FingerprintRecord | undefined {
  try {
    return JSON.parse(readFileSync(join(stateDir, 'runtime-fingerprint.json'), 'utf8'));
  } catch {
    return undefined;
  }
}

export function removeDaemonFingerprint(stateDir: string, instanceId: string): void {
  const path = join(stateDir, 'runtime-fingerprint.json');
  if (readDaemonFingerprint(stateDir)?.instanceId === instanceId) {
    rmSync(path, { force: true });
  }
}

type UpdateLockResult =
  | { lock: string; locked?: never; error?: never }
  | { lock?: never; locked: true; error?: never }
  | { lock?: never; locked?: never; error: string };

function claimStagedLock(stage: string, lock: string): boolean {
  try {
    renameSync(stage, lock);
    return true;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === 'EEXIST' || code === 'ENOTEMPTY') {
      return false;
    }
    throw error;
  }
}

function acquireReclaimGate(reclaim: string, stateDir: string): boolean {
  try {
    mkdirSync(reclaim, { mode: 0o700 });
  } catch {
    const gateOwner = readProcessOwner(join(reclaim, 'owner.json'));
    if (gateOwner && !isDeadProcessOwner(gateOwner, stateDir)) {
      return false;
    }
    rmSync(reclaim, { recursive: true, force: true });
    try {
      mkdirSync(reclaim, { mode: 0o700 });
    } catch {
      return false;
    }
  }
  try {
    atomicJson(join(reclaim, 'owner.json'), processOwnerRecord());
  } catch {
    rmSync(reclaim, { recursive: true, force: true });
    return false;
  }
  return true;
}

/**
 * Crash-recoverable claim: the owner record is staged before the rename, so a lock is
 * never observable without a provable owner. A lock whose owner is dead is reclaimed
 * through an owner-backed gate; a lock without any readable owner is surfaced as an
 * error instead of disabling updates silently.
 */
function acquireUpdateLock(stateDir: string): UpdateLockResult {
  const lock = join(stateDir, 'auto-update.lock');
  const stage = `${lock}.stage.${process.pid}`;
  const prepareStage = () => {
    rmSync(stage, { recursive: true, force: true });
    mkdirSync(stage, { mode: 0o700 });
    atomicJson(join(stage, 'owner.json'), processOwnerRecord());
  };
  mkdirSync(stateDir, { recursive: true, mode: 0o700 });
  try {
    prepareStage();
    if (!existsSync(lock) && claimStagedLock(stage, lock)) {
      return { lock };
    }
  } catch (error) {
    rmSync(stage, { recursive: true, force: true });
    return { error: `Unable to prepare auto-update lock: ${String(error)}` };
  }
  rmSync(stage, { recursive: true, force: true });
  const owner = readProcessOwner(join(lock, 'owner.json'));
  if (!owner) {
    return { error: `Auto-update lock ${lock} has no readable owner record` };
  }
  if (!isDeadProcessOwner(owner, stateDir)) {
    return { locked: true };
  }
  const reclaim = `${lock}.reclaim`;
  const stale = `${lock}.stale.${process.pid}`;
  if (!acquireReclaimGate(reclaim, stateDir)) {
    return { locked: true };
  }
  try {
    // Re-read the owner while holding the gate: a competitor may have claimed the
    // lock after this process failed to create it.
    const current = readProcessOwner(join(lock, 'owner.json'));
    if (current && !isDeadProcessOwner(current, stateDir)) {
      return { locked: true };
    }
    // Move the stale lock out of the claim path before staging the replacement, so
    // the path is never observably missing while this process holds the gate.
    rmSync(stale, { recursive: true, force: true });
    renameSync(lock, stale);
    prepareStage();
    return claimStagedLock(stage, lock) ? { lock } : { locked: true };
  } catch (error) {
    return { error: `Unable to reclaim auto-update lock: ${String(error)}` };
  } finally {
    rmSync(stale, { recursive: true, force: true });
    rmSync(stage, { recursive: true, force: true });
    rmSync(reclaim, { recursive: true, force: true });
  }
}

export interface SelfUpdateOptions {
  stateDir: string;
  checkoutDir?: string;
  homeDir?: string;
  geminiDir?: string;
}

export function readAutoUpdateStatus(stateDir: string): any {
  try {
    return JSON.parse(readFileSync(join(stateDir, 'auto-update.json'), 'utf8'));
  } catch {
    return undefined;
  }
}

function syncSkills(root: string, options: SelfUpdateOptions): string[] {
  const home = options.homeDir ?? process.env.HOME ?? '';
  const locations = [
    [join(home, '.agents'), join(home, '.agents', 'skills')],
    [join(home, '.claude'), join(home, '.claude', 'skills')],
    [options.geminiDir ?? join(home, '.gemini'), join(options.geminiDir ?? join(home, '.gemini'), 'config', 'skills')],
  ] as const;
  const synced: string[] = [];
  for (const [parent, destination] of locations) {
    if (!existsSync(parent)) {
      continue;
    }
    mkdirSync(destination, { recursive: true, mode: 0o700 });
    for (const skill of ['the-dealer', 'the-dealer-setup', 'the-dealer-extend']) {
      const target = join(root, 'skills', skill);
      const link = join(destination, skill);
      if (!existsSync(target)) {
        continue;
      }
      try {
        let existing;
        try {
          existing = lstatSync(link);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
            throw error;
          }
        }
        if (existing && !existing.isSymbolicLink()) {
          continue;
        }
        if (existing) {
          rmSync(link, { recursive: true, force: true });
        }
        symlinkSync(target, link, 'dir');
        synced.push(link);
      } catch (error) {
        throw new Error(`Unable to sync skill link ${link}: ${String(error)}`);
      }
    }
  }
  return synced;
}

export function selfUpdate(options: SelfUpdateOptions): any {
  const checkedAt = new Date().toISOString();
  const acquired = acquireUpdateLock(options.stateDir);
  if (acquired.error) {
    return { status: 'failed', sourceStatus: 'corrupt_lock', lastCheckedAt: checkedAt, syncedSkills: [], restartedDaemon: false, error: acquired.error };
  }
  if (!acquired.lock) {
    return { status: 'skipped_locked', sourceStatus: 'skipped_locked', lastCheckedAt: checkedAt, syncedSkills: [], restartedDaemon: false };
  }
  const lock = acquired.lock;
  const previous = readAutoUpdateStatus(options.stateDir) ?? {};
  let status = 'skipped';
  let sourceStatus = 'missing_checkout';
  let headSha: string | undefined;
  let error: string | undefined;
  let syncedSkills: string[] = [];
  try {
    const root = options.checkoutDir ?? (existsSync(join(dealerRootFor(), '.git')) ? dealerRootFor() : join(options.homeDir ?? process.env.HOME ?? '', 'the-dealer'));
    if (existsSync(join(root, '.git'))) {
      const git = (args: string[]) => execFileSync('git', args, { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
      const dirty = git(['status', '--porcelain']);
      const branch = git(['branch', '--show-current']);
      headSha = git(['rev-parse', 'HEAD']);
      if (dirty) {
        sourceStatus = 'skipped_dirty_checkout';
      } else if (branch !== 'main') {
        sourceStatus = 'skipped_non_main_branch';
      } else {
        git(['pull', '--ff-only']);
        const nextSha = git(['rev-parse', 'HEAD']);
        sourceStatus = nextSha === headSha ? 'up_to_date' : 'updated';
        status = sourceStatus === 'updated' ? 'updated' : 'complete';
        headSha = nextSha;
      }
      syncedSkills = syncSkills(root, options);
    }
  } catch (caught) {
    sourceStatus = 'error';
    error = String(caught);
  }
  const report = { ...previous, lastCheckedAt: checkedAt, ...(status === 'updated' ? { lastUpdatedAt: new Date().toISOString() } : {}), status, sourceStatus, ...(headSha ? { headSha } : {}), syncedSkills, restartedDaemon: false, ...(error ? { error } : {}) };
  atomicJson(join(options.stateDir, 'auto-update.json'), report);
  writeFileSync(join(options.stateDir, 'auto-update.log'), `${JSON.stringify(report)}\n`, { flag: 'a', mode: 0o600 });
  rmSync(lock, { recursive: true, force: true });
  return report;
}

export function maybeTriggerBackgroundAutoUpdate(options: SelfUpdateOptions): void {
  if (process.env.DEALER_DISABLE_AUTO_UPDATE === '1' || process.env.DEALER_AUTO_UPDATE === '0') {
    return;
  }
  if (process.env.NODE_TEST_CONTEXT && process.env.DEALER_AUTO_UPDATE !== '1') {
    return;
  }
  const state = readAutoUpdateStatus(options.stateDir);
  const last = state?.lastCheckedAt ? Date.parse(state.lastCheckedAt) : 0;
  const now = Date.now();
  const mismatch = existsSync(join(options.stateDir, 'control.sock')) && readDaemonFingerprint(options.stateDir)?.fingerprint !== computeRuntimeFingerprint(options.stateDir, options.checkoutDir);
  if ((!mismatch && last && now - last < 6 * 3600_000) || (mismatch && last && now - last < 30_000)) {
    return;
  }
  const bin = fileURLToPath(new URL('../../bin/dealer.js', import.meta.url));
  const child = spawn(process.execPath, ['--no-warnings', '--experimental-strip-types', bin, 'self-update', '--quiet', '--json'], { detached: true, stdio: 'ignore', env: { ...process.env, DEALER_STATE_DIR: options.stateDir } });
  child.unref();
}
