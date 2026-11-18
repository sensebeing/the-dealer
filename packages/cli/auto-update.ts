import { spawn, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, readlinkSync, realpathSync, rmSync, symlinkSync, unlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { canonicalJson } from '../core-types/index.ts';
import { appendFileDurably, tryAcquireOwnedLock, withOwnedLock, writeFileAtomically } from '../fs-atomic/index.ts';
import { isDeadProcessOwner, readProcessOwner } from '../process-lock/index.ts';

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
  writeFileAtomically(path, JSON.stringify(value));
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

export function liveDaemonOwner(stateDir: string) {
  try {
    const owner = readProcessOwner(join(stateDir, 'runtime.lock', 'owner.json'));
    return owner && !isDeadProcessOwner(owner, stateDir) ? owner : undefined;
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

export interface SelfUpdateOptions {
  stateDir: string;
  checkoutDir?: string;
  homeDir?: string;
  geminiDir?: string;
}

export interface SelfUpdateReport {
  status: string;
  sourceStatus: string;
  lastCheckedAt: string;
  lastUpdatedAt?: string;
  headSha?: string;
  runtimeFingerprint?: string;
  syncedSkills: string[];
  syncedBinaries: string[];
  restartedDaemon: boolean;
  stashedLocalChanges?: boolean;
  error?: string;
}

export function readAutoUpdateStatus(stateDir: string): SelfUpdateReport | undefined {
  try {
    return JSON.parse(readFileSync(join(stateDir, 'auto-update.json'), 'utf8'));
  } catch {
    return undefined;
  }
}

function checkoutGit(root: string, args: string[]): string {
  return execFileSync('git', args, { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
}

function updateCheckoutFor(options: SelfUpdateOptions): string {
  const root = dealerRootFor(options.checkoutDir);
  if (options.checkoutDir !== undefined) {
    return root;
  }
  const canonical = resolve(options.homeDir ?? process.env.HOME ?? '', 'the-dealer');
  if (root === canonical) {
    return root;
  }
  return existsSync(join(canonical, '.git')) &&
    (!existsSync(join(root, '.git')) || checkoutGit(root, ['branch', '--show-current']) !== 'main')
    ? canonical
    : root;
}

const UPDATE_SOURCE_LOCK_TIMEOUT_MS = 30_000;

function withUpdateSourceLocks<T>(root: string, options: SelfUpdateOptions, update: () => T): T {
  const gitCommonDir = realpathSync(resolve(root, checkoutGit(root, ['rev-parse', '--git-common-dir'])));
  const checkoutLock = join(gitCommonDir, 'dealer-self-update.lock');
  const home = resolve(options.homeDir ?? process.env.HOME ?? '');
  return withOwnedLock(checkoutLock, 'Auto-update checkout', () => {
    if (!existsSync(home)) {
      return update();
    }
    // The home can itself be a worktree. Keep the installation lock outside it.
    const homeLock = join(tmpdir(), `dealer-self-update-home-${sha256Hex(realpathSync(home))}.lock`);
    return withOwnedLock(homeLock, 'Auto-update home', update, UPDATE_SOURCE_LOCK_TIMEOUT_MS);
  }, UPDATE_SOURCE_LOCK_TIMEOUT_MS);
}

function syncLink(target: string, link: string, type: 'file' | 'dir'): boolean {
  const existing = lstatSync(link, { throwIfNoEntry: false });
  if (existing) {
    if (!existing.isSymbolicLink()) {
      return false;
    }
    if (readlinkSync(link) === target) {
      return true;
    }
    unlinkSync(link);
  }
  symlinkSync(target, link, type);
  return true;
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
        if (syncLink(target, link, 'dir')) {
          synced.push(link);
        }
      } catch (error) {
        throw new Error(`Unable to sync skill link ${link}: ${String(error)}`);
      }
    }
  }
  return synced;
}

function syncBinaries(root: string, options: SelfUpdateOptions): string[] {
  const destination = join(options.homeDir ?? process.env.HOME ?? '', '.local', 'bin');
  if (!existsSync(destination)) {
    return [];
  }
  const synced: string[] = [];
  for (const name of ['dealer', 'dealerd']) {
    const link = join(destination, name);
    if (syncLink(join(root, 'bin', `${name}.js`), link, 'file')) {
      synced.push(link);
    }
  }
  return synced;
}

function persistUpdateReport(stateDir: string, report: SelfUpdateReport): void {
  atomicJson(join(stateDir, 'auto-update.json'), report);
  appendFileDurably(join(stateDir, 'auto-update.log'), `${JSON.stringify(report)}\n`);
}

type ReconcileDaemon = (runtimeFingerprint: string, checkoutDir: string) => Promise<boolean>;

async function reconcileUpdateReport(
  stateDir: string,
  report: SelfUpdateReport,
  reconcile: () => Promise<boolean>,
): Promise<SelfUpdateReport> {
  let restarted: boolean;
  try {
    restarted = await reconcile();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    report.status = 'failed';
    report.error = report.error ? `${report.error}; ${message}` : message;
    persistUpdateReport(stateDir, report);
    throw error;
  }
  if (restarted) {
    report.restartedDaemon = true;
    report.lastUpdatedAt = new Date().toISOString();
    persistUpdateReport(stateDir, report);
  }
  return report;
}

export function selfUpdate(options: SelfUpdateOptions): SelfUpdateReport;
export function selfUpdate(options: SelfUpdateOptions, reconcileDaemon: ReconcileDaemon): Promise<SelfUpdateReport>;
export function selfUpdate(options: SelfUpdateOptions, reconcileDaemon?: ReconcileDaemon): SelfUpdateReport | Promise<SelfUpdateReport> {
  const checkedAt = new Date().toISOString();
  const acquired = tryAcquireOwnedLock(join(options.stateDir, 'auto-update.lock'), 'Auto-update');
  const { release } = acquired;
  if (!release) {
    const report: SelfUpdateReport = {
      status: acquired.error ? 'failed' : 'skipped_locked',
      sourceStatus: acquired.error ? 'corrupt_lock' : 'skipped_locked',
      lastCheckedAt: checkedAt,
      syncedSkills: [],
      syncedBinaries: [],
      restartedDaemon: false,
      ...(acquired.error ? { error: acquired.error } : {}),
    };
    return reconcileDaemon ? Promise.resolve(report) : report;
  }
  try {
    const { report, root, runtimeFingerprint, usableCheckout } = updateCheckout(options, checkedAt);
    persistUpdateReport(options.stateDir, report);
    if (reconcileDaemon && usableCheckout) {
      // Keep the update lock through the drain and report write so another updater cannot
      // claim the same restart or replace its report while the restart is still in flight.
      return reconcileUpdateReport(options.stateDir, report, () => reconcileDaemon(runtimeFingerprint, root))
        .finally(release);
    }
    release();
    return reconcileDaemon ? Promise.resolve(report) : report;
  } catch (error) {
    release();
    throw error;
  }
}

function updateCheckout(options: SelfUpdateOptions, checkedAt: string) {
  const previous = readAutoUpdateStatus(options.stateDir);
  let root = dealerRootFor(options.checkoutDir);
  let status = 'skipped';
  let sourceStatus = 'missing_checkout';
  let headSha: string | undefined;
  let error: string | undefined;
  let syncedSkills: string[] = [];
  let syncedBinaries: string[] = [];
  let stashedLocalChanges = false;
  let usableCheckout = false;
  let runtimeFingerprint: string | undefined;
  try {
    root = updateCheckoutFor(options);
    if (existsSync(join(root, '.git'))) {
      withUpdateSourceLocks(root, options, () => {
        try {
          const git = (args: string[]) => checkoutGit(root, args);
          const dirty = git(['status', '--porcelain']);
          const branch = git(['branch', '--show-current']);
          headSha = git(['rev-parse', 'HEAD']);
          const hasConflicts = (porcelain: string) => /^(UU|AA|DD|AU|UA|DU|UD) /m.test(porcelain) ||
            ['MERGE_HEAD', 'rebase-merge', 'rebase-apply'].some(name => existsSync(resolve(root, git(['rev-parse', '--git-path', name]))));
          if (hasConflicts(dirty)) {
            status = 'failed';
            sourceStatus = 'conflicted_checkout';
            error = 'Checkout has unresolved merge or rebase state';
          } else if (branch !== 'main') {
            sourceStatus = 'skipped_non_main_branch';
            usableCheckout = true;
          } else {
            const beforeSha = headSha;
            const stashRef = () => {
              try { return git(['rev-parse', '--verify', '--quiet', 'refs/stash']); }
              catch { return ''; }
            };
            let savedStash: string | undefined;
            const restore = () => {
              if (!savedStash || stashRef() !== savedStash) throw new Error('Temporary update stash is no longer the newest stash');
              git(['reset', '--hard', beforeSha]);
              git(['clean', '-fd']);
              git(['stash', 'pop', '--index']);
            };
            if (dirty) {
              const stashBefore = stashRef();
              git(['stash', 'push', '--include-untracked', '-m', 'dealer-self-update']);
              savedStash = stashRef();
              if (!savedStash || savedStash === stashBefore) throw new Error('Failed to preserve local changes in update stash');
              stashedLocalChanges = true;
            }
            try { git(['pull', '--ff-only']); }
            catch (pullError) {
              if (savedStash) restore();
              usableCheckout = !hasConflicts(git(['status', '--porcelain']));
              throw pullError;
            }
            const pulledSha = git(['rev-parse', 'HEAD']);
            let restored = true;
            if (savedStash) {
              try { git(['stash', 'pop', '--index']); }
              catch {
                restore();
                usableCheckout = true;
                restored = false;
                status = 'failed';
                sourceStatus = 'stash_conflict_rolled_back';
                headSha = beforeSha;
                error = 'Local changes conflicted with upstream update; rolled back cleanly to previous HEAD';
              }
            }
            if (restored) {
              usableCheckout = true;
              sourceStatus = pulledSha === beforeSha ? 'up_to_date' : 'updated';
              status = sourceStatus === 'updated' ? 'updated' : 'complete';
              headSha = pulledSha;
            }
          }
          if (usableCheckout) {
            syncedSkills = syncSkills(root, options);
            syncedBinaries = syncBinaries(root, options);
          }
        } finally {
          runtimeFingerprint = computeRuntimeFingerprint(options.stateDir, root);
        }
      });
    }
  } catch (caught) {
    status = 'failed';
    sourceStatus = 'error';
    error = String(caught);
  }
  runtimeFingerprint ??= computeRuntimeFingerprint(options.stateDir, root);
  const lastUpdatedAt = status === 'updated' ? new Date().toISOString() : previous?.lastUpdatedAt;
  const report: SelfUpdateReport = {
    status, sourceStatus, lastCheckedAt: checkedAt, runtimeFingerprint, syncedSkills, syncedBinaries,
    restartedDaemon: false,
    ...(lastUpdatedAt ? { lastUpdatedAt } : {}),
    ...(stashedLocalChanges ? { stashedLocalChanges: true } : {}),
    ...(headSha ? { headSha } : {}),
    ...(error ? { error } : {}),
  };
  return { report, root, runtimeFingerprint, usableCheckout };
}

const MIN_UPDATE_SPAWN_INTERVAL_MS = 30_000;

function lastCheckedAt(report?: SelfUpdateReport): number {
  return report?.lastCheckedAt ? Date.parse(report.lastCheckedAt) : 0;
}

function reserveBackgroundUpdate(stateDir: string): boolean {
  const acquired = tryAcquireOwnedLock(join(stateDir, 'auto-update.lock'), 'Auto-update');
  if (acquired.error) {
    throw new Error(acquired.error);
  }
  if (!acquired.release) {
    return false;
  }
  try {
    const triggerPath = join(stateDir, 'auto-update-spawn.json');
    const lastSpawn = existsSync(triggerPath) ? Number(readFileSync(triggerPath, 'utf8')) : 0;
    const last = Math.max(lastCheckedAt(readAutoUpdateStatus(stateDir)) || 0, lastSpawn || 0);
    const now = Date.now();
    if (last && now - last < MIN_UPDATE_SPAWN_INTERVAL_MS) {
      return false;
    }
    // Record the claim before spawning, since the child may not have written its report
    // by the time another command or the daemon's startup makes the same check.
    atomicJson(triggerPath, now);
    return true;
  } finally {
    acquired.release();
  }
}

export function maybeTriggerBackgroundAutoUpdate(options: SelfUpdateOptions): void {
  if (process.env.DEALER_DISABLE_AUTO_UPDATE === '1' || process.env.DEALER_AUTO_UPDATE === '0') {
    return;
  }
  if (process.env.NODE_TEST_CONTEXT && process.env.DEALER_AUTO_UPDATE !== '1') {
    return;
  }
  const state = readAutoUpdateStatus(options.stateDir);
  const last = lastCheckedAt(state);
  const now = Date.now();
  if (last && now - last < MIN_UPDATE_SPAWN_INTERVAL_MS) {
    return;
  }
  const currentFingerprint = computeRuntimeFingerprint(options.stateDir, updateCheckoutFor(options));
  const daemonOwner = existsSync(join(options.stateDir, 'control.sock')) ? liveDaemonOwner(options.stateDir) : undefined;
  const daemonRecord = daemonOwner ? readDaemonFingerprint(options.stateDir) : undefined;
  const daemonFingerprint = daemonOwner && daemonRecord?.pid === daemonOwner.pid && daemonRecord.instanceId === daemonOwner.instanceId
    ? daemonRecord.fingerprint
    : undefined;
  const localSyncNeeded = state?.runtimeFingerprint !== currentFingerprint ||
    (daemonFingerprint !== undefined && daemonFingerprint !== currentFingerprint);
  const cooldownMs = state?.sourceStatus === 'error' ? 15 * 60_000 : 6 * 3600_000;
  if (!localSyncNeeded && last && now - last < cooldownMs) {
    return;
  }
  if (!reserveBackgroundUpdate(options.stateDir)) {
    return;
  }
  const bin = fileURLToPath(new URL('../../bin/dealer.js', import.meta.url));
  const args = ['--no-warnings', '--experimental-strip-types', bin, 'self-update', '--quiet', '--json'];
  if (options.checkoutDir !== undefined) args.push('--checkout-dir', options.checkoutDir);
  if (options.homeDir !== undefined) args.push('--home-dir', options.homeDir);
  if (options.geminiDir !== undefined) args.push('--gemini-dir', options.geminiDir);
  const child = spawn(process.execPath, args, {
    detached: true, stdio: 'ignore', env: { ...process.env, DEALER_STATE_DIR: options.stateDir },
  });
  child.on('error', error => console.error(`Unable to start background self-update: ${error.message}`));
  child.unref();
}
