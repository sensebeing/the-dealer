import { createHash, randomUUID } from 'node:crypto';
import {
  chmodSync,
  closeSync,
  copyFileSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
  writeSync,
} from 'node:fs';
import type { BigIntStats } from 'node:fs';
import { basename, dirname, join } from 'node:path';
import {
  holdOwnedDirectory,
  isDeadProcessOwner,
  processOwnerRecord,
  readProcessOwner,
  releaseOwnedDirectory,
} from '../process-lock/index.ts';

import type { ProcessOwnerRecord } from '../process-lock/index.ts';

const LOCK_RETRY_MS = 20;
const LOCK_TIMEOUT_MS = 2_000;

/**
 * Persist the rename itself. Some platforms/filesystems reject fsync on a read-only
 * directory descriptor; those are benign, while any other I/O failure must still surface.
 */
export function syncDirectory(directory: string): void {
  let fd: number | undefined;
  try {
    fd = openSync(directory, 'r');
    fsyncSync(fd);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code !== 'EINVAL' && code !== 'EBADF' && code !== 'EPERM' && code !== 'ENOTSUP') {
      throw error;
    }
  } finally {
    if (fd !== undefined) {
      closeSync(fd);
    }
  }
}

/** Sole atomic JSON/data write owner: temp file, file fsync, rename, parent fsync. */
export function writeFileAtomically(
  filePath: string,
  data: string | Uint8Array,
  mode = 0o600,
): void {
  mkdirSync(dirname(filePath), { recursive: true, mode: 0o700 });
  const temporary = `${filePath}.${randomUUID()}.tmp`;
  try {
    const fd = openSync(temporary, 'wx', mode);
    try {
      writeFileSync(fd, data);
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    renameSync(temporary, filePath);
    syncDirectory(dirname(filePath));
  } finally {
    rmSync(temporary, { force: true });
  }
}

/** Copy a file into a durable destination with the same fsync/rename contract. */
export function copyFileAtomically(sourcePath: string, destPath: string, mode = 0o600): void {
  mkdirSync(dirname(destPath), { recursive: true, mode: 0o700 });
  const temporary = `${destPath}.${randomUUID()}.tmp`;
  try {
    copyFileSync(sourcePath, temporary);
    chmodSync(temporary, mode);
    const fd = openSync(temporary, 'r');
    try {
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    renameSync(temporary, destPath);
    syncDirectory(dirname(destPath));
  } finally {
    rmSync(temporary, { force: true });
  }
}

/**
 * Wait briefly for a store's directory lock and release it when the operation unwinds.
 */
export function withOwnedLock<T>(
  lockDir: string,
  label: string,
  operation: () => T,
  timeoutMs = LOCK_TIMEOUT_MS,
): T {
  const deadline = Date.now() + timeoutMs;
  const sleeper = new Int32Array(new SharedArrayBuffer(4));
  for (;;) {
    const acquired = tryAcquireOwnedLock(lockDir, label);
    if (acquired.release) {
      try {
        return operation();
      } finally {
        acquired.release();
      }
    }
    if (!acquired.locked) {
      throw new Error(acquired.error);
    }
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      throw new Error(`${label} is locked: ${lockDir}`);
    }
    Atomics.wait(sleeper, 0, 0, Math.min(LOCK_RETRY_MS, remaining));
  }
}

export type NonBlockingLockResult =
  | { release: () => void; locked?: never; error?: never }
  | { release?: never; locked: true; error?: never }
  | { release?: never; locked?: never; error: string };

function lockHostKey(host: string): string {
  return createHash('sha256').update(host).digest('base64url').slice(0, 16);
}

function makeLockArtifact(dir: string, kind: 'stage' | 'release' | 'stale', owner: ProcessOwnerRecord): string {
  // Include provenance even while the temp directory is empty, before its owner file exists.
  return mkdtempSync(`${dir}.${kind}.${owner.pid}.${owner.instanceId}.${lockHostKey(owner.host)}.`);
}

function moveOwnedDirectory(dir: string, kind: 'release' | 'stale'): string {
  const artifact = makeLockArtifact(dir, kind, processOwnerRecord());
  try {
    renameSync(dir, artifact);
  } catch (error) {
    rmSync(artifact, { recursive: true, force: true });
    throw error;
  }
  return artifact;
}

function sameProcessOwner(current: ProcessOwnerRecord | undefined, owner: ProcessOwnerRecord): boolean {
  return current?.pid === owner.pid && current.instanceId === owner.instanceId && current.host === owner.host;
}

/** Stage the owner before publishing the directory. Release retires the entire directory. */
function claimOwnedDirectory(dir: string): (() => void) | undefined {
  if (existsSync(dir)) {
    return undefined;
  }
  const owner = processOwnerRecord();
  const stage = makeLockArtifact(dir, 'stage', owner);
  try {
    writeFileAtomically(join(stage, 'owner.json'), JSON.stringify(owner));
    if (existsSync(dir)) {
      return undefined;
    }
    renameSync(stage, dir);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === 'EEXIST' || code === 'ENOTEMPTY') {
      return undefined;
    }
    throw error;
  } finally {
    rmSync(stage, { recursive: true, force: true });
  }
  holdOwnedDirectory(dir);
  let released = false;
  return () => {
    if (released) return;
    released = true;
    const current = readProcessOwner(join(dir, 'owner.json'));
    releaseOwnedDirectory(dir);
    if (sameProcessOwner(current, owner)) {
      rmSync(moveOwnedDirectory(dir, 'release'), { recursive: true, force: true });
    }
  };
}

function ownedDirectoryStat(dir: string): BigIntStats | undefined {
  try {
    return statSync(dir, { bigint: true });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return undefined;
    }
    throw error;
  }
}

function readOwnedDirectoryOwner(dir: string, gateDir: string): ProcessOwnerRecord | undefined {
  const owner = readProcessOwner(join(dir, 'owner.json'));
  if (owner || dir !== gateDir) {
    return owner;
  }
  // Earlier releases used a single per-incarnation filename for reclaim gates.
  let entries: string[];
  try {
    entries = readdirSync(dir);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return undefined;
    }
    throw error;
  }
  const legacy = entries.length === 1 ? /^owner\.(\d+)\.([a-zA-Z0-9-]+)\.json$/.exec(entries[0]) : null;
  if (legacy) {
    const recorded = readProcessOwner(join(dir, entries[0]));
    if (recorded?.pid === Number(legacy[1]) && recorded.instanceId === legacy[2]) {
      return recorded;
    }
  }
  return undefined;
}

/** Use an inode-scoped ticket so a stale observation cannot retire a replacement owner. */
function replaceDeadOwnedDirectory(
  dir: string,
  stat: BigIntStats,
  owner: ProcessOwnerRecord,
  gateDir: string,
  level = 0,
): (() => void) | undefined {
  const ticket = `${gateDir}.evict.${stat.ino}${level ? `.${level}` : ''}`;
  let releaseTicket = claimOwnedDirectory(ticket);
  if (!releaseTicket) {
    const ticketStat = ownedDirectoryStat(ticket);
    const ticketOwner = readOwnedDirectoryOwner(ticket, gateDir);
    if (!ticketStat || !ticketOwner || !isDeadProcessOwner(ticketOwner, ticket)) {
      return undefined;
    }
    // Recover a dead ticket under the same protocol. The level keeps ticket paths
    // distinct even when the filesystem reuses an inode after an earlier eviction.
    releaseTicket = replaceDeadOwnedDirectory(ticket, ticketStat, ticketOwner, gateDir, level + 1);
    if (!releaseTicket) {
      return undefined;
    }
  }
  let reclaimed: (() => void) | undefined;
  try {
    try {
      const current = readOwnedDirectoryOwner(dir, gateDir);
      const currentStat = ownedDirectoryStat(dir);
      if (currentStat?.ino !== stat.ino || currentStat.dev !== stat.dev ||
          currentStat.birthtimeNs !== stat.birthtimeNs ||
          !current || !sameProcessOwner(current, owner) || !isDeadProcessOwner(current, dir)) {
        return undefined;
      }
      const stale = moveOwnedDirectory(dir, 'stale');
      try {
        reclaimed = claimOwnedDirectory(dir);
      } finally {
        rmSync(stale, { recursive: true, force: true });
      }
      return reclaimed;
    } finally {
      releaseTicket();
    }
  } catch (error) {
    reclaimed?.();
    throw error;
  }
}

function tryAcquireReclaimGate(gateDir: string, label: string): NonBlockingLockResult {
  const release = claimOwnedDirectory(gateDir);
  if (release) {
    return { release };
  }
  const stat = ownedDirectoryStat(gateDir);
  if (!stat) {
    return { locked: true };
  }
  const owner = readOwnedDirectoryOwner(gateDir, gateDir);
  if (!owner) {
    return !existsSync(gateDir) || readOwnedDirectoryOwner(gateDir, gateDir)
      ? { locked: true }
      : { error: `${label} reclaim gate ${gateDir} has no readable owner record` };
  }
  if (!isDeadProcessOwner(owner, gateDir)) {
    return { locked: true };
  }
  const reclaimed = replaceDeadOwnedDirectory(gateDir, stat, owner, gateDir);
  return reclaimed ? { release: reclaimed } : { locked: true };
}

function sweepLockArtifacts(lockDir: string, gateDir: string): void {
  const parent = dirname(lockDir);
  const prefix = basename(lockDir);
  const { host } = processOwnerRecord();
  const hostKey = lockHostKey(host);
  for (const entry of readdirSync(parent, { withFileTypes: true })) {
    if (!entry.isDirectory() || !entry.name.startsWith(prefix)) {
      continue;
    }
    const suffix = entry.name.slice(prefix.length);
    const artifact = /^(?:\.reclaim(?:\.evict\.\d+(?:\.\d+)?)?)?\.(?:stage|release|stale)\.(.*)$/.exec(suffix);
    const path = join(parent, entry.name);
    if (artifact) {
      const provenance = /^([1-9]\d*)\.([a-zA-Z0-9-]+)\.([a-zA-Z0-9_-]{16})\./.exec(artifact[1]);
      const owner = readProcessOwner(join(path, 'owner.json')) ?? (provenance ? {
        pid: Number(provenance[1]),
        instanceId: provenance[2],
        host: provenance[3] === hostKey ? host : '',
      } : undefined);
      if (owner && isDeadProcessOwner(owner, path)) {
        rmSync(path, { recursive: true, force: true });
      }
      continue;
    }
    const ticket = /^\.reclaim\.evict\.\d+(?:\.(\d+))?$/.exec(suffix);
    if (ticket) {
      const stat = ownedDirectoryStat(path);
      const owner = readOwnedDirectoryOwner(path, gateDir);
      if (stat && owner && isDeadProcessOwner(owner, path)) {
        replaceDeadOwnedDirectory(path, stat, owner, gateDir, Number(ticket[1] ?? 0) + 1)?.();
      }
    }
  }
}

/** Claim a directory without waiting, or recover it when its recorded owner is dead. */
export function tryAcquireOwnedLock(lockDir: string, label = 'Directory'): NonBlockingLockResult {
  const gateDir = `${lockDir}.reclaim`;
  try {
    mkdirSync(dirname(lockDir), { recursive: true, mode: 0o700 });
    if (!existsSync(lockDir) && !existsSync(gateDir)) {
      const release = claimOwnedDirectory(lockDir);
      if (release) {
        return { release };
      }
    }
    const ownerPath = join(lockDir, 'owner.json');
    const owner = readProcessOwner(ownerPath);
    if (owner && !isDeadProcessOwner(owner, lockDir)) {
      return { locked: true };
    }
    const invalid = { error: `${label} lock ${lockDir} has no readable owner record` };
    if (!owner && existsSync(lockDir)) {
      // A successful claim may have appeared between reading the file and checking
      // the directory. Its owner file was staged before publication.
      return readProcessOwner(ownerPath) ? { locked: true } : invalid;
    }
    const gate = tryAcquireReclaimGate(gateDir, label);
    if (!gate.release) {
      return gate;
    }
    let reclaimed: (() => void) | undefined;
    try {
      // Another process may have recovered the lock before we claimed the gate.
      const current = readProcessOwner(ownerPath);
      if (current) {
        if (!isDeadProcessOwner(current, lockDir)) {
          return { locked: true };
        }
        rmSync(moveOwnedDirectory(lockDir, 'stale'), { recursive: true, force: true });
      } else if (existsSync(lockDir)) {
        return readProcessOwner(ownerPath) ? { locked: true } : invalid;
      }
      sweepLockArtifacts(lockDir, gateDir);
      reclaimed = claimOwnedDirectory(lockDir);
      return reclaimed ? { release: reclaimed } : { locked: true };
    } finally {
      try {
        gate.release();
      } catch (error) {
        reclaimed?.();
        throw error;
      }
    }
  } catch (error) {
    return { error: `Unable to acquire ${label} lock: ${String(error)}` };
  }
}

/**
 * Append one durable record and fsync it. Callers serialize their own writes; this helper
 * only owns the append/fsync contract shared by append-only journals.
 */
export function appendFileDurably(filePath: string, line: string): void {
  mkdirSync(dirname(filePath), { recursive: true, mode: 0o700 });
  const existed = existsSync(filePath);
  const fd = openSync(filePath, 'a', 0o600);
  try {
    writeSync(fd, line);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  if (!existed) {
    // Persist the journal's directory entry on first creation; later appends rely on it.
    syncDirectory(dirname(filePath));
  }
}

/** Read a journal, ignoring only a torn final line left by a crash mid-append. */
export function readDurableJournal(filePath: string): unknown[] {
  let raw: string;
  try {
    raw = readFileSync(filePath, 'utf8');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return [];
    }
    throw error;
  }
  const lines = raw.split('\n');
  const records: unknown[] = [];
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (line === '') {
      continue;
    }
    try {
      records.push(JSON.parse(line));
    } catch {
      // Only the tail can be torn: each earlier append was fsynced as a complete line.
      if (lines.slice(index + 1).every(rest => rest === '')) {
        break;
      }
      throw new Error('Invalid durable journal');
    }
  }
  return records;
}
