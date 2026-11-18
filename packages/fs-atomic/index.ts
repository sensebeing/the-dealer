import { randomUUID } from 'node:crypto';
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
  rmdirSync,
  writeFileSync,
  writeSync,
} from 'node:fs';
import { dirname, join } from 'node:path';
import {
  holdOwnedDirectory,
  isDeadProcessOwner,
  processOwnerRecord,
  readProcessOwner,
  releaseOwnedDirectory,
} from '../process-lock/index.ts';

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

/** Stage the owner record before publishing the directory. */
function stageOwnedDirectory(lockDir: string, ownerFile: string, owner: ReturnType<typeof processOwnerRecord>): string | undefined {
  if (existsSync(lockDir)) {
    return undefined;
  }
  const stage = mkdtempSync(`${lockDir}.stage.`);
  try {
    writeFileAtomically(join(stage, ownerFile), JSON.stringify(owner));
    if (existsSync(lockDir)) {
      return undefined;
    }
    renameSync(stage, lockDir);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === 'EEXIST' || code === 'ENOTEMPTY') {
      return undefined;
    }
    throw error;
  } finally {
    rmSync(stage, { recursive: true, force: true });
  }
  holdOwnedDirectory(lockDir);
  return stage;
}

function claimOwnedDirectory(lockDir: string): (() => void) | undefined {
  const owner = processOwnerRecord();
  const stage = stageOwnedDirectory(lockDir, 'owner.json', owner);
  if (!stage) {
    return undefined;
  }
  let released = false;
  return () => {
    if (released) return;
    released = true;
    const current = readProcessOwner(join(lockDir, 'owner.json'));
    releaseOwnedDirectory(lockDir);
    if (current?.pid === owner.pid && current.instanceId === owner.instanceId && current.host === owner.host) {
      // Remove the whole directory from the claim path before cleaning up its owner file.
      renameSync(lockDir, stage);
      rmSync(stage, { recursive: true, force: true });
    }
  };
}

function claimReclaimGate(gateDir: string): (() => void) | undefined {
  const owner = processOwnerRecord();
  const ownerFile = `owner.${owner.pid}.${owner.instanceId}.json`;
  if (!stageOwnedDirectory(gateDir, ownerFile, owner)) {
    return undefined;
  }
  return () => {
    releaseOwnedDirectory(gateDir);
    rmSync(join(gateDir, ownerFile), { force: true });
    try {
      rmdirSync(gateDir);
    } catch {
      // A concurrent reclaimer's owner file may now keep the directory in place.
    }
  };
}

function tryAcquireReclaimGate(gateDir: string, label: string): NonBlockingLockResult {
  const release = claimReclaimGate(gateDir);
  if (release) {
    return { release };
  }
  let entries: string[];
  try {
    entries = readdirSync(gateDir);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return { locked: true };
    }
    throw error;
  }
  const invalid = { error: `${label} reclaim gate ${gateDir} has no readable owner record` };
  if (entries.length === 0) {
    return invalid;
  }
  for (const entry of entries) {
    const path = join(gateDir, entry);
    const owner = readProcessOwner(path);
    if (!owner) {
      return existsSync(path) ? invalid : { locked: true };
    }
    if (!isDeadProcessOwner(owner, gateDir)) {
      return { locked: true };
    }
  }
  // Delete only the filenames inspected above. A replacement gate has its own filename.
  for (const entry of entries) {
    rmSync(join(gateDir, entry), { force: true });
  }
  try {
    rmdirSync(gateDir);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === 'ENOENT' || code === 'ENOTEMPTY' || code === 'EEXIST') {
      return { locked: true };
    }
    throw error;
  }
  const reclaimed = claimReclaimGate(gateDir);
  return reclaimed ? { release: reclaimed } : { locked: true };
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
        const stale = `${lockDir}.stale.${randomUUID()}`;
        renameSync(lockDir, stale);
        rmSync(stale, { recursive: true, force: true });
      } else if (existsSync(lockDir)) {
        return readProcessOwner(ownerPath) ? { locked: true } : invalid;
      }
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
