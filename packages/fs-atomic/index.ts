import { randomUUID } from 'node:crypto';
import {
  chmodSync,
  closeSync,
  copyFileSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
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
const STALE_LOCK_MS = 30_000;

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
 * Sole crash-recoverable directory-lock owner shared by every durable store. It claims the
 * directory, records the process incarnation and held-directory truth from process-lock,
 * reclaims only provably dead owners, and always releases the claim on unwind.
 */
export function withOwnedLock<T>(
  lockDir: string,
  label: string,
  operation: () => T,
): T {
  mkdirSync(dirname(lockDir), { recursive: true, mode: 0o700 });
  const ownerPath = join(lockDir, 'owner.json');
  const deadline = Date.now() + LOCK_TIMEOUT_MS;
  const sleeper = new Int32Array(new SharedArrayBuffer(4));
  let acquired = false;
  while (!acquired) {
    try {
      mkdirSync(lockDir);
      acquired = true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') {
        throw error;
      }
      clearStaleOwnedLock(lockDir, ownerPath);
      if (Date.now() >= deadline) {
        throw new Error(`${label} is locked: ${lockDir}`);
      }
      Atomics.wait(sleeper, 0, 0, LOCK_RETRY_MS);
    }
  }
  try {
    holdOwnedDirectory(lockDir);
    writeFileSync(ownerPath, JSON.stringify(processOwnerRecord()), { mode: 0o600 });
    return operation();
  } finally {
    rmSync(lockDir, { recursive: true, force: true });
    releaseOwnedDirectory(lockDir);
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

function clearStaleOwnedLock(lockDir: string, ownerPath: string): void {
  const owner = readProcessOwner(ownerPath);
  if (owner !== undefined) {
    if (isDeadProcessOwner(owner, lockDir)) {
      rmSync(lockDir, { recursive: true, force: true });
    }
    return;
  }
  try {
    if (Date.now() - statSync(lockDir).mtimeMs > STALE_LOCK_MS) {
      rmSync(lockDir, { recursive: true, force: true });
    }
  } catch {
    // The lock disappeared while it was inspected.
  }
}
