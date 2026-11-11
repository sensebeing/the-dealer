import { randomUUID } from 'node:crypto';
import { readFileSync, realpathSync } from 'node:fs';
import { hostname } from 'node:os';
import { resolve } from 'node:path';

/**
 * One process incarnation's identity. A daemon that is killed and restarted — even
 * inside the same container, where it receives the same PID again — is a new
 * incarnation, so its owner files can never be mistaken for the live one.
 */
const processInstanceId = randomUUID();
const processHost = hostname();

/** Directories whose owner file this process currently holds, keyed by real path. */
const heldDirectories = new Set<string>();

/** The record every crash-recoverable owner file carries; runtime and mailbox locks share it. */
export interface ProcessOwnerRecord {
  pid: number;
  instanceId: string;
  host: string;
}

export function processOwnerRecord(): ProcessOwnerRecord {
  return { pid: process.pid, instanceId: processInstanceId, host: processHost };
}

/** Mark a directory as guarded by this process; call right after claiming it. */
export function holdOwnedDirectory(dir: string): void {
  heldDirectories.add(canonicalDirectory(dir));
}

export function releaseOwnedDirectory(dir: string): void {
  heldDirectories.delete(canonicalDirectory(dir));
}

/** The owner recorded in a lock file, or undefined when it is missing or malformed. */
export function readProcessOwner(filePath: string): ProcessOwnerRecord | undefined {
  let record: Partial<Record<keyof ProcessOwnerRecord, unknown>>;
  try {
    record = JSON.parse(readFileSync(filePath, 'utf8')) as typeof record;
  } catch {
    return undefined;
  }
  const { pid, instanceId, host } = record;
  if (typeof pid !== 'number' || !Number.isSafeInteger(pid) || pid <= 0) {
    return undefined;
  }
  return {
    pid,
    instanceId: typeof instanceId === 'string' ? instanceId : '',
    host: typeof host === 'string' ? host : '',
  };
}

/**
 * True only when the owner is proven to belong to a process that no longer runs.
 * `ownedDirectory` is the directory this process would guard when the verdict is about
 * its own lock: a file naming this very process is live while that directory is held.
 */
export function isDeadProcessOwner(owner: ProcessOwnerRecord, ownedDirectory: string): boolean {
  if (owner.pid === process.pid) {
    if (owner.instanceId === processInstanceId) {
      // This process wrote the file, so only its own current claim decides liveness.
      return !heldDirectories.has(canonicalDirectory(ownedDirectory));
    }
    // The same PID from another incarnation. Only a file written inside this same
    // container proves a predecessor wrote it; another container sharing the volume
    // can hold the same PID number and still be live, so it keeps its file.
    return owner.host === processHost;
  }
  try {
    process.kill(owner.pid, 0);
    return false;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === 'ESRCH';
  }
}

function canonicalDirectory(dir: string): string {
  try {
    return realpathSync(dir);
  } catch {
    return resolve(dir);
  }
}
