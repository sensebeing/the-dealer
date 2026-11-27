import { spawn } from 'node:child_process';
import { accessSync, constants, statSync } from 'node:fs';
import { homedir } from 'node:os';
import { delimiter, join } from 'node:path';
import type { Readable, Writable } from 'node:stream';

/** A single external command invocation; always executed with shell: false. */
export interface CommandRequest {
  command: string;
  args: readonly string[];
  cwd?: string;
  input?: string;
  signal?: AbortSignal;
  env?: NodeJS.ProcessEnv;
}

export type CommandRunner = (
  request: CommandRequest,
) => Promise<{ stdout: string; stderr?: string; exitCode?: number }>;

/** Minimal host-neutral streaming stdio child contract for interactive line/RPC protocols. */
export interface StdioChildProcess {
  readonly pid?: number;
  readonly stdin: Writable;
  readonly stdout: Readable;
  readonly stderr: Readable;
  kill(signal?: NodeJS.Signals | number): boolean;
  on(event: string | symbol, listener: (...args: any[]) => void): this;
  once(event: string | symbol, listener: (...args: any[]) => void): this;
  off(event: string | symbol, listener: (...args: any[]) => void): this;
}

export interface StdioProcessRequest {
  command: string;
  args: readonly string[];
  cwd?: string;
  env?: NodeJS.ProcessEnv;
}

export type StdioProcessSpawner = (request: StdioProcessRequest) => StdioChildProcess;

export const spawnStdioProcess: StdioProcessSpawner = request =>
  spawn(request.command, [...request.args], {
    cwd: request.cwd,
    ...(request.env ? { env: request.env } : {}),
    stdio: ['pipe', 'pipe', 'pipe'],
    shell: false,
  });

/** Only the process boundary may prove that no external process was started. */
export class CommandNotStartedError extends Error {
  constructor(cause: unknown) {
    super(`Command did not start: ${String(cause)}`, { cause });
    this.name = 'CommandNotStartedError';
  }
}

/** True only when the subprocess boundary positively proved the executable was not found (ENOENT). */
export function isCommandNotFoundError(error: unknown): boolean {
  if (!(error instanceof CommandNotStartedError)) return false;
  const cause = error.cause;
  if (typeof cause !== 'object' || cause === null) return false;
  const { code, syscall } = cause as { code?: unknown; syscall?: unknown };
  return code === 'ENOENT' && (syscall === undefined || (typeof syscall === 'string' && syscall.startsWith('spawn')));
}

export const runCommand: CommandRunner = request =>
  new Promise((resolve, reject) => {
    if (request.signal?.aborted) {
      reject(new CommandNotStartedError(request.signal.reason));
      return;
    }
    let child;
    try {
      child = spawn(request.command, [...request.args], {
        cwd: request.cwd,
        signal: request.signal,
        ...(request.env ? { env: request.env } : {}),
        stdio: ['pipe', 'pipe', 'pipe'],
        shell: false,
      });
    } catch (error) {
      reject(new CommandNotStartedError(error));
      return;
    }
    let started = false;
    child.once('spawn', () => { started = true; });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', chunk => {
      stdout += chunk;
    });
    child.stderr.on('data', chunk => {
      stderr += chunk;
    });
    child.on('error', error => reject(
      started || child.pid !== undefined ? error : new CommandNotStartedError(error),
    ));
    child.on('close', code =>
      code === 0
        ? resolve({ stdout, stderr, exitCode: code })
        : reject(Object.assign(new Error(`${request.command} exited ${code}: ${(stderr || stdout).trim()}`), { stdout, stderr, exitCode: code })),
    );
    child.stdin.on('error', reject);
    child.stdin.end(request.input);
  });

export async function checkedCommand(runner: CommandRunner, request: CommandRequest) {
  const result = await runner(request);
  if (result.exitCode !== undefined && result.exitCode !== 0) {
    throw new Error(
      `${request.command} exited ${result.exitCode}: ${(result.stderr || result.stdout).trim()}`,
    );
  }
  return result;
}

/** Collapse arbitrary text into exactly one line; wakeup pointers must never span lines. */
export function singleLine(value: string): string {
  return value.replace(/\s+/g, ' ').trim();
}

/** Resolve executable PATH entries before the optional per-user installation directory. */
export function findExecutable(command: string, env: NodeJS.ProcessEnv = process.env, localBin = true): string | undefined {
  const directories = (env.PATH ?? '').split(delimiter).filter(Boolean);
  const fallback = join(homedir(), '.local/bin');
  if (localBin && !directories.includes(fallback)) directories.push(fallback);
  for (const directory of directories) {
    const path = join(directory, command);
    try {
      accessSync(path, constants.X_OK);
      if (statSync(path).isFile()) return path;
    } catch (error) {
      if (!['ENOENT', 'EACCES', 'ENOTDIR'].includes((error as NodeJS.ErrnoException).code ?? '')) throw error;
    }
  }
  return undefined;
}
