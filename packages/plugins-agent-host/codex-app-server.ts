import { createInterface } from 'node:readline';
import { CommandNotStartedError, spawnStdioProcess } from '../subprocess/index.ts';
import type { StdioChildProcess, StdioProcessSpawner } from '../subprocess/index.ts';
import { assertValidCodexOptions } from './hosts.ts';
import type { ActiveConversationMode } from './hosts.ts';

const DEFAULT_RPC_TIMEOUT_MS = 30_000;
const DEFAULT_CLOSE_GRACE_MS = 3_000;
const DEFAULT_KILL_GRACE_MS = 2_000;
const MAX_STDERR_BYTES = 16 * 1024;
const MAX_EARLY_COMPLETIONS = 32;

export interface CodexAppServerTurnOptions {
  command?: string;
  cwd?: string;
  mode: ActiveConversationMode;
  sessionId?: string;
  promptText: string;
  agentProfile?: string;
  signal?: AbortSignal;
  stdioSpawner?: StdioProcessSpawner;
  /** Bounded timeout per JSON-RPC handshake/request step in milliseconds. */
  rpcTimeoutMs?: number;
  /** Grace window after closing stdin before sending SIGTERM. */
  closeGraceMs?: number;
  /** Delay between SIGTERM and SIGKILL escalation. */
  killGraceMs?: number;
}

export interface CodexAppServerTurnResult {
  threadId: string;
  turnId?: string;
  status: 'completed';
  stdout: string;
  stderr: string;
}

type JsonRpcId = number | string;

interface JsonRpcErrorObject {
  code?: unknown;
  message?: unknown;
  data?: unknown;
}

interface JsonRpcMessage {
  jsonrpc?: unknown;
  id?: JsonRpcId | null;
  method?: unknown;
  params?: unknown;
  result?: unknown;
  error?: JsonRpcErrorObject;
}

interface CompletedTurnEvent {
  threadId?: string;
  turnId?: string;
  status?: unknown;
  error?: unknown;
}

type TurnPhase = 'pre-turn-send' | 'turn-sent' | 'turn-running' | 'completed';

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function formatRpcError(error: unknown): string {
  if (isRecord(error)) {
    const code = error.code !== undefined ? `code ${String(error.code)}` : '';
    const message = typeof error.message === 'string' && error.message.trim() ? error.message.trim() : '';
    if (code && message) return `${code}: ${message}`;
    if (message) return message;
    if (code) return code;
  }
  return typeof error === 'string' ? error : JSON.stringify(error);
}

function formatTurnError(error: unknown): string {
  if (error === undefined || error === null) return '';
  if (typeof error === 'string') return error.trim();
  if (isRecord(error)) {
    const message = typeof error.message === 'string' && error.message.trim() ? error.message.trim() : '';
    if (message) return message;
  }
  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
}

class BoundedStderrTail {
  private readonly chunks: Buffer[] = [];
  private totalBytes = 0;
  private readonly maxBytes: number;

  constructor(maxBytes = MAX_STDERR_BYTES) {
    this.maxBytes = maxBytes;
  }

  append(chunk: Buffer | string): void {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, 'utf8');
    if (buf.length === 0) return;
    if (buf.length >= this.maxBytes) {
      this.chunks.length = 0;
      this.chunks.push(buf.subarray(buf.length - this.maxBytes));
      this.totalBytes = this.maxBytes;
      return;
    }
    this.chunks.push(buf);
    this.totalBytes += buf.length;
    while (this.totalBytes > this.maxBytes && this.chunks.length > 0) {
      const overflow = this.totalBytes - this.maxBytes;
      if (this.chunks[0].length <= overflow) {
        this.totalBytes -= this.chunks[0].length;
        this.chunks.shift();
      } else {
        this.chunks[0] = this.chunks[0].subarray(overflow);
        this.totalBytes -= overflow;
      }
    }
  }

  toString(): string {
    return Buffer.concat(this.chunks, this.totalBytes).toString('utf8');
  }
}

/**
 * Sole owner of Codex `app-server --stdio` JSON-RPC 2.0 execution.
 * Any failure before `turn/start` is accepted (including an explicit JSON-RPC rejection of
 * `turn/start`) throws `CommandNotStartedError` so wakeup callers defer retryably instead of
 * fencing `deliveryInDoubt`; post-`turn/start` ambiguity or non-completed status throws a regular
 * `Error` so `deliveryInDoubt` protects against duplicate side effects.
 */
export async function runCodexAppServerTurn(
  options: CodexAppServerTurnOptions,
): Promise<CodexAppServerTurnResult> {
  const sessionId = options.sessionId?.trim() || undefined;
  assertValidCodexOptions({
    conversationMode: options.mode,
    sessionId,
    agentProfile: options.agentProfile,
  });

  if (options.signal?.aborted) {
    throw new CommandNotStartedError(options.signal.reason ?? 'Aborted before spawn');
  }

  const command = options.command?.trim() || 'codex';
  const spawner = options.stdioSpawner ?? spawnStdioProcess;
  const rpcTimeoutMs = options.rpcTimeoutMs ?? DEFAULT_RPC_TIMEOUT_MS;
  const closeGraceMs = options.closeGraceMs ?? DEFAULT_CLOSE_GRACE_MS;
  const killGraceMs = options.killGraceMs ?? DEFAULT_KILL_GRACE_MS;

  let child: StdioChildProcess;
  try {
    child = spawner({
      command,
      args: ['app-server', '--stdio'],
      cwd: options.cwd,
    });
  } catch (error) {
    throw new CommandNotStartedError(error);
  }

  const stderrTail = new BoundedStderrTail(MAX_STDERR_BYTES);
  let phase: TurnPhase = 'pre-turn-send';
  let childExited = false;
  let shuttingDown = false;
  let activeThreadId: string | undefined;
  let expectedTurnId: string | undefined;
  const earlyTurnCompletions: CompletedTurnEvent[] = [];
  let completedTurn: CompletedTurnEvent | undefined;
  let terminationTimer: ReturnType<typeof setTimeout> | undefined;
  let activeRpc:
    | {
        id: number;
        method: string;
        resolve: (result: unknown) => void;
        reject: (error: Error) => void;
        timer: ReturnType<typeof setTimeout>;
      }
    | undefined;
  let turnWaiter:
    | {
        resolve: (event: CompletedTurnEvent) => void;
        reject: (error: Error) => void;
      }
    | undefined;
  let exitWaiter: (() => void) | undefined;
  let fatalError: Error | undefined;

  const formatStderrSuffix = (): string => {
    const text = stderrTail.toString().trim();
    return text ? ` (${text})` : '';
  };

  const makeBoundaryError = (message: string, forceNotStarted = false): Error => {
    const full = `${message}${formatStderrSuffix()}`;
    return forceNotStarted || phase === 'pre-turn-send'
      ? new CommandNotStartedError(full)
      : new Error(full);
  };

  const terminateChild = (): void => {
    if (childExited) return;
    try {
      child.kill('SIGTERM');
    } catch {
      // Child may already be gone.
    }
    if (!terminationTimer && !childExited) {
      terminationTimer = setTimeout(() => {
        terminationTimer = undefined;
        if (childExited) return;
        try {
          child.kill('SIGKILL');
        } catch {
          // Ignore escalation errors on already-exited processes.
        }
      }, killGraceMs);
      terminationTimer.unref();
    }
  };

  const failActive = (error: Error): void => {
    if (!fatalError) fatalError = error;
    if (activeRpc) {
      const pending = activeRpc;
      activeRpc = undefined;
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    if (turnWaiter) {
      const waiter = turnWaiter;
      turnWaiter = undefined;
      waiter.reject(error);
    }
  };

  const writeJsonLine = (
    payload: Record<string, unknown>,
    strict = true,
    onBeforeWrite?: () => void,
  ): void => {
    if (child.stdin.destroyed || child.stdin.writableEnded) {
      if (strict) throw makeBoundaryError('codex app-server stdin is closed');
      return;
    }
    onBeforeWrite?.();
    try {
      child.stdin.write(`${JSON.stringify(payload)}\n`);
    } catch (error) {
      if (strict) {
        throw makeBoundaryError(`Failed to write to codex app-server stdin: ${String(error)}`);
      }
    }
  };

  const onStderrData = (chunk: Buffer | string): void => {
    stderrTail.append(chunk);
  };

  const onStdinError = (error: unknown): void => {
    if (shuttingDown || phase === 'completed') return;
    failActive(makeBoundaryError(`codex app-server stdin error: ${String(error)}`));
  };

  const onChildError = (error: unknown): void => {
    childExited = true;
    if (terminationTimer) {
      clearTimeout(terminationTimer);
      terminationTimer = undefined;
    }
    exitWaiter?.();
    if (shuttingDown || phase === 'completed') return;
    failActive(makeBoundaryError(`codex app-server process error: ${String(error)}`));
  };

  // In Node.js, 'exit' can fire before stdout finishes draining buffered lines; never fail active
  // RPC/turn waiters here — wait for 'close' / rl 'close' after stdio streams finish draining.
  const onChildExit = (): void => {
    childExited = true;
    if (terminationTimer) {
      clearTimeout(terminationTimer);
      terminationTimer = undefined;
    }
    exitWaiter?.();
  };

  const onChildClose = (code: number | null, signal: NodeJS.Signals | null): void => {
    childExited = true;
    if (terminationTimer) {
      clearTimeout(terminationTimer);
      terminationTimer = undefined;
    }
    exitWaiter?.();
    if (shuttingDown || phase === 'completed' || completedTurn) return;
    const detail = signal ? `signal ${signal}` : `code ${String(code ?? 'unknown')}`;
    failActive(makeBoundaryError(`codex app-server exited unexpectedly with ${detail}`));
  };

  const onStdoutClose = (): void => {
    if (shuttingDown || phase === 'completed' || completedTurn) return;
    failActive(makeBoundaryError('codex app-server stdout closed before turn completion'));
  };

  const onAbort = (): void => {
    const reason = options.signal?.reason;
    const error = makeBoundaryError(`codex app-server aborted: ${String(reason ?? 'aborted')}`);
    terminateChild();
    failActive(error);
  };

  const rl = createInterface({ input: child.stdout, crlfDelay: Infinity });

  const onLine = (line: string): void => {
    const trimmed = line.trim();
    if (!trimmed || trimmed[0] !== '{') return;
    let message: JsonRpcMessage;
    try {
      const parsed: unknown = JSON.parse(trimmed);
      if (!isRecord(parsed)) return;
      message = parsed as JsonRpcMessage;
    } catch {
      return;
    }

    // Server-initiated request: immediately reject so non-interactive turns never deadlock.
    if (message.id !== undefined && typeof message.method === 'string') {
      writeJsonLine(
        {
          jsonrpc: '2.0',
          id: message.id,
          error: {
            code: -32601,
            message: 'Non-interactive Dealer host does not accept server requests',
          },
        },
        false,
      );
      return;
    }

    // JSON-RPC response matching our in-flight request.
    if (message.id !== undefined && activeRpc && message.id === activeRpc.id) {
      const pending = activeRpc;
      activeRpc = undefined;
      clearTimeout(pending.timer);
      if (message.error !== undefined) {
        // Explicit RPC rejection of initialize, thread/start, thread/resume, OR turn/start
        // proves the turn never started and must stay retryable via CommandNotStartedError.
        pending.reject(
          makeBoundaryError(
            `codex app-server ${pending.method} failed: ${formatRpcError(message.error)}`,
            true,
          ),
        );
        return;
      }
      if (pending.method === 'turn/start') {
        const resultTurn = isRecord(message.result) && isRecord(message.result.turn) ? message.result.turn : undefined;
        const returnedTurnId =
          typeof resultTurn?.id === 'string' && resultTurn.id.trim().length > 0
            ? resultTurn.id.trim()
            : undefined;
        if (!returnedTurnId) {
          pending.reject(
            makeBoundaryError('codex app-server turn/start did not return a valid turn.id'),
          );
          return;
        }
        expectedTurnId = returnedTurnId;
        phase = 'turn-running';
        const matchedEarly = earlyTurnCompletions.find(
          event => event.turnId === expectedTurnId && event.threadId === activeThreadId,
        );
        earlyTurnCompletions.length = 0;
        if (matchedEarly) {
          completedTurn = matchedEarly;
        }
      }
      pending.resolve(message.result);
      return;
    }

    // Notification: only correlate turn/completed after turn/start; buffer early notifications
    // arriving before the turn/start response so they are validated once expectedTurnId is known.
    if (message.method === 'turn/completed') {
      if (phase !== 'turn-sent' && phase !== 'turn-running') {
        return;
      }
      const params = isRecord(message.params) ? message.params : undefined;
      const turn = params && isRecord(params.turn) ? params.turn : undefined;
      const turnId = typeof turn?.id === 'string' && turn.id.trim().length > 0 ? turn.id.trim() : undefined;
      const rawThreadId =
        typeof params?.threadId === 'string' && params.threadId.trim().length > 0
          ? params.threadId.trim()
          : typeof turn?.threadId === 'string' && turn.threadId.trim().length > 0
            ? turn.threadId.trim()
            : undefined;
      if (!turnId || !rawThreadId) {
        return;
      }
      const event: CompletedTurnEvent = {
        threadId: rawThreadId,
        turnId,
        status: turn?.status,
        error: turn?.error ?? params?.error,
      };
      if (phase === 'turn-sent') {
        if (earlyTurnCompletions.length >= MAX_EARLY_COMPLETIONS) {
          earlyTurnCompletions.shift();
        }
        earlyTurnCompletions.push(event);
        return;
      }
      if (turnId !== expectedTurnId || rawThreadId !== activeThreadId) {
        return;
      }
      completedTurn = event;
      if (turnWaiter) {
        const waiter = turnWaiter;
        turnWaiter = undefined;
        waiter.resolve(event);
      }
    }
  };

  child.stderr.on('data', onStderrData);
  child.stdin.on('error', onStdinError);
  child.on('error', onChildError);
  child.once('exit', onChildExit);
  child.once('close', onChildClose);
  rl.on('line', onLine);
  rl.once('close', onStdoutClose);
  options.signal?.addEventListener('abort', onAbort, { once: true });

  const sendRequest = (
    id: number,
    method: string,
    params: Record<string, unknown>,
    onBeforeWrite?: () => void,
  ): Promise<unknown> => {
    if (fatalError) return Promise.reject(fatalError);
    if (options.signal?.aborted) {
      return Promise.reject(makeBoundaryError(`codex app-server aborted: ${String(options.signal.reason ?? 'aborted')}`));
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (activeRpc?.id !== id) return;
        activeRpc = undefined;
        const timeoutError = makeBoundaryError(
          `codex app-server ${method} timed out after ${rpcTimeoutMs}ms`,
        );
        terminateChild();
        reject(timeoutError);
      }, rpcTimeoutMs);
      timer.unref();
      activeRpc = { id, method, resolve, reject, timer };
      try {
        writeJsonLine({ jsonrpc: '2.0', id, method, params }, true, onBeforeWrite);
      } catch (error) {
        if (activeRpc?.id === id) {
          activeRpc = undefined;
          clearTimeout(timer);
        }
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  };

  const waitForTurnCompletion = (): Promise<CompletedTurnEvent> => {
    if (completedTurn) return Promise.resolve(completedTurn);
    if (fatalError) return Promise.reject(fatalError);
    if (options.signal?.aborted) {
      return Promise.reject(makeBoundaryError(`codex app-server aborted: ${String(options.signal.reason ?? 'aborted')}`));
    }
    return new Promise((resolve, reject) => {
      turnWaiter = { resolve, reject };
    });
  };

  const teardownChild = async (mode: 'graceful' | 'terminate'): Promise<boolean> => {
    shuttingDown = true;
    try {
      if (!child.stdin.destroyed && !child.stdin.writableEnded) {
        child.stdin.end();
      }
    } catch {
      // Ignore stdin close errors during shutdown.
    }
    if (mode === 'terminate') {
      terminateChild();
    }
    if (childExited) return true;
    await new Promise<void>(resolve => {
      let settled = false;
      let graceTimer: ReturnType<typeof setTimeout> | undefined;
      let hardTimer: ReturnType<typeof setTimeout> | undefined;
      const done = () => {
        if (settled) return;
        settled = true;
        exitWaiter = undefined;
        if (graceTimer) clearTimeout(graceTimer);
        if (hardTimer) clearTimeout(hardTimer);
        resolve();
      };
      exitWaiter = done;
      if (childExited) {
        done();
        return;
      }
      if (mode === 'terminate') {
        hardTimer = setTimeout(done, killGraceMs + 100);
        hardTimer.unref();
        return;
      }
      graceTimer = setTimeout(() => {
        graceTimer = undefined;
        terminateChild();
        hardTimer = setTimeout(done, killGraceMs + 100);
        hardTimer.unref();
      }, closeGraceMs);
      graceTimer.unref();
    });
    return childExited;
  };

  let succeeded = false;
  let tornDown = false;
  let cleanupError: Error | undefined;
  let thrownError: unknown;
  try {
    await sendRequest(1, 'initialize', {
      clientInfo: { name: 'the-dealer', version: '1.0.0' },
    });

    writeJsonLine({ jsonrpc: '2.0', method: 'initialized', params: {} });

    const resumeThread = options.mode === 'fixed' || (options.mode === 'wave' && Boolean(sessionId));
    const threadResult = resumeThread
      ? await sendRequest(2, 'thread/resume', {
          threadId: sessionId!,
          ...(options.cwd ? { cwd: options.cwd } : {}),
          approvalPolicy: 'never',
          sandbox: 'danger-full-access',
          excludeTurns: true,
        })
      : await sendRequest(2, 'thread/start', {
          ...(options.cwd ? { cwd: options.cwd } : {}),
          approvalPolicy: 'never',
          sandbox: 'danger-full-access',
        });

    const threadRecord =
      isRecord(threadResult) && isRecord(threadResult.thread) ? threadResult.thread : undefined;
    const threadId =
      typeof threadRecord?.id === 'string' && threadRecord.id.trim().length > 0
        ? threadRecord.id.trim()
        : undefined;
    if (!threadId) {
      throw new CommandNotStartedError(
        `codex app-server ${resumeThread ? 'thread/resume' : 'thread/start'} did not return a valid thread.id${formatStderrSuffix()}`,
      );
    }
    activeThreadId = threadId;

    // Crossing the external-effect boundary: transition phase inside sendRequest right before
    // child.stdin.write executes (after all pre-write guards pass), so closed-stdin or pre-write
    // abort still throws CommandNotStartedError instead of falsely fencing deliveryInDoubt.
    await sendRequest(
      3,
      'turn/start',
      {
        threadId,
        input: [{ type: 'text', text: options.promptText }],
      },
      () => {
        phase = 'turn-sent';
      },
    );

    const completion = await waitForTurnCompletion();
    phase = 'completed';
    const exitedCleanly = await teardownChild('graceful');
    tornDown = true;

    if (completion.status !== 'completed') {
      const detail = formatTurnError(completion.error);
      throw new Error(
        `Codex turn ended with status ${String(completion.status ?? 'unknown')}${detail ? `: ${detail}` : ''}${formatStderrSuffix()}`,
      );
    }
    if (!exitedCleanly) {
      throw new Error(`codex app-server failed to terminate within bounded teardown (${closeGraceMs + killGraceMs}ms)`);
    }

    succeeded = true;
    return {
      threadId,
      ...(completion.turnId ? { turnId: completion.turnId } : {}),
      status: 'completed',
      stdout: JSON.stringify({ threadId }),
      stderr: stderrTail.toString(),
    };
  } catch (error) {
    thrownError = error;
    throw error;
  } finally {
    if (activeRpc) {
      clearTimeout(activeRpc.timer);
      activeRpc = undefined;
    }
    options.signal?.removeEventListener('abort', onAbort);
    rl.off('line', onLine);
    rl.off('close', onStdoutClose);
    rl.close();
    child.stderr.off('data', onStderrData);
    if (!succeeded && !tornDown) {
      const exited = await teardownChild('terminate');
      if (!exited) {
        cleanupError = new Error(`codex app-server failed to terminate within bounded teardown (${killGraceMs}ms)`);
      }
    }
    if (terminationTimer) {
      clearTimeout(terminationTimer);
      terminationTimer = undefined;
    }
    // Keep a no-op error sink before removing active handlers so a late EPIPE or process error
    // from a closing child can never become an uncaught exception.
    const absorbLateError = () => {};
    child.stdin.on('error', absorbLateError);
    child.on('error', absorbLateError);
    child.stdin.off('error', onStdinError);
    child.off('error', onChildError);
    child.off('exit', onChildExit);
    child.off('close', onChildClose);
    if (cleanupError && thrownError !== undefined) {
      if (thrownError instanceof Error) {
        (thrownError as Error & { cleanupError?: Error }).cleanupError = cleanupError;
        thrownError.message = `${thrownError.message}; ${cleanupError.message}`;
      }
    }
  }
}
