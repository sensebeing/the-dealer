import { EventEmitter } from 'node:events';
import { createInterface } from 'node:readline';
import { PassThrough } from 'node:stream';
import type {
  StdioChildProcess,
  StdioProcessRequest,
  StdioProcessSpawner,
} from '../../packages/subprocess/index.ts';

export interface FakeCodexJsonRpcMessage {
  jsonrpc?: string;
  id?: number | string | null;
  method?: string;
  params?: Record<string, unknown>;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
}

export interface FakeCodexSessionRecord {
  index: number;
  request: StdioProcessRequest;
  requests: FakeCodexJsonRpcMessage[];
  clientResponses: FakeCodexJsonRpcMessage[];
  killedSignals: (NodeJS.Signals | number | undefined)[];
  stdinEnded: boolean;
  threadId?: string;
  threadName?: string;
  turnId?: string;
  promptText?: string;
  threadMethod?: 'thread/start' | 'thread/resume';
  threadParams?: Record<string, unknown>;
}

export interface FakeCodexTurnContext {
  session: FakeCodexSessionRecord;
  child: FakeCodexChild;
  send: (message: Record<string, unknown>) => void;
  sendRawStdout: (text: string) => void;
  sendStderr: (text: string) => void;
  exit: (code?: number | null, signal?: NodeJS.Signals | null) => void;
  emitError: (error: Error) => void;
}

export interface FakeCodexAppServerOptions {
  /** Throw synchronously from spawner. */
  spawnThrow?: (request: StdioProcessRequest, index: number) => Error | undefined;
  /** Emit an async error event on the child right after spawn. */
  spawnError?: (request: StdioProcessRequest, index: number) => Error | undefined;
  /** Override thread id returned by thread/start. */
  nextThreadId?: (index: number, params: Record<string, unknown>, session: FakeCodexSessionRecord) => string;
  /** Threads that reject `thread/resume` with JSON-RPC `-32600: thread already has an active writer`. */
  activeWriterThreadIds?:
    | ReadonlySet<string>
    | readonly string[]
    | ((threadId: string, session: FakeCodexSessionRecord) => boolean);
  /** Custom handler per JSON-RPC request/notification; return true to suppress default reply. */
  onMessage?: (
    message: FakeCodexJsonRpcMessage,
    ctx: FakeCodexTurnContext,
  ) => boolean | void | Promise<boolean | void>;
  /** Optional hook executed before emitting turn/completed (return status/error or throw/exit via ctx). */
  onTurnStart?: (
    params: Record<string, unknown>,
    ctx: FakeCodexTurnContext,
  ) =>
    | void
    | { status?: string; error?: unknown; suppressCompletion?: boolean }
    | Promise<void | { status?: string; error?: unknown; suppressCompletion?: boolean }>;
  /** If true, do not exit automatically when stdin ends (for testing SIGTERM/SIGKILL grace). */
  ignoreStdinEnd?: boolean;
  /** If true, ignore SIGTERM and only exit on SIGKILL. */
  ignoreSigterm?: boolean;
}

class FakeCodexChild extends EventEmitter implements StdioChildProcess {
  pid?: number;
  readonly stdin = new PassThrough();
  readonly stdout = new PassThrough();
  readonly stderr = new PassThrough();
  exited = false;
  private readonly record: FakeCodexSessionRecord;
  private readonly ignoreSigterm: boolean;
  private readonly keepAlive: ReturnType<typeof setInterval>;

  constructor(record: FakeCodexSessionRecord, pid: number, ignoreSigterm = false) {
    super();
    this.record = record;
    this.pid = pid;
    this.ignoreSigterm = ignoreSigterm;
    // Mirror a live OS child process holding the event loop open until it exits.
    this.keepAlive = setInterval(() => {}, 60_000);
  }

  stopKeepAlive(): void {
    clearInterval(this.keepAlive);
  }

  finish(code: number | null = 0, signal: NodeJS.Signals | null = null): void {
    if (this.exited) return;
    this.exited = true;
    clearInterval(this.keepAlive);
    this.stdout.end();
    this.stderr.end();
    this.emit('exit', code, signal);
    this.emit('close', code, signal);
  }

  kill(signal?: NodeJS.Signals | number): boolean {
    this.record.killedSignals.push(signal);
    if (this.exited) return false;
    if (this.ignoreSigterm && (signal === undefined || signal === 'SIGTERM')) {
      return true;
    }
    queueMicrotask(() => {
      this.finish(null, typeof signal === 'string' ? signal : 'SIGTERM');
    });
    return true;
  }
}

export function createFakeCodexAppServer(options: FakeCodexAppServerOptions = {}) {
  const sessions: FakeCodexSessionRecord[] = [];
  let threadCounter = 0;

  const spawner: StdioProcessSpawner = request => {
    const index = sessions.length + 1;
    const syncErr = options.spawnThrow?.(request, index);
    if (syncErr) throw syncErr;

    const record: FakeCodexSessionRecord = {
      index,
      request,
      requests: [],
      clientResponses: [],
      killedSignals: [],
      stdinEnded: false,
    };
    sessions.push(record);

    const asyncErr = options.spawnError?.(request, index);
    const child = new FakeCodexChild(record, asyncErr ? undefined! : 10_000 + index, options.ignoreSigterm);

    const send = (message: Record<string, unknown>) => {
      if (!child.stdout.destroyed && !child.stdout.writableEnded) {
        child.stdout.write(`${JSON.stringify(message)}\n`);
      }
    };
    const sendRawStdout = (text: string) => {
      if (!child.stdout.destroyed && !child.stdout.writableEnded) {
        child.stdout.write(text);
      }
    };
    const sendStderr = (text: string) => {
      if (!child.stderr.destroyed && !child.stderr.writableEnded) {
        child.stderr.write(text);
      }
    };
    const ctx: FakeCodexTurnContext = {
      session: record,
      child,
      send,
      sendRawStdout,
      sendStderr,
      exit: (code = 0, signal = null) => child.finish(code, signal),
      emitError: error => {
        child.exited = true;
        child.stopKeepAlive();
        child.emit('error', error);
      },
    };

    if (asyncErr) {
      queueMicrotask(() => {
        ctx.emitError(asyncErr);
      });
      return child;
    }

    const rl = createInterface({ input: child.stdin, crlfDelay: Infinity });
    rl.on('line', line => {
      const trimmed = line.trim();
      if (!trimmed) return;
      let parsed: FakeCodexJsonRpcMessage;
      try {
        parsed = JSON.parse(trimmed) as FakeCodexJsonRpcMessage;
      } catch {
        return;
      }
      if (parsed.method === undefined && parsed.id !== undefined) {
        record.clientResponses.push(parsed);
        return;
      }
      record.requests.push(parsed);

      void (async () => {
        const handled = await options.onMessage?.(parsed, ctx);
        if (handled === true || child.exited) return;

        if (parsed.method === 'initialize') {
          send({ jsonrpc: '2.0', id: parsed.id, result: { userAgent: 'codex-fake/0.172.0' } });
          return;
        }
        if (parsed.method === 'initialized') {
          return;
        }
        if (parsed.method === 'thread/start') {
          record.threadMethod = 'thread/start';
          record.threadParams = parsed.params;
          threadCounter += 1;
          const threadId = options.nextThreadId
            ? options.nextThreadId(threadCounter, parsed.params ?? {}, record)
            : `codex-thread-${threadCounter}`;
          record.threadId = threadId;
          send({ jsonrpc: '2.0', id: parsed.id, result: { thread: { id: threadId } } });
          return;
        }
        if (parsed.method === 'thread/resume') {
          record.threadMethod = 'thread/resume';
          record.threadParams = parsed.params;
          const threadId = String(parsed.params?.threadId ?? '');
          const isActiveWriter =
            typeof options.activeWriterThreadIds === 'function'
              ? options.activeWriterThreadIds(threadId, record)
              : options.activeWriterThreadIds instanceof Set
                ? options.activeWriterThreadIds.has(threadId)
                : Array.isArray(options.activeWriterThreadIds)
                  ? options.activeWriterThreadIds.includes(threadId)
                  : false;
          if (isActiveWriter) {
            send({
              jsonrpc: '2.0',
              id: parsed.id,
              error: { code: -32600, message: 'thread already has an active writer' },
            });
            return;
          }
          record.threadId = threadId;
          send({ jsonrpc: '2.0', id: parsed.id, result: { thread: { id: threadId } } });
          return;
        }
        if (parsed.method === 'thread/name/set') {
          record.threadName = typeof parsed.params?.name === 'string' ? parsed.params.name : undefined;
          send({ jsonrpc: '2.0', id: parsed.id, result: {} });
          return;
        }
        if (parsed.method === 'turn/start') {
          const input = Array.isArray(parsed.params?.input) ? parsed.params!.input : [];
          const firstInput = input[0] as { text?: string } | undefined;
          record.promptText = typeof firstInput?.text === 'string' ? firstInput.text : undefined;
          const turnId = `turn-${index}`;
          record.turnId = turnId;
          send({ jsonrpc: '2.0', id: parsed.id, result: { turn: { id: turnId, status: 'inProgress' } } });
          const turnOutcome = await options.onTurnStart?.(parsed.params ?? {}, ctx);
          if (child.exited || turnOutcome?.suppressCompletion) return;
          send({
            jsonrpc: '2.0',
            method: 'turn/completed',
            params: {
              threadId: record.threadId,
              turn: {
                id: turnId,
                status: turnOutcome?.status ?? 'completed',
                ...(turnOutcome?.error !== undefined ? { error: turnOutcome.error } : {}),
              },
            },
          });
        }
      })();
    });

    child.stdin.once('end', () => {
      record.stdinEnded = true;
      rl.close();
      if (!options.ignoreStdinEnd && !child.exited) {
        queueMicrotask(() => child.finish(0, null));
      }
    });

    return child;
  };

  return { spawner, sessions };
}
