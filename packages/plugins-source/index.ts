import { existsSync, readFileSync } from 'node:fs';
import { writeFileAtomically } from '../fs-atomic/index.ts';
import { isConfirmedHandoffReceipt } from '../core-types/index.ts';
import { createServer } from 'node:http';
import type { Server } from 'node:http';
import type {
  DeliveryReceipt,
  EmitMessageInput,
  EmitterService,
  SourcePlugin,
} from '../core-types/index.ts';
import { checkedCommand, runCommand, singleLine } from '../subprocess/index.ts';
import type { CommandRunner } from '../subprocess/index.ts';

export interface WebhookSourceConfig {
  port?: number;
  host?: string;
  path?: string;
  bearerToken?: string;
  maxBodyBytes?: number;
  toInput?: (payload: unknown) => EmitMessageInput | Promise<EmitMessageInput>;
}

export interface WebhookSource extends SourcePlugin {
  address(): ReturnType<Server['address']>;
}

export function createWebhookSourcePlugin(config: WebhookSourceConfig = {}): WebhookSource {
  let server: Server | undefined;
  return {
    name: 'webhook',
    address: () => server?.address() ?? null,
    async start(emitter) {
      if (server) {
        throw new Error('Webhook already started');
      }
      server = createServer(async (request, response) => {
        if (request.method !== 'POST' || request.url !== (config.path ?? '/')) {
          response.writeHead(404).end();
          return;
        }
        if (config.bearerToken && request.headers.authorization !== `Bearer ${config.bearerToken}`) {
          response.writeHead(401).end();
          return;
        }
        try {
          let size = 0;
          const chunks: Buffer[] = [];
          for await (const chunk of request) {
            size += chunk.length;
            if (size > (config.maxBodyBytes ?? 1_048_576)) {
              response.writeHead(413).end();
              return;
            }
            chunks.push(Buffer.from(chunk));
          }
          const payload = JSON.parse(Buffer.concat(chunks).toString('utf8'));
          const input = config.toInput ? await config.toInput(payload) : payload;
          if (
            !input ||
            typeof input.networkId !== 'string' ||
            typeof input.kind !== 'string' ||
            typeof input.subject !== 'string' ||
            typeof input.body !== 'string' ||
            !input.routing ||
            input.payload === undefined
          ) {
            response.writeHead(400).end('Invalid emit input');
            return;
          }
          const result = await emitter.emit(input);
          response.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(result));
        } catch (error) {
          response
            .writeHead(error instanceof SyntaxError ? 400 : 500)
            .end('Webhook delivery failed');
        }
      });
      await new Promise<void>((resolve, reject) => {
        server!.once('error', reject);
        server!.listen(config.port ?? 0, config.host ?? '127.0.0.1', () => {
          server!.off('error', reject);
          resolve();
        });
      });
    },
    async stop() {
      const current = server;
      server = undefined;
      if (current?.listening) {
        await new Promise<void>((resolve, reject) =>
          current.close(error => (error ? reject(error) : resolve())),
        );
      }
    },
  };
}

/** Persist only complete JSON snapshots; failed writes never replace the previous cursor. */
export function writeAtomicCursorFile(path: string, cursor: unknown): void {
  const json = JSON.stringify(cursor);
  if (json === undefined) throw new Error('Cursor must be JSON serializable');
  writeFileAtomically(path, json + '\n');
}

export function readJsonCursorFile<T>(path: string, fallback: T): T {
  try {
    return JSON.parse(readFileSync(path, 'utf8')) as T;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return fallback;
    throw error;
  }
}

export function everyReceiptSettled(
  receipts: readonly DeliveryReceipt[],
  predicate: (receipt: DeliveryReceipt) => boolean,
): boolean {
  return receipts.length > 0 && receipts.every(predicate);
}

export function formatSingleLineError(error: unknown, maxLength = 240): string {
  const detail = error as { stderr?: unknown; message?: unknown } | null | undefined;
  const stderr = detail?.stderr;
  const firstLine = (value: string) => value.split(/[\r\n\u2028\u2029]/u)
    .map(singleLine).find(line => line && !line.startsWith('at '));
  const summary = firstLine(typeof stderr === 'string' ? stderr : Buffer.isBuffer(stderr) ? stderr.toString() : '')
    ?? firstLine(typeof detail?.message === 'string' ? detail.message : String(error))
    ?? 'Unknown error';
  return summary.slice(0, Math.max(0, maxLength));
}

export interface PollSourceStatusEvent {
  readonly source: string;
  readonly state: 'failed' | 'backoff' | 'recovered';
  readonly failures: number;
  readonly nextDelayMs?: number;
  readonly error?: string;
}

export interface PollWatermarkSourceConfig<T, Cursor> {
  name?: string;
  initialCursor: Cursor;
  cursorPath?: string;
  settlementMode?: 'settled' | 'queued-or-settled';
  wake?: { port?: number; host?: string; path?: string; allowedOrigins?: readonly string[] };
  /** Read-only fetch phase. The signal aborts the fetch; the cursor is only touched after it resolves. */
  poll: (cursor: Cursor, signal: AbortSignal) => Promise<{ items: readonly T[]; cursor: Cursor }>;
  toInput: (item: T) => EmitMessageInput | Promise<EmitMessageInput>;
  advanceItemCursor?: (
    cursor: Cursor,
    item: T,
    receipts: readonly DeliveryReceipt[],
  ) => Cursor | undefined;
  intervalMs?: number;
  maxBackoffMs?: number;
  saveCursor?: (cursor: Cursor) => Promise<void>;
  onError?: (error: unknown) => void;
  onPollStatus?: (event: PollSourceStatusEvent) => void;
}

export interface PollWatermarkSource<Cursor> extends SourcePlugin {
  wake(): void;
  pollOnce(): Promise<void>;
  getCursor(): Cursor;
  wakeAddress(): ReturnType<Server['address']>;
}

export function createPollWatermarkSourcePlugin<T, Cursor>(
  config: PollWatermarkSourceConfig<T, Cursor>,
): PollWatermarkSource<Cursor> {
  const name = config.name?.trim() || 'poll-watermark';
  const loadCursor = () => config.cursorPath && existsSync(config.cursorPath)
    ? readJsonCursorFile(config.cursorPath, config.initialCursor) : config.initialCursor;
  let cursor = loadCursor();
  const saveCursor = config.saveCursor ?? (config.cursorPath
    ? async (value: Cursor) => writeAtomicCursorFile(config.cursorPath!, value) : undefined);
  let wakeServer: Server | undefined;
  let wakeStarting: Promise<void> | undefined;
  let emitter: EmitterService | undefined;
  const intervalMs = config.intervalMs ?? 0;
  const maxBackoffMs = config.maxBackoffMs ?? Math.max(intervalMs * 8, 10 * 60_000);
  let timer: ReturnType<typeof setTimeout> | undefined;
  let consecutiveFailures = 0;
  let lastErrorSummary: string | undefined;
  let lastReportedDelayMs: number | undefined;
  let active: Promise<void> | undefined;
  let wakeQueued = false;
  let settling: Promise<void> | undefined;
  let pollController: AbortController | undefined;
  let stopped = true;
  const clearSchedule = () => {
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  };
  const nextPollDelay = () => {
    if (!(intervalMs > 0)) return undefined;
    return consecutiveFailures === 0 ? intervalMs
      : Math.min(intervalMs * 2 ** Math.min(consecutiveFailures, 4), maxBackoffMs);
  };
  const schedule = () => {
    const nextDelay = nextPollDelay();
    if (stopped || nextDelay === undefined) return;
    timer = setTimeout(() => {
      timer = undefined;
      runScheduledPoll();
    }, nextDelay);
    timer.unref();
  };
  const clearFailureStatus = () => {
    lastErrorSummary = undefined;
    lastReportedDelayMs = undefined;
  };
  const reportStatus = config.onPollStatus ?? (config.onError ? undefined : (event: PollSourceStatusEvent) => {
    console.error(`[dealer:source] ${JSON.stringify(event)}`);
  });
  const reportFailure = (error: unknown) => {
    const summary = formatSingleLineError(error);
    const nextDelayMs = nextPollDelay();
    const changed = summary !== lastErrorSummary;
    if (!changed && nextDelayMs === lastReportedDelayMs) return;
    lastErrorSummary = summary;
    lastReportedDelayMs = nextDelayMs;
    reportStatus?.({
      source: name,
      state: changed ? 'failed' : 'backoff',
      failures: consecutiveFailures,
      ...(nextDelayMs !== undefined ? { nextDelayMs } : {}),
      error: summary,
    });
  };

  const pollOnce = (background = false): Promise<void> => {
    if (active) {
      return active;
    }
    if (!emitter || stopped || !pollController) {
      return Promise.reject(new Error('Poll source not started'));
    }
    clearSchedule();
    wakeQueued = false;
    const target = emitter;
    const runController = pollController;
    const signal = runController.signal;
    const run = (async () => {
      const page = await config.poll(cursor, signal);
      // A page that arrives after this run was cancelled is read-only data: dropping it
      // keeps the cursor and delivery state untouched.
      if (stopped || signal.aborted || pollController !== runController) {
        return;
      }
      let settlePromise: Promise<void> | undefined;
      settlePromise = (async () => {
        const advance = config.advanceItemCursor;
        let firstError: unknown;
        if (advance) {
          // Bootstrap or baseline cursor updates must survive a later item failure.
          if (JSON.stringify(page.cursor) !== JSON.stringify(cursor)) {
            await saveCursor?.(page.cursor);
            cursor = page.cursor;
          }
          for (const item of page.items) {
            try {
              const result = await target.emit(await config.toInput(item));
              const nextCursor = advance(cursor, item, result.receipts);
              if (nextCursor !== undefined) {
                await saveCursor?.(nextCursor);
                cursor = nextCursor;
              } else {
                firstError ??= new Error('Poll item delivery failed; cursor retained');
              }
            } catch (error) {
              firstError ??= error;
            }
          }
        } else {
          // Attempt every item so one failure cannot starve independent items behind it.
          for (const item of page.items) {
            try {
              const result = await target.emit(await config.toInput(item));
              if (!everyReceiptSettled(result.receipts, receipt => isConfirmedHandoffReceipt(receipt, { allowQueued: config.settlementMode === 'queued-or-settled' }))) {
                firstError ??= new Error('Poll item delivery failed; cursor retained');
              }
            } catch (error) {
              firstError ??= error;
            }
          }
          if (firstError === undefined) {
            await saveCursor?.(page.cursor);
            cursor = page.cursor;
          }
        }
        if (firstError !== undefined) {
          throw firstError;
        }
      })().finally(() => {
        if (settling === settlePromise) settling = undefined;
      });
      settling = settlePromise;
      await settlePromise;
    })().then(() => {
      if (pollController !== runController) return;
      const previousFailures = consecutiveFailures;
      consecutiveFailures = 0;
      if (lastErrorSummary !== undefined) {
        clearFailureStatus();
        reportStatus?.({ source: name, state: 'recovered', failures: previousFailures });
      }
    }, error => {
      if (pollController === runController) {
        consecutiveFailures++;
        if (background) reportFailure(error);
      }
      throw error;
    }).finally(() => {
      if (active === run) active = undefined;
      if (pollController !== runController || stopped) return;
      if (wakeQueued) {
        wakeQueued = false;
        runScheduledPoll();
      } else {
        schedule();
      }
    });
    active = run;
    return run;
  };
  const runScheduledPoll = () => {
    if (!active && !stopped) void pollOnce(true).catch(error => config.onError?.(error));
  };
  const triggerWake = () => {
    if (stopped || !emitter) throw new Error('Poll source not started');
    consecutiveFailures = 0;
    clearSchedule();
    if (active) wakeQueued = true;
    else runScheduledPoll();
  };
  return {
    name,
    getCursor: () => cursor,
    wakeAddress: () => wakeServer?.address() ?? null,
    pollOnce: () => pollOnce(),
    wake: triggerWake,
    async start(value) {
      if (!stopped || wakeStarting) {
        throw new Error('Poll source already started');
      }
      if (config.cursorPath && existsSync(config.cursorPath)) cursor = loadCursor();
      emitter = value;
      stopped = false;
      wakeQueued = false;
      consecutiveFailures = 0;
      clearFailureStatus();
      pollController = new AbortController();
      if (config.wake) {
        const wake = config.wake;
        wakeServer = createServer((request, response) => {
          if (request.url !== (wake.path ?? '/wake')) {
            response.writeHead(404).end();
            return;
          }
          const origin = request.headers.origin;
          if (origin && wake.allowedOrigins && !wake.allowedOrigins.includes(origin)) {
            response.writeHead(403).end();
            return;
          }
          if (origin && wake.allowedOrigins?.includes(origin)) {
            response.setHeader('access-control-allow-origin', origin);
            response.setHeader('vary', 'Origin');
            response.setHeader('access-control-allow-methods', 'POST, OPTIONS');
            response.setHeader('access-control-allow-headers', 'Content-Type');
          }
          if (request.method === 'OPTIONS') {
            response.writeHead(204).end();
          } else if (request.method === 'POST') {
            if (!stopped) triggerWake();
            response.writeHead(200, { 'content-type': 'application/json' }).end('{"ok":true}');
          } else {
            response.writeHead(405, { allow: 'POST, OPTIONS' }).end();
          }
          request.resume();
        });
        try {
          wakeStarting = new Promise<void>((resolve, reject) => {
            wakeServer!.once('error', reject);
            wakeServer!.listen(wake.port ?? 0, wake.host ?? '127.0.0.1', () => {
              wakeServer!.off('error', reject);
              resolve();
            });
          });
          await wakeStarting;
        } catch (error) {
          wakeServer = undefined;
          stopped = true;
          pollController?.abort();
          pollController = undefined;
          emitter = undefined;
          throw error;
        } finally {
          wakeStarting = undefined;
        }
      }
      if (intervalMs > 0 && !stopped) {
        // Drain the current watermark immediately instead of waiting one full interval.
        const controller = pollController;
        queueMicrotask(() => {
          if (pollController === controller) runScheduledPoll();
        });
      }
    },
    async stop() {
      stopped = true;
      wakeQueued = false;
      clearSchedule();
      clearFailureStatus();
      // Abort the read-only fetch phase first: a signal-aware poll cancels immediately, and a
      // legacy poll that ignores it must not hold shutdown open before any external effect.
      pollController?.abort();
      pollController = undefined;
      await wakeStarting?.catch(() => {});
      const server = wakeServer;
      wakeServer = undefined;
      if (server?.listening) {
        await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
      }
      if (!settling) {
        active = undefined;
      }
      // Item emission and cursor persistence are one atomic settlement that stop must await.
      // Poll failures are reported through onError; stop only waits for quiescence.
      await settling?.catch(() => {});
      emitter = undefined;
    },
  };
}

export interface ScriptSourceConfig {
  command: string;
  args?: readonly string[];
  cwd?: string;
  commandRunner?: CommandRunner;
  toInput?: (value: unknown) => EmitMessageInput;
}

export function createScriptSourcePlugin(config: ScriptSourceConfig): SourcePlugin {
  let controller: AbortController | undefined;
  let active: Promise<void> | undefined;
  return {
    name: 'script',
    start(emitter) {
      if (active) {
        throw new Error('Script already running');
      }
      controller = new AbortController();
      active = (async () => {
        const result = await checkedCommand(config.commandRunner ?? runCommand, {
          command: config.command,
          args: config.args ?? [],
          cwd: config.cwd,
          signal: controller!.signal,
        });
        for (const line of result.stdout.split(/\r?\n/).filter(line => line.trim())) {
          if (controller!.signal.aborted) {
            break;
          }
          const data = JSON.parse(line);
          await emitter.emit(config.toInput ? config.toInput(data) : data);
        }
      })();
      return active;
    },
    async stop() {
      controller?.abort();
      try {
        await active;
      } catch (error) {
        if (!(error instanceof Error && error.name === 'AbortError')) {
          throw error;
        }
      } finally {
        active = undefined;
      }
    },
  };
}
