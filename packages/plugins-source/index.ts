import { createServer } from 'node:http';
import type { Server } from 'node:http';
import type { EmitMessageInput, EmitterService, SourcePlugin } from '../core-types/index.ts';
import { checkedCommand, runCommand } from '../plugins-executor/index.ts';
import type { CommandRunner } from '../plugins-executor/index.ts';

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

export interface PollWatermarkSourceConfig<T, Cursor> {
  initialCursor: Cursor;
  poll: (cursor: Cursor) => Promise<{ items: readonly T[]; cursor: Cursor }>;
  toInput: (item: T) => EmitMessageInput | Promise<EmitMessageInput>;
  intervalMs?: number;
  saveCursor?: (cursor: Cursor) => Promise<void>;
  onError?: (error: unknown) => void;
}

export interface PollWatermarkSource<Cursor> extends SourcePlugin {
  pollOnce(): Promise<void>;
  getCursor(): Cursor;
}

export function createPollWatermarkSourcePlugin<T, Cursor>(
  config: PollWatermarkSourceConfig<T, Cursor>,
): PollWatermarkSource<Cursor> {
  let cursor = config.initialCursor;
  let emitter: EmitterService | undefined;
  let timer: ReturnType<typeof setInterval> | undefined;
  let active: Promise<void> | undefined;
  let stopped = true;
  const pollOnce = (): Promise<void> => {
    if (active) {
      return active;
    }
    if (!emitter || stopped) {
      return Promise.reject(new Error('Poll source not started'));
    }
    const target = emitter;
    active = (async () => {
      const page = await config.poll(cursor);
      for (const item of page.items) {
        const result = await target.emit(await config.toInput(item));
        if (result.receipts.some(r => r.stage === 'failed' || r.stage === 'rejected')) {
          throw new Error('Poll item delivery failed; cursor retained');
        }
      }
      await config.saveCursor?.(page.cursor);
      cursor = page.cursor;
    })().finally(() => {
      active = undefined;
    });
    return active;
  };
  const run = () => {
    void pollOnce().catch(error => config.onError?.(error));
  };
  return {
    name: 'poll-watermark',
    getCursor: () => cursor,
    pollOnce,
    start(value) {
      if (!stopped) {
        throw new Error('Poll source already started');
      }
      emitter = value;
      stopped = false;
      if (config.intervalMs !== undefined && config.intervalMs > 0) {
        // Drain the current watermark immediately instead of waiting one full interval.
        queueMicrotask(() => {
          if (!stopped) {
            run();
          }
        });
        timer = setInterval(() => {
          if (!stopped) {
            run();
          }
        }, config.intervalMs);
        timer.unref();
      }
    },
    async stop() {
      stopped = true;
      if (timer) {
        clearInterval(timer);
      }
      // Poll failures are reported through onError; stop only waits for quiescence.
      await active?.catch(() => {});
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
