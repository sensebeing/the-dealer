import { randomUUID } from 'node:crypto';
import { appendFile, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import type { ExecutionContext, ExecutorPlugin, MessageEnvelope } from '../core-types/index.ts';
import { MailboxStore, resolveMailboxId } from '../mailbox/index.ts';
import type { MailboxTopicBinding, WakeupRedriveService } from '../mailbox/index.ts';
import { checkedCommand, CommandNotStartedError, runCommand } from '../subprocess/index.ts';
import type { CommandRunner } from '../subprocess/index.ts';

export function renderWave(batch: readonly MessageEnvelope[]): string {
  return batch
    .map(
      (e) =>
        `Subject: ${e.subject}\nMessage: ${e.id}\n${e.body}\nPayload: ${JSON.stringify(e.payload)}\nAttachments: ${JSON.stringify(e.attachments)}`,
    )
    .join('\n\n---\n\n');
}

export function beforeExternalEffect<T extends Error>(error: T): T & { beforeExternalEffect: true } {
  return Object.assign(error, { beforeExternalEffect: true as const });
}

/** Only wrap validation/read-only preparation, never a mutating command or durable handoff. */
export function prepareExternalEffect<T>(prepare: () => T): T {
  try {
    return prepare();
  } catch (error) {
    throw beforeExternalEffect(error instanceof Error ? error : new Error(String(error)));
  }
}

export async function prepareExternalEffectAsync<T>(prepare: () => Promise<T>): Promise<T> {
  try {
    return await prepare();
  } catch (error) {
    throw beforeExternalEffect(error instanceof Error ? error : new Error(String(error)));
  }
}

export function rethrowBeforeCommandStart(error: unknown): never {
  throw error instanceof CommandNotStartedError ? beforeExternalEffect(error) : error;
}

export interface RawAgentOptions {
  command?: string;
  args?: readonly string[];
  cwd?: string;
  commandRunner?: CommandRunner;
}

/** Generic command/pipe execution. Host-specific launch conventions belong to host plugins. */
export function createRawCommandExecutorPlugin(options: RawAgentOptions = {}): ExecutorPlugin {
  return {
    name: 'agent-pty',
    async execute(batch, context) {
      if (!options.command?.trim()) throw beforeExternalEffect(new Error('agent-pty requires a command'));
      const output = await checkedCommand(options.commandRunner ?? runCommand, {
        command: options.command,
        args: options.args ?? [],
        cwd: options.cwd,
        input: renderWave(batch),
        signal: context.signal,
      }).catch(rethrowBeforeCommandStart);
      let result: unknown;
      try {
        result = JSON.parse(output.stdout);
      } catch {
        result = output.stdout;
      }
      return {
        executor: 'agent-pty',
        executionId: randomUUID(),
        output: result,
        stderr: output.stderr ?? '',
      };
    },
  };
}

export interface NotifyOptions {
  inboxPath?: string;
  notify?: (batch: readonly MessageEnvelope[], context: ExecutionContext) => Promise<void>;
  commandRunner?: CommandRunner;
}

export function createNotifyExecutorPlugin(options: NotifyOptions = {}): ExecutorPlugin {
  return {
    name: 'notify',
    async execute(batch, context) {
      if (options.inboxPath) {
        await mkdir(dirname(options.inboxPath), { recursive: true });
        await appendFile(options.inboxPath, `${batch.map((e) => JSON.stringify(e)).join('\n')}\n`, {
          mode: 0o600,
        });
      }
      if (options.notify) {
        await options.notify(batch, context);
      } else if (!options.inboxPath) {
        await checkedCommand(options.commandRunner ?? runCommand, {
          command: 'osascript',
          args: [
            '-e',
            'on run argv\ndisplay notification (item 1 of argv) with title (item 2 of argv)\nend run',
            renderWave(batch),
            'The Dealer',
          ],
          signal: context.signal,
        });
      }
      return { executor: 'notify', executionId: randomUUID(), count: batch.length };
    },
  };
}

export interface InboxExecutorOptions {
  resolveTopicBinding?: (kind: string, streamKey?: string) => MailboxTopicBinding | undefined;
  inboxPath?: string;
  store?: MailboxStore;
  /** Reactive redrive for replies/questions/settlements landing in a live wake target. */
  wakeup?: WakeupRedriveService;
  commandRunner?: CommandRunner;
}

export function createInboxExecutorPlugin(options: InboxExecutorOptions = {}): ExecutorPlugin {
  if (!options.store && !options.inboxPath) {
    throw new Error('Inbox executor requires store or inboxPath');
  }
  const store = options.store ?? new MailboxStore(options.inboxPath!);
  return {
    name: 'inbox',
    async execute(batch, context, runtimeConfig) {
      const config = (runtimeConfig ?? {}) as InboxExecutorOptions;
      const wakeup = options.wakeup ?? config.wakeup;
      const groups = new Map<string, MessageEnvelope[]>();
      for (const envelope of batch) {
        const mailboxId = prepareExternalEffect(() =>
          resolveMailboxId(
            envelope,
            (options.resolveTopicBinding ?? config.resolveTopicBinding)?.(
              envelope.kind,
              typeof envelope?.payload?.streamKey === 'string' ? envelope.payload.streamKey : undefined,
            ),
            store.resolveThreadLocalMailbox(envelope.semantic?.threadId, envelope.networkId),
          ),
        );
        const group = groups.get(mailboxId) ?? [];
        group.push(envelope);
        groups.set(mailboxId, group);
      }
      const prepared = [];
      for (const [mailboxId, envelopes] of groups) {
        const wakeTarget = await prepareExternalEffectAsync(() =>
          Promise.resolve(wakeup?.resolveSessionWakeTarget(mailboxId)),
        );
        prepared.push({ mailboxId, envelopes, wakeTarget });
      }
      const inserted: string[] = [];
      for (const { mailboxId, envelopes, wakeTarget } of prepared) {
        const result = store.enqueue(envelopes, { ...(context?.signedEnvelopes ? { signedEnvelopes: context.signedEnvelopes } : {}), mailboxId, ...(wakeTarget ? { wakeTarget } : {}) });
        inserted.push(...result.inserted);
        if (result.inserted.length > 0) {
          wakeup?.notifyMailbox(mailboxId);
        }
      }
      return {
        executor: 'inbox',
        executionId: randomUUID(),
        ...(prepared.length === 1 ? { mailboxId: prepared[0].mailboxId } : {}),
        mailboxIds: prepared.map(item => item.mailboxId),
        count: batch.length,
        inserted,
      };
    },
  };
}
