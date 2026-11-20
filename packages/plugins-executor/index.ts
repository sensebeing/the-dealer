import { randomUUID } from 'node:crypto';
import { appendFile, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { PartialBatchExecutionError } from '../core-types/index.ts';
import type { ExecutionContext, ExecutionReceipt, ExecutorPlugin, MessageEnvelope } from '../core-types/index.ts';
import { envelopeThreadId, MailboxStore, resolveMailboxId } from '../mailbox/index.ts';
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

export interface PreparedDelivery {
  envelopes: readonly MessageEnvelope[];
  deliver(commit: () => void): ExecutionReceipt | Promise<ExecutionReceipt>;
}

type ThreadCustody = (envelopes: readonly MessageEnvelope[]) => void;
const threadReservation = Symbol('thread reservation');
type ReservedExecutionContext = ExecutionContext & {
  [threadReservation]?: {
    storePath: string;
    batch: readonly MessageEnvelope[];
    commit: ThreadCustody;
    reused: boolean;
  };
};
const pendingDeliveries = new Map<string, Promise<void>>();

const threadDeliveryKey = (store: MailboxStore, envelope: MessageEnvelope) =>
  JSON.stringify([store.path, envelope.networkId, envelopeThreadId(envelope)]);

async function serializeDeliveryKeys<T>(
  keys: Iterable<string>, deliver: (release: (key: string) => void) => Promise<T>,
): Promise<T> {
  const releases = new Map<string, () => void>();
  const previous: Promise<void>[] = [];
  for (const key of keys) {
    const prior = pendingDeliveries.get(key);
    if (prior) previous.push(prior);
    const { promise, resolve } = Promise.withResolvers<void>();
    pendingDeliveries.set(key, promise);
    releases.set(key, () => {
      releases.delete(key);
      resolve();
      if (pendingDeliveries.get(key) === promise) pendingDeliveries.delete(key);
    });
  }
  try {
    if (previous.length) await Promise.all(previous);
    return await deliver(key => releases.get(key)?.());
  } finally {
    for (const release of releases.values()) release();
  }
}

/** A receiver-configured mailbox may be shared by several threads. */
export function serializeMailboxInitialization<T>(
  store: MailboxStore, mailboxId: string, deliver: (commit: () => void) => Promise<T>,
): Promise<T> {
  const key = JSON.stringify([store.path, mailboxId]);
  return serializeDeliveryKeys([key], release => deliver(() => release(key)));
}

/** Queue before yielding and release each thread when its final member takes custody. */
export async function serializeThreadDeliveries<T>(
  store: MailboxStore | undefined, batch: readonly MessageEnvelope[], context: ExecutionContext,
  deliver: (commit: ThreadCustody, context: ExecutionContext) => Promise<T>,
): Promise<T> {
  if (!store) return deliver(() => {}, context);
  const active = (context as ReservedExecutionContext)?.[threadReservation];
  // The runtime prepares attachments before the mailbox-backed executor takes custody.
  if (active?.storePath === store.path && !active.reused && active.batch.length === batch.length &&
      batch.every((envelope, index) => {
        const original = active.batch[index];
        return envelope.id === original.id && envelope.networkId === original.networkId &&
          envelopeThreadId(envelope) === envelopeThreadId(original);
      })) {
    active.reused = true;
    return deliver(active.commit, context);
  }
  const remaining = new Map<string, number>();
  for (const envelope of batch) {
    const key = threadDeliveryKey(store, envelope);
    remaining.set(key, (remaining.get(key) ?? 0) + 1);
  }
  return serializeDeliveryKeys(remaining.keys(), release => {
    const commit: ThreadCustody = envelopes => {
      for (const envelope of envelopes) {
        const key = threadDeliveryKey(store, envelope);
        const count = (remaining.get(key) ?? 0) - 1;
        if (count === 0) release(key);
        remaining.set(key, count);
      }
    };
    const reservedContext: ReservedExecutionContext = {
      ...context, [threadReservation]: { storePath: store.path, batch, commit, reused: false },
    };
    return deliver(commit, reservedContext);
  });
}

function mergeDeliveryReceipts(
  executor: string, batch: readonly MessageEnvelope[], receipts: ReadonlyMap<string, ExecutionReceipt>,
): ExecutionReceipt {
  const results = [...new Set(receipts.values())];
  const primary = receipts.get(batch[0]?.id);
  const { mailboxId: _mailboxId, mailboxIds: _mailboxIds, envelopeMailboxes: _envelopeMailboxes, ...primaryDetails } = primary ?? {};
  const receipt = { ...primaryDetails, executionId: primary?.executionId ?? randomUUID(), executor };
  if (!results.some(result => result.envelopeMailboxes !== undefined)) {
    return receipt;
  }
  const destinations = new Map<string, string>();
  for (const { id } of batch) {
    const mailboxes = receipts.get(id)?.envelopeMailboxes;
    if (mailboxes && Object.hasOwn(mailboxes, id)) destinations.set(id, mailboxes[id]);
  }
  const mailboxIds = [...new Set(destinations.values())];
  const woke = results.some(result => result.woke === true);
  const reason = results.find(result => result.reason !== undefined)?.reason;
  return {
    ...receipt,
    ...(destinations.size === batch.length && mailboxIds.length === 1 ? { mailboxId: mailboxIds[0] } : {}),
    mailboxIds,
    envelopeMailboxes: Object.fromEntries(destinations),
    inserted: results.flatMap(result => Array.isArray(result.inserted) ? result.inserted : []),
    ...(results.some(result => result.woke !== undefined) ? { woke } : {}),
    ...(reason !== undefined ? { reason } : {}),
  };
}

/** Run validated delivery groups with independent outcomes across external-effect boundaries. */
export async function executeDeliveryGroups(
  executor: string, batch: readonly MessageEnvelope[], groups: readonly PreparedDelivery[], commit: ThreadCustody,
): Promise<ExecutionReceipt> {
  const settled = new Map<string, ExecutionReceipt>();
  const unattempted = new Set(batch.map(envelope => envelope.id));
  for (const { envelopes, deliver } of groups) {
    let receipt: ExecutionReceipt;
    let committed = false;
    const commitGroup = () => {
      if (!committed) commit(envelopes);
      committed = true;
    };
    try {
      receipt = { ...await deliver(commitGroup), executor };
      commitGroup();
    } catch (error) {
      const beforeEffect = (error as { beforeExternalEffect?: boolean } | null)?.beforeExternalEffect === true;
      if (!settled.size && (beforeEffect || groups.length === 1)) throw error;
      if (!beforeEffect) for (const { id } of envelopes) unattempted.delete(id);
      throw new PartialBatchExecutionError(error, Object.fromEntries(settled), unattempted);
    }
    for (const { id } of envelopes) {
      settled.set(id, receipt);
      unattempted.delete(id);
    }
  }
  return mergeDeliveryReceipts(executor, batch, settled);
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

/** Prepare wake targets for callers that have already resolved each envelope's mailbox. */
export async function prepareInboxDeliveries(
  store: MailboxStore,
  wakeup: WakeupRedriveService | undefined,
  groups: ReadonlyMap<string, readonly MessageEnvelope[]>,
  context: ExecutionContext,
): Promise<PreparedDelivery[]> {
  const prepared: PreparedDelivery[] = [];
  for (const [mailboxId, envelopes] of groups) {
    const wakeTarget = await prepareExternalEffectAsync(() =>
      Promise.resolve(wakeup?.resolveSessionWakeTarget(mailboxId)),
    );
    prepared.push({ envelopes, deliver(commit) {
      const result = store.enqueue(envelopes, {
        signedEnvelopes: context?.signedEnvelopes, mailboxId, ...(wakeTarget ? { wakeTarget } : {}),
      });
      commit();
      if (result.inserted.length > 0) {
        for (const actualMailbox of result.mailboxIds) wakeup?.notifyMailbox(actualMailbox);
      }
      return { executor: 'inbox', executionId: randomUUID(), ...result };
    } });
  }
  return prepared;
}

export function prepareInboxBatch(
  batch: readonly MessageEnvelope[], context: ExecutionContext, store: MailboxStore,
  options: Pick<InboxExecutorOptions, 'resolveTopicBinding' | 'wakeup'>,
): Promise<PreparedDelivery[]> {
  const groups = prepareExternalEffect(() => {
    const byMailbox = new Map<string, MessageEnvelope[]>();
    for (const envelope of batch) {
      const streamKey = typeof envelope.payload?.streamKey === 'string' ? envelope.payload.streamKey : undefined;
      const mailboxId = resolveMailboxId(
        envelope,
        options.resolveTopicBinding?.(envelope.kind, streamKey),
        store.resolveThreadLocalMailbox(envelope.semantic?.threadId, envelope.networkId, {
          excludeInboundMessageId: envelope.id,
        }),
      );
      const group = byMailbox.get(mailboxId) ?? [];
      group.push(envelope);
      byMailbox.set(mailboxId, group);
    }
    return byMailbox;
  });
  return prepareInboxDeliveries(store, options.wakeup, groups, context);
}

export function createInboxExecutorPlugin(options: InboxExecutorOptions = {}): ExecutorPlugin {
  if (!options.store && !options.inboxPath) {
    throw new Error('Inbox executor requires store or inboxPath');
  }
  const store = options.store ?? new MailboxStore(options.inboxPath!);
  return {
    name: 'inbox',
    execute(batch, context, runtimeConfig) {
      const config = (runtimeConfig ?? {}) as InboxExecutorOptions;
      return serializeThreadDeliveries(store, batch, context, async commit => {
        const groups = await prepareInboxBatch(batch, context, store, {
          wakeup: options.wakeup ?? config.wakeup,
          resolveTopicBinding: options.resolveTopicBinding ?? config.resolveTopicBinding,
        });
        return { ...await executeDeliveryGroups('inbox', batch, groups, commit), count: batch.length };
      });
    },
  };
}
