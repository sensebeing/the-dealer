import { join } from 'node:path';
import { MailboxStore, OutboxStore } from '../mailbox/index.ts';
import { readLarkConsumedSequence } from '../plugins-lark-notifier/index.ts';
import type { LarkNotifyConfig } from '../plugins-lark-notifier/index.ts';

function stateMailboxPaths(stateDir: string, legacyInboxPath = join(stateDir, 'inbox.json')) {
  return { sqlite: join(stateDir, 'mailbox.sqlite'), legacyInboxPath };
}

export function openStateMailboxSnapshot(stateDir: string, legacyInboxPath?: string): MailboxStore | undefined {
  const paths = stateMailboxPaths(stateDir, legacyInboxPath);
  return MailboxStore.openReadOnly(paths.sqlite, paths.legacyInboxPath);
}

/** Wire retention dependencies through lock-free peer snapshots. */
export function openStateLedgers(
  stateDir: string,
  legacyInboxPath?: string,
  readLarkConfig?: () => LarkNotifyConfig | undefined,
) {
  const paths = stateMailboxPaths(stateDir, legacyInboxPath);
  const outbox = new OutboxStore(join(stateDir, 'outbox.json'), join(stateDir, 'outbox-blobs'), {
    readUnsettledOutboundKeys: () => mailbox.readUnsettledOutboundKeys(),
  });
  const mailbox = new MailboxStore(paths.sqlite, {
    legacyInboxPath: paths.legacyInboxPath, outbox, readConsumedSequence: () => readLarkConsumedSequence(stateDir, readLarkConfig?.()),
  });
  return { mailbox, outbox };
}

export function compactQueueSummary(mailbox?: MailboxStore) {
  const { activeMailboxes = 0, queuedItems = 0, leasedItems = 0, pendingWakes = 0, fencedTargets = 0 } =
    mailbox?.summarizeQueues().summary ?? {};
  return { activeMailboxes, queuedItems, leasedItems, pendingWakes, fencedTargets };
}
