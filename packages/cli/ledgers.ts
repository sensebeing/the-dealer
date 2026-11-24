import { join } from 'node:path';
import { MailboxStore, OutboxStore } from '../mailbox/index.ts';
import { readLarkConsumedSequence } from '../plugins-lark-notifier/index.ts';
import type { LarkNotifyConfig } from '../plugins-lark-notifier/index.ts';

/** Wire retention dependencies through lock-free peer snapshots. */
export function openStateLedgers(
  stateDir: string,
  legacyInboxPath = join(stateDir, 'inbox.json'),
  readLarkConfig?: () => LarkNotifyConfig | undefined,
) {
  const outbox = new OutboxStore(join(stateDir, 'outbox.json'), join(stateDir, 'outbox-blobs'), {
    readUnsettledOutboundKeys: () => mailbox.readUnsettledOutboundKeys(),
  });
  const mailbox = new MailboxStore(join(stateDir, 'mailbox.json'), {
    legacyInboxPath, outbox, readConsumedSequence: () => readLarkConsumedSequence(stateDir, readLarkConfig?.()),
  });
  return { mailbox, outbox };
}
