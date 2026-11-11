import { createHash, randomUUID } from 'node:crypto';
import { closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import type { Disposable, MessageEnvelope, PendingApprovalItem } from '../core-types/index.ts';
import type { Plugin } from '../kernel/index.ts';
import type { MailboxItem, MailboxStore } from '../mailbox/store.ts';
import { findExecutable } from '../orca-workspace/executable.ts';
import { checkedCommand, runCommand } from '../orca-workspace/index.ts';
import type { CommandRunner } from '../orca-workspace/index.ts';
import type { LarkNotifyConfig } from '../cli/config.ts';
import { validateLarkNotify } from '../cli/config.ts';

interface NotifyState {
  schema: 'the-dealer-lark-notify/v1';
  cursorSequence: number;
  deliveredItems: Record<string, { larkMessageId?: string; deliveredAt: string }>;
  threadRoots: Record<string, { messageId: string; targetKey: string; updatedAt: string }>;
}
interface Target {
  flag: '--chat-id' | '--user-id';
  value: string;
  as?: 'bot' | 'user';
  key: string;
}
export interface LarkMailboxNotifierOptions {
  store: MailboxStore;
  config: LarkNotifyConfig;
  stateDir: string;
  localNodeId: string;
  localMember: string;
  defaultBoundMailboxes?: string[];
  commandRunner?: CommandRunner;
  onError?: (error: unknown) => void;
}

function record(value: unknown): value is Record<string, any> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function nonempty(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}
function readState(path: string): NotifyState {
  if (!existsSync(path)) return { schema: 'the-dealer-lark-notify/v1', cursorSequence: -1, deliveredItems: {}, threadRoots: {} };
  const state = JSON.parse(readFileSync(path, 'utf8'));
  if (!record(state) || state.schema !== 'the-dealer-lark-notify/v1' || !Number.isSafeInteger(state.cursorSequence) || state.cursorSequence < -1 ||
      !record(state.deliveredItems) || !record(state.threadRoots) ||
      Object.values(state.deliveredItems).some(item => !record(item) || !nonempty(item.deliveredAt) || (item.larkMessageId !== undefined && !nonempty(item.larkMessageId))) ||
      Object.values(state.threadRoots).some(root => !record(root) || !nonempty(root.messageId) || !nonempty(root.targetKey) || !nonempty(root.updatedAt))) {
    throw new Error('Invalid Lark notifier state');
  }
  return state as unknown as NotifyState;
}
function prune<T>(entries: Record<string, T>, timestamp: (value: T) => string): Record<string, T> {
  return Object.fromEntries(Object.entries(entries).reverse().sort((a, b) => timestamp(b[1]).localeCompare(timestamp(a[1]))).slice(0, 1024));
}

/** One serialized mirror consumer for mailbox arrivals, gate knocks, and outbound thread events. */
export class LarkMailboxNotifier {
  private readonly options: LarkMailboxNotifierOptions;
  private readonly statePath: string;
  private state: NotifyState;
  private boundMailboxes: string[];
  private subscription?: Disposable;
  private active?: Promise<void>;
  private retry?: ReturnType<typeof setTimeout>;
  private attempts = 0;
  private stopped = true;
  private dirty = false;
  private target?: Target;
  private pendingApprovals = new Map<string, PendingApprovalItem>();
  private pendingOutbound = new Map<string, MessageEnvelope>();
  private controller = new AbortController();

  constructor(options: LarkMailboxNotifierOptions) {
    validateLarkNotify(options.config);
    this.options = { ...options, config: structuredClone(options.config) };
    this.statePath = options.config.statePath ?? join(options.stateDir, 'lark-notify.json');
    this.state = readState(this.statePath);
    this.boundMailboxes = [...(options.defaultBoundMailboxes ?? [])];
  }
  setDefaultBoundMailboxes(mailboxes: string[]): void {
    this.boundMailboxes = [...mailboxes];
  }
  start(): void {
    if (!this.stopped || !this.options.config.enabled) return;
    this.stopped = false;
    this.controller = new AbortController();
    this.subscription = this.options.store.onMail(() => this.kick());
    this.kick();
  }
  async stop(): Promise<void> {
    this.stopped = true;
    this.subscription?.dispose();
    this.subscription = undefined;
    if (this.retry) clearTimeout(this.retry);
    this.retry = undefined;
    this.controller.abort();
    await this.active;
  }
  /** Await the current event-driven catch-up, without forcing retries. */
  async flush(): Promise<void> {
    while (this.active) await this.active;
  }
  notifyApproval(item: PendingApprovalItem): void {
    if (this.options.config.notifyGateApprovals === false || !this.acceptsEnvelope(item.envelope) ||
        (this.options.config.skipSelfMessages && item.envelope.from.nodeId === this.options.localNodeId)) return;
    this.pendingApprovals.set(item.id, structuredClone(item));
    this.kick();
  }
  notifyOutbound(envelope: MessageEnvelope, _targetNodeIds?: readonly string[]): void {
    if (this.options.config.mirrorOutboundThreads === false || envelope.from.nodeId !== this.options.localNodeId ||
        !this.acceptsEnvelope(envelope)) return;
    this.pendingOutbound.set(envelope.id, structuredClone(envelope));
    this.kick();
  }
  private kick(): void {
    if (this.stopped) return;
    this.dirty = true;
    if (this.active || this.retry) return;
    this.active = this.drain().catch(error => {
      if (this.stopped) return;
      try { this.options.onError?.(error); } catch { /* Reporting must not break retry ownership. */ }
      const delay = Math.min(60_000, (this.options.config.retryBaseMs ?? 2000) * 2 ** Math.min(this.attempts++, 16));
      this.retry = setTimeout(() => {
        this.retry = undefined;
        this.kick();
      }, delay);
      this.retry.unref();
    }).finally(() => {
      this.active = undefined;
      if (this.dirty && !this.retry && !this.stopped) this.kick();
    });
  }
  private async drain(): Promise<void> {
    do {
      this.dirty = false;
      for (const item of this.pendingApprovals.values()) {
        if (this.stopped) return;
        const key = `approval:${item.id}`;
        if (!Object.hasOwn(this.state.deliveredItems, key)) {
          const next = structuredClone(this.state);
          const envelope = item.envelope;
          const threadId = envelope.semantic?.threadId ?? envelope.id;
          const markdown = `**[gate · pending_approval] ${envelope.subject}**\n- Approval ID: \`${item.id}\`\n- From: \`${envelope.from.member}\` (\`${envelope.from.nodeName}\`)\n- Topic: \`${envelope.kind}\` | Priority: \`${envelope.priority}\`\n- Thread: \`${threadId}\`\n- Approve: \`dealer gate approve ${item.id} --json\`\n- Trust member: \`dealer gate approve ${item.id} --trust-sender --json\`\n\n${envelope.body}${this.attachments(envelope)}`;
          await this.deliver(next, key, threadId, () => markdown);
          this.persist(next);
          this.state = next;
          this.attempts = 0;
        }
        this.pendingApprovals.delete(item.id);
      }
      for (const item of this.options.store.listItemsAfterSequence(this.state.cursorSequence)) {
        if (this.stopped) return;
        const key = `${item.mailboxId}:${item.id}`;
        const next = structuredClone(this.state);
        if (this.accepts(item) && !Object.hasOwn(next.deliveredItems, key)) {
          const threadId = item.envelope.semantic?.threadId ?? item.id;
          await this.deliver(next, key, threadId, followup => this.render(item, followup));
        }
        next.cursorSequence = item.sequence;
        this.persist(next);
        this.state = next;
        this.attempts = 0;
      }
      // Arrivals during an awaited send must be scanned before outbound deduplication.
      if (this.dirty) continue;
      for (const envelope of this.pendingOutbound.values()) {
        if (this.stopped) return;
        const key = `outbound:${envelope.id}`;
        const threadId = envelope.semantic?.threadId ?? envelope.id;
        const next = structuredClone(this.state);
        if (Object.hasOwn(next.threadRoots, threadId) && !Object.hasOwn(next.deliveredItems, key) &&
            !Object.keys(next.deliveredItems).some(delivered => delivered.endsWith(`:${envelope.id}`))) {
          const markdown = `**[${envelope.semantic?.type ?? 'message'} · outbound] ${envelope.from.member}**\n\n${envelope.body}${this.attachments(envelope)}`;
          await this.deliver(next, key, threadId, () => markdown);
          this.persist(next);
          this.state = next;
          this.attempts = 0;
        }
        this.pendingOutbound.delete(envelope.id);
        if (this.dirty) break;
      }
    } while (this.dirty && !this.stopped);
  }
  private async deliver(next: NotifyState, key: string, threadId: string, render: (followup: boolean) => string): Promise<void> {
    const target = await this.resolveTarget();
    const root = Object.hasOwn(next.threadRoots, threadId) ? next.threadRoots[threadId] : undefined;
    const matching = root?.targetKey === target.key ? root : undefined;
    const hash = createHash('sha256').update(`${target.key}:${key}`).digest('hex');
    let messageId: string | undefined;
    let newRoot = !matching;
    try {
      messageId = await this.send(target, render(Boolean(matching)), `dlr-${hash.slice(0, 40)}`, matching?.messageId);
    } catch (error) {
      if (!matching || !/(?:message|root).*(?:not found|deleted|withdrawn|not exist)|(?:not found|deleted|withdrawn).*(?:message|root)/i.test(String(error))) throw error;
      messageId = await this.send(target, render(false), `dlr-r-${hash.slice(0, 38)}`);
      newRoot = true;
    }
    const now = new Date().toISOString();
    if (newRoot && !messageId) delete next.threadRoots[threadId];
    if (matching && !newRoot) next.threadRoots[threadId].updatedAt = now;
    if (newRoot && messageId) {
      Object.defineProperty(next.threadRoots, threadId, { value: { messageId, targetKey: target.key, updatedAt: now }, enumerable: true, writable: true, configurable: true });
    }
    Object.defineProperty(next.deliveredItems, key, { value: { ...(messageId ? { larkMessageId: messageId } : {}), deliveredAt: now }, enumerable: true, writable: true, configurable: true });
  }
  private acceptsEnvelope(envelope: MessageEnvelope): boolean {
    const config = this.options.config;
    return (!config.includeKinds || config.includeKinds.includes(envelope.kind)) && !config.excludeKinds?.includes(envelope.kind);
  }
  private accepts(item: MailboxItem): boolean {
    const config = this.options.config;
    const allowed = new Set(config.mailboxes ?? ['default', ...this.boundMailboxes]);
    return allowed.has(item.mailboxId) && !config.excludeMailboxes?.includes(item.mailboxId) && this.acceptsEnvelope(item.envelope) &&
      !(config.skipSelfMessages && item.envelope.from.nodeId === this.options.localNodeId);
  }
  private attachments(envelope: MessageEnvelope): string {
    return envelope.attachments.length ? `\n\nAttachments:\n${envelope.attachments.map(file => `- ${file.name}`).join('\n')}` : '';
  }
  private async invoke(args: string[]): Promise<any> {
    const config = this.options.config;
    const command = config.command ?? ((!this.options.commandRunner || this.options.commandRunner === runCommand) ? findExecutable('lark-cli') ?? 'lark-cli' : 'lark-cli');
    const result = await checkedCommand(this.options.commandRunner ?? runCommand, {
      command, args: [...(config.profile ? ['--profile', config.profile] : []), ...args], signal: this.controller.signal,
    });
    const parsed = JSON.parse(result.stdout.trim());
    if (parsed?.ok === false || (typeof parsed?.code === 'number' && parsed.code !== 0)) throw new Error(`Lark notification rejected: ${JSON.stringify(parsed)}`);
    return parsed;
  }
  private async resolveTarget(): Promise<Target> {
    if (this.target) return this.target;
    const config = this.options.config;
    let flag: Target['flag'] = '--user-id';
    let value = config.target?.userId;
    let as = config.as;
    if (config.target?.chatId) {
      flag = '--chat-id';
      value = config.target.chatId;
    } else if (!value) {
      const auth = await this.invoke(['auth', 'status']);
      value = auth.identities?.user?.openId;
      if (!nonempty(value)) throw new Error('Lark auth status has no user openId; configure a target');
      as ??= auth.identities?.bot?.available === true ? 'bot' : 'user';
    }
    this.target = { flag, value: value!, as, key: JSON.stringify([config.profile ?? '', as ?? '', flag, value]) };
    return this.target;
  }
  private async send(target: Target, markdown: string, key: string, root?: string): Promise<string | undefined> {
    const result = await this.invoke(['im', root ? '+messages-reply' : '+messages-send',
      ...(target.as ? ['--as', target.as] : []),
      ...(root ? ['--message-id', root, '--reply-in-thread'] : [target.flag, target.value]),
      '--markdown', markdown, '--idempotency-key', key, '--format', 'json']);
    const id = result.data?.message_id ?? result.data?.messageId ?? result.message_id ?? result.result?.message_id;
    return nonempty(id) ? id : undefined;
  }
  private render(item: MailboxItem, followup: boolean): string {
    const envelope = item.envelope;
    const notification = envelope.payload?.notification;
    if (record(notification)) {
      const personal = record(notification.recipientMarkdown) && Object.hasOwn(notification.recipientMarkdown, this.options.localMember)
        ? notification.recipientMarkdown[this.options.localMember] : undefined;
      if (nonempty(personal)) return personal;
      if (nonempty(notification.markdown)) return notification.markdown;
    }
    const type = envelope.semantic?.type ?? 'message';
    const threadId = envelope.semantic?.threadId ?? envelope.id;
    const markdown = followup
      ? `**[${type}] ${envelope.from.member}**\n\n${envelope.body}`
      : `**[${envelope.kind}] ${envelope.subject}**${type !== 'message' ? `\n- Type: \`${type}\`` : ''}\n- From: \`${envelope.from.member}\` (\`${envelope.from.nodeName}\`)\n- Mailbox: \`${item.mailboxId}\` | Priority: \`${envelope.priority}\`\n- Thread: \`${threadId}\`\n\n${envelope.body}`;
    return markdown + this.attachments(envelope);
  }
  private persist(state: NotifyState): void {
    state.deliveredItems = prune(state.deliveredItems, item => item.deliveredAt);
    state.threadRoots = prune(state.threadRoots, item => item.updatedAt);
    mkdirSync(dirname(this.statePath), { recursive: true, mode: 0o700 });
    const temporary = `${this.statePath}.${randomUUID()}.tmp`;
    const fd = openSync(temporary, 'wx', 0o600);
    try {
      writeFileSync(fd, `${JSON.stringify(state, null, 2)}\n`);
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    try {
      renameSync(temporary, this.statePath);
    } finally {
      rmSync(temporary, { force: true });
    }
  }
}

/** Human Copilot Mirror: observes admission and delivery without owning either decision. */
export function createLarkMirrorPlugin(options: LarkMailboxNotifierOptions & { notifier?: LarkMailboxNotifier }): Plugin {
  return {
    name: 'lark-mirror',
    inject: ['gate'],
    apply(ctx) {
      const notifier = options.notifier ?? new LarkMailboxNotifier(options);
      ctx.on('gate/pending-approval', item => notifier.notifyApproval(item));
      ctx.on('emitter/sent', ({ envelope, targetNodeIds }) => notifier.notifyOutbound(envelope, targetNodeIds));
      for (const item of ctx.services.gate?.listPendingApprovals?.() ?? []) notifier.notifyApproval(item);
      ctx.effect(() => {
        notifier.start();
        return () => notifier.stop();
      });
    },
  };
}
