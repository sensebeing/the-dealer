import { isValidMailboxId } from '../core-types/index.ts';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { writeFileAtomically } from '../fs-atomic/index.ts';
import type { Disposable, MessageEnvelope, PendingApprovalItem, SessionLinkResolution } from '../core-types/index.ts';
import type { Plugin } from '../kernel/index.ts';
import type { MailboxItem, MailboxStore } from '../mailbox/store.ts';
import { findExecutable, checkedCommand, runCommand } from '../subprocess/index.ts';
import type { CommandRunner } from '../subprocess/index.ts';

declare module '../kernel/index.ts' {
  interface Services { larkNotifier: LarkMailboxNotifier; }
}

interface ThreadRootState {
  messageId: string;
  targetKey: string;
  updatedAt: string;
  mailboxId?: string;
  itemSequence?: number;
  awaitingSessionLink?: boolean;
  lastSessionUrl?: string;
}

interface NotifyState {
  schema: 'the-dealer-lark-notify/v1';
  cursorSequence: number;
  deliveredItems: Record<string, { larkMessageId?: string; deliveredAt: string }>;
  threadRoots: Record<string, ThreadRootState>;
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
  acceptsDefaultItem?: (item: MailboxItem) => boolean;
  resolveSessionLink?: (mailboxId: string, itemSequence?: number) => SessionLinkResolution | undefined;
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
      Object.values(state.threadRoots).some(root =>
        !record(root) || !nonempty(root.messageId) || !nonempty(root.targetKey) || !nonempty(root.updatedAt) ||
        (root.mailboxId !== undefined && !isValidMailboxId(root.mailboxId)) ||
        (root.itemSequence !== undefined && (!Number.isSafeInteger(root.itemSequence) || root.itemSequence < 0)) ||
        (root.awaitingSessionLink !== undefined && typeof root.awaitingSessionLink !== 'boolean') ||
        (root.lastSessionUrl !== undefined && !nonempty(root.lastSessionUrl))
      )) {
    throw new Error('Invalid Lark notifier state');
  }
  return state as unknown as NotifyState;
}

function notifyStatePath(stateDir: string, config: LarkNotifyConfig): string {
  return config.statePath ?? join(stateDir, 'lark-notify.json');
}

/** Read the committed cursor used for mailbox catch-up, including when the daemon is offline. */
export function readLarkConsumedSequence(stateDir: string, config?: LarkNotifyConfig): number | undefined {
  if (!config?.enabled) return undefined;
  const state = readState(notifyStatePath(stateDir, config));
  let consumed = state.cursorSequence;
  for (const root of Object.values(state.threadRoots)) {
    if (root.awaitingSessionLink === true && root.itemSequence !== undefined) {
      consumed = Math.min(consumed, root.itemSequence - 1);
    }
  }
  return consumed;
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
  private mailSubscription?: Disposable;
  private wakeTargetSubscription?: Disposable;
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
    this.statePath = notifyStatePath(options.stateDir, options.config);
    this.state = readState(this.statePath);
    this.boundMailboxes = [...(options.defaultBoundMailboxes ?? [])];
  }
  setDefaultBoundMailboxes(mailboxes: readonly string[]): void {
    this.boundMailboxes = [...mailboxes];
  }
  start(): void {
    if (!this.stopped || !this.options.config.enabled) return;
    this.stopped = false;
    this.controller = new AbortController();
    this.mailSubscription = this.options.store.onMail(() => this.kick());
    this.wakeTargetSubscription = this.options.store.onWakeTarget(() => this.kick());
    this.kick();
  }
  async stop(): Promise<void> {
    this.stopped = true;
    this.mailSubscription?.dispose();
    this.mailSubscription = undefined;
    this.wakeTargetSubscription?.dispose();
    this.wakeTargetSubscription = undefined;
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
          const sessionLink = this.options.resolveSessionLink?.(item.mailboxId, item.sequence);
          await this.deliver(next, key, threadId, followup => this.render(item, followup, sessionLink));
          if (Object.hasOwn(next.threadRoots, threadId)) {
            const root = next.threadRoots[threadId];
            root.mailboxId = item.mailboxId;
            root.itemSequence = item.sequence;
            if (sessionLink?.state === 'ready') {
              root.lastSessionUrl = sessionLink.url;
              delete root.awaitingSessionLink;
            } else if (sessionLink?.state === 'pending') {
              root.awaitingSessionLink = true;
            } else {
              delete root.awaitingSessionLink;
            }
          }
        }
        next.cursorSequence = item.sequence;
        this.persist(next);
        this.state = next;
        this.attempts = 0;
      }
      // Arrivals during an awaited send must be scanned before session-link follow-ups and outbound deduplication.
      if (this.dirty) continue;
      for (const [threadId, root] of Object.entries(this.state.threadRoots)) {
        if (this.stopped) return;
        if (root.awaitingSessionLink !== true || !root.mailboxId) continue;
        const sessionLink = this.options.resolveSessionLink?.(root.mailboxId, root.itemSequence);
        if (sessionLink === undefined || sessionLink.state === 'unavailable') {
          const next = structuredClone(this.state);
          if (Object.hasOwn(next.threadRoots, threadId)) {
            delete next.threadRoots[threadId].awaitingSessionLink;
            this.persist(next);
            this.state = next;
            this.attempts = 0;
          }
          continue;
        }
        if (sessionLink.state !== 'ready') continue;
        const next = structuredClone(this.state);
        const key = `session-link:${threadId}:${sessionLink.url}`;
        if (!Object.hasOwn(next.deliveredItems, key) && root.lastSessionUrl !== sessionLink.url) {
          await this.deliver(next, key, threadId, () => `**[${sessionLink.label}]** Session ready: \`${sessionLink.url}\``);
        }
        if (Object.hasOwn(next.threadRoots, threadId)) {
          next.threadRoots[threadId].mailboxId = root.mailboxId;
          if (root.itemSequence !== undefined) next.threadRoots[threadId].itemSequence = root.itemSequence;
          delete next.threadRoots[threadId].awaitingSessionLink;
          next.threadRoots[threadId].lastSessionUrl = sessionLink.url;
        }
        this.persist(next);
        this.state = next;
        this.attempts = 0;
        if (this.dirty) break;
      }
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
    const allowed = config.mailboxes !== undefined
      ? new Set(config.mailboxes).has(item.mailboxId)
      : this.options.acceptsDefaultItem?.(item) ?? (item.mailboxId === 'default' || this.boundMailboxes.includes(item.mailboxId));
    return allowed && !config.excludeMailboxes?.includes(item.mailboxId) && this.acceptsEnvelope(item.envelope) &&
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
  private render(item: MailboxItem, followup: boolean, sessionLink?: SessionLinkResolution): string {
    const envelope = item.envelope;
    const linkSuffix = sessionLink?.state === 'ready'
      ? `\n\n- ${sessionLink.label}: \`${sessionLink.url}\``
      : '';
    const notification = envelope.payload?.notification;
    if (record(notification)) {
      const personal = record(notification.recipientMarkdown) && Object.hasOwn(notification.recipientMarkdown, this.options.localMember)
        ? notification.recipientMarkdown[this.options.localMember] : undefined;
      if (nonempty(personal)) return personal + linkSuffix;
      if (nonempty(notification.markdown)) return notification.markdown + linkSuffix;
    }
    const type = envelope.semantic?.type ?? 'message';
    const threadId = envelope.semantic?.threadId ?? envelope.id;
    const markdown = followup
      ? `**[${type}] ${envelope.from.member}**\n\n${envelope.body}`
      : `**[${envelope.kind}] ${envelope.subject}**${type !== 'message' ? `\n- Type: \`${type}\`` : ''}\n- From: \`${envelope.from.member}\` (\`${envelope.from.nodeName}\`)\n- Mailbox: \`${item.mailboxId}\` | Priority: \`${envelope.priority}\`\n- Thread: \`${threadId}\`\n\n${envelope.body}`;
    return markdown + this.attachments(envelope) + linkSuffix;
  }
  private persist(state: NotifyState): void {
    state.deliveredItems = prune(state.deliveredItems, item => item.deliveredAt);
    state.threadRoots = prune(state.threadRoots, item => item.updatedAt);
    writeFileAtomically(this.statePath, `${JSON.stringify(state, null, 2)}\n`);
  }
}

/** Human Copilot Mirror: observes admission and delivery without owning either decision. */
export function createLarkMirrorPlugin(options: LarkMailboxNotifierOptions & { notifier?: LarkMailboxNotifier }): Plugin {
  return {
    name: 'lark-mirror',
    inject: ['gate'],
    apply(ctx) {
      const notifier = options.notifier ?? new LarkMailboxNotifier(options);
      ctx.provide('larkNotifier', notifier);
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

export interface LarkNotifyConfig {
  enabled: boolean;
  command?: string;
  profile?: string;
  as?: 'bot' | 'user';
  target?: { chatId?: string; userId?: string };
  mailboxes?: string[];
  excludeMailboxes?: string[];
  includeKinds?: string[];
  excludeKinds?: string[];
  skipSelfMessages?: boolean;
  notifyGateApprovals?: boolean;
  mirrorOutboundThreads?: boolean;
  statePath?: string;
  retryBaseMs?: number;
}

export function validateLarkNotify(config: unknown): asserts config is LarkNotifyConfig {
  const strings = ['command', 'profile', 'statePath'];
  const arrays = ['mailboxes', 'excludeMailboxes', 'includeKinds', 'excludeKinds'];
  const booleans = ['skipSelfMessages', 'notifyGateApprovals', 'mirrorOutboundThreads'];
  const allowed = new Set(['enabled', 'as', 'target', 'retryBaseMs', ...booleans, ...strings, ...arrays]);
  if (!record(config) || typeof config.enabled !== 'boolean' ||
      Object.keys(config).some(key => !allowed.has(key)) ||
      strings.some(key => config[key] !== undefined && !nonempty(config[key])) ||
      arrays.some(key => config[key] !== undefined && (!Array.isArray(config[key]) || config[key].some((value: unknown) => !nonempty(value)))) ||
      (config.as !== undefined && !['bot', 'user'].includes(config.as)) ||
      booleans.some(key => config[key] !== undefined && typeof config[key] !== 'boolean') ||
      (config.retryBaseMs !== undefined && (!Number.isSafeInteger(config.retryBaseMs) || config.retryBaseMs < 1))) {
    throw new Error('Invalid larkNotify');
  }
  if (config.target !== undefined && (!record(config.target) ||
      Object.keys(config.target).some(key => !['chatId', 'userId'].includes(key)) ||
      ['chatId', 'userId'].some(key => config.target[key] !== undefined && !nonempty(config.target[key])) ||
      (config.target.chatId !== undefined && config.target.userId !== undefined))) {
    throw new Error('Invalid larkNotify target');
  }
  for (const key of ['mailboxes', 'excludeMailboxes']) {
    if (config[key]?.some((id: string) => !isValidMailboxId(id))) throw new Error('Invalid notification mailbox');
  }
}
