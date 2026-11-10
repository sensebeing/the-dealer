import { randomBytes } from 'node:crypto';
import { probeDealerSetup, buildOnboardingPrompts, buildAgentBindingPreset } from './setup.ts';
import type { SupportedAgentHost, AgentConversationMode } from '../plugins-executor/index.ts';
import type { SupportedOrcaAgent } from '../orca-workspace/index.ts';
import { existsSync, readFileSync } from 'node:fs';
import { resolve, join, basename } from 'node:path';
import { request } from 'node:http';
import type { ClientRequest } from 'node:http';
import { spawn } from 'node:child_process';
import { parseArgs } from 'node:util';
import { MailboxStore } from '../mailbox/index.ts';
import type { ResolvedReplyTarget } from '../mailbox/index.ts';
import type { SettlementOutcome } from '../core-types/index.ts';
import { DealerNodeRuntime, defaultStateDir } from './runtime.ts';
import { defaultAdvertisedHost } from '../transport/index.ts';
import { admissionMode, readPatch, encodeNetworkInvite, decodeNetworkInvite, initProjectScaffold } from './config.ts';

export * from './runtime.ts';
export * from './config.ts';
export * from './setup.ts';

const booleanFlags = ['json', 'auto-route', 'trust-sender', 'all', 'peek', 'wait'] as const;
const stringFlags = [
  'host', 'mode', 'workspace-path', 'agent', 'agent-profile', 'target-bot', 'session-id', 'peer-member',
  'thread',
  'state',
  'limit',
  'root',
  'id',
  'project',
  'user-patch',
  'module',
  'description',
  'profile',
  'patch',
  'member',
  'name',
  'skills',
  'port',
  'network',
  'passphrase',
  'role',
  'gate',
  'seed',
  'ttl',
  'skill',
  'status',
  'reason',
  'to',
  'topic',
  'body',
  'urgency',
  'Urgency',
  'type',
  'config',
  'command',
  'endpoint',
  'model',
  'url',
  'cwd',
  'args',
  'interval-ms',
  'token',
  'path',
  'mailbox',
  'ack',
  'timeout-ms',
  'outcome',
  'summary',
  'reply-to',
  'reply-mailbox',
  'target-mailbox',
  'subject',
] as const;

function parse(argv: string[]) {
  return parseArgs({
    args: argv,
    allowPositionals: true,
    strict: true,
    options: {
      ...Object.fromEntries(booleanFlags.map(k => [k, { type: 'boolean' as const }])) as Record<typeof booleanFlags[number], { type: 'boolean' }>,
      ...Object.fromEntries(stringFlags.map(k => [k, { type: 'string' as const }])) as Record<typeof stringFlags[number], { type: 'string' }>,
      attach: { type: 'string', multiple: true },
    },
  });
}

function required(value: unknown, name: string): string {
  if (typeof value !== 'string' || !value) {
    throw new Error(`Required: ${name}`);
  }
  return value;
}

function duration(value = '24h'): number {
  const match = /^(\d+)(ms|s|m|h|d)$/.exec(value);
  if (!match) {
    throw new Error('TTL must be a positive duration such as 30m or 24h');
  }
  const units: Record<string, number> = {
    ms: 1,
    s: 1000,
    m: 60000,
    h: 3600000,
    d: 86400000,
  };
  return Number(match[1]) * units[match[2]]!;
}

function runtimeOptions(values: Record<string, any>, stateDir: string) {
  return {
    stateDir,
    profile: values.profile,
    member: values.member,
    name: values.name,
    skills: values.skills?.split(',').filter(Boolean),
    port: values.port === undefined ? undefined : Number(values.port),
    patch: values.patch ? readPatch(values.patch) : undefined,
  };
}

export interface DealerCliOptions {
  stateDir?: string;
  runtime?: DealerNodeRuntime;
  stdout?: (line: string) => void;
  stderr?: (line: string) => void;
  /** Aborted when the calling control request disconnects, so long waits release listeners. */
  signal?: AbortSignal;
}

export interface ControlRpcOptions {
  /** Inactivity window reset by every NDJSON line; defaults to 45000ms. */
  inactivityMs?: number;
}

const DEFAULT_RPC_INACTIVITY_MS = 45_000;

/**
 * POST /cli-stream keeps normal commands fast while allowing long waits: the server emits an
 * NDJSON keepalive every 15s, and every line resets the client inactivity window.
 */
export async function rpc(
  socketPath: string,
  argv: string[],
  options: ControlRpcOptions = {},
): Promise<{ code: number; stdout: string[]; stderr: string[] }> {
  const inactivityMs = options.inactivityMs ?? DEFAULT_RPC_INACTIVITY_MS;
  if (!Number.isSafeInteger(inactivityMs) || inactivityMs <= 0) {
    throw new Error('Inactivity timeout must be a positive integer');
  }
  return new Promise((resolve, reject) => {
    let settled = false;
    let buffer = '';
    let timer: ReturnType<typeof setTimeout> | undefined;
    let req: ClientRequest | undefined;
    const finish = (
      error?: Error,
      value?: { code: number; stdout: string[]; stderr: string[] },
    ) => {
      if (settled) {
        return;
      }
      settled = true;
      if (timer !== undefined) {
        clearTimeout(timer);
      }
      if (error) {
        req?.destroy();
        reject(error);
      } else {
        resolve(value!);
      }
    };
    const arm = () => {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
      timer = setTimeout(
        () => finish(new Error('Daemon control request timed out')),
        inactivityMs,
      );
    };
    req = request(
      {
        socketPath,
        path: '/cli-stream',
        method: 'POST',
        headers: { 'content-type': 'application/json' },
      },
      res => {
        if (res.statusCode !== 200) {
          let body = '';
          res.setEncoding('utf8');
          res.on('data', chunk => {
            body += chunk;
          });
          res.on('end', () =>
            finish(new Error(body || `Daemon control HTTP ${res.statusCode}`)),
          );
          return;
        }
        res.setEncoding('utf8');
        arm();
        res.on('data', chunk => {
          arm();
          buffer += chunk;
          let newline = buffer.indexOf('\n');
          while (newline !== -1) {
            const line = buffer.slice(0, newline);
            buffer = buffer.slice(newline + 1);
            newline = buffer.indexOf('\n');
            if (!line.trim()) {
              continue;
            }
            let message: Record<string, unknown>;
            try {
              message = JSON.parse(line) as Record<string, unknown>;
            } catch {
              finish(new Error(`Invalid control stream line: ${line}`));
              return;
            }
            if (message.type === 'keepalive') {
              arm();
              continue;
            }
            if (message.type === 'result') {
              finish(
                undefined,
                message.result as { code: number; stdout: string[]; stderr: string[] },
              );
              return;
            }
            if (message.type === 'error') {
              finish(new Error(String(message.error)));
              return;
            }
            finish(new Error('Unexpected control stream message'));
            return;
          }
        });
        res.on('end', () =>
          finish(
            new Error(
              buffer.trim()
                ? `Incomplete control stream response: ${buffer.trim()}`
                : 'Daemon closed the control stream without a result',
            ),
          ),
        );
        res.on('error', error => finish(error));
      },
    );
    req.on('error', error => finish(error));
    arm();
    req.end(JSON.stringify(argv));
  });
}

async function launchDaemon(argv: string[], stateDir: string): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      ['--experimental-strip-types', new URL('../../bin/dealerd.js', import.meta.url).pathname, ...argv],
      {
        detached: true,
        stdio: ['ignore', 'pipe', 'pipe'],
        env: { ...process.env, DEALER_STATE_DIR: stateDir },
      },
    );
    let output = '';
    let errors = '';
    let settled = false;
    const timer = setTimeout(() => {
      child.kill();
      finish(new Error('Daemon startup timed out'));
    }, 30000);
    function finish(error?: Error, value?: unknown) {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      if (error) {
        reject(error);
      } else {
        child.stdout.destroy();
        child.stderr.destroy();
        child.unref();
        resolve(value);
      }
    }
    child.on('error', error => finish(error));
    child.on('exit', code => finish(new Error(`Daemon exited ${code}: ${errors}`)));
    child.stderr.on('data', chunk => {
      errors += chunk;
    });
    child.stdout.on('data', chunk => {
      output += chunk;
      if (output.includes('\n')) {
        try {
          finish(undefined, JSON.parse(output.split('\n')[0]));
        } catch (error) {
          finish(error as Error);
        }
      }
    });
  });
}

function positiveInteger(value: unknown, flag: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== 'string' || !/^[1-9]\d*$/.test(value)) {
    throw new Error(`${flag} must be a positive integer`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${flag} must be a positive integer`);
  }
  return parsed;
}

async function runInboxCommand(
  store: MailboxStore,
  action: string | undefined,
  target: string | undefined,
  v: Record<string, any>,
  signal?: AbortSignal,
) {
  const command = action ?? 'list';
  const mailboxId = v.mailbox as string | undefined;
  const checkOnlyFlags =
    v.ack !== undefined || v.peek === true || v.wait === true || v['timeout-ms'] !== undefined;
  if (command === 'check') {
    const timeoutMs = positiveInteger(v['timeout-ms'], '--timeout-ms');
    const checkOptions = {
      ...(mailboxId !== undefined ? { mailboxId } : {}),
      ...(v.ack !== undefined ? { ackLeaseId: String(v.ack) } : {}),
      ...(v.peek === true ? { peek: true } : {}),
    };
    if (v.wait === true) {
      return store.waitForMail({
        ...checkOptions,
        ...(timeoutMs !== undefined ? { timeoutMs } : {}),
        ...(signal !== undefined ? { signal } : {}),
      });
    }
    if (timeoutMs !== undefined) {
      throw new Error('--timeout-ms requires --wait');
    }
    return store.check(checkOptions);
  }
  if (command === 'ack') {
    if (checkOnlyFlags) {
      throw new Error('--ack, --peek, --wait and --timeout-ms are only valid with inbox check');
    }
    if (v.all === true) {
      throw new Error('--all is only valid with inbox list');
    }
    return store.ack(required(target, 'delivery-id'), mailboxId);
  }
  if (command === 'list') {
    if (checkOnlyFlags) {
      throw new Error('--ack, --peek, --wait and --timeout-ms are only valid with inbox check');
    }
    const limit = positiveInteger(v.limit, '--limit');
    return store.list({
      ...(mailboxId !== undefined ? { mailboxId } : {}),
      ...(v.all === true ? { includeAcked: true } : {}),
      ...(limit !== undefined ? { limit } : {}),
    });
  }
  throw new Error(`Unknown inbox command: ${String(action)}`);
}

function payloadFromBody(body: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(body);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
      ? parsed
      : { value: parsed };
  } catch {
    return {};
  }
}

function offlineMailboxStore(stateDir: string): MailboxStore {
  const file = join(stateDir, 'config.json');
  const savedConfig = existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')) : {};
  return new MailboxStore(join(stateDir, 'mailbox.json'), {
    legacyInboxPath:
      savedConfig.executorConfig?.inbox?.inboxPath ?? join(stateDir, 'inbox.json'),
  });
}

async function mailboxStoreFor(
  options: DealerCliOptions,
  stateDir: string,
): Promise<MailboxStore> {
  if (options.runtime) {
    await options.runtime.start();
    return options.runtime.mailbox;
  }
  return offlineMailboxStore(stateDir);
}

function replyTargetOrThrow(store: MailboxStore, messageId: string): ResolvedReplyTarget {
  const route = store.resolveReplyTarget(messageId);
  if (!route) {
    throw new Error(`Unknown message or thread: ${messageId}`);
  }
  if (!route.peerNodeId) {
    throw new Error(`No reply route for: ${messageId}`);
  }
  return route;
}

function settlementOutcome(value: unknown): SettlementOutcome {
  if (value === 'succeeded' || value === 'failed' || value === 'cancelled') {
    return value;
  }
  throw new Error('Required: --outcome succeeded|failed|cancelled');
}

function runThreadCommand(
  store: MailboxStore,
  action: string | undefined,
  target: string | undefined,
  v: Record<string, any>,
) {
  if (action === 'list') {
    return store.listThreads({ kind: v.topic, state: v.state, limit: v.limit === undefined ? undefined : Number(v.limit) });
  }
  if (action !== 'show') {
    throw new Error('Unknown thread command; use thread show <id>');
  }
  const id = required(target, 'thread-id');
  const records = store.listThreadRecords(id);
  if (!records.length) {
    throw new Error(`Unknown thread: ${id}`);
  }
  const rank: Record<'open' | 'replied' | 'settled', number> = {
    open: 0,
    replied: 1,
    settled: 2,
  };
  let state: 'open' | 'replied' | 'settled' = 'open';
  for (const record of records) {
    if (rank[record.state] > rank[state]) {
      state = record.state;
    }
  }
  const settlement = records.filter(record => record.settlement !== undefined).at(-1)
    ?.settlement;
  return {
    threadId: records[0].threadId,
    state,
    ...(settlement ? { settlement } : {}),
    records,
  };
}

/**
 * Non-blocking session control: ask/reply/settle all return as soon as the signed envelope is
 * dispatched. Replies wake the caller's mailbox through the durable wake target, not a wait loop.
 */
async function runSessionCommand(
  runtime: DealerNodeRuntime,
  group: 'ask' | 'reply' | 'settle',
  positional: string | undefined,
  v: Record<string, any>,
) {
  const store = runtime.mailbox;
  const callerMailboxFlag =
    typeof v.mailbox === 'string' && v.mailbox ? String(v.mailbox) : undefined;
  const ackId = typeof v.ack === 'string' && v.ack ? String(v.ack) : undefined;

  if (group === 'ask') {
    const replyTo = typeof v['reply-to'] === 'string' && v['reply-to']
      ? String(v['reply-to'])
      : undefined;
    if (replyTo !== undefined) {
      const route = replyTargetOrThrow(store, replyTo);
      if (ackId) {
        store.ack(ackId, callerMailboxFlag ?? route.localMailbox);
      }
      const body = required(v.body, '--body');
      const replyMailbox = callerMailboxFlag ?? route.localMailbox;
      const targetMailbox =
        typeof v['target-mailbox'] === 'string' && v['target-mailbox']
          ? String(v['target-mailbox'])
          : route.targetMailbox;
      const emitted = await runtime.emitter.emit({
        networkId: typeof v.network === 'string' && v.network ? String(v.network) : route.networkId,
        kind: typeof v.topic === 'string' && v.topic ? String(v.topic) : route.kind,
        subject:
          typeof v.subject === 'string' && v.subject
            ? String(v.subject)
            : `Question: ${route.subject}`,
        body,
        payload: {},
        replyToId: replyTo,
        traceId: route.traceId,
        routing: { mode: 'unicast', targets: [route.peerNodeId], topic: route.kind },
        semantic: {
          type: 'question',
          threadId: route.threadId,
          targetMailbox,
          replyMailbox,
        },
        filePaths: v.attach as string[] | undefined,
      });
      return {
        questionId: emitted.messageId,
        threadId: route.threadId,
        targetNodeId: route.peerNodeId,
        targetMailbox,
        replyMailbox,
        receipts: emitted.receipts,
        next: 'End your turn now; Dealer will wake this mailbox when the reply arrives.',
      };
    }
    const networkId = required(v.network, '--network');
    const topic = required(v.topic, '--topic');
    const body = required(v.body, '--body');
    if (Boolean(v.to) === Boolean(v['auto-route'])) {
      throw new Error('Choose exactly one of --to or --auto-route');
    }
    if (ackId) {
      store.ack(ackId, callerMailboxFlag ?? 'default');
    }
    const targetMailbox =
      typeof v['target-mailbox'] === 'string' && v['target-mailbox']
        ? String(v['target-mailbox'])
        : undefined;
    const emitted = await runtime.emitter.emit({
      networkId,
      kind: topic,
      subject:
        typeof v.subject === 'string' && v.subject ? String(v.subject) : `Question: ${topic}`,
      body,
      payload: {},
      routing: {
        mode: v['auto-route'] ? 'auto' : 'unicast',
        ...(v.to ? { targets: [String(v.to)] } : {}),
        topic,
      },
      semantic: {
        type: 'question',
        ...(targetMailbox ? { targetMailbox } : {}),
        ...(callerMailboxFlag ? { replyMailbox: callerMailboxFlag } : {}),
      },
      filePaths: v.attach as string[] | undefined,
    });
    return {
      questionId: emitted.messageId,
      threadId: emitted.messageId,
      targetNodeId: emitted.receipts[0]?.fromNodeId,
      targetMailbox: targetMailbox ?? 'default',
      replyMailbox: callerMailboxFlag ?? 'default',
      receipts: emitted.receipts,
      next: 'End your turn now; Dealer will wake this mailbox when the reply arrives.',
    };
  }

  const messageId = required(positional, 'message-id');
  const route = replyTargetOrThrow(store, messageId);
  if (ackId) {
    store.ack(ackId, callerMailboxFlag ?? route.localMailbox);
  }
  const replyMailbox = callerMailboxFlag ?? route.localMailbox;

  if (group === 'reply') {
    const body = required(v.body, '--body');
    const emitted = await runtime.emitter.emit({
      networkId: route.networkId,
      kind: route.kind,
      subject:
        typeof v.subject === 'string' && v.subject
          ? String(v.subject)
          : `Re: ${route.subject}`,
      body,
      payload: {},
      replyToId: messageId,
      traceId: route.traceId,
      routing: { mode: 'unicast', targets: [route.peerNodeId], topic: route.kind },
      semantic: {
        type: 'reply',
        threadId: route.threadId,
        targetMailbox: route.targetMailbox,
        replyMailbox,
      },
      filePaths: v.attach as string[] | undefined,
    });
    return {
      replyId: emitted.messageId,
      threadId: route.threadId,
      targetNodeId: route.peerNodeId,
      targetMailbox: route.targetMailbox,
      receipts: emitted.receipts,
    };
  }

  const outcome = settlementOutcome(v.outcome);
  const summary = required(
    typeof v.summary === 'string' && v.summary ? v.summary : v.body,
    '--summary',
  );
  const emitted = await runtime.emitter.emit({
    networkId: route.networkId,
    kind: route.kind,
    subject:
      typeof v.subject === 'string' && v.subject
        ? String(v.subject)
        : `[${outcome}] ${route.subject}`,
    body: summary,
    payload: { summary, outcome, settledMessageId: messageId },
    replyToId: messageId,
    traceId: route.traceId,
    routing: { mode: 'unicast', targets: [route.peerNodeId], topic: route.kind },
    semantic: {
      type: 'settle',
      threadId: route.threadId,
      targetMailbox: route.targetMailbox,
      replyMailbox,
      outcome,
    },
    filePaths: v.attach as string[] | undefined,
  });
  return {
    settlementId: emitted.messageId,
    threadId: route.threadId,
    outcome,
    targetNodeId: route.peerNodeId,
    targetMailbox: route.targetMailbox,
    receipts: emitted.receipts,
  };
}

export async function runDealerCli(argv: string[], options: DealerCliOptions = {}): Promise<number> {
  const stdout = options.stdout ?? console.log;
  const stderr = options.stderr ?? console.error;
  let owned: DealerNodeRuntime | undefined;
  try {
    const { values: v, positionals: p } = parse(argv);
    const [group, action] = p;
    let target = p[2];
    if (group === 'project' && action === 'init') {
      const root = resolve(String(v.root ?? process.cwd()));
      const projectId = String(v.id ?? basename(root));
      stdout(JSON.stringify(initProjectScaffold(root, {
        projectId,
        defaultNetworkId: String(v.network ?? projectId),
        ...(v.description !== undefined ? { description: String(v.description) } : {}),
        ...(v.skills !== undefined ? { skills: String(v.skills).split(',').filter(Boolean) } : {}),
      }), null, v.json ? undefined : 2));
      return 0;
    }
    if (group === 'project' && action === 'link') {
      target = resolve(target ?? process.cwd());
      p[2] = target;
      if (v['user-patch'] !== undefined) {
        v['user-patch'] = resolve(String(v['user-patch']));
      }
    }
    if (group === 'plugin' && action === 'add' && v.config !== undefined) {
      v.config = resolve(String(v.config));
    }
    // Preserve caller-relative paths when the daemon has a different cwd.
    if (group === 'project' || group === 'plugin') {
      argv = [...p];
      for (const [key, value] of Object.entries(v)) {
        if (value === true) {
          argv.push(`--${key}`);
        } else if (typeof value === 'string') {
          argv.push(`--${key}`, value);
        }
      }
    }
    const stateDir = resolve(options.stateDir ?? defaultStateDir());
    if (group === 'setup') {
      let result: unknown;
      if (action === 'probe') {
        result = probeDealerSetup({ stateDir, cwd: v.root });
      } else if (action === 'binding-preset') {
        result = buildAgentBindingPreset({
          host: required(v.host, '--host') as SupportedAgentHost,
          conversationMode: v.mode as AgentConversationMode | undefined,
          topic: v.topic, projectId: v.project, workspacePath: v['workspace-path'],
          agent: v.agent as SupportedOrcaAgent | undefined, agentProfile: v['agent-profile'],
          targetBot: v['target-bot'], sessionId: v['session-id'],
        });
      } else if (action === 'invite-prompt') {
        const networkId = required(v.network, '--network');
        const configFile = join(stateDir, 'config.json');
        const config = options.runtime?.config ?? (existsSync(configFile) ? JSON.parse(readFileSync(configFile, 'utf8')) : {});
        const existing = config.networks?.find((entry: { networkId: string }) => entry.networkId === networkId);
        const passphrase = v.passphrase ?? existing?.passphrase ?? randomBytes(32).toString('base64url');
        let bootstrapPeers = v.seed?.split(',').filter(Boolean) ?? existing?.bootstrapPeers;
        if (!v.seed && !existing?.bootstrapPeers?.length) {
          let endpoint = options.runtime?.status().endpoint;
          let liveStatus = Boolean(options.runtime);
          if (!options.runtime && existsSync(join(stateDir, 'control.sock'))) {
            try {
              const status = await rpc(join(stateDir, 'control.sock'), ['node', 'status', '--json'], { inactivityMs: 2000 });
              if (status.code !== 0) throw new Error(status.stderr.join('\n'));
              endpoint = JSON.parse(status.stdout.join('')).endpoint;
              liveStatus = true;
            } catch (error) {
              if (!['ENOENT', 'ECONNREFUSED'].includes((error as NodeJS.ErrnoException).code ?? '')) throw error;
            }
          }
          if (!liveStatus && Number.isInteger(config.port) && config.port > 0 && config.port <= 65535) {
            endpoint = `${defaultAdvertisedHost()}:${config.port}`;
          }
          if (endpoint) {
            const host = new URL(`tcp://${endpoint}`).hostname;
            if (!['localhost', '0.0.0.0', '[::]', '[::1]'].includes(host) && !host.startsWith('127.')) bootstrapPeers = [endpoint];
          }
        }
        const inviteUri = encodeNetworkInvite({ networkId, passphrase, bootstrapPeers }, duration(v.ttl));
        result = buildOnboardingPrompts({ networkId, inviteUri, member: v.member ?? config.member,
          peerMember: v['peer-member'], nodeName: v.name, seed: bootstrapPeers, projectId: v.project });
      } else {
        throw new Error('Unknown setup command');
      }
      stdout(JSON.stringify(result, null, v.json ? undefined : 2));
      return 0;
    }
    if (!options.runtime && existsSync(join(stateDir, 'control.sock'))) {
      const result = await rpc(join(stateDir, 'control.sock'), argv);
      result.stdout.forEach(line => stdout(line));
      result.stderr.forEach(line => stderr(line));
      return result.code;
    }
    if (group === 'inbox') {
      // Offline access is serialized by the mailbox file lock, so it stays coherent even if
      // the daemon owns the state directory.
      const store = await mailboxStoreFor(options, stateDir);
      stdout(JSON.stringify(await runInboxCommand(store, action, target, v, options.signal), null, v.json ? undefined : 2));
      return 0;
    }
    if (group === 'thread') {
      const store = await mailboxStoreFor(options, stateDir);
      stdout(JSON.stringify(runThreadCommand(store, action, target, v), null, v.json ? undefined : 2));
      return 0;
    }
    if (!options.runtime && group === 'node' && action === 'up') {
      stdout(JSON.stringify(await launchDaemon(argv, stateDir)));
      return 0;
    }
    if (!options.runtime && group === 'node' && action === 'status') {
      const file = join(stateDir, 'config.json');
      const config = existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')) : {};
      stdout(
        JSON.stringify({
          running: false,
          profile: config.profile,
          member: config.member,
          networks: (config.networks ?? []).map((n: { networkId: string }) => n.networkId),
        }),
      );
      return 0;
    }
    const runtime = options.runtime ?? (owned = new DealerNodeRuntime({ stateDir }));
    await runtime.start();
    let result: unknown;
    const network = v.network as string | undefined;
    if (group === 'ask' || group === 'reply' || group === 'settle') {
      result = await runSessionCommand(runtime, group, action, v);
    } else if (group === 'project') {
      if (action === 'link') {
        result = await runtime.linkProject(target!, v['user-patch'] as string | undefined);
      } else if (action === 'list') {
        result = runtime.projectStatus().projects;
      } else if (action === 'status') {
        result = runtime.projectStatus(v.project as string | undefined);
      } else if (action === 'unlink') {
        const projectId = required(target, 'projectId');
        await runtime.unlinkProject(projectId);
        result = { unlinked: projectId };
      } else {
        throw new Error('Unknown project command');
      }
    } else if (group === 'plugin' && action === 'add') {
      result = await runtime.installUserPlugin({
        name: required(v.name, '--name'),
        module: required(v.module, '--module'),
        config: v.config ? JSON.parse(readFileSync(String(v.config), 'utf8')) : {},
      });
    } else if (group === 'node') {
      if (action === 'up' || action === 'status') {
        if (action === 'up' && (['profile', 'patch', 'member', 'name', 'skills', 'port'] as const).some(key => v[key] !== undefined)) {
          throw new Error('Node already running; stop it before changing startup configuration');
        }
        result = runtime.status();
      } else if (action === 'card') {
        result = network
          ? runtime.node.getSelfCard(network)
          : runtime.transport.listMountedNetworks().map(id => runtime.node.getSelfCard(id));
      } else {
        throw new Error('Unknown node command');
      }
    } else if (group === 'network') {
      if (action === 'mount') {
        const invite = required(target, 'networkId').startsWith('dealer-net:')
          ? decodeNetworkInvite(target)
          : { networkId: target, passphrase: required(v.passphrase, '--passphrase') };
        await runtime.mountNetwork({
          ...invite,
          ...(v.seed ? { bootstrapPeers: String(v.seed).split(',') } : {}),
          ...(v.gate ? { admissionMode: admissionMode(String(v.gate)) } : {}),
          ...(v.role ? { role: String(v.role) } : {}),
        });
        result = { mounted: invite.networkId };
      } else if (action === 'unmount') {
        await runtime.unmountNetwork(required(target, 'networkId'));
        result = { unmounted: target };
      } else if (action === 'list') {
        result = runtime.config.networks.map(({ passphrase, ...rest }) => rest);
      } else if (action === 'invite') {
        const config = runtime.config.networks.find(n => n.networkId === target);
        if (!config) {
          throw new Error('Network not mounted');
        }
        result = {
          uri: encodeNetworkInvite(
            { ...config, passphrase: String(v.passphrase ?? config.passphrase) },
            duration(v.ttl as string | undefined),
          ),
        };
      } else {
        throw new Error('Unknown network command');
      }
    } else if (group === 'peers') {
      if (action === 'list') {
        result = runtime.node
          .listPeers({ networkId: network, skillTag: v.skill as string | undefined })
          .filter(c => !v.status || c.runtimeState.load === v.status || c.admissionMode === v.status);
      } else if (action === 'resolve') {
        result = runtime.node.resolveTarget(required(target, 'selector'), network);
      } else {
        throw new Error('Unknown peers command');
      }
    } else if (group === 'gate') {
      if (action === 'mode') {
        await runtime.setGateMode(admissionMode(required(target, 'mode')), network);
        result = { mode: network ? runtime.gate.getNetworkMode(network) : runtime.gate.getMode() };
      } else if (action === 'list') {
        result = runtime.gate.listPendingApprovals().filter(item => !network || item.envelope.networkId === network);
      } else if (action === 'approve' || action === 'reject') {
        const id = required(target, 'envelopeId');
        const pending = runtime.gate.listPendingApprovals().filter(item => item.id === id || item.envelope.id === id);
        if (pending.length !== 1) {
          throw new Error('Unknown or ambiguous approval');
        }
        if (action === 'approve') {
          await runtime.gate.approve(pending[0].id, { trustSender: v['trust-sender'] === true });
        } else {
          await runtime.gate.reject(pending[0].id, v.reason as string | undefined);
        }
        result = { [action === 'approve' ? 'approved' : 'rejected']: id };
      } else if (action === 'drain') {
        await runtime.gate.drainDnd(network);
        result = { drained: true };
      } else {
        throw new Error('Unknown gate command');
      }
    } else if (group === 'send' || group === 'broadcast') {
      const topic = required(v.topic, '--topic');
      const body = required(v.body, '--body');
      const rawUrgency = (v.urgency ?? v.Urgency) as string | undefined;
      if (group === 'send' && Boolean(v.to) === Boolean(v['auto-route'])) {
        throw new Error('Choose exactly one of --to or --auto-route');
      }
      if (rawUrgency && !['normal', 'high', 'critical'].includes(String(rawUrgency))) {
        throw new Error('Invalid urgency');
      }
      const targetMailbox =
        typeof v.mailbox === 'string' && v.mailbox ? String(v.mailbox) : undefined;
      const replyMailbox =
        typeof v['reply-mailbox'] === 'string' && v['reply-mailbox']
          ? String(v['reply-mailbox'])
          : undefined;
      const threadId = v.thread === undefined ? undefined : required(v.thread?.trim(), '--thread');
      result = await runtime.emitter.emit({
        networkId: required(v.network, '--network'),
        kind: topic,
        subject:
          typeof v.subject === 'string' && v.subject ? String(v.subject) : topic,
        body,
        payload: payloadFromBody(body),
        routing: {
          mode: group === 'broadcast' ? 'broadcast' : v['auto-route'] ? 'auto' : 'unicast',
          ...(v.to ? { targets: [String(v.to)] } : {}),
          topic,
        },
        ...(threadId !== undefined || targetMailbox !== undefined || replyMailbox !== undefined
          ? {
              semantic: {
                type: 'message' as const,
                ...(threadId !== undefined ? { threadId } : {}),
                ...(targetMailbox !== undefined ? { targetMailbox } : {}),
                ...(replyMailbox !== undefined ? { replyMailbox } : {}),
              },
            }
          : {}),
        filePaths: v.attach as string[] | undefined,
        priority: rawUrgency === 'critical' ? 'urgent' : (rawUrgency as 'normal' | 'high' | undefined),
      });
    } else if ((group === 'route' || group === 'source') && action === 'add') {
      if (owned && group === 'source') {
        throw new Error('Start dealer node up before adding a live source');
      }
      const config = v.config ? JSON.parse(readFileSync(String(v.config), 'utf8')) : {};
      for (const key of ['command', 'endpoint', 'model', 'url', 'cwd', 'path'] as const) {
        if (v[key]) {
          config[key] = v[key];
        }
      }
      if (v.port !== undefined) {
        config.port = Number(v.port);
      }
      if (v.args) {
        config.args = JSON.parse(String(v.args));
      }
      if (v['interval-ms']) {
        config.intervalMs = Number(v['interval-ms']);
      }
      if (v.token) {
        config.bearerToken = v.token;
      }
      const spec = { type: required(v.type, '--type'), config };
      const registration = group === 'route' ? runtime.installRoute(spec) : await runtime.installSource(spec);
      const list = group === 'route' ? runtime.config.routes : runtime.config.sources;
      list.push(spec);
      try {
        runtime.persist();
      } catch (error) {
        list.pop();
        await registration.dispose();
        throw error;
      }
      result = { added: spec.type };
    } else {
      throw new Error('Unknown command; see skills/the-dealer/SKILL.md');
    }
    stdout(JSON.stringify(result, null, v.json ? undefined : 2));
    return 0;
  } catch (error) {
    stderr(JSON.stringify({ error: error instanceof Error ? error.message : String(error) }));
    return 1;
  } finally {
    if (owned) {
      await owned.stop();
    }
  }
}

export async function runDealerDaemon(argv: string[]): Promise<DealerNodeRuntime> {
  const { values } = parse(argv);
  const runtime = new DealerNodeRuntime(runtimeOptions(values, defaultStateDir()));
  await runtime.start();
  try {
    await runtime.serveControl(async (args, context) => {
      const stdout: string[] = [];
      const stderr: string[] = [];
      const code = await runDealerCli(args, {
        runtime,
        stdout: line => stdout.push(line),
        stderr: line => stderr.push(line),
        signal: context.signal,
      });
      return { code, stdout, stderr };
    });
  } catch (error) {
    await runtime.stop();
    throw error;
  }
  return runtime;
}
