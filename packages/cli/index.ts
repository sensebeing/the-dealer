import { randomBytes } from 'node:crypto';
import { probeDealerSetup, buildOnboardingPrompts, resolveAgentBindingPreset, readAgentPresetContext } from './setup.ts';
import type { SupportedAgentHost, AgentConversationMode } from '../plugins-executor/index.ts';
import type { SupportedOrcaAgent } from '../orca-workspace/index.ts';
import { existsSync, readFileSync, rmSync, watch } from 'node:fs';
import { resolve, join, basename } from 'node:path';
import { fileURLToPath } from 'node:url';
import { request } from 'node:http';
import type { ClientRequest } from 'node:http';
import { spawn } from 'node:child_process';
import { parseArgs } from 'node:util';
import { MailboxStore, resolveBoundMailboxId } from '../mailbox/index.ts';
import type { ResolvedReplyTarget } from '../mailbox/index.ts';
import type { SettlementOutcome } from '../core-types/index.ts';
import { DealerNodeRuntime, defaultStateDir } from './runtime.ts';
import { computeRuntimeFingerprint, readDaemonFingerprint, readAutoUpdateStatus, selfUpdate, maybeTriggerBackgroundAutoUpdate } from './auto-update.ts';
import { defaultAdvertisedHost } from '../transport/index.ts';
import { admissionMode, readProjectManifest, readPatch, encodeNetworkInvite, decodeNetworkInvite, initProjectScaffold } from './config.ts';
import { isDeadProcessOwner, readProcessOwner } from '../process-lock/index.ts';

export * from './runtime.ts';
export * from './config.ts';
export * from './setup.ts';
export * from './auto-update.ts';

const booleanFlags = ['json', 'quiet', 'force', 'auto-route', 'trust-sender', 'all', 'peek', 'wait', 'notify-lark', 'set-default'] as const;
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
  'terminal-handle',
  'worktree-id',
  'subject',
  'bootstrap-peers',
  'advertised-host',
  'checkout-dir',
  'home-dir',
  'gemini-dir',
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
    ...(values['advertised-host'] !== undefined
      ? { transport: { advertisedHost: String(values['advertised-host']) } }
      : {}),
  };
}

export interface DealerCliOptions {
  stateDir?: string;
  runtime?: DealerNodeRuntime;
  stdout?: (line: string) => void;
  stderr?: (line: string) => void;
  /** Aborted when the calling control request disconnects, so long waits release listeners. */
  signal?: AbortSignal;
  env?: NodeJS.ProcessEnv;
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
      ['--no-warnings', '--experimental-strip-types', new URL('../../bin/dealerd.js', import.meta.url).pathname, ...argv],
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

function liveDaemonOwner(stateDir: string) {
  try {
    const owner = readProcessOwner(join(stateDir, 'runtime.lock', 'owner.json'));
    return owner && !isDeadProcessOwner(owner, stateDir) ? owner : undefined;
  } catch {
    return undefined;
  }
}

type DaemonProbe = 'live' | 'absent' | 'stale_recovered';

/**
 * A control socket file alone is not proof of a live daemon: a process killed without a
 * graceful shutdown leaves it behind. Probe the listener only while no lock owner is live,
 * and treat connection refusal as stale residue.
 */
async function probeDaemonState(stateDir: string): Promise<DaemonProbe> {
  if (liveDaemonOwner(stateDir)) {
    return 'live';
  }
  const socket = join(stateDir, 'control.sock');
  if (!existsSync(socket)) {
    return 'absent';
  }
  try {
    await rpc(socket, ['node', 'status', '--json'], { inactivityMs: 1_000 });
    return 'live';
  } catch {
    rmSync(socket, { force: true });
    return 'stale_recovered';
  }
}

/**
 * A supervisor such as `launchd` (`KeepAlive`) starts a replacement daemon the instant the
 * target exits, so a live listener is not proof that the shutdown is still pending. Track
 * the target incarnation by identity: the shutdown is complete once the listener is gone or
 * the lock owner is a different incarnation than the one asked to stop.
 */
async function waitForDaemonStopped(
  stateDir: string,
  targetInstanceId?: string,
  targetPid?: number,
  timeoutMs = 20_000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if ((await probeDaemonState(stateDir)) !== 'live') {
      return true;
    }
    const current = liveDaemonOwner(stateDir);
    const replaced = current !== undefined && (targetInstanceId
      ? current.instanceId !== targetInstanceId
      : targetPid !== undefined && current.pid !== targetPid);
    if (replaced) {
      return true;
    }
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  return false;
}

export async function stopDaemonIfRunning(
  stateDir: string,
  options: { force?: boolean } = {},
): Promise<{ running: boolean; stopped: boolean; pid?: number; respawned?: boolean }> {
  const probe = await probeDaemonState(stateDir);
  if (probe === 'absent') {
    return { running: false, stopped: false };
  }
  if (probe === 'stale_recovered') {
    return { running: false, stopped: true };
  }
  // Capture the incarnation being shut down before asking it to stop; the RPC pid is only
  // a fallback witness when the lock owner could not be read.
  const targetOwner = liveDaemonOwner(stateDir);
  let pid: number | undefined = targetOwner?.pid;
  try {
    const result = await rpc(join(stateDir, 'control.sock'), ['node', 'down', '--json'], { inactivityMs: 2_000 });
    const parsed = JSON.parse(result.stdout.join('') || '{}');
    if (typeof parsed.pid === 'number') {
      pid = parsed.pid;
    }
  } catch { /* the daemon may remove the socket while responding */ }
  let stopped = await waitForDaemonStopped(stateDir, targetOwner?.instanceId, pid);
  if (!stopped && options.force) {
    const owner = liveDaemonOwner(stateDir);
    // Never signal a replacement incarnation a supervisor started after the target drained;
    // only the captured target is ours to escalate.
    if (owner && (!targetOwner || owner.instanceId === targetOwner.instanceId)) {
      try {
        process.kill(owner.pid, 'SIGTERM');
      } catch { /* exited */ }
      stopped = await waitForDaemonStopped(stateDir, targetOwner?.instanceId, pid, 5_000);
    }
  }
  const stillRunning = stopped ? (await probeDaemonState(stateDir)) === 'live' : true;
  return {
    running: stillRunning,
    stopped,
    ...(pid ? { pid } : {}),
    ...(stopped && stillRunning ? { respawned: true } : {}),
  };
}

export async function ensureDaemonRunning(stateDir: string): Promise<unknown> {
  if (existsSync(join(stateDir, 'control.sock'))) {
    try {
      const status = JSON.parse((await rpc(join(stateDir, 'control.sock'), ['node', 'status', '--json'], { inactivityMs: 2_000 })).stdout.join(''));
      if (status.runtimeFingerprint === computeRuntimeFingerprint(stateDir)) return status;
    } catch { /* stale socket */ }
  }
  try {
    return await launchDaemon([], stateDir);
  } catch (error) {
    const deadline = Date.now() + 3_000;
    while (Date.now() < deadline) {
      try {
        const result = await rpc(join(stateDir, 'control.sock'), ['node', 'status', '--json'], { inactivityMs: 500 });
        const status = JSON.parse(result.stdout.join(''));
        if (status.runtimeFingerprint === computeRuntimeFingerprint(stateDir)) return status;
      } catch { /* concurrent launch still settling */ }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw error;
  }
}

export async function restartDaemon(stateDir: string, options: { force?: boolean } = {}): Promise<unknown> {
  const stopped = await stopDaemonIfRunning(stateDir, options);
  // Only a drain that could not quiesce leaves the target serving. A supervisor that already
  // started a replacement counts as a completed stop, so confirm that live instance instead
  // of reporting a false deferral for the incarnation that really did exit.
  if (!stopped.stopped && stopped.running) {
    return { ...stopped, restarted: false, deferred: true };
  }
  return ensureDaemonRunning(stateDir);
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
  const caller = resolveCallerOrcaBinding(v);
  const mailboxId = (v.mailbox as string | undefined) ?? caller?.mailboxId;
  if (caller && (mailboxId ?? 'default') === caller.mailboxId) bindCallerWakeTarget(store, caller.mailboxId, caller);
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

interface CallerOrcaBinding {
  mailboxId: string;
  terminalHandle?: string;
  worktreeId?: string;
  workspacePath?: string;
}

function resolveCallerOrcaBinding(v: Record<string, any>): CallerOrcaBinding | undefined {
  const terminalHandle = typeof v['terminal-handle'] === 'string' && v['terminal-handle'].trim()
    ? v['terminal-handle'].trim() : undefined;
  const worktreeId = typeof v['worktree-id'] === 'string' && v['worktree-id'].trim()
    ? v['worktree-id'].trim() : undefined;
  const explicitPath = typeof v['workspace-path'] === 'string' && v['workspace-path'].trim()
    ? resolve(v['workspace-path'].trim()) : undefined;
  const suffixPath = worktreeId?.includes('::') ? worktreeId.slice(worktreeId.indexOf('::') + 2) : undefined;
  const workspacePath = explicitPath ?? (suffixPath ? resolve(suffixPath) : undefined);
  if (!terminalHandle && !worktreeId && !workspacePath) return undefined;
  const mailboxId = workspacePath ? `worktree:${basename(workspacePath)}`
    : worktreeId ? `worktree:${basename(worktreeId)}` : `terminal:${terminalHandle}`;
  return { mailboxId, ...(terminalHandle ? { terminalHandle } : {}), ...(worktreeId ? { worktreeId } : {}), ...(workspacePath ? { workspacePath } : {}) };
}

function bindCallerWakeTarget(store: MailboxStore, mailboxId: string, caller: CallerOrcaBinding | undefined): void {
  if (!caller) return;
  const existing = store.getWakeTarget(mailboxId);
  if (existing?.mode === 'orca-run' || (existing?.mode === 'passive' && existing.hostAgent !== undefined)) return;
  store.upsertWakeTarget(mailboxId, {
    mode: 'orca-terminal',
    ...(caller.workspacePath ? { workspacePath: caller.workspacePath } : {}),
    ...(caller.worktreeId ? { worktreeId: caller.worktreeId } : {}),
    ...(caller.terminalHandle ? { terminalHandle: caller.terminalHandle } : {}),
  });
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
  caller?: CallerOrcaBinding,
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
      const replyMailbox = callerMailboxFlag ?? (route.localMailbox !== 'default' ? route.localMailbox : caller?.mailboxId ?? 'default');
      bindCallerWakeTarget(store, replyMailbox, caller);
      if (ackId) {
        store.ack(ackId, replyMailbox);
      }
      const body = required(v.body, '--body');
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
    const topic = typeof v.topic === 'string' && v.topic ? v.topic : 'collab.question.v1';
    const body = required(v.body, '--body');
    if (Boolean(v.to) === Boolean(v['auto-route'])) {
      throw new Error('Choose exactly one of --to or --auto-route');
    }
    if (ackId) {
      store.ack(ackId, callerMailboxFlag ?? caller?.mailboxId ?? 'default');
    }
    const replyMailbox = callerMailboxFlag ?? caller?.mailboxId ?? 'default';
    bindCallerWakeTarget(store, replyMailbox, caller);
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
        ...(replyMailbox !== 'default' ? { replyMailbox } : {}),
      },
      filePaths: v.attach as string[] | undefined,
    });
    return {
      questionId: emitted.messageId,
      threadId: emitted.messageId,
      targetNodeId: emitted.receipts[0]?.fromNodeId,
      targetMailbox: targetMailbox ?? 'default',
      replyMailbox,
      receipts: emitted.receipts,
      next: 'End your turn now; Dealer will wake this mailbox when the reply arrives.',
    };
  }

  const messageId = required(positional, 'message-id');
  const route = replyTargetOrThrow(store, messageId);
  const replyMailbox = callerMailboxFlag ?? (route.localMailbox !== 'default' ? route.localMailbox : caller?.mailboxId ?? 'default');
  bindCallerWakeTarget(store, replyMailbox, caller);
  if (ackId) {
    store.ack(ackId, replyMailbox);
  }

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
      replyMailbox,
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
    const callerEnv = options.env ?? (options.runtime || options.stateDir ? {} : process.env);
    if (['ask', 'reply', 'settle', 'send', 'inbox'].includes(group ?? '')) {
      if (v['terminal-handle'] === undefined && callerEnv.ORCA_TERMINAL_HANDLE?.trim()) v['terminal-handle'] = callerEnv.ORCA_TERMINAL_HANDLE.trim();
      if (v['worktree-id'] === undefined && (callerEnv.ORCA_WORKTREE_ID ?? callerEnv.ORCA_WORKSPACE_ID)?.trim()) v['worktree-id'] = (callerEnv.ORCA_WORKTREE_ID ?? callerEnv.ORCA_WORKSPACE_ID)!.trim();
    }
    let target = p[2];
    if (group === 'project' && action === 'init') {
      const root = resolve(String(v.root ?? process.cwd()));
      const projectId = String(v.id ?? basename(root));
      const bootstrapPeers = typeof v['bootstrap-peers'] === 'string'
        ? String(v['bootstrap-peers']).split(',').map(peer => peer.trim()).filter(Boolean)
        : [];
      stdout(JSON.stringify(initProjectScaffold(root, {
        projectId,
        defaultNetworkId: String(v.network ?? projectId),
        ...(v.description !== undefined ? { description: String(v.description) } : {}),
        ...(v.skills !== undefined ? { skills: String(v.skills).split(',').filter(Boolean) } : {}),
        ...(bootstrapPeers.length ? { bootstrapPeers } : {}),
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
    if (group === 'setup' && ['binding-preset', 'apply-preset'].includes(action)) {
      v.root = resolve(String(v.root ?? process.cwd()));
    }
    if (group === 'network' && action === 'mount' && target && !target.startsWith('dealer-net:')) {
      v.root = resolve(String(v.root ?? process.cwd()));
      if (v.passphrase === undefined && existsSync(join(v.root, '.dealer/project.json'))) {
        const manifest = readProjectManifest(v.root);
        if (manifest.onboarding?.passphrase && (manifest.onboarding.networkId ?? manifest.defaultNetworkId) === target) {
          v.passphrase = manifest.onboarding.passphrase;
          if (v.seed === undefined && manifest.onboarding.bootstrapPeers?.length) {
            v.seed = manifest.onboarding.bootstrapPeers.join(',');
          }
        }
      }
    }
    // Preserve caller-relative paths when the daemon has a different cwd.
    if (['ask', 'reply', 'settle', 'send', 'inbox'].includes(group ?? '') || group === 'project' || group === 'plugin' || (group === 'network' && action === 'mount') || (group === 'setup' && ['binding-preset', 'apply-preset'].includes(action))) {
      argv = [...p];
      for (const [key, value] of Object.entries(v)) {
        if (value === true) {
          argv.push(`--${key}`);
        } else if (Array.isArray(value)) {
          for (const item of value) argv.push(`--${key}`, String(item));
        } else if (typeof value === 'string') {
          argv.push(`--${key}`, value);
        }
      }
    }
    const stateDir = resolve(options.stateDir ?? defaultStateDir());
    const localSetup = group === 'setup' && !['binding-preset', 'apply-preset'].includes(action);
    const localLifecycle = group === 'node' && ['down', 'restart', 'up'].includes(action ?? '') || group === 'self-update';
    if (!localSetup && !localLifecycle && !options.runtime && existsSync(join(stateDir, 'control.sock'))) {
      // The daemon's idle-gated watcher is the sole automatic reload owner; ordinary
      // commands always forward to the running daemon without a synchronous restart.
      const result = await rpc(join(stateDir, 'control.sock'), argv);
      result.stdout.forEach(line => stdout(line));
      result.stderr.forEach(line => stderr(line));
      maybeTriggerBackgroundAutoUpdate({ stateDir });
      return result.code;
    }
    if (group === 'setup' && action !== 'apply-preset') {
      let result: unknown;
      if (action === 'probe') {
        result = probeDealerSetup({ stateDir, cwd: v.root });
      } else if (action === 'binding-preset') {
        const presetOptions = {
          host: required(v.host, '--host') as SupportedAgentHost,
          conversationMode: v.mode as AgentConversationMode | undefined,
          topic: v.topic, projectId: v.project, workspacePath: v['workspace-path'],
          agent: v.agent as SupportedOrcaAgent | undefined, agentProfile: v['agent-profile'],
          targetBot: v['target-bot'], sessionId: v['session-id'], notifyLark: v['notify-lark'], cwd: v.root,
        };
        result = options.runtime ? options.runtime.previewAgentPreset(presetOptions)
          : resolveAgentBindingPreset(presetOptions, { ...readAgentPresetContext(stateDir), cwd: v.root });
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
    if (!options.runtime && group === 'node' && action === 'down') {
      const result = await stopDaemonIfRunning(stateDir, { force: v.force === true });
      stdout(JSON.stringify(result, null, v.json ? undefined : 2));
      return 0;
    }
    if (!options.runtime && group === 'node' && action === 'restart') {
      const result = await restartDaemon(stateDir, { force: v.force === true });
      stdout(JSON.stringify(result, null, v.json ? undefined : 2));
      return 0;
    }
    if (!options.runtime && group === 'self-update') {
      const report = selfUpdate({ stateDir, checkoutDir: v['checkout-dir'], homeDir: v['home-dir'], geminiDir: v['gemini-dir'] });
      if (existsSync(join(stateDir, 'control.sock')) && readDaemonFingerprint(stateDir)?.fingerprint !== computeRuntimeFingerprint(stateDir, v['checkout-dir'])) {
        const restarted = await restartDaemon(stateDir);
        report.restartedDaemon = !(restarted as { deferred?: boolean } | null)?.deferred;
      }
      stdout(JSON.stringify(report, null, v.json ? undefined : 2));
      return 0;
    }
    const runtime = options.runtime ?? (owned = new DealerNodeRuntime({ stateDir }));
    await runtime.start();
    const caller = resolveCallerOrcaBinding(v);
    let result: unknown;
    const network = v.network as string | undefined;
    if (group === 'setup' && action === 'apply-preset') {
      result = await runtime.applyAgentPreset({
        host: required(v.host, '--host') as SupportedAgentHost,
        conversationMode: v.mode as AgentConversationMode | undefined,
        topic: v.topic, projectId: v.project, workspacePath: v['workspace-path'],
        agent: v.agent as SupportedOrcaAgent | undefined, agentProfile: v['agent-profile'],
        targetBot: v['target-bot'], sessionId: v['session-id'],
        notifyLark: v['notify-lark'], setDefault: v['set-default'], cwd: v.root,
      });
    } else if (group === 'ask' || group === 'reply' || group === 'settle') {
      result = await runSessionCommand(runtime, group, action, v, caller);
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
    } else if (group === 'plugin') {
      if (action === 'add') {
        result = await runtime.installUserPlugin({
          name: required(v.name, '--name'),
          module: required(v.module, '--module'),
          config: v.config ? JSON.parse(readFileSync(String(v.config), 'utf8')) : {},
        });
      } else if (action === 'remove') {
        result = await runtime.removeUserPlugin(required(target ?? v.name, 'plugin name'));
      } else if (action === 'list') {
        result = runtime.projectStatus().userPlugins;
      } else {
        throw new Error('Unknown plugin command');
      }
    } else if (group === 'node') {
      if (action === 'up' || action === 'status') {
        if (action === 'up' && (['profile', 'patch', 'member', 'name', 'skills', 'port', 'advertised-host'] as const).some(key => v[key] !== undefined)) {
          throw new Error('Node already running; stop it before changing startup configuration');
        }
        result = runtime.status();
      } else if (action === 'down') {
        result = { ok: true, stopping: true, pid: process.pid };
        setImmediate(() => {
          void runtime.gracefulStop().then(() => process.exit(0)).catch(error => {
            runtime.lastError = String(error);
          });
        });
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
      const callerBinding = caller;
      const payload = payloadFromBody(body);
      const streamKey = typeof payload.streamKey === 'string' ? payload.streamKey : undefined;
      const resolvedTopic = group === 'send' ? runtime.resolveTopicBinding(topic, streamKey) : undefined;
      const boundTopicMailbox = resolvedTopic
        ? resolveBoundMailboxId(streamKey ? `${topic}:${streamKey}` : topic, resolvedTopic.binding)
        : undefined;
      const callerMatchesBoundTopic = Boolean(
        callerBinding && resolvedTopic && (
          callerBinding.mailboxId === boundTopicMailbox ||
          (callerBinding.workspacePath && resolvedTopic.binding.workspacePath &&
            resolve(callerBinding.workspacePath) === resolve(resolvedTopic.binding.workspacePath)) ||
          (callerBinding.worktreeId && resolvedTopic.binding.worktreeId &&
            callerBinding.worktreeId === resolvedTopic.binding.worktreeId) ||
          (callerBinding.terminalHandle && resolvedTopic.binding.terminalHandle &&
            callerBinding.terminalHandle === resolvedTopic.binding.terminalHandle)
        ),
      );
      const replyMailbox =
        typeof v['reply-mailbox'] === 'string' && v['reply-mailbox']
          ? String(v['reply-mailbox'])
          : (group === 'send' && callerBinding?.mailboxId && !callerMatchesBoundTopic
            ? callerBinding.mailboxId
            : undefined);
      if (callerBinding && replyMailbox) bindCallerWakeTarget(runtime.mailbox, replyMailbox, callerBinding);
      const threadId = v.thread === undefined ? undefined : required(v.thread?.trim(), '--thread');
      result = await runtime.emitter.emit({
        networkId: required(v.network, '--network'),
        kind: topic,
        subject:
          typeof v.subject === 'string' && v.subject ? String(v.subject) : topic,
        body,
        payload,
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
  let reloadPending = false;
  let restartTriggered = false;
  let debounce: ReturnType<typeof setTimeout> | undefined;
  const watchers: Array<{ close(): void }> = [];
  const watchedPaths = new Set<string>();
  const maybeSpawnSelfRestart = () => {
    if (!reloadPending || restartTriggered || process.env.DEALER_AUTO_RESTART === '0' || !runtime.isIdleForRestart()) return;
    restartTriggered = true;
    for (const item of watchers) item.close();
    const bin = fileURLToPath(new URL('../../bin/dealer.js', import.meta.url));
    const child = spawn(process.execPath, ['--no-warnings', '--experimental-strip-types', bin, 'node', 'restart', '--json'], { detached: true, stdio: 'ignore', env: { ...process.env, DEALER_STATE_DIR: runtime.stateDir } });
    child.unref();
  };
  const schedule = () => {
    if (debounce) clearTimeout(debounce);
    debounce = setTimeout(() => {
      if (computeRuntimeFingerprint(runtime.stateDir) !== runtime.currentDaemonFingerprint()) reloadPending = true;
      maybeSpawnSelfRestart();
    }, 400);
    debounce.unref();
  };
  const addWatch = (path: string, recursive = true) => {
    if (watchedPaths.has(path)) return;
    watchedPaths.add(path);
    if (!existsSync(path)) return;
    try { watchers.push(watch(path, { recursive, persistent: false }, schedule)); }
    catch { try { watchers.push(watch(path, { persistent: false }, schedule)); } catch { /* missing optional paths */ } }
  };
  const dealerRoot = resolve(fileURLToPath(new URL('../..', import.meta.url)));
  addWatch(join(dealerRoot, 'bin'));
  addWatch(join(dealerRoot, 'packages'));
  addWatch(join(dealerRoot, 'package.json'), false);
  addWatch(runtime.stateDir);
  for (const project of runtime.config.projects) addWatch(join(project.rootPath, '.dealer'));
  const dispatcher = runtime.kernel.ctx.services.dispatcher;
  dispatcher.onIdle(maybeSpawnSelfRestart);
  runtime.onConfigPersisted = () => {
    for (const project of runtime.config.projects) addWatch(join(project.rootPath, '.dealer'));
    schedule();
  };
  runtime.onControlIdle = maybeSpawnSelfRestart;
  // Remote Git updates cannot emit local filesystem events, so check upstream every six hours.
  const timer = setInterval(() => maybeTriggerBackgroundAutoUpdate({ stateDir: runtime.stateDir }), 6 * 3600_000);
  timer.unref();
  return runtime;
}
