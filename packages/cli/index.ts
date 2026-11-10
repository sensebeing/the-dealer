import { existsSync, readFileSync } from 'node:fs';
import { resolve, join, basename } from 'node:path';
import { request } from 'node:http';
import type { ClientRequest } from 'node:http';
import { spawn } from 'node:child_process';
import { parseArgs } from 'node:util';
import { MailboxStore } from '../mailbox/index.ts';
import { DealerNodeRuntime, defaultStateDir } from './runtime.ts';
import { admissionMode, readPatch, encodeNetworkInvite, decodeNetworkInvite, initProjectScaffold } from './config.ts';

export * from './runtime.ts';
export * from './config.ts';

const booleanFlags = ['json', 'auto-route', 'trust-sender', 'all', 'peek', 'wait'];
const stringFlags = [
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
];

function parse(argv: string[]) {
  return parseArgs({
    args: argv,
    allowPositionals: true,
    strict: true,
    options: {
      ...Object.fromEntries(booleanFlags.map(k => [k, { type: 'boolean' as const }])),
      ...Object.fromEntries(stringFlags.map(k => [k, { type: 'string' as const }])),
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
    if (!options.runtime && existsSync(join(stateDir, 'control.sock'))) {
      const result = await rpc(join(stateDir, 'control.sock'), argv);
      result.stdout.forEach(line => stdout(line));
      result.stderr.forEach(line => stderr(line));
      return result.code;
    }
    if (group === 'inbox') {
      let store: MailboxStore;
      if (options.runtime) {
        await options.runtime.start();
        store = options.runtime.mailbox;
      } else {
        // Offline access is serialized by the mailbox file lock, so it stays coherent even if
        // the daemon owns the state directory.
        const file = join(stateDir, 'config.json');
        const savedConfig = existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')) : {};
        store = new MailboxStore(join(stateDir, 'mailbox.json'), {
          legacyInboxPath:
            savedConfig.executorConfig?.inbox?.inboxPath ?? join(stateDir, 'inbox.json'),
        });
      }
      stdout(JSON.stringify(await runInboxCommand(store, action, target, v, options.signal), null, v.json ? undefined : 2));
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
    if (group === 'project') {
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
        if (action === 'up' && ['profile', 'patch', 'member', 'name', 'skills', 'port'].some(key => v[key] !== undefined)) {
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
      let payload = {};
      try {
        const parsed = JSON.parse(body);
        payload = parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : { value: parsed };
      } catch {
        // Plain text body
      }
      result = await runtime.emitter.emit({
        networkId: required(v.network, '--network'),
        kind: topic,
        subject: topic,
        body,
        payload,
        routing: {
          mode: group === 'broadcast' ? 'broadcast' : v['auto-route'] ? 'auto' : 'unicast',
          ...(v.to ? { targets: [String(v.to)] } : {}),
          topic,
        },
        filePaths: v.attach as string[] | undefined,
        priority: rawUrgency === 'critical' ? 'urgent' : (rawUrgency as 'normal' | 'high' | undefined),
      });
    } else if ((group === 'route' || group === 'source') && action === 'add') {
      if (owned && group === 'source') {
        throw new Error('Start dealer node up before adding a live source');
      }
      const config = v.config ? JSON.parse(readFileSync(String(v.config), 'utf8')) : {};
      for (const key of ['command', 'endpoint', 'model', 'url', 'cwd', 'path']) {
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
