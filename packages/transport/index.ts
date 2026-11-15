import { createServer, createConnection, type Socket, type Server } from 'node:net';
import { createSocket, type Socket as DatagramSocket } from 'node:dgram';
import { createReadStream, createWriteStream } from 'node:fs';
import {
  existsSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
} from 'node:fs';
import { mkdir, readdir, readFile, writeFile, rename, rm } from 'node:fs/promises';
import { tmpdir, networkInterfaces } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { once } from 'node:events';
import { execFile, spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { createInterface } from 'node:readline';
import {
  DeliveryTimeoutError,
  LocalDeliveryError,
  NetworkUnmountedError,
  OfflinePeerSpooledError,
  PeerUnreachableError,
  deriveNetworkKeys,
  generateNodeKeyPair,
  sealNetworkFrame,
  unsealNetworkFrame,
} from '../core-types/index.ts';
import { appendFileDurably, readDurableJournal, writeFileAtomically } from '../fs-atomic/index.ts';
import type {
  BlobAttachment,
  DerivedNetworkKeys,
  Disposable,
  InboundFrameEvent,
  MountedNetworkHandle,
  NetworkMountConfig,
  Result,
  SealedNetworkFrame,
  TransportService,
} from '../core-types/index.ts';
import type { Context, Plugin } from '../kernel/index.ts';

// BLAKE3's unkeyed, 32-byte digest. Incremental tree reduction keeps file hashing bounded.
const IV = new Uint32Array([
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
  0x5be0cd19,
]);
const PERM = [2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8];

interface HashOutput {
  cv: Uint32Array;
  words: Uint32Array;
  counter: number;
  length: number;
  flags: number;
}

function compress(o: HashOutput, root = false): Uint32Array {
  const s = new Uint32Array(16);
  s.set(o.cv);
  s.set(IV.subarray(0, 4), 8);
  s[12] = root ? 0 : o.counter >>> 0;
  s[13] = root ? 0 : Math.floor(o.counter / 0x100000000);
  s[14] = o.length;
  s[15] = o.flags | (root ? 8 : 0);
  let m = o.words;
  const rotate = (x: number, n: number) => (x >>> n) | (x << (32 - n));
  const g = (a: number, b: number, c: number, d: number, x: number, y: number) => {
    s[a] = s[a] + s[b] + x;
    s[d] = rotate(s[d] ^ s[a], 16);
    s[c] += s[d];
    s[b] = rotate(s[b] ^ s[c], 12);
    s[a] = s[a] + s[b] + y;
    s[d] = rotate(s[d] ^ s[a], 8);
    s[c] += s[d];
    s[b] = rotate(s[b] ^ s[c], 7);
  };
  for (let r = 0; r < 7; r++) {
    g(0, 4, 8, 12, m[0], m[1]);
    g(1, 5, 9, 13, m[2], m[3]);
    g(2, 6, 10, 14, m[4], m[5]);
    g(3, 7, 11, 15, m[6], m[7]);
    g(0, 5, 10, 15, m[8], m[9]);
    g(1, 6, 11, 12, m[10], m[11]);
    g(2, 7, 8, 13, m[12], m[13]);
    g(3, 4, 9, 14, m[14], m[15]);
    m = Uint32Array.from(PERM.map(i => m[i]));
  }
  for (let i = 0; i < 8; i++) {
    s[i] ^= s[i + 8];
    s[i + 8] ^= o.cv[i];
  }
  return s;
}

function chunkOutput(bytes: Buffer, counter: number): HashOutput {
  let cv = IV;
  let output!: HashOutput;
  for (let offset = 0; offset < Math.max(1, bytes.length); offset += 64) {
    const length = Math.min(64, bytes.length - offset);
    const block = Buffer.alloc(64);
    bytes.copy(block, 0, offset, offset + length);
    output = {
      cv,
      words: Uint32Array.from({ length: 16 }, (_, i) => block.readUInt32LE(i * 4)),
      counter,
      length,
      flags: (offset === 0 ? 1 : 0) | (offset + 64 >= bytes.length ? 2 : 0),
    };
    cv = compress(output).slice(0, 8);
  }
  return output;
}

function parent(left: Uint32Array, right: Uint32Array): HashOutput {
  const words = new Uint32Array(16);
  words.set(left);
  words.set(right, 8);
  return { cv: IV, words, counter: 0, length: 64, flags: 4 };
}

class Blake3Hasher {
  private pending = Buffer.alloc(0);
  private chunks = 0;
  private stack: Uint32Array[] = [];

  update(bytes: Uint8Array): void {
    let data = Buffer.concat([this.pending, bytes]);
    while (data.length > 1024) {
      let cv = compress(chunkOutput(data.subarray(0, 1024), this.chunks)).slice(0, 8);
      let total = ++this.chunks;
      while (total % 2 === 0) {
        cv = compress(parent(this.stack.pop()!, cv)).slice(0, 8);
        total /= 2;
      }
      this.stack.push(cv);
      data = data.subarray(1024);
    }
    this.pending = Buffer.from(data);
  }

  digest(): string {
    let output = chunkOutput(this.pending, this.chunks);
    for (let i = this.stack.length - 1; i >= 0; i--) {
      output = parent(this.stack[i], compress(output).slice(0, 8));
    }
    const words = compress(output, true);
    const result = Buffer.alloc(32);
    for (let i = 0; i < 8; i++) {
      result.writeUInt32LE(words[i], i * 4);
    }
    return result.toString('hex');
  }
}

export function blake3(bytes: Uint8Array): string {
  const h = new Blake3Hasher();
  h.update(bytes);
  return h.digest();
}

const MAX_FRAME = 8 * 1024 * 1024;
/** Maximum raw attachment bytes spooled alongside one relay entry (base64 inflates on the wire). */
const MAX_RELAY_SPOOL_BLOB_BYTES = 8 * 1024 * 1024;
const RELAY_SPOOL_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const MAX_RELAY_SPOOL_ENTRIES = 256;
const MAX_RELAY_SPOOL_TARGETS = 128;
const MAX_RELAY_SPOOL_TOTAL_ENTRIES = 1_024;
const MAX_RELAY_SPOOL_TOTAL_BYTES = 32 * 1024 * 1024;
/** A journal larger than the live spool bound is folded into one snapshot; normal writes append. */
const MAX_RELAY_SPOOL_JOURNAL_BYTES = 32 * 1024 * 1024;
const MAX_RELAY_SPOOL_SENDER_ENTRIES = 256;
const MAX_RELAY_SPOOL_KEY_LENGTH = 128;
const RELAY_SPOOL_SCHEMA = 'the-dealer-relay-spool/v1';
const GROUP = '239.255.42.99';
// A relay candidate means the advertised endpoint may be NAT-bound; fail over fast
// instead of waiting the full request timeout on every unicast.
const RELAY_CONNECT_TIMEOUT_MS = 1_500;
const BEACON_EXPIRY_GRACE_MS = 30_000;
// Relay responses carry one base64 blob inside a JSON packet, so the raw blob may only
// fill three quarters of the frame budget minus the packet envelope.
const RELAY_BLOB_LIMIT = Math.floor((MAX_FRAME - 4096) * 3 / 4);
const PRIVATE_IPV4 = /^(100\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.)/;
const TAILSCALE_COMMANDS = ['tailscale', '/Applications/Tailscale.app/Contents/MacOS/Tailscale'];
const TAILSCALE_CACHE_MS = 30_000;
let tailscalePeersCache: { readAt: number; peers: string[] } | undefined;

/** Tailscale's routed CGNAT range (100.64.0.0/10); these addresses stay reachable across subnets. */
export function isTailscaleIpv4Host(host: string): boolean {
  return /^100\.(6[4-9]|[7-9]\d|1[01]\d|12[0-7])\./.test(host);
}

function readTailscaleStatus(command: string): Promise<string> {
  return new Promise((resolve, reject) => {
    execFile(command, ['status', '--json'], { timeout: 1500, maxBuffer: 4 * 1024 * 1024 }, (error, stdout) => {
      if (error) {
        reject(error);
      } else {
        resolve(stdout);
      }
    });
  });
}

/** Online Tailscale peers as IPv4 addresses; cached so announcements do not spawn the CLI each tick. */
export async function discoverTailscaleIpv4Peers(): Promise<string[]> {
  const now = Date.now();
  if (tailscalePeersCache && now - tailscalePeersCache.readAt < TAILSCALE_CACHE_MS) {
    return [...tailscalePeersCache.peers];
  }
  const peers = new Set<string>();
  for (const command of TAILSCALE_COMMANDS) {
    let status: { Peer?: Record<string, { Online?: boolean; TailscaleIPs?: unknown[] }> };
    try {
      status = JSON.parse(await readTailscaleStatus(command));
    } catch {
      // Tailscale is optional; a missing binary or a stopped backend falls through.
      continue;
    }
    for (const peer of Object.values(status?.Peer ?? {})) {
      if (peer?.Online !== true || !Array.isArray(peer.TailscaleIPs)) {
        continue;
      }
      for (const address of peer.TailscaleIPs) {
        if (typeof address === 'string' && PRIVATE_IPV4.test(address)) {
          peers.add(address);
        }
      }
    }
    break;
  }
  tailscalePeersCache = { readAt: now, peers: [...peers] };
  return [...peers];
}

/** IPv4 broadcast targets for the LAN links that carry unicast discovery frames. */
export function lanBroadcastAddresses(
  interfaces = networkInterfaces(),
): { address: string; broadcast: string }[] {
  const targets: { address: string; broadcast: string }[] = [];
  for (const [name, addresses] of Object.entries(interfaces)) {
    if (!/^(en|eth|wlan)/.test(name)) {
      continue;
    }
    for (const address of addresses ?? []) {
      if (
        address.family !== 'IPv4' ||
        address.internal ||
        !address.netmask ||
        address.netmask === '255.255.255.255'
      ) {
        continue;
      }
      const broadcast = ipv4Broadcast(address.address, address.netmask);
      if (broadcast) {
        targets.push({ address: address.address, broadcast });
      }
    }
  }
  return targets;
}

function ipv4Broadcast(address: string, netmask: string): string | undefined {
  const parts = address.split('.').map(Number);
  const mask = netmask.split('.').map(Number);
  if (
    parts.length !== 4 ||
    mask.length !== 4 ||
    [...parts, ...mask].some(part => !Number.isInteger(part) || part < 0 || part > 255)
  ) {
    return undefined;
  }
  return parts.map((part, index) => part | (~mask[index] & 255)).join('.');
}

function packet(value: unknown): Buffer {
  const body = Buffer.from(JSON.stringify(value));
  if (body.length > MAX_FRAME) {
    throw new LocalDeliveryError('Frame exceeds limit');
  }
  const size = Buffer.alloc(4);
  size.writeUInt32BE(body.length);
  return Buffer.concat([size, body]);
}

/** A stream ending before its declared frame length is a connection failure, not a protocol decision. */
class TruncatedStreamError extends Error {
  readonly code = 'truncated_stream';

  constructor() {
    super('Truncated stream');
    this.name = 'TruncatedStreamError';
  }
}

/** One oversized or unparsable frame body is a deterministic protocol failure. */
class SocketReader {
  private iterator: AsyncIterator<Buffer>;
  private buffered = Buffer.alloc(0);

  constructor(socket: Socket) {
    this.iterator = socket[Symbol.asyncIterator]();
  }

  async exact(size: number): Promise<Buffer> {
    while (this.buffered.length < size) {
      const next = await this.iterator.next();
      if (next.done) {
        throw new TruncatedStreamError();
      }
      this.buffered = Buffer.concat([this.buffered, next.value]);
    }
    const result = this.buffered.subarray(0, size);
    this.buffered = this.buffered.subarray(size);
    return result;
  }

  async json(): Promise<any> {
    const size = (await this.exact(4)).readUInt32BE();
    if (size > MAX_FRAME) {
      throw new LocalDeliveryError('Frame exceeds limit');
    }
    const body = await this.exact(size);
    try {
      return JSON.parse(body.toString());
    } catch {
      throw new LocalDeliveryError('Malformed relay response');
    }
  }

  async *remaining(): AsyncGenerator<Buffer> {
    if (this.buffered.length) {
      yield this.buffered;
      this.buffered = Buffer.alloc(0);
    }
    while (true) {
      const next = await this.iterator.next();
      if (next.done) {
        return;
      }
      yield next.value;
    }
  }
}

export interface TransportPluginOptions {
  localNodeId?: string;
  discoveryDir?: string;
  /** Durable directory for the always-on relay store-and-forward spool and blob cache. */
  spoolDir?: string;
  listenPort?: number;
  listenHost?: string;
  advertisedHost?: string;
  discoveryInterface?: string;
  multicast?: boolean;
  mDnsPort?: number;
  discoveryIntervalMs?: number;
  socketTimeoutMs?: number;
  /** Deterministic Tailscale peer source for tests; defaults to the local `tailscale status` CLI. */
  tailscalePeers?: () => Promise<readonly string[]> | readonly string[];
}

interface RelayPending {
  resolve: (message: any) => void;
  reject: (error: Error) => void;
}

/** A live tunnel socket and the relay requests waiting for its owner's replies. */
interface Tunnel {
  socket: Socket;
  pending: Map<string, RelayPending>;
}

/** How this node reaches a peer whose advertised endpoint is not routable from here. */
type PeerFallback = { tunnel: Tunnel } | { mount: Mount; seed: string };

/** Relay attempt policy: spooling and attachment bytes are only offered to relay seeds. */
interface RelayRequestOptions {
  spoolIfOffline?: boolean;
  spoolKey?: string;
  blobHashes?: readonly string[];
}

/**
 * Connection-class failures are the only errors a relay fallback may reinterpret as an
 * offline peer. Protocol, framing, signature, and local I/O errors stay deterministic.
 */
const CONNECTION_FAILURE_CODES = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'EHOSTUNREACH',
  'ENETUNREACH',
  // Darwin/BSD report a same-subnet peer that fails ARP as EHOSTDOWN and a local
  // interface transition as ENETDOWN; other POSIX stacks use ENONET/ESHUTDOWN.
  'EHOSTDOWN',
  'ENETDOWN',
  'ENONET',
  'ESHUTDOWN',
  'EAI_AGAIN',
  'ENOTFOUND',
  'EPIPE',
  'ECONNABORTED',
  'ERR_STREAM_PREMATURE_CLOSE',
  'truncated_stream',
  'delivery_timeout',
]);

const CONNECTION_FAILURE_MESSAGES = new Set([
  'Truncated stream',
  'Transport timeout',
  'Relay tunnel timeout',
  'Relay tunnel closed',
  'Relay target unreachable',
]);

/**
 * One classification for every reachability symptom, including the AggregateError that
 * Node's Happy Eyeballs can raise when each per-address connect attempt failed. A mixed
 * aggregate is deterministic unless every member is itself connection-class.
 */
function isConnectionFailure(error: unknown, depth = 0): boolean {
  let current: unknown = error;
  for (let hop = depth; hop < 5 && current !== undefined && current !== null; hop += 1) {
    if (typeof current !== 'object') {
      return false;
    }
    const candidate = current as {
      code?: unknown;
      message?: unknown;
      errors?: unknown;
      cause?: unknown;
    };
    if (typeof candidate.code === 'string' && CONNECTION_FAILURE_CODES.has(candidate.code)) {
      return true;
    }
    if (
      typeof candidate.message === 'string' &&
      CONNECTION_FAILURE_MESSAGES.has(candidate.message)
    ) {
      return true;
    }
    if (
      Array.isArray(candidate.errors) &&
      candidate.errors.length > 0 &&
      candidate.errors.every(entry => isConnectionFailure(entry, hop + 1))
    ) {
      return true;
    }
    current = candidate.cause;
  }
  return false;
}

function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

/** One sealed frame (and its verified attachment cache) parked on an always-on relay seed. */
export interface SpooledRelayEntry {
  id: string;
  targetNodeId: string;
  networkId: string;
  frame: SealedNetworkFrame;
  /** Serialized sealed-frame bytes, counted against the global spool byte budget. */
  frameBytes: number;
  /** hash -> raw byte length for attachments already verified into the network blob cache. */
  blobHashes?: Record<string, number>;
  spooledAt: string;
  expiresAt: string;
}

interface SpooledRelayBlob {
  size: number;
  expiresAt: string;
}

interface RelaySpoolFile {
  schema: typeof RELAY_SPOOL_SCHEMA;
  networkId: string;
  entries: SpooledRelayEntry[];
  /** Post-drain attachment cache; kept until the originating entry's TTL so late fetches work. */
  blobs?: Record<string, SpooledRelayBlob>;
}

interface Mount {
  config: NetworkMountConfig;
  keys: DerivedNetworkKeys;
  peers: Map<string, string>;
  /** Latest valid sealed discovery beacon per sender, including this node; gossiped to seeds. */
  beacons: Map<string, { frame: SealedNetworkFrame; expires: number }>;
  /** Peer node -> seed endpoint that introduced or can relay to that peer. */
  relays: Map<string, string>;
  /** Tunnels peers opened towards this node, keyed by peer node, so relays can push back. */
  inboundTunnels: Map<string, Tunnel>;
  /** Persistent tunnels this node keeps open towards its seeds, keyed by verified seed node. */
  outboundTunnels: Map<string, { socket: Socket; seed: string; acked: boolean }>;
  /** Store-and-forward frames waiting for an offline target, keyed by target node, FIFO. */
  relaySpool: Map<string, SpooledRelayEntry[]>;
  /** Attachment bytes retained for late blob fetches after their frame entry drained. */
  spoolBlobCache: Map<string, SpooledRelayBlob>;
  /** Durable spool snapshot for this network, when spoolDir is configured. */
  spoolFile?: string;
  file: string;
  lastSeedProbeAt: number;
}

type Handler = (
  event: InboundFrameEvent & { rawBytes?: Uint8Array },
) => Promise<Uint8Array | void> | Uint8Array | void;

function isPrivateLanIpv4Host(host: string): boolean {
  const parts = host.split('.');
  if (parts.length !== 4 || parts.some(part => !/^(0|[1-9]\d{0,2})$/.test(part))) {
    return false;
  }
  const [a, b, c, d] = parts.map(Number);
  if (a > 255 || b > 255 || c > 255 || d > 255) {
    return false;
  }
  return a === 10 || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168);
}

/** Shared by the listener and offline invite generation. */
export function defaultAdvertisedHost(
  options: TransportPluginOptions = {},
  interfaces = networkInterfaces(),
): string {
  if (options.advertisedHost) return options.advertisedHost;
  if (options.listenHost && !['0.0.0.0', '::'].includes(options.listenHost)) return options.listenHost;
  const candidates = Object.entries(interfaces).flatMap(([name, addresses]) =>
    (addresses ?? []).filter(address => address.family === 'IPv4' && !address.internal &&
      !/^(169\.254|198\.(18|19))\./.test(address.address)).map(address => ({ name, address: address.address })),
  );
  const lan = candidates.find(({ name, address }) => /^(en|eth|wlan)/.test(name) &&
    isPrivateLanIpv4Host(address));
  return lan?.address ?? candidates[0]?.address ?? '127.0.0.1';
}

export class P2PTransportService implements TransportService {
  readonly localNodeId: string;
  readonly identityKeyPair?: ReturnType<typeof generateNodeKeyPair>;
  endpointAddr?: string;
  private options: TransportPluginOptions;
  private ctx?: Context;
  private mounts = new Map<string, Mount>();
  private handlers = new Set<Handler>();
  private mountHandlers = new Set<() => void | Promise<void>>();
  private server?: Server;
  private starting?: Promise<void>;
  private udp?: DatagramSocket;
  private discoveryReplyAt = new Map<string, number>();
  private timer?: ReturnType<typeof setInterval>;
  private tick?: Promise<void>;
  private closed = false;
  private sockets = new Set<Socket>();
  private blobs = new Map<string, { path: string; size: number }>();
  /** Per-target drain locks: only one spool drain may run for a (network, target) pair. */
  private spoolDrains = new Map<string, Promise<void>>();
  /** Last drain attempt per target, so repeated beacons never hammer an offline endpoint. */
  private spoolDrainAttempts = new Map<string, number>();
  /** Relay-spooled attachment hashes, scoped by network swarm topic for strict isolation. */
  private spooledBlobs = new Set<string>();

  constructor(options: TransportPluginOptions = {}, ctx?: Context) {
    this.options = options;
    this.ctx = ctx;
    this.identityKeyPair = options.localNodeId ? undefined : generateNodeKeyPair();
    this.localNodeId = options.localNodeId ?? this.identityKeyPair!.nodeId;
  }

  private track(socket: Socket): Socket {
    if (this.closed) {
      socket.destroy();
      return socket;
    }
    this.sockets.add(socket);
    socket.once('close', () => this.sockets.delete(socket));
    socket.setTimeout(this.options.socketTimeoutMs ?? 90_000, () => {
      // A TCP handshake that never completed proves zero application bytes left this
      // kernel: the peer is simply unreachable, so this must not be reported as a
      // possibly-executed in-doubt send. Once connected, a silent reply is ambiguous.
      socket.destroy(
        socket.connecting
          ? Object.assign(new Error('connect ETIMEDOUT'), { code: 'ETIMEDOUT' })
          : new DeliveryTimeoutError('Transport timeout'),
      );
    });
    socket.on('error', () => {});
    return socket;
  }

  private async start(port?: number): Promise<void> {
    if (this.closed) {
      throw new Error('Transport disposed');
    }
    if (this.starting) {
      return this.starting;
    }
    this.starting = (async () => {
      const server = createServer(socket => {
        this.track(socket);
        void this.receive(socket).catch(() => socket.destroy());
      });
      this.server = server;
      server.listen(
        port ?? this.options.listenPort ?? 0,
        this.options.listenHost ?? '0.0.0.0',
      );
      await once(server, 'listening');
      const address = server.address();
      if (!address || typeof address === 'string') {
        throw new Error('Invalid listener address');
      }
      const host = defaultAdvertisedHost(this.options);
      this.endpointAddr = `${host.includes(':') ? `[${host}]` : host}:${address.port}`;
      if (this.options.multicast !== false) {
        const udp = createSocket({ type: 'udp4', reuseAddr: true });
        this.udp = udp;
        udp.on('error', () => {}); // Registry discovery remains available on hosts disabling multicast.
        udp.on('message', (bytes, rinfo) => {
          try {
            const frame = JSON.parse(bytes.toString());
            const { mount, reply } = this.learnWithMeta(frame, rinfo?.address);
            if (mount && frame.senderNodeId !== this.localNodeId && !reply && rinfo?.address && rinfo?.port) {
              // Answer the sender directly so unicast discovery over Tailscale or LAN
              // completes a bidirectional handshake in one round trip. Never answer a
              // reply frame: that would start an endless discovery ping-pong. Multicast
              // announces arrive every tick, so bound one answer per host per probe
              // window instead of multiplying traffic by the peer count.
              const now = Date.now();
              const lastReply = this.discoveryReplyAt.get(rinfo.address) ?? Number.NEGATIVE_INFINITY;
              if (now - lastReply >= Math.max(this.options.discoveryIntervalMs ?? 50, 2000)) {
                this.discoveryReplyAt.set(rinfo.address, now);
                udp.send(Buffer.from(JSON.stringify(this.discoveryFrame(mount, true))), rinfo.port, rinfo.address);
              }
            }
          } catch {
            // Unauthenticated discovery
          }
        });
        udp.bind(this.options.mDnsPort ?? 45873, () => {
          try {
            udp.setBroadcast(true);
          } catch {
            // Hosts without broadcast support keep unicast seeds.
          }
          try {
            udp.setMulticastLoopback(true);
          } catch {
            // Multicast unavailable
          }
          if (this.options.discoveryInterface) {
            try {
              udp.addMembership(GROUP, this.options.discoveryInterface);
              udp.setMulticastInterface(this.options.discoveryInterface);
            } catch {
              // A configured interface without multicast support must not fail startup.
            }
            return;
          }
          for (const address of ['127.0.0.1', ...lanBroadcastAddresses().map(target => target.address)]) {
            try {
              udp.addMembership(GROUP, address);
            } catch {
              // An interface without multicast support must not fail startup.
            }
          }
        });
      }
      this.timer = setInterval(() => {
        if (!this.tick) {
          this.tick = this.discover().finally(() => {
            this.tick = undefined;
          });
        }
      }, this.options.discoveryIntervalMs ?? 50);
      this.timer.unref();
    })();
    return this.starting;
  }

  private get(networkId: string): Mount {
    const m = this.mounts.get(networkId);
    if (!m) {
      throw new NetworkUnmountedError(networkId);
    }
    return m;
  }

  async mountNetwork(config: NetworkMountConfig): Promise<MountedNetworkHandle> {
    if (this.mounts.has(config.networkId)) {
      throw new Error('Network already mounted');
    }
    await this.start(config.listenPort);
    if (this.closed) {
      throw new Error('Transport disposed');
    }
    if (this.mounts.has(config.networkId)) {
      throw new Error('Network already mounted');
    }
    const keys = deriveNetworkKeys(config.networkId, config.passphrase);
    const dir = join(
      this.options.discoveryDir ?? join(tmpdir(), 'the-dealer-mesh'),
      keys.swarmTopicId,
    );
    await mkdir(dir, { recursive: true, mode: 0o700 });
    const file = join(dir, `${createHash('sha256').update(this.localNodeId).digest('hex')}.json`);
    const mount: Mount = {
      config: structuredClone(config),
      keys,
      peers: new Map<string, string>(),
      beacons: new Map<string, { frame: SealedNetworkFrame; expires: number }>(),
      relays: new Map<string, string>(),
      inboundTunnels: new Map<string, Tunnel>(),
      outboundTunnels: new Map<string, { socket: Socket; seed: string; acked: boolean }>(),
      relaySpool: new Map<string, SpooledRelayEntry[]>(),
      spoolBlobCache: new Map<string, SpooledRelayBlob>(),
      ...(this.options.spoolDir
        ? { spoolFile: join(resolve(this.options.spoolDir), `${keys.swarmTopicId}.json`) }
        : {}),
      file,
      lastSeedProbeAt: Number.NEGATIVE_INFINITY,
    };
    this.loadRelaySpool(mount);
    this.mounts.set(config.networkId, mount);
    try {
      await this.announce(mount);
      await this.discover(false);
      await this.notifyMount();
    } catch (error) {
      this.mounts.delete(config.networkId);
      await rm(file, { force: true });
      throw error;
    }
    return {
      networkId: config.networkId,
      swarmTopicId: keys.swarmTopicId,
      endpointAddr: this.endpointAddr,
      dispose: () => this.unmountNetwork(config.networkId),
    };
  }

  getNetworkConfig(networkId: string): NetworkMountConfig | undefined {
    const mount = this.mounts.get(networkId);
    return mount ? structuredClone(mount.config) : undefined;
  }

  onNetworksChanged(handler: () => void | Promise<void>): Disposable {
    this.mountHandlers.add(handler);
    return {
      dispose: () => {
        this.mountHandlers.delete(handler);
      },
    };
  }

  private async notifyMount(): Promise<void> {
    for (const handler of this.mountHandlers) {
      await handler();
    }
  }

  async unmountNetwork(networkId: string): Promise<void> {
    const m = this.mounts.get(networkId);
    if (!m) {
      return;
    }
    this.mounts.delete(networkId);
    // A stale mount must not keep serving relay deliveries after it is unmounted.
    for (const tunnel of m.outboundTunnels.values()) {
      tunnel.socket.destroy();
    }
    for (const tunnel of m.inboundTunnels.values()) {
      tunnel.socket.destroy();
    }
    m.outboundTunnels.clear();
    m.inboundTunnels.clear();
    m.beacons.clear();
    m.relays.clear();
    await this.tick;
    // Fold the append-only delta into one snapshot at shutdown so the next start reads a
    // compact base; live admission/drain never performs this full rewrite.
    this.persistRelaySpool(m);
    await rm(m.file, { force: true });
    await this.notifyMount();
  }

  listMountedNetworks(): readonly string[] {
    return [...this.mounts.keys()];
  }

  listPeerEndpoints(networkId: string): ReadonlyMap<string, string> {
    return new Map(this.get(networkId).peers);
  }

  registerPeerEndpoint(networkId: string, nodeId: string, address: string): void {
    const parsed = parseEndpoint(address);
    if (nodeId === this.localNodeId) {
      return;
    }
    const peers = this.get(networkId).peers;
    const existing = peers.get(nodeId);
    const existingHost = existing ? parseEndpoint(existing).host : undefined;
    // The transport owns endpoint reachability: once a peer is reachable through a
    // routed Tailscale address, a later private LAN address may only update its port.
    peers.set(
      nodeId,
      existingHost && isTailscaleIpv4Host(existingHost) && isPrivateLanIpv4Host(parsed.host)
        ? `${existingHost}:${parsed.port}`
        : address,
    );
  }

  /** Make a file addressable by its content hash, exactly like `shareFile` does. */
  registerBlob(hash: string, filePath: string): void {
    if (!/^[a-f0-9]{64}$/.test(hash)) {
      throw new Error('Invalid blob hash');
    }
    const info = statSync(filePath);
    if (!info.isFile()) {
      throw new Error('Blob path is not a file');
    }
    this.blobs.set(hash, { path: filePath, size: info.size });
  }

  /** Content-addressed blob cache is network-scoped: a spooled hash never leaks across mounts. */
  private spoolBlobDirectory(m: Mount): string {
    return this.options.spoolDir
      ? join(resolve(this.options.spoolDir), 'blobs', m.keys.swarmTopicId)
      : join(tmpdir(), 'the-dealer-relay-blobs', m.keys.swarmTopicId);
  }

  private loadRelaySpool(m: Mount): void {
    const spoolFile = m.spoolFile;
    if (!spoolFile) {
      return;
    }
    const journalFile = this.relaySpoolJournalFile(m)!;
    const hadSnapshot = existsSync(spoolFile);
    const hadJournal = existsSync(journalFile);
    const now = Date.now();
    if (hadSnapshot) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(readFileSync(spoolFile, 'utf8'));
      } catch {
        throw new Error('Invalid relay spool store');
      }
      if (
        typeof parsed !== 'object' ||
        parsed === null ||
        Array.isArray(parsed) ||
        (parsed as RelaySpoolFile).schema !== RELAY_SPOOL_SCHEMA ||
        (parsed as RelaySpoolFile).networkId !== m.config.networkId ||
        !Array.isArray((parsed as RelaySpoolFile).entries)
      ) {
        throw new Error('Invalid relay spool store');
      }
      const entries: SpooledRelayEntry[] = [];
      for (const raw of (parsed as RelaySpoolFile).entries) {
        const entry = this.parseStoredSpoolEntry(raw, m, now);
        if (entry) {
          entries.push(entry);
        }
      }
      entries.sort(
        (a, b) => Date.parse(a.spooledAt) - Date.parse(b.spooledAt) || a.id.localeCompare(b.id),
      );
      for (const entry of entries) {
        this.insertSpoolEntry(m, entry);
      }
      const savedBlobs = (parsed as RelaySpoolFile).blobs;
      if (savedBlobs !== undefined) {
        if (typeof savedBlobs !== 'object' || savedBlobs === null || Array.isArray(savedBlobs)) {
          throw new Error('Invalid relay spool store');
        }
        for (const [hash, meta] of Object.entries(savedBlobs)) {
          if (
            !/^[a-f0-9]{64}$/.test(hash) ||
            typeof meta !== 'object' ||
            meta === null ||
            !Number.isSafeInteger(meta.size) ||
            meta.size < 0 ||
            meta.size > MAX_RELAY_SPOOL_BLOB_BYTES ||
            typeof meta.expiresAt !== 'string' ||
            !Number.isFinite(Date.parse(meta.expiresAt))
          ) {
            throw new Error('Invalid relay spool store');
          }
          if (Date.parse(meta.expiresAt) <= now) {
            continue;
          }
          m.spoolBlobCache.set(hash, { size: meta.size, expiresAt: meta.expiresAt });
        }
      }
    }
    // Apply the append-only delta written since the last snapshot, then compact once at
    // startup. Live admission/drain only append one record, so the seed never rewrites the
    // whole spool on the request path.
    this.loadRelaySpoolJournal(m, now);
    // Files from a previous process are addressable and must participate in orphan GC.
    const directory = this.spoolBlobDirectory(m);
    if (existsSync(directory)) {
      for (const name of readdirSync(directory)) {
        if (/^[a-f0-9]{64}$/.test(name)) {
          this.spooledBlobs.add(this.spoolBlobKey(m, name));
        }
      }
    }
    // Expired entries and orphaned blobs are dropped from the durable snapshot during mount.
    let changed = this.pruneRelaySpool(m);
    // A tampered snapshot must not bypass the same capacity bounds as live admission.
    changed = this.enforceRelaySpoolBudget(m) || changed;
    // A crash can leave blob files without any snapshot/journal reference. The directory
    // scan above makes those orphans collectable; only write a snapshot when state changed.
    if (hadSnapshot || hadJournal || changed) {
      this.persistRelaySpool(m);
    }
  }

  private parseStoredSpoolEntry(
    raw: unknown,
    m: Mount,
    now: number,
  ): SpooledRelayEntry | undefined {
    const entry = raw as Partial<SpooledRelayEntry> & {
      blobHashes?: unknown;
      frame?: SpooledRelayEntry['frame'];
    };
    if (
      typeof entry !== 'object' ||
      entry === null ||
      typeof entry.id !== 'string' ||
      !entry.id ||
      typeof entry.targetNodeId !== 'string' ||
      !entry.targetNodeId ||
      entry.networkId !== m.config.networkId ||
      typeof entry.spooledAt !== 'string' ||
      typeof entry.expiresAt !== 'string' ||
      !Number.isFinite(Date.parse(entry.expiresAt)) ||
      entry.frame?.protocol !== 'the-dealer-wire/v1' ||
      entry.frame.swarmTopicId !== m.keys.swarmTopicId ||
      typeof entry.frame.nonce !== 'string'
    ) {
      throw new Error('Invalid relay spool store');
    }
    if (Date.parse(entry.expiresAt) <= now) {
      return undefined;
    }
    const blobHashes: Record<string, number> = {};
    if (entry.blobHashes !== undefined) {
      if (
        typeof entry.blobHashes !== 'object' ||
        entry.blobHashes === null ||
        Array.isArray(entry.blobHashes)
      ) {
        throw new Error('Invalid relay spool store');
      }
      for (const [hash, size] of Object.entries(entry.blobHashes)) {
        if (
          !/^[a-f0-9]{64}$/.test(hash) ||
          !Number.isSafeInteger(size) ||
          (size as number) < 0 ||
          (size as number) > MAX_RELAY_SPOOL_BLOB_BYTES
        ) {
          throw new Error('Invalid relay spool store');
        }
        blobHashes[hash] = size as number;
      }
    }
    return {
      id: entry.id,
      targetNodeId: entry.targetNodeId,
      networkId: m.config.networkId,
      frame: entry.frame,
      frameBytes:
        Number.isSafeInteger(entry.frameBytes) && (entry.frameBytes as number) > 0
          ? entry.frameBytes as number
          : Buffer.byteLength(JSON.stringify(entry.frame)),
      ...(Object.keys(blobHashes).length ? { blobHashes } : {}),
      spooledAt: entry.spooledAt,
      expiresAt: entry.expiresAt,
    };
  }

  private insertSpoolEntry(m: Mount, entry: SpooledRelayEntry): void {
    const queue = m.relaySpool.get(entry.targetNodeId) ?? [];
    queue.push(entry);
    m.relaySpool.set(entry.targetNodeId, queue);
    queue.sort(
      (a, b) => Date.parse(a.spooledAt) - Date.parse(b.spooledAt) || a.id.localeCompare(b.id),
    );
  }

  private loadRelaySpoolJournal(m: Mount, now: number): void {
    const journalFile = this.relaySpoolJournalFile(m);
    if (!journalFile) {
      return;
    }
    for (const raw of readDurableJournal(journalFile)) {
      if (typeof raw !== 'object' || raw === null || typeof (raw as { op?: unknown }).op !== 'string') {
        throw new Error('Invalid relay spool store');
      }
      const record = raw as {
        op: string;
        entry?: unknown;
        targetNodeId?: unknown;
        id?: unknown;
        blobs?: unknown;
      };
      if (record.op === 'upsert') {
        const entry = this.parseStoredSpoolEntry(record.entry, m, now);
        if (entry) {
          this.dropSpoolEntry(m, entry.targetNodeId, entry.id);
          this.insertSpoolEntry(m, entry);
        }
      } else if (record.op === 'remove') {
        if (typeof record.targetNodeId !== 'string' || typeof record.id !== 'string') {
          throw new Error('Invalid relay spool store');
        }
        this.dropSpoolEntry(m, record.targetNodeId, record.id);
      } else if (record.op === 'drain') {
        if (typeof record.targetNodeId !== 'string' || typeof record.id !== 'string') {
          throw new Error('Invalid relay spool store');
        }
        this.dropSpoolEntry(m, record.targetNodeId, record.id);
        this.mergeRelaySpoolCache(m, record.blobs, now);
      } else if (record.op === 'cache') {
        this.mergeRelaySpoolCache(m, record.blobs, now);
      } else if (record.op === 'uncache') {
        if (!Array.isArray(record.blobs) || record.blobs.some(hash => typeof hash !== 'string')) {
          throw new Error('Invalid relay spool store');
        }
        for (const hash of record.blobs) {
          m.spoolBlobCache.delete(hash);
        }
      } else {
        throw new Error('Invalid relay spool store');
      }
    }
  }

  private mergeRelaySpoolCache(m: Mount, raw: unknown, now: number): void {
    if (raw === undefined) {
      return;
    }
    if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
      throw new Error('Invalid relay spool store');
    }
    for (const [hash, meta] of Object.entries(raw)) {
      if (
        !/^[a-f0-9]{64}$/.test(hash) ||
        typeof meta !== 'object' ||
        meta === null ||
        !Number.isSafeInteger((meta as SpooledRelayBlob).size) ||
        (meta as SpooledRelayBlob).size < 0 ||
        (meta as SpooledRelayBlob).size > MAX_RELAY_SPOOL_BLOB_BYTES ||
        typeof (meta as SpooledRelayBlob).expiresAt !== 'string' ||
        !Number.isFinite(Date.parse((meta as SpooledRelayBlob).expiresAt))
      ) {
        throw new Error('Invalid relay spool store');
      }
      if (Date.parse((meta as SpooledRelayBlob).expiresAt) <= now) {
        continue;
      }
      m.spoolBlobCache.set(hash, {
        size: (meta as SpooledRelayBlob).size,
        expiresAt: (meta as SpooledRelayBlob).expiresAt,
      });
    }
  }

  private relaySpoolJournalFile(m: Mount): string | undefined {
    return m.spoolFile ? `${m.spoolFile}.journal` : undefined;
  }

  private appendRelaySpoolRecord(m: Mount, record: Record<string, unknown>): void {
    const journalFile = this.relaySpoolJournalFile(m);
    if (!journalFile) {
      return;
    }
    appendFileDurably(journalFile, `${JSON.stringify(record)}\n`);
  }

  private relaySpoolJournalOverBudget(m: Mount): boolean {
    const journalFile = this.relaySpoolJournalFile(m);
    if (!journalFile) {
      return false;
    }
    try {
      return statSync(journalFile).size > MAX_RELAY_SPOOL_JOURNAL_BYTES;
    } catch {
      return false;
    }
  }

  /** Compact the base snapshot and truncate the journal; called at load and shutdown only. */
  private persistRelaySpool(m: Mount): void {
    const spoolFile = m.spoolFile;
    if (!spoolFile) {
      return;
    }
    const state: RelaySpoolFile = {
      schema: RELAY_SPOOL_SCHEMA,
      networkId: m.config.networkId,
      entries: [...m.relaySpool.values()].flat(),
      ...(m.spoolBlobCache.size
        ? { blobs: Object.fromEntries(m.spoolBlobCache) }
        : {}),
    };
    writeFileAtomically(spoolFile, JSON.stringify(state));
    rmSync(`${spoolFile}.journal`, { force: true });
  }

  /** Drop expired entries/cache, then enforce every global and per-target spool bound. */
  private pruneRelaySpool(m: Mount, journal = false): boolean {
    const now = Date.now();
    let changed = false;
    for (const [target, entries] of m.relaySpool) {
      const live = entries.filter(entry => Date.parse(entry.expiresAt) > now);
      if (live.length !== entries.length) {
        changed = true;
        if (journal) {
          for (const entry of entries) {
            if (Date.parse(entry.expiresAt) <= now) {
              this.appendRelaySpoolRecord(m, {
                op: 'remove',
                targetNodeId: target,
                id: entry.id,
              });
            }
          }
        }
        if (live.length) {
          m.relaySpool.set(target, live);
        } else {
          m.relaySpool.delete(target);
        }
      }
    }
    const uncached: string[] = [];
    for (const [hash, meta] of m.spoolBlobCache) {
      if (Date.parse(meta.expiresAt) <= now) {
        m.spoolBlobCache.delete(hash);
        uncached.push(hash);
        changed = true;
      }
    }
    if (journal && uncached.length) {
      this.appendRelaySpoolRecord(m, { op: 'uncache', blobs: uncached });
    }
    return this.collectSpoolBlobFiles(m) || changed;
  }

  /** Delete every cached blob file no live entry or unexpired post-drain cache refers to. */
  private collectSpoolBlobFiles(m: Mount): boolean {
    const referenced = new Set<string>();
    for (const entries of m.relaySpool.values()) {
      for (const entry of entries) {
        for (const hash of Object.keys(entry.blobHashes ?? {})) {
          referenced.add(hash);
        }
      }
    }
    for (const hash of m.spoolBlobCache.keys()) {
      referenced.add(hash);
    }
    let changed = false;
    const directory = this.spoolBlobDirectory(m);
    const prefix = `${m.keys.swarmTopicId}\n`;
    for (const key of [...this.spooledBlobs]) {
      if (!key.startsWith(prefix)) {
        continue;
      }
      const hash = key.slice(prefix.length);
      if (referenced.has(hash)) {
        continue;
      }
      rmSync(join(directory, hash), { force: true });
      this.spooledBlobs.delete(key);
      changed = true;
    }
    for (const hash of m.spoolBlobCache.keys()) {
      if (!existsSync(join(directory, hash))) {
        m.spoolBlobCache.delete(hash);
        this.spooledBlobs.delete(this.spoolBlobKey(m, hash));
        changed = true;
      }
    }
    return changed;
  }

  /**
   * Keep a drained entry's attachment bytes until its original TTL: the target commonly
   * fetches blobs after its gate/dispatcher receipt, when the original sender may be gone.
   */
  private retainSpoolBlobs(m: Mount, entry: SpooledRelayEntry): void {
    for (const [hash, size] of Object.entries(entry.blobHashes ?? {})) {
      const existing = m.spoolBlobCache.get(hash);
      if (!existing || Date.parse(existing.expiresAt) < Date.parse(entry.expiresAt)) {
        m.spoolBlobCache.set(hash, { size, expiresAt: entry.expiresAt });
      }
    }
  }

  /** Stable, sender-scoped spool identity from caller-owned opaque data; never parses frames. */
  private relaySpoolEntryId(frame: SealedNetworkFrame, spoolKey?: string): string {
    return `${frame.senderNodeId}:${spoolKey ?? `nonce:${frame.nonce}`}`;
  }

  private relaySpoolBytes(m: Mount): number {
    const sizes = new Map<string, number>();
    let frameBytes = 0;
    for (const entries of m.relaySpool.values()) {
      for (const entry of entries) {
        frameBytes += entry.frameBytes;
        for (const [hash, size] of Object.entries(entry.blobHashes ?? {})) {
          sizes.set(hash, size);
        }
      }
    }
    for (const [hash, meta] of m.spoolBlobCache) {
      if (!sizes.has(hash)) {
        sizes.set(hash, meta.size);
      }
    }
    let total = frameBytes;
    for (const size of sizes.values()) {
      total += size;
    }
    return total;
  }

  private entryCount(m: Mount, predicate: (entry: SpooledRelayEntry) => boolean): number {
    let count = 0;
    for (const entries of m.relaySpool.values()) {
      count += entries.filter(predicate).length;
    }
    return count;
  }

  private oldestSpoolEntry(
    m: Mount,
    predicate: (target: string, entry: SpooledRelayEntry) => boolean,
  ): { target: string; entry: SpooledRelayEntry } | undefined {
    let oldest: { target: string; entry: SpooledRelayEntry } | undefined;
    for (const [target, entries] of m.relaySpool) {
      for (const entry of entries) {
        if (!predicate(target, entry)) {
          continue;
        }
        if (
          !oldest ||
          Date.parse(entry.spooledAt) < Date.parse(oldest.entry.spooledAt) ||
          (entry.spooledAt === oldest.entry.spooledAt && entry.id < oldest.entry.id)
        ) {
          oldest = { target, entry };
        }
      }
    }
    return oldest;
  }

  private dropSpoolEntry(m: Mount, target: string, id: string): void {
    const entries = m.relaySpool.get(target);
    if (!entries) {
      return;
    }
    const next = entries.filter(entry => entry.id !== id);
    if (next.length) {
      m.relaySpool.set(target, next);
    } else {
      m.relaySpool.delete(target);
    }
  }

  /**
   * Bound total entries/bytes and each target/sender share by evicting the oldest first.
   * Protection is the unique (target, id) admission pair, never an id shared across targets.
   * The entry being admitted fits within the global byte budget on its own, so admission
   * can never be starved by older entries.
   */
  private enforceRelaySpoolBudget(
    m: Mount,
    protectedEntry?: { targetNodeId: string; id: string },
    journal = false,
  ): boolean {
    let changed = false;
    const isProtected = (target: string, entry: SpooledRelayEntry): boolean =>
      protectedEntry !== undefined &&
      protectedEntry.targetNodeId === target &&
      protectedEntry.id === entry.id;
    const evict = (target: string, entry: SpooledRelayEntry) => {
      this.dropSpoolEntry(m, target, entry.id);
      if (journal) {
        this.appendRelaySpoolRecord(m, {
          op: 'remove',
          targetNodeId: target,
          id: entry.id,
        });
      }
      changed = true;
    };
    while (m.relaySpool.size > MAX_RELAY_SPOOL_TARGETS) {
      const oldest = this.oldestSpoolEntry(
        m,
        (target, entry) => !isProtected(target, entry),
      );
      if (!oldest) break;
      evict(oldest.target, oldest.entry);
    }
    for (;;) {
      const targetOver = [...m.relaySpool].find(
        ([, entries]) => entries.length > MAX_RELAY_SPOOL_ENTRIES,
      );
      if (targetOver) {
        const [target, entries] = targetOver;
        const oldest = entries
          .filter(entry => !isProtected(target, entry))
          .sort(
            (a, b) => Date.parse(a.spooledAt) - Date.parse(b.spooledAt) ||
              a.id.localeCompare(b.id),
          )[0];
        if (!oldest) break;
        evict(target, oldest);
        continue;
      }
      const counts = new Map<string, number>();
      for (const entries of m.relaySpool.values()) {
        for (const entry of entries) {
          const sender = entry.frame.senderNodeId;
          counts.set(sender, (counts.get(sender) ?? 0) + 1);
        }
      }
      // A self-asserted sender id still gets its own bounded share on load and admission.
      const senderOver = [...counts].find(
        ([, count]) => count > MAX_RELAY_SPOOL_SENDER_ENTRIES,
      )?.[0];
      const totalOver = this.entryCount(m, () => true) > MAX_RELAY_SPOOL_TOTAL_ENTRIES;
      if (senderOver === undefined && !totalOver) {
        break;
      }
      const oldest = this.oldestSpoolEntry(
        m,
        (target, entry) =>
          !isProtected(target, entry) &&
          (senderOver === undefined || entry.frame.senderNodeId === senderOver),
      );
      if (!oldest) break;
      evict(oldest.target, oldest.entry);
    }
    while (this.relaySpoolBytes(m) > MAX_RELAY_SPOOL_TOTAL_BYTES) {
      const cacheEviction = [...m.spoolBlobCache]
        .filter(([hash]) => !this.spoolHashReferencedByEntries(m, hash))
        .sort((a, b) => Date.parse(a[1].expiresAt) - Date.parse(b[1].expiresAt))[0];
      if (cacheEviction) {
        const [hash] = cacheEviction;
        m.spoolBlobCache.delete(hash);
        this.spooledBlobs.delete(this.spoolBlobKey(m, hash));
        rmSync(join(this.spoolBlobDirectory(m), hash), { force: true });
        if (journal) {
          this.appendRelaySpoolRecord(m, { op: 'uncache', blobs: [hash] });
        }
        changed = true;
        continue;
      }
      const oldest = this.oldestSpoolEntry(
        m,
        (target, entry) => !isProtected(target, entry),
      );
      if (!oldest) break;
      evict(oldest.target, oldest.entry);
    }
    // Evicted entries' blobs are unreachable now unless another live entry still names them.
    return this.collectSpoolBlobFiles(m) || changed;
  }

  private spoolHashReferencedByEntries(m: Mount, hash: string): boolean {
    for (const entries of m.relaySpool.values()) {
      for (const entry of entries) {
        if (entry.blobHashes?.[hash] !== undefined) {
          return true;
        }
      }
    }
    return false;
  }

  private spoolBlobKey(m: Mount, hash: string): string {
    return `${m.keys.swarmTopicId}\n${hash}`;
  }

  private writeSpoolBlob(m: Mount, hash: string, bytes: Buffer): string {
    const blobPath = join(this.spoolBlobDirectory(m), hash);
    writeFileAtomically(blobPath, bytes);
    // Never place a spooled attachment in the process-global sender cache: spool files are
    // network-scoped and are served only through readSpooledBlob/relay_blob.
    this.spooledBlobs.add(this.spoolBlobKey(m, hash));
    return blobPath;
  }

  /**
   * Serve a hash from this network's content-addressed spool cache only. The file is the
   * durable truth after restart; a missing file simply falls back to the original sender.
   */
  private async readSpooledBlob(m: Mount, hash: string): Promise<Buffer | undefined> {
    const blobPath = join(this.spoolBlobDirectory(m), hash);
    if (!existsSync(blobPath)) {
      this.spooledBlobs.delete(this.spoolBlobKey(m, hash));
      m.spoolBlobCache.delete(hash);
      return undefined;
    }
    const bytes = await readFile(blobPath);
    if (blake3(bytes) !== hash) {
      throw new LocalDeliveryError('Invalid cached blob');
    }
    return bytes;
  }

  /**
   * Store a sealed frame for an offline target. Transport stays blind: the sender supplies
   * an opaque, stable spool key, every attachment is verified against its BLAKE3 hash, and
   * the target must be a node id rather than an arbitrary member-chosen string.
   */
  private spoolRelayFrame(
    m: Mount,
    target: string,
    frame: SealedNetworkFrame,
    blobs: unknown,
    spoolKey?: string,
  ): { spooled: true; seedNodeId: string; spoolId: string } {
    if (
      !/^ed25519:[0-9a-f]{64}$/.test(target) ||
      target === this.localNodeId
    ) {
      throw new LocalDeliveryError('Invalid relay spool target');
    }
    if (
      spoolKey !== undefined &&
      (typeof spoolKey !== 'string' ||
        spoolKey.length === 0 ||
        spoolKey.length > MAX_RELAY_SPOOL_KEY_LENGTH ||
        /[\u0000-\u001f\u007f]/.test(spoolKey))
    ) {
      throw new LocalDeliveryError('Invalid relay spool key');
    }
    const prepared: Array<{ hash: string; bytes: Buffer }> = [];
    if (blobs !== undefined) {
      if (typeof blobs !== 'object' || blobs === null || Array.isArray(blobs)) {
        throw new LocalDeliveryError('Invalid relay spool blobs');
      }
      let total = 0;
      for (const [hash, encoded] of Object.entries(blobs)) {
        if (!/^[a-f0-9]{64}$/.test(hash) || typeof encoded !== 'string') {
          throw new LocalDeliveryError('Invalid relay spool blob');
        }
        const bytes = Buffer.from(encoded, 'base64');
        if (bytes.toString('base64') !== encoded || blake3(bytes) !== hash) {
          throw new LocalDeliveryError('Invalid relay spool blob');
        }
        total += bytes.length;
        if (total > MAX_RELAY_SPOOL_BLOB_BYTES) {
          throw new LocalDeliveryError('Relay spool blobs exceed limit');
        }
        prepared.push({ hash, bytes });
      }
    }
    this.pruneRelaySpool(m, true);
    const id = this.relaySpoolEntryId(frame, spoolKey);
    const now = new Date();
    const expiresAt = new Date(now.getTime() + RELAY_SPOOL_TTL_MS).toISOString();
    const frameBytes = Buffer.byteLength(JSON.stringify(frame));
    const entries = m.relaySpool.get(target) ?? [];
    const existing = entries.find(entry => entry.id === id);
    if (existing && existing.frame.senderNodeId !== frame.senderNodeId) {
      throw new LocalDeliveryError('Relay spool identity conflict');
    }
    const blobHashes: Record<string, number> = { ...(existing?.blobHashes ?? {}) };
    for (const { hash, bytes } of prepared) {
      blobHashes[hash] = bytes.length;
    }
    // Materialize verified bytes before publishing the entry, so a failed write can never
    // leave the in-memory obligation pointing at a hash that was never stored.
    for (const { hash, bytes } of prepared) {
      this.writeSpoolBlob(m, hash, bytes);
    }
    const entry: SpooledRelayEntry = {
      id,
      targetNodeId: target,
      networkId: m.config.networkId,
      frame,
      frameBytes,
      ...(Object.keys(blobHashes).length ? { blobHashes } : {}),
      spooledAt: existing?.spooledAt ?? now.toISOString(),
      expiresAt,
    };
    // Append the one-entry delta before publishing in memory: a crash either replays this
    // upsert or leaves the previous snapshot/delta untouched. The seed never rewrites the
    // full spool snapshot on the admission path.
    this.appendRelaySpoolRecord(m, { op: 'upsert', entry });
    if (existing) {
      // A redrive re-seals the same obligation with a new nonce; keep FIFO position but
      // refresh the frame, attachment metadata and expiry.
      existing.frame = frame;
      existing.frameBytes = frameBytes;
      existing.expiresAt = expiresAt;
      existing.blobHashes = blobHashes;
    } else {
      entries.push(entry);
    }
    entries.sort(
      (a, b) => Date.parse(a.spooledAt) - Date.parse(b.spooledAt) || a.id.localeCompare(b.id),
    );
    m.relaySpool.set(target, entries);
    this.enforceRelaySpoolBudget(m, { targetNodeId: target, id }, true);
    if (this.relaySpoolJournalOverBudget(m)) {
      this.persistRelaySpool(m);
    }
    return { spooled: true, seedNodeId: this.localNodeId, spoolId: id };
  }

  private spoolDrainKey(m: Mount, peerNodeId: string): string {
    return `${m.keys.swarmTopicId}\n${peerNodeId}`;
  }

  private scheduleSpoolDrain(m: Mount, peerNodeId: string, immediate = false): void {
    if (!m.relaySpool.get(peerNodeId)?.length) {
      return;
    }
    const key = this.spoolDrainKey(m, peerNodeId);
    const now = Date.now();
    if (!immediate && now - (this.spoolDrainAttempts.get(key) ?? 0) < 2_000) {
      return;
    }
    this.spoolDrainAttempts.set(key, now);
    void this.drainRelaySpool(m, peerNodeId).catch(() => {});
  }

  /** FIFO drain of one target's spool; a per-target lock makes concurrent pulses harmless. */
  private drainRelaySpool(m: Mount, peerNodeId: string): Promise<void> {
    const key = this.spoolDrainKey(m, peerNodeId);
    const prior = this.spoolDrains.get(key) ?? Promise.resolve();
    const run = prior
      .catch(() => {})
      .then(() => this.drainRelaySpoolNow(m, peerNodeId));
    this.spoolDrains.set(key, run);
    return run.finally(() => {
      if (this.spoolDrains.get(key) === run) {
        this.spoolDrains.delete(key);
      }
    });
  }

  private async drainRelaySpoolNow(m: Mount, peerNodeId: string): Promise<void> {
    const queued = m.relaySpool.get(peerNodeId);
    if (!queued?.length || this.closed || !this.mounts.has(m.config.networkId)) {
      return;
    }
    const timeoutMs = Math.min(this.options.socketTimeoutMs ?? 90_000, 30_000);
    let removed = false;
    for (const entry of [...queued]) {
      if (Date.parse(entry.expiresAt) <= Date.now()) {
        this.appendRelaySpoolRecord(m, {
          op: 'remove',
          targetNodeId: peerNodeId,
          id: entry.id,
        });
        this.dropSpoolEntry(m, peerNodeId, entry.id);
        removed = true;
        continue;
      }
      try {
        const tunnel = m.inboundTunnels.get(peerNodeId);
        let replyFrame: SealedNetworkFrame;
        if (tunnel) {
          const reply = await this.requestOverTunnel(
            tunnel,
            { type: 'relay_deliver', frame: entry.frame },
            undefined,
            timeoutMs,
          );
          if (reply?.error) {
            throw new Error(String(reply.error));
          }
          replyFrame = reply.frame;
        } else {
          const address = m.peers.get(peerNodeId);
          if (!address) {
            break;
          }
          // Without an inbound tunnel this is only a speculative direct attempt before the
          // target's own tunnel arrives; keep it inside the fast-fail window so the
          // per-target drain lock cannot delay serveInboundTunnel's authoritative drain.
          replyFrame = await this.directFrame(
            m,
            peerNodeId,
            address,
            entry.frame,
            Math.min(timeoutMs, RELAY_CONNECT_TIMEOUT_MS),
          );
        }
        const payload = unsealNetworkFrame<{ requestNonce?: string }>(replyFrame, m.keys);
        if (
          replyFrame.senderNodeId !== peerNodeId ||
          payload.requestNonce !== entry.frame.nonce
        ) {
          throw new Error('Response binding mismatch');
        }
        // The frame handoff is confirmed, but the target may fetch attachments afterwards;
        // retain their bytes until this entry's original TTL instead of deleting them now.
        this.retainSpoolBlobs(m, entry);
        const retained = Object.fromEntries(
          Object.entries(entry.blobHashes ?? {}).map(([hash, size]) => [
            hash,
            {
              size,
              expiresAt: m.spoolBlobCache.get(hash)?.expiresAt ?? entry.expiresAt,
            },
          ]),
        );
        this.appendRelaySpoolRecord(m, {
          op: 'drain',
          targetNodeId: peerNodeId,
          id: entry.id,
          ...(Object.keys(retained).length ? { blobs: retained } : {}),
        });
        this.dropSpoolEntry(m, peerNodeId, entry.id);
        removed = true;
      } catch {
        // The target is still offline (or the frame could not be confirmed); keep the
        // entry and retry on the next tunnel/beacon pulse.
        break;
      }
    }
    if (removed) {
      this.collectSpoolBlobFiles(m);
      if (this.relaySpoolJournalOverBudget(m)) {
        this.persistRelaySpool(m);
      }
    }
  }

  private learn(
    frame: SealedNetworkFrame,
    remoteHost?: string,
    scheduleDrain = true,
  ): Mount | undefined {
    return this.learnWithMeta(frame, remoteHost, undefined, scheduleDrain).mount;
  }

  private learnWithMeta(
    frame: SealedNetworkFrame,
    remoteHost?: string,
    viaSeed?: string,
    scheduleDrain = true,
  ): { mount?: Mount; reply: boolean } {
    const m = [...this.mounts.values()].find(m => m.keys.swarmTopicId === frame.swarmTopicId);
    if (!m) {
      return { reply: false };
    }
    const beacon = unsealNetworkFrame<{
      type: string;
      endpointAddr: string;
      expires: number;
      reply?: boolean;
    }>(frame, m.keys);
    if (
      beacon.type !== 'discovery' ||
      beacon.expires < Date.now() ||
      beacon.expires > Date.now() + 30_000
    ) {
      return { reply: false };
    }
    const current = m.beacons.get(frame.senderNodeId);
    if (current && beacon.expires <= current.expires) {
      // Beacons travel by gossip, so a delayed frame from the same or an older
      // generation must never rewrite this peer's endpoint or relay mapping.
      return { mount: m, reply: beacon.reply === true };
    }
    this.registerPeerEndpoint(
      m.config.networkId,
      frame.senderNodeId,
      this.reachableEndpoint(beacon.endpointAddr, remoteHost),
    );
    if (frame.senderNodeId !== this.localNodeId) {
      // Sealed discovery frames are the only beacon form a seed can gossip without
      // being able to forge endpoints; keep the freshest one per sender.
      m.beacons.set(frame.senderNodeId, { frame, expires: beacon.expires });
      if (viaSeed !== undefined) {
        // Remember who can relay to this peer when its advertised endpoint is unreachable.
        m.relays.set(frame.senderNodeId, viaSeed);
      } else if (remoteHost && remoteHost !== '127.0.0.1' && remoteHost !== '0.0.0.0') {
        // A fresher beacon that reached us directly makes an obsolete relay seed mapping wrong.
        m.relays.delete(frame.senderNodeId);
      }
      // A fresh endpoint is the cheapest proof that a spooled target may be reachable again.
      // Discovery probes deliberately skip this: their peer opens a tunnel moments later,
      // and a speculative direct drain would hold the per-target lock ahead of it.
      if (scheduleDrain) {
        this.scheduleSpoolDrain(m, frame.senderNodeId);
      }
    }
    return { mount: m, reply: beacon.reply === true };
  }

  /**
   * Prefer the address we actually reached the peer on when it is routable across
   * subnets. A peer reached over Tailscale advertises whatever LAN address it sees
   * locally, which is useless to us; its observed 100.64/10 address is not.
   */
  private reachableEndpoint(endpointAddr: string, remoteHost?: string): string {
    const host = remoteHost?.replace(/^::ffff:/, '');
    if (!host || host === '127.0.0.1' || host === '0.0.0.0' || this.options.advertisedHost) {
      return endpointAddr;
    }
    let parsed: { host: string; port: number };
    try {
      parsed = parseEndpoint(endpointAddr);
    } catch {
      return endpointAddr;
    }
    if (parsed.host === '127.0.0.1') {
      return endpointAddr;
    }
    if ((isTailscaleIpv4Host(host) && isPrivateLanIpv4Host(parsed.host)) || parsed.host === host) {
      return `${host.includes(':') ? `[${host}]` : host}:${parsed.port}`;
    }
    return endpointAddr;
  }

  private localBeacon(m: Mount, reply = false): { frame: SealedNetworkFrame; expires: number } {
    const expires = Date.now() + 10_000;
    return {
      frame: sealNetworkFrame(
        {
          type: 'discovery',
          endpointAddr: this.endpointAddr,
          expires,
          ...(reply ? { reply: true } : {}),
        },
        m.keys,
        this.localNodeId,
      ),
      expires,
    };
  }

  private discoveryFrame(m: Mount, reply = false): SealedNetworkFrame {
    return this.localBeacon(m, reply).frame;
  }

  private async announce(m: Mount): Promise<void> {
    if (this.closed || !this.mounts.has(m.config.networkId)) {
      return;
    }
    const { frame, expires } = this.localBeacon(m);
    m.beacons.set(this.localNodeId, { frame, expires });
    this.pruneBeacons(m);
    const bytes = Buffer.from(JSON.stringify(frame));
    const port = m.config.mDnsPort ?? this.options.mDnsPort ?? 45873;
    const temp = `${m.file}.${randomUUID()}`;
    try {
      await writeFile(temp, bytes, { mode: 0o600 });
      await rename(temp, m.file);
    } finally {
      await rm(temp, { force: true });
    }
    try {
      this.udp?.send(bytes, port, GROUP);
    } catch {
      // TCP seeds and the local registry remain available without multicast.
    }
    // Seed discovery has its own cadence; local registry/multicast discovery stays fast.
    // A single bounded round also rate-limits unreachable seeds without per-seed timers.
    const now = Date.now();
    if (now - m.lastSeedProbeAt < Math.max(this.options.discoveryIntervalMs ?? 50, 2000)) return;
    m.lastSeedProbeAt = now;
    if (this.udp && this.options.multicast !== false) {
      // Subnet broadcasts cover peers on the same LAN without requiring multicast routing.
      for (const { broadcast } of lanBroadcastAddresses()) {
        try {
          this.udp.send(bytes, port, broadcast);
        } catch {
          // A link without broadcast support must not stop seed probes.
        }
      }
      const peers = this.options.tailscalePeers
        ? await this.options.tailscalePeers()
        : await discoverTailscaleIpv4Peers();
      for (const host of peers) {
        try {
          this.udp.send(bytes, port, host);
        } catch {
          // Unreachable peers are retried on the next probe.
        }
      }
    }
    await Promise.allSettled([...new Set(m.config.bootstrapPeers ?? [])].filter(seed => seed !== this.endpointAddr).map(async seed => {
      // The persistent tunnel lets the seed push relayed requests back; the probe
      // below exchanges sealed beacons so isolated peers can discover each other.
      const prior = [...m.outboundTunnels.entries()].find(([, entry]) => entry.seed === seed && entry.acked);
      const socket = this.track(createConnection(parseEndpoint(seed)));
      socket.setTimeout(Math.min(this.options.socketTimeoutMs ?? 2000, 2000));
      try {
        socket.write(packet({
          type: 'discovery',
          frame,
          peers: [...m.beacons.values()].map(entry => entry.frame),
        }));
        const response = await new SocketReader(socket).json();
        if (response.frame?.swarmTopicId !== m.keys.swarmTopicId) {
          throw new Error('Invalid discovery response');
        }
        const seedHost = parseEndpoint(seed).host;
        if (!this.learnWithMeta(response.frame, seedHost).mount) {
          throw new Error('Invalid discovery response');
        }
        const seedNodeId = response.frame.senderNodeId;
        if (seedNodeId === this.localNodeId) return;
        this.ensureOutboundTunnel(m, seedNodeId, seed, response.tunneled === true, prior?.[1]);
        const activeTunnel = m.outboundTunnels.get(seedNodeId);
        if (!activeTunnel || activeTunnel.seed === seed) {
          m.relays.set(seedNodeId, seed);
        }
        for (const peerFrame of Array.isArray(response.peers) ? response.peers : []) {
          try {
            // Gossiped beacons were not reached directly, so keep the advertised
            // endpoint and remember the seed that can relay to their sender.
            this.learnWithMeta(peerFrame, undefined, seed);
          } catch {
            // One unreadable gossiped frame must not fail the whole probe.
          }
        }
      } catch {
        if (prior && m.outboundTunnels.get(prior[0]) === prior[1]) {
          prior[1].socket.destroy();
          m.outboundTunnels.delete(prior[0]);
        }
      } finally {
        socket.destroy();
      }
    }));
  }

  private async discover(announce = true): Promise<void> {
    for (const m of this.mounts.values()) {
      try {
        if (announce) await this.announce(m);
        for (const name of await readdir(dirname(m.file))) {
          if (!name.endsWith('.json')) {
            continue;
          }
          try {
            this.learn(JSON.parse(await readFile(join(dirname(m.file), name), 'utf8')));
          } catch {
            // Stale or foreign beacon
          }
        }
      } catch {
        // Transient discovery failures must not break active streams
      }
    }
  }

  onInboundFrame(handler: Handler): Disposable {
    this.handlers.add(handler);
    return {
      dispose: () => {
        this.handlers.delete(handler);
      },
    };
  }

  private sealed(
    m: Mount,
    mode: 'unicast' | 'broadcast',
    bytes: Uint8Array,
    subTopic?: string,
  ): SealedNetworkFrame {
    let candidate: any;
    try {
      candidate = JSON.parse(Buffer.from(bytes).toString());
    } catch {
      // Binary payload
    }
    if (candidate?.protocol === 'the-dealer-wire/v1') {
      unsealNetworkFrame(candidate, m.keys);
      return candidate;
    }
    return sealNetworkFrame(
      {
        mode,
        subTopic,
        dataBase64: Buffer.from(bytes).toString('base64'),
      },
      m.keys,
      this.localNodeId,
    );
  }

  private async exchange(
    m: Mount,
    target: string,
    frame: SealedNetworkFrame,
    timeoutMs = this.options.socketTimeoutMs ?? 90_000,
    options: RelayRequestOptions = {},
  ): Promise<Uint8Array> {
    // The caller's timeout is the whole exchange budget, not a per-attempt allowance: a
    // blackholed direct endpoint followed by hanging relay seeds must not multiply it.
    const deadline = Date.now() + timeoutMs;
    const remaining = () => Math.max(1, deadline - Date.now());
    const address = m.peers.get(target);
    // Any fallback means the advertised endpoint may be NAT-bound; fail over fast
    // instead of waiting the full request timeout on the direct attempt.
    const fallbacks = this.peerFallbacks(target, [m]);
    let lastError: Error = new PeerUnreachableError(target, `Unknown peer: ${target}`);
    if (address) {
      try {
        return await this.directExchange(
          m,
          target,
          address,
          frame,
          fallbacks.length ? Math.min(remaining(), RELAY_CONNECT_TIMEOUT_MS) : remaining(),
        );
      } catch (error) {
        lastError = asError(error);
        // A signature/binding failure is never an offline symptom: retrying it through a
        // relay or queueing it in an outbox would hide a real security decision. Unknown
        // errors are equally deterministic until proven connection-class.
        if (lastError instanceof LocalDeliveryError || !isConnectionFailure(lastError)) {
          throw lastError;
        }
        if (!fallbacks.length) {
          throw this.unreachable(target, lastError);
        }
      }
    } else if (!fallbacks.length) {
      throw this.unreachable(target, lastError);
    }
    let blobs: Promise<{ blobs: Record<string, string>; complete: boolean }> | undefined;
    for (const [index, fallback] of fallbacks.entries()) {
      if (Date.now() >= deadline) {
        break;
      }
      // Split what is left across the fallbacks still untried: a blackholed seed must never
      // starve a live seed behind it, and the total still obeys the caller's deadline.
      const attemptTimeoutMs = Math.max(1, Math.floor(remaining() / (fallbacks.length - index)));
      try {
        if ('tunnel' in fallback) {
          return await this.exchangeOverTunnel(m, target, fallback.tunnel, frame, attemptTimeoutMs);
        }
        const attachmentPacket = options.blobHashes?.length
          ? await (blobs ??= this.readSpoolBlobs(
              options.blobHashes,
              Buffer.byteLength(JSON.stringify({
                type: options.spoolIfOffline ? 'relay_send' : 'relay',
                networkId: m.config.networkId,
                target,
                frame,
                ...(options.spoolIfOffline === true ? { spoolIfOffline: true } : {}),
                ...(options.spoolKey !== undefined ? { spoolKey: options.spoolKey } : {}),
                blobs: {},
              })),
            ))
          : { blobs: {}, complete: true };
        return await this.relayExchange(
          m,
          fallback.seed,
          target,
          frame,
          attemptTimeoutMs,
          {
            // An incomplete offer (an attachment exceeds one frame) stays live-only: the
            // target could never fetch an omitted blob from this spool entry.
            ...(options.spoolIfOffline === true && attachmentPacket.complete
              ? { spoolIfOffline: true }
              : {}),
            ...(options.spoolKey !== undefined ? { spoolKey: options.spoolKey } : {}),
            ...(Object.keys(attachmentPacket.blobs).length
              ? { blobs: attachmentPacket.blobs }
              : {}),
          },
        );
      } catch (error) {
        // A confirmed spool is a successful handoff to durable relay storage: never try a
        // second seed, which would duplicate entries and extend the caller's latency.
        if (error instanceof OfflinePeerSpooledError) {
          throw error;
        }
        // Local attachment/framing failures are deterministic and must never be masked as
        // peer unreachability (which would enqueue the same broken bytes forever).
        if (error instanceof LocalDeliveryError) {
          throw error;
        }
        // Only a connection-class failure may advance to the next seed; a malformed or
        // mismatched response is deterministic and must surface as such.
        if (!isConnectionFailure(error)) {
          throw error;
        }
        lastError = asError(error);
      }
    }
    throw this.unreachable(target, lastError);
  }

  /** Keep transport reachability decisions in one place; protocol/security errors pass through. */
  private unreachable(target: string, error: Error): Error {
    if (
      error instanceof OfflinePeerSpooledError ||
      error instanceof PeerUnreachableError ||
      error instanceof LocalDeliveryError
    ) {
      return error;
    }
    // Never reinterpret an arbitrary failure as offline: only connection-class errors may
    // become PeerUnreachableError, because only those can justify a durable queued receipt.
    if (isConnectionFailure(error)) {
      return new PeerUnreachableError(target, error.message, { cause: error });
    }
    return error;
  }

  /**
   * Read the sender's attachment cache for one relay request. Every offered hash must
   * resolve and re-hash to its declared content. Completeness is judged against the exact
   * serialized relay packet, including the sealed frame, JSON keys, and base64
   * expansion, so a packet that would fail `packet()` is offered live-only instead.
   */
  private async readSpoolBlobs(
    hashes: readonly string[],
    emptyPacketBytes: number,
  ): Promise<{ blobs: Record<string, string>; complete: boolean }> {
    const blobs: Record<string, string> = {};
    // Replacing the empty `{}` with entries removes two bytes and adds one comma between
    // entries. Each entry contributes `"<hash>":"<base64>"`, i.e. hash.length + 5 + base64.
    let entryBytes = -2;
    let entries = 0;
    for (const hash of new Set(hashes)) {
      const blob = this.blobs.get(hash);
      if (!blob) {
        throw new LocalDeliveryError(`Missing attachment blob for relay spooling: ${hash}`);
      }
      const entryBytesWithOverhead =
        (entries > 0 ? 1 : 0) + hash.length + 5 + 4 * Math.ceil(blob.size / 3);
      if (emptyPacketBytes + entryBytes + entryBytesWithOverhead > MAX_FRAME) {
        return { blobs: {}, complete: false };
      }
      const bytes = await readFile(blob.path);
      if (blake3(bytes) !== hash) {
        throw new LocalDeliveryError(
          `Attachment content changed before relay spooling: ${hash}`,
        );
      }
      const base64 = bytes.toString('base64');
      const exactEntryBytes =
        (entries > 0 ? 1 : 0) + hash.length + 5 + base64.length;
      if (emptyPacketBytes + entryBytes + exactEntryBytes > MAX_FRAME) {
        return { blobs: {}, complete: false };
      }
      blobs[hash] = base64;
      entryBytes += exactEntryBytes;
      entries += 1;
    }
    return { blobs, complete: true };
  }

  /**
   * Ordered ways to reach a peer whose advertised endpoint may be unreachable: the
   * tunnels it opened towards this node first, then every seed that can relay to it.
   */
  private peerFallbacks(target: string, mounts: Iterable<Mount>): PeerFallback[] {
    const scoped = [...mounts];
    const fallbacks: PeerFallback[] = [];
    for (const mount of scoped) {
      const tunnel = mount.inboundTunnels.get(target);
      if (tunnel) {
        fallbacks.push({ tunnel });
      }
    }
    for (const mount of scoped) {
      for (const seed of this.relayCandidates(mount, target)) {
        fallbacks.push({ mount, seed });
      }
    }
    return fallbacks;
  }

  /** Seeds that may forward to this target: its introducing seed first, then configured seeds. */
  private relayCandidates(m: Mount, target: string): string[] {
    const seeds = [m.relays.get(target), ...(m.config.bootstrapPeers ?? [])]
      .filter((seed): seed is string => typeof seed === 'string' && seed.length > 0 && seed !== this.endpointAddr);
    return [...new Set(seeds)];
  }

  private endpointFor(m: Mount, nodeId: string): string {
    const address = m.peers.get(nodeId);
    if (!address) {
      throw new Error('Relay target unreachable');
    }
    return address;
  }

  private async directFrame(
    m: Mount,
    target: string,
    address: string,
    frame: SealedNetworkFrame,
    timeoutMs: number,
  ): Promise<SealedNetworkFrame> {
    const socket = this.track(createConnection(parseEndpoint(address)));
    socket.setTimeout(timeoutMs);
    try {
      socket.write(packet({ type: 'frame', frame }));
      const response = await new SocketReader(socket).json();
      if (response?.error) {
        throw new LocalDeliveryError(String(response.error));
      }
      if (response?.frame?.senderNodeId !== target) {
        throw new LocalDeliveryError('Response binding mismatch');
      }
      return response.frame;
    } finally {
      socket.destroy();
    }
  }

  private async directExchange(
    m: Mount,
    target: string,
    address: string,
    frame: SealedNetworkFrame,
    timeoutMs: number,
  ): Promise<Uint8Array> {
    const responseFrame = await this.directFrame(m, target, address, frame, timeoutMs);
    let payload: { requestNonce: string; dataBase64: string };
    try {
      payload = unsealNetworkFrame<{ requestNonce: string; dataBase64: string }>(
        responseFrame,
        m.keys,
      );
    } catch {
      throw new LocalDeliveryError('Unable to authenticate or decode direct response');
    }
    if (payload.requestNonce !== frame.nonce) {
      throw new LocalDeliveryError('Response binding mismatch');
    }
    return Buffer.from(payload.dataBase64, 'base64');
  }

  /** Forward a sealed frame and, when offered, durably spool it on an always-on seed. */
  private async relayExchange(
    m: Mount,
    seed: string,
    target: string,
    frame: SealedNetworkFrame,
    timeoutMs: number,
    options: { spoolIfOffline?: boolean; spoolKey?: string; blobs?: Record<string, string> } = {},
  ): Promise<Uint8Array> {
    const startedAt = Date.now();
    const socket = this.track(createConnection(parseEndpoint(seed)));
    socket.setTimeout(timeoutMs);
    try {
      socket.write(packet({
        // Live non-spooling sends use the original `relay` verb so pre-upgrade seeds keep
        // forwarding them; only a spool offer needs the newer `relay_send` verb.
        type: options.spoolIfOffline ? 'relay_send' : 'relay',
        networkId: m.config.networkId,
        target,
        frame,
        ...(options.spoolIfOffline ? { spoolIfOffline: true } : {}),
        ...(options.spoolKey !== undefined ? { spoolKey: options.spoolKey } : {}),
        ...(options.blobs && Object.keys(options.blobs).length
          ? { blobs: options.blobs }
          : {}),
      }));
      let response: any;
      try {
        response = await new SocketReader(socket).json();
      } catch (error) {
        // A pre-upgrade seed answers a `relay_send` offer by dropping the connection. The
        // beacon is the only evidence that decides whether that target is still live: with
        // a fresh beacon the peer is demonstrably online behind NAT, so retrying the
        // original `relay` verb delivers now instead of queueing a reachable peer; without
        // one the fast failure is genuine and flows into the durable outbox.
        if (
          options.spoolIfOffline === true &&
          (error as Error | undefined)?.message === 'Truncated stream'
        ) {
          const hasFreshBeacon = (m.beacons.get(target)?.expires ?? 0) >= Date.now();
          const remainingMs = timeoutMs - (Date.now() - startedAt);
          if (hasFreshBeacon && remainingMs > 0) {
            socket.destroy();
            return await this.relayExchange(m, seed, target, frame, remainingMs, {});
          }
        }
        throw error;
      }
      if (response?.error) {
        throw new LocalDeliveryError(String(response.error));
      }
      if (response?.spooled === true) {
        if (typeof response.seedNodeId !== 'string' || typeof response.spoolId !== 'string') {
          throw new LocalDeliveryError('Invalid relay spool response');
        }
        throw new OfflinePeerSpooledError(target, response.seedNodeId, response.spoolId);
      }
      if (!response?.frame) {
        throw new LocalDeliveryError('Invalid relay response');
      }
      let payload: { requestNonce: string; dataBase64: string };
      try {
        payload = unsealNetworkFrame<{ requestNonce: string; dataBase64: string }>(
          response.frame,
          m.keys,
        );
      } catch {
        throw new LocalDeliveryError('Unable to authenticate or decode relay response');
      }
      if (response.frame.senderNodeId !== target || payload.requestNonce !== frame.nonce) {
        throw new LocalDeliveryError('Response binding mismatch');
      }
      return Buffer.from(payload.dataBase64, 'base64');
    } finally {
      socket.destroy();
    }
  }

  /** Push one frame down the target's own inbound tunnel and bind its sealed reply. */
  private async exchangeOverTunnel(
    m: Mount,
    target: string,
    tunnel: Tunnel,
    frame: SealedNetworkFrame,
    timeoutMs: number,
  ): Promise<Uint8Array> {
    const response = await this.requestOverTunnel(tunnel, { type: 'relay_deliver', frame }, undefined, timeoutMs);
    if (response?.error) {
      const detail = String(response.error);
      const error = new Error(detail);
      if (isConnectionFailure(error)) {
        throw new PeerUnreachableError(target, detail, { cause: error });
      }
      throw new LocalDeliveryError(detail);
    }
    if (!response?.frame) {
      throw new LocalDeliveryError('Invalid relay response');
    }
    let payload: { requestNonce: string; dataBase64: string };
    try {
      payload = unsealNetworkFrame<{ requestNonce: string; dataBase64: string }>(
        response.frame,
        m.keys,
      );
    } catch {
      throw new LocalDeliveryError('Unable to authenticate or decode relay response');
    }
    if (response.frame.senderNodeId !== target || payload.requestNonce !== frame.nonce) {
      throw new LocalDeliveryError('Response binding mismatch');
    }
    return Buffer.from(payload.dataBase64, 'base64');
  }

  async sendUnicast(
    networkId: string,
    target: string,
    bytes: Uint8Array,
    opts?: {
      timeoutMs?: number;
      spoolIfOffline?: boolean;
      spoolKey?: string;
      spoolBlobHashes?: readonly string[];
    },
  ): Promise<Uint8Array> {
    const m = this.mounts.get(networkId);
    if (target === this.localNodeId) {
      return this.deliverLocal(networkId, bytes, m);
    }
    if (!m) {
      throw new NetworkUnmountedError(networkId);
    }
    // Spooling is caller-owned policy: an explicit opt-out must leave no durable relay
    // copy. Attachment hashes still come from the caller, so transport stays envelope-blind.
    return this.exchange(m, target, this.sealed(m, 'unicast', bytes), opts?.timeoutMs, {
      ...(opts?.spoolIfOffline === true ? { spoolIfOffline: true } : {}),
      ...(opts?.spoolKey !== undefined ? { spoolKey: opts.spoolKey } : {}),
      blobHashes: opts?.spoolBlobHashes ?? [],
    });
  }

  /** Same-machine mode: deliver a frame addressed to this node without opening a socket. */
  private async deliverLocal(
    networkId: string,
    bytes: Uint8Array,
    mount?: Mount,
  ): Promise<Uint8Array> {
    let payload: unknown;
    try {
      payload = JSON.parse(Buffer.from(bytes).toString('utf8'));
    } catch {
      payload = bytes;
    }
    let event: InboundFrameEvent & { rawBytes: Uint8Array };
    if (
      payload &&
      typeof payload === 'object' &&
      (payload as { protocol?: string }).protocol === 'the-dealer-wire/v1'
    ) {
      if (!mount) {
        throw new NetworkUnmountedError(networkId);
      }
      const frame = payload as SealedNetworkFrame;
      if (frame.swarmTopicId !== mount.keys.swarmTopicId) {
        throw new Error('Unknown swarm');
      }
      event = this.inboundEvent(mount, frame);
    } else {
      event = {
        networkId,
        senderNodeId: this.localNodeId,
        mode: 'unicast',
        payload,
        rawBytes: bytes,
      };
    }
    if (this.handlers.size === 0 && !this.ctx) {
      throw new Error('No inbound handler registered');
    }
    return this.inboundReply(event);
  }

  async sendMulticast(
    networkId: string,
    targets: readonly string[],
    bytes: Uint8Array,
  ): Promise<Map<string, Result<Uint8Array>>> {
    return new Map<string, Result<Uint8Array>>(
      await Promise.all(
        targets.map(async target => {
          try {
            return [target, { ok: true, value: await this.sendUnicast(networkId, target, bytes) }] as const;
          } catch (error) {
            return [
              target,
              {
                ok: false,
                error: error instanceof Error ? error : new Error(String(error)),
              },
            ] as const;
          }
        }),
      ),
    );
  }

  async broadcast(
    networkId: string,
    subTopic: string,
    bytes: Uint8Array,
    opts?: { timeoutMs?: number },
  ): Promise<void> {
    const m = this.get(networkId);
    const frame = this.sealed(m, 'broadcast', bytes, subTopic);
    await Promise.allSettled(
      [...m.peers.keys()].map(target => this.exchange(m, target, frame, opts?.timeoutMs)),
    );
  }

  /** Drop beacons whose discovery frame expired beyond the gossip grace window. */
  private pruneBeacons(m: Mount): void {
    const cutoff = Date.now() - BEACON_EXPIRY_GRACE_MS;
    for (const [nodeId, entry] of m.beacons) {
      if (entry.expires < cutoff) {
        m.beacons.delete(nodeId);
      }
    }
  }

  /**
   * Keep one persistent tunnel per seed. It carries no requests of its own; the
   * seed uses it to push relayed frames back through the seed's address, which
   * stays reachable even when this node advertises a NAT-bound endpoint.
   */
  private ensureOutboundTunnel(
    m: Mount,
    seedNodeId: string,
    seed: string,
    seedHasTunnel: boolean,
    priorEntry?: { socket: Socket; seed: string; acked: boolean },
  ): void {
    if (this.closed || !this.mounts.has(m.config.networkId)) {
      return;
    }
    const existing = m.outboundTunnels.get(seedNodeId);
    if (existing && !existing.socket.destroyed) {
      if (existing !== priorEntry || !existing.acked || seedHasTunnel) return;
      existing.socket.destroy();
      m.outboundTunnels.delete(seedNodeId);
    }
    const socket = this.track(createConnection(parseEndpoint(seed)));
    const entry = { socket, seed, acked: false };
    m.outboundTunnels.set(seedNodeId, entry);
    // A persistent tunnel must outlive the idle window that bounds one-shot requests.
    socket.setTimeout(0);
    socket.setKeepAlive(true, 15_000);
    const ackTimer = setTimeout(() => {
      if (!entry.acked) socket.destroy();
    }, Math.min(this.options.socketTimeoutMs ?? 2000, 2000));
    ackTimer.unref?.();
    socket.once('close', () => {
      clearTimeout(ackTimer);
      if (m.outboundTunnels.get(seedNodeId) === entry) {
        m.outboundTunnels.delete(seedNodeId);
      }
    });
    socket.write(packet({ type: 'tunnel', frame: this.discoveryFrame(m) }));
    void this.serveOutboundTunnel(m, entry, seed, () => clearTimeout(ackTimer)).catch(() => socket.destroy());
  }

  private async serveOutboundTunnel(m: Mount, entry: { socket: Socket; seed: string; acked: boolean }, seed: string, onAck?: () => void): Promise<void> {
    const socket = entry.socket;
    const reader = new SocketReader(socket);
    while (!socket.destroyed) {
      const message = await reader.json();
      if (message?.type === 'tunnel_ack') {
        entry.acked = true;
        onAck?.();
        continue;
      }
      if (message?.type === 'relay_deliver' && typeof message.relayId === 'string' && message.frame) {
        try {
          const event = this.inboundEvent(m, message.frame);
          // A frame this seed carried down our tunnel proves the seed can relay back
          // to its sender even when the sender's advertised endpoint is NAT-bound.
          if (!m.relays.has(event.senderNodeId)) {
            m.relays.set(event.senderNodeId, seed);
          }
          const reply = await this.inboundReply(event);
          socket.write(packet({
            type: 'relay_reply',
            relayId: message.relayId,
            frame: sealNetworkFrame(
              {
                requestNonce: message.frame.nonce,
                dataBase64: Buffer.from(reply).toString('base64'),
              },
              m.keys,
              this.localNodeId,
            ),
          }));
        } catch (error) {
          socket.write(packet({
            type: 'relay_reply',
            relayId: message.relayId,
            error: error instanceof Error ? error.message : String(error),
          }));
        }
        continue;
      }
      if (message?.type === 'relay_blob_req' && typeof message.relayId === 'string') {
        const blob = typeof message.hash === 'string' ? this.blobs.get(message.hash) : undefined;
        if (!blob || blob.size > RELAY_BLOB_LIMIT) {
          socket.write(packet({ type: 'relay_blob_res', relayId: message.relayId, error: 'Unavailable' }));
          continue;
        }
        try {
          const bytes = await readFile(blob.path);
          socket.write(packet({
            type: 'relay_blob_res',
            relayId: message.relayId,
            size: blob.size,
            dataBase64: bytes.toString('base64'),
          }));
        } catch {
          socket.write(packet({ type: 'relay_blob_res', relayId: message.relayId, error: 'Unavailable' }));
        }
      }
    }
  }

  /** Register a peer's persistent tunnel and resolve its relay replies until it closes. */
  private async serveInboundTunnel(socket: Socket, frame: SealedNetworkFrame): Promise<void> {
    const mount = this.learn(frame, socket.remoteAddress, false);
    if (!mount) {
      throw new Error('Invalid tunnel request');
    }
    // A persistent tunnel must outlive the idle window that bounds one-shot requests.
    socket.setTimeout(0);
    const peerNodeId = frame.senderNodeId;
    const tunnel: Tunnel = { socket, pending: new Map() };
    const previous = mount.inboundTunnels.get(peerNodeId);
    mount.inboundTunnels.set(peerNodeId, tunnel);
    if (previous && previous.socket !== socket) {
      previous.socket.destroy();
    }
    socket.write(packet({ type: 'tunnel_ack' }));
    // The target just proved it is online; drain its durable store-and-forward queue
    // before serving any further requests, so a returning peer receives mail immediately.
    this.scheduleSpoolDrain(mount, peerNodeId, true);
    socket.once('close', () => {
      if (mount.inboundTunnels.get(peerNodeId) === tunnel) {
        mount.inboundTunnels.delete(peerNodeId);
      }
      for (const entry of tunnel.pending.values()) {
        entry.reject(new Error('Relay tunnel closed'));
      }
      tunnel.pending.clear();
    });
    try {
      const reader = new SocketReader(socket);
      while (true) {
        const message = await reader.json();
        if (message?.type === 'relay_reply' || message?.type === 'relay_blob_res') {
          tunnel.pending.get(String(message.relayId))?.resolve(message);
        }
      }
    } catch {
      // The peer closed the tunnel; its close handler owns cleanup.
    }
  }

  /** Send one request down a peer's tunnel; a request socket closing aborts it. */
  private requestOverTunnel(
    tunnel: Tunnel,
    body: Record<string, unknown>,
    requestSocket?: Socket,
    timeoutMs = this.options.socketTimeoutMs ?? 90_000,
  ): Promise<any> {
    const relayId = randomUUID();
    return new Promise((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const finish = (error?: Error, message?: any) => {
        clearTimeout(timer);
        tunnel.pending.delete(relayId);
        requestSocket?.off('close', abort);
        tunnel.socket.off('close', abort);
        if (error) reject(error); else resolve(message);
      };
      const abort = () => finish(new Error('Relay tunnel closed'));
      tunnel.pending.set(relayId, {
        resolve: message => finish(undefined, message),
        reject: error => finish(error),
      });
      requestSocket?.once('close', abort);
      tunnel.socket.once('close', abort);
      // The persistent tunnel socket itself never times out: a locally issued request
      // needs its own deadline, while a forwarded one is bounded by its requester.
      if (!requestSocket) {
        timer = setTimeout(() => finish(new DeliveryTimeoutError('Relay tunnel timeout')), timeoutMs);
      }
      try {
        tunnel.socket.write(packet({ ...body, relayId }));
      } catch (error) {
        finish(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  private async blobOverTunnel(tunnel: Tunnel, hash: string, requestSocket?: Socket): Promise<Buffer> {
    const response = await this.requestOverTunnel(tunnel, { type: 'relay_blob_req', hash }, requestSocket);
    if (response?.error) {
      throw new Error(String(response.error));
    }
    if (typeof response?.size !== 'number' || response.size > RELAY_BLOB_LIMIT ||
        typeof response.dataBase64 !== 'string') {
      throw new Error('Invalid relay blob response');
    }
    const bytes = Buffer.from(response.dataBase64, 'base64');
    if (bytes.length !== response.size) {
      throw new Error('Relay blob size mismatch');
    }
    return bytes;
  }

  private async readBlobAt(address: string, hash: string): Promise<Buffer> {
    const socket = this.track(createConnection(parseEndpoint(address)));
    socket.setTimeout(this.options.socketTimeoutMs ?? 90_000);
    const reader = new SocketReader(socket);
    try {
      socket.write(packet({ type: 'blob', hash }));
      const metadata = await reader.json();
      if (typeof metadata?.size !== 'number' || metadata.size > RELAY_BLOB_LIMIT) {
        throw new Error('Relay blob unavailable');
      }
      const chunks: Buffer[] = [];
      let received = 0;
      for await (const chunk of reader.remaining()) {
        received += chunk.length;
        if (received > metadata.size) {
          throw new Error('Blob size exceeded');
        }
        chunks.push(chunk);
      }
      if (received !== metadata.size) {
        throw new Error('Blob size mismatch');
      }
      return Buffer.concat(chunks);
    } finally {
      socket.destroy();
    }
  }

  private async receive(socket: Socket): Promise<void> {
    const request = await new SocketReader(socket).json();
    if (request.type === 'discovery') {
      // Do not drain from this probe: the peer opens its persistent tunnel immediately
      // afterwards, and serveInboundTunnel owns the first authoritative drain.
      const mount = this.learn(request.frame, socket.remoteAddress, false);
      if (!mount) throw new Error('Invalid discovery request');
      for (const peerFrame of Array.isArray(request.peers) ? request.peers : []) {
        try {
          this.learnWithMeta(peerFrame, undefined);
        } catch {
          // One unreadable gossiped frame must not fail the reply.
        }
      }
      socket.end(packet({
        frame: this.discoveryFrame(mount),
        peers: [...mount.beacons.values()].map(entry => entry.frame),
        tunneled: mount.inboundTunnels.has(request.frame.senderNodeId),
      }));
      return;
    }
    if (request.type === 'tunnel') {
      await this.serveInboundTunnel(socket, request.frame);
      return;
    }
    if (request.type === 'relay' || request.type === 'relay_send') {
      const frame: SealedNetworkFrame = request.frame;
      const reject = (error: string) => {
        socket.end(packet({ error }));
      };
      const mount = [...this.mounts.values()].find(
        m => m.keys.swarmTopicId === frame?.swarmTopicId,
      );
      if (!mount) {
        reject('Unknown swarm');
        return;
      }
      if (
        request.networkId !== undefined &&
        request.networkId !== mount.config.networkId
      ) {
        reject('Relay network mismatch');
        return;
      }
      // Membership proof: only a holder of the network key may relay or spool a frame.
      try {
        unsealNetworkFrame(frame, mount.keys);
      } catch {
        reject('Unable to authenticate or decode relay request');
        return;
      }
      const target = typeof request.target === 'string' ? request.target : '';
      if (!target) {
        reject('Invalid relay target');
        return;
      }
      if (
        request.spoolKey !== undefined &&
        (typeof request.spoolKey !== 'string' ||
          request.spoolKey.length === 0 ||
          request.spoolKey.length > MAX_RELAY_SPOOL_KEY_LENGTH ||
          /[\u0000-\u001f\u007f]/.test(request.spoolKey))
      ) {
        socket.end(packet({ error: 'Invalid relay spool key' }));
        return;
      }
      const spoolIfOffline = request.spoolIfOffline === true;
      if (
        spoolIfOffline &&
        (!/^ed25519:[0-9a-f]{64}$/.test(target) || target === this.localNodeId)
      ) {
        socket.end(packet({ error: 'Invalid relay spool target' }));
        return;
      }
      let liveError: Error | undefined;
      try {
        const tunnel = mount.inboundTunnels.get(target);
        let replyFrame: SealedNetworkFrame;
        if (tunnel) {
          const reply = await this.requestOverTunnel(tunnel, { type: 'relay_deliver', frame }, socket);
          if (reply?.error) {
            // A reachable target's signed decision is deterministic; never convert it to
            // durable offline spooling or let target-controlled text drive classification.
            socket.end(packet({ error: String(reply.error) }));
            return;
          }
          replyFrame = reply.frame;
        } else {
          // A spoolable request must fail over fast: an unreachable advertised endpoint
          // can never be allowed to burn the sender's whole frame budget before spooling.
          const directTimeoutMs = spoolIfOffline
            ? Math.min(this.options.socketTimeoutMs ?? 90_000, RELAY_CONNECT_TIMEOUT_MS)
            : this.options.socketTimeoutMs ?? 90_000;
          replyFrame = await this.directFrame(
            mount,
            target,
            this.endpointFor(mount, target),
            frame,
            directTimeoutMs,
          );
        }
        socket.end(packet({ ok: true, frame: replyFrame }));
        return;
      } catch (error) {
        liveError = asError(error);
      }
      // Only connection-class failures prove the target is offline. Authentication/binding
      // mismatches, malformed responses, and protocol errors remain visible to the sender;
      // spooling them would fabricate a durable handoff and retry a poison frame for its TTL.
      const targetOffline =
        !(liveError instanceof LocalDeliveryError) &&
        (liveError instanceof PeerUnreachableError ||
          liveError instanceof DeliveryTimeoutError ||
          isConnectionFailure(liveError));
      if (!targetOffline) {
        socket.end(packet({ error: liveError.message }));
        return;
      }
      if (!spoolIfOffline) {
        throw liveError;
      }
      try {
        socket.end(packet(this.spoolRelayFrame(mount, target, frame, request.blobs, request.spoolKey)));
      } catch (error) {
        socket.end(packet({ error: error instanceof Error ? error.message : String(error) }));
      }
      return;
    }
    if (request.type === 'relay_blob') {
      const mount = [...this.mounts.values()].find(
        m => m.keys.swarmTopicId === request.swarmTopicId,
      );
      if (!mount) throw new Error('Unknown swarm');
      const target = typeof request.target === 'string' ? request.target : '';
      const hash = typeof request.hash === 'string' ? request.hash : '';
      if (!target || !/^[a-f0-9]{64}$/.test(hash)) throw new Error('Invalid relay blob request');
      // The seed's own spool is authoritative: serve it before trying to reach the
      // original sender, which may itself be offline. The lookup is network-scoped.
      const cached = await this.readSpooledBlob(mount, hash);
      if (cached) {
        socket.write(packet({ size: cached.length }));
        socket.end(cached);
        return;
      }
      const tunnel = mount.inboundTunnels.get(target);
      const bytes = tunnel
        ? await this.blobOverTunnel(tunnel, hash, socket)
        : await this.readBlobAt(this.endpointFor(mount, target), hash);
      socket.write(packet({ size: bytes.length }));
      socket.end(bytes);
      return;
    }
    if (request.type === 'blob') {
      const blob = this.blobs.get(request.hash);
      if (!blob) {
        throw new Error('Unknown blob');
      }
      socket.write(packet({ size: blob.size }));
      for await (const chunk of createReadStream(blob.path)) {
        if (!socket.write(chunk)) {
          await once(socket, 'drain');
        }
      }
      socket.end();
      return;
    }
    if (request.type !== 'frame') {
      throw new Error('Unknown request');
    }
    const frame: SealedNetworkFrame = request.frame;
    const m = [...this.mounts.values()].find(m => m.keys.swarmTopicId === frame.swarmTopicId);
    if (!m) {
      throw new Error('Unknown swarm');
    }
    const event = this.inboundEvent(m, frame);
    const reply = await this.inboundReply(event);
    socket.end(
      packet({
        frame: sealNetworkFrame(
          {
            requestNonce: frame.nonce,
            dataBase64: Buffer.from(reply).toString('base64'),
          },
          m.keys,
          this.localNodeId,
        ),
      }),
    );
  }

  private async inboundReply(
    event: InboundFrameEvent & { rawBytes: Uint8Array },
  ): Promise<Uint8Array> {
    let reply: Uint8Array = new Uint8Array();
    for (const handler of this.handlers) {
      const value = await handler(event);
      if (value !== undefined) {
        reply = value;
      }
    }
    const value = await this.ctx?.serial('transport/inbound', event);
    if (value !== undefined) {
      reply = value;
    }
    return reply;
  }

  private inboundEvent(
    m: Mount,
    frame: SealedNetworkFrame,
  ): InboundFrameEvent & { rawBytes: Uint8Array } {
    const inner = unsealNetworkFrame<Record<string, unknown>>(frame, m.keys);
    if (!inner || typeof inner !== 'object') {
      throw new Error('Invalid frame payload');
    }
    const wrapped = Object.hasOwn(inner, 'dataBase64');
    const mode = wrapped ? inner.mode : 'unicast';
    if (mode !== 'unicast' && mode !== 'broadcast') {
      throw new Error('Invalid frame mode');
    }
    if (wrapped && typeof inner.dataBase64 !== 'string') {
      throw new Error('Invalid frame payload');
    }
    const rawBytes = wrapped
      ? Buffer.from(inner.dataBase64 as string, 'base64')
      : Buffer.from(JSON.stringify(inner));
    let payload: unknown = wrapped ? rawBytes : inner;
    if (wrapped) {
      try {
        payload = JSON.parse(rawBytes.toString('utf8'));
      } catch {
        // Preserve opaque binary
      }
    }
    return {
      networkId: m.config.networkId,
      senderNodeId: frame.senderNodeId,
      mode,
      subTopic: typeof inner.subTopic === 'string' ? inner.subTopic : undefined,
      payload,
      rawBytes,
    };
  }

  async shareFile(filePath: string): Promise<BlobAttachment> {
    await this.start();
    const h = new Blake3Hasher();
    let size = 0;
    for await (const bytes of createReadStream(filePath)) {
      h.update(bytes);
      size += bytes.length;
    }
    const hash = h.digest();
    this.blobs.set(hash, { path: filePath, size });
    const name = basename(filePath);
    return {
      name,
      mimeType: mimeType(name),
      sizeBytes: size,
      blake3Hash: hash,
      ticket: `dealer-blob://${encodeURIComponent(this.localNodeId)}@${this.endpointAddr}/${hash}?size=${size}&name=${encodeURIComponent(name)}`,
    };
  }

  async fetchBlobToPath(ticket: string, destPath: string): Promise<void> {
    if (this.closed) {
      throw new Error('Transport disposed');
    }
    const url = new URL(ticket);
    const hash = url.pathname.slice(1);
    const size = Number(url.searchParams.get('size'));
    if (
      url.protocol !== 'dealer-blob:' ||
      !/^[a-f0-9]{64}$/.test(hash) ||
      !url.searchParams.has('size') ||
      !Number.isSafeInteger(size) ||
      size < 0
    ) {
      throw new Error('Invalid blob ticket');
    }
    await mkdir(dirname(destPath), { recursive: true, mode: 0o700 });
    const temp = `${destPath}.${randomUUID()}.part`;
    let nodeId: string;
    try {
      nodeId = decodeURIComponent(url.username);
    } catch {
      throw new Error('Invalid blob ticket');
    }
    const owner = [...this.mounts.values()].find(mount => mount.peers.has(nodeId));
    const address = nodeId === this.localNodeId ? this.endpointAddr : owner?.peers.get(nodeId) ?? url.host;
    if (!address) {
      throw new Error('Transport endpoint unavailable');
    }
    const fallbacks = nodeId === this.localNodeId ? [] : this.peerFallbacks(nodeId, this.mounts.values());
    // Relay seeds keep a content-addressed cache of spooled attachments. When the original
    // sender is now offline, ask each configured seed *inside that seed's own network* so
    // the blob cache never becomes a cross-network read path. writeBlob still proves BLAKE3.
    const seedFallbacks = new Map<string, { mount: Mount; seed: string }>();
    if (nodeId !== this.localNodeId) {
      for (const mount of this.mounts.values()) {
        for (const seed of mount.config.bootstrapPeers ?? []) {
          if (seed === this.endpointAddr || seed === address) {
            continue;
          }
          seedFallbacks.set(`${mount.keys.swarmTopicId}\n${seed}`, { mount, seed });
        }
      }
    }
    try {
      let lastError: unknown;
      try {
        // Seed fallbacks prove the sender may be offline: fail over fast instead of burning
        // the whole socket budget on its blackholed endpoint before asking the seed cache.
        await this.fetchBlobDirect(
          address,
          hash,
          size,
          temp,
          fallbacks.length > 0 || seedFallbacks.size > 0,
        );
      } catch (error) {
        lastError = error;
        // The advertised endpoint is NAT-bound: try the peer's own tunnel to this node,
        // then every seed that can relay to it, until one transfer walks the hash.
        for (const fallback of fallbacks) {
          await rm(temp, { force: true });
          try {
            if ('tunnel' in fallback) {
              await this.writeBlobBytes(await this.blobOverTunnel(fallback.tunnel, hash), size, hash, temp);
            } else {
              await this.fetchBlobRelay(fallback, nodeId, hash, size, temp);
            }
            lastError = undefined;
            break;
          } catch (next) {
            lastError = next;
          }
        }
        // The sender may be the one offline; its relay seed still holds the spooled blob.
        for (const seedFallback of seedFallbacks.values()) {
          if (lastError === undefined) {
            break;
          }
          await rm(temp, { force: true });
          try {
            await this.fetchBlobRelay(seedFallback, nodeId, hash, size, temp);
            lastError = undefined;
          } catch (next) {
            lastError = next;
          }
        }
      }
      if (lastError !== undefined) {
        throw lastError;
      }
      await rename(temp, destPath);
    } finally {
      await rm(temp, { force: true });
    }
  }

  private async fetchBlobDirect(
    address: string,
    hash: string,
    size: number,
    temp: string,
    fastFail: boolean,
  ): Promise<void> {
    const socket = this.track(createConnection(parseEndpoint(address)));
    const timeoutMs = this.options.socketTimeoutMs ?? 90_000;
    socket.setTimeout(fastFail ? Math.min(timeoutMs, RELAY_CONNECT_TIMEOUT_MS) : timeoutMs);
    const reader = new SocketReader(socket);
    try {
      socket.write(packet({ type: 'blob', hash }));
      const metadata = await reader.json();
      if (metadata.size !== size) {
        throw new Error('Blob size mismatch');
      }
      await this.writeBlob(reader, size, hash, temp, error => socket.destroy(error));
    } finally {
      socket.destroy();
    }
  }

  private async fetchBlobRelay(
    relay: { mount: Mount; seed: string },
    nodeId: string,
    hash: string,
    size: number,
    temp: string,
  ): Promise<void> {
    const socket = this.track(createConnection(parseEndpoint(relay.seed)));
    const reader = new SocketReader(socket);
    try {
      socket.write(packet({
        type: 'relay_blob',
        swarmTopicId: relay.mount.keys.swarmTopicId,
        target: nodeId,
        hash,
      }));
      const metadata = await reader.json();
      if (metadata?.error) {
        throw new Error(String(metadata.error));
      }
      if (metadata?.size !== size) {
        throw new Error('Blob size mismatch');
      }
      await this.writeBlob(reader, size, hash, temp, error => socket.destroy(error));
    } finally {
      socket.destroy();
    }
  }

  private async writeBlob(
    reader: SocketReader,
    size: number,
    hash: string,
    temp: string,
    abort: (error: Error) => void,
  ): Promise<void> {
    const output = createWriteStream(temp, { flags: 'wx', mode: 0o600 });
    // Keep stream errors handled during network reads; writes observe them below.
    let outputError: Error | undefined;
    output.on('error', error => {
      outputError = error;
      abort(error);
    });
    try {
      const digest = new Blake3Hasher();
      let received = 0;
      for await (const chunk of reader.remaining()) {
        received += chunk.length;
        if (received > size) {
          throw new Error('Blob size exceeded');
        }
        digest.update(chunk);
        if (outputError) {
          throw outputError;
        }
        if (!output.write(chunk)) {
          await once(output, 'drain');
        }
      }
      if (received !== size || digest.digest() !== hash) {
        throw new Error('Blob hash or size mismatch');
      }
      output.end();
      await once(output, 'close');
      if (outputError) {
        throw outputError;
      }
    } finally {
      output.destroy();
      if (!output.closed) {
        await once(output, 'close').catch(() => {});
      }
    }
  }

  /** Persist a blob received as one packet under the same size/hash contract as streaming. */
  private async writeBlobBytes(bytes: Buffer, size: number, hash: string, temp: string): Promise<void> {
    const digest = new Blake3Hasher();
    digest.update(bytes);
    if (bytes.length !== size || digest.digest() !== hash) {
      throw new Error('Blob hash or size mismatch');
    }
    await writeFile(temp, bytes, { flags: 'wx', mode: 0o600 });
  }

  async dispose(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    await this.starting?.catch(() => {});
    clearInterval(this.timer);
    const serverClose = this.server?.listening
      ? new Promise<void>((resolve, reject) =>
          this.server!.close(error => (error ? reject(error) : resolve())),
        )
      : undefined;
    for (const socket of this.sockets) {
      socket.destroy();
    }
    if (this.udp) {
      try {
        this.udp.close();
      } catch {
        // Not bound
      }
    }
    await this.tick;
    await serverClose;
    for (const m of this.mounts.values()) {
      this.persistRelaySpool(m);
    }
    await Promise.all([...this.mounts.values()].map(m => rm(m.file, { force: true })));
    this.mounts.clear();
    this.handlers.clear();
    this.mountHandlers.clear();
  }
}

function parseEndpoint(address: string): { host: string; port: number } {
  const url = new URL(`tcp://${address}`);
  const port = Number(url.port);
  if (!url.hostname || !Number.isInteger(port) || port < 1 || port > 65535 || url.pathname !== '') {
    throw new Error('Invalid endpoint');
  }
  return { host: url.hostname.replace(/^\[|\]$/g, ''), port };
}

function mimeType(name: string): string {
  const ext = name.split('.').pop()?.toLowerCase();
  return (
    (
      {
        png: 'image/png',
        jpg: 'image/jpeg',
        jpeg: 'image/jpeg',
        mp4: 'video/mp4',
        mp3: 'audio/mpeg',
        wav: 'audio/wav',
        json: 'application/json',
        txt: 'text/plain',
      } as Record<string, string>
    )[ext ?? ''] ?? 'application/octet-stream'
  );
}

export function createTransportPlugin(
  options: TransportPluginOptions = {},
): Plugin<TransportPluginOptions> {
  return {
    name: 'transport',
    apply(ctx, config) {
      const service = new P2PTransportService({ ...options, ...config }, ctx);
      ctx.provide('transport', service);
      return () => service.dispose();
    },
  };
}

/** Optional native engine bridge. No cargo or binary is needed by the Node transport. */
export class IrohSidecarBridge {
  private child: ChildProcessWithoutNullStreams;
  private sequence = 0;
  private pending = new Map<
    number,
    {
      resolve: (value: any) => void;
      reject: (error: Error) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();

  constructor(binaryPath: string) {
    this.child = spawn(binaryPath, [], { stdio: 'pipe' });
    this.child.stderr.resume();
    this.child.stdin.on('error', error => this.fail(error));
    const lines = createInterface({ input: this.child.stdout });
    lines.on('line', line => {
      try {
        const message = JSON.parse(line);
        const entry = this.pending.get(message.id);
        if (!entry) {
          return;
        }
        clearTimeout(entry.timer);
        this.pending.delete(message.id);
        if (message.error) {
          entry.reject(new Error(message.error.message));
        } else {
          entry.resolve(message.result);
        }
      } catch {
        this.fail(new Error('Invalid sidecar JSON-RPC response'));
      }
    });
    this.child.on('error', error => this.fail(error));
    this.child.on('exit', () => {
      lines.close();
      this.fail(new Error('Sidecar exited'));
    });
  }

  private fail(error: Error): void {
    for (const entry of this.pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(error);
    }
    this.pending.clear();
  }

  call(method: string, params: unknown = {}, timeoutMs = 10000): Promise<any> {
    if (this.child.exitCode !== null || this.child.killed) {
      return Promise.reject(new Error('Sidecar is stopped'));
    }
    const id = ++this.sequence;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error('Sidecar timeout'));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.child.stdin.write(
        `${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`,
        error => {
          if (error) {
            this.fail(error);
          }
        },
      );
    });
  }

  dispose(): void {
    this.fail(new Error('Sidecar disposed'));
    this.child.kill();
  }
}
