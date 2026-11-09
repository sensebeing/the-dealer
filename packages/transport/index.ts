import { createServer, createConnection, type Socket, type Server } from 'node:net';
import { createSocket, type Socket as DatagramSocket } from 'node:dgram';
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, readdir, readFile, writeFile, rename, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { once } from 'node:events';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { createInterface } from 'node:readline';
import {
  deriveNetworkKeys,
  generateNodeKeyPair,
  sealNetworkFrame,
  unsealNetworkFrame,
} from '../core-types/index.ts';
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
const GROUP = '239.255.42.99';

function packet(value: unknown): Buffer {
  const body = Buffer.from(JSON.stringify(value));
  if (body.length > MAX_FRAME) {
    throw new Error('Frame exceeds limit');
  }
  const size = Buffer.alloc(4);
  size.writeUInt32BE(body.length);
  return Buffer.concat([size, body]);
}

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
        throw new Error('Truncated stream');
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
      throw new Error('Frame exceeds limit');
    }
    return JSON.parse((await this.exact(size)).toString());
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
  listenPort?: number;
  listenHost?: string;
  advertisedHost?: string;
  discoveryInterface?: string;
  mDnsPort?: number;
  discoveryIntervalMs?: number;
}

interface Mount {
  config: NetworkMountConfig;
  keys: DerivedNetworkKeys;
  peers: Map<string, string>;
  file: string;
}

type Handler = (
  event: InboundFrameEvent & { rawBytes?: Uint8Array },
) => Promise<Uint8Array | void> | Uint8Array | void;

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
  private timer?: ReturnType<typeof setInterval>;
  private tick?: Promise<void>;
  private closed = false;
  private sockets = new Set<Socket>();
  private blobs = new Map<string, { path: string; size: number }>();

  constructor(options: TransportPluginOptions = {}, ctx?: Context) {
    this.options = options;
    this.ctx = ctx;
    this.identityKeyPair = options.localNodeId ? undefined : generateNodeKeyPair();
    this.localNodeId = options.localNodeId ?? this.identityKeyPair!.nodeId;
  }

  private track(socket: Socket): Socket {
    this.sockets.add(socket);
    socket.once('close', () => this.sockets.delete(socket));
    socket.setTimeout(10_000, () => socket.destroy(new Error('Transport timeout')));
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
        this.options.listenHost ?? '127.0.0.1',
      );
      await once(server, 'listening');
      const address = server.address();
      if (!address || typeof address === 'string') {
        throw new Error('Invalid listener address');
      }
      this.endpointAddr = `${this.options.advertisedHost ?? '127.0.0.1'}:${address.port}`;
      const udp = createSocket({ type: 'udp4', reuseAddr: true });
      this.udp = udp;
      udp.on('error', () => {}); // Registry discovery remains available on hosts disabling multicast.
      udp.on('message', bytes => {
        try {
          this.learn(JSON.parse(bytes.toString()));
        } catch {
          // Unauthenticated discovery
        }
      });
      udp.bind(this.options.mDnsPort ?? 45873, () => {
        try {
          udp.addMembership(GROUP, this.options.discoveryInterface ?? '127.0.0.1');
          udp.setMulticastInterface(this.options.discoveryInterface ?? '127.0.0.1');
          udp.setMulticastLoopback(true);
        } catch {
          // Multicast unavailable
        }
      });
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
      throw new Error(`Network not mounted: ${networkId}`);
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
    const mount = {
      config: structuredClone(config),
      keys,
      peers: new Map<string, string>(),
      file,
    };
    this.mounts.set(config.networkId, mount);
    try {
      await this.announce(mount);
      await this.discover();
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

  getNetworkConfig(networkId: string): NetworkMountConfig {
    return structuredClone(this.get(networkId).config);
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
    await this.tick;
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
    parseEndpoint(address);
    if (nodeId !== this.localNodeId) {
      this.get(networkId).peers.set(nodeId, address);
    }
  }

  private learn(frame: SealedNetworkFrame): void {
    const m = [...this.mounts.values()].find(m => m.keys.swarmTopicId === frame.swarmTopicId);
    if (!m) {
      return;
    }
    const beacon = unsealNetworkFrame<{
      type: string;
      endpointAddr: string;
      expires: number;
    }>(frame, m.keys);
    if (
      beacon.type !== 'discovery' ||
      beacon.expires < Date.now() ||
      beacon.expires > Date.now() + 30_000
    ) {
      return;
    }
    this.registerPeerEndpoint(m.config.networkId, frame.senderNodeId, beacon.endpointAddr);
  }

  private async announce(m: Mount): Promise<void> {
    const frame = sealNetworkFrame(
      {
        type: 'discovery',
        endpointAddr: this.endpointAddr,
        expires: Date.now() + 10_000,
      },
      m.keys,
      this.localNodeId,
    );
    const bytes = Buffer.from(JSON.stringify(frame));
    const temp = `${m.file}.${randomUUID()}`;
    try {
      await writeFile(temp, bytes, { mode: 0o600 });
      await rename(temp, m.file);
    } finally {
      await rm(temp, { force: true });
    }
    try {
      this.udp?.send(bytes, m.config.mDnsPort ?? this.options.mDnsPort ?? 45873, GROUP);
    } catch {
      // Local registry is authoritative fallback
    }
  }

  private async discover(): Promise<void> {
    for (const m of this.mounts.values()) {
      try {
        await this.announce(m);
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
    timeoutMs = 5000,
  ): Promise<Uint8Array> {
    const address = m.peers.get(target);
    if (!address) {
      throw new Error(`Unknown peer: ${target}`);
    }
    const socket = this.track(createConnection(parseEndpoint(address)));
    socket.setTimeout(timeoutMs);
    try {
      socket.write(packet({ type: 'frame', frame }));
      const response = await new SocketReader(socket).json();
      const payload = unsealNetworkFrame<{
        requestNonce: string;
        dataBase64: string;
      }>(response.frame, m.keys);
      if (response.frame.senderNodeId !== target || payload.requestNonce !== frame.nonce) {
        throw new Error('Response binding mismatch');
      }
      return Buffer.from(payload.dataBase64, 'base64');
    } finally {
      socket.destroy();
    }
  }

  async sendUnicast(
    networkId: string,
    target: string,
    bytes: Uint8Array,
    opts?: { timeoutMs?: number },
  ): Promise<Uint8Array> {
    const m = this.get(networkId);
    return this.exchange(m, target, this.sealed(m, 'unicast', bytes), opts?.timeoutMs);
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

  async broadcast(networkId: string, subTopic: string, bytes: Uint8Array): Promise<void> {
    const m = this.get(networkId);
    const frame = this.sealed(m, 'broadcast', bytes, subTopic);
    await Promise.allSettled([...m.peers.keys()].map(target => this.exchange(m, target, frame)));
  }

  private async receive(socket: Socket): Promise<void> {
    const request = await new SocketReader(socket).json();
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
    const event: InboundFrameEvent & { rawBytes: Uint8Array } = {
      networkId: m.config.networkId,
      senderNodeId: frame.senderNodeId,
      mode,
      subTopic: typeof inner.subTopic === 'string' ? inner.subTopic : undefined,
      payload,
      rawBytes,
    };
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
    const socket = this.track(createConnection(parseEndpoint(url.host)));
    const reader = new SocketReader(socket);
    const output = createWriteStream(temp, { flags: 'wx', mode: 0o600 });
    // Keep stream errors handled during network reads; writes observe them below.
    let outputError: Error | undefined;
    output.on('error', error => {
      outputError = error;
      socket.destroy(error);
    });
    try {
      socket.write(packet({ type: 'blob', hash }));
      const metadata = await reader.json();
      if (metadata.size !== size) {
        throw new Error('Blob size mismatch');
      }
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
      await rename(temp, destPath);
    } finally {
      socket.destroy();
      output.destroy();
      if (!output.closed) {
        await once(output, 'close').catch(() => {});
      }
      await rm(temp, { force: true });
    }
  }

  async dispose(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    await this.starting?.catch(() => {});
    clearInterval(this.timer);
    await this.tick;
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
    if (this.server?.listening) {
      await new Promise<void>((resolve, reject) =>
        this.server!.close(error => (error ? reject(error) : resolve())),
      );
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
  return { host: url.hostname, port };
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
