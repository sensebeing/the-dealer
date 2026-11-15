import { randomUUID } from 'node:crypto';
import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { dirname } from 'node:path';
import { generateNodeKeyPair, signNodeCard, verifyNodeCard } from '../core-types/index.ts';
import type {
  AdmissionMode,
  Disposable,
  InboundFrameEvent,
  NetworkMountConfig,
  NodeCard,
  NodeService,
  TransportService,
} from '../core-types/index.ts';
import type { Context, Plugin } from '../kernel/index.ts';

export interface NodePluginConfig {
  keyPair?: ReturnType<typeof generateNodeKeyPair>;
  member: string;
  nodeName: string;
  capabilities?: NodeCard['capabilities'];
  runtimeState?: NodeCard['runtimeState'];
  admissionMode?: AdmissionMode;
  networks?: readonly NetworkMountConfig[];
  exchangeIntervalMs?: number;
  /** Presence gossip budget per peer; ephemeral beacons never inherit the RPC socket timeout. */
  exchangeTimeoutMs?: number;
  /** Durable verified peer cards so member selectors resolve before the peer reconnects. */
  peersPath?: string;
}

const PEER_DIRECTORY_SCHEMA = 'the-dealer-peer-directory/v1';
const MAX_GOSSIPED_PEER_CARDS = 128;


interface ObservableTransport extends TransportService {
  identityKeyPair?: ReturnType<typeof generateNodeKeyPair>;
  getNetworkConfig?(networkId: string): NetworkMountConfig | undefined;
  onNetworksChanged?(handler: () => void | Promise<void>): Disposable;
}

/** The directory owns signed presence; transport owns endpoints and network secrets. */
export class PeerDirectoryService implements NodeService {
  readonly privateKeyPem: string;
  private transport: ObservableTransport;
  private ctx: Context;
  private config: NodePluginConfig;
  private keyPair: ReturnType<typeof generateNodeKeyPair>;
  private self = new Map<string, NodeCard>();
  private peers = new Map<string, Map<string, NodeCard>>();
  private revisions = new Map<string, number>();
  private modes = new Map<string, AdmissionMode>();
  private timer: ReturnType<typeof setInterval>;
  private listeners: Disposable[] = [];
  private pending: Promise<void> = Promise.resolve();
  private disposed = false;
  private exchanging = false;
  private readonly peersPath?: string;

  constructor(ctx: Context, config: NodePluginConfig) {
    this.ctx = ctx;
    this.config = structuredClone(config);
    this.transport = ctx.services.transport;
    this.keyPair = config.keyPair ?? this.transport.identityKeyPair ?? generateNodeKeyPair();
    this.privateKeyPem = this.keyPair.privateKeyPem;
    if (this.keyPair.nodeId !== this.transport.localNodeId) {
      throw new Error('Node keyPair must match transport localNodeId');
    }
    this.peersPath = config.peersPath;
    this.loadPersistedPeers();
    const observe = this.transport.onNetworksChanged?.(() => this.publish());
    if (observe) {
      this.listeners.push(observe);
    }
    // Revision checks make duplicate delivery through both supported ingress APIs harmless.
    this.listeners.push(ctx.on('transport/inbound', event => this.accept(event)));
    const inbound = this.transport.onInboundFrame?.(event => this.accept(event));
    if (inbound) {
      this.listeners.push(inbound);
    }
    this.timer = setInterval(() => {
      if (this.exchanging) {
        return;
      }
      this.exchanging = true;
      void this.publish(false)
        .catch(() => {})
        .finally(() => {
          this.exchanging = false;
        });
    }, config.exchangeIntervalMs ?? 250);
    this.timer.unref();
    void this.publish().catch(() => {});
  }

  /** Load signed cards persisted by a previous incarnation; each card is verified again. */
  private loadPersistedPeers(): void {
    if (!this.peersPath || !existsSync(this.peersPath)) {
      return;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(readFileSync(this.peersPath, 'utf8'));
    } catch {
      throw new Error('Invalid peer directory store');
    }
    if (
      typeof parsed !== 'object' ||
      parsed === null ||
      Array.isArray(parsed) ||
      (parsed as { schema?: unknown }).schema !== PEER_DIRECTORY_SCHEMA ||
      !Array.isArray((parsed as { cards?: unknown }).cards)
    ) {
      throw new Error('Invalid peer directory store');
    }
    for (const card of (parsed as { cards: unknown[] }).cards) {
      this.acceptCard(card, undefined, false);
    }
  }

  /**
   * Persist the verified peer directory atomically. The self card is rebuilt on every
   * publish and is deliberately excluded: only remote members are durable facts here.
   */
  private persistPeers(): void {
    if (!this.peersPath) {
      return;
    }
    const cards = [...this.peers.values()]
      .flatMap(directory => [...directory.values()])
      .filter(card => card.nodeId !== this.keyPair.nodeId)
      .sort((a, b) => a.networkId.localeCompare(b.networkId) || a.nodeId.localeCompare(b.nodeId));
    mkdirSync(dirname(this.peersPath), { recursive: true, mode: 0o700 });
    const temporary = `${this.peersPath}.${randomUUID()}.tmp`;
    try {
      const fd = openSync(temporary, 'wx', 0o600);
      try {
        writeFileSync(
          fd,
          JSON.stringify({ schema: PEER_DIRECTORY_SCHEMA, cards }),
        );
        fsyncSync(fd);
      } finally {
        closeSync(fd);
      }
      renameSync(temporary, this.peersPath);
    } finally {
      rmSync(temporary, { force: true });
    }
  }

  /** Accept the newest verified card for one network; returns true when the directory changed. */
  private acceptCard(
    value: unknown,
    expectedNetworkId: string | undefined,
    emit: boolean,
  ): boolean {
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
      return false;
    }
    const card = value as NodeCard;
    if (
      typeof card.nodeId !== 'string' ||
      typeof card.networkId !== 'string' ||
      (expectedNetworkId !== undefined && card.networkId !== expectedNetworkId) ||
      card.nodeId === this.keyPair.nodeId
    ) {
      return false;
    }
    if (!Number.isSafeInteger(card.revision) || card.revision < 1 || !verifyNodeCard(card)) {
      return false;
    }
    if (
      !card.identity ||
      typeof card.identity.member !== 'string' ||
      typeof card.identity.nodeName !== 'string' ||
      !Array.isArray(card.capabilities?.skills) ||
      !card.runtimeState
    ) {
      return false;
    }
    if (!['open', 'approval_required', 'dnd', 'closed'].includes(card.admissionMode)) {
      return false;
    }
    const directory = this.peers.get(card.networkId) ?? new Map<string, NodeCard>();
    if ((directory.get(card.nodeId)?.revision ?? 0) >= card.revision) {
      return false;
    }
    if (card.endpointAddr) {
      try {
        this.transport.registerPeerEndpoint?.(card.networkId, card.nodeId, card.endpointAddr);
      } catch {
        // The card is verified identity; its endpoint is transport-owned reachability and
        // may be registered later once the network is mounted. A stale endpoint must never
        // discard durable member knowledge.
      }
    }
    directory.set(card.nodeId, structuredClone(card));
    this.peers.set(card.networkId, directory);
    if (emit) {
      this.ctx.emit('node/peer-updated', structuredClone(card));
    }
    return true;
  }

  private accept(event: InboundFrameEvent): void {
    const message = event.payload as
      | { type?: string; card?: NodeCard; knownCards?: unknown }
      | null;
    if (message?.type !== 'peer.card') {
      return;
    }
    if (!this.transport.listMountedNetworks().includes(event.networkId)) {
      return;
    }
    let changed = false;
    if (message.card) {
      changed = this.acceptCard(message.card, event.networkId, true) || changed;
    }
    // Seed card gossip: a relay seed (or any member) shares the verified cards it knows,
    // so a restarted peer learns its team before the members themselves reconnect.
    if (Array.isArray(message.knownCards)) {
      for (const knownCard of message.knownCards.slice(0, MAX_GOSSIPED_PEER_CARDS)) {
        changed = this.acceptCard(knownCard, event.networkId, true) || changed;
      }
    }
    if (changed) {
      this.persistPeers();
    }
  }

  private build(networkId: string): NodeCard {
    const mount =
      this.transport.getNetworkConfig?.(networkId) ??
      this.config.networks?.find(n => n.networkId === networkId);
    const capabilities = this.config.capabilities ?? { summary: '', skills: [] };
    const revision = (this.revisions.get(networkId) ?? 0) + 1;
    this.revisions.set(networkId, revision);
    return signNodeCard(
      {
        nodeId: this.keyPair.nodeId,
        networkId,
        revision,
        identity: { member: this.config.member, nodeName: this.config.nodeName },
        admissionMode:
          this.modes.get(networkId) ??
          mount?.admissionMode ??
          this.config.admissionMode ??
          'open',
        capabilities: {
          ...capabilities,
          skills: capabilities.skills.filter(
            s => !mount?.exposedSkills || mount.exposedSkills.includes(s.id),
          ),
        },
        runtimeState: this.config.runtimeState ?? {
          load: 'idle',
          activeTasks: 0,
          availableAgents: [],
        },
        endpointAddr: this.transport.endpointAddr,
        updatedAt: new Date().toISOString(),
      },
      this.keyPair.privateKeyPem,
    );
  }

  private publish(refresh = true): Promise<void> {
    if (this.disposed) {
      return Promise.resolve();
    }
    this.pending = this.pending.catch(() => {}).then(async () => {
      if (this.disposed) {
        return;
      }
      const networks = this.transport.listMountedNetworks();
      for (const id of this.self.keys()) {
        if (!networks.includes(id)) {
          // Verified remote cards outlive a temporary unmount: a later remount and the
          // offline outbox both still need member selectors, so only self state is dropped.
          this.self.delete(id);
        }
      }
      for (const networkId of networks) {
        if (refresh || !this.self.has(networkId)) {
          this.self.set(networkId, this.build(networkId));
        }
        const selfCard = this.self.get(networkId)!;
        // Persisted cards are loaded before the network mounts; re-register their last known
        // endpoints now so a daemon restart can reach peers that never stopped.
        for (const card of this.peers.get(networkId)?.values() ?? []) {
          if (!card.endpointAddr) {
            continue;
          }
          try {
            this.transport.registerPeerEndpoint?.(networkId, card.nodeId, card.endpointAddr);
          } catch {
            // A stale endpoint is replaced by the next verified card or discovery beacon.
          }
        }
        // Card gossip carries the verified team directory: a relay seed or any member can
        // hand a restarted peer the cards it needs before that member reconnects itself.
        const knownCards = [...(this.peers.get(networkId)?.values() ?? [])]
          .filter(card => card.nodeId !== selfCard.nodeId)
          .slice(0, MAX_GOSSIPED_PEER_CARDS);
        await this.transport.broadcast(
          networkId,
          'peer.card',
          Buffer.from(JSON.stringify({ type: 'peer.card', card: selfCard, knownCards })),
          // Presence is best-effort: one direct fast-fail plus a few relay seeds still gets a
          // useful share of the budget, while a blackholed peer can never stall the node.
          { timeoutMs: this.config.exchangeTimeoutMs ?? 4_000 },
        );
      }
    });
    return this.pending;
  }

  getSelfCard(networkId: string): NodeCard {
    // The self card is local identity, available before any network mount. Zero-config
    // single-machine use addresses this card without ever creating a network scope.
    if (!this.self.has(networkId)) {
      this.self.set(networkId, this.build(networkId));
    }
    return structuredClone(this.self.get(networkId)!);
  }

  async updateSelfProfile(
    patch: Partial<Pick<NodePluginConfig, 'member' | 'nodeName' | 'capabilities'>>,
  ): Promise<void> {
    Object.assign(this.config, structuredClone(patch));
    await this.publish();
  }

  async updateRuntimeState(state: Partial<NodeCard['runtimeState']>): Promise<void> {
    this.config.runtimeState = {
      load: 'idle',
      activeTasks: 0,
      availableAgents: [],
      ...this.config.runtimeState,
      ...structuredClone(state),
    };
    await this.publish();
  }

  async updateNetworkAdmissionMode(networkId: string, mode: AdmissionMode): Promise<void> {
    if (!this.transport.listMountedNetworks().includes(networkId)) {
      throw new Error(`Network not mounted: ${networkId}`);
    }
    this.getSelfCard(networkId);
    this.modes.set(networkId, mode);
    await this.publish();
  }

  listPeers(
    filter: {
      networkId?: string;
      member?: string;
      skillTag?: string;
      onlyAvailable?: boolean;
    } = {},
  ): readonly NodeCard[] {
    const mounted = this.transport.listMountedNetworks();
    return [...this.peers.values()]
      .flatMap(d => [...d.values()])
      .filter(
        card =>
          mounted.includes(card.networkId) &&
          (!filter.networkId || card.networkId === filter.networkId) &&
          (!filter.member || card.identity.member === filter.member) &&
          (!filter.skillTag ||
            card.capabilities.skills.some(
              s => s.id === filter.skillTag || s.tags?.includes(filter.skillTag!),
            )) &&
          (!filter.onlyAvailable ||
            (card.runtimeState.load === 'idle' && card.admissionMode === 'open')),
      )
      .map(card => structuredClone(card));
  }

  private matchSelector(card: NodeCard, selector: string): boolean {
    if (selector.startsWith('ed25519:')) {
      return card.nodeId === selector;
    }
    if (selector.startsWith('member:')) {
      return card.identity.member === selector.slice(7);
    }
    if (selector.startsWith('skill:')) {
      const term = selector.slice(6);
      return card.capabilities.skills.some(s => s.id === term || s.tags?.includes(term));
    }
    return `${card.identity.member}@${card.identity.nodeName}` === selector;
  }

  resolveTarget(selector: string, defaultNetworkId?: string): readonly NodeCard[] {
    let networkId = defaultNetworkId;
    const slash = selector.indexOf('/');
    if (slash >= 0) {
      networkId = selector.slice(0, slash);
      selector = selector.slice(slash + 1);
    }
    const remote = this.listPeers({ networkId }).filter(card => this.matchSelector(card, selector));
    if (remote.length > 0) {
      return remote;
    }
    const selfCard = this.getSelfCard(networkId ?? 'local');
    return this.matchSelector(selfCard, selector) ? [selfCard] : [];
  }

  async dispose(): Promise<void> {
    this.disposed = true;
    clearInterval(this.timer);
    for (const listener of this.listeners) {
      await listener.dispose();
    }
    await this.pending.catch(() => {});
    this.peers.clear();
    this.self.clear();
  }
}

export function createNodePlugin(config: NodePluginConfig): Plugin<NodePluginConfig> {
  return {
    name: 'node',
    inject: ['transport'],
    apply(ctx, override) {
      const service = new PeerDirectoryService(ctx, { ...config, ...override });
      ctx.provide('node', service);
      return () => service.dispose();
    },
  };
}
