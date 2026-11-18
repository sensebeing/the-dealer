import { existsSync, readFileSync } from 'node:fs';
import {
  verifyNodeCard,
} from '../core-types/index.ts';
import { writeFileAtomically } from '../fs-atomic/index.ts';
import { generateNodeKeyPair, signNodeCard } from '../core-types/index.ts';
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

type PeerCandidates = { newest: NodeCard; reachable?: NodeCard };

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && Object.getPrototypeOf(value) === Object.prototype;
}

function peerIdentityKey(identity: NodeCard['identity']): string {
  return JSON.stringify([identity.member, identity.nodeName]);
}

function isNewerPeerCard(a: NodeCard, b: NodeCard): boolean {
  const aTime = Date.parse(a.updatedAt) || 0;
  const bTime = Date.parse(b.updatedAt) || 0;
  if (aTime !== bTime) return aTime > bTime;
  if (a.revision !== b.revision) return a.revision > b.revision;
  return a.nodeId > b.nodeId;
}

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
  /** Networks with a newly accepted direct peer; the next beacon carries known cards once. */
  private readonly gossipRequested = new Set<string>();

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
    // Revision checks prevent duplicate delivery from rewriting the store.
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
      !isPlainObject(parsed) ||
      parsed.schema !== PEER_DIRECTORY_SCHEMA ||
      !Array.isArray(parsed.cards)
    ) {
      throw new Error('Invalid peer directory store');
    }
    const { cards, selfRevisions } = parsed;
    if (selfRevisions !== undefined) {
      if (!isPlainObject(selfRevisions)) {
        throw new Error('Invalid peer directory store');
      }
      for (const [networkId, revision] of Object.entries(selfRevisions)) {
        if (
          !networkId ||
          typeof revision !== 'number' ||
          !Number.isSafeInteger(revision) ||
          revision < 0
        ) {
          throw new Error('Invalid peer directory store');
        }
        this.revisions.set(networkId, revision);
      }
    }
    for (const card of cards) {
      // Persisted cards carry durable identity, not live reachability: the peer may be
      // offline, and its last endpoint is owned by transport discovery, not this store.
      this.acceptCard(card, undefined, false, false);
    }
    const retained = [...this.peers.values()].reduce((total, directory) => total + directory.size, 0);
    if (retained < cards.length) {
      this.persistPeers();
    }
  }

  /** Persist each remote key's latest verified card and the last self revision per network. */
  private persistPeers(): void {
    if (!this.peersPath) {
      return;
    }
    const cards = [...this.peers.values()]
      .flatMap(directory => [...directory.values()])
      .filter(card => card.nodeId !== this.keyPair.nodeId)
      .sort((a, b) => a.networkId.localeCompare(b.networkId) || a.nodeId.localeCompare(b.nodeId));
    writeFileAtomically(
      this.peersPath,
      JSON.stringify({
        schema: PEER_DIRECTORY_SCHEMA,
        selfRevisions: Object.fromEntries(this.revisions),
        cards,
      }),
    );
  }

  /** Project one active card per identity without changing the durable directory. */
  private activeNetworkCards(networkId: string, directory: Map<string, NodeCard>): NodeCard[] {
    const groups = new Map<string, PeerCandidates>();
    for (const card of directory.values()) {
      if (this.claimsLocalIdentity(card)) {
        continue;
      }
      const key = peerIdentityKey(card.identity);
      const group: PeerCandidates = groups.get(key) ?? { newest: card };
      if (isNewerPeerCard(card, group.newest)) {
        group.newest = card;
      }
      if (this.transport.isPeerReachable?.(networkId, card.nodeId) === true) {
        if (!group.reachable || isNewerPeerCard(card, group.reachable)) {
          group.reachable = card;
        }
      }
      groups.set(key, group);
    }
    return Array.from(groups.values(), ({ newest, reachable }) => reachable ?? newest);
  }

  private claimsLocalIdentity(card: NodeCard): boolean {
    return card.identity.member === this.config.member && card.identity.nodeName === this.config.nodeName;
  }

  /**
   * Record the newest verified revision for the signing key.
   * Only a card carried directly by its signing node may contribute an endpoint:
   * persisted and gossiped third-party cards prove identity, never live reachability.
   */
  private acceptCard(
    value: unknown,
    expectedNetworkId: string | undefined,
    registerEndpoint: boolean,
    emit: boolean,
  ): boolean {
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
      return false;
    }
    const card = value as NodeCard;
    if (
      typeof card.nodeId !== 'string' ||
      typeof card.networkId !== 'string' ||
      !card.networkId ||
      (expectedNetworkId !== undefined && card.networkId !== expectedNetworkId)
    ) {
      return false;
    }
    if (!Number.isSafeInteger(card.revision) || card.revision < 1 || !verifyNodeCard(card)) {
      return false;
    }
    if (card.nodeId === this.keyPair.nodeId) {
      // A verified card carrying our own identity is our previously published state. Advance
      // the local revision past it so a restarted daemon's cards are accepted immediately.
      if (card.revision <= (this.revisions.get(card.networkId) ?? 0)) {
        return false;
      }
      this.revisions.set(card.networkId, card.revision);
      return true;
    }
    if (
      !card.identity ||
      typeof card.identity.member !== 'string' ||
      typeof card.identity.nodeName !== 'string' ||
      typeof card.updatedAt !== 'string' ||
      !Array.isArray(card.capabilities?.skills) ||
      !card.runtimeState
    ) {
      return false;
    }
    if (!['open', 'approval_required', 'dnd', 'closed'].includes(card.admissionMode)) {
      return false;
    }
    const directory = this.peers.get(card.networkId) ?? new Map<string, NodeCard>();
    const existing = directory.get(card.nodeId);
    const unchanged = existing?.revision === card.revision;
    if (existing && (existing.revision > card.revision || (unchanged && existing.signature !== card.signature))) {
      return false;
    }
    if (!unchanged) {
      directory.set(card.nodeId, structuredClone(card));
      this.peers.set(card.networkId, directory);
    }
    if (this.claimsLocalIdentity(card)) {
      // Keep the signed revision as the floor for this key, while excluding it from routing.
      return !unchanged;
    }
    if (!unchanged && emit && registerEndpoint) {
      // Share known cards once when a peer announces a new revision directly.
      this.gossipRequested.add(card.networkId);
    }
    if (registerEndpoint && card.endpointAddr) {
      try {
        this.transport.registerPeerEndpoint?.(card.networkId, card.nodeId, card.endpointAddr);
      } catch {
        // Endpoint registration is transport-owned reachability; a failed registration
        // never discards verified member identity.
      }
    }
    if (emit && !unchanged) {
      // `direct` distinguishes live reachability from identity-only gossip so retry
      // schedulers never treat a third-party card as proof the member reconnected.
      this.ctx.emit('node/peer-updated', structuredClone(card), {
        direct: registerEndpoint,
      });
    }
    return !unchanged;
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
      const directSender = event.senderNodeId === message.card.nodeId;
      changed = this.acceptCard(message.card, event.networkId, directSender, true);
    }
    // Seed card gossip: a relay seed (or any member) shares the verified cards it knows,
    // so a restarted peer learns its team before the members themselves reconnect. Those
    // third-party entries never contribute endpoints: a gossiped NAT address is not proof
    // that the member is reachable from here.
    if (Array.isArray(message.knownCards)) {
      for (const knownCard of message.knownCards.slice(0, MAX_GOSSIPED_PEER_CARDS)) {
        changed = this.acceptCard(knownCard, event.networkId, false, true) || changed;
      }
    }
    if (changed) {
      this.persistPeers();
    }
  }

  private signSelfCard(networkId: string, revision: number, updatedAt = new Date().toISOString()): NodeCard {
    const mount =
      this.transport.getNetworkConfig?.(networkId) ??
      this.config.networks?.find(n => n.networkId === networkId);
    const capabilities = this.config.capabilities ?? { summary: '', skills: [] };
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
        updatedAt,
      },
      this.keyPair.privateKeyPem,
    );
  }

  private refreshSelfCards(refresh = true): void {
    const mounted = this.transport.listMountedNetworks();
    for (const id of this.self.keys()) {
      if (id !== 'local' && !mounted.includes(id)) {
        this.self.delete(id);
      }
    }
    let changed = false;
    for (const networkId of new Set(['local', ...mounted])) {
      if (refresh || !this.self.has(networkId)) {
        // The next self card must advance past any persisted or gossiped revision.
        const revision = Math.max((this.revisions.get(networkId) ?? 0) + 1, Date.now());
        this.revisions.set(networkId, revision);
        this.self.set(networkId, this.signSelfCard(networkId, revision));
        changed = true;
      }
    }
    if (changed) {
      this.persistPeers();
    }
  }

  private async publish(refresh = true): Promise<void> {
    if (this.disposed) {
      return;
    }
    this.refreshSelfCards(refresh);
    this.pending = this.pending.catch(() => {}).then(async () => {
      if (this.disposed) {
        return;
      }
      this.refreshSelfCards(false);
      for (const networkId of this.transport.listMountedNetworks()) {
        const selfCard = this.self.get(networkId);
        if (!selfCard) {
          continue;
        }
        // Card gossip is event-driven: startup/profile changes include the verified team
        // directory, and a newly accepted direct peer receives it once on the next beacon.
        // Steady-state 250ms presence heartbeats carry only the self card.
        const includeKnownCards = refresh || this.gossipRequested.delete(networkId);
        const directory = this.peers.get(networkId);
        const knownCards = includeKnownCards && directory
          ? this.activeNetworkCards(networkId, directory).slice(0, MAX_GOSSIPED_PEER_CARDS)
          : [];
        await this.transport.broadcast(
          networkId,
          'peer.card',
          Buffer.from(JSON.stringify({
            type: 'peer.card',
            card: selfCard,
            ...(includeKnownCards ? { knownCards } : {}),
          })),
          // Presence is best-effort: one direct fast-fail plus a few relay seeds still gets a
          // useful share of the budget, while a blackholed peer can never stall the node.
          { timeoutMs: this.config.exchangeTimeoutMs ?? 4_000 },
        );
      }
    });
    return this.pending;
  }

  getSelfCard(networkId: string): NodeCard {
    // The last local publication time keeps an unmounted projection stable across reads.
    const card = this.self.get(networkId) ?? this.signSelfCard(
      networkId,
      Math.max(this.revisions.get(networkId) ?? 1, 1),
      this.self.get('local')?.updatedAt,
    );
    return structuredClone(card);
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
    this.modes.set(networkId, mode);
    await this.publish();
  }

  private mountedPeerDirectories(networkId?: string): [string, Map<string, NodeCard>][] {
    const mounted = this.transport.listMountedNetworks();
    return [...this.peers].filter(([id]) => mounted.includes(id) && (!networkId || id === networkId));
  }

  listPeers(
    filter: {
      networkId?: string;
      member?: string;
      skillTag?: string;
      onlyAvailable?: boolean;
    } = {},
  ): readonly NodeCard[] {
    return this.mountedPeerDirectories(filter.networkId)
      .flatMap(([networkId, directory]) => this.activeNetworkCards(networkId, directory))
      .filter(
        card =>
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
    const selfCard = this.getSelfCard(networkId ?? 'local');
    if (!selector.startsWith('skill:') && this.matchSelector(selfCard, selector)) {
      return [selfCard];
    }
    if (selector.startsWith('ed25519:')) {
      return this.mountedPeerDirectories(networkId)
        .map(([, directory]) => directory.get(selector))
        .filter((card): card is NodeCard => card !== undefined && !this.claimsLocalIdentity(card))
        .map(card => structuredClone(card));
    }
    const remote = this.listPeers({ networkId }).filter(card => this.matchSelector(card, selector));
    if (selector.startsWith('member:') && remote.length > 1 && typeof this.transport.isPeerReachable === 'function') {
      const reachable = remote.filter(card => this.transport.isPeerReachable?.(card.networkId, card.nodeId) === true);
      if (reachable.length > 0) {
        return reachable;
      }
    }
    if (remote.length > 0) {
      return remote;
    }
    return this.matchSelector(selfCard, selector) ? [selfCard] : [];
  }

  async dispose(): Promise<void> {
    this.disposed = true;
    clearInterval(this.timer);
    for (const listener of this.listeners) {
      await listener.dispose();
    }
    await this.pending.catch(() => {});
    this.persistPeers();
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
