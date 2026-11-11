import { generateNodeKeyPair, isTailscaleIpv4Host, signNodeCard, verifyNodeCard } from '../core-types/index.ts';
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

  constructor(ctx: Context, config: NodePluginConfig) {
    this.ctx = ctx;
    this.config = structuredClone(config);
    this.transport = ctx.services.transport;
    this.keyPair = config.keyPair ?? this.transport.identityKeyPair ?? generateNodeKeyPair();
    this.privateKeyPem = this.keyPair.privateKeyPem;
    if (this.keyPair.nodeId !== this.transport.localNodeId) {
      throw new Error('Node keyPair must match transport localNodeId');
    }
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

  private accept(event: InboundFrameEvent): void {
    const message = event.payload as { type?: string; card?: NodeCard } | null;
    if (message?.type !== 'peer.card' || !message.card) {
      return;
    }
    const card = message.card;
    if (
      !this.transport.listMountedNetworks().includes(event.networkId) ||
      card.networkId !== event.networkId ||
      card.nodeId === this.keyPair.nodeId
    ) {
      return;
    }
    if (!Number.isSafeInteger(card.revision) || card.revision < 1 || !verifyNodeCard(card)) {
      return;
    }
    if (
      !card.identity ||
      typeof card.identity.member !== 'string' ||
      typeof card.identity.nodeName !== 'string' ||
      !Array.isArray(card.capabilities?.skills) ||
      !card.runtimeState
    ) {
      return;
    }
    if (!['open', 'approval_required', 'dnd', 'closed'].includes(card.admissionMode)) {
      return;
    }
    const directory = this.peers.get(card.networkId) ?? new Map<string, NodeCard>();
    if ((directory.get(card.nodeId)?.revision ?? 0) >= card.revision) {
      return;
    }
    if (card.endpointAddr) {
      try {
        // A card's self-reported LAN address must not replace the routed Tailscale
        // endpoint we observed when the peer was discovered over the tailnet.
        const existing = this.transport.listPeerEndpoints?.(card.networkId)?.get(card.nodeId);
        const endpointAddr = existing && isTailscaleIpv4Host(existing) && !isTailscaleIpv4Host(card.endpointAddr)
          ? `${existing.slice(0, existing.lastIndexOf(':'))}${card.endpointAddr.slice(card.endpointAddr.lastIndexOf(':'))}`
          : card.endpointAddr;
        this.transport.registerPeerEndpoint?.(card.networkId, card.nodeId, endpointAddr);
      } catch {
        return;
      }
    }
    directory.set(card.nodeId, structuredClone(card));
    this.peers.set(card.networkId, directory);
    this.ctx.emit('node/peer-updated', structuredClone(card));
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
          'approval_required',
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
          this.self.delete(id);
          this.peers.delete(id);
        }
      }
      for (const networkId of networks) {
        if (refresh || !this.self.has(networkId)) {
          this.self.set(networkId, this.build(networkId));
        }
        await this.transport.broadcast(
          networkId,
          'peer.card',
          Buffer.from(JSON.stringify({ type: 'peer.card', card: this.self.get(networkId) })),
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
