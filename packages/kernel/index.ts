import type {
  Disposable, TransportService, NodeService, EmitterService, RouterService, GateService,
  DispatcherService, NodeCard, MessageEnvelope, GateVerdict, DispatchReceipt,
} from '../core-types/index.ts';

export type { Disposable } from '../core-types/index.ts';
export type Cleanup = () => void | Promise<void>;
export interface Services {
  transport: TransportService; node: NodeService; emitter: EmitterService;
  router: RouterService; gate: GateService; dispatcher: DispatcherService;
}
/** Extend these interfaces via module augmentation for application services and events. */
export interface Events {
  'node/peer-updated': (card: NodeCard) => void;
  'emitter/prepare': (envelope: MessageEnvelope, next: (envelope: MessageEnvelope) => Promise<MessageEnvelope>) => MessageEnvelope | Promise<MessageEnvelope>;
  'gate/evaluate': (envelope: MessageEnvelope) => GateVerdict | null | undefined | Promise<GateVerdict | null | undefined>;
  'dispatcher/route': (envelope: MessageEnvelope) => DispatchReceipt | undefined | Promise<DispatchReceipt | undefined>;
}
export interface Plugin<C = unknown> {
  name: string; inject?: readonly (keyof Services)[];
  apply(ctx: Context, config: C): void | Cleanup;
}
// Heterogeneous profiles erase config types only at the composition boundary.
// Kernel.use preserves the plugin/config relationship for typed programmatic mounting.
export interface PluginEntry<C = any> { plugin: Plugin<C>; config?: C; enabled?: boolean }
export interface PluginPatch { name?: string; plugin?: Plugin<any>; config?: unknown; enabled?: boolean }
export type Profile = readonly PluginEntry[];
export type Patch = readonly PluginPatch[];
export type Bundle = Profile;

export function composeProfile(basePlugins: Profile, patches: readonly Patch[] = []): PluginEntry[] {
  const entries = new Map<string, PluginEntry>();
  for (const entry of basePlugins) {
    if (entries.has(entry.plugin.name)) throw new Error(`Duplicate plugin: ${entry.plugin.name}`);
    entries.set(entry.plugin.name, { ...entry });
  }
  for (const patch of patches) for (const change of patch) {
    const name = change.name ?? change.plugin?.name;
    if (!name || (change.plugin && change.plugin.name !== name)) throw new Error('Patch plugin name mismatch or missing name');
    const prior = entries.get(name);
    if (!prior && !change.plugin) throw new Error(`Unknown plugin: ${name}`);
    entries.set(name, {
      plugin: change.plugin ?? prior!.plugin,
      config: Object.hasOwn(change, 'config') ? change.config : prior?.config,
      enabled: change.enabled ?? prior?.enabled ?? true,
    });
  }
  return [...entries.values()];
}

type Listener = (...args: any[]) => any;
type WithoutLast<T extends unknown[]> = T extends [...infer Rest, unknown] ? Rest : never;
type EventResult<K extends keyof Events> = Awaited<ReturnType<Events[K]>>;
interface Registration { listener: Listener }
interface PluginState { entry: PluginEntry; context?: Context; failed?: boolean }
interface ServiceRegistration { value: unknown; owner: Context }

/** A context is one activation's ownership scope, never reused after disposal. */
export class Context {
  readonly services: Services;
  #kernel: Kernel;
  #cleanups: Cleanup[] = [];
  #closed = false;
  constructor(kernel: Kernel) {
    this.#kernel = kernel;
    this.services = new Proxy(Object.create(null) as Services, {
      get: (_target, name) => kernel.getService(name as keyof Services),
      set: () => { throw new Error('Services are read-only; use ctx.provide()'); },
    });
  }
  #assertOpen(): void { if (this.#closed) throw new Error('Context has been disposed'); }
  effect(setup: () => void | Cleanup): void {
    this.#assertOpen();
    const cleanup = setup();
    if (cleanup) this.#cleanups.push(cleanup);
  }
  provide<K extends keyof Services>(name: K, service: Services[K]): Disposable {
    this.#assertOpen();
    const registration = this.#kernel.registerService(name, service, this);
    this.#cleanups.push(() => this.#kernel.withdrawService(name, registration));
    return { dispose: () => this.#closed
      ? this.#kernel.withdrawService(name, registration)
      : this.#kernel.removeService(name, registration) };
  }
  on<K extends keyof Events>(event: K, listener: Events[K], opts?: { prepend?: boolean }): Disposable {
    this.#assertOpen();
    const remove = this.#kernel.addListener(event, listener, opts?.prepend);
    this.#cleanups.push(remove);
    return { dispose: remove };
  }
  emit<K extends keyof Events>(event: K, ...args: Parameters<Events[K]>): void {
    for (const listener of this.#kernel.listeners(event)) listener(...args);
  }
  async parallel<K extends keyof Events>(event: K, ...args: Parameters<Events[K]>): Promise<void> {
    // Promise jobs ensure a synchronous throw does not prevent other listeners from running.
    await Promise.all(this.#kernel.listeners(event).map(listener => Promise.resolve().then(() => listener(...args))));
  }
  async serial<K extends keyof Events>(event: K, ...args: Parameters<Events[K]>): Promise<EventResult<K> | undefined> {
    let result: EventResult<K> | undefined;
    for (const listener of this.#kernel.listeners(event)) {
      const next = await listener(...args);
      if (next !== undefined) result = next;
    }
    return result;
  }
  async bail<K extends keyof Events>(event: K, ...args: Parameters<Events[K]>): Promise<NonNullable<EventResult<K>> | undefined> {
    for (const listener of this.#kernel.listeners(event)) {
      const result = await listener(...args);
      if (result !== undefined && result !== null) return result;
    }
    return undefined;
  }
  async waterfall<K extends keyof Events>(event: K, ...args: WithoutLast<Parameters<Events[K]>>): Promise<EventResult<K>> {
    const listeners = this.#kernel.listeners(event);
    const [value, ...extra] = args;
    const dispatch = async (index: number, current: unknown): Promise<any> => {
      if (index === listeners.length) return current;
      let called = false;
      return listeners[index](current, ...extra, (nextValue: unknown) => {
        if (called) throw new Error('waterfall next() may only be called once');
        called = true;
        return dispatch(index + 1, nextValue);
      });
    };
    return dispatch(0, value);
  }
  /** Internal lifecycle operation: release every effect, even when an earlier cleanup fails. */
  async unwind(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    const errors: unknown[] = [];
    while (this.#cleanups.length) {
      try { await this.#cleanups.pop()!(); } catch (error) { errors.push(error); }
    }
    if (errors.length) throw new AggregateError(errors, 'Plugin effect cleanup failed');
  }
}

export class Kernel {
  readonly ctx: Context;
  #states = new Map<string, PluginState>();
  #services = new Map<keyof Services, ServiceRegistration>();
  #events = new Map<keyof Events, Registration[]>();
  #tail: Promise<void> = Promise.resolve();
  #operating = false;
  #running = false;
  #stopped = false;
  #activationOrder: PluginState[] = [];
  constructor(profile: Profile = []) {
    this.ctx = new Context(this);
    for (const entry of composeProfile(profile)) this.#states.set(entry.plugin.name, { entry });
  }
  #enqueue(operation: () => Promise<void>): Promise<void> {
    const result = this.#tail.catch(() => {}).then(async () => {
      this.#operating = true;
      try { await operation(); } finally { this.#operating = false; }
    });
    this.#tail = result;
    // Automatic service reconciliation is observable via whenIdle, without an unhandled rejection.
    void result.catch(() => {});
    return result;
  }
  /** Await pending lifecycle work, including late service registration. */
  whenIdle(): Promise<void> { return this.#tail; }
  #assertLive(): void { if (this.#stopped) throw new Error('Kernel has stopped'); }
  async start(): Promise<void> {
    return this.#enqueue(async () => { this.#assertLive(); this.#running = true; await this.#reconcile(); });
  }
  async use<C>(plugin: Plugin<C>, config?: C): Promise<void> {
    return this.applyPatch([{ plugin, config }]);
  }
  async applyPatch(patch: Patch): Promise<void> {
    return this.#enqueue(async () => {
      this.#assertLive();
      // Validate the entire patch before removing any live effects.
      const next = composeProfile([...this.#states.values()].map(state => state.entry), [patch]);
      const changed = new Set(patch.map(change => change.name ?? change.plugin!.name));
      const errors: unknown[] = [];
      for (const name of changed) {
        const state = this.#states.get(name);
        if (state) try { await this.#deactivate(state); } catch (error) { errors.push(error); }
      }
      for (const entry of next) {
        if (changed.has(entry.plugin.name)) this.#states.set(entry.plugin.name, { entry });
      }
      this.#running = true;
      if (errors.length) throw new AggregateError(errors, 'Patch cleanup failed');
      await this.#reconcile();
    });
  }
  async stop(): Promise<void> {
    return this.#enqueue(async () => {
      if (this.#stopped) return;
      this.#stopped = true; this.#running = false;
      const errors: unknown[] = [];
      for (const state of [...this.#activationOrder].reverse()) {
        try { await this.#deactivate(state); } catch (error) { errors.push(error); }
      }
      try { await this.ctx.unwind(); } catch (error) { errors.push(error); }
      if (errors.length) throw new AggregateError(errors, 'Kernel cleanup failed');
    });
  }
  getService<K extends keyof Services>(name: K): Services[K] {
    const service = this.#services.get(name);
    if (!service) throw new Error(`Service unavailable: ${String(name)}`);
    return service.value as Services[K];
  }
  registerService<K extends keyof Services>(name: K, value: Services[K], owner: Context): ServiceRegistration {
    this.#assertLive();
    if (this.#services.has(name)) throw new Error(`Service already provided: ${String(name)}`);
    const registration = { value, owner };
    this.#services.set(name, registration);
    if (!this.#operating) void this.#enqueue(() => this.#reconcile());
    return registration;
  }
  /** Remove dependents while their dependencies still exist, then withdraw the service. */
  async withdrawService(name: keyof Services, registration: ServiceRegistration): Promise<void> {
    if (this.#services.get(name) !== registration) return;
    const errors: unknown[] = [];
    for (const state of [...this.#activationOrder].reverse()) {
      if (state.context !== registration.owner && state.entry.plugin.inject?.includes(name)) {
        try { await this.#deactivate(state); } catch (error) { errors.push(error); }
      }
    }
    this.#services.delete(name);
    if (errors.length) throw new AggregateError(errors, `Service ${String(name)} cleanup failed`);
  }
  async removeService(name: keyof Services, registration: ServiceRegistration): Promise<void> {
    return this.#enqueue(async () => { await this.withdrawService(name, registration); await this.#reconcile(); });
  }
  async #deactivate(state: PluginState): Promise<void> {
    const context = state.context;
    if (!context) return;
    state.context = undefined;
    this.#activationOrder = this.#activationOrder.filter(item => item !== state);
    const errors: unknown[] = [];
    // Stop all transitive consumers before *any* provider effect (including its apply cleanup).
    for (const [name, service] of this.#services) if (service.owner === context) {
      for (const dependent of [...this.#activationOrder].reverse()) if (dependent.entry.plugin.inject?.includes(name)) {
        try { await this.#deactivate(dependent); } catch (error) { errors.push(error); }
      }
    }
    try { await context.unwind(); } catch (error) { errors.push(error); }
    if (errors.length) throw new AggregateError(errors, `Plugin ${state.entry.plugin.name} cleanup failed`);
  }
  async #reconcile(): Promise<void> {
    if (!this.#running) return;
    let activated: boolean;
    do {
      activated = false;
      for (const state of this.#states.values()) {
        if (state.context || state.failed || state.entry.enabled === false) continue;
        if (!state.entry.plugin.inject?.every(name => this.#services.has(name)) && state.entry.plugin.inject?.length) continue;
        const context = new Context(this);
        state.context = context; this.#activationOrder.push(state);
        try { context.effect(() => state.entry.plugin.apply(context, state.entry.config)); }
        catch (error) {
          state.failed = true;
          try { await this.#deactivate(state); }
          catch (cleanupError) { throw new AggregateError([error, cleanupError], 'Plugin activation and cleanup failed'); }
          throw error;
        }
        activated = true;
      }
    } while (activated);
  }
  addListener<K extends keyof Events>(event: K, listener: Events[K], prepend = false): () => void {
    const entries = this.#events.get(event) ?? [];
    const registration = { listener };
    if (prepend) entries.unshift(registration); else entries.push(registration);
    this.#events.set(event, entries);
    return () => {
      const index = entries.indexOf(registration);
      if (index !== -1) entries.splice(index, 1);
      if (!entries.length && this.#events.get(event) === entries) this.#events.delete(event);
    };
  }
  listeners(event: keyof Events): Listener[] { return this.#events.get(event)?.map(entry => entry.listener) ?? []; }
}
