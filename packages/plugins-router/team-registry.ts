import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { parseYaml } from './yaml.ts';
import type { Plugin } from '../kernel/index.ts';

export interface TeamRegistry {
  team: { name: string; git_names?: string[]; modules?: string[]; [key: string]: any }[];
  areas: Record<string, { owner: string; default_recipients?: string[]; docs_root?: string }>;
  review_policies: Record<string, { reviewer: string; gate: string }>;
  modules: Record<string, { services: string[] }>;
}

export function parseTeamRegistry(yamlOrObject: string | Record<string, any>): TeamRegistry {
  const parsed = typeof yamlOrObject === 'string' ? parseYaml(yamlOrObject) : yamlOrObject;
  const record = (value: unknown): value is Record<string, any> =>
    value !== null && typeof value === 'object' && !Array.isArray(value);
  const names = (value: unknown): value is string[] =>
    Array.isArray(value) && value.every(name => typeof name === 'string' && name.trim().length > 0);
  if (!record(parsed) || !Array.isArray(parsed.team) || !record(parsed.modules) ||
      parsed.team.some(member => !record(member) || typeof member.name !== 'string' || !member.name.trim() ||
        (member.modules !== undefined && !names(member.modules)) ||
        (member.git_names !== undefined && !names(member.git_names))) ||
      new Set(parsed.team.map(member => member.name)).size !== parsed.team.length ||
      Object.values(parsed.modules).some(module => !record(module) || !names(module.services)) ||
      !record(parsed.areas ?? {}) || !record(parsed.review_policies ?? {})) {
    throw new Error('Invalid team registry');
  }
  for (const area of Object.values(parsed.areas ?? {})) {
    if (!record(area) || typeof area.owner !== 'string' || !area.owner.trim() ||
        (area.default_recipients !== undefined && !names(area.default_recipients))) throw new Error('Invalid team area');
  }
  for (const review of Object.values(parsed.review_policies ?? {})) {
    if (!record(review) || typeof review.reviewer !== 'string' || !review.reviewer.trim()) throw new Error('Invalid review policy');
  }
  return { ...parsed, areas: parsed.areas ?? {}, review_policies: parsed.review_policies ?? {} } as TeamRegistry;
}

export function resolveTeamRegistryMembers(registry: TeamRegistry, selectorOrHint: string): string[] {
  const colon = selectorOrHint.indexOf(':');
  const type = colon < 0 ? 'member' : selectorOrHint.slice(0, colon);
  const key = colon < 0 ? selectorOrHint : selectorOrHint.slice(colon + 1);
  let names: string[] = [];
  if (type === 'area') {
    const area = registry.areas[key];
    if (area) names = [area.owner, ...(area.default_recipients ?? [])];
    else names = registry.team.filter(m => m.modules?.includes(key)).map(m => m.name);
  } else if (type === 'module') names = registry.team.filter(m => m.modules?.includes(key)).map(m => m.name);
  else if (type === 'service') {
    const modules = Object.entries(registry.modules).filter(([, module]) => module.services.includes(key)).map(([name]) => name);
    names = registry.team.filter(m => m.modules?.some(module => modules.includes(module))).map(m => m.name);
  } else if (type === 'review') {
    const reviewer = registry.review_policies[key]?.reviewer;
    if (reviewer) names = [reviewer];
  } else if (type === 'git') names = registry.team.filter(m => m.git_names?.includes(key)).map(m => m.name);
  else if (type === 'member') names = registry.team.filter(m => m.name === key).map(m => m.name);
  for (const name of names) {
    if (!registry.team.some(member => member.name === name)) throw new Error(`Unknown registry member: ${name}`);
  }
  return [...new Set(names)];
}

export function createTeamRegistryRouterPlugin(config: {
  registry?: TeamRegistry;
  registryPath?: string;
  projectRoot?: string;
  networkId?: string;
  name?: string;
  priority?: number;
} = {}): Plugin {
  const name = config.name ?? 'team-registry-router';
  return {
    name,
    inject: ['router'],
    apply(ctx) {
      const registry = parseTeamRegistry(config.registry ?? readFileSync(resolve(config.projectRoot ?? process.cwd(), config.registryPath ?? 'build_docs/_team.yaml'), 'utf8'));
      ctx.on('emitter/prepare', async (envelope, next) => {
        if (config.networkId !== undefined && envelope.networkId !== config.networkId) return next(envelope);
        const targets = (envelope.routing.targets ?? []).flatMap(target => {
          if (!/^(area|module|service|review|git):/.test(target)) return [target];
          const members = resolveTeamRegistryMembers(registry, target);
          if (!members.length) throw new Error(`Unknown team selector: ${target}`);
          return members.map(member => `member:${member}`);
        });
        return next({ ...envelope, routing: { ...envelope.routing, targets: [...new Set(targets)] } });
      });
      const judge = ctx.services.router.registerJudge({
        name,
        priority: config.priority ?? 50,
        async decide(envelope, candidates) {
          if (config.networkId !== undefined && envelope.networkId !== config.networkId) return null;
          const selectors = ['area', 'module', 'service'].flatMap(field => typeof envelope.payload?.[field] === 'string' ? [`${field}:${envelope.payload[field]}`] : []);
          if (envelope.routing.hint) selectors.push(envelope.routing.hint);
          const members = [...new Set(selectors.flatMap(selector => resolveTeamRegistryMembers(registry, selector)))];
          const eligible = candidates.filter(card => card.networkId === envelope.networkId && card.admissionMode !== 'closed' && card.runtimeState.load !== 'overloaded');
          const matched = members.flatMap(member => {
            const nodes = eligible.filter(card => card.identity.member === member);
            const idle = nodes.filter(card => card.runtimeState.load === 'idle');
            return idle.length ? idle : nodes;
          });
          if (!matched.length) return null;
          return {
            mode: matched.length === 1 ? 'unicast' : 'multicast',
            targetNodeIds: matched.map(card => card.nodeId),
            matchedBy: name,
            reason: 'Canonical team registry ownership',
          };
        },
      });
      ctx.effect(() => () => judge.dispose());
    },
  };
}
