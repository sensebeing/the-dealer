import type { MessageEnvelope, NodeCard, RouteDecision, RouteJudgePlugin } from '../core-types/index.ts';
import type { Plugin } from '../kernel/index.ts';

export function validateDecision(decision: RouteDecision, candidates: readonly NodeCard[]): RouteDecision {
  const ids = [...new Set(decision.targetNodeIds)];
  if (!['broadcast', 'unicast', 'multicast'].includes(decision.mode)) {
    throw new Error('Invalid route mode');
  }
  if (
    decision.mode !== 'broadcast' &&
    (!ids.length || ids.some(id => !candidates.some(c => c.nodeId === id)))
  ) {
    throw new Error('Route contains no targets or unknown targets');
  }
  if (decision.mode === 'unicast' && ids.length !== 1) {
    throw new Error('Unicast requires one target');
  }
  return { ...decision, targetNodeIds: decision.mode === 'broadcast' ? [] : ids };
}

export function createRouterPlugin(): Plugin {
  return {
    name: 'router',
    inject: ['node'],
    apply(ctx) {
      const judges = new Set<RouteJudgePlugin>();
      const decision = (cards: readonly NodeCard[], matchedBy: string): RouteDecision => {
        const ids = [...new Set(cards.map(c => c.nodeId))];
        return {
          mode: ids.length === 1 ? 'unicast' : 'multicast',
          targetNodeIds: ids,
          matchedBy,
          reason: matchedBy,
        };
      };
      ctx.provide('router', {
        registerJudge(judge) {
          judges.add(judge);
          return {
            dispose() {
              judges.delete(judge);
            },
          };
        },
        async route(envelope: MessageEnvelope) {
          const { node } = ctx.services;
          if (envelope.routing.mode === 'broadcast') {
            return {
              mode: 'broadcast',
              targetNodeIds: [],
              matchedBy: 'explicit',
              reason: 'Explicit broadcast',
            };
          }
          const selectors = envelope.routing.targets ?? [];
          const explicit = selectors.filter(t => !t.startsWith('skill:'));
          if (explicit.length) {
            const cards = explicit.flatMap(t => {
              const resolved = node
                .resolveTarget(t, envelope.networkId)
                .filter(c => c.networkId === envelope.networkId);
              if (!resolved.length) {
                throw new Error(`Unresolved explicit target: ${t}`);
              }
              return resolved;
            });
            return decision(cards, 'explicit');
          }
          const candidates = node
            .listPeers({ networkId: envelope.networkId, onlyAvailable: true })
            .filter(
              c =>
                c.networkId === envelope.networkId &&
                c.admissionMode !== 'closed' &&
                c.runtimeState.load !== 'overloaded',
            );
          const terms = [
            ...selectors.filter(t => t.startsWith('skill:')).map(t => t.slice(6)),
            envelope.payload?.requiredSkill,
            envelope.payload?.area,
          ].filter((v): v is string => typeof v === 'string' && v.length > 0);
          const matches = candidates.filter(c =>
            c.capabilities.skills.some(
              s =>
                terms.some(
                  t => s.id === t || s.tags?.includes(t) || s.acceptedTypes?.includes(t),
                ) || s.acceptedTypes?.includes(envelope.kind),
            ),
          );
          if (matches.length) {
            const idle = matches.filter(c => c.runtimeState.load === 'idle');
            return decision(idle.length ? idle : matches, 'capability');
          }
          for (const judge of [...judges].sort((a, b) => b.priority - a.priority)) {
            const result = await judge.decide(envelope, candidates);
            if (result) {
              return validateDecision(result, candidates);
            }
          }
          throw new Error('No route matched envelope');
        },
      });
    },
  };
}
