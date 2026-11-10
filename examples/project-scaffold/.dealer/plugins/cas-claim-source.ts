import { join } from 'node:path';

// The API must atomically claim by version and return the same claim on retries.
// Cursor advancement is owned by core; failed/empty deliveries retain the page.
export default function createPlugin(config: any, context: any) {
  return {
    name: 'example-cas-claim-source',
    inject: ['emitter'],
    apply(ctx: any) {
      if (!config.enabled) return;
      const source = context.api.createPollWatermarkSourcePlugin({
        initialCursor: 0,
        cursorPath: join(context.stateDir, 'example-claims.cursor.json'),
        settlementMode: 'settled',
        wake: { port: 18965 },
        intervalMs: 60_000,
        async poll(cursor: number) {
          const url = new URL(config.claimEndpoint);
          url.searchParams.set('cursor', String(cursor));
          const response = await fetch(url, { signal: AbortSignal.timeout(10_000) });
          if (!response.ok) throw new Error(`Claim page failed: ${response.status}`);
          return response.json(); // { items: [{ id, version, area, summary }], cursor }
        },
        toInput: (claim: any) => ({
          networkId: config.networkId ?? 'local',
          kind: 'claim.example.v1',
          routing: { mode: 'auto' },
          idempotencyKey: `claim:${claim.id}`,
          subject: `Claim ${claim.id}`,
          body: claim.summary,
          payload: { executor: 'orca-spawn', projectId: 'example-project', area: claim.area,
            claimId: claim.id, claimVersion: claim.version },
        }),
        onError: (error: unknown) => console.error('Claim source:', error),
      });
      ctx.effect(() => {
        const registration = ctx.services.emitter.registerSource(source);
        return () => registration.dispose();
      });
    },
  };
}
