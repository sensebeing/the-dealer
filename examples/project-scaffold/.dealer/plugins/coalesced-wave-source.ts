import { join } from 'node:path';

export default function createPlugin(config: any, context: any) {
  return {
    name: 'example-coalesced-wave-source',
    inject: ['emitter'],
    apply(ctx: any) {
      if (!config.enabled) return;
      const source = context.api.createPollWatermarkSourcePlugin({
        initialCursor: 0,
        cursorPath: join(context.stateDir, 'example-waves.cursor.json'),
        settlementMode: 'queued-or-settled',
        intervalMs: 60_000,
        async poll(cursor: number) {
          const url = new URL(config.batchEndpoint);
          url.searchParams.set('cursor', String(cursor));
          const response = await fetch(url, { signal: AbortSignal.timeout(10_000) });
          if (!response.ok) throw new Error(`Wave page failed: ${response.status}`);
          return response.json(); // { items: [{ id, revision, streamKey, summary }], cursor }
        },
        toInput: (batch: any) => ({
          networkId: config.networkId ?? 'local',
          kind: 'wave.alert.v1',
          routing: { mode: 'unicast', targets: [`member:${config.member}`] },
          idempotencyKey: `wave:${batch.id}`,
          subject: `Wave ${batch.id}`,
          body: batch.summary,
          payload: { executor: 'orca-wave', streamKey: batch.streamKey, revision: batch.revision },
        }),
        onError: (error: unknown) => console.error('Wave source:', error),
      });
      ctx.effect(() => {
        const registration = ctx.services.emitter.registerSource(source);
        return () => registration.dispose();
      });
    },
  };
}
