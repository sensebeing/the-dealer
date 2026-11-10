import { readFileSync, realpathSync } from 'node:fs';
import { isAbsolute, relative, resolve } from 'node:path';

export default function createPlugin(_config: any, context: any) {
  return {
    name: 'example-collab-request',
    inject: ['transport'],
    apply(ctx: any) {
      ctx.on('emitter/prepare', async (envelope: any, next: any) => {
        if (envelope.kind !== 'collab.request.v4' || !envelope.payload.requestFile) return next(envelope);
        const root = realpathSync(context.projectRoot);
        const path = realpathSync(resolve(root, envelope.payload.requestFile));
        const within = relative(root, path);
        if (within === '..' || within.startsWith('../') || isAbsolute(within)) throw new Error('Request must be inside the project');
        const { frontmatter: request, body } = context.api.parseMarkdownFrontmatter(readFileSync(path, 'utf8'));
        if (request.schema !== 'collaboration-request/v4' || typeof request.request_id !== 'string' ||
            !Array.isArray(request.to) || !request.to.length || request.to.some((name: unknown) => typeof name !== 'string' || !name.trim())) {
          throw new Error('Invalid collaboration request');
        }
        const attachment = await ctx.services.transport.shareFile(path);
        return next({ ...envelope, body,
          attachments: [...envelope.attachments, attachment],
          payload: { ...envelope.payload, requestId: request.request_id, area: request.area },
          routing: { mode: 'auto', targets: request.to.map((name: string) => `member:${name}`) },
        });
      });
    },
  };
}
