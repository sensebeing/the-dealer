import { readFileSync } from 'node:fs';
import { registerHooks, stripTypeScriptTypes } from 'node:module';
import { fileURLToPath } from 'node:url';

const packageSources = new URL('../packages/', import.meta.url).href;

// Node's native loader excludes node_modules; handle only Dealer's own sources.
if (packageSources.includes('/node_modules/')) {
  registerHooks({
    load(url, context, nextLoad) {
      if (url.startsWith(packageSources) && url.endsWith('.ts')) {
        return {
          format: 'module',
          shortCircuit: true,
          source: stripTypeScriptTypes(readFileSync(fileURLToPath(url), 'utf8'), {
            mode: 'strip',
            sourceUrl: url,
          }),
        };
      }
      return nextLoad(url, context);
    },
  });
}
