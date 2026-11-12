#!/usr/bin/env -S node --no-warnings --experimental-strip-types
import './register-ts.js';
const { runDealerDaemon } = await import('../packages/cli/index.ts');

try {
  const runtime = await runDealerDaemon(process.argv.slice(2));
  process.stdout.write(`${JSON.stringify(runtime.status())}\n`);
  let stopping = false;
  const stop = async () => {
    if (stopping) {
      return;
    }
    stopping = true;
    await runtime.stop();
    // Residue from external plugins must not keep a stopping daemon alive.
    process.exit(0);
  };
  process.once('SIGTERM', stop);
  process.once('SIGINT', stop);
  // Only an explicit `dealer node down --force` escalates to SIGTERM. The default
  // shutdown path is `node down`'s graceful drain, never a signal from this process.
} catch (error) {
  console.error(String(error));
  process.exitCode = 1;
}
