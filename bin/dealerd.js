#!/usr/bin/env -S node --experimental-strip-types
import { runDealerDaemon } from '../packages/cli/index.ts';
try {
  const runtime = await runDealerDaemon(process.argv.slice(2));
  process.stdout.write(JSON.stringify(runtime.status()) + '\n');
  let stopping = false;
  const stop = async () => { if (stopping) return; stopping = true; await runtime.stop(); };
  process.once('SIGTERM', stop);
  process.once('SIGINT', stop);
} catch (error) { console.error(String(error)); process.exitCode = 1; }
