#!/usr/bin/env -S node --no-warnings --experimental-strip-types
import './register-ts.js';
const { runDealerCli } = await import('../packages/cli/index.ts');
process.exitCode = await runDealerCli(process.argv.slice(2));
