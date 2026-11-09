#!/usr/bin/env -S node --experimental-strip-types
import { runDealerCli } from '../packages/cli/index.ts';
process.exitCode = await runDealerCli(process.argv.slice(2));
