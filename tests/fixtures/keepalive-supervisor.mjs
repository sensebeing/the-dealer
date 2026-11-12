// A launchd-style KeepAlive supervisor for the daemon lifecycle tests.
//
// It stays warm (its module graph is already loaded) and claims the state-directory lock
// the instant the supervised daemon releases it. That keeps the replacement live inside the
// CLI's 100ms shutdown probe window, which is the race a real KeepAlive supervisor creates;
// a cold `dealer node up` would leave a visible absence window and never exercise it.
//
// The optional third argument delays `serveControl()` after the lock is claimed, reproducing a
// real cold start whose plugin loading and network mounting take seconds before `control.sock`
// appears.
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [repoRoot, stateDir, serveDelayMsRaw] = process.argv.slice(2);
const serveDelayMs = Number(serveDelayMsRaw ?? 0);
const ownerPath = join(stateDir, 'runtime.lock', 'owner.json');
const { DealerNodeRuntime, runDealerCli } = await import(pathToFileURL(join(repoRoot, 'packages/cli/index.ts')).href);

process.stdout.write('armed\n');
// Spin synchronously on the lock owner record: any timer delay would let the CLI probe see
// an absent daemon instead of the supervisor-started replacement.
while (existsSync(ownerPath)) {
  // The supervised daemon still owns the state directory.
}

const runtime = new DealerNodeRuntime({ stateDir });
await runtime.start();
const owner = JSON.parse(readFileSync(ownerPath, 'utf8'));
process.stdout.write(`locked ${owner.pid} ${owner.instanceId}\n`);
if (serveDelayMs > 0) {
  await new Promise(resolve => setTimeout(resolve, serveDelayMs));
}
await runtime.serveControl(async (args, context) => {
  const stdout = [];
  const stderr = [];
  const code = await runDealerCli(args, {
    runtime,
    stdout: line => stdout.push(line),
    stderr: line => stderr.push(line),
    signal: context.signal,
    expectedInstance: context.expectedInstance,
  });
  return { code, stdout, stderr };
});
process.stdout.write(`respawned ${owner.pid} ${owner.instanceId}\n`);
const stop = () => {
  void runtime.stop().then(() => process.exit(0), () => process.exit(1));
};
process.once('SIGTERM', stop);
process.once('SIGINT', stop);
