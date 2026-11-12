// A launchd-style KeepAlive supervisor for the daemon lifecycle tests.
//
// It stays warm (its module graph is already loaded) and claims the state-directory lock
// the instant the supervised daemon releases it. That keeps the replacement live inside the
// CLI's 100ms shutdown probe window, which is the race a real KeepAlive supervisor creates;
// a cold `dealer node up` would leave a visible absence window and never exercise it.
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [repoRoot, stateDir] = process.argv.slice(2);
const ownerPath = join(stateDir, 'runtime.lock', 'owner.json');
const { runDealerDaemon } = await import(pathToFileURL(join(repoRoot, 'packages/cli/index.ts')).href);

process.stdout.write('armed\n');
// Spin synchronously on the lock owner record: any timer delay would let the CLI probe see
// an absent daemon instead of the supervisor-started replacement.
while (existsSync(ownerPath)) {
  // The supervised daemon still owns the state directory.
}

const runtime = await runDealerDaemon([]);
const owner = JSON.parse(readFileSync(ownerPath, 'utf8'));
process.stdout.write(`respawned ${owner.pid} ${owner.instanceId}\n`);
const stop = () => {
  void runtime.stop().then(() => process.exit(0), () => process.exit(1));
};
process.once('SIGTERM', stop);
process.once('SIGINT', stop);
