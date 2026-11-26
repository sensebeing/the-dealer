import { createOrcaWakeupDrivers } from '../../packages/plugins-orca/index.ts';
import type { OrcaWakeupDriverOptions } from '../../packages/plugins-orca/index.ts';
import { createAgentHostWakeupDriver } from '../../packages/plugins-agent-host/index.ts';
import type { StdioProcessSpawner } from '../../packages/subprocess/index.ts';

export interface TestWakeupDriverOptions extends OrcaWakeupDriverOptions {
  stdioSpawner?: StdioProcessSpawner;
  agySettingsPath?: string;
}

/** Integration fixtures use the same explicit driver composition as the runtime. */
export function createTestWakeupDrivers(options: TestWakeupDriverOptions = {}) {
  return [...createOrcaWakeupDrivers(options), createAgentHostWakeupDriver(options)];
}
