import { createOrcaWakeupDrivers } from '../../packages/plugins-orca/index.ts';
import type { OrcaWakeupDriverOptions } from '../../packages/plugins-orca/index.ts';
import { createAgentHostWakeupDriver } from '../../packages/plugins-agent-host/index.ts';

/** Integration fixtures use the same explicit driver composition as the runtime. */
export function createTestWakeupDrivers(options: OrcaWakeupDriverOptions = {}) {
  return [...createOrcaWakeupDrivers(options), createAgentHostWakeupDriver(options)];
}
