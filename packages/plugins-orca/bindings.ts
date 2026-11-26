import type { OrcaTopicBinding } from './executors.ts';
import { orcaAgent } from './workspace.ts';
import { pinOrcaTerminalSessionMailbox, resolveConfiguredOrcaWakeTargetPatch, resolveOrcaManagedAutoCreateWorkspace, resolveOrcaWaveRotateAfterWaves } from './wakeup.ts';
import type { MailboxStore, MailboxTopicBinding, WakeTargetState } from '../mailbox/index.ts';
import { normalizeConversationModeInput } from '../core-types/index.ts';

export function validateOrcaBinding(binding: OrcaTopicBinding): void {
  if (binding.agent !== undefined) orcaAgent(binding.agent);
}

export function orcaMailboxBinding(binding: OrcaTopicBinding, explicit?: MailboxTopicBinding['mode']): MailboxTopicBinding {
  const mode = explicit ?? (binding.executor === 'orca-wave' ||
    (binding.executor === 'orca' && (binding.rotateAfterWaves !== undefined || binding.streamKey !== undefined || binding.workspacePath !== undefined || binding.runId !== undefined || binding.terminalHandle !== undefined)) ? 'wave' : 'spawn');
  return { mailboxId: binding.mailboxId, streamKey: binding.streamKey, mode,
    ...(binding.executor === 'orca-wave' || (binding.executor === 'orca' && (mode === 'wave' || mode === 'fixed')) ? { stream: true } : {}) };
}

export function syncOrcaTopicWakeTarget(ctx: {
  binding: OrcaTopicBinding; mailboxId: string; projectRoot?: string; store: MailboxStore;
  wave?: { rotateAfterWaves?: number }; addressing?: MailboxTopicBinding;
}): void {
  const { binding, mailboxId, projectRoot, store } = ctx;
  const effectiveMode = ctx.addressing?.mode ?? orcaMailboxBinding(binding, normalizeConversationModeInput(binding)).mode;
  if (!((binding.executor === 'orca-wave' || (binding.executor === 'orca' && effectiveMode !== 'spawn')) && (binding.workspacePath || binding.runId || binding.terminalHandle))) return;
  const { workspacePath, worktreeId, runId, terminalHandle, autoCreateWorkspace, workspaceComment, wakeupPrefix, coalesceMs, rotateAfterWaves, interactiveCommand } = binding;
  const current = store.getWakeTarget(mailboxId);
  const resolvedRotateAfterWaves = resolveOrcaWaveRotateAfterWaves({
    mode: effectiveMode, bindingRotateAfterWaves: rotateAfterWaves,
    waveRotateAfterWaves: ctx.wave?.rotateAfterWaves, managesWorkspace: Boolean(workspacePath), runId, pinnedTerminalHandle: terminalHandle,
  });
  const resolvedAutoCreateWorkspace = resolveOrcaManagedAutoCreateWorkspace({
    explicitAutoCreate: autoCreateWorkspace, managesWorkspace: Boolean(workspacePath), hasRepoRoot: Boolean(projectRoot),
    runId, pinnedTerminalHandle: terminalHandle,
  });
  const identityPatch = resolveConfiguredOrcaWakeTargetPatch(current, {
    mailboxId, runId, workspacePath, worktreeId, configuredTerminalHandle: terminalHandle,
    configuredAgent: binding.agent ? orcaAgent(binding.agent) : undefined,
    configuredSessionId: binding.sessionId, configuredInteractiveCommand: interactiveCommand,
    configuredResumeCommand: binding.resumeCommand, hasTopicBinding: true, authoritativeConfig: true,
  });
  store.upsertWakeTarget(mailboxId, {
    ...identityPatch,
    wakeupPrefix, coalesceMs: coalesceMs ?? 0,
    rotateAfterWaves: resolvedRotateAfterWaves,
    driverConfig: {
      ...identityPatch.driverConfig,
      workspacePath, runId, repoRoot: projectRoot,
      autoCreateWorkspace: resolvedAutoCreateWorkspace, workspaceComment,
    },
  });
}

export function pinDeliveredOrcaMailbox(store: MailboxStore, mailboxId: string, target: WakeTargetState): string {
  const handle = target.driverConfig?.terminalHandle;
  return !target.pendingWake && typeof handle === 'string'
    ? pinOrcaTerminalSessionMailbox(store, mailboxId, handle) : mailboxId;
}
