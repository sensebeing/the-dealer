import { singleLine } from '../subprocess/index.ts';
export { trustAntigravityWorkspace, defaultAgentLaunchCommand } from '../plugins-agent-host/hosts.ts';

/** Recognize an Autopilot recommendation copied into the terminal composer. */
export function isAutopilotComposerPrefill(preview: string): boolean {
  const lines = preview.split(/\r?\n/);
  return lines.some((line, index) => {
    const draft = /^\s*>\s+(\S.*)$/.exec(line)?.[1]?.trim();
    return (
      draft !== undefined &&
      lines
        .slice(0, index)
        .some(
          (earlier) => /下一步建议|,1（主推）|,1 \(主推\)|自动预填/.test(earlier) || earlier.includes(draft),
        )
    );
  });
}

/**
 * Antigravity (agy >= 1.5.9) draws these exact execution-mode hints inside an empty `> `
 * composer. Orca's plain-text preview strips the dim styling, so only this vocabulary can mark a
 * `> ` line as a placeholder: everything else remains indistinguishable from an unsent human
 * draft. A changed hint or an unknown agent fails closed, keeping the composer busy.
 */
const ANTIGRAVITY_MODE_PLACEHOLDERS: ReadonlySet<string> = new Set([
  '> Auto mode: edits & safe commands auto-approved (shift+tab to cycle)',
  '> Plan mode: research & plan only (shift+tab to cycle)',
  '> Accept-edits mode: file edits auto-approved (shift+tab to cycle)',
  '> Best-of-N mode: fork into parallel arms and pick a winner (shift+tab to cycle)',
]);

/** True only for an exact Antigravity mode hint on an Antigravity-owned composer line. */
export function isAntigravityModePlaceholder(previewLine: string, agentIdentity?: unknown): boolean {
  return agentIdentity === 'antigravity' && ANTIGRAVITY_MODE_PLACEHOLDERS.has(singleLine(previewLine));
}
