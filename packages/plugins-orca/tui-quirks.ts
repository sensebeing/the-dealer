import { singleLine } from '../subprocess/index.ts';
export { trustAntigravityWorkspace, defaultAgentLaunchCommand } from '../plugins-agent-host/hosts.ts';

/** A human draft or unconfirmed Pilot recommendation owns the composer until submitted. */
export function isOrcaComposerEmpty(preview: string, agentIdentity?: unknown): boolean {
  const lines = preview.split(/\r?\n/);
  let promptIndex = -1;
  let firstLine = '';
  for (let i = 0; i < lines.length; i++) {
    const match = /^\s*>\s*(.*)$/.exec(lines[i]);
    if (match) {
      promptIndex = i;
      firstLine = match[1].trim();
    }
  }
  if (promptIndex < 0) return true;
  for (const line of lines.slice(promptIndex + 1)) {
    if (/^\s*[─━═\-]{5,}\s*$/.test(line)) break;
    if (line.trim()) return false;
  }
  return firstLine === '' || isAntigravityModePlaceholder('> ' + firstLine, agentIdentity);
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
