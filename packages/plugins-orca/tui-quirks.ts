import { singleLine } from '../subprocess/index.ts';
export { trustAntigravityWorkspace, defaultAgentLaunchCommand } from '../plugins-agent-host/hosts.ts';

function findOrcaComposerPrompt(lines: string[]): { index: number; firstLine: string } | undefined {
  for (let i = lines.length - 1; i >= 0; i--) {
    const match = /^\s*>\s*(.*)$/.exec(lines[i]);
    if (match) return { index: i, firstLine: match[1].trim() };
  }
  return undefined;
}

export function hasOrcaComposerPrompt(preview: string): boolean {
  return findOrcaComposerPrompt(preview.split(/\r?\n/)) !== undefined;
}

/** A human draft or unconfirmed Pilot recommendation owns the composer until submitted. */
export function isOrcaComposerEmpty(preview: string, agentIdentity?: unknown): boolean {
  const lines = preview.split(/\r?\n/);
  const prompt = findOrcaComposerPrompt(lines);
  if (!prompt) return true;
  for (const line of lines.slice(prompt.index + 1)) {
    if (/^\s*[─━═\-]{5,}\s*$/.test(line)) break;
    if (line.trim()) return false;
  }
  return prompt.firstLine === '' || isAntigravityModePlaceholder('> ' + prompt.firstLine, agentIdentity);
}

/**
 * Antigravity (agy >= 1.5.9) draws these exact execution-mode hints inside an empty `> `
 * composer. Orca's plain-text preview strips the dim styling, and Antigravity may truncate the
 * hint with an ellipsis in narrow background tabs (around 58 columns). Only this vocabulary
 * identifies placeholders. A changed hint or an unknown agent keeps the composer busy.
 */
const ANTIGRAVITY_MODE_PLACEHOLDERS: ReadonlySet<string> = new Set([
  '> Auto mode: edits & safe commands auto-approved (shift+tab to cycle)',
  '> Plan mode: research & plan only (shift+tab to cycle)',
  '> Accept-edits mode: file edits auto-approved (shift+tab to cycle)',
  '> Best-of-N mode: fork into parallel arms and pick a winner (shift+tab to cycle)',
]);

/** Recognize complete or ellipsis-truncated official hints on an Antigravity composer. */
export function isAntigravityModePlaceholder(previewLine: string, agentIdentity?: unknown): boolean {
  if (agentIdentity !== 'antigravity') return false;
  const normalized = singleLine(previewLine);
  if (ANTIGRAVITY_MODE_PLACEHOLDERS.has(normalized)) return true;

  const prefix = normalized.replace(/(?:\.\.\.|…)$/, '');
  if (prefix === normalized) return false;
  const modeSeparator = ' mode: ';
  const modeIndex = prefix.indexOf(modeSeparator);
  if (modeIndex === -1 || prefix.trimEnd().length <= modeIndex + modeSeparator.length) return false;

  for (const placeholder of ANTIGRAVITY_MODE_PLACEHOLDERS) {
    if (prefix.length < placeholder.length && placeholder.startsWith(prefix)) return true;
  }
  return false;
}
