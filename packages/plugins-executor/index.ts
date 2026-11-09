import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { appendFile, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import type { ExecutionContext, ExecutorPlugin, MessageEnvelope } from '../core-types/index.ts';

export interface CommandRequest { command: string; args: readonly string[]; cwd?: string; input?: string; signal?: AbortSignal }
export type CommandRunner = (request: CommandRequest) => Promise<{ stdout: string; stderr?: string; exitCode?: number }>;
export const runCommand: CommandRunner = request => new Promise((resolve, reject) => {
  const child = spawn(request.command, [...request.args], { cwd: request.cwd, signal: request.signal, stdio: ['pipe', 'pipe', 'pipe'], shell: false });
  let stdout = '', stderr = '';
  child.stdout.setEncoding('utf8'); child.stderr.setEncoding('utf8');
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  child.on('error', reject);
  child.on('close', code => code === 0 ? resolve({ stdout, stderr, exitCode: code }) : reject(new Error(`${request.command} exited ${code}: ${stderr}`)));
  child.stdin.on('error', reject);
  child.stdin.end(request.input);
});
export async function checkedCommand(runner: CommandRunner, request: CommandRequest) {
  const result = await runner(request);
  if (result.exitCode !== undefined && result.exitCode !== 0) throw new Error(`${request.command} exited ${result.exitCode}: ${result.stderr ?? ''}`);
  return result;
}
export function renderWave(batch: readonly MessageEnvelope[]): string {
  return batch.map(e => `Subject: ${e.subject}\nMessage: ${e.id}\n${e.body}\nPayload: ${JSON.stringify(e.payload)}\nAttachments: ${JSON.stringify(e.attachments)}`).join('\n\n---\n\n');
}
export interface AgentOptions { command?: string; args?: readonly string[]; cwd?: string; commandRunner?: CommandRunner }
function agentArgs(command: string): string[] { return command === 'codex' ? ['exec', '-'] : command === 'claude' ? ['-p'] : []; }
export function createAgentPtyExecutorPlugin(options: AgentOptions = {}): ExecutorPlugin {
  return { name: 'agent-pty', async execute(batch, context) {
    const command = options.command ?? 'codex';
    const output = await checkedCommand(options.commandRunner ?? runCommand, { command, args: options.args ?? agentArgs(command), cwd: options.cwd, input: renderWave(batch), signal: context.signal });
    let structuredOutput: unknown;
    try { structuredOutput = JSON.parse(output.stdout); } catch { structuredOutput = output.stdout; }
    return { executor: 'agent-pty', executionId: randomUUID(), output: structuredOutput, stderr: output.stderr ?? '' };
  } };
}
export interface OrcaSpawnOptions extends AgentOptions { namePrefix?: string; orcaCommand?: string }
const shellQuote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
export function createOrcaSpawnExecutorPlugin(options: OrcaSpawnOptions = {}): ExecutorPlugin {
  return { name: 'orca-spawn', async execute(batch, context) {
    const runner = options.commandRunner ?? runCommand;
    const command = options.orcaCommand ?? 'orca';
    const name = `${options.namePrefix ?? 'dealer'}-${randomUUID()}`;
    const created = await checkedCommand(runner, { command, args: ['worktree', 'create', '--no-parent', '--name', name, '--json'], cwd: options.cwd, signal: context.signal });
    const raw = JSON.parse(created.stdout);
    if (raw.ok === false) throw new Error('Orca worktree creation failed');
    const workspace = (raw.result ?? raw) as { path?: string; worktree?: { path?: string } };
    const path = workspace.path ?? workspace.worktree?.path;
    if (!path) throw new Error('Orca worktree create returned no path');
    const agent = options.command ?? 'codex';
    // Orca terminal's --command is a shell command; quote every untrusted argument.
    const agentCommand = [agent, ...(options.args ?? (agent === 'codex' ? ['exec'] : agentArgs(agent))), renderWave(batch)].map(shellQuote).join(' ');
    const launched = await checkedCommand(runner, { command, args: ['terminal', 'create', '--worktree', `path:${path}`, '--command', agentCommand, '--json'], cwd: path, signal: context.signal });
    const terminal = JSON.parse(launched.stdout);
    if (terminal.ok === false) throw new Error('Orca terminal launch failed');
    return { executor: 'orca-spawn', executionId: randomUUID(), worktreePath: path, terminal };
  } };
}
export interface OrcaWaveOptions {
  runId?: string; terminalHandle?: string; cwd?: string; orcaCommand?: string; commandRunner?: CommandRunner;
  idleCheck?: () => Promise<{ idle: boolean; reason?: string }>;
}
export function createOrcaWaveExecutorPlugin(options: OrcaWaveOptions): ExecutorPlugin {
  const runner = options.commandRunner ?? runCommand;
  const command = options.orcaCommand ?? 'orca';
  const checkIdle = async () => {
    if (options.idleCheck) return options.idleCheck();
    if (!options.cwd) return { idle: false, reason: 'cwd or authoritative idleCheck required' };
    const output = await checkedCommand(runner, { command, args: ['worktree', 'ps', '--agent-wait', '--limit', '10000', '--json'], cwd: options.cwd });
    const raw = JSON.parse(output.stdout);
    const inventory = raw.result;
    if (raw.ok !== true || !inventory || inventory.truncated || !Array.isArray(inventory.worktrees)) return { idle: false, reason: 'Incomplete workspace inventory' };
    type Workspace = { path: string; worktreeId: string; childWorktreeIds?: string[]; unverifiableTerminalCount?: number; agents?: { state?: string }[] };
    const rows: Workspace[] = inventory.worktrees;
    const root = rows.find(row => row.path === options.cwd);
    if (!root) return { idle: false, reason: 'Workspace not found' };
    const visited = new Set<string>();
    const idleTree = async (row: Workspace): Promise<boolean> => {
      if (visited.has(row.worktreeId)) return false;
      visited.add(row.worktreeId);
      if (row.unverifiableTerminalCount !== 0 || !row.agents || row.agents.some(agent => agent.state !== 'done')) return false;
      const listed = await checkedCommand(runner, { command, args: ['terminal', 'list', '--worktree', `path:${row.path}`, '--json'], cwd: options.cwd });
      const terminals = JSON.parse(listed.stdout);
      if (terminals.ok !== true || terminals.result?.truncated || !Array.isArray(terminals.result?.terminals)) return false;
      const handles: { handle: string; connected: boolean; writable: boolean }[] = terminals.result.terminals;
      // A chat-only or otherwise unobservable agent needs an injected authoritative idleCheck.
      if (handles.length < row.agents.length) return false;
      for (const terminal of handles) {
        if (!terminal.connected || !terminal.writable) return false;
        try {
          const waited = await checkedCommand(runner, { command, args: ['terminal', 'wait', '--terminal', terminal.handle, '--for', 'tui-idle', '--timeout-ms', '1', '--json'], cwd: options.cwd });
          if (JSON.parse(waited.stdout).ok !== true) return false;
        } catch { return false; }
      }
      for (const id of row.childWorktreeIds ?? []) {
        const child = rows.find(item => item.worktreeId === id);
        if (!child || !(await idleTree(child))) return false;
      }
      return true;
    };
    const idle = await idleTree(root);
    return { idle, reason: idle ? undefined : 'Workspace or descendant is busy or unverifiable' };
  };
  return { name: 'orca-wave', checkIdle, async execute(batch, context) {
    if (!(await checkIdle()).idle) throw new Error('Orca workspace is busy');
    if (!options.runId && !options.terminalHandle) throw new Error('Orca wave requires runId or terminalHandle');
    const args = options.runId
      ? ['orchestration', 'send', '--to', `run:${options.runId}`, '--subject', `Dealer wave (${batch.length})`, '--body', renderWave(batch), '--json']
      : ['terminal', 'send', '--terminal', options.terminalHandle!, '--text', renderWave(batch), '--enter', '--json'];
    const output = await checkedCommand(runner, { command, args, cwd: options.cwd, signal: context.signal });
    const delivery = JSON.parse(output.stdout);
    if (delivery.ok === false) throw new Error('Orca wave delivery failed');
    const comment = await checkedCommand(runner, { command, args: ['worktree', 'set', '--worktree', options.cwd ? `path:${options.cwd}` : 'current', '--comment', `Dealer: dispatched ${batch.length} message(s)`, '--json'], cwd: options.cwd, signal: context.signal });
    if (JSON.parse(comment.stdout).ok === false) throw new Error('Orca comment update failed after delivery');
    return { executor: 'orca-wave', executionId: randomUUID(), delivery };
  } };
}
export interface NotifyOptions {
  inboxPath?: string;
  notify?: (batch: readonly MessageEnvelope[], context: ExecutionContext) => Promise<void>;
  commandRunner?: CommandRunner;
  larkWebhookUrl?: string;
  fetchFn?: typeof fetch;
}
export function createNotifyExecutorPlugin(options: NotifyOptions = {}): ExecutorPlugin {
  return { name: 'notify', async execute(batch, context) {
    if (options.inboxPath) {
      await mkdir(dirname(options.inboxPath), { recursive: true });
      await appendFile(options.inboxPath, batch.map(e => JSON.stringify(e)).join('\n') + '\n', { mode: 0o600 });
    }
    if (options.notify) await options.notify(batch, context);
    else if (options.larkWebhookUrl) {
      const response = await (options.fetchFn ?? fetch)(options.larkWebhookUrl, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ msg_type: 'text', content: { text: renderWave(batch) } }), signal: context.signal });
      if (!response.ok) throw new Error(`Lark notification failed: ${response.status}`);
      const data = await response.json() as { code?: number; StatusCode?: number };
      if ((data.code ?? data.StatusCode ?? 0) !== 0) throw new Error('Lark notification rejected');
    } else if (!options.inboxPath) {
      await checkedCommand(options.commandRunner ?? runCommand, { command: 'osascript', args: ['-e', 'on run argv\ndisplay notification (item 1 of argv) with title (item 2 of argv)\nend run', renderWave(batch), 'The Dealer'], signal: context.signal });
    }
    return { executor: 'notify', executionId: randomUUID(), count: batch.length };
  } };
}
