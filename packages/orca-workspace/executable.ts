import { accessSync, constants, statSync } from 'node:fs';
import { homedir } from 'node:os';
import { delimiter, join } from 'node:path';

/** Resolve executable PATH entries before the optional per-user installation directory. */
export function findExecutable(command: string, env: NodeJS.ProcessEnv = process.env, localBin = true): string | undefined {
  const directories = (env.PATH ?? '').split(delimiter).filter(Boolean);
  const fallback = join(homedir(), '.local/bin');
  if (localBin && !directories.includes(fallback)) directories.push(fallback);
  for (const directory of directories) {
    const path = join(directory, command);
    try {
      accessSync(path, constants.X_OK);
      if (statSync(path).isFile()) return path;
    } catch (error) {
      if (!['ENOENT', 'EACCES', 'ENOTDIR'].includes((error as NodeJS.ErrnoException).code ?? '')) throw error;
    }
  }
  return undefined;
}
