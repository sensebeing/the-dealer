import { closeSync, fstatSync, openSync, readFileSync, statSync } from 'node:fs';
import type { Stats } from 'node:fs';

type Fingerprint = Pick<Stats, 'dev' | 'ino' | 'size' | 'mtimeMs'>;

/** A validated read-only snapshot of an atomically replaced ledger file. */
export class LedgerSnapshot<T> {
  private cached?: Fingerprint & { state: T };

  read(path: string, load: (raw: string) => T, empty: () => T): T {
    try {
      const { dev, ino, size, mtimeMs } = statSync(path);
      const cached = this.cached;
      if (cached && cached.dev === dev && cached.ino === ino && cached.size === size && cached.mtimeMs === mtimeMs) {
        return cached.state;
      }

      const fd = openSync(path, 'r');
      try {
        // Pair the fingerprint with the file we actually opened if another writer renamed
        // the ledger between stat and open.
        const { dev, ino, size, mtimeMs } = fstatSync(fd);
        const state = load(readFileSync(fd, 'utf8'));
        this.cached = { dev, ino, size, mtimeMs, state };
        return state;
      } finally {
        closeSync(fd);
      }
    } catch (error) {
      this.invalidate();
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return empty();
      throw error;
    }
  }

  invalidate(): void {
    this.cached = undefined;
  }
}
