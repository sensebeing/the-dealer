import { chmodSync, closeSync, fchmodSync, mkdirSync, openSync, renameSync } from 'node:fs';
import { dirname } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import type { SQLInputValue, SQLOutputValue, StatementSync } from 'node:sqlite';
import { syncDirectory } from '../fs-atomic/index.ts';
import type { MailboxItem, MailboxItemStatus, MailboxStateRecord, ThreadRouteRecord, WakeTargetState } from './store.ts';

const SCHEMA = `
  CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
  CREATE TABLE mailboxes(
    mailbox_id TEXT PRIMARY KEY, outstanding_json TEXT, acked_lease_ids_json TEXT NOT NULL
  );
  CREATE TABLE items(
    sequence INTEGER PRIMARY KEY, id TEXT NOT NULL, mailbox_id TEXT NOT NULL,
    status TEXT NOT NULL, lease_id TEXT, received_at TEXT NOT NULL, leased_at TEXT,
    acked_at TEXT, wakeup_hint TEXT, envelope_json TEXT NOT NULL, signed_envelope_json TEXT,
    UNIQUE(mailbox_id, id)
  );
  CREATE INDEX idx_items_mailbox_status_seq ON items(mailbox_id, status, sequence);
  CREATE TABLE wake_targets(
    mailbox_id TEXT PRIMARY KEY, driver TEXT NOT NULL, driver_config_json TEXT,
    wakeup_prefix TEXT, coalesce_ms INTEGER NOT NULL, rotate_after_waves INTEGER,
    completed_waves INTEGER NOT NULL, pending_wake INTEGER NOT NULL,
    earliest_wake_at INTEGER NOT NULL, attempt_count INTEGER NOT NULL, last_wake_error TEXT,
    delivery_in_doubt INTEGER NOT NULL, binding_revision INTEGER NOT NULL, pending_delivery_json TEXT
  );
  CREATE INDEX idx_wake_targets_pending ON wake_targets(pending_wake, delivery_in_doubt, earliest_wake_at);
  CREATE TABLE threads(
    record_key TEXT PRIMARY KEY, thread_id TEXT NOT NULL, message_id TEXT NOT NULL,
    direction TEXT NOT NULL, local_mailbox TEXT NOT NULL, kind TEXT NOT NULL,
    state TEXT NOT NULL, updated_at TEXT NOT NULL, record_json TEXT NOT NULL
  );
  CREATE INDEX idx_threads_thread_updated ON threads(thread_id, updated_at);
  CREATE INDEX idx_threads_mailbox_state ON threads(local_mailbox, state);
`;

type Row = Record<string, SQLOutputValue>;

interface RowValidation {
  item(value: unknown): MailboxItem;
  mailbox(key: string, value: unknown): MailboxStateRecord;
  wakeTarget(value: unknown): WakeTargetState;
  thread(value: unknown): ThreadRouteRecord;
}

interface ItemsQuery {
  mailboxId?: string;
  status?: MailboxItemStatus;
  unacked?: boolean;
  afterSequence?: number;
  throughSequence?: number;
}

interface MigrationArchive {
  source: string;
  backup: string;
}

export type MailboxInitialization = 'uninitialized' | 'archive-pending' | 'ready';

export interface QueueRow {
  mailboxId: string;
  queuedCount: number;
  leasedCount: number;
  lease?: MailboxStateRecord['outstanding'];
  wakeTarget?: WakeTargetState;
}

export interface ThreadRetentionRow {
  key: string;
  threadId: string;
  networkId: string;
  messageId: string;
  direction: ThreadRouteRecord['direction'];
  state: ThreadRouteRecord['state'];
  updatedAt: string;
  kind: string;
  hasEnvelope: boolean;
  semanticType?: string;
  replyToId?: string;
  localMailbox: string;
  replyMailbox?: string;
  mailboxCommitted: boolean;
}

// Preserve the difference between an omitted legacy field and an explicitly stored zero/false.
const ABSENT = -1;

function invalidStore(): Error {
  return new Error('Invalid mailbox store');
}

function decodeJson(value: SQLOutputValue): unknown {
  if (typeof value !== 'string') throw invalidStore();
  try {
    return JSON.parse(value);
  } catch {
    throw invalidStore();
  }
}

function encodeOptional(value: unknown): string | null {
  return value === undefined ? null : JSON.stringify(value);
}

function chmodIfPresent(path: string, mode: number): boolean {
  try {
    chmodSync(path, mode);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    return false;
  }
}

/** Internal SQLite row access. MailboxStore owns validation and all domain decisions. */
export class MailboxSqliteBackend {
  private readonly db: DatabaseSync;
  private readonly statements = new Map<string, StatementSync>();
  private readonly validation: RowValidation;

  constructor(path: string, validation: RowValidation, readOnly = false) {
    this.validation = validation;
    if (!readOnly) {
      mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
      const fd = openSync(path, 'a', 0o600);
      try {
        fchmodSync(fd, 0o600);
      } finally {
        closeSync(fd);
      }
    }

    this.db = new DatabaseSync(path, { readOnly });
    try {
      this.db.exec('PRAGMA busy_timeout = 5000');
      if (!readOnly) {
        this.db.exec('PRAGMA synchronous = FULL');
        this.enableWal();
        // SQLite creates new sidecars with the database's mode. Fix any preexisting sidecars.
        chmodIfPresent(`${path}-wal`, 0o600);
        chmodIfPresent(`${path}-shm`, 0o600);
        if (this.hasSchema() && !this.hasPendingDeliveryColumn()) {
          this.write(() => {
            if (!this.hasPendingDeliveryColumn()) this.db.exec('ALTER TABLE wake_targets ADD COLUMN pending_delivery_json TEXT');
          });
        }
      }
    } catch (error) {
      this.close();
      throw error;
    }
  }

  private hasPendingDeliveryColumn(): boolean {
    return this.db.prepare("SELECT 1 FROM pragma_table_info('wake_targets') WHERE name = 'pending_delivery_json'").get() !== undefined;
  }

  private enableWal(): void {
    // The initial journal-mode change may return BUSY without invoking the busy handler.
    // Reissue only that startup transition while a concurrent opener is doing the same work.
    const retry = new Int32Array(new SharedArrayBuffer(4));
    const deadline = Date.now() + 5_000;
    for (;;) {
      try {
        if (this.db.prepare('PRAGMA journal_mode = WAL').get()?.journal_mode !== 'wal') {
          throw new Error('Mailbox store requires SQLite WAL mode');
        }
        return;
      } catch (error) {
        if ((error as { errcode?: number }).errcode !== 5 || Date.now() >= deadline) throw error;
        Atomics.wait(retry, 0, 0, 10);
      }
    }
  }

  initializationState(schema: string): MailboxInitialization {
    return this.read(() => {
      if (!this.hasSchema()) return 'uninitialized';
      if (this.meta('schema_version') !== schema) throw invalidStore();
      this.sequence();
      return this.meta('pending_archive') === undefined ? 'ready' : 'archive-pending';
    });
  }

  initialize(schema: string, importLegacy?: () => MigrationArchive | undefined): void {
    if (this.hasSchema()) return;
    this.write(() => {
      if (this.hasSchema()) return;
      this.db.exec(SCHEMA);
      const archive = importLegacy?.();
      if (!archive) this.setSequence(0);
      this.statement('INSERT INTO meta VALUES (?, ?)').run('schema_version', schema);
      if (archive) this.statement('INSERT INTO meta VALUES (?, ?)').run('pending_archive', JSON.stringify(archive));
    });
  }

  // The store holds the legacy lock from the import through this durable archive.
  finishArchive(): void {
    this.write(() => {
      const pending = this.meta('pending_archive');
      if (pending === undefined) return;
      const { source, backup } = decodeJson(pending) as MigrationArchive;
      try {
        renameSync(source, backup);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      }
      if (chmodIfPresent(backup, 0o400)) syncDirectory(dirname(backup));
      this.statement("DELETE FROM meta WHERE key = 'pending_archive'").run();
    });
  }

  close(): void {
    if (this.db.isOpen) this.db.close();
    this.statements.clear();
  }

  private hasSchema(): boolean {
    return this.statement("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'meta'").get() !== undefined;
  }

  private statement(sql: string): StatementSync {
    let statement = this.statements.get(sql);
    if (!statement) {
      statement = this.db.prepare(sql);
      this.statements.set(sql, statement);
    }
    return statement;
  }

  read<T>(operation: () => T): T {
    return this.transaction('BEGIN', operation);
  }

  write<T>(operation: () => T): T {
    return this.transaction('BEGIN IMMEDIATE', operation);
  }

  private transaction<T>(begin: string, operation: () => T): T {
    this.db.exec(begin);
    try {
      const value = operation();
      this.db.exec('COMMIT');
      return value;
    } catch (error) {
      try {
        this.db.exec('ROLLBACK');
      } catch (rollbackError) {
        throw new AggregateError([error, rollbackError], 'Mailbox SQLite transaction and rollback failed');
      }
      throw error;
    }
  }

  private meta(key: string): string | undefined {
    return this.statement('SELECT value FROM meta WHERE key = ?').get(key)?.value as string | undefined;
  }

  sequence(): number {
    const raw = this.meta('sequence');
    const sequence = Number(raw);
    if (!raw || !Number.isSafeInteger(sequence) || sequence < 0) throw invalidStore();
    return sequence;
  }

  setSequence(sequence: number): void {
    this.statement("INSERT INTO meta VALUES ('sequence', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
      .run(String(sequence));
  }

  mailbox(id: string): MailboxStateRecord | undefined {
    const row = this.statement('SELECT * FROM mailboxes WHERE mailbox_id = ?').get(id);
    return row && this.decodeMailbox(row);
  }

  leaseMailbox(leaseId: string): MailboxStateRecord | undefined {
    const row = this.statement(`SELECT * FROM mailboxes WHERE json_extract(outstanding_json, '$.leaseId') = ?
      OR EXISTS (SELECT 1 FROM json_each(acked_lease_ids_json) WHERE value = ?) ORDER BY rowid LIMIT 1`)
      .get(leaseId, leaseId);
    return row && this.decodeMailbox(row);
  }

  writeMailbox(record: MailboxStateRecord): void {
    this.statement(`INSERT INTO mailboxes VALUES (?, ?, ?) ON CONFLICT(mailbox_id) DO UPDATE SET
      outstanding_json = excluded.outstanding_json, acked_lease_ids_json = excluded.acked_lease_ids_json`)
      .run(record.mailboxId, encodeOptional(record.outstanding), JSON.stringify(record.ackedLeaseIds));
  }

  private decodeMailbox(row: Row): MailboxStateRecord {
    const id = row.mailbox_id as string;
    return this.validation.mailbox(id, {
      mailboxId: id,
      ...(row.outstanding_json === null ? {} : { outstanding: decodeJson(row.outstanding_json) }),
      ackedLeaseIds: decodeJson(row.acked_lease_ids_json),
    });
  }

  item(mailboxId: string, id: string): MailboxItem | undefined {
    const row = this.statement('SELECT * FROM items WHERE mailbox_id = ? AND id = ?').get(mailboxId, id);
    return row && this.decodeItem(row);
  }

  items(query: ItemsQuery = {}): MailboxItem[] {
    const predicates: string[] = [];
    const parameters: SQLInputValue[] = [];
    if (query.mailboxId !== undefined) {
      predicates.push('mailbox_id = ?');
      parameters.push(query.mailboxId);
    }
    if (query.status !== undefined) {
      predicates.push('status = ?');
      parameters.push(query.status);
    }
    if (query.unacked) predicates.push("status <> 'acked'");
    if (query.afterSequence !== undefined) {
      predicates.push('sequence > ?');
      parameters.push(query.afterSequence);
    }
    if (query.throughSequence !== undefined) {
      predicates.push('sequence <= ?');
      parameters.push(query.throughSequence);
    }
    const where = predicates.length ? 'WHERE ' + predicates.join(' AND ') : '';
    return this.statement(`SELECT * FROM items ${where} ORDER BY sequence`).all(...parameters)
      .map(row => this.decodeItem(row));
  }

  countItems(mailboxId?: string): { queued: number; leased: number; acked: number } {
    const counts = { queued: 0, leased: 0, acked: 0 };
    const rows = mailboxId === undefined
      ? this.statement('SELECT status, count(*) AS count FROM items GROUP BY status').all()
      : this.statement('SELECT status, count(*) AS count FROM items WHERE mailbox_id = ? GROUP BY status').all(mailboxId);
    for (const { status, count } of rows) {
      if (typeof status !== 'string' || !Object.hasOwn(counts, status)) throw invalidStore();
      counts[status as MailboxItemStatus] = count as number;
    }
    return counts;
  }

  hasQueuedItems(mailboxId: string, afterSequence = -1): boolean {
    return this.statement("SELECT 1 FROM items WHERE mailbox_id = ? AND status = 'queued' AND sequence > ? LIMIT 1")
      .get(mailboxId, afterSequence) !== undefined;
  }

  hasUnackedItems(mailboxId: string): boolean {
    return this.statement("SELECT 1 FROM items WHERE mailbox_id = ? AND status IN ('queued', 'leased') LIMIT 1")
      .get(mailboxId) !== undefined;
  }

  insertItem(item: MailboxItem): void {
    this.statement('INSERT INTO items VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').run(
      item.sequence, item.id, item.mailboxId, item.status, item.leaseId ?? null, item.receivedAt,
      item.leasedAt ?? null, item.ackedAt ?? null, item.wakeupHint ?? null,
      JSON.stringify(item.envelope), encodeOptional(item.signedEnvelope),
    );
  }

  leaseItems(mailboxId: string, leaseId: string, leasedAt: string): void {
    this.statement("UPDATE items SET status = 'leased', lease_id = ?, leased_at = ? WHERE mailbox_id = ? AND status = 'queued'")
      .run(leaseId, leasedAt, mailboxId);
  }

  ackItem(mailboxId: string, id: string, ackedAt: string): void {
    this.statement("UPDATE items SET status = 'acked', acked_at = ? WHERE mailbox_id = ? AND id = ? AND status = 'leased'")
      .run(ackedAt, mailboxId, id);
  }

  trimAckedItems(throughSequence: number, limit: number): void {
    this.statement(`DELETE FROM items WHERE sequence IN (
      SELECT sequence FROM items WHERE status = 'acked' AND sequence <= ?
      ORDER BY sequence DESC LIMIT -1 OFFSET ?
    )`).run(throughSequence, limit);
  }

  hasAckedHistoryBeyond(limit: number): boolean {
    return this.statement("SELECT 1 FROM items WHERE status = 'acked' LIMIT 1 OFFSET ?").get(limit) !== undefined;
  }

  private decodeItem(row: Row): MailboxItem {
    return this.validation.item({
      sequence: row.sequence,
      id: row.id,
      mailboxId: row.mailbox_id,
      status: row.status,
      receivedAt: row.received_at,
      ...(row.lease_id === null ? {} : { leaseId: row.lease_id }),
      ...(row.leased_at === null ? {} : { leasedAt: row.leased_at }),
      ...(row.acked_at === null ? {} : { ackedAt: row.acked_at }),
      ...(row.wakeup_hint === null ? {} : { wakeupHint: row.wakeup_hint }),
      envelope: decodeJson(row.envelope_json),
      ...(row.signed_envelope_json === null ? {} : { signedEnvelope: decodeJson(row.signed_envelope_json) }),
    });
  }

  wakeTarget(id: string): WakeTargetState | undefined {
    const row = this.statement('SELECT * FROM wake_targets WHERE mailbox_id = ?').get(id);
    return row && this.decodeWakeTarget(row);
  }

  wakeTargets(now?: number): WakeTargetState[] {
    const rows = now === undefined
      ? this.statement('SELECT * FROM wake_targets ORDER BY rowid').all()
      : this.statement(`SELECT * FROM wake_targets WHERE pending_wake = 1
          AND delivery_in_doubt IN (?, 0) AND earliest_wake_at <= ?`).all(ABSENT, now);
    return rows.map(row => this.decodeWakeTarget(row));
  }

  writeWakeTarget(target: WakeTargetState, previous?: WakeTargetState): void {
    const values = [
      target.driver, encodeOptional(target.driverConfig), target.wakeupPrefix ?? null,
      target.coalesceMs, target.rotateAfterWaves ?? null, target.completedWaves, Number(target.pendingWake),
      target.earliestWakeAt, target.attemptCount, target.lastWakeError ?? null,
      target.deliveryInDoubt === undefined ? ABSENT : Number(target.deliveryInDoubt), target.bindingRevision ?? ABSENT,
      encodeOptional(target.pendingDelivery),
    ];
    if (previous === undefined) {
      this.statement(`INSERT INTO wake_targets (
        mailbox_id, driver, driver_config_json, wakeup_prefix, coalesce_ms, rotate_after_waves, completed_waves,
        pending_wake, earliest_wake_at, attempt_count, last_wake_error, delivery_in_doubt, binding_revision, pending_delivery_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(target.mailboxId, ...values);
      return;
    }
    const { changes } = this.statement(`UPDATE wake_targets SET driver = ?, driver_config_json = ?,
      wakeup_prefix = ?, coalesce_ms = ?, rotate_after_waves = ?, completed_waves = ?, pending_wake = ?,
      earliest_wake_at = ?, attempt_count = ?, last_wake_error = ?, delivery_in_doubt = ?, binding_revision = ?, pending_delivery_json = ?
      WHERE mailbox_id = ? AND binding_revision = ?`)
      .run(...values, target.mailboxId, previous.bindingRevision ?? ABSENT);
    if (changes !== 1) throw new Error(`Mailbox wake target changed inside the write transaction: ${target.mailboxId}`);
  }

  private decodeWakeTarget(row: Row): WakeTargetState {
    if ((row.pending_wake !== 0 && row.pending_wake !== 1) ||
        (row.delivery_in_doubt !== ABSENT && row.delivery_in_doubt !== 0 && row.delivery_in_doubt !== 1)) {
      throw invalidStore();
    }
    return this.validation.wakeTarget({
      mailboxId: row.mailbox_id,
      driver: row.driver,
      ...(row.driver_config_json === null ? {} : { driverConfig: decodeJson(row.driver_config_json) }),
      ...(row.wakeup_prefix === null ? {} : { wakeupPrefix: row.wakeup_prefix }),
      coalesceMs: row.coalesce_ms,
      ...(row.rotate_after_waves === null ? {} : { rotateAfterWaves: row.rotate_after_waves }),
      completedWaves: row.completed_waves,
      pendingWake: row.pending_wake === 1,
      earliestWakeAt: row.earliest_wake_at,
      attemptCount: row.attempt_count,
      ...(row.last_wake_error === null ? {} : { lastWakeError: row.last_wake_error }),
      ...(row.delivery_in_doubt === ABSENT ? {} : { deliveryInDoubt: row.delivery_in_doubt === 1 }),
      ...(row.binding_revision === ABSENT ? {} : { bindingRevision: row.binding_revision }),
      ...(row.pending_delivery_json == null ? {} : { pendingDelivery: decodeJson(row.pending_delivery_json) }),
    });
  }

  thread(key: string): ThreadRouteRecord | undefined {
    const row = this.statement('SELECT * FROM threads WHERE record_key = ?').get(key);
    return row && this.decodeThread(row);
  }

  threads(query: { threadId?: string; localMailbox?: string; unsettledOutbound?: boolean } = {}): ThreadRouteRecord[] {
    if (query.threadId !== undefined) {
      return this.statement('SELECT * FROM threads WHERE thread_id = ? ORDER BY rowid')
        .all(query.threadId).map(row => this.decodeThread(row));
    }
    if (query.localMailbox !== undefined) {
      return this.statement('SELECT * FROM threads WHERE local_mailbox = ? ORDER BY rowid')
        .all(query.localMailbox).map(row => this.decodeThread(row));
    }
    const where = query.unsettledOutbound ? "WHERE direction = 'outbound' AND state <> 'settled'" : '';
    return this.statement(`SELECT * FROM threads ${where} ORDER BY rowid`).all().map(row => this.decodeThread(row));
  }

  writeThread(key: string, record: ThreadRouteRecord): void {
    this.statement(`INSERT INTO threads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(record_key) DO UPDATE SET thread_id = excluded.thread_id, message_id = excluded.message_id,
      direction = excluded.direction, local_mailbox = excluded.local_mailbox, kind = excluded.kind,
      state = excluded.state, updated_at = excluded.updated_at, record_json = excluded.record_json`).run(
      key, record.threadId, record.messageId, record.direction, record.localMailbox,
      record.kind, record.state, record.updatedAt, JSON.stringify(record),
    );
  }

  deleteThread(key: string): void {
    this.statement('DELETE FROM threads WHERE record_key = ?').run(key);
  }

  private decodeThread(row: Row): ThreadRouteRecord {
    const record = this.validation.thread(decodeJson(row.record_json));
    if (row.record_key !== `${record.direction}:${record.messageId}` || row.thread_id !== record.threadId ||
        row.message_id !== record.messageId || row.direction !== record.direction || row.local_mailbox !== record.localMailbox ||
        row.kind !== record.kind || row.state !== record.state || row.updated_at !== record.updatedAt) {
      throw invalidStore();
    }
    return record;
  }

  threadRetentionRows(minimum: number): ThreadRetentionRow[] {
    if (!this.statement('SELECT 1 FROM threads LIMIT 1 OFFSET ?').get(minimum)) return [];
    // Read routing metadata only. Envelope bodies and attachments never enter the retention scan.
    return this.statement(`SELECT record_key, thread_id, message_id, direction, local_mailbox, kind, state, updated_at,
        json_extract(record_json, '$.networkId') AS network_id,
        json_type(record_json, '$.envelope') IS NOT NULL AS has_envelope,
        json_extract(record_json, '$.envelope.semantic.type') AS semantic_type,
        json_extract(record_json, '$.envelope.replyToId') AS reply_to_id,
        json_extract(record_json, '$.envelope.semantic.replyMailbox') AS reply_mailbox,
        json_extract(record_json, '$.mailboxCommitted') AS mailbox_committed
      FROM threads`).all().map(row => ({
      key: row.record_key, threadId: row.thread_id, networkId: row.network_id, messageId: row.message_id,
      direction: row.direction, state: row.state, updatedAt: row.updated_at, kind: row.kind,
      hasEnvelope: row.has_envelope === 1, semanticType: row.semantic_type ?? undefined,
      replyToId: row.reply_to_id ?? undefined, localMailbox: row.local_mailbox,
      replyMailbox: row.reply_mailbox ?? undefined, mailboxCommitted: row.mailbox_committed === 1,
    } as ThreadRetentionRow));
  }

  queueRows(mailboxId?: string): QueueRow[] {
    const where = mailboxId === undefined ? '' : 'WHERE mailbox_id = ?';
    const parameters = mailboxId === undefined ? [] : [mailboxId];
    const queues = new Map<string, QueueRow>();
    const queueFor = (id: string): QueueRow => {
      let queue = queues.get(id);
      if (!queue) {
        queue = { mailboxId: id, queuedCount: 0, leasedCount: 0 };
        queues.set(id, queue);
      }
      return queue;
    };

    const counts = this.statement(`SELECT mailbox_id, status, count(*) AS count FROM items
      ${where} GROUP BY mailbox_id, status`).all(...parameters);
    for (const { mailbox_id: id, status, count } of counts) {
      const queue = queueFor(id as string);
      if (status === 'queued') queue.queuedCount = count as number;
      else if (status === 'leased') queue.leasedCount = count as number;
      else if (status !== 'acked') throw invalidStore();
    }
    for (const row of this.statement(`SELECT * FROM mailboxes ${where}`).all(...parameters)) {
      const mailbox = this.decodeMailbox(row);
      queueFor(mailbox.mailboxId).lease = mailbox.outstanding;
    }
    for (const row of this.statement(`SELECT * FROM wake_targets ${where}`).all(...parameters)) {
      const target = this.decodeWakeTarget(row);
      queueFor(target.mailboxId).wakeTarget = target;
    }
    return [...queues.keys()].sort().map(id => queues.get(id)!);
  }
}
