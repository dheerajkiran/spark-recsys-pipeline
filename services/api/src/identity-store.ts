import { randomUUID } from 'node:crypto';
import { chmod, mkdir, open, readFile, rename, unlink } from 'node:fs/promises';
import { isAbsolute, join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { DependencyUnavailable } from './failure.js';

export interface StoreKey { pk: string; sk: string }
export interface StoreRecord extends StoreKey { revision: number; data: Record<string, unknown> }
export interface StoreWrite { key: StoreKey; expectedRevision: number | null; record: StoreRecord | null }
export interface IdentityStore {
  get(key: StoreKey): Promise<StoreRecord | null>;
  query(pk: string, skPrefix: string): Promise<StoreRecord[]>;
  transact(writes: StoreWrite[]): Promise<void>;
}

export class StoreConflict extends Error {
  constructor() { super('Identity state changed; retry the operation'); this.name = 'StoreConflict'; }
}

// Only filesystem promises pass through here; validation and snapshot decoding stay outside it.
async function io<T>(operation: Promise<T>): Promise<T> {
  try { return await operation; }
  catch (error) {
    if (error instanceof Error && 'code' in error && typeof error.code === 'string' && /^E[A-Z]+$/.test(error.code)) {
      throw new DependencyUnavailable('Identity storage is unavailable', { cause: error });
    }
    throw error;
  }
}

export function validateKey(key: StoreKey): void {
  if (!key || typeof key.pk !== 'string' || !key.pk || Buffer.byteLength(key.pk) > 2048
    || typeof key.sk !== 'string' || !key.sk || Buffer.byteLength(key.sk) > 1024) {
    throw new Error('Invalid identity storage key');
  }
}

export function validateRecord(value: unknown): asserts value is StoreRecord {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid identity record');
  const record = value as StoreRecord;
  validateKey(record);
  if (!Number.isSafeInteger(record.revision) || record.revision < 1
    || !record.data || typeof record.data !== 'object' || Array.isArray(record.data)) {
    throw new Error('Invalid identity record');
  }
}

export function validateWrites(writes: StoreWrite[]): void {
  if (!Array.isArray(writes) || !writes.length || writes.length > 100) throw new Error('Invalid transaction size');
  const keys = new Set<string>();
  for (const write of writes) {
    validateKey(write.key);
    const key = JSON.stringify([write.key.pk, write.key.sk]);
    if (keys.has(key)) throw new Error('Duplicate identity transaction key');
    keys.add(key);
    if (write.expectedRevision !== null && (!Number.isSafeInteger(write.expectedRevision) || write.expectedRevision < 1)) {
      throw new Error('Invalid expected identity revision');
    }
    if (write.record) {
      validateRecord(write.record);
      if (write.record.pk !== write.key.pk || write.record.sk !== write.key.sk
        || write.record.revision !== (write.expectedRevision ?? 0) + 1) {
        throw new Error('Invalid identity revision or transaction key');
      }
    }
  }
}

export function validateQuery(pk: string, skPrefix: string): void {
  validateKey({ pk, sk: skPrefix || '_' });
  if (typeof skPrefix !== 'string') throw new Error('Invalid identity query prefix');
}

/** Local durable identity state. A crashed writer's lock requires explicit operator recovery. */
export class LocalIdentityStore implements IdentityStore {
  private readonly directory: string;
  private readonly snapshot: string;
  private readonly lockPath: string;
  constructor(root: string, private readonly lockTimeoutMs = 5000) {
    if (!isAbsolute(root)) throw new Error('Identity root must be absolute');
    this.directory = join(resolve(root), 'identity');
    this.snapshot = join(this.directory, 'records.json');
    this.lockPath = join(this.directory, 'write.lock');
  }

  private async read(): Promise<StoreRecord[]> {
    let raw: string;
    try { raw = await readFile(this.snapshot, 'utf8'); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return [];
      return io(Promise.reject(error));
    }
    const value: unknown = JSON.parse(raw);
    if (!value || typeof value !== 'object' || !('format' in value) || value.format !== 1
      || !('records' in value) || !Array.isArray(value.records)) throw new Error('Invalid identity snapshot');
    const keys = new Set<string>();
    for (const record of value.records) {
      validateRecord(record);
      const key = JSON.stringify([record.pk, record.sk]);
      if (keys.has(key)) throw new Error('Duplicate identity snapshot record');
      keys.add(key);
    }
    return value.records as StoreRecord[];
  }

  async get(key: StoreKey): Promise<StoreRecord | null> {
    validateKey(key);
    return (await this.read()).find((record) => record.pk === key.pk && record.sk === key.sk) ?? null;
  }

  async query(pk: string, skPrefix: string): Promise<StoreRecord[]> {
    validateQuery(pk, skPrefix);
    return (await this.read()).filter((record) => record.pk === pk && record.sk.startsWith(skPrefix))
      .sort((left, right) => Buffer.compare(Buffer.from(left.sk), Buffer.from(right.sk)));
  }

  async transact(writes: StoreWrite[]): Promise<void> {
    validateWrites(writes);
    // Clone before waiting for the lock so a caller cannot mutate an in-flight transaction.
    const transaction = JSON.parse(JSON.stringify(writes)) as StoreWrite[];
    validateWrites(transaction);
    await io(mkdir(this.directory, { recursive: true, mode: 0o700 }));
    await io(chmod(this.directory, 0o700));
    const deadline = Date.now() + this.lockTimeoutMs;
    let lock;
    while (!lock) {
      try { lock = await open(this.lockPath, 'wx', 0o600); }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'EEXIST') return io(Promise.reject(error));
        if (Date.now() >= deadline) throw new DependencyUnavailable('Identity storage is locked; inspect the writer before recovery', { cause: error });
        await delay(10);
      }
    }
    let temp: string | undefined;
    let failed = false;
    try {
      await io(lock.writeFile(JSON.stringify({ pid: process.pid })));
      const records = await this.read();
      for (const write of transaction) {
        const current = records.find((record) => record.pk === write.key.pk && record.sk === write.key.sk);
        if ((current?.revision ?? null) !== write.expectedRevision) throw new StoreConflict();
      }
      const result = records.filter((record) => !transaction.some((write) => write.key.pk === record.pk && write.key.sk === record.sk));
      for (const write of transaction) if (write.record) result.push(write.record);
      temp = join(this.directory, `records.${randomUUID()}.tmp`);
      const file = await io(open(temp, 'wx', 0o600));
      try { await io(file.writeFile(JSON.stringify({ format: 1, records: result }))); await io(file.sync()); }
      finally { await io(file.close()); }
      await io(rename(temp, this.snapshot));
      temp = undefined;
    } catch (error) {
      failed = true;
      throw error;
    } finally {
      if (temp) await unlink(temp).catch(() => undefined);
      let cleanupError: unknown;
      try { await io(lock.close()); } catch (error) { cleanupError = error; }
      try { await io(unlink(this.lockPath)); } catch (error) { cleanupError ??= error; }
      // A release failure must not replace a conflict, corruption, or failed write's original cause.
      if (cleanupError && !failed) throw cleanupError;
    }
  }
}
