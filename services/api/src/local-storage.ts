import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { join, resolve, sep } from 'node:path';

export type JobState = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';
export interface Job { id: string; state: JobState; updatedAt: string; error?: string }
const allowed: Record<JobState, JobState[]> = {
  queued: ['running', 'cancelled'], running: ['succeeded', 'failed', 'cancelled'],
  succeeded: [], failed: [], cancelled: [],
};

function safeKey(key: string): string {
  if (!/^[a-zA-Z0-9_-]{1,128}$/.test(key)) throw new Error('Invalid storage key');
  return key;
}

export class LocalStorage {
  constructor(readonly root: string) {
    if (!resolve(root).startsWith(sep) || root !== resolve(root)) throw new Error('Storage root must be absolute');
  }
  async init(): Promise<void> {
    await mkdir(join(this.root, 'jobs'), { recursive: true });
    await mkdir(join(this.root, 'artifacts'), { recursive: true });
  }
  private jobPath(id: string): string { return join(this.root, 'jobs', `${safeKey(id)}.json`); }
  async createJob(): Promise<Job> {
    await this.init();
    const job: Job = { id: `job_${randomUUID().replaceAll('-', '')}`, state: 'queued', updatedAt: new Date().toISOString() };
    await writeFile(this.jobPath(job.id), JSON.stringify(job), { flag: 'wx' });
    return job;
  }
  async getJob(id: string): Promise<Job | null> {
    try { return JSON.parse(await readFile(this.jobPath(id), 'utf8')) as Job; }
    catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null; throw error; }
  }
  async transitionJob(id: string, expected: JobState, next: JobState, error?: string): Promise<Job> {
    const job = await this.getJob(id);
    if (!job) throw new Error('Job not found');
    if (job.state !== expected || !allowed[expected].includes(next)) throw new Error('Invalid job transition');
    const updated: Job = { id, state: next, updatedAt: new Date().toISOString(), ...(error ? { error } : {}) };
    const path = this.jobPath(id);
    const temp = `${path}.${randomUUID()}.tmp`;
    await writeFile(temp, JSON.stringify(updated), { flag: 'wx' });
    await rename(temp, path);
    return updated;
  }
  async putArtifact(bytes: Uint8Array): Promise<{ sha256: string; path: string }> {
    await this.init();
    const sha256 = createHash('sha256').update(bytes).digest('hex');
    const path = join(this.root, 'artifacts', sha256);
    try { await writeFile(path, bytes, { flag: 'wx' }); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error; }
    return { sha256, path };
  }
  async getArtifact(sha256: string): Promise<Buffer> {
    if (!/^[a-f0-9]{64}$/.test(sha256)) throw new Error('Invalid artifact digest');
    const bytes = await readFile(join(this.root, 'artifacts', sha256));
    if (createHash('sha256').update(bytes).digest('hex') !== sha256) throw new Error('Artifact digest mismatch');
    return bytes;
  }
}
