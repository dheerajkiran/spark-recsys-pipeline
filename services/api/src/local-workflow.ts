import type { Job } from './local-storage.js';
import { LocalStorage } from './local-storage.js';

export class LocalWorkflowRunner {
  private running = false;
  constructor(private readonly storage: LocalStorage) {}
  async run(work: () => Promise<void>): Promise<Job> {
    if (this.running) throw new Error('A local job is already running');
    this.running = true;
    let job: Job | undefined;
    try {
      job = await this.storage.createJob();
      await this.storage.transitionJob(job.id, 'queued', 'running');
      await work();
      return await this.storage.transitionJob(job.id, 'running', 'succeeded');
    } catch (error) {
      if (job) await this.storage.transitionJob(job.id, 'running', 'failed', error instanceof Error ? error.message : 'Job failed');
      throw error;
    } finally { this.running = false; }
  }
}
