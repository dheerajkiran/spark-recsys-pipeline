/** Lazy, bounded JSON-lines connection to the local inference-only Python worker. */
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const unavailable = (message = 'The local DeepFM preview is unavailable.', code = 'model_unavailable') => Object.assign(new Error(message), { code });

export function createDeepFMClient({
  pythonPath = path.join(ROOT, '.venv', 'bin', 'python'),
  workerPath = path.join(ROOT, 'serving', 'preview.py'),
  timeoutMs = 20000,
} = {}) {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new TypeError('timeoutMs must be finite and positive');
  let child = null, nextId = 0, closed = false;
  const pending = new Map();
  function rejectPending(error) {
    for (const entry of pending.values()) { clearTimeout(entry.timer); entry.reject(error); }
    pending.clear();
  }
  function fail(worker, error) {
    if (worker !== child) return;
    child = null;
    rejectPending(error);
    worker.stdin.destroy();
    worker.kill();
  }
  function start() {
    if (child) return child;
    const worker = spawn(pythonPath, [workerPath], { cwd: ROOT, stdio: ['pipe', 'pipe', 'pipe'], shell: false });
    child = worker;
    let buffer = '';
    worker.stdout.setEncoding('utf8');
    worker.stdout.on('data', chunk => {
      if (child !== worker) return;
      buffer += chunk;
      if (buffer.length > 1024 * 1024) return fail(worker, unavailable('DeepFM returned an oversized response.'));
      let newline;
      while ((newline = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, newline); buffer = buffer.slice(newline + 1);
        let message;
        try { message = JSON.parse(line); } catch { return fail(worker, unavailable('DeepFM returned an invalid response.')); }
        if (!message || typeof message !== 'object' || typeof message.id !== 'string'
            || (!Object.hasOwn(message, 'result') && !Object.hasOwn(message, 'error'))) {
          return fail(worker, unavailable('DeepFM returned an invalid response.'));
        }
        const entry = pending.get(message.id);
        if (!entry) continue;
        pending.delete(message.id); clearTimeout(entry.timer);
        if (message.error) {
          const code = typeof message.error.code === 'string' ? message.error.code : 'model_unavailable';
          const description = typeof message.error.message === 'string' ? message.error.message.slice(0, 512) : 'DeepFM could not complete this request.';
          entry.reject(unavailable(description, code));
        } else entry.resolve(message.result);
      }
    });
    // Drain diagnostics without exposing local paths or raw feature data over HTTP.
    worker.stderr.on('data', () => {});
    worker.stdin.on('error', () => fail(worker, unavailable('The DeepFM worker connection closed.')));
    worker.once('error', () => fail(worker, unavailable('Cannot start DeepFM. Set up the project Python environment first.')));
    worker.once('close', () => fail(worker, unavailable('The DeepFM worker stopped before completing the request.')));
    return worker;
  }
  return {
    request(method, params = {}) {
      if (closed) return Promise.reject(unavailable('The DeepFM client is closed.', 'client_closed'));
      if (!['status', 'profiles', 'score'].includes(method)
          || (method === 'score' && (typeof params?.profile_id !== 'string' || !/^\d{1,20}$/.test(params.profile_id)))) {
        return Promise.reject(unavailable('Invalid DeepFM preview request.', 'invalid_request'));
      }
      return new Promise((resolve, reject) => {
        const worker = start(), id = String(++nextId);
        const timer = setTimeout(() => fail(worker, unavailable('DeepFM took too long to respond. Try again.')), timeoutMs);
        pending.set(id, { resolve, reject, timer });
        const message = { id, method };
        if (method === 'score') message.profile_id = params.profile_id;
        worker.stdin.write(JSON.stringify(message) + '\n', error => {
          if (error) fail(worker, unavailable('The DeepFM worker connection closed.'));
        });
      });
    },
    close() {
      if (closed) return;
      closed = true;
      const error = unavailable('The DeepFM client is closed.', 'client_closed');
      if (child) fail(child, error); else rejectPending(error);
    },
  };
}
