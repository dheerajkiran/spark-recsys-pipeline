/** Dependency-free local static server with byte ranges for seekable video. */
import http from 'node:http';
import { createReadStream } from 'node:fs';
import { realpath, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createDeepFMClient } from './deepfm-client.mjs';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json', '.svg': 'image/svg+xml', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.webp': 'image/webp', '.mp4': 'video/mp4', '.webm': 'video/webm', '.woff2': 'font/woff2', '.md': 'text/plain; charset=utf-8' };

export function createStaticServer(directory = ROOT, options = {}) {
  const ownsClient = !options.deepfmClient;
  const deepfm = options.deepfmClient || createDeepFMClient();
  const server = http.createServer(async (req, res) => {
    const fail = (status, message) => { res.writeHead(status, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end(message); };
    try {
      const url = new URL(req.url, 'http://localhost');
      const pathname = decodeURIComponent(url.pathname);
      if (pathname.startsWith('/api/')) {
        const json = (status, value) => { res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' }); res.end(JSON.stringify(value)); };
        const error = (status, code, message) => json(status, { error: { code, message } });
        if (req.method !== 'GET') { res.setHeader('Allow', 'GET'); return error(405, 'method_not_allowed', 'Model preview endpoints accept GET only.'); }
        const methods = { '/api/deepfm/status': 'status', '/api/deepfm/profiles': 'profiles', '/api/deepfm/score': 'score' };
        const method = methods[pathname];
        if (!method) return error(404, 'not_found', 'Unknown preview endpoint.');
        const profile = url.searchParams.get('profile_id');
        const validQuery = method === 'score'
          ? url.searchParams.getAll('profile_id').length === 1 && [...url.searchParams.keys()].every(key => key === 'profile_id') && /^\d{1,20}$/.test(profile || '')
          : url.searchParams.size === 0;
        if (!validQuery) return error(400, 'invalid_request', 'Provide one numeric profile_id for scoring; other parameters are not supported.');
        try {
          const result = await deepfm.request(method, method === 'score' ? { profile_id: profile } : {});
          return json(200, method === 'profiles' ? { profiles: result } : result);
        } catch (problem) {
          if (problem.code === 'unknown_profile') return error(404, 'unknown_profile', 'Choose a profile from the available preview profiles.');
          if (problem.code === 'invalid_request') return error(400, 'invalid_request', 'Invalid model preview request.');
          return error(503, 'model_unavailable', 'DeepFM preview is unavailable. Check the local Python environment, selected checkpoint and training data.');
        }
      }
      if (!['GET', 'HEAD'].includes(req.method)) { res.setHeader('Allow', 'GET, HEAD'); return fail(405, 'Method not allowed'); }
      if (pathname.split('/').some(segment => segment.startsWith('.') && segment !== '')) return fail(403, 'Forbidden');
      const root = await realpath(directory);
      const filename = await realpath(path.resolve(root, `.${pathname === '/' ? '/index.html' : pathname}`));
      if (!filename.startsWith(root + path.sep)) return fail(403, 'Forbidden');
      const info = await stat(filename);
      if (!info.isFile()) return fail(404, 'Not found');
      res.setHeader('Content-Type', TYPES[path.extname(filename)] || 'application/octet-stream');
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Accept-Ranges', 'bytes');
      let start = 0, end = info.size - 1;
      if (req.headers.range) {
        const match = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range);
        if (!match || (!match[1] && !match[2])) { res.setHeader('Content-Range', `bytes */${info.size}`); return fail(416, 'Invalid byte range'); }
        start = match[1] ? Number(match[1]) : Math.max(0, info.size - Number(match[2]));
        end = match[1] && match[2] ? Math.min(Number(match[2]), info.size - 1) : info.size - 1;
        if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || start > end || start >= info.size) { res.setHeader('Content-Range', `bytes */${info.size}`); return fail(416, 'Invalid byte range'); }
        res.statusCode = 206;
        res.setHeader('Content-Range', `bytes ${start}-${end}/${info.size}`);
      }
      res.setHeader('Content-Length', Math.max(0, end - start + 1));
      if (req.method === 'HEAD' || info.size === 0) return res.end();
      const stream = createReadStream(filename, { start, end });
      stream.on('error', () => res.destroy());
      res.on('close', () => stream.destroy());
      stream.pipe(res);
    } catch (error) {
      if (error instanceof URIError) return fail(400, 'Invalid URL');
      if (error.code === 'ENOENT' || error.code === 'ENOTDIR') return fail(404, 'Not found');
      fail(500, 'Unable to serve file');
    }
  });
  if (ownsClient) server.once('close', () => deepfm.close());
  return server;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const port = Number(process.env.PORT || 4173);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('PORT must be an integer from 1 to 65535');
  const server = createStaticServer();
  server.on('error', error => { console.error(`Unable to start Loop: ${error.message}`); process.exitCode = 1; });
  server.listen(port, '127.0.0.1', () => console.log(`Loop preview: http://localhost:${port}`));
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => server.close());
}
