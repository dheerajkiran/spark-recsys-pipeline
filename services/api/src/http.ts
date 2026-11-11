import { randomBytes } from 'node:crypto';
import { createServer, type Server } from 'node:http';
import type { ApiRequest, ApiResponse } from './application.js';

const MAX_BODY = 262144;
const SINGLE_HEADERS = new Set(['authorization', 'x-api-key', 'idempotency-key', 'x-demo-session']);

function failure(status: number, code: string, message: string): ApiResponse {
  const requestId = `req_${randomBytes(16).toString('hex')}`;
  return {
    status,
    headers: { 'content-type': 'application/json', 'x-request-id': requestId, 'cache-control': 'no-store' },
    body: JSON.stringify({ code, message, request_id: requestId, details: [] }),
  };
}

export function createHttpServer(application: (request: ApiRequest) => ApiResponse | Promise<ApiResponse>): Server {
  return createServer((request, response) => {
    const send = (result: ApiResponse): void => {
      if (response.destroyed || response.writableEnded) return;
      response.writeHead(result.status, result.headers);
      response.end(result.body);
    };
    const reject = (status: number, code: string, message: string): void => {
      response.setHeader('connection', 'close');
      send(failure(status, code, message));
      request.resume();
    };
    const chunks: Buffer[] = [];
    request.on('error', () => {
      chunks.length = 0;
      if (!response.destroyed && !response.writableEnded) reject(400, 'invalid_request', 'Request body could not be read');
    });
    request.on('aborted', () => { chunks.length = 0; });

    let url: URL;
    try {
      const target = request.url ?? '/';
      // Accept only origin-form targets. URL's normalisation must not turn a
      // supplied authority or malformed escape into an ordinary API route.
      if (!target.startsWith('/') || target.startsWith('//') || /[\\#]/.test(target)) throw new Error('Invalid request target');
      const decodedPath = decodeURIComponent(target.split('?')[0] ?? '');
      if ([...decodedPath].some((character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127)) throw new Error('Invalid request target');
      url = new URL(target, 'http://localhost');
      const seen = new Set<string>();
      for (let index = 0; index < request.rawHeaders.length; index += 2) {
        const name = request.rawHeaders[index]?.toLowerCase() ?? '';
        if (SINGLE_HEADERS.has(name) && seen.has(name)) throw new Error('Ambiguous request headers');
        seen.add(name);
      }
    } catch {
      reject(400, 'invalid_request', 'Malformed request target or headers');
      return;
    }

    let total = 0;
    let oversized = false;
    request.on('data', (chunk: Buffer) => {
      if (oversized) return;
      // The sum never advances beyond the configured limit, and an oversized
      // request does not cause an allocation proportional to its full length.
      if (chunk.length > MAX_BODY - total) {
        oversized = true;
        chunks.length = 0;
        reject(413, 'payload_too_large', 'Request body exceeds 256 KiB');
        return;
      }
      total += chunk.length;
      chunks.push(chunk);
    });
    request.on('end', () => {
      if (oversized || request.aborted || response.writableEnded) return;
      void (async () => {
        try {
          const result = await application({
            method: request.method ?? 'GET', path: url.pathname, query: url.search.slice(1),
            body: Buffer.concat(chunks, total),
            headers: Object.fromEntries(Object.entries(request.headers).map(([key, value]) => [key, Array.isArray(value) ? value.join(',') : value ?? ''])),
          });
          send(result);
        } catch {
          send(failure(500, 'internal_error', 'The request could not be completed'));
        }
      })();
    });
  });
}
