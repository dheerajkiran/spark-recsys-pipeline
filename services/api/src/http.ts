import { createServer, type Server } from 'node:http';
import type { Application } from './application.js';

export function createHttpServer(application: Application): Server {
  return createServer((request, response) => {
    const chunks: Buffer[] = [];
    let total = 0;
    request.on('data', (chunk: Buffer) => {
      total += chunk.length;
      if (total <= 262145) chunks.push(chunk);
    });
    request.on('end', () => {
      const url = new URL(request.url ?? '/', 'http://localhost');
      const result = application({
        method: request.method ?? 'GET', path: url.pathname, query: url.search.slice(1),
        body: total > 262144 ? new Uint8Array(total) : Buffer.concat(chunks),
        headers: Object.fromEntries(Object.entries(request.headers).map(([key, value]) => [key, Array.isArray(value) ? value.join(',') : value ?? ''])),
      });
      response.writeHead(result.status, result.headers);
      response.end(result.body);
    });
  });
}
