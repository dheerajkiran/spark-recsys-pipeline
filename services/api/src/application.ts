import { randomBytes } from 'node:crypto';
import type { Config } from './config.js';

export interface ApiRequest {
  method: string;
  path: string;
  query: string;
  body: Uint8Array;
  headers: Record<string, string>;
}
export interface ApiResponse {
  status: number;
  headers: Record<string, string>;
  body: string;
}
type ErrorCode = 'invalid_request' | 'unauthenticated' | 'not_found' | 'payload_too_large';
const MAX_BODY = 262144;

export function createApplication(_config: Config) {
  return function handle(request: ApiRequest): ApiResponse {
    const requestId = `req_${randomBytes(16).toString('hex')}`;
    const headers = { 'content-type': 'application/json', 'x-request-id': requestId };
    const json = (status: number, value: object): ApiResponse => ({ status, headers, body: JSON.stringify(value) });
    const error = (status: number, code: ErrorCode, message: string): ApiResponse =>
      json(status, { code, message, request_id: requestId });
    if (request.body.byteLength > MAX_BODY) return error(413, 'payload_too_large', 'Request body exceeds 256 KiB');
    if (request.method === 'GET' && request.path === '/health') {
      if (request.query) return error(400, 'invalid_request', 'Health does not accept query parameters');
      if (request.body.byteLength) return error(400, 'invalid_request', 'Health does not accept a body');
      return json(200, { status: 'ok', api_version: 'v1', request_id: requestId });
    }
    if (request.path === '/v1' || request.path.startsWith('/v1/')) {
      return error(401, 'unauthenticated', 'Authentication is required');
    }
    return error(404, 'not_found', 'Route not found');
  };
}
export type Application = ReturnType<typeof createApplication>;
