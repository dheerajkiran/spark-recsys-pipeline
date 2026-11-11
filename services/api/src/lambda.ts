import { randomBytes } from 'node:crypto';
import type { ApiRequest, ApiResponse } from './application.js';
import { DependencyUnavailable, reportFailure, type ErrorLogger } from './failure.js';

const MAX_BODY = 262144;
const MAX_BASE64 = Math.ceil(MAX_BODY / 3) * 4;

export interface HttpApiEvent {
  version: '2.0';
  rawPath: string;
  rawQueryString?: string;
  headers?: Record<string, string>;
  body?: string;
  isBase64Encoded?: boolean;
  requestContext: { http: { method: string } };
}

function failure(status: number, code: string, message: string, requestId = `req_${randomBytes(16).toString('hex')}`): ApiResponse {
  return {
    status,
    headers: { 'content-type': 'application/json', 'x-request-id': requestId, 'cache-control': 'no-store' },
    body: JSON.stringify({ code, message, request_id: requestId, details: [] }),
  };
}

function decodeBody(event: HttpApiEvent): Buffer | ApiResponse {
  if (event.body === undefined || event.body === '') return Buffer.alloc(0);
  if (typeof event.body !== 'string') return failure(400, 'invalid_request', 'Malformed request body');
  if (!event.isBase64Encoded) {
    if (Buffer.byteLength(event.body, 'utf8') > MAX_BODY) return failure(413, 'payload_too_large', 'Request body exceeds 256 KiB');
    return Buffer.from(event.body, 'utf8');
  }
  if (event.body.length > MAX_BASE64) return failure(413, 'payload_too_large', 'Request body exceeds 256 KiB');
  if (event.body.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(event.body)) {
    return failure(400, 'invalid_request', 'Malformed base64 request body');
  }
  const padding = event.body.endsWith('==') ? 2 : event.body.endsWith('=') ? 1 : 0;
  const decodedBytes = event.body.length / 4 * 3 - padding;
  if (decodedBytes > MAX_BODY) return failure(413, 'payload_too_large', 'Request body exceeds 256 KiB');
  const bytes = Buffer.from(event.body, 'base64');
  if (bytes.toString('base64') !== event.body) return failure(400, 'invalid_request', 'Malformed base64 request body');
  return bytes;
}

function validPath(path: unknown): path is string {
  if (typeof path !== 'string' || !path.startsWith('/') || path.startsWith('//') || /[?\\#]/.test(path)) return false;
  try {
    const decoded = decodeURIComponent(path);
    return ![...decoded].some((character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127);
  } catch { return false; }
}

export function createLambdaHandler(application: (request: ApiRequest) => ApiResponse | Promise<ApiResponse>, logger?: ErrorLogger) {
  return async (event: HttpApiEvent) => {
    let result: ApiResponse;
    try {
      const validEvent = event?.version === '2.0'
        && validPath(event.rawPath)
        && typeof event.requestContext?.http?.method === 'string'
        && (event.rawQueryString === undefined || typeof event.rawQueryString === 'string')
        && (event.isBase64Encoded === undefined || typeof event.isBase64Encoded === 'boolean');
      if (!validEvent) {
        result = failure(400, 'invalid_request', 'Malformed HTTP API event');
      } else {
        const headers: Record<string, string> = {};
        let validHeaders = true;
        for (const [name, value] of Object.entries(event.headers ?? {})) {
          const normalised = name.toLowerCase();
          if (typeof value !== 'string' || Object.hasOwn(headers, normalised)) { validHeaders = false; break; }
          headers[normalised] = value;
        }
        const body = decodeBody(event);
        if (!validHeaders) result = failure(400, 'invalid_request', 'Malformed request headers');
        else if (!Buffer.isBuffer(body)) result = body;
        else result = await application({
          method: event.requestContext.http.method,
          path: event.rawPath,
          query: event.rawQueryString ?? '',
          headers,
          body,
        });
      }
    } catch (error) {
      const requestId = `req_${randomBytes(16).toString('hex')}`;
      reportFailure(error, requestId, logger);
      result = error instanceof DependencyUnavailable
        ? failure(503, 'unavailable', 'A platform dependency is unavailable', requestId)
        : failure(500, 'internal_error', 'The request could not be completed', requestId);
    }
    return { statusCode: result.status, headers: result.headers, body: result.body, isBase64Encoded: false };
  };
}
