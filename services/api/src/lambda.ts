import type { Application } from './application.js';

export interface HttpApiEvent {
  version: '2.0';
  rawPath: string;
  rawQueryString?: string;
  headers?: Record<string, string>;
  body?: string;
  isBase64Encoded?: boolean;
  requestContext: { http: { method: string } };
}

export function createLambdaHandler(application: Application) {
  return async (event: HttpApiEvent) => {
    const result = application({
      method: event.requestContext.http.method,
      path: event.rawPath,
      query: event.rawQueryString ?? '',
      headers: event.headers ?? {},
      body: event.body ? Buffer.from(event.body, event.isBase64Encoded ? 'base64' : 'utf8') : Buffer.alloc(0),
    });
    return { statusCode: result.status, headers: result.headers, body: result.body, isBase64Encoded: false };
  };
}
