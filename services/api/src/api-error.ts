export type ErrorCode = 'invalid_request' | 'unauthenticated' | 'permission_denied' | 'not_found' |
  'conflict' | 'payload_too_large' | 'quota_exceeded' | 'unavailable' | 'internal_error';

export class ApiError extends Error {
  constructor(readonly status: number, readonly code: ErrorCode, message: string) { super(message); }
}

export const invalid = (message = 'Request does not match the API contract') => new ApiError(400, 'invalid_request', message);
export const unauthenticated = () => new ApiError(401, 'unauthenticated', 'Valid credentials are required');
export const denied = () => new ApiError(403, 'permission_denied', 'This credential cannot perform this operation');
export const notFound = () => new ApiError(404, 'not_found', 'Resource not found');
