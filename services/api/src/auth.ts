import { readFile } from 'node:fs/promises';
import { isAbsolute } from 'node:path';
import {
  createLocalJWKSet, createRemoteJWKSet, customFetch, decodeProtectedHeader, errors, jwtVerify,
  type JSONWebKeySet, type JWTVerifyGetKey,
} from 'jose';
import { DependencyUnavailable } from './failure.js';

export const JWT_CLOCK_TOLERANCE_SECONDS = 30;

interface VerifiedAuthConfig {
  issuer: string;
  clientId: string;
  requiredScopes: string[];
  audience?: string;
}
export type AuthConfig =
  | { mode: 'disabled' }
  | (VerifiedAuthConfig & { mode: 'local'; jwksPath: string })
  | (VerifiedAuthConfig & { mode: 'cognito' });

export interface DashboardIdentity { kind: 'dashboard'; subject: string }
export interface DashboardVerifier { verify(token: string): Promise<DashboardIdentity> }

// Neither jose error messages nor credential bytes cross the HTTP/log boundary.
export class AuthenticationError extends Error {
  constructor() { super('Authentication is required'); this.name = 'AuthenticationError'; }
}

export function isCognitoIssuer(issuer: string): boolean {
  try {
    const url = new URL(issuer);
    const host = /^cognito-idp\.([a-z0-9-]+)\.amazonaws\.com$/.exec(url.hostname);
    const pool = /^\/([a-z0-9-]+)_[A-Za-z0-9]+$/.exec(url.pathname);
    return url.protocol === 'https:' && !url.username && !url.password && !url.port
      && !url.search && !url.hash && !!host && !!pool && host[1] === pool[1]
      && url.href === issuer;
  } catch { return false; }
}

function validSubject(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= 256
    && !/[\u0000-\u001f\u007f]/.test(value);
}

/** Resolve only configured keys. JWT jku/x5u/embedded jwk never select a trust source. */
export async function createDashboardVerifier(auth: AuthConfig): Promise<DashboardVerifier> {
  if (auth.mode === 'disabled') return { async verify() { throw new AuthenticationError(); } };
  if (!auth.clientId || !auth.requiredScopes.length) throw new Error('JWT client and scopes are required');
  let keys: JWTVerifyGetKey;
  if (auth.mode === 'local') {
    if (process.env.AWS_LAMBDA_FUNCTION_NAME || process.env.AWS_EXECUTION_ENV?.startsWith('AWS_Lambda')) {
      throw new Error('Local authentication is forbidden in Lambda');
    }
    if (auth.issuer !== 'http://loop.local' || auth.clientId !== 'loop-local' || !isAbsolute(auth.jwksPath)) {
      throw new Error('Local authentication requires the fixed local issuer/client and an absolute JWKS path');
    }
    const bytes = await readFile(auth.jwksPath);
    if (bytes.length > 65536) throw new Error('Local public JWKS exceeds 64 KiB');
    const jwks = JSON.parse(bytes.toString('utf8')) as JSONWebKeySet;
    if (!Array.isArray(jwks.keys) || jwks.keys.length < 1 || jwks.keys.length > 10
      || jwks.keys.some((key) => key.kty !== 'RSA' || key.d !== undefined
        || typeof key.kid !== 'string' || !key.kid || (key.alg !== undefined && key.alg !== 'RS256'))) {
      throw new Error('Local authentication requires a public RSA JWKS with key identifiers');
    }
    keys = createLocalJWKSet(jwks);
  } else {
    if (!isCognitoIssuer(auth.issuer)) throw new Error('A standard HTTPS Cognito user-pool issuer is required');
    const remoteKeys = createRemoteJWKSet(new URL(`${auth.issuer}/.well-known/jwks.json`), {
      timeoutDuration: 5000, cooldownDuration: 30000, cacheMaxAge: 600000,
      [customFetch]: async (url, options) => {
        let response: Response;
        try { response = await fetch(url, options); }
        catch (cause) { throw new DependencyUnavailable('Dashboard signing keys are unavailable', { cause }); }
        if (response.status !== 200) throw new DependencyUnavailable('Dashboard signing keys are unavailable');
        return response;
      },
    });
    keys = async (header, token) => {
      try { return await remoteKeys(header, token); }
      catch (cause) {
        if (cause instanceof errors.JWKSTimeout || cause instanceof errors.JWKSInvalid
          || cause instanceof errors.JWKInvalid || cause instanceof errors.JWKSMultipleMatchingKeys
          || (cause instanceof errors.JOSEError && cause.code === 'ERR_JOSE_GENERIC')) {
          throw new DependencyUnavailable('Dashboard signing keys are unavailable', { cause });
        }
        throw cause;
      }
    };
  }
  return {
    async verify(token: string): Promise<DashboardIdentity> {
      try {
        if (token.length > 16384 || !/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(token)) {
          throw new AuthenticationError();
        }
        const header = decodeProtectedHeader(token);
        if (header.alg !== 'RS256' || typeof header.kid !== 'string' || !header.kid || header.kid.length > 256) {
          throw new AuthenticationError();
        }
        const { payload, protectedHeader } = await jwtVerify(token, keys, {
          algorithms: ['RS256'], issuer: auth.issuer,
          ...(auth.audience ? { audience: auth.audience } : {}),
          requiredClaims: ['sub', 'iss', 'exp', 'iat', 'token_use', 'client_id', 'scope', 'username'],
          clockTolerance: JWT_CLOCK_TOLERANCE_SECONDS, maxTokenAge: 86400,
        });
        const now = Math.floor(Date.now() / 1000);
        if (typeof protectedHeader.kid !== 'string' || !protectedHeader.kid || protectedHeader.kid.length > 256
          || payload.token_use !== 'access' || payload.client_id !== auth.clientId
          || !validSubject(payload.sub) || !validSubject(payload.username)
          || typeof payload.iat !== 'number' || !Number.isInteger(payload.iat) || payload.iat > now + JWT_CLOCK_TOLERANCE_SECONDS
          || typeof payload.exp !== 'number' || !Number.isInteger(payload.exp)
          || payload.exp <= payload.iat || payload.exp - payload.iat > 86400
          || typeof payload.scope !== 'string') throw new AuthenticationError();
        const granted = new Set(payload.scope.split(' '));
        if (!auth.requiredScopes.every((scope) => granted.has(scope))) throw new AuthenticationError();
        return { kind: 'dashboard', subject: `${auth.issuer}#${payload.sub}` };
      } catch (error) {
        if (error instanceof AuthenticationError) throw error;
        if (error instanceof errors.JOSEAlgNotAllowed || error instanceof errors.JOSENotSupported
          || error instanceof errors.JWSInvalid || error instanceof errors.JWTInvalid
          || error instanceof errors.JWTClaimValidationFailed || error instanceof errors.JWTExpired
          || error instanceof errors.JWSSignatureVerificationFailed || error instanceof errors.JWKSNoMatchingKey) {
          throw new AuthenticationError();
        }
        throw error;
      }
    },
  };
}
