import { isAbsolute, resolve } from 'node:path';
import { isCognitoIssuer, type AuthConfig } from './auth.js';

export interface Config {
  host: string;
  port: number;
  dataRoot: string;
  auth?: AuthConfig;
  cursorSecret?: string;
  storage?: 'local' | 'dynamodb';
  tableName?: string;
  region?: string;
  dynamoEndpoint?: string;
}

export function loadConfig(env: NodeJS.ProcessEnv): Config {
  const root = env.LOOP_DATA_ROOT;
  if (!root || !isAbsolute(root)) {
    throw new Error('LOOP_DATA_ROOT must be an absolute path to writable local platform data');
  }
  const portText = env.LOOP_PORT ?? '4100';
  const port = Number(portText);
  if (!/^[0-9]+$/.test(portText) || !Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error('LOOP_PORT must be an integer from 1 to 65535');
  }
  const host = env.LOOP_HOST ?? '127.0.0.1';
  if (host !== '127.0.0.1' && host !== 'localhost') {
    throw new Error('LOOP_HOST must be 127.0.0.1 or localhost for local development');
  }
  const mode = env.LOOP_AUTH_MODE ?? 'disabled';
  if (mode !== 'disabled' && mode !== 'local' && mode !== 'cognito') {
    throw new Error('LOOP_AUTH_MODE must be disabled, local, or cognito');
  }
  const storage = env.LOOP_STORAGE ?? 'local';
  if (storage !== 'local' && storage !== 'dynamodb') throw new Error('LOOP_STORAGE must be local or dynamodb');
  if (mode === 'cognito' && env.LOOP_STORAGE !== 'dynamodb') {
    throw new Error('Cognito authentication requires explicit LOOP_STORAGE=dynamodb');
  }
  const config: Config = { host, port, dataRoot: resolve(root), auth: { mode: 'disabled' }, storage };
  if (mode !== 'disabled') {
    const cursorSecret = env.LOOP_CURSOR_SECRET;
    if (!cursorSecret || Buffer.byteLength(cursorSecret) < 32) {
      throw new Error('LOOP_CURSOR_SECRET must contain at least 32 bytes when authentication is enabled');
    }
    config.cursorSecret = cursorSecret;
    const requiredScopes = (env.LOOP_JWT_SCOPES ?? 'loop/api').split(' ');
    if (!requiredScopes.length || requiredScopes.some((scope) => !/^[\x21\x23-\x5b\x5d-\x7e]+$/.test(scope))
      || new Set(requiredScopes).size !== requiredScopes.length) {
      throw new Error('LOOP_JWT_SCOPES must contain unique nonempty space-separated OAuth scopes');
    }
    if (mode === 'local') {
      if (env.AWS_LAMBDA_FUNCTION_NAME || env.AWS_EXECUTION_ENV?.startsWith('AWS_Lambda')) {
        throw new Error('Local authentication is forbidden in Lambda');
      }
      const jwksPath = env.LOOP_LOCAL_JWKS_PATH;
      if (!jwksPath || !isAbsolute(jwksPath)) throw new Error('LOOP_LOCAL_JWKS_PATH must be an absolute public JWKS path');
      if (env.LOOP_COGNITO_ISSUER || env.LOOP_COGNITO_CLIENT_ID || env.LOOP_COGNITO_AUDIENCE) {
        throw new Error('Local authentication cannot be combined with Cognito configuration');
      }
      config.auth = { mode, issuer: 'http://loop.local', clientId: 'loop-local', jwksPath, requiredScopes };
    } else {
      const issuer = env.LOOP_COGNITO_ISSUER;
      const clientId = env.LOOP_COGNITO_CLIENT_ID;
      if (!issuer || !isCognitoIssuer(issuer)) throw new Error('LOOP_COGNITO_ISSUER must be a standard HTTPS Cognito user-pool issuer');
      if (!clientId || !/^[A-Za-z0-9]{1,128}$/.test(clientId)) throw new Error('LOOP_COGNITO_CLIENT_ID is required');
      if (env.LOOP_LOCAL_JWKS_PATH) throw new Error('Cognito authentication cannot load local signing keys');
      const audience = env.LOOP_COGNITO_AUDIENCE;
      if (audience !== undefined && (!audience.trim() || audience.length > 2048)) {
        throw new Error('LOOP_COGNITO_AUDIENCE must be a nonempty bounded value');
      }
      config.auth = { mode, issuer, clientId, requiredScopes, ...(audience ? { audience } : {}) };
    }
  }
  if (storage === 'dynamodb') {
    if (mode === 'local' && !env.LOOP_DYNAMODB_ENDPOINT) {
      throw new Error('Local authentication requires an explicit loopback LOOP_DYNAMODB_ENDPOINT');
    }
    const tableName = env.LOOP_DYNAMODB_TABLE;
    const region = env.AWS_REGION;
    if (!tableName || !/^[A-Za-z0-9_.-]{3,255}$/.test(tableName)) throw new Error('LOOP_DYNAMODB_TABLE is required');
    if (!region || !/^[a-z0-9-]+$/.test(region)) throw new Error('AWS_REGION is required for DynamoDB');
    config.tableName = tableName;
    config.region = region;
    if (env.LOOP_DYNAMODB_ENDPOINT) {
      let endpoint: URL;
      try { endpoint = new URL(env.LOOP_DYNAMODB_ENDPOINT); }
      catch { throw new Error('LOOP_DYNAMODB_ENDPOINT must be a local HTTP endpoint'); }
      if (mode !== 'local' || endpoint.protocol !== 'http:'
        || !['localhost', '127.0.0.1', '[::1]'].includes(endpoint.hostname)
        || endpoint.username || endpoint.password || endpoint.search || endpoint.hash || endpoint.pathname !== '/') {
        throw new Error('LOOP_DYNAMODB_ENDPOINT is allowed only for explicit local mode on loopback');
      }
      config.dynamoEndpoint = endpoint.href;
    }
  } else if (env.LOOP_DYNAMODB_ENDPOINT || env.LOOP_DYNAMODB_TABLE) {
    throw new Error('DynamoDB settings require LOOP_STORAGE=dynamodb');
  }
  return config;
}
