import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, test } from 'node:test';
import { exportJWK, generateKeyPair, SignJWT, type JWTPayload } from 'jose';
import { AuthenticationError, createDashboardVerifier, isCognitoIssuer, type AuthConfig } from '../src/auth.js';
import { loadConfig } from '../src/config.js';

const root = await mkdtemp(join(tmpdir(), 'loop-auth-'));
after(async () => { await rm(root, { recursive: true, force: true }); });
const pair = await generateKeyPair('RS256');
const publicJwk = { ...await exportJWK(pair.publicKey), kid: 'local-test', alg: 'RS256', use: 'sig' };
const jwksPath = join(root, 'public-jwks.json');
await writeFile(jwksPath, JSON.stringify({ keys: [publicJwk] }), { mode: 0o600 });
const auth: AuthConfig = {
  mode: 'local', issuer: 'http://loop.local', clientId: 'loop-local',
  requiredScopes: ['loop/api'], jwksPath,
};
const verifier = await createDashboardVerifier(auth);
const now = () => Math.floor(Date.now() / 1000);
async function token(overrides: Record<string, unknown> = {}, header: Record<string, unknown> = {}) {
  const payload = {
    sub: 'alice', username: 'alice', iss: auth.mode === 'local' ? auth.issuer : '',
    client_id: 'loop-local', token_use: 'access', scope: 'openid loop/api',
    iat: now(), exp: now() + 900, ...overrides,
  };
  return new SignJWT(payload).setProtectedHeader({ alg: 'RS256', kid: 'local-test', ...header }).sign(pair.privateKey);
}

test('local RSA JWT verifies signature and returns issuer-scoped immutable subject only', async () => {
  const signed = await token({ email: 'private@example.test', 'cognito:groups': ['owners'] });
  assert.deepEqual(await verifier.verify(signed), { kind: 'dashboard', subject: 'http://loop.local#alice' });
});

for (const [name, overrides] of [
  ['expired', { exp: 1, iat: 0 }],
  ['wrong issuer', { iss: 'https://untrusted.example.test' }],
  ['wrong client', { client_id: 'another-client' }],
  ['ID token', { token_use: 'id', aud: 'loop-local' }],
  ['missing scope', { scope: 'openid' }],
  ['scope substring', { scope: 'other-loop/api-scope' }],
  ['non-string scope', { scope: ['loop/api'] }],
  ['future not-before', { nbf: now() + 3600 }],
  ['future issuance', { iat: now() + 60, exp: now() + 900 }],
  ['missing expiry', { exp: undefined }],
  ['missing issuance', { iat: undefined }],
  ['excess lifetime', { exp: now() + 90000 }],
  ['fractional issuance', { iat: now() - 0.5 }],
  ['empty subject', { sub: '' }],
  ['control-character subject', { sub: 'alice\nadmin' }],
  ['machine token', { username: undefined }],
] as const) {
  test(`JWT rejects ${name} with a sanitized error`, async () => {
    const signed = await token(overrides);
    await assert.rejects(verifier.verify(signed), (error: unknown) => {
      assert.ok(error instanceof AuthenticationError);
      assert.equal(error.message, 'Authentication is required');
      assert.ok(!error.message.includes(signed));
      return true;
    });
  });
}

test('JWT rejects bad signature, unknown kid, missing kid and algorithm confusion', async () => {
  const attacker = await generateKeyPair('RS256');
  const signed = await token();
  const payload = JSON.parse(Buffer.from(signed.split('.')[1] ?? '', 'base64url').toString()) as JWTPayload;
  const wrongSignature = await new SignJWT(payload).setProtectedHeader({ alg: 'RS256', kid: 'local-test' }).sign(attacker.privateKey);
  await assert.rejects(verifier.verify(wrongSignature), AuthenticationError);
  await assert.rejects(verifier.verify(await token({}, { kid: 'unknown' })), AuthenticationError);
  const missingKid = await new SignJWT(payload).setProtectedHeader({ alg: 'RS256' }).sign(pair.privateKey);
  await assert.rejects(verifier.verify(missingKid), AuthenticationError);
  const symmetric = await new SignJWT(payload).setProtectedHeader({ alg: 'HS256', kid: 'local-test' }).sign(new Uint8Array(32));
  await assert.rejects(verifier.verify(symmetric), AuthenticationError);
  await assert.rejects(verifier.verify(`${signed.slice(0, signed.lastIndexOf('.') + 1)}`), AuthenticationError);
  await assert.rejects(verifier.verify('a'.repeat(16385)), AuthenticationError);
});

test('configured resource audience and every required scope are enforced', async () => {
  const audienceVerifier = await createDashboardVerifier({ ...auth, audience: 'https://api.example.test', requiredScopes: ['loop/api', 'openid'] });
  await assert.rejects(audienceVerifier.verify(await token()), AuthenticationError);
  await assert.rejects(audienceVerifier.verify(await token({ aud: 'https://other.example.test' })), AuthenticationError);
  await assert.rejects(audienceVerifier.verify(await token({ aud: 'https://api.example.test', scope: 'loop/api' })), AuthenticationError);
  assert.equal((await audienceVerifier.verify(await token({ aud: 'https://api.example.test' }))).subject, 'http://loop.local#alice');
});

test('disabled auth and invalid trust configuration fail closed', async () => {
  const disabled = await createDashboardVerifier({ mode: 'disabled' });
  await assert.rejects(disabled.verify(await token()), AuthenticationError);
  await assert.rejects(createDashboardVerifier({ ...auth, issuer: 'https://untrusted.test' }), /fixed local/);
  await assert.rejects(createDashboardVerifier({ ...auth, clientId: 'other' }), /fixed local/);
  const privateJwks = join(root, 'private-jwks.json');
  const privatePair = await generateKeyPair('RS256', { extractable: true });
  await writeFile(privateJwks, JSON.stringify({ keys: [{ ...await exportJWK(privatePair.privateKey), kid: 'private' }] }));
  await assert.rejects(createDashboardVerifier({ ...auth, jwksPath: privateJwks }), /public RSA JWKS/);
});

const baseEnv = { LOOP_DATA_ROOT: '/tmp/loop-config', LOOP_CURSOR_SECRET: 'a'.repeat(32) };
test('auth configuration is explicit, validated, and local trust cannot escape into Cognito or Lambda', () => {
  assert.deepEqual(loadConfig({ LOOP_DATA_ROOT: '/tmp/loop-config' }).auth, { mode: 'disabled' });
  const local = { ...baseEnv, LOOP_AUTH_MODE: 'local', LOOP_LOCAL_JWKS_PATH: jwksPath };
  assert.deepEqual(loadConfig(local).auth, auth);
  assert.throws(() => loadConfig({ ...local, LOOP_CURSOR_SECRET: 'short' }), /LOOP_CURSOR_SECRET/);
  assert.throws(() => loadConfig({ ...local, LOOP_LOCAL_JWKS_PATH: 'relative' }), /absolute/);
  assert.throws(() => loadConfig({ ...local, AWS_LAMBDA_FUNCTION_NAME: 'handler' }), /forbidden in Lambda/);
  assert.throws(() => loadConfig({ ...local, AWS_EXECUTION_ENV: 'AWS_Lambda_nodejs22.x' }), /forbidden in Lambda/);
  assert.throws(() => loadConfig({ ...local, LOOP_COGNITO_ISSUER: 'https://example.test' }), /cannot be combined/);
  assert.throws(() => loadConfig({ ...local, LOOP_JWT_SCOPES: '' }), /LOOP_JWT_SCOPES/);
  assert.throws(() => loadConfig({ ...local, LOOP_JWT_SCOPES: 'loop/api loop/api' }), /LOOP_JWT_SCOPES/);
  assert.throws(() => loadConfig({ ...baseEnv, LOOP_AUTH_MODE: 'typo' }), /LOOP_AUTH_MODE/);
  const cognito = {
    ...baseEnv, LOOP_AUTH_MODE: 'cognito', LOOP_COGNITO_ISSUER: 'https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Test',
    LOOP_COGNITO_CLIENT_ID: 'client123', LOOP_COGNITO_AUDIENCE: 'https://api.example.test',
  };
  assert.deepEqual(loadConfig(cognito).auth, {
    mode: 'cognito', issuer: cognito.LOOP_COGNITO_ISSUER, clientId: 'client123', requiredScopes: ['loop/api'], audience: 'https://api.example.test',
  });
  assert.throws(() => loadConfig({ ...cognito, LOOP_LOCAL_JWKS_PATH: jwksPath }), /cannot load local/);
  assert.throws(() => loadConfig({ ...cognito, LOOP_COGNITO_CLIENT_ID: '' }), /CLIENT_ID/);
  assert.throws(() => loadConfig({ ...cognito, LOOP_COGNITO_ISSUER: 'http://cognito-idp.us-east-1.amazonaws.com/us-east-1_Test' }), /HTTPS Cognito/);
});

test('Cognito trust source cannot select arbitrary URL, query, credentials, or mismatched region', () => {
  assert.equal(isCognitoIssuer('https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Test'), true);
  for (const issuer of [
    'https://example.test/pool',
    'https://cognito-idp.us-east-1.amazonaws.com.evil.test/us-east-1_Test',
    'https://cognito-idp.us-east-1.amazonaws.com/us-west-2_Test',
    'https://user:pass@cognito-idp.us-east-1.amazonaws.com/us-east-1_Test',
    'https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Test?redirect=evil',
    'https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Test#fragment',
    'https://cognito-idp.us-east-1.amazonaws.com:443/us-east-1_Test',
  ]) assert.equal(isCognitoIssuer(issuer), false, issuer);
});

test('Cognito verifier pins its JWKS source and validates access-token client plus resource audience', async () => {
  const issuer = 'https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Test';
  const originalFetch = globalThis.fetch;
  const seen: string[] = [];
  globalThis.fetch = async (input, init) => {
    seen.push(String(input));
    assert.equal(String(input), `${issuer}/.well-known/jwks.json`);
    assert.equal(init?.redirect, 'manual');
    return new Response(JSON.stringify({ keys: [publicJwk] }), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  try {
    const remote = await createDashboardVerifier({
      mode: 'cognito', issuer, clientId: 'client123', requiredScopes: ['loop/api'], audience: 'https://api.example.test',
    });
    const claims = { iss: issuer, client_id: 'client123', aud: 'https://api.example.test' };
    assert.deepEqual(await remote.verify(await token(claims, { jku: 'http://attacker.test/jwks.json' })), {
      kind: 'dashboard', subject: `${issuer}#alice`,
    });
    await assert.rejects(remote.verify(await token({ ...claims, client_id: 'other-client' })), AuthenticationError);
    await assert.rejects(remote.verify(await token({ ...claims, aud: 'https://other.test' })), AuthenticationError);
    assert.deepEqual(seen, [`${issuer}/.well-known/jwks.json`]);
  } finally { globalThis.fetch = originalFetch; }
});

test('DynamoDB settings require explicit backend, table, region, and loopback local endpoint', () => {
  const dynamo = {
    ...baseEnv, LOOP_AUTH_MODE: 'local', LOOP_LOCAL_JWKS_PATH: jwksPath, LOOP_STORAGE: 'dynamodb',
    LOOP_DYNAMODB_TABLE: 'loop-local', AWS_REGION: 'us-east-1', LOOP_DYNAMODB_ENDPOINT: 'http://127.0.0.1:8000',
  };
  assert.equal(loadConfig(dynamo).dynamoEndpoint, 'http://127.0.0.1:8000/');
  assert.throws(() => loadConfig({ ...dynamo, LOOP_DYNAMODB_ENDPOINT: '' }), /explicit loopback/);
  assert.throws(() => loadConfig({ ...dynamo, LOOP_STORAGE: 'local' }), /require LOOP_STORAGE/);
  assert.throws(() => loadConfig({ ...dynamo, LOOP_DYNAMODB_TABLE: '' }), /TABLE/);
  assert.throws(() => loadConfig({ ...dynamo, AWS_REGION: '' }), /AWS_REGION/);
  assert.throws(() => loadConfig({ ...dynamo, LOOP_DYNAMODB_ENDPOINT: 'http://example.test:8000' }), /loopback/);
  assert.throws(() => loadConfig({ ...dynamo, LOOP_DYNAMODB_ENDPOINT: 'http://127.0.0.1:8000/path' }), /loopback/);
  assert.throws(() => loadConfig({ ...dynamo, LOOP_AUTH_MODE: 'disabled' }), /explicit local mode/);
});
