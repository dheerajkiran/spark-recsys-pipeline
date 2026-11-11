import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createApplication } from '../src/application.js';
import type { Config } from '../src/config.js';
import { DynamoIdentityStore } from '../src/dynamo-identity-store.js';
import { assertIdentityStorageConfig, createIdentityStore } from '../src/identity-runtime.js';
import { LocalIdentityStore } from '../src/identity-store.js';

const cognito: Config = {
  host: '127.0.0.1', port: 4100, dataRoot: '/tmp/loop-identity-runtime', cursorSecret: 'a'.repeat(32),
  auth: { mode: 'cognito', issuer: 'https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Test', clientId: 'client123', requiredScopes: ['loop/api'] },
};

test('Cognito identity never falls back to local files for missing or explicit local storage', () => {
  for (const config of [cognito, { ...cognito, storage: 'local' as const }]) {
    assert.throws(() => assertIdentityStorageConfig(config), /explicit LOOP_STORAGE=dynamodb/);
    assert.throws(() => createIdentityStore(config), /explicit LOOP_STORAGE=dynamodb/);
    assert.throws(() => createApplication(config, { store: new LocalIdentityStore('/tmp/loop-injected-identity') }), /explicit LOOP_STORAGE=dynamodb/);
  }
});

test('Cognito requires configured DynamoDB table/region and rejects custom local endpoints', () => {
  assert.throws(() => createIdentityStore({ ...cognito, storage: 'dynamodb' }), /table and region/);
  const config = { ...cognito, storage: 'dynamodb' as const, tableName: 'loop-identity', region: 'us-east-1' };
  assert.ok(createIdentityStore(config) instanceof DynamoIdentityStore);
  assert.throws(() => createIdentityStore({ ...config, dynamoEndpoint: 'http://127.0.0.1:8000' }), /loopback DynamoDB endpoints.*local mode/);
});

test('local or disabled identities retain explicit local-file support', () => {
  const local: Config = { ...cognito, auth: { mode: 'local', issuer: 'http://loop.local', clientId: 'loop-local', requiredScopes: ['loop/api'], jwksPath: '/tmp/public-jwks.json' } };
  assert.ok(createIdentityStore(local) instanceof LocalIdentityStore);
  assert.ok(createIdentityStore({ ...local, auth: { mode: 'disabled' } }) instanceof LocalIdentityStore);
  assert.throws(() => createIdentityStore({ ...local, storage: 'dynamodb', tableName: 'loop-local', region: 'us-east-1' }), /loopback DynamoDB endpoint/);
});
