import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { Config } from './config.js';
import { DynamoIdentityStore } from './dynamo-identity-store.js';
import { LocalIdentityStore, type IdentityStore } from './identity-store.js';

export function createIdentityStore(config: Config): IdentityStore {
  if (config.storage !== 'dynamodb') return new LocalIdentityStore(config.dataRoot);
  if (!config.tableName || !config.region) throw new Error('DynamoDB table and region are required');
  if (config.auth?.mode === 'local' && !config.dynamoEndpoint) throw new Error('Local identity requires a loopback DynamoDB endpoint');
  if (config.dynamoEndpoint) {
    const endpoint = new URL(config.dynamoEndpoint);
    if (config.auth?.mode !== 'local' || endpoint.protocol !== 'http:' || !['localhost', '127.0.0.1', '[::1]'].includes(endpoint.hostname)
      || endpoint.username || endpoint.password || endpoint.search || endpoint.hash || endpoint.pathname !== '/') throw new Error('Only loopback DynamoDB endpoints are supported in local mode');
  }
  // Local mode never consults the workstation's AWS credential chain.
  const client = new DynamoDBClient({
    region: config.region,
    ...(config.dynamoEndpoint ? { endpoint: config.dynamoEndpoint, credentials: { accessKeyId: 'local', secretAccessKey: 'local' } } : {}),
  });
  return new DynamoIdentityStore(DynamoDBDocumentClient.from(client), config.tableName);
}
