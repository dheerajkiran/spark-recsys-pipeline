import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { test } from 'node:test';
import { CreateTableCommand, DeleteTableCommand, DynamoDBClient, waitUntilTableExists } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient, GetCommand, QueryCommand, TransactWriteCommand,
  type GetCommandOutput, type QueryCommandOutput,
} from '@aws-sdk/lib-dynamodb';
import { DynamoIdentityStore } from '../src/dynamo-identity-store.js';
import { StoreConflict, type StoreRecord, type StoreWrite } from '../src/identity-store.js';

type SentCommand = GetCommand | QueryCommand | TransactWriteCommand;
const record = (pk: string, sk = 'META', revision = 1): StoreRecord => ({ pk, sk, revision, data: { name: 'example' } });
const put = (value: StoreRecord, expectedRevision: number | null = null): StoreWrite => ({ key: value, expectedRevision, record: value });
const clientWith = (send: (command: SentCommand) => Promise<unknown>) => ({ send }) as unknown as DynamoDBDocumentClient;
const wire = (value: StoreRecord) => ({ PK: value.pk, SK: value.sk, revision: value.revision, data: value.data });

test('DynamoDB get and paginated queries bind strongly consistent reads to one partition', async () => {
  const sent: SentCommand[] = [];
  let queries = 0;
  const client = clientWith(async (command) => {
    sent.push(command);
    if (command instanceof GetCommand) return { Item: wire(record('PROJECT#a')), $metadata: {} } as GetCommandOutput;
    if (!(command instanceof QueryCommand)) throw new Error('Unexpected command');
    queries++;
    return {
      Items: [wire(record('PROJECT#a', `KEY#${queries}`))], $metadata: {},
      ...(queries === 1 ? { LastEvaluatedKey: { PK: 'PROJECT#a', SK: 'KEY#1' } } : {}),
    } as QueryCommandOutput;
  });
  const store = new DynamoIdentityStore(client, 'loop-identity');
  assert.equal((await store.get({ pk: 'PROJECT#a', sk: 'META' }))?.pk, 'PROJECT#a');
  assert.equal((await store.query('PROJECT#a', 'KEY#')).length, 2);
  assert.deepEqual(sent[0]?.input, { TableName: 'loop-identity', Key: { PK: 'PROJECT#a', SK: 'META' }, ConsistentRead: true });
  for (const command of sent.slice(1)) {
    assert.ok(command instanceof QueryCommand);
    assert.equal(command.input.ConsistentRead, true);
    assert.equal(command.input.KeyConditionExpression, '#pk = :pk AND begins_with(#sk, :prefix)');
    assert.deepEqual(command.input.ExpressionAttributeValues, { ':pk': 'PROJECT#a', ':prefix': 'KEY#' });
    assert.equal(command.input.IndexName, undefined);
    assert.equal(command.input.FilterExpression, undefined);
  }
  assert.deepEqual((sent[2] as QueryCommand).input.ExclusiveStartKey, { PK: 'PROJECT#a', SK: 'KEY#1' });
});

test('DynamoDB writes enforce atomic absence/revision conditions and never retain implicit writes', async () => {
  const sent: SentCommand[] = [];
  const store = new DynamoIdentityStore(clientWith(async (command) => { sent.push(command); return {}; }), 'loop-identity');
  const created = record('PROJECT#a');
  await store.transact([put(created), put(record('PRINCIPAL#alice', 'PROJECT#a'))]);
  const command = sent[0];
  assert.ok(command instanceof TransactWriteCommand);
  assert.equal(command.input.TransactItems?.length, 2);
  const first = command.input.TransactItems?.[0]?.Put;
  assert.deepEqual(first?.Item, wire(created));
  assert.equal(first?.ConditionExpression, 'attribute_not_exists(#pk)');
  assert.equal(typeof command.input.ClientRequestToken, 'string');
  await store.transact([put(record('PROJECT#a', 'META', 2), 1)]);
  const update = (sent[1] as TransactWriteCommand).input.TransactItems?.[0]?.Put;
  assert.equal(update?.ConditionExpression, '#revision = :expected');
  assert.deepEqual(update?.ExpressionAttributeValues, { ':expected': 1 });
  await store.transact([{ key: created, expectedRevision: 2, record: null }]);
  const deletion = (sent[2] as TransactWriteCommand).input.TransactItems?.[0]?.Delete;
  assert.deepEqual(deletion?.Key, { PK: 'PROJECT#a', SK: 'META' });
  assert.deepEqual(deletion?.ExpressionAttributeValues, { ':expected': 2 });
});

test('DynamoDB conditional failures map to conflicts, infrastructure failures remain failures', async () => {
  let failure: object = { name: 'TransactionCanceledException', CancellationReasons: [{ Code: 'ConditionalCheckFailed' }] };
  const store = new DynamoIdentityStore(clientWith(async () => { throw failure; }), 'loop-identity');
  await assert.rejects(store.transact([put(record('PROJECT#a'))]), StoreConflict);
  failure = { name: 'TransactionConflictException' };
  await assert.rejects(store.transact([put(record('PROJECT#a'))]), StoreConflict);
  failure = new Error('backend unavailable');
  await assert.rejects(store.transact([put(record('PROJECT#a'))]), /backend unavailable/);
});

test('DynamoDB adapter rejects malformed or foreign responses and invalid writes', async () => {
  const client = clientWith(async (command) => command instanceof QueryCommand
    ? { Items: [wire(record('PROJECT#foreign', 'KEY#one'))] }
    : { Item: wire(record('PROJECT#foreign')) });
  const store = new DynamoIdentityStore(client, 'loop-identity');
  await assert.rejects(store.get({ pk: 'PROJECT#a', sk: 'META' }), /Unexpected identity key/);
  await assert.rejects(store.query('PROJECT#a', 'KEY#'), /Unexpected identity query key/);
  await assert.rejects(store.transact([put(record('PROJECT#a')), put(record('PROJECT#a'))]), /Duplicate/);
});

const endpoint = process.env.LOOP_DYNAMODB_TEST_ENDPOINT;
test('DynamoDB Local proves conditional transaction isolation and all-or-nothing persistence', { skip: !endpoint }, async () => {
  const url = new URL(endpoint!);
  assert.ok(url.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname), 'Test endpoint must be local');
  const low = new DynamoDBClient({ endpoint: url.href, region: 'us-east-1', credentials: { accessKeyId: 'local', secretAccessKey: 'local' } });
  const client = DynamoDBDocumentClient.from(low);
  const table = `loop-identity-test-${randomUUID()}`;
  try {
    await low.send(new CreateTableCommand({
      TableName: table, BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [{ AttributeName: 'PK', AttributeType: 'S' }, { AttributeName: 'SK', AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'PK', KeyType: 'HASH' }, { AttributeName: 'SK', KeyType: 'RANGE' }],
    }));
    await waitUntilTableExists({ client: low, maxWaitTime: 20, minDelay: 1 }, { TableName: table });
    const store = new DynamoIdentityStore(client, table);
    const outcomes = await Promise.allSettled(Array.from({ length: 5 }, () => store.transact([put(record('PROJECT#a'))])));
    assert.equal(outcomes.filter((outcome) => outcome.status === 'fulfilled').length, 1);
    assert.equal(outcomes.filter((outcome) => outcome.status === 'rejected' && outcome.reason instanceof StoreConflict).length, 4);
    await assert.rejects(store.transact([
      put(record('PROJECT#b')),
      put(record('PROJECT#a', 'META', 2), 1),
      put(record('PRINCIPAL#alice', 'PROJECT#a', 2), 1),
    ]), StoreConflict);
    assert.equal(await store.get({ pk: 'PROJECT#b', sk: 'META' }), null);
    assert.equal((await store.get({ pk: 'PROJECT#a', sk: 'META' }))?.revision, 1);
    await store.transact([put(record('PROJECT#a', 'KEY#same')), put(record('PROJECT#b', 'KEY#same'))]);
    assert.equal((await store.query('PROJECT#a', 'KEY#')).length, 1);
    assert.equal((await store.query('PROJECT#b', 'KEY#')).length, 1);
  } finally {
    await low.send(new DeleteTableCommand({ TableName: table })).catch(() => undefined);
    low.destroy();
  }
});
