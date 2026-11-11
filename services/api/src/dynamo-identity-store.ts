import { randomUUID } from 'node:crypto';
import { GetCommand, QueryCommand, TransactWriteCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import {
  StoreConflict, validateKey, validateQuery, validateRecord, validateWrites,
  type IdentityStore, type StoreKey, type StoreRecord, type StoreWrite,
} from './identity-store.js';

function fromItem(item: Record<string, unknown>): StoreRecord {
  const value: unknown = { pk: item.PK, sk: item.SK, revision: item.revision, data: item.data };
  validateRecord(value);
  return value;
}

/** No scans or global indexes: all reads carry a complete logical partition identity. */
export class DynamoIdentityStore implements IdentityStore {
  constructor(private readonly client: DynamoDBDocumentClient, private readonly tableName: string) {
    if (!/^[A-Za-z0-9_.-]{3,255}$/.test(tableName)) throw new Error('Invalid identity table name');
  }

  async get(key: StoreKey): Promise<StoreRecord | null> {
    validateKey(key);
    const response = await this.client.send(new GetCommand({
      TableName: this.tableName, Key: { PK: key.pk, SK: key.sk }, ConsistentRead: true,
    }));
    const record = response.Item ? fromItem(response.Item) : null;
    if (record && (record.pk !== key.pk || record.sk !== key.sk)) throw new Error('Unexpected identity key');
    return record;
  }

  async query(pk: string, skPrefix: string): Promise<StoreRecord[]> {
    validateQuery(pk, skPrefix);
    const records: StoreRecord[] = [];
    let cursor: Record<string, unknown> | undefined;
    do {
      const response = await this.client.send(new QueryCommand({
        TableName: this.tableName, ConsistentRead: true,
        KeyConditionExpression: '#pk = :pk AND begins_with(#sk, :prefix)',
        ExpressionAttributeNames: { '#pk': 'PK', '#sk': 'SK' },
        ExpressionAttributeValues: { ':pk': pk, ':prefix': skPrefix },
        ...(cursor ? { ExclusiveStartKey: cursor } : {}),
      }));
      for (const item of response.Items ?? []) {
        const record = fromItem(item);
        if (record.pk !== pk || !record.sk.startsWith(skPrefix)) throw new Error('Unexpected identity query key');
        records.push(record);
      }
      cursor = response.LastEvaluatedKey;
    } while (cursor && Object.keys(cursor).length);
    return records;
  }

  async transact(writes: StoreWrite[]): Promise<void> {
    validateWrites(writes);
    const command = new TransactWriteCommand({
      ClientRequestToken: randomUUID(),
      TransactItems: writes.map((write) => {
        const condition = write.expectedRevision === null ? {
          ConditionExpression: 'attribute_not_exists(#pk)',
          ExpressionAttributeNames: { '#pk': 'PK' },
        } : {
          ConditionExpression: '#revision = :expected',
          ExpressionAttributeNames: { '#revision': 'revision' },
          ExpressionAttributeValues: { ':expected': write.expectedRevision },
        };
        return write.record ? { Put: {
          TableName: this.tableName,
          Item: { PK: write.record.pk, SK: write.record.sk, revision: write.record.revision, data: write.record.data },
          ...condition,
        } } : { Delete: {
          TableName: this.tableName, Key: { PK: write.key.pk, SK: write.key.sk }, ...condition,
        } };
      }),
    });
    try { await this.client.send(command); }
    catch (error) {
      const failure = error as { name?: string; CancellationReasons?: { Code?: string }[] };
      if (failure.name === 'ConditionalCheckFailedException'
        || (failure.name === 'TransactionCanceledException'
          && failure.CancellationReasons?.some((reason) => reason.Code === 'ConditionalCheckFailed' || reason.Code === 'TransactionConflict'))
        || failure.name === 'TransactionConflictException') throw new StoreConflict();
      throw error;
    }
  }
}
