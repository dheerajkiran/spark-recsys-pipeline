import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import { test } from 'node:test';
import { DependencyUnavailable, reportFailure, type ErrorLog } from '../src/failure.js';

const requestId = `req_${'1'.repeat(32)}`;

test('failure reporter logs only correlated class and source frames, stripping complete multiline messages and causes', () => {
  const logs: ErrorLog[] = [];
  const error = new TypeError('Bearer secret-jwt\n    at /private/body.json:12:34\nbody: private-body', { cause: new Error('private-cause') });
  Object.assign(error, { headers: { authorization: 'private-header' }, body: 'private-body' });
  reportFailure(error, requestId, (entry) => { logs.push(entry); });
  assert.equal(logs.length, 1);
  assert.deepEqual(Object.keys(logs[0]!).sort(), ['level', 'request_id', 'error_name', 'stack'].sort());
  assert.equal(logs[0]?.request_id, requestId);
  assert.equal(logs[0]?.level, 'error');
  assert.equal(logs[0]?.error_name, 'TypeError');
  assert.match(logs[0]?.stack ?? '', /failure\.test\.ts:\d+:\d+/);
  assert.doesNotMatch(JSON.stringify(logs), /secret-jwt|private\/body|private-body|private-cause|private-header/);
});

test('unknown error names, thrown objects, and hostile getters never enter failure logs', () => {
  const logs: ErrorLog[] = [];
  const renamed = new Error('private-message');
  renamed.name = 'private-secret-name';
  const hostile = new Error('private-message');
  Object.defineProperty(hostile, 'stack', { get() { throw new Error('private-getter'); } });
  for (const error of [renamed, hostile, 'private-thrown-string', { headers: 'private-header', stack: 'private-stack' }]) {
    reportFailure(error, requestId, (entry) => { logs.push(entry); });
  }
  assert.equal(logs.length, 4);
  assert.ok(logs.every((entry) => entry.error_name === 'Error' && entry.request_id === requestId));
  assert.ok(logs.every((entry) => /failure(?:\.test)?\.ts:\d+:\d+/.test(entry.stack)));
  assert.doesNotMatch(JSON.stringify(logs), /private-message|private-secret-name|private-getter|private-thrown-string|private-header|private-stack/);
});

test('failure reporter bounds stacks and ignores stack annotations and frame function labels', () => {
  const logs: ErrorLog[] = [];
  const error = new Error('private-message');
  error.stack = `Error: private-message\nprivate-annotation\n    at private-function (/app/trusted.ts:2:3)\n${'    at /app/other.ts:4:5\n'.repeat(2000)}`;
  reportFailure(error, requestId, (entry) => { logs.push(entry); });
  const stack = logs[0]?.stack ?? '';
  assert.match(stack, /\/app\/trusted\.ts:2:3/);
  assert.equal(stack.split('\n').length, 16);
  assert.ok(stack.length <= 4096);
  assert.doesNotMatch(JSON.stringify(logs), /private-message|private-annotation|private-function/);
});

test('default failure logger emits one JSON record and preserves typed dependency identity', (t) => {
  const output = t.mock.method(console, 'error', () => undefined);
  reportFailure(new DependencyUnavailable('private-dependency-message', { cause: new Error('private-cause') }), requestId);
  assert.equal(output.mock.callCount(), 1);
  const line: unknown = output.mock.calls[0]?.arguments[0];
  assert.equal(typeof line, 'string');
  const entry = JSON.parse(String(line)) as ErrorLog;
  assert.equal(entry.request_id, requestId);
  assert.equal(entry.error_name, 'DependencyUnavailable');
  assert.match(entry.stack, /failure\.test\.ts:\d+:\d+/);
  assert.doesNotMatch(String(line), /private-dependency-message|private-cause/);
});

test('failure reporter contains synchronous and accidental asynchronous logger errors', async () => {
  assert.doesNotThrow(() => reportFailure(new Error('private-message'), requestId, () => { throw new Error('private-logger-error'); }));
  reportFailure(new Error('private-message'), requestId, async () => { throw new Error('private-async-logger-error'); });
  await setImmediate();
});
