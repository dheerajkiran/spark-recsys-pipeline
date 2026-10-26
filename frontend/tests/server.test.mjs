import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { request } from 'node:http';
import { mkdtemp, mkdir, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createStaticServer } from '../server.mjs';

let fixture, server, port;
const video = Buffer.from('0123456789abcdefghijklmnopqrstuvwxyz');

before(async () => {
  fixture = await mkdtemp(path.join(tmpdir(), 'loop-static-test-'));
  const publicDir = path.join(fixture, 'frontend');
  await mkdir(path.join(publicDir, 'assets'), { recursive: true });
  await mkdir(path.join(fixture, 'data', 'gold'), { recursive: true });
  await writeFile(path.join(publicDir, 'index.html'), '<!doctype html><title>Loop test</title>');
  await writeFile(path.join(publicDir, 'app.js'), 'export const ready = true;');
  await writeFile(path.join(publicDir, 'assets', 'clip.mp4'), video);
  await writeFile(path.join(publicDir, 'empty.txt'), '');
  await writeFile(path.join(publicDir, '.env'), 'PRIVATE_VALUE=do-not-serve');
  await writeFile(path.join(fixture, 'data', 'gold', 'private.parquet'), 'private dataset sentinel');
  await symlink(path.join(fixture, 'data', 'gold', 'private.parquet'), path.join(publicDir, 'escape.parquet'));
  server = createStaticServer(publicDir);
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  port = server.address().port;
});

after(async () => {
  if (server?.listening) await new Promise(resolve => server.close(resolve));
  if (fixture) await rm(fixture, { recursive: true, force: true });
});

function fetchLocal(urlPath, { method = 'GET', headers = {} } = {}) {
  // Raw HTTP preserves encoded traversal strings rather than normalizing them.
  return new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port, path: urlPath, method, headers }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.end();
  });
}

test('serves HTML, JavaScript and MP4 with useful content types', async () => {
  const html = await fetchLocal('/?feed=preview');
  assert.equal(html.status, 200);
  assert.equal(html.headers['content-type'], 'text/html; charset=utf-8');
  assert.match(html.body.toString(), /Loop test/);
  assert.equal(html.headers['x-content-type-options'], 'nosniff');
  const script = await fetchLocal('/app.js');
  assert.equal(script.status, 200);
  assert.equal(script.headers['content-type'], 'text/javascript; charset=utf-8');
  const media = await fetchLocal('/assets/clip.mp4');
  assert.equal(media.status, 200);
  assert.equal(media.headers['content-type'], 'video/mp4');
  assert.equal(media.headers['accept-ranges'], 'bytes');
  assert.deepEqual(media.body, video);
});

test('serves exact, open-ended and suffix byte ranges for video seeking', async () => {
  for (const [range, start, end] of [['bytes=4-9', 4, 9], ['bytes=30-', 30, 35], ['bytes=-5', 31, 35]]) {
    const response = await fetchLocal('/assets/clip.mp4', { headers: { Range: range } });
    assert.equal(response.status, 206, range);
    assert.equal(response.headers['content-range'], `bytes ${start}-${end}/${video.length}`);
    assert.equal(Number(response.headers['content-length']), end - start + 1);
    assert.deepEqual(response.body, video.subarray(start, end + 1));
  }
});

test('clamps an oversized range to the file and handles HEAD without a body', async () => {
  const partial = await fetchLocal('/assets/clip.mp4', { headers: { Range: 'bytes=32-9999' } });
  assert.equal(partial.status, 206);
  assert.deepEqual(partial.body, video.subarray(32));
  const whole = await fetchLocal('/assets/clip.mp4', { method: 'HEAD' });
  assert.equal(whole.status, 200);
  assert.equal(Number(whole.headers['content-length']), video.length);
  assert.equal(whole.body.length, 0);
  const rangeHead = await fetchLocal('/assets/clip.mp4', { method: 'HEAD', headers: { Range: 'bytes=2-6' } });
  assert.equal(rangeHead.status, 206);
  assert.equal(Number(rangeHead.headers['content-length']), 5);
  assert.equal(rangeHead.body.length, 0);
});

test('rejects impossible and malformed ranges with the current file size', async () => {
  for (const range of ['bytes=36-', 'bytes=8-2', 'bytes=-0', 'bytes=-', 'bytes=0-1,3-4', 'items=0-4']) {
    const response = await fetchLocal('/assets/clip.mp4', { headers: { Range: range } });
    assert.equal(response.status, 416, range);
    assert.equal(response.headers['content-range'], `bytes */${video.length}`);
  }
});

test('does not expose hidden files, traversal targets or symlinks outside the frontend', async () => {
  for (const urlPath of ['/.env', '/%2e%2e%2fdata/gold/private.parquet', '/escape.parquet']) {
    const response = await fetchLocal(urlPath);
    assert.equal(response.status, 403, urlPath);
    assert.doesNotMatch(response.body.toString(), /private dataset sentinel|PRIVATE_VALUE/);
  }
  const outside = await fetchLocal('/data/gold/private.parquet');
  assert.equal(outside.status, 404);
  assert.doesNotMatch(outside.body.toString(), /private dataset sentinel/);
});

test('returns errors for missing files, directories and invalid URL encoding', async () => {
  assert.equal((await fetchLocal('/missing.mp4')).status, 404);
  assert.equal((await fetchLocal('/assets/')).status, 404);
  assert.equal((await fetchLocal('/bad%ZZ')).status, 400);
});

test('serves empty files without starting a stream and rejects write methods', async () => {
  const empty = await fetchLocal('/empty.txt');
  assert.equal(empty.status, 200);
  assert.equal(empty.headers['content-length'], '0');
  assert.equal(empty.body.length, 0);
  for (const method of ['POST', 'PUT', 'DELETE']) {
    const response = await fetchLocal('/index.html', { method });
    assert.equal(response.status, 405);
    assert.equal(response.headers.allow, 'GET, HEAD');
  }
});
