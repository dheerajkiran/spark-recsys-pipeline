import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import { invalid } from './api-error.js';

interface Cursor { route: string; actor: string; snapshot: number; after: number; expires: number }
export function paginate<T extends { sequence: number }>(items: T[], limit: number, snapshot: number, after: number, context: { secret: string; route: string; actor: string; now: number }): { items: Omit<T, 'sequence'>[]; next_cursor: string | null } {
  const remaining = items.filter((item) => item.sequence > after);
  const page = remaining.slice(0, limit);
  let next: string | null = null;
  if (remaining.length > page.length && page.length) {
    const nonce = randomBytes(12);
    const cipher = createCipheriv('aes-256-gcm', createHash('sha256').update(context.secret).digest(), nonce);
    const payload: Cursor = { route: context.route, actor: context.actor, snapshot, after: page.at(-1)?.sequence ?? 0, expires: context.now + 15 * 60000 };
    const bytes = Buffer.concat([cipher.update(JSON.stringify(payload), 'utf8'), cipher.final()]);
    next = Buffer.concat([nonce, cipher.getAuthTag(), bytes]).toString('base64url');
  }
  return { items: page.map(({ sequence: _sequence, ...item }) => item), next_cursor: next };
}
export function readCursor(token: string | null, secret: string, route: string, actor: string, now: number): { snapshot?: number; after: number } {
  if (!token) return { after: 0 };
  try {
    if (token.length > 2048 || !/^[A-Za-z0-9_-]+$/.test(token)) throw new Error();
    const bytes = Buffer.from(token, 'base64url');
    if (bytes.length < 29) throw new Error();
    const decipher = createDecipheriv('aes-256-gcm', createHash('sha256').update(secret).digest(), bytes.subarray(0, 12));
    decipher.setAuthTag(bytes.subarray(12, 28));
    const data = JSON.parse(Buffer.concat([decipher.update(bytes.subarray(28)), decipher.final()]).toString('utf8')) as Cursor;
    if (data.route !== route || data.actor !== actor || data.expires <= now || !Number.isSafeInteger(data.expires) || !Number.isSafeInteger(data.snapshot) || !Number.isSafeInteger(data.after) || data.after < 0 || data.snapshot < data.after) throw new Error();
    return { snapshot: data.snapshot, after: data.after };
  } catch { throw invalid('Cursor is invalid, expired, or belongs to a different request'); }
}
