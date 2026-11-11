import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { ApiError, denied, invalid, notFound, unauthenticated } from './api-error.js';
import type { DashboardIdentity } from './auth.js';
import { StoreConflict, type IdentityStore, type StoreKey, type StoreRecord, type StoreWrite } from './identity-store.js';

export type Role = 'owner' | 'viewer';
export type Scope = 'catalog:write' | 'events:write' | 'recommendations:read';
export interface IntegrationIdentity { kind: 'integration'; subject: string; projectId: string; scopes: Scope[] }
export type Identity = DashboardIdentity | IntegrationIdentity;
export interface Project { project_id: string; name: string; role: Role; created_at: string }
export interface KeySummary { key_id: string; name: string; scopes: Scope[]; masked_key: string; expires_at: string; revoked: boolean }
interface KeyData { summary: KeySummary; digest: string; projectId: string; sequence: number }
export interface IdentityContext { subject: string; requestId: string }
export const hash = (text: string): string => createHash('sha256').update(text).digest('hex');
export const principalPk = (subject: string): string => `PRINCIPAL#${hash(subject)}`;
export const projectPk = (projectId: string): string => `PROJECT#${projectId}`;
const memberKey = (projectId: string, subject: string): StoreKey => ({ pk: projectPk(projectId), sk: `MEMBER#${hash(subject)}` });
const resourceId = (prefix: string) => `${prefix}_${randomBytes(16).toString('hex')}`;
const DAY = 86400000;
const write = (key: StoreKey, before: StoreRecord | null, data: Record<string, unknown>): StoreWrite => ({
  key, expectedRevision: before?.revision ?? null, record: { ...key, revision: (before?.revision ?? 0) + 1, data },
});
const canonical = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(',')}}`;
  return JSON.stringify(value);
};

export class IdentityService {
  constructor(readonly store: IdentityStore, readonly now: () => number = Date.now) {}

  private audit(pk: string, context: IdentityContext, action: string, fields: Record<string, unknown>): StoreWrite {
    return write({ pk, sk: `AUDIT#${this.now()}#${randomBytes(8).toString('hex')}` }, null, {
      action, actor: hash(context.subject), request_id: context.requestId, occurred_at: new Date(this.now()).toISOString(), ...fields,
    });
  }
  private async retry<T>(action: () => Promise<T>): Promise<T> {
    for (let attempt = 0; attempt < 8; attempt++) {
      try { return await action(); } catch (error) { if (!(error instanceof StoreConflict)) throw error; }
    }
    throw new ApiError(409, 'conflict', 'Concurrent update; retry with the same idempotency key');
  }
  private idempotency(pk: string, context: IdentityContext, operation: string, key: string): StoreKey {
    return { pk, sk: `IDEMPOTENCY#${hash(`${context.subject}\n${operation}\n${key}`)}` };
  }
  private async replay(key: StoreKey, digest: string): Promise<{ before: StoreRecord | null; data: Record<string, unknown> | null }> {
    const before = await this.store.get(key);
    if (before && Number(before.data.expiresAt) > this.now()) {
      if (before.data.digest !== digest) throw new ApiError(409, 'conflict', 'Idempotency key was already used for a different request');
      return { before, data: before.data };
    }
    return { before, data: null };
  }

  async createProject(context: IdentityContext, name: string, idempotencyKey: string): Promise<Project> {
    const pk = principalPk(context.subject);
    const idemKey = this.idempotency(pk, context, 'create_project', idempotencyKey);
    const digest = hash(canonical({ name }));
    return this.retry(async () => {
      const replay = await this.replay(idemKey, digest);
      if (replay.data) return await this.getProject(String(replay.data.projectId), context.subject);
      const sequenceKey = { pk, sk: 'SEQUENCE' };
      const sequence = await this.store.get(sequenceKey);
      const number = Number(sequence?.data.value ?? 0) + 1;
      const projectId = resourceId('prj');
      const createdAt = new Date(this.now()).toISOString();
      await this.store.transact([
        write({ pk: projectPk(projectId), sk: 'META' }, null, { name, created_at: createdAt }),
        write(memberKey(projectId, context.subject), null, { role: 'owner' }),
        write({ pk, sk: `PROJECT#${projectId}` }, null, { projectId, sequence: number }),
        write(sequenceKey, sequence, { value: number }),
        write(idemKey, replay.before, { digest, projectId, expiresAt: this.now() + 7 * DAY }),
        this.audit(projectPk(projectId), context, 'project.created', { project_id: projectId }),
      ]);
      return { project_id: projectId, name, role: 'owner', created_at: createdAt };
    });
  }

  async membership(projectId: string, subject: string): Promise<StoreRecord> {
    const member = await this.store.get(memberKey(projectId, subject));
    if (!member || !['owner', 'viewer'].includes(String(member.data.role))) throw notFound();
    return member;
  }
  async authorize(projectId: string, identity: Identity, actors: string[], scopes: string[]): Promise<Role | 'integration'> {
    if (identity.kind === 'integration') {
      if (identity.projectId !== projectId) throw notFound();
      if (!actors.includes('integration') || scopes.some((scope) => !identity.scopes.includes(scope as Scope))) throw denied();
      return 'integration';
    }
    const member = await this.membership(projectId, identity.subject);
    const role = member.data.role as Role;
    if (!actors.includes(role)) throw denied();
    return role;
  }
  async getProject(projectId: string, subject: string): Promise<Project> {
    const member = await this.membership(projectId, subject);
    const project = await this.store.get({ pk: projectPk(projectId), sk: 'META' });
    if (!project) throw notFound();
    return { project_id: projectId, name: String(project.data.name), role: member.data.role as Role, created_at: String(project.data.created_at) };
  }

  async listProjects(subject: string, snapshot?: number): Promise<{ items: (Project & { sequence: number })[]; snapshot: number }> {
    const pk = principalPk(subject);
    const high = snapshot ?? Number((await this.store.get({ pk, sk: 'SEQUENCE' }))?.data.value ?? 0);
    const memberships = await this.store.query(pk, 'PROJECT#');
    const items: (Project & { sequence: number })[] = [];
    for (const entry of memberships) {
      const sequence = Number(entry.data.sequence);
      if (sequence > high) continue;
      try { items.push({ ...await this.getProject(String(entry.data.projectId), subject), sequence }); }
      catch (error) { if (!(error instanceof ApiError && error.status === 404)) throw error; }
    }
    return { items: items.sort((a, b) => a.sequence - b.sequence), snapshot: high };
  }

  // Trusted operator setup, deliberately absent from the public HTTP routes.
  async setMembership(projectId: string, subject: string, role: Role, operator = 'local-operator'): Promise<void> {
    if (!['owner', 'viewer'].includes(role)) throw invalid('Unsupported role');
    await this.retry(async () => {
      if (!await this.store.get({ pk: projectPk(projectId), sk: 'META' })) throw notFound();
      const key = memberKey(projectId, subject);
      const member = await this.store.get(key);
      // Owner transfer/removal needs its own future, explicitly reviewed protocol.
      if (member?.data.role === 'owner' && role !== 'owner') throw new ApiError(409, 'conflict', 'Owner demotion is not supported');
      const pk = principalPk(subject);
      const indexKey = { pk, sk: `PROJECT#${projectId}` };
      const index = await this.store.get(indexKey);
      const sequenceKey = { pk, sk: 'SEQUENCE' };
      const sequence = await this.store.get(sequenceKey);
      const number = index ? Number(index.data.sequence) : Number(sequence?.data.value ?? 0) + 1;
      await this.store.transact([
        write(key, member, { role }), write(indexKey, index, { projectId, sequence: number }),
        ...(!index ? [write(sequenceKey, sequence, { value: number })] : []),
        this.audit(projectPk(projectId), { subject: operator, requestId: resourceId('req') }, 'membership.updated', { member: hash(subject), role }),
      ]);
    });
  }

  async issueKey(context: IdentityContext, projectId: string, input: { name: string; scopes: Scope[]; expires_at: string }, idempotencyKey: string): Promise<{ key: KeySummary; secret: string | null }> {
    const pk = projectPk(projectId);
    const idemKey = this.idempotency(pk, context, 'issue_key', idempotencyKey);
    const digest = hash(canonical(input));
    return this.retry(async () => {
      const member = await this.membership(projectId, context.subject);
      if (member.data.role !== 'owner') throw denied();
      const replay = await this.replay(idemKey, digest);
      if (replay.data) {
        const current = await this.store.get({ pk: `KEY#${String(replay.data.keyId)}`, sk: 'LOOKUP' });
        if (!current) throw new Error('Missing issued credential');
        return { key: (current.data as unknown as KeyData).summary, secret: null };
      }
      const expiry = Date.parse(input.expires_at);
      if (!Number.isFinite(expiry) || expiry <= this.now() || expiry > this.now() + 30 * DAY) throw invalid('Key expiry must be in the future and within 30 days');
      const sequenceKey = { pk, sk: 'KEY_SEQUENCE' };
      const sequence = await this.store.get(sequenceKey);
      const number = Number(sequence?.data.value ?? 0) + 1;
      const keyId = resourceId('key');
      const secret = `loop_${keyId}.${randomBytes(32).toString('base64url')}`;
      const summary: KeySummary = { key_id: keyId, name: input.name, scopes: [...input.scopes].sort(), masked_key: `loop_****${secret.slice(-4)}`, expires_at: input.expires_at, revoked: false };
      const keyData = { summary, digest: hash(secret), projectId, sequence: number };
      await this.store.transact([
        write({ pk: `KEY#${keyId}`, sk: 'LOOKUP' }, null, keyData),
        write({ pk, sk: `KEY#${keyId}` }, null, { summary, sequence: number }),
        write(sequenceKey, sequence, { value: number }),
        write(member, member, member.data),
        write(idemKey, replay.before, { digest, keyId, expiresAt: this.now() + 7 * DAY }),
        this.audit(pk, context, 'key.issued', { project_id: projectId, key_id: keyId, scopes: summary.scopes }),
      ]);
      return { key: summary, secret };
    });
  }

  async verifyKey(secret: string): Promise<IntegrationIdentity> {
    const match = /^loop_(key_[0-9a-f]{32})\.([A-Za-z0-9_-]{43})$/.exec(secret);
    if (!match) throw unauthenticated();
    const record = await this.store.get({ pk: `KEY#${match[1]}`, sk: 'LOOKUP' });
    if (!record) throw unauthenticated();
    const data = record.data as unknown as KeyData;
    const digest = Buffer.from(data.digest, 'hex');
    if (digest.length !== 32 || !timingSafeEqual(digest, Buffer.from(hash(secret), 'hex')) || data.summary.revoked || Date.parse(data.summary.expires_at) <= this.now()) throw unauthenticated();
    return { kind: 'integration', subject: data.summary.key_id, projectId: data.projectId, scopes: data.summary.scopes };
  }
  async listKeys(projectId: string, snapshot?: number): Promise<{ items: (KeySummary & { sequence: number })[]; snapshot: number }> {
    const pk = projectPk(projectId);
    const high = snapshot ?? Number((await this.store.get({ pk, sk: 'KEY_SEQUENCE' }))?.data.value ?? 0);
    const records = await this.store.query(pk, 'KEY#');
    return { snapshot: high, items: records.filter((r) => Number(r.data.sequence) <= high).map((r) => ({ ...(r.data.summary as unknown as KeySummary), sequence: Number(r.data.sequence) })).sort((a, b) => a.sequence - b.sequence) };
  }
  async revokeKey(context: IdentityContext, projectId: string, keyId: string, idempotencyKey: string): Promise<KeySummary> {
    const pk = projectPk(projectId);
    const idemKey = this.idempotency(pk, context, `revoke_key/${keyId}`, idempotencyKey);
    const digest = hash(canonical({ keyId }));
    return this.retry(async () => {
      const member = await this.membership(projectId, context.subject);
      if (member.data.role !== 'owner') throw denied();
      const replay = await this.replay(idemKey, digest);
      const key = { pk, sk: `KEY#${keyId}` };
      const index = await this.store.get(key);
      if (!index) throw notFound();
      const summary = { ...(index.data.summary as unknown as KeySummary), revoked: true };
      if (replay.data) return summary;
      const lookupKey = { pk: `KEY#${keyId}`, sk: 'LOOKUP' };
      const lookup = await this.store.get(lookupKey);
      if (!lookup || lookup.data.projectId !== projectId) throw notFound();
      await this.store.transact([
        write(key, index, { ...index.data, summary }), write(lookupKey, lookup, { ...lookup.data, summary }),
        write(member, member, member.data),
        write(idemKey, replay.before, { digest, keyId, expiresAt: this.now() + 7 * DAY }),
        this.audit(pk, context, 'key.revoked', { project_id: projectId, key_id: keyId }),
      ]);
      return summary;
    });
  }
}
