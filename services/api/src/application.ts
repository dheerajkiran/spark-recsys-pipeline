import { randomBytes } from 'node:crypto';
import type { Config } from './config.js';
import { createDashboardVerifier, type DashboardVerifier } from './auth.js';
import { ApiError, denied, invalid, notFound, unauthenticated } from './api-error.js';
import { matchRoute, validateInput, validateOutput } from './api-contract.js';
import { IdentityService, type Identity, type Scope } from './identity-service.js';
import { createIdentityStore } from './identity-runtime.js';
import type { IdentityStore } from './identity-store.js';
import { paginate, readCursor } from './pagination.js';

export interface ApiRequest {
  method: string;
  path: string;
  query: string;
  body: Uint8Array;
  headers: Record<string, string>;
}
export interface ApiResponse {
  status: number;
  headers: Record<string, string>;
  body: string;
}
interface Dependencies { store?: IdentityStore; verifier?: DashboardVerifier; now?: () => number }

export function createApplication(config: Config, dependencies: Dependencies = {}) {
  const now = dependencies.now ?? Date.now;
  const service = new IdentityService(dependencies.store ?? createIdentityStore(config), now);
  const auth = config.auth ?? { mode: 'disabled' as const };
  if (auth.mode !== 'disabled' && (!config.cursorSecret || Buffer.byteLength(config.cursorSecret) < 32)) throw new Error('Authentication requires a cursor signing secret');
  let verifier: Promise<DashboardVerifier> | undefined;
  return async function handle(request: ApiRequest): Promise<ApiResponse> {
    const requestId = `req_${randomBytes(16).toString('hex')}`;
    const headers = { 'content-type': 'application/json', 'x-request-id': requestId, 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' };
    const json = (status: number, value: object): ApiResponse => ({ status, headers, body: JSON.stringify(value) });
    try {
      if (request.body.byteLength > 262144) throw new ApiError(413, 'payload_too_large', 'Request body exceeds 256 KiB');
      if (request.method === 'GET' && request.path === '/health') {
        if (request.query || request.body.byteLength) throw invalid('Health does not accept query parameters or a body');
        return json(200, { status: 'ok', api_version: 'v1', request_id: requestId });
      }
      if (request.path !== '/v1' && !request.path.startsWith('/v1/')) throw notFound();
      const normalized: Record<string, string> = {};
      for (const [key, value] of Object.entries(request.headers)) {
        const lower = key.toLowerCase();
        if (Object.hasOwn(normalized, lower)) throw invalid('Ambiguous request headers');
        normalized[lower] = value;
      }
      const route = matchRoute(request.method, request.path);
      if (route?.operation.operationId === 'create_demo_session') {
        validateInput(route, request.query, normalized, request.body);
        throw new ApiError(503, 'unavailable', 'Public demonstration sessions are not enabled');
      }
      if (auth.mode === 'disabled') throw unauthenticated();
      const credentialCount = ['authorization', 'x-api-key', 'x-demo-session'].filter((name) => normalized[name] !== undefined).length;
      if (credentialCount !== 1 || normalized['x-demo-session'] !== undefined) throw unauthenticated();
      let identity: Identity;
      if (normalized['x-api-key'] !== undefined) identity = await service.verifyKey(normalized['x-api-key']);
      else {
        const bearer = /^Bearer ([A-Za-z0-9_.-]+)$/i.exec(normalized.authorization ?? '');
        if (!bearer) throw unauthenticated();
        verifier ??= dependencies.verifier ? Promise.resolve(dependencies.verifier) : createDashboardVerifier(auth);
        const dashboardVerifier = await verifier;
        try { identity = await dashboardVerifier.verify(bearer[1] as string); } catch { throw unauthenticated(); }
      }
      if (!route) throw notFound();
      if (route.template.startsWith('/v1/demo/')) throw unauthenticated();
      const permissions = route.operation['x-permissions'];
      const projectId = route.params.project_id;
      if (projectId) {
        if (!/^prj_[0-9a-f]{32}$/.test(projectId)) throw invalid('Invalid project identifier');
        await service.authorize(projectId, identity, permissions.actors, permissions.integration_scopes);
      } else if (identity.kind !== 'dashboard') throw denied();
      const input = validateInput(route, request.query, normalized, request.body);
      const context = { subject: identity.subject, requestId };
      const idem = normalized['idempotency-key'] ?? '';
      const operation = route.operation.operationId;
      let status = 200;
      let result: object;
      if (operation === 'create_project') {
        status = 201;
        result = { project: await service.createProject(context, input.body.name as string, idem) };
      } else if (operation === 'get_project' && projectId) {
        result = { project: await service.getProject(projectId, identity.subject) };
      } else if (operation === 'list_projects' || operation === 'list_keys') {
        const cursorContext = { secret: config.cursorSecret as string, route: request.path, actor: identity.subject, now: now() };
        const cursor = readCursor(input.query.get('cursor'), cursorContext.secret, cursorContext.route, cursorContext.actor, cursorContext.now);
        const limit = Number(input.query.get('limit') ?? 20);
        if (operation === 'list_projects') {
          const list = await service.listProjects(identity.subject, cursor.snapshot);
          result = paginate(list.items, limit, list.snapshot, cursor.after, cursorContext);
        } else {
          const list = await service.listKeys(projectId as string, cursor.snapshot);
          result = paginate(list.items, limit, list.snapshot, cursor.after, cursorContext);
        }
      } else if (operation === 'issue_key' && projectId) {
        status = 201;
        result = await service.issueKey(context, projectId, input.body as unknown as { name: string; scopes: Scope[]; expires_at: string }, idem);
      } else if (operation === 'revoke_key' && projectId) {
        result = { key: await service.revokeKey(context, projectId, route.params.key_id as string, idem) };
      } else {
        // Check permissions before any future resource lookup or compute dispatch.
        throw new ApiError(503, 'unavailable', 'This operation is not enabled in the current checkpoint');
      }
      validateOutput(route.operation, status, { ...result, request_id: requestId });
      return json(status, { ...result, request_id: requestId });
    } catch (error) {
      if (error instanceof ApiError) return json(error.status, { code: error.code, message: error.message, request_id: requestId, details: [] });
      return json(503, { code: 'unavailable', message: 'Platform state is temporarily unavailable', request_id: requestId, details: [] });
    }
  };
}
export type Application = ReturnType<typeof createApplication>;
