import { readFileSync } from 'node:fs';
import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { invalid } from './api-error.js';

interface Parameter { name: string; in: string; required?: boolean; schema: object }
export interface Operation {
  operationId: string;
  parameters: ({ $ref: string } | Parameter)[];
  'x-permissions': { actors: string[]; integration_scopes: string[] };
  requestBody?: { content: { 'application/json': { schema: object } } };
  responses: Record<string, { content?: { 'application/json': { schema: object } } }>;
}
interface Contract {
  paths: Record<string, Record<string, Operation>>;
  components: { parameters: Record<string, Parameter>; schemas: Record<string, object> };
}
const contract = JSON.parse(readFileSync(new URL('../../../contracts/openapi.json', import.meta.url), 'utf8')) as Contract;
const data = JSON.parse(readFileSync(new URL('../../../contracts/schemas/data.schema.json', import.meta.url), 'utf8')) as Record<string, unknown>;
const apiId = 'https://loop.example/contracts/openapi.json';
// OpenAPI embeds JSON Schema under components; compile only referenced schemas.
const ajv = new Ajv2020.default({ strict: false, allErrors: false, validateFormats: true });
addFormats.default(ajv);
const validators = new Map<string, ReturnType<typeof ajv.compile>>();
function validator(schema: object) {
  const key = JSON.stringify(schema);
  let compiled = validators.get(key);
  if (!compiled) { compiled = ajv.compile(schema); validators.set(key, compiled); }
  return compiled;
}
const dataSchema = { ...data };
delete dataSchema.$id;
ajv.addSchema(dataSchema, 'https://loop.example/contracts/schemas/data.schema.json');
ajv.addSchema({ $id: apiId, components: contract.components }, apiId);
const routes = Object.entries(contract.paths).flatMap(([template, operations]) => {
  const names: string[] = [];
  const pattern = template.split('/').map((part) => {
    if (part.startsWith('{')) { names.push(part.slice(1, -1)); return '([^/]+)'; }
    return part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }).join('/');
  return Object.entries(operations).map(([method, operation]) => ({ method: method.toUpperCase(), template, names, pattern: new RegExp(`^${pattern}$`), operation }));
});

export interface MatchedRoute { template: string; operation: Operation; params: Record<string, string> }
export function matchRoute(method: string, path: string): MatchedRoute | null {
  for (const route of routes) {
    const match = route.method === method && route.pattern.exec(path);
    if (match) return { template: route.template, operation: route.operation,
      params: Object.fromEntries(route.names.map((name, i) => [name, match[i + 1] as string])) };
  }
  return null;
}

export function validateSchema(schema: object, value: unknown): void {
  if (!validator(schema)(value)) throw invalid();
}
function absoluteSchema(schema: object): object {
  return JSON.parse(JSON.stringify(schema).replaceAll('"#/components/', `"${apiId}#/components/`).replaceAll('"schemas/data.schema.json', '"https://loop.example/contracts/schemas/data.schema.json')) as object;
}
export function validateInput(route: MatchedRoute, query: string, headers: Record<string, string>, bytes: Uint8Array): { body: Record<string, unknown>; query: URLSearchParams } {
  const parsed = new URLSearchParams(query);
  const parameters = route.operation.parameters.map((p) => '$ref' in p ? contract.components.parameters[p.$ref.split('/').at(-1) as string] as Parameter : p);
  const allowedQuery = parameters.filter((p) => p.in === 'query').map((p) => p.name);
  for (const key of parsed.keys()) if (!allowedQuery.includes(key) || parsed.getAll(key).length !== 1) throw invalid('Unknown or repeated query parameter');
  for (const parameter of parameters) {
    let value: unknown = parameter.in === 'path' ? route.params[parameter.name] : parameter.in === 'header' ? headers[parameter.name.toLowerCase()] : parsed.get(parameter.name) ?? undefined;
    if (value === undefined) { if (parameter.required) throw invalid(`Missing ${parameter.name}`); else continue; }
    if (parameter.name === 'limit') {
      if (!/^[1-9][0-9]{0,2}$/.test(String(value))) throw invalid('Invalid limit');
      value = Number(value);
    }
    validateSchema(absoluteSchema(parameter.schema), value);
  }
  let body: unknown = {};
  if (route.operation.requestBody) {
    if (!/^application\/json(?:\s*;\s*charset=utf-8)?$/i.test(headers['content-type'] ?? '')) throw invalid('Content-Type must be application/json');
    try { body = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); } catch { throw invalid('Body must be valid UTF-8 JSON'); }
    validateSchema(absoluteSchema(route.operation.requestBody.content['application/json'].schema), body);
  } else if (bytes.byteLength) throw invalid('This operation does not accept a body');
  return { body: body as Record<string, unknown>, query: parsed };
}

export function validateOutput(operation: Operation, status: number, value: object): void {
  const schema = operation.responses[String(status)]?.content?.['application/json'].schema;
  if (schema && !validator(absoluteSchema(schema))(value)) throw new Error('API response failed contract validation');
}

export function validateError(value: object): boolean {
  return !!ajv.getSchema(`${apiId}#/components/schemas/Error`)?.(value);
}
