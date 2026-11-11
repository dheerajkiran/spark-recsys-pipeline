export class DependencyUnavailable extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'DependencyUnavailable';
  }
}

export interface ErrorLog {
  level: 'error';
  request_id: string;
  error_name: string;
  stack: string;
}
export type ErrorLogger = (entry: ErrorLog) => void;

const NAMES = new Set([
  'Error', 'TypeError', 'RangeError', 'ReferenceError', 'SyntaxError',
  'URIError', 'EvalError', 'AggregateError', 'DependencyUnavailable',
]);
const MAX_STACK = 4096;

function frameLocations(stack: string): string {
  const frames: string[] = [];
  // Messages and causes can contain request data. Only source locations from
  // bounded V8-style frames are retained; function labels are not logged.
  for (const line of stack.slice(0, 32768).split('\n')) {
    if (!line.startsWith('    at ') || line.length > 2048) continue;
    let location = line.slice(7);
    const opening = location.lastIndexOf(' (');
    if (opening >= 0 && location.endsWith(')')) location = location.slice(opening + 2, -1);
    else if (location.startsWith('async ')) location = location.slice(6);
    if (!/^(?:file:\/\/\/|\/|node:)[^()\r\n?#]{1,768}:\d{1,9}:\d{1,9}$/.test(location)) continue;
    frames.push(`    at ${location}`);
    if (frames.length === 16) break;
  }
  return frames.join('\n').slice(0, MAX_STACK);
}

function safeFailure(error: unknown): { name: string; stack: string } {
  try {
    if (error instanceof Error && typeof error.name === 'string' && typeof error.message === 'string') {
      const name = NAMES.has(error.name) ? error.name : 'Error';
      // Remove the full native message prefix, including embedded newlines or
      // text imitating stack frames, rather than dropping only its first line.
      const prefix = !error.name ? error.message : !error.message ? error.name : `${error.name}: ${error.message}`;
      const raw = error.stack;
      if (typeof raw === 'string' && raw.startsWith(`${prefix}\n`)) {
        const frames = frameLocations(raw.slice(prefix.length + 1));
        if (frames) return { name, stack: frames };
      }
      return { name, stack: frameLocations(new Error().stack?.split('\n').slice(1).join('\n') ?? '') || '<stack unavailable>' };
    }
  } catch { /* An unusual error getter must not break the failure response. */ }
  return { name: 'Error', stack: frameLocations(new Error().stack?.split('\n').slice(1).join('\n') ?? '') || '<stack unavailable>' };
}

/** Emit only a correlated, sanitized record; logger failures never escape. */
export function reportFailure(error: unknown, requestId: string, logger?: ErrorLogger): void {
  try {
    const safe = safeFailure(error);
    const entry: ErrorLog = { level: 'error', request_id: requestId, error_name: safe.name, stack: safe.stack };
    const delivery: unknown = logger ? logger(entry) : console.error(JSON.stringify(entry));
    // The public logger contract is synchronous, but contain accidental async
    // logger rejections too, without logging their potentially sensitive error.
    if (delivery !== undefined) void Promise.resolve(delivery).catch(() => undefined);
  } catch { /* Observability must not change the HTTP outcome. */ }
}
