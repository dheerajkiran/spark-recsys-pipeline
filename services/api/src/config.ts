import { isAbsolute, resolve } from 'node:path';

export interface Config {
  host: string;
  port: number;
  dataRoot: string;
}

export function loadConfig(env: NodeJS.ProcessEnv): Config {
  const root = env.LOOP_DATA_ROOT;
  if (!root || !isAbsolute(root)) {
    throw new Error('LOOP_DATA_ROOT must be an absolute path to writable local platform data');
  }
  const portText = env.LOOP_PORT ?? '4100';
  const port = Number(portText);
  if (!/^[0-9]+$/.test(portText) || !Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error('LOOP_PORT must be an integer from 1 to 65535');
  }
  const host = env.LOOP_HOST ?? '127.0.0.1';
  if (host !== '127.0.0.1' && host !== 'localhost') {
    throw new Error('LOOP_HOST must be 127.0.0.1 or localhost for local development');
  }
  return { host, port, dataRoot: resolve(root) };
}
