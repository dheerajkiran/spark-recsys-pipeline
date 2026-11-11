/** Explicit local operator tool. No credential or membership mutation HTTP route. */
import { randomBytes } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { exportJWK, generateKeyPair, importJWK, SignJWT, type JWK } from 'jose';
import { IdentityService, type Role } from './identity-service.js';
import { LocalIdentityStore } from './identity-store.js';

async function run(): Promise<void> {
  if (process.env.AWS_LAMBDA_FUNCTION_NAME || process.env.AWS_EXECUTION_ENV?.startsWith('AWS_Lambda')) throw new Error('Local operator tooling is forbidden in Lambda');
  const [command, root, subject, projectId, role] = process.argv.slice(2);
  if (!root || !isAbsolute(root) || /[\r\n]/.test(root)) throw new Error('Usage: local-admin <init|token|member> <absolute-data-root> [subject] [project-id] [viewer|owner]');
  const directory = join(root, 'local-auth');
  if (command === 'init') {
    await mkdir(directory, { recursive: true, mode: 0o700 });
    const { privateKey, publicKey } = await generateKeyPair('RS256', { extractable: true });
    const kid = randomBytes(16).toString('hex');
    const privateJwk = { ...await exportJWK(privateKey), kid, alg: 'RS256', use: 'sig' };
    const publicJwk = { ...await exportJWK(publicKey), kid, alg: 'RS256', use: 'sig' };
    // Existing keys are never silently replaced, which would invalidate all sessions.
    await writeFile(join(directory, 'private-jwk.json'), JSON.stringify(privateJwk), { flag: 'wx', mode: 0o600 });
    await writeFile(join(directory, 'jwks.json'), JSON.stringify({ keys: [publicJwk] }), { flag: 'wx', mode: 0o600 });
    await writeFile(join(directory, 'local.env'), [
      `LOOP_DATA_ROOT=${root}`, 'LOOP_AUTH_MODE=local', `LOOP_LOCAL_JWKS_PATH=${join(directory, 'jwks.json')}`,
      `LOOP_CURSOR_SECRET=${randomBytes(32).toString('hex')}`, 'LOOP_STORAGE=local', '',
    ].join('\n'), { flag: 'wx', mode: 0o600 });
    process.stdout.write('Local signing keys and local.env created under <data-root>/local-auth. Keep this directory private.\n');
    return;
  }
  if (!subject || !/^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$/.test(subject)) throw new Error('Subject must contain 1–128 safe identifier characters');
  if (command === 'token') {
    const key = JSON.parse(await readFile(join(directory, 'private-jwk.json'), 'utf8')) as JWK;
    const token = await new SignJWT({ token_use: 'access', client_id: 'loop-local', scope: 'loop/api', username: subject })
      .setProtectedHeader({ alg: 'RS256', kid: key.kid as string }).setIssuer('http://loop.local').setSubject(subject)
      .setIssuedAt().setExpirationTime('1h').sign(await importJWK(key, 'RS256'));
    await writeFile(join(directory, `${subject}.token`), token, { mode: 0o600 });
    process.stdout.write(`One-hour token written to <data-root>/local-auth/${subject}.token\n`);
    return;
  }
  if (command === 'member') {
    if (!projectId || !/^prj_[0-9a-f]{32}$/.test(projectId) || (role !== 'viewer' && role !== 'owner')) throw new Error('member requires a project ID and viewer or owner role');
    const service = new IdentityService(new LocalIdentityStore(root));
    await service.setMembership(projectId, `http://loop.local#${subject}`, role as Role);
    process.stdout.write('Local project membership updated.\n');
    return;
  }
  throw new Error('Supported commands are init, token, and member');
}
run().catch(() => { process.stderr.write('Local setup failed. Check command arguments, file permissions, and existing initialization.\n'); process.exitCode = 1; });
