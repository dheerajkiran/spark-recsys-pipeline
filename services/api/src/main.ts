import { createApplication } from './application.js';
import { loadConfig } from './config.js';
import { createHttpServer } from './http.js';
import { LocalStorage } from './local-storage.js';
import { createDashboardVerifier } from './auth.js';

try {
  const config = loadConfig(process.env);
  await new LocalStorage(config.dataRoot).init();
  const verifier = await createDashboardVerifier(config.auth ?? { mode: 'disabled' });
  const server = createHttpServer(createApplication(config, { verifier }));
  server.on('error', (error) => {
    process.stderr.write(`HTTP server failed: ${error.message}\n`);
    process.exitCode = 1;
  });
  server.listen(config.port, config.host, () => {
    process.stdout.write(`Loop API listening on http://${config.host}:${config.port}\n`);
  });
} catch (error) {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
}
