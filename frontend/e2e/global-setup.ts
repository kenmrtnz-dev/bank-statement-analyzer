import { request, type FullConfig } from '@playwright/test';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import crypto from 'node:crypto';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function globalSetup(config: FullConfig) {
  const baseURL = config.projects[0]?.use?.baseURL as string;
  const authDir = path.join(__dirname, '.auth');
  const statePath = path.join(authDir, 'evaluator.json');

  await fs.mkdir(authDir, { recursive: true });

  const admin = await request.newContext({ baseURL });
  const adminLogin = await admin.post('/auth/login', {
    form: { username: 'admin', password: 'admin123' },
  });
  if (!adminLogin.ok()) {
    throw new Error(`Admin login failed: ${adminLogin.status()}`);
  }

  const username = `pw_eval_${Date.now()}_${crypto.randomUUID().slice(0, 8)}`;
  const password = 'pw_eval_smoke123';

  const createEvaluator = await admin.post('/admin/evaluators', {
    headers: { 'Content-Type': 'application/json' },
    data: { username, password },
  });
  if (!createEvaluator.ok()) {
    const body = await createEvaluator.text();
    throw new Error(`Create evaluator failed: ${createEvaluator.status()} ${body}`);
  }

  const enableUploadTesting = await admin.post('/admin/settings/upload-testing', {
    headers: { 'Content-Type': 'application/json' },
    data: { enabled: true },
  });
  if (!enableUploadTesting.ok()) {
    const body = await enableUploadTesting.text();
    throw new Error(`Enable upload testing failed: ${enableUploadTesting.status()} ${body}`);
  }
  await admin.dispose();

  const evaluator = await request.newContext({ baseURL });
  const evaluatorLogin = await evaluator.post('/auth/login', {
    form: { username, password },
  });
  if (!evaluatorLogin.ok()) {
    throw new Error(`Evaluator login failed: ${evaluatorLogin.status()}`);
  }

  await evaluator.storageState({ path: statePath });
  await evaluator.dispose();
}

export default globalSetup;
