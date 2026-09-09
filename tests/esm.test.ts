import { execSync, spawn } from 'child_process';
import path from 'path';
import { Client } from 'pg';

describe('DBOS ESM Tests', () => {
  const esmTestDir = path.join(__dirname, 'esm-test');
  const dbPassword = process.env.PGPASSWORD || 'dbos';
  const sysDbName = 'esm_test_dbos_sys';

  beforeAll(async () => {
    execSync('npm run build', { cwd: path.join(__dirname, '..'), stdio: 'inherit' });

    const client = new Client({
      host: 'localhost',
      port: 5432,
      user: 'postgres',
      password: dbPassword,
      database: 'postgres',
    });
    await client.connect();
    await client.query(`DROP DATABASE IF EXISTS "${sysDbName}" WITH (FORCE)`);
    await client.end();
  }, 300000);

  test('test app with native ESM', async () => {
    // Installing links the SDK into the app, so it resolves through the published exports map.
    execSync('npm install', {
      cwd: esmTestDir,
      stdio: 'inherit',
      timeout: 120000,
    });

    execSync('npm run build', {
      cwd: esmTestDir,
      stdio: 'inherit',
      timeout: 120000,
    });

    let stdout = '';
    let stderr = '';

    const exitCode = await new Promise<number>((resolve, reject) => {
      const child = spawn('node', [path.join('dist', 'main.js')], {
        cwd: esmTestDir,
        stdio: 'pipe',
        env: {
          ...process.env,
          PGPASSWORD: dbPassword,
          DBOS_SYSTEM_DATABASE_URL: `postgresql://postgres:${dbPassword}@localhost:5432/${sysDbName}`,
        },
      });

      child.stdout.on('data', (data: Buffer) => {
        stdout += data.toString();
      });

      child.stderr.on('data', (data: Buffer) => {
        stderr += data.toString();
      });

      child.on('close', (code) => {
        resolve(code ?? 0);
      });

      child.on('error', (error) => {
        reject(error);
      });
    });

    console.log(stdout + stderr);
    expect(exitCode).toBe(0);
    expect(stdout).toContain('workflow result: step:queued|from cjs');
    expect(stdout).toContain('recorded steps: esmStep,describeError');
    expect(stdout).toContain('esm-only dependency: from esm');
    expect(stdout).toContain('ESM test completed successfully!');
  }, 300000);
});
