import { execSync, spawn } from 'child_process';
import { existsSync, readFileSync, statSync } from 'fs';
import path from 'path';
import { Client } from 'pg';

describe('DBOS Bundler Tests', () => {
  const bundlerTestDir = path.join(__dirname, 'bundler-test');
  const bundleFile = path.join(bundlerTestDir, 'dist', 'bundle.js');

  beforeAll(() => {
    execSync('npm run build', { cwd: path.join(__dirname, '..'), stdio: 'inherit' });
  }, 120000);

  test('test app with bundler', async () => {
    const dbPassword = process.env.PGPASSWORD || 'dbos';
    const sysDbName = 'bundler_test_dbos_sys';

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

    // Install dependencies and build the app with the bundler
    execSync('npm install', {
      cwd: bundlerTestDir,
      stdio: 'inherit',
      timeout: 120000,
    });

    execSync('npm run build', {
      cwd: bundlerTestDir,
      stdio: 'inherit',
      timeout: 60000,
    });

    // Validate the bundled app
    expect(existsSync(bundleFile)).toBe(true);
    const bundleContent = readFileSync(bundleFile, 'utf8');
    expect(bundleContent).toContain('DBOS');
    expect(bundleContent).toContain('testWorkflow');
    const bundleSize = statSync(bundleFile).size / (1024 * 1024);
    expect(bundleSize).toBeLessThan(50);

    // Run the bundled app, verifying it creates its system database and runs a workflow to completion
    let output = '';
    const { code, signal } = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
      (resolve, reject) => {
        const child = spawn('node', [bundleFile], {
          cwd: bundlerTestDir,
          stdio: 'pipe',
          env: {
            ...process.env,
            DBOS_SYSTEM_DATABASE_URL: `postgresql://postgres:${dbPassword}@localhost:5432/${sysDbName}`,
          },
        });

        child.stdout.on('data', (data: Buffer) => {
          output += data.toString();
        });

        child.stderr.on('data', (data: Buffer) => {
          output += data.toString();
        });

        // Kill a hung app so the test reports it rather than timing out.
        const timer = setTimeout(() => child.kill('SIGTERM'), 60000);

        child.on('close', (exitCode, exitSignal) => {
          clearTimeout(timer);
          resolve({ code: exitCode, signal: exitSignal });
        });

        child.on('error', (error) => {
          clearTimeout(timer);
          reject(error);
        });
      },
    );

    expect(output).toContain('DBOS bundler test completed successfully!');
    expect({ code, signal }).toEqual({ code: 0, signal: null });
  }, 300000);
});
