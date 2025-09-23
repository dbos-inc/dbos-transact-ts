import { execSync, spawn } from 'child_process';
import { existsSync, readFileSync, statSync } from 'fs';
import path from 'path';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { DBOS } from '../src';
import { Client } from 'pg';

describe('DBOS Bundler Tests', () => {
  const bundlerTestDir = path.join(__dirname, 'bundler-test');
  const bundleFile = path.join(bundlerTestDir, 'dist', 'bundle.js');

  beforeAll(async () => {
    execSync('npm run build', { cwd: path.join(__dirname, '..'), stdio: 'inherit' });
    const config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
  }, 120000);

  afterAll(async () => {
    await DBOS.shutdown();
  }, 30000);

  test('test app with bundler', async () => {
    const dbPassword = process.env.PGPASSWORD || 'dbos';
    const testDbName = 'bundler_test';
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

    // Run the bundled app again, verify it works (historically, issues w/ creating sys DB prevent this)
    let stdout = '';
    let stderr = '';

    const runPromise2 = new Promise<number>((resolve, reject) => {
      const child = spawn('node', [bundleFile], {
        cwd: bundlerTestDir,
        stdio: 'pipe',
        env: {
          ...process.env,
          PGPASSWORD: dbPassword,
          DBOS_DATABASE_URL: `postgresql://postgres:${dbPassword}@localhost:5432/${testDbName}`,
        },
      });

      child.stdout.on('data', (data: Buffer) => {
        stdout += data.toString();
      });

      child.stderr.on('data', (data: Buffer) => {
        stderr += data.toString();
      });

      child.on('close', (code) => {
        resolve(code || 0);
      });

      child.on('error', (error) => {
        reject(error);
      });

      setTimeout(() => {
        if (!child.killed) {
          child.kill('SIGTERM');
        }
      }, 15000);
    });

    const exitCode2 = await runPromise2;
    console.log(stdout + stderr);
    expect(exitCode2).toBe(0);
  }, 300000);
});
