import { execSync, spawn } from 'child_process';
import { existsSync, readFileSync, statSync } from 'fs';
import path from 'path';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { DBOS } from '../src';
import { Client } from 'pg';

describe('DBOS Bundler Tests', () => {
  const bundlerTestDir = path.join(__dirname, 'bundler-test');
  const bundleFile = path.join(bundlerTestDir, 'dist', 'bundle.js');

  beforeAll(async () => {
    execSync('npm run build', { cwd: path.join(__dirname, '..'), stdio: 'inherit' });
    const config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
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

    // Run the bundled app, validate it fails because the system database does not exist
    let stdout = '';
    let stderr = '';

    const runPromise = new Promise<number>((resolve, reject) => {
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

    const exitCode1 = await runPromise;
    const output = stdout + stderr;
    console.log(output);
    expect(output).toContain('migration files not found');
    expect(exitCode1).not.toBe(0);

    // Create the system database externally
    try {
      execSync('npx dbos migrate', {
        cwd: bundlerTestDir,
        stdio: 'inherit',
        timeout: 60000,
        env: {
          ...process.env,
          PGPASSWORD: dbPassword,
          DBOS_DATABASE_URL: `postgresql://postgres:${dbPassword}@localhost:5432/${testDbName}`,
        },
      });
    } catch (error) {
      throw new Error(`Migration failed`);
    }

    // Run the bundled app again, verify it works
    stdout = '';
    stderr = '';

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
