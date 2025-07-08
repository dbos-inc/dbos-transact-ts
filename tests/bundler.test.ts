import { execSync, spawn } from 'child_process';
import { existsSync, readFileSync, statSync } from 'fs';
import path from 'path';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { DBOS } from '../src';

describe('DBOS Bundler Tests', () => {
  const bundlerTestDir = path.join(__dirname, 'bundler-test');
  const bundleFile = path.join(bundlerTestDir, 'dist', 'bundle.js');

  beforeAll(async () => {
    execSync('npm run build', { cwd: path.join(__dirname, '..'), stdio: 'pipe' });
    const config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  }, 120000);

  afterAll(async () => {
    await DBOS.shutdown();
  }, 30000);

  test('should bundle DBOS app and show migration warning', async () => {
    const dbPassword = process.env.PGPASSWORD || 'dbos';
    const testDbName = 'bundler_test';

    execSync('npm install', {
      cwd: bundlerTestDir,
      stdio: 'pipe',
      timeout: 120000,
    });

    execSync('npm run build', {
      cwd: bundlerTestDir,
      stdio: 'pipe',
      timeout: 60000,
    });

    expect(existsSync(bundleFile)).toBe(true);
    const bundleContent = readFileSync(bundleFile, 'utf8');
    expect(bundleContent).toContain('DBOS');
    expect(bundleContent).toContain('testWorkflow');

    const bundleSize = statSync(bundleFile).size / (1024 * 1024);
    expect(bundleSize).toBeLessThan(50);

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

    await runPromise;

    const output = stdout + stderr;
    expect(output).toContain('migration files not found');
    expect(output).toContain('npx dbos migrate');
  }, 300000);
});
