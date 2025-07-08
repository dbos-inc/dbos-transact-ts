import { execSync, spawn } from 'child_process';
import { existsSync, readFileSync, statSync } from 'fs';
import path from 'path';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { DBOS } from '../src';

describe('DBOS Bundler Tests', () => {
  const bundlerTestDir = path.join(__dirname, 'bundler-test');
  const bundleFile = path.join(bundlerTestDir, 'dist', 'bundle.js');

  beforeAll(async () => {
    // Ensure the main project is built
    console.log('Building main DBOS project...');
    execSync('npm run build', { cwd: path.join(__dirname, '..'), stdio: 'inherit' });

    // Set up test database
    const config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  afterAll(async () => {
    // Clean up any test processes
    await DBOS.shutdown();
  });

  test('should bundle DBOS app and show migration warning', async () => {
    const dbPassword = process.env.PGPASSWORD || 'dbos';
    const testDbName = 'bundler_test';

    console.log('=== Installing dependencies and bundling ===');

    // Install dependencies
    execSync('npm install', {
      cwd: bundlerTestDir,
      stdio: 'inherit',
      timeout: 120000,
    });

    // Run webpack build
    execSync('npm run build', {
      cwd: bundlerTestDir,
      stdio: 'inherit',
      timeout: 60000,
    });

    // Verify bundle was created
    expect(existsSync(bundleFile)).toBe(true);
    const bundleContent = readFileSync(bundleFile, 'utf8');
    expect(bundleContent).toContain('DBOS');
    expect(bundleContent).toContain('testWorkflow');

    const bundleSize = statSync(bundleFile).size / (1024 * 1024);
    console.log(`Bundle size: ${bundleSize.toFixed(2)} MB`);
    expect(bundleSize).toBeLessThan(50);

    console.log('=== Running bundled app (should show migration warning) ===');

    // Run the bundled app - should show migration warning
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

      // Kill after 15 seconds
      setTimeout(() => {
        if (!child.killed) {
          child.kill('SIGTERM');
        }
      }, 15000);
    });

    await runPromise;

    console.log('Bundled app output:', stdout);
    console.log('Bundled app errors:', stderr);

    // Verify the migration warning is shown
    const output = stdout + stderr;
    expect(output).toContain('migration files not found');
    expect(output).toContain('npx dbos migrate');

    console.log('✅ Successfully demonstrated bundler compatibility with proper migration warning');
  }, 300000); // 5 minute timeout
});
