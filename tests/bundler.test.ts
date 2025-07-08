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

  test('should install dependencies, bundle successfully, and fail execution with clear error', async () => {
    console.log('Installing bundler test dependencies...');

    // Install dependencies
    expect(() => {
      execSync('npm install', {
        cwd: bundlerTestDir,
        stdio: 'inherit',
        timeout: 120000, // 2 minutes timeout
      });
    }).not.toThrow();

    console.log('Running webpack build...');

    // Run webpack build
    expect(() => {
      execSync('npm run build', {
        cwd: bundlerTestDir,
        stdio: 'inherit',
        timeout: 60000, // 1 minute timeout
      });
    }).not.toThrow();

    // Verify bundle file exists and has content
    expect(existsSync(bundleFile)).toBe(true);

    const bundleContent = readFileSync(bundleFile, 'utf8');
    expect(bundleContent.length).toBeGreaterThan(0);

    // Check that it contains expected DBOS references
    expect(bundleContent).toContain('DBOS');
    expect(bundleContent).toContain('workflow');
    expect(bundleContent).toContain('step');
    expect(bundleContent).toContain('BundlerTestApp');
    expect(bundleContent).toContain('testWorkflow');
    expect(bundleContent).toContain('testStep');
    expect(bundleContent).toContain('launch');
    expect(bundleContent).toContain('shutdown');

    // Check bundle size is reasonable
    const stats = statSync(bundleFile);
    const sizeInMB = stats.size / (1024 * 1024);
    console.log(`Bundle size: ${sizeInMB.toFixed(2)} MB`);
    expect(sizeInMB).toBeLessThan(50);

    // Test bundle execution (this should fail with current DBOS limitations)
    let stdout = '';
    let stderr = '';

    const promise = new Promise<number>((resolve, reject) => {
      const child = spawn('node', [bundleFile], {
        cwd: bundlerTestDir,
        stdio: 'pipe',
        env: {
          ...process.env,
          DBOS_DATABASE_URL: process.env.DBOS_DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/dbostest',
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

      // Kill process after 30 seconds if it doesn't exit
      setTimeout(() => {
        if (!child.killed) {
          child.kill('SIGTERM');
          reject(new Error('Test timed out after 30 seconds'));
        }
      }, 30000);
    });

    const exitCode = await promise;

    console.log('Bundled app stdout:', stdout);
    console.log('Bundled app stderr:', stderr);

    // Expect the bundled app to fail with a clear error
    expect(exitCode).not.toBe(0);

    // Verify the error is related to dynamic module loading
    expect(stderr).toContain('Cannot find module');

    // Document the specific limitation
    console.log('\n=== BUNDLING LIMITATION DETECTED ===');
    console.log('DBOS currently cannot be bundled due to:');
    console.log('1. Dynamic module resolution in the framework');
    console.log('2. Runtime file system access for configuration');
    console.log('3. Database migration files that need to be accessible');
    console.log('=====================================\n');
  }, 300000); // 5 minute timeout for entire test
});
