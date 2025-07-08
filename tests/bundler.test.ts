import { execSync, spawn } from 'child_process';
import { existsSync } from 'fs';
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

  describe('Webpack Bundling', () => {
    test('should install dependencies and bundle successfully', async () => {
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

      // Verify bundle file exists
      expect(existsSync(bundleFile)).toBe(true);
    }, 300000); // 5 minute timeout for entire test

    test('should create a valid bundle file', () => {
      expect(existsSync(bundleFile)).toBe(true);

      // Check that bundle file is not empty
      const fs = require('fs');
      const bundleContent = fs.readFileSync(bundleFile, 'utf8');
      expect(bundleContent.length).toBeGreaterThan(0);

      // Check that it contains expected DBOS references
      expect(bundleContent).toContain('DBOS');
      expect(bundleContent).toContain('workflow');
      expect(bundleContent).toContain('step');
    });
  });

  describe('Bundled App Execution', () => {
    test('should fail with clear error demonstrating current bundling limitations', async () => {
      // This test documents the current limitation where DBOS cannot be bundled
      // due to dynamic module resolution requirements

      let stdout = '';
      let stderr = '';

      const promise = new Promise<number>((resolve, reject) => {
        const child = spawn('node', [bundleFile], {
          cwd: bundlerTestDir,
          stdio: 'pipe',
          env: {
            ...process.env,
            DBOS_DATABASE_URL:
              process.env.DBOS_DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/dbostest',
          },
        });

        child.stdout.on('data', (data) => {
          stdout += data.toString();
        });

        child.stderr.on('data', (data) => {
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
    }, 60000); // 1 minute timeout
  });

  describe('Bundle Analysis', () => {
    test('should have reasonable bundle size', () => {
      const fs = require('fs');
      const stats = fs.statSync(bundleFile);
      const sizeInMB = stats.size / (1024 * 1024);

      console.log(`Bundle size: ${sizeInMB.toFixed(2)} MB`);

      // Bundle should be less than 50MB (reasonable for a Node.js app)
      expect(sizeInMB).toBeLessThan(50);
    });

    test('should contain core DBOS functionality', () => {
      const fs = require('fs');
      const bundleContent = fs.readFileSync(bundleFile, 'utf8');

      // Check for core DBOS components
      expect(bundleContent).toContain('BundlerTestApp');
      expect(bundleContent).toContain('testWorkflow');
      expect(bundleContent).toContain('testStep');

      // Check for DBOS framework presence
      expect(bundleContent).toContain('DBOS');
      expect(bundleContent).toContain('launch');
      expect(bundleContent).toContain('shutdown');
    });
  });
});
