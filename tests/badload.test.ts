import { spawnSync } from 'child_process';

describe('v2api-temp-wf', () => {
  test('loadCodeTwice', async () => {
    // Run the TypeScript test script under ts-node
    const result = spawnSync('npx', ['ts-node', './tests/codereloader.ts'], {
      encoding: 'utf-8',
      cwd: process.cwd(),
      env: { ...process.env },
      stdio: ['inherit', 'pipe', 'pipe'], // Capture stdout and stderr
    });

    // Uncomment to see what happened above...
    //console.log('STDOUT:', result.stdout);
    //console.error('STDERR:', result.stderr);

    // Check if the expected error appears in stderr
    expect(result.stderr).toContain('DBOSConflictingRegistrationError');
    return Promise.resolve();
  });
});
