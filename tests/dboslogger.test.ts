import { spawnSync } from 'child_process';

describe('dbos-logger', () => {
  test('logFromWf', async () => {
    // Run the TypeScript test script under ts-node
    const result = spawnSync('npx', ['ts-node', './tests/logtodboslogger.ts'], {
      encoding: 'utf-8',
      cwd: process.cwd(),
      env: { ...process.env },
      stdio: ['inherit', 'pipe', 'pipe'], // Capture stdout and stderr
    });

    //s Uncomment to see what happened above...
    console.log('STDOUT:', result.stdout);
    //console.error('STDERR:', result.stderr);

    // Check if the expected error appears in stderr
    //expect(result.stderr).toContain('DBOSConflictingRegistrationError');
    return Promise.resolve();
  });
});
