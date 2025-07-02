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

    // Uncomment to see what happened above...
    //console.log('STDOUT:', result.stdout);
    //console.error('STDERR:', result.stderr);

    // Check if the expected error appears in stderr
    let foundTx = false;
    let foundStep = false;
    let foundWf = false;
    const lines = result.stdout.split('\n');
    for (const l of lines) {
      if (/Info: WFID should be logged .*"operationUUID":"loggerWorkflowId"/.test(l)) foundWf = true;
      if (/Info: Step should be logged .*"operationUUID":"loggerWorkflowId"/.test(l)) foundStep = true;
      if (/Info: Transaction should be logged .*"operationUUID":"loggerWorkflowId"/.test(l)) foundTx = true;
    }
    expect(foundWf).toBeTruthy();
    expect(foundStep).toBeTruthy();
    expect(foundTx).toBeTruthy();

    return Promise.resolve();
  });
});
