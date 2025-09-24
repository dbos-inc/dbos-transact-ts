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
    let foundStep = false;
    let foundWf = false;
    const lines = result.stdout.split('\n');
    for (const l of lines) {
      if (
        /Info: WFID should be logged/.test(l) &&
        /"operationType":"workflow"/.test(l) &&
        /"operationName":"loggingWorkflow"/ &&
        /"operationUUID":"loggerWorkflowId"/.test(l) &&
        /"authenticatedUser":""/.test(l) &&
        /"authenticatedRoles":\[\]/.test(l) &&
        /"assumedRole":""/.test(l)
      ) {
        foundWf = true;
      }

      if (
        /Info: Step should be logged/.test(l) &&
        /"operationType":"step"/.test(l) &&
        /"operationName":"loggingStep"/ &&
        /"operationUUID":"loggerWorkflowId"/.test(l) &&
        /"authenticatedUser":""/.test(l) &&
        /"authenticatedRoles":\[\]/.test(l) &&
        /"assumedRole":""/.test(l)
      ) {
        foundStep = true;
      }
    }

    if (!foundWf || !foundStep) {
      console.warn(
        `
*** This test is about to fail, something was not found. ***
  Found workflow: ${foundWf}.
  Found step: ${foundStep}.
The log was:\n${result.stdout}
`,
      );
    }

    expect(foundWf).toBeTruthy();
    expect(foundStep).toBeTruthy();

    return Promise.resolve();
  });
});
