import { inspect } from 'node:util';
import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import * as protocol from '../src/conductor/protocol';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

// Regression tests for the conductor protocol's human-readable string
// representations of workflow/step input and output.
//
// These exercise the exact code that feeds Conductor (protocol.WorkflowsOutput /
// protocol.WorkflowSteps) WITHOUT needing a live Conductor connection: Conductor
// is only the consumer of the wire object; all the rendering happens on the DBOS
// side, so we can construct the wire object directly and assert on its fields.
describe('conductor-protocol-string-representations', () => {
  let config: DBOSConfig;

  // Deeply nested (> 2 levels) so inspect's default depth would collapse it.
  const nested = [
    {
      abc: {
        def: { one: 1, two: { three: 3, four: [4, 4, 4] } },
        xyz: { alpha: 'a', beta: { gamma: 'g', delta: ['d1', 'd2'] } },
      },
    },
  ];

  const nestedStep = DBOS.registerStep(
    async () => {
      return await Promise.resolve(nested);
    },
    { name: 'nestedReproStep' },
  );

  const nestedWorkflow = DBOS.registerWorkflow(
    async (_input: unknown) => {
      return await nestedStep();
    },
    { name: 'nestedReproWorkflow' },
  );

  // A value that JSON.stringify cannot serialize, to guard against a regression
  // of #1167 (such workflows must remain viewable in Conductor).
  const exoticStep = DBOS.registerStep(
    async () => {
      return await Promise.resolve({ big: 10n, when: new Date(0), tags: new Set(['x', 'y']) });
    },
    { name: 'exoticStep' },
  );

  const exoticWorkflow = DBOS.registerWorkflow(
    async () => {
      return await exoticStep();
    },
    { name: 'exoticWorkflow' },
  );

  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('nested workflow input/output are not truncated to [Object]', async () => {
    const handle = await DBOS.startWorkflow(nestedWorkflow)(nested);
    await handle.getResult();

    const statuses = await DBOS.listWorkflows({ workflowIDs: [handle.workflowID] });
    expect(statuses).toHaveLength(1);

    // The wire object Conductor receives.
    const wire = new protocol.WorkflowsOutput(statuses[0]);

    // No depth-truncation placeholders (the symptom of #1313).
    expect(wire.Input).not.toContain('[Object]');
    expect(wire.Input).not.toContain('[Array]');
    expect(wire.Output).not.toContain('[Object]');
    expect(wire.Output).not.toContain('[Array]');

    // The deepest leaf values are actually present in the rendered strings.
    // Input is the args array (one arg, `nested`).
    expect(wire.Input).toBe(inspect([nested], { depth: null, maxArrayLength: null, maxStringLength: null }));
    expect(wire.Output).toBe(inspect(nested, { depth: null, maxArrayLength: null, maxStringLength: null }));
    expect(wire.Output).toContain('three');
    expect(wire.Output).toContain('delta');
    expect(wire.Output).toContain("'d2'");
  });

  test('nested step output is not truncated to [Object]', async () => {
    const handle = await DBOS.startWorkflow(nestedWorkflow)(nested);
    await handle.getResult();

    const steps = await DBOS.listWorkflowSteps(handle.workflowID);
    expect(steps).toBeDefined();

    // Find the nested step and assert its rendered output is complete.
    const nestedStepInfo = steps!.find((s) => s.name === 'nestedReproStep');
    expect(nestedStepInfo).toBeDefined();

    const wireStep = new protocol.WorkflowSteps(nestedStepInfo!);
    expect(wireStep.output).toBeDefined();
    expect(wireStep.output).not.toContain('[Object]');
    expect(wireStep.output).not.toContain('[Array]');
    expect(wireStep.output).toContain('three');
    expect(wireStep.output).toContain('delta');
  });

  test('non-JSON-serializable output still renders (no regression of issue 1167)', async () => {
    const handle = await DBOS.startWorkflow(exoticWorkflow)();
    await handle.getResult();

    const statuses = await DBOS.listWorkflows({ workflowIDs: [handle.workflowID] });
    expect(statuses).toHaveLength(1);

    // Building the wire object must not throw on BigInt/Date/Set, and must
    // render their values rather than dropping them.
    const wire = new protocol.WorkflowsOutput(statuses[0]);
    expect(wire.Output).toBeDefined();
    expect(wire.Output).toContain('10n');
    expect(wire.Output).toContain('Set');
  });
});
