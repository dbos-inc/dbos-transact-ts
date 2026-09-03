import { inspect } from 'node:util';
import { AddressInfo } from 'node:net';
import { WebSocket, WebSocketServer } from 'ws';
import { DBOS } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import * as protocol from '../src/conductor/protocol';
import { generateDBOSTestConfig, retryUntilSuccess, setUpDBOSTestSysDb } from './helpers';

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

// A retention round takes minutes, so the conductor runs it off its command loop. That is a
// property of the live connection rather than of a wire object, so unlike the suite above
// this one stands up a real websocket for Conductor's side of it.
describe('conductor-retention-dispatch', () => {
  let config: DBOSConfig;
  let server: WebSocketServer;
  let conductorSocket: WebSocket;
  let received: string[];

  const retentionWorkflow = DBOS.registerWorkflow(
    (x: number) => {
      return Promise.resolve(x);
    },
    { name: 'retentionWorkflow' },
  );

  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await setUpDBOSTestSysDb(config);
    received = [];
    server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
    await new Promise<void>((resolve) => server.once('listening', resolve));
    const connected = new Promise<WebSocket>((resolve) => server.once('connection', resolve));

    const { port } = server.address() as AddressInfo;
    await DBOS.launch({ conductorKey: 'test-key', conductorURL: `ws://127.0.0.1:${port}` });

    conductorSocket = await connected;
    conductorSocket.on('message', (data: Buffer) => received.push(data.toString('utf-8')));
  });

  afterEach(async () => {
    await DBOS.shutdown();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  /** The responses this stand-in has received for one request ID. */
  const answersTo = (requestID: string) =>
    received.map((m) => JSON.parse(m) as protocol.BaseResponse).filter((m) => m.request_id === requestID);

  test('answers a retention request at once and still collects', async () => {
    await expect(retentionWorkflow(1)).resolves.toBe(1);
    await expect(DBOS.listWorkflows({})).resolves.toHaveLength(1);

    conductorSocket.send(
      JSON.stringify(
        new protocol.RetentionRequest('retention-round-1', {
          gc_cutoff_epoch_ms: Date.now() + 60_000,
          // Null rather than absent: a Conductor that clears the batch size must fall back
          // to the default rather than failing the round.
          gc_batch_size: null,
        }),
      ),
    );

    await retryUntilSuccess(() => {
      const answers = answersTo('retention-round-1');
      expect(answers).toHaveLength(1);
      expect((answers[0] as protocol.RetentionResponse).success).toBe(true);
      expect(answers[0].error_message).toBeUndefined();
    });

    // The answer above did not wait for the round, so the collection is what proves it ran.
    await retryUntilSuccess(async () => {
      await expect(DBOS.listWorkflows({})).resolves.toHaveLength(0);
    });

    // The round ran off the command loop, so the loop is still serving commands.
    conductorSocket.send(
      JSON.stringify(
        new protocol.ListWorkflowsRequest('after-retention', {
          workflow_uuids: ['no-such-workflow'],
          sort_desc: false,
        }),
      ),
    );
    await retryUntilSuccess(() => {
      const answers = answersTo('after-retention');
      expect(answers).toHaveLength(1);
      expect(answers[0].error_message).toBeUndefined();
    });
  });

  test('skips a round while the previous one on this executor is still running', async () => {
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    await expect(retentionWorkflow(1)).resolves.toBe(1);

    // Held from the test, so a round that reaches the system database cannot get past the
    // lock and collect. Both requests must still be answered.
    const held = await sysdb.acquireRetentionLock();
    try {
      for (const requestID of ['round-a', 'round-b']) {
        conductorSocket.send(
          JSON.stringify(new protocol.RetentionRequest(requestID, { gc_cutoff_epoch_ms: Date.now() + 60_000 })),
        );
      }
      await retryUntilSuccess(() => {
        expect(answersTo('round-a')).toHaveLength(1);
        expect(answersTo('round-b')).toHaveLength(1);
      });
      // Neither round got past the lock, so the workflow is still there.
      await expect(DBOS.listWorkflows({})).resolves.toHaveLength(1);
    } finally {
      await held?.release();
    }
  });
});
