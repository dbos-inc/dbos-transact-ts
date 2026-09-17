import { inspect } from 'node:util';
import { AddressInfo } from 'node:net';
import { WebSocket, WebSocketServer } from 'ws';
import { DBOS } from '../src';
import type { DBOSLaunchOptions } from '../src/dbos';
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

/** Launches DBOS connected to a real websocket server standing in for Conductor's side of the connection. */
async function launchWithConductorStandIn(options: DBOSLaunchOptions = {}) {
  const received: string[] = [];
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise<void>((resolve) => server.once('listening', resolve));
  const connected = new Promise<WebSocket>((resolve) => server.once('connection', resolve));

  const { port } = server.address() as AddressInfo;
  await DBOS.launch({ conductorKey: 'test-key', conductorURL: `ws://127.0.0.1:${port}`, ...options });

  const conductorSocket = await connected;
  conductorSocket.on('message', (data: Buffer) => received.push(data.toString('utf-8')));

  /** The responses this stand-in has received for one request ID. */
  const answersTo = (requestID: string) =>
    received.map((m) => JSON.parse(m) as protocol.BaseResponse).filter((m) => m.request_id === requestID);

  const shutdown = async () => {
    // shutdown() returns once it has asked the socket to close, but ws finishes the handshake
    // afterwards, and its close handler logs. Wait for it, or that log lands after teardown.
    const socket = DBOSExecutor.globalInstance?.conductor?.websocket;
    const closed =
      socket === undefined || socket.readyState === WebSocket.CLOSED
        ? Promise.resolve()
        : new Promise<void>((resolve) => socket.once('close', () => resolve()));
    await DBOS.shutdown();
    await closed;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  };

  return { conductorSocket, received, answersTo, shutdown };
}

// Command dispatch and request parsing are properties of the live connection rather than of a
// wire object, so unlike the suite above this one stands up a real websocket for Conductor's side of it.
describe('conductor-live-connection', () => {
  let config: DBOSConfig;
  let conductorSocket: WebSocket;
  let answersTo: (requestID: string) => protocol.BaseResponse[];
  let shutdown: () => Promise<void>;

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
    ({ conductorSocket, answersTo, shutdown } = await launchWithConductorStandIn());
  });

  afterEach(async () => {
    await shutdown();
  });

  test('answers a retention request at once and still collects', async () => {
    await expect(retentionWorkflow(1)).resolves.toBe(1);
    await expect(DBOS.listWorkflows({})).resolves.toHaveLength(1);

    conductorSocket.send(
      JSON.stringify({
        type: protocol.MessageType.RETENTION,
        request_id: 'retention-round-1',
        body: {
          gc_cutoff_epoch_ms: Date.now() + 60_000,
          // Zero rather than absent: a Conductor that clears the batch size must fall back to
          // the default rather than failing the round, as Python does. Null takes the same path.
          gc_batch_size: 0,
        },
      } satisfies protocol.RetentionRequest),
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
      JSON.stringify({
        type: protocol.MessageType.LIST_WORKFLOWS,
        request_id: 'after-retention',
        body: { workflow_uuids: ['no-such-workflow'], sort_desc: false },
      } satisfies protocol.ListWorkflowsRequest),
    );
    await retryUntilSuccess(() => {
      const answers = answersTo('after-retention');
      expect(answers).toHaveLength(1);
      expect(answers[0].error_message).toBeUndefined();
    });
  });

  test('skips a round while the previous one on this executor is still running', async () => {
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const logger = DBOSExecutor.globalInstance!.logger;
    await expect(retentionWorkflow(1)).resolves.toBe(1);

    // The first round parks on its lock acquisition until the test lets it go, so the second
    // request is guaranteed to arrive while it is still running. It then resolves to "not
    // acquired", so that round ends without collecting either.
    let openGate!: (lock: undefined) => void;
    const gate = new Promise<undefined>((resolve) => (openGate = resolve));
    const acquire = jest.spyOn(sysdb, 'acquireRetentionLock').mockImplementationOnce(() => gate);
    const warn = jest.spyOn(logger, 'warn');
    try {
      const request = (requestID: string) =>
        conductorSocket.send(
          JSON.stringify({
            type: protocol.MessageType.RETENTION,
            request_id: requestID,
            body: { gc_cutoff_epoch_ms: Date.now() + 60_000 },
          } satisfies protocol.RetentionRequest),
        );
      request('round-a');
      await retryUntilSuccess(() => {
        expect(acquire).toHaveBeenCalledTimes(1);
      });

      // Skipped by the executor, before it ever reaches the database.
      request('round-b');
      await retryUntilSuccess(() => {
        expect(warn).toHaveBeenCalledWith('Skipping retention: the previous round on this executor is still running.');
      });
      expect(acquire).toHaveBeenCalledTimes(1);

      openGate(undefined);
      // Both requests are answered, and neither collected anything.
      await retryUntilSuccess(() => {
        expect(answersTo('round-a')).toHaveLength(1);
        expect(answersTo('round-b')).toHaveLength(1);
      });
      await expect(DBOS.listWorkflows({})).resolves.toHaveLength(1);
    } finally {
      openGate(undefined);
      warn.mockRestore();
      acquire.mockRestore();
    }
  });

  test('filters workflow aggregates by the workflow IDs and user Conductor sends', async () => {
    const workflowIDs = ['agg-filter-a', 'agg-filter-b'];
    for (const [i, workflowID] of workflowIDs.entries()) {
      const handle = await DBOS.startWorkflow(retentionWorkflow, { workflowID, authenticatedUser: `user-${i}` })(i);
      await handle.getResult();
    }

    // Field names exactly as Conductor's server sends them.
    const aggregate = (requestID: string, body: protocol.GetWorkflowAggregatesBody) =>
      conductorSocket.send(
        JSON.stringify({
          type: protocol.MessageType.GET_WORKFLOW_AGGREGATES,
          request_id: requestID,
          body: { group_by_status: true, select_count: true, ...body },
        } satisfies protocol.GetWorkflowAggregatesRequest),
      );
    aggregate('by-workflow-id', { workflow_ids: [workflowIDs[0]] });
    aggregate('by-user', { user: ['user-1'] });
    aggregate('unfiltered', {});

    await retryUntilSuccess(() => {
      const expected = { 'by-workflow-id': 1, 'by-user': 1, unfiltered: 2 };
      for (const [requestID, count] of Object.entries(expected)) {
        const answers = answersTo(requestID) as protocol.GetWorkflowAggregatesResponse[];
        expect(answers).toHaveLength(1);
        expect(answers[0].error_message).toBeUndefined();
        expect(answers[0].output).toEqual([expect.objectContaining({ count })]);
      }
    });
  });
});

// Metadata-only mode is enforced by the executor, so it must hold even when Conductor asks for data.
describe('conductor-metadata-only-mode', () => {
  let config: DBOSConfig;
  let shutdown: (() => Promise<void>) | undefined;

  const metadataOnlyStep = DBOS.registerStep(
    (x: string) => {
      return Promise.resolve(`${x}-output`);
    },
    { name: 'metadataOnlyStep' },
  );

  const metadataOnlyWorkflow = DBOS.registerWorkflow(
    async (x: string) => {
      await DBOS.setEvent('event', x);
      await DBOS.writeStream('stream', x);
      return await metadataOnlyStep(x);
    },
    { name: 'metadataOnlyWorkflow' },
  );

  const metadataOnlyFailingWorkflow = DBOS.registerWorkflow(
    (x: string) => {
      return Promise.reject(new Error(x));
    },
    { name: 'metadataOnlyFailingWorkflow' },
  );

  const metadataOnlyScheduledWorkflow = DBOS.registerWorkflow(
    (_scheduledAt: Date, _context: unknown) => {
      return Promise.resolve();
    },
    { name: 'metadataOnlyScheduledWorkflow' },
  );

  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await setUpDBOSTestSysDb(config);
  });

  afterEach(async () => {
    await shutdown?.();
    shutdown = undefined;
  });

  test.each([true, false])(
    'sends workflow data only outside metadata-only mode (metadata-only: %s)',
    async (metadataOnly) => {
      const standIn = await launchWithConductorStandIn({ conductorMetadataOnlyMode: metadataOnly });
      shutdown = standIn.shutdown;
      const sendsData = !metadataOnly;

      /** Sends a command as Conductor would and returns the executor's one response to it. */
      const roundTrip = async <Resp extends protocol.BaseResponse>(
        request: protocol.BaseMessage & Record<string, unknown>,
      ) => {
        standIn.conductorSocket.send(JSON.stringify(request));
        await retryUntilSuccess(() => {
          expect(standIn.answersTo(request.request_id)).toHaveLength(1);
        });
        return standIn.answersTo(request.request_id)[0] as Resp;
      };

      const handle = await DBOS.startWorkflow(metadataOnlyWorkflow)('secret');
      await expect(handle.getResult()).resolves.toBe('secret-output');
      // Delayed so it is still on the queue when Conductor lists queued workflows.
      const queue = await DBOS.registerQueue('metadata-only-queue');
      const delayedHandle = await DBOS.startWorkflow(metadataOnlyWorkflow, {
        queueName: queue.name,
        enqueueOptions: { delaySeconds: 3600 },
      })('secret');
      const failingHandle = await DBOS.startWorkflow(metadataOnlyFailingWorkflow)('secret');
      await expect(failingHandle.getResult()).rejects.toThrow('secret');
      await DBOS.send(handle.workflowID, 'secret', 'topic');
      await DBOS.createSchedule({
        scheduleName: 'metadata-only-schedule',
        workflowFn: metadataOnlyScheduledWorkflow,
        schedule: '0 0 1 1 *',
        context: 'secret',
      });

      // Conductor explicitly asks for data; metadata-only mode must override it.
      const listed = await roundTrip<protocol.ListWorkflowsResponse>({
        type: protocol.MessageType.LIST_WORKFLOWS,
        request_id: 'list-workflows',
        body: {
          workflow_uuids: [handle.workflowID, failingHandle.workflowID],
          load_input: true,
          load_output: true,
          sort_desc: false,
        },
      } satisfies protocol.ListWorkflowsRequest);
      expect(listed.error_message).toBeUndefined();
      const byID = new Map(listed.output.map((wf) => [wf.WorkflowUUID, wf]));
      expect(byID.get(handle.workflowID)?.Status).toBe('SUCCESS');
      expect(byID.get(failingHandle.workflowID)?.Status).toBe('ERROR');
      expect([...byID.values()].map((wf) => wf.Input !== undefined)).toEqual([sendsData, sendsData]);
      expect(byID.get(handle.workflowID)?.Output !== undefined).toBe(sendsData);
      expect(byID.get(failingHandle.workflowID)?.Error !== undefined).toBe(sendsData);

      const queued = await roundTrip<protocol.ListQueuedWorkflowsResponse>({
        type: protocol.MessageType.LIST_QUEUED_WORKFLOWS,
        request_id: 'list-queued-workflows',
        body: { load_input: true, load_output: true, sort_desc: false },
      } satisfies protocol.ListQueuedWorkflowsRequest);
      expect(queued.output.map((wf) => wf.WorkflowUUID)).toEqual([delayedHandle.workflowID]);
      expect(queued.output[0].Input !== undefined).toBe(sendsData);

      for (const [workflowID, field] of [
        [handle.workflowID, 'Output'],
        [failingHandle.workflowID, 'Error'],
      ] as const) {
        const got = await roundTrip<protocol.GetWorkflowResponse>({
          type: protocol.MessageType.GET_WORKFLOW,
          request_id: `get-workflow-${workflowID}`,
          workflow_id: workflowID,
          load_input: true,
          load_output: true,
        } satisfies protocol.GetWorkflowRequest);
        expect(got.output?.WorkflowUUID).toBe(workflowID);
        expect(got.output?.Input !== undefined).toBe(sendsData);
        expect(got.output?.[field] !== undefined).toBe(sendsData);
      }

      const steps = await roundTrip<protocol.ListStepsResponse>({
        type: protocol.MessageType.LIST_STEPS,
        request_id: 'list-steps',
        workflow_id: handle.workflowID,
        load_output: true,
      } satisfies protocol.ListStepsRequest);
      expect(steps.output?.map((s) => s.function_name)).toContain('metadataOnlyStep');
      expect(steps.output?.some((s) => s.output !== undefined)).toBe(sendsData);

      const schedules = await roundTrip<protocol.ListSchedulesResponse>({
        type: protocol.MessageType.LIST_SCHEDULES,
        request_id: 'list-schedules',
        body: { load_context: true },
      } satisfies protocol.ListSchedulesRequest);
      expect(schedules.output.map((s) => s.schedule_name)).toEqual(['metadata-only-schedule']);
      expect(schedules.output[0].context !== undefined).toBe(sendsData);
      const schedule = await roundTrip<protocol.GetScheduleResponse>({
        type: protocol.MessageType.GET_SCHEDULE,
        request_id: 'get-schedule',
        schedule_name: 'metadata-only-schedule',
        load_context: true,
      } satisfies protocol.GetScheduleRequest);
      expect(schedule.output?.schedule_name).toBe('metadata-only-schedule');
      expect(schedule.output?.context !== undefined).toBe(sendsData);

      // Commands that only move data are refused outright.
      const dataRequests = [
        {
          type: protocol.MessageType.GET_WORKFLOW_EVENTS,
          request_id: 'events',
          workflow_id: handle.workflowID,
        } satisfies protocol.GetWorkflowEventsRequest,
        {
          type: protocol.MessageType.GET_WORKFLOW_NOTIFICATIONS,
          request_id: 'notifications',
          workflow_id: handle.workflowID,
        } satisfies protocol.GetWorkflowNotificationsRequest,
        {
          type: protocol.MessageType.GET_WORKFLOW_STREAMS,
          request_id: 'streams',
          workflow_id: handle.workflowID,
        } satisfies protocol.GetWorkflowStreamsRequest,
        {
          type: protocol.MessageType.EXPORT_WORKFLOW,
          request_id: 'export',
          workflow_id: handle.workflowID,
          export_children: false,
        } satisfies protocol.ExportWorkflowRequest,
        {
          type: protocol.MessageType.IMPORT_WORKFLOW,
          request_id: 'import',
          serialized_workflow: 'not-a-workflow',
        } satisfies protocol.ImportWorkflowRequest,
      ];
      for (const request of dataRequests) {
        const answer = await roundTrip(request);
        expect(answer.error_message === `${request.type} is not allowed in conductor metadata-only mode`).toBe(
          metadataOnly,
        );
      }
      const events = standIn.answersTo('events')[0] as protocol.GetWorkflowEventsResponse;
      expect(events.events).toEqual(metadataOnly ? undefined : [{ key: 'event', value: "'secret'" }]);

      // Nothing the executor sent carries the workflow data, whatever Conductor asked for.
      expect(standIn.received.some((m) => m.includes('secret'))).toBe(sendsData);
    },
  );
});
