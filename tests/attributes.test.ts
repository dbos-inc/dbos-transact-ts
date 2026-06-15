import { randomUUID } from 'crypto';
import { DBOS, DBOSClient, Debouncer, WorkflowHandle } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { DEBOUNCER_WORKLOW_NAME } from '../src/utils';
import * as protocol from '../src/conductor/protocol';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, Event } from './helpers';

// Custom workflow attributes: a JSON-serializable Record attached to a workflow at creation,
// stored as GIN-indexed JSONB and searchable via the `attributes` containment filter.
describe('workflow-attributes', () => {
  let config: DBOSConfig;
  let systemDatabaseUrl: string;

  beforeAll(() => {
    config = generateDBOSTestConfig();
    systemDatabaseUrl = config.systemDatabaseUrl!;
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();
    await DBOS.registerQueue(attrQueue.name, { onConflict: 'always_update' });
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  const attrQueue = { name: 'attr_test_queue' };

  // Lets a blocking workflow signal that it has started and wait for release, reset per test.
  const sync: { start?: Event; block?: Event } = {};

  const noopWorkflow = DBOS.registerWorkflow(
    async () => {
      return Promise.resolve();
    },
    { name: 'attrNoop' },
  );

  const childWorkflow = DBOS.registerWorkflow(
    async () => {
      return Promise.resolve(DBOS.workflowID!);
    },
    { name: 'attrChild' },
  );

  const parentWorkflow = DBOS.registerWorkflow(
    async () => {
      return await childWorkflow();
    },
    { name: 'attrParent' },
  );

  const echoWorkflow = DBOS.registerWorkflow(
    async (x: number) => {
      return Promise.resolve(x);
    },
    { name: 'attrEcho' },
  );

  const blockingWorkflow = DBOS.registerWorkflow(
    async () => {
      sync.start?.set();
      await sync.block?.wait();
    },
    { name: 'attrBlocking' },
  );

  const matchedIds = async (input: Parameters<typeof DBOS.listWorkflows>[0]): Promise<Set<string>> => {
    return new Set((await DBOS.listWorkflows(input)).map((s) => s.workflowID));
  };

  test('records attributes on direct invocation without leaking to children', async () => {
    const wfid = randomUUID();
    const attributes = { customer: 'acme', tier: 3 };
    const childId = await DBOS.withWorkflowAttributes(attributes, () =>
      DBOS.withNextWorkflowID(wfid, () => parentWorkflow()),
    );

    const status = (await DBOS.listWorkflows({ workflowIDs: [wfid] }))[0];
    expect(status.attributes).toEqual(attributes);

    // Child workflows do not inherit their parent's attributes
    const childStatus = (await DBOS.listWorkflows({ workflowIDs: [childId] }))[0];
    expect(childStatus.attributes).toBeUndefined();

    // Workflows started outside the block have no attributes
    const wfidNoAttrs = randomUUID();
    await DBOS.withNextWorkflowID(wfidNoAttrs, () => parentWorkflow());
    expect((await DBOS.listWorkflows({ workflowIDs: [wfidNoAttrs] }))[0].attributes).toBeUndefined();
  });

  test('nested blocks override and restore attributes', async () => {
    let innerHandle: WorkflowHandle<void>;
    let outerHandle: WorkflowHandle<void>;
    await DBOS.withWorkflowAttributes({ region: 'us-east-1' }, async () => {
      innerHandle = await DBOS.withWorkflowAttributes({ region: 'eu-west-1' }, () =>
        DBOS.startWorkflow(noopWorkflow)(),
      );
      outerHandle = await DBOS.startWorkflow(noopWorkflow)();
    });

    await innerHandle!.getResult();
    await outerHandle!.getResult();
    expect((await innerHandle!.getStatus())?.attributes).toEqual({ region: 'eu-west-1' });
    expect((await outerHandle!.getStatus())?.attributes).toEqual({ region: 'us-east-1' });
  });

  test('records attributes on enqueue', async () => {
    const handle = await DBOS.withWorkflowAttributes({ source: 'queue' }, () =>
      DBOS.startWorkflow(echoWorkflow, { queueName: attrQueue.name })(5),
    );
    expect(await handle.getResult()).toBe(5);
    expect((await handle.getStatus())?.attributes).toEqual({ source: 'queue' });
  });

  test('start params override surrounding context attributes', async () => {
    const handle = await DBOS.withWorkflowAttributes({ from: 'context' }, () =>
      DBOS.startWorkflow(noopWorkflow, { workflowAttributes: { from: 'params' } })(),
    );
    await handle.getResult();
    expect((await handle.getStatus())?.attributes).toEqual({ from: 'params' });
  });

  test('forked workflows inherit attributes', async () => {
    const wfid = randomUUID();
    const attributes = { customer: 'acme' };
    await DBOS.withWorkflowAttributes(attributes, () => DBOS.withNextWorkflowID(wfid, () => noopWorkflow()));

    const forkedHandle = await DBOS.forkWorkflow<void>(wfid, 1);
    await forkedHandle.getResult();
    expect((await forkedHandle.getStatus())?.attributes).toEqual(attributes);
  });

  test('client enqueue records attributes', async () => {
    const client = await DBOSClient.create({ systemDatabaseUrl });
    try {
      // Enqueue to a queue nothing consumes; the workflow stays ENQUEUED, which is
      // enough to check the attributes recorded at creation.
      const handle = await client.enqueue<typeof echoWorkflow>(
        {
          queueName: 'unconsumed_queue',
          workflowName: 'clientWorkflow',
          attributes: { source: 'client' },
        },
        1,
      );
      expect((await handle.getStatus())?.attributes).toEqual({ source: 'client' });
    } finally {
      await client.destroy();
    }
  });

  test('filters listWorkflows by attribute containment', async () => {
    const h1 = await DBOS.withWorkflowAttributes({ customer: 'acme', tier: 1, beta: true, note: null }, () =>
      DBOS.startWorkflow(noopWorkflow)(),
    );
    const h2 = await DBOS.withWorkflowAttributes({ customer: 'bigco', tier: 2, meta: { region: 'us-east-1' } }, () =>
      DBOS.startWorkflow(noopWorkflow)(),
    );
    await h1.getResult();
    await h2.getResult();

    // Single key
    expect(await matchedIds({ attributes: { customer: 'acme' } })).toEqual(new Set([h1.workflowID]));
    // Multiple keys AND together
    expect(await matchedIds({ attributes: { customer: 'bigco', tier: 2 } })).toEqual(new Set([h2.workflowID]));
    // Value mismatch on one key matches nothing
    expect(await matchedIds({ attributes: { customer: 'acme', tier: 2 } })).toEqual(new Set());
    // Non-string value types
    expect(await matchedIds({ attributes: { tier: 1 } })).toEqual(new Set([h1.workflowID]));
    expect(await matchedIds({ attributes: { beta: true } })).toEqual(new Set([h1.workflowID]));
    expect(await matchedIds({ attributes: { note: null } })).toEqual(new Set([h1.workflowID]));
    expect(await matchedIds({ attributes: { meta: { region: 'us-east-1' } } })).toEqual(new Set([h2.workflowID]));
    // Composes with other filters
    expect(await matchedIds({ attributes: { tier: 1 }, workflowIDs: [h2.workflowID] })).toEqual(new Set());
    // Workflows without a matching key never match
    expect(await matchedIds({ attributes: { missing: 'key' } })).toEqual(new Set());
  });

  test('filters listQueuedWorkflows by attributes', async () => {
    sync.start = new Event();
    sync.block = new Event();

    const handle = await DBOS.withWorkflowAttributes({ side: 'queued' }, () =>
      DBOS.startWorkflow(blockingWorkflow, { queueName: attrQueue.name })(),
    );
    await sync.start.wait();

    const statuses = await DBOS.listQueuedWorkflows({ attributes: { side: 'queued' } });
    expect(statuses.map((s) => s.workflowID)).toEqual([handle.workflowID]);
    expect(await DBOS.listQueuedWorkflows({ attributes: { side: 'other' } })).toEqual([]);

    sync.block.set();
    await handle.getResult();
  });

  test('attributes survive the conductor protocol as JSON', async () => {
    const handle = await DBOS.withWorkflowAttributes({ customer: 'acme', tier: 1 }, () =>
      DBOS.startWorkflow(noopWorkflow)(),
    );
    await handle.getResult();

    const statuses = await DBOS.listWorkflows({ attributes: { customer: 'acme' } });
    expect(statuses.map((s) => s.workflowID)).toEqual([handle.workflowID]);

    // Attributes are JSON on the wire and survive response serialization.
    const output = new protocol.WorkflowsOutput(statuses[0]);
    expect(output.Attributes).toBeDefined();
    expect(JSON.parse(output.Attributes!)).toEqual({ customer: 'acme', tier: 1 });

    const response = new protocol.ListWorkflowsResponse('test-request', [output]);
    const serialized = JSON.parse(JSON.stringify(response)) as { output: { Attributes: string }[] };
    expect(JSON.parse(serialized.output[0].Attributes)).toEqual({ customer: 'acme', tier: 1 });
  });

  test('debounced user workflow gets attributes; internal debouncer does not', async () => {
    const debouncer = new Debouncer({ workflow: echoWorkflow });
    const handle = await DBOS.withWorkflowAttributes({ source: 'debouncer' }, () => debouncer.debounce('key', 1000, 5));
    expect(await handle.getResult()).toBe(5);
    expect((await handle.getStatus())?.attributes).toEqual({ source: 'debouncer' });

    // The internal debouncer workflow itself does not get the user's attributes.
    const internalStatuses = await DBOS.listWorkflows({ workflowName: DEBOUNCER_WORKLOW_NAME });
    expect(internalStatuses.length).toBeGreaterThan(0);
    for (const status of internalStatuses) {
      expect(status.attributes).toBeUndefined();
    }
  });
});
