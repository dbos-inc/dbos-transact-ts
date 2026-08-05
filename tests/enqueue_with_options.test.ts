import { Client } from 'pg';
import { DBOS, StatusString } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

const QUEUE = 'enqueue-with-options-queue';

describe('enqueue-workflow-with-options', () => {
  let config: DBOSConfig;
  let client: Client;

  beforeEach(async () => {
    config = generateDBOSTestConfig();
    config.name = 'enqueue-options-app';
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    client = new Client({ connectionString: config.systemDatabaseUrl });
    await client.connect();
  });

  afterEach(async () => {
    await DBOS.shutdown();
    await client.end();
  });

  test('enqueues a workflow this process does not implement', async () => {
    await DBOS.launch();
    await DBOS.registerQueue(QUEUE);

    const handle = await DBOS.enqueueWorkflowWithOptions(
      { queueName: QUEUE, workflowName: 'aWorkflowElsewhere', workflowClassName: 'RemoteClass' },
      'arg-one',
      2,
    );

    const { rows } = await client.query<{
      status: string;
      name: string;
      class_name: string | null;
      queue_name: string | null;
      application_version: string | null;
      application_name: string | null;
      inputs: string;
    }>(
      `SELECT status, name, class_name, queue_name, application_version, application_name, inputs
       FROM dbos.workflow_status WHERE workflow_uuid = $1`,
      [handle.workflowID],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].status).toBe(StatusString.ENQUEUED);
    expect(rows[0].name).toBe('aWorkflowElsewhere');
    expect(rows[0].class_name).toBe('RemoteClass');
    expect(rows[0].queue_name).toBe(QUEUE);
    // Left unset unless given: the target may belong to another executor.
    expect(rows[0].application_version).toBeNull();
    expect(rows[0].application_name).toBe('enqueue-options-app');
  });

  test('portable form carries named arguments', async () => {
    await DBOS.launch();
    await DBOS.registerQueue(QUEUE);

    const handle = await DBOS.enqueueWorkflowWithOptionsPortable(
      { queueName: QUEUE, workflowName: 'aPythonWorkflow' },
      ['positional'],
      { keyword: 42 },
    );

    const { rows } = await client.query<{ inputs: string; serialization: string | null }>(
      `SELECT inputs, serialization FROM dbos.workflow_status WHERE workflow_uuid = $1`,
      [handle.workflowID],
    );
    expect(rows[0].serialization).toBe('portable_json');
    expect(JSON.parse(rows[0].inputs)).toEqual({ positionalArgs: ['positional'], namedArgs: { keyword: 42 } });
  });

  test('honours explicit options over the ambient defaults', async () => {
    await DBOS.launch();
    await DBOS.registerQueue(QUEUE);

    const handle = await DBOS.enqueueWorkflowWithOptions({
      queueName: QUEUE,
      workflowName: 'aWorkflowElsewhere',
      workflowID: 'enqueue-options-preset-id',
      appVersion: 'v-explicit',
      applicationName: 'another-app',
      priority: 5,
      deduplicationID: 'dedup-key',
    });
    expect(handle.workflowID).toBe('enqueue-options-preset-id');

    const { rows } = await client.query<{
      application_version: string | null;
      application_name: string | null;
      priority: number;
      deduplication_id: string | null;
    }>(
      `SELECT application_version, application_name, priority, deduplication_id
       FROM dbos.workflow_status WHERE workflow_uuid = $1`,
      ['enqueue-options-preset-id'],
    );
    expect(rows[0].application_version).toBe('v-explicit');
    expect(rows[0].application_name).toBe('another-app');
    expect(rows[0].priority).toBe(5);
    expect(rows[0].deduplication_id).toBe('dedup-key');
  });

  test('takes the workflow ID from the ambient context', async () => {
    await DBOS.launch();
    await DBOS.registerQueue(QUEUE);

    const handle = await DBOS.withNextWorkflowID('enqueue-options-ambient-id', async () => {
      return await DBOS.enqueueWorkflowWithOptions({ queueName: QUEUE, workflowName: 'aWorkflowElsewhere' });
    });
    expect(handle.workflowID).toBe('enqueue-options-ambient-id');
  });

  test('records the enqueue as a child of the calling workflow, replay-safe', async () => {
    let parentRuns = 0;

    class ParentTest {
      @DBOS.workflow()
      static async parent(): Promise<string> {
        parentRuns += 1;
        const child = await DBOS.enqueueWorkflowWithOptions({
          queueName: QUEUE,
          workflowName: 'aWorkflowElsewhere',
        });
        return child.workflowID;
      }
    }

    await DBOS.launch();
    await DBOS.registerQueue(QUEUE);

    const handle = await DBOS.startWorkflow(ParentTest).parent();
    const childID = await handle.getResult();
    expect(parentRuns).toBe(1);

    // The ID derives from the caller and its function ID, so a replay rebuilds the same
    // one and collides on workflow_uuid instead of enqueueing a second workflow.
    const childStepID = Number(childID.slice(handle.workflowID.length + 1));
    expect(childID.startsWith(`${handle.workflowID}-`)).toBe(true);
    expect(Number.isInteger(childStepID)).toBe(true);

    // The child is linked to its parent, so it is reachable as a child workflow.
    const childStatus = await DBOS.getWorkflowStatus(childID);
    expect(childStatus?.parentWorkflowID).toBe(handle.workflowID);
    const children = await DBOS.listWorkflows({ parentWorkflowID: handle.workflowID });
    expect(children.map((w) => w.workflowID)).toContain(childID);

    // The enqueue is checkpointed as a child step of the parent.
    const steps = await DBOS.listWorkflowSteps(handle.workflowID);
    const childStep = steps?.find((s) => s.childWorkflowID === childID);
    expect(childStep).toBeDefined();
    expect(childStep!.name).toBe('aWorkflowElsewhere');

    // A fork replaying from step 0 returns the recorded child rather than enqueueing a second workflow.
    const forkHandle = await DBOS.forkWorkflow<string>(handle.workflowID, 1, { queueName: QUEUE });
    expect(await forkHandle.getResult()).toBe(childID);

    const { rows } = await client.query<{ count: string }>(
      `SELECT COUNT(*) as count FROM dbos.workflow_status WHERE name = 'aWorkflowElsewhere'`,
    );
    expect(Number(rows[0].count)).toBe(1);
  });
});
