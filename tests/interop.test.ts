/**
 * Cross-language interoperability tests for portable workflow serialization.
 *
 * These tests prove that portable-serialized DB records are identical across
 * TypeScript, Python, and Java by:
 * 1. Running a canonical workflow and verifying DB records match golden JSON strings
 * 2. Replaying from raw SQL inserts (proving cross-language compatibility)
 * 3. Export/import replay
 *
 * Corresponding tests in other languages:
 *   Python: dbos-transact-py/tests/test_interop.py
 *   Java:   dbos-transact-java/transact/src/test/java/dev/dbos/transact/json/InteropTest.java
 */

import { Client } from 'pg';
import { DBOS, WorkflowQueue } from '../src';
import { generateDBOSTestConfig, reexecuteWorkflowById, setUpDBOSTestSysDb } from './helpers';
import { workflow_events, workflow_events_history, workflow_status, streams } from '../schemas/system_db_schema';
import { DBOSExecutor } from '../src/dbos-executor';
import { PostgresSystemDatabase } from '../src/system_database';
import { DBOSConfig } from '../src/dbos-executor';
import { z } from 'zod';

// ============================================================================
// Golden JSON strings (byte-identical across TypeScript, Python, Java tests)
// ============================================================================

// Golden inputs (positionalArgs only, no namedArgs — the minimal portable form)
const GOLDEN_INPUTS_JSON =
  '{"positionalArgs":["hello-interop",42,"2025-06-15T10:30:00.000Z",' +
  '["alpha","beta","gamma"],' +
  '{"key1":"value1","key2":99,"nested":{"deep":true}},' +
  'true,null]}';

const GOLDEN_MESSAGE_JSON = '{"sender":"test","payload":[1,2,3]}';

// Golden output JSON — the exact string each language's portable serializer must produce.
const GOLDEN_OUTPUT_JSON =
  '{"echo_text":"hello-interop","echo_num":42,' +
  '"echo_dt":"2025-06-15T10:30:00.000Z",' +
  '"items_count":3,"meta_keys":["key1","key2","nested"],' +
  '"flag":true,"empty":null,' +
  '"received":{"sender":"test","payload":[1,2,3]}}';

// Golden event value JSON
const GOLDEN_EVENT_JSON = '{"text":"hello-interop","num":42,"flag":true}';

// Golden stream value JSON
const GOLDEN_STREAM_JSON = '{"item":"hello-interop"}';

// Parsed forms (for programmatic result assertions)
const EXPECTED_RESULT = JSON.parse(GOLDEN_OUTPUT_JSON) as unknown;

// Canonical test input values
const CANONICAL_TEXT = 'hello-interop';
const CANONICAL_NUM = 42;
const CANONICAL_DT = '2025-06-15T10:30:00.000Z';
const CANONICAL_ITEMS = ['alpha', 'beta', 'gamma'];
const CANONICAL_META = { key1: 'value1', key2: 99, nested: { deep: true } };
const CANONICAL_FLAG = true;
const CANONICAL_EMPTY = null;

const CANONICAL_MESSAGE = { sender: 'test', payload: [1, 2, 3] };

// ============================================================================
// Canonical workflow registration
// ============================================================================

const _interopQueue = new WorkflowQueue('interopq');

const canonicalWorkflow = DBOS.registerWorkflow(
  async (
    text: string,
    num: number,
    dt: string,
    items: string[],
    meta: Record<string, unknown>,
    flag: boolean,
    empty: null,
  ) => {
    await DBOS.setEvent('interop_status', { text, num, flag }, { serializationType: 'portable' });
    await DBOS.writeStream('interop_stream', { item: text }, { serializationType: 'portable' });
    const msg = await DBOS.recv('interop_topic');
    return {
      echo_text: text,
      echo_num: num,
      echo_dt: dt,
      items_count: items.length,
      meta_keys: Object.keys(meta).sort(),
      flag,
      empty,
      received: msg,
    };
  },
  {
    name: 'canonicalWorkflow',
    className: 'interop',
    serialization: 'portable',
    inputSchema: z.tuple([
      z.string(),
      z.number(),
      z.string(),
      z.array(z.string()),
      z.record(z.string(), z.unknown()),
      z.boolean(),
      z.null(),
    ]),
  },
);

// ============================================================================
// Tests
// ============================================================================

describe('interop-tests', () => {
  let config: DBOSConfig;
  let systemDBClient: Client;

  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    process.env.DBOS__APPVERSION = 'v0';
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();

    systemDBClient = new Client({
      connectionString: config.systemDatabaseUrl,
    });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await DBOS.shutdown();
    process.env.DBOS__APPVERSION = undefined;
  });

  // --------------------------------------------------------------------------
  // Test: Canonical workflow execution and DB record verification
  // --------------------------------------------------------------------------

  test('test-interop-canonical', async () => {
    // Start the canonical workflow
    const wfh = await DBOS.startWorkflow(canonicalWorkflow)(
      CANONICAL_TEXT,
      CANONICAL_NUM,
      CANONICAL_DT,
      CANONICAL_ITEMS,
      CANONICAL_META,
      CANONICAL_FLAG,
      CANONICAL_EMPTY,
    );

    // Send the canonical message
    await DBOS.send(wfh.workflowID, CANONICAL_MESSAGE, 'interop_topic', undefined, {
      serializationType: 'portable',
    });

    // Verify result
    const result = await wfh.getResult();
    expect(result).toStrictEqual(EXPECTED_RESULT);

    // ---- Verify DB records ----

    // workflow_status
    const wsResult = await systemDBClient.query<workflow_status>(
      'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
      [wfh.workflowID],
    );
    expect(wsResult.rows).toHaveLength(1);
    const wsRow = wsResult.rows[0];
    expect(wsRow.serialization).toBe('portable_json');
    expect(wsRow.status).toBe('SUCCESS');
    expect(wsRow.name).toBe('canonicalWorkflow');
    expect(wsRow.class_name).toBe('interop');

    // Input string should actually match (no need for structural compare)
    expect(wsRow.inputs).toBe(GOLDEN_INPUTS_JSON);

    // Output: exact string comparison
    expect(wsRow.output).toBe(GOLDEN_OUTPUT_JSON);

    // workflow_events: exact string comparison
    const evtResult = await systemDBClient.query<workflow_events>(
      'SELECT * FROM dbos.workflow_events WHERE workflow_uuid = $1 AND key = $2',
      [wfh.workflowID, 'interop_status'],
    );
    expect(evtResult.rows).toHaveLength(1);
    expect(evtResult.rows[0].serialization).toBe('portable_json');
    expect(evtResult.rows[0].value).toBe(GOLDEN_EVENT_JSON);

    // workflow_events_history: exact string comparison
    const evthResult = await systemDBClient.query<workflow_events_history>(
      'SELECT * FROM dbos.workflow_events_history WHERE workflow_uuid = $1 AND key = $2',
      [wfh.workflowID, 'interop_status'],
    );
    expect(evthResult.rows).toHaveLength(1);
    expect(evthResult.rows[0].serialization).toBe('portable_json');
    expect(evthResult.rows[0].value).toBe(GOLDEN_EVENT_JSON);

    // streams: exact string comparison
    const streamResult = await systemDBClient.query<streams>(
      'SELECT * FROM dbos.streams WHERE workflow_uuid = $1 AND key = $2',
      [wfh.workflowID, 'interop_stream'],
    );
    expect(streamResult.rows).toHaveLength(1);
    expect(streamResult.rows[0].serialization).toBe('portable_json');
    expect(streamResult.rows[0].offset).toBe(0);
    expect(streamResult.rows[0].value).toBe(GOLDEN_STREAM_JSON);
  });

  // --------------------------------------------------------------------------
  // Test: Direct-insert replay (proves cross-language compatibility)
  // --------------------------------------------------------------------------

  test('test-interop-direct-insert', async () => {
    const id = `interop-di-${Date.now()}`;

    // Insert golden workflow_status
    await systemDBClient.query(
      `
      INSERT INTO dbos.workflow_status(
        workflow_uuid, name, class_name, queue_name,
        status, inputs, created_at, serialization
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
      `,
      [id, 'canonicalWorkflow', 'interop', 'interopq', 'ENQUEUED', GOLDEN_INPUTS_JSON, Date.now(), 'portable_json'],
    );

    // Insert golden notification
    await systemDBClient.query(
      `
      INSERT INTO dbos.notifications(
        destination_uuid, topic, message, serialization
      )
      VALUES ($1, $2, $3, $4);
      `,
      [id, 'interop_topic', GOLDEN_MESSAGE_JSON, 'portable_json'],
    );

    // Retrieve and verify the workflow executes correctly
    const wfh = DBOS.retrieveWorkflow(id);
    const result = await wfh.getResult();
    expect(result).toStrictEqual(EXPECTED_RESULT);
  });

  // --------------------------------------------------------------------------
  // Test: Export/import replay
  // --------------------------------------------------------------------------

  test('test-interop-replay', async () => {
    // Run the canonical workflow
    const wfh = await DBOS.startWorkflow(canonicalWorkflow)(
      CANONICAL_TEXT,
      CANONICAL_NUM,
      CANONICAL_DT,
      CANONICAL_ITEMS,
      CANONICAL_META,
      CANONICAL_FLAG,
      CANONICAL_EMPTY,
    );

    await DBOS.send(wfh.workflowID, CANONICAL_MESSAGE, 'interop_topic', undefined, {
      serializationType: 'portable',
    });

    expect(await wfh.getResult()).toStrictEqual(EXPECTED_RESULT);

    // Export
    const sysDb = DBOSExecutor.globalInstance!.systemDatabase as PostgresSystemDatabase;
    const exported = await sysDb.exportWorkflow(wfh.workflowID, true);

    // Delete
    await DBOS.deleteWorkflow(wfh.workflowID, true);

    // Re-import
    await sysDb.importWorkflow(exported);

    // Verify result still available
    expect(await DBOS.retrieveWorkflow(wfh.workflowID).getResult()).toStrictEqual(EXPECTED_RESULT);

    // Re-execute (replay from recorded steps)
    const reHandle = await reexecuteWorkflowById(wfh.workflowID);
    expect(await reHandle?.getResult()).toStrictEqual(EXPECTED_RESULT);
  });
});
