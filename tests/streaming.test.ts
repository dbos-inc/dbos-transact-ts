import { DBOS } from '../src/';
import { generateDBOSTestConfig, reexecuteWorkflowById, setUpDBOSTestSysDb } from './helpers';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { randomUUID } from 'node:crypto';
import { DBOSClient } from '../src/client';
import { DBOSNonExistentWorkflowError, DBOSStreamTimeoutError, getDBOSErrorCode, StreamTimeout } from '../src/error';
import { deserializeValue, serializeValue } from '../src/serialization';
import { DBOS_STREAM_CLOSED_SENTINEL, DBOS_STREAMS_CHANNEL } from '../src/system_database';
import { DBOSWorkflowCancelledError } from '../src/error';

describe('dbos-streaming-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('basic-stream-write-read', async () => {
    // Test basic stream write and read functionality
    const testValues = ['hello', 42, { key: 'value' }, [1, 2, 3], null];
    const streamKey = 'test_stream';

    const wfid = randomUUID();

    const writerWorkflow = DBOS.registerWorkflow(
      async (streamKey: string, testValues: unknown[]) => {
        for (const value of testValues) {
          await DBOS.writeStream(streamKey, value);
        }
        await DBOS.closeStream(streamKey);
      },
      { name: 'basic-stream-write-read' },
    );
    await DBOS.launch();

    // Start the writer workflow
    await DBOS.withNextWorkflowID(wfid, async () => {
      await writerWorkflow(streamKey, testValues);
    });

    // Read the stream
    const readValues: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, streamKey)) {
      readValues.push(value);
    }

    expect(readValues).toEqual(testValues);

    // Read the stream again, verify no changes
    const readValues2: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, streamKey)) {
      readValues2.push(value);
    }

    expect(readValues2).toEqual(testValues);
  });

  test('stream-read-offset', async () => {
    // Test reading a stream starting from a non-zero offset
    const streamKey = 'offset_stream';

    const writerWorkflow = DBOS.registerWorkflow(
      async (streamKey: string) => {
        for (let i = 0; i < 5; i++) {
          await DBOS.writeStream(streamKey, i);
        }
        await DBOS.closeStream(streamKey);
      },
      { name: 'stream-read-offset-writer' },
    );
    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await writerWorkflow(streamKey);
    });

    const readFrom = async (options?: { offset?: number }) => {
      const values: unknown[] = [];
      for await (const value of DBOS.readStream(wfid, streamKey, options)) {
        values.push(value);
      }
      return values;
    };

    // No options, or an explicit offset of 0, reads the whole stream
    expect(await readFrom()).toEqual([0, 1, 2, 3, 4]);
    expect(await readFrom({})).toEqual([0, 1, 2, 3, 4]);
    expect(await readFrom({ offset: 0 })).toEqual([0, 1, 2, 3, 4]);

    // A non-zero offset skips earlier values
    expect(await readFrom({ offset: 2 })).toEqual([2, 3, 4]);
    expect(await readFrom({ offset: 4 })).toEqual([4]);

    // An offset at or past the close sentinel yields nothing
    expect(await readFrom({ offset: 5 })).toEqual([]);
    expect(await readFrom({ offset: 100 })).toEqual([]);

    // Invalid offsets are rejected
    await expect(DBOS.readStream(wfid, streamKey, { offset: -1 }).next()).rejects.toThrow(
      'offset must be a non-negative integer',
    );
    await expect(DBOS.readStream(wfid, streamKey, { offset: 1.5 }).next()).rejects.toThrow(
      'offset must be a non-negative integer',
    );
  });

  test('unclosed-stream', async () => {
    // Test that reading from a stream stops when the workflow terminates
    const testValues = ['hello', 42, { key: 'value' }, [1, 2, 3], null];
    const streamKey = 'test_stream';

    const writerWorkflow = DBOS.registerWorkflow(
      async (streamKey: string, testValues: unknown[]) => {
        for (const value of testValues) {
          await DBOS.writeStream(streamKey, value);
        }
        // Note: Not closing the stream
      },
      { name: 'unclosed-stream-writer' },
    );

    const writerWorkflowError = DBOS.registerWorkflow(
      async (streamKey: string, testValues: unknown[]) => {
        for (const value of testValues) {
          await DBOS.writeStream(streamKey, value);
        }
        throw new Error('Test error');
      },
      { name: 'unclosed-stream-writer-error' },
    );

    await DBOS.launch();

    // Test successful workflow without closing
    const wfid1 = randomUUID();
    await DBOS.withNextWorkflowID(wfid1, async () => {
      await writerWorkflow(streamKey, testValues);
    });

    const readValues1: unknown[] = [];
    for await (const value of DBOS.readStream(wfid1, streamKey)) {
      readValues1.push(value);
    }
    expect(readValues1).toEqual(testValues);

    // Test error workflow without closing
    const wfid2 = randomUUID();
    await expect(
      DBOS.withNextWorkflowID(wfid2, async () => {
        await writerWorkflowError(streamKey, testValues);
      }),
    ).rejects.toThrow('Test error');

    const readValues2: unknown[] = [];
    for await (const value of DBOS.readStream(wfid2, streamKey)) {
      readValues2.push(value);
    }
    expect(readValues2).toEqual(testValues);
  });

  test('multiple-keys', async () => {
    // Test multiple streams with different keys in the same workflow
    const multiStreamWorkflow = DBOS.registerWorkflow(
      async () => {
        // Write to stream A
        await DBOS.writeStream('stream_a', 'a1');
        await DBOS.writeStream('stream_a', 'a2');

        // Write to stream B
        await DBOS.writeStream('stream_b', 'b1');
        await DBOS.writeStream('stream_b', 'b2');
        await DBOS.writeStream('stream_b', 'b3');

        // Close both streams
        await DBOS.closeStream('stream_a');
        await DBOS.closeStream('stream_b');
      },
      { name: 'multiple-keys-workflow' },
    );

    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await multiStreamWorkflow();
    });

    // Read stream A
    const streamAValues: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, 'stream_a')) {
      streamAValues.push(value);
    }
    expect(streamAValues).toEqual(['a1', 'a2']);

    // Read stream B
    const streamBValues: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, 'stream_b')) {
      streamBValues.push(value);
    }
    expect(streamBValues).toEqual(['b1', 'b2', 'b3']);
  });

  test('empty-stream', async () => {
    // Test reading from an empty stream (only close marker)
    const emptyStreamWorkflow = DBOS.registerWorkflow(
      async () => {
        await DBOS.closeStream('empty_stream');
      },
      { name: 'empty-stream-workflow' },
    );

    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await emptyStreamWorkflow();
    });

    // Read the empty stream
    const values: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, 'empty_stream')) {
      values.push(value);
    }
    expect(values).toEqual([]);
  });

  test('serialization-types', async () => {
    // Test that various data types are properly serialized/deserialized
    const testValues = ['string', 42, 3.14, true, false, null, [1, 2, 3], { nested: { dict: 'value' } }];

    const serializationTestWorkflow = DBOS.registerWorkflow(
      async (testValues: unknown[]) => {
        for (const value of testValues) {
          await DBOS.writeStream('serialize_test', value);
        }
        await DBOS.closeStream('serialize_test');
      },
      { name: 'serialization-test-workflow' },
    );

    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await serializationTestWorkflow(testValues);
    });

    // Read and verify
    const readValues: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, 'serialize_test')) {
      readValues.push(value);
    }

    expect(readValues).toEqual(testValues);
  });

  test('error-cases', async () => {
    // Test error cases and edge conditions
    await DBOS.launch();

    // Test writing to stream outside of workflow
    await expect(DBOS.writeStream('test', 'value')).rejects.toThrow(
      'Invalid call to `DBOS.writeStream` outside of a workflow or step',
    );

    // Test closing stream outside of workflow
    await expect(DBOS.closeStream('test')).rejects.toThrow(
      'Invalid call to `DBOS.closeStream` outside of a workflow or step',
    );

    // A negative per-value timeout is rejected before any read happens.
    await expect(DBOS.readStream(randomUUID(), 'test', { timeoutSeconds: -1 }).next()).rejects.toThrow(
      'timeoutSeconds must not be negative',
    );
    await expect(DBOS.readStreamOffset(randomUUID(), 'test', 0, { timeoutSeconds: -1 })).rejects.toThrow(
      'timeoutSeconds must not be negative',
    );
  });

  test('large-data', async () => {
    // Test streaming with larger amounts of data
    const largeDataWorkflow = DBOS.registerWorkflow(
      async () => {
        // Write 100 items
        for (let i = 0; i < 100; i++) {
          const data = { id: i, data: `item_${i}`, large_field: 'x'.repeat(1000) };
          await DBOS.writeStream('large_stream', data);
        }
        await DBOS.closeStream('large_stream');
      },
      { name: 'large-data-workflow' },
    );

    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await largeDataWorkflow();
    });

    // Read all values
    const values: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, 'large_stream')) {
      values.push(value);
    }

    expect(values).toHaveLength(100);
    for (let i = 0; i < values.length; i++) {
      const value = values[i] as { id: number; data: string; large_field: string };
      expect(value.id).toBe(i);
      expect(value.data).toBe(`item_${i}`);
      expect(value.large_field).toBe('x'.repeat(1000));
    }
  });

  test('interleaved-operations', async () => {
    // Test interleaved write operations across multiple streams
    const interleavedWorkflow = DBOS.registerWorkflow(
      async () => {
        await DBOS.writeStream('stream1', '1a');
        await DBOS.writeStream('stream2', '2a');
        await DBOS.writeStream('stream1', '1b');
        await DBOS.writeStream('stream3', '3a');
        await DBOS.writeStream('stream2', '2b');
        await DBOS.writeStream('stream1', '1c');

        await DBOS.closeStream('stream1');
        await DBOS.closeStream('stream2');
        await DBOS.closeStream('stream3');
      },
      { name: 'interleaved-workflow' },
    );

    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await interleavedWorkflow();
    });

    // Verify each stream has the correct values in order
    const stream1Values: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, 'stream1')) {
      stream1Values.push(value);
    }
    expect(stream1Values).toEqual(['1a', '1b', '1c']);

    const stream2Values: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, 'stream2')) {
      stream2Values.push(value);
    }
    expect(stream2Values).toEqual(['2a', '2b']);

    const stream3Values: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, 'stream3')) {
      stream3Values.push(value);
    }
    expect(stream3Values).toEqual(['3a']);
  });

  test('concurrent-write-read', async () => {
    // Test reading from a stream while it's being written to
    const streamKey = 'concurrent_stream';
    const numValues = 10;

    const writerWorkflow = DBOS.registerWorkflow(
      async (streamKey: string, numValues: number) => {
        for (let i = 0; i < numValues; i++) {
          await DBOS.writeStream(streamKey, `value_${i}`);
          // Small delay to simulate real work
          await DBOS.sleepms(200);
        }
        await DBOS.closeStream(streamKey);
      },
      { name: 'concurrent-writer-workflow' },
    );

    await DBOS.launch();

    const wfid = randomUUID();

    // Start the writer workflow in the background
    const workflowHandle = await DBOS.withNextWorkflowID(wfid, async () => {
      return DBOS.startWorkflow(writerWorkflow, {})(streamKey, numValues);
    });

    // Start reading immediately (while writing)
    const readValues: unknown[] = [];
    const startTime = Date.now();

    for await (const value of DBOS.readStream(wfid, streamKey)) {
      readValues.push(value);
      // Ensure we're not waiting too long for each value (30 second safety timeout)
      expect(Date.now() - startTime).toBeLessThan(30000);
    }

    // Wait for writer to complete
    await workflowHandle.getResult();

    // Verify all values were read
    const expectedValues = Array.from({ length: numValues }, (_, i) => `value_${i}`);
    expect(readValues).toEqual(expectedValues);
  });

  test('stream-termination-while-reader-blocked', async () => {
    // A reader that catches up to an open stream while the writer is still running
    // must terminate promptly once the workflow completes, even though no value or
    // close marker wakes it. Unlike the other unclosed-stream tests, which read only
    // after the workflow finished, this forces the blocking wait path.
    const streamKey = 'termination_latency_stream';

    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        // Write once, then stay alive without writing or closing, so the reader
        // catches up and blocks waiting for the workflow to terminate.
        await DBOS.writeStream(streamKey, 'only_value');
        await DBOS.sleepms(2000);
      },
      { name: 'termination-while-blocked-writer' },
    );

    await DBOS.launch();

    const wfid = randomUUID();
    const handle = await DBOS.withNextWorkflowID(wfid, async () => {
      return DBOS.startWorkflow(writerWorkflow, {})();
    });

    const start = Date.now();
    const readValues: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, streamKey)) {
      readValues.push(value);
    }
    const elapsed = Date.now() - start;

    await handle.getResult();
    expect(readValues).toEqual(['only_value']);
    // Termination fires no notification, so the reader only notices once its wait
    // times out and re-checks the workflow status (one ~1s polling interval).
    expect(elapsed).toBeLessThan(10000);
  });

  test('stream-low-latency-delivery', async () => {
    // Values should reach a blocked reader promptly via LISTEN/NOTIFY rather than
    // after a fixed polling interval. Each value carries the wall-clock time it was
    // written; the reader asserts it received the value shortly after.
    const streamKey = 'latency_stream';
    const numValues = 3;

    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        for (let i = 0; i < numValues; i++) {
          // Capture the write time as close to the write as possible, then pause so
          // the reader is genuinely blocked waiting for the next one.
          await DBOS.writeStream(streamKey, Date.now());
          await DBOS.sleepms(1000);
        }
        await DBOS.closeStream(streamKey);
      },
      { name: 'low-latency-writer' },
    );

    const measure = async (readIter: AsyncGenerator<unknown>): Promise<{ count: number; maxLatency: number }> => {
      let maxLatency = 0;
      let count = 0;
      for await (const writtenAt of readIter) {
        maxLatency = Math.max(maxLatency, Date.now() - (writtenAt as number));
        count += 1;
      }
      return { count, maxLatency };
    };

    await DBOS.launch();

    // In-process DBOS reader: woken by LISTEN/NOTIFY, so delivery is single-digit
    // milliseconds. A 1s polling fallback would average ~0.5s and frequently exceed
    // this across several values.
    const wfid = randomUUID();
    const handle = await DBOS.withNextWorkflowID(wfid, async () => {
      return DBOS.startWorkflow(writerWorkflow, {})();
    });
    const notifyResult = await measure(DBOS.readStream(wfid, streamKey));
    await handle.getResult();
    expect(notifyResult.count).toBe(numValues);
    expect(notifyResult.maxLatency).toBeLessThan(500);
  });

  test('workflow-recovery', async () => {
    // Test that stream operations are properly recovered during workflow replay
    let callCount = 0;
    let workflowCallCount = 0;

    const countingStep = DBOS.registerStep(
      async () => {
        callCount += 1;
        await DBOS.writeStream('recovery_stream', `in step`);
        return Promise.resolve(callCount);
      },
      { name: 'counting-step' },
    );

    const recoveryTestWorkflow = DBOS.registerWorkflow(
      async () => {
        workflowCallCount += 1;
        const count1 = await countingStep();
        await DBOS.writeStream('recovery_stream', `step_${count1}`);

        const count2 = await countingStep();
        await DBOS.writeStream('recovery_stream', `step_${count2}`);

        await DBOS.closeStream('recovery_stream');
      },
      { name: 'recovery-test-workflow' },
    );

    await DBOS.launch();

    const wfid = randomUUID();

    // Start the workflow
    await DBOS.withNextWorkflowID(wfid, async () => {
      await recoveryTestWorkflow();
    });

    // Reset call count and run the same workflow ID again (should replay)
    callCount = 0;
    await (await reexecuteWorkflowById(wfid))?.getResult();

    // The counting step should not have been called again (replayed from recorded results)
    expect(callCount).toBe(0);
    expect(workflowCallCount).toBe(2);

    // Stream should still be readable and contain the same values
    const values: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, 'recovery_stream')) {
      values.push(value);
    }
    expect(values).toEqual(['in step', 'step_1', 'in step', 'step_2']);

    // Check workflow steps were recorded correctly
    const steps = await DBOS.listWorkflowSteps(wfid);
    expect(steps).toBeDefined();
    expect(steps!.length).toBe(5);
    expect(steps![1].name).toBe('DBOS.writeStream');
    expect(steps![3].name).toBe('DBOS.writeStream');
    expect(steps![4].name).toBe('DBOS.closeStream');
  });

  test('write-from-step', async () => {
    // Test writing to a stream from inside a step function that retries and throws exceptions
    let callCount = 0;

    const stepThatWritesAndFails = DBOS.registerStep(
      async (streamKey: string, value: string) => {
        callCount += 1;

        // Always write to stream first
        await DBOS.writeStream(streamKey, `${value}_attempt_${callCount}`);

        // Throw exception to trigger retry (will succeed after 3 attempts)
        if (callCount < 4) {
          throw new Error(`Step failed on attempt ${callCount}`);
        }

        const stepId = DBOS.stepID;
        expect(stepId).toBeDefined();
        return stepId!;
      },
      { name: 'step-that-writes-and-fails', retriesAllowed: true, maxAttempts: 4, intervalSeconds: 0 },
    );

    const stepThatCloses = DBOS.registerStep(
      async (streamKey: string) => {
        await DBOS.closeStream(streamKey);
      },
      { name: 'step-that-closes' },
    );

    const workflowWithFailingStep = DBOS.registerWorkflow(
      async () => {
        // This step will fail 3 times, then succeed on the 4th attempt
        // But each failure should still write to the stream
        const result = await stepThatWritesAndFails('retry_stream', 'test_value');
        expect(result).toBe(0);

        // Also write directly from workflow
        await DBOS.writeStream('retry_stream', 'from_workflow');

        // Close the stream from a step, as a workflow may
        await stepThatCloses('retry_stream');
      },
      { name: 'workflow-with-failing-step' },
    );

    await DBOS.launch();

    const wfid = randomUUID();

    // Start the workflow
    await DBOS.withNextWorkflowID(wfid, async () => {
      await workflowWithFailingStep();
    });

    // Read the stream and verify all values are present
    // Should have 4 writes from the step (one per attempt) plus 1 from workflow
    const streamValues: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, 'retry_stream')) {
      streamValues.push(value);
    }

    // Verify we have the expected number of values
    expect(streamValues).toHaveLength(5);

    // Verify the step writes (one per retry attempt)
    expect(streamValues[0]).toBe('test_value_attempt_1');
    expect(streamValues[1]).toBe('test_value_attempt_2');
    expect(streamValues[2]).toBe('test_value_attempt_3');
    expect(streamValues[3]).toBe('test_value_attempt_4');

    // Verify the workflow write
    expect(streamValues[4]).toBe('from_workflow');

    // Verify the step was called exactly 4 times (3 failures + 1 success)
    expect(callCount).toBe(4);
  });

  test('read-stream-value-returns-status-and-value', async () => {
    // readStreamValue answers both "is there a value at this offset?" and "is the workflow still running?" in one round trip.
    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        await DBOS.writeStream('s', 0);
        await DBOS.writeStream('s', null);
        await DBOS.closeStream('s');
      },
      { name: 'read-stream-value-writer' },
    );
    await DBOS.launch();

    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await writerWorkflow();
    });

    const deser = async (v: { serializedValue: string; serialization: string | null } | undefined) =>
      v === undefined ? undefined : await deserializeValue(v.serializedValue, v.serialization, sysdb.getSerializer());

    // The value at the offset and the status, together.
    let r = await sysdb.readStreamValue(wfid, 's', 0);
    expect(r.status).toBe('SUCCESS');
    expect(await deser(r.value)).toBe(0);

    // A written null is a value, not an absence -- which is why absence needs its own sentinel.
    r = await sysdb.readStreamValue(wfid, 's', 1);
    expect(r.status).toBe('SUCCESS');
    expect(r.value).not.toBeUndefined();
    expect(await deser(r.value)).toBeNull();

    // Past the end: still reports status, so the reader can tell "not yet" from "never".
    r = await sysdb.readStreamValue(wfid, 's', 99);
    expect(r.status).toBe('SUCCESS');
    expect(r.value).toBeUndefined();

    // A non-existent workflow is distinguishable from a workflow with no value at the offset.
    r = await sysdb.readStreamValue(randomUUID(), 's', 0);
    expect(r.status).toBeNull();
    expect(r.value).toBeUndefined();
  });

  test('read-stream-nonexistent-workflow-raises', async () => {
    // The in-process reader raises on an unknown workflow, where the client's generator ends quietly.
    await DBOS.launch();
    const gen = DBOS.readStream(randomUUID(), 's');
    await expect(gen.next()).rejects.toThrow(DBOSNonExistentWorkflowError);
  });

  test('stream-trigger-dropped-notifier-delivers', async () => {
    // The per-row NOTIFY trigger is dropped; assert it's gone and that the coalescing notifier still wakes a blocked reader well under the 1s poll.
    const streamKey = 'notifier_stream';
    const numValues = 3;

    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        for (let i = 0; i < numValues; i++) {
          await DBOS.writeStream(streamKey, Date.now());
          await DBOS.sleepms(1000);
        }
        await DBOS.closeStream(streamKey);
      },
      { name: 'notifier-delivery-writer' },
    );
    await DBOS.launch();

    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    // No per-row trigger may remain on the streams table.
    const trigger = await sysdb.pool.query(
      `SELECT 1 FROM pg_trigger t
       JOIN pg_class cl ON t.tgrelid = cl.oid
       JOIN pg_namespace n ON cl.relnamespace = n.oid
       WHERE n.nspname = $1 AND cl.relname = 'streams' AND t.tgname = 'dbos_streams_trigger'`,
      [sysdb.schemaName],
    );
    expect(trigger.rows.length).toBe(0);

    const wfid = randomUUID();
    const handle = await DBOS.withNextWorkflowID(wfid, async () => {
      return DBOS.startWorkflow(writerWorkflow, {})();
    });

    let maxLatency = 0;
    let count = 0;
    for await (const writtenAt of DBOS.readStream<number>(wfid, streamKey)) {
      maxLatency = Math.max(maxLatency, Date.now() - writtenAt);
      count += 1;
    }
    await handle.getResult();
    expect(count).toBe(numValues);
    // Delivery via the notifier (~10ms coalesce) is far faster than the 1s polling fallback.
    expect(maxLatency).toBeLessThan(500);
  });

  test('read-stream-is-one-round-trip-per-value', async () => {
    // Each reader tick issues a single joined query fetching value and status together; a regression to two queries would be correct but slow.
    const n = 25;
    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        for (let i = 0; i < n; i++) {
          await DBOS.writeStream('s', i);
        }
        await DBOS.closeStream('s');
      },
      { name: 'one-round-trip-writer' },
    );
    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await writerWorkflow();
    });

    // Only the reader joins streams to workflow_status, so background threads sharing the pool can't match; matched loosely, not by table name.
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const spy = jest.spyOn(sysdb.pool, 'query');
    const values: unknown[] = [];
    let queryTexts: string[] = [];
    try {
      for await (const value of DBOS.readStream(wfid, 's')) {
        values.push(value);
      }
      // Capture before mockRestore(), which clears spy.mock.calls.
      queryTexts = spy.mock.calls.map((c) =>
        typeof c[0] === 'string' ? c[0] : ((c[0] as { text?: string })?.text ?? ''),
      );
    } finally {
      spy.mockRestore();
    }

    expect(values).toEqual(Array.from({ length: n }, (_, i) => i));
    const joined = queryTexts.filter((text) => {
      const s = text.toLowerCase().replace(/\s+/g, ' ');
      return s.includes('outer join') && s.includes('streams') && s.includes('workflow_status');
    });
    // Guards against passing vacuously: reading the status separately issues no joined query at all.
    expect(joined.length).toBeGreaterThan(0);
    // One joined query per delivered value, plus the one that finds the close sentinel; two queries per tick would double this.
    expect(joined.length).toBe(n + 1);
  });

  test('stream-notifier-drops-unsendable-payload', async () => {
    // A rejected batch (e.g. a payload over the 8000-byte limit) is dropped, not requeued, so a poison payload can't permanently stall the notifier.
    await DBOS.launch();
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const internals = sysdb as unknown as {
      pendingNotifications: Map<string, Set<string>>;
      flushNotifications: () => Promise<void>;
    };
    const addPending = (channel: string, payload: string) => {
      let batch = internals.pendingNotifications.get(channel);
      if (!batch) {
        batch = new Set();
        internals.pendingNotifications.set(channel, batch);
      }
      batch.add(payload);
    };

    // A stream key past pg_notify's 8000-byte payload limit makes its batch unsendable.
    const poison = `${randomUUID()}::${'x'.repeat(9000)}`;
    internals.pendingNotifications.clear();
    addPending(DBOS_STREAMS_CHANNEL, poison);
    await internals.flushNotifications();

    // The poison batch is dropped, not requeued (requeuing would loop forever on it).
    expect(internals.pendingNotifications.get(DBOS_STREAMS_CHANNEL)?.has(poison) ?? false).toBe(false);

    // The notifier still works afterward: a subsequent good payload is delivered.
    const goodWf = randomUUID();
    const goodKey = 'deliverable';
    let fired = false;
    const cbr = sysdb.streamsMap.registerCallback(`${goodWf}::${goodKey}`, () => {
      fired = true;
    });
    try {
      addPending(DBOS_STREAMS_CHANNEL, `${goodWf}::${goodKey}`);
      await internals.flushNotifications();
      for (let i = 0; i < 100 && !fired; i++) {
        await new Promise((r) => setTimeout(r, 50));
      }
      expect(fired).toBe(true);
    } finally {
      sysdb.streamsMap.deregisterCallback(cbr);
    }
  });

  test('stream-notifier-survives-flush-error', async () => {
    // An exception escaping a flush must not kill the notifier loop; it logs, backs off, and resumes delivering.
    await DBOS.launch();
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const internals = sysdb as unknown as {
      pendingNotifications: Map<string, Set<string>>;
      flushNotifications: () => Promise<void>;
    };
    const realFlush = internals.flushNotifications.bind(sysdb);
    let raised = false;
    internals.flushNotifications = async () => {
      // Raise once (even on an empty batch) to simulate an unexpected error; then behave normally.
      if (!raised) {
        raised = true;
        throw new Error('simulated flush failure');
      }
      return realFlush();
    };

    try {
      // Wait until the running notifier loop has hit the injected failure.
      for (let i = 0; i < 200 && !raised; i++) {
        await new Promise((r) => setTimeout(r, 20));
      }
      expect(raised).toBe(true);

      // The loop must survive and still deliver a subsequently signaled stream; it backs off ~1s after the error, so allow generous time.
      const goodWf = randomUUID();
      const goodKey = 'post_error';
      let fired = false;
      const cbr = sysdb.streamsMap.registerCallback(`${goodWf}::${goodKey}`, () => {
        fired = true;
      });
      try {
        let batch = internals.pendingNotifications.get(DBOS_STREAMS_CHANNEL);
        if (!batch) {
          batch = new Set();
          internals.pendingNotifications.set(DBOS_STREAMS_CHANNEL, batch);
        }
        batch.add(`${goodWf}::${goodKey}`);
        for (let i = 0; i < 200 && !fired; i++) {
          await new Promise((r) => setTimeout(r, 50));
        }
        expect(fired).toBe(true);
      } finally {
        sysdb.streamsMap.deregisterCallback(cbr);
      }
    } finally {
      internals.flushNotifications = realFlush;
    }
  });

  test('event-notifier-delivers-without-workflow-events-trigger', async () => {
    // The per-row workflow_events trigger is dropped; assert it's gone and that the coalescing notifier still wakes a blocked getEvent well under the 10s event poll.
    const key = 'notifier_event';

    const setterWorkflow = DBOS.registerWorkflow(
      async () => {
        await DBOS.sleepms(1000);
        await DBOS.setEvent(key, 'notifier_value');
      },
      { name: 'event-notifier-setter' },
    );
    await DBOS.launch();

    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    // No per-row trigger may remain on the workflow_events table.
    const trigger = await sysdb.pool.query(
      `SELECT 1 FROM pg_trigger t
       JOIN pg_class cl ON t.tgrelid = cl.oid
       JOIN pg_namespace n ON cl.relnamespace = n.oid
       WHERE n.nspname = $1 AND cl.relname = 'workflow_events' AND t.tgname = 'dbos_workflow_events_trigger'`,
      [sysdb.schemaName],
    );
    expect(trigger.rows.length).toBe(0);

    const wfid = randomUUID();
    const handle = await DBOS.withNextWorkflowID(wfid, async () => {
      return DBOS.startWorkflow(setterWorkflow, {})();
    });

    // getEvent blocks until the value is set ~1s in; prompt delivery must come from the notifier, not the 10s poll.
    const begin = Date.now();
    const value = await DBOS.getEvent<string>(wfid, key, 30);
    const latency = Date.now() - begin;
    await handle.getResult();

    expect(value).toBe('notifier_value');
    // Delivery via the notifier (~10ms coalesce after the value lands) is far faster than the 10s polling fallback.
    expect(latency).toBeLessThan(3000);
  });

  test('message-notifications-trigger-is-kept', async () => {
    // Messages keep their in-transaction NOTIFY trigger (they can be sent from processes with no notifier to buffer them); assert it exists and that send still wakes a blocked recv.
    const recvWorkflow = DBOS.registerWorkflow(
      async () => {
        return await DBOS.recv<string>(undefined, { timeoutSeconds: 30 });
      },
      { name: 'notifications-trigger-recv' },
    );
    await DBOS.launch();

    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    // The notifications trigger must be kept.
    const trigger = await sysdb.pool.query(
      `SELECT 1 FROM pg_trigger t
       JOIN pg_class cl ON t.tgrelid = cl.oid
       JOIN pg_namespace n ON cl.relnamespace = n.oid
       WHERE n.nspname = $1 AND cl.relname = 'notifications' AND t.tgname = 'dbos_notifications_trigger'`,
      [sysdb.schemaName],
    );
    expect(trigger.rows.length).toBe(1);

    const dest = randomUUID();
    const handle = await DBOS.withNextWorkflowID(dest, async () => {
      return DBOS.startWorkflow(recvWorkflow, {})();
    });

    // Let the recv block on the listener, then send; the trigger's NOTIFY must wake it well under the 10s poll.
    await DBOS.sleepms(1000);
    const begin = Date.now();
    await DBOS.send(dest, 'hello_trigger');
    const result = await handle.getResult();
    const latency = Date.now() - begin;

    expect(result).toBe('hello_trigger');
    expect(latency).toBeLessThan(3000);
  });

  test('workflow-read-stream-checkpointing', async () => {
    // A workflow reading a stream records one step per value, so a replay re-yields what it read
    // rather than re-reading a stream that has moved on. Reads from a step are not recorded.
    const streamKey = 'checkpointed_stream';
    const testValues: unknown[] = ['a', null, { k: 'v' }, 42];
    let readerCalls = 0;
    let crashingReaderAttempts = 0;
    let stepCalls = 0;

    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        for (const value of testValues) {
          await DBOS.writeStream(streamKey, value);
        }
        // Left unclosed: a reader stops once the writer goes terminal.
      },
      { name: 'checkpoint-writer' },
    );

    const readerWorkflow = DBOS.registerWorkflow(
      async (targetID: string) => {
        readerCalls += 1;
        const seen: unknown[] = [];
        for await (const value of DBOS.readStream(targetID, streamKey)) {
          seen.push(value);
        }
        return seen;
      },
      { name: 'checkpoint-reader' },
    );

    const crashingReaderWorkflow = DBOS.registerWorkflow(
      async (targetID: string) => {
        crashingReaderAttempts += 1;
        const seen: unknown[] = [];
        for await (const value of DBOS.readStream(targetID, streamKey)) {
          seen.push(value);
          if (crashingReaderAttempts === 1 && seen.length === 2) {
            throw new Error('reader crashed');
          }
        }
        return seen;
      },
      { name: 'checkpoint-crashing-reader' },
    );

    const countingStep = DBOS.registerStep(
      async () => {
        stepCalls += 1;
        return Promise.resolve(stepCalls);
      },
      { name: 'checkpoint-counting-step' },
    );

    const partialReaderWorkflow = DBOS.registerWorkflow(
      async (targetID: string) => {
        const seen: unknown[] = [];
        for await (const value of DBOS.readStream(targetID, streamKey)) {
          seen.push(value);
          if (seen.length === 2) break;
        }
        return [seen, await countingStep()];
      },
      { name: 'checkpoint-partial-reader' },
    );

    const readInStep = DBOS.registerStep(
      async (targetID: string) => {
        const seen: unknown[] = [];
        for await (const value of DBOS.readStream(targetID, streamKey)) {
          seen.push(value);
        }
        return seen;
      },
      { name: 'checkpoint-read-in-step' },
    );

    const stepReaderWorkflow = DBOS.registerWorkflow(async (targetID: string) => readInStep(targetID), {
      name: 'checkpoint-step-reader',
    });

    await DBOS.launch();

    const writerID = randomUUID();
    await DBOS.withNextWorkflowID(writerID, async () => {
      await writerWorkflow();
    });

    // One step per value, including the null, plus one recording that the stream ended.
    const readerID = randomUUID();
    await DBOS.withNextWorkflowID(readerID, async () => {
      expect(await readerWorkflow(writerID)).toEqual(testValues);
    });
    expect((await DBOS.listWorkflowSteps(readerID))!.map((step) => step.name)).toEqual(
      new Array(testValues.length + 1).fill('DBOS.readStream'),
    );

    // A reader that failed partway records only what it delivered, then reads on from there.
    const crashingReaderID = randomUUID();
    await DBOS.withNextWorkflowID(crashingReaderID, async () => {
      await expect(crashingReaderWorkflow(writerID)).rejects.toThrow('reader crashed');
    });
    expect((await DBOS.listWorkflowSteps(crashingReaderID))!.length).toBe(2);
    expect(await (await reexecuteWorkflowById(crashingReaderID)).getResult()).toEqual(testValues);
    expect(crashingReaderAttempts).toBe(2);

    // Each value consumes a function ID, so a step after a half-read stream still replays.
    const partialReaderID = randomUUID();
    await DBOS.withNextWorkflowID(partialReaderID, async () => {
      expect(await partialReaderWorkflow(writerID)).toEqual([testValues.slice(0, 2), 1]);
    });
    expect(await (await reexecuteWorkflowById(partialReaderID)).getResult()).toEqual([testValues.slice(0, 2), 1]);
    expect(stepCalls).toBe(1);

    // A read inside a step is covered by that step's own checkpoint.
    const stepReaderID = randomUUID();
    await DBOS.withNextWorkflowID(stepReaderID, async () => {
      expect(await stepReaderWorkflow(writerID)).toEqual(testValues);
    });
    const stepReaderSteps = (await DBOS.listWorkflowSteps(stepReaderID))!;
    expect(stepReaderSteps.map((step) => step.name)).toEqual(['checkpoint-read-in-step']);

    // Extend the stream now the readers are done, so a live re-read would see more than they did.
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    for (const value of ['d', 'e']) {
      const serval = await serializeValue(value, sysdb.getSerializer(), undefined);
      await sysdb.writeStreamFromStep(writerID, 100, streamKey, serval.serializedValue!, serval.serialization);
    }
    const liveValues: unknown[] = [];
    for await (const value of DBOS.readStream(writerID, streamKey)) {
      liveValues.push(value);
    }
    expect(liveValues).toEqual([...testValues, 'd', 'e']);

    expect(await (await reexecuteWorkflowById(readerID)).getResult()).toEqual(testValues);
    expect(readerCalls).toBe(2);
  });

  test('close-marker-ends-stream-consistently', async () => {
    // Every reader ends a stream at its close marker: values written past a close are invisible,
    // a stream closed with nothing in it reads as empty rather than absent, and a marker left by a
    // release that wrote it unserialized still ends the stream.
    const streamKey = 'closed_stream';
    const emptyKey = 'closed_empty_stream';
    const legacyKey = 'legacy_marker_stream';

    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        await DBOS.writeStream(streamKey, 'v0');
        await DBOS.closeStream(streamKey);
        // Writing past a close is still accepted; every reader must agree it is not part of the stream.
        await DBOS.writeStream(streamKey, 'after');
        await DBOS.closeStream(emptyKey);
        await DBOS.writeStream(legacyKey, 'v0');
        // Left open here: the older unserialized marker is appended below.
      },
      { name: 'closed-stream-writer' },
    );
    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await writerWorkflow();
    });

    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    await sysdb.writeStreamFromStep(wfid, 100, legacyKey, DBOS_STREAM_CLOSED_SENTINEL, 'portable_json');

    const readAll = async (key: string) => {
      const values: unknown[] = [];
      for await (const value of DBOS.readStream(wfid, key)) {
        values.push(value);
      }
      return values;
    };
    expect(await readAll(streamKey)).toEqual(['v0']);
    expect(await readAll(emptyKey)).toEqual([]);
    expect(await readAll(legacyKey)).toEqual(['v0']);

    // The bulk fetch the conductor uses reports exactly what readStream yields.
    expect(await sysdb.getAllStreamEntries(wfid)).toEqual({
      [streamKey]: ['v0'],
      [emptyKey]: [],
      [legacyKey]: ['v0'],
    });
  });

  test('read-stream-notices-cancellation', async () => {
    // A reader waiting on a live producer observes its own cancellation, rather than blocking
    // until the producer finishes.
    const streamKey = 'cancel_stream';
    let release!: () => void;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    let entered!: () => void;
    const hasEntered = new Promise<void>((resolve) => {
      entered = resolve;
    });

    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        await DBOS.writeStream(streamKey, 'v0');
        // Stay active and silent so the reader waits rather than draining to the end.
        await released;
      },
      { name: 'cancel-stream-writer' },
    );

    const readerWorkflow = DBOS.registerWorkflow(
      async (targetID: string) => {
        const seen: unknown[] = [];
        for await (const value of DBOS.readStream(targetID, streamKey)) {
          seen.push(value);
          entered();
        }
        return seen;
      },
      { name: 'cancel-stream-reader' },
    );
    await DBOS.launch();

    const wfid = randomUUID();
    const writerHandle = await DBOS.withNextWorkflowID(wfid, async () => {
      return DBOS.startWorkflow(writerWorkflow, {})();
    });
    try {
      const readerID = randomUUID();
      const readerHandle = await DBOS.withNextWorkflowID(readerID, async () => {
        return DBOS.startWorkflow(readerWorkflow, {})(wfid);
      });
      await hasEntered;
      await DBOS.cancelWorkflow(readerID);
      // A local handle propagates the workflow's own error rather than the awaiter's variant.
      await expect(readerHandle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    } finally {
      release();
      await writerHandle.getResult();
    }
  });

  test('read-stream-timeout', async () => {
    // The timeout is per value and the polling interval is per call. The writer stays active and
    // silent, so only the timeout can end a wait, and a value delivered in between restarts the
    // clock rather than counting against a single overall deadline. Notifications are off and the
    // configured fallback is far longer than the test can wait, so nothing is delivered unless the
    // per-call interval is honored.
    const streamKey = 'timeout_stream';
    let release!: () => void;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });

    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        // Stay active and silent so a reader waits rather than draining to the end.
        await released;
      },
      { name: 'timeout-writer' },
    );

    const priorUseListenNotify = config.useListenNotify;
    config.useListenNotify = false;
    DBOS.setConfig(config);
    try {
      await DBOS.launch();
      const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
      sysdb.dbPollingIntervalStreamMs = 30000;

      const wfid = randomUUID();
      const handle = await DBOS.withNextWorkflowID(wfid, async () => {
        return DBOS.startWorkflow(writerWorkflow, {})();
      });

      const insertLater = (value: string, delayMs: number) =>
        setTimeout(() => {
          void (async () => {
            const serval = await serializeValue(value, sysdb.getSerializer(), undefined);
            await sysdb.writeStreamFromStep(wfid, 100, streamKey, serval.serializedValue!, serval.serialization);
          })();
        }, delayMs);

      await expect(DBOS.readStream(wfid, streamKey, { timeoutSeconds: 0.3 }).next()).rejects.toThrow(
        DBOSStreamTimeoutError,
      );

      // Two values, each arriving within the timeout but together exceeding it: a per-value clock
      // delivers both, a single overall deadline would raise on the second.
      const timers = [insertLater('v0', 500), insertLater('v1', 1000)];
      const reader = DBOS.readStream<string>(wfid, streamKey, { timeoutSeconds: 0.75, pollingIntervalMs: 50 });
      expect([(await reader.next()).value, (await reader.next()).value]).toEqual(['v0', 'v1']);
      await reader.return(undefined);
      timers.forEach((timer) => clearTimeout(timer));

      // A terminal writer drains to the end of the stream rather than timing out.
      release();
      await handle.getResult();
      const drained: string[] = [];
      for await (const value of DBOS.readStream<string>(wfid, streamKey, {
        timeoutSeconds: 0.3,
        pollingIntervalMs: 50,
      })) {
        drained.push(value);
      }
      expect(drained).toEqual(['v0', 'v1']);
    } finally {
      // Restore the shared config so a disabled listener does not leak into a later test.
      config.useListenNotify = priorUseListenNotify;
      release();
    }
  });

  test('workflow-read-stream-timeout-is-checkpointed', async () => {
    // A timeout is an outcome, not a failure of the read, so it is recorded: a replayed reader
    // raises it again straight from its checkpoint instead of waiting out the timeout a second time.
    const streamKey = 'timeout_checkpoint_stream';
    let release!: () => void;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    let attempts = 0;

    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        await DBOS.writeStream(streamKey, 'only');
        // Stay active and silent so the reader times out waiting for a second value.
        await released;
      },
      { name: 'timeout-checkpoint-writer' },
    );

    const readWithTimeout = async (targetID: string) => {
      const seen: unknown[] = [];
      try {
        for await (const value of DBOS.readStream(targetID, streamKey, { timeoutSeconds: 3 })) {
          seen.push(value);
        }
      } catch (e) {
        if (getDBOSErrorCode(e as Error) !== StreamTimeout) throw e;
        seen.push('timed out');
      }
      return seen;
    };

    const readerWorkflow = DBOS.registerWorkflow(
      async (targetID: string) => {
        attempts += 1;
        return readWithTimeout(targetID);
      },
      { name: 'timeout-checkpoint-reader' },
    );

    // A portable workflow records the timeout under the app's serializer too, so its replay rebuilds
    // the exact error rather than the PortableWorkflowError that portable serialization would flatten
    // it into.
    const portableReaderWorkflow = DBOS.registerWorkflow(async (targetID: string) => readWithTimeout(targetID), {
      name: 'timeout-checkpoint-portable-reader',
      serialization: 'portable',
    });

    await DBOS.launch();

    const wfid = randomUUID();
    const handle = await DBOS.withNextWorkflowID(wfid, async () => {
      return DBOS.startWorkflow(writerWorkflow, {})();
    });
    try {
      const readerID = randomUUID();
      await DBOS.withNextWorkflowID(readerID, async () => {
        expect(await readerWorkflow(wfid)).toEqual(['only', 'timed out']);
      });

      // The delivered value and the timeout that followed it, one step each, the timeout recorded
      // as the step's error rather than as an output.
      const steps = (await DBOS.listWorkflowSteps(readerID))!;
      expect(steps.map((step) => step.name)).toEqual(['DBOS.readStream', 'DBOS.readStream']);
      expect(steps[0].error).toBeNull();
      expect(steps[0].output).toBe('only');
      expect(steps[1].output).toBeNull();
      expect(getDBOSErrorCode(steps[1].error!)).toBe(StreamTimeout);

      // Replays from the checkpoint: the writer is still silent, so a live re-read would wait out
      // the full timeout again before raising.
      const start = Date.now();
      expect(await (await reexecuteWorkflowById(readerID)).getResult()).toEqual(['only', 'timed out']);
      expect(attempts).toBe(2);
      expect(Date.now() - start).toBeLessThan(2500);

      const portableID = randomUUID();
      await DBOS.withNextWorkflowID(portableID, async () => {
        expect(await portableReaderWorkflow(wfid)).toEqual(['only', 'timed out']);
      });
      expect(await (await reexecuteWorkflowById(portableID)).getResult()).toEqual(['only', 'timed out']);
    } finally {
      release();
      await handle.getResult();
    }
  });

  test('read-stream-offset-api', async () => {
    // readStreamOffset returns the one value at an offset, waiting for it and throwing if it never
    // arrives -- because the timeout passed or because the stream ended short of it. From a workflow
    // it is one checkpointed step, so a replay returns the recorded value or re-throws.
    const streamKey = 'offset_api_stream';
    let release!: () => void;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    let readerCalls = 0;

    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        await DBOS.writeStream(streamKey, 'v0');
        await DBOS.writeStream(streamKey, 'v1');
        // Stay active so a read past the end waits rather than giving up immediately.
        await released;
        await DBOS.closeStream(streamKey);
      },
      { name: 'offset-api-writer' },
    );

    const readerWorkflow = DBOS.registerWorkflow(
      async (targetID: string) => {
        readerCalls += 1;
        const seen: unknown[] = [await DBOS.readStreamOffset(targetID, streamKey, 1)];
        try {
          await DBOS.readStreamOffset(targetID, streamKey, 5, { timeoutSeconds: 0.3 });
        } catch (e) {
          if (getDBOSErrorCode(e as Error) !== StreamTimeout) throw e;
          seen.push('timed out');
        }
        return seen;
      },
      { name: 'offset-api-reader' },
    );

    await DBOS.launch();

    const wfid = randomUUID();
    const handle = await DBOS.withNextWorkflowID(wfid, async () => {
      return DBOS.startWorkflow(writerWorkflow, {})();
    });
    try {
      // A value already written comes back without waiting.
      expect(await DBOS.readStreamOffset(wfid, streamKey, 0)).toBe('v0');

      // An offset the stream has not reached waits, then throws.
      await expect(DBOS.readStreamOffset(wfid, streamKey, 5, { timeoutSeconds: 0.3 })).rejects.toThrow(
        DBOSStreamTimeoutError,
      );
      // One step per read, the timed-out one recorded as an error, and a replay repeats both without
      // waiting out the timeout again.
      const readerID = randomUUID();
      await DBOS.withNextWorkflowID(readerID, async () => {
        expect(await readerWorkflow(wfid)).toEqual(['v1', 'timed out']);
      });
      const steps = (await DBOS.listWorkflowSteps(readerID))!;
      expect(steps.map((step) => step.name)).toEqual(['DBOS.readStreamOffset', 'DBOS.readStreamOffset']);
      expect(steps[0].output).toBe('v1');
      expect(steps[1].output).toBeNull();
      expect(getDBOSErrorCode(steps[1].error!)).toBe(StreamTimeout);

      expect(await (await reexecuteWorkflowById(readerID)).getResult()).toEqual(['v1', 'timed out']);
      expect(readerCalls).toBe(2);
    } finally {
      release();
      await handle.getResult();
    }

    // A closed stream gives up on an offset it never reached, rather than waiting out a timeout.
    const start = Date.now();
    await expect(DBOS.readStreamOffset(wfid, streamKey, 5, { timeoutSeconds: 30 })).rejects.toThrow(
      DBOSStreamTimeoutError,
    );
    expect(Date.now() - start).toBeLessThan(2000);
  });
});

describe('dbos-client-streaming-tests', () => {
  let config: DBOSConfig;
  let client: DBOSClient;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
  });

  beforeEach(async () => {});

  afterEach(async () => {
    await DBOS.shutdown();
  });

  afterAll(async () => {
    await client.destroy();
  });

  test('client-basic-stream-read', async () => {
    // Test basic client stream read functionality
    const testValues = ['client_hello', 123, { client_key: 'client_value' }, [4, 5, 6], null];
    const streamKey = 'client_test_stream';

    const writerWorkflow = DBOS.registerWorkflow(
      async (streamKey: string, testValues: unknown[]) => {
        for (const value of testValues) {
          await DBOS.writeStream(streamKey, value);
        }
        await DBOS.closeStream(streamKey);
      },
      { name: 'client-basic-stream-writer' },
    );
    await DBOS.launch();

    const wfid = randomUUID();

    // Start the writer workflow
    await DBOS.withNextWorkflowID(wfid, async () => {
      await writerWorkflow(streamKey, testValues);
    });

    // Read the stream using client
    const readValues: unknown[] = [];
    for await (const value of client.readStream(wfid, streamKey)) {
      readValues.push(value);
    }

    expect(readValues).toEqual(testValues);
  });

  test('client-stream-read-offset', async () => {
    // Test client reading a stream starting from a non-zero offset
    const streamKey = 'client_offset_stream';

    const writerWorkflow = DBOS.registerWorkflow(
      async (streamKey: string) => {
        for (let i = 0; i < 5; i++) {
          await DBOS.writeStream(streamKey, i);
        }
        await DBOS.closeStream(streamKey);
      },
      { name: 'client-stream-read-offset-writer' },
    );
    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await writerWorkflow(streamKey);
    });

    const readFrom = async (options?: { offset?: number }) => {
      const values: unknown[] = [];
      for await (const value of client.readStream(wfid, streamKey, options)) {
        values.push(value);
      }
      return values;
    };

    expect(await readFrom()).toEqual([0, 1, 2, 3, 4]);
    expect(await readFrom({ offset: 2 })).toEqual([2, 3, 4]);
    expect(await readFrom({ offset: 4 })).toEqual([4]);

    // An offset at or past the close sentinel yields nothing
    expect(await readFrom({ offset: 5 })).toEqual([]);
    expect(await readFrom({ offset: 100 })).toEqual([]);

    // Invalid offsets are rejected
    await expect(client.readStream(wfid, streamKey, { offset: -1 }).next()).rejects.toThrow(
      'offset must be a non-negative integer',
    );
  });

  test('client-empty-stream', async () => {
    // Test client reading from an empty stream
    const emptyStreamWorkflow = DBOS.registerWorkflow(
      async () => {
        await DBOS.closeStream('client_empty_stream');
      },
      { name: 'client-empty-stream-workflow' },
    );
    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await emptyStreamWorkflow();
    });

    // Read the empty stream using client
    const values: unknown[] = [];
    for await (const value of client.readStream(wfid, 'client_empty_stream')) {
      values.push(value);
    }
    expect(values).toEqual([]);
  });

  test('client-unclosed-stream', async () => {
    // Test client reading from unclosed stream stops when workflow terminates
    const testValues = ['client_a', 'client_b', 'client_c'];
    const streamKey = 'client_unclosed_stream';

    const writerWorkflow = DBOS.registerWorkflow(
      async (streamKey: string, testValues: unknown[]) => {
        for (const value of testValues) {
          await DBOS.writeStream(streamKey, value);
        }
        // Note: Not closing the stream
      },
      { name: 'client-unclosed-stream-writer' },
    );
    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await writerWorkflow(streamKey, testValues);
    });

    // Read the stream using client - should get all values and stop
    const readValues: unknown[] = [];
    for await (const value of client.readStream(wfid, streamKey)) {
      readValues.push(value);
    }
    expect(readValues).toEqual(testValues);
  });

  test('client-multiple-streams', async () => {
    // Test client reading from multiple streams
    const multiStreamWorkflow = DBOS.registerWorkflow(
      async () => {
        await DBOS.writeStream('client_stream_x', 'x1');
        await DBOS.writeStream('client_stream_y', 'y1');
        await DBOS.writeStream('client_stream_x', 'x2');
        await DBOS.writeStream('client_stream_y', 'y2');

        await DBOS.closeStream('client_stream_x');
        await DBOS.closeStream('client_stream_y');
      },
      { name: 'client-multiple-streams-workflow' },
    );
    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await multiStreamWorkflow();
    });

    // Read stream X using client
    const streamXValues: unknown[] = [];
    for await (const value of client.readStream(wfid, 'client_stream_x')) {
      streamXValues.push(value);
    }
    expect(streamXValues).toEqual(['x1', 'x2']);

    // Read stream Y using client
    const streamYValues: unknown[] = [];
    for await (const value of client.readStream(wfid, 'client_stream_y')) {
      streamYValues.push(value);
    }
    expect(streamYValues).toEqual(['y1', 'y2']);
  });

  test('client-stream-low-latency-polling', async () => {
    // The client has no notification listener thread, so its registered event is
    // never signaled and each read falls back to re-reading the offset once the
    // wait times out (~1s polling interval). Verify it still delivers every value,
    // confirming it actually polls rather than blocking forever on a notification
    // that never arrives.
    const streamKey = 'client_latency_stream';
    const numValues = 3;

    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        for (let i = 0; i < numValues; i++) {
          await DBOS.writeStream(streamKey, Date.now());
          await DBOS.sleepms(1000);
        }
        await DBOS.closeStream(streamKey);
      },
      { name: 'client-low-latency-writer' },
    );
    await DBOS.launch();

    const wfid = randomUUID();
    const handle = await DBOS.withNextWorkflowID(wfid, async () => {
      return DBOS.startWorkflow(writerWorkflow, {})();
    });

    let maxLatency = 0;
    let count = 0;
    for await (const writtenAt of client.readStream<number>(wfid, streamKey)) {
      maxLatency = Math.max(maxLatency, Date.now() - writtenAt);
      count += 1;
    }
    await handle.getResult();
    expect(count).toBe(numValues);
    expect(maxLatency).toBeLessThan(2000);
  });

  test('stream-low-latency-polling-fallback', async () => {
    // With LISTEN/NOTIFY off the notifier stays idle and no notifications fire; the reader is woken by the polling fallback instead.
    const streamKey = 'polling_fallback_stream';
    const numValues = 3;

    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        for (let i = 0; i < numValues; i++) {
          await DBOS.writeStream(streamKey, Date.now());
          await DBOS.sleepms(1000);
        }
        await DBOS.closeStream(streamKey);
      },
      { name: 'polling-fallback-writer' },
    );

    const priorUseListenNotify = config.useListenNotify;
    config.useListenNotify = false;
    DBOS.setConfig(config);
    try {
      await DBOS.launch();

      const wfid = randomUUID();
      const handle = await DBOS.withNextWorkflowID(wfid, async () => {
        return DBOS.startWorkflow(writerWorkflow, {})();
      });

      let maxLatency = 0;
      let count = 0;
      for await (const writtenAt of DBOS.readStream<number>(wfid, streamKey)) {
        maxLatency = Math.max(maxLatency, Date.now() - writtenAt);
        count += 1;
      }
      await handle.getResult();
      expect(count).toBe(numValues);
      expect(maxLatency).toBeLessThan(2000);
    } finally {
      // Restore the shared config so a disabled listener does not leak into any
      // later test that reuses it. setConfig is rejected post-launch, but the
      // stored config is this same object reference, so mutating it back is enough;
      // the next test's launch reads the restored value.
      config.useListenNotify = priorUseListenNotify;
    }
  });

  test('client-read-stream-nonexistent-workflow', async () => {
    // A stream on an unknown workflow raises rather than reading as empty, from a client as well as in-process.
    await DBOS.launch();
    await expect(client.readStream(randomUUID(), 's').next()).rejects.toThrow(DBOSNonExistentWorkflowError);
    await expect(DBOS.readStream(randomUUID(), 's').next()).rejects.toThrow(DBOSNonExistentWorkflowError);
  });

  test('client-read-stream-offset-and-checkpointing', async () => {
    // The client reads a single offset, waits for one the stream never reaches, and -- because it
    // reads its own system database, which knows nothing of the workflow that happens to be calling
    // -- records no steps even when a workflow calls it.
    const streamKey = 'client_offset_stream';
    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        await DBOS.writeStream(streamKey, 'v0');
        await DBOS.writeStream(streamKey, 'v1');
        await DBOS.closeStream(streamKey);
      },
      { name: 'client-offset-writer' },
    );
    const readerWorkflow = DBOS.registerWorkflow(
      async (targetID: string) => {
        const seen: unknown[] = [];
        for await (const value of client.readStream(targetID, streamKey)) {
          seen.push(value);
        }
        return [seen, await client.readStreamOffset(targetID, streamKey, 1)];
      },
      { name: 'client-offset-reader' },
    );
    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await writerWorkflow();
    });

    expect(await client.readStreamOffset(wfid, streamKey, 0)).toBe('v0');
    expect(await client.readStreamOffset(wfid, streamKey, 1)).toBe('v1');
    // The stream ends before this offset, so no value will ever arrive.
    await expect(client.readStreamOffset(wfid, streamKey, 5, { timeoutSeconds: 30 })).rejects.toThrow(
      DBOSStreamTimeoutError,
    );

    const readerID = randomUUID();
    await DBOS.withNextWorkflowID(readerID, async () => {
      expect(await readerWorkflow(wfid)).toEqual([['v0', 'v1'], 'v1']);
    });
    expect(await DBOS.listWorkflowSteps(readerID)).toEqual([]);
  });

  test('client-read-stream-timeout', async () => {
    // The client takes the same per-value timeout as the in-process reader.
    const streamKey = 'client_timeout_stream';
    let release!: () => void;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        await DBOS.writeStream(streamKey, 'only');
        // Stay active so a reader waits rather than draining to the end.
        await released;
      },
      { name: 'client-timeout-writer' },
    );
    await DBOS.launch();

    const wfid = randomUUID();
    const handle = await DBOS.withNextWorkflowID(wfid, async () => {
      return DBOS.startWorkflow(writerWorkflow, {})();
    });
    try {
      const reader = client.readStream<string>(wfid, streamKey, { timeoutSeconds: 0.3, pollingIntervalMs: 50 });
      expect((await reader.next()).value).toBe('only');
      await expect(reader.next()).rejects.toThrow(DBOSStreamTimeoutError);
    } finally {
      release();
      await handle.getResult();
    }
  });

  test('client-read-stream-is-one-round-trip-per-value', async () => {
    // The client reader fetches value and status in one joined query per tick, like the in-process one.
    const n = 25;
    const writerWorkflow = DBOS.registerWorkflow(
      async () => {
        for (let i = 0; i < n; i++) {
          await DBOS.writeStream('s', i);
        }
        await DBOS.closeStream('s');
      },
      { name: 'client-one-round-trip-writer' },
    );
    await DBOS.launch();

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await writerWorkflow();
    });

    const clientSysdb = (client as unknown as { systemDatabase: { pool: import('pg').Pool } }).systemDatabase;
    const spy = jest.spyOn(clientSysdb.pool, 'query');
    const values: unknown[] = [];
    let queryTexts: string[] = [];
    try {
      for await (const value of client.readStream(wfid, 's')) {
        values.push(value);
      }
      // Capture before mockRestore(), which clears spy.mock.calls.
      queryTexts = spy.mock.calls.map((c) =>
        typeof c[0] === 'string' ? c[0] : ((c[0] as { text?: string })?.text ?? ''),
      );
    } finally {
      spy.mockRestore();
    }

    expect(values).toEqual(Array.from({ length: n }, (_, i) => i));
    const joined = queryTexts.filter((text) => {
      const s = text.toLowerCase().replace(/\s+/g, ' ');
      return s.includes('outer join') && s.includes('streams') && s.includes('workflow_status');
    });
    // Guards against passing vacuously, then asserts one joined query per value plus the sentinel read.
    expect(joined.length).toBeGreaterThan(0);
    expect(joined.length).toBe(n + 1);
  });
});
