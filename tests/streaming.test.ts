import { DBOS } from '../src/';
import { generateDBOSTestConfig, reexecuteWorkflowById, setUpDBOSTestSysDb } from './helpers';
import { DBOSConfig } from '../src/dbos-executor';
import { randomUUID } from 'node:crypto';
import { DBOSClient } from '../src/client';

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
    await expect(DBOS.closeStream('test')).rejects.toThrow('Invalid call to `DBOS.closeStream` outside of a workflow');
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
  }, 10000);

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

    const workflowWithFailingStep = DBOS.registerWorkflow(
      async () => {
        // This step will fail 3 times, then succeed on the 4th attempt
        // But each failure should still write to the stream
        const result = await stepThatWritesAndFails('retry_stream', 'test_value');
        expect(result).toBe(0);

        // Also write directly from workflow
        await DBOS.writeStream('retry_stream', 'from_workflow');

        // Close the stream
        await DBOS.closeStream('retry_stream');
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
});
