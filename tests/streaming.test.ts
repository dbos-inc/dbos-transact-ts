import { DBOS } from '../src/';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { DBOSConfig } from '../src/dbos-executor';
import { randomUUID } from 'node:crypto';

describe('dbos-streaming-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
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
});
