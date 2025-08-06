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

    const writerWorkflow = DBOS.registerWorkflow(async (streamKey: string, testValues: unknown[]) => {
      for (const value of testValues) {
        await DBOS.writeStream(streamKey, value);
      }
      await DBOS.closeStream(streamKey);
    });
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
});
