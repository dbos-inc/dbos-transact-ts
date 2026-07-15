import { after, before, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { createRequire } from 'node:module';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { Kafka, KafkaConfig, KafkaMessage, logLevel, Producer } from 'kafkajs';
import { KafkaReceiver } from '..';

// The receiver's core promise: a Kafka offset advances only once its message is durably enqueued,
// so a crash costs redelivery rather than data. Drive it against a real broker by failing every
// enqueue, killing the consumer mid-retry, and asking Kafka itself what it committed. Any offset
// committed there is a message that would never run again.
const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-kafka-durability-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  retry: { retries: 5 },
  logLevel: logLevel.NOTHING,
};

const kafkaReceiver = new KafkaReceiver(kafkaConfig);

// The receiver reads these off the module's live exports on every call, so reassigning them here is
// what lets the test drive its failure paths. Resolve the same CommonJS module object the receiver
// does rather than an ES namespace import, whose bindings are read-only.
const eventreceiver = createRequire(__filename)(
  '@dbos-inc/dbos-sdk/eventreceiver',
) as typeof import('@dbos-inc/dbos-sdk/eventreceiver');

const suffix = Math.floor(Math.random() * 1_000_000_000);
const durableTopic = `dbos-durable-${suffix}`;
// Stable across both launches, so the second run rejoins the group the first one left.
const durableGroup = `dbos-durable-grp-${suffix}`;
const NUM_MESSAGES = 5;

const runs: number[] = [];
let resolveAll: () => void;
const allDone = new Promise<void>((r) => (resolveAll = r));

let enqueueAttempts = 0;
let outageActive = true;

async function durableWorkflow(_topic: string, _partition: number, message: KafkaMessage) {
  await Promise.resolve();
  runs.push(Number(message.value!.toString()));
  if (runs.length === NUM_MESSAGES) resolveAll();
}

kafkaReceiver.registerConsumer(DBOS.registerWorkflow(durableWorkflow, { name: 'durableWorkflow' }), durableTopic, {
  name: 'durableWorkflow',
  config: { groupId: durableGroup },
});

async function validateKafka(config: KafkaConfig) {
  const kafka = new Kafka(config);
  const admin = kafka.admin();
  try {
    await admin.connect();
    await admin.listTopics();
    return true;
  } catch {
    return false;
  } finally {
    await admin.disconnect();
  }
}

/** Poll until `predicate` holds, so the test waits on the condition itself rather than a guess. */
async function waitUntil(predicate: () => boolean, timeoutMS: number, message: string) {
  const deadline = Date.now() + timeoutMS;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(message);
    await new Promise((r) => setTimeout(r, 50));
  }
}

/** What Kafka has committed for the consumer group, or -1 when it has committed nothing. */
async function committedOffset(): Promise<number> {
  const kafka = new Kafka(kafkaConfig);
  const admin = kafka.admin();
  try {
    await admin.connect();
    const [entry] = await admin.fetchOffsets({ groupId: durableGroup, topics: [durableTopic] });
    return Number(entry.partitions[0].offset);
  } finally {
    await admin.disconnect();
  }
}

suite('kafkajs-receive-durability', async () => {
  const kafkaAvailable = await validateKafka(kafkaConfig);
  let producer: Producer | undefined = undefined;
  const originalInit = eventreceiver.initWorkflows;

  before(
    async () => {
      if (!kafkaAvailable) return;

      const kafka = new Kafka(kafkaConfig);
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({ topics: [{ topic: durableTopic, numPartitions: 1 }], timeout: 10000 });
      await admin.disconnect();

      producer = kafka.producer();
      await producer.connect();
      for (let i = 0; i < NUM_MESSAGES; i++) {
        await producer.send({ topic: durableTopic, messages: [{ value: String(i) }] });
      }

      // Fail every enqueue for this topic until the outage is lifted. Scope it to this topic: the
      // patch is global, and the workflow ID carries the topic it came from.
      (eventreceiver as { initWorkflows: typeof originalInit }).initWorkflows = async (statuses) => {
        if (outageActive && statuses.every((s) => s.workflowUUID.includes(durableTopic))) {
          enqueueAttempts += 1;
          throw new Error('Simulated database outage');
        }
        return await originalInit(statuses);
      };

      const client = new Client({ user: 'postgres', database: 'postgres' });
      try {
        await client.connect();
        await dropDB(client, 'kafka_durability_test_dbos_sys', true);
      } finally {
        await client.end();
      }
    },
    { timeout: 60000 },
  );

  after(async () => {
    (eventreceiver as { initWorkflows: typeof originalInit }).initWorkflows = originalInit;
    await DBOS.shutdown({ deregister: true });
    await producer?.disconnect();
  });

  await test(
    'a shutdown mid-outage commits no offset, so nothing is consumed without being enqueued',
    { skip: !kafkaAvailable, timeout: 90000 },
    async () => {
      DBOS.setConfig({ name: 'kafka-durability-test' });
      await DBOS.launch();
      // The consumer has read the batch and is retrying the enqueue that will never succeed.
      await waitUntil(() => enqueueAttempts >= 2, 60000, 'Timeout: the enqueue was never attempted');
      assert.equal(runs.length, 0, 'no workflow should run while every enqueue is failing');

      // Abort mid-retry: the enqueue never went durable, so the batch must go unresolved.
      await DBOS.shutdown();

      // Ask the broker directly. -1 is "this group has committed nothing": every message is still
      // owed to us. Any other value is a message consumed and acknowledged but never enqueued.
      assert.equal(
        await committedOffset(),
        -1,
        'an offset was committed for messages that were never durably enqueued',
      );
    },
  );

  await test(
    'the messages are redelivered and run exactly once once the outage lifts',
    { skip: !kafkaAvailable, timeout: 90000 },
    async () => {
      outageActive = false;
      await DBOS.launch();

      await withTimeout(allDone, 60000, 'Timeout: the redelivered messages never ran');
      // Nothing lost, and the deterministic workflow IDs mean nothing ran twice.
      assert.deepEqual(
        [...runs].sort((a, b) => a - b),
        Array.from({ length: NUM_MESSAGES }, (_, i) => i),
        `every message should run exactly once, got ${JSON.stringify(runs)}`,
      );
      assert.equal(await committedOffset(), NUM_MESSAGES, 'the offset should advance once the batch is durable');
    },
  );
}).catch(assert.fail);
