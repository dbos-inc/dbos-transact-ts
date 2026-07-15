import { after, before, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { createRequire } from 'node:module';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { Kafka, KafkaConfig, logLevel, Producer } from 'kafkajs';
import { KafkaJS as ConfluentKafkaJS } from '@confluentinc/kafka-javascript';
import { ConfluentKafkaReceiver } from '..';

// The receiver's core promise: a Kafka offset advances only once its message is durably enqueued,
// so a crash costs redelivery rather than data. This receiver keeps that promise differently from
// the kafkajs one — it stores offsets and leaves librdkafka's auto-commit to flush them — so the
// property needs driving against a real broker here too, by failing every enqueue, killing the
// consumer mid-retry, and asking Kafka itself what it committed.
//
// The broker-side setup uses kafkajs (as the sibling suites do); only the receiver under test is
// the Confluent one.
const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-conf-kafka-durability-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  retry: { retries: 5 },
  logLevel: logLevel.NOTHING,
};

const kafkaReceiver = new ConfluentKafkaReceiver({
  clientId: 'dbos-conf-kafka-durability-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  logLevel: 0,
});

// The receiver reads these off the module's live exports on every call, so resolving the same
// CommonJS module object it does — rather than an ES namespace, whose bindings are read-only — is
// what lets the test drive its failure paths by reassigning them.
const eventreceiver = createRequire(__filename)(
  '@dbos-inc/dbos-sdk/eventreceiver',
) as typeof import('@dbos-inc/dbos-sdk/eventreceiver');

const suffix = Math.floor(Math.random() * 1_000_000_000);
const durableTopic = `dbos-conf-durable-${suffix}`;
// Stable across both launches, so the second run rejoins the group the first one left.
const durableGroup = `dbos-conf-durable-grp-${suffix}`;
const NUM_MESSAGES = 5;

const runs: number[] = [];
let resolveAll: () => void;
const allDone = new Promise<void>((r) => (resolveAll = r));

let enqueueAttempts = 0;
let outageActive = true;

async function durableWorkflow(_topic: string, _partition: number, message: ConfluentKafkaJS.Message) {
  await Promise.resolve();
  runs.push(Number(message.value!.toString()));
  if (runs.length === NUM_MESSAGES) resolveAll();
}

kafkaReceiver.registerConsumer(DBOS.registerWorkflow(durableWorkflow, { name: 'confDurableWorkflow' }), durableTopic, {
  name: 'confDurableWorkflow',
  config: { 'group.id': durableGroup },
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
async function waitUntil(predicate: () => boolean | Promise<boolean>, timeoutMS: number, message: string) {
  const deadline = Date.now() + timeoutMS;
  while (!(await predicate())) {
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

suite('confluent-kafka-receive-durability', async () => {
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
        await dropDB(client, 'conf_kafka_durability_test_dbos_sys', true);
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
      DBOS.setConfig({ name: 'conf-kafka-durability-test' });
      await DBOS.launch();
      // The consumer has read the batch and is retrying the enqueue that will never succeed.
      await waitUntil(() => enqueueAttempts >= 2, 60000, 'Timeout: the enqueue was never attempted');
      assert.equal(runs.length, 0, 'no workflow should run while every enqueue is failing');

      // Abort mid-retry. Nothing was stored, so the commit librdkafka makes as it closes — the one
      // most likely to outrun durable state — has nothing to flush.
      await DBOS.shutdown();

      // Ask the broker directly. A committed offset is the next message the group will read, so
      // anything above 0 is a message consumed and acknowledged but never enqueued. Unlike the
      // kafkajs suite this floors at 0 rather than -1: librdkafka commits its position as it
      // closes, which is a rewind to the first message, not an acknowledgement of one.
      const committed = await committedOffset();
      assert.ok(committed <= 0, `offset ${committed} was committed for messages that were never durably enqueued`);
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
      // Auto-commit flushes the stored offsets on its own timer, so poll rather than race it.
      await waitUntil(
        async () => (await committedOffset()) === NUM_MESSAGES,
        30000,
        'the offset never advanced once the batch was durable, so the backlog would be reprocessed on restart',
      );
    },
  );
}).catch(assert.fail);
