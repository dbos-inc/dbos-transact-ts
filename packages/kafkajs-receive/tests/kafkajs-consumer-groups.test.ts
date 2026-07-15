import { after, before, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { Kafka, KafkaConfig, KafkaMessage, logLevel, Producer } from 'kafkajs';
import { KafkaReceiver } from '..';

// Several consumer groups may share a topic: each group gets its own copy of every message. The
// group ID is baked into both the workflow ID and the ordering partition key to keep the groups
// from colliding, and nothing else pins that.
const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-kafka-groups-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  retry: { retries: 5 },
  logLevel: logLevel.NOTHING,
};

const kafkaReceiver = new KafkaReceiver(kafkaConfig);

const suffix = Math.floor(Math.random() * 1_000_000_000);
const sharedTopic = `dbos-groups-${suffix}`;
const orderedTopic = `dbos-groups-ordered-${suffix}`;
const NUM_MESSAGES = 4;
const ORDERED_PER_PARTITION = 4;

/** Collects one group's view of a topic and resolves once it has seen `total` messages. */
class GroupView {
  readonly seen: number[] = [];
  #resolve!: () => void;
  readonly done: Promise<void>;
  constructor(private readonly total: number) {
    this.done = new Promise<void>((r) => (this.#resolve = r));
  }
  record(value: number) {
    this.seen.push(value);
    if (this.seen.length === this.total) this.#resolve();
  }
}

const groupA = new GroupView(NUM_MESSAGES);
const groupB = new GroupView(NUM_MESSAGES);
const orderedA = new GroupView(ORDERED_PER_PARTITION);
const orderedB = new GroupView(ORDERED_PER_PARTITION);

async function groupAWorkflow(_topic: string, _partition: number, message: KafkaMessage) {
  await Promise.resolve();
  groupA.record(Number(message.value!.toString()));
}
async function groupBWorkflow(_topic: string, _partition: number, message: KafkaMessage) {
  await Promise.resolve();
  groupB.record(Number(message.value!.toString()));
}

// Two *ordered* consumers on one topic. Their partition keys must differ by group, or they would
// share a key on the shared partitioned queue (concurrency=1 per key) and serialize against each
// other — the limitation this port set out to remove.
async function orderedAWorkflow(_topic: string, _partition: number, message: KafkaMessage) {
  await new Promise((r) => setTimeout(r, 150));
  orderedA.record(Number(message.value!.toString()));
}
async function orderedBWorkflow(_topic: string, _partition: number, message: KafkaMessage) {
  await new Promise((r) => setTimeout(r, 150));
  orderedB.record(Number(message.value!.toString()));
}

kafkaReceiver.registerConsumer(DBOS.registerWorkflow(groupAWorkflow, { name: 'groupAWorkflow' }), sharedTopic, {
  name: 'groupAWorkflow',
  config: { groupId: `dbos-groups-a-${suffix}` },
});
kafkaReceiver.registerConsumer(DBOS.registerWorkflow(groupBWorkflow, { name: 'groupBWorkflow' }), sharedTopic, {
  name: 'groupBWorkflow',
  config: { groupId: `dbos-groups-b-${suffix}` },
});
kafkaReceiver.registerConsumer(DBOS.registerWorkflow(orderedAWorkflow, { name: 'orderedAWorkflow' }), orderedTopic, {
  name: 'orderedAWorkflow',
  ordering: 'partition',
  config: { groupId: `dbos-groups-ord-a-${suffix}` },
});
kafkaReceiver.registerConsumer(DBOS.registerWorkflow(orderedBWorkflow, { name: 'orderedBWorkflow' }), orderedTopic, {
  name: 'orderedBWorkflow',
  ordering: 'partition',
  config: { groupId: `dbos-groups-ord-b-${suffix}` },
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

suite('kafkajs-receive-consumer-groups', async () => {
  const kafkaAvailable = await validateKafka(kafkaConfig);
  let producer: Producer | undefined = undefined;

  before(
    async () => {
      if (!kafkaAvailable) return;

      const kafka = new Kafka(kafkaConfig);
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [
          { topic: sharedTopic, numPartitions: 1 },
          { topic: orderedTopic, numPartitions: 1 },
        ],
        timeout: 10000,
      });
      await admin.disconnect();

      producer = kafka.producer();
      await producer.connect();
      for (let i = 0; i < NUM_MESSAGES; i++) {
        await producer.send({ topic: sharedTopic, messages: [{ value: String(i) }] });
      }
      for (let i = 0; i < ORDERED_PER_PARTITION; i++) {
        await producer.send({ topic: orderedTopic, messages: [{ partition: 0, value: String(i) }] });
      }

      const client = new Client({ user: 'postgres', database: 'postgres' });
      try {
        await client.connect();
        await dropDB(client, 'kafka_groups_test_dbos_sys', true);
      } finally {
        await client.end();
      }

      DBOS.setConfig({ name: 'kafka-groups-test' });
      await DBOS.launch();
    },
    { timeout: 60000 },
  );

  after(
    async () => {
      if (!kafkaAvailable) return;
      await producer?.disconnect();
      await DBOS.shutdown();
    },
    { timeout: 30000 },
  );

  await test(
    'each consumer group receives every message on a shared topic',
    {
      skip: !kafkaAvailable,
      timeout: 60000,
    },
    async () => {
      // The group ID is part of the workflow ID. Were it not, both groups would compute the same IDs
      // and the batch insert would dedup them away, so whichever group polled second would silently
      // process nothing.
      await withTimeout(
        Promise.all([groupA.done, groupB.done]),
        50000,
        'Timeout: a consumer group did not receive every message',
      );
      const expected = Array.from({ length: NUM_MESSAGES }, (_, i) => i);
      assert.deepEqual(
        [...groupA.seen].sort((a, b) => a - b),
        expected,
        'group A did not see every message',
      );
      assert.deepEqual(
        [...groupB.seen].sort((a, b) => a - b),
        expected,
        'group B did not see every message',
      );
    },
  );

  await test(
    'two ordered consumer groups on one topic do not block each other',
    {
      skip: !kafkaAvailable,
      timeout: 60000,
    },
    async () => {
      // The group ID is part of the partition key, so the two groups occupy different keys on the
      // shared partitioned queue and run concurrently while each stays in offset order.
      await withTimeout(
        Promise.all([orderedA.done, orderedB.done]),
        50000,
        'Timeout: ordered consumer groups did not both drain',
      );
      const expected = Array.from({ length: ORDERED_PER_PARTITION }, (_, i) => i);
      assert.deepEqual(orderedA.seen, expected, 'ordered group A lost its offset order');
      assert.deepEqual(orderedB.seen, expected, 'ordered group B lost its offset order');
    },
  );
}).catch(assert.fail);
