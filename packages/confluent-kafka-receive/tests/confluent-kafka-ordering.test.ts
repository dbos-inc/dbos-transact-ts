import { after, before, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS, WorkflowQueue } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { Kafka, KafkaConfig, logLevel, Producer } from 'kafkajs';
import { KafkaJS as ConfluentKafkaJS } from '@confluentinc/kafka-javascript';
import { ConfluentKafkaReceiver } from '..';

// The broker-side setup uses kafkajs (as the sibling suite does); only the receiver under test is
// the Confluent one.
const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-conf-kafka-ordering-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  retry: { retries: 5 },
  logLevel: logLevel.NOTHING,
};

const kafkaReceiver = new ConfluentKafkaReceiver({
  clientId: 'dbos-conf-kafka-ordering-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  logLevel: 0,
});

const suffix = Math.floor(Math.random() * 1_000_000_000);
const partitionTopic = `dbos-conf-order-part-${suffix}`;
const topicOrderTopic = `dbos-conf-order-topic-${suffix}`;
const customQueueTopic = `dbos-conf-order-customq-${suffix}`;

const NUM_PARTITIONS = 4;
const PER_PARTITION = 5;
const PARTITION_TOTAL = NUM_PARTITIONS * PER_PARTITION;

/** Resolves once `total` messages have been recorded, so a test can await completion. */
class Collector {
  readonly byPartition = new Map<number, number[]>();
  active = 0;
  maxActive = 0;
  #resolve!: () => void;
  readonly done: Promise<void>;

  constructor(private readonly total: number) {
    this.done = new Promise<void>((r) => (this.#resolve = r));
  }

  enter() {
    this.active += 1;
    this.maxActive = Math.max(this.maxActive, this.active);
  }

  record(partition: number, value: number) {
    this.active -= 1;
    const list = this.byPartition.get(partition) ?? [];
    list.push(value);
    this.byPartition.set(partition, list);
    if (this.count() === this.total) this.#resolve();
  }

  count() {
    let n = 0;
    for (const v of this.byPartition.values()) n += v.length;
    return n;
  }
}

const partitionCollector = new Collector(PARTITION_TOTAL);
const topicCollector = new Collector(3 * 3);
const customQueueCollector = new Collector(5);

// A small batchSize splits each partition's backlog across several batches, which reorder unless
// created_at is monotonic per partition key across batches.
async function partitionWorkflow(_topic: string, partition: number, message: ConfluentKafkaJS.Message) {
  partitionCollector.enter();
  // Widen the window so cross-partition parallelism is observable.
  await new Promise((r) => setTimeout(r, 300));
  partitionCollector.record(partition, Number(message.value!.toString()));
}

async function topicWorkflow(_topic: string, partition: number, message: ConfluentKafkaJS.Message) {
  topicCollector.enter();
  await new Promise((r) => setTimeout(r, 100));
  topicCollector.record(partition, Number(message.value!.toString()));
}

async function customQueueWorkflow(_topic: string, partition: number, message: ConfluentKafkaJS.Message) {
  customQueueCollector.enter();
  await new Promise((r) => setTimeout(r, 100));
  customQueueCollector.record(partition, Number(message.value!.toString()));
}

const customQueueName = `conf-kafka-custom-q-${suffix}`;
new WorkflowQueue(customQueueName, { concurrency: 1 });

const registeredPartition = DBOS.registerWorkflow(partitionWorkflow, { name: 'confPartitionWorkflow' });
kafkaReceiver.registerConsumer(registeredPartition, partitionTopic, {
  name: 'confPartitionWorkflow',
  ordering: 'partition',
  batchSize: 3,
  config: { 'group.id': `dbos-conf-order-part-grp-${suffix}` },
});

const registeredTopic = DBOS.registerWorkflow(topicWorkflow, { name: 'confTopicWorkflow' });
kafkaReceiver.registerConsumer(registeredTopic, topicOrderTopic, {
  name: 'confTopicWorkflow',
  ordering: 'topic',
  config: { 'group.id': `dbos-conf-order-topic-grp-${suffix}` },
});

const registeredCustomQueue = DBOS.registerWorkflow(customQueueWorkflow, { name: 'confCustomQueueWorkflow' });
kafkaReceiver.registerConsumer(registeredCustomQueue, customQueueTopic, {
  name: 'confCustomQueueWorkflow',
  queueName: customQueueName,
  config: { 'group.id': `dbos-conf-order-customq-grp-${suffix}` },
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

suite('confluent-kafka-receive-ordering', async () => {
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
          { topic: partitionTopic, numPartitions: NUM_PARTITIONS },
          { topic: topicOrderTopic, numPartitions: 3 },
          { topic: customQueueTopic, numPartitions: 1 },
        ],
        timeout: 10000,
      });
      await admin.disconnect();

      producer = kafka.producer();
      await producer.connect();
      for (let p = 0; p < NUM_PARTITIONS; p++) {
        for (let i = 0; i < PER_PARTITION; i++) {
          await producer.send({ topic: partitionTopic, messages: [{ partition: p, value: String(i) }] });
        }
      }
      for (let p = 0; p < 3; p++) {
        for (let i = 0; i < 3; i++) {
          await producer.send({ topic: topicOrderTopic, messages: [{ partition: p, value: String(i) }] });
        }
      }
      for (let i = 0; i < 5; i++) {
        await producer.send({ topic: customQueueTopic, messages: [{ value: String(i) }] });
      }

      const client = new Client({ user: 'postgres', database: 'postgres' });
      try {
        await client.connect();
        await dropDB(client, 'conf_kafka_order_test_dbos_sys', true);
      } finally {
        await client.end();
      }

      DBOS.setConfig({ name: 'conf-kafka-order-test' });
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
    'partition ordering is serial per partition and parallel across them',
    { skip: !kafkaAvailable, timeout: 90000 },
    async () => {
      await withTimeout(partitionCollector.done, 80000, 'Timeout waiting for partition-ordered messages');
      // Kafka's guarantee: per-partition order is preserved exactly, across every batch boundary.
      for (let p = 0; p < NUM_PARTITIONS; p++) {
        assert.deepEqual(
          partitionCollector.byPartition.get(p),
          Array.from({ length: PER_PARTITION }, (_, i) => i),
          `partition ${p} was processed out of order`,
        );
      }
      // Partitions were processed in parallel with one another.
      assert.ok(
        partitionCollector.maxActive >= 2,
        `expected parallelism, got maxActive=${partitionCollector.maxActive}`,
      );
    },
  );

  await test('topic ordering runs the whole topic serially', { skip: !kafkaAvailable, timeout: 90000 }, async () => {
    await withTimeout(topicCollector.done, 80000, 'Timeout waiting for topic-ordered messages');
    for (let p = 0; p < 3; p++) {
      assert.deepEqual(topicCollector.byPartition.get(p), [0, 1, 2], `partition ${p} was processed out of order`);
    }
    // The whole topic ran serially: never two workflows at once.
    assert.equal(topicCollector.maxActive, 1);
  });

  await test(
    'a custom queue runs the consumer and honors its concurrency limit',
    { skip: !kafkaAvailable, timeout: 90000 },
    async () => {
      await withTimeout(customQueueCollector.done, 80000, 'Timeout waiting for custom-queue messages');
      assert.deepEqual(
        [...(customQueueCollector.byPartition.get(0) ?? [])].sort((a, b) => a - b),
        [0, 1, 2, 3, 4],
      );
      assert.equal(customQueueCollector.maxActive, 1); // concurrency=1 honored
    },
  );
}).catch(assert.fail);
