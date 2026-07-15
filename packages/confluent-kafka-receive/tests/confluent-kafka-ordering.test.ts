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
  /** Peak workflows in flight across the whole consumer. */
  maxActive = 0;
  /** Peak workflows in flight within a single partition: 1 iff that partition ran serially. */
  readonly maxActiveByPartition = new Map<number, number>();
  #active = 0;
  readonly #activeByPartition = new Map<number, number>();
  #resolve!: () => void;
  readonly done: Promise<void>;

  constructor(private readonly total: number) {
    this.done = new Promise<void>((r) => (this.#resolve = r));
  }

  enter(partition: number) {
    this.#active += 1;
    this.maxActive = Math.max(this.maxActive, this.#active);
    const n = (this.#activeByPartition.get(partition) ?? 0) + 1;
    this.#activeByPartition.set(partition, n);
    this.maxActiveByPartition.set(partition, Math.max(this.maxActiveByPartition.get(partition) ?? 0, n));
  }

  record(partition: number, value: number) {
    this.#active -= 1;
    this.#activeByPartition.set(partition, (this.#activeByPartition.get(partition) ?? 1) - 1);
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
  const value = Number(message.value!.toString());
  partitionCollector.enter(partition);
  // Sleep *shorter* the later the message, so if a partition's messages ever ran concurrently they
  // would finish in reverse. Serial execution is then the only way to observe offset order, and the
  // window is still wide enough to see cross-partition parallelism.
  await new Promise((r) => setTimeout(r, 300 - value * 50));
  partitionCollector.record(partition, value);
}

async function topicWorkflow(_topic: string, partition: number, message: ConfluentKafkaJS.Message) {
  const value = Number(message.value!.toString());
  topicCollector.enter(partition);
  await new Promise((r) => setTimeout(r, 150 - value * 40));
  topicCollector.record(partition, value);
}

async function customQueueWorkflow(_topic: string, partition: number, message: ConfluentKafkaJS.Message) {
  const value = Number(message.value!.toString());
  customQueueCollector.enter(partition);
  await new Promise((r) => setTimeout(r, 100));
  customQueueCollector.record(partition, value);
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
      for (let p = 0; p < NUM_PARTITIONS; p++) {
        // Each partition ran serially: never two of its messages in flight at once. This is the
        // claim; the order assertion below cannot make it on its own, because the dispatcher starts
        // workflows in created_at order regardless of concurrency.
        assert.equal(
          partitionCollector.maxActiveByPartition.get(p),
          1,
          `partition ${p} ran ${partitionCollector.maxActiveByPartition.get(p)} workflows at once; expected serial`,
        );
        // Kafka's guarantee: per-partition order is preserved exactly, across every batch boundary.
        assert.deepEqual(
          partitionCollector.byPartition.get(p),
          Array.from({ length: PER_PARTITION }, (_, i) => i),
          `partition ${p} was processed out of order`,
        );
      }
      // Partitions were processed in parallel with one another, so the serialization above is
      // per-partition rather than a global bottleneck.
      assert.ok(
        partitionCollector.maxActive > 1,
        `expected cross-partition parallelism, got maxActive=${partitionCollector.maxActive}`,
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
