import { after, before, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { createRequire } from 'node:module';

import { DBOS, Error as DBOSErrors } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { Kafka, KafkaConfig, KafkaMessage, logLevel, Producer } from 'kafkajs';
import { KafkaReceiver } from '..';

// The durability guarantees live in #eachBatch's failure paths: a transient enqueue failure must
// be retried without losing or duplicating anything, and a single unprocessable message must be
// dropped rather than wedge the partition behind it. Both are driven here by patching the
// eventreceiver entry point the receiver calls into.
const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-kafka-failure-test',
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
const outageTopic = `dbos-outage-${suffix}`;
const poisonTopic = `dbos-poison-${suffix}`;
const NUM_MESSAGES = 5;
const POISON_VALUE = 2;

const outageRuns: number[] = [];
let resolveOutage: () => void;
const outageDone = new Promise<void>((r) => (resolveOutage = r));

const poisonRuns: number[] = [];
let resolvePoison: () => void;
// Every message except the poisoned one should run.
const poisonDone = new Promise<void>((r) => (resolvePoison = r));

let failuresInjected = 0;
let poisonInjected = 0;
let insertedTotal = 0;

async function outageWorkflow(_topic: string, _partition: number, message: KafkaMessage) {
  await Promise.resolve();
  outageRuns.push(Number(message.value!.toString()));
  if (outageRuns.length === NUM_MESSAGES) resolveOutage();
}

async function poisonWorkflow(_topic: string, _partition: number, message: KafkaMessage) {
  await Promise.resolve();
  poisonRuns.push(Number(message.value!.toString()));
  if (poisonRuns.length === NUM_MESSAGES - 1) resolvePoison();
}

kafkaReceiver.registerConsumer(DBOS.registerWorkflow(outageWorkflow, { name: 'outageWorkflow' }), outageTopic, {
  name: 'outageWorkflow',
  config: { groupId: `dbos-outage-grp-${suffix}` },
});
kafkaReceiver.registerConsumer(DBOS.registerWorkflow(poisonWorkflow, { name: 'poisonWorkflow' }), poisonTopic, {
  name: 'poisonWorkflow',
  config: { groupId: `dbos-poison-grp-${suffix}` },
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

suite('kafkajs-receive-failure-paths', async () => {
  const kafkaAvailable = await validateKafka(kafkaConfig);
  let producer: Producer | undefined = undefined;
  const originalInit = eventreceiver.initWorkflows;
  const originalPrepare = eventreceiver.prepareEnqueuedWorkflow;

  before(
    async () => {
      if (!kafkaAvailable) return;

      const kafka = new Kafka(kafkaConfig);
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [
          { topic: outageTopic, numPartitions: 1 },
          { topic: poisonTopic, numPartitions: 1 },
        ],
        timeout: 10000,
      });
      await admin.disconnect();

      producer = kafka.producer();
      await producer.connect();
      for (let i = 0; i < NUM_MESSAGES; i++) {
        await producer.send({ topic: outageTopic, messages: [{ value: String(i) }] });
        await producer.send({ topic: poisonTopic, messages: [{ value: String(i) }] });
      }

      // Simulate a database outage for the first three enqueue attempts. The backoff between them
      // is 1s + 2s + 4s, so the consumer takes ~7s to get through.
      // Scope both to the outage topic: the patch is global, and the poison consumer enqueues
      // through here too. The workflow ID carries the topic it came from.
      const isOutageBatch = (statuses: { workflowUUID: string }[]) =>
        statuses.every((s) => s.workflowUUID.includes(outageTopic));
      let failRemaining = 3;
      (eventreceiver as { initWorkflows: typeof originalInit }).initWorkflows = async (statuses) => {
        if (isOutageBatch(statuses) && failRemaining > 0) {
          failRemaining -= 1;
          failuresInjected += 1;
          throw new Error('Simulated database outage');
        }
        const inserted = await originalInit(statuses);
        for (const id of inserted) {
          if (id.includes(outageTopic)) insertedTotal += 1;
        }
        return inserted;
      };

      // Make exactly one message unbuildable, the way a message whose own content won't serialize
      // would be. Only DBOSInvalidWorkflowInputError is per-message; any other error applies to
      // every message, must not be dropped, and is covered by the launch-validation suite.
      (eventreceiver as { prepareEnqueuedWorkflow: typeof originalPrepare }).prepareEnqueuedWorkflow = async (
        wf,
        args,
        options,
      ) => {
        const message = (args as unknown as [string, number, KafkaMessage])[2];
        if (options.queueName.includes('kafkajs') && Number(message.value!.toString()) === POISON_VALUE) {
          const topic = (args as unknown as [string, number, KafkaMessage])[0];
          if (topic === poisonTopic) {
            poisonInjected++;
            throw new DBOSErrors.DBOSInvalidWorkflowInputError(
              'poisonWorkflow',
              new TypeError('Simulated unserializable message'),
            );
          }
        }
        return await originalPrepare(wf, args, options);
      };

      const client = new Client({ user: 'postgres', database: 'postgres' });
      try {
        await client.connect();
        await dropDB(client, 'kafka_failure_test_dbos_sys', true);
      } finally {
        await client.end();
      }

      DBOS.setConfig({ name: 'kafka-failure-test' });
      await DBOS.launch();
    },
    { timeout: 60000 },
  );

  after(
    async () => {
      (eventreceiver as { initWorkflows: typeof originalInit }).initWorkflows = originalInit;
      (eventreceiver as { prepareEnqueuedWorkflow: typeof originalPrepare }).prepareEnqueuedWorkflow = originalPrepare;
      if (!kafkaAvailable) return;
      await producer?.disconnect();
      await DBOS.shutdown();
    },
    { timeout: 30000 },
  );

  await test(
    'a transient enqueue failure is retried, losing and duplicating nothing',
    {
      skip: !kafkaAvailable,
      timeout: 60000,
    },
    async () => {
      await withTimeout(outageDone, 50000, 'Timeout: the consumer did not recover from the simulated outage');
      // The retry path really was exercised.
      assert.equal(failuresInjected, 3, 'the injected failures were never hit');
      // No loss and no duplicate execution despite the retried batch: each message ran exactly once.
      assert.deepEqual(
        [...outageRuns].sort((a, b) => a - b),
        Array.from({ length: NUM_MESSAGES }, (_, i) => i),
        `each message should run exactly once, got ${JSON.stringify(outageRuns)}`,
      );
      // The idempotent insert held: the retried batch created each row exactly once rather than
      // duplicating it.
      assert.equal(insertedTotal, NUM_MESSAGES, `expected ${NUM_MESSAGES} rows enqueued, got ${insertedTotal}`);
    },
  );

  await test(
    'an unprocessable message is dropped without wedging the ones behind it',
    {
      skip: !kafkaAvailable,
      timeout: 60000,
    },
    async () => {
      await withTimeout(poisonDone, 50000, 'Timeout: a poison message wedged the partition behind it');
      // Pin the injection: without this, a patch that stopped matching would leave the assertions
      // below passing on a run where nothing was ever poisoned.
      assert.ok(poisonInjected > 0, 'the injected poison message was never hit');
      // The poisoned message never ran, and every other message did — including the ones after it,
      // which a wedged partition would have blocked forever.
      assert.ok(!poisonRuns.includes(POISON_VALUE), 'the unprocessable message should not have run');
      assert.deepEqual(
        [...poisonRuns].sort((a, b) => a - b),
        Array.from({ length: NUM_MESSAGES }, (_, i) => i).filter((i) => i !== POISON_VALUE),
        `every other message should run, got ${JSON.stringify(poisonRuns)}`,
      );
    },
  );
}).catch(assert.fail);
