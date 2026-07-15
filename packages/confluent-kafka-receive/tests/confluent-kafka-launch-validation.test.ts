import { afterEach, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS, WorkflowQueue } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB } from './test-helpers';
import { KafkaJS as ConfluentKafkaJS } from '@confluentinc/kafka-javascript';
import { ConfluentKafkaReceiver } from '..';

// Validation that can only run once every queue is registered, so it lands at launch. It happens
// before any consumer connects, so these tests need a database but no broker.
const kafkaConfig = {
  clientId: 'dbos-conf-kafka-launchval-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  logLevel: 0,
};

const rand = () => Math.floor(Math.random() * 1_000_000_000);

// A distinct function object per registration: a function may only be registered as a workflow once,
// and each test starts from a cleared registry.
function makeWorkflow(name: string) {
  const noop = async (_topic: string, _partition: number, _message: ConfluentKafkaJS.Message) => {
    await Promise.resolve();
  };
  return DBOS.registerWorkflow(noop, { name });
}

suite('confluent-kafka-receive-launch-validation', async () => {
  const client = new Client({ user: 'postgres', database: 'postgres' });
  await client.connect();
  try {
    await dropDB(client, 'conf_kafka_launchval_test_dbos_sys', true);
  } finally {
    await client.end();
  }

  afterEach(async () => {
    // Clear the registry so the next test registers its own receiver and consumers from scratch.
    await DBOS.shutdown({ deregister: true });
  });

  await test('a partitioned custom queue is rejected at launch', { timeout: 30000 }, async () => {
    const receiver = new ConfluentKafkaReceiver(kafkaConfig);
    const queueName = `conf-partq-${rand()}`;
    new WorkflowQueue(queueName, { partitionQueue: true });
    receiver.registerConsumer(makeWorkflow('confPartQWf'), `t-${rand()}`, {
      name: 'confPartQWf',
      queueName,
      config: { 'group.id': `conf-partq-grp-${rand()}` },
    });

    DBOS.setConfig({ name: 'conf-kafka-launchval-test' });
    await assert.rejects(DBOS.launch(), /is a partitioned queue/);
  });

  await test(
    'a consumer function that is not a registered workflow is rejected at launch',
    { timeout: 30000 },
    async () => {
      // Regression: this fails identically for every message. Left to the batch loop, it would look
      // like an endless stream of poison messages, so every message would be dropped and its offset
      // committed — the whole topic silently discarded while the app reported healthy.
      const receiver = new ConfluentKafkaReceiver(kafkaConfig);
      const notAWorkflow = async (_topic: string, _partition: number, _message: ConfluentKafkaJS.Message) => {
        await Promise.resolve();
      };
      receiver.registerConsumer(notAWorkflow, `t-${rand()}`, {
        name: 'confNotAWorkflow',
        config: { 'group.id': `conf-unreg-grp-${rand()}` },
      });

      DBOS.setConfig({ name: 'conf-kafka-launchval-test' });
      await assert.rejects(DBOS.launch(), /is not a registered DBOS workflow/);
    },
  );

  await test('two consumers sharing a group and topic are rejected at launch', { timeout: 30000 }, async () => {
    const receiver = new ConfluentKafkaReceiver(kafkaConfig);
    const topic = `conf-dup-${rand()}`;
    const groupId = `conf-dup-grp-${rand()}`;
    receiver.registerConsumer(makeWorkflow('confDupA'), topic, { name: 'confDupA', config: { 'group.id': groupId } });
    receiver.registerConsumer(makeWorkflow('confDupB'), topic, { name: 'confDupB', config: { 'group.id': groupId } });

    DBOS.setConfig({ name: 'conf-kafka-launchval-test' });
    await assert.rejects(DBOS.launch(), /share group\.id .* and topic/s);
  });
}).catch(assert.fail);
