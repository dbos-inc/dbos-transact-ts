import { afterEach, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS, WorkflowQueue } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB } from './test-helpers';
import { KafkaConfig, KafkaMessage, logLevel } from 'kafkajs';
import { KafkaReceiver } from '..';

// Validation that can only run once every queue is registered, so it lands at launch. It happens
// before any consumer connects, so these tests need a database but no broker.
const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-kafka-launchval-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  logLevel: logLevel.NOTHING,
};

const rand = () => Math.floor(Math.random() * 1_000_000_000);

// A distinct function object per registration: a function may only be registered as a workflow once,
// and each test starts from a cleared registry.
function makeWorkflow(name: string) {
  const noop = async (_topic: string, _partition: number, _message: KafkaMessage) => {
    await Promise.resolve();
  };
  return DBOS.registerWorkflow(noop, { name });
}

suite('kafkajs-receive-launch-validation', async () => {
  const client = new Client({ user: 'postgres', database: 'postgres' });
  await client.connect();
  try {
    await dropDB(client, 'kafka_launchval_test_dbos_sys', true);
  } finally {
    await client.end();
  }

  afterEach(async () => {
    // Clear the registry so the next test registers its own receiver and consumers from scratch.
    await DBOS.shutdown({ deregister: true });
  });

  await test('a partitioned custom queue is rejected at launch', { timeout: 30000 }, async () => {
    const receiver = new KafkaReceiver(kafkaConfig);
    const queueName = `partq-${rand()}`;
    new WorkflowQueue(queueName, { partitionQueue: true });
    receiver.registerConsumer(makeWorkflow('partQWf'), `t-${rand()}`, {
      name: 'partQWf',
      queueName,
      config: { groupId: `partq-grp-${rand()}` },
    });

    DBOS.setConfig({ name: 'kafka-launchval-test' });
    await assert.rejects(DBOS.launch(), /is a partitioned queue/);
  });

  await test('two consumers sharing a group and topic are rejected at launch', { timeout: 30000 }, async () => {
    const receiver = new KafkaReceiver(kafkaConfig);
    const topic = `dup-${rand()}`;
    const groupId = `dup-grp-${rand()}`;
    receiver.registerConsumer(makeWorkflow('dupA'), topic, { name: 'dupA', config: { groupId } });
    receiver.registerConsumer(makeWorkflow('dupB'), topic, { name: 'dupB', config: { groupId } });

    DBOS.setConfig({ name: 'kafka-launchval-test' });
    await assert.rejects(DBOS.launch(), /share group\.id .* and topic/s);
  });

  await test('a consumer may name a queue that does not exist yet', { timeout: 30000 }, async () => {
    // The queue can be registered after launch, so naming an unknown one must not fail launch.
    const receiver = new KafkaReceiver(kafkaConfig);
    receiver.registerConsumer(makeWorkflow('unknownQWf'), `t-${rand()}`, {
      name: 'unknownQWf',
      queueName: `never-registered-${rand()}`,
      config: { groupId: `unknownq-grp-${rand()}` },
    });

    DBOS.setConfig({ name: 'kafka-launchval-test' });
    await DBOS.launch();
  });
}).catch(assert.fail);
