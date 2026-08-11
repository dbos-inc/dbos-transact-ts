import { suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { KafkaMessage, logLevel } from 'kafkajs';
import { KafkaReceiver } from '..';

// Registration-time validation only: no broker and no database are needed, since nothing here
// launches DBOS or waits on a message.
const kafkaReceiver = new KafkaReceiver({
  clientId: 'dbos-kafka-validation-test',
  brokers: ['localhost:9092'],
  logLevel: logLevel.NOTHING,
});

// A distinct function object per registration: a function may only be registered as a workflow once.
function makeWorkflow(name: string) {
  const noop = async (_topic: string, _partition: number, _message: KafkaMessage) => {
    await Promise.resolve();
  };
  return DBOS.registerWorkflow(noop, { name });
}

suite('kafkajs-receive-validation', async () => {
  await test('a custom queue is rejected with an ordering other than "none"', () => {
    const wf = makeWorkflow('validationOrderingQueue');
    assert.throws(
      () =>
        kafkaReceiver.registerConsumer(wf, 'some-topic', {
          name: 'validationOrderingQueue',
          ordering: 'partition',
          queueName: 'some-queue',
        }),
      /only supported with ordering/,
    );
  });

  await test('a non-positive batchSize is rejected', () => {
    const wf = makeWorkflow('validationBatchSize');
    assert.throws(
      () => kafkaReceiver.registerConsumer(wf, 'some-topic', { name: 'validationBatchSize', batchSize: 0 }),
      /batchSize must be a positive integer/,
    );
    assert.throws(
      () => kafkaReceiver.registerConsumer(wf, 'some-topic', { name: 'validationBatchSize', batchSize: 1.5 }),
      /batchSize must be a positive integer/,
    );
  });

  await test('an unknown ordering is rejected', () => {
    const wf = makeWorkflow('validationOrdering');
    assert.throws(
      () =>
        kafkaReceiver.registerConsumer(wf, 'some-topic', {
          name: 'validationOrdering',
          ordering: 'bogus' as 'none',
        }),
      /must be "none", "partition", or "topic"/,
    );
  });
}).catch(assert.fail);
