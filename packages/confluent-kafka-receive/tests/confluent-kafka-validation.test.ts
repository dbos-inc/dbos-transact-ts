import { suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { KafkaJS as ConfluentKafkaJS } from '@confluentinc/kafka-javascript';
import { ConfluentKafkaReceiver } from '..';

// Registration-time validation only: no broker and no database are needed, since nothing here
// launches DBOS or waits on a message.
const kafkaReceiver = new ConfluentKafkaReceiver({
  clientId: 'dbos-conf-kafka-validation-test',
  brokers: ['localhost:9092'],
  logLevel: 2,
});

// A distinct function object per registration: a function may only be registered as a workflow once.
function makeWorkflow(name: string) {
  const noop = async (_topic: string, _partition: number, _message: ConfluentKafkaJS.Message) => {
    await Promise.resolve();
  };
  return DBOS.registerWorkflow(noop, { name });
}

suite('confluent-kafka-receive-validation', async () => {
  await test('a custom queue is rejected with an ordering other than "none"', () => {
    const wf = makeWorkflow('confValidationOrderingQueue');
    assert.throws(
      () =>
        kafkaReceiver.registerConsumer(wf, 'some-topic', {
          name: 'confValidationOrderingQueue',
          ordering: 'partition',
          queueName: 'some-queue',
        }),
      /only supported with ordering/,
    );
  });

  await test('a non-positive batchSize is rejected', () => {
    const wf = makeWorkflow('confValidationBatchSize');
    assert.throws(
      () => kafkaReceiver.registerConsumer(wf, 'some-topic', { name: 'confValidationBatchSize', batchSize: 0 }),
      /batchSize must be a positive integer/,
    );
  });

  await test('an unknown ordering is rejected', () => {
    const wf = makeWorkflow('confValidationOrdering');
    assert.throws(
      () =>
        kafkaReceiver.registerConsumer(wf, 'some-topic', {
          name: 'confValidationOrdering',
          ordering: 'bogus' as 'none',
        }),
      /must be "none", "partition", or "topic"/,
    );
  });
}).catch(assert.fail);
