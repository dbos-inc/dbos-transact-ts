import { suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB } from './test-helpers';
import { Consumer, Kafka, KafkaConfig, KafkaMessage, logLevel } from 'kafkajs';
import { KafkaReceiver } from '..';

// KafkaJS restarts itself only for retriable crashes. A non-retriable one — a TypeError and
// friends, the most common accidental bug — yields CRASH{restart:false} and leaves the consumer
// stopped and disconnected for good, silently halting the whole consumer group. The receiver
// rebuilds it; this pins that behavior.
const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-kafka-crash-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  logLevel: logLevel.NOTHING,
};

const rand = () => Math.floor(Math.random() * 1_000_000_000);

type CrashListener = (event: { payload: { error: Error; groupId: string; restart: boolean } }) => void;

/**
 * Wrap `Kafka.prototype.consumer` so the test can see every consumer the receiver builds and reach
 * the CRASH listener it installs. There is no other seam: the receiver owns its Kafka client and
 * keeps its consumers private.
 */
function spyOnConsumers() {
  // eslint-disable-next-line @typescript-eslint/unbound-method -- captured to restore and to call with an explicit `this`
  const original = Kafka.prototype.consumer;
  const created: Consumer[] = [];
  const crashListeners: CrashListener[] = [];
  let connects = 0;

  Kafka.prototype.consumer = function (this: Kafka, config) {
    const consumer = original.call(this, config);
    created.push(consumer);
    const originalConnect = consumer.connect.bind(consumer);
    consumer.connect = async () => {
      connects += 1;
      return await originalConnect();
    };
    const originalOn = consumer.on.bind(consumer);
    consumer.on = ((event: string, listener: unknown) => {
      if (event === consumer.events.CRASH) crashListeners.push(listener as CrashListener);
      return originalOn(event as never, listener as never);
    }) as typeof consumer.on;
    return consumer;
  };

  return {
    created,
    crashListeners,
    connects: () => connects,
    restore: () => {
      Kafka.prototype.consumer = original;
    },
  };
}

suite('kafkajs-receive-crash-recovery', async () => {
  const client = new Client({ user: 'postgres', database: 'postgres' });
  await client.connect();
  try {
    await dropDB(client, 'kafka_crash_test_dbos_sys', true);
  } finally {
    await client.end();
  }

  await test('a non-retriable crash rebuilds the consumer', { timeout: 60000 }, async () => {
    const spy = spyOnConsumers();
    try {
      const receiver = new KafkaReceiver(kafkaConfig);
      const handler = async (_topic: string, _partition: number, _message: KafkaMessage) => {
        await Promise.resolve();
      };
      receiver.registerConsumer(DBOS.registerWorkflow(handler, { name: 'crashWf' }), `dbos-crash-${rand()}`, {
        name: 'crashWf',
        config: { groupId: `crash-grp-${rand()}` },
      });

      DBOS.setConfig({ name: 'kafka-crash-test' });
      await DBOS.launch();

      assert.equal(spy.created.length, 1, 'expected one consumer at launch');
      assert.equal(spy.connects(), 1);
      assert.equal(spy.crashListeners.length, 1, 'receiver did not install a CRASH listener');

      // KafkaJS restarts this one itself, so the receiver must not build a second.
      spy.crashListeners[0]({ payload: { error: new Error('retriable'), groupId: 'g', restart: true } });
      await new Promise((r) => setTimeout(r, 2000));
      assert.equal(spy.created.length, 1, 'receiver rebuilt a consumer KafkaJS was already restarting');

      // KafkaJS gives up on this one, so the receiver must rebuild it.
      spy.crashListeners[0]({ payload: { error: new TypeError('boom'), groupId: 'g', restart: false } });
      // #recreateConsumer backs off one MIN_RETRY_WAIT_MS (1s) before its first attempt.
      for (let i = 0; i < 40 && spy.created.length < 2; i++) {
        await new Promise((r) => setTimeout(r, 250));
      }
      assert.equal(spy.created.length, 2, 'receiver did not rebuild the permanently crashed consumer');
      assert.equal(spy.connects(), 2, 'the rebuilt consumer was never connected');
    } finally {
      spy.restore();
      // Shut down first: that aborts the receiver, so the disconnects below can't be mistaken for
      // fresh crashes and rebuilt. Only then clean up the consumer the receiver was told to forget,
      // which its own destroy() no longer tracks.
      await DBOS.shutdown({ deregister: true });
      await Promise.allSettled(spy.created.map((c) => c.disconnect()));
    }
  });
}).catch(assert.fail);
