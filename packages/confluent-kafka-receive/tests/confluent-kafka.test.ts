import { after, afterEach, before, beforeEach, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { dropDB, withTimeout } from './test-helpers';
import { Kafka, KafkaConfig, Producer, Admin, ConfigResourceTypes } from 'kafkajs';
import { EventEmitter } from 'node:events';
import { ConfluentKafkaReceiver } from '..';
import { KafkaJS as ConfluentKafkaJS } from '@confluentinc/kafka-javascript';

const kafkaConfig = {
  clientId: 'dbos-conf-kafka-test',
  brokers: [process.env['KAFKA_BROKER'] ?? 'localhost:9092'],
  retry: { retries: 5 },
  logLevel: 2,
};

const kafkaReceiver = new ConfluentKafkaReceiver(kafkaConfig);

interface KafkaEvents {
  message: (functionName: string, topic: string, partition: number, message: ConfluentKafkaJS.Message) => void;
}

class KafkaEmitter extends EventEmitter {
  override on<K extends keyof KafkaEvents>(event: K, listener: KafkaEvents[K]): this {
    return super.on(event, listener);
  }

  override emit<K extends keyof KafkaEvents>(event: K, ...args: Parameters<KafkaEvents[K]>): boolean {
    DBOS.logger.info(`KafkaEmitter topic ${args[1]} partition ${args[2]}`);
    return super.emit(event, ...args);
  }
}

type KafkaMessageEvent = {
  topic: string;
  partition: number;
  message: ConfluentKafkaJS.Message;
};

function waitForMessage(
  emitter: KafkaEmitter,
  funcName: string,
  topic: string,
  timeoutMS = 45000,
): Promise<KafkaMessageEvent> {
  return withTimeout(
    new Promise<KafkaMessageEvent>((resolve) => {
      const handler = (f: string, t: string, partition: number, message: ConfluentKafkaJS.Message) => {
        if (f === funcName && t === topic) {
          emitter.off('message', handler);
          resolve({ topic: t, partition, message });
        }
      };
      emitter.on('message', handler);
    }),
    timeoutMS,
    `Timeout waiting for message for function ${funcName}`,
  );
}

class KafkaTestClass {
  static readonly emitter = new KafkaEmitter();

  @kafkaReceiver.consumer('string-topic')
  @DBOS.workflow()
  static async stringTopic(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'stringTopic', topic, partition, message);
  }

  @kafkaReceiver.consumer(/^regex-topic-.*/)
  @DBOS.workflow()
  static async regexTopic(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'regexTopic', topic, partition, message);
  }

  @kafkaReceiver.consumer(['a-topic', 'b-topic'])
  @DBOS.workflow()
  static async stringArrayTopic(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'stringArrayTopic', topic, partition, message);
  }

  @kafkaReceiver.consumer([/^z-topic-.*/, /^y-topic-.*/])
  @DBOS.workflow()
  static async regexArrayTopic(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'regexArrayTopic', topic, partition, message);
  }

  static async registeredConsumer(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    await Promise.resolve();
    KafkaTestClass.emitter.emit('message', 'registeredConsumer', topic, partition, message);
  }
}

KafkaTestClass.registeredConsumer = DBOS.registerWorkflow(KafkaTestClass.registeredConsumer, 'registeredConsumer');
kafkaReceiver.registerConsumer(KafkaTestClass.registeredConsumer, 'registered-topic');

async function validateKafka(config: KafkaConfig) {
  const kafka = new Kafka(config);
  const admin = kafka.admin();
  try {
    await admin.connect();
    await admin.listTopics();
    return true;
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    DBOS.logger.error(message);
    return false;
  } finally {
    await admin.disconnect();
  }
}

async function createTopics(admin: Admin, topics: string[]) {
  const existingTopics = await admin.listTopics();
  const topicsToCreate = topics.filter((t) => !existingTopics.includes(t));
  await admin.createTopics({
    topics: topicsToCreate.map((t) => ({
      topic: t,
      numPartitions: 1,
      replicationFactor: 1,
    })),
    timeout: 5000,
  });
}

async function purgeTopic(admin: Admin, topic: string) {
  const { resources } = await admin.describeConfigs({
    includeSynonyms: false,
    resources: [
      {
        type: ConfigResourceTypes.TOPIC,
        name: topic,
        configNames: ['retention.ms'],
      },
    ],
  });

  const resource = resources.find((r) => r.resourceName === topic);
  const configEntry = (resource?.configEntries ?? []).find((ce) => ce.configName === 'retention.ms');
  const retentionMS = configEntry?.configValue ?? '604800000';

  await admin.alterConfigs({
    validateOnly: false,
    resources: [
      {
        type: ConfigResourceTypes.TOPIC,
        name: topic,
        configEntries: [{ name: 'retention.ms', value: '0' }],
      },
    ],
  });

  await new Promise((resolve) => setTimeout(resolve, 2000));

  await admin.alterConfigs({
    validateOnly: false,
    resources: [
      {
        type: ConfigResourceTypes.TOPIC,
        name: topic,
        configEntries: [{ name: 'retention.ms', value: retentionMS }],
      },
    ],
  });

  await new Promise((resolve) => setTimeout(resolve, 2000));
}

// eslint-disable-next-line @typescript-eslint/no-floating-promises
suite('confluent-kafka-receive', async () => {
  const kafkaAvailable = await validateKafka(kafkaConfig);
  let producer: Producer | undefined = undefined;
  let admin: Admin | undefined = undefined;

  const testCases = [
    { topic: 'string-topic', functionName: 'stringTopic' },
    { topic: 'regex-topic-foo', functionName: 'regexTopic' },
    { topic: 'a-topic', functionName: 'stringArrayTopic' },
    { topic: 'b-topic', functionName: 'stringArrayTopic' },
    { topic: 'z-topic-foo', functionName: 'regexArrayTopic' },
    { topic: 'y-topic-foo', functionName: 'regexArrayTopic' },
    { topic: 'registered-topic', functionName: 'registeredConsumer' },
  ];

  before(async () => {
    if (!kafkaAvailable) {
      return;
    }

    const kafka = new Kafka(kafkaConfig);
    producer = kafka.producer();
    admin = kafka.admin();
    await Promise.all([producer.connect(), admin.connect()]);

    await createTopics(
      admin,
      testCases.map((tc) => tc.topic),
    );

    const client = new Client({ user: 'postgres', database: 'postgres' });
    try {
      await client.connect();
      await Promise.all([
        dropDB(client, 'conf_kafka_recv_test', true),
        dropDB(client, 'conf_kafka_recv_test_dbos_sys', true),
      ]);
    } finally {
      await client.end();
    }
  });

  after(async () => {
    await admin?.disconnect();
    await producer?.disconnect();
  });

  beforeEach(async () => {
    if (kafkaAvailable) {
      DBOS.setConfig({ name: 'conf-kafka-recv-test' });
      await DBOS.launch();
    }
  });

  afterEach(async () => {
    if (kafkaAvailable) {
      await DBOS.shutdown();
    }
  });

  for (const { functionName, topic } of testCases) {
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    test(`${topic}-${functionName}`, { skip: !kafkaAvailable, timeout: 125000 }, async () => {
      await purgeTopic(admin!, topic);
      const message = `test-message-${Date.now()}`;
      await producer!.send({ topic, messages: [{ value: message }] });
      const result = await waitForMessage(KafkaTestClass.emitter, functionName, topic, 120000);

      assert.equal(topic, result.topic);
      assert.equal(message, String(result.message.value));
    });
  }
});
