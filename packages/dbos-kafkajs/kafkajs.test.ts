import {
  DBOS,
  TestingRuntime,
  Transaction,
  TransactionContext,
  Workflow,
  WorkflowContext,
  WorkflowQueue,
  createTestingRuntime, // This test intentionally tests v1 and v2
  parseConfigFile,
} from '@dbos-inc/dbos-sdk';

import { KafkaConfig, Kafka, KafkaConsume, logLevel, KafkaProduceStep, KafkaMessage, Partitioners } from './index';
import { setUpDBOSTestDb } from '@dbos-inc/dbos-sdk/tests/helpers';
import { DBOSConfigInternal } from '@dbos-inc/dbos-sdk/dist/src/dbos-executor';

import { Knex } from 'knex';

// These tests require local Kafka to run.
// Without it, they're automatically skipped.
// Here's a docker-compose script you can use to set up local Kafka:

const _ = `
version: "3.7"
services:
  broker:
      image: bitnami/kafka:latest
      hostname: broker
      container_name: broker
      ports:
        - '9092:9092'
        - '29093:29093'
        - '19092:19092'
      environment:
        KAFKA_CFG_NODE_ID: 1
        KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
        KAFKA_CFG_ADVERTISED_LISTENERS: 'PLAINTEXT_HOST://localhost:9092,PLAINTEXT://broker:19092'
        KAFKA_CFG_PROCESS_ROLES: 'broker,controller'
        KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: '1@broker:29093'
        KAFKA_CFG_LISTENERS: 'CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092'
        KAFKA_CFG_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
        KAFKA_CFG_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
`;

const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-kafka-test',
  brokers: [`${process.env['KAFKA_BROKER'] ?? 'localhost:9092'}`],
  requestTimeout: 100, // FOR TESTING
  retry: {
    // FOR TESTING
    retries: 5,
  },
  logLevel: logLevel.NOTHING, // FOR TESTING
};

const wfq = new WorkflowQueue('kafkaq', 2);

const txnTopic = 'dbos-test-txn-topic';
const txnMessage = 'dbos-txn';
let txnCounter = 0;

const wfTopic = 'dbos-test-wf-topic';
const wfMessage = 'dbos-wf';
let wfCounter = 0;

const patternTopic = new RegExp(/dbos-test-.*/);
let patternTopicCounter = 0;

const arrayTopics = [txnTopic, new RegExp(/dbos-test-wf-topic/)];
let arrayTopicsCounter = 0;

describe('kafka-tests', () => {
  let testRuntime: TestingRuntime | undefined = undefined;
  let kafkaIsAvailable = true;
  let wfKafkaCfg: KafkaProduceStep | undefined = undefined;
  let txKafkaCfg: KafkaProduceStep | undefined = undefined;

  beforeAll(async () => {
    // Check if Kafka is available, skip the test if it's not
    if (process.env['KAFKA_BROKER']) {
      kafkaIsAvailable = true;
      const [config] = parseConfigFile({ configfile: 'kafkajs-test-dbos-config.yaml' }) as [
        DBOSConfigInternal,
        unknown,
      ];
      await setUpDBOSTestDb(config);
    } else {
      kafkaIsAvailable = false;
    }

    return Promise.resolve();
  }, 30000);

  beforeEach(async () => {
    if (kafkaIsAvailable) {
      // This would normally be a global or static or something
      wfKafkaCfg = new KafkaProduceStep('wfKafka', kafkaConfig, wfTopic, {
        createPartitioner: Partitioners.DefaultPartitioner,
      });
      txKafkaCfg = new KafkaProduceStep('txKafka', kafkaConfig, txnTopic, {
        createPartitioner: Partitioners.DefaultPartitioner,
      });

      testRuntime = await createTestingRuntime(undefined, 'kafkajs-test-dbos-config.yaml');
    }
  }, 30000);

  afterEach(async () => {
    if (kafkaIsAvailable) {
      await testRuntime?.destroy();
      await wfKafkaCfg?.disconnect();
      await txKafkaCfg?.disconnect();
    }
  }, 30000);

  test('txn-kafka', async () => {
    if (!kafkaIsAvailable) {
      console.log('Kafka unavailable, skipping Kafka tests');
      return;
    }

    // Send messages
    await testRuntime?.invoke(txKafkaCfg!).sendMessage({ value: txnMessage }); // v1 API
    await wfKafkaCfg!.send({ value: wfMessage }); // v2 API

    // Check that both messages are consumed
    await DBOSTestClass.txnPromise;
    expect(txnCounter).toBe(1);
    await DBOSTestClass.wfPromise;
    expect(wfCounter).toBe(1);
    await DBOSTestClass.patternTopicPromise;
    expect(patternTopicCounter).toBe(2);
    await DBOSTestClass.arrayTopicsPromise;
    expect(arrayTopicsCounter).toBe(2);
  }, 60000);
});

@Kafka(kafkaConfig)
class DBOSTestClass {
  static txnResolve: () => void;
  static txnPromise = new Promise<void>((r) => {
    DBOSTestClass.txnResolve = r;
  });

  static wfResolve: () => void;
  static wfPromise = new Promise<void>((r) => {
    DBOSTestClass.wfResolve = r;
  });

  static patternTopicResolve: () => void;
  static patternTopicPromise = new Promise<void>((r) => {
    DBOSTestClass.patternTopicResolve = r;
  });

  static arrayTopicsResolve: () => void;
  static arrayTopicsPromise = new Promise<void>((r) => {
    DBOSTestClass.arrayTopicsResolve = r;
  });

  @KafkaConsume(txnTopic)
  @Transaction()
  static async testTxn(_ctxt: TransactionContext<Knex>, topic: string, _partition: number, message: KafkaMessage) {
    if (topic === txnTopic && message.value?.toString() === txnMessage) {
      txnCounter = txnCounter + 1;
      DBOSTestClass.txnResolve();
    }
    await DBOSTestClass.txnPromise;
  }

  @KafkaConsume(wfTopic)
  @Workflow()
  static async testWorkflow(_ctxt: WorkflowContext, topic: string, _partition: number, message: KafkaMessage) {
    if (topic === wfTopic && message.value?.toString() === wfMessage) {
      wfCounter = wfCounter + 1;
      DBOSTestClass.wfResolve();
    }
    await DBOSTestClass.wfPromise;
  }

  @KafkaConsume(patternTopic, undefined, wfq.name)
  @DBOS.workflow()
  static async testConsumeTopicsByPattern(topic: string, _partition: number, message: KafkaMessage) {
    const isWfMessage = topic === wfTopic && message.value?.toString() === wfMessage;
    const isTxnMessage = topic === txnTopic && message.value?.toString() === txnMessage;
    if (isWfMessage || isTxnMessage) {
      patternTopicCounter = patternTopicCounter + 1;
      if (patternTopicCounter === 2) {
        DBOSTestClass.patternTopicResolve();
      }
    }
    await DBOSTestClass.patternTopicPromise;
  }

  @KafkaConsume(arrayTopics)
  @Workflow()
  static async testConsumeTopicsArray(
    _ctxt: WorkflowContext,
    topic: string,
    _partition: number,
    message: KafkaMessage,
  ) {
    const isWfMessage = topic === wfTopic && message.value?.toString() === wfMessage;
    const isTxnMessage = topic === txnTopic && message.value?.toString() === txnMessage;
    if (isWfMessage || isTxnMessage) {
      arrayTopicsCounter = arrayTopicsCounter + 1;
      if (arrayTopicsCounter === 2) {
        DBOSTestClass.arrayTopicsResolve();
      }
    }
    await DBOSTestClass.arrayTopicsPromise;
  }
}
