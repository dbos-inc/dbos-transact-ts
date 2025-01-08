import {
  TestingRuntime,
  createTestingRuntime,
  configureInstance,
  WorkflowContext,
  Workflow,
} from "@dbos-inc/dbos-sdk";

import {
  KafkaProduceCommunicator,
  CKafkaConsume,
  CKafka,
  KafkaConfig,
  Message,
  logLevel,
}
from "./index";

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
`

const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-kafka-test',
  brokers: [`${process.env['KAFKA_BROKER'] ?? 'localhost:9092'}`],
  requestTimeout: 100, // FOR TESTING
  retry: { // FOR TESTING
    retries: 5
  },
  logLevel: logLevel.INFO,
}

const wf1Topic = 'dbos-test-wf-topic';
const wf2Topic = 'dbos-test-wf-topic2';
const wfMessage = 'dbos-wf'
let wfCounter = 0;

const patternTopic = new RegExp(/dbos-test-.*/);
let patternTopicCounter = 0;

const arrayTopics = [new RegExp(/dbos-test-wf-topic/)];
let arrayTopicsCounter = 0;

describe("kafka-tests", () => {
  let testRuntime: TestingRuntime | undefined = undefined;
  let kafkaIsAvailable = true;
  let wfKafkaCfg: KafkaProduceCommunicator | undefined = undefined;
  let wf2KafkaCfg: KafkaProduceCommunicator | undefined = undefined;

  beforeAll(async () => {
    // Check if Kafka is available, skip the test if it's not
    if (process.env['KAFKA_BROKER']) {
      kafkaIsAvailable = true;
    } else {
      kafkaIsAvailable = false;
    }

    // This would normally be a global or static or something
    wfKafkaCfg = configureInstance(KafkaProduceCommunicator, 'wfKafka', kafkaConfig, wf1Topic);
    wf2KafkaCfg = configureInstance(KafkaProduceCommunicator, 'wf2Kafka', kafkaConfig, wf2Topic);
    return Promise.resolve();
  }, 30000);

  beforeEach(async () => {
    if (kafkaIsAvailable) {
      testRuntime = await createTestingRuntime(undefined, 'confluentkafka-test-dbos-config.yaml');
    }
  }, 30000);

  afterEach(async () => {
    if (kafkaIsAvailable) {
      await testRuntime?.destroy();
    }
  }, 30000);

  test("txn-kafka", async () => {
    if (!kafkaIsAvailable) {
      console.log("Kafka unavailable, skipping Kafka tests")
      return
    }
    else {
      console.log("Kafka tests running...")
    }
    // Create a producer to send a message
    await testRuntime?.invoke(wf2KafkaCfg!).sendMessage({
      value: wfMessage,
    });
    await testRuntime?.invoke(wfKafkaCfg!).sendMessage({
      value: wfMessage,
    });
    console.log("Messages sent");

    // Check that both messages are consumed
    await DBOSTestClass.txnPromise;
    await DBOSTestClass.wfPromise;
    expect(wfCounter).toBe(1);
    await DBOSTestClass.patternTopicPromise;
    expect(patternTopicCounter).toBe(2);
    await DBOSTestClass.arrayTopicsPromise;
    expect(arrayTopicsCounter).toBe(2);
  }, 30000);
});

@CKafka(kafkaConfig)
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

  @CKafkaConsume(wf1Topic)
  @Workflow()
  static async testWorkflow(_ctxt: WorkflowContext, topic: string, _partition: number, message: Message) {
    console.log(`got something 1 ${topic}`);
    if (topic == wf1Topic && message.value?.toString() === wfMessage) {
      wfCounter = wfCounter + 1;
      DBOSTestClass.wfResolve();
    }
    await DBOSTestClass.wfPromise;
  }

  @CKafkaConsume(patternTopic)
  @Workflow()
  static async testConsumeTopicsByPattern(_ctxt: WorkflowContext, topic: string, _partition: number, message: Message) {
    console.log(`got something 2 ${topic}`);
    const isWfMessage = topic == wf1Topic && message.value?.toString() === wfMessage;
    const isWf2Message = topic == wf2Topic && message.value?.toString() === wfMessage;
    if ( isWfMessage || isWf2Message ) {
      patternTopicCounter = patternTopicCounter + 1;
      if (patternTopicCounter === 2) {
        DBOSTestClass.patternTopicResolve();
      }
    }
    await DBOSTestClass.patternTopicPromise;
  }

  @CKafkaConsume(arrayTopics)
  @Workflow()
  static async testConsumeTopicsArray(_ctxt: WorkflowContext, topic: string, _partition: number, message: Message) {
    console.log(`got something 3 ${topic}`);
    const isWfMessage = topic == wf1Topic && message.value?.toString() === wfMessage;
    const isWf2Message = topic == wf2Topic && message.value?.toString() === wfMessage;
    if ( isWfMessage || isWf2Message) {
      arrayTopicsCounter = arrayTopicsCounter + 1;
      if (arrayTopicsCounter === 2) {
        DBOSTestClass.arrayTopicsResolve();
      }
    }
    await DBOSTestClass.arrayTopicsPromise;
  }
}
