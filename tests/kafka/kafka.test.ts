import { KafkaConsume, TestingRuntime, Transaction, TransactionContext, Workflow, WorkflowContext } from "../../src";
import { DBOSConfig } from "../../src/dbos-executor";
import { Kafka } from "../../src/kafka/kafka";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { Kafka as KafkaJS, KafkaConfig, KafkaMessage, Partitioners, logLevel } from "kafkajs";
import { Knex } from "knex";

// These tests require local Kafka to run.
// Without it, they're automatically skipped.
// Here's a docker-compose script you can use to set up local Kafka:

`
version: "3.7"
services:
  broker:
      image: bitnami/kafka:latest
      hostname: broker
      container_name: broker
      ports:
        - '9092:9092'
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
  brokers: ['localhost:9092'],
  requestTimeout: 100, // FOR TESTING
  retry: { // FOR TESTING
    retries: 5
  },
  logLevel: logLevel.NOTHING, // FOR TESTING
}
const kafka = new KafkaJS(kafkaConfig)
const patternTopic = new RegExp(/dbos-test-.*/);
let patternTopicCounter = 0;

const txnTopic = 'dbos-test-txn-topic';
const txnMessage = 'dbos-txn'
let txnCounter = 0;
const wfTopic = 'dbos-test-wf-topic';
const wfMessage = 'dbos-wf'
let wfCounter = 0;

describe("kafka-tests", () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;
  let kafkaIsAvailable = true;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);

    // Check if Kafka is available, skip the test if it's not
    const producer = kafka.producer({
      createPartitioner: Partitioners.DefaultPartitioner
    });
    try {
      await producer.connect();
      kafkaIsAvailable = true;
    } catch (error) {
      kafkaIsAvailable = false;
    } finally {
      await producer.disconnect();
    }
  }, 30000);

  beforeEach(async () => {
    if (kafkaIsAvailable) {
      testRuntime = await createInternalTestRuntime([DBOSTestClass], config);
    }
  }, 30000);

  afterEach(async () => {
    if (kafkaIsAvailable) {
      await testRuntime.destroy();
    }
  }, 30000);

  test("txn-kafka", async () => {
    if (!kafkaIsAvailable) {
      console.log("Kafka unavailable, skipping Kafka tests")
      return
    }
    // Create a producer to send a message
    const producer = kafka.producer({
      createPartitioner: Partitioners.DefaultPartitioner
    });
    await producer.connect()
    await producer.send({
      topic: txnTopic,
      messages: [
        { value: txnMessage },
      ],
    })
    await producer.send({
      topic: wfTopic,
      messages: [
        { value: wfMessage },
      ],
    })
    await producer.disconnect()

    // Check that both messages are consumed
    await DBOSTestClass.txnPromise;
    expect(txnCounter).toBe(1);
    await DBOSTestClass.wfPromise;
    expect(wfCounter).toBe(1);
    await DBOSTestClass.patternTopicPromise;
    expect(patternTopicCounter).toBe(2);
  }, 30000);
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

  @KafkaConsume(txnTopic)
  @Transaction()
  static async testTxn(_ctxt: TransactionContext<Knex>, topic: string, _partition: number, message: KafkaMessage) {
    if (topic == txnTopic && message.value?.toString() === txnMessage) {
      txnCounter = txnCounter + 1;
      DBOSTestClass.txnResolve();
    }
    await DBOSTestClass.txnPromise;
  }

  @KafkaConsume(wfTopic)
  @Workflow()
  static async testWorkflow(_ctxt: WorkflowContext, topic: string, _partition: number, message: KafkaMessage) {
    if (topic == wfTopic && message.value?.toString() === wfMessage) {
      wfCounter = wfCounter + 1;
      DBOSTestClass.wfResolve();
    }
    await DBOSTestClass.wfPromise;
  }

  @KafkaConsume(patternTopic)
  @Workflow()
  static async testConsumeTopicsByPattern(_ctxt: WorkflowContext, topic: string, _partition: number, message: KafkaMessage) {
    const isWfMessage = topic == wfTopic && message.value?.toString() === wfMessage;
    const isTxnMessage = txnTopic && message.value?.toString() === txnMessage;
    if ( isWfMessage || isTxnMessage ) {
      patternTopicCounter = patternTopicCounter + 1;
      if (patternTopicCounter === 2) {
        DBOSTestClass.patternTopicResolve();
      }
    }
    await DBOSTestClass.patternTopicPromise;
  }
}
