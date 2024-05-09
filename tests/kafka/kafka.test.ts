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
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - 2181:2181

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - 9092:9092
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_LISTENERS: PLAINTEXT://:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
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

  @KafkaConsume(txnTopic)
  @Transaction()
  static async testTxn(_ctxt: TransactionContext<Knex>, topic: string, _partition: number, message: KafkaMessage) {
    if (topic == txnTopic && message.value?.toString() === txnMessage) {
      txnCounter = txnCounter + 1;
      DBOSTestClass.txnResolve()
    }
    await DBOSTestClass.txnPromise;
  }

  @KafkaConsume(wfTopic)
  @Workflow()
  static async testWorkflow(_ctxt: WorkflowContext, topic: string, _partition: number, message: KafkaMessage) {
    if (topic == wfTopic && message.value?.toString() === wfMessage) {
      wfCounter = wfCounter + 1;
      DBOSTestClass.wfResolve()
    }
    await DBOSTestClass.wfPromise;
  }
}
