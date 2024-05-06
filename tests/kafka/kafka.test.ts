import { KafkaConsume, TestingRuntime, Transaction, TransactionContext, Workflow, WorkflowContext } from "../../src";
import { DBOSConfig } from "../../src/dbos-executor";
import { Kafka } from "../../src/kafka/kafka";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { Kafka as KafkaJS, KafkaConfig, KafkaMessage, Partitioners, logLevel } from "kafkajs";
import { Knex } from "knex";

// These tests require local Kafka to run.
// Without it, they're automatically skipped.
// Quick guide on setting it up: https://kafka.js.org/docs/running-kafka-in-development

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
    return this.txnPromise;
  }

  @KafkaConsume(wfTopic)
  @Workflow()
  static async testWorkflow(_ctxt: WorkflowContext, topic: string, _partition: number, message: KafkaMessage) {
    if (topic == wfTopic && message.value?.toString() === wfMessage) {
      wfCounter = wfCounter + 1;
      DBOSTestClass.wfResolve()
    }
    return this.wfPromise;
  }
}
