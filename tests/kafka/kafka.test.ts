import { DBOSInitializer, InitContext, KafkaConsume, TestingRuntime, Transaction, TransactionContext, Workflow, WorkflowContext } from "../../src";
import { DBOSConfig } from "../../src/dbos-executor";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { Kafka, KafkaMessage, Partitioners, logLevel } from "kafkajs";
import { Knex } from "knex";

// These tests require local Kafka to run.
// Without it, they're automatically skipped.
// Quick guide on setting it up: https://kafka.js.org/docs/running-kafka-in-development

const kafka = new Kafka({
  clientId: 'dbos-kafka-test',
  brokers: ['localhost:9092'],
  requestTimeout: 100, // FOR TESTING
  retry: { // FOR TESTING
    retries: 5
  },
  logLevel: logLevel.NOTHING, // FOR TESTING
})
const txnTopic = 'dbos-test-txn-topic';
const txnMessage = 'dbos-txn'
const txnConsumer = kafka.consumer({ groupId: 'dbos-test-txn-group' })
let txnCounter = 0;
const wfTopic = 'dbos-test-wf-topic';
const wfMessage = 'dbos-wf'
const wfConsumer = kafka.consumer({ groupId: 'dbos-test-wf-group' })
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
  });

  beforeEach(async () => {
    if (kafkaIsAvailable) {
      testRuntime = await createInternalTestRuntime([DBOSTestClass], config);
    }
  });

  afterEach(async () => {
    if (kafkaIsAvailable) {
      await testRuntime.destroy();
    }
  });

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

    // Clean up the consumers
    await txnConsumer.disconnect();
    await wfConsumer.disconnect();
  }, 20000);
});

class DBOSTestClass {

  static txnResolve: () => void;
  static txnPromise = new Promise<void>((r) => {
    DBOSTestClass.txnResolve = r;
  });

  static wfResolve: () => void;
  static wfPromise = new Promise<void>((r) => {
    DBOSTestClass.wfResolve = r;
  });

  @DBOSInitializer()
  static async init(_ctx: InitContext) {
    await txnConsumer.connect()
    await txnConsumer.subscribe({ topic: txnTopic, fromBeginning: true })
    await wfConsumer.connect()
    await wfConsumer.subscribe({ topic: wfTopic, fromBeginning: true })
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @KafkaConsume(txnConsumer)
  @Transaction()
  static async testTxn(_ctxt: TransactionContext<Knex>, topic: string, _partition: number, message: KafkaMessage) {
    if (topic == txnTopic && message.value?.toString() === txnMessage) {
      txnCounter = txnCounter + 1;
      DBOSTestClass.txnResolve()
    }
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @KafkaConsume(wfConsumer)
  @Workflow()
  static async testWorkflow(_ctxt: WorkflowContext, topic: string, _partition: number, message: KafkaMessage) {
    if (topic == wfTopic && message.value?.toString() === wfMessage) {
      wfCounter = wfCounter + 1;
      DBOSTestClass.wfResolve()
    }
  }
}
