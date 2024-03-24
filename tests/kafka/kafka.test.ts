import { DBOSInitializer, InitContext, KafkaConsume, TestingRuntime, Transaction, TransactionContext } from "../../src";
import { DBOSConfig } from "../../src/dbos-executor";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { Kafka, KafkaMessage, Partitioners } from "kafkajs";
import { sleep } from "../../src/utils";
import { Knex } from "knex";

const kafka = new Kafka({
  clientId: 'dbos-kafka-test',
  brokers: ['localhost:9092'],
})
const txnTopic = 'dbos-test-txn-topic';
const txnMessage = 'dbos-txn'
const txnConsumer = kafka.consumer({ groupId: 'dbos-test-group' })
let txnCounter = 0;

describe("kafka-tests", () => {
  let username: string;
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    username = config.poolConfig.user || "postgres";
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createInternalTestRuntime([DBOSTestClass], config);
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("simple-kafka", async () => {
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
    await producer.disconnect()

    // Check that the message is consumed
    await DBOSTestClass.txnPromise;
    expect(txnCounter).toBe(1)

  // Clean up the consumer
    await txnConsumer.disconnect()
  }, 15000);
});

class DBOSTestClass {

  static txnResolve: () => void;
  static txnPromise = new Promise<void>((r) => {
    DBOSTestClass.txnResolve = r;
  });

  @DBOSInitializer()
  static async init(_ctx: InitContext) {
    await txnConsumer.connect()
    await txnConsumer.subscribe({ topic: txnTopic, fromBeginning: true })
  }

  @KafkaConsume(txnConsumer)
  @Transaction()
  static async testTxn(_ctxt: TransactionContext<Knex>, topic: string, _partition: number, message: KafkaMessage) {
    if (topic == txnTopic && message.value?.toString() == txnMessage) {
      txnCounter = txnCounter + 1;
      DBOSTestClass.txnResolve()
    }
  }
}
