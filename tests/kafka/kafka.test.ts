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
const consumer = kafka.consumer({ groupId: 'test-group' })
let counter = 0;

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
    const producer = kafka.producer({
      createPartitioner: Partitioners.DefaultPartitioner
    });

    await producer.connect()
    await producer.send({
      topic: 'dbos-test-topic',
      messages: [
        { value: 'Hello KafkaJS user!' },
      ],
    })

    await producer.disconnect()

    await sleep(1000);

    await consumer.disconnect()

    expect(counter).toBe(1)
  }, 15000);
});

class DBOSTestClass {

  @DBOSInitializer()
  static async init(_ctx: InitContext) {
    await consumer.connect()
    await consumer.subscribe({ topic: 'dbos-test-topic', fromBeginning: true })
  }

  @KafkaConsume(consumer)
  @Transaction()
  static async testTxn(_ctxt: TransactionContext<Knex>, _topic: string, _partition: number, _message: KafkaMessage) {
    counter = counter + 1;
  }
}
