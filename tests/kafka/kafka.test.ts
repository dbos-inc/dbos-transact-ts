import { TestingRuntime } from "../../src";
import { DBOSConfig } from "../../src/dbos-executor";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { Kafka, Partitioners } from "kafkajs";
import { sleep } from "../../src/utils";

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
    let counter = 0
    const kafka = new Kafka({
      clientId: 'my-app',
      brokers: ['localhost:9092'],
    })

    const producer = kafka.producer({
      createPartitioner: Partitioners.DefaultPartitioner
    });

    await producer.connect()
    await producer.send({
      topic: 'test-topic',
      messages: [
        { value: 'Hello KafkaJS user!' },
      ],
    })

    await producer.disconnect()

    const consumer = kafka.consumer({ groupId: 'test-group' })

    await consumer.connect()
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        counter += 1;
        console.log(topic)
        console.log(partition)
        console.log(message)
      },
    })

    await sleep(1000);

    await consumer.disconnect()

    expect(counter).toBe(1)
  }, 15000);
});

class DBOSTestClass {

}
