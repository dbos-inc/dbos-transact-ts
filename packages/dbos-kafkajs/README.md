# DBOS Kafka Library (KafkaJS Version)

Publish/subscribe message queues are a common building block for distributed systems.  Message queues allow processing to occur at a different place or time, perhaps in multiple client programming environments.  Due to its performance, flexibility, and simple, scalable design, [Kafka](https://www.confluent.io/cloud-kafka) is a popular choice for publish/subscribe.

This package includes a [DBOS](https://docs.dbos.dev/) [communicator](https://docs.dbos.dev/tutorials/communicator-tutorial) for sending Kafka messages, as well as an event receiver for exactly-once processing of incoming messages (even using standard queues).

This package is based on [KafkaJS](https://kafka.js.org/).  We are working on other client libraries for Kafka, please reach out to [us](https://www.dbos.dev/) if you are interested in a different client library.

## Configuring a DBOS Application with Kafka
Ensure that the DBOS SQS package is installed into the application:
```
npm install --save @dbos-inc/dbos-kafkajs
```

## Sending Messages

### Imports
First, ensure that the communicator and associated classes are imported:
```typescript
import {
  KafkaConfig,
  logLevel,
  KafkaProduceCommunicator,
  Partitioners,
} from "@dbos-inc/dbos-kafkajs";
```

### Selecting A Configuration
`KafkaProduceCommunicator` is a configured class.  This means that the configuration (or config file key name) must be provided when a class instance is created, for example:
```typescript
const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-kafka-test',
  brokers: [`${process.env['KAFKA_BROKER'] ?? 'localhost:9092'}`],
  requestTimeout: 100, // FOR TESTING
  retry: { // FOR TESTING
    retries: 5
  },
  logLevel: logLevel.NOTHING, // FOR TESTING
}

kafkaCfg = configureInstance(KafkaProduceCommunicator, 'defKafka', kafkaConfig, defTopic, {
    createPartitioner: Partitioners.DefaultPartitioner
});
```

### Sending
Within a [DBOS Transact Workflow](https://docs.dbos.dev/tutorials/workflow-tutorial), invoke the `KafkaProduceCommunicator` function from the workflow context:
```typescript   
const sendRes = await wfCtx.invoke(kafkaCfg).sendMessage({value: ourMessage});
```

## Receiving Messages
A tutorial for receiving and processing Kafka messages can be found [here](https://docs.dbos.dev/tutorials/kafka-integration).

## Simple Testing
The `kafkajs.test.ts` file included in the source repository demonstrates sending and processing Kafka messages.  Before running, set the following environment variables:
- `KAFKA_BROKER`: Broker URL

## Next Steps
- For a detailed DBOS Transact tutorial, check out our [programming quickstart](https://docs.dbos.dev/getting-started/quickstart-programming).
- To learn how to deploy your application to DBOS Cloud, visit our [cloud quickstart](https://docs.dbos.dev/getting-started/quickstart-cloud/)
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
