# DBOS Kafka Client Library (Confluent Version)

Publish/subscribe message queues are a common building block for distributed systems. Message queues allow processing to occur at a different place or time, perhaps in multiple client programming environments. Due to its performance, flexibility, and simple, scalable design, [Kafka](https://www.confluent.io/cloud-kafka) is a popular choice for publish/subscribe.

This package includes a [DBOS](https://docs.dbos.dev/) [step](https://docs.dbos.dev/typescript/tutorials/step-tutorial) for sending Kafka messages, as well as an event receiver for exactly-once processing of incoming messages (even using standard queues).

This package is based on [KafkaJS](https://kafka.js.org/). We are working on other client libraries for Kafka, please reach out to [us](https://www.dbos.dev/) if you are interested in a different client library.

## Configuring a DBOS Application with Kafka

Ensure that the DBOS Kafka (Confluent version) package is installed into the application:

```
npm install --save @dbos-inc/dbos-confluent-kafka
```

## Sending Messages

### Imports

First, ensure that the package classes are imported:

```typescript
import { KafkaProducer, KafkaConfig, logLevel } from '@dbos-inc/dbos-confluent-kafka';
```

### Selecting A Configuration

`KafkaProducer` is a configured class. This means that the configuration (or config file key name) must be provided when a class instance is created, for example:

```typescript
const kafkaConfig: KafkaConfig = {
  clientId: 'dbos-kafka-test',
  brokers: [`${process.env['KAFKA_BROKER'] ?? 'localhost:9092'}`],
  requestTimeout: 100, // FOR TESTING
  retry: {
    // FOR TESTING
    retries: 5,
  },
  logLevel: logLevel.INFO, // FOR TESTING
};

kafkaCfg = new KafkaProducer('defKafka', kafkaConfig, kafkaTopic);
```

### Sending

Within a [DBOS Workflow](https://docs.dbos.dev/typescript/tutorials/workflow-tutorial), call the `KafkaProducer` function from a workflow:

```typescript
const sendRes = await kafkaCfg.send({ value: ourMessage });
```

## Receiving Messages

A tutorial for receiving and processing Kafka messages can be found [here](https://docs.dbos.dev/typescript/tutorials/requestsandevents/kafka-integration). This library provides an alternate implementation of the Kafka consumer that can be updated independently of the DBOS Transact core packages.

## Simple Testing

The `confluent-kafkajs.test.ts` file included in the source repository demonstrates setting up topics, and sending and processing Kafka messages. Before running, set the following environment variables:

- `KAFKA_BROKER`: Broker URL

## Next Steps

- To start a DBOS app from a template, visit our [quickstart](https://docs.dbos.dev/quickstart).
- For DBOS programming tutorials, check out our [programming guide](https://docs.dbos.dev/typescript/programming-guide).
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
