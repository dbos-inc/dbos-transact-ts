/////////////////////////////
/* Kafka Decorators  */
/////////////////////////////

/**
 * @deprecated The `@KafkaConsume` decorator function has moved to an extension package.
 * Please install @dbos-inc/dbos-kafkajs, and change your import.
 */
export function KafkaConsume(_topics: unknown, _consumerConfig?: unknown, _queueName ?: unknown) : never {
  throw new Error("@KafkaConsume decorator has moved to the `@dbos-inc/dbos-kafkajs` package, please install and change the imports");
}

/**
 * @deprecated The `@Kafka` decorator function has moved to an extension package.
 * Please install @dbos-inc/dbos-kafkajs, and change your import.
 */
export function Kafka(_kafkaConfig: unknown) : never {
  throw new Error("@Kafka decorator has moved to the `@dbos-inc/dbos-kafkajs` package, please install and change the imports");
}