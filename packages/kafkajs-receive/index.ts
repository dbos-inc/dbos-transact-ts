import { DBOS, DBOSLifecycleCallback, Error as DBOSErrors, FunctionName, WorkflowQueue } from '@dbos-inc/dbos-sdk';
import {
  getOrCreateQueue,
  getQueue,
  initWorkflows,
  prepareEnqueuedWorkflow,
  PreparedWorkflow,
  registerPollerQueue,
} from '@dbos-inc/dbos-sdk/eventreceiver';
import {
  Kafka as KafkaJS,
  Consumer,
  ConsumerConfig,
  EachBatchPayload,
  KafkaConfig,
  KafkaMessage,
  KafkaJSProtocolError,
} from 'kafkajs';

export type KafkaArgs = [string, number, KafkaMessage];
type KafkaMessageHandler<Return> = (...args: KafkaArgs) => Promise<Return>;

/**
 * How to order the workflows a consumer starts.
 *  - `none` (default): messages are processed in parallel.
 *  - `partition`: serial per topic partition (Kafka's delivery-order guarantee), parallel across partitions.
 *  - `topic`: serial per topic.
 */
export type KafkaOrdering = 'none' | 'partition' | 'topic';

/** Queue for ordering="none" consumers that don't name their own queue. */
const KAFKA_QUEUE_NAME = '_dbos_kafkajs_queue';
/** Shared partitioned queue for ordered consumers: concurrency=1 is enforced per partition key. */
const KAFKA_ORDERED_QUEUE_NAME = '_dbos_kafkajs_ordered_queue';

const DEFAULT_BATCH_SIZE = 250;
const MIN_RETRY_WAIT_MS = 1000;
const MAX_RETRY_WAIT_MS = 60000;

const sleepms = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const finish = () => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', finish);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    signal?.addEventListener('abort', finish, { once: true });
  });

interface KafkaMethodConfig {
  topics?: Array<string | RegExp>;
  config?: ConsumerConfig;
  /** Custom queue the consumer's workflows run on, if the caller named one. */
  queueName?: string;
  ordering?: KafkaOrdering;
  batchSize?: number;
  /** Queue this consumer actually enqueues onto: the custom queue, or an internal one. */
  consumerQueueName?: string;
}

interface KafkaRetryConfig {
  maxRetries: number;
  retryTime: number;
  multiplier: number;
}

/** Everything needed to (re)build one consumer. */
interface ConsumerSpec {
  func: KafkaMessageHandler<unknown>;
  topics: Array<string | RegExp>;
  config: ConsumerConfig;
  ordering: KafkaOrdering;
  batchSize: number;
  queueName: string;
}

export interface KafkaConsumerOptions {
  queueName?: string;
  config?: ConsumerConfig;
  /** Ordering guarantee for this consumer's workflows. Defaults to `none`. */
  ordering?: KafkaOrdering;
  /** Maximum number of messages durably enqueued per batch. Defaults to 250. */
  batchSize?: number;
}

function safeGroupName(className: string, methodName: string, topics: Array<string | RegExp>) {
  const safeGroupIdPart = [className, methodName, ...topics]
    .map((r) => r.toString())
    .map((r) => r.replaceAll(/[^a-zA-Z0-9\\-]/g, ''))
    .join('-');
  return `dbos-kafka-group-${safeGroupIdPart}`.slice(0, 255);
}

/** Several receivers in one process share the internal queues, so resolve rather than construct. */
function getKafkaQueue(): WorkflowQueue {
  return getOrCreateQueue(KAFKA_QUEUE_NAME);
}

function getKafkaOrderedQueue(): WorkflowQueue {
  // One shared partitioned queue: concurrency=1 is enforced per partition key, so execution is
  // serial per key and parallel across keys.
  return getOrCreateQueue(KAFKA_ORDERED_QUEUE_NAME, { partitionQueue: true, concurrency: 1 });
}

function partitionKeyFor(
  ordering: KafkaOrdering,
  groupId: string,
  topic: string,
  partition: number,
): string | undefined {
  switch (ordering) {
    case 'partition':
      return `${groupId}:${topic}:${partition}`;
    case 'topic':
      return `${groupId}:${topic}`;
    default:
      return undefined;
  }
}

/**
 * Rethrow an error that cannot be blamed on one message.
 *
 * Building a workflow row fails per message only when that message's own content won't serialize.
 * Every other cause — the consumer's function not being a registered workflow, DBOS being torn
 * down — fails identically for every message, so dropping is never right: it would discard the
 * entire stream and commit the offsets, losing the data with nothing but a log line.
 */
function rethrowIfNotPerMessage(e: unknown): void {
  if (e instanceof DBOSErrors.DBOSNotRegisteredError || !(e instanceof Error)) throw e;
}

/** A consumer's display name; bare functions registered outside a class have no class name. */
function qualifiedName(className: string, name: string): string {
  return className ? `${className}.${name}` : name;
}

/**
 * Run `operation` until it succeeds, backing off between attempts. Gives up only once `signal`
 * is aborted, in which case it returns `undefined`.
 */
async function retryUntilSuccess<T>(
  operation: () => Promise<T>,
  description: string,
  signal: AbortSignal,
): Promise<{ value: T } | undefined> {
  let waitMS = MIN_RETRY_WAIT_MS;
  while (!signal.aborted) {
    try {
      return { value: await operation() };
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      DBOS.logger.error(
        `Kafka consumer failed to ${description}: ${message}. Retrying in ${Math.round(waitMS / 1000)}s.`,
      );
      await sleepms(waitMS, signal);
      waitMS = Math.min(waitMS * 2, MAX_RETRY_WAIT_MS);
    }
  }
  return undefined;
}

export type ConsumerTopics = string | RegExp | Array<string | RegExp>;

export class KafkaReceiver implements DBOSLifecycleCallback {
  readonly #consumers = new Array<Consumer>();
  #abortController = new AbortController();

  constructor(
    private readonly config: KafkaConfig,
    private readonly retryConfig: KafkaRetryConfig = { maxRetries: 5, retryTime: 300, multiplier: 2 },
  ) {
    DBOS.registerLifecycleCallback(this);
  }

  /**
   * Two consumers on the same group and topic would each receive only some of the messages, which
   * is never what a caller wants; the same group on different topics only risks rebalance churn.
   */
  #validateConsumerGroups(registrations: { funcName: string; groupId: string; topics: Array<string | RegExp> }[]) {
    for (let i = 0; i < registrations.length; i++) {
      for (let j = i + 1; j < registrations.length; j++) {
        const a = registrations[i];
        const b = registrations[j];
        if (a.groupId !== b.groupId) continue;
        const aTopics = new Set(a.topics.map((t) => t.toString()));
        const shared = b.topics.map((t) => t.toString()).filter((t) => aTopics.has(t));
        if (shared.length > 0) {
          throw new Error(
            `Kafka consumers ${a.funcName} and ${b.funcName} share group.id ${a.groupId} and topic(s) ` +
              `${shared.sort().join(', ')}, so each message would be delivered to only one of them. ` +
              `Use distinct group IDs.`,
          );
        }
        DBOS.logger.warn(
          `Kafka consumers ${a.funcName} and ${b.funcName} share group.id ${a.groupId} with different topics. ` +
            `This can cause rebalance churn; consider using distinct group IDs.`,
        );
      }
    }
  }

  /**
   * A custom queue must not be partitioned: ordering="none" enqueues no partition key, which a
   * partitioned queue never dequeues, so its workflows would sit ENQUEUED forever.
   */
  async #validateConsumerQueue(funcName: string, queueName: string) {
    let queue: WorkflowQueue | null;
    try {
      queue = await getQueue(queueName);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      DBOS.logger.warn(
        `Could not check the configuration of Kafka consumer ${funcName}'s queue ${queueName}: ${message}`,
      );
      return;
    }
    // A consumer may name a queue that does not exist yet: it can be registered after launch.
    if (queue === null) return;
    if (queue.partitionQueue) {
      throw new Error(
        `Kafka consumer ${funcName}'s queue ${queueName} is a partitioned queue, which a custom Kafka ` +
          `queue must not be; use ordering="partition" or "topic" for ordered processing`,
      );
    }
  }

  async initialize() {
    this.#abortController = new AbortController();
    const kafka = new KafkaJS(this.config);

    const regOps = DBOS.getAssociatedInfo(this);
    const registrations: { funcName: string; groupId: string; topics: Array<string | RegExp> }[] = [];
    for (const regOp of regOps) {
      const methodConfig = regOp.methodConfig as KafkaMethodConfig;
      const topics = methodConfig.topics ?? [];
      if (regOp.methodReg.registeredFunction === undefined || topics.length === 0) continue;
      const { name, className } = regOp.methodReg;
      // Fail here rather than per message: an unregistered function fails identically for every
      // message, which the batch loop would mistake for a stream of poison messages and drop.
      if (regOp.methodReg.workflowConfig === undefined) {
        throw new Error(
          `Kafka consumer ${qualifiedName(className, name)} is not a registered DBOS workflow. Register it ` +
            `with DBOS.registerWorkflow, or apply the @DBOS.workflow() decorator beneath @consumer().`,
        );
      }
      const groupId = methodConfig.config?.groupId ?? safeGroupName(className, name, topics);
      registrations.push({ funcName: qualifiedName(className, name), groupId, topics });
      if (methodConfig.queueName !== undefined) {
        await this.#validateConsumerQueue(qualifiedName(className, name), methodConfig.queueName);
      }
    }
    this.#validateConsumerGroups(registrations);

    for (const regOp of regOps) {
      const func = regOp.methodReg.registeredFunction as KafkaMessageHandler<unknown> | undefined;
      if (func === undefined) {
        continue; // TODO: Log?
      }

      const methodConfig = regOp.methodConfig as KafkaMethodConfig;
      const topics = methodConfig.topics ?? [];
      if (topics.length === 0) {
        continue; // TODO: Log?
      }

      const { name, className } = regOp.methodReg;
      await this.#startConsumer(kafka, {
        func,
        topics,
        config: methodConfig.config ?? { groupId: safeGroupName(className, name, topics) },
        ordering: methodConfig.ordering ?? 'none',
        batchSize: methodConfig.batchSize ?? DEFAULT_BATCH_SIZE,
        queueName: methodConfig.consumerQueueName ?? KAFKA_QUEUE_NAME,
      });
    }
  }

  /** Connect, subscribe, and start consuming for one registration. */
  async #startConsumer(kafka: KafkaJS, spec: ConsumerSpec) {
    const { maxRetries, multiplier } = this.retryConfig;
    const consumer = kafka.consumer(spec.config);
    await consumer.connect();

    // A temporary workaround for https://github.com/tulios/kafkajs/pull/1558 until it gets fixed
    // If topic auto-creation is on and you try to subscribe to a nonexistent topic, KafkaJS should retry until the topic is created.
    // However, it has a bug where it won't. Thus, we retry instead.
    let { retryTime } = this.retryConfig;
    for (let i = 1; i <= maxRetries; i++) {
      try {
        await consumer.subscribe({ topics: spec.topics, fromBeginning: true });
        break;
      } catch (e) {
        if (e instanceof KafkaJSProtocolError && e.code === 3 && i < maxRetries) {
          await sleepms(retryTime);
          retryTime *= multiplier;
        } else {
          throw e;
        }
      }
    }

    // KafkaJS restarts itself only for retriable crashes. A non-retriable one (a TypeError and
    // friends) leaves the consumer stopped and disconnected for good, so rebuild it ourselves.
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      if (payload.restart || this.#abortController.signal.aborted) return;
      DBOS.logger.error(
        `Kafka consumer for group ${spec.config.groupId} stopped on a non-retriable error: ${payload.error.message}. Recreating it.`,
      );
      const idx = this.#consumers.indexOf(consumer);
      if (idx >= 0) this.#consumers.splice(idx, 1);
      void this.#recreateConsumer(kafka, spec);
    });

    await consumer.run({
      // DBOS resolves offsets itself, only after a batch is durably enqueued, so commits never
      // outrun durable state.
      eachBatchAutoResolve: false,
      eachBatch: (payload) =>
        this.#eachBatch(payload, spec.func, spec.config.groupId, spec.ordering, spec.batchSize, spec.queueName),
    });

    this.#consumers.push(consumer);
  }

  /** Rebuild a consumer KafkaJS stopped for good, backing off between attempts. */
  async #recreateConsumer(kafka: KafkaJS, spec: ConsumerSpec) {
    const signal = this.#abortController.signal;
    let waitMS = MIN_RETRY_WAIT_MS;
    while (!signal.aborted) {
      await sleepms(waitMS, signal);
      if (signal.aborted) return;
      try {
        await this.#startConsumer(kafka, spec);
        return;
      } catch (e) {
        const message = e instanceof Error ? e.message : String(e);
        DBOS.logger.error(
          `Failed to recreate Kafka consumer for group ${spec.config.groupId}: ${message}. Retrying in ${Math.round(waitMS / 1000)}s.`,
        );
        waitMS = Math.min(waitMS * 2, MAX_RETRY_WAIT_MS);
      }
    }
  }

  async #eachBatch(
    payload: EachBatchPayload,
    func: KafkaMessageHandler<unknown>,
    groupId: string,
    ordering: KafkaOrdering,
    batchSize: number,
    queueName: string,
  ) {
    const { batch } = payload;
    const { topic, partition } = batch;
    const signal = this.#abortController.signal;
    const queuePartitionKey = partitionKeyFor(ordering, groupId, topic, partition);

    for (let start = 0; start < batch.messages.length; start += batchSize) {
      // A rebalance revoked this partition, or we're shutting down: stop without resolving offsets,
      // so the remaining messages are redelivered.
      if (!payload.isRunning() || payload.isStale() || signal.aborted) return;
      const chunk = batch.messages.slice(start, start + batchSize);

      const prepared: PreparedWorkflow[] = [];
      for (const message of chunk) {
        try {
          prepared.push(
            await prepareEnqueuedWorkflow(func, [topic, partition, message], {
              queueName,
              // This ID format is the dedup key for redelivered messages; never change it.
              workflowID: `kafkajs-${topic}-${partition}-${groupId}-${message.offset}`,
              queuePartitionKey,
            }),
          );
        } catch (e) {
          // Only this message is unprocessable, so drop it rather than wedge the partition behind
          // it. Never drop for a reason that applies to every message (see rethrowIfNotPerMessage):
          // that would silently discard the whole stream, offsets and all.
          rethrowIfNotPerMessage(e);
          const message_ = e instanceof Error ? e.message : String(e);
          DBOS.logger.error(
            `Dropping unprocessable Kafka message ${topic}[${partition}]@${message.offset}: ${message_}`,
          );
        }
      }

      if (prepared.length > 0) {
        // Retry this same chunk until durable, rather than dropping it: nothing has been committed,
        // so giving up here would lose these messages until the next rebalance.
        const result = await retryUntilSuccess(
          () => initWorkflows(prepared),
          'durably enqueue consumed messages',
          signal,
        );
        // Aborted before the chunk was durable; offsets weren't resolved, so Kafka redelivers.
        if (result === undefined) return;
      }

      // Every message in the chunk is now handled (enqueued or dropped), so advance past all of
      // them: a poison message must not be redelivered forever.
      for (const message of chunk) {
        payload.resolveOffset(message.offset);
      }
      await payload.heartbeat();
    }
  }

  async destroy() {
    this.#abortController.abort();
    const disconnectPromises = this.#consumers.splice(0, this.#consumers.length).map((c) => c.disconnect());
    await Promise.allSettled(disconnectPromises);
  }

  logRegisteredEndpoints() {
    DBOS.logger.info('KafkaJS receiver endpoints:');

    const regOps = DBOS.getAssociatedInfo(this);
    for (const regOp of regOps) {
      const methodConfig = regOp.methodConfig as KafkaMethodConfig;
      const { name, className } = regOp.methodReg;
      for (const topic of methodConfig.topics ?? []) {
        DBOS.logger.info(`    ${topic} -> ${qualifiedName(className, name)}`);
      }
    }
  }

  registerConsumer<This, Return>(
    func: (this: This, ...args: KafkaArgs) => Promise<Return>,
    topics: ConsumerTopics,
    options: FunctionName & KafkaConsumerOptions = {},
  ) {
    const ordering = options.ordering ?? 'none';
    if (ordering !== 'none' && ordering !== 'partition' && ordering !== 'topic') {
      throw new Error(`Invalid Kafka ordering "${String(ordering)}": must be "none", "partition", or "topic"`);
    }
    const batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    if (!Number.isInteger(batchSize) || batchSize < 1) {
      throw new Error('Kafka batchSize must be a positive integer');
    }
    if (options.queueName !== undefined && ordering !== 'none') {
      throw new Error(
        'A custom queue is only supported with ordering="none"; ordered consumers share an internal partitioned queue',
      );
    }

    const { regInfo } = DBOS.associateFunctionWithInfo(this, func, options);

    const kafkaRegInfo = regInfo as KafkaMethodConfig;
    kafkaRegInfo.topics = Array.isArray(topics) ? topics : [topics];
    kafkaRegInfo.queueName = options.queueName;
    kafkaRegInfo.config = options.config;
    kafkaRegInfo.ordering = ordering;
    kafkaRegInfo.batchSize = batchSize;

    // Resolve the consumer's queue now, before launch: the dispatcher snapshots in-memory queues
    // when it starts, and a queue created later would never be dispatched.
    if (ordering === 'none') {
      kafkaRegInfo.consumerQueueName = options.queueName ?? getKafkaQueue().name;
    } else {
      kafkaRegInfo.consumerQueueName = getKafkaOrderedQueue().name;
    }
    // This process runs the consumer and enqueues onto that queue, so it must poll it even under
    // a listenQueues filter.
    registerPollerQueue(kafkaRegInfo.consumerQueueName);
  }

  consumer(topics: ConsumerTopics, options: KafkaConsumerOptions = {}) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const $this = this;
    function methodDecorator<This, Return>(
      target: object,
      propertyKey: PropertyKey,
      descriptor: TypedPropertyDescriptor<(this: This, ...args: KafkaArgs) => Promise<Return>>,
    ) {
      if (descriptor.value) {
        $this.registerConsumer(descriptor.value, topics, {
          ctorOrProto: target,
          name: String(propertyKey),
          ...options,
        });
      }
      return descriptor;
    }
    return methodDecorator;
  }
}
