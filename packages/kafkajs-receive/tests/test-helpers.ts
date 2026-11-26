import { Client } from 'pg';
import { ITopicConfig, Kafka, KafkaJSNumberOfRetriesExceeded, KafkaJSProtocolError } from 'kafkajs';

export async function ensureDB(client: Client, name: string) {
  const results = await client.query('SELECT 1 FROM pg_database WHERE datname = $1', [name]);
  if (results.rows.length === 0) {
    await client.query(`CREATE DATABASE ${name}`);
  }
}

export async function dropDB(client: Client, name: string, force: boolean = false) {
  const withForce = force ? ' WITH (FORCE)' : '';
  await client.query(`DROP DATABASE IF EXISTS ${name} ${withForce}`);
}

export function withTimeout<T>(promise: Promise<T>, ms: number, message = 'Timeout'): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout>;

  const timeout = new Promise<never>((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(message)), ms);
  });

  return Promise.race([promise, timeout]).finally(() => clearTimeout(timeoutId));
}

// Polls `check` until it returns true, failing after `timeoutMs`.
async function waitFor(check: () => Promise<boolean>, what: string, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

// Creates the topics and waits until every partition has a leader. kafkajs's own wait gives up when a
// new topic still reads as unknown, which the broker reports for a moment while it is busy.
export async function createTopics(kafka: Kafka, topics: ITopicConfig[], { recreate = false } = {}) {
  const admin = kafka.admin();
  const names = topics.map((t) => t.topic);
  try {
    await admin.connect();
    if (recreate) {
      const existing = await admin.listTopics();
      await admin.deleteTopics({ topics: names.filter((t) => existing.includes(t)), timeout: 5000 });
      await waitFor(
        async () => (await admin.listTopics()).every((t) => !names.includes(t)),
        `topics ${names.join(', ')} to be deleted`,
      );
    }
    await admin.createTopics({ topics, waitForLeaders: false, timeout: 10000 });
    await waitFor(
      async () => {
        try {
          const metadata = await admin.fetchTopicMetadata({ topics: names });
          return metadata.topics.every((t) => t.partitions.length > 0 && t.partitions.every((p) => p.leader >= 0));
        } catch (e) {
          // kafkajs retries LEADER_NOT_AVAILABLE itself, then gives up with KafkaJSNumberOfRetriesExceeded.
          if ((e instanceof KafkaJSProtocolError && e.retriable) || e instanceof KafkaJSNumberOfRetriesExceeded)
            return false;
          throw e;
        }
      },
      `leaders for topics ${names.join(', ')}`,
    );
  } finally {
    await admin.disconnect();
  }
}
