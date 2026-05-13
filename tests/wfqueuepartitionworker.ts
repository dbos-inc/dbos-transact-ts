/**
 * Persistent worker for the partitioned-queue orphan-PENDING reproduction tests.
 *
 * Spawned by the Jest stress test ('partition-orphan-parent-child-stress') to
 * simulate competing executors. Multiple instances (different DBOS__VMID) race
 * each other and the test process to dispatch workflows from the partitioned
 * queue, exercising the FOR UPDATE NOWAIT contention path that previously left
 * workflows permanently stuck in PENDING.
 *
 * Usage:
 *   DBOS__VMID=worker-0 npx ts-node ./tests/wfqueuepartitionworker.ts <maxDurationMs> <queueName>
 */
import { DBOS } from '../src';
import { generateDBOSTestConfig } from './helpers';

const maxDurationMs = parseInt(process.argv[2] ?? '60000', 10);
const queueName = process.argv[3];
if (!queueName) {
  console.error('[partition-worker] queueName argument required');
  process.exit(1);
}

const workerId = process.env['DBOS__VMID'] ?? 'partition-worker';

class PartitionWorkerWF {
  @DBOS.workflow()
  static async run(partitionKey: string): Promise<string> {
    await DBOS.sleepms(150);
    return partitionKey;
  }

  @DBOS.workflow()
  static async child(partitionKey: string): Promise<string> {
    await DBOS.sleepms(150);
    return partitionKey;
  }

  @DBOS.workflow()
  static async parent(childWorkflowId: string, partitionKey: string, childQueueName: string): Promise<string> {
    await DBOS.startWorkflow(PartitionWorkerWF, {
      workflowID: childWorkflowId,
      queueName: childQueueName,
      enqueueOptions: { queuePartitionKey: partitionKey },
    }).child(partitionKey);
    return childWorkflowId;
  }
}

void PartitionWorkerWF;

async function main() {
  const config = generateDBOSTestConfig();
  config.listenQueues = [queueName];
  DBOS.setConfig(config);
  await DBOS.launch();

  console.log(`[${workerId}] launched, listening on queue '${queueName}', will run for ${maxDurationMs}ms`);

  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, maxDurationMs);
    process.on('SIGTERM', () => {
      clearTimeout(timer);
      resolve();
    });
  });

  console.log(`[${workerId}] shutting down`);
  await DBOS.shutdown();
  process.exit(0);
}

main().catch((err: unknown) => {
  console.error('[partition-worker] fatal:', err);
  process.exit(1);
});
