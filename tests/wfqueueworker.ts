import { DBOS, WorkflowQueue } from '../src';
import { generateDBOSTestConfig, Event } from '../tests/helpers';
import { sleepms } from '../src/utils';

// Extract worker arguments from command-line input
const workerId = process.env.DBOS__VMID || process.pid;
const globalConcurrencyLimit = parseInt(process.argv[2] || '1', 10);
const parentWorkflowID = process.argv[3];
if (!parentWorkflowID) {
  console.error('Parent workflow ID not provided');
  process.exit(1);
}
const queueName = process.argv[4];
if (!queueName) {
  console.error('Queue name not provided');
  process.exit(1);
}
console.log(`Worker ID: ${workerId}, Global Concurrency Limit: ${globalConcurrencyLimit}`);

// Declare worker queue with controlled concurrency
new WorkflowQueue(queueName, {
  workerConcurrency: 1,
  concurrency: globalConcurrencyLimit,
});

class InterProcessWorkflowTask {
  static start_event = new Event();
  static end_event = new Event();

  @DBOS.workflow()
  static async task() {
    InterProcessWorkflowTask.start_event.set();
    await InterProcessWorkflowTask.end_event.wait();
    return Promise.resolve();
  }
}

async function queueEntriesAreCleanedUp() {
  let maxTries = 10;
  while (maxTries > 0) {
    const r = await DBOS.getWorkflowQueue({});
    if (r.workflows.length === 0) return true;
    await sleepms(1000);
    --maxTries;
  }
  return false;
}

async function main() {
  const config = generateDBOSTestConfig();
  DBOS.setConfig(config);
  await DBOS.launch();

  // Wait for a task to be dequeued
  await InterProcessWorkflowTask.start_event.wait();

  // Notify the main process this worker has dequeued a task
  await DBOS.send<String>(parentWorkflowID, 'worker_dequeue', 'worker_dequeue', workerId.toString());

  // Wait for a resume signal from the main process
  const can_resume = await DBOS.getEvent(parentWorkflowID, 'worker_resume', 10);
  if (!can_resume) {
    console.error(`${workerId} did not receive resume signal`);
    process.exit(1);
  }

  // Complete the task
  InterProcessWorkflowTask.end_event.set();

  const success = await queueEntriesAreCleanedUp();
  if (!success) {
    console.error(`${workerId} detected unhandled workflow queue entries`);
    process.exit(1);
  }

  await DBOS.shutdown();
  process.exit(0);
}

// Run the worker if executed as a standalone script
if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
