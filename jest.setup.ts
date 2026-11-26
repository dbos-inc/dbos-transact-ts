import { workflowTimeoutConfig } from './src/workflow_management';
import { wfQueueRunner } from './src/wfqueue';

process.env.PGPASSWORD = process.env.PGPASSWORD ?? 'dbos';

// Timeout and delay tests assume the background jobs run well within their margins, which the product's 1s interval would not guarantee.
workflowTimeoutConfig.pollingIntervalMs = 100;

// Tests mostly wait on queues: by default a queue polls every 1s, and the runner picks up a newly registered one within 1s.
// Queues that set their own minPollingIntervalMs keep it.
Object.assign(wfQueueRunner.constructor, { defaultMinPollingIntervalMs: 100, reconcileIntervalMs: 100 });
