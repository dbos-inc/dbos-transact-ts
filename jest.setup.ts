import { workflowTimeoutConfig } from './src/workflow_management';

process.env.PGPASSWORD = process.env.PGPASSWORD ?? 'dbos';

// Timeout and delay tests assume the background jobs run well within their margins, which the product's 1s interval would not guarantee.
workflowTimeoutConfig.pollingIntervalMs = 100;
