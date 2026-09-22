import { workflowTimeoutConfig } from './src/workflow_management';

process.env.PGPASSWORD = process.env.PGPASSWORD ?? 'dbos';

// The product's 1s timeout sweep would dominate every test that waits for a timeout.
workflowTimeoutConfig.pollingIntervalMs = 100;
