import { workflowTimeoutConfig } from './src/workflow_management';

process.env.PGPASSWORD = process.env.PGPASSWORD ?? 'dbos';

// Timeout tests assume cancellation lands well within their margins, which the product's 1s sweep would not guarantee.
workflowTimeoutConfig.pollingIntervalMs = 100;
