import { DBOS } from '@dbos-inc/dbos-sdk';

class BundlerTestApp {
  @DBOS.step()
  static async testStep(input: string): Promise<string> {
    console.log(`Processing step with input: ${input}`);
    return Promise.resolve(`Step processed: ${input}`);
  }

  @DBOS.workflow()
  static async testWorkflow(input: string): Promise<string> {
    console.log(`Starting workflow with input: ${input}`);
    const stepResult = await BundlerTestApp.testStep(input);
    console.log(`Workflow completed with result: ${stepResult}`);
    return stepResult;
  }
}

async function main() {
  try {
    console.log('Starting DBOS bundler test app...');

    // Configure DBOS with minimal configuration
    const config = {
      name: 'bundler-test',
      database_url: process.env.DBOS_DATABASE_URL || 'postgresql://postgres:dbos@localhost:5432/dbostest',
      telemetry: {
        logs: {
          silent: true,
        },
      },
    };
    DBOS.setConfig(config);

    // Initialize DBOS
    await DBOS.launch();

    // Test workflow execution (this is the main validation)
    const workflowResult = await BundlerTestApp.testWorkflow('bundler-test-input');
    console.log('Workflow result:', workflowResult);

    console.log('DBOS bundler test completed successfully!');

    // Shutdown DBOS
    await DBOS.shutdown();

    process.exit(0);
  } catch (error) {
    console.error('Error in bundler test:', error);
    process.exit(1);
  }
}

// Only run main if this is the entry point
if (require.main === module) {
  main().catch(console.log);
}

export { BundlerTestApp, main };
