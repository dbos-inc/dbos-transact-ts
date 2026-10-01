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
    DBOS.setConfig({
      name: 'bundler-test',
      systemDatabaseUrl: process.env.DBOS_SYSTEM_DATABASE_URL,
    });

    // Initialize DBOS
    await DBOS.launch();

    // Test workflow execution (this is the main validation)
    const workflowResult = await BundlerTestApp.testWorkflow('bundler-test-input');
    console.log('Workflow result:', workflowResult);

    // Shutdown DBOS
    await DBOS.shutdown();

    // Without @dbos-inc/dbos-enterprise installed, the bundle still builds and Conductor asks for the package.
    try {
      await DBOS.launch({ conductorKey: 'bundler-test-key', conductorURL: 'ws://127.0.0.1:1' });
      throw new Error('Conductor launched without @dbos-inc/dbos-enterprise');
    } catch (error) {
      if (!(error instanceof Error) || !error.message.includes('npm install @dbos-inc/dbos-enterprise')) throw error;
      console.log('Conductor asked for @dbos-inc/dbos-enterprise');
    }
    await DBOS.shutdown();

    console.log('DBOS bundler test completed successfully!');

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
