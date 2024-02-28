import { DBOSConfig, DBOSExecutor } from "../dbos-executor";
import { DBOSError, DBOSFailLoadOperationsError } from "../error";
import { GlobalLogger } from "../telemetry/logs";
import { DBOSRuntime, DBOSRuntimeConfig,  } from "./runtime";

export async function debugWorkflow(dbosConfig: DBOSConfig, runtimeConfig: DBOSRuntimeConfig, proxy: string, workflowUUID: string) {
  dbosConfig = {...dbosConfig, debugProxy: proxy};
  const logger = new GlobalLogger();
  try {
    // Load classes
    const classes = await DBOSRuntime.loadClasses(runtimeConfig.entrypoint);
    const dbosExec = new DBOSExecutor(dbosConfig);
    await dbosExec.init(...classes);

    // Invoke the workflow in debug mode.
    const handle = await dbosExec.executeWorkflowUUID(workflowUUID);
    await handle.getResult();

    // Destroy testing runtime.
    await dbosExec.destroy();
  } catch (e) {
    const errorLabel = `Debug mode error`;
    logger.error(`${errorLabel}: ${(e as Error).message}`);
    if (e instanceof AggregateError) {
      console.error(e.errors);
      for (const err of e.errors) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        if (err.code && err.code === "ECONNREFUSED") {
          console.error('\x1b[31m%s\x1b[0m', `Is DBOS time-travel debug proxy running at ${proxy} ?`);
          break;
        }
      }
    } else if (e instanceof DBOSFailLoadOperationsError) {
      console.error('\x1b[31m%s\x1b[0m', "Did you correctly compile this application? Hint: run `npm run build` and try again");
    }
    process.exit(1);
  }
  return;
}
