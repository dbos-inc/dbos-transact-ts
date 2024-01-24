import { DBOSConfig, DBOSExecutor } from "../dbos-executor";
import { DBOSRuntime, DBOSRuntimeConfig,  } from "./runtime";

export async function debugWorkflow(dbosConfig: DBOSConfig, runtimeConfig: DBOSRuntimeConfig, proxy: string, workflowUUID: string) {
  const provDB = `${dbosConfig.poolConfig.database}_prov`;
  dbosConfig = {...dbosConfig, debugProxy: proxy, system_database: provDB };
  dbosConfig.poolConfig.database = provDB;

  // Load classes
  const classes = await DBOSRuntime.loadClasses(runtimeConfig.entrypoint);
  const dbosExec = new DBOSExecutor(dbosConfig);
  await dbosExec.init(...classes);

  // Invoke the workflow in debug mode.
  const handle = await dbosExec.executeWorkflowUUID(workflowUUID);
  await handle.getResult();

  // Destroy testing runtime.
  await dbosExec.destroy();
}
