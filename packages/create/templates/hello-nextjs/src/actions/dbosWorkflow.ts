"use server";

import { DBOS } from "@dbos-inc/dbos-sdk";
// import { dbosWorkflowClass } from "../dbos/background";
import { dbosWorkflowClass } from "../dbos/operations";

console.log("Hello from dbosWorkflow.ts");

// The exported function is the entry point for the workflow
// The function is exported and not the class because Next does not support exporting classes
export async function dbosWorkflow(userName: string) {
    DBOS.logger.info("Hello from DBOS!");
    return await dbosWorkflowClass.helloDBOS(userName);
}

export async function dbosBackgroundTask(workflowID: string) {
    DBOS.logger.info("Hello from DBOS!");
    return DBOS.startWorkflow(dbosWorkflowClass, {workflowID: workflowID}).backgroundTask(10);
}