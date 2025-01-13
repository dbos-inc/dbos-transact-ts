"use server";

import { DBOS } from "@dbos-inc/dbos-sdk";
import { dbosWorkflowClass } from "../dbos/operations";

console.log("Hello from dbosWorkflow.ts");


export async function dbosBackgroundTask(workflowID: string) {
    DBOS.logger.info("Hello from DBOS!");
    return DBOS.startWorkflow(dbosWorkflowClass, {workflowID: workflowID}).backgroundTask(10);
}