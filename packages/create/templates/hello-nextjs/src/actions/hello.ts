"use server";

import { DBOS } from "@dbos-inc/dbos-sdk";
// import {dbos_hello, dbosWorkflow, dbosBackgroundTask} from "./dbosWorkflow";
import { helloWorkflowClass } from "../dbos/operations";


 console.log("Hello from foo.ts");

// The exported function is the entry point for the workflow
// The function is exported and not the class because Next does not support exporting classes
export async function helloWorkflow(userName: string) {
    DBOS.logger.info("Hello from DBOS!");
    return await helloWorkflowClass.helloDBOS(userName);
}

