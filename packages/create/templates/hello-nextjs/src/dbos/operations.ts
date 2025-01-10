import { DBOS } from "@dbos-inc/dbos-sdk";
export  { dbosWorkflowClass } from "./background";
export  { fooWorkflowClass } from "./foo";

console.log("Hello from operations.ts");

if (process.env.NEXT_PHASE !== "phase-production-build") {
   await DBOS.launch()
} 