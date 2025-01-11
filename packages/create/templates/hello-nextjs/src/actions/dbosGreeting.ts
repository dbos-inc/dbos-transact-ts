"use server";

import { DBOS } from '@dbos-inc/dbos-sdk';
import { DbosGreeting } from '../dbos/DbosGreeting';

// The exported function is the entry point for the workflow
export async function dbosWorkflow(userName: string) {
  DBOS.logger.info("Hello from DBOS!");
  return await DbosGreeting.helloDBOS(userName);
}
