import { DbosDecoratorKind } from '../compiler';

export const sampleDbosClass = /*ts*/ `
import {
  DBOS,
  HandlerContext,
  WorkflowContext,
  CommunicatorContext,
  StepContext,
  TransactionContext,
  StoredProcedureContext,
  InitContext,
} from "@dbos-inc/dbos-sdk";
import { Knex } from 'knex';

export class Test {
  @DBOS.getApi('/test')
  static async testGetHandler(): Promise<void> {  }

  @DBOS.postApi('/test')
  static async testPostHandler(): Promise<void> {  }

  @DBOS.patchApi('/test')
  static async testPatchHandler(): Promise<void> {  }

  @DBOS.putApi('/test')
  static async testPutHandler(): Promise<void> {  }

  @DBOS.deleteApi('/test')
  static async testDeleteHandler(): Promise<void> {  }

  @DBOS.getApi('/test')
  @DBOS.workflow()
  static async testGetHandlerWorkflow(): Promise<void> {  }

  @DBOS.getApi('/test')
  @DBOS.transaction()
  static async testGetHandlerTx(): Promise<void> {  }

  @DBOS.getApi('/test')
  @DBOS.step()
  static async testGetHandlerStep(): Promise<void> {  }

  @DBOS.workflow()
  static async testWorkflow(): Promise<void> {  }

  @DBOS.step()
  static async testStep(message: string): Promise<void> {  }

  @DBOS.step()
  static async testCommunicator(message: string): Promise<void> {  }

  @DBOS.transaction()
  static async testTransaction(message: string): Promise<void> {  }

  @DBOS.storedProcedure()
  static async testProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ readOnly: true })
  static async testReadOnlyProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ isolationLevel: "REPEATABLE READ" })
  static async testRepeatableReadProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ readOnly: true, isolationLevel: "READ COMMITTED" })
  static async testConfiguredProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ executeLocally: true})
  static async testLocalProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ readOnly: true, executeLocally: true })
  static async testLocalReadOnlyProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ isolationLevel: "REPEATABLE READ", executeLocally: true })
  static async testLocalRepeatableReadProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ readOnly: true, isolationLevel: "READ COMMITTED", executeLocally: true })
  static async testLocalConfiguredProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }
}
`;

export const sampleDbosClassAliased = /*ts*/ `
import {
  DBOS as TestDBOS,
  HandlerContext,
  WorkflowContext,
  CommunicatorContext,
  StepContext,
  TransactionContext,
  StoredProcedureContext,
  InitContext,
} from "@dbos-inc/dbos-sdk";
import { Knex } from 'knex';

export class Test {
  @TestDBOS.getApi('/test')
  static async testGetHandler(ctxt: HandlerContext): Promise<void> {  }

  @TestDBOS.postApi('/test')
  static async testPostHandler(ctxt: HandlerContext): Promise<void> {  }

  @TestDBOS.patchApi('/test')
  static async testPatchHandler(ctxt: HandlerContext): Promise<void> {  }

  @TestDBOS.putApi('/test')
  static async testPutHandler(ctxt: HandlerContext): Promise<void> {  }

  @TestDBOS.deleteApi('/test')
  static async testDeleteHandler(ctxt: HandlerContext): Promise<void> {  }

  @TestDBOS.getApi('/test')
  @TestDBOS.workflow()
  static async testGetHandlerWorkflow(): Promise<void> {  }

  @TestDBOS.getApi('/test')
  @TestDBOS.transaction()
  static async testGetHandlerTx(): Promise<void> {  }

  @TestDBOS.getApi('/test')
  @TestDBOS.step()
  static async testGetHandlerStep(): Promise<void> {  }

  @TestDBOS.workflow()
  static async testWorkflow(): Promise<void> {  }

  @TestDBOS.step()
  static async testStep(message: string): Promise<void> {  }

  @TestDBOS.step()
  static async testCommunicator(message: string): Promise<void> {  }

  @TestDBOS.transaction()
  static async testTransaction(message: string): Promise<void> {  }

  @TestDBOS.storedProcedure()
  static async testProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ readOnly: true })
  static async testReadOnlyProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ isolationLevel: "REPEATABLE READ" })
  static async testRepeatableReadProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ readOnly: true, isolationLevel: "READ COMMITTED" })
  static async testConfiguredProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ executeLocally: true})
  static async testLocalProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ readOnly: true, executeLocally: true })
  static async testLocalReadOnlyProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ isolationLevel: "REPEATABLE READ", executeLocally: true })
  static async testLocalRepeatableReadProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ readOnly: true, isolationLevel: "READ COMMITTED", executeLocally: true })
  static async testLocalConfiguredProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }
}`;

export const testCodeTypes: [string, DbosDecoratorKind][] = [
  ['testGetHandler', 'handler'],
  ['testPostHandler', 'handler'],
  ['testDeleteHandler', 'handler'],
  ['testPutHandler', 'handler'],
  ['testPatchHandler', 'handler'],
  ['testGetHandlerWorkflow', 'workflow'],
  ['testGetHandlerTx', 'transaction'],
  ['testGetHandlerStep', 'step'],
  ['testWorkflow', 'workflow'],
  ['testCommunicator', 'step'],
  ['testStep', 'step'],
  ['testTransaction', 'transaction'],
  ['testProcedure', 'storedProcedure'],
  ['testReadOnlyProcedure', 'storedProcedure'],
  ['testRepeatableReadProcedure', 'storedProcedure'],
  ['testConfiguredProcedure', 'storedProcedure'],
  ['testLocalProcedure', 'storedProcedure'],
  ['testLocalReadOnlyProcedure', 'storedProcedure'],
  ['testLocalRepeatableReadProcedure', 'storedProcedure'],
  ['testLocalConfiguredProcedure', 'storedProcedure'],
];

export const storedProcParam: [string, object][] = [
  ['testReadOnlyProcedure', { readOnly: true }],
  ['testRepeatableReadProcedure', { isolationLevel: 'REPEATABLE READ' }],
  ['testConfiguredProcedure', { readOnly: true, isolationLevel: 'READ COMMITTED' }],
  ['testLocalProcedure', { executeLocally: true }],
  ['testLocalReadOnlyProcedure', { readOnly: true, executeLocally: true }],
  ['testLocalRepeatableReadProcedure', { isolationLevel: 'REPEATABLE READ', executeLocally: true }],
  ['testLocalConfiguredProcedure', { readOnly: true, isolationLevel: 'READ COMMITTED', executeLocally: true }],
];
