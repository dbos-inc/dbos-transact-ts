export const sampleDbosClass = /*ts*/`
import {
  DBOS,
  GetApi, PostApi, PutApi, PatchApi, DeleteApi, HandlerContext,
  Workflow, WorkflowContext,
  Communicator, CommunicatorContext,
  Step, StepContext,
  Transaction, TransactionContext,
  StoredProcedure, StoredProcedureContext,
  DBOSInitializer, DBOSDeploy, InitContext,
} from "@dbos-inc/dbos-sdk";
import { Knex } from 'knex';

export class Test {
  @GetApi('/test')
  static async testGetHandler(ctxt: HandlerContext): Promise<void> {  }

  @PostApi('/test-post')
  static async testPostHandler(ctxt: HandlerContext): Promise<void> {  }

  @PatchApi('/test-patch')
  static async testPatchHandler(ctxt: HandlerContext): Promise<void> {  }

  @PutApi('/test-put')
  static async testPutHandler(ctxt: HandlerContext): Promise<void> {  }

  @DeleteApi('/test-put')
  static async testDeleteHandler(ctxt: HandlerContext): Promise<void> {  }

  @DBOS.getApi('/test')
  static async testGetHandler_v2(): Promise<void> {  }

  @DBOS.postApi('/test')
  static async testPostHandler_v2(): Promise<void> {  }

  @DBOS.patchApi('/test')
  static async testPatchHandler_v2(): Promise<void> {  }

  @DBOS.putApi('/test')
  static async testPutHandler_v2(): Promise<void> {  }

  @DBOS.deleteApi('/test')
  static async testDeleteHandler_v2(): Promise<void> {  }

  @GetApi('/test')
  @Workflow()
  static async testGetHandlerWorkflow(ctxt: WorkflowContext): Promise<void> {  }

  @GetApi('/test')
  @Transaction()
  static async testGetHandlerTx(ctxt: TransactionContext<Knex>): Promise<void> {  }

  @GetApi('/test')
  @Communicator()
  static async testGetHandlerComm(ctxt: CommunicatorContext): Promise<void> {  }

  @GetApi('/test')
  @Step()
  static async testGetHandlerStep(ctxt: StepContext): Promise<void> {  }

  @DBOS.getApi('/test')
  @DBOS.workflow()
  static async testGetHandlerWorkflow_v2(): Promise<void> {  }

  @DBOS.getApi('/test')
  @DBOS.transaction()
  static async testGetHandlerTx_v2(): Promise<void> {  }

  @GetApi('/test')
  @DBOS.step()
  static async testGetHandlerComm_v2(ctxt: CommunicatorContext): Promise<void> {  }

  @DBOS.getApi('/test')
  @DBOS.step()
  static async testGetHandlerStep_v2(): Promise<void> {  }

  @Workflow()
  static async testWorkflow(ctxt: WorkflowContext): Promise<void> {  }

  @Communicator()
  static async testCommunicator(ctxt: CommunicatorContext, message: string): Promise<void> {  }

  @Step()
  static async testStep(ctxt: StepContext, message: string): Promise<void> {  }

  @Transaction()
  static async testTransaction(ctxt: TransactionContext<Knex>, message: string): Promise<void> {  }

  @DBOS.workflow()
  static async testWorkflow_v2(): Promise<void> {  }

  @DBOS.step()
  static async testStep_v2(message: string): Promise<void> {  }

  @DBOS.step()
  static async testCommunicator_v2(message: string): Promise<void> {  }

  @DBOS.transaction()
  static async testTransaction_v2(message: string): Promise<void> {  }

  @StoredProcedure()
  static async testProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @StoredProcedure({ readOnly: true })
  static async testReadOnlyProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @StoredProcedure({ isolationLevel: "REPEATABLE READ" })
  static async testRepeatableReadProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @StoredProcedure({ readOnly: true, isolationLevel: "READ COMMITTED" })
  static async testConfiguredProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @StoredProcedure({ executeLocally: true})
  static async testLocalProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @StoredProcedure({ readOnly: true, executeLocally: true })
  static async testLocalReadOnlyProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @StoredProcedure({ isolationLevel: "REPEATABLE READ", executeLocally: true })
  static async testLocalRepeatableReadProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @StoredProcedure({ readOnly: true, isolationLevel: "READ COMMITTED", executeLocally: true })
  static async testLocalConfiguredProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure()
  static async testProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ readOnly: true })
  static async testReadOnlyProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ isolationLevel: "REPEATABLE READ" })
  static async testRepeatableReadProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ readOnly: true, isolationLevel: "READ COMMITTED" })
  static async testConfiguredProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ executeLocally: true})
  static async testLocalProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ readOnly: true, executeLocally: true })
  static async testLocalReadOnlyProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ isolationLevel: "REPEATABLE READ", executeLocally: true })
  static async testLocalRepeatableReadProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOS.storedProcedure({ readOnly: true, isolationLevel: "READ COMMITTED", executeLocally: true })
  static async testLocalConfiguredProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @DBOSInitializer()
  static async testDBOSInitializer(ctxt: InitContext): Promise<void> {  }

  @DBOSDeploy()
  static async testDBOSDeploy(ctxt: InitContext): Promise<void> {  }
}
`;

export const sampleDbosClassAliased = /*ts*/`
import {
  DBOS as TestDBOS,
  GetApi as TestGetApi, PostApi as TestPostApi,
  PutApi as TestPutApi, PatchApi as TestPatchApi,
  DeleteApi as TestDeleteApi, HandlerContext,
  Workflow as TestWorkflow, WorkflowContext,
  Communicator as TestCommunicator, CommunicatorContext,
  Step as TestStep, StepContext,
  Transaction as TestTransaction, TransactionContext,
  StoredProcedure as TestStoredProcedure, StoredProcedureContext,
  DBOSInitializer as TestInitializer, DBOSDeploy as TestDeploy, InitContext,
} from "@dbos-inc/dbos-sdk";
import { Knex } from 'knex';

export class Test {
  @TestGetApi('/test')
  static async testGetHandler(ctxt: HandlerContext): Promise<void> {  }

  @TestPostApi('/test-post')
  static async testPostHandler(ctxt: HandlerContext): Promise<void> {  }

  @TestPatchApi('/test-patch')
  static async testPatchHandler(ctxt: HandlerContext): Promise<void> {  }

  @TestPutApi('/test-put')
  static async testPutHandler(ctxt: HandlerContext): Promise<void> {  }

  @TestDeleteApi('/test-put')
  static async testDeleteHandler(ctxt: HandlerContext): Promise<void> {  }

  @TestDBOS.getApi('/test')
  static async testGetHandler_v2(ctxt: HandlerContext): Promise<void> {  }

  @TestDBOS.postApi('/test')
  static async testPostHandler_v2(ctxt: HandlerContext): Promise<void> {  }

  @TestDBOS.patchApi('/test')
  static async testPatchHandler_v2(ctxt: HandlerContext): Promise<void> {  }

  @TestDBOS.putApi('/test')
  static async testPutHandler_v2(ctxt: HandlerContext): Promise<void> {  }

  @TestDBOS.deleteApi('/test')
  static async testDeleteHandler_v2(ctxt: HandlerContext): Promise<void> {  }

  @TestGetApi('/test')
  @TestWorkflow()
  static async testGetHandlerWorkflow(ctxt: HandlerContext): Promise<void> {  }

  @TestGetApi('/test')
  @TestTransaction()
  static async testGetHandlerTx(ctxt: HandlerContext): Promise<void> {  }

  @TestGetApi('/test')
  @TestCommunicator()
  static async testGetHandlerComm(ctxt: HandlerContext): Promise<void> {  }

  @TestGetApi('/test')
  @TestDBOS.step()
  static async testGetHandlerComm_v2(ctxt: CommunicatorContext): Promise<void> {  }

  @TestGetApi('/test')
  @TestStep()
  static async testGetHandlerStep(ctxt: StepContext): Promise<void> {  }

  @TestDBOS.getApi('/test')
  @TestDBOS.workflow()
  static async testGetHandlerWorkflow_v2(): Promise<void> {  }

  @TestDBOS.getApi('/test')
  @TestDBOS.transaction()
  static async testGetHandlerTx_v2(): Promise<void> {  }

  @TestDBOS.getApi('/test')
  @TestDBOS.step()
  static async testGetHandlerStep_v2(): Promise<void> {  }

  @TestWorkflow()
  static async testWorkflow(ctxt: WorkflowContext): Promise<void> {  }

  @TestCommunicator()
  static async testCommunicator(ctxt: CommunicatorContext, message: string): Promise<void> {  }

  @TestStep()
  static async testStep(ctxt: StepContext, message: string): Promise<void> {  }

  @TestTransaction()
  static async testTransaction(ctxt: TransactionContext<Knex>, message: string): Promise<void> {  }

  @TestDBOS.workflow()
  static async testWorkflow_v2(): Promise<void> {  }

  @TestDBOS.step()
  static async testStep_v2(message: string): Promise<void> {  }

  @TestDBOS.step()
  static async testCommunicator_v2(message: string): Promise<void> {  }

  @TestDBOS.transaction()
  static async testTransaction_v2(message: string): Promise<void> {  }

  @TestStoredProcedure()
  static async testProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestStoredProcedure({ readOnly: true })
  static async testReadOnlyProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestStoredProcedure({ isolationLevel: "REPEATABLE READ" })
  static async testRepeatableReadProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestStoredProcedure({ readOnly: true, isolationLevel: "READ COMMITTED" })
  static async testConfiguredProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestStoredProcedure({ executeLocally: true})
  static async testLocalProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestStoredProcedure({ readOnly: true, executeLocally: true })
  static async testLocalReadOnlyProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestStoredProcedure({ isolationLevel: "REPEATABLE READ", executeLocally: true })
  static async testLocalRepeatableReadProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestStoredProcedure({ readOnly: true, isolationLevel: "READ COMMITTED", executeLocally: true })
  static async testLocalConfiguredProcedure(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure()
  static async testProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ readOnly: true })
  static async testReadOnlyProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ isolationLevel: "REPEATABLE READ" })
  static async testRepeatableReadProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ readOnly: true, isolationLevel: "READ COMMITTED" })
  static async testConfiguredProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ executeLocally: true})
  static async testLocalProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ readOnly: true, executeLocally: true })
  static async testLocalReadOnlyProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ isolationLevel: "REPEATABLE READ", executeLocally: true })
  static async testLocalRepeatableReadProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ readOnly: true, isolationLevel: "READ COMMITTED", executeLocally: true })
  static async testLocalConfiguredProcedure_v2(ctxt: StoredProcedureContext, message: string): Promise<void> {  }

  @TestInitializer()
  static async testDBOSInitializer(ctxt: InitContext): Promise<void> {  }

  @TestDeploy()
  static async testDBOSDeploy(ctxt: InitContext): Promise<void> {  }
}`;
