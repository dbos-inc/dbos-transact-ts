export const sampleDbosClass = /*ts*/`
import {
  DBOS,
  GetApi, PostApi, PutApi, PatchApi, DeleteApi, HandlerContext,
  Workflow, WorkflowContext,
  Communicator, CommunicatorContext,
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
  static async testGetHandler_v2(ctxt: HandlerContext): Promise<void> {  }

  @DBOS.postApi('/test')
  static async testPostHandler_v2(ctxt: HandlerContext): Promise<void> {  }

  @DBOS.patchApi('/test')
  static async testPatchHandler_v2(ctxt: HandlerContext): Promise<void> {  }

  @DBOS.putApi('/test')
  static async testPutHandler_v2(ctxt: HandlerContext): Promise<void> {  }

  @DBOS.deleteApi('/test')
  static async testDeleteHandler_v2(ctxt: HandlerContext): Promise<void> {  }

  @GetApi('/test')
  @Workflow()
  static async testGetHandlerWorkflow(ctxt: HandlerContext): Promise<void> {  }

  @GetApi('/test')
  @Transaction()
  static async testGetHandlerTx(ctxt: HandlerContext): Promise<void> {  }

  @GetApi('/test')
  @Communicator()
  static async testGetHandlerComm(ctxt: HandlerContext): Promise<void> {  }

  @Workflow()
  static async testWorkflow(ctxt: WorkflowContext): Promise<void> {  }

  @Communicator()
  static async testCommunicator(ctxt: CommunicatorContext, message: string): Promise<void> {  }

  @Transaction()
  static async testTransaction(ctxt: TransactionContext<Knex>, message: string): Promise<void> {  }

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

  @TestWorkflow()
  static async testWorkflow(ctxt: WorkflowContext): Promise<void> {  }

  @TestCommunicator()
  static async testCommunicator(ctxt: CommunicatorContext, message: string): Promise<void> {  }

  @TestTransaction()
  static async testTransaction(ctxt: TransactionContext<Knex>, message: string): Promise<void> {  }

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

  @TestInitializer()
  static async testDBOSInitializer(ctxt: InitContext): Promise<void> {  }

  @TestDeploy()
  static async testDBOSDeploy(ctxt: InitContext): Promise<void> {  }
}`;
