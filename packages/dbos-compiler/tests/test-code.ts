import { DbosDecoratorKind } from '../compiler';

export const sampleDbosClass = /*ts*/ `
import { DBOS } from "@dbos-inc/dbos-sdk";
import { Knex } from 'knex';

export class Test {

  @DBOS.workflow()
  static async testWorkflow(): Promise<void> {  }

  @DBOS.step()
  static async testStep(message: string): Promise<void> {  }

  @DBOS.step()
  static async testCommunicator(message: string): Promise<void> {  }

  @DBOS.transaction()
  static async testTransaction(message: string): Promise<void> {  }

  @DBOS.storedProcedure()
  static async testProcedure(message: string): Promise<void> {  }

  @DBOS.storedProcedure({ readOnly: true })
  static async testReadOnlyProcedure(message: string): Promise<void> {  }

  @DBOS.storedProcedure({ isolationLevel: "REPEATABLE READ" })
  static async testRepeatableReadProcedure(message: string): Promise<void> {  }

  @DBOS.storedProcedure({ readOnly: true, isolationLevel: "READ COMMITTED" })
  static async testConfiguredProcedure(message: string): Promise<void> {  }

  @DBOS.storedProcedure({ executeLocally: true})
  static async testLocalProcedure(message: string): Promise<void> {  }

  @DBOS.storedProcedure({ readOnly: true, executeLocally: true })
  static async testLocalReadOnlyProcedure(message: string): Promise<void> {  }

  @DBOS.storedProcedure({ isolationLevel: "REPEATABLE READ", executeLocally: true })
  static async testLocalRepeatableReadProcedure(message: string): Promise<void> {  }

  @DBOS.storedProcedure({ readOnly: true, isolationLevel: "READ COMMITTED", executeLocally: true })
  static async testLocalConfiguredProcedure(message: string): Promise<void> {  }
}
`;

export const sampleDbosClassAliased = /*ts*/ `
import { DBOS as TestDBOS } from "@dbos-inc/dbos-sdk";
import { Knex } from 'knex';

export class Test {

  @TestDBOS.workflow()
  static async testWorkflow(): Promise<void> {  }

  @TestDBOS.step()
  static async testStep(message: string): Promise<void> {  }

  @TestDBOS.step()
  static async testCommunicator(message: string): Promise<void> {  }

  @TestDBOS.transaction()
  static async testTransaction(message: string): Promise<void> {  }

  @TestDBOS.storedProcedure()
  static async testProcedure(message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ readOnly: true })
  static async testReadOnlyProcedure(message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ isolationLevel: "REPEATABLE READ" })
  static async testRepeatableReadProcedure(message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ readOnly: true, isolationLevel: "READ COMMITTED" })
  static async testConfiguredProcedure(message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ executeLocally: true})
  static async testLocalProcedure(message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ readOnly: true, executeLocally: true })
  static async testLocalReadOnlyProcedure(message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ isolationLevel: "REPEATABLE READ", executeLocally: true })
  static async testLocalRepeatableReadProcedure(message: string): Promise<void> {  }

  @TestDBOS.storedProcedure({ readOnly: true, isolationLevel: "READ COMMITTED", executeLocally: true })
  static async testLocalConfiguredProcedure(message: string): Promise<void> {  }
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
