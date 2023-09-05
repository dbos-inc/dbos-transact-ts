import { deserializeError, serializeError } from "serialize-error";
import { OperonNull, operonNull } from "../operon";
import { SystemDatabase } from "../system_database";
import { StatusString, WorkflowStatus } from "../workflow";
import * as fdb from "foundationdb";
import { OperonWorkflowConflictUUIDError } from "../error";
import { NativeValue } from "foundationdb/dist/lib/native";

interface WorkflowOutput<R> {
  status: string;
  error: string;
  output: R;
  updatedAtEpochMs: number;
}

interface OperationOutput<R> {
  output: R;
  error: string;
}

const Tables = {
  WorkflowStatus: "operon_workflow_status",
  OperationOutputs: "operon_operation_outputs",
  Notifications: "operon_notifications",
} as const;

export class FoundationDBSystemDatabase implements SystemDatabase {
  dbRoot: fdb.Database<NativeValue, Buffer, NativeValue, Buffer>;
  workflowStatusDB: fdb.Database<string, string, unknown, unknown>;
  operationOutputsDB: fdb.Database<fdb.TupleItem, fdb.TupleItem, unknown, unknown>;
  notificationsDB: fdb.Database<fdb.TupleItem, fdb.TupleItem, unknown, unknown>;

  readonly workflowOutputBuffer: Map<string, unknown> = new Map();

  constructor() {
    fdb.setAPIVersion(710, 710);
    this.dbRoot = fdb.open();
    this.workflowStatusDB = this.dbRoot
      .at(Tables.WorkflowStatus)
      .withKeyEncoding(fdb.encoders.string) // We use workflowUUID as the key
      .withValueEncoding(fdb.encoders.json); // and values using JSON
    this.operationOutputsDB = this.dbRoot
      .at(Tables.OperationOutputs)
      .withKeyEncoding(fdb.encoders.tuple) // We use [workflowUUID, function_id] as the key
      .withValueEncoding(fdb.encoders.json); // and values using JSON
    this.notificationsDB = this.dbRoot
      .at(Tables.Notifications)
      .withKeyEncoding(fdb.encoders.tuple) // We use [destinationUUID, topic] as the key
      .withValueEncoding(fdb.encoders.json); // and values using JSON
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  async init(): Promise<void> {}

  // eslint-disable-next-line @typescript-eslint/require-await
  async destroy(): Promise<void> {
    this.dbRoot.close();
  }

  async checkWorkflowOutput<R>(workflowUUID: string): Promise<R | OperonNull> {
    const output = (await this.workflowStatusDB.get(workflowUUID)) as WorkflowOutput<R> | undefined;
    if (output === undefined) {
      return operonNull;
    } else if (output.status === StatusString.ERROR) {
      throw deserializeError(JSON.parse(output.error));
    } else {
      return output.output;
    }
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  async bufferWorkflowOutput<R>(workflowUUID: string, output: R): Promise<void> {
    this.workflowOutputBuffer.set(workflowUUID, output);
  }

  async flushWorkflowOutputBuffer(): Promise<string[]> {
    const localBuffer = new Map(this.workflowOutputBuffer);
    this.workflowOutputBuffer.clear();
    // eslint-disable-next-line @typescript-eslint/require-await
    await this.workflowStatusDB.doTransaction(async (txn) => {
      for (const [workflowUUID, output] of localBuffer) {
        txn.set(workflowUUID, {
          status: StatusString.SUCCESS,
          error: null,
          output: output,
          updatedAtEpochMs: Math.floor(new Date().getTime() / 1000),
        });
      }
    });
    return Array.from(localBuffer.keys());
  }

  async recordWorkflowError(workflowUUID: string, error: Error): Promise<void> {
    const serialErr = JSON.stringify(serializeError(error));
    await this.workflowStatusDB.set(workflowUUID, {
      status: StatusString.ERROR,
      error: serialErr,
      output: null,
    });
  }

  async checkCommunicatorOutput<R>(workflowUUID: string, functionID: number): Promise<OperonNull | R> {
    const output = (await this.operationOutputsDB.get([workflowUUID, functionID])) as OperationOutput<R> | undefined;
    if (output === undefined) {
      return operonNull;
    } else if (JSON.parse(output.error) !== null) {
      throw deserializeError(JSON.parse(output.error));
    } else {
      return output.output;
    }
  }

  async recordCommunicatorOutput<R>(workflowUUID: string, functionID: number, output: R): Promise<void> {
    await this.operationOutputsDB.doTransaction(async (txn) => {
      // Check if the key exists.
      const keyOutput = await txn.get([workflowUUID, functionID]);
      if (keyOutput !== undefined) {
        throw new OperonWorkflowConflictUUIDError(workflowUUID);
      }
      txn.set([workflowUUID, functionID], {
        error: null,
        output: output,
      });
    });
  }

  async recordCommunicatorError(workflowUUID: string, functionID: number, error: Error): Promise<void> {
    const serialErr = JSON.stringify(serializeError(error));
    await this.operationOutputsDB.doTransaction(async (txn) => {
      // Check if the key exists.
      const keyOutput = await txn.get([workflowUUID, functionID]);
      if (keyOutput !== undefined) {
        throw new OperonWorkflowConflictUUIDError(workflowUUID);
      }
      txn.set([workflowUUID, functionID], {
        error: serialErr,
        output: null,
      });
    });
  }

  async getWorkflowStatus(workflowUUID: string): Promise<WorkflowStatus> {
    const output = (await this.workflowStatusDB.get(workflowUUID)) as WorkflowOutput<unknown> | undefined;
    if (output === undefined) {
      return { status: StatusString.UNKNOWN, updatedAtEpochMs: -1 };
    }
    return { status: output.status, updatedAtEpochMs: output.updatedAtEpochMs };
  }

  async getWorkflowResult<R>(workflowUUID: string): Promise<R> {
    const watch = await this.workflowStatusDB.getAndWatch(workflowUUID);
    let value = watch.value;
    if (value === undefined) {
      await watch.promise;
      value = await this.workflowStatusDB.get(workflowUUID);
    } else {
      watch.cancel();
    }
    const output = value as WorkflowOutput<R>;
    const status = output.status;
    if (status === StatusString.SUCCESS) {
      return output.output;
    } else {
      throw deserializeError(JSON.parse(output.error));
    }
  }

  async send<T>(workflowUUID: string, functionID: number, destinationUUID: string, topic: string, message: T): Promise<void> {
    return this.dbRoot.doTransaction(async (txn) => {
      const operationOutputs = txn.at(this.operationOutputsDB);
      const notifications = txn.at(this.notificationsDB);
      const output = (await operationOutputs.get([workflowUUID, functionID])) as OperationOutput<boolean>;
      if (output !== undefined) {
        return;
      }

      // Retrieve the message queue.
      const exists = (await notifications.get([destinationUUID, topic])) as Array<unknown> | undefined;
      if (exists === undefined) {
        notifications.set([destinationUUID, topic], [message]);
      } else {
        // Append to the existing message queue.
        exists.push(message);
        notifications.set([destinationUUID, topic], exists);
      }
      operationOutputs.set([workflowUUID, functionID], { error: null, output: undefined });
    });
  }

  async recv<T>(workflowUUID: string, functionID: number, topic: string, timeoutSeconds: number): Promise<T | null> {
    const output = (await this.operationOutputsDB.get([workflowUUID, functionID])) as OperationOutput<T | null> | undefined;
    if (output !== undefined) {
      return output.output;
    }
    const watch = await this.notificationsDB.getAndWatch([workflowUUID, topic]);
    if (watch.value === undefined) {
      const timeout = setTimeout(() => {
        watch.cancel();
      }, timeoutSeconds * 1000);
      await watch.promise;
      clearInterval(timeout);
    } else {
      watch.cancel();
    }
    return this.dbRoot.doTransaction(async (txn) => {
      const operationOutputs = txn.at(this.operationOutputsDB);
      const notifications = txn.at(this.notificationsDB);
      const messages = (await notifications.get([workflowUUID, topic])) as Array<unknown> | undefined;
      const message = (messages ? messages.shift() as T : undefined) ?? null;  // Force the message to be null.
      const output = await operationOutputs.get([workflowUUID, functionID]);
      if (output !== undefined) {
        throw new OperonWorkflowConflictUUIDError(workflowUUID);
      }
      operationOutputs.set([workflowUUID, functionID], { error: null, output: message });
      if (messages && messages.length > 0) {
        notifications.set([workflowUUID, topic], messages);  // Update the message table.
      } else {
        notifications.clear([workflowUUID, topic]);
      }
      return message;
    });
  }
}
