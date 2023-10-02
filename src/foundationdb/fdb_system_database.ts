/* eslint-disable @typescript-eslint/no-explicit-any */

import { deserializeError, serializeError } from "serialize-error";
import { OperonNull, operonNull } from "../operon";
import { SystemDatabase } from "../system_database";
import { StatusString, WorkflowStatus } from "../workflow";
import * as fdb from "foundationdb";
import { OperonDuplicateWorkflowEventError, OperonWorkflowConflictUUIDError } from "../error";
import { NativeValue } from "foundationdb/dist/lib/native";

interface WorkflowOutput<R> {
  status: string;
  error: string;
  output: R;
}

interface OperationOutput<R> {
  output: R;
  error: string;
}

const Tables = {
  WorkflowStatus: "operon_workflow_status",
  OperationOutputs: "operon_operation_outputs",
  Notifications: "operon_notifications",
  WorkflowEvents: "workflow_events"
} as const;

export class FoundationDBSystemDatabase implements SystemDatabase {
  dbRoot: fdb.Database<NativeValue, Buffer, NativeValue, Buffer>;
  workflowStatusDB: fdb.Database<string, string, unknown, unknown>;
  operationOutputsDB: fdb.Database<fdb.TupleItem, fdb.TupleItem, unknown, unknown>;
  notificationsDB: fdb.Database<fdb.TupleItem, fdb.TupleItem, unknown, unknown>;
  workflowEventsDB: fdb.Database<fdb.TupleItem, fdb.TupleItem, unknown, unknown>;

  readonly workflowStatusBuffer: Map<string, unknown> = new Map();

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
    this.workflowEventsDB = this.dbRoot
      .at(Tables.WorkflowEvents)
      .withKeyEncoding(fdb.encoders.tuple) // We use [workflowUUID, key] as the key
      .withValueEncoding(fdb.encoders.json); // and values using JSON
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  async init(): Promise<void> { }

  // eslint-disable-next-line @typescript-eslint/require-await
  async destroy(): Promise<void> {
    this.dbRoot.close();
  }

  async checkWorkflowOutput<R>(workflowUUID: string): Promise<R | OperonNull> {
    const output = (await this.workflowStatusDB.get(workflowUUID)) as WorkflowOutput<R> | undefined;
    if (output === undefined || output.status === StatusString.PENDING) {
      return operonNull;
    } else if (output.status === StatusString.ERROR) {
      throw deserializeError(JSON.parse(output.error));
    } else {
      return output.output;
    }
  }

  async setWorkflowStatus(workflowUUID: string) {
    await this.workflowStatusDB.doTransaction(async (txn) => {
      const present = await txn.get(workflowUUID);
      if (present === undefined) {
        txn.set(workflowUUID, {
          status: StatusString.PENDING,
          error: null,
          output: null,
        });
      }
    });
  }

  bufferWorkflowOutput<R>(workflowUUID: string, output: R) {
    this.workflowStatusBuffer.set(workflowUUID, output);
  }

  async flushWorkflowStatusBuffer(): Promise<string[]> {
    const localBuffer = new Map(this.workflowStatusBuffer);
    this.workflowStatusBuffer.clear();
    // eslint-disable-next-line @typescript-eslint/require-await
    await this.workflowStatusDB.doTransaction(async (txn) => {
      for (const [workflowUUID, output] of localBuffer) {
          txn.set(workflowUUID, {
            status: StatusString.SUCCESS,
            error: null,
            output: output,
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
      return { status: StatusString.UNKNOWN };
    }
    return { status: output.status };
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
    } else if (status === StatusString.ERROR) {
      throw deserializeError(JSON.parse(output.error));
    } else { // StatusString.PENDING
      return this.getWorkflowResult(workflowUUID);
    }
  }

  async send<T>(workflowUUID: string, functionID: number, destinationUUID: string, topic: string, message: T): Promise<void> {
    return this.dbRoot.doTransaction(async (txn) => {
      const operationOutputs = txn.at(this.operationOutputsDB);
      const notifications = txn.at(this.notificationsDB);
      // For OAOO, check if the send already ran.
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
    // For OAOO, check if the recv already ran.
    const output = (await this.operationOutputsDB.get([workflowUUID, functionID])) as OperationOutput<T | null> | undefined;
    if (output !== undefined) {
      return output.output;
    }
    // Check if there is a message in the queue, waiting for one to arrive if not.
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
    // Consume and return the message, recording the operation for OAOO.
    return this.dbRoot.doTransaction(async (txn) => {
      const operationOutputs = txn.at(this.operationOutputsDB);
      const notifications = txn.at(this.notificationsDB);
      const messages = (await notifications.get([workflowUUID, topic])) as Array<unknown> | undefined;
      const message = (messages ? messages.shift() as T : undefined) ?? null;  // If no message is found, return null.
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

  async setEvent<T extends NonNullable<any>>(workflowUUID: string, functionID: number, key: string, value: T): Promise<void> {
    return this.dbRoot.doTransaction(async (txn) => {
      const operationOutputs = txn.at(this.operationOutputsDB);
      const workflowEvents = txn.at(this.workflowEventsDB);
      // For OAOO, check if the set already ran.
      const output = (await operationOutputs.get([workflowUUID, functionID])) as OperationOutput<boolean>;
      if (output !== undefined) {
        return;
      }

      const exists = await workflowEvents.get([workflowUUID, key]);
      if (exists === undefined) {
        workflowEvents.set([workflowUUID, key], value);
      } else {
        throw new OperonDuplicateWorkflowEventError(workflowUUID, key);
      }
      // For OAOO, record the set.
      operationOutputs.set([workflowUUID, functionID], { error: null, output: undefined });
    });
  }

  async getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number): Promise<T | null> {
    // Check if the value is present, otherwise wait for it to arrive.
    const watch = await this.workflowEventsDB.getAndWatch([workflowUUID, key]);
    if (watch.value === undefined) {
      const timeout = setTimeout(() => {
        watch.cancel();
      }, timeoutSeconds * 1000);
      await watch.promise;
      clearInterval(timeout);
    } else {
      watch.cancel();
    }
    // Return the value, or null if none exists.
    return (await this.workflowEventsDB.get([workflowUUID, key])) as T ?? null;
  }
}
