/* eslint-disable @typescript-eslint/no-explicit-any */

import { deserializeError, serializeError } from "serialize-error";
import { DBOSExecutor, DBOSNull, dbosNull } from "../dbos-executor";
import { WorkflowStatusInternal, SystemDatabase } from "../system_database";
import { StatusString, WorkflowStatus } from "../workflow";
import * as fdb from "foundationdb";
import { DuplicateWorkflowEventError, DBOSWorkflowConflictUUIDError } from "../error";
import { NativeValue } from "foundationdb/dist/lib/native";
import { sleep } from "../utils";

interface OperationOutput<R> {
  output: R;
  error: string;
}

const Tables = {
  WorkflowStatus: "dbos_workflow_status",
  OperationOutputs: "dbos_operation_outputs",
  Notifications: "dbos_notifications",
  WorkflowEvents: "workflow_events",
  WorkflowInpus: "workflow_inputs"
} as const;

export class FoundationDBSystemDatabase implements SystemDatabase {
  dbRoot: fdb.Database<NativeValue, Buffer, NativeValue, Buffer>;
  workflowStatusDB: fdb.Database<string, string, unknown, unknown>;
  operationOutputsDB: fdb.Database<fdb.TupleItem, fdb.TupleItem, unknown, unknown>;
  notificationsDB: fdb.Database<fdb.TupleItem, fdb.TupleItem, unknown, unknown>;
  workflowEventsDB: fdb.Database<fdb.TupleItem, fdb.TupleItem, unknown, unknown>;
  workflowInputsDB: fdb.Database<string, string, unknown, unknown>;

  readonly workflowStatusBuffer: Map<string, WorkflowStatusInternal> = new Map();
  readonly workflowInputsBuffer: Map<string, any[]> = new Map();

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
    this.workflowInputsDB = this.dbRoot
      .at(Tables.WorkflowInpus)
      .withKeyEncoding(fdb.encoders.string) // We use workflowUUID as the key
      .withValueEncoding(fdb.encoders.json);
  }

  async init(): Promise<void> {}

  async destroy(): Promise<void> {
    this.dbRoot.close();
    return Promise.resolve()
  }

  async checkWorkflowOutput<R>(workflowUUID: string): Promise<R | DBOSNull> {
    const output = (await this.workflowStatusDB.get(workflowUUID)) as WorkflowStatusInternal | undefined;
    if (output === undefined || output.status === StatusString.PENDING) {
      return dbosNull;
    } else if (output.status === StatusString.ERROR) {
      throw deserializeError(JSON.parse(output.error));
    } else {
      return output.output as R;
    }
  }

  async initWorkflowStatus<T extends any[]>(initStatus: WorkflowStatusInternal, args: T): Promise<T> {
    return this.dbRoot.doTransaction(async (txn) => {
      const statusDB = txn.at(this.workflowStatusDB);
      const inputsDB = txn.at(this.workflowInputsDB);
      const present = await statusDB.get(initStatus.workflowUUID);
      if (present === undefined) {
        statusDB.set(initStatus.workflowUUID, {
          status: initStatus.status,
          error: null,
          output: null,
          name: initStatus.name,
          authenticatedUser: initStatus.authenticatedUser,
          assumedRole: initStatus.assumedRole,
          authenticatedRoles: initStatus.authenticatedRoles,
          request: initStatus.request,
          executorID: initStatus.executorID,
        });
      }

      const inputs = await inputsDB.get(initStatus.workflowUUID);
      if (inputs === undefined) {
        inputsDB.set(initStatus.workflowUUID, args);
        return args;
      }
      return inputs as T;
    });
  }

  bufferWorkflowOutput(workflowUUID: string, status: WorkflowStatusInternal) {
    this.workflowStatusBuffer.set(workflowUUID, status);
  }

  // TODO: support batching
  async flushWorkflowSystemBuffers(): Promise<void> {
    await this.flushWorkflowStatusBuffer();
    await this.flushWorkflowInputsBuffer();
  }

  async flushWorkflowStatusBuffer(): Promise<void> {
    const localBuffer = new Map(this.workflowStatusBuffer);
    this.workflowStatusBuffer.clear();
    // eslint-disable-next-line @typescript-eslint/require-await
    await this.workflowStatusDB.doTransaction(async (txn) => {
      for (const [workflowUUID, status] of localBuffer) {
        txn.set(workflowUUID, {
          status: status.status,
          error: null,
          output: status.output,
          name: status.name,
          authenticatedUser: status.authenticatedUser,
          authenticatedRoles: status.authenticatedRoles,
          assumedRole: status.assumedRole,
          request: status.request,
        });
      }
    });
    return;
  }

  async recordWorkflowError(workflowUUID: string, status: WorkflowStatusInternal): Promise<void> {
    await this.workflowStatusDB.set(workflowUUID, {
      status: StatusString.ERROR,
      error: status.error,
      output: null,
      name: status.name,
      authenticatedUser: status.authenticatedUser,
      authenticatedRoles: status.authenticatedRoles,
      assumedRole: status.assumedRole,
      request: status.request,
    });
  }

  async getPendingWorkflows(executorID: string): Promise<string[]> {
    const workflows = (await this.workflowStatusDB.getRangeAll("", "\xff")) as Array<[string, WorkflowStatusInternal]>;
    return workflows.filter((i) => i[1].status === StatusString.PENDING && i[1].executorID === executorID).map((i) => i[0]);
  }

  bufferWorkflowInputs<T extends any[]>(workflowUUID: string, args: T): void {
    this.workflowInputsBuffer.set(workflowUUID, args);
  }

  // TODO: support batching
  async flushWorkflowInputsBuffer(): Promise<void> {
    const localBuffer = new Map(this.workflowInputsBuffer);
    this.workflowInputsBuffer.clear();
    await this.workflowInputsDB.doTransaction(async (txn) => {
      for (const [workflowUUID, args] of localBuffer) {
        const inputs = await txn.get(workflowUUID);
        if (inputs === undefined) {
          txn.set(workflowUUID, args);
        }
      }
    });
    return;
  }

  async getWorkflowInputs<T extends any[]>(workflowUUID: string): Promise<T | null> {
    return ((await this.workflowInputsDB.get(workflowUUID)) as T) ?? null;
  }

  async checkOperationOutput<R>(workflowUUID: string, functionID: number): Promise<DBOSNull | R> {
    const output = (await this.operationOutputsDB.get([workflowUUID, functionID])) as OperationOutput<R> | undefined;
    if (output === undefined) {
      return dbosNull;
    } else if (JSON.parse(output.error) !== null) {
      throw deserializeError(JSON.parse(output.error));
    } else {
      return output.output;
    }
  }

  async recordOperationOutput<R>(workflowUUID: string, functionID: number, output: R): Promise<void> {
    await this.operationOutputsDB.doTransaction(async (txn) => {
      // Check if the key exists.
      const keyOutput = await txn.get([workflowUUID, functionID]);
      if (keyOutput !== undefined) {
        throw new DBOSWorkflowConflictUUIDError(workflowUUID);
      }
      txn.set([workflowUUID, functionID], {
        error: null,
        output: output,
      });
    });
  }

  async recordOperationError(workflowUUID: string, functionID: number, error: Error): Promise<void> {
    const serialErr = JSON.stringify(serializeError(error));
    await this.operationOutputsDB.doTransaction(async (txn) => {
      // Check if the key exists.
      const keyOutput = await txn.get([workflowUUID, functionID]);
      if (keyOutput !== undefined) {
        throw new DBOSWorkflowConflictUUIDError(workflowUUID);
      }
      txn.set([workflowUUID, functionID], {
        error: serialErr,
        output: null,
      });
    });
  }

  async getWorkflowStatus(workflowUUID: string, callerUUID?: string, functionID?: number): Promise<WorkflowStatus | null> {
    // Check if the operation has been done before for OAOO (only do this inside a workflow).
    if (callerUUID !== undefined && functionID !== undefined) {
      const prev = (await this.operationOutputsDB.get([callerUUID, functionID])) as OperationOutput<WorkflowStatus | null> | undefined;
      if (prev !== undefined) {
        return prev.output;
      }
    }

    const output = (await this.workflowStatusDB.get(workflowUUID)) as WorkflowStatusInternal | undefined;
    let value = null;
    if (output !== undefined) {
      value = {
        status: output.status,
        workflowName: output.name,
        authenticatedUser: output.authenticatedUser,
        authenticatedRoles: output.authenticatedRoles,
        assumedRole: output.assumedRole,
        request: output.request,
      };
    }

    // Record the output if it is inside a workflow.
    if (callerUUID !== undefined && functionID !== undefined) {
      await this.recordOperationOutput(callerUUID, functionID, value);
    }
    return value;
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
    const output = value as WorkflowStatusInternal;
    const status = output.status;
    if (status === StatusString.SUCCESS) {
      return output.output as R;
    } else if (status === StatusString.ERROR) {
      throw deserializeError(JSON.parse(output.error));
    } else {
      // StatusString.PENDING
      return this.getWorkflowResult(workflowUUID);
    }
  }

  readonly nullTopic = "__null__topic__";

  async send<T>(workflowUUID: string, functionID: number, destinationUUID: string, message: T, topic?: string): Promise<void> {
    const currTopic: string = topic ?? this.nullTopic;

    return this.dbRoot.doTransaction(async (txn) => {
      const operationOutputs = txn.at(this.operationOutputsDB);
      const notifications = txn.at(this.notificationsDB);
      // For OAOO, check if the send already ran.
      const output = (await operationOutputs.get([workflowUUID, functionID])) as OperationOutput<boolean>;
      if (output !== undefined) {
        return;
      }

      // Retrieve the message queue.
      const exists = (await notifications.get([destinationUUID, currTopic])) as Array<unknown> | undefined;
      if (exists === undefined) {
        notifications.set([destinationUUID, currTopic], [message]);
      } else {
        // Append to the existing message queue.
        exists.push(message);
        notifications.set([destinationUUID, currTopic], exists);
      }
      operationOutputs.set([workflowUUID, functionID], { error: null, output: undefined });
    });
  }

  async recv<T>(workflowUUID: string, functionID: number, topic?: string, timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec): Promise<T | null> {
    const currTopic = topic ?? this.nullTopic;
    // For OAOO, check if the recv already ran.
    const output = (await this.operationOutputsDB.get([workflowUUID, functionID])) as OperationOutput<T | null> | undefined;
    if (output !== undefined) {
      return output.output;
    }
    // Check if there is a message in the queue, waiting for one to arrive if not.
    const watch = await this.notificationsDB.getAndWatch([workflowUUID, currTopic]);
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
      const messages = (await notifications.get([workflowUUID, currTopic])) as Array<unknown> | undefined;
      const message = (messages ? (messages.shift() as T) : undefined) ?? null; // If no message is found, return null.
      const output = await operationOutputs.get([workflowUUID, functionID]);
      if (output !== undefined) {
        throw new DBOSWorkflowConflictUUIDError(workflowUUID);
      }
      operationOutputs.set([workflowUUID, functionID], { error: null, output: message });
      if (messages && messages.length > 0) {
        notifications.set([workflowUUID, currTopic], messages); // Update the message table.
      } else {
        notifications.clear([workflowUUID, currTopic]);
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
        throw new DuplicateWorkflowEventError(workflowUUID, key);
      }
      // For OAOO, record the set.
      operationOutputs.set([workflowUUID, functionID], { error: null, output: undefined });
    });
  }

  async getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number, callerUUID?: string, functionID?: number): Promise<T | null> {
    // Check if the operation has been done before for OAOO (only do this inside a workflow).
    if (callerUUID !== undefined && functionID !== undefined) {
      const output = (await this.operationOutputsDB.get([callerUUID, functionID])) as OperationOutput<T | null> | undefined;
      if (output !== undefined) {
        return output.output;
      }
    }

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
    let value: T | null = null;
    if (watch.value !== undefined) {
      value = watch.value as T;
    } else {
      value = ((await this.workflowEventsDB.get([workflowUUID, key])) as T) ?? null;
    }

    // Record the output if it is inside a workflow.
    if (callerUUID !== undefined && functionID !== undefined) {
      await this.recordOperationOutput(callerUUID, functionID, value);
    }
    return value;
  }


  async sleep(_workflowUUID: string, _functionID: number, durationSec: number): Promise<void> {
    await sleep(durationSec * 1000); // TODO: Implement
  }

  /* SCHEDULER */
  getLastScheduledTime(_wfn: string): Promise<number | null> {
    return Promise.resolve(null);
  }
  setLastScheduledTime(_wfn: string, _invtime: number): Promise<number | null> {
    return Promise.resolve(null);
  }
}
