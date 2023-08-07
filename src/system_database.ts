/* eslint-disable @typescript-eslint/no-explicit-any */

import { OperonNull } from "./operon";

interface SystemDatabase {
  checkWorkflowOutput<R>() : Promise<OperonNull | R>;
  recordWorkflowOutput<R>(output: R) : Promise<void>;
  recordWorkflowError(error: Error) : Promise<void>;

  checkCommunicatorOutput<R>() : Promise<OperonNull | R>;
  recordCommunicatorOutput<R>(output: R) : Promise<void>;
  recordCommunicatorError(error: Error): Promise<void>;

  send<T extends NonNullable<any>>(topic: string, key: string, message: T) : Promise<boolean>;
  recv<T extends NonNullable<any>>(topic: string, key: string, timeout: number) : Promise<T | null>;
}

class PostgresSystemDatabase implements SystemDatabase {
  checkWorkflowOutput<R>(): Promise<R | OperonNull> {
    throw new Error("Method not implemented.");
  }
  recordWorkflowOutput<R>(output: R): Promise<void> {
    throw new Error("Method not implemented.");
  }
  recordWorkflowError(error: Error): Promise<void> {
    throw new Error("Method not implemented.");
  }
  checkCommunicatorOutput<R>(): Promise<OperonNull | R> {
    throw new Error("Method not implemented.");
  }
  recordCommunicatorOutput<R>(output: R): Promise<void> {
    throw new Error("Method not implemented.");
  }
  recordCommunicatorError(error: Error): Promise<void> {
    throw new Error("Method not implemented.");
  }
  send<T extends NonNullable<any>>(topic: string, key: string, message: T): Promise<boolean> {
    throw new Error("Method not implemented.");
  }
  recv<T extends NonNullable<any>>(topic: string, key: string, timeout: number): Promise<T | null> {
    throw new Error("Method not implemented.");
  }
    
}