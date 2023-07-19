/* eslint-disable @typescript-eslint/no-explicit-any */
export type OperonCommunicator<T extends any[], R> = (ctxt: CommunicatorContext, ...args: T) => Promise<R>;

export class CommunicatorContext {

  // TODO: decide what goes into this context.

  readonly functionID: number;

  constructor(functionID: number) {
    this.functionID = functionID;
  }

}