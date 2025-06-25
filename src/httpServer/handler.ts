import {
  MethodParameter,
  getOrCreateMethodArgsRegistration,
  MethodRegistrationBase,
  ConfiguredInstance,
} from '../decorators';
import { TailParameters, WorkflowHandle, WorkflowContext } from '../workflow';
import { APITypes, ArgSources } from './handlerTypes';

// local type declarations for workflow functions
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type WFFunc = (ctxt: WorkflowContext, ...args: any[]) => Promise<unknown>;

export type AsyncHandlerWfFuncs<T> = T extends ConfiguredInstance
  ? never
  : {
      [P in keyof T as T[P] extends WFFunc ? P : never]: T[P] extends WFFunc
        ? (...args: TailParameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>>
        : never;
    };

export interface HandlerRegistrationBase extends MethodRegistrationBase {
  apiType: APITypes;
  apiURL: string;
  args: HandlerParameter[];
}

export class HandlerParameter extends MethodParameter {
  argSource: ArgSources = ArgSources.DEFAULT;

  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  constructor(idx: number, at: Function) {
    super(idx, at);
  }
}

///////////////////////////////////
/* ENDPOINT PARAMETER DECORATORS */
///////////////////////////////////

export function ArgSource(source: ArgSources) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateMethodArgsRegistration(target, undefined, propertyKey);

    const curParam = existingParameters[parameterIndex] as HandlerParameter;
    curParam.argSource = source;
  };
}
