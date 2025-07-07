import { MethodParameter, getOrCreateMethodArgsRegistration, MethodRegistrationBase } from '../decorators';
import { APITypes, ArgSources } from './handlerTypes';

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
    const existingParameters = getOrCreateMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex] as HandlerParameter;
    curParam.argSource = source;
  };
}
