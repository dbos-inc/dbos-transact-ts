/* eslint-disable @typescript-eslint/no-non-null-assertion */
/* May make sense: eslint-disable @typescript-eslint/ban-types */

// TODO List
// General
//   Class level decorators - defaults
//   Field / property decorators - persistent data
//
//   Integrate parameter extraction / validation that currently lives in demo app repo
//
//   Find a way to unit test - perhaps a mock log collector?
//     Or is it easier once there is a real log collector?
//
// Workflow
//
// Logging
//   Integrate with Logger setup
//   Integrate with Logger buffer

import "reflect-metadata";

import * as crypto from "crypto";
import { TransactionConfig, TransactionContext } from "./transaction";
import { WorkflowConfig, WorkflowContext } from "./workflow";
import { CommunicatorConfig, CommunicatorContext } from "./communicator";
import { OperonContext } from "./context";
import { OperonDataValidationError } from "./error";

/**
 * Any column type column can be.
 */
type OperonFieldType = "integer" | "double" | "decimal" | "timestamp" | "text" | "varchar" | "boolean" | "uuid" | "json";

interface OperonDataType {
  fieldType: OperonFieldType;
  length?: number;
  precision?: number;
  scale?: number;
}

function makeDataType(type: Function): OperonDataType {
  if (type === String) return { fieldType: "text"};
  if (type === Date) return { fieldType: "timestamp"}
  if (type === Number) return { fieldType: "double"};
  if (type === Boolean) return { fieldType: "boolean" };
  return { fieldType: "json"};
}

interface OperonParamMetadata {
  name?: string;
  readonly index: number;
  readonly type: Function;

  logMask: LogMasks;
  dataType: OperonDataType;
}

function makeParamMetadata(type: Function, index: number): OperonParamMetadata {
  return {
    type,
    index,
    logMask: LogMasks.NONE,
    dataType: makeDataType(type)
  }
}

const operonParameterMetadataKey = Symbol("operon:parameter");

function getParamMetadata(target: object, propertyKey: string | symbol): OperonParamMetadata[] {
  let params = Reflect.getOwnMetadata(operonParameterMetadataKey, target, propertyKey) as OperonParamMetadata[] | undefined;
  if (!params) {
    // eslint-disable-next-line @typescript-eslint/ban-types
    const designParamTypes = Reflect.getMetadata("design:paramtypes", target, propertyKey) as Function[];
    params = designParamTypes.map(makeParamMetadata);
    Reflect.defineMetadata(operonParameterMetadataKey, params, target, propertyKey);
  }
  return params;
}

interface OperonMethodMetadata<This, Args extends unknown[], Return> {
  readonly name: string;
  readonly paramNames: string[];
  readonly origFunction: (this: This, ...args: Args) => Promise<Return>;
  requiredRole?: string[];
  traceLevel?: TraceLevels;
}

const operonMethodMetadataKey = Symbol("operon:method");

function getMethodMetadata<This, Args extends unknown[], Return>(
  target: object, 
  propertyKey: string | symbol, 
  func: (this: This, ...args: Args) => Promise<Return>
): OperonMethodMetadata<This, Args, Return> {
  let md = Reflect.getOwnMetadata(operonMethodMetadataKey, target, propertyKey) as OperonMethodMetadata<This, Args, Return> | undefined;
  if (!md) {
    md = {
      name: propertyKey.toString(),
      paramNames: getParamNames(func),
      origFunction: func,
    };
    Reflect.defineMetadata(operonMethodMetadataKey, md, target, propertyKey);
  }
  return md;
}

/* Arguments parsing heuristic:
 * - Convert the function to a string
 * - Minify the function
 * - Remove everything before the first open parenthesis and after the first closed parenthesis
 * This will obviously not work on code that has been obfuscated or optimized as the names get
 *   changed to be really small and useless.
 * 
 * TODO: consider using https://github.com/rphansen91/es-arguments
 **/

// eslint-disable-next-line @typescript-eslint/ban-types
export function getParamNames(func: Function): string[] {
  let fn = func.toString();
  fn = fn.replace(/\s/g, "");
  fn = fn.substring(fn.indexOf("(") + 1, fn.indexOf(")"));
  return fn.split(",");
}

export enum TraceLevels {
  DEBUG = "DEBUG",
  INFO = "INFO",
  WARN = "WARN",
  ERROR = "ERROR",
  CRITICAL = "CRITICAL",
}

export enum LogMasks {
  NONE = "NONE",
  HASH = "HASH",
  SKIP = "SKIP",
}

// export enum TraceEventTypes {
//   METHOD_ENTER = "METHOD_ENTER",
//   METHOD_EXIT = "METHOD_EXIT",
//   METHOD_ERROR = "METHOD_ERROR",
// }

// class BaseTraceEvent {
//   eventType: TraceEventTypes = TraceEventTypes.METHOD_ENTER;
//   eventComponent: string = "";
//   eventLevel: TraceLevels = TraceLevels.DEBUG;
//   eventTime: Date = new Date();
//   authorizedUser: string = "";
//   authorizedRole: string = "";
//   positionalArgs: unknown[] = [];
//   namedArgs: { [x: string]: unknown } = {};

//   toString(): string {
//     return `
//     eventType: ${this.eventType}
//     eventComponent: ${this.eventComponent}
//     eventLevel: ${this.eventLevel}
//     eventTime: ${this.eventTime.toString()}
//     authorizedUser: ${this.authorizedUser}
//     authorizedRole: ${this.authorizedRole}
//     positionalArgs: ${JSON.stringify(this.positionalArgs)}
//     namedArgs: ${JSON.stringify(this.namedArgs)}
//     `;
//   }
// }

// export class OperonParameter {
//   name: string = "";
//   required: boolean = false;
//   validate: boolean = true;
//   logMask: LogMasks = LogMasks.NONE;

//   // eslint-disable-next-line @typescript-eslint/ban-types
//   argType: Function = String;
//   dataType: OperonDataType;
//   index: number = -1;

//   // eslint-disable-next-line @typescript-eslint/ban-types
//   constructor(idx: number, at: Function) {
//     this.index = idx;
//     this.argType = at;
//     this.dataType = OperonDataType.fromArg(at);
//   }
// }


// export interface OperonMethodRegistrationBase {
//   name: string;
//   traceLevel: TraceLevels;

//   args: OperonParameter[];

//   requiredRole: string [];

//   workflowConfig?: WorkflowConfig;
//   txnConfig?: TransactionConfig;
//   commConfig?: CommunicatorConfig;

//   // eslint-disable-next-line @typescript-eslint/ban-types
//   registeredFunction: Function | undefined;

//   invoke(pthis: unknown, args: unknown[]): unknown;
// }

// export class OperonMethodRegistration <This, Args extends unknown[], Return>
// implements OperonMethodRegistrationBase
// {
//   name: string = "";
//   traceLevel: TraceLevels = TraceLevels.INFO;

//   requiredRole: string[] = [];

//   args: OperonParameter[] = [];

//   constructor(origFunc: (this: This, ...args: Args) => Promise<Return>)
//   {
//     this.origFunction = origFunc;
//   }
//   needInitialized: boolean = true;
//   origFunction: (this: This, ...args: Args) => Promise<Return>;
//   registeredFunction: ((this: This, ...args: Args) => Promise<Return>) | undefined;
//   workflowConfig?: WorkflowConfig;
//   txnConfig?: TransactionConfig;
//   commConfig?: CommunicatorConfig;

//   invoke(pthis:This, args: Args) : Promise<Return> {
//     return this.registeredFunction!.call(pthis, ...args);
//   }

//   // TODO: Permissions, attachment point, error handling, etc.
// }

// // Quick and dirty method registration list...
// const methodRegistry: OperonMethodRegistrationBase[] = [];
// export function forEachMethod(f: (m: OperonMethodRegistrationBase) => void) {
//   methodRegistry.forEach(f);
// }

// export function getOrCreateOperonMethodArgsRegistration(target: object, propertyKey: string | symbol): OperonParameter[] {
//   let mParameters: OperonParameter[] = (Reflect.getOwnMetadata(operonParamMetadataKey, target, propertyKey) as OperonParameter[]) || [];

//   if (!mParameters.length) {
//     // eslint-disable-next-line @typescript-eslint/ban-types
//     const designParamTypes = Reflect.getMetadata("design:paramtypes", target, propertyKey) as Function[];
//     mParameters = designParamTypes.map((value, index) => new OperonParameter(index, value));

//     Reflect.defineMetadata(operonParamMetadataKey, mParameters, target, propertyKey);
//   }

//   return mParameters;
// }

// function generateSaltedHash(data: string, salt: string): string {
//   const hash = crypto.createHash("sha256"); // You can use other algorithms like 'md5', 'sha512', etc.
//   hash.update(data + salt);
//   return hash.digest("hex");
// }

// function getOrCreateOperonMethodRegistration<This, Args extends unknown[], Return>(
//   target: object,
//   propertyKey: string | symbol,
//   descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>
// ) {
//   const methReg: OperonMethodRegistration<This, Args, Return> =
//     (Reflect.getOwnMetadata(operonMethodMetadataKey, target, propertyKey) as OperonMethodRegistration<This, Args, Return>) || new OperonMethodRegistration<This, Args, Return>(descriptor.value!);

//   if (methReg.needInitialized) {
//     methReg.name = propertyKey.toString();

//     methReg.args = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

//     const argNames = getArgNames(descriptor.value!);
//     methReg.args.forEach((e) => {
//       if (!e.name) {
//         if (e.index < argNames.length) {
//           e.name = argNames[e.index];
//         }
//         if (e.argType === TransactionContext || e.argType == WorkflowContext || e.argType == CommunicatorContext) {
//           e.logMask = LogMasks.SKIP;
//         }
//         // TODO else warn/log something
//       }
//     });

//     Reflect.defineMetadata(operonMethodMetadataKey, methReg, target, propertyKey);

//     // This is the replacement method
//     const nmethod = async function (this: This, ...args: Args) {
//       const mn = methReg.name;

//       // TODO: Validate the user authentication

//       // TODO: Here let's validate the arguments, being careful to log any validation errors that occur
//       //        And skip/mask arguments
//       methReg.args.forEach((v, idx) => {
//         if (idx === 0)
//         {
//           // Context, may find a more robust way.
//           return;
//         }

//         // Do we have an arg at all
//         if (idx >= args.length) {
//           if (v.required) {
//             throw new OperonDataValidationError(`Insufficient number of arguments calling ${methReg.name} - ${args.length}/${methReg.args.length}`);
//           }
//           return;
//         }

//         let iv = args[idx];
//         if (iv === undefined && v.required) {
//           throw new OperonDataValidationError(`Missing required argument ${v.name} calling ${methReg.name}`);
//         }

//         if (iv instanceof String) {
//           iv = iv.toString();
//           args[idx] = iv;
//         }

//         if (v.dataType.dataType === 'text') {
//           if ((typeof iv !== 'string')) {
//             throw new OperonDataValidationError(`Argument ${v.name} is marked as type 'text' and should be a string calling ${methReg.name}`);
//           }
//         }
//       });

//       // Here let's log the structured record
//       const sLogRec = new BaseTraceEvent();
//       sLogRec.authorizedUser = '';
//       sLogRec.authorizedRole = '';
//       sLogRec.eventType = TraceEventTypes.METHOD_ENTER;
//       sLogRec.eventComponent = mn;
//       sLogRec.eventLevel = methReg.traceLevel;

//       args.forEach((v, idx) => {
//         let isCtx = false;
//         // TODO: we assume the first argument is always a context, need a more robust way to test it.
//         if (idx === 0)
//         {
//           // Context -- I suppose we could just instanceof
//           const ctx = v as OperonContext;
//           sLogRec.authorizedUser = ctx.authUser;
//           sLogRec.authorizedRole = ctx.authRole;
//           isCtx = true;
//         }

//         let lv = v;
//         if (isCtx || methReg.args[idx].logMask === LogMasks.SKIP) {
//           return;
//         } else {
//           if (methReg.args[idx].logMask !== LogMasks.NONE) {
//             // For now this means hash
//             if (methReg.args[idx].dataType.dataType === "json") {
//               lv = generateSaltedHash(JSON.stringify(v), "JSONSALT");
//             } else {
//               // Yes, we are doing the same as above for now.
//               //  It can be better if we have verified the type of the data
//               lv = generateSaltedHash(JSON.stringify(v), "OPERONSALT");
//             }
//           }
//           sLogRec.positionalArgs.push(lv);
//           sLogRec.namedArgs[methReg.args[idx].name] = lv;
//         }
//       });

//       // console.log(`${methReg.traceLevel}: ${mn}: Invoked - ` + sLogRec.toString());
//       // eslint-disable-next-line no-useless-catch
//       try {
//         return methReg.origFunction.call(this, ...args);
//         // console.log(`${methReg.traceLevel}: ${mn}: Returned`);
//       } catch (e) {
//         // console.log(`${methReg.traceLevel}: ${mn}: Threw`, e);
//         throw e;
//       }
//     };
//     Object.defineProperty(nmethod, "name", {
//       value: methReg.name,
//     });

//     descriptor.value = nmethod;
//     methReg.registeredFunction = nmethod;

//     methReg.needInitialized = false;
//     methodRegistry.push(methReg);
//   }

//   return methReg;
// }

// export function registerAndWrapFunction<This, Args extends unknown[], Return>(target: object, propertyKey: string, descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>) {
//   if (!descriptor.value) {
//     throw Error("Use of operon decorator when original method is undefined");
//   }

//   const registration = getOrCreateOperonMethodRegistration(target, propertyKey, descriptor);

//   return { descriptor, registration };
// }

// export function Required(target: object, propertyKey: string | symbol, parameterIndex: number) {
//   const existingParameters = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

//   const curParam = existingParameters[parameterIndex];
//   curParam.required = true;
// }


export function SkipLogging(target: object, propertyKey: string | symbol, parameterIndex: number) {
  LogMask(LogMasks.SKIP)(target, propertyKey, parameterIndex);
}

export function LogMask(mask: LogMasks) {
  return function(target: object, propertyKey: string | symbol, parameterIndex: number) {
    const params = getParamMetadata(target, propertyKey);
    params[parameterIndex].logMask = mask;
  };
}

export function ArgName(name: string) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const params = getParamMetadata(target, propertyKey);
    params[parameterIndex].name = name;
  };
}

export function ArgInteger() {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const params = getParamMetadata(target, propertyKey);
    const param = params[parameterIndex];
    if (param.type !== Number) throw new Error(`Invalid Integer Typescript Type ${param.type}`);
    params[parameterIndex].dataType.fieldType = "integer";
  };
}

export function ArgDecimal(precision: number, scale: number) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const params = getParamMetadata(target, propertyKey);
    const param = params[parameterIndex];
    if (param.type !== Number) throw new Error(`Invalid Decimal Typescript Type ${param.type}`);
    param.dataType.fieldType = "decimal";
    param.dataType.precision = precision;
    param.dataType.scale = scale;
  };
}

export function ArgVarChar(length: number) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const params = getParamMetadata(target, propertyKey);
    const param = params[parameterIndex];
    // TODO: Should we automatically use toString on non-string types?
    if (param.type !== String) throw new Error(`Invalid VarChar Typescript Type ${param.type}`);
    param.dataType.fieldType = "varchar";
    param.dataType.length = length;
  };
}

function createDecoratorWrapper<This, Args extends unknown[], Return>(
  target: object,
  propertyKey: string | symbol,
  descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
  mdUpdate: (md: OperonMethodMetadata<This, Args, Return>) => void
) {
  const md = getMethodMetadata<This, Args, Return>(target, propertyKey, descriptor.value! as any)
  mdUpdate(md);

  return async function (this: This, ...args: Args) {
    const mdParams = getParamMetadata(target, propertyKey);

    // TODO valiadate md.requiredRole
    // TODO validate args
    // TODO trace

    try {
      return md.origFunction.call(this, ...args);
    } catch (error) {
      // TODO: log error
      throw error;
    }
  }
}

export function RequiredRole(anyOf: string[]) {
  return function <This, Ctx extends OperonContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    descriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>)
  {
    createDecoratorWrapper(target, propertyKey, descriptor, (md) => {
      md.requiredRole = anyOf;
    })
  }
}

export function TraceLevel(level: TraceLevels = TraceLevels.INFO) {
  return function <This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
  {
    createDecoratorWrapper(target, propertyKey, descriptor, (md) => {
      md.traceLevel = level;
    })
  }
}

export function Traced<This, Args extends unknown[], Return>(target: object, propertyKey: string, descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>) {
  return TraceLevel(TraceLevels.INFO)(target, propertyKey, descriptor);
}

const operonConfigMetadataKey = Symbol("operon:config");

export type OperonConfigKind = "workflow" | "transaction" | "communicator";

export interface OperonConfig {
  kind: OperonConfigKind;
  config: WorkflowConfig | TransactionConfig | CommunicatorConfig;
}

export function getOperonConfig(target: { name: string }, propertyKey: string) {
  return Reflect.getOwnMetadata(operonConfigMetadataKey, target, propertyKey) as OperonConfig | undefined;
}

function defineConfigMetadata(target: { name: string }, propertyKey: string, config: OperonConfig) {
  const existingConfig = getOperonConfig(target, propertyKey);
  if (existingConfig !== undefined) throw Error(`${target.name}.${propertyKey} already defines ${existingConfig.kind} config`);
  Reflect.defineMetadata(operonConfigMetadataKey, config, target, propertyKey);
}

export function OperonWorkflow(config: WorkflowConfig={}) {
  return function <This, Args extends unknown[], Return>(
    target: { name: string },
    propertyKey: string,
    _descriptor: TypedPropertyDescriptor<(this: This, ctx: WorkflowContext, ...args: Args) => Promise<Return>>)
  {
    defineConfigMetadata(target, propertyKey, { kind: "workflow", config});
  }
}

export function OperonTransaction(config: TransactionConfig={}) {
  return function <This, Args extends unknown[], Return>(
    target: { name: string },
    propertyKey: string,
    _descriptor: TypedPropertyDescriptor<(this: This, ctx: TransactionContext, ...args: Args) => Promise<Return>>)
  {
    defineConfigMetadata(target, propertyKey, { kind: "transaction", config});
  }
}

export function OperonCommunicator(config: CommunicatorConfig={}) {
  return function <This, Args extends unknown[], Return>(
    target: { name: string },
    propertyKey: string,
    _descriptor: TypedPropertyDescriptor<(this: This, ctx: CommunicatorContext, ...args: Args) => Promise<Return>>)
  {
    defineConfigMetadata(target, propertyKey, { kind: "communicator", config});
  }
}
