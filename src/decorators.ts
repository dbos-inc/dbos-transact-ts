/* eslint-disable @typescript-eslint/no-non-null-assertion */

import "reflect-metadata";

import * as crypto from "crypto";
import { TransactionConfig, TransactionContext } from "./transaction";
import { WorkflowConfig, WorkflowContext } from "./workflow";
import { OperonContext, OperonContextImpl } from "./context";
import { CommunicatorConfig, CommunicatorContext } from "./communicator";
import { OperonDataValidationError, OperonNotAuthorizedError } from "./error";

/**
 * Any column type column can be.
 */
export type OperonFieldType = "integer" | "double" | "decimal" | "timestamp" | "text" | "varchar" | "boolean" | "uuid" | "json";

export class OperonDataType {
  dataType: OperonFieldType = "text";
  length: number = -1;
  precision: number = -1;
  scale: number = -1;

  /** Varchar has length */
  static varchar(length: number) {
    const dt = new OperonDataType();
    dt.dataType = "varchar";
    dt.length = length;
    return dt;
  }

  /** Some decimal has precision / scale (as opposed to floating point decimal) */
  static decimal(precision: number, scale: number) {
    const dt = new OperonDataType();
    dt.dataType = "decimal";
    dt.precision = precision;
    dt.scale = scale;

    return dt;
  }

  /** Take type from reflect metadata */
  // eslint-disable-next-line @typescript-eslint/ban-types
  static fromArg(arg: Function) {
    const dt = new OperonDataType();

    if (arg === String) {
      dt.dataType = "text";
    } else if (arg === Date) {
      dt.dataType = "timestamp";
    } else if (arg === Number) {
      dt.dataType = "double";
    } else if (arg === Boolean) {
      dt.dataType = "boolean";
    } else {
      dt.dataType = "json";
    }

    return dt;
  }

  formatAsString(): string {
    let rv: string = this.dataType;
    if (this.dataType === "varchar" && this.length > 0) {
      rv += `(${this.length})`;
    }
    if (this.dataType === "decimal" && this.precision > 0) {
      if (this.scale > 0) {
        rv += `(${this.precision},${this.scale})`;
      } else {
        rv += `(${this.precision})`;
      }
    }
    return rv;
  }
}

const operonParamMetadataKey = Symbol("operon:parameter");
const operonMethodMetadataKey = Symbol("operon:method");
const operonClassMetadataKey = Symbol("operon:class");

/* Arguments parsing heuristic:
 * - Convert the function to a string
 * - Minify the function
 * - Remove everything before the first open parenthesis and after the first closed parenthesis
 * This will obviously not work on code that has been obfuscated or optimized as the names get
 *   changed to be really small and useless.
 **/
// eslint-disable-next-line @typescript-eslint/ban-types
function getArgNames(func: Function): string[] {
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

export enum TraceEventTypes {
  METHOD_ENTER = "METHOD_ENTER",
  METHOD_EXIT = "METHOD_EXIT",
  METHOD_ERROR = "METHOD_ERROR",
}

class BaseTraceEvent {
  eventType: TraceEventTypes = TraceEventTypes.METHOD_ENTER;
  eventComponent: string = "";
  eventLevel: TraceLevels = TraceLevels.DEBUG;
  eventTime: Date = new Date();
  authorizedUser: string = "";
  authorizedRole: string = "";
  positionalArgs: unknown[] = [];
  namedArgs: { [x: string]: unknown } = {};

  toString(): string {
    return `
    eventType: ${this.eventType}
    eventComponent: ${this.eventComponent}
    eventLevel: ${this.eventLevel}
    eventTime: ${this.eventTime.toString()}
    authorizedUser: ${this.authorizedUser}
    authorizedRole: ${this.authorizedRole}
    positionalArgs: ${JSON.stringify(this.positionalArgs)}
    namedArgs: ${JSON.stringify(this.namedArgs)}
    `;
  }
}

export class OperonParameter {
  name: string = "";
  required: boolean = false;
  validate: boolean = true;
  logMask: LogMasks = LogMasks.NONE;

  // eslint-disable-next-line @typescript-eslint/ban-types
  argType: Function = String;
  dataType: OperonDataType;
  index: number = -1;

  // eslint-disable-next-line @typescript-eslint/ban-types
  constructor(idx: number, at: Function) {
    this.index = idx;
    this.argType = at;
    this.dataType = OperonDataType.fromArg(at);
  }
}

//////////////////////////
/* REGISTRATION OBJECTS */
//////////////////////////

export interface OperonRegistrationDefaults
{
  name: string;
  requiredRole: string[] | undefined;
}

export interface OperonMethodRegistrationBase {
  name: string;
  traceLevel: TraceLevels;

  args: OperonParameter[];

  defaults?: OperonRegistrationDefaults;

  getRequiredRoles(): string [];

  workflowConfig?: WorkflowConfig;
  txnConfig?: TransactionConfig;
  commConfig?: CommunicatorConfig;

  // eslint-disable-next-line @typescript-eslint/ban-types
  registeredFunction: Function | undefined;

  invoke(pthis: unknown, args: unknown[]): unknown;
}

export class OperonMethodRegistration <This, Args extends unknown[], Return>
implements OperonMethodRegistrationBase
{
  defaults?: OperonRegistrationDefaults | undefined;

  name: string = "";
  traceLevel: TraceLevels = TraceLevels.INFO;

  requiredRole: string[] | undefined = undefined;

  args: OperonParameter[] = [];

  constructor(origFunc: (this: This, ...args: Args) => Promise<Return>)
  {
    this.origFunction = origFunc;
  }
  needInitialized: boolean = true;
  origFunction: (this: This, ...args: Args) => Promise<Return>;
  registeredFunction: ((this: This, ...args: Args) => Promise<Return>) | undefined;
  workflowConfig?: WorkflowConfig;
  txnConfig?: TransactionConfig;
  commConfig?: CommunicatorConfig;

  invoke(pthis:This, args: Args) : Promise<Return> {
    return this.registeredFunction!.call(pthis, ...args);
  }

  getRequiredRoles() {
    if (this.requiredRole) {
      return this.requiredRole;
    }
    return this.defaults?.requiredRole || [];
  }
}

export class OperonClassRegistration <CT extends { new (...args: unknown[]) : object }> implements OperonRegistrationDefaults
{
  name: string = "";
  requiredRole: string[] | undefined;
  needsInitialized: boolean = true;
  ormEntities: Function[] = [];

  ctor: CT;
  constructor(ctor: CT) {
    this.ctor = ctor;
  }
}

////////////////////////////////////////////////////////////////////////////////
// DECORATOR REGISTRATION
// These manage registration objects, creating them at decorator evaluation time
// and making wrapped methods available for function registration at Operon
// initialization time.
////////////////////////////////////////////////////////////////////////////////

export function getRegisteredOperations(target: object): ReadonlyArray<OperonMethodRegistrationBase> {
  const registeredOperations: OperonMethodRegistrationBase[] = [];

  for (const name of Object.getOwnPropertyNames(target)) {
    const operation = Reflect.getOwnMetadata(operonMethodMetadataKey, target, name) as OperonMethodRegistrationBase | undefined;
    if (operation) { registeredOperations.push(operation); }
  }

  return registeredOperations;
}

export function getOrCreateOperonMethodArgsRegistration(target: object, propertyKey: string | symbol): OperonParameter[] {
  let mParameters: OperonParameter[] = (Reflect.getOwnMetadata(operonParamMetadataKey, target, propertyKey) as OperonParameter[]) || [];

  if (!mParameters.length) {
    // eslint-disable-next-line @typescript-eslint/ban-types
    const designParamTypes = Reflect.getMetadata("design:paramtypes", target, propertyKey) as Function[];
    mParameters = designParamTypes.map((value, index) => new OperonParameter(index, value));

    Reflect.defineMetadata(operonParamMetadataKey, mParameters, target, propertyKey);
  }

  return mParameters;
}

function generateSaltedHash(data: string, salt: string): string {
  const hash = crypto.createHash("sha256"); // You can use other algorithms like 'md5', 'sha512', etc.
  hash.update(data + salt);
  return hash.digest("hex");
}

function getOrCreateOperonMethodRegistration<This, Args extends unknown[], Return>(
  target: object,
  propertyKey: string | symbol,
  descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>
) {
  const methReg: OperonMethodRegistration<This, Args, Return> =
    (Reflect.getOwnMetadata(operonMethodMetadataKey, target, propertyKey) as OperonMethodRegistration<This, Args, Return>) || new OperonMethodRegistration<This, Args, Return>(descriptor.value!);

  if (methReg.needInitialized) {
    methReg.name = propertyKey.toString();

    methReg.args = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

    const argNames = getArgNames(descriptor.value!);
    methReg.args.forEach((e) => {
      if (!e.name) {
        if (e.index < argNames.length) {
          e.name = argNames[e.index];
        }
        if (e.index === 0) { // The first argument is always the context.
          e.logMask = LogMasks.SKIP;
        }
        // TODO else warn/log something
      }
    });

    Reflect.defineMetadata(operonMethodMetadataKey, methReg, target, propertyKey);

    const wrappedMethod = async function (this: This, ...args: Args) {
      const mn = methReg.name;

      let opCtx : OperonContextImpl | undefined = undefined;

      // Validate the user authentication and populate the role field\
      const rr = methReg.getRequiredRoles();
      if (rr.length > 0) {
        opCtx = args[0] as OperonContextImpl;
        const curRoles = opCtx.authenticatedRoles;
        let authorized = false;
        let authRole = ''
        const set = new Set(curRoles);
        for (const str of rr) {
          if (set.has(str)) {
            authorized = true;
            authRole = str;
          }
        }
        if (!authorized) {
          const err = new OperonNotAuthorizedError(`User does not have a role with permission to call ${methReg.name}`, 403);
          throw err;
        }
        opCtx.assumedRole = authRole;
      }

      // Input validation
      methReg.args.forEach((argDescriptor, idx) => {
        if (idx === 0)
        {
          // Context, may find a more robust way.
          opCtx = args[idx] as OperonContextImpl;
          return;
        }

        // Do we have an arg at all
        if (idx >= args.length) {
          if (argDescriptor.required) {
            throw new OperonDataValidationError(`Insufficient number of arguments calling ${methReg.name} - ${args.length}/${methReg.args.length}`);
          }
          return;
        }

        let argValue = args[idx];
        if (argValue === undefined && argDescriptor.required) {
          throw new OperonDataValidationError(`Missing required argument ${argDescriptor.name} calling ${methReg.name}`);
        }

        if (argValue instanceof String) { // TODO: Add input validation for types other than String.
          argValue = argValue.toString();
          args[idx] = argValue;
        }

        if (argDescriptor.dataType.dataType === 'text') {
          if ((typeof argValue !== 'string')) {
            throw new OperonDataValidationError(`Argument ${argDescriptor.name} is marked as type 'text' and should be a string calling ${methReg.name}`);
          }
        }
      });

      // Here let's log the structured record
      const sLogRec = new BaseTraceEvent();
      sLogRec.authorizedUser = opCtx?.authenticatedUser || '';
      sLogRec.authorizedRole = opCtx?.assumedRole || '';
      sLogRec.eventType = TraceEventTypes.METHOD_ENTER;
      sLogRec.eventComponent = mn;
      sLogRec.eventLevel = methReg.traceLevel;

      // Argument masking
      args.forEach((argValue, idx) => {
        let isCtx = false;
        // TODO: we assume the first argument is always a context, need a more robust way to test it.
        if (idx === 0)
        {
          // Context -- I suppose we could just instanceof
          isCtx = true;
        }

        let loggedArgValue = argValue;
        if (isCtx || methReg.args[idx].logMask === LogMasks.SKIP) {
          return;
        } else {
          if (methReg.args[idx].logMask !== LogMasks.NONE) {
            // For now this means hash
            if (methReg.args[idx].dataType.dataType === "json") {
              loggedArgValue = generateSaltedHash(JSON.stringify(argValue), "JSONSALT");
            } else {
              // Yes, we are doing the same as above for now.
              //  It can be better if we have verified the type of the data
              loggedArgValue = generateSaltedHash(JSON.stringify(argValue), "OPERONSALT");
            }
          }
          sLogRec.positionalArgs.push(loggedArgValue);
          sLogRec.namedArgs[methReg.args[idx].name] = loggedArgValue;
        }
      });

      return methReg.origFunction.call(this, ...args);
    };
    Object.defineProperty(wrappedMethod, "name", {
      value: methReg.name,
    });

    descriptor.value = wrappedMethod;
    methReg.registeredFunction = wrappedMethod;

    methReg.needInitialized = false;
  }

  return methReg;
}

export function registerAndWrapFunction<This, Args extends unknown[], Return>(target: object, propertyKey: string, descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>) {
  if (!descriptor.value) {
    throw Error("Use of operon decorator when original method is undefined");
  }

  const registration = getOrCreateOperonMethodRegistration(target, propertyKey, descriptor);

  return { descriptor, registration };
}

export function getOrCreateOperonClassRegistration<CT extends { new (...args: unknown[]) : object }>(
  ctor: CT
) {
  const clsReg: OperonClassRegistration<CT> =
    (Reflect.getOwnMetadata(operonClassMetadataKey, ctor, "opclassreg") as OperonClassRegistration<CT>) || new OperonClassRegistration<CT>(ctor);

  if (clsReg.needsInitialized) {
    clsReg.name = ctor.name;

    Reflect.defineMetadata(operonClassMetadataKey, clsReg, ctor, "opclassreg");

    const ops = getRegisteredOperations(ctor);
    ops.forEach((op) => {
      op.defaults = clsReg;
    });
  }
  return clsReg;
}

//////////////////////////
/* PARAMETER DECORATORS */
//////////////////////////

export function Required(target: object, propertyKey: string | symbol, parameterIndex: number) {
  const existingParameters = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

  const curParam = existingParameters[parameterIndex];
  curParam.required = true;
}

export function SkipLogging(target: object, propertyKey: string | symbol, parameterIndex: number) {
  const existingParameters = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

  const curParam = existingParameters[parameterIndex];
  curParam.logMask = LogMasks.SKIP;
}

export function LogMask(mask: LogMasks) {
  return function(target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex];
    curParam.logMask = mask;
  };
}

export function ArgName(name: string) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex];
    curParam.name = name;
  };
}

export function ArgDate() { // TODO a little more info about it
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex];
    curParam.dataType.dataType = 'timestamp';
  };
}

///////////////////////
/* CLASS DECORATORS */
///////////////////////

export function DefaultRequiredRole(anyOf: string[]) {
  function clsdec<T extends { new (...args: unknown[]) : object }>(ctor: T)
  {
     const clsreg = getOrCreateOperonClassRegistration(ctor);
     clsreg.requiredRole = anyOf;
  }
  return clsdec;
}

///////////////////////
/* METHOD DECORATORS */
///////////////////////

export function RequiredRole(anyOf: string[]) {
  function apidec<This, Ctx extends OperonContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>)
  {
    const {descriptor, registration} = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.requiredRole = anyOf;

    return descriptor;
  }
  return apidec;
}

// Outer shell is the factory that produces decorator - which gets parameters for building the decorator code
export function TraceLevel(level: TraceLevels) {
  // This is the decorator that will get applied to the decorator item
  function logdec<This, Ctx extends OperonContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>)
  {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.traceLevel = level;
    return descriptor;
  }
  return logdec;
}

export function Traced<This, Ctx extends OperonContext, Args extends unknown[], Return>(target: object, propertyKey: string, descriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>) {
  return TraceLevel(TraceLevels.INFO)(target, propertyKey, descriptor);
}

export function OperonWorkflow(config: WorkflowConfig={}) {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: WorkflowContext, ...args: Args) => Promise<Return>>)
  {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.workflowConfig = config;
    return descriptor;
  }
  return decorator;
}

export function OperonTransaction(config: TransactionConfig={}) {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: TransactionContext, ...args: Args) => Promise<Return>>)
  {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.txnConfig = config;
    return descriptor;
  }
  return decorator;
}

export function OperonCommunicator(config: CommunicatorConfig={}) {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: CommunicatorContext, ...args: Args) => Promise<Return>>)
  {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.commConfig = config;
    return descriptor;
  }
  return decorator;

}

export function OrmEntities(entities: Function[]) {

  entities.forEach((entity) => {
    console.log("decorator " + entity.name + " " + typeof entity);
  });

  /* if (check(entities[0], "EntitySchema")) {
    console.log("Valid entity");
  } else {
    console.log("Not Valid entity");
  } */

  function clsdec<T extends { new (...args: unknown[]) : object }>(ctor: T)
  {
     const clsreg = getOrCreateOperonClassRegistration(ctor);
     clsreg.ormEntities = entities;
  }
  return clsdec; 

 
}

function check(obj: unknown, name: string) {

  /* if (typeof obj === "object") {
    console.log("It is an object");
  } else {
    console.log("It is not an object");
  }
  console.log(Symbol.for(name));
  const v = obj as { "@instanceof": Symbol };
  console.log(v);
  console.log(v["@instanceof"]); */

  return (
      typeof obj === "object" &&
      obj !== null &&
      (obj as { "@instanceof": Symbol })["@instanceof"] ===
          Symbol.for(name)
  )
}
