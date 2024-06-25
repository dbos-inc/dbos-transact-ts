import "reflect-metadata";

import * as crypto from "crypto";
import { TransactionConfig, TransactionContext } from "./transaction";
import { WorkflowConfig, WorkflowContext } from "./workflow";
import { DBOSContext, DBOSContextImpl, InitContext } from "./context";
import { CommunicatorConfig, CommunicatorContext } from "./communicator";
import { DBOSError, DBOSNotAuthorizedError } from "./error";
import { validateMethodArgs } from "./data_validation";
import { StoredProcedureConfig, StoredProcedureContext } from "./procedure";
import { DBOSEventReceiver } from "./eventreceiver";

/**
 * Any column type column can be.
 */
export type DBOSFieldType = "integer" | "double" | "decimal" | "timestamp" | "text" | "varchar" | "boolean" | "uuid" | "json";

export class DBOSDataType {
  dataType: DBOSFieldType = "text";
  length: number = -1;
  precision: number = -1;
  scale: number = -1;

  /** Varchar has length */
  static varchar(length: number) {
    const dt = new DBOSDataType();
    dt.dataType = "varchar";
    dt.length = length;
    return dt;
  }

  /** Some decimal has precision / scale (as opposed to floating point decimal) */
  static decimal(precision: number, scale: number) {
    const dt = new DBOSDataType();
    dt.dataType = "decimal";
    dt.precision = precision;
    dt.scale = scale;

    return dt;
  }

  /** Take type from reflect metadata */
  // eslint-disable-next-line @typescript-eslint/ban-types
  static fromArg(arg: Function) {
    const dt = new DBOSDataType();

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

const paramMetadataKey = Symbol.for("dbos:parameter");

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

export enum LogMasks {
  NONE = "NONE",
  HASH = "HASH",
  SKIP = "SKIP",
}

export enum ArgRequiredOptions {
  REQUIRED = "REQUIRED",
  OPTIONAL = "OPTIONAL",
  DEFAULT = "DEFAULT",
}

export class MethodParameter {
  name: string = "";
  required: ArgRequiredOptions = ArgRequiredOptions.DEFAULT;
  validate: boolean = true;
  logMask: LogMasks = LogMasks.NONE;

  // eslint-disable-next-line @typescript-eslint/ban-types
  argType: Function = String;
  dataType: DBOSDataType;
  index: number = -1;

  // eslint-disable-next-line @typescript-eslint/ban-types
  constructor(idx: number, at: Function) {
    this.index = idx;
    this.argType = at;
    this.dataType = DBOSDataType.fromArg(at);
  }
}

//////////////////////////////////////////
/* REGISTRATION OBJECTS and read access */
//////////////////////////////////////////

export interface RegistrationDefaults
{
  name: string;
  requiredRole: string[] | undefined;
  defaultArgRequired: ArgRequiredOptions;
  eventReceiverInfo: Map<DBOSEventReceiver, unknown>;
}

export interface MethodRegistrationBase {
  name: string;
  className: string;

  args: MethodParameter[];

  defaults?: RegistrationDefaults;

  getRequiredRoles(): string [];

  workflowConfig?: WorkflowConfig;
  txnConfig?: TransactionConfig;
  commConfig?: CommunicatorConfig;
  procConfig?: TransactionConfig;
  isInstance: boolean;

eventReceiverInfo: Map<DBOSEventReceiver, unknown>;

  // eslint-disable-next-line @typescript-eslint/ban-types
  registeredFunction: Function | undefined;

  invoke(pthis: unknown, args: unknown[]): unknown;
}

export class MethodRegistration <This, Args extends unknown[], Return>
implements MethodRegistrationBase
{
  defaults?: RegistrationDefaults | undefined;

  name: string = "";
  className: string = "";

  requiredRole: string[] | undefined = undefined;

  args: MethodParameter[] = [];

  constructor(origFunc: (this: This, ...args: Args) => Promise<Return>, isInstance: boolean)
  {
    this.origFunction = origFunc;
    this.isInstance = isInstance;
  }
  needInitialized: boolean = true;
  isInstance: boolean;
  origFunction: (this: This, ...args: Args) => Promise<Return>;
  registeredFunction: ((this: This, ...args: Args) => Promise<Return>) | undefined;
  workflowConfig?: WorkflowConfig;
  txnConfig?: TransactionConfig;
  commConfig?: CommunicatorConfig;
  procConfig?: TransactionConfig;
  eventReceiverInfo: Map<DBOSEventReceiver, unknown> = new Map();

  init: boolean = false;

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

export abstract class ConfiguredInstance {
  readonly name: string;
  constructor(name: string) {
    this.name = name;
  }
  abstract initialize(ctx: InitContext): Promise<void>;
}

export class ClassRegistration <CT extends { new (...args: unknown[]) : object }> implements RegistrationDefaults
{
  name: string = "";
  requiredRole: string[] | undefined;
  defaultArgRequired: ArgRequiredOptions = ArgRequiredOptions.REQUIRED;
  needsInitialized: boolean = true;

  // eslint-disable-next-line @typescript-eslint/ban-types
  ormEntities: Function[] = [];

  registeredOperations: Map<string, MethodRegistrationBase> = new Map();

  configuredInstances: Map<string, ConfiguredInstance> = new Map();

  eventReceiverInfo: Map<DBOSEventReceiver, unknown> = new Map();

  ctor: CT;
  constructor(ctor: CT) {
    this.ctor = ctor;
  }
}

// This is a bit ugly, if we got the class / instance it would help avoid this auxiliary structure
const methodToRegistration: Map<unknown, MethodRegistration<unknown, unknown[], unknown>> = new Map();
export function getRegisteredMethodClassName(func: unknown): string {
  let rv:string = "";
  if (methodToRegistration.has(func)) {
    rv = methodToRegistration.get(func)!.className;
  }
  return rv;
}
export function getRegisteredMethodName(func: unknown): string {
  let rv:string = "";
  if (methodToRegistration.has(func)) {
    rv = methodToRegistration.get(func)!.name;
  }
  return rv;
}

export function getRegisteredOperations(target: object): ReadonlyArray<MethodRegistrationBase> {
  const registeredOperations: MethodRegistrationBase[] = [];

  if (typeof target === 'function') { // Constructor case
    const classReg = classesByName.get(target.name);
    classReg?.registeredOperations?.forEach((m) =>registeredOperations.push(m));
  }
  else {
    let current: object | undefined = target;
    while (current) {
      const cname = current.constructor.name;
      if (classesByName.has(cname)) {
        registeredOperations.push(...getRegisteredOperations(current.constructor));
      }
      current = Object.getPrototypeOf(current) as object | undefined;
    }
  }

  return registeredOperations;
}

export function getConfiguredInstance(clsname: string, cfgname: string): ConfiguredInstance | null {
  const classReg = classesByName.get(clsname);
  if (!classReg) return null;
  return classReg.configuredInstances.get(cfgname) ?? null;
}

////////////////////////////////////////////////////////////////////////////////
// DECORATOR REGISTRATION
// These manage registration objects, creating them at decorator evaluation time
// and making wrapped methods available for function registration at runtime
// initialization time.
////////////////////////////////////////////////////////////////////////////////

export function getOrCreateMethodArgsRegistration(target: object, propertyKey: string | symbol): MethodParameter[] {
  let regtarget = target;
  if (typeof regtarget !== 'function') {
    regtarget = regtarget.constructor;
  }
  let mParameters: MethodParameter[] = (Reflect.getOwnMetadata(paramMetadataKey, regtarget, propertyKey) as MethodParameter[]) || [];

  if (!mParameters.length) {
    // eslint-disable-next-line @typescript-eslint/ban-types
    const designParamTypes = Reflect.getMetadata("design:paramtypes", target, propertyKey) as Function[];
    mParameters = designParamTypes.map((value, index) => new MethodParameter(index, value));

    Reflect.defineMetadata(paramMetadataKey, mParameters, regtarget, propertyKey);
  }

  return mParameters;
}

function generateSaltedHash(data: string, salt: string): string {
  const hash = crypto.createHash("sha256"); // You can use other algorithms like 'md5', 'sha512', etc.
  hash.update(data + salt);
  return hash.digest("hex");
}

function getOrCreateMethodRegistration<This, Args extends unknown[], Return>(
  target: object,
  propertyKey: string | symbol,
  descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>
) {
  let regtarget: AnyConstructor;
  let isInstance = false;
  if (typeof target === 'function') {
    // Static method case
    regtarget = target as AnyConstructor;
  }
  else
  {
    // Instance method case
    regtarget = target.constructor as AnyConstructor;
    isInstance = true;
  }

  const classReg = getOrCreateClassRegistration(regtarget);

  const fname = propertyKey.toString();
  if (!classReg.registeredOperations.has(fname)) {
    classReg.registeredOperations.set(fname, new MethodRegistration<This, Args, Return>(descriptor.value!, isInstance));
  }
  const methReg: MethodRegistration<This, Args, Return> = classReg.registeredOperations.get(fname)! as MethodRegistration<This, Args, Return>;

  if (methReg.needInitialized) {
    methReg.needInitialized = false;
    methReg.name = fname;
    methReg.className = classReg.name;
    methReg.defaults = classReg;

    methReg.args = getOrCreateMethodArgsRegistration(target, propertyKey);

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

    const wrappedMethod = async function (this: This, ...rawArgs: Args) {

      let opCtx : DBOSContextImpl | undefined = undefined;

      // Validate the user authentication and populate the role field
      const requiredRoles = methReg.getRequiredRoles();
      if (requiredRoles.length > 0) {
        opCtx = rawArgs[0] as DBOSContextImpl;
        opCtx.span.setAttribute("requiredRoles", requiredRoles);
        const curRoles = opCtx.authenticatedRoles;
        let authorized = false;
        const set = new Set(curRoles);
        for (const role of requiredRoles) {
          if (set.has(role)) {
            authorized = true;
            opCtx.assumedRole = role;
            break;
          }
        }
        if (!authorized) {
          const err = new DBOSNotAuthorizedError(`User does not have a role with permission to call ${methReg.name}`, 403);
          opCtx.span.addEvent("DBOSNotAuthorizedError", { message: err.message });
          throw err;
        }
      }

      const validatedArgs = validateMethodArgs(methReg, rawArgs);

      // Argument logging
      validatedArgs.forEach((argValue, idx) => {
        let isCtx = false;
        // TODO: we assume the first argument is always a context, need a more robust way to test it.
        if (idx === 0)
        {
          // Context -- I suppose we could just instanceof
          opCtx = validatedArgs[0] as DBOSContextImpl;
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
              // It can be better if we have verified the type of the data
              loggedArgValue = generateSaltedHash(JSON.stringify(argValue), "DBOSSALT");
            }
          }
          opCtx?.span.setAttribute(methReg.args[idx].name, loggedArgValue as string);
        }
      });

      return methReg.origFunction.call(this, ...validatedArgs);
    };
    Object.defineProperty(wrappedMethod, "name", {
      value: methReg.name,
    });

    descriptor.value = wrappedMethod;
    methReg.registeredFunction = wrappedMethod;

    methodToRegistration.set(methReg.registeredFunction, methReg as MethodRegistration<unknown, unknown[], unknown>);
    methodToRegistration.set(methReg.origFunction, methReg as MethodRegistration<unknown, unknown[], unknown>);
  }

  return methReg;
}

export function registerAndWrapFunction<This, Args extends unknown[], Return>(target: object, propertyKey: string, descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>) {
  if (!descriptor.value) {
    throw Error("Use of decorator when original method is undefined");
  }

  const registration = getOrCreateMethodRegistration(target, propertyKey, descriptor);

  return { descriptor, registration };
}

type AnyConstructor = new (...args: unknown[]) => object;
const classesByName: Map<string, ClassRegistration<AnyConstructor> > = new Map();

export function getAllRegisteredClasses() {
  const ctors: AnyConstructor[] = [];
  for (const [_cn, creg] of classesByName) {
    ctors.push(creg.ctor);
  }
  return ctors;
}

export function getOrCreateClassRegistration<CT extends { new (...args: unknown[]) : object }>(
  ctor: CT
) {
  const name = ctor.name;
  if (!classesByName.has(name)) {
    classesByName.set(name, new ClassRegistration<CT>(ctor));
  }
  const clsReg: ClassRegistration<AnyConstructor> = classesByName.get(name)!;

  if (clsReg.needsInitialized) {
    clsReg.name = name;
    clsReg.needsInitialized = false;
  }
  return clsReg;
}

export function associateClassWithEventReceiver<CT extends { new (...args: unknown[]) : object }>(
  rcvr: DBOSEventReceiver, ctor: CT
)
{
  const clsReg = getOrCreateClassRegistration(ctor);
  if (!clsReg.eventReceiverInfo.has(rcvr)) {
    clsReg.eventReceiverInfo.set(rcvr, {});
  }
  return clsReg.eventReceiverInfo.get(rcvr)!;
}

export function associateMethodWithEventReceiver<This, Args extends unknown[], Return>(rcvr: DBOSEventReceiver, target: object, propertyKey: string, inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
{
  const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
  if (!registration.eventReceiverInfo.has(rcvr)) {
    registration.eventReceiverInfo.set(rcvr, {});
  }
  return {descriptor, registration, receiverInfo: registration.eventReceiverInfo.get(rcvr)!};
}

//////////////////////////
/* PARAMETER DECORATORS */
//////////////////////////

export function ArgRequired(target: object, propertyKey: string | symbol, parameterIndex: number) {
  const existingParameters = getOrCreateMethodArgsRegistration(target, propertyKey);

  const curParam = existingParameters[parameterIndex];
  curParam.required = ArgRequiredOptions.REQUIRED;
}

export function ArgOptional(target: object, propertyKey: string | symbol, parameterIndex: number) {
  const existingParameters = getOrCreateMethodArgsRegistration(target, propertyKey);

  const curParam = existingParameters[parameterIndex];
  curParam.required = ArgRequiredOptions.OPTIONAL;
}

export function SkipLogging(target: object, propertyKey: string | symbol, parameterIndex: number) {
  const existingParameters = getOrCreateMethodArgsRegistration(target, propertyKey);

  const curParam = existingParameters[parameterIndex];
  curParam.logMask = LogMasks.SKIP;
}

export function LogMask(mask: LogMasks) {
  return function(target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex];
    curParam.logMask = mask;
  };
}

export function ArgName(name: string) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex];
    curParam.name = name;
  };
}

export function ArgDate() { // TODO a little more info about it - is it a date or timestamp precision?
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex];
    curParam.dataType.dataType = 'timestamp';
  };
}

export function ArgVarchar(length: number) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex];
    curParam.dataType = DBOSDataType.varchar(length);
  };
}

///////////////////////
/* CLASS DECORATORS */
///////////////////////

export function DefaultRequiredRole(anyOf: string[]) {
  function clsdec<T extends { new (...args: unknown[]) : object }>(ctor: T)
  {
     const clsreg = getOrCreateClassRegistration(ctor);
     clsreg.requiredRole = anyOf;
  }
  return clsdec;
}

export function DefaultArgRequired<T extends { new (...args: unknown[]) : object }>(ctor: T)
{
   const clsreg = getOrCreateClassRegistration(ctor);
   clsreg.defaultArgRequired = ArgRequiredOptions.REQUIRED;
}

export function DefaultArgOptional<T extends { new (...args: unknown[]) : object }>(ctor: T)
{
   const clsreg = getOrCreateClassRegistration(ctor);
   clsreg.defaultArgRequired = ArgRequiredOptions.OPTIONAL;
}

export function configureInstance<R extends ConfiguredInstance, T extends unknown[]>(cls: new (name:string, ...args: T) => R, name: string, ...args: T) : R
{
  const inst = new cls(name, ...args);
  const creg = getOrCreateClassRegistration(cls as new(...args: unknown[])=>R);
  if (creg.configuredInstances.has(name)) {
    throw new DBOSError(`Registration: Class ${cls.name} configuration ${name} is not unique`);
  }
  creg.configuredInstances.set(name, inst);
  return inst;
}

///////////////////////
/* METHOD DECORATORS */
///////////////////////

export function RequiredRole(anyOf: string[]) {
  function apidec<This, Ctx extends DBOSContext, Args extends unknown[], Return>(
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

export function Workflow(config: WorkflowConfig={}) {
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

export function Transaction(config: TransactionConfig={}) {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: TransactionContext<any>, ...args: Args) => Promise<Return>>)
  {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.txnConfig = config;
    return descriptor;
  }
  return decorator;
}

export function StoredProcedure(config: StoredProcedureConfig={}) {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: StoredProcedureContext, ...args: Args) => Promise<Return>>)
  {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.procConfig = config;
    return descriptor;
  }
  return decorator;
}

export function Communicator(config: CommunicatorConfig={}) {
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

// eslint-disable-next-line @typescript-eslint/ban-types
export function OrmEntities(entities: Function[]) {

  function clsdec<T extends { new (...args: unknown[]) : object }>(ctor: T)
  {
     const clsreg = getOrCreateClassRegistration(ctor);
     clsreg.ormEntities = entities;
  }
  return clsdec;
}

export function DBOSInitializer() {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: InitContext, ...args: Args) => Promise<Return>>)
  {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.init = true;
    return descriptor;
  }
  return decorator;
}

// For future use with Deploy
export function DBOSDeploy() {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: InitContext, ...args: Args) => Promise<Return>>)
  {
    const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.init = true;
    return descriptor;
  }
  return decorator;
}