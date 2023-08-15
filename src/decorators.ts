/* eslint-disable @typescript-eslint/no-non-null-assertion */
/* May make sense: eslint-disable @typescript-eslint/ban-types */

// TODO List
// General
//   Class level decorators - defaults
//   Field / property decorators - persistent data
//   Integrate with API registration
//   Integrate with Operon workflows
//   Integrate parameter validation
//   Integrate with Authentication
//   Integrate with error handling
//   Find a way to unit test - perhaps a mock log collector?
//     Or is it easier once there is a real log collector?
//
// Logging
//   Integrate with Logger setup
//   Integrate with Logger buffer

import "reflect-metadata";

import * as crypto from 'crypto';
import { TransactionConfig, TransactionContext } from "./transaction";
import { WorkflowConfig, WorkflowContext } from "./workflow";

/**
 * Any column type column can be.
 */
export type OperonFieldType =
    | 'integer'
    | 'double'
    | 'decimal'
    | 'timestamp'
    | 'text'
    | 'varchar'
    | 'boolean'
    | 'uuid'
    | 'json'
;

export class OperonDataType {
  dataType : OperonFieldType = 'text';
  length : number = -1;
  precision : number = -1;
  scale : number = -1;

  /** Varchar has length */
  static varchar(length: number) {
    const dt = new OperonDataType();
    dt.dataType = 'varchar';
    dt.length = length;
    return dt;
  }

  /** Some decimal has precision / scale (as opposed to floating point decimal) */
  static decimal(precision: number, scale: number) {
    const dt = new OperonDataType();
    dt.dataType = 'decimal';
    dt.precision = precision;
    dt.scale = scale;

    return dt;
  }

  /** Take type from reflect metadata */
  // eslint-disable-next-line @typescript-eslint/ban-types
  static fromArg(arg: Function) {
    const dt = new OperonDataType();

    if (arg === String) {
      dt.dataType = 'text';
    }
    else if (arg === Date) {
      dt.dataType = 'timestamp';
    }
    else if (arg === Number) {
      dt.dataType = 'double';
    }
    else if (arg === Boolean) {
      dt.dataType = 'boolean';
    }
    else {
      dt.dataType = 'json';
    }

    return dt;
  }

  formatAsString(): string {
    let rv: string = this.dataType;
    if (this.dataType === 'varchar' && this.length > 0) {
      rv += `(${this.length})`;
    }
    if (this.dataType === 'decimal' && this.precision > 0) {
      if (this.scale > 0) {
        rv += `(${this.precision},${this.scale})`;
      }
      else {
        rv += `(${this.precision})`;
      }
    }
    return rv;
  }
}

const operonParamMetadataKey = Symbol("operon:parameter");
const operonMethodMetadataKey = Symbol("operon:method");

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

export enum LogLevel {
    DEBUG = "DEBUG",
    INFO = "INFO",
    WARN = "WARN",
    ERROR = "ERROR",
    CRITICAL = "CRITICAL"
}

export enum LogMask {
    NONE = "NONE",
    HASH = "HASH",
}

export enum LogEventType {
    METHOD_ENTER = 'METHOD_ENTER',
    METHOD_EXIT = 'METHOD_EXIT',
    METHOD_ERROR = 'METHOD_ERROR',
}

export enum APIType {
    GET = 'GET',
    POST = 'POST',
}

export enum ArgType {
  DEFAULT = 'DEFAULT',
  BODY = 'BODY',
  QUERY = 'QUERY',
  URL = 'URL',
}

class BaseLogEvent {
  eventType: LogEventType = LogEventType.METHOD_ENTER;
  eventComponent: string = '';
  eventLevel: LogLevel = LogLevel.DEBUG;
  eventTime: Date = new Date();
  authorizedUser: string = '';
  authorizedRole: string = '';
  positionalArgs: unknown[] = [];
  namedArgs: {[x: string]: unknown} = {};

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

class OperonParameter {
  name: string = "";
  required: boolean = false;
  validate: boolean = true;
  skipLogging: boolean = false;
  logMask: LogMask = LogMask.NONE;
  // eslint-disable-next-line @typescript-eslint/ban-types
  argType: Function = String;
  dataType: OperonDataType;
  // TODO: If we override the logging behavior (say to mask/hash it), where do we record that?
  index:number = -1;

  // eslint-disable-next-line @typescript-eslint/ban-types
  constructor(idx: number, at: Function) {
    this.index = idx;
    this.argType = at;
    this.dataType = OperonDataType.fromArg(at);
  }
}

export class OperonMethodRegistrationBase {
  name: string = "";
  logLevel : LogLevel = LogLevel.INFO;
  args : OperonParameter[] = [];

  apiType : APIType = APIType.GET;
  apiURL : string = '';
}

export class OperonMethodRegistration <This, Args extends unknown[], Return>
  extends OperonMethodRegistrationBase
{
  constructor(origFunc: (this: This, ...args: Args) => Promise<Return>)
  {
    super();
    this.origFunction = origFunc;
  }
  needInitialized: boolean = true;
  origFunction : ((this: This, ...args: Args) => Promise<Return>);
  replacementFunction : ((this: This, ...args: Args) => Promise<Return>) | undefined;
  workflowConfig?: WorkflowConfig;
  txnConfig?: TransactionConfig;

  // TODO: Permissions, attachment point, error handling, etc.
}

// Quick and dirty method registration list...
const methodRegistry: OperonMethodRegistrationBase[] = [];
export function forEachMethod(f: (m: OperonMethodRegistrationBase) => void) {
  methodRegistry.forEach(f);
}

function getOrCreateOperonMethodArgsRegistration(target: object, propertyKey: string | symbol) : OperonParameter[]
{
  let mParameters: OperonParameter[]
        = Reflect.getOwnMetadata(operonParamMetadataKey, target, propertyKey) as OperonParameter[]
        || [];

  if (!mParameters.length) {
    // eslint-disable-next-line @typescript-eslint/ban-types
    const designParamTypes = Reflect.getMetadata('design:paramtypes', target, propertyKey) as Function [];
    mParameters = designParamTypes.map((value, index) => new OperonParameter(index, value));

    Reflect.defineMetadata(operonParamMetadataKey, mParameters, target, propertyKey);
  }

  return mParameters;
}

function generateSaltedHash(data: string, salt: string): string {
  const hash = crypto.createHash('sha256'); // You can use other algorithms like 'md5', 'sha512', etc.
  hash.update(data + salt);
  return hash.digest('hex');
}

function getOrCreateOperonMethodRegistration<This, Args extends unknown[], Return>(target: object, propertyKey: string | symbol, descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
{
  const methReg : OperonMethodRegistration<This, Args, Return> = 
       Reflect.getOwnMetadata(operonMethodMetadataKey, target, propertyKey) as OperonMethodRegistration<This, Args, Return>
       || new OperonMethodRegistration<This, Args, Return>(descriptor.value!);

  if (methReg.needInitialized) {
    methReg.name = propertyKey.toString();

    methReg.args = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

    const argNames = getArgNames(descriptor.value!);
    methReg.args.forEach( (e) => {
      if (!e.name) {
        if (e.index < argNames.length) {
          e.name = argNames[e.index];
        }
        if (e.argType === TransactionContext || e.argType == WorkflowContext) {
          e.skipLogging = true;
        }
        // TODO else warn/log something
      }
    });

    Reflect.defineMetadata(operonMethodMetadataKey, methReg, target, propertyKey);

    // This is the replacement method
    const nmethod = async function(this: This, ...args: Args) {
      const mn = methReg.name;
        
      // TODO: Validate the user authentication

      // TODO: Here let's validate the arguments, being careful to log any validation errors that occur
      //        And skip/mask arguments

      // Here let's log the structured record
      const sLogRec = new BaseLogEvent();
      sLogRec.authorizedUser = "Get user from middleware arg 0?";
      sLogRec.authorizedRole = "Get role from middleware arg 0?";
      sLogRec.eventType = LogEventType.METHOD_ENTER;
      sLogRec.eventComponent = mn;
      sLogRec.eventLevel = methReg.logLevel;

      args.forEach((v, idx) => {
        let lv = v;
        if (methReg.args[idx].skipLogging) {
          return;
        }
        else {
          if (methReg.args[idx].logMask !== LogMask.NONE) {
            // For now this means hash
            if (methReg.args[idx].dataType.dataType === 'json') {
              lv = generateSaltedHash(JSON.stringify(v), 'JSONSALT');
            }
            else {
              // Yes, we are doing the same as above for now.
              //  It can be better if we have verified the type of the data
              lv = generateSaltedHash(JSON.stringify(v), 'OPERONSALT');
            }
          }
          sLogRec.positionalArgs.push(lv);
          sLogRec.namedArgs[methReg.args[idx].name] = lv;
        }
      });

      console.log(`${methReg.logLevel}: ${mn}: Invoked - `+ sLogRec.toString());
      try {
        // It is unclear if this is the right thing to do about async... in some contexts await may not be desired
        const result = await methReg.origFunction.call(this, ...args);
        console.log(`${methReg.logLevel}: ${mn}: Returned`);
        return result;
      }
      catch (e) {
        console.log(`${methReg.logLevel}: ${mn}: Threw`, e);
        throw e;
      }
    };
    Object.defineProperty(nmethod, 'name', {
      value: methReg.name,
    })

    descriptor.value = nmethod;
    methReg.replacementFunction = nmethod;

    methReg.needInitialized = false;
    methodRegistry.push(methReg);
  }

  return methReg;
}

function registerAndWrapFunction<This, Args extends unknown[], Return>(
  target: object,
  propertyKey: string,
  descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
{
  if (!descriptor.value) {
    throw Error("Use of operon decorator when original method is undefined");
  }

  const registration = getOrCreateOperonMethodRegistration(target, propertyKey, descriptor);
    
  return {descriptor, registration};
}

export function required(target: object, propertyKey: string | symbol, parameterIndex: number) {
  const existingParameters = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

  const curParam = existingParameters[parameterIndex];
  curParam.required = true;
}

export function skipLogging(target: object, propertyKey: string | symbol, parameterIndex: number) {
  const existingParameters = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

  const curParam = existingParameters[parameterIndex];
  curParam.skipLogging = true;
}

export function argName(name: string) {
  return function(target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex];
    curParam.name = name;
  };
}

export function logMask(mask: LogMask) {
  return function(target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateOperonMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex];
    curParam.logMask = mask;
  };
}

/*
type MethodDecorator = <T>(
  target: Object,
  propertyKey: string | symbol,
  descriptor: TypedPropertyDescriptor<T>
) => TypedPropertyDescriptor<T> | void;
*/

// Outer shell is the factory that produces decorator - which gets parameters for building the decorator code
export function logLevel(level: LogLevel) {
  // This is the decorator that will get applied to the decorator item
  function logdec<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
  {
    const {descriptor, registration} = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.logLevel = level;    
    return descriptor;
  }
  return logdec;
}

export function logged<This, Args extends unknown[], Return>(
  target: object,
  propertyKey: string,
  descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
{
  return logLevel(LogLevel.INFO)(target, propertyKey, descriptor);
}

export function getApi(url: string) {
  function apidec<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
  {
    const {descriptor, registration} = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.apiURL = url;
    registration.apiType = APIType.GET;
    return descriptor;
  }
  return apidec;
}

export function postApi(url: string) {
  function apidec<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
  {
    const {descriptor, registration} = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.apiURL = url;
    registration.apiType = APIType.POST;
    return descriptor;
  }
  return apidec;
}

export function operonWorkflow(config: WorkflowConfig={}) {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
  {
    const {descriptor, registration} = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.workflowConfig = config;
    return descriptor;
  }
  return decorator;
}

export function operonTransaction(config: TransactionConfig={}) {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
  {
    const {descriptor, registration} = registerAndWrapFunction(target, propertyKey, inDescriptor);
    registration.txnConfig = config;
    return descriptor;
  }
  return decorator;
}
