import 'reflect-metadata';

import { TransactionConfig, TransactionContext } from './transaction';
import { WorkflowConfig, WorkflowContext } from './workflow';
import { StepConfig, StepContext } from './step';
import { DBOSConflictingRegistrationError, DBOSNotRegisteredError } from './error';
import { StoredProcedureConfig, StoredProcedureContext } from './procedure';
import { DBOSEventReceiver } from './eventreceiver';
import { InitContext } from './dbos';
import { DataSourceTransactionHandler } from './datasource';

/**
 * Interface for integrating into the DBOS startup/shutdown lifecycle
 */
export abstract class DBOSLifecycleCallback {
  /** Called back during DBOS launch */
  initialize(): Promise<void> {
    return Promise.resolve();
  }
  /** Called back upon shutdown (usually in tests) to close connections and free resources */
  destroy(): Promise<void> {
    return Promise.resolve();
  }
  /** Called at launch; Implementers should emit a diagnostic list of all registrations */
  logRegisteredEndpoints(): void {}
}

const lifecycleListeners: DBOSLifecycleCallback[] = [];
export function registerLifecycleCallback(lcl: DBOSLifecycleCallback) {
  if (!lifecycleListeners.includes(lcl)) lifecycleListeners.push(lcl);
}
export function getLifecycleListeners() {
  return lifecycleListeners as readonly DBOSLifecycleCallback[];
}

// Middleware installation
export interface DBOSMethodMiddlewareInstaller {
  installMiddleware(methodReg: MethodRegistrationBase): void;
}
let installedMiddleware = false;
const middlewareInstallers: DBOSMethodMiddlewareInstaller[] = [];
export function registerMiddlewareInstaller(i: DBOSMethodMiddlewareInstaller) {
  if (installedMiddleware) throw new TypeError('Attempt to provide method middleware after insertion was performed');
  if (!middlewareInstallers.includes(i)) middlewareInstallers.push(i);
}
export function insertAllMiddleware() {
  if (installedMiddleware) return;
  installedMiddleware = true;

  for (const [_cn, c] of classesByName) {
    for (const [_fn, f] of c.reg.registeredOperations) {
      for (const i of middlewareInstallers) {
        i.installMiddleware(f);
      }
    }
  }
}

/**
 * Any column type column can be.
 */
export type DBOSFieldType =
  | 'integer'
  | 'double'
  | 'decimal'
  | 'timestamp'
  | 'text'
  | 'varchar'
  | 'boolean'
  | 'uuid'
  | 'json';

export class DBOSDataType {
  dataType: DBOSFieldType = 'text';
  length: number = -1;
  precision: number = -1;
  scale: number = -1;

  /** Varchar has length */
  static varchar(length: number) {
    const dt = new DBOSDataType();
    dt.dataType = 'varchar';
    dt.length = length;
    return dt;
  }

  /** Some decimal has precision / scale (as opposed to floating point decimal) */
  static decimal(precision: number, scale: number) {
    const dt = new DBOSDataType();
    dt.dataType = 'decimal';
    dt.precision = precision;
    dt.scale = scale;

    return dt;
  }

  /** Take type from reflect metadata */
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  static fromArg(arg?: Function): DBOSDataType | undefined {
    if (!arg) return undefined;

    const dt = new DBOSDataType();

    if (arg === String) {
      dt.dataType = 'text';
    } else if (arg === Date) {
      dt.dataType = 'timestamp';
    } else if (arg === Number) {
      dt.dataType = 'double';
    } else if (arg === Boolean) {
      dt.dataType = 'boolean';
    } else {
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
      } else {
        rv += `(${this.precision})`;
      }
    }
    return rv;
  }
}

// eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
function getArgNames(func: Function): string[] {
  const fstr = func.toString();
  const args: string[] = [];
  let currentArgName = '';
  let nestDepth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inComment = false;
  let inBlockComment = false;
  let inDefaultValue = false;

  // Extract parameter list from function signature
  const paramStart = fstr.indexOf('(');
  if (paramStart === -1) return [];

  const paramStr = fstr.substring(paramStart + 1);

  for (let i = 0; i < paramStr.length; i++) {
    const char = paramStr[i];
    const nextChar = paramStr[i + 1];

    // Handle comments
    if (inBlockComment) {
      if (char === '*' && nextChar === '/') {
        inBlockComment = false;
        i++; // Skip closing '/'
      }
      continue;
    } else if (inComment) {
      if (char === '\n') inComment = false;
      continue;
    } else if (char === '/' && nextChar === '*') {
      inBlockComment = true;
      i++;
      continue;
    } else if (char === '/' && nextChar === '/') {
      inComment = true;
      continue;
    }
    if (inComment || inBlockComment) continue;

    // Handle quotes (for default values)
    if (char === "'" && !inDoubleQuote && !inBacktick) {
      inSingleQuote = !inSingleQuote;
    } else if (char === '"' && !inSingleQuote && !inBacktick) {
      inDoubleQuote = !inDoubleQuote;
    } else if (char === '`' && !inSingleQuote && !inDoubleQuote) {
      inBacktick = !inBacktick;
    }

    // Skip anything inside quotes
    if (inSingleQuote || inDoubleQuote || inBacktick) {
      continue;
    }

    // Handle default values
    if (char === '=' && nestDepth === 0) {
      inDefaultValue = true;
      continue;
    }

    // These can mean default values.  Or destructuring (which is a problem)...
    if (char === '(' || char === '{' || char === '[') {
      nestDepth++;
    }
    if (char === ')' || char === '}' || char === ']') {
      if (nestDepth === 0) break; // Done
      nestDepth--;
    }

    // Handle rest parameters `...arg`; this is a problem.
    if (char === '.' && nextChar === '.' && paramStr[i + 2] === '.') {
      i += 2; // Skip the other dots
      continue;
    }

    // Handle argument separators (`,`) at depth 0
    if (char === ',' && nestDepth === 0) {
      if (currentArgName.trim()) {
        args.push(currentArgName.trim());
      }
      currentArgName = '';
      inDefaultValue = false;
      continue;
    }

    // Add valid characters to the current argument
    if (!inDefaultValue) {
      currentArgName += char;
    }
  }

  // Push the last argument if it exists
  if (currentArgName.trim()) {
    if (currentArgName.trim()) {
      args.push(currentArgName.trim());
    }
  }

  return args;
}

export interface ArgDataType {
  dataType?: DBOSDataType; // Also a very simplistic data type format... for native scalars or JSON
}

export class MethodParameter {
  name: string = '';
  index: number = -1;

  externalRegInfo: Map<AnyConstructor | object | string, object> = new Map();

  getRegisteredInfo(reg: AnyConstructor | object | string) {
    if (!this.externalRegInfo.has(reg)) {
      this.externalRegInfo.set(reg, {});
    }
    return this.externalRegInfo.get(reg)!;
  }

  get dataType() {
    return (this.getRegisteredInfo('type') as ArgDataType).dataType;
  }

  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  initializeBaseType(at?: Function) {
    if (!this.externalRegInfo.has('type')) {
      this.externalRegInfo.set('type', {});
    }
    const adt = this.externalRegInfo.get('type') as ArgDataType;
    adt.dataType = DBOSDataType.fromArg(at);
  }

  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  constructor(idx: number, at?: Function) {
    this.index = idx;
    this.initializeBaseType(at);
  }
}

//////////////////////////////////////////
/* REGISTRATION OBJECTS and read access */
//////////////////////////////////////////

export const DBOS_AUTH = 'auth';

export interface ClassAuthDefaults {
  requiredRole?: string[] | undefined;
}

export interface MethodAuth {
  requiredRole?: string[] | undefined;
}

export interface RegistrationDefaults {
  name: string;

  getRegisteredInfo(reg: AnyConstructor | object | string): unknown;

  eventReceiverInfo: Map<DBOSEventReceiver, unknown>;
  externalRegInfo: Map<AnyConstructor | object | string, unknown>;
}

export interface MethodRegistrationBase {
  name: string;
  className: string;

  args: MethodParameter[];

  defaults?: RegistrationDefaults; // This is the class-level info

  getRequiredRoles(): string[];

  workflowConfig?: WorkflowConfig;
  txnConfig?: TransactionConfig;
  stepConfig?: StepConfig;
  procConfig?: TransactionConfig;
  isInstance: boolean;

  // This is for DBOSEventReceivers (an older approach) to keep stuff
  eventReceiverInfo: Map<DBOSEventReceiver, unknown>;
  // This is for any class or object to keep stuff associated with a class
  externalRegInfo: Map<AnyConstructor | object | string, unknown>;

  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  wrappedFunction: Function | undefined; // Function that is user-callable, including the WF engine transition
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  registeredFunction: Function | undefined; // Function that is called by DBOS engine, including input validation and role check
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  origFunction: Function; // Function that the app provided
  // Pass context as first arg?
  readonly passContext: boolean;

  // Add an interceptor that, when function is run, get a chance to process arguments / throw errors
  addEntryInterceptor(func: (reg: MethodRegistrationBase, args: unknown[]) => unknown[], seqNum?: number): void;

  getRegisteredInfo(reg: AnyConstructor | object | string): unknown;

  invoke(pthis: unknown, args: unknown[]): unknown;
}

export class MethodRegistration<This, Args extends unknown[], Return> implements MethodRegistrationBase {
  defaults?: RegistrationDefaults | undefined;

  name: string = '';
  className: string = '';

  // Interceptors
  onEnter: { seqNum: number; func: (reg: MethodRegistrationBase, args: unknown[]) => unknown[] }[] = [];
  addEntryInterceptor(func: (reg: MethodRegistrationBase, args: unknown[]) => unknown[], seqNum: number = 10) {
    this.onEnter.push({ seqNum, func });
    this.onEnter.sort((a, b) => a.seqNum - b.seqNum);
  }

  args: MethodParameter[] = [];
  passContext: boolean = false;

  constructor(origFunc: (this: This, ...args: Args) => Promise<Return>, isInstance: boolean, passContext: boolean) {
    this.origFunction = origFunc;
    this.isInstance = isInstance;
    this.passContext = passContext;
  }

  needInitialized: boolean = true;
  isInstance: boolean;
  origFunction: (this: This, ...args: Args) => Promise<Return>;
  registeredFunction: ((this: This, ...args: Args) => Promise<Return>) | undefined;
  wrappedFunction: ((this: This, ...args: Args) => Promise<Return>) | undefined = undefined;
  workflowConfig?: WorkflowConfig;
  txnConfig?: TransactionConfig;
  stepConfig?: StepConfig;
  procConfig?: TransactionConfig;
  regLocation?: string[];
  eventReceiverInfo: Map<DBOSEventReceiver, unknown> = new Map();
  externalRegInfo: Map<AnyConstructor | object | string, unknown> = new Map();

  getRegisteredInfo(reg: AnyConstructor | object | string) {
    if (!this.externalRegInfo.has(reg)) {
      this.externalRegInfo.set(reg, {});
    }
    return this.externalRegInfo.get(reg)!;
  }

  getAssignedType(): 'Transaction' | 'Workflow' | 'Step' | 'Procedure' | undefined {
    if (this.txnConfig) return 'Transaction';
    if (this.workflowConfig) return 'Workflow';
    if (this.stepConfig) return 'Step';
    if (this.procConfig) return 'Procedure';
    return undefined;
  }

  checkFuncTypeUnassigned(newType: string) {
    const oldType = this.getAssignedType();
    let error: string | undefined = undefined;
    if (oldType && newType !== oldType) {
      error = `Operation (Name: ${this.className}.${this.name}) is already registered with a conflicting function type: ${oldType} vs. ${newType}`;
    } else if (oldType) {
      error = `Operation (Name: ${this.className}.${this.name}) is already registered.`;
    }
    if (error) {
      if (this.regLocation) {
        error = error + `\nPrior registration occurred at:\n${this.regLocation.join('\n')}`;
      }
      throw new DBOSConflictingRegistrationError(`${error}`);
    } else {
      this.regLocation = new StackGrabber().getCleanStack(3);
    }
  }

  setTxnConfig(txCfg: TransactionConfig): void {
    this.checkFuncTypeUnassigned('Transaction');
    this.txnConfig = txCfg;
  }

  setStepConfig(stepCfg: StepConfig): void {
    this.checkFuncTypeUnassigned('Step');
    this.stepConfig = stepCfg;
  }

  setProcConfig(procCfg: TransactionConfig): void {
    this.checkFuncTypeUnassigned('Procedure');
    this.procConfig = procCfg;
  }

  setWorkflowConfig(wfCfg: WorkflowConfig): void {
    this.checkFuncTypeUnassigned('Workflow');
    this.workflowConfig = wfCfg;
  }

  init: boolean = false;

  invoke(pthis: This, args: Args): Promise<Return> {
    const f = this.wrappedFunction ?? this.registeredFunction ?? this.origFunction;
    return f.call(pthis, ...args);
  }

  getRequiredRoles() {
    const rr = this.getRegisteredInfo(DBOS_AUTH) as MethodAuth;

    if (rr?.requiredRole) {
      return rr.requiredRole;
    }

    const drr = this.defaults?.getRegisteredInfo(DBOS_AUTH) as ClassAuthDefaults;
    return drr?.requiredRole || [];
  }
}

function registerClassInstance(inst: ConfiguredInstance, name: string) {
  const creg = getOrCreateClassRegistration(inst.constructor as AnyConstructor);
  if (creg.configuredInstances.has(name)) {
    throw new DBOSConflictingRegistrationError(
      `An instance of class '${inst.constructor.name}' with name '${name}' was already registered.  Earlier registration occurred at:\n${(creg.configuredInstanceRegLocs.get(name) ?? []).join('\n')}`,
    );
  }
  creg.configuredInstances.set(name, inst);
  creg.configuredInstanceRegLocs.set(name, new StackGrabber().getCleanStack(3) ?? []);
}

export abstract class ConfiguredInstance {
  readonly name: string;
  constructor(name: string) {
    if (dbosLaunchPoint) {
      console.warn(
        `ConfiguredInstance '${name}' is being created after DBOS initialization and was not available for recovery.`,
      );
    }
    this.name = name;
    registerClassInstance(this, name);
  }
  /**
   * Override this method to perform async initialization.
   * @param _ctx - @deprecated This parameter is unnecessary, use `DBOS` instead.
   */
  initialize(_ctx: InitContext): Promise<void> {
    return Promise.resolve();
  }
}

export class ClassRegistration implements RegistrationDefaults {
  name: string = '';
  needsInitialized: boolean = true;

  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  ormEntities: Function[] | { [key: string]: object } = [];

  registeredOperations: Map<string, MethodRegistrationBase> = new Map();

  configuredInstances: Map<string, ConfiguredInstance> = new Map();
  configuredInstanceRegLocs: Map<string, string[]> = new Map();

  eventReceiverInfo: Map<DBOSEventReceiver, unknown> = new Map();
  externalRegInfo: Map<AnyConstructor | object | string, unknown> = new Map();

  getRegisteredInfo(reg: AnyConstructor | object | string) {
    if (!this.externalRegInfo.has(reg)) {
      this.externalRegInfo.set(reg, {});
    }
    return this.externalRegInfo.get(reg)!;
  }

  constructor() {}
}

class StackGrabber extends Error {
  constructor() {
    super('StackGrabber');
    Error.captureStackTrace(this, StackGrabber); // Excludes constructor from the stack
  }

  getCleanStack(frames: number = 1) {
    return this.stack
      ?.split('\n')
      .slice(frames + 1)
      .map((l) => '>>> ' + l.replace(/^\s*at\s*/, '')); // Remove the first lines
  }
}

let dbosLaunchPoint: string[] | undefined = undefined;
export function recordDBOSLaunch() {
  dbosLaunchPoint = new StackGrabber().getCleanStack(2); // Remove one for record, one for registerAndWrap...
}
export function recordDBOSShutdown() {
  dbosLaunchPoint = undefined;
}

export function ensureDBOSIsNotLaunched() {
  if (dbosLaunchPoint) {
    throw new DBOSConflictingRegistrationError(
      `DBOS code is being registered after DBOS.launch().  DBOS was launched from:\n${dbosLaunchPoint.join('\n')}\n`,
    );
  }
}

// This is a bit ugly, if we got the class / instance it would help avoid this auxiliary structure
const methodToRegistration: Map<unknown, MethodRegistration<unknown, unknown[], unknown>> = new Map();
export function getRegisteredMethodClassName(func: unknown): string {
  let rv: string = '';
  if (methodToRegistration.has(func)) {
    rv = methodToRegistration.get(func)!.className;
  }
  return rv;
}
export function getRegisteredMethodName(func: unknown): string {
  let rv: string = '';
  if (methodToRegistration.has(func)) {
    rv = methodToRegistration.get(func)!.name;
  }
  return rv;
}
export function registerFunctionWrapper(func: unknown, reg: MethodRegistration<unknown, unknown[], unknown>) {
  methodToRegistration.set(func, reg);
}
export function getRegistrationForFunction(func: unknown): MethodRegistration<unknown, unknown[], unknown> | undefined {
  return methodToRegistration.get(func);
}

export function getRegisteredOperations(target: object): ReadonlyArray<MethodRegistrationBase> {
  const registeredOperations: MethodRegistrationBase[] = [];

  if (typeof target === 'function') {
    // Constructor case
    const classReg = classesByName.get(target.name);
    classReg?.reg?.registeredOperations?.forEach((m) => registeredOperations.push(m));
  } else {
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

export function getRegisteredOperationsByClassname(target: string): ReadonlyArray<MethodRegistrationBase> {
  const registeredOperations: MethodRegistrationBase[] = [];
  const cls = getClassRegistrationByName(target);
  cls.registeredOperations?.forEach((m) => registeredOperations.push(m));
  return registeredOperations;
}

export function getConfiguredInstance(clsname: string, cfgname: string): ConfiguredInstance | null {
  const classReg = classesByName.get(clsname)?.reg;
  if (!classReg) return null;
  return classReg.configuredInstances.get(cfgname) ?? null;
}

/////
// Transactional data source registration
/////
export const transactionalDataSources: Map<string, DataSourceTransactionHandler> = new Map();

// Register data source (user version)
export function registerTransactionalDataSource(name: string, ds: DataSourceTransactionHandler) {
  if (transactionalDataSources.has(name)) {
    if (transactionalDataSources.get(name) !== ds) {
      throw new DBOSConflictingRegistrationError(`Data source with name ${name} is already registered`);
    }
    return;
  }
  ensureDBOSIsNotLaunched();
  transactionalDataSources.set(name, ds);
}

export function getTransactionalDataSource(name: string) {
  if (transactionalDataSources.has(name)) return transactionalDataSources.get(name)!;
  throw new DBOSNotRegisteredError(name, `Data source '${name}' is not registered`);
}

////////////////////////////////////////////////////////////////////////////////
// DECORATOR REGISTRATION
// These manage registration objects, creating them at decorator evaluation time
// and making wrapped methods available for function registration at runtime
// initialization time.
////////////////////////////////////////////////////////////////////////////////

const methodArgsByFunction: Map<string, MethodParameter[]> = new Map();

export function getOrCreateMethodArgsRegistration(
  target: object | undefined,
  className: string | undefined,
  funcName: string | symbol,
  func?: (...args: unknown[]) => unknown,
): MethodParameter[] {
  let regtarget = target;
  if (regtarget && typeof regtarget !== 'function') {
    regtarget = regtarget.constructor;
  }

  className = className ?? (target ? getNameForClass(target) : '');

  const mkey = className + '|' + funcName.toString();

  let mParameters: MethodParameter[] | undefined = methodArgsByFunction.get(mkey);
  if (mParameters === undefined) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    let designParamTypes: Function[] | undefined = undefined;
    if (target) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
      designParamTypes = Reflect.getMetadata('design:paramtypes', target, funcName) as Function[] | undefined;
    }
    if (designParamTypes) {
      mParameters = designParamTypes.map((value, index) => new MethodParameter(index, value));
    } else {
      if (func) {
        const argnames = getArgNames(func);
        mParameters = argnames.map((_value, index) => new MethodParameter(index));
      } else {
        const descriptor = Object.getOwnPropertyDescriptor(target, funcName);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
        const argnames = getArgNames(descriptor?.value as Function);
        mParameters = argnames.map((_value, index) => new MethodParameter(index));
      }
    }

    methodArgsByFunction.set(mkey, mParameters);
  }

  return mParameters;
}

function getOrCreateMethodRegistration<This, Args extends unknown[], Return>(
  target: object | undefined,
  className: string | undefined,
  propertyKey: string | symbol,
  func: (this: This, ...args: Args) => Promise<Return>,
  passContext: boolean,
) {
  let regtarget: AnyConstructor | undefined = undefined;
  let isInstance = false;

  if (target) {
    if (typeof target === 'function') {
      // Static method case
      regtarget = target as AnyConstructor;
    } else {
      // Instance method case
      regtarget = target.constructor as AnyConstructor;
      isInstance = true;
    }
  }
  if (!className) {
    if (regtarget) className = regtarget.name;
  }
  if (!className) {
    className = '';
  }

  const classReg = getClassRegistrationByName(className, true);

  const fname = propertyKey.toString();
  if (!classReg.registeredOperations.has(fname)) {
    classReg.registeredOperations.set(fname, new MethodRegistration<This, Args, Return>(func, isInstance, passContext));
  }
  const methReg: MethodRegistration<This, Args, Return> = classReg.registeredOperations.get(
    fname,
  )! as MethodRegistration<This, Args, Return>;

  // Note: We cannot tell if the method takes a context or not.
  //  Our @Workflow, @Transaction, and @Step decorators are the only ones that would know to set passContext.
  // So, if passContext is indicated, add it to the registration.
  if (passContext && !methReg.passContext) {
    methReg.passContext = true;
  }

  if (methReg.needInitialized) {
    methReg.needInitialized = false;
    methReg.name = fname;
    methReg.className = classReg.name;
    methReg.defaults = classReg;

    methReg.args = getOrCreateMethodArgsRegistration(
      target,
      className,
      propertyKey,
      func as (...args: unknown[]) => unknown,
    );

    const argNames = getArgNames(func);

    methReg.args.forEach((e) => {
      if (!e.name) {
        if (e.index < argNames.length) {
          e.name = argNames[e.index];
        }
      }
    });

    const wrappedMethod = async function (this: This, ...rawArgs: Args) {
      let validatedArgs = rawArgs;
      for (const vf of methReg.onEnter) {
        validatedArgs = vf.func(methReg, validatedArgs) as Args;
      }

      return methReg.origFunction.call(this, ...validatedArgs);
    };
    Object.defineProperty(wrappedMethod, 'name', {
      value: methReg.name,
    });

    methReg.registeredFunction = wrappedMethod;

    methodToRegistration.set(methReg.registeredFunction, methReg as MethodRegistration<unknown, unknown[], unknown>);
    methodToRegistration.set(methReg.origFunction, methReg as MethodRegistration<unknown, unknown[], unknown>);
  }

  return methReg;
}

export function registerAndWrapFunctionTakingContext<This, Args extends unknown[], Return>(
  target: object,
  propertyKey: string,
  descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
) {
  ensureDBOSIsNotLaunched();
  if (!descriptor.value) {
    throw Error('Use of decorator when original method is undefined');
  }

  const registration = getOrCreateMethodRegistration(target, undefined, propertyKey, descriptor.value, true);
  descriptor.value = registration.wrappedFunction ?? registration.registeredFunction;

  return { descriptor, registration };
}

export function registerAndWrapDBOSFunction<This, Args extends unknown[], Return>(
  target: object,
  propertyKey: string,
  descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
) {
  ensureDBOSIsNotLaunched();
  if (!descriptor.value) {
    throw Error('Use of decorator when original method is undefined');
  }

  const registration = getOrCreateMethodRegistration(target, undefined, propertyKey, descriptor.value, false);
  descriptor.value = registration.wrappedFunction ?? registration.registeredFunction;

  return { descriptor, registration };
}

export function registerAndWrapDBOSFunctionByName<This, Args extends unknown[], Return>(
  target: object | undefined,
  className: string | undefined,
  funcName: string,
  func: (this: This, ...args: Args) => Promise<Return>,
) {
  ensureDBOSIsNotLaunched();

  const registration = getOrCreateMethodRegistration(target, className, funcName, func, false);

  return { registration };
}

// Data structure notes:
//  Everything is registered under a "className", but this may be blank.
//   This often corresponds to a real `class`, but it does not have to, if the user
//   registered stuff without decorators.  Or for a bare function.
//  Thus, if you have a "class name", look it up in classesByName, as this is exhaustive
//  If you have a class or instance, you can look that up in the classesByCtor map,
//   this contains all decorator-registered classes, but may omit other things.
type AnyConstructor = new (...args: unknown[]) => object;
const classesByName: Map<string, { reg: ClassRegistration; ctor?: AnyConstructor }> = new Map();
const classesByCtor: Map<AnyConstructor, { name: string; reg: ClassRegistration }> = new Map();
export function getNameForClass(ctor: object): string {
  let regtarget: AnyConstructor;
  if (typeof ctor === 'function') {
    // Static method case
    regtarget = ctor as AnyConstructor;
  } else {
    // Instance method case
    regtarget = ctor.constructor as AnyConstructor;
  }

  if (!classesByCtor.has(regtarget)) return regtarget.name;
  return classesByCtor.get(regtarget)!.name;
}

export function getAllRegisteredClassNames() {
  const cnames: string[] = [];
  for (const [cn, _creg] of classesByName) {
    cnames.push(cn);
  }
  return cnames;
}

export function getClassRegistrationByName(name: string, create: boolean = false) {
  if (!classesByName.has(name) && !create) {
    throw new DBOSNotRegisteredError(name, `Class '${name}' is not registered`);
  }

  if (!classesByName.has(name)) {
    classesByName.set(name, { reg: new ClassRegistration() });
  }

  const clsReg: ClassRegistration = classesByName.get(name)!.reg;

  if (clsReg.needsInitialized) {
    clsReg.name = name;
    clsReg.needsInitialized = false;
  }
  return clsReg;
}

export function getOrCreateClassRegistration<CT extends { new (...args: unknown[]): object }>(ctor: CT) {
  const name = getNameForClass(ctor);
  if (!classesByName.has(name)) {
    classesByName.set(name, { ctor, reg: new ClassRegistration() });
  }
  const clsReg: ClassRegistration = classesByName.get(name)!.reg;

  if (clsReg.needsInitialized) {
    clsReg.name = name;
    clsReg.needsInitialized = false;
  }
  return clsReg;
}

/**
 * Associates a class with a `DBOSEventReceiver`, which will be calling the class's DBOS methods.
 * Allows class-level default values or other storage to be associated with the class, rather than
 *   separately for each registered method.
 *
 * @param rcvr - Event receiver which will dispatch DBOS methods from the class specified by `ctor`
 * @param ctor - Constructor of the class that is being registered and associated with `rcvr`
 * @returns - Class-specific registration info cumulatively collected for `rcvr`
 */
export function associateClassWithEventReceiver<CT extends { new (...args: unknown[]): object }>(
  rcvr: DBOSEventReceiver,
  ctor: CT,
) {
  const clsReg = getOrCreateClassRegistration(ctor);
  if (!clsReg.eventReceiverInfo.has(rcvr)) {
    clsReg.eventReceiverInfo.set(rcvr, {});
  }
  return clsReg.eventReceiverInfo.get(rcvr)!;
}

export function associateClassWithExternal(
  external: AnyConstructor | object | string,
  cls: AnyConstructor | string,
): object {
  const clsn: string = typeof cls === 'string' ? cls : getNameForClass(cls);
  const clsreg = getClassRegistrationByName(clsn, true);
  return clsreg.getRegisteredInfo(external);
}

/**
 * Associates a workflow method with a `DBOSEventReceiver` which will be in charge of calling the method
 *   in response to received events.
 * This version is to be used in "Stage 2" decorators, as it applies the DBOS wrapper to the registered method.
 *
 * @param rcvr - `DBOSEventReceiver` instance that should be informed of the `target` method's registration
 * @param target - A DBOS method to associate with the event receiver
 * @param propertyKey - For Stage 2 decorator use, this is the property key used for replacing the method with its wrapper
 * @param inDescriptor - For Stage 2 decorator use, this is the method descriptor used for replacing the method with its wrapper
 * @returns The new method descriptor, registration, and event receiver info
 */
export function associateMethodWithEventReceiver<This, Args extends unknown[], Return>(
  rcvr: DBOSEventReceiver,
  target: object,
  propertyKey: string,
  inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
) {
  const { descriptor, registration } = registerAndWrapDBOSFunction(target, propertyKey, inDescriptor);
  if (!registration.eventReceiverInfo.has(rcvr)) {
    registration.eventReceiverInfo.set(rcvr, {});
  }
  return { descriptor, registration, receiverInfo: registration.eventReceiverInfo.get(rcvr)! };
}

/*
 * Associates a DBOS function or method with an external class or object.
 *   Likely, this will be invoking or intercepting the method.
 */
export function associateMethodWithExternal<This, Args extends unknown[], Return>(
  external: AnyConstructor | object | string,
  target: object | undefined,
  className: string | undefined,
  funcName: string,
  func: (this: This, ...args: Args) => Promise<Return>,
): {
  registration: MethodRegistration<This, Args, Return>;
  regInfo: object;
} {
  const { registration } = registerAndWrapDBOSFunctionByName(target, className, funcName, func);
  if (!registration.externalRegInfo.has(external)) {
    registration.externalRegInfo.set(external, {});
  }
  return { registration, regInfo: registration.externalRegInfo.get(external)! };
}

/*
 * Associates a DBOS function or method with an external class or object.
 *   Likely, this will be invoking or intercepting the method.
 */
export function associateParameterWithExternal<This, Args extends unknown[], Return>(
  external: AnyConstructor | object | string,
  target: object | undefined,
  className: string | undefined,
  funcName: string,
  func: ((this: This, ...args: Args) => Promise<Return>) | undefined,
  paramId: number | string,
): object | undefined {
  if (!func) {
    func = Object.getOwnPropertyDescriptor(target, funcName)!.value as (this: This, ...args: Args) => Promise<Return>;
  }
  const { registration } = registerAndWrapDBOSFunctionByName(target, className, funcName, func);
  let param: MethodParameter | undefined;
  if (typeof paramId === 'number') {
    param = registration.args[paramId];
  } else {
    param = registration.args.find((p) => p.name === paramId);
  }

  if (!param) return undefined;

  if (!param.externalRegInfo.has(external)) {
    param.externalRegInfo.set(external, {});
  }

  return param.externalRegInfo.get(external)!;
}

export function getRegistrationsForExternal(
  external: AnyConstructor | object | string,
  cls?: object | string,
  funcName?: string,
) {
  const res: {
    methodConfig: unknown;
    classConfig: unknown;
    methodReg: MethodRegistrationBase;
    paramConfig: { name: string; index: number; paramConfig?: object }[];
  }[] = [];

  if (cls) {
    const clsname = typeof cls === 'string' ? cls : getNameForClass(cls);
    const c = classesByName.get(clsname);
    if (c) {
      if (funcName) {
        const f = c.reg.registeredOperations.get(funcName);
        if (f) {
          collectRegForFunction(f);
        }
      } else {
        collectRegForClass(c);
      }
    }
  } else {
    for (const [_cn, c] of classesByName) {
      collectRegForClass(c);
    }
  }
  return res;

  function collectRegForClass(c: { reg: ClassRegistration; ctor?: AnyConstructor }) {
    for (const [_fn, f] of c.reg.registeredOperations) {
      collectRegForFunction(f);
    }
  }

  function collectRegForFunction(f: MethodRegistrationBase) {
    const methodConfig = f.externalRegInfo.get(external);
    const classConfig = f.defaults?.externalRegInfo.get(external);
    const paramConfig: { name: string; index: number; paramConfig?: object }[] = [];
    let hasParamConfig = false;
    for (const arg of f.args) {
      if (arg.externalRegInfo.has(external)) hasParamConfig = true;

      paramConfig.push({
        name: arg.name,
        index: arg.index,
        paramConfig: arg.externalRegInfo.get(external),
      });
    }
    if (!methodConfig && !classConfig && !hasParamConfig) return;
    res.push({ methodReg: f, methodConfig, classConfig: classConfig ?? {}, paramConfig });
  }
}

//////////////////////////
/* PARAMETER DECORATORS */
//////////////////////////

export function ArgName(name: string) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const existingParameters = getOrCreateMethodArgsRegistration(target, undefined, propertyKey);

    const curParam = existingParameters[parameterIndex];
    curParam.name = name;
  };
}

///////////////////////
/* CLASS DECORATORS */
///////////////////////

/** @deprecated Use `new` */
export function configureInstance<R extends ConfiguredInstance, T extends unknown[]>(
  cls: new (name: string, ...args: T) => R,
  name: string,
  ...args: T
): R {
  const inst = new cls(name, ...args);
  return inst;
}

///////////////////////
/* METHOD DECORATORS */
///////////////////////

/**
 * @deprecated Use `@DBOS.workflow`
 * To upgrade to DBOS 2.0+ syntax:
 *   Use `@DBOS.workflow` to decorate the method
 *   Remove the `WorkflowContext` parameter
 *   Use `DBOS` instead of the context to access DBOS functions
 *   Change all callers of the decorated function to call the function directly, or with `DBOS.startWorkflow`
 */
export function Workflow(config: WorkflowConfig = {}) {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: WorkflowContext, ...args: Args) => Promise<Return>>,
  ) {
    const { descriptor, registration } = registerAndWrapFunctionTakingContext(target, propertyKey, inDescriptor);
    registration.setWorkflowConfig(config);
    return descriptor;
  }
  return decorator;
}

/**
 * @deprecated Use `@DBOS.transaction`
 * To upgrade to DBOS 2.0+ syntax:
 *   Use `@DBOS.transaction` to decorate the method
 *   Remove the `TransactionContext` parameter
 *   Use `DBOS` instead of the context to access DBOS functions
 *   Change all callers to call the decorated function directly
 */
export function Transaction(config: TransactionConfig = {}) {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: TransactionContext<any>, ...args: Args) => Promise<Return>>,
  ) {
    const { descriptor, registration } = registerAndWrapFunctionTakingContext(target, propertyKey, inDescriptor);
    registration.setTxnConfig(config);
    return descriptor;
  }
  return decorator;
}

/**
 * @deprecated Use `@DBOS.storedProcedure`
 * To upgrade to DBOS 2.0+ syntax:
 *   Use `@DBOS.storedProcedure` to decorate the method
 *   Remove the context parameter and use `DBOS` instead of the context to access DBOS functions
 *   Change all callers to call the decorated function directly
 */
export function StoredProcedure(config: StoredProcedureConfig = {}) {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: StoredProcedureContext, ...args: Args) => Promise<Return>>,
  ) {
    const { descriptor, registration } = registerAndWrapFunctionTakingContext(target, propertyKey, inDescriptor);
    registration.setProcConfig(config);
    return descriptor;
  }
  return decorator;
}

/**
 * @deprecated Use `@DBOS.step`
 * To upgrade to DBOS 2.0+ syntax:
 *   Use `@DBOS.step` to decorate the method
 *   Remove the `StepContext` parameter
 *   Use `DBOS` instead of the context to access DBOS functions
 *   Change all callers to call the decorated function directly
 */
export function Step(config: StepConfig = {}) {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: StepContext, ...args: Args) => Promise<Return>>,
  ) {
    const { descriptor, registration } = registerAndWrapFunctionTakingContext(target, propertyKey, inDescriptor);
    registration.setStepConfig(config);
    return descriptor;
  }
  return decorator;
}

/**
 * @deprecated Use ORM DSs
 */
// eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
export function OrmEntities(entities: Function[] | { [key: string]: object } = []) {
  function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
    const clsreg = getOrCreateClassRegistration(ctor);
    clsreg.ormEntities = entities;
  }
  return clsdec;
}

export function DBOSInitializer() {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
  ) {
    const { descriptor, registration } = registerAndWrapDBOSFunction(target, propertyKey, inDescriptor);
    registration.init = true;
    return descriptor;
  }
  return decorator;
}

/** @deprecated */
export function DBOSDeploy() {
  function decorator<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: InitContext, ...args: Args) => Promise<Return>>,
  ) {
    const { descriptor, registration } = registerAndWrapFunctionTakingContext(target, propertyKey, inDescriptor);
    registration.init = true;
    return descriptor;
  }
  return decorator;
}
