import { WorkflowConfig } from './workflow';
import { StepConfig } from './step';
import { DBOSConflictingRegistrationError, DBOSNotRegisteredError } from './error';
import { DataSourceTransactionHandler } from './datasource';

// #region Interfaces and supporting types

export type TypedAsyncFunction<T extends unknown[], R> = (...args: T) => Promise<R>;
export type UntypedAsyncFunction = TypedAsyncFunction<unknown[], unknown>;

/**
 * Interface for naming DBOS-registered functions.
 *   These names are used for log and database entries.
 *   They are used for lookup in some cases (like workflow recovery)
 */
export interface FunctionName {
  /** Function name; if not provided, this will be taken from the function's `name` */
  name?: string;
  /** Class name; if not provided, the class constructor or prototype's `name` will be used, or blank otherwise */
  className?: string;
  /**
   * For member functions, class constructor (for `static` methods) or prototype (for instance methods)
   *   This will be used to get the class name if `className` is not provided.
   */
  ctorOrProto?: object;
}

/**
 * Interface for integrating into the DBOS startup/shutdown lifecycle
 */
export interface DBOSLifecycleCallback {
  /** Called back during DBOS launch */
  initialize?(): Promise<void>;
  /** Called back upon shutdown (usually in tests) to close connections and free resources */
  destroy?(): Promise<void>;
  /** Called at launch; Implementers should emit a diagnostic list of all registrations */
  logRegisteredEndpoints?(): void;
}

// Middleware installation
export interface DBOSMethodMiddlewareInstaller {
  installMiddleware(methodReg: MethodRegistrationBase): void;
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

  externalRegInfo: Map<AnyConstructor | object | string, unknown>;
}

export interface MethodRegistrationBase {
  name: string;
  className: string;

  args: MethodParameter[];

  defaults?: RegistrationDefaults; // This is the class-level info

  getRequiredRoles(): string[];

  workflowConfig?: WorkflowConfig;
  stepConfig?: StepConfig;
  isInstance: boolean;

  // This is for any class or object to keep stuff associated with a class
  externalRegInfo: Map<AnyConstructor | object | string, unknown>;

  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  wrappedFunction: Function | undefined; // Function that is user-callable, including the WF engine transition
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  registeredFunction: Function | undefined; // Function that is called by DBOS engine, including input validation and role check
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  origFunction: Function; // Function that the app provided

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

  constructor(origFunc: (this: This, ...args: Args) => Promise<Return>, isInstance: boolean) {
    this.origFunction = origFunc;
    this.isInstance = isInstance;
  }

  needInitialized: boolean = true;
  isInstance: boolean;
  origFunction: (this: This, ...args: Args) => Promise<Return>;
  registeredFunction: ((this: This, ...args: Args) => Promise<Return>) | undefined;
  wrappedFunction: ((this: This, ...args: Args) => Promise<Return>) | undefined = undefined;
  workflowConfig?: WorkflowConfig;
  stepConfig?: StepConfig;
  regLocation?: string[];
  externalRegInfo: Map<AnyConstructor | object | string, unknown> = new Map();

  getRegisteredInfo(reg: AnyConstructor | object | string) {
    if (!this.externalRegInfo.has(reg)) {
      this.externalRegInfo.set(reg, {});
    }
    return this.externalRegInfo.get(reg)!;
  }

  getAssignedType(): 'Workflow' | 'Step' | undefined {
    if (this.workflowConfig) return 'Workflow';
    if (this.stepConfig) return 'Step';
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

  setStepConfig(stepCfg: StepConfig): void {
    this.checkFuncTypeUnassigned('Step');
    this.stepConfig = stepCfg;
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
   * Override this method to perform async initialization between construction and `DBOS.launch()`.
   */
  initialize(): Promise<void> {
    return Promise.resolve();
  }
}

export class ClassRegistration implements RegistrationDefaults {
  name: string = '';
  needsInitialized: boolean = true;

  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  ormEntities: Function[] | { [key: string]: object } = [];

  registeredOperationsByName: Map<string, MethodRegistrationBase> = new Map();
  allRegisteredOperations: Map<unknown, MethodRegistrationBase> = new Map();

  configuredInstances: Map<string, ConfiguredInstance> = new Map();
  configuredInstanceRegLocs: Map<string, string[]> = new Map();

  externalRegInfo: Map<AnyConstructor | object | string, unknown> = new Map();

  registerOperationByName(name: string, reg: MethodRegistrationBase) {
    const er = this.registeredOperationsByName.get(name) as MethodRegistration<unknown, unknown[], unknown>;
    if (er && er !== reg) {
      let error = `Operation (Name: ${this.name}.${name}) is already registered.`;
      if (er.regLocation) {
        error = error + `\nPrior registration occurred at:\n${er.regLocation.join('\n')}`;
      }
      throw new DBOSConflictingRegistrationError(`${error}`);
    }
    this.registeredOperationsByName.set(name, reg);
    (reg as MethodRegistration<unknown, unknown[], unknown>).regLocation = new StackGrabber().getCleanStack(3);
  }

  getRegisteredInfo(reg: AnyConstructor | object | string) {
    if (!this.externalRegInfo.has(reg)) {
      this.externalRegInfo.set(reg, {});
    }
    return this.externalRegInfo.get(reg)!;
  }

  constructor() {}
}

// #endregion

// #region Global registration structures and functions

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

// Track if DBOS is launched, and if so, from where
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

export function ensureDBOSIsLaunched(reason: string) {
  if (!dbosLaunchPoint) {
    throw new TypeError(`\`DBOS.launch()\` must be called before running ${reason}.`);
  }
}

// DBOS launch lifecycle listener
const lifecycleListeners: DBOSLifecycleCallback[] = [];
export function registerLifecycleCallback(lcl: DBOSLifecycleCallback) {
  if (!lifecycleListeners.includes(lcl)) lifecycleListeners.push(lcl);
}
export function getLifecycleListeners() {
  return lifecycleListeners as readonly DBOSLifecycleCallback[];
}

// Middleware installers - insert middleware in registered functions prior to launch
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
    for (const f of c.reg.allRegisteredOperations.values()) {
      for (const i of middlewareInstallers) {
        i.installMiddleware(f);
      }
    }
  }
}

// Registration of functions, and classes
const functionToRegistration: Map<unknown, MethodRegistration<unknown, unknown[], unknown>> = new Map();

// Registration of instance, by constructor+name
function registerClassInstance(inst: ConfiguredInstance, instname: string) {
  const creg = getOrCreateClassRegistration(inst.constructor as AnyConstructor);
  if (creg.configuredInstances.has(instname)) {
    throw new DBOSConflictingRegistrationError(
      `An instance of class '${inst.constructor.name}' with name '${instname}' was already registered.  Earlier registration occurred at:\n${(creg.configuredInstanceRegLocs.get(instname) ?? []).join('\n')}`,
    );
  }
  creg.configuredInstances.set(instname, inst);
  creg.configuredInstanceRegLocs.set(instname, new StackGrabber().getCleanStack(3) ?? []);
}

export function getRegisteredFunctionFullName(func: unknown) {
  let className: string = '';
  let funcName: string = (func as { name?: string }).name ?? '';
  if (functionToRegistration.has(func)) {
    const fr = functionToRegistration.get(func)!;
    className = fr.className;
    funcName = fr.name;
  }
  return { className, name: funcName };
}

export function getRegisteredFunctionQualifiedName(func: unknown) {
  const fn = getRegisteredFunctionFullName(func);
  return fn.className + '.' + fn.name;
}

export function getRegisteredFunctionClassName(func: unknown): string {
  return getRegisteredFunctionFullName(func).className;
}

export function getRegisteredFunctionName(func: unknown): string {
  return getRegisteredFunctionFullName(func).name;
}

export function registerFunctionWrapper<This, Args extends unknown[], Return>(
  func: (this: This, ...args: Args) => Promise<Return>,
  reg: MethodRegistration<This, Args, Return>,
) {
  reg.wrappedFunction = func;
  functionToRegistration.set(func, reg as MethodRegistration<unknown, unknown[], unknown>);
}

export function getFunctionRegistration(func: unknown): MethodRegistration<unknown, unknown[], unknown> | undefined {
  return functionToRegistration.get(func);
}

export function getFunctionRegistrationByName(className: string, name: string) {
  const clsreg = getClassRegistrationByName(className, false);
  if (!clsreg) return undefined;
  const methReg = clsreg.registeredOperationsByName.get(name);
  if (!methReg) return undefined;
  return methReg;
}

export function getRegisteredOperations(target: object): ReadonlyArray<MethodRegistrationBase> {
  const registeredOperations: MethodRegistrationBase[] = [];

  if (typeof target === 'function') {
    // Constructor case
    const classReg = classesByName.get(target.name);
    classReg?.reg?.allRegisteredOperations?.forEach((m) => registeredOperations.push(m));
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

export function getRegisteredFunctionsByClassname(target: string): ReadonlyArray<MethodRegistrationBase> {
  const registeredOperations: MethodRegistrationBase[] = [];
  const cls = getClassRegistrationByName(target);
  cls?.allRegisteredOperations?.forEach((m) => registeredOperations.push(m));
  return registeredOperations;
}

const methodArgsByFunction: Map<unknown, MethodParameter[]> = new Map();

export function getOrCreateMethodArgsRegistration(
  target: object | undefined,
  funcName: PropertyKey,
  origFunc?: (...args: unknown[]) => unknown,
): MethodParameter[] {
  let regtarget = target;
  if (regtarget && typeof regtarget !== 'function') {
    regtarget = regtarget.constructor;
  }

  if (!origFunc) {
    origFunc = Object.getOwnPropertyDescriptor(target, funcName)!.value as UntypedAsyncFunction;
  }

  let mParameters: MethodParameter[] | undefined = methodArgsByFunction.get(origFunc);

  if (mParameters === undefined) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    let designParamTypes: Function[] | undefined = undefined;
    if (target) {
      function getDesignType(target: object, key: string | symbol) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
        const getMetadata = (Reflect as any)?.getMetadata as
          | ((k: unknown, t: unknown, p?: unknown) => unknown)
          | undefined;

        if (!getMetadata) return undefined; // polyfill not present
        return getMetadata('design:paramtypes', target, key); // safe to use
      }
      designParamTypes = getDesignType(target, funcName as string | symbol) as Function[] | undefined; // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    }
    if (designParamTypes) {
      mParameters = designParamTypes.map((value, index) => new MethodParameter(index, value));
    } else {
      if (origFunc) {
        const argnames = getArgNames(origFunc);
        mParameters = argnames.map((_value, index) => new MethodParameter(index));
      } else {
        const descriptor = Object.getOwnPropertyDescriptor(target, funcName);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
        const argnames = getArgNames(descriptor?.value as Function);
        mParameters = argnames.map((_value, index) => new MethodParameter(index));
      }
    }

    methodArgsByFunction.set(origFunc, mParameters);
  }

  return mParameters;
}

function getOrCreateMethodRegistration<This, Args extends unknown[], Return>(
  target: object | undefined,
  className: string | undefined,
  propertyKey: PropertyKey,
  func: (this: This, ...args: Args) => Promise<Return>,
) {
  let classReg;
  let isInstance;
  ({ classReg, isInstance, className } = getOrCreateClassRegistrationByName(target, className));

  const fname = propertyKey.toString();

  const origFunc = functionToRegistration.get(func)?.origFunction ?? func;

  if (!classReg.allRegisteredOperations.has(origFunc)) {
    const reg = new MethodRegistration<This, Args, Return>(func, isInstance);
    classReg.allRegisteredOperations.set(func, reg);
  }
  const methReg: MethodRegistration<This, Args, Return> = classReg.allRegisteredOperations.get(
    func,
  )! as MethodRegistration<This, Args, Return>;

  if (methReg.needInitialized) {
    methReg.needInitialized = false;
    methReg.name = fname;
    methReg.className = classReg.name;
    methReg.defaults = classReg;

    methReg.args = getOrCreateMethodArgsRegistration(target, propertyKey, func as UntypedAsyncFunction);

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

    functionToRegistration.set(methReg.registeredFunction, methReg as MethodRegistration<unknown, unknown[], unknown>);
    functionToRegistration.set(methReg.origFunction, methReg as MethodRegistration<unknown, unknown[], unknown>);
  }

  return methReg;
}

export function wrapDBOSFunctionAndRegisterByUniqueNameDec<This, Args extends unknown[], Return>(
  target: object,
  propertyKey: PropertyKey,
  descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
) {
  if (!descriptor.value) {
    throw Error('Use of decorator when original method is undefined');
  }

  const registration = wrapDBOSFunctionAndRegisterByUniqueName(target, undefined, propertyKey, descriptor.value);

  descriptor.value = registration.wrappedFunction ?? registration.registeredFunction;

  return { descriptor, registration };
}

export function wrapDBOSFunctionAndRegisterByUniqueName<This, Args extends unknown[], Return>(
  ctorOrProto: object | undefined,
  className: string | undefined,
  propertyKey: PropertyKey,
  func: (this: This, ...args: Args) => Promise<Return>,
) {
  ensureDBOSIsNotLaunched();

  const name = typeof propertyKey === 'string' ? propertyKey : propertyKey.toString();
  const freg = getFunctionRegistration(func) as MethodRegistration<This, Args, Return>;
  if (freg) {
    const r = getOrCreateClassRegistrationByName(ctorOrProto, className);
    r.classReg.registerOperationByName(name, freg);
    return freg;
  }

  const registration = getOrCreateMethodRegistration(ctorOrProto, className, propertyKey, func);
  const r = getOrCreateClassRegistrationByName(ctorOrProto, className);
  r.classReg.registerOperationByName(name, registration);

  return registration;
}

export function wrapDBOSFunctionAndRegisterDec<This, Args extends unknown[], Return>(
  target: object,
  propertyKey: PropertyKey,
  descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
) {
  if (!descriptor.value) {
    throw Error('Use of decorator when original method is undefined');
  }

  const registration = wrapDBOSFunctionAndRegister(target, undefined, propertyKey, descriptor.value);

  descriptor.value = registration.wrappedFunction ?? registration.registeredFunction;

  return { descriptor, registration };
}

export function wrapDBOSFunctionAndRegister<This, Args extends unknown[], Return>(
  ctorOrProto: object | undefined,
  className: string | undefined,
  propertyKey: PropertyKey,
  func: (this: This, ...args: Args) => Promise<Return>,
) {
  ensureDBOSIsNotLaunched();

  const freg = getFunctionRegistration(func) as MethodRegistration<This, Args, Return>;
  if (freg) {
    return freg;
  }

  const registration = getOrCreateMethodRegistration(ctorOrProto, className, propertyKey, func);

  return registration;
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

export function getAllRegisteredFunctions() {
  const s: Set<MethodRegistrationBase> = new Set();
  const fregs: MethodRegistrationBase[] = [];
  for (const [_f, reg] of functionToRegistration) {
    if (s.has(reg)) continue;
    fregs.push(reg);
    s.add(reg);
  }
  return fregs;
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

function getOrCreateClassRegistrationByName(target: object | undefined, className: string | undefined) {
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
  return { classReg, isInstance, className };
}

export function getConfiguredInstance(clsname: string, cfgname: string): ConfiguredInstance | null {
  const classReg = classesByName.get(clsname)?.reg;
  if (!classReg) return null;
  return classReg.configuredInstances.get(cfgname) ?? null;
}

// #endregion

// #region Transactional data source registration

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

// #endregion

// #region External (event receiver v3)

export function associateClassWithExternal(
  external: AnyConstructor | object | string,
  cls: AnyConstructor | string,
): object {
  const clsn: string = typeof cls === 'string' ? cls : getNameForClass(cls);
  const clsreg = getClassRegistrationByName(clsn, true);
  return clsreg.getRegisteredInfo(external);
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
  const registration = wrapDBOSFunctionAndRegister(target, className, funcName, func);
  if (!registration.externalRegInfo.has(external)) {
    registration.externalRegInfo.set(external, {});
  }
  return { registration, regInfo: registration.externalRegInfo.get(external)! };
}

/*
 * Associates a DBOS function or method parameters with an external class or object.
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
  const registration = wrapDBOSFunctionAndRegister(target, className, funcName, func);
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

export interface ExternalRegistration {
  classConfig?: unknown;
  methodConfig?: unknown;
  paramConfig: {
    name: string;
    index: number;
    paramConfig?: object;
  }[];
  methodReg: MethodRegistrationBase;
}

export function getRegistrationsForExternal(
  external: AnyConstructor | object | string,
  cls?: object | string,
  funcName?: string,
): readonly ExternalRegistration[] {
  const res = new Array<ExternalRegistration>();

  if (cls) {
    const clsname = typeof cls === 'string' ? cls : getNameForClass(cls);
    const c = classesByName.get(clsname);
    if (c) {
      if (funcName) {
        const f = c.reg.registeredOperationsByName.get(funcName);
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
    for (const f of c.reg.allRegisteredOperations.values()) {
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

// #endregion

// #region Parameter decorators

export function ArgName(name: string) {
  return function (target: object, propertyKey: PropertyKey, parameterIndex: number) {
    const existingParameters = getOrCreateMethodArgsRegistration(target, propertyKey);

    const curParam = existingParameters[parameterIndex];
    curParam.name = name;
  };
}

// #endregion
