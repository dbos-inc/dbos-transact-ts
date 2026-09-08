import { WorkflowConfig } from './workflow';
import { StepConfig, validateStepConfig } from './step';
import { DBOSConflictingRegistrationError, DBOSNotRegisteredError } from './error';
import { DataSourceTransactionHandler } from './datasource';

// #region Alert handler

/**
 * Alert handler function signature for receiving alerts from DBOS Conductor.
 * @param name - Name/type of the alert
 * @param message - Alert message content
 * @param metadata - Additional key-value metadata
 */
export type AlertHandler = (name: string, message: string, metadata: Record<string, string>) => Promise<void>;

// Module-level alert handler storage (internal use only)
let alertHandler: AlertHandler | undefined = undefined;

/** @internal */
export function getAlertHandler(): AlertHandler | undefined {
  return alertHandler;
}

/** @internal */
export function setAlertHandler(handler: AlertHandler): void {
  alertHandler = handler;
}

// #endregion

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
  classReg: ClassRegistration;

  // Interceptors
  onEnter: { seqNum: number; func: (reg: MethodRegistrationBase, args: unknown[]) => unknown[] }[] = [];
  addEntryInterceptor(func: (reg: MethodRegistrationBase, args: unknown[]) => unknown[], seqNum: number = 10) {
    this.onEnter.push({ seqNum, func });
    this.onEnter.sort((a, b) => a.seqNum - b.seqNum);
  }

  constructor(
    classReg: ClassRegistration,
    origFunc: (this: This, ...args: Args) => Promise<Return>,
    isInstance: boolean,
  ) {
    this.classReg = classReg;
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

  getClassName() {
    return this.className || this.classReg.getClassName();
  }

  checkFuncTypeUnassigned(newType: string) {
    const oldType = this.getAssignedType();
    let error: string | undefined = undefined;
    if (oldType && newType !== oldType) {
      error = `Operation (Name: ${this.getClassName()}.${this.name}) is already registered with a conflicting function type: ${oldType} vs. ${newType}`;
    } else if (oldType) {
      error = `Operation (Name: ${this.getClassName()}.${this.name}) is already registered.`;
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
    validateStepConfig(stepCfg, `${this.getClassName()}.${this.name}`);
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

  ctor: AnyConstructor | undefined;

  constructor(ctor: AnyConstructor | undefined) {
    this.ctor = ctor;
  }

  getClassName() {
    return this.name || this.ctor?.name || '';
  }

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

export function clearAllRegistrations() {
  lifecycleListeners.length = 0;
  installedMiddleware = false;
  middlewareInstallers.length = 0;
  functionToRegistration.clear();
  classesByName.clear();
  classesByCtor.clear();
  transactionalDataSources.clear();
  alertHandler = undefined;
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
type MiddlewareInstaller = (methodReg: MethodRegistrationBase) => void;
let installedMiddleware = false;
const middlewareInstallers: MiddlewareInstaller[] = [];

export function registerMiddlewareInstaller(i: MiddlewareInstaller) {
  if (installedMiddleware) throw new TypeError('Attempt to provide method middleware after insertion was performed');
  if (!middlewareInstallers.includes(i)) middlewareInstallers.push(i);
}

export function insertAllMiddleware() {
  if (installedMiddleware) return;
  installedMiddleware = true;

  const regs = getAllClassRegistrations();

  for (const c of regs) {
    for (const f of c.allRegisteredOperations.values()) {
      for (const i of middlewareInstallers) {
        i(f);
      }
    }
  }
}

// Registration of functions, and classes
const functionToRegistration: Map<unknown, MethodRegistration<unknown, unknown[], unknown>> = new Map();

// Registration of instance, by constructor+name
function registerClassInstance(inst: ConfiguredInstance, instname: string) {
  const creg = getOrCreateClassRegistrationByTarget(inst.constructor as AnyConstructor);
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
    className = fr.getClassName();
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
    const classReg = getClassRegistration(target, false);
    classReg.reg?.reg?.allRegisteredOperations?.forEach((m) => registeredOperations.push(m));
  } else {
    let current: object | undefined = target;
    while (current) {
      // Walk prototype chain
      registeredOperations.push(...getRegisteredOperations(current.constructor));
      current = Object.getPrototypeOf(current) as object | undefined;
    }
  }

  return registeredOperations;
}

function getOrCreateMethodRegistration<This, Args extends unknown[], Return>(
  target: object | undefined,
  className: string | undefined,
  propertyKey: PropertyKey,
  name: string | undefined,
  func: (this: This, ...args: Args) => Promise<Return>,
) {
  const { classReg, isInstance } = getOrCreateClassRegistration(target, className);

  const fname = name ?? propertyKey.toString();

  const origFunc = functionToRegistration.get(func)?.origFunction ?? func;

  if (!classReg.allRegisteredOperations.has(origFunc)) {
    const reg = new MethodRegistration<This, Args, Return>(classReg, func, isInstance);
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

export function wrapDBOSFunctionAndRegisterByTarget<This, Args extends unknown[], Return>(
  target: object,
  propertyKey: PropertyKey,
  name: string | undefined,
  descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
) {
  if (!descriptor.value) {
    throw Error('Use of decorator when original method is undefined');
  }

  const registration = wrapDBOSFunctionAndRegisterByUniqueName(target, undefined, propertyKey, name, descriptor.value);

  descriptor.value = registration.wrappedFunction ?? registration.registeredFunction;

  return { descriptor, registration };
}

export function wrapDBOSFunctionAndRegisterByUniqueName<This, Args extends unknown[], Return>(
  ctorOrProto: object | undefined,
  className: string | undefined,
  propertyKey: PropertyKey,
  name: string | undefined,
  func: (this: This, ...args: Args) => Promise<Return>,
) {
  ensureDBOSIsNotLaunched();

  if (!name) {
    name = typeof propertyKey === 'string' ? propertyKey : propertyKey.toString();
  }

  const freg = getFunctionRegistration(func) as MethodRegistration<This, Args, Return>;
  if (freg) {
    const r = getOrCreateClassRegistration(ctorOrProto, className);
    r.classReg.registerOperationByName(name, freg);
    return freg;
  }

  const registration = getOrCreateMethodRegistration(ctorOrProto, className, propertyKey, name, func);
  const r = getOrCreateClassRegistration(ctorOrProto, className);
  r.classReg.registerOperationByName(name, registration);

  return registration;
}

export function wrapDBOSFunctionAndRegisterDec<This, Args extends unknown[], Return>(
  target: object,
  propertyKey: PropertyKey,
  name: string | undefined,
  descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
) {
  if (!descriptor.value) {
    throw Error('Use of decorator when original method is undefined');
  }

  const registration = wrapDBOSFunctionAndRegister(target, undefined, propertyKey, name, descriptor.value);

  descriptor.value = registration.wrappedFunction ?? registration.registeredFunction;

  return { descriptor, registration };
}

export function wrapDBOSFunctionAndRegister<This, Args extends unknown[], Return>(
  ctorOrProto: object | undefined,
  className: string | undefined,
  propertyKey: PropertyKey,
  name: string | undefined,
  func: (this: This, ...args: Args) => Promise<Return>,
) {
  ensureDBOSIsNotLaunched();

  const freg = getFunctionRegistration(func) as MethodRegistration<This, Args, Return>;
  if (freg) {
    return freg;
  }

  const registration = getOrCreateMethodRegistration(ctorOrProto, className, propertyKey, name, func);

  return registration;
}

// Data structure notes:
//  Everything is registered under a "className", but this may be blank.
//   This often corresponds to a real `class`, but it does not have to, if the user
//   registered stuff without decorators.  Or for a bare function.
//  Thus, if you have a "class name", look it up in classesByName, as this is exhaustive
//  If you have a class or instance, you can look that up in the classesByCtor map,
//   this contains all decorator-registered classes, but may omit other things.
//   You should use this map if you can, since the names for these may be aliased.
//
// The support for aliasing classes in the decorator registration scheme is a bit tricky,
//  since the class decorator runs after the method decorators.  To get this to work:
//  1. We put the methods into the class by using the ctor
//  2. We complete the name->class registration later
type AnyConstructor = new (...args: unknown[]) => object;
const classesByName: Map<string, { reg: ClassRegistration; ctor?: AnyConstructor; regloc: string[] }> = new Map();
const classesByCtor: Map<AnyConstructor, { name: string; reg: ClassRegistration; regloc: string[] }> = new Map();

export function getNameForClass(ctor: object): string {
  const reg = getClassRegistration(ctor, false);
  return reg.reg?.name || reg.regTarget.name;
}

function getAllClassRegistrations() {
  const seen: Set<ClassRegistration> = new Set();
  for (const [_cn, c] of classesByName) {
    seen.add(c.reg);
  }
  for (const [_c, c] of classesByCtor) {
    seen.add(c.reg);
  }
  return seen;
}

export function getClassRegistration(target: object, create: boolean) {
  let regTarget: AnyConstructor;
  if (typeof target === 'function') {
    // Static method case
    regTarget = target as AnyConstructor;
  } else {
    // Instance method case
    regTarget = target.constructor as AnyConstructor;
  }

  if (classesByCtor.has(regTarget)) return { regTarget, reg: classesByCtor.get(regTarget)! };
  if (!create) return { regTarget };
  classesByCtor.set(regTarget, {
    reg: new ClassRegistration(regTarget),
    name: regTarget.name,
    regloc: new StackGrabber().getCleanStack(1) ?? [],
  });
  return { regTarget, reg: classesByCtor.get(regTarget)! };
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
    classesByName.set(name, {
      reg: new ClassRegistration(undefined),
      regloc: new StackGrabber().getCleanStack(1) ?? [],
    });
  }

  const clsReg: ClassRegistration = classesByName.get(name)!.reg;

  if (clsReg.needsInitialized) {
    clsReg.name = name;
    clsReg.needsInitialized = false;
  }
  return clsReg;
}

export function getOrCreateClassRegistrationByTarget<CT extends { new (...args: unknown[]): object }>(ctor: CT) {
  const existing = getClassRegistration(ctor, true);
  const reg = existing.reg!.reg;
  // This registration will need initialized... that happens later
  return reg;
}

function getOrCreateClassRegistration(target: object | undefined, className: string | undefined) {
  if (!target && className === undefined) {
    className = '';
  }

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

  // If we have no class name, this might get assigned later.  Put in placeholder reg
  if (className === undefined) {
    const reg = getClassRegistration(regtarget!, true);
    return { classReg: reg.reg!.reg, isInstance, className };
  }

  // If we have no regtarget, this is plain function registration
  if (!regtarget) {
    return { classReg: getClassRegistrationByName(className, true), isInstance: false, className };
  }

  // We have a regtarget and a name ... assign the name
  const reg = getClassRegistration(regtarget, true);
  if (reg.reg!.name && reg.reg!.name !== className) {
    throw new TypeError(`Attempt to register class under two names: ${reg.reg!.name} vs. ${className}`);
  }
  reg.reg!.name = className;
  return { classReg: reg.reg!.reg, isInstance, className };
}

export function getConfiguredInstance(clsname: string, cfgname: string): ConfiguredInstance | null {
  const classReg = getClassRegistrationByName(clsname);
  if (!classReg) return null;
  return classReg.configuredInstances.get(cfgname) ?? null;
}

export function finalizeClassRegistrations() {
  function setName(reg: ClassRegistration, cname: string) {
    reg.name = cname;
    reg.needsInitialized = false;
    for (const [_fn, f] of reg.registeredOperationsByName) {
      f.className = cname;
    }
  }

  for (const [cls, reg] of classesByCtor) {
    const cname = reg.name || reg.reg.name || getNameForClass(cls);
    const ereg = classesByName.get(cname);
    if (!ereg) {
      classesByName.set(cname, { reg: reg.reg, ctor: cls, regloc: reg.regloc });
      reg.name = cname;
      setName(reg.reg, cname);
      continue;
    }
    if (ereg.ctor && ereg.ctor !== cls) {
      throw new DBOSConflictingRegistrationError(
        `Class ${cname}(${cls.name}) has been given a name that conflicts with another class ${ereg.ctor?.name}.`,
      );
    }
    if (ereg.reg !== reg.reg) {
      throw new DBOSConflictingRegistrationError(
        `Class: ${cname}(${cls.name}) has been given a name that was registered directly by name without a class.`,
      );
    }
    classesByName.set(cname, { reg: reg.reg, ctor: cls, regloc: reg.regloc });
    reg.name = cname;
    setName(reg.reg, cname);
  }
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
  let clsreg: ClassRegistration | undefined = undefined;
  if (typeof cls === 'string') {
    clsreg = getClassRegistrationByName(cls, true);
  } else {
    clsreg = getClassRegistration(cls, true).reg!.reg;
  }
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
  const registration = wrapDBOSFunctionAndRegister(target, className, funcName, funcName, func);
  if (!registration.externalRegInfo.has(external)) {
    registration.externalRegInfo.set(external, {});
  }
  return { registration, regInfo: registration.externalRegInfo.get(external)! };
}

export interface ExternalRegistration {
  classConfig?: unknown;
  methodConfig?: unknown;
  methodReg: MethodRegistrationBase;
}

export function getRegistrationsForExternal(
  external: AnyConstructor | object | string,
  cls?: object | string,
  funcName?: string,
): readonly ExternalRegistration[] {
  const res = new Array<ExternalRegistration>();

  if (cls) {
    let reg: ClassRegistration | undefined = undefined;
    if (typeof cls === 'string') {
      reg = classesByName.get(cls)?.reg;
    } else if (typeof cls === 'function') {
      reg = classesByCtor.get(cls as AnyConstructor)?.reg;
    } else if (cls !== undefined && typeof cls === 'object') {
      reg = classesByCtor.get(cls.constructor as AnyConstructor)?.reg;
    }

    if (reg) {
      if (funcName) {
        const f = reg.registeredOperationsByName.get(funcName);
        if (f) {
          collectRegForFunction(f);
        }
      } else {
        collectRegForClass(reg);
      }
    }
  } else {
    const seen = getAllClassRegistrations();
    for (const c of seen) {
      collectRegForClass(c);
    }
  }
  return res;

  function collectRegForClass(reg: ClassRegistration) {
    for (const f of reg.allRegisteredOperations.values()) {
      collectRegForFunction(f);
    }
  }

  function collectRegForFunction(f: MethodRegistrationBase) {
    const methodConfig = f.externalRegInfo.get(external);
    const classConfig = f.defaults?.externalRegInfo.get(external);
    if (!methodConfig && !classConfig) return;
    res.push({ methodReg: f, methodConfig, classConfig: classConfig ?? {} });
  }
}

// #endregion
