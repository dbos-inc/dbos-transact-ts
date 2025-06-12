import { DBOSContext, DBOSContextImpl, getCurrentDBOSContext } from './context';
import { DBOS } from './dbos';
import {
  associateClassWithExternal,
  associateMethodWithExternal,
  ClassAuthDefaults,
  DBOS_AUTH,
  DBOSMethodMiddlewareInstaller,
  MethodAuth,
  MethodRegistrationBase,
  registerMiddlewareInstaller,
} from './decorators';
import { DBOSNotAuthorizedError } from './error';

function checkMethodAuth(methReg: MethodRegistrationBase, args: unknown[]) {
  // Validate the user authentication and populate the role field
  const requiredRoles = methReg.getRequiredRoles();
  if (requiredRoles.length > 0) {
    DBOS.span?.setAttribute('requiredRoles', requiredRoles);
    const curRoles = DBOS.authenticatedRoles;
    let authorized = false;
    const set = new Set(curRoles);
    for (const role of requiredRoles) {
      if (set.has(role)) {
        authorized = true;
        (getCurrentDBOSContext() as DBOSContextImpl).assumedRole = role;
        break;
      }
    }
    if (!authorized) {
      const err = new DBOSNotAuthorizedError(`User does not have a role with permission to call ${methReg.name}`, 403);
      DBOS.span?.addEvent('DBOSNotAuthorizedError', { message: err.message });
      throw err;
    }
  }

  return args;
}

class AuthChecker implements DBOSMethodMiddlewareInstaller {
  installMiddleware(methReg: MethodRegistrationBase): void {
    const classAuth = methReg?.defaults?.getRegisteredInfo(DBOS_AUTH) as ClassAuthDefaults;
    const methodAuth = methReg?.getRegisteredInfo(DBOS_AUTH) as MethodAuth;

    const shouldCheck = classAuth?.requiredRole !== undefined || methodAuth?.requiredRole !== undefined;

    if (shouldCheck) {
      methReg.addEntryInterceptor(checkMethodAuth, 10);
    }
  }
}

const authChecker = new AuthChecker();

export function registerAuthChecker() {
  registerMiddlewareInstaller(authChecker);
}

/** @deprecated Use `DBOS.defaultRequiredRole` */
export function DefaultRequiredRole(anyOf: string[]) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function clsdec<T extends { new (...args: any[]): object }>(ctor: T) {
    const clsreg = associateClassWithExternal(DBOS_AUTH, ctor) as ClassAuthDefaults;
    clsreg.requiredRole = anyOf;
    registerAuthChecker();
  }
  return clsdec;
}

/**
 * @deprecated - use `DBOS.requiredRole`
 */
export function RequiredRole(anyOf: string[]) {
  function apidec<This, Ctx extends DBOSContext, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: Args) => Promise<Return>>,
  ) {
    const rr = associateMethodWithExternal(DBOS_AUTH, target, undefined, propertyKey.toString(), inDescriptor.value!);

    (rr.regInfo as MethodAuth).requiredRole = anyOf;

    inDescriptor.value = rr.registration.wrappedFunction ?? rr.registration.registeredFunction;

    registerAuthChecker();

    return inDescriptor;
  }
  return apidec;
}
