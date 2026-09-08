import { getCurrentContextStore } from './context';
import { DBOS } from './dbos';
import {
  ClassAuthDefaults,
  DBOS_AUTH,
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
        if (getCurrentContextStore()) {
          getCurrentContextStore()!.assumedRole = role;
        }
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

function installAuthMiddleware(methReg: MethodRegistrationBase): void {
  const classAuth = methReg?.defaults?.getRegisteredInfo(DBOS_AUTH) as ClassAuthDefaults;
  const methodAuth = methReg?.getRegisteredInfo(DBOS_AUTH) as MethodAuth;

  const shouldCheck = classAuth?.requiredRole !== undefined || methodAuth?.requiredRole !== undefined;

  if (shouldCheck) {
    methReg.addEntryInterceptor(checkMethodAuth, 10);
  }
}

export function registerAuthChecker() {
  registerMiddlewareInstaller(installAuthMiddleware);
}
