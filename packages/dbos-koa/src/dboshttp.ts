import { IncomingHttpHeaders } from 'http';
import { randomUUID } from 'node:crypto';

import {
  DBOS,
  DBOSLifecycleCallback,
  Error as DBOSErrors,
  MethodParameter,
  requestArgValidation,
} from '@dbos-inc/dbos-sdk';

export enum APITypes {
  GET = 'GET',
  POST = 'POST',
  PUT = 'PUT',
  PATCH = 'PATCH',
  DELETE = 'DELETE',
}

export enum ArgSources {
  AUTO = 'AUTO', // Look both places
  DEFAULT = 'DEFAULT', // Look in the standard place for the method
  BODY = 'BODY', // Look in body only
  QUERY = 'QUERY', // Look in query string only
}

export interface DBOSHTTPAuthReturn {
  authenticatedUser: string;
  authenticatedRoles: string[];
}

export interface DBOSHTTPReg {
  apiURL: string;
  apiType: APITypes;
}

export interface DBOSHTTPMethodInfo {
  registrations?: DBOSHTTPReg[];
}

export interface DBOSHTTPArgInfo {
  argSource?: ArgSources;
}

export const DBOSHTTP = 'dboshttp';

export const WorkflowIDHeader = 'dbos-idempotency-key';

export const RequestIDHeader = 'X-Request-ID';
export function getOrGenerateRequestID(headers: IncomingHttpHeaders): string {
  const reqID = headers[RequestIDHeader.toLowerCase()] as string | undefined; // RequestIDHeader is expected to be a single value, so we dismiss the possible string[] returned type.
  if (reqID) {
    return reqID;
  }
  const newID = randomUUID();
  headers[RequestIDHeader.toLowerCase()] = newID; // This does not carry through the response
  return newID;
}

export function isClientRequestError(e: Error) {
  return DBOSErrors.isDataValidationError(e);
}

export interface DBOSHTTPConfig {
  corsMiddleware?: boolean;
  credentials?: boolean;
  allowedOrigins?: string[];
}

export class DBOSHTTPBase extends DBOSLifecycleCallback {
  static HTTP_OPERATION_TYPE: string = 'http';

  httpApiDec(verb: APITypes, url: string) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const er = this;
    return function apidec<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const { registration, regInfo } = DBOS.associateFunctionWithInfo(er, descriptor.value!, {
        classOrInst: target,
        name: propertyKey,
      });
      const handlerRegistration = regInfo as DBOSHTTPMethodInfo;
      if (!handlerRegistration.registrations) handlerRegistration.registrations = [];
      handlerRegistration.registrations.push({
        apiURL: url,
        apiType: verb,
      });
      requestArgValidation(registration);

      return descriptor;
    };
  }

  /** Decorator indicating that the method is the target of HTTP GET operations for `url` */
  getApi(url: string) {
    return this.httpApiDec(APITypes.GET, url);
  }

  /** Decorator indicating that the method is the target of HTTP POST operations for `url` */
  postApi(url: string) {
    return this.httpApiDec(APITypes.POST, url);
  }

  /** Decorator indicating that the method is the target of HTTP PUT operations for `url` */
  putApi(url: string) {
    return this.httpApiDec(APITypes.PUT, url);
  }

  /** Decorator indicating that the method is the target of HTTP PATCH operations for `url` */
  patchApi(url: string) {
    return this.httpApiDec(APITypes.PATCH, url);
  }

  /** Decorator indicating that the method is the target of HTTP DELETE operations for `url` */
  deleteApi(url: string) {
    return this.httpApiDec(APITypes.DELETE, url);
  }

  /** Parameter decorator indicating which source to use (URL, BODY, etc) for arg data */
  argSource(source: ArgSources) {
    return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
      const curParam = DBOS.associateParamWithInfo(
        DBOSHTTP,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        Object.getOwnPropertyDescriptor(target, propertyKey)!.value,
        {
          classOrInst: target,
          name: propertyKey.toString(),
          param: parameterIndex,
        },
      ) as DBOSHTTPArgInfo;

      curParam.argSource = source;
    };
  }

  getArgSource(arg: MethodParameter) {
    const arginfo = arg.getRegisteredInfo(DBOSHTTP) as DBOSHTTPArgInfo;
    return arginfo?.argSource ?? ArgSources.AUTO;
  }

  override logRegisteredEndpoints(): void {
    DBOS.logger.info('HTTP endpoints supported:');
    const eps = DBOS.getAssociatedInfo(this);

    for (const e of eps) {
      const { methodConfig, methodReg } = e;
      const httpmethod = methodConfig as DBOSHTTPMethodInfo;
      for (const ro of httpmethod.registrations ?? []) {
        if (ro.apiURL) {
          DBOS.logger.info('    ' + ro.apiType.padEnd(6) + '  :  ' + ro.apiURL);
          const roles = methodReg.getRequiredRoles();
          if (roles.length > 0) {
            DBOS.logger.info('        Required Roles: ' + JSON.stringify(roles));
          }
        }
      }
    }
  }
}
