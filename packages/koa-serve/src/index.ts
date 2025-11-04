import { DBOSKoa } from './dboskoa';

export {
  ArgRequiredOptions,
  ArgSources,
  DBOSHTTP,
  DBOSHTTPArgInfo,
  DBOSHTTPAuthReturn,
  DBOSHTTPBase,
  DBOSHTTPMethodInfo,
  DBOSHTTPReg,
  DBOSHTTPRequest,
  DBOSResponseError,
  LogMask,
  LogMasks,
  RequestIDHeader,
  SkipLogging,
  WorkflowIDHeader,
} from './dboshttp';

export { DBOSKoa, DBOSKoaAuthContext, DBOSKoaClassReg, DBOSKoaAuthMiddleware, DBOSKoaConfig } from './dboskoa';

// Export these as unbound functions.  We know this is safe,
//  and it more closely matches the existing library syntax.
// (Using the static function as a decorator, for some reason,
//  is erroneously getting considered as unbound by some lint versions,
//  as there are no parens following it?)
export const DefaultArgOptional = DBOSKoa.defaultArgOptional;
export const DefaultArgRequired = DBOSKoa.defaultArgRequired;
export const DefaultArgValidate = DBOSKoa.defaultArgValidate;
export const ArgDate = DBOSKoa.argDate;
export const ArgOptional = DBOSKoa.argOptional;
export const ArgRequired = DBOSKoa.argRequired;
export const ArgSource = DBOSKoa.argSource;
export const ArgVarchar = DBOSKoa.argVarchar;
export { ArgName } from '@dbos-inc/dbos-sdk';
