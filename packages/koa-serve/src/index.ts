import { DBOSKoa } from './dboskoa';

export {
  ArgSources,
  DBOSHTTP,
  DBOSHTTPArgInfo,
  DBOSHTTPAuthReturn,
  DBOSHTTPBase,
  DBOSHTTPMethodInfo,
  DBOSHTTPReg,
  DBOSHTTPRequest,
  RequestIDHeader,
  WorkflowIDHeader,
} from './dboshttp';

export { DBOSKoa, DBOSKoaAuthContext, DBOSKoaClassReg, DBOSKoaAuthMiddleware, DBOSKoaConfig } from './dboskoa';

// Export these as unbound functions.  We know this is safe,
//  and it more closely matches the existing library syntax.
// (Using the static function as a decorator, for some reason,
//  is erroneously getting considered as unbound by some lint versions,
//  as there are no parens following it?)
export const ArgSource = DBOSKoa.argSource;
