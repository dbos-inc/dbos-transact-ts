//import { Span } from "@opentelemetry/sdk-trace-base";

export class OperonContext {
  rawContext: unknown;  // Raw context from HTTP server. For example, Koa.Context.
  request: unknown;
  response: unknown;

  authUser: string = '';
  authRole: string = ''; // Role in use
  authRoles: string[] = []; // All roles the user has

  //readonly span: Span;

  // TODO: Validate the parameters.
  constructor(args ?: {parentCtx?:OperonContext}) {
    if (args && args.parentCtx) {
      this.copyBaseFields(args.parentCtx);
    }
  }

  copyBaseFields(other: OperonContext) {
    this.rawContext = other.rawContext;
    this.request = other.request;
    this.response = other.response;
    this.authRoles = other.authRoles;
    this.authRole = other.authRole;
    this.authUser = other.authUser;
  }
}