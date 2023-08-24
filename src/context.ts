//import { Span } from "@opentelemetry/sdk-trace-base";

export class OperonContext {
  request: unknown;
  response: unknown;

  authUser: string = '';
  authRoles: string[] = [];

  //readonly span: Span;

  // TODO: Validate the parameters.
  constructor(args ?: {parentCtx?:OperonContext}) {
    if (args && args.parentCtx) {
      this.copyBaseFields(args.parentCtx);
    }
  }

  copyBaseFields(other: OperonContext) {
    this.request = other.request;
    this.response = other.response;
    this.authRoles = other.authRoles;
    this.authUser = other.authUser;
  }
}