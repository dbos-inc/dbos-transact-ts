//import { Span } from "@opentelemetry/sdk-trace-base";
import { IncomingMessage } from 'http';

export class OperonContext {
  request?: IncomingMessage;  // Raw incoming HTTP request.

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
    this.request = other.request;
    this.authRoles = other.authRoles;
    this.authRole = other.authRole;
    this.authUser = other.authUser;
  }
}