//import { Span } from "@opentelemetry/sdk-trace-base";
import { IncomingMessage } from 'http';

export class OperonContext {
  request?: IncomingMessage;  // Raw incoming HTTP request.

  authenticatedUser: string = ''; ///< The user that has been authenticated
  authenticatedRoles: string[] = []; ///< All roles the user has according to authentication
  assumedRole: string = ''; ///< Role in use - that user has and provided authorization to current function

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  applicationConfig?: any; // applicationConfiguration

  //readonly span: Span;

  constructor(args ?: {parentCtx?:OperonContext}) {
    if (args && args.parentCtx) {
      this.copyBaseFields(args.parentCtx);
    }
  }

  copyBaseFields(other: OperonContext) {
    this.request = other.request;
    this.authenticatedUser = other.authenticatedUser;
    this.authenticatedRoles = other.authenticatedRoles;
    this.assumedRole = other.assumedRole;
  }
}
