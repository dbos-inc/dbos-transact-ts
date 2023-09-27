import { Span } from "@opentelemetry/sdk-trace-base";
import { IncomingMessage } from 'http';

export class OperonContext {
  request?: IncomingMessage;  // Raw incoming HTTP request.

  authenticatedUser: string = ''; ///< The user that has been authenticated
  authenticatedRoles: string[] = []; ///< All roles the user has according to authentication
  assumedRole: string = ''; ///< Role in use - that user has and provided authorization to current function

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  applicationConfig?: any; // applicationConfiguration

  workflowUUID: string = '';

  constructor(readonly operationName: string, readonly span: Span, parentCtx ?: OperonContext) {
    if (parentCtx) {
      this.request = parentCtx.request;
      this.authenticatedUser = parentCtx.authenticatedUser;
      this.authenticatedRoles = parentCtx.authenticatedRoles;
      this.assumedRole = parentCtx.assumedRole;
      this.workflowUUID = parentCtx.workflowUUID;
    }
  }
}
