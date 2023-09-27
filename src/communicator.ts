import { Span } from "@opentelemetry/sdk-trace-base";
import { Logger } from "./telemetry";
import { WorkflowContext } from "./workflow";
import { OperonContext } from "./context";

/* eslint-disable @typescript-eslint/no-explicit-any */
export type OperonCommunicator<T extends any[], R> = (ctxt: CommunicatorContext, ...args: T) => Promise<R>;

export interface CommunicatorConfig {
  retriesAllowed?: boolean; // Should failures be retried? (default true)
  intervalSeconds?: number; // Seconds to wait before the first retry attempt (default 1).
  maxAttempts?: number; // Maximum number of retry attempts (default 3). If the error occurs more times than this, return null.
  backoffRate?: number; // The multiplier by which the retry interval increases after every retry attempt (default 2).
}

export class CommunicatorContext extends OperonContext
{
  readonly functionID: number;
  readonly retriesAllowed: boolean;
  readonly intervalSeconds: number;
  readonly maxAttempts: number;
  readonly backoffRate: number;
  private readonly logger: Logger;

  // TODO: Validate the parameters.
  constructor(workflowContext: WorkflowContext, functionID: number, logger: Logger, span: Span, params: CommunicatorConfig, commName: string) {
    super(commName, span, workflowContext);
    this.functionID = functionID;
    this.logger = logger;
    this.retriesAllowed = params.retriesAllowed ?? true;
    this.intervalSeconds = params.intervalSeconds ?? 1;
    this.maxAttempts = params.maxAttempts ?? 3;
    this.backoffRate = params.backoffRate ?? 2;
    if (workflowContext.applicationConfig) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
       this.applicationConfig = workflowContext.applicationConfig;
    }
  }

  log(severity: string, message: string): void {
    this.logger.log(this, severity, message);
  }
}
