import { Span } from "@opentelemetry/sdk-trace-base";
import { GlobalLogger as Logger } from "./telemetry/logs";
import { WorkflowContextImpl } from "./workflow";
import { DBOSContext, DBOSContextImpl } from "./context";
import { WorkflowContextDebug } from "./debugger/debug_workflow";
import { DBOSError } from "./error";
import { ConfiguredClass, InitConfigMethod } from "./decorators";

export type Communicator<T extends unknown[], R> = (ctxt: CommunicatorContext, ...args: T) => Promise<R>;

export interface CommunicatorConfig {
  retriesAllowed?: boolean; // Should failures be retried? (default true)
  intervalSeconds?: number; // Seconds to wait before the first retry attempt (default 1).
  maxAttempts?: number; // Maximum number of retry attempts (default 3). If errors occur more times than this, throw an exception.
  backoffRate?: number; // The multiplier by which the retry interval increases after every retry attempt (default 2).
}

export interface CommunicatorContext extends DBOSContext {
  // These fields reflect the communictor's configuration.
  readonly retriesAllowed: boolean;
  readonly maxAttempts: number;
  getConfiguredClass<C extends InitConfigMethod>(cls: C): ConfiguredClass<C, Parameters<C['initConfiguration']>[1]>;
}

export class CommunicatorContextImpl extends DBOSContextImpl implements CommunicatorContext {
  readonly functionID: number;
  readonly retriesAllowed: boolean;
  readonly intervalSeconds: number;
  readonly maxAttempts: number;
  readonly backoffRate: number;
  readonly configuredClass: ConfiguredClass<unknown> | null;

  // TODO: Validate the parameters.
  constructor(workflowContext: WorkflowContextImpl | WorkflowContextDebug, functionID: number, span: Span, logger: Logger,
     params: CommunicatorConfig, commName: string, configuredClass: ConfiguredClass<unknown> | null)
  {
    super(commName, span, logger, workflowContext);
    this.functionID = functionID;
    this.retriesAllowed = params.retriesAllowed ?? true;
    this.intervalSeconds = params.intervalSeconds ?? 1;
    this.maxAttempts = params.maxAttempts ?? 3;
    this.backoffRate = params.backoffRate ?? 2;
    this.applicationConfig = workflowContext.applicationConfig;
    this.configuredClass = configuredClass;
  }

  getConfiguredClass<C extends InitConfigMethod>(cls: C): ConfiguredClass<C, Parameters<C['initConfiguration']>[1]> {
    if (!this.configuredClass) throw new DBOSError(`Configuration is required for ${this.operationName} but was not provided.`);
    const cc = this.configuredClass as ConfiguredClass<C, Parameters<C['initConfiguration']>[1]>;
    if (cc.classCtor !== cls) throw new DBOSError(`Configration retrieval was attempted for class '${cls.name}' but saved for class '${cc.classCtor.name}'`);
    return cc;
  }
}
