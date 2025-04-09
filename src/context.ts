import { Span } from '@opentelemetry/sdk-trace-base';
import { GlobalLogger as Logger, Logger as DBOSLogger } from './telemetry/logs';
import { get } from 'lodash';
import { IncomingHttpHeaders } from 'http';
import { ParsedUrlQuery } from 'querystring';
import { UserDatabaseClient } from './user_database';
import { DBOSConfigKeyTypeError } from './error';
import { AsyncLocalStorage } from 'async_hooks';
import { WorkflowContext, WorkflowContextImpl } from './workflow';
import { TransactionContextImpl } from './transaction';
import { StepContextImpl } from './step';
import { DBOSInvalidWorkflowTransitionError } from './error';
import { StoredProcedureContextImpl } from './procedure';
import { HandlerContextImpl } from './httpServer/handler';
import { globalParams } from './utils';

export interface StepStatus {
  stepID: number;
  currentAttempt?: number;
  maxAttempts?: number;
}

export interface DBOSLocalCtx {
  ctx?: DBOSContext;
  parentCtx?: DBOSLocalCtx;
  idAssignedForNextWorkflow?: string;
  queueAssignedForWorkflows?: string;
  workflowId?: string;
  inRecovery?: boolean;
  curStepFunctionId?: number; // If currently in a step, its function ID
  stepStatus?: StepStatus; // If currently in a step, its public status object
  curTxFunctionId?: number;
  isInStoredProc?: boolean;
  sqlClient?: UserDatabaseClient;
  span?: Span;
  authenticatedUser?: string;
  authenticatedRoles?: string[];
  assumedRole?: string;
  request?: HTTPRequest;
  operationType?: string; // A custom helper for users to set a operation type of their choice. Intended for functions setting a pctx to run DBOS operations from.
  operationCaller?: string; // This is made to pass through the operationName to DBOS contexts, and potentially the caller span name.
}

export function isWithinWorkflowCtx(ctx: DBOSLocalCtx) {
  if (ctx.workflowId === undefined) return false;
  return true;
}

export function isInStepCtx(ctx: DBOSLocalCtx) {
  if (ctx.workflowId === undefined) return false;
  if (ctx.curStepFunctionId) return true;
  return false;
}

export function isInTxnCtx(ctx: DBOSLocalCtx) {
  if (ctx.workflowId === undefined) return false;
  if (ctx.curTxFunctionId) return true;
  return false;
}

export function isInWorkflowCtx(ctx: DBOSLocalCtx) {
  if (!isWithinWorkflowCtx(ctx)) return false;
  if (isInStepCtx(ctx)) return false;
  if (isInTxnCtx(ctx)) return false;
  return true;
}

export const asyncLocalCtx = new AsyncLocalStorage<DBOSLocalCtx>();

export function getCurrentContextStore(): DBOSLocalCtx | undefined {
  return asyncLocalCtx.getStore();
}

export function getCurrentDBOSContext(): DBOSContext | undefined {
  return asyncLocalCtx.getStore()?.ctx;
}

export function assertCurrentDBOSContext(): DBOSContext {
  const ctx = asyncLocalCtx.getStore()?.ctx;
  if (!ctx) throw new DBOSInvalidWorkflowTransitionError('No current DBOS Context');
  return ctx;
}

export function assertCurrentWorkflowContext(): WorkflowContextImpl {
  const ctxs = getCurrentContextStore();
  if (!ctxs || !isInWorkflowCtx(ctxs)) {
    throw new DBOSInvalidWorkflowTransitionError();
  }
  const ctx = assertCurrentDBOSContext();
  return ctx as WorkflowContextImpl;
}

export function getNextWFID(assignedID?: string) {
  let wfId = assignedID;
  if (!wfId) {
    const pctx = getCurrentContextStore();
    const nextID = pctx?.idAssignedForNextWorkflow;
    if (nextID) {
      wfId = nextID;
      pctx.idAssignedForNextWorkflow = undefined;
    }
  }
  return wfId;
}

export async function runWithDBOSContext<R>(ctx: DBOSContext, callback: () => Promise<R>) {
  return await asyncLocalCtx.run(
    {
      ctx,
      idAssignedForNextWorkflow: ctx.workflowUUID,
      request: ctx.request,
      authenticatedRoles: ctx.authenticatedRoles,
      authenticatedUser: ctx.authenticatedUser,
      span: ctx.span,
    },
    callback,
  );
}

export async function runWithHandlerContext<R>(ctx: HandlerContextImpl, callback: () => Promise<R>) {
  return await asyncLocalCtx.run(
    {
      ctx,
      idAssignedForNextWorkflow: ctx.workflowUUID,
      request: ctx.request,
      authenticatedRoles: ctx.authenticatedRoles,
      authenticatedUser: ctx.authenticatedUser,
      span: ctx.span,
    },
    callback,
  );
}

export async function runWithTopContext<R>(ctx: DBOSLocalCtx, callback: () => Promise<R>): Promise<R> {
  return await asyncLocalCtx.run(ctx, callback);
}

export async function runWithTransactionContext<Client extends UserDatabaseClient, R>(
  ctx: TransactionContextImpl<Client>,
  callback: () => Promise<R>,
) {
  // Check we are in a workflow context and not in a step / transaction already
  const pctx = getCurrentContextStore();
  if (!pctx) throw new DBOSInvalidWorkflowTransitionError();
  if (!isInWorkflowCtx(pctx)) throw new DBOSInvalidWorkflowTransitionError();
  return await asyncLocalCtx.run(
    {
      ctx,
      workflowId: ctx.workflowUUID,
      curTxFunctionId: ctx.functionID,
      parentCtx: pctx,
    },
    callback,
  );
}

export async function runWithStoredProcContext<R>(ctx: StoredProcedureContextImpl, callback: () => Promise<R>) {
  // Check we are in a workflow context and not in a step / transaction already
  const pctx = getCurrentContextStore();
  if (!pctx) throw new DBOSInvalidWorkflowTransitionError();
  if (!isInWorkflowCtx(pctx)) throw new DBOSInvalidWorkflowTransitionError();
  return await asyncLocalCtx.run(
    {
      ctx,
      workflowId: ctx.workflowUUID,
      curTxFunctionId: ctx.functionID,
      parentCtx: pctx,
      isInStoredProc: true,
    },
    callback,
  );
}

export async function runWithStepContext<R>(
  ctx: StepContextImpl,
  currentAttempt: number | undefined,
  callback: () => Promise<R>,
) {
  // Check we are in a workflow context and not in a step / transaction already
  const pctx = getCurrentContextStore();
  if (!pctx) throw new DBOSInvalidWorkflowTransitionError();
  if (!isInWorkflowCtx(pctx)) throw new DBOSInvalidWorkflowTransitionError();

  const stepStatus: StepStatus = {
    stepID: ctx.functionID,
    currentAttempt: currentAttempt,
    maxAttempts: currentAttempt ? ctx.maxAttempts : undefined,
  };

  return await asyncLocalCtx.run(
    {
      ctx,
      stepStatus: stepStatus,
      workflowId: ctx.workflowUUID,
      curStepFunctionId: ctx.functionID,
      parentCtx: pctx,
    },
    callback,
  );
}

export async function runWithWorkflowContext<R>(ctx: WorkflowContext, callback: () => Promise<R>) {
  // TODO: Check context, this could be a child workflow?
  return await asyncLocalCtx.run(
    {
      ctx,
      workflowId: ctx.workflowUUID,
    },
    callback,
  );
}

// HTTPRequest includes useful information from http.IncomingMessage and parsed body, URL parameters, and parsed query string.
export interface HTTPRequest {
  readonly headers?: IncomingHttpHeaders; // A node's http.IncomingHttpHeaders object.
  readonly rawHeaders?: string[]; // Raw headers.
  readonly params?: unknown; // Parsed path parameters from the URL.
  readonly body?: unknown; // parsed HTTP body as an object.
  readonly rawBody?: string; // Unparsed raw HTTP body string.
  readonly query?: ParsedUrlQuery; // Parsed query string.
  readonly querystring?: string; // Unparsed raw query string.
  readonly url?: string; // Request URL.
  readonly method?: string; // Request HTTP method.
  readonly ip?: string; // Request remote address.
  readonly requestID?: string; // Request ID. Gathered from headers or generated if missing.
}

/**
 * @deprecated Use `DBOS.workflow`, `DBOS.step`, `DBOS.transaction`, and other decorators that do not pass contexts around.
 */
export interface DBOSContext {
  readonly request: HTTPRequest;
  readonly workflowUUID: string;
  readonly authenticatedUser: string;
  readonly authenticatedRoles: string[];
  readonly assumedRole: string;

  readonly logger: DBOSLogger;
  readonly span: Span;

  getConfig<T>(key: string): T | undefined;
  getConfig<T>(key: string, defaultValue: T): T;
}

export class DBOSContextImpl implements DBOSContext {
  request: HTTPRequest = {}; // Raw incoming HTTP request.
  authenticatedUser: string = ''; // The user that has been authenticated
  authenticatedRoles: string[] = []; // All roles the user has according to authentication
  assumedRole: string = ''; // Role in use - that user has and provided authorization to current function
  workflowUUID: string = ''; // Workflow UUID. Empty for HandlerContexts.
  executorID: string = globalParams.executorID; // Executor ID. Gathered from the environment and "local" otherwise
  applicationID: string = globalParams.appID; // Application ID. Gathered from the environment and empty otherwise
  readonly logger: DBOSLogger; // Wrapper around the global logger for this context.

  constructor(
    readonly operationName: string,
    readonly span: Span,
    logger: Logger,
    parentCtx?: DBOSContextImpl,
  ) {
    if (parentCtx) {
      this.request = parentCtx.request;
      this.authenticatedUser = parentCtx.authenticatedUser;
      this.authenticatedRoles = parentCtx.authenticatedRoles;
      this.assumedRole = parentCtx.assumedRole;
      this.workflowUUID = parentCtx.workflowUUID;
    }
    this.logger = new DBOSLogger(logger, this);
  }

  applicationConfig?: object;
  getConfig<T>(key: string): T | undefined;
  getConfig<T>(key: string, defaultValue: T): T;
  getConfig<T>(key: string, defaultValue?: T): T | undefined {
    const value = get(this.applicationConfig, key, defaultValue);
    // If the key is found and the default value is provided, check whether the value is of the same type.
    if (value && defaultValue && typeof value !== typeof defaultValue) {
      throw new DBOSConfigKeyTypeError(key, typeof defaultValue, typeof value);
    }
    return value;
  }
}
