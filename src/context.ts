import { DBOSContextualLogger } from './telemetry/logs';
import { IncomingHttpHeaders } from 'http';
import { ParsedUrlQuery } from 'querystring';
import { UserDatabaseClient } from './user_database';
import { AsyncLocalStorage } from 'async_hooks';
import { DBOSInvalidWorkflowTransitionError } from './error';
import Koa from 'koa';
import { DBOSExecutor } from './dbos-executor';

export interface StepStatus {
  stepID: number;
  currentAttempt?: number;
  maxAttempts?: number;
}

export interface DBOSContextOptions {
  idAssignedForNextWorkflow?: string;
  queueAssignedForWorkflows?: string;
  logger?: DBOSContextualLogger;
  authenticatedUser?: string;
  authenticatedRoles?: string[];
  assumedRole?: string;
  request?: object;
  operationType?: string; // A custom helper for users to set a operation type of their choice. Intended for functions setting a pctx to run DBOS operations from.
  operationCaller?: string; // This is made to pass through the operationName to DBOS contexts, and potentially the caller span name.
  workflowTimeoutMS?: number | null;
}

export interface DBOSLocalCtx extends DBOSContextOptions {
  parentCtx?: DBOSLocalCtx;
  workflowId?: string;
  curWFFunctionId?: number; // If currently in a WF, the current call number / ID
  presetID?: boolean;
  deadlineEpochMS?: number;
  inRecovery?: boolean;
  curStepFunctionId?: number; // If currently in a step, its function ID
  stepStatus?: StepStatus; // If currently in a step, its public status object
  curTxFunctionId?: number; // If currently in a tx, its function ID
  isInStoredProc?: boolean;
  sqlClient?: UserDatabaseClient;
  koaContext?: Koa.Context;
}

function isWithinWorkflowCtx(ctx: DBOSLocalCtx) {
  if (ctx.workflowId === undefined) return false;
  return true;
}

function isInStepCtx(ctx: DBOSLocalCtx) {
  if (ctx.workflowId === undefined) return false;
  if (ctx.curStepFunctionId) return true;
  return false;
}

function isInTxnCtx(ctx: DBOSLocalCtx) {
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

const asyncLocalCtx = new AsyncLocalStorage<DBOSLocalCtx>();

export function getCurrentContextStore(): DBOSLocalCtx | undefined {
  return asyncLocalCtx.getStore();
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

export function functionIDGetIncrement(): number {
  const pctx = getCurrentContextStore();
  if (!pctx) throw new DBOSInvalidWorkflowTransitionError(`Attempt to get a call ID number outside of a workflow`);
  if (!isInWorkflowCtx(pctx))
    throw new DBOSInvalidWorkflowTransitionError(
      `Attempt to get a call ID number in a workflow that is already in a call`,
    );
  if (pctx.curWFFunctionId === undefined) pctx.curWFFunctionId = 0;
  return pctx.curWFFunctionId++;
}

export async function runWithTopContext<R>(ctx: DBOSLocalCtx, callback: () => Promise<R>): Promise<R> {
  return await asyncLocalCtx.run(ctx, callback);
}

export async function runWithParentContext<R>(
  pctx: DBOSLocalCtx | undefined,
  ctx: DBOSLocalCtx,
  callback: () => Promise<R>,
): Promise<R> {
  return await asyncLocalCtx.run(
    {
      ...pctx,
      ...ctx,
      parentCtx: pctx,
    },
    callback,
  );
}

export async function runWithDataSourceContext<R>(callnum: number, callback: () => Promise<R>) {
  // Check we are in a workflow context and not in a step / transaction already
  const pctx = getCurrentContextStore() ?? {};
  return await asyncLocalCtx.run(
    {
      ...pctx,
      curTxFunctionId: callnum,
      parentCtx: pctx,
      logger: DBOSExecutor.globalInstance!.ctxLogger,
    },
    callback,
  );
}

export async function runInStepContext<R>(
  pctx: DBOSLocalCtx,
  stepID: number,
  maxAttempts: number | undefined,
  currentAttempt: number | undefined,
  callback: () => Promise<R>,
) {
  // Check we are in a workflow context and not in a step / transaction already
  if (!pctx) throw new DBOSInvalidWorkflowTransitionError();
  if (!isInWorkflowCtx(pctx)) throw new DBOSInvalidWorkflowTransitionError();

  const stepStatus: StepStatus = {
    stepID: stepID,
    currentAttempt: currentAttempt,
    maxAttempts: currentAttempt ? maxAttempts : undefined,
  };

  return await runWithParentContext(
    pctx,
    {
      stepStatus: stepStatus,
      curStepFunctionId: stepID,
      parentCtx: pctx,
      logger: DBOSExecutor.globalInstance!.ctxLogger,
    },
    callback,
  );
}

/**
 * HTTPRequest includes useful information from http.IncomingMessage and parsed body,
 *   URL parameters, and parsed query string.
 * In essence, it is the serializable part of the request.
 */
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
