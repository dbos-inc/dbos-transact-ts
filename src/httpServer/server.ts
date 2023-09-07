import express, { Request, Response, NextFunction } from "express";
import cors from "cors";
import { APITypes, ArgSources, forEachMethod } from "../decorators";
import { OperonContext } from "../context";
import { OperonTransaction, TransactionContext } from "../transaction";
import { OperonWorkflow, WorkflowContext } from "../workflow";
import { CommunicatorContext } from "../communicator";
import { OperonDataValidationError } from "src/error";
import { Operon } from "src/operon";

export class OperonHttpServer {
  readonly app: express.Application;

  /**
   * Create an Express app.
   * @param operon User pass in an Operon instance.
   * TODO: maybe call operon.init() somewhere in this class?
   */
  constructor(readonly operon: Operon) {
    this.app = express();
    // Use default middlewares.
    // TODO: support customized middlewares.
    this.app.use(express.json());
    this.app.use(express.urlencoded({ extended: true }));
    this.app.use(cors());

    // Register operon endpoints.
    this.#registerDecoratedEndpoints();
  }

  /**
   * Register Operon endpoints and attach to the app. Then start the server at the given port.
   * @param port
   */
  listen(port: number) {
    // Start the HTTP server.
    this.app.listen(port, () => {
      console.log(`[Operon Server]: Server is running at http://localhost:${port}`);
    });
  }

  #registerDecoratedEndpoints() {
    // Register user declared endpoints, wrap around the endpoint with request parsing and response.
    forEachMethod((registeredOperation) => {
      const ro = registeredOperation;
      if (ro.apiURL) {
        // Wrapper function that parses request and send response.
        const wrappedHandler = async (req: Request, res: Response) => {
          const oc: OperonContext = new OperonContext();
          oc.request = req;
          oc.response = res;

          // Parse the arguments.
          const args: unknown[] = [];
          ro.args.forEach((marg, idx) => {
            if (idx === 0 && (marg.argType === TransactionContext || marg.argType === WorkflowContext || marg.argType === CommunicatorContext || marg.argType === OperonContext)) {
              return; // Do not parse the context.
            }

            let foundArg = undefined;
            if ((ro.apiType === APITypes.GET && marg.argSource === ArgSources.DEFAULT) || marg.argSource === ArgSources.QUERY) {
              foundArg = req.query[marg.name];
              if (foundArg) {
                args.push(foundArg);
              }
            } else if ((ro.apiType === APITypes.POST && marg.argSource === ArgSources.DEFAULT) || marg.argSource === ArgSources.BODY) {
              if (!req.body) {
                throw new OperonDataValidationError(`Argument ${marg.name} requires a method body.`);
              }
              // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
              foundArg = req.body[marg.name];
              if (foundArg) {
                args.push(foundArg);
              }
            }

            // Try to parse the argument from the URL if nothing found.
            if (!foundArg) {
              args.push(req.params[marg.name]);
            }
          });

          // Finally, invoke the transaction/workflow/plain function.
          // Return the function return value with 200, or server error with 500.
          try {
            let retValue;
            if (ro.txnConfig) {
              // TODO: should we call the replacement function instead?
              // For now call the original function because we registered it.
              retValue = await this.operon.transaction(ro.origFunction as OperonTransaction<unknown[], unknown>, { parentCtx: oc }, ...args);
            } else if (ro.workflowConfig) {
              retValue = await this.operon.workflow(ro.origFunction as OperonWorkflow<unknown[], unknown>, { parentCtx: oc }, ...args).getResult();
            } else {
              // Directly invoke the handler code.
              retValue = await ro.invoke(undefined, [oc, ...args]);
            }
            if (!res.headersSent) {
              // If the headers have been sent, it means the program has responded, then we don't send anything.
              res.status(200).send(retValue);
            }
          } catch (e) {
            if (!res.headersSent) {
              if (e instanceof Error) {
                res.status(500).send(e.message);
              } else {
                res.status(500).send(e);
              }
            }
          }
        };

        // Actually register the endpoint.
        if (ro.apiType === APITypes.GET) {
          this.app.get(ro.apiURL, asyncHandler(wrappedHandler));
        } else if (ro.apiType === APITypes.POST) {
          this.app.post(ro.apiURL, asyncHandler(wrappedHandler));
        }
      }
    });
  }
}

const asyncHandler = (fn: (req: Request, res: Response, next: NextFunction) => Promise<void>) => (req: Request, res: Response, next: NextFunction) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};
