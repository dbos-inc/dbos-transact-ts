import Koa from 'koa';
import Router from '@koa/router';
import { bodyParser } from '@koa/bodyparser';
import cors from "@koa/cors";
import { APITypes, ArgSources, forEachMethod } from "../decorators";
import { OperonTransaction } from "../transaction";
import { OperonWorkflow } from "../workflow";
import { OperonDataValidationError } from "src/error";
import { Operon } from "src/operon";
import { HandlerContext } from './handler';

export class OperonHttpServer {
  readonly app = new Koa();
  readonly router = new Router();

  /**
   * Create an Express app.
   * @param operon User pass in an Operon instance.
   * TODO: maybe call operon.init() somewhere in this class?
   */
  constructor(readonly operon: Operon) {
    // Use default middlewares.
    // TODO: support customized middlewares.
    this.app.use(bodyParser());
    this.app.use(cors());

    // Register operon endpoints.
    this.#registerDecoratedEndpoints();
    this.app.use(this.router.routes()).use(this.router.allowedMethods());
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
        const wrappedHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
          const oc: HandlerContext = new HandlerContext(this.operon, koaCtxt);
          oc.request = koaCtxt.request;
          oc.response = koaCtxt.response;

          // Parse the arguments.
          const args: unknown[] = [];
          ro.args.forEach((marg, idx) => {
            if (idx === 0) {
              return; // Do not parse the context.
            }

            let foundArg = undefined;
            if ((ro.apiType === APITypes.GET && marg.argSource === ArgSources.DEFAULT) || marg.argSource === ArgSources.QUERY) {
              foundArg = koaCtxt.request.query[marg.name];
              if (foundArg) {
                args.push(foundArg);
              }
            } else if ((ro.apiType === APITypes.POST && marg.argSource === ArgSources.DEFAULT) || marg.argSource === ArgSources.BODY) {
              if (!koaCtxt.request.body) {
                throw new OperonDataValidationError(`Argument ${marg.name} requires a method body.`);
              }
              // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
              foundArg = koaCtxt.request.body[marg.name];
              if (foundArg) {
                args.push(foundArg);
              }
            }

            // Try to parse the argument from the URL if nothing found.
            if (!foundArg) {
              // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
              args.push(koaCtxt.params[marg.name]);
            }
          });

          // Finally, invoke the transaction/workflow/plain function.
          // Return the function return value with 200, or server error with 500.
          try {
            let retValue;
            if (ro.txnConfig) {
              retValue = await this.operon.transaction(ro.registeredFunction as OperonTransaction<unknown[], unknown>, { parentCtx: oc }, ...args);
            } else if (ro.workflowConfig) {
              retValue = await this.operon.workflow(ro.registeredFunction as OperonWorkflow<unknown[], unknown>, { parentCtx: oc }, ...args).getResult();
            } else {
              // Directly invoke the handler code.
              retValue = await ro.invoke(undefined, [oc, ...args]);
            }
            if (koaCtxt.body === undefined) {
              koaCtxt.body = retValue;
              koaCtxt.status = 200;
            }
          } catch (e) {
            if (koaCtxt.body === undefined) {
              if (e instanceof Error) {
                koaCtxt.message = e.message;
              }
              koaCtxt.body = e;
              koaCtxt.status = 500;
            }
          } finally {
            await koaNext();
          }
        };

        // Actually register the endpoint.
        if (ro.apiType === APITypes.GET) {
          this.router.get(ro.apiURL, wrappedHandler);
        } else if (ro.apiType === APITypes.POST) {
          this.router.post(ro.apiURL, wrappedHandler);
        }
      }
    });
  }
}
