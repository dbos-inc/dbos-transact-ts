import Koa from 'koa';
import Router from '@koa/router';
import { bodyParser } from '@koa/bodyparser';
import cors from "@koa/cors";
import { APITypes, ArgSources, HandlerContext, HttpEnpoint, getHttpEndpoint } from "./handler";
import { OperonTransaction } from "../transaction";
import { OperonWorkflow } from "../workflow";
import { OperonDataValidationError } from "../error";
import { Operon } from "../operon";
import { getArgNames, getOperonConfig } from 'src/decorators';

export interface ResponseError extends Error {
  status?: number;
}

export interface OperonHttpServerOptions {
  classes?: { name: string }[]
  koa?: Koa;
  router?: Router;
}

export class OperonHttpServer {
  readonly app: Koa;
  readonly router: Router;

  /**
   * Create an Express app.
   * @param operon User pass in an Operon instance.
   * TODO: maybe call operon.init() somewhere in this class?
   */
  constructor(readonly operon: Operon, { classes, koa, router }: OperonHttpServerOptions) {
    this.router = router ?? new Router();

    if (!koa) {
      koa = new Koa();
      koa.use(bodyParser());
      koa.use(cors());
    }
    this.app = koa;

    // Register operon endpoints.
    for (const cls of classes ?? []) {
      this.#registerClass(cls);
    }

    // Register operon endpoints.
    // OperonHttpServer.registerDecoratedEndpoints(this.operon, this.router);
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

  // static registerDecoratedEndpoints(operon : Operon, irouter : unknown) {

  #registerClass(target: { name: string }) {
    for (const propertyKey of Object.getOwnPropertyNames(target)) {

      const mdEndpoint = getHttpEndpoint(target, propertyKey);
      if (!mdEndpoint) continue;

      const configKind = getOperonConfig(target, propertyKey)?.kind;
      const propDescValue = Object.getOwnPropertyDescriptor(target, propertyKey)?.value as Function | undefined;
      if (!propDescValue) throw new Error(`invalid property descriptor ${target.name}.${propertyKey}`);

      let endpointInvoker: (ctx: HandlerContext, args: any[]) => Promise<any>;

      switch (configKind) {
        case 'workflow':
          endpointInvoker = async (parentCtx: HandlerContext, args: any[]) => {
            return await this.operon.workflow(propDescValue as OperonWorkflow<any, any>, { parentCtx }, ...args).getResult();
          };
          break;
        case 'transaction':
          endpointInvoker = async (parentCtx: HandlerContext, args: any[]) => {
            return await this.operon.transaction(propDescValue as OperonTransaction<any, any>, { parentCtx }, ...args);
          };
          break;
        case undefined:
          endpointInvoker = async (parentCtx: HandlerContext, args: any[]) => {
            return await propDescValue.call(undefined, [parentCtx, ...args]);
          };
          break;
        default:
          // TODO: should we allow communicator methods to be invoked via Http endpoint directly? 
          throw new Error(`unexpected Operon config kind ${configKind} on ${target.name}.${propertyKey}`);
      }

      const paramNames = getArgNames(propDescValue);
      const argSources = Reflect.getOwnMetadata("operon:http-arg-source", target, propertyKey) as ArgSources[] | undefined ?? [];

      const handler = async (koaCtxt: Koa.Context, next: Koa.Next) => {
        const handlerCtx = new HandlerContext(this.operon, koaCtxt);

        // TODO: look to see if there's an existing HTTP request parsing library that we can reuse

        // TODO: move as much arg parsing logic as possible out to registration time logic

        const args: any[] = [];
        paramNames.forEach((name, index) => {
          if (index === 0) return;

          const src = argSources[index] ?? ArgSources.DEFAULT;

          let arg: unknown;
          if ((mdEndpoint.type === APITypes.GET && src === ArgSources.DEFAULT) || src === ArgSources.QUERY) {
            arg = koaCtxt.request.query[name];
          } else if ((mdEndpoint.type === APITypes.POST && src === ArgSources.DEFAULT) || src === ArgSources.BODY) {
            if (!koaCtxt.request.body) {
              throw new OperonDataValidationError(`Argument ${name} requires a method body.`);
            }
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
            arg = koaCtxt.req.body[name];
          }

          if (arg) {
            args.push(arg);
          } else {
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            args.push(koaCtxt.params[name]);
          }
        });

        try {
          const result = await endpointInvoker(handlerCtx, args);
          if (koaCtxt.body === undefined) {
            koaCtxt.body = result;
            koaCtxt.status = 200;
          }
        } catch (e) {
          console.log(e); // CB - Guys!  We really need telemetry on by default!
          if (koaCtxt.body === undefined) { // CB - this is a bad idea
            if (e instanceof OperonDataValidationError) {
              const st = 400;
              koaCtxt.response.status = st;
              koaCtxt.body = {
                status: st,
                message: e.message,
                details: e,
              }
            }
            else if (e instanceof Error) {
              const st = ((e as ResponseError)?.status || 400); // CB - I disagree that this is a 500 - a 500 means go fix the server, 400 means go fix your request
              koaCtxt.response.status = st;
              koaCtxt.body = {
                status: st,
                message: e.message,
                details: e,
              }
            }
            else {
              koaCtxt.body = e;
              koaCtxt.status = 500;
            }
          }
        } finally {
          await next();
        }
      }

      switch (mdEndpoint.type) {
        case APITypes.GET:
          this.router.get(mdEndpoint.url, handler);
          break;
        case APITypes.POST:
          this.router.post(mdEndpoint.url, handler);
          break;
        default:
          throw new Error(`Invalid Http Endpoint type ${mdEndpoint.type}`)
      }
    }
  }
}
