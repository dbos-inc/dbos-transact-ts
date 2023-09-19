import Koa from 'koa';
import Router from '@koa/router';
import { bodyParser } from '@koa/bodyparser';
import cors from "@koa/cors";
import { APITypes, ArgSources, HandlerContext, HttpEnpoint } from "./handler";
import { OperonTransaction } from "../transaction";
import { OperonWorkflow } from "../workflow";
import { OperonDataValidationError } from "../error";
import { Operon } from "../operon";
import { getArgNames, getOperonContextKind } from 'src/decorators';

export class OperonHttpServer {
  readonly app = new Koa();
  readonly router = new Router();

  /**
   * Create an Express app.
   * @param operon User pass in an Operon instance.
   * TODO: maybe call operon.init() somewhere in this class?
   */
  constructor(readonly operon: Operon, ...classes: { name: string }[]) {
    // Use default middlewares.
    // TODO: support customized middlewares.
    this.app.use(bodyParser());
    this.app.use(cors());

    // Register operon endpoints.
    for (const cls of classes) {
      this.#registerClass(cls);
    }
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

  #registerClass(target: { name: string }) {
    for (const propertyKey of Object.getOwnPropertyNames(target)) {

      const mdEndpoint = Reflect.getOwnMetadata("operon:http-endpoint", target, propertyKey) as HttpEnpoint | undefined;
      if (!mdEndpoint) continue;

      const contextKey = getOperonContextKind(target, propertyKey);
      const propDescValue = Object.getOwnPropertyDescriptor(target, propertyKey)?.value as Function | undefined;
      if (!propDescValue) throw new Error(`invalid property descriptor ${target.name}.${propertyKey}`);

      let endpointInvoker: (ctx: HandlerContext, args: any[]) => Promise<any>;
      switch (contextKey) {
        case 'operon:context:workflow':
          endpointInvoker = async (parentCtx: HandlerContext, args: any[]) => {
            return await this.operon.workflow(propDescValue as OperonWorkflow<any, any>, { parentCtx }, ...args).getResult();
          };
          break;

        case 'operon:context:transaction':
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
          throw new Error(`unexpected Operon context type ${contextKey} on ${target.name}.${propertyKey}`);
      }

      const paramNames = getArgNames(propDescValue);
      const argSources = Reflect.getOwnMetadata("operon:http-arg-source", target, propertyKey) as ArgSources[] | undefined ?? [];

      const handler = async (koaCtx: Koa.Context, next: Koa.Next) => {
        const handlerCtx = new HandlerContext(this.operon, koaCtx);

        // TODO: look to see if there's an existing HTTP request parsing library that we can reuse

        // TODO: move as much arg parsing logic as possible out to registration time logic

        const args: any[] = [];
        paramNames.forEach((name, index) => {
          if (index === 0) return;

          const src = argSources[index] ?? ArgSources.DEFAULT;

          let arg: unknown;
          if ((mdEndpoint.type === APITypes.GET && src === ArgSources.DEFAULT) || src === ArgSources.QUERY) {
            arg = koaCtx.request.query[name];
          } else if ((mdEndpoint.type === APITypes.POST && src === ArgSources.DEFAULT) || src === ArgSources.BODY) {
            if (!koaCtx.request.body) {
              throw new OperonDataValidationError(`Argument ${name} requires a method body.`);
            }
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
            arg = koaCtx.req.body[name];
          }

          if (arg) {
            args.push(arg);
          } else {
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            args.push(koaCtx.params[name]);
          }
        });

        try {
          const result = await endpointInvoker(handlerCtx, args);
          if (koaCtx.body === undefined) {
            koaCtx.body = result;
            koaCtx.status = 200;
          }
        } catch (e) {
          if (koaCtx.body === undefined) {
            if (e instanceof Error) {
              koaCtx.message = e.message;
            }
            koaCtx.body = e;
            koaCtx.status = 500;
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
