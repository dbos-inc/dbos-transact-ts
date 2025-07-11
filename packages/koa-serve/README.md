# Serve DBOS Functions over HTTP with Koa

## Introduction

[DBOS](https://github.com/dbos-inc/dbos-transact-ts) provides a lightweight durable execution framework that can be used as a library within any app. This package provides a simple way to serve DBOS functions with the [Koa](https://koajs.com/) web framework.

## Registering Endpoints

First, create an instance of `DBOSKoa`. This object is used to keep track of which functions are to be registered with routable URLs, so it is OK to create and use it very early in your code. Then, use this instance to register some HTTP endpoint URLs.

```typescript
const dhttp = new DBOSKoa();

export class HTTPEndpoints {
  @dhttp.getApi('/foobar')
  static async foobar(arg: string) {
    return Promise.resolve(`Value of 'arg': ${arg}`);
  }
}
```

The example above registers an HTTP `GET` endpoint. `postApi`, `deleteApi`, `putApi`, and `patchApi` are also available.

## Starting a Koa App

The Koa server can be started by your main startup function. Note the order of operations below:

```typescript
await DBOS.launch(); // Starts DBOS components and begins any necessary recovery
DBOS.logRegisteredEndpoints(); // Optional - list out all of the registered DBOS event receivers and URLs

// Create a new Koa and router
app = new Koa();
appRouter = new Router();

// Add any URLs and middleware registered via DBOS with your Koa
dhttp.registerWithApp(app, appRouter);

// Tell Koa to serve on port 3000
app.listen(3000, () => {
  console.log('Server running on http://localhost:3000');
});
```

## Integrating With an Existing App

If you already have a Koa server in your app, you can add the DBOS HTTP endpoints to it:

```
    dhttp.registerWithApp(app, appRouter);
```

You should also be sure to call `DBOS.launch` at some point prior to starting request handling.

Node.js HTTP servers are also generally compatible with each other. Your Koa's `app.callback()` can be served within another framework (such as [express](https://expressjs.com/)), or by a HTTP server you set up.

## Inputs and HTTP Requests

When a function has arguments, DBOS automatically parses them from the HTTP request, and returns an error to the client if they are not found.

Automatic argument handling is usually sufficent. If not, see the sections below.

### URL Path Parameters

You can include a path parameter placeholder in a URL by prefixing it with a colon, like `name` in this example:

```typescript
@dhttp.getApi('/greeting/:name')
static async greetingEndpoint(name: string) {
  return `Greeting, ${name}`;
}
```

Then, give your method an argument with a matching name (such as `name: string` above) and it is automatically parsed from the path parameter.

For example, if we send our app this request, then our method is called with `name` set to `dbos`:

```
GET /greeting/dbos
```

### URL Query String Parameters

`GET` and `DELETE` endpoints automatically parse arguments from query strings.

For example, the following endpoint expects the `id` and `name` parameters to be passed through a query string:

```typescript
@dhttp.getApi('/example')
static async exampleGet(id: number, name: string) {
  return `${id} and ${name} are parsed from URL query string parameters`;
}
```

If we send our app this request, then our method is called with `id` set to `123` and `name` set to `dbos`:

```
GET /example?id=123&name=dbos
```

### HTTP Body Fields

`POST`, `PATCH`, and `PUT` endpoints automatically parse arguments from the HTTP request body.

For example, the following endpoint expects the `id` and `name` parameters to be passed through the HTTP request body:

```typescript
@DBOS.postApi('/example')
static async examplePost(id: number, name: string) {
  return `${id} and ${name} are parsed from the HTTP request body`;
}
```

If we send our app this request, then our method is called with `id` set to `123` and `name` set to `dbos`:

```javascript
POST /example
Content-Type: application/json
{
  "name": "dbos",
  "id": 123
}
```

When sending an HTTP request with a JSON body, make sure you set the [`Content-Type`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type) header to `application/json`.

### Overriding Argument Sources

Argument sources can be overriden with the `DBOSKoa.argSource` parameter decorator. For example, to force a POST parameter to be taken from the query string instead of the body:

```typescript
  @dhttp.postApi('/postquery')
  static async postQuery(@DBOSKoa.argSource(ArgSources.QUERY) name: string) {
    return Promise.resolve(`hello ${name}`);
  }
```

## Raw Requests

If you need finer-grained request parsing, any DBOS method invoked via HTTP request can access raw request information from `DBOSKoa.httpRequest`. This returns the following information:

```typescript
interface HTTPRequest {
  readonly headers?: IncomingHttpHeaders; // A node's http.IncomingHttpHeaders object.
  readonly rawHeaders?: string[]; // Raw headers.
  readonly params?: unknown; // Parsed path parameters from the URL.
  readonly body?: unknown; // parsed HTTP body as an object.
  readonly rawBody?: string; // Unparsed raw HTTP body string.
  readonly query?: ParsedUrlQuery; // Parsed query string.
  readonly querystring?: string; // Unparsed raw query string.
  readonly url?: string; // Request URL.
  readonly ip?: string; // Request remote address.
}
```

### Accessing the Koa Context

For code that needs to access the Koa context to perform a redirect, send a large response, etc., this can be accessed with `DBOSKoa.koaContext`:

```typescript
DBOSKoa.koaContext.redirect(url + '-dbos');
```

## Outputs and HTTP Responses

If a function invoked via HTTP request returns successfully, its return value is sent in the HTTP response body with status code `200` (or `204` if nothing is returned).

If the function throws an exception, the error message is sent in the response body with a `400` or `500` status code.
If the error contains a `status` field, the response uses that status code instead.

If you need custom HTTP response behavior, you can access the HTTP response directly via [`DBOSKoa.koaContext.response`].

## Koa Middleware

Middleware may be set up directly on the Koa app or router. Additionally, DBOS provides class-level decorators for placing middleware onto all URLs registered to methods in the class.

DBOS supports running arbitrary [Koa](https://koajs.com/) middleware for serving HTTP requests.
Middlewares are configured at the class level through the `@DBOSKoa.koaMiddleware` decorator.
Here is an example of a simple middleware checking an HTTP header:

```typescript
import { Middleware } from "koa";

const middleware: Middleware = async (ctx, next) => {
  const contentType = ctx.request.headers["content-type"];
  await next();
};

@dhttp.koaMiddleware(middleware)
class Hello {
  ...
}
```

### CORS

[Cross-Origin Resource Sharing (CORS)](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS) is a security feature that controls access to resources from different domains. DBOS uses [`@koa/cors`](https://github.com/koajs/cors) with a permissive default configuration.

If more complex logic is needed, or if the CORS configuration differs between operation classes, the `@DBOSKoa.koaCors` class-level decorator can be used to specify the CORS middleware in full.

```typescript
import cors from '@koa/cors';

@dhttp.koaCors(
  cors({
    credentials: true,
    origin: (o: Context) => {
      const whitelist = ['https://us.com', 'https://partner.com'];
      const origin = o.request.header.origin ?? '*';
      return whitelist.includes(origin) ? origin : '';
    },
  }),
)
class EndpointsWithSpecialCORS {}
```

### BodyParser

By default, DBOS uses [`@koa/bodyparser`](https://github.com/koajs/bodyparser) to support JSON in requests. If this default behavior is not desired, you can configure a custom body parser with the `@DBOSKoa.koaBodyParser` decorator.

```typescript
import { bodyParser } from '@koa/bodyparser';

@dhttp.koaBodyParser(
  bodyParser({
    extendTypes: {
      json: ['application/json', 'application/custom-content-type'],
    },
    encoding: 'utf-8',
  }),
)
class OperationEndpoints {}
```

### Integration With DBOS User Authentication / Authorization

DBOS provides [role-based authorization](https://docs.dbos.dev/typescript/reference/transactapi/dbos-class#declarative-role-based-security). This package provides a middleware-based approach for collecting authentication information, to populate the authenticated user and allowed roles.

First, define an authentication middleware function:

```typescript
async function authTestMiddleware(ctx: DBOSKoaAuthContext) {
  if (ctx.requiredRole.length > 0) {
    const { userid } = ctx.koaContext.request.query; // Or other source of user from request, header, cookie, etc.
    const uid = userid?.toString();

    if (!uid || uid.length === 0) {
      return Promise.reject(new DBOSError.DBOSNotAuthorizedError('Not logged in.', 401));
    } else {
      if (uid === 'unwelcome') {
        return Promise.reject(new DBOSError.DBOSNotAuthorizedError('Go away.', 401));
      }
      return Promise.resolve({
        authenticatedUser: uid,
        authenticatedRoles: uid === 'a_real_user' ? ['user'] : ['other'],
      });
    }
  }
  return;
}
```

Then, attach it to the classes as a middleware:

```typescript
@DBOS.defaultRequiredRole(['user'])
@dhttp.authentication(authTestMiddleware)
class EndpointsWithAuthMiddleware {
  // ...
}
```

### Global Middleware

Middleware that is to be run on all routes should be directly added to your Koa app or router. However, for compatibility with prior "serverless" DBOS SDKs, a `@DBOSKoa.koaGlobalMiddleware` decorator exists.

## Unit Testing

DBOS HTTP functions are easily called in existing test frameworks. See [the unit tests](./tests/basic.test.ts) for examples of setting up DBOS and calling `app.callback()` using `jest` and `supertest`.
