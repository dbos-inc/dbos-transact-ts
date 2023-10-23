import * as ts from 'typescript';
import { WinstonLogger } from "../telemetry/logs";
import { DecoratorInfo, MethodInfo, TypeParser, ClassInfo, ParameterInfo } from './TypeParser';
import { BaseParameter, Operation3, Parameter3, Path3, PathParameter, QueryParameter, RequestBody, Schema3, Spec3 } from './swagger';
import { APITypes, ArgSources } from '../httpServer/handler';

interface HttpEndpointInfo { verb: APITypes; path: string; }

function getOperonDecorator(decorated: MethodInfo | ParameterInfo | ClassInfo, name: string ): DecoratorInfo | undefined {
  const filtered = decorated.decorators.filter(d => d.module === '@dbos-inc/operon' && d.name === name);
  if (filtered.length === 0) return undefined;
  if (filtered.length > 1) throw new Error(`Multiple ${JSON.stringify(name)} decorators found on ${decorated.name ?? "<unknown>"}`);
  return filtered[0];
}

function getHttpInfo(method: MethodInfo): HttpEndpointInfo | undefined {
  const getApiDecorator = getOperonDecorator(method, 'GetApi');
  const postApiDecorator = getOperonDecorator(method, 'PostApi');
  if (getApiDecorator && postApiDecorator) throw new Error(`Method ${method.name} has both GetApi and PostApi decorators`);
  if (!getApiDecorator && !postApiDecorator) return undefined;

  const verb = getApiDecorator ? APITypes.GET : APITypes.POST;
  const arg = getApiDecorator ? getApiDecorator.args[0] : postApiDecorator?.args[0];
  if (!arg) throw new Error(`Missing path argument for ${verb}Api decorator`);
  if (!ts.isStringLiteral(arg)) throw new Error(`Unexpected path argument type: ${ts.SyntaxKind[arg.kind]}`);
  return { verb, path: arg.text };
}

function getDefaultArgSource(verb: APITypes): ArgSources.BODY | ArgSources.QUERY {
  switch (verb) {
    case APITypes.GET: return ArgSources.QUERY;
    case APITypes.POST: return ArgSources.BODY;
  }
}

function getParamSource(parameter: ParameterInfo, verb: APITypes): ArgSources.BODY | ArgSources.QUERY | ArgSources.URL {
  const argSource = getOperonDecorator(parameter, 'ArgSource');
  if (!argSource) return getDefaultArgSource(verb);

  if (!ts.isPropertyAccessExpression(argSource.args[0])) throw new Error(`Unexpected ArgSource argument type: ${ts.SyntaxKind[argSource.args[0].kind]}`);
  switch (argSource.args[0].name.text) {
    case "BODY": return ArgSources.BODY;
    case "QUERY": return ArgSources.QUERY;
    case "URL": return ArgSources.URL;
    case "DEFAULT": return getDefaultArgSource(verb);
    default: throw new Error(`Unexpected ArgSource argument: ${argSource.args[0].name.text}`);
  }
}

function getParamName(parameter: ParameterInfo): string {
  const argName = getOperonDecorator(parameter, 'ArgName');
  if (!argName) return parameter.name;

  const nameParam = argName.args[0];
  if (nameParam && ts.isStringLiteral(nameParam)) return nameParam.text;

  throw new Error(`Unexpected ArgName argument type: ${ts.SyntaxKind[nameParam.kind]}`);
}

function getType(type: ts.Type | undefined): string {
  // temporarily stub out type generation
  // eslint-disable-next-line @typescript-eslint/no-unsafe-return
  return type?.getSymbol()?.getName()
    ?? type?.aliasSymbol?.getName()
    ?? (type && 'intrinsicName' in type)
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
      ? (type as any)['intrinsicName']
      : 'unknown';
}

function generateParameter([parameter, argSource]: [ParameterInfo, ArgSources]): Parameter3 {
  if (argSource == ArgSources.URL && !parameter.required) throw new Error(`URL parameters must be required: ${parameter.name}`);

  const param: Omit<BaseParameter, 'in'> = {
    name: getParamName(parameter),
    required: parameter.required,
    // temporarily store basic type info in param description
    description: `Type: ${getType(parameter.type)}`,
    schema: {}
  };

  switch (argSource) {
    // case ArgSources.BODY: return <BodyParameter>{ in: 'body', ...param };
    case ArgSources.QUERY: return <QueryParameter>{ in: 'query', ...param };
    case ArgSources.URL: return <PathParameter>{ in: 'path', ...param };
    default: throw new Error(`Unexpected ArgSource: ${argSource}`);
  }
}

function getRequestBody(parameters: readonly ParameterInfo[]): RequestBody | undefined {
  if (parameters.length === 0) return undefined;

  const namedParams = parameters.map(p => [getParamName(p), p] as [string, ParameterInfo]);
  // TODO: param type info
  const properties = namedParams.map(([name, param]) => [name, { description: `Type: ${getType(param.type)}`}] as [string, Schema3]);
  const required = namedParams.filter(([_, param]) => param.required).map(([name, _]) => name);

  return {
    required: true,
    content: {
      "application/json": {
        schema: {
          type: 'object',
          properties: Object.fromEntries(properties),
          required
        }
      }
    }
  }
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function generatePath([method, $class, { verb, path: name }]: [MethodInfo, ClassInfo, HttpEndpointInfo]): [string, Path3] {

  // The first parameter of a handle method must be an OperonContext, which is not exposed via the API
  const sourcedParams = method.parameters.slice(1)
    .map(p => [p, getParamSource(p, verb)] as [ParameterInfo, ArgSources]);

  // QUERY and URL parameters are specified in the Operation.parameters field
  const parameters = sourcedParams.filter(([_, source]) => source !== ArgSources.BODY).map(generateParameter);
  // BODY parameters are specified in the Operation.requestBody field
  const requestBody = getRequestBody(sourcedParams.filter(([_, source]) => source === ArgSources.BODY).map(([p, _]) => p));

  const operation: Operation3 = {
    operationId: method.name,
    responses: {
      "200": {
        "description": "Ok",
      }
    },
    security: [],
    parameters,
    requestBody
  }

  switch (verb) {
    case APITypes.GET: return [name, { get: operation }];
    case APITypes.POST: return [name, { post: operation }];
  }
}

export function generateOpenApi(program: ts.Program, logger?: WinstonLogger) {
  const parser = new TypeParser(program, logger);
  const classes = parser.parse();

  const methods = classes.flatMap(c => c.methods.map(method => [method, c] as [MethodInfo, ClassInfo]));
  const handlers = methods.map(([method, $class]) => {
    const http = getHttpInfo(method);
    return http ? [method, $class, http] as [MethodInfo, ClassInfo, HttpEndpointInfo] : undefined;
  }).filter(isValid);

  const spec: Spec3 = {
    // TODO: OpenAPI 3.1 support
    openapi: "3.0.3", // https://spec.openapis.org/oas/v3.0.3
    info: {
      // TODO: Where to get this info from? package.json?
      title: "Operon API",
      version: "1.0.0",
    },
    // When we are cloud hosting, we will have useful info to put here.
    servers: [],
    //
    components: {},
    paths: Object.fromEntries(handlers.map(generatePath)),
  }

  return spec;

  function isValid<T>(value: T | undefined): value is T { return value !== undefined; }
}
