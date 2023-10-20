import * as ts from 'typescript';
import { WinstonLogger } from "../telemetry/logs";
import { DecoratorInfo, MethodInfo, TypeParser, ClassInfo, ParameterInfo } from './TypeParser';
import { BaseParameter, BodyParameter, Operation3, Parameter3, Path3, PathParameter, QueryParameter, RequestBody, Spec3 } from './swagger';
import { APITypes, ArgSources } from '../httpServer/handler';

function getOperonDecorator(decorated: MethodInfo | ParameterInfo | ClassInfo, names: string | readonly string[]) {
  const filtered = decorated.decorators.filter(decoratorFilter(names));
  if (filtered.length === 0) return undefined;
  if (filtered.length > 1) throw new Error(`Multiple ${JSON.stringify(names)} decorators found on ${decorated.name ?? "<unknown>"}`);
  return filtered[0];

  function decoratorFilter(name: string | readonly string[]) {
    return (decorator: DecoratorInfo) => {
      if (decorator.module !== '@dbos-inc/operon') return false;

      return typeof name === 'string'
        ? decorator.name === name
        : name.some(n => decorator.name === n);
    }
  }
}

export interface HttpEndpointInfo { verb: APITypes; path: string; };

function getHttpInfo(method: MethodInfo): HttpEndpointInfo | undefined {
  const decorator = getOperonDecorator(method, ['GetApi', 'PostApi']);
  if (!decorator) return undefined;
  const [path] = decorator.args;
  if (ts.isStringLiteral(path)) {
    return { verb: getHttpVerb(decorator), path: path.text };
  } else {
    throw new Error(`Unexpected path type: ${ts.SyntaxKind[path.kind]}`);
  }

  function getHttpVerb(decorator: DecoratorInfo): APITypes {
    if (decorator.name === 'GetApi') return APITypes.GET;
    if (decorator.name === 'PostApi') return APITypes.POST;
    throw new Error(`Unexpected HTTP decorator: ${decorator.name}`);
  }
}

function getDefaultArgSource(verb: APITypes) {
  switch (verb) {
    case APITypes.GET: return ArgSources.QUERY;
    case APITypes.POST: return ArgSources.BODY;
    default: throw new Error(`Unexpected HTTP verb: ${verb}`);
  }
}

function getParamSource(parameter: ParameterInfo, verb: APITypes) {
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

function getParamName(parameter: ParameterInfo) {
  const argName = getOperonDecorator(parameter, 'ArgName');
  if (!argName) return parameter.name;

  const nameParam = argName.args[0];
  if (nameParam && ts.isStringLiteral(nameParam)) return nameParam.text;

  throw new Error(`Unexpected ArgName argument type: ${ts.SyntaxKind[nameParam.kind]}`);
}

function generateParameter(parameter: ParameterInfo, verb: APITypes): Parameter3 {

  // temporarily store basic type info in param description
  const type = parameter.type?.getSymbol()?.getName()
    ?? parameter.type?.aliasSymbol?.getName()
    ?? (parameter.type && 'intrinsicName' in parameter.type)
      ? (parameter.type as any)['intrinsicName']
      : 'unknown';

  const argSource = getParamSource(parameter, verb);
  const required = !parameter.node.questionToken && !parameter.node.initializer;

  if (argSource == ArgSources.URL && !required) throw new Error(`URL parameters must be required: ${parameter.name}`);

  const param: Omit<BaseParameter, 'in'> = {
    name: getParamName(parameter),
    required,
    description: `Type: ${type}`,
    schema: {}
  };

  switch (argSource) {
    case ArgSources.BODY: return <BodyParameter>{ in: 'body', ...param };
    case ArgSources.QUERY: return <QueryParameter>{ in: 'query', ...param };
    case ArgSources.URL: return <PathParameter>{ in: 'path', ...param };
    default: throw new Error(`Unexpected ArgSource: ${argSource}`);
  }
}

function generatePath([method, $class, { verb, path: name }]: [MethodInfo, ClassInfo, HttpEndpointInfo]): [string, Path3] {

  // Note: slice the first parameter off here because the first parameter of a handle method must be an OperonContext, which is not exposed via the API
  const parameters = method.parameters.slice(1).map(p => generateParameter(p, verb));

  const requestBody: RequestBody | undefined = undefined;
  const operation: Operation3 = {
    operationId: method.name,
    responses: {
      "200": {
        "description": "Ok",
      }
    },
    security: [],
    // TODO: body parameters need to be put into the requestBody  instead of the parameters collection
    parameters,
    requestBody
  }

  switch (verb) {
    case APITypes.GET: return [name, { get: operation }];
    case APITypes.POST: return [name, { post: operation }];
    default: throw new Error(`Unexpected HTTP verb: ${verb}`);
  }
}

export function generateOpenApi(program: ts.Program, logger?: WinstonLogger) {
  const parser = new TypeParser(program, logger);
  const classes = parser.parse();

  //
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
