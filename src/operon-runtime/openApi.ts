import * as ts from 'typescript';
import { WinstonLogger } from "../telemetry/logs";
import { DecoratorInfo, MethodInfo, TypeParser, ClassInfo, ParameterInfo } from './TypeParser';
import { BodyParameter, Operation3, Parameter3, Path3, PathParameter, QueryParameter, Spec3 } from './swagger';
import { APITypes, ArgSources } from '../httpServer/handler';

function isHttpDecorator(decorator: DecoratorInfo): boolean {
  return decorator.module === '@dbos-inc/operon' && (decorator.name === 'GetApi' || decorator.name === 'PostApi');
}

export interface HttpEndpointInfo { verb: APITypes; path: string; };

function getHttpInfo(method: MethodInfo): HttpEndpointInfo | undefined {
  const decorators = method.decorators.filter(isHttpDecorator);
  if (decorators.length === 0) return undefined;
  if (decorators.length > 1) throw new Error(`Method ${method.name} has multiple HTTP decorators`);
  const decorator = decorators[0];
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

function isValid<T>(value: T | undefined): value is T {
  return value !== undefined;
}

function getArgSource(parameter: ParameterInfo) {
  const argSources = parameter.decorators.filter(d => d.module === '@dbos-inc/operon' && d.name === 'ArgSource');
  if (argSources.length > 1) throw new Error(`Parameter ${parameter.name} has multiple ArgSource decorators`);
  const argSource = argSources.length === 1 ? argSources[0] : undefined;
  if (!argSource) return ArgSources.DEFAULT;

  if (!ts.isPropertyAccessExpression(argSource.args[0])) throw new Error(`Unexpected ArgSource argument type: ${ts.SyntaxKind[argSource.args[0].kind]}`);
  switch (argSource.args[0].name.text) {
    case "BODY": return ArgSources.BODY;
    case "QUERY": return ArgSources.QUERY;
    case "URL": return ArgSources.URL;
    case "DEFAULT": return ArgSources.DEFAULT;
    default: throw new Error(`Unexpected ArgSource argument: ${argSource.args[0].name.text}`);
  }
}

function getDefaultArgSource(verb: APITypes) {
  switch (verb) {
    case APITypes.GET: return ArgSources.QUERY;
    case APITypes.POST: return ArgSources.BODY;
    default: throw new Error(`Unexpected HTTP verb: ${verb}`);
  }
}
function generateParameter(parameter: ParameterInfo, verb: APITypes): Parameter3 {

  // temporary type hack
  let type = parameter.type?.getSymbol()?.getName() ?? parameter.type?.aliasSymbol?.getName();
  type ??= parameter.type && 'intrinsicName' in parameter.type
    ? (parameter.type as any)['intrinsicName']
    : 'unknown';

  const param = {
    name: parameter.name,
    required: !parameter.node.questionToken,
    schema: {
      type
    }
  };

  let argSource = getArgSource(parameter);
  argSource = argSource === ArgSources.DEFAULT ? getDefaultArgSource(verb) : argSource;

  switch (argSource) {
    case ArgSources.BODY: return <BodyParameter>{ in: 'body', ...param };
    case ArgSources.QUERY: return <QueryParameter>{ in: 'query', ...param };
    case ArgSources.URL: return <PathParameter>{ in: 'path', ...param };
    default: throw new Error(`Unexpected ArgSource: ${argSource}`);
  }
}

function generatePath([method, $class, { verb, path: name }]: [MethodInfo, ClassInfo, HttpEndpointInfo]): [string, Path3] {

  const parameters = method.parameters.slice(1).map(p => generateParameter(p, verb));
  const operation: Operation3 = {
    operationId: method.name,
    responses: {
      "200": {
        "description": "Ok",
      }
    },
    security: [],
    parameters
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

  const methods = classes.flatMap(c => c.methods.map(method => [method, c] as [MethodInfo, ClassInfo]));
  const handlers = methods.map(([method, $class]) => {
    const http = getHttpInfo(method);
    return http ? [method, $class, http] as [MethodInfo, ClassInfo, HttpEndpointInfo] : undefined;
  }).filter(isValid);

  const spec: Spec3 = {
    openapi: "3.0.0",
    info: {
      title: "Operon API",
      version: "1.0.0",
    },
    paths: Object.fromEntries(handlers.map(generatePath)),
    components: {},
    servers: [
      {
        url: "/"
      }
    ],
  }

  return spec;
}
