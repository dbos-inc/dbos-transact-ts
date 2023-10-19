import * as ts from 'typescript';
import { WinstonLogger } from "../telemetry/logs";
import { DecoratorInfo, MethodInfo, TypeParser, ClassInfo } from './TypeParser';
import { Operation3, Path3, Spec3 } from './swagger';

function isHttpDecorator(decorator: DecoratorInfo): boolean {
  return decorator.module === '@dbos-inc/operon' && (decorator.name === 'GetApi' || decorator.name === 'PostApi');
}

export interface HttpEndpointInfo { verb: string; path: string; };

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

  function getHttpVerb(decorator: DecoratorInfo): string {
    if (decorator.name === 'GetApi') return 'get';
    if (decorator.name === 'PostApi') return 'post';
    throw new Error(`Unexpected HTTP decorator: ${decorator.name}`);
  }
}

function isValid<T>(value: T | undefined): value is T {
  return value !== undefined;
}


function generatePath([method, $class, { verb, path: name }]: [MethodInfo, ClassInfo, HttpEndpointInfo]): [string, Path3] {
  const operation: Operation3 = {
    operationId: method.name,
    responses: {
      "200": {
        "description": "Ok",
      }
    }
  }

  switch (verb) {
    case "get": return [name, { get: operation }];
    case "post": return [name, { post: operation }];
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
