import * as ts from 'typescript';
import { WinstonLogger, createGlobalLogger } from "../telemetry/logs";
import { DecoratorInfo, MethodInfo, TypeParser, ClassInfo, ParameterInfo } from './TypeParser';
import { OpenAPI, Operation, Parameter, Path, RequestBody, Response, Schema } from './swagger';
import { APITypes, ArgSources } from '../httpServer/handler';

function isValid<T>(value: T | undefined): value is T { return value !== undefined; }

interface HttpEndpointInfo { verb: APITypes; path: string; }

function getOperonDecorator(decorated: MethodInfo | ParameterInfo | ClassInfo, name: string): DecoratorInfo | undefined {
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

  function getDefaultArgSource(verb: APITypes): ArgSources.BODY | ArgSources.QUERY {
    switch (verb) {
      case APITypes.GET: return ArgSources.QUERY;
      case APITypes.POST: return ArgSources.BODY;
    }
  }
}

function getParamName(parameter: ParameterInfo): string {
  const argName = getOperonDecorator(parameter, 'ArgName');
  if (!argName) return parameter.name;

  const nameParam = argName.args[0];
  if (nameParam && ts.isStringLiteral(nameParam)) return nameParam.text;

  throw new Error(`Unexpected ArgName argument type: ${ts.SyntaxKind[nameParam.kind]}`);
}

class OpenApiGenerator {
  constructor(readonly checker: ts.TypeChecker, readonly log: WinstonLogger) { }

  generate(classes: readonly ClassInfo[]): OpenAPI {
    const methods = classes.flatMap(c => c.methods.map(method => [method, c] as [MethodInfo, ClassInfo]));
    const handlers = methods.map(([method, _]) => {
      const http = getHttpInfo(method);
      return http ? [method, http] as [MethodInfo, HttpEndpointInfo] : undefined;
    }).filter(isValid);
    const paths = handlers.map(h => this.generatePath(h));

    return {
      // TODO: OpenAPI 3.1 support
      openapi: "3.0.3", // https://spec.openapis.org/oas/v3.0.3
      info: {
        // TODO: Where to get this info from? package.json?
        title: "Operon API",
        version: "1.0.0",
      },
      // When we are cloud hosting, we will have useful info to put here.
      servers: [],
      components: {},
      paths: Object.fromEntries(paths),
    }
  }

  generatePath([method, { verb, path }]: [MethodInfo, HttpEndpointInfo]): [string, Path] {
    // The first parameter of a handle method must be an OperonContext, which is not exposed via the API
    const sourcedParams = method.parameters.slice(1).map(p => [p, getParamSource(p, verb)] as [ParameterInfo, ArgSources]);

    const operation: Operation = {
      operationId: method.name,
      responses: {
        "200": this.generateResponse(method),
      },
      parameters: this.generateParameters(sourcedParams),
      requestBody: this.generateRequestBody(sourcedParams),
    }

    switch (verb) {
      case APITypes.GET: return [path, { get: operation }];
      case APITypes.POST: return [path, { post: operation }];
    }
  }

  generateRequestBody(sourcedParams: [ParameterInfo, ArgSources][]): import("./swagger").Reference | RequestBody | undefined {
    // BODY parameters are specified in the Operation.requestBody field
    const parameters = sourcedParams
      .filter(([_, source]) => source === ArgSources.BODY)
      .map(([parameter, _]) => [getParamName(parameter), parameter] as [name: string, ParameterInfo]);

    if (parameters.length === 0) return undefined;

    const properties = Object.fromEntries(parameters.map(([name, parameter]) => [name, this.generateSchema(parameter.type)]));
    const required = parameters.filter(([_, parameter]) => parameter.required).map(([name, _]) => name);

    return {
      required: true,
      content: {
        "application/json": {
          schema: {
            type: 'object',
            properties,
            required
          }
        }
      }
    }
  }

  generateParameters(sourcedParams: [ParameterInfo, ArgSources][]): Parameter[] {
    return sourcedParams
      // QUERY and URL parameters are specified in the Operation.parameters field
      .filter(([_, source]) => source === ArgSources.QUERY || source === ArgSources.URL)
      .map(([parameter, argSource]) => {
        if (argSource == ArgSources.URL && !parameter.required) throw new Error(`URL parameters must be required: ${parameter.name}`);

        return {
          name: getParamName(parameter),
          in: getLocation(argSource),
          required: parameter.required,
          schema: this.generateSchema(parameter.type),
        };
      });

    function getLocation(argSource: ArgSources): "query" | "path" {
      switch (argSource) {
        // case ArgSources.BODY: return <BodyParameter>{ in: 'body', ...param };
        case ArgSources.QUERY: return 'query';
        case ArgSources.URL: return 'path';
        default: throw new Error(`Unsupported Parameter ArgSource: ${argSource}`);
      }
    }
  }

  generateResponse(method: MethodInfo): Response {
    return {
      description: "Ok",
      content: {
        "application/json": {
          schema: this.generateSchema(method.returnType)
        }
      }
    }
  }

  generateSchema(type: ts.Type | undefined): Schema {
    if (!type) return { description: "not specified" };
    return { description: this.checker.typeToString(type) };
  }
}


export function generateOpenApi(program: ts.Program, logger?: WinstonLogger): OpenAPI {
  logger ??= createGlobalLogger();
  const checker = program.getTypeChecker();

  const parser = new TypeParser(program, logger);
  const classes = parser.parse();

  const generator = new OpenApiGenerator(checker, logger);
  return generator.generate(classes);
}
