import * as ts from 'typescript';
import { WinstonLogger, createGlobalLogger } from "../telemetry/logs";
import { DecoratorInfo, MethodInfo, TypeParser, ClassInfo, ParameterInfo } from './TypeParser';
import { APITypes, ArgSources } from '../httpServer/handler';
import { createParser, createFormatter, SchemaGenerator, SubNodeParser, BaseType, Context, ReferenceType, Schema, PrimitiveType, SubTypeFormatter, Definition, Config } from 'ts-json-schema-generator';
import { OpenAPIV3 as OpenApi3 } from 'openapi-types';
import path from 'node:path';
import fs from 'node:fs/promises';

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

// ts-json-schema-generator does not support BigInt, so OpenApiGenerator needs a custom type, formatter and parser for it
class BigIntType extends PrimitiveType {
  getId(): string { return "integer"; }
}


class BigIntKeywordParser implements SubNodeParser {
  supportsNode(node: ts.Node): boolean {
    return node.kind === ts.SyntaxKind.BigIntKeyword;
  }
  createType(_node: ts.Node, _context: Context, _reference?: ReferenceType | undefined): BaseType {
    return new BigIntType();
  }
}

class BigIntTypeFormatter implements SubTypeFormatter {
  public supportsType(type: BigIntType): boolean {
    return type instanceof BigIntType;
  }
  public getDefinition(_type: BigIntType): Definition {
    // Note, JSON Schema integer type is not constrained to a specific size, so it is a valid type for BigInt
    return { type: "integer", description: "bigint" };
  }
  public getChildren(_type: BigIntType): BaseType[] {
    return [];
  }
}

class OpenApiGenerator {
  readonly #checker: ts.TypeChecker;
  readonly #schemaGenerator: SchemaGenerator;
  readonly #schemaMap: Map<string, OpenApi3.SchemaObject | OpenApi3.ReferenceObject> = new Map();

  constructor(readonly program: ts.Program, readonly log: WinstonLogger) {
    this.#checker = program.getTypeChecker();
    const config: Config = {
      discriminatorType: 'open-api',
      encodeRefs: false
    };
    const parser = createParser(program, config, aug => aug.addNodeParser(new BigIntKeywordParser()));
    const formatter = createFormatter(config, (fmt) => fmt.addTypeFormatter(new BigIntTypeFormatter()));
    this.#schemaGenerator = new SchemaGenerator(program, parser, formatter, {});
  }

  generate(classes: readonly ClassInfo[], title: string, version: string): OpenApi3.Document {
    const handlers = classes
      .flatMap(c => c.methods)
      .map(method => {
        const http = getHttpInfo(method);
        return http ? [method, http] as [MethodInfo, HttpEndpointInfo] : undefined;
      })
      .filter(isValid);

    const paths = handlers
      .map(h => this.generatePath(h));

    const openApi: OpenApi3.Document = {
      openapi: "3.0.3", // https://spec.openapis.org/oas/v3.0.3
      info: { title, version },
      paths: Object.fromEntries(paths),
      components: {
        schemas: Object.fromEntries(this.#schemaMap)
      }
    }
    return openApi;

    // ts-json-schema-generator emits all definitions in the definitions field, but OpenApi expects them in the components.schemas field
    // I'm going to open an issue w/ ts-json-schema-generator to make this configurable.
    // const text = JSON.stringify(openApi, null, 4);
    // const newText = text.replaceAll("#/definitions/", "#/components/schemas/");
    // return JSON.parse(newText) as OpenApi3.Document;
  }

  generatePath([method, { verb, path }]: [MethodInfo, HttpEndpointInfo]): [string, OpenApi3.PathItemObject] {
    const sourcedParams = method.parameters
      // The first parameter of a handle method must be an OperonContext, which is not exposed via the API
      .slice(1)
      .map(p => [p, getParamSource(p, verb)] as [ParameterInfo, ArgSources]);

    const parameters = this.generateParameters(sourcedParams);
    const requestBody = this.generateRequestBody(sourcedParams);

    const operation: OpenApi3.OperationObject = {
      operationId: method.name,
      responses: Object.fromEntries([this.generateResponse(method)]),
      parameters,
      requestBody,
    }

    // validate all path parameters have matching parameters with URL ArgSource
    const pathParams = path.split('/')
      .filter(p => p.startsWith(':'))
      .map(p => p.substring(1));

    for (const pathParam of pathParams) {
      const param = sourcedParams.find(([parameter, _]) => parameter.name === pathParam);
      if (!param) throw new Error(`Missing path parameter ${pathParam} for ${method.name}`);
      if (param[1] !== ArgSources.URL) throw new Error(`Path parameter ${pathParam} must be a URL parameter: ${method.name}`);
    }

    // OpenAPI indicates path parameters with curly braces, but Operon uses colons
    path = path.split('/')
      .map(p => p.startsWith(':') ? `{${p.substring(1)}}` : p)
      .join('/');

    switch (verb) {
      case APITypes.GET: return [path, { get: operation }];
      case APITypes.POST: return [path, { post: operation }];
    }
  }

  generateResponse(method: MethodInfo): [string, OpenApi3.ResponseObject] {
    const signature = this.#checker.getSignatureFromDeclaration(method.node);
    const returnType = signature?.getReturnType();
    if (!returnType) throw new Error(`Method ${method.name} has no return type`);

    // Operon handlers must return a promise. Since this is enforced by the type system
    // extract the first type argument w/o checking if it's a promise
    const typeArg = (returnType as ts.TypeReference).typeArguments![0];

    // if the type argument is void, return a 204 status code with no content
    if (typeArg.flags & ts.TypeFlags.Void) return ["204", { description: "No Content" }];

    const decl = signature ? this.#checker.signatureToSignatureDeclaration(signature, method.node.kind, undefined, undefined) : undefined;
    const schema = this.generateSchema(decl?.type);

    return ["200", {
      description: "Ok",
      content: {
        "application/json": { schema }
      }
    }];
  }

  generateParameters(sourcedParams: [ParameterInfo, ArgSources][]): OpenApi3.ParameterObject[] | undefined {
    if (sourcedParams.length === 0) return undefined;
    return sourcedParams
      // QUERY and URL parameters are specified in the Operation.parameters field
      .filter(([_, source]) => source === ArgSources.QUERY || source === ArgSources.URL)
      .map(([parameter, argSource]) => this.generateParameter(parameter, argSource));
  }

  generateParameter(parameter: ParameterInfo, argSource: ArgSources): OpenApi3.ParameterObject {
    if (argSource === ArgSources.BODY) throw new Error(`BODY parameters must be specified in the Operation.requestBody field: ${parameter.name}`);
    if (argSource === ArgSources.URL && !parameter.required) throw new Error(`URL parameters must be required: ${parameter.name}`);

    const schema = this.generateSchema(parameter.node);

    return {
      name: getParamName(parameter),
      in: getLocation(argSource),
      required: parameter.required,
      schema
    };

    function getLocation(argSource: ArgSources): "query" | "path" {
      switch (argSource) {
        case ArgSources.QUERY: return 'query';
        case ArgSources.URL: return 'path';
        default: throw new Error(`Unsupported Parameter ArgSource: ${argSource}`);
      }
    }
  }

  generateRequestBody(sourcedParams: [ParameterInfo, ArgSources][]): OpenApi3.RequestBodyObject | undefined {
    // BODY parameters are specified in the Operation.requestBody field
    const parameters = sourcedParams
      .filter(([_, source]) => source === ArgSources.BODY)
      .map(([parameter, _]) => [getParamName(parameter), parameter] as [name: string, ParameterInfo]);

    if (parameters.length === 0) return undefined;

    const properties = parameters.map(([name, parameter]) => [name, this.generateSchema(parameter.node)] as [string, OpenApi3.SchemaObject]);
    const schema: OpenApi3.SchemaObject = {
      type: 'object',
      properties: Object.fromEntries(properties),
      required: parameters.filter(([_, parameter]) => parameter.required).map(([name, _]) => name),
    }

    return {
      required: true,
      content: {
        "application/json": { schema }
      }
    }
  }

  generateSchema(node?: ts.Node): OpenApi3.SchemaObject | OpenApi3.ReferenceObject {
    if (!node) return { description: "Undefined Node" };

    const schema = this.#schemaGenerator.createSchemaFromNodes([node]);
    if (!schema.$ref) return { description: "No $ref" };

    const slashIndex = schema.$ref.lastIndexOf('/');
    if (slashIndex === -1) return { description: `Invalid $ref ${schema.$ref}` };
    const name = schema.$ref.substring(slashIndex + 1);

    const defs = new Map<string, Schema | boolean>(Object.entries(schema.definitions ?? {}));
    if (defs.size === 0) return { description: "No definitions" };
    const def = defs.get(name);

    if (!def) return { description: `No definition ${name}` };
    if (typeof def === 'boolean') return { description: `Definition ${name} is a boolean` };

    if (defs.size > 1) {
      defs.delete(name);
      for (const [$name, $def] of defs) {
        if (typeof $def === 'boolean') continue;
        this.#schemaMap.set($name, mapSchema($def));
      }
    }

    return mapSchema(def);
  }
}

function mapSchema(schema: Schema): OpenApi3.SchemaObject | OpenApi3.ReferenceObject {

  if (Array.isArray(schema.type)) throw new Error(`OpenApi 3.0.x doesn't support type arrays: ${JSON.stringify(schema.type)}`);

  if (schema.$ref) {
    const $ref = schema.$ref.replace("#/definitions/", "#/components/schemas/");
    return <OpenApi3.ReferenceObject>{ $ref }
  }

  const [maximum, exclusiveMaximum] = getMaxes();
  const [minimum, exclusiveMinimum] = getMins();

  const base: OpenApi3.BaseSchemaObject = {
    title: schema.title,
    multipleOf: schema.multipleOf,
    maximum,
    exclusiveMaximum,
    minimum,
    exclusiveMinimum,
    maxLength: schema.maxLength,
    minLength: schema.minLength,
    pattern: schema.pattern,
    maxItems: schema.maxItems,
    minItems: schema.minItems,
    uniqueItems: schema.uniqueItems,
    maxProperties: schema.maxProperties,
    minProperties: schema.minProperties,
    required: schema.required,
    enum: schema.enum,
    description: schema.description,
    format: schema.format,
    default: schema.default,

    // OpenApi 3.0.x doesn't support boolean schema types, so filter those out when mapping these fields
    allOf: schema.allOf?.filter(isSchema).map(mapSchema),
    oneOf: schema.oneOf?.filter(isSchema).map(mapSchema),
    anyOf: schema.anyOf?.filter(isSchema).map(mapSchema),
    not: schema.not
      ? isSchema(schema.not) ? mapSchema(schema.not) : undefined
      : undefined,
    properties: schema.properties
      ? Object.fromEntries(
        Object.entries(schema.properties)
          .filter((entry): entry is [string, Schema] => isSchema(entry[1]) )
          .map(([name, prop]) => [name, mapSchema(prop)]))
      : undefined,
    additionalProperties: typeof schema.additionalProperties === 'object'
      ? mapSchema(schema.additionalProperties)
      : schema.additionalProperties,
  }

  if (schema.type === 'array') {
    if (schema.items === undefined) throw new Error(`Array schema has no items`);
    if (Array.isArray(schema.items)) throw new Error(`OpenApi 3.0.x doesn't support array items arrays: ${JSON.stringify(schema.items)}`);
    if (typeof schema.items === 'boolean') throw new Error(`OpenApi 3.0.x doesn't support array items booleans: ${JSON.stringify(schema.items)}`);

    return <OpenApi3.ArraySchemaObject>{
      type: schema.type,
      items: mapSchema(schema.items),
      ...base,
    }
  }

  return <OpenApi3.NonArraySchemaObject>{
    // OpenApi 3.0.x doesn't support null type value, so map null type to undefined and add nullable: true property
    type: schema.type === 'null' ? undefined : schema.type,
    ...base,
    nullable: schema.type === 'null' ? true : undefined,
  }

  function isSchema(schema: Schema | boolean): schema is Schema {
    return typeof schema === "object";
  }

  // Convert JSON Schema Draft 7 (used by ts-json-schema-generator) max/min and exclusive max/min values to JSON Schema Draft 5 (used by OpenAPI).
  // In Draft 5, exclusive max/min values are booleans
  // In Draft 7, exclusive max/min values are numbers

  function getMaxes(): [maximum: number | undefined, exclusiveMaximum: boolean | undefined] {
    const { maximum: max, exclusiveMaximum: xMax} = schema;

    if (max) {
      if (xMax) {
        return (xMax < max) ? [xMax, true] : [max, false];
      } else {
        return [max, false];
      }
    } else {
      return xMax ? [xMax, true] : [undefined, undefined];
    }
  }

  function getMins(): [minimum: number | undefined, exclusiveMinimum: boolean | undefined] {
    const { minimum: min, exclusiveMinimum: xMin} = schema;

    if (min) {
      if (xMin) {
        return (xMin > min) ? [xMin, true] : [min, false];
      } else {
        return [min, false];
      }

    } else {
      return xMin ? [xMin, true] : [undefined, undefined];
    }
  }
}

async function findPackageInfo(entrypoint: string): Promise<{ name: string, version: string}> {
  let dirname = path.dirname(entrypoint);
  while (dirname !== '/') {
    try {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      const packageJson = JSON.parse(await fs.readFile(path.join(dirname, 'package.json'), { encoding: 'utf-8' }));
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      const name = packageJson.name as string ?? "unknown";
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      const version = packageJson.version as string ?? "unknown";
      return { name, version };
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
      if ((error as any).code !== 'ENOENT') throw error;
    }
    dirname = path.dirname(dirname);
  }
  return { name: "unknown", version: "unknown" };
}

export async function generateOpenApi(entrypoint: string, logger?: WinstonLogger): Promise<OpenApi3.Document | undefined> {
  logger ??= createGlobalLogger();

  const { name, version } = await findPackageInfo(entrypoint);
  const program = ts.createProgram([entrypoint], {});
  const classes = TypeParser.parse(program, logger);
  const generator = new OpenApiGenerator(program, logger);
  return generator.generate(classes, name, version);
}
