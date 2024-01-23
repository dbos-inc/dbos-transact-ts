import ts from 'typescript';
import {
  DecoratorInfo,
  MethodInfo,
  TypeParser,
  ClassInfo,
  ParameterInfo,
  findPackageInfo,
} from './TypeParser';

import { ArgSources } from "../../src/httpServer/handlerTypes";
import { APITypes } from "../../src/httpServer/handlerTypes";

import {
  createParser,
  createFormatter,
  SchemaGenerator,
  SubNodeParser,
  BaseType,
  Context,
  ReferenceType,
  Schema,
  PrimitiveType,
  SubTypeFormatter,
  Definition,
  Config,
} from 'ts-json-schema-generator';

import { OpenAPIV3 as OpenApi3 } from 'openapi-types';
import { diagResult, logDiagnostics, DiagnosticsCollector } from './tsDiagUtil';

function isValid<T>(value: T | undefined): value is T { return value !== undefined; }

interface HttpEndpointInfo { verb: APITypes; path: string; }

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

// type guard functions for ts.Type/TypeNode
function isNodeWithTypeArguments(node: ts.TypeNode): node is ts.NodeWithTypeArguments {
  return "typeArguments" in node;
}

function isObjectType(node: ts.Type): node is ts.ObjectType {
  return !!(node.flags & ts.TypeFlags.Object);
}

function isTypeReference(node: ts.Type): node is ts.TypeReference {
  return isObjectType(node) && !!(node.objectFlags & ts.ObjectFlags.Reference);
}

// workflow UUID header parameter info which is added to every generated endpoint
const workflowUuidParamName = "dbosWorkflowUUID";
const workflowUuidRef: OpenApi3.ReferenceObject = { $ref: `#/components/parameters/${workflowUuidParamName}` }
const workflowUuidParam: readonly [string, OpenApi3.ParameterObject] = [workflowUuidParamName, {
  name: 'dbos-workflowuuid',
  in: 'header',
  required: false,
  description: "Caller specified [workflow idempotency key](https://docs.dbos.dev/tutorials/idempotency-tutorial#setting-idempotency-keys)",
  schema: { type: 'string' },
}] as const;

export class OpenApiGenerator {
  readonly #checker: ts.TypeChecker;
  readonly #schemaGenerator: SchemaGenerator;
  readonly #schemaMap: Map<string, OpenApi3.SchemaObject | OpenApi3.ReferenceObject> = new Map();
  readonly #securitySchemeMap: Map<string, OpenApi3.SecuritySchemeObject | OpenApi3.ReferenceObject> = new Map();
  readonly #diags = new DiagnosticsCollector();
  get diags() { return this.#diags.diags; }

  constructor(readonly program: ts.Program) {
    this.#checker = program.getTypeChecker();
    const config: Config = {
      discriminatorType: 'open-api',
      encodeRefs: false
    };
    const parser = createParser(program, config, aug => aug.addNodeParser(new BigIntKeywordParser()));
    const formatter = createFormatter(config, (fmt) => fmt.addTypeFormatter(new BigIntTypeFormatter()));
    this.#schemaGenerator = new SchemaGenerator(program, parser, formatter, {});
  }

  generate(classes: readonly ClassInfo[], title: string, version: string): OpenApi3.Document | undefined {
    const paths = new Array<[string, OpenApi3.PathItemObject]>();
    classes.forEach((cls, index) => {
      // if the class name is not specified, manufacture a name using the class index as the prefix
      // JS class identifiers cannot start with a digit but OpenApi security scheme names can
      const securitySchemeName = cls.name ? `${cls.name}Auth` : `${index}ClassAuth`;
      const securitySchemeDecorator = this.getDBOSDecorator(cls, 'OpenApiSecurityScheme');
      const securityScheme = this.parseSecurityScheme(securitySchemeDecorator?.args[0]);
      const defaultRoles = this.parseStringLiteralArray(this.getDBOSDecorator(cls, 'DefaultRequiredRole')?.args[0]) ?? [];

      if (securityScheme) {
        this.#securitySchemeMap.set(securitySchemeName, securityScheme);
      }

      for (const method of cls.methods) {
        const http = this.getHttpInfo(method);
        if (!http) continue;
        const path = this.generatePath(method, http, defaultRoles, securityScheme ? securitySchemeName : undefined);
        if (path) paths.push(path);
      }
    })

    const openApi: OpenApi3.Document = {
      openapi: "3.0.3", // https://spec.openapis.org/oas/v3.0.3
      info: { title, version },
      paths: Object.fromEntries(paths),
      components: {
        parameters: Object.fromEntries([workflowUuidParam]),
        schemas: Object.fromEntries(this.#schemaMap),
        securitySchemes: Object.fromEntries(this.#securitySchemeMap)
      }
    }
    return diagResult(openApi, this.diags);
  }

  generatePath(method: MethodInfo, { verb, path } : HttpEndpointInfo, defaultRoles: readonly string[], securityScheme: string | undefined): [string, OpenApi3.PathItemObject] | undefined {
    const sourcedParams = method.parameters
      // The first parameter of a handle method must be an DBOSContext, which is not exposed via the API
      .slice(1)
      .map(p => [p, this.getParamSource(p, verb)] as [ParameterInfo, ArgSources]);

    const parameters: Array<OpenApi3.ReferenceObject | OpenApi3.ParameterObject> = this.generateParameters(sourcedParams);
    // add optional parameter for workflow UUID header
    parameters.push(workflowUuidRef);

    const requestBody = this.generateRequestBody(sourcedParams);
    const response = this.generateResponse(method);
    if (!response) return;

    // if there is a security scheme specified, create a security requirement for it
    // unless the method has no required roles
    const security = new Array<OpenApi3.SecurityRequirementObject>();
    if (securityScheme) {
      const roles = this.parseStringLiteralArray(this.getDBOSDecorator(method, 'RequiredRole')?.args[0]) ?? defaultRoles;
      if (roles.length > 0) {
        security.push(<OpenApi3.SecurityRequirementObject>{ [securityScheme]: [] });
      }
    }

    const operation: OpenApi3.OperationObject = {
      operationId: method.name,
      responses: Object.fromEntries([response]),
      parameters,
      requestBody,
      security: security.length > 0 ? security : undefined,
    }

    // validate all path parameters have matching parameters with URL ArgSource
    const pathParams = path.split('/')
      .filter(p => p.startsWith(':'))
      .map(p => p.substring(1));

    for (const pathParam of pathParams) {
      const param = sourcedParams.find(([parameter, _]) => parameter.name === pathParam);
      if (!param) {
        this.#diags.raise(`Missing path parameter ${pathParam} for ${method.name}`, method.node);
        return;
      }
      if (param[1] !== ArgSources.URL) {
        this.#diags.raise(`Path parameter ${pathParam} must be a URL parameter: ${method.name}`, param[0].node);
        return;
      }
    }

    // OpenAPI indicates path parameters with curly braces, but DBOS uses colons
    const $path = path.split('/')
      .map(p => p.startsWith(':') ? `{${p.substring(1)}}` : p)
      .join('/');

    switch (verb) {
      case APITypes.GET: return [$path, { get: operation }];
      case APITypes.POST: return [$path, { post: operation }];
    }
  }

  getMethodReturnType(node: ts.MethodDeclaration): ts.Type | undefined {
    // if the method has a declared return type, try to use that
    if (node.type) {
      if (!isNodeWithTypeArguments(node.type)) {
        this.#diags.warn(`Unexpected type node kind ${ts.SyntaxKind[node.type.kind]}`, node.type);
      } else {
        // return type must be a Promise<T>, so take the first type argument
        const typeArg = node.type.typeArguments?.[0];
        if (typeArg) return this.#checker.getTypeFromTypeNode(typeArg);

        const printer = ts.createPrinter();
        const text = printer.printNode(ts.EmitHint.Unspecified, node.type, node.getSourceFile());
        this.#diags.warn(`Unexpected return type without at least one type argument ${text}`, node.type);
      }
    }

    // if the return type is inferred or the Promise type argument could not be determined,
    // infer the return type via the type checker
    const signature = this.#checker.getSignatureFromDeclaration(node);
    const returnType = signature?.getReturnType();
    if (!returnType || !isTypeReference(returnType)) return undefined;
    // again, return type must be a Promise<T>, so take the first type argument
    return returnType.typeArguments?.[0];
  }

  generateResponse(method: MethodInfo): [string, OpenApi3.ResponseObject] | undefined {

    const returnType = this.getMethodReturnType(method.node);
    if (!returnType) {
      this.#diags.raise(`Could not determine return type of ${method.name}`, method.node);
      return;
    }

    if (returnType.flags & ts.TypeFlags.VoidLike) {
      return ["204", { description: "No Content" }];
    }

    const typeNode = this.#checker.typeToTypeNode(returnType, undefined, undefined);
    if (!typeNode) {
      this.#diags.warn(`Could not determine return type node of ${method.name}`, method.node);
    }

    return ["200", {
      description: "Ok",
      content: {
        "application/json": {
          schema: this.generateSchema(typeNode)
        }
      }
    }];
  }

  generateParameters(sourcedParams: [ParameterInfo, ArgSources][]): OpenApi3.ParameterObject[] {
    if (sourcedParams.length === 0) return [];
    return sourcedParams
      // QUERY and URL parameters are specified in the Operation.parameters field
      .filter(([_, source]) => source === ArgSources.QUERY || source === ArgSources.URL)
      .map(([parameter, argSource]) => this.generateParameter(parameter, argSource))
      .filter(isValid);
  }

  generateParameter(parameter: ParameterInfo, argSource: ArgSources): OpenApi3.ParameterObject | undefined {
    if (argSource === ArgSources.BODY) {
      this.#diags.raise(`BODY parameters must be specified in the Operation.requestBody field: ${parameter.name}`, parameter.node);
      return;
    }
    if (argSource === ArgSources.URL && !parameter.required) {
      this.#diags.raise(`URL parameters must be required: ${parameter.name}`, parameter.node);
      return;
    }

    const schema = this.generateSchema(parameter.node);
    const name = this.getParamName(parameter);
    const location = this.getLocation(parameter, argSource);
    if (!name || !location) return;

    return {
      name,
      in: location,
      required: parameter.required,
      schema
    };

  }

  getLocation(parameter: ParameterInfo, argSource: ArgSources): "query" | "path" | undefined {
    switch (argSource) {
      case ArgSources.QUERY: return 'query';
      case ArgSources.URL: return 'path';
      default:
        this.#diags.raise(`Unsupported Parameter ArgSource: ${argSource}`, parameter.node);
        return;
    }
  }

  generateRequestBody(sourcedParams: [ParameterInfo, ArgSources][]): OpenApi3.RequestBodyObject | undefined {
    // BODY parameters are specified in the Operation.requestBody field
    const parameters = sourcedParams
      .filter(([_, source]) => source === ArgSources.BODY)
      .map(([parameter, _]) => [this.getParamName(parameter), parameter] as [name: string, ParameterInfo]);

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

  generateSchema(node?: ts.Node): OpenApi3.SchemaObject | OpenApi3.ReferenceObject | undefined {
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
        const $schema = this.mapSchema($def);
        if ($schema) this.#schemaMap.set($name, $schema);
      }
    }

    return this.mapSchema(def);
  }

  getDBOSDecorator(decorated: MethodInfo | ParameterInfo | ClassInfo, name: string): DecoratorInfo | undefined {
    const filtered = decorated.decorators.filter(d => d.module === '@dbos-inc/dbos-sdk' && d.name === name);
    if (filtered.length === 0) return undefined;
    if (filtered.length > 1) {
      this.#diags.raise(`Multiple ${JSON.stringify(name)} decorators found on ${decorated.name ?? "<unknown>"}`, decorated.node);
    }
    return filtered[0];
  }

  getHttpInfo(method: MethodInfo): HttpEndpointInfo | undefined {
    const getApiDecorator = this.getDBOSDecorator(method, 'GetApi');
    const postApiDecorator = this.getDBOSDecorator(method, 'PostApi');
    if (getApiDecorator && postApiDecorator) {
      this.#diags.raise(`Method ${method.name} has both GetApi and PostApi decorators`);
      return;
    }
    if (!getApiDecorator && !postApiDecorator) return undefined;

    const verb = getApiDecorator ? APITypes.GET : APITypes.POST;
    const arg = getApiDecorator ? getApiDecorator.args[0] : postApiDecorator?.args[0];
    if (!arg) {
      this.#diags.raise(`Missing path argument for ${verb}Api decorator`, method.node);
      return;
    }
    if (!ts.isStringLiteral(arg)) {
      this.#diags.raise(`Unexpected path argument type: ${ts.SyntaxKind[arg.kind]}`, method.node);
      return;
    }
    return { verb, path: arg.text };
  }

  getParamSource(parameter: ParameterInfo, verb: APITypes): ArgSources.BODY | ArgSources.QUERY | ArgSources.URL | undefined {
    const argSource = this.getDBOSDecorator(parameter, 'ArgSource');
    if (!argSource) return getDefaultArgSource(verb);

    if (!ts.isPropertyAccessExpression(argSource.args[0])) {
      this.#diags.raise(`Unexpected ArgSource argument type: ${ts.SyntaxKind[argSource.args[0].kind]}`, parameter.node);
      return;
    }
    switch (argSource.args[0].name.text) {
      case "BODY": return ArgSources.BODY;
      case "QUERY": return ArgSources.QUERY;
      case "URL": return ArgSources.URL;
      case "DEFAULT": return getDefaultArgSource(verb);
      default:
        this.#diags.raise(`Unexpected ArgSource argument: ${argSource.args[0].name.text}`, parameter.node);
        return;
    }

    function getDefaultArgSource(verb: APITypes): ArgSources.BODY | ArgSources.QUERY {
      switch (verb) {
        case APITypes.GET: return ArgSources.QUERY;
        case APITypes.POST: return ArgSources.BODY;
      }
    }
  }

  getParamName(parameter: ParameterInfo): string | undefined {
    const argName = this.getDBOSDecorator(parameter, 'ArgName');
    if (!argName) return parameter.name;

    const nameParam = argName.args[0];
    if (nameParam && ts.isStringLiteral(nameParam)) return nameParam.text;

    this.#diags.raise(`Unexpected ArgName argument type: ${ts.SyntaxKind[nameParam.kind]}`, parameter.node);
  }

  mapSchema(schema: Schema): OpenApi3.SchemaObject | OpenApi3.ReferenceObject | undefined {

    if (Array.isArray(schema.type)) {
      this.#diags.raise(`OpenApi 3.0.x doesn't support type arrays: ${JSON.stringify(schema.type)}`);
      return;
    }

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
      allOf: schema.allOf?.filter(isSchema).map(s => this.mapSchema(s)).filter(isValid),
      oneOf: schema.oneOf?.filter(isSchema).map(s => this.mapSchema(s)).filter(isValid),
      anyOf: schema.anyOf?.filter(isSchema).map(s => this.mapSchema(s)).filter(isValid),
      not: schema.not
        ? isSchema(schema.not) ? this.mapSchema(schema.not) : undefined
        : undefined,
      properties: schema.properties
        ? Object.fromEntries(
          Object.entries(schema.properties)
            .filter((entry): entry is [string, Schema] => isSchema(entry[1]))
            .map(([name, prop]) => [name, this.mapSchema(prop)])
            .filter((entry): entry is [string, OpenApi3.SchemaObject | OpenApi3.ReferenceObject] => isValid(entry[1])))
        : undefined,
      additionalProperties: typeof schema.additionalProperties === 'object'
        ? this.mapSchema(schema.additionalProperties)
        : schema.additionalProperties,
    }

    if (schema.type === 'array') {
      if (schema.items === undefined) {
        this.#diags.raise(`Array schema has no items`);
        return;
      }
      if (Array.isArray(schema.items)) {
        this.#diags.raise(`OpenApi 3.0.x doesn't support array items arrays: ${JSON.stringify(schema.items)}`);
        return;
      }
      if (typeof schema.items === 'boolean') {
        this.#diags.raise(`OpenApi 3.0.x doesn't support array items booleans: ${JSON.stringify(schema.items)}`);
        return;
      }

      return <OpenApi3.ArraySchemaObject>{
        type: schema.type,
        items: this.mapSchema(schema.items),
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
      const { maximum: max, exclusiveMaximum: xMax } = schema;

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
      const { minimum: min, exclusiveMinimum: xMin } = schema;

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

  parseStringLiteralArray(node: ts.Expression | undefined): string[] | undefined {
    if (!node) return undefined;
    if (!ts.isArrayLiteralExpression(node)) {
      this.#diags.raise(`Expected Array Literal node, received: ${ts.SyntaxKind[node.kind]}`, node);
      return;
    }

    const values = new Array<string>();
    for (const exp of node.elements) {
      if (ts.isStringLiteral(exp)) {
        values.push(exp.getText());
      } else {
        this.#diags.raise(`Expected String Literal node, received: ${ts.SyntaxKind[exp.kind]}`, exp);
      }
    }
    return values;
  }

  parseSecurityScheme(arg?: ts.Expression): SecurityScheme | undefined {
    if (!arg) return undefined;
    if (!ts.isObjectLiteralExpression(arg)) {
      this.#diags.raise(`Expected Object Literal node, received: ${ts.SyntaxKind[arg.kind]}`, arg);
      return undefined;
    }

    const map = new Map<string, string>();
    for (const prop of arg.properties) {
      if (!ts.isPropertyAssignment(prop)) {
        this.#diags.raise(`Expected Property Assignment node, received: ${ts.SyntaxKind[prop.kind]}`, prop);
        continue;
      }

      if (!ts.isStringLiteral(prop.initializer)) {
        this.#diags.raise(`Expected String Literal node, received: ${ts.SyntaxKind[prop.initializer.kind]}`, prop.initializer);
        continue;
      }

      map.set(prop.name.getText(), prop.initializer.text);
    }

    const type = map.get("type");
    const description = map.get("description");
    if (!type) {
      this.#diags.raise(`missing type property`, arg);
      return;
    }

    if (type === 'http') {
      const scheme = map.get("scheme");
      if (!scheme) {
        this.#diags.raise(`missing scheme property`, arg);
        return;
      }
      const bearerFormat = map.get("bearerFormat");

      return <OpenApi3.HttpSecurityScheme>{ type, scheme, bearerFormat, description };
    }

    if (type === 'apiKey') {
      const name = map.get("name");
      if (!name) {
        this.#diags.raise(`missing name property`, arg);
        return;
      }
      const $in = map.get("id");
      if (!$in) {
        this.#diags.raise(`missing in property`, arg);
        return;
      }

      return <OpenApi3.ApiKeySecurityScheme>{ type, name, in: $in, description };
    }

    if (type === "openIdConnect") {
      const openIdConnectUrl = map.get("openIdConnectUrl");
      if (!openIdConnectUrl) {
        this.#diags.raise(`missing openIdConnectUrl property`, arg);
        return;
      }
      return <OpenApi3.OpenIdSecurityScheme>{ type, openIdConnectUrl, description };
    }


    if (type === 'oauth2') {
      this.#diags.raise(`OAuth2 Security Scheme not supported`, arg);
    } else {
      this.#diags.raise(`unrecognized Security Scheme type ${type}`, arg);
    }
    return;
  }
}

type SecurityScheme = Exclude<OpenApi3.SecuritySchemeObject, OpenApi3.OAuth2SecurityScheme>;

export async function generateOpenApi(entrypoint: string): Promise<OpenApi3.Document | undefined> {

  const program = ts.createProgram([entrypoint], {});

  const parser = new TypeParser(program);
  const classes = parser.parse();
  logDiagnostics(parser.diags);
  if (!classes || classes.length === 0) return undefined;

  const { name, version } = await findPackageInfo([entrypoint]);
  const generator = new OpenApiGenerator(program);
  const openapi = generator.generate(classes, name, version);
  logDiagnostics(generator.diags);
  return openapi;
}
