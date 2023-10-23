export type DataType = 'integer' | 'number' | 'boolean' | 'string' | 'array' | 'object' | 'file' | 'undefined';

export type DataFormat = 'int32' | 'int64' | 'float' | 'double' | 'byte' | 'binary' | 'date' | 'date-time' | 'password';

export interface OpenAPI {
  openapi: '3.0.3';
  info: Info;
  servers?: Server[];
  paths: { [name: string]: Path };
  components?: Components;
  security?: SecurityRequirement;
  tags?: Tag[];
  externalDocs?: ExternalDocs;
}

export interface Info {
  title: string;
  description?: string;
  termsOfService?: string;
  contact?: Contact;
  license?: License;
  version: string;
}


export interface Contact {
  name?: string;
  email?: string;
  url?: string;
}

export interface License {
  name: string;
  url?: string;
}

export interface Server {
  url: string;
  description?: string;
  variables: { [name: string]: ServerVariable };
}

export interface ServerVariable {
  enum?: string[];
  default: string;
  description?: string;
}

export interface Components {
  schemas?: { [name: string]: Schema | Reference };
  responses?: { [name: string]: Response | Reference };
  parameters?: { [name: string]: Parameter | Reference };
  examples?: { [name: string]: Example | Reference };
  requestBodies?: { [name: string]: RequestBody | Reference };
  headers?: { [name: string]: Header | Reference };
  securitySchemes?: { [name: string]: SecurityScheme | Reference };
  links?: { [name: string]: Link | Reference };
  callbacks?: { [name: string]: Callback | Reference };
}

export interface Path {
  $ref?: string;
  summary?: string;
  description?: string;
  get?: Operation;
  put?: Operation;
  post?: Operation;
  delete?: Operation;
  options?: Operation;
  head?: Operation;
  patch?: Operation;
  trace?: Operation;
  servers?: Server[];
  parameters?: (Parameter | Reference)[];
}

export interface Operation {
  tags?: string[];
  summary?: string;
  description?: string;
  externalDocs?: ExternalDocs;
  operationId?: string;
  parameters?: (Parameter | Reference)[];
  requestBody?: RequestBody | Reference;
  responses: Responses;
  callbacks?: { [name: string]: Callback | Reference };
  deprecated?: boolean;
  security?: SecurityRequirement[];
  servers?: Server[];
}

export interface ExternalDocs {
  description?: string;
  url: string;
}

export interface Parameter {
  name: string;
  in: "query" | "header" | "path" | "cookie";
  description?: string;
  required?: boolean;
  deprecated?: boolean;
  allowEmptyValue?: boolean;
  style?: string;
  explode?: boolean;
  allowReserved?: boolean;
  schema?: Schema | Reference;
  example?: unknown;
  examples?: { [name: string]: Example | Reference };
  content?: { [name: string]: MediaType };
}

export interface RequestBody {
  description?: string;
  content: { [name: string]: MediaType };
  required?: boolean;
}

export interface MediaType {
  schema?: Schema | Reference;
  example?: unknown;
  examples?: { [name: string]: Example | Reference };
  encoding?: { [name: string]: Encoding };
}


export interface Encoding {
  contentType?: string;
  headers?: { [name: string]: Header | Reference };
  style?: string;
  explode?: boolean;
  allowReserved?: boolean;
}

export interface Responses {
  [name: string]: Response | Reference;
}

export interface Response {
  description: string;
  headers?: { [name: string]: Header | Reference };
  content?: { [name: string]: MediaType };
  links?: { [name: string]: Link | Reference };
}

export interface Callback {
  [name: string]: Path;
}

export interface Example {
  summary?: string;
  description?: string;
  value?: unknown;
  externalValue?: string;
}

export interface Link {
  operationRef?: string;
  operationId?: string;
  parameters?: { [name: string]: unknown };
  requestBody?: unknown;
  description?: string;
  server?: Server;
}

export type Header = Omit<Parameter, 'name' | 'in'>;

export interface Tag {
  name: string;
  description?: string;
  externalDocs?: ExternalDocs;
}

export interface Reference {
  $ref: string;
}

export interface Schema {
  nullable?: boolean;
  discriminator?: Discriminator;
  readOnly?: boolean;
  writeOnly?: boolean;
  xml?: XML;
  externalDocs?: ExternalDocs;
  example?: unknown;
  deprecated?: boolean;

  title?: string;
  multipleOf?: number;
  maximum?: number;
  exclusiveMaximum?: number;
  minimum?: number;
  exclusiveMinimum?: number;
  maxLength?: number;
  minLength?: number;
  pattern?: string;
  maxItems?: number;
  minItems?: number;
  uniqueItems?: boolean;
  maxProperties?: number;
  minProperties?: number;
  required?: string[];
  enum?: unknown[];

  type?: DataType;
  allOf?: (Schema | Reference)[];
  oneOf?: (Schema | Reference)[];
  anyOf?: (Schema | Reference)[];
  not?: Schema | Reference;
  items?: Schema | Reference;
  properties?: { [name: string]: Schema | Reference };
  additionalProperties?: boolean | Schema | Reference;
  description?: string;
  format?: DataFormat;
  default?: unknown;

}

export interface Discriminator {
  propertyName: string;
  mapping?: { [name: string]: string };
}

export interface XML {
  name?: string;
  namespace?: string;
  prefix?: string;
  attribute?: boolean;
  wrapped?: boolean;
}

export interface SecurityScheme {
  type: string;
  description?: string;
  name?: string;
  in?: string;
  scheme?: string;
  bearerFormat?: string;
  flows?: OAuthFlows;
  openIdConnectUrl?: string;
}

export interface OAuthFlows {
  implicit?: OAuthFlow;
  password?: OAuthFlow;
  clientCredentials?: OAuthFlow;
  authorizationCode?: OAuthFlow;
}

export interface OAuthFlow {
  authorizationUrl?: string;
  tokenUrl?: string;
  refreshUrl?: string;
  scopes: { [name: string]: string };
}

export interface SecurityRequirement {
  [name: string]: string[];
}
