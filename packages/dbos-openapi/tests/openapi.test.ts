import ts from "typescript";
import { TypeParser } from "../TypeParser";
import { OpenApiGenerator } from "../openApi";
import path from "node:path";
import { makeTestTypescriptProgram } from "./makeProgram";

const printer = ts.createPrinter();

const entrypoint1 = path.join(__dirname, "./hello-contexts/operations.ts");
const program1 = ts.createProgram([entrypoint1], {});

const entrypoint2 = path.join(__dirname, "./hello/operations.ts");
const program2 = ts.createProgram([entrypoint2], {});

describe("TypeParser", () => {
  it("examples/hello-context", () => {
    const parser = new TypeParser(program1);
    const classes = parser.parse();
    expect(parser.diags.length).toBe(0);
    expect(classes).toBeDefined();
    expect(classes!.length).toBe(1);

    const [$class] = classes!;
    expect($class.name).toBe("Hello");
    expect($class.decorators.length).toBe(0);
    expect($class.methods.length).toBe(3);

    const method = $class.methods[0].name === "helloTransaction" ? $class.methods[0] :
                   $class.methods[1].name === "helloTransaction" ? $class.methods[1] :
                   $class.methods[2];
    expect(method.name).toBe("helloTransaction");
    expect(method.decorators.length).toBe(2);

    const [dec1, dec2] = method.decorators;
    expect(dec1.name).toBe("GetApi");
    expect(dec1.args.length).toBe(1);
    const [arg1] = dec1.args;
    expect(arg1.kind).toBe(ts.SyntaxKind.StringLiteral);
    expect((arg1 as ts.StringLiteral).text).toBe("/greeting/:user");

    expect(dec2.name).toBe("Transaction");
    expect(dec2.args.length).toBe(0);

    expect(method.parameters.length).toBe(2);
    const [param1, param2] = method.parameters;
    expect(param1.name).toBe("ctxt");
    expect(param1.decorators.length).toBe(0);

    expect(param2.name).toBe("user");
    expect(param2.decorators.length).toBe(1);
    const [dec3] = param2.decorators;
    expect(dec3.name).toBe("ArgSource");

    const dec3param = printer.printNode(ts.EmitHint.Unspecified, dec3.args[0], dec3.args[0].getSourceFile());
    expect(dec3param).toBe("ArgSources.URL");
  });

  it("examples/hello", () => {
    const parser = new TypeParser(program2);
    const classes = parser.parse();
    expect(parser.diags.length).toBe(0);
    expect(classes).toBeDefined();
    expect(classes!.length).toBe(1);

    const [$class] = classes!;
    expect($class.name).toBe("Hello");
    expect($class.decorators.length).toBe(0);
    expect($class.methods.length).toBe(3);

    const method = $class.methods[0].name === "helloTransaction" ? $class.methods[0] :
                   $class.methods[1].name === "helloTransaction" ? $class.methods[1] :
                   $class.methods[2];
    expect(method.name).toBe("helloTransaction");
    expect(method.decorators.length).toBe(2);

    const [dec1, dec2] = method.decorators;
    expect(dec1.className).toBe("DBOS");
    expect(dec1.module).toBe('@dbos-inc/dbos-sdk');
    expect(dec1.name).toBe("getApi");
    expect(dec1.args.length).toBe(1);
    const [arg1] = dec1.args;
    expect(arg1.kind).toBe(ts.SyntaxKind.StringLiteral);
    expect((arg1 as ts.StringLiteral).text).toBe("/greeting/:user");

    expect(dec2.className).toBe("DBOS");
    expect(dec2.module).toBe('@dbos-inc/dbos-sdk');
    expect(dec2.name).toBe("transaction");
    expect(dec2.args.length).toBe(0);

    expect(method.parameters.length).toBe(1);
    const [param2] = method.parameters;

    expect(param2.name).toBe("user");
    expect(param2.decorators.length).toBe(1);
    const [dec3] = param2.decorators;
    expect(dec3.name).toBe("ArgSource");

    const dec3param = printer.printNode(ts.EmitHint.Unspecified, dec3.args[0], dec3.args[0].getSourceFile());
    expect(dec3param).toBe("ArgSources.URL");
  });
});


describe("OpenApiGenerator", () => {

  it("examples/hello", () => {
    const expected = {
      openapi: "3.0.3",
      info: {
        title: "dbos-hello",
        version: "0.0.1"
      },
      paths: {
        "/greeting/{user}": {
          get: {
            operationId: "helloTransaction",
            responses: {
              "200": {
                description: "Ok",
                content: {
                  "application/json": {
                    schema: {
                      type: "string"
                    }
                  }
                }
              }
            },
            parameters: [
              {
                name: "user",
                in: "path",
                required: true,
                schema: {
                  type: "string"
                }
              },
              {
                "$ref": "#/components/parameters/dbosWorkflowUUID"
              }
            ]
          }
        }
      },
      components: {
        schemas: {},
        parameters: {
          dbosWorkflowUUID: {
            name: "dbos-idempotency-key",
            in: "header",
            required: false,
            schema: {
              type: "string"
            }
          }
        }
      }
    };

    const parser = new TypeParser(program1);
    const classes = parser.parse();
    expect(parser.diags.length).toBe(0);
    expect(classes).toBeDefined();
    expect(classes!.length).toBe(1);

    const generator = new OpenApiGenerator(program1);
    const openApi = generator.generate(classes!, "dbos-hello", "0.0.1");
    expect(generator.diags.length).toBe(0);

    expect(openApi).toBeDefined();
    expect(openApi).toMatchObject(expected);
  });

  it("OpenApiSecurityScheme RequiredRole", () => {
    const source = /*javascript*/`
    import { TransactionContext, Transaction, ArgSource, ArgSources, OpenApiSecurityScheme, RequiredRole } from '@dbos-inc/dbos-sdk';
    import * as dbos from '@dbos-inc/dbos-sdk';
    import dbossdk from '@dbos-inc/dbos-sdk';

    @OpenApiSecurityScheme({ type: 'http', scheme: 'bearer' })
    export class Hello {
      @dbos.GetApi('/greeting/:user')
      @RequiredRole(['user'])
      static async helloTransaction(ctxt: HandlerContext, @dbossdk.ArgSource(ArgSources.URL) user: string): Promise<string>  {
        return "";
      }
    }
    `;

    const expected = {
      openapi: "3.0.3",
      info: {
        title: "dbos-hello",
        version: "0.0.1"
      },
      paths: {
        "/greeting/{user}": {
          get: {
            operationId: "helloTransaction",
            responses: {
              200: {
                description: "Ok",
                content: {
                  "application/json": {
                    schema: {
                      type: "string"
                    }
                  }
                }
              }
            },
            parameters: [
              {
                name: "user",
                in: "path",
                required: true,
                schema: {
                  type: "string"
                }
              },
              {
                $ref: "#/components/parameters/dbosWorkflowUUID"
              }
            ],
            security: [
              {
                HelloAuth: []
              }
            ]
          }
        }
      },
      components: {
        parameters: {
          dbosWorkflowUUID: {
            name: "dbos-idempotency-key",
            in: "header",
            required: false,
            description: "Caller specified [workflow idempotency key](https://docs.dbos.dev/tutorials/idempotency-tutorial#setting-idempotency-keys)",
            schema: {
              type: "string"
            }
          }
        },
        schemas: {},
        securitySchemes: {
          HelloAuth: {
            type: "http",
            scheme: "bearer"
          }
        }
      }
    };

    const program = makeTestTypescriptProgram(source);
    const parser = new TypeParser(program);
    const classes = parser.parse();
    expect(parser.diags.length).toBe(0);
    const generator = new OpenApiGenerator(program);
    const openApi = generator.generate(classes!, "dbos-hello", "0.0.1");
    expect(generator.diags.length).toBe(0);
    expect(openApi).toBeDefined();
    expect(openApi).toMatchObject(expected);
  });

  it("OpenApiSecurityScheme RequiredRole V2", () => {
    const source = /*javascript*/`
    import { TransactionContext, Transaction, ArgSource, ArgSources, OpenApiSecurityScheme, RequiredRole, DBOS } from '@dbos-inc/dbos-sdk';
    import * as dbos from '@dbos-inc/dbos-sdk';
    import dbossdk from '@dbos-inc/dbos-sdk';

    @OpenApiSecurityScheme({ type: 'http', scheme: 'bearer' })
    export class Hello {
      @DBOS.getApi('/greeting/:user')
      @DBOS.requiredRole(['user'])
      static async helloTransaction(@dbossdk.ArgSource(ArgSources.URL) user: string): Promise<string>  {
        return "";
      }
    }
    `;

    const expected = {
      openapi: "3.0.3",
      info: {
        title: "dbos-hello",
        version: "0.0.1"
      },
      paths: {
        "/greeting/{user}": {
          get: {
            operationId: "helloTransaction",
            responses: {
              200: {
                description: "Ok",
                content: {
                  "application/json": {
                    schema: {
                      type: "string"
                    }
                  }
                }
              }
            },
            parameters: [
              {
                name: "user",
                in: "path",
                required: true,
                schema: {
                  type: "string"
                }
              },
              {
                $ref: "#/components/parameters/dbosWorkflowUUID"
              }
            ],
            security: [
              {
                HelloAuth: []
              }
            ]
          }
        }
      },
      components: {
        parameters: {
          dbosWorkflowUUID: {
            name: "dbos-idempotency-key",
            in: "header",
            required: false,
            description: "Caller specified [workflow idempotency key](https://docs.dbos.dev/tutorials/idempotency-tutorial#setting-idempotency-keys)",
            schema: {
              type: "string"
            }
          }
        },
        schemas: {},
        securitySchemes: {
          HelloAuth: {
            type: "http",
            scheme: "bearer"
          }
        }
      }
    };

    const program = makeTestTypescriptProgram(source);
    const parser = new TypeParser(program);
    const classes = parser.parse();
    expect(parser.diags.length).toBe(0);
    const generator = new OpenApiGenerator(program);
    const openApi = generator.generate(classes!, "dbos-hello", "0.0.1");
    expect(generator.diags.length).toBe(0);
    expect(openApi).toBeDefined();
    expect(openApi).toMatchObject(expected);
  });

  it("OpenApiSecurityScheme DefaultRequiredRole", () => {
    const source = /*javascript*/`
    import { TransactionContext, Transaction, GetApi, ArgSource, ArgSources, DefaultRequiredRole, OpenApiSecurityScheme } from '@dbos-inc/dbos-sdk'

    @OpenApiSecurityScheme({ type: 'http', scheme: 'bearer' })
    @DefaultRequiredRole(['user'])
    export class Hello {
      @GetApi('/greeting/:user')
      static async helloTransaction(ctxt: HandlerContext, @ArgSource(ArgSources.URL) user: string): Promise<string>  {
        return "";
      }
    }
    `;

    const expected = {
      openapi: "3.0.3",
      info: {
        title: "dbos-hello",
        version: "0.0.1"
      },
      paths: {
        "/greeting/{user}": {
          get: {
            operationId: "helloTransaction",
            responses: {
              200: {
                description: "Ok",
                content: {
                  "application/json": {
                    schema: {
                      type: "string"
                    }
                  }
                }
              }
            },
            parameters: [
              {
                name: "user",
                in: "path",
                required: true,
                schema: {
                  type: "string"
                }
              },
              {
                $ref: "#/components/parameters/dbosWorkflowUUID"
              }
            ],
            security: [
              {
                HelloAuth: []
              }
            ]
          }
        }
      },
      components: {
        parameters: {
          dbosWorkflowUUID: {
            name: "dbos-idempotency-key",
            in: "header",
            required: false,
            description: "Caller specified [workflow idempotency key](https://docs.dbos.dev/tutorials/idempotency-tutorial#setting-idempotency-keys)",
            schema: {
              type: "string"
            }
          }
        },
        schemas: {},
        securitySchemes: {
          HelloAuth: {
            type: "http",
            scheme: "bearer"
          }
        }
      }
    };

    const program = makeTestTypescriptProgram(source);
    const parser = new TypeParser(program);
    const classes = parser.parse();
    expect(parser.diags.length).toBe(0);
    const generator = new OpenApiGenerator(program);
    const openApi = generator.generate(classes!, "dbos-hello", "0.0.1");
    expect(generator.diags.length).toBe(0);
    expect(openApi).toBeDefined();
    expect(openApi).toMatchObject(expected);
  });

  it("OpenApiSecurityScheme empty RequiredRole array", () => {
    const source = /*javascript*/`
    import { DBOS, ArgSource, ArgSources, OpenApiSecurityScheme } from '@dbos-inc/dbos-sdk'

    @DBOS.defaultRequiredRole(['user'])
    @OpenApiSecurityScheme({ type: 'http', scheme: 'bearer' })
    export class Hello {
      @DBOS.getApi('/greeting/:user')
      @DBOS.requiredRole([])
      static async helloTransaction(@ArgSource(ArgSources.URL) user: string): Promise<string>  {
        return "";
      }
    }
    `;

    const expected = {
      openapi: "3.0.3",
      info: {
        title: "dbos-hello",
        version: "0.0.1"
      },
      paths: {
        "/greeting/{user}": {
          get: {
            operationId: "helloTransaction",
            responses: {
              200: {
                description: "Ok",
                content: {
                  "application/json": {
                    schema: {
                      type: "string"
                    }
                  }
                }
              }
            },
            parameters: [
              {
                name: "user",
                in: "path",
                required: true,
                schema: {
                  type: "string"
                }
              },
              {
                $ref: "#/components/parameters/dbosWorkflowUUID"
              }
            ]
          }
        }
      },
      components: {
        parameters: {
          dbosWorkflowUUID: {
            name: "dbos-idempotency-key",
            in: "header",
            required: false,
            description: "Caller specified [workflow idempotency key](https://docs.dbos.dev/tutorials/idempotency-tutorial#setting-idempotency-keys)",
            schema: {
              type: "string"
            }
          }
        },
        schemas: {},
        securitySchemes: {
          HelloAuth: {
            type: "http",
            scheme: "bearer"
          }
        }
      }
    };

    const program = makeTestTypescriptProgram(source);
    const parser = new TypeParser(program);
    const classes = parser.parse();
    expect(parser.diags.length).toBe(0);
    const generator = new OpenApiGenerator(program);
    const openApi = generator.generate(classes!, "dbos-hello", "0.0.1");
    expect(generator.diags.length).toBe(0);
    expect(openApi).toBeDefined();
    expect(openApi).toMatchObject(expected);
  });

  it("Uses multiple decorators and doesn't use a path part", () => {
    const source = /*javascript*/`
    import { DBOS, ArgSource, ArgSources, OpenApiSecurityScheme } from '@dbos-inc/dbos-sdk'

    export class Hello {
      @DBOS.getApi('/greetingget/:user')
      @DBOS.postApi('/greetingpost/:user')
      static async helloTransaction(): Promise<string>  {
        return "";
      }
    }
    `;

    const expected = {
      "components": {
        "parameters": {
          "dbosWorkflowUUID": {
            "description": "Caller specified [workflow idempotency key](https://docs.dbos.dev/tutorials/idempotency-tutorial#setting-idempotency-keys)",
            "in": "header",
            "name": "dbos-idempotency-key",
            "required": false,
            "schema": {
              "type": "string",
            },
          },
        },
        "schemas": {},
        "securitySchemes": {},
      },
      "info": {
        "title": "dbos-hello",
        "version": "0.0.1",
      },
      "openapi": "3.0.3",
      "paths": {
        "/greetingget/{user}": {
          "get": {
            "operationId": "helloTransaction",
            "parameters": [
              {
                "$ref": "#/components/parameters/dbosWorkflowUUID",
              },
            ],
          "responses": {
          "200": {
            "content": {
                "application/json": {
                  "schema": {
                      "type": "string",
                    },
                  },
                },
                "description": "Ok",
              },
            },
          },
        },
        "/greetingpost/{user}": {
          "post": {
            "operationId": "helloTransaction",
            "parameters": [
              {
                "$ref": "#/components/parameters/dbosWorkflowUUID",
              },
            ],
            "responses": {
              "200": {
                "content": {
                  "application/json": {
                    "schema": {
                      "type": "string",
                    },
                  },
                },
                "description": "Ok",
              },
            },
          },
        },
      },
    };

    const program = makeTestTypescriptProgram(source);
    const parser = new TypeParser(program);
    const classes = parser.parse();
    expect(parser.diags.length).toBe(0);
    const generator = new OpenApiGenerator(program);
    const openApi = generator.generate(classes!, "dbos-hello", "0.0.1");
    expect(generator.diags.length).toBe(2);
    for (const d of generator.diags) {
      expect(d.category).toBe(ts.DiagnosticCategory.Warning);
    }
    expect(openApi).toBeDefined();
    expect(openApi).toMatchObject(expected);
  });

  it("Uses default arg sources for a path param", () => {
    const source = /*javascript*/`
    import { DBOS, ArgSource, ArgSources, OpenApiSecurityScheme } from '@dbos-inc/dbos-sdk'

    export class Hello {
      @DBOS.getApi('/greetingget/:user')
      static async helloTransaction(user: string): Promise<string>  {
        return "";
      }

      @DBOS.getApi('/greetingget2/:user')
      static async helloTransaction2(@ArgSource(ArgSources.DEFAULT) user: string): Promise<string>  {
        return "";
      }
    }
    `;

    const program = makeTestTypescriptProgram(source);
    const parser = new TypeParser(program);
    const classes = parser.parse();
    expect(parser.diags.length).toBe(0);
    const generator = new OpenApiGenerator(program);
    const openApi = generator.generate(classes!, "dbos-hello", "0.0.1");
    expect(generator.diags.length).toBe(0);
    expect(openApi).toBeDefined();
  });
});
