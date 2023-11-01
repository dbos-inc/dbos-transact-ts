import ts from "typescript";
import { TypeParser } from "../../src/operon-runtime/TypeParser";

import path from "node:path";
import { OpenApiGenerator } from "../../src/operon-runtime/openApi";

const printer = ts.createPrinter();

const entrypoint = path.join(__dirname, "../../examples/hello/src/operations.ts");
const program = ts.createProgram([entrypoint], {});

describe("TypeParser", () => {
  it("examples/hello", () => {
    const parser = new TypeParser(program);
    const classes = parser.parse();
    expect(parser.diags.length).toBe(0);

    expect(classes).toBeDefined();
    expect(classes!.length).toBe(1);

    const [$class] = classes!;
    expect($class.name).toBe("Hello");
    expect($class.decorators.length).toBe(0);
    expect($class.methods.length).toBe(1);

    const [method] = $class.methods;
    expect(method.name).toBe("helloTransaction");
    expect(method.decorators.length).toBe(2);

    const [dec1, dec2] = method.decorators;
    expect(dec1.name).toBe("GetApi");
    expect(dec1.args.length).toBe(1);
    const [arg1] = dec1.args;
    expect(arg1.kind).toBe(ts.SyntaxKind.StringLiteral);
    expect((arg1 as ts.StringLiteral).text).toBe("/greeting/:user");

    expect(dec2.name).toBe("OperonTransaction");
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
});

describe("OpenApiGenerator", () => {
  it("examples/hello", () => {
    const classes = TypeParser.parse(program);
    expect(classes!.length).toBe(1);

    const generator = new OpenApiGenerator(program);
    const openApi = generator.generate(classes!, "operon-hello", "0.0.1");
    expect(generator.diags.length).toBe(0);

    expect(openApi).toBeDefined();
    expect(openApi).toMatchObject(helloExampleExpected);
  });
});

const helloExampleExpected = {
  "openapi": "3.0.3",
  "info": {
    "title": "operon-hello",
    "version": "0.0.1"
  },
  "paths": {
    "/greeting/{user}": {
      "get": {
        "operationId": "helloTransaction",
        "responses": {
          "200": {
            "description": "Ok",
            "content": {
              "application/json": {
                "schema": {
                  "type": "string"
                }
              }
            }
          }
        },
        "parameters": [
          {
            "name": "user",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string"
            }
          },
          {
            "name": "operon-workflowuuid",
            "in": "header",
            "required": false,
            "schema": {
              "type": "string"
            }
          }
        ]
      }
    }
  },
  "components": {
    "schemas": {}
  }
};
