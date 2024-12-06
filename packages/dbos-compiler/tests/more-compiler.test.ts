import * as tsm from "ts-morph";
import { DecoratorArgument, getDbosMethodKind, getImportSpecifier, getStoredProcConfig, parseDecoratorArgument } from "../compiler.js";
import { sampleDbosClass, sampleDbosClassAliased } from "./test-code.js";
import { makeTestProject } from "./test-utility.js";
import { describe, it, expect } from 'vitest';

describe("more compiler", () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow("operations.ts");
    const cls = file.getClassOrThrow("Test");

    const { project: aliasProject } = makeTestProject(sampleDbosClassAliased);

    it.each([project, aliasProject])("getDbosMethodKind", (project: tsm.Project) => {
        const file = project.getSourceFileOrThrow("operations.ts");
        const cls = file.getClassOrThrow("Test");
        const entries = cls.getStaticMethods().map(m => [m.getName(), getDbosMethodKind(m)]);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        const actual = Object.fromEntries(entries);
        const expected = {
            testGetHandler: "handler",
            testPostHandler: "handler",
            testDeleteHandler: "handler",
            testPutHandler: "handler",
            testPatchHandler: "handler",
            testGetHandlerWorkflow: "workflow",
            testGetHandlerTx: "transaction",
            testGetHandlerComm: "step",
            testGetHandlerStep: "step",
            testWorkflow: "workflow",
            testCommunicator: "step",
            testStep: "step",
            testTransaction: "transaction",
            testProcedure: "storedProcedure",
            testReadOnlyProcedure: "storedProcedure",
            testRepeatableReadProcedure: "storedProcedure",
            testConfiguredProcedure: "storedProcedure",
            testLocalProcedure: "storedProcedure",
            testLocalReadOnlyProcedure: "storedProcedure",
            testLocalRepeatableReadProcedure: "storedProcedure",
            testLocalConfiguredProcedure: "storedProcedure",
            testDBOSInitializer: "initializer",
            testDBOSDeploy: "initializer",

            testGetHandler_v2: "handler",
            testPostHandler_v2: "handler",
            testDeleteHandler_v2: "handler",
            testPutHandler_v2: "handler",
            testPatchHandler_v2: "handler",
            testGetHandlerWorkflow_v2: "workflow",
            testGetHandlerTx_v2: "transaction",
            testGetHandlerStep_v2: "step",

            testStep_v2: "step",
            testTransaction_v2: "transaction",
            testWorkflow_v2: "workflow",
        };
        expect(actual).toEqual(expected);
    });

    describe("aliased getDecoratorInfo", () => {
        it("testGetHandler", () => {
            const file = aliasProject.getSourceFileOrThrow("operations.ts");
            const cls = file.getClassOrThrow("Test");
            const method = cls.getStaticMethodOrThrow("testGetHandler");

            const expected = <DecoratorInfo[]>[{
                name: "GetApi",
                alias: "TestGetApi",
                module: "@dbos-inc/dbos-sdk",
                args: ["/test"]
            }];

            testDecorators(expected, method.getDecorators());
        });
    });

    describe("getDecoratorInfo", () => {
        it("testGetHandler", () => {
            const method = cls.getStaticMethodOrThrow("testGetHandler");

            const expected = <DecoratorInfo[]>[{
                name: "GetApi",
                alias: undefined,
                module: "@dbos-inc/dbos-sdk",
                args: ["/test"]
            }];

            testDecorators(expected, method.getDecorators());
        });

        it("testGetHandlerWorkflow", () => {
            const method = cls.getStaticMethodOrThrow("testGetHandlerWorkflow");

            const expected: DecoratorInfo[] = [{
                name: "GetApi",
                alias: undefined,
                module: "@dbos-inc/dbos-sdk",
                args: ["/test"]
            }, {
                name: "Workflow",
                alias: undefined,
                module: "@dbos-inc/dbos-sdk",
                args: []
            }];

            testDecorators(expected, method.getDecorators());
        });

        it("testConfiguredProcedure", () => {
            const method = cls.getStaticMethodOrThrow("testConfiguredProcedure");

            const expected: DecoratorInfo[] = [{
                name: "StoredProcedure",
                alias: undefined,
                module: "@dbos-inc/dbos-sdk",
                args: [{ readOnly: true, isolationLevel: "READ COMMITTED" }]
            }];

            testDecorators(expected, method.getDecorators());
        });
    })

    describe("getStoredProcConfig", () => {
        it("testProcedure", () => {
            const method = cls.getStaticMethodOrThrow("testProcedure");
            const config = getStoredProcConfig(method);
            expect(config).toEqual({});
        });

        it("testReadOnlyProcedure", () => {
            const method = cls.getStaticMethodOrThrow("testReadOnlyProcedure");
            const config = getStoredProcConfig(method);
            expect(config).toEqual({ readOnly: true });
        });

        it("testRepeatableReadProcedure", () => {
            const method = cls.getStaticMethodOrThrow("testRepeatableReadProcedure");
            const config = getStoredProcConfig(method);
            expect(config).toEqual({ isolationLevel: "REPEATABLE READ" });
        });

        it("testConfiguredProcedure", () => {
            const method = cls.getStaticMethodOrThrow("testConfiguredProcedure");
            const config = getStoredProcConfig(method);
            expect(config).toEqual({ readOnly: true, isolationLevel: "READ COMMITTED" });
        });

        it("testLocalProcedure", () => {
            const method = cls.getStaticMethodOrThrow("testLocalProcedure");
            const config = getStoredProcConfig(method);
            expect(config).toEqual({ executeLocally: true });
        });

        it("testLocalReadOnlyProcedure", () => {
            const method = cls.getStaticMethodOrThrow("testLocalReadOnlyProcedure");
            const config = getStoredProcConfig(method);
            expect(config).toEqual({ readOnly: true, executeLocally: true });
        });

        it("testLocalRepeatableReadProcedure", () => {
            const method = cls.getStaticMethodOrThrow("testLocalRepeatableReadProcedure");
            const config = getStoredProcConfig(method);
            expect(config).toEqual({ isolationLevel: "REPEATABLE READ", executeLocally: true });
        });

        it("testLocalConfiguredProcedure", () => {
            const method = cls.getStaticMethodOrThrow("testLocalConfiguredProcedure");
            const config = getStoredProcConfig(method);
            expect(config).toEqual({ readOnly: true, isolationLevel: "READ COMMITTED", executeLocally: true });
        });
    })
});

interface DecoratorInfo {
    name: string;
    alias?: string;
    module?: string;
    args: DecoratorArgument[];
  }


function testDecorators(expected: DecoratorInfo[], actual: tsm.Decorator[]) {
    expect(actual.length).toEqual(expected.length);
    for (let i = 0; i < expected.length; i++) {
        testDecorator(expected[i], actual[i]);
    }
}
function testDecorator(expected: DecoratorInfo, actual: tsm.Decorator) {

    const callExpr = actual.getCallExpressionOrThrow();

    const expr = callExpr.getExpression();
    const idExpr = tsm.Node.isIdentifier(expr) 
        ? expr
        : expr.asKindOrThrow(tsm.SyntaxKind.PropertyAccessExpression).getExpressionIfKindOrThrow(tsm.SyntaxKind.Identifier);

    const impSpec = getImportSpecifier(idExpr)
    expect(impSpec).not.toBeNull();

    const { name, alias } = impSpec!.getStructure();
    const modSpec = impSpec!.getImportDeclaration().getModuleSpecifier();

    expect(expected.name).toEqual(name);
    expect(expected.alias).toEqual(alias);
    expect(expected.module).toEqual(modSpec.getLiteralText());

    const args = callExpr.getArguments().map(parseDecoratorArgument) ?? undefined;
    expect(args).toEqual(expected.args);
}