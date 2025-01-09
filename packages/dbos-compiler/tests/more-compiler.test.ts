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
            testProcedure_v2: "storedProcedure",
            testReadOnlyProcedure_v2: "storedProcedure",
            testRepeatableReadProcedure_v2: "storedProcedure",
            testConfiguredProcedure_v2: "storedProcedure",
            testLocalProcedure_v2: "storedProcedure",
            testLocalReadOnlyProcedure_v2: "storedProcedure",
            testLocalRepeatableReadProcedure_v2: "storedProcedure",
            testLocalConfiguredProcedure_v2: "storedProcedure",

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
        const data = {
            "testProcedure": {},
            "testReadOnlyProcedure": { readOnly: true },
            "testRepeatableReadProcedure": { isolationLevel: "REPEATABLE READ" },
            "testConfiguredProcedure": { readOnly: true, isolationLevel: "READ COMMITTED" },
            "testLocalProcedure": { executeLocally: true },
            "testLocalReadOnlyProcedure": { readOnly: true, executeLocally: true },
            "testLocalRepeatableReadProcedure": { isolationLevel: "REPEATABLE READ", executeLocally: true },
            "testLocalConfiguredProcedure": { readOnly: true, isolationLevel: "READ COMMITTED", executeLocally: true }
        }

        for (const [name, config] of Object.entries(data)) {
            it(name, () => {
                const method = cls.getStaticMethodOrThrow(name);
                const actual = getStoredProcConfig(method);
                expect(actual).toEqual(config);
            });

            const v2name = `${name}_v2`;
            it(v2name, () => {
                const method = cls.getStaticMethodOrThrow(v2name);
                const actual = getStoredProcConfig(method);
                expect(actual).toEqual(config);
            });
        }
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