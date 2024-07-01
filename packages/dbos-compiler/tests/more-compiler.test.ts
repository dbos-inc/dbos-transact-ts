import * as tsm from "ts-morph";
import { DecoratorArgument, DecoratorInfo, getDbosMethodKind, getDecoratorInfo, getStoredProcConfig, parseDecoratorArgument } from "../compiler.js";
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
            testGetHandlerWorkflow: "workflow",
            testGetHandlerTx: "transaction",
            testGetHandlerComm: "communicator",
            testPostHandler: "handler",
            testWorkflow: "workflow",
            testCommunicator: "communicator",
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
            testDBOSDeploy: "initializer"
        };
        expect(actual).toEqual(expected);
    });

    describe("aliased getDecoratorInfo", () => {
        it("testGetHandler", () => {
            const file = aliasProject.getSourceFileOrThrow("operations.ts");
            const cls = file.getClassOrThrow("Test");
            const method = cls.getStaticMethodOrThrow("testGetHandler");
            const decoratorInfo = method.getDecorators().map(getDecoratorInfo);

            const expected = <TestDecoratorInfo[]>[{
                name: "GetApi",
                alias: "TestGetApi",
                module: "@dbos-inc/dbos-sdk",
                args: ["/test"]
            }];

            testDecorators(expected, decoratorInfo);
        });
    });

    describe("getDecoratorInfo", () => {
        it("testGetHandler", () => {
            const method = cls.getStaticMethodOrThrow("testGetHandler");
            const decoratorInfo = method.getDecorators().map(getDecoratorInfo);

            const expected = <TestDecoratorInfo[]>[{
                name: "GetApi",
                alias: undefined,
                module: "@dbos-inc/dbos-sdk",
                args: ["/test"]
            }];

            testDecorators(expected, decoratorInfo);
        });

        it("testGetHandlerWorkflow", () => {
            const method = cls.getStaticMethodOrThrow("testGetHandlerWorkflow");
            const decoratorInfo = method.getDecorators().map(getDecoratorInfo);

            const expected: TestDecoratorInfo[] = [{
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

            testDecorators(expected, decoratorInfo);
        });

        it("testConfiguredProcedure", () => {
            const method = cls.getStaticMethodOrThrow("testConfiguredProcedure");
            const decoratorInfo = method.getDecorators().map(getDecoratorInfo);

            const expected: TestDecoratorInfo[] = [{
                name: "StoredProcedure",
                alias: undefined,
                module: "@dbos-inc/dbos-sdk",
                args: [{ readOnly: true, isolationLevel: "READ COMMITTED" }]
            }];

            testDecorators(expected, decoratorInfo);
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

type TestDecoratorInfo = Omit<DecoratorInfo, 'args'> & {
    args: DecoratorArgument[];
};

function testDecorators(expected: TestDecoratorInfo[], actual: DecoratorInfo[]) {
    expect(actual.length).toEqual(expected.length);
    for (let i = 0; i < expected.length; i++) {
        testDecorator(expected[i], actual[i]);
    }
}
function testDecorator(expected: TestDecoratorInfo, actual: DecoratorInfo) {
    expect(actual.name).toEqual(expected.name);
    expect(actual.alias).toEqual(expected.alias);
    expect(actual.module).toEqual(expected.module);

    const args = actual.args?.map(parseDecoratorArgument);
    expect(args).toEqual(expected.args);
}