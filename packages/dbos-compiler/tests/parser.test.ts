import * as tsm from 'ts-morph';
import {
  DecoratorArgument,
  mapStoredProcConfig,
  parseDbosMethodInfo,
  parseDecoratorArgument,
  parseImportSpecifier,
} from '../compiler.js';
import { sampleDbosClass, sampleDbosClassAliased } from './test-code.js';
import { makeTestProject } from './test-utility.js';
import { suite, test } from 'node:test';
import assert from 'node:assert/strict';

suite('parser', () => {
  const { project } = makeTestProject(sampleDbosClass);
  const { project: aliasProject } = makeTestProject(sampleDbosClassAliased);
  const file = project.getSourceFileOrThrow('operations.ts');
  const cls = file.getClassOrThrow('Test');

  function testProject(name: string, project: tsm.Project) {
    test(`getDbosMethodKind ${name}`, () => {
      const file = project.getSourceFileOrThrow('operations.ts');
      const cls = file.getClassOrThrow('Test');
      const entries = cls.getStaticMethods().map((m) => [m.getName(), parseDbosMethodInfo(m)]);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      const actual = Object.fromEntries(entries);

      const raw_expected = {
        testGetHandler: 'handler',
        testPostHandler: 'handler',
        testDeleteHandler: 'handler',
        testPutHandler: 'handler',
        testPatchHandler: 'handler',
        testGetHandlerWorkflow: 'workflow',
        testGetHandlerTx: 'transaction',
        testGetHandlerComm: 'step',
        testGetHandlerStep: 'step',
        testWorkflow: 'workflow',
        testCommunicator: 'step',
        testStep: 'step',
        testTransaction: 'transaction',
        testProcedure: 'storedProcedure',
        testReadOnlyProcedure: 'storedProcedure',
        testRepeatableReadProcedure: 'storedProcedure',
        testConfiguredProcedure: 'storedProcedure',
        testLocalProcedure: 'storedProcedure',
        testLocalReadOnlyProcedure: 'storedProcedure',
        testLocalRepeatableReadProcedure: 'storedProcedure',
        testLocalConfiguredProcedure: 'storedProcedure',
        testDBOSInitializer: 'initializer',
        testDBOSDeploy: 'initializer',
      };

      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      const expected = Object.fromEntries(
        Object.entries(raw_expected).flatMap(([key, value]) => {
          const v1 = [key, { kind: value, version: 1 }];
          const v2 = [`${key}_v2`, { kind: value, version: 2 }];
          return value === 'initializer' ? [v1] : [v1, v2];
        }),
      );

      assert.deepEqual(actual, expected);
    });
  }

  testProject('project', project);
  testProject('aliasProject', aliasProject);

  suite('aliased getDecoratorInfo', () => {
    test('testGetHandler', () => {
      const file = aliasProject.getSourceFileOrThrow('operations.ts');
      const cls = file.getClassOrThrow('Test');
      const method = cls.getStaticMethodOrThrow('testGetHandler');

      const expected = <DecoratorInfo[]>[
        {
          name: 'GetApi',
          alias: 'TestGetApi',
          module: '@dbos-inc/dbos-sdk',
          args: ['/test'],
        },
      ];

      testDecorators(expected, method.getDecorators());
    });
  });

  suite('getDecoratorInfo', () => {
    test('testGetHandler', () => {
      const method = cls.getStaticMethodOrThrow('testGetHandler');

      const expected = <DecoratorInfo[]>[
        {
          name: 'GetApi',
          alias: undefined,
          module: '@dbos-inc/dbos-sdk',
          args: ['/test'],
        },
      ];

      testDecorators(expected, method.getDecorators());
    });

    test('testGetHandlerWorkflow', () => {
      const method = cls.getStaticMethodOrThrow('testGetHandlerWorkflow');

      const expected: DecoratorInfo[] = [
        {
          name: 'GetApi',
          alias: undefined,
          module: '@dbos-inc/dbos-sdk',
          args: ['/test'],
        },
        {
          name: 'Workflow',
          alias: undefined,
          module: '@dbos-inc/dbos-sdk',
          args: [],
        },
      ];

      testDecorators(expected, method.getDecorators());
    });

    test('testConfiguredProcedure', () => {
      const method = cls.getStaticMethodOrThrow('testConfiguredProcedure');

      const expected: DecoratorInfo[] = [
        {
          name: 'StoredProcedure',
          alias: undefined,
          module: '@dbos-inc/dbos-sdk',
          args: [{ readOnly: true, isolationLevel: 'READ COMMITTED' }],
        },
      ];

      testDecorators(expected, method.getDecorators());
    });
  });

  suite('getStoredProcConfig', () => {
    test('fake', () => {
      assert(true);
    });

    const data = {
      testProcedure: { executeLocally: undefined, readOnly: undefined, isolationLevel: undefined },
      testReadOnlyProcedure: { executeLocally: undefined, readOnly: true, isolationLevel: undefined },
      testRepeatableReadProcedure: {
        executeLocally: undefined,
        readOnly: undefined,
        isolationLevel: 'REPEATABLE READ',
      },
      testConfiguredProcedure: { executeLocally: undefined, readOnly: true, isolationLevel: 'READ COMMITTED' },
      testLocalProcedure: { executeLocally: true, readOnly: undefined, isolationLevel: undefined },
      testLocalReadOnlyProcedure: { readOnly: true, executeLocally: true, isolationLevel: undefined },
      testLocalRepeatableReadProcedure: {
        readOnly: undefined,
        isolationLevel: 'REPEATABLE READ',
        executeLocally: true,
      },
      testLocalConfiguredProcedure: { readOnly: true, isolationLevel: 'READ COMMITTED', executeLocally: true },
    };

    for (const [name, config] of Object.entries(data)) {
      test(name, () => {
        const expected = { ...config, version: 1 };
        const method = cls.getStaticMethodOrThrow(name);
        const [_, actual] = mapStoredProcConfig([method, 1]);
        assert.deepEqual(actual, expected);
      });

      const v2name = `${name}_v2`;
      test(v2name, () => {
        const expected = { ...config, version: 2 };
        const method = cls.getStaticMethodOrThrow(v2name);
        const [_, actual] = mapStoredProcConfig([method, 2]);
        assert.deepEqual(actual, expected);
      });
    }
  });
});

interface DecoratorInfo {
  name: string;
  alias?: string;
  module?: string;
  args: DecoratorArgument[];
}

function testDecorators(expected: DecoratorInfo[], actual: tsm.Decorator[]) {
  assert.equal(actual.length, expected.length);
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

  const impSpec = parseImportSpecifier(idExpr);
  assert(impSpec !== undefined);

  const { name, alias } = impSpec!.getStructure();
  const modSpec = impSpec!.getImportDeclaration().getModuleSpecifier();

  assert.equal(expected.name, name);
  assert.equal(expected.alias, alias);
  assert.equal(expected.module, modSpec.getLiteralText());

  const args = callExpr.getArguments().map(parseDecoratorArgument) ?? undefined;
  assert.deepEqual(args, expected.args);
}
