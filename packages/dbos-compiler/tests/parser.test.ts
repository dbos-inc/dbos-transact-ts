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
import { describe, it, expect } from 'vitest';

describe('parser', () => {
  const { project } = makeTestProject(sampleDbosClass);
  const { project: aliasProject } = makeTestProject(sampleDbosClassAliased);
  const file = project.getSourceFileOrThrow('operations.ts');
  const cls = file.getClassOrThrow('Test');

  function testProject(name: string, project: tsm.Project) {
    it(`getDbosMethodKind ${name}`, () => {
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

      expect(actual).toEqual(expected);
    });
  }

  testProject('project', project);
  testProject('aliasProject', aliasProject);

  describe('aliased getDecoratorInfo', () => {
    it('testGetHandler', () => {
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

  describe('getDecoratorInfo', () => {
    it('testGetHandler', () => {
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

    it('testGetHandlerWorkflow', () => {
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

    it('testConfiguredProcedure', () => {
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

  describe('getStoredProcConfig', () => {
    it('fake', () => {
      expect(true);
    });

    const data = {
      testProcedure: {},
      testReadOnlyProcedure: { readOnly: true },
      testRepeatableReadProcedure: { isolationLevel: 'REPEATABLE READ' },
      testConfiguredProcedure: { readOnly: true, isolationLevel: 'READ COMMITTED' },
      testLocalProcedure: { executeLocally: true },
      testLocalReadOnlyProcedure: { readOnly: true, executeLocally: true },
      testLocalRepeatableReadProcedure: { isolationLevel: 'REPEATABLE READ', executeLocally: true },
      testLocalConfiguredProcedure: { readOnly: true, isolationLevel: 'READ COMMITTED', executeLocally: true },
    };

    for (const [name, config] of Object.entries(data)) {
      it(name, () => {
        const expected = { ...config, version: 1 };
        const method = cls.getStaticMethodOrThrow(name);
        const [_, actual] = mapStoredProcConfig([method, 1]);
        expect(actual).toEqual(expected);
      });

      const v2name = `${name}_v2`;
      it(v2name, () => {
        const expected = { ...config, version: 2 };
        const method = cls.getStaticMethodOrThrow(v2name);
        const [_, actual] = mapStoredProcConfig([method, 2]);
        expect(actual).toEqual(expected);
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

  const impSpec = parseImportSpecifier(idExpr);
  expect(impSpec).not.toBeNull();

  const { name, alias } = impSpec!.getStructure();
  const modSpec = impSpec!.getImportDeclaration().getModuleSpecifier();

  expect(expected.name).toEqual(name);
  expect(expected.alias).toEqual(alias);
  expect(expected.module).toEqual(modSpec.getLiteralText());

  const args = callExpr.getArguments().map(parseDecoratorArgument) ?? undefined;
  expect(args).toEqual(expected.args);
}
