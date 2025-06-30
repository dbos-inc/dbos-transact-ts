import tsm from 'ts-morph';
import {
  checkStoredProc,
  collectUsedDeclarations,
  getStoredProcMethods,
  mapStoredProcConfig,
  removeDecorators,
  removeNonProcDbosMethods,
  removeUnusedDeclarations,
} from '../compiler';
import { makeTestProject, readTestContent } from './test-utility';
import { sampleDbosClass, sampleDbosClassAliased, testCodeTypes } from './test-code';
import { suite, test } from 'node:test';
import assert from 'node:assert/strict';

suite('compiler', () => {
  const testCodeProcCount = testCodeTypes.filter(([name, type]) => type === 'storedProcedure').length;

  test('removeDbosMethods', () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow('operations.ts');

    removeNonProcDbosMethods(file);

    const testClass = file.getClassOrThrow('Test');
    const methods = testClass.getStaticMethods();
    assert.equal(testCodeProcCount, methods.length);

    for (const [name, type] of testCodeTypes) {
      const method = testClass.getStaticMethod(name);
      if (type === 'storedProcedure') {
        assert.notEqual(method, undefined, `Expected method ${name} to be present`);
      } else {
        assert.equal(method, undefined, `Expected method ${name} to be absent`);
      }
    }
  });

  test('aliased removeDbosMethods', () => {
    const { project } = makeTestProject(sampleDbosClassAliased);
    const file = project.getSourceFileOrThrow('operations.ts');

    removeNonProcDbosMethods(file);

    const testClass = file.getClassOrThrow('Test');
    const methods = testClass.getStaticMethods();
    assert.equal(testCodeProcCount, methods.length);

    for (const [name, type] of testCodeTypes) {
      const method = testClass.getStaticMethod(name);
      if (type === 'storedProcedure') {
        assert.notEqual(method, undefined, `Expected method ${name} to be present`);
      } else {
        assert.equal(method, undefined, `Expected method ${name} to be absent`);
      }
    }
  });

  test('getProcMethods', () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow('operations.ts');
    const testClass = file.getClassOrThrow('Test');

    const procMethods = getStoredProcMethods(file);

    assert.equal(testCodeProcCount, procMethods.length);

    for (const [name, type] of testCodeTypes) {
      if (type !== 'storedProcedure') {
        continue;
      }
      const method = procMethods.find(([m]) => m.getName() === name);
      assert(method, `Expected method ${name} to be present in stored procedures`);
      const $method = testClass.getStaticMethodOrThrow(name);
      assert.equal(method[0], $method, `Expected method ${name} to be present`);
    }
  });

  test('removeDecorators', () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow('operations.ts');

    removeDecorators(file);

    file.forEachDescendant((node) => {
      if (tsm.Node.isDecorator(node)) {
        assert.fail('Found a decorator after removing them');
      }
    });
  });

  test('fails to compile really long routine names', () => {
    const longMethodNameFile = /*ts*/ `
      import { DBOS } from "@dbos-inc/dbos-sdk";
      export class TestOne {
        @DBOS.storedProcedure()
        static async testStoredProcedureWithReallyLongNameThatIsLongerThanTheMaximumAllowedLength(): Promise<void> {}
      }`;

    const { project } = makeTestProject(longMethodNameFile);
    const file = project.getSourceFileOrThrow('operations.ts');
    const procMethods = getStoredProcMethods(file).map(mapStoredProcConfig);
    assert.equal(procMethods.length, 1);

    const diags = procMethods.flatMap(checkStoredProc);
    assert.equal(diags.length, 1);
    assert.equal(diags[0].category, tsm.DiagnosticCategory.Error);
  });

  test('executeLocally warns', () => {
    const executeLocallyFile = /*ts*/ `
      import { DBOS } from "@dbos-inc/dbos-sdk";

      export class TestOne {
        @DBOS.storedProcedure({ executeLocally: true })
        static async testStoredProcedure(): Promise<void> {}
      }`;
    const { project } = makeTestProject(executeLocallyFile);
    const file = project.getSourceFileOrThrow('operations.ts');
    const procMethods = getStoredProcMethods(file).map(mapStoredProcConfig);
    assert.equal(procMethods.length, 1);
    assert(procMethods[0][1].executeLocally);

    const diags = procMethods.flatMap(checkStoredProc);
    assert.equal(diags.length, 1);
    assert.equal(diags[0].category, tsm.DiagnosticCategory.Warning);
  });

  test('single file compile', async () => {
    const main = await readTestContent('main.ts.txt');
    const { project } = makeTestProject(main);

    const files = project.getSourceFiles();
    assert.equal(3, files.length);
    const file = files.find((f) => f.getBaseName() === 'operations.ts')!;
    assert(file);

    const methods = getStoredProcMethods(file).map(mapStoredProcConfig);
    assert.equal(methods.length, 1);

    const $class = file.getClass('Example')!;
    assert($class);

    assert(file.getFunction('main'));

    assert.equal($class.getStaticMethods().length, 4);
    removeNonProcDbosMethods(file);
    assert.equal($class.getStaticMethods().length, 1);

    const usedDecls = collectUsedDeclarations(project, methods);
    assert.equal(usedDecls.size, 12);

    removeUnusedDeclarations(file, usedDecls);
    assert.equal(file.getFunction('main'), undefined, 'Main function should be removed');
  });
});
