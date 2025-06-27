import tsm from 'ts-morph';
import {
  checkStoredProc,
  collectUsedDeclarations,
  getStoredProcMethods,
  mapStoredProcConfig,
  removeDecorators,
  removeNonProcDbosMethods,
  removeUnusedDeclarations,
} from '../compiler.js';
import { makeTestProject, readTestContent } from './test-utility.js';
import { sampleDbosClass, sampleDbosClassAliased } from './test-code.js';
import { suite, test } from 'node:test';
import assert from 'node:assert/strict';

suite('compiler', () => {
  test('removeDbosMethods', () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow('operations.ts');

    removeNonProcDbosMethods(file);

    const testClass = file.getClassOrThrow('Test');
    const methods = testClass.getStaticMethods();
    assert(methods.length == 16);
    assert(testClass.getStaticMethod('testProcedure') !== undefined);
  });

  test('aliased removeDbosMethods', () => {
    const { project } = makeTestProject(sampleDbosClassAliased);
    const file = project.getSourceFileOrThrow('operations.ts');

    removeNonProcDbosMethods(file);

    const testClass = file.getClassOrThrow('Test');
    const methods = testClass.getStaticMethods();
    assert(methods.length == 16);
    assert(testClass.getStaticMethod('testProcedure') !== undefined);
  });

  test('getProcMethods', () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow('operations.ts');

    const procMethods = getStoredProcMethods(file);

    assert(procMethods.length === 16);
    const testClass = file.getClassOrThrow('Test');
    const testProcMethod = testClass.getStaticMethodOrThrow('testProcedure');
    assert.equal(procMethods[0][0], testProcMethod);
  });

  test('removeDecorators', () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow('operations.ts');

    removeDecorators(file);

    let decoratorFound = false;
    file.forEachDescendant((node) => {
      if (tsm.Node.isDecorator(node)) {
        decoratorFound = true;
      }
    });
    assert(decoratorFound === false);
  });

  test('fails to compile really long routine names', () => {
    const longMethodNameFile = /*ts*/ `
      import { StoredProcedure } from "@dbos-inc/dbos-sdk";
      export class TestOne {
        @StoredProcedure()
        static async testStoredProcedureWithReallyLongNameThatIsLongerThanTheMaximumAllowedLength(): Promise<void> {}
      }`;

    const { project } = makeTestProject(longMethodNameFile);
    const file = project.getSourceFileOrThrow('operations.ts');
    const procMethods = getStoredProcMethods(file).map(mapStoredProcConfig);
    assert(procMethods.length === 1);

    const diags = procMethods.flatMap(checkStoredProc);
    assert(diags.length === 1);
    assert(diags[0].category === tsm.DiagnosticCategory.Error);
  });

  test('executeLocally warns', () => {
    const executeLocallyFile = /*ts*/ `
    import { StoredProcedure } from "@dbos-inc/dbos-sdk";

    export class TestOne {
      @StoredProcedure({ executeLocally: true })
      static async testStoredProcedure(): Promise<void> {}
    }`;
    const { project } = makeTestProject(executeLocallyFile);
    const file = project.getSourceFileOrThrow('operations.ts');
    const procMethods = getStoredProcMethods(file).map(mapStoredProcConfig);
    assert((procMethods.length = 1));
    assert(procMethods[0][1].executeLocally);

    const diags = procMethods.flatMap(checkStoredProc);
    assert(diags.length === 1);
    assert(diags[0].category === tsm.DiagnosticCategory.Warning);
  });

  test('single file compile', async () => {
    const main = await readTestContent('main.ts.txt');
    const { project } = makeTestProject(main);

    const files = project.getSourceFiles();
    assert(files.length === 3);
    const file = files.find((f) => f.getBaseName() === 'operations.ts')!;
    assert(file !== undefined);

    const methods = getStoredProcMethods(file).map(mapStoredProcConfig);
    assert(methods.length === 1);

    const $class = file.getClass('Example')!;
    assert($class !== undefined);

    assert(file.getFunction('main') !== undefined);

    assert($class.getStaticMethods().length === 4);
    removeNonProcDbosMethods(file);
    assert($class.getStaticMethods().length === 1);

    const usedDecls = collectUsedDeclarations(project, methods);
    assert(usedDecls.size === 12);

    removeUnusedDeclarations(file, usedDecls);
    assert(file.getFunction('main') === undefined);
  });
});
