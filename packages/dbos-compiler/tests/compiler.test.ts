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
import { describe, it, expect } from 'vitest';

describe('compiler', () => {
  it('removeDbosMethods', () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow('operations.ts');

    removeNonProcDbosMethods(file);

    const testClass = file.getClassOrThrow('Test');
    const methods = testClass.getStaticMethods();
    expect(methods.length).toBe(16);
    expect(testClass.getStaticMethod('testProcedure')).toBeDefined();
  });

  it('aliased removeDbosMethods', () => {
    const { project } = makeTestProject(sampleDbosClassAliased);
    const file = project.getSourceFileOrThrow('operations.ts');

    removeNonProcDbosMethods(file);

    const testClass = file.getClassOrThrow('Test');
    const methods = testClass.getStaticMethods();
    expect(methods.length).toBe(16);
    expect(testClass.getStaticMethod('testProcedure')).toBeDefined();
  });

  it('getProcMethods', () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow('operations.ts');

    const procMethods = getStoredProcMethods(file);

    expect(procMethods.length).toBe(16);
    const testClass = file.getClassOrThrow('Test');
    const testProcMethod = testClass.getStaticMethodOrThrow('testProcedure');
    expect(procMethods[0][0]).toEqual(testProcMethod);
  });

  it('removeDecorators', () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow('operations.ts');

    removeDecorators(file);

    let decoratorFound = false;
    file.forEachDescendant((node) => {
      if (tsm.Node.isDecorator(node)) {
        decoratorFound = true;
      }
    });
    expect(decoratorFound).toBe(false);
  });

  it('fails to compile really long routine names', () => {
    const longMethodNameFile = /*ts*/ `
      import { StoredProcedure } from "@dbos-inc/dbos-sdk";
      export class TestOne {
        @StoredProcedure()
        static async testStoredProcedureWithReallyLongNameThatIsLongerThanTheMaximumAllowedLength(): Promise<void> {}
      }`;

    const { project } = makeTestProject(longMethodNameFile);
    const file = project.getSourceFileOrThrow('operations.ts');
    const procMethods = getStoredProcMethods(file).map(mapStoredProcConfig);
    expect(procMethods.length).toBe(1);

    const diags = procMethods.flatMap(checkStoredProc);
    expect(diags.length).toBe(1);
    expect(diags[0].category === tsm.DiagnosticCategory.Error);
  });

  it('executeLocally warns', () => {
    const executeLocallyFile = /*ts*/ `
    import { StoredProcedure } from "@dbos-inc/dbos-sdk";

    export class TestOne {
      @StoredProcedure({ executeLocally: true })
      static async testStoredProcedure(): Promise<void> {}
    }`;
    const { project } = makeTestProject(executeLocallyFile);
    const file = project.getSourceFileOrThrow('operations.ts');
    const procMethods = getStoredProcMethods(file).map(mapStoredProcConfig);
    expect(procMethods.length).toBe(1);
    expect(procMethods[0][1].executeLocally).toBe(true);

    const diags = procMethods.flatMap(checkStoredProc);
    expect(diags.length).toBe(1);
    expect(diags[0].category === tsm.DiagnosticCategory.Warning);
  });

  it('single file compile', async () => {
    const main = await readTestContent('main.ts.txt');
    const { project } = makeTestProject(main);

    const files = project.getSourceFiles();
    expect(files.length).toBe(3);
    const file = files.find((f) => f.getBaseName() === 'operations.ts')!;
    expect(file).toBeDefined();

    const methods = getStoredProcMethods(file).map(mapStoredProcConfig);
    expect(methods.length).toBe(1);

    const $class = file.getClass('Example')!;
    expect($class).toBeDefined();

    expect(file.getFunction('main')).toBeDefined();

    expect($class.getStaticMethods().length).toBe(4);
    removeNonProcDbosMethods(file);
    expect($class.getStaticMethods().length).toBe(1);

    const usedDecls = collectUsedDeclarations(project, methods);
    expect(usedDecls.size).toBe(12);

    removeUnusedDeclarations(file, usedDecls);
    expect(file.getFunction('main')).toBeUndefined();
  });
});
