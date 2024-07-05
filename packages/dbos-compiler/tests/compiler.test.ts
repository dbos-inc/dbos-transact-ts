import tsm from 'ts-morph';
import { checkStoredProcConfig, checkStoredProcNames, getProcMethods, getStoredProcConfig, removeDbosMethods, removeDecorators, removeUnusedFiles } from '../compiler.js';
import { makeTestProject } from './test-utility.js';
import { sampleDbosClass, sampleDbosClassAliased } from './test-code.js';
import { describe, it, expect } from 'vitest';

describe("compiler", () => {
  it("removeDbosMethods", () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow("operations.ts");

    removeDbosMethods(file);

    const testClass = file.getClassOrThrow("Test");
    const methods = testClass.getStaticMethods();
    expect(methods.length).toBe(8);
    expect(testClass.getStaticMethod("testProcedure")).toBeDefined();
  });

  it("aliased removeDbosMethods", () => {
    const { project } = makeTestProject(sampleDbosClassAliased);
    const file = project.getSourceFileOrThrow("operations.ts");

    removeDbosMethods(file);

    const testClass = file.getClassOrThrow("Test");
    const methods = testClass.getStaticMethods();
    expect(methods.length).toBe(8);
    expect(testClass.getStaticMethod("testProcedure")).toBeDefined();
  });

  it("getProcMethods", () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow("operations.ts");

    const procMethods = getProcMethods(file);

    expect(procMethods.length).toBe(8);
    const testClass = file.getClassOrThrow("Test");
    const testProcMethod = testClass.getStaticMethodOrThrow("testProcedure");
    expect(procMethods[0]).toEqual(testProcMethod);
  });

  it("removeDecorators", () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow("operations.ts");

    removeDecorators(file);

    let decoratorFound = false;
    file.forEachDescendant(node => {
      if (tsm.Node.isDecorator(node)) {
        decoratorFound = true;
      }
    });
    expect(decoratorFound).toBe(false);
  });


  it("removeUnusedFiles removes unused files", () => {
    const fileOne = /*ts*/`
      import { StoredProcedure } from "@dbos-inc/dbos-sdk";
      export class TestOne {
        @StoredProcedure()
        static async testStoredProcedure(): Promise<void> {}
      }`;

    const fileTwo = /*ts*/`
      export { TestOne } from './fileOne';
  
      export const test_export = "hello, world!";`;

    const { project } = makeTestProject(
      { code: fileOne, filename: "fileOne.ts" },
      { code: fileTwo, filename: "fileTwo.ts" }
    );

    removeUnusedFiles(project);

    expect(project.getSourceFiles().length).toBe(1);
  });

  it("removeUnusedFiles does not remove used files", () => {
    const fileOne = /*ts*/`
      import { StoredProcedure } from "@dbos-inc/dbos-sdk";
      import { test_export } from './fileTwo';

      export class TestOne {
        @StoredProcedure()
        static async testStoredProcedure(): Promise<void> {}
      }`;

    const fileTwo = /*ts*/`
      export { TestOne } from './fileOne';
  
      export const test_export = "hello, world!";`;

    const { project } = makeTestProject(
      { code: fileOne, filename: "fileOne.ts" },
      { code: fileTwo, filename: "fileTwo.ts" }
    );

    removeUnusedFiles(project);

    expect(project.getSourceFiles().length).toBe(2);
  });

  it("fails to compile really long routine names", () => {
    const longMethodNameFile = /*ts*/`
      import { StoredProcedure } from "@dbos-inc/dbos-sdk";
      export class TestOne {
        @StoredProcedure()
        static async testStoredProcedureWithReallyLongNameThatIsLongerThanTheMaximumAllowedLength(): Promise<void> {}
      }`;

      const { project } = makeTestProject(longMethodNameFile);
      const file = project.getSourceFileOrThrow("operations.ts");
      const procMethods = getProcMethods(file);
      expect(procMethods.length).toBe(1);

      const diags = checkStoredProcNames(procMethods);
      expect(diags.length).toBe(1);
      expect(diags[0].category === tsm.DiagnosticCategory.Error);
  });

  it("executeLocally warns or errors", () => {
    const executeLocallyFile = /*ts*/`
    import { StoredProcedure } from "@dbos-inc/dbos-sdk";

    export class TestOne {
      @StoredProcedure({ executeLocally: true })
      static async testStoredProcedure(): Promise<void> {}
    }`;
    const { project } = makeTestProject(executeLocallyFile);
    const file = project.getSourceFileOrThrow("operations.ts");
    const procMethods = getProcMethods(file).map(m => [m, getStoredProcConfig(m)] as const);
    expect(procMethods.length).toBe(1);
    expect(procMethods[0][1].executeLocally).toBe(true);

    const diags = checkStoredProcConfig(procMethods);
    expect(diags.length).toBe(1);
    expect(diags[0].category === tsm.DiagnosticCategory.Warning);

    const diags2 = checkStoredProcConfig(procMethods, true);
    expect(diags2.length).toBe(1);
    expect(diags2[0].category === tsm.DiagnosticCategory.Warning);
  })
});

