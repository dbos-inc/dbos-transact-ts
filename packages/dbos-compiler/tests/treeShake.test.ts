import tsm from 'ts-morph';
import { getProcMethods, removeDbosMethods, removeDecorators, removeUnusedFiles } from '../src/treeShake.js';
import { makeTestProject } from './test-utility.js';
import { sampleDbosClass, sampleDbosClassAliased } from './test-code.js';
import { describe, it, expect } from 'vitest';

describe("treeShake", () => {
  it("removeDbosMethods", () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow("operations.ts");

    removeDbosMethods(file);

    const testClass = file.getClassOrThrow("Test");
    const methods = testClass.getStaticMethods();
    expect(methods.length).toBe(4);
    expect(testClass.getStaticMethod("testProcedure")).toBeDefined();
  });

  it("aliased removeDbosMethods", () => {
    const { project } = makeTestProject(sampleDbosClassAliased);
    const file = project.getSourceFileOrThrow("operations.ts");

    removeDbosMethods(file);

    const testClass = file.getClassOrThrow("Test");
    const methods = testClass.getStaticMethods();
    expect(methods.length).toBe(4);
    expect(testClass.getStaticMethod("testProcedure")).toBeDefined();
  });

  it("getProcMethods", () => {
    const { project } = makeTestProject(sampleDbosClass);
    const file = project.getSourceFileOrThrow("operations.ts");

    const procMethods = getProcMethods(file);

    expect(procMethods.length).toBe(4);
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
});

