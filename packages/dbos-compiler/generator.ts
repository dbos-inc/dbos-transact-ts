import path from 'node:path';
import tsm from 'ts-morph';
import { Liquid } from "liquidjs";
import type { StoredProcedureConfig, CompileResult } from './compiler.js';

const engine = new Liquid({
  root: path.resolve(__dirname, 'templates'),
  extname: ".liquid"
});

async function render(file: string, ctx?: object): Promise<string> {
  return await engine.renderFile(file, ctx) as string;
}

function getAppVersion(appVersion: string | boolean | undefined) {
  const version = function() {
    if (typeof appVersion === "string") { return appVersion; }
    if (appVersion === false) { return undefined; }
    return process.env.DBOS__APPVERSION;
  }();
  return version ? `v${version}_` : undefined;
}

export async function generateCreate(executeSql: (sql: string) => Promise<void>, { project, methods }: CompileResult, appVersionOption?: string | boolean) {
  const appVersion = getAppVersion(appVersionOption);

  const dbosSql = await generateDbosCreate(appVersion);
  await executeSql(dbosSql);

  for (const sourceFile of project.getSourceFiles()) {
    const moduleSql = await generateModuleCreate(sourceFile, appVersion);
    await executeSql(moduleSql);
  }

  for (const [method, config] of methods) {
    const methodSql = await generateMethodCreate(method, config, appVersion);
    await executeSql(methodSql);
  }
}

export async function generateDrop(executeSql: (sql: string) => Promise<void>, { project, methods }: CompileResult, appVersionOption?: string | boolean) {
  const appVersion = getAppVersion(appVersionOption);

  const dbosSql = await generateDbosDrop(appVersion);
  await executeSql(dbosSql);

  for (const sourceFile of project.getSourceFiles()) {
    const moduleSql = await generateModuleDrop(sourceFile, appVersion);
    await executeSql(moduleSql);
  }

  for (const [method, config] of methods) {
    const methodSql = await generateMethodDrop(method, config, appVersion);
    await executeSql(methodSql);
  }
}

async function generateDbosCreate(appVersion: string | undefined) {
  const context = { appVersion };
  return await render("dbos.create.liquid", context);
}

async function generateDbosDrop(appVersion: string | undefined) {
  const context = { appVersion };
  return await render("dbos.drop.liquid", context);
}

function getMethodContext(method: tsm.MethodDeclaration, config: StoredProcedureConfig, appVersion: string | undefined) {
  const readOnly = config?.readOnly ?? false;
  const executeLocally = config?.executeLocally ?? false;
  const isolationLevel = config?.isolationLevel ?? "SERIALIZABLE";

  const methodName = method.getName();
  const className = method.getParentIfKindOrThrow(tsm.SyntaxKind.ClassDeclaration).getName();
  if (!className) throw new Error("Method must be a member of a class");
  const moduleName = method.getSourceFile().getBaseNameWithoutExtension();

  return { 
    readOnly, 
    isolationLevel, 
    executeLocally, 
    methodName, 
    className, 
    moduleName, 
    appVersion 
  };
}

async function generateMethodCreate(method: tsm.MethodDeclaration, config: StoredProcedureConfig, appVersion: string | undefined) {
  const context = getMethodContext(method, config, appVersion);
  return await render("method.create.liquid", context);
}


async function generateMethodDrop(method: tsm.MethodDeclaration, config: StoredProcedureConfig, appVersion: string | undefined) {
  const context = getMethodContext(method, config, appVersion);
  return await render("method.drop.liquid", context);
}

function getModuleContext(sourceFile: tsm.SourceFile, appVersion: string | undefined) {
  const results = sourceFile.getEmitOutput();
  const contents = results.getEmitSkipped() ? "" : results.getOutputFiles().map(f => f.getText()).join("\n");
  const moduleName = sourceFile.getBaseNameWithoutExtension();

  const context = { moduleName, contents, appVersion };
  return context;
}

async function generateModuleCreate(sourceFile: tsm.SourceFile, appVersion: string | undefined) {
  const context = getModuleContext(sourceFile, appVersion);
  return await render("module.create.liquid", context);
}

async function generateModuleDrop(sourceFile: tsm.SourceFile, appVersion: string | undefined) {
  const context = getModuleContext(sourceFile, appVersion);
  return await render("module.drop.liquid", context);
}