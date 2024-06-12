#!/usr/bin/env node

import { Command } from "commander";
import tsm from 'ts-morph';
import path from 'node:path';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import { deAsync, getProcMethods, removeDecorators, treeShake } from "./treeShake.js";
import { emitDbos, emitMethod, emitModule } from "./generator.js";
import { TransactionConfig, getStoredProcConfig } from "./utility.js";

async function compile(tsConfigFilePath: string, options: EmitOptions) {
  const project = new tsm.Project({
    tsConfigFilePath,
    compilerOptions: {
      sourceMap: false,
      declaration: false,
      declarationMap: false,
    }
  });

  // remove test files
  for (const sourceFile of project.getSourceFiles()) {
    if (sourceFile.getBaseName().endsWith(".test.ts")) {
      sourceFile.delete();
    }
  }

  const preEmitDiags = project.getPreEmitDiagnostics();
  if (preEmitDiags.length > 0) {
    printDiagnostics(preEmitDiags.map(d => d.compilerObject));
    return;
  }

  treeShake(project);

  const methods = project.getSourceFiles()
    .flatMap(getProcMethods)
    .map(m => [m, getStoredProcConfig(m)] as const);

  deAsync(project);
  removeDecorators(project);

  await emitProject(project, methods, options);
}

function printDiagnostics(diags: readonly tsm.ts.Diagnostic[]) {
  const formatHost: tsm.ts.FormatDiagnosticsHost = {
    getCurrentDirectory: () => tsm.ts.sys.getCurrentDirectory(),
    getNewLine: () => tsm.ts.sys.newLine,
    getCanonicalFileName: (fileName: string) => tsm.ts.sys.useCaseSensitiveFileNames
      ? fileName : fileName.toLowerCase()
  }

  const msg = tsm.ts.formatDiagnosticsWithColorAndContext(diags, formatHost);
  console.log(msg);
}

interface EmitOptions {
  outDir: string,
  appVersion?: string,
}

async function emitProject(project: tsm.Project, methods: (readonly [tsm.MethodDeclaration, TransactionConfig | undefined])[], { outDir, appVersion }: EmitOptions) {

  appVersion = appVersion ? `v${appVersion}_` : undefined;

  await fsp.mkdir(outDir, { recursive: true });

  const createPath = path.join(outDir, "create.sql");
  const dropPath = path.join(outDir, "drop.sql");
  const paths = { createPath, dropPath };

  await fsp.rm(createPath, { force: true });
  await fsp.rm(dropPath, { force: true });

  await emitDbos(paths, appVersion);
  for (const sourceFile of project.getSourceFiles()) {
    await emitModule(sourceFile, paths, appVersion);
  }

  for (const [method, config] of methods) {
    const name = method.getName();
    if (!config) { throw new Error(`Missing config for method: ${name}`); }
    await emitMethod(method, config, paths, appVersion);
  }
}

function getVersion(): string {
  const __dirname = import.meta.dirname;
  const packageJsonPath = path.join(__dirname, "..", "package.json");
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf-8")) as { version: string };
  return packageJson.version;
}

function getAppVersion(appVersion: string | boolean | undefined) {
  if (typeof appVersion === "string") { return appVersion; }
  if (appVersion === true) {
    const envAppVersion = process.env.DBOS__APPVERSION;
    if (envAppVersion === undefined) {
      return new Error("--app-version option specified but DBOS__APPVERSION environment variable is not set");
    }
    return envAppVersion;
  }
  return undefined;
}

const program = new Command()
  .version(getVersion())
  .argument('<tsconfigPath>', 'path to tsconfig.json')
  .option('-o, --out <string>', 'path to output folder')
  .option('-v, --app-version [string]', 'emit version')
  .action(async (tsconfigPath: string, options: { out?: string, appVersion?: string | boolean }) => {
    const appVersion = getAppVersion(options.appVersion);
    if (appVersion instanceof Error) {
      console.error(appVersion.message);
      process.exitCode = 1;
      return;
    }
    await compile(tsconfigPath, { appVersion, outDir: options.out ?? process.cwd() });
  });

program.parse(process.argv);
