#!/usr/bin/env node

import { Command } from "commander";
import tsm from 'ts-morph';
import path from 'node:path';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import { deAsync, getProcMethods, removeDecorators, treeShake } from "./treeShake.js";
import { generateCreate, generateDrop } from "./generator.js";
import { TransactionConfig, getStoredProcConfig } from "./utility.js";
import { parseConfigFile } from '@dbos-inc/dbos-sdk/dist/src/dbos-runtime/config.js'
import { Client } from 'pg';

type CompileResult = {
  project: tsm.Project;
  methods: (readonly [tsm.MethodDeclaration, TransactionConfig])[];
};

async function compile(tsConfigFilePath: string): Promise<tsm.Diagnostic[] | CompileResult> {
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
    return preEmitDiags;
  }

  treeShake(project);

  const methods = project.getSourceFiles()
    .flatMap(getProcMethods)
    .map(m => [m, getStoredProcConfig(m)] as const);

  deAsync(project);
  removeDecorators(project);

  return { project, methods }
}

function printDiagnostics(diags: readonly tsm.Diagnostic[]) {
  const formatHost: tsm.ts.FormatDiagnosticsHost = {
    getCurrentDirectory: () => tsm.ts.sys.getCurrentDirectory(),
    getNewLine: () => tsm.ts.sys.newLine,
    getCanonicalFileName: (fileName: string) => tsm.ts.sys.useCaseSensitiveFileNames
      ? fileName : fileName.toLowerCase()
  }

  const $diags = diags.map(d => d.compilerObject);
  const msg = tsm.ts.formatDiagnosticsWithColorAndContext($diags, formatHost);
  console.log(msg);
}

function getAppVersion(appVersion: string | boolean | undefined) {
  if (typeof appVersion === "string") { return appVersion; }
  if (appVersion === false) { return undefined; }
  return process.env.DBOS__APPVERSION;
}

async function emitSqlFiles(outDir: string, result: CompileResult, appVersion?: string | boolean) {
  appVersion = getAppVersion(appVersion);
  
  await fsp.mkdir(outDir, { recursive: true });

  const createFile = await fsp.open(path.join(outDir, "create.sql"), "w");
  try {
    const append = async (sql: string) => { await createFile.write(sql); }; 
    await generateCreate(append, result.project, result.methods, appVersion);
  } finally {
    createFile.close();
  }

  const dropFile = await fsp.open(path.join(outDir, "drop.sql"), "w");
  try {
    const append = async (sql: string) => { await dropFile.write(sql); }; 
    await generateDrop(append, result.project, result.methods, appVersion);
  } finally {
    createFile.close();
  }
}

async function deployToDatabase(client: Client, result: CompileResult, appVersion?: string | boolean) {
  appVersion = getAppVersion(appVersion);
  const append = async (sql: string) => { await client.query(sql); }; 
  await generateCreate(append, result.project, result.methods, appVersion);
}

function getVersion(): string {
  const __dirname = import.meta.dirname;
  const packageJsonPath = path.join(__dirname, "..", "package.json");
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf-8")) as { version: string };
  return packageJson.version;
}

const program = new Command();

program.version(getVersion());

interface CompileOptions {
  outDir: string,
  appVersion?: string | boolean,
}

program
  .argument('<tsconfigPath>', 'path to tsconfig.json')
  .option('-o, --out <string>', 'path to output folder')
  .option('--app-version <string>', 'override DBOS__APPVERSION environment variable')
  .option('--no-app-version', 'ignore DBOS__APPVERSION environment variable')
  .action(async (tsconfigPath: string, options: CompileOptions) => {
    const compileResult = await compile(tsconfigPath);
    if (Array.isArray(compileResult)) {
      printDiagnostics(compileResult);
      return;
    }

    await emitSqlFiles(options.outDir, compileResult, options.appVersion);
  });

interface DeployOptions {
  appDir?: string;
  appVersion?: string | boolean;
}

program
  .command("deploy")
  .description("Start the server")
  .argument('<tsconfigPath>', 'path to tsconfig.json')
  .option("-d, --appDir <string>", "Specify the application root directory")
  .option('--app-version <string>', 'override DBOS__APPVERSION environment variable')
  .option('--no-app-version', 'ignore DBOS__APPVERSION environment variable')
  .action(async (tsconfigPath: string, options: DeployOptions) => {
    const compileResult = await compile(tsconfigPath);
    if (Array.isArray(compileResult)) {
      printDiagnostics(compileResult);
      return;
    }

    const [dbosConfig,] = parseConfigFile(options);
    const client = new Client(dbosConfig.poolConfig);
    try {
      await client.connect();
      await deployToDatabase(client, compileResult, options.appVersion);
    } finally {
      await client.end();
    }
  });

program.parse(process.argv);
