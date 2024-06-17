#!/usr/bin/env node

import { Command } from "commander";
import tsm from 'ts-morph';
import path from 'node:path';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import { deAsync, getProcMethods, removeDecorators, treeShake } from "./treeShake.js";
import { type CompileResult, generateCreate, generateDrop } from "./generator.js";
import { getStoredProcConfig } from "./utility.js";
import { parseConfigFile } from '@dbos-inc/dbos-sdk/dist/src/dbos-runtime/config.js'
import * as pg from 'pg'

function compile(tsConfigFilePath: string): CompileResult | undefined {
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
    printDiagnostics(preEmitDiags);
    return undefined;
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


async function emitSqlFiles(outDir: string, result: CompileResult, appVersion?: string | boolean) {
  await fsp.mkdir(outDir, { recursive: true });

  const createFile = await fsp.open(path.join(outDir, "create.sql"), "w");
  try {
    const append = async (sql: string) => { await createFile.write(sql); };
    await generateCreate(append, result, appVersion);
  } finally {
    await createFile.close();
  }

  const dropFile = await fsp.open(path.join(outDir, "drop.sql"), "w");
  try {
    const append = async (sql: string) => { await dropFile.write(sql); };
    await generateDrop(append, result, appVersion);
  } finally {
    await createFile.close();
  }
}

async function deployToDatabase(config: pg.ClientConfig, result: CompileResult, appVersion?: string | boolean) {
  const client = new pg.default.Client(config);
  try {
    await client.connect();
    console.log(`Deploying to database: ${client.host}:${client.port ?? 5432}/${client.database}`);
    const append = async (sql: string) => { await client.query(sql); };
    await generateCreate(append, result, appVersion);
  } finally {
    await client.end();
  }
}

function getPackageVersion(): string {
  const __dirname = import.meta.dirname;
  const packageJsonPath = path.join(__dirname, "..", "package.json");
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf-8")) as { version: string };
  return packageJson.version;
}

const program = new Command();

program
  .name("dbosc")
  .description('DBOS Stored Procedure compiler')
  .version(getPackageVersion());

interface CompileOptions {
  outDir?: string,
  appVersion?: string | boolean,
}

program
  // FYI, commander package doesn't seem to handle app version options correctly in deploy subcommand if compile isn't also a subcommand
  .command("compile")
  .argument('<tsconfigPath>', 'path to tsconfig.json')
  .option('-o, --out <string>', 'path to output folder')
  .option('--app-version <string>', 'override DBOS__APPVERSION environment variable')
  .option('--no-app-version', 'ignore DBOS__APPVERSION environment variable')
  .action(async (tsconfigPath: string, options: CompileOptions) => {
    const compileResult = compile(tsconfigPath);
    if (compileResult) {
      const outDir = options.outDir ?? process.cwd();
      await emitSqlFiles(outDir, compileResult, options.appVersion);
    }
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
    const compileResult = compile(tsconfigPath);
    if (compileResult) {
      const [dbosConfig,] = parseConfigFile(options);
      await deployToDatabase(dbosConfig.poolConfig, compileResult, options.appVersion);
    }
  });

program.parse(process.argv);
