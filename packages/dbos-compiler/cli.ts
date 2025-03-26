#!/usr/bin/env node

import { Command, Option } from 'commander';
import path from 'node:path';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import { generateCreate, generateDrop } from './generator.js';
import { parseConfigFile } from '@dbos-inc/dbos-sdk/dist/src/dbos-runtime/config.js';
import { DBOSConfigInternal } from '@dbos-inc/dbos-sdk/dist/src/dbos-executor';
import { Client, ClientConfig } from 'pg';
import { type CompileResult, compile } from './compiler.js';

async function emitSqlFiles(outDir: string, result: CompileResult, appVersion?: string | boolean) {
  await fsp.mkdir(outDir, { recursive: true });

  const createFile = await fsp.open(path.join(outDir, 'create.sql'), 'w');
  try {
    const executeSql = async (sql: string) => {
      await createFile.write(sql);
    };
    await generateCreate(executeSql, result, appVersion);
  } finally {
    await createFile.close();
  }

  const dropFile = await fsp.open(path.join(outDir, 'drop.sql'), 'w');
  try {
    const executeSql = async (sql: string) => {
      await dropFile.write(sql);
    };
    await generateDrop(executeSql, result, appVersion);
  } finally {
    await createFile.close();
  }
}

async function deployToDatabase(config: ClientConfig, result: CompileResult, appVersion?: string | boolean) {
  const client = new Client(config);
  try {
    await client.connect();
    console.log(`Deploying to database: ${client.host}:${client.port ?? 5432}/${client.database}`);
    const executeSql = async (sql: string) => {
      await client.query(sql);
    };
    await generateCreate(executeSql, result, appVersion);
  } finally {
    await client.end();
  }
}

async function dropFromDatabase(config: ClientConfig, result: CompileResult, appVersion?: string | boolean) {
  const client = new Client(config);
  try {
    await client.connect();
    console.log(`Dropping from database: ${client.host}:${client.port ?? 5432}/${client.database}`);
    const executeSql = async (sql: string) => {
      await client.query(sql);
    };
    await generateDrop(executeSql, result, appVersion);
  } finally {
    await client.end();
  }
}

function getPackageVersion(): string {
  const packageJsonPath = path.join(__dirname, '..', 'package.json');
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8')) as { version: string };
  return packageJson.version;
}

const program = new Command();

program.name('dbosc').description('DBOS Stored Procedure compiler').version(getPackageVersion());

export interface CommonOptions {
  appVersion?: string | boolean;
  suppressWarnings?: boolean;
}

interface CompileOptions extends CommonOptions {
  out?: string;
}

function verifyTsConfigPath(tsconfigPath: string | undefined, cwd?: string): string | undefined {
  cwd = cwd ?? process.cwd();

  if (!tsconfigPath) {
    tsconfigPath = path.join(cwd, 'tsconfig.json');
  }

  if (!fs.existsSync(tsconfigPath)) {
    console.error(`tsconfig file not found: ${tsconfigPath}`);
    return undefined;
  }

  return tsconfigPath;
}

const suppressWarningsOption = new Option('--suppress-warnings', 'Suppress warnings').hideHelp();

program
  // FYI, commander package doesn't seem to handle app version options correctly in deploy subcommand if compile isn't also a subcommand
  .command('compile')
  .description('Generate SQL files to create and drop @StoredProcedure routines from DBOS application')
  .argument('[tsconfigPath]', 'path to tsconfig.json')
  .option('-o, --out <string>', 'path to output folder')
  .option('--app-version <string>', 'override DBOS__APPVERSION environment variable')
  .option('--no-app-version', 'ignore DBOS__APPVERSION environment variable')
  .addOption(suppressWarningsOption)
  .action(async (tsconfigPath: string | undefined, options: CompileOptions) => {
    tsconfigPath = verifyTsConfigPath(tsconfigPath);
    if (tsconfigPath) {
      const compileResult = compile(tsconfigPath, options.suppressWarnings);
      if (compileResult) {
        const outDir = options.out ?? process.cwd();
        await emitSqlFiles(outDir, compileResult, options.appVersion);
      } else {
        console.warn('Compilation produced no result.');
      }
    }
  });

interface DeployOptions extends CommonOptions {
  appDir?: string;
}

program
  .command('deploy')
  .description('Deploy DBOS application @StoredProcedure routines to database specified in dbos-config.yaml')
  .argument('[tsconfigPath]', 'path to tsconfig.json')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .option('--app-version <string>', 'override DBOS__APPVERSION environment variable')
  .option('--no-app-version', 'ignore DBOS__APPVERSION environment variable')
  .addOption(suppressWarningsOption)
  .action(async (tsconfigPath: string | undefined, options: DeployOptions) => {
    tsconfigPath = verifyTsConfigPath(tsconfigPath, options.appDir);
    if (tsconfigPath) {
      const compileResult = compile(tsconfigPath, options.suppressWarnings);
      if (compileResult) {
        const [dbosConfig] = parseConfigFile(options) as [DBOSConfigInternal, unknown];
        await deployToDatabase(dbosConfig.poolConfig, compileResult, options.appVersion);
      }
    }
  });

program
  .command('drop')
  .description('Drop DBOS application @StoredProcedure routines from database specified in dbos-config.yaml')
  .argument('[tsconfigPath]', 'path to tsconfig.json')
  .option('-d, --appDir <string>', 'Specify the application root directory')
  .option('--app-version <string>', 'override DBOS__APPVERSION environment variable')
  .option('--no-app-version', 'ignore DBOS__APPVERSION environment variable')
  .addOption(suppressWarningsOption)
  .action(async (tsconfigPath: string | undefined, options: DeployOptions) => {
    tsconfigPath = verifyTsConfigPath(tsconfigPath, options.appDir);
    if (tsconfigPath) {
      const compileResult = compile(tsconfigPath, options.suppressWarnings);
      if (compileResult) {
        const [dbosConfig] = parseConfigFile(options) as [DBOSConfigInternal, unknown];
        await dropFromDatabase(dbosConfig.poolConfig, compileResult, options.appVersion);
      }
    }
  });

program.parse(process.argv);
