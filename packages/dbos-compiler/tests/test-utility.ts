import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import tsm from 'ts-morph';
import * as Diff from 'diff';
import colors from 'colors/safe'

// TODO: move this code back into the tests folder when we remove the -t option

// const testDataRoot = path.join(__dirname, "data");
const testDataRoot = path.join(__dirname, "..", "tests", "data");

function getTestDataInputPath(projectName: string) {
  return path.join(testDataRoot, projectName, "input");
}

function getTestDataOutputPath(projectName: string) {
  return path.join(testDataRoot, projectName, "output");
}

export function getTestProjects() {
  const projects = new Array<string>();
  for (const file of fs.readdirSync(testDataRoot)) {
    const stat = fs.statSync(path.join(testDataRoot, file));
    if (stat.isDirectory()) {
      projects.push(file);
    }
  }
  return projects
}

async function copyFolder(srcPath: string, destPath: string, $fs: tsm.FileSystemHost) {
  const files = await fsp.readdir(srcPath);
  for (const file of files) {
    const srcFilePath = path.join(srcPath, file);
    const destFilePath = path.join(destPath, file);
    const srcStat = await fsp.stat(srcFilePath);
    if (srcStat.isDirectory()) {
      await copyFolder(srcFilePath, destFilePath, $fs);
    } else {
      const contents = await fsp.readFile(srcFilePath, "utf-8");
      await $fs.writeFile(destFilePath, contents);
    }
  }
}

export async function copyPackage(packageName: string, $fs: tsm.FileSystemHost) {
  const packageJsonPath = require.resolve(packageName + "/package.json");
  const packageRoot = path.dirname(packageJsonPath);
  const targetRoot = path.join("/node_modules", packageName);
  await copyFolder(packageRoot, targetRoot, $fs);
}

export async function createTestProject(name: string) {
  const fs = new tsm.InMemoryFileSystemHost();
  const srcPath = getTestDataInputPath(name);
  await copyFolder(srcPath, "/", fs);

  const project = new tsm.Project({
    compilerOptions: {
      declaration: false,
      declarationMap: false,
      experimentalDecorators: true,
      emitDecoratorMetadata: false,
      module: tsm.ts.ModuleKind.CommonJS,
      sourceMap: false,
      target: tsm.ts.ScriptTarget.ES2022,
    },
    fileSystem: fs,
  });

  const sourceFiles = await fs.glob(["**/*.ts"]);
  for (const sourceFile of sourceFiles) {
    project.addSourceFileAtPath(sourceFile);
  }

  return project;
}

export async function verifyTestOutput(project: tsm.Project, name: string) {
  const outputRoot = getTestDataOutputPath(name);
  for (const file of project.getSourceFiles()) {
    const results = file.getEmitOutput();
    expect(results.getEmitSkipped()).toBe(false);
    for (const outputFile of results.getOutputFiles()) {
      const filePath = outputFile.getFilePath();
      const expectedPath = path.join(outputRoot, filePath);
      const expected = await fsp.readFile(expectedPath, "utf-8");
      const actual = outputFile.getText();

      const patch = Diff.structuredPatch(name, name, expected, actual);
      if (patch.hunks.length > 0) {
        const hunks = patch.hunks.map(h => {
          const lines = h.lines.map(l => {
            if (l[0] === '+') { return colors.red(l);}
            if (l[0] === '-') { return colors.green(l);}
            return l;
          })
          return lines.join('\n');
        }).join('\n');
        throw new Error(hunks);
      }
    }
  }
}

type TestSource = string | { code: string, filename?: string };

export function makeTestProject(...sources: TestSource[]) {

  const project = new tsm.Project({
    compilerOptions: {
      target: tsm.ScriptTarget.ES2015
    },
    useInMemoryFileSystem: true
  });
  project.createSourceFile("knex.d.ts", knex);
  project.createSourceFile("dbos-sdk.d.ts", dbosSdk);

  const sourceFiles = new Array<tsm.SourceFile>();
  for (const source of sources) {
    const { code, filename = "operations.ts" } = typeof source === "string" ? { code: source } : source;
    const file = project.createSourceFile(filename, code);
    sourceFiles.push(file);
  }

  const diags = formatDiagnostics(project.getPreEmitDiagnostics());
  if (diags) { throw new Error(diags); }

  return { project, sourceFiles };
}

function formatDiagnostics(diags: readonly tsm.Diagnostic[]) {
  if (diags.length === 0) { return; }

  const formatHost: tsm.ts.FormatDiagnosticsHost = {
    getCurrentDirectory: () => tsm.ts.sys.getCurrentDirectory(),
    getNewLine: () => tsm.ts.sys.newLine,
    getCanonicalFileName: (fileName: string) => tsm.ts.sys.useCaseSensitiveFileNames
      ? fileName : fileName.toLowerCase()
  }

  return tsm.ts.formatDiagnosticsWithColorAndContext(diags.map(d => d.compilerObject), formatHost);
}

const knex = /*ts*/`
declare module 'knex' {
  export interface Knex {}
}
`;

const dbosSdk = /*ts*/`
declare module "@dbos-inc/dbos-sdk" {
  export interface Logger {
    info(logEntry: unknown): void;
    debug(logEntry: unknown): void;
    warn(logEntry: unknown): void;
    error(inputError: unknown): void;
  }

  export interface DBOSContext {
    readonly logger: Logger;
  }

  export interface WorkflowConfig { }
  export interface TransactionConfig {
    isolationLevel?: "READ UNCOMMITTED" | "READ COMMITTED" | "REPEATABLE READ" | "SERIALIZABLE";
    readOnly?: boolean;
  }
  export interface CommunicatorConfig {
    retriesAllowed?: boolean;
    intervalSeconds?: number;
    maxAttempts?: number;
    backoffRate?: number;
  }

  export function GetApi(url:string);
  export function PostApi(url:string);
  export function Workflow(config?: WorkflowConfig);
  export function Communicator(config?: CommunicatorConfig);
  export function Transaction(config?: TransactionConfig);
  export function StoredProcedure(config?: TransactionConfig);
  export function DBOSDeploy();
  export function DBOSInitializer();

  export interface HandlerContext extends DBOSContext { }
  export interface WorkflowContext extends DBOSContext { }
  export interface CommunicatorContext extends DBOSContext { }
  export interface TransactionContext<T> extends DBOSContext { }
  export interface StoredProcedureContext extends DBOSContext { }
  export interface InitContext extends DBOSContext {}
}
`;
