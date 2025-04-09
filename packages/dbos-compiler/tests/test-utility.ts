import tsm from 'ts-morph';
import * as path from 'node:path';
import * as fs from 'node:fs/promises';

type TestSource = string | { code: string; filename?: string };

export async function readTestContent(filename: string) {
  return await fs.readFile(path.join(__dirname, filename), 'utf-8');
}

export function makeTestProject(...sources: TestSource[]) {
  const project = new tsm.Project({
    compilerOptions: {
      target: tsm.ScriptTarget.ES2015,
    },
    useInMemoryFileSystem: true,
  });
  project.createSourceFile('knex.d.ts', knex);
  project.createSourceFile('dbos-sdk.d.ts', dbosSdk);

  const sourceFiles = new Array<tsm.SourceFile>();
  for (const source of sources) {
    const { code, filename = 'operations.ts' } = typeof source === 'string' ? { code: source } : source;
    const file = project.createSourceFile(filename, code);
    sourceFiles.push(file);
  }

  const diags = formatDiagnostics(project.getPreEmitDiagnostics());
  if (diags) {
    throw new Error(diags);
  }

  return { project, sourceFiles };
}

function formatDiagnostics(diags: readonly tsm.Diagnostic[]) {
  if (diags.length === 0) {
    return;
  }

  const formatHost: tsm.ts.FormatDiagnosticsHost = {
    getCurrentDirectory: () => tsm.ts.sys.getCurrentDirectory(),
    getNewLine: () => tsm.ts.sys.newLine,
    getCanonicalFileName: (fileName: string) =>
      tsm.ts.sys.useCaseSensitiveFileNames ? fileName : fileName.toLowerCase(),
  };

  return tsm.ts.formatDiagnosticsWithColorAndContext(
    diags.map((d) => d.compilerObject),
    formatHost,
  );
}

const knex = /*ts*/ `
declare module 'knex' {
  export interface Knex {}
}
`;

const dbosSdk = /*ts*/ `
declare module "@dbos-inc/dbos-sdk" {
  export interface Logger {
    info(logEntry: unknown): void;
    debug(logEntry: unknown): void;
    warn(logEntry: unknown): void;
    error(inputError: unknown): void;
  }

  export interface PGClient {
    query<T>(query: string, values?: any[]): Promise<{ rows: T[] }>;
  }

  export interface DBOSContext {
    readonly logger: Logger;
  }

  export interface WorkflowConfig { }
  export interface TransactionConfig {
    isolationLevel?: "READ UNCOMMITTED" | "READ COMMITTED" | "REPEATABLE READ" | "SERIALIZABLE";
    readOnly?: boolean;
  }
  export interface StepConfig {
    retriesAllowed?: boolean;
    intervalSeconds?: number;
    maxAttempts?: number;
    backoffRate?: number;
  }
  export interface StoredProcedureConfig {
    isolationLevel?: "READ UNCOMMITTED" | "READ COMMITTED" | "REPEATABLE READ" | "SERIALIZABLE";
    readOnly?: boolean;
    executeLocally?: boolean;
  }

  export function GetApi(url:string);
  export function PostApi(url:string);
  export function PutApi(url:string);
  export function PatchApi(url:string);
  export function DeleteApi(url:string);

  export function Workflow(config?: WorkflowConfig);
  export function Communicator(config?: StepConfig);
  export function Step(config?: StepConfig);
  export function Transaction(config?: TransactionConfig);
  export function StoredProcedure(config?: StoredProcedureConfig);
  export function DBOSDeploy();
  export function DBOSInitializer();

  export interface HandlerContext extends DBOSContext { }
  export interface WorkflowContext extends DBOSContext { }
  export interface CommunicatorContext extends DBOSContext { }
  export interface StepContext extends DBOSContext { }
  export interface TransactionContext<T> extends DBOSContext { }
  export interface StoredProcedureContext extends DBOSContext { }
  export interface InitContext extends DBOSContext {}

  export class DBOS {
    static readonly logger: Logger;
    static readonly pgClient: PGClient;

    static workflow(config?: WorkflowConfig);
    static transaction(config?: TransactionConfig);
    static step(config?: StepConfig);
    static storedProcedure(config?: StoredProcedureConfig);
    static getApi(url: string);
    static postApi(url: string);
    static putApi(url: string);
    static patchApi(url: string);
    static deleteApi(url: string);
    static launch();
    static shutdown();
  }
}
`;
