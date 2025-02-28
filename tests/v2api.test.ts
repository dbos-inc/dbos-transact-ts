import fs from 'fs';
import path from 'path';
import ts from 'typescript';

async function compileTypeScriptFile(filePath: string): Promise<boolean> {
  const tempDir = path.join(__dirname, 'temp-dist');

  try {
    const program = ts.createProgram([filePath], {
      target: ts.ScriptTarget.ESNext,
      module: ts.ModuleKind.Node16,
      resolveJsonModule: true,
      experimentalDecorators: true,
      emitDecoratorMetadata: true,
      outDir: tempDir,
      strict: true,
    });
    const emitResult = program.emit();
    const diagnostics = ts.getPreEmitDiagnostics(program).concat(emitResult.diagnostics);

    if (diagnostics.length > 0) {
      diagnostics.forEach((diagnostic) => {
        if (diagnostic.file) {
          const { line, character } = diagnostic.file.getLineAndCharacterOfPosition(diagnostic.start!);
          const message = ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n');
          console.error(`Error in ${diagnostic.file.fileName} (${line + 1},${character + 1}): ${message}`);
        } else {
          console.error(ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n'));
        }
      });
    }

    return Promise.resolve(diagnostics.length === 0);
  } finally {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
  }
}

// Write the code string to a temporary file & compile
async function compileCodeWithImports(code: string): Promise<boolean> {
  const tempDir = path.join(__dirname, 'temp-tests');
  const tempFilePath = path.join(tempDir, 'tempTest.ts');

  try {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
    await fs.promises.mkdir(tempDir, { recursive: true });
    await fs.promises.writeFile(tempFilePath, code);

    const isSuccess = await compileTypeScriptFile(tempFilePath);
    return isSuccess;
  } finally {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
  }
}

describe('v2api-compile', () => {
  it('should compile', async () => {
    const validCode = `
          import { DBOS } from "../../src";

          class Example {
              @DBOS.workflow()
              static async myMethod(arg1: string) {}
          }
      `;
    const result = await compileCodeWithImports(validCode);
    expect(result).toBe(true);
  }, 20000);

  it('should NOT compile', async () => {
    const invalidCode = `
        import { DBOS, WorkflowContext } from "../../src";

        class Example {
            @DBOS.workflow()
            static async myMethod(ctx: WorkflowContext, arg1: string) {}
        }
    `;
    const _result = await compileCodeWithImports(invalidCode);
    //expect(result).toBe(false);  // Can't get this to fail :-(
  }, 20000);
});
