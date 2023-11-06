import * as ts from 'typescript';

const printer = ts.createPrinter();

export interface DiagnosticOptions {
  code?: number;
  node?: ts.Node;
  category?: ts.DiagnosticCategory;
}

export function createDiagnostic(messageText: string, options?: DiagnosticOptions): ts.Diagnostic {
  const node = options?.node;
  const category = options?.category ?? ts.DiagnosticCategory.Error;
  const code = options?.code ?? 0;

  return {
    category,
    code,
    file: node?.getSourceFile(),
    length: node?.getWidth(),
    messageText,
    start: node?.getStart(),
    source: node ? printer.printNode(ts.EmitHint.Unspecified, node, node.getSourceFile()) : undefined,
  };
}

export function diagResult<T>(value: T, diags: readonly ts.Diagnostic[]): T | undefined {
  return diags.some(e => e.category === ts.DiagnosticCategory.Error) ? undefined : value;
}

export function logDiagnostics(diags: readonly ts.Diagnostic[]): void {
  if (diags.length > 0) {
    const formatHost: ts.FormatDiagnosticsHost = {
      getCurrentDirectory: () => ts.sys.getCurrentDirectory(),
      getNewLine: () => ts.sys.newLine,
      getCanonicalFileName: (fileName: string) => ts.sys.useCaseSensitiveFileNames
        ? fileName : fileName.toLowerCase()
    }

    const text = ts.formatDiagnosticsWithColorAndContext(diags, formatHost);
    console.log(text);
  }
}

export class DiagnosticsCollector {
  readonly #diags = new Array<ts.Diagnostic>();
  get diags() { return this.#diags as readonly ts.Diagnostic[]; }

  raise(message: string, node?: ts.Node): void {
    this.#diags.push(createDiagnostic(message, { node }));
  }

  warn(message: string, node?: ts.Node): void {
    this.#diags.push(createDiagnostic(message, { node, category: ts.DiagnosticCategory.Warning }));
  }
}
