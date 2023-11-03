import ts from 'typescript';
import { createDiagnostic, diagResult } from './tsDiagUtil';

export interface ClassInfo {
  readonly node: ts.ClassDeclaration;
  readonly name?: string;
  readonly decorators: readonly DecoratorInfo[];
  readonly methods: readonly MethodInfo[];
}

export interface MethodInfo {
  readonly node: ts.MethodDeclaration;
  readonly name: string;
  readonly decorators: readonly DecoratorInfo[];
  readonly parameters: readonly ParameterInfo[];
}

export interface ParameterInfo {
  readonly node: ts.ParameterDeclaration;
  readonly name: string;
  readonly decorators: readonly DecoratorInfo[];
  readonly required: boolean;
}

export interface DecoratorInfo {
  node: ts.Decorator;
  identifier: ts.Identifier;
  args: readonly ts.Expression[];
  name?: string;
  module?: string;
}

function isStaticMethod(node: ts.MethodDeclaration): boolean {
  const mods = node.modifiers ?? [];
  return mods.some(m => m.kind === ts.SyntaxKind.StaticKeyword);
}

export class TypeParser {
  readonly #program: ts.Program;
  readonly #checker: ts.TypeChecker;
  readonly #diags = new Array<ts.Diagnostic>();
  get diags() { return this.#diags as readonly ts.Diagnostic[]; }

  constructor(program: ts.Program) {
    this.#program = program;
    this.#checker = program.getTypeChecker();
  }

  #raise(message: string, node?: ts.Node): void {
    this.#diags.push(createDiagnostic(message, { node }));
  }

  static parse(program: ts.Program): readonly ClassInfo[] | undefined {
    const parser = new TypeParser(program);
    return parser.parse();
  }

  parse(): readonly ClassInfo[] | undefined {
    const classes = new Array<ClassInfo>();
    for (const file of this.#program.getSourceFiles()) {
      if (file.isDeclarationFile) continue;
      for (const stmt of file.statements) {
        if (ts.isClassDeclaration(stmt)) {

          const staticMethods = stmt.members
            .filter(ts.isMethodDeclaration)
            // Operon only supports static methods, so filter out instance methods by default
            .filter(isStaticMethod)
            .map(m => this.#getMethod(m));

          classes.push({
            node: stmt,
            // a class may not have a name if it's the default export
            name: stmt.name?.getText(),
            decorators: this.#getDecorators(stmt),
            methods: staticMethods,
          });
        }
      }
    }

    if (classes.length === 0) {
      this.#diags.push(createDiagnostic(`no classes found in ${JSON.stringify(this.#program.getRootFileNames())}`, { category: ts.DiagnosticCategory.Warning }));
    }

    return diagResult(classes, this.#diags);
  }

  #getMethod(node: ts.MethodDeclaration): MethodInfo {
    const name = node.name.getText();
    const decorators = this.#getDecorators(node);
    const parameters = node.parameters.map(p => this.#getParameter(p));
    return { node, name, decorators, parameters };
  }

  #getParameter(node: ts.ParameterDeclaration): ParameterInfo {
    const decorators = this.#getDecorators(node);
    const name = node.name.getText();
    const required = !node.questionToken && !node.initializer;
    return { node, name, decorators, required };
  }

  #getDecoratorIdentifier(node: ts.Decorator): { identifier: ts.Identifier; args: readonly ts.Expression[]; } | undefined {
    if (ts.isCallExpression(node.expression)) {
      if (ts.isIdentifier(node.expression.expression)) {
        return { identifier: node.expression.expression, args: node.expression.arguments };
      }
      this.#raise(`Unexpected decorator CallExpression.expression type: ${ts.SyntaxKind[node.expression.expression.kind]}`, node);
    }

    if (ts.isIdentifier(node.expression)) {
      return { identifier: node.expression, args: [] };
    }
    this.#raise(`Unexpected decorator expression type: ${ts.SyntaxKind[node.expression.kind]}`, node);
  }

  #getDecorators(node: ts.HasDecorators): DecoratorInfo[] {

    return (ts.getDecorators(node) ?? [])
      .map(node => {
        const decoratorIdentifier = this.#getDecoratorIdentifier(node);
        if (!decoratorIdentifier) return undefined;
        const { identifier, args } = decoratorIdentifier;
        const { name, module } = getImportSpecifier(identifier, this.#checker) ?? {};
        return { node, identifier, name, module, args } as DecoratorInfo;
      })
      .filter((d): d is DecoratorInfo => !!d);

    function getImportSpecifier(node: ts.Node, checker: ts.TypeChecker): { name: string; module: string; } | undefined {
      const symbol = checker.getSymbolAtLocation(node);
      const decls = symbol?.getDeclarations() ?? [];
      for (const decl of decls) {
        if (ts.isImportSpecifier(decl)) {
          // decl.name is the name for this type used in the local module.
          // If the type name was overridden in the local module, the original type name is stored in decl.propertyName.
          // Otherwise, decl.propertyName is undefined.
          const name = (decl.propertyName ?? decl.name).getText();

          // comment in TS AST declaration indicates moduleSpecifier *must* be a string literal
          //    "If [ImportDeclaration.moduleSpecifier] is not a StringLiteral it will be a grammar error."
          const module = decl.parent.parent.parent.moduleSpecifier as ts.StringLiteral;

          return { name, module: module.text };
        }
      }
      return undefined;
    }
  }
}
