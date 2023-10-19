import * as ts from 'typescript';
import { WinstonLogger, createGlobalLogger } from "../telemetry/logs";

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
  readonly returnType?: ts.Type;
}

export interface ParameterInfo {
  readonly node: ts.ParameterDeclaration;
  readonly name: string;
  readonly decorators: readonly DecoratorInfo[];
  readonly type?: ts.Type;
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
  readonly #checker: ts.TypeChecker;
  readonly log: WinstonLogger;
  constructor(readonly program: ts.Program, logger?: WinstonLogger) {
    this.log = logger ?? createGlobalLogger();
    this.#checker = program.getTypeChecker();
  }

  parse(): readonly ClassInfo[] {
    const classes = new Array<ClassInfo>();
    for (const file of this.program.getSourceFiles()) {
      if (file.isDeclarationFile) continue;
      for (const stmt of file.statements) {
        if (ts.isClassDeclaration(stmt)) {

          const staticMethods = stmt.members
            .filter(ts.isMethodDeclaration)
            // Operon only supports static methods, so filter out instance methods by default
            .filter(isStaticMethod)
            .map(m => this.getMethod(m));

          classes.push({
            node: stmt,
            // a class may not have a name if it's the default export
            name: stmt.name?.getText(),
            decorators: this.getDecorators(stmt),
            methods: staticMethods,
          });
        }
      }
    }
    return classes;
  }

  getMethod(node: ts.MethodDeclaration): MethodInfo {
    const name = node.name.getText();
    const decorators = this.getDecorators(node);
    const parameters = node.parameters.map(p => this.getParameter(p));

    const signature = this.#checker.getSignatureFromDeclaration(node);
    const returnType = signature ? this.#checker.getReturnTypeOfSignature(signature) : undefined;
    return { node, name, decorators, parameters, returnType };
  }

  getParameter(node: ts.ParameterDeclaration): ParameterInfo {
    const decorators = this.getDecorators(node);
    const name = node.name.getText();
    const type = node.type ? this.#checker.getTypeFromTypeNode(node.type) : undefined;
    return { node, name, decorators, type };
  }

  getDecorators(node: ts.HasDecorators): DecoratorInfo[] {

    return (ts.getDecorators(node) ?? [])
      .map(node => {
        const { identifier, args, } = getDecoratorIdentifier(node);
        const { name, module } = getImportSpecifier(identifier, this.#checker) ?? {};
        return { node, identifier, name, module, args };
      });

    function getDecoratorIdentifier(node: ts.Decorator): { identifier: ts.Identifier; args: readonly ts.Expression[]; } {
      if (ts.isCallExpression(node.expression)) {
        if (ts.isIdentifier(node.expression.expression)) {
          return { identifier: node.expression.expression, args: node.expression.arguments };
        }
        throw new Error(`Unexpected decorator CallExpression.expression type: ${ts.SyntaxKind[node.expression.expression.kind]}`);
      }

      if (ts.isIdentifier(node.expression)) {
        return { identifier: node.expression, args: [] };
      }
      throw new Error(`Unexpected decorator expression type: ${ts.SyntaxKind[node.expression.kind]}`);
    }

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
    };
  }
}
