import * as ts from 'typescript';
import { WinstonLogger, createGlobalLogger } from "../telemetry/logs";

export interface DecoratorInfo {
  readonly node: ts.Decorator;
  readonly args: readonly ts.Expression[];
  readonly identifier: ts.Identifier;
  readonly module: string;
}

export interface ClassInfo {
  readonly node: ts.ClassDeclaration;
  readonly symbol?: ts.Symbol;
  readonly decorators: readonly DecoratorInfo[];
  readonly methods: readonly MethodInfo[];
}

export interface MethodInfo {
  readonly node: ts.MethodDeclaration;
  readonly symbol?: ts.Symbol;
  readonly decorators: readonly DecoratorInfo[];
  readonly parameters: readonly ParameterInfo[];
  readonly returnType?: ts.Type;
}

export interface ParameterInfo {
  readonly node: ts.ParameterDeclaration;
  readonly symbol?: ts.Symbol;
  readonly decorators: readonly DecoratorInfo[];
  readonly type?: ts.Type;
}

export class TypeParser {
  readonly #checker: ts.TypeChecker;
  readonly log: WinstonLogger;
  constructor(readonly program: ts.Program, logger?: WinstonLogger) {
    this.log = logger ?? createGlobalLogger();
    this.#checker = program.getTypeChecker();
  }

  getTypeInfo(): ClassInfo[] {
    const sourceFiles = this.program.getSourceFiles()
      .filter(sf => !sf.isDeclarationFile);

    const classInfos = new Array<ClassInfo>();
    for (const sourceFile of sourceFiles) {
      ts.forEachChild(sourceFile, node => {
        if (ts.isClassDeclaration(node)) {
          classInfos.push(this.getClassInfo(node));
        }
      });
    }
    return classInfos;
  }

  getClassInfo(node: ts.ClassDeclaration): ClassInfo {
    const symbol = node.name ? this.#checker.getSymbolAtLocation(node.name) : undefined;
    const decorators = this.getDecorators(node);
    const methods = node.members
      .filter(m => ts.isMethodDeclaration(m))
      .map(m => this.getMethodInfo(m as ts.MethodDeclaration));
    return { node, symbol, decorators, methods };
  }

  getMethodInfo(node: ts.MethodDeclaration): MethodInfo {
    const symbol = this.#checker.getSymbolAtLocation(node.name);
    const decorators = this.getDecorators(node);
    const parameters = node.parameters
      .map(p => this.getParameterInfo(p))
      .filter(p => p !== undefined) as ParameterInfo[];
    const signature = this.#checker.getSignatureFromDeclaration(node);
    const returnType = signature ? this.#checker.getReturnTypeOfSignature(signature) : undefined;
    return { node, symbol, decorators, parameters, returnType };
  }

  getParameterInfo(node: ts.ParameterDeclaration): ParameterInfo {
    const decorators = this.getDecorators(node);
    const symbol = this.#checker.getSymbolAtLocation(node.name);

    const type = node.type
      ? this.#checker.getTypeFromTypeNode(node.type)
      : symbol ? this.#checker.getTypeOfSymbol(symbol) : undefined;

    return { node, symbol, decorators, type };
  }

  getDecorators(node: ts.HasDecorators): readonly DecoratorInfo[] {
    return ts.getDecorators(node)?.map(d => this.getDecorator(d)) ?? [];
  }

  getDecorator(node: ts.Decorator): DecoratorInfo {
    if (ts.isCallExpression(node.expression)) {
      if (ts.isIdentifier(node.expression.expression)) {
        const { identifier, module } = this.getImportName(node.expression.expression);
        return { node, identifier, module, args: node.expression.arguments };
      }
      throw new Error(`Unexpected decorator CallExpression.expression type: ${ts.SyntaxKind[node.expression.expression.kind]}`)
    }

    if (ts.isIdentifier(node.expression)) {
      const { identifier, module } = this.getImportName(node.expression);
      return { node, identifier, module, args: [] };
    }

    throw new Error(`Unexpected decorator expression type: ${ts.SyntaxKind[node.expression.kind]}`)
  }

  getImportName(node: ts.Identifier): { identifier: ts.Identifier; module: string; } {
    const symbol = this.#checker.getSymbolAtLocation(node);
    const decls = symbol?.getDeclarations() ?? [];

    for (const decl of decls) {
      if (ts.isImportSpecifier(decl)) {
        // propertyName is the name as specified in the defining module
        // name is the (potentially different) name used in the local module
        // propertyName is undefined if the name isn't overridden
        const identifier = (decl.propertyName ?? decl.name);
        const module = decl.parent.parent.parent.moduleSpecifier;
        if (ts.isStringLiteral(module)) {
          return { identifier, module: module.text };
        }

        throw new Error(`Unexpected module specifier type: ${ts.SyntaxKind[module.kind]}`)
      }
      throw new Error(`Unexpected decorator declaration type: ${ts.SyntaxKind[decl.kind]}`)
    }
    throw new Error(`Could not find import specifier for ${node.getText()}`);
  }
}
