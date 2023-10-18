import * as ts from 'typescript';
import { WinstonLogger, createGlobalLogger } from "../telemetry/logs";

export interface ImportNameInfo {
  identifier: ts.Identifier;
  module: string;
}

export interface DecoratorInfo {
  args: readonly ts.Expression[];
  identifier: ts.Identifier;
  module: string;
}

export interface ParameterInfo {
  symbol: ts.Symbol;
  type: ts.Type;
}

export interface MethodInfo {
  symbol: ts.Symbol | undefined;
  decorators: DecoratorInfo[];
  parameters: ParameterInfo[];
  returnType: ts.Type;
}

export interface ClassInfo {
  symbol: ts.Symbol | undefined;
  methods: MethodInfo[];
}

export class TypeParser {
  readonly #checker: ts.TypeChecker;
  readonly log: WinstonLogger;
  constructor(readonly program: ts.Program, logger?: WinstonLogger) {
    this.log = logger ?? createGlobalLogger();
    this.#checker = program.getTypeChecker();
  }

  getTypeInfo(): ClassInfo[] {
    const sourceFiles = this.program.getSourceFiles().filter(sf => !sf.isDeclarationFile);

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
    const methods = node.members.filter(m => ts.isMethodDeclaration(m)).map(m => this.getMethodInfo(m as ts.MethodDeclaration));
    return { symbol, methods };
  }

  getMethodInfo(node: ts.MethodDeclaration): MethodInfo {
    if (!ts.isIdentifier(node.name)) throw new Error(`Expected method name to be an identifier but found ${ts.SyntaxKind[node.name.kind]}`);
    const symbol = this.#checker.getSymbolAtLocation(node.name);
    const decorators = (ts.getDecorators(node) ?? []).map(d => this.getDecorator(d));

    const sig = this.#checker.getSignatureFromDeclaration(node);
    const returnType = this.#checker.getReturnTypeOfSignature(sig!);
    const parameters = sig!.parameters.map(s => ({ symbol: s, type: this.#checker.getTypeOfSymbol(s) }));
    return { symbol, decorators, parameters, returnType };
  }

  getImportName(node: ts.Identifier): ImportNameInfo {
    const symbol = this.#checker.getSymbolAtLocation(node);
    const decls = symbol?.getDeclarations() ?? [];
    if (decls.length !== 1) {
      throw new Error(`Expected exactly one declaration for ${node.text} symbol but found ${decls.length}`);
    }
    for (const decl of decls) {
      if (ts.isImportSpecifier(decl)) {
        // propertyName is the name as specified in the defining module
        // name is the (potentially different) name used in the local module
        // propertyName is undefined if the name isn't overridden
        const identifier = (decl.propertyName ?? decl.name);
        const moduleSpecifier = decl.parent.parent.parent.moduleSpecifier;
        if (ts.isStringLiteral(moduleSpecifier)) {
          const module = moduleSpecifier.text;
          return { identifier, module };
        } else {
          throw new Error(`Unsupported module specifier kind ${ts.SyntaxKind[moduleSpecifier.kind]}`);
        }
      }
    }
    throw new Error(`No supported declarations for ${node.text} symbol found`);
  }

  getDecorator(node: ts.Decorator): DecoratorInfo {
    if (ts.isCallExpression(node.expression) && ts.isIdentifier(node.expression.expression)) {
      const importName = this.getImportName(node.expression.expression);
      return { ...importName, args: node.expression.arguments };
    } else if (ts.isIdentifier(node.expression)) {
      const importName = this.getImportName(node.expression);
      return { ...importName, args: [] };
    }

    throw new Error(`Invalid decorator expression kind ${ts.SyntaxKind[node.expression.kind]}`);
  }
}
