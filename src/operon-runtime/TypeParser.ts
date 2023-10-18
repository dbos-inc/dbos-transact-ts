import * as ts from 'typescript';
import { WinstonLogger, createGlobalLogger } from "../telemetry/logs";

export interface ImportNameInfo {
  readonly identifier: ts.Identifier;
  readonly module: string;
}

export interface DecoratorInfo {
  readonly args: readonly ts.Expression[];
  readonly identifier: ts.Identifier;
  readonly module: string;
}

export interface ParameterInfo {
  readonly symbol?: ts.Symbol;
  readonly type?: ts.Type;
  readonly decorators: readonly DecoratorInfo[];
}

export interface MethodInfo {
  readonly symbol?: ts.Symbol;
  readonly decorators: readonly DecoratorInfo[];
  readonly parameters: readonly ParameterInfo[];
  readonly returnType?: ts.Type;
}

export interface ClassInfo {
  readonly symbol?: ts.Symbol;
  readonly decorators: readonly DecoratorInfo[];
  readonly methods: readonly MethodInfo[];
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
          const decorators = this.getDecorators(node);
          const symbol = node.name ? this.#checker.getSymbolAtLocation(node.name) : undefined;
          const methods = node.members.filter(m => ts.isMethodDeclaration(m)).map(m => this.getMethodInfo(m as ts.MethodDeclaration));
          classInfos.push({ symbol, decorators, methods });
        }
      });
    }
    return classInfos;
  }

  getMethodInfo(node: ts.MethodDeclaration): MethodInfo {
    if (!ts.isIdentifier(node.name)) throw new Error(`Expected method name to be an identifier but found ${ts.SyntaxKind[node.name.kind]}`);

    const symbol = this.#checker.getSymbolAtLocation(node.name);
    const decorators = this.getDecorators(node);

    const signature = this.#checker.getSignatureFromDeclaration(node);
    const returnType = signature ? this.#checker.getReturnTypeOfSignature(signature) : undefined;
    const parameters = node.parameters.map(p => this.getParameterInfo(p));
    return { symbol, decorators, parameters, returnType };
  }

  getParameterInfo(node: ts.ParameterDeclaration): ParameterInfo {
    const decorators = this.getDecorators(node);
    const symbol = this.#checker.getSymbolAtLocation(node.name);
    const type = node.type
      ? this.#checker.getTypeFromTypeNode(node.type)
      : symbol ? this.#checker.getTypeOfSymbol(symbol) : undefined;

    return { symbol, type, decorators };
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

  getDecorators(node: ts.HasDecorators): readonly DecoratorInfo[] {
    return ts.getDecorators(node)?.map(d => this.getDecorator(d)) ?? [];
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
