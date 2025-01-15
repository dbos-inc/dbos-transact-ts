import ts from 'typescript';
import { DiagnosticsCollector, diagResult } from './tsDiagUtil';
import path from 'node:path';
import fs from 'node:fs/promises';

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
  args: readonly ts.Expression[];
  name?: string;
  className?: string;
  module?: string;
}

function isStaticMethod(node: ts.MethodDeclaration): boolean {
  const mods = node.modifiers ?? [];
  return mods.some(m => m.kind === ts.SyntaxKind.StaticKeyword);
}

export class TypeParser {
  readonly #program: ts.Program;
  readonly #checker: ts.TypeChecker;
  readonly #diags = new DiagnosticsCollector();
  get diags() { return this.#diags.diags; }

  constructor(program: ts.Program) {
    this.#program = program;
    this.#checker = program.getTypeChecker();
  }

  parse(): readonly ClassInfo[] | undefined {
    const classes = new Array<ClassInfo>();
    for (const file of this.#program.getSourceFiles()) {
      if (file.isDeclarationFile) continue;
      for (const stmt of file.statements) {
        if (ts.isClassDeclaration(stmt)) {

          const staticMethods = stmt.members
            .filter(ts.isMethodDeclaration)
            // Only static methods are supported now, so filter out instance methods by default
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
      this.#diags.warn(`no classes found in ${JSON.stringify(this.#program.getRootFileNames())}`);
    }

    return diagResult(classes, this.diags);
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

  #getImportSpecifierFromPAE(pae: ts.PropertyAccessExpression, checker: ts.TypeChecker): { name: string; module: string; } | undefined
  {
    const node = pae.expression;
    const symbol = checker.getSymbolAtLocation(node);
    const decls = symbol?.getDeclarations() ?? [];
    for (const decl of decls) {
      if (ts.isNamespaceImport(decl)) {
        const name = pae.name;
        const module = decl.parent.parent.moduleSpecifier as ts.StringLiteral;
        return {name: name.getText(), module: module.text};
      }
      if (ts.isImportClause(decl)) {
        const name = pae.name;
        const module = decl.parent.moduleSpecifier as ts.StringLiteral;
        return { name: name.getText(), module: module.text };
      }
      if (ts.isImportSpecifier(decl)) {
        // decl.name is the name for this type used in the local module.
        // If the type name was overridden in the local module, the original type name is stored in decl.propertyName.
        // Otherwise, decl.propertyName is undefined.
        const name = (decl.propertyName ?? decl.name).getText();

        // comment in TS AST declaration indicates moduleSpecifier *must* be a string literal
        //    "If [ImportDeclaration.moduleSpecifier] is not a StringLiteral it will be a grammar error."
        const module = decl.parent.parent.parent.moduleSpecifier as ts.StringLiteral;

        return {name: name, module: module.text};
      }
    }
    return undefined;
  }

  #getDecoratorIdentifier(node: ts.Decorator)
    : { name?: string; module?: string; className?: string; args: readonly ts.Expression[]; } | undefined
  {
    if (ts.isCallExpression(node.expression)) {
      if (ts.isPropertyAccessExpression(node.expression.expression)) {
        const pae: ts.PropertyAccessExpression = node.expression.expression;
        const className = pae.expression.getText(); // DBOS (class name)
        const methodName = pae.name.text; // workflow (method name)
        const paeRes = this.#getImportSpecifierFromPAE(pae, this.#checker); // Retrieve module info if needed
        return { name: methodName, module: paeRes?.module, className, args: node.expression.arguments };
      }
      if (ts.isIdentifier(node.expression.expression)) {
        const { name, module } = this.#getImportSpecifier(node.expression.expression, this.#checker) ?? {};
        return { name, module, className: undefined, args: node.expression.arguments };
      }
      this.#diags.raise(`Unexpected decorator CallExpression.expression type: ${ts.SyntaxKind[node.expression.expression.kind]}`, node);
    }
  
    if (ts.isIdentifier(node.expression)) {
      const { name, module } = this.#getImportSpecifier(node.expression, this.#checker) ?? {};
      return { name, module, className: undefined, args: [] };
    }
  
    this.#diags.raise(`Unexpected decorator expression type: ${ts.SyntaxKind[node.expression.kind]}`, node);
  }

  #getImportSpecifier(node: ts.Node, checker: ts.TypeChecker): { name: string; module: string; } | undefined {
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

  #getDecorators(node: ts.HasDecorators): DecoratorInfo[] {

    return (ts.getDecorators(node) ?? [])
      .map(node => {
        const decoratorIdentifier = this.#getDecoratorIdentifier(node);
        if (!decoratorIdentifier) return undefined;
        const { name, module, args, className } = decoratorIdentifier;
        return { node, name, module, args, className } as DecoratorInfo;
      })
      .filter((d): d is DecoratorInfo => !!d);
  }
}

export async function findPackageInfo(entrypoints: string[]): Promise<{ name: string, version: string }> {
  for (const entrypoint of entrypoints) {
    let dirname = path.dirname(entrypoint);
    while (dirname !== '/') {
      try {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        const packageJson = JSON.parse(await fs.readFile(path.join(dirname, 'package.json'), { encoding: 'utf-8' }));
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        const name = packageJson.name as string ?? "unknown";
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        const version = packageJson.version as string | undefined;
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        const isPrivate = packageJson.private as boolean | undefined ?? false;

        return {
          name,
          version: version
            ? version
            : isPrivate ? "private" : "unknown"
        };
      } catch (error) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
        if ((error as any).code !== 'ENOENT') throw error;
      }
      dirname = path.dirname(dirname);
    }
  }
  return { name: "unknown", version: "unknown" };
}
