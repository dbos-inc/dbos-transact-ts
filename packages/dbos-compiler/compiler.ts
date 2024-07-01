import tsm from 'ts-morph';

export type CompileResult = {
  project: tsm.Project;
  methods: (readonly [tsm.MethodDeclaration, StoredProcedureConfig])[];
};

export type IsolationLevel = "READ UNCOMMITTED" | "READ COMMITTED" | "REPEATABLE READ" | "SERIALIZABLE";
export interface StoredProcedureConfig {
  isolationLevel?: IsolationLevel;
  readOnly?: boolean;
  executeLocally?: boolean
}

function hasError(diags: readonly tsm.ts.Diagnostic[]) {
  return diags.some(diag => diag.category === tsm.ts.DiagnosticCategory.Error);
}

export function compile(tsConfigFilePath: string): CompileResult | undefined {
  const diags = new Array<tsm.ts.Diagnostic>();
  try {
    const project = new tsm.Project({
      tsConfigFilePath,
      compilerOptions: {
        sourceMap: false,
        declaration: false,
        declarationMap: false,
      }
    });

    // remove test files
    for (const sourceFile of project.getSourceFiles()) {
      if (sourceFile.getBaseName().endsWith(".test.ts")) {
        sourceFile.delete();
      }
    }

    diags.push(...project.getPreEmitDiagnostics().map(d => d.compilerObject));
    if (hasError(diags)) { return undefined; }

    treeShake(project);

    const methods = project.getSourceFiles()
      .flatMap(getProcMethods)
      .map(m => [m, getStoredProcConfig(m)] as const);

    diags.push(...checkStoredProcNames(methods.map(([m]) => m)));
    diags.push(...checkStoredProcConfig(methods, false));

    if (hasError(diags)) { return undefined; }

    deAsync(project);
    removeDecorators(project);

    return { project, methods }
  } finally {
    printDiagnostics(diags);
  }
}

interface DiagnosticOptions {
  code?: number,
  node?: tsm.Node,
  endNode?: tsm.Node,
  category?: tsm.ts.DiagnosticCategory
};

function createDiagnostic(messageText: string, options?: DiagnosticOptions): tsm.ts.Diagnostic {
  const node = options?.node;
  const endNode = options?.endNode;
  const category = options?.category ?? tsm.ts.DiagnosticCategory.Error;
  const code = options?.code ?? 0;
  const length = node 
    ? endNode 
      ? endNode.getEnd() - node.getPos()
      : node.getEnd() - node.getPos()
    : undefined;

  return {
    category,
    code,
    file: node?.getSourceFile().compilerNode,
    length,
    messageText,
    start: node?.getPos(),
  };
}

function printDiagnostics(diags: readonly tsm.ts.Diagnostic[]) {
  const formatHost: tsm.ts.FormatDiagnosticsHost = {
    getCurrentDirectory: () => tsm.ts.sys.getCurrentDirectory(),
    getNewLine: () => tsm.ts.sys.newLine,
    getCanonicalFileName: (fileName: string) => tsm.ts.sys.useCaseSensitiveFileNames
      ? fileName : fileName.toLowerCase()
  }

  const msg = tsm.ts.formatDiagnosticsWithColorAndContext(diags, formatHost);
  console.log(msg);
}

export function checkStoredProcNames(methods: readonly tsm.MethodDeclaration[]): readonly tsm.ts.Diagnostic[] {
  const diags = new Array<tsm.ts.Diagnostic>();
  for (const method of methods) {
    const $class = method.getParentIfKind(tsm.SyntaxKind.ClassDeclaration);
    if (!$class) {
      diags.push(createDiagnostic("Stored procedure method must be a static method of a class", { node: method, category: tsm.ts.DiagnosticCategory.Error }));
    }

    const className = $class?.getName() ?? "";
    const methodName = method.getName();
    if (className.length + methodName.length > 48) {
      diags.push(createDiagnostic("Stored procedure class and method names combined must not be longer that 48 characters", { node: method, category: tsm.ts.DiagnosticCategory.Error }));
    }
  }
  return diags;
}

export function checkStoredProcConfig(methods: readonly (readonly [tsm.MethodDeclaration, StoredProcedureConfig])[], error: boolean = false): readonly tsm.ts.Diagnostic[] {
  const category = error ? tsm.ts.DiagnosticCategory.Error : tsm.ts.DiagnosticCategory.Warning;
  const diags = new Array<tsm.ts.Diagnostic>();
  for (const [method, config] of methods) {
    if (config.executeLocally) {
      const decorator = getStoredProcDecorator(method);
      const node = decorator ?? method;
      const endNode = decorator ? method.getFirstChildByKind(tsm.SyntaxKind.CloseParenToken) : undefined;
      diags.push(createDiagnostic(`executeLocally enabled for ${method.getName()}`, { node, category, endNode }));
    }
  }
  return diags;

  function getStoredProcDecorator(method: tsm.MethodDeclaration) {
    for (const decorator of method.getDecorators()) {
      const kind = getDbosDecoratorKind(decorator);
      if (kind === "storedProcedure") { return decorator; }
    }
  }
}

export function removeDbosMethods(file: tsm.SourceFile) {
  file.forEachDescendant((node, traversal) => {
    if (tsm.Node.isClassDeclaration(node)) {
      traversal.skip();
      for (const method of node.getStaticMethods()) {
        const kind = getDbosMethodKind(method);
        switch (kind) {
          case 'workflow':
          case 'communicator':
          case 'initializer':
          case 'transaction':
          case 'handler': {
            method.remove();
            break;
          }
          case 'storedProcedure':
          case undefined:
            break;
          default: {
            const _: never = kind;
            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
            throw new Error(`Unexpected DBOS method kind: ${kind}`);
          }
        }
      }
    }
  });
}

export function getProcMethods(file: tsm.SourceFile) {
  const methods = new Array<tsm.MethodDeclaration>();
  file.forEachDescendant((node, traversal) => {
    if (tsm.Node.isClassDeclaration(node)) {
      traversal.skip();
      for (const method of node.getStaticMethods()) {
        const kind = getDbosMethodKind(method);
        if (kind === 'storedProcedure') {
          methods.push(method);
        }
      }
    }
  });
  return methods;
}

function getProcMethodDeclarations(file: tsm.SourceFile) {
  // initialize set of declarations with all tx methods and their class declaration parents
  const declSet = new Set<tsm.Node>();
  for (const method of getProcMethods(file)) {
    declSet.add(method);
    const parent = method.getParentIfKind(tsm.SyntaxKind.ClassDeclaration);
    if (parent) { declSet.add(parent); }
  }

  while (true) {
    const size = declSet.size;
    for (const decl of Array.from(declSet)) {
      switch (true) {
        case tsm.Node.isFunctionDeclaration(decl):
        case tsm.Node.isMethodDeclaration(decl): {
          decl.getBody()?.forEachDescendant(node => {
            if (tsm.Node.isIdentifier(node)) {
              const _name = node.getSymbol()?.getName();
              const nodeDecls = node.getSymbol()?.getDeclarations() ?? [];
              nodeDecls.forEach(decl => declSet.add(decl));
            }
          })
        }
      }
    }
    if (declSet.size === size) { break; }
  }

  return declSet;
}

function shakeFile(file: tsm.SourceFile) {

  removeDbosMethods(file);

  const txDecls = getProcMethodDeclarations(file);

  file.forEachDescendant((node, traverse) => {
    if (tsm.Node.isExportable(node)) {
      if (node.isExported()) { return; }
    }
    if (tsm.Node.isMethodDeclaration(node)) {
      traverse.skip();
    }

    switch (true) {
      case tsm.Node.isClassDeclaration(node):
      case tsm.Node.isEnumDeclaration(node):
      case tsm.Node.isFunctionDeclaration(node):
      case tsm.Node.isInterfaceDeclaration(node):
      case tsm.Node.isMethodDeclaration(node):
      case tsm.Node.isPropertyDeclaration(node):
      case tsm.Node.isTypeAliasDeclaration(node):
      case tsm.Node.isVariableDeclaration(node):
        if (!txDecls.has(node)) {
          node.remove();
        }
        break;
    }
  })
}

export function removeDecorators(file: tsm.SourceFile | tsm.Project) {
  if (tsm.Node.isNode(file)) {
    file.forEachDescendant(node => {
      if (tsm.Node.isDecorator(node)) {
        node.remove();
      }
    });
  } else {
    for (const $file of file.getSourceFiles()) {
      removeDecorators($file);
    }
  }
}

export function removeUnusedFiles(project: tsm.Project) {
  // get the files w/ one or more @Transaction functions
  const procFiles = new Set<tsm.SourceFile>();
  for (const file of project.getSourceFiles()) {
    const procMethods = getProcMethods(file);
    if (procMethods.length > 0) {
      procFiles.add(file);
    }
  }

  // get all the files that are imported by the txFiles
  const procImports = new Set<tsm.SourceFile>();
  for (const file of procFiles) {
    procImports.add(file);
    file.forEachDescendant(node => {
      if (tsm.Node.isImportDeclaration(node)) {
        const moduleFile = node.getModuleSpecifierSourceFile();
        if (moduleFile) { procImports.add(moduleFile); }
      }
    })
  }

  // remove all files that don't have @StoredProcedure methods and are not
  // imported by files with @StoredProcedure methods
  for (const file of project.getSourceFiles()) {
    if (!procImports.has(file)) {
      project.removeSourceFile(file);
    }
  }
}

function treeShake(project: tsm.Project) {

  removeUnusedFiles(project);

  // delete all workflow/communicator/init/handler methods
  for (const file of project.getSourceFiles()) {
    shakeFile(file);
  }
}

function deAsync(project: tsm.Project) {
  // pass: remove async from transaction method declaration and remove await keywords
  for (const sourceFile of project.getSourceFiles()) {
    sourceFile.forEachChild(node => {
      if (tsm.Node.isClassDeclaration(node)) {
        for (const method of node.getStaticMethods()) {
          if (getDbosMethodKind(method) === 'storedProcedure') {
            method.setIsAsync(false);
            method.getBody()?.transform(traversal => {
              const node = traversal.visitChildren();
              return tsm.ts.isAwaitExpression(node) ? node.expression : node;
            })
          }
        }
      }
    });
  }
}

// can be removed once TS 5.5 is released
// https://devblogs.microsoft.com/typescript/announcing-typescript-5-5-beta/#inferred-type-predicates
function isValid<T>(value: T | null | undefined): value is T { return !!value; }

export interface DecoratorInfo {
  name: string;
  alias?: string;
  module?: string;
  args: tsm.Node[] | undefined;
}

// helper function to get the actual name (along with any alias) and module of a decorator
// from its import declaration
export function getDecoratorInfo(node: tsm.Decorator): DecoratorInfo {
  const isFactory = node.isDecoratorFactory();

  const identifier = isFactory
    ? node.getCallExpression()?.getExpressionIfKind(tsm.SyntaxKind.Identifier)
    : node.getExpressionIfKind(tsm.SyntaxKind.Identifier);

  const args = isFactory
    ? node.getCallExpression()?.getArguments()
    : undefined;

  const symbol = identifier?.getSymbol();
  if (symbol) {
    const importSpecifiers = symbol.getDeclarations()
      .map(n => n.asKind(tsm.ts.SyntaxKind.ImportSpecifier))
      .filter(isValid);

    if (importSpecifiers.length === 1) {
      const { name, alias } = importSpecifiers[0].getStructure();
      const modSpec = importSpecifiers[0].getImportDeclaration().getModuleSpecifier();
      return { name, alias, module: modSpec.getLiteralText(), args };
    }

    if (importSpecifiers.length > 1) { throw new Error("Too many import specifiers"); }
  }

  return { name: node.getName(), args };
}

export type DecoratorArgument = boolean | string | number | DecoratorArgument[] | Record<string, unknown>;

export function parseDecoratorArgument(node: tsm.Node): DecoratorArgument {
  switch (true) {
    case tsm.Node.isTrueLiteral(node): return true;
    case tsm.Node.isFalseLiteral(node): return false;
    case tsm.Node.isStringLiteral(node): return node.getLiteralValue();
    case tsm.Node.isNumericLiteral(node): return node.getLiteralValue();
    case tsm.Node.isArrayLiteralExpression(node): return node.getElements().map(parseDecoratorArgument);
    case tsm.Node.isObjectLiteralExpression(node): {
      const obj: Record<string, unknown> = {};
      const props = node.getProperties().map(parseProperty);
      for (const { name, value } of props) {
        obj[name] = value;
      }
      return obj;
    }
    default:
      throw new Error(`Unexpected argument type: ${node.getKindName()}`);
  }

  function parseProperty(node: tsm.ObjectLiteralElementLike) {
    switch (true) {
      case tsm.Node.isPropertyAssignment(node): {
        const name = node.getName();
        const init = node.getInitializer();
        const value = init ? parseDecoratorArgument(init) : undefined;
        return { name, value };
      }
      default:
        throw new Error(`Unexpected property type: ${node.getKindName()}`);
    }
  }
}

type DbosDecoratorKind = "handler" | "storedProcedure" | "transaction" | "workflow" | "communicator" | "initializer";

function getDbosDecoratorKind(node: tsm.Decorator | DecoratorInfo): DbosDecoratorKind | undefined {
  const decoratorInfo = tsm.Node.isNode(node) ? getDecoratorInfo(node) : node;
  if (!decoratorInfo) { return undefined; }
  const { name, module } = decoratorInfo;
  if (module !== "@dbos-inc/dbos-sdk") { return undefined; }
  switch (name) {
    case "GetApi":
    case "PostApi":
      return "handler";
    case "StoredProcedure": return "storedProcedure";
    case "Transaction": return "transaction";
    case "Workflow": return "workflow";
    case "Communicator": return "communicator";
    case "DBOSInitializer":
    case "DBOSDeploy":
      return "initializer";
  }
}

// helper function to determine the kind of DBOS method
export function getDbosMethodKind(node: tsm.MethodDeclaration): DbosDecoratorKind | undefined {
  // Note, other DBOS method decorators (Scheduled, KafkaConsume, RequiredRole) modify runtime behavior
  //       of DBOS methods, but are not their own unique kind.
  //       Get/PostApi decorators are atypical in that they can be used on @Communicator/@Transaction/@Workflow
  //       methods as well as on their own.
  let isHandler = false;
  for (const decorator of node.getDecorators()) {
    const kind = getDbosDecoratorKind(decorator);
    switch (kind) {
      case "storedProcedure":
      case "transaction":
      case "workflow":
      case "communicator":
      case "initializer":
        return kind;
      case "handler":
        isHandler = true;
        break;
      case undefined:
        break;
      default: {
        const _never: never = kind;
        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        throw new Error(`Unexpected DBOS method kind: ${kind}`);
      }
    }
  }
  return isHandler ? "handler" : undefined;
}

export function getStoredProcConfig(node: tsm.MethodDeclaration): StoredProcedureConfig {
  const decorators = node.getDecorators().map(getDecoratorInfo);
  const procDecorator = decorators.find(d => getDbosDecoratorKind(d) === "storedProcedure");
  if (!procDecorator) { throw new Error("Missing StoredProcedure decorator"); }

  const arg0 = procDecorator.args?.[0];
  const configArg = arg0 ? parseDecoratorArgument(arg0) as Partial<StoredProcedureConfig> : undefined;
  const readOnly = configArg?.readOnly;
  const executeLocally = configArg?.executeLocally;
  const isolationLevel = configArg?.isolationLevel;
  return { isolationLevel, readOnly, executeLocally };
}
