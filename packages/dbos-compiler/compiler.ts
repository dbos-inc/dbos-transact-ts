import tsm from 'ts-morph';

export type CompileMethodInfo = readonly [tsm.MethodDeclaration, StoredProcedureConfig];
export type CompileResult = {
  project: tsm.Project;
  methods: readonly CompileMethodInfo[];
  diagnostics: readonly tsm.ts.Diagnostic[];
};

export type IsolationLevel = 'READ UNCOMMITTED' | 'READ COMMITTED' | 'REPEATABLE READ' | 'SERIALIZABLE';
export type DbosDecoratorVersion = 1 | 2;
export interface StoredProcedureConfig {
  isolationLevel?: IsolationLevel;
  readOnly?: boolean;
  executeLocally?: boolean;
  version: DbosDecoratorVersion;
}

export type DecoratorArgument = boolean | string | number | DecoratorArgument[] | Record<string, unknown>;
type DbosDecoratorKind = 'handler' | 'storedProcedure' | 'transaction' | 'workflow' | 'step' | 'initializer';

interface DbosDecoratorInfo {
  kind: DbosDecoratorKind;
  version: DbosDecoratorVersion;
}

interface DiagnosticOptions {
  code?: number;
  node?: tsm.Node;
  endNode?: tsm.Node;
  category?: tsm.ts.DiagnosticCategory;
}

export function compile(configFileOrProject: string | tsm.Project): CompileResult {
  const diagnostics = new Array<tsm.ts.Diagnostic>();
  const project =
    typeof configFileOrProject === 'string'
      ? new tsm.Project({
          tsConfigFilePath: configFileOrProject,
          compilerOptions: {
            sourceMap: false,
            declaration: false,
            declarationMap: false,
            removeComments: true,
          },
        })
      : configFileOrProject;

  const methods = project.getSourceFiles().flatMap(getStoredProcMethods).map(mapStoredProcConfig);
  if (methods.length === 0) {
    diagnostics.push(createDiagnostic('No stored procedure methods found'));
  } else {
    methods.forEach((m) => diagnostics.push(...checkStoredProc(m)));
  }

  // bail early if there are errors at this point
  if (hasError(diagnostics)) {
    return { project, methods, diagnostics };
  }

  // A stored proc method may not call other DBOS methods (workflows, transactions, handlers, steps, etc)
  // Explicitly remove them before continuing
  project.getSourceFiles().forEach(removeNonProcDbosMethods);

  // find all the declarations used in stored proc methods
  const usedDecls = collectUsedDeclarations(project, methods);

  // remove all declarations that aren't used by stored proc methods
  project.getSourceFiles().forEach((file) => removeUnusedDeclarations(file, usedDecls));

  // Add any diagnostics from the TypeScript compiler after tree shaking
  // before we remove decorators and async/await from the code
  // (which will make the resulting TS code appear invalid, but is exactly what we want for DBOS stored procedures)
  diagnostics.push(...project.getPreEmitDiagnostics().map((d) => d.compilerObject));
  if (hasError(diagnostics)) {
    return { project, methods, diagnostics };
  }

  removeDecorators(project);
  deAsync(project);

  return { project, methods, diagnostics };
}

export function getStoredProcMethods(file: tsm.SourceFile): [tsm.MethodDeclaration, DbosDecoratorVersion][] {
  const methods = new Array<[tsm.MethodDeclaration, DbosDecoratorVersion]>();
  file.forEachDescendant((node, traversal) => {
    if (tsm.Node.isClassDeclaration(node)) {
      traversal.skip();
      for (const method of node.getStaticMethods()) {
        const info = parseDbosMethodInfo(method);
        if (info?.kind === 'storedProcedure') {
          methods.push([method, info.version] as const);
        }
      }
    }
  });
  return methods;
}

export function mapStoredProcConfig([method, version]: [tsm.MethodDeclaration, DbosDecoratorVersion]): [
  tsm.MethodDeclaration,
  StoredProcedureConfig,
] {
  const decorators = method.getDecorators();
  const procDecorator = decorators.find((d) => {
    const info = parseDbosDecoratorInfo(d);
    return info?.kind === 'storedProcedure';
  });

  if (!procDecorator) {
    // only methods with stored proc decorator should be passed to this function
    throw new Error(`Missing StoredProcedure decorator on method ${method.getName()}`);
  }

  const arg0 = procDecorator.getCallExpression()?.getArguments()[0] ?? undefined;
  const configArg = arg0 ? (parseDecoratorArgument(arg0) as Partial<StoredProcedureConfig>) : undefined;
  const readOnly = configArg?.readOnly;
  const executeLocally = configArg?.executeLocally;
  const isolationLevel = configArg?.isolationLevel;

  return [method, { isolationLevel, readOnly, executeLocally, version }] as const;
}

export function checkStoredProc([method, config]: CompileMethodInfo): tsm.ts.Diagnostic[] {
  const diags = new Array<tsm.ts.Diagnostic>();
  const $class = method.getParentIfKind(tsm.SyntaxKind.ClassDeclaration);
  const className = $class?.getName();

  if (!$class || !className) {
    diags.push(
      createDiagnostic(`Can't find class parent of ${method.getName()}`, {
        node: method,
      }),
    );
  } else {
    const fullName = className ? `${className}.${method.getName()}` : method.getName();
    if (fullName.length > 48) {
      diags.push(
        createDiagnostic(`Stored procedure ${fullName} name must not bes longer that 48 characters`, {
          node: method,
        }),
      );
    }
  }

  if (config.executeLocally) {
    const decorator = getStoredProcDecorator(method);
    const node = decorator ?? method;
    const endNode = decorator ? method.getFirstChildByKind(tsm.SyntaxKind.CloseParenToken) : undefined;
    diags.push(
      createDiagnostic(`executeLocally enabled for ${method.getName()}`, {
        node,
        endNode,
        category: tsm.ts.DiagnosticCategory.Warning,
      }),
    );
  }
  return diags;

  function getStoredProcDecorator(method: tsm.MethodDeclaration) {
    for (const decorator of method.getDecorators()) {
      const info = parseDbosDecoratorInfo(decorator);
      if (info?.kind === 'storedProcedure') {
        return decorator;
      }
    }
  }
}

export function collectUsedDeclarations(
  project: tsm.Project,
  procMethods: readonly CompileMethodInfo[],
): Set<tsm.Node> {
  const langSvc = project.getLanguageService();

  // find all the symbols referenced by the stored procedure methods and their dependencies
  const usedDecls = new Set<tsm.Node>();
  for (const [method, _] of procMethods) {
    // checkStoredProc ensures all stored proc methods have valid ClassDeclaration parents
    const $class = method.getParentIfKindOrThrow(tsm.SyntaxKind.ClassDeclaration);

    // add stored proc method and parent class declarations to tracking set
    usedDecls.add(method);
    usedDecls.add($class);

    // add all identifier declarations used in the method body to the tracking set
    processBody(method.getBody(), usedDecls, langSvc);
  }
  return usedDecls;

  // helper function to process the body, and the bodies of any bodied declarations found
  function processBody(body: tsm.Node | undefined, set: Set<tsm.Node>, langSvc: tsm.LanguageService) {
    body?.forEachDescendant((node) => {
      if (tsm.Node.isIdentifier(node)) {
        for (const defInfo of langSvc.getDefinitions(node)) {
          const decl = defInfo.getDeclarationNode();
          if (decl) {
            set.add(decl);
            // if the declaration has a body, process it as well
            if (tsm.Node.isBodied(decl) || tsm.Node.isBodyable(decl)) {
              processBody(decl.getBody(), set, langSvc);
            }
          }
        }
      }
    });
  }
}

export function removeUnusedDeclarations(file: tsm.SourceFile, usedDecls: Set<tsm.Node>) {
  file.forEachChild((node) => {
    if (node.getKind() === tsm.ts.SyntaxKind.EndOfFileToken) {
      // obviously, skip the EOF token
      return;
    }

    if (tsm.Node.isInterfaceDeclaration(node) || tsm.Node.isTypeAliasDeclaration(node)) {
      // TS Compilation will remove interfaces and type aliases
      return;
    }

    if (tsm.Node.isImportDeclaration(node)) {
      // TS Compilation will remove unused imports
      return;
    }

    if (tsm.Node.isClassDeclaration(node)) {
      // remove entire class if it isn't in the tracking set of declarations
      if (!usedDecls.has(node)) {
        node.remove();
        return;
      }

      // remove any members of the class that are not in the tracking set of declarations
      for (const member of node.getMembers()) {
        if (!usedDecls.has(member)) {
          member.remove();
        }
      }
      return;
    }

    // remove all variable declarations that are not in the tracking set of declarations
    // TS compiler API will remove the variable statement if there are no remaining declarations
    if (tsm.Node.isVariableStatement(node)) {
      for (const decl of node.getDeclarations()) {
        if (!usedDecls.has(decl)) {
          decl.remove();
        }
      }
      return;
    }

    // remove all enum and function declarations that are not in the tracking set of declarations
    if (tsm.Node.isFunctionDeclaration(node) || tsm.Node.isEnumDeclaration(node)) {
      if (!usedDecls.has(node)) {
        node.remove();
      }
      return;
    }

    // remove any other kind of node that has a remove function
    // these are typically module level statements that are  not valid for stored procedures
    if ('remove' in node && typeof node.remove === 'function') {
      node.remove();
    }
  });
}

export function removeNonProcDbosMethods(file: tsm.SourceFile) {
  file.forEachDescendant((node, traversal) => {
    if (tsm.Node.isClassDeclaration(node)) {
      traversal.skip();
      for (const method of node.getStaticMethods()) {
        const info = parseDbosMethodInfo(method);
        if (!info) {
          continue;
        }

        switch (info.kind) {
          case 'workflow':
          case 'step':
          case 'initializer':
          case 'transaction':
          case 'handler': {
            method.remove();
            break;
          }
          case 'storedProcedure':
            break;
          default: {
            const _never: never = info.kind;
            // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
            throw new Error(`Unexpected DBOS method kind: ${info.kind}`);
          }
        }
      }
    }
  });
}

function deAsync(project: tsm.Project) {
  // pass: remove async from transaction method declaration and remove await keywords
  for (const sourceFile of project.getSourceFiles()) {
    sourceFile.forEachChild((node) => {
      if (tsm.Node.isClassDeclaration(node)) {
        for (const method of node.getStaticMethods()) {
          method.setIsAsync(false);
          removeAwaits(method.getBody());
        }
      }
      if (tsm.Node.isFunctionDeclaration(node)) {
        node.setIsAsync(false);
        removeAwaits(node.getBody());
      }
    });
  }

  function removeAwaits(body: tsm.Node | undefined) {
    body?.transform((traversal) => {
      const node = traversal.visitChildren();
      return tsm.ts.isAwaitExpression(node) ? node.expression : node;
    });
  }
}

export function removeDecorators(file: tsm.SourceFile | tsm.Project) {
  if (tsm.Node.isNode(file)) {
    file.forEachDescendant((node) => {
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

export function hasError(diags: readonly tsm.ts.Diagnostic[]) {
  return diags.some((diag) => diag.category === tsm.ts.DiagnosticCategory.Error);
}

function createDiagnostic(messageText: string, options?: DiagnosticOptions): tsm.ts.Diagnostic {
  const node = options?.node;
  const endNode = options?.endNode;
  const category = options?.category ?? tsm.ts.DiagnosticCategory.Error;
  const code = options?.code ?? 0;
  const length = node ? (endNode ? endNode.getEnd() - node.getPos() : node.getEnd() - node.getPos()) : undefined;

  return {
    category,
    code,
    file: node?.getSourceFile().compilerNode,
    length,
    messageText,
    start: node?.getPos(),
  };
}

export function parseImportSpecifier(node: tsm.Identifier | undefined): tsm.ImportSpecifier | undefined {
  const symbol = node?.getSymbol();
  if (symbol) {
    const importSpecifiers = symbol
      .getDeclarations()
      .map((n) => n.asKind(tsm.ts.SyntaxKind.ImportSpecifier))
      .filter(isValid);

    if (importSpecifiers.length === 1) {
      return importSpecifiers[0];
    }
    if (importSpecifiers.length > 1) {
      throw new Error('Too many import specifiers');
    }
  }

  return undefined;

  // can be removed once we move to TS 5.5
  // https://devblogs.microsoft.com/typescript/announcing-typescript-5-5-beta/#inferred-type-predicates
  function isValid<T>(value: T | null | undefined): value is T {
    return !!value;
  }
}

function parseDbosDecoratorInfo(node: tsm.Decorator): DbosDecoratorInfo | undefined {
  if (!node.isDecoratorFactory()) {
    return undefined;
  }

  const expr = node.getCallExpressionOrThrow().getExpression();

  // v1 decorators single identifiers i.e. such as @Workflow()
  if (tsm.Node.isIdentifier(expr)) {
    const impSpec = parseImportSpecifier(expr);
    if (impSpec && isDbosImport(impSpec)) {
      const kind = parseImportSpecifierStructureKind(impSpec.getStructure());
      if (kind) {
        return { kind, version: 1 };
      }
    }
  }

  // v2 decorators property access expressions i.e. such as @DBOS.workflow()
  if (tsm.Node.isPropertyAccessExpression(expr)) {
    const impSpec = parseImportSpecifier(expr.getExpressionIfKind(tsm.SyntaxKind.Identifier));
    if (impSpec && isDbosImport(impSpec)) {
      const { name } = impSpec.getStructure();
      if (name === 'DBOS') {
        const kind = parsePropertyAccessExpressionKind(expr);
        if (kind) {
          return { kind, version: 2 };
        }
      }
    }
  }

  return undefined;

  function isDbosImport(node: tsm.ImportSpecifier): boolean {
    const modSpec = node.getImportDeclaration().getModuleSpecifier();
    return modSpec.getLiteralText() === '@dbos-inc/dbos-sdk';
  }

  function parseImportSpecifierStructureKind({ name }: tsm.ImportSpecifierStructure): DbosDecoratorKind | undefined {
    switch (name) {
      case 'GetApi':
      case 'PostApi':
      case 'PutApi':
      case 'PatchApi':
      case 'DeleteApi':
        return 'handler';
      case 'StoredProcedure':
        return 'storedProcedure';
      case 'Transaction':
        return 'transaction';
      case 'Workflow':
        return 'workflow';
      case 'Communicator':
      case 'Step':
        return 'step';
      case 'DBOSInitializer':
      case 'DBOSDeploy':
        return 'initializer';
      default:
        return undefined;
    }
  }

  function parsePropertyAccessExpressionKind(node: tsm.PropertyAccessExpression): DbosDecoratorKind | undefined {
    switch (node.getName()) {
      case 'getApi':
      case 'postApi':
      case 'putApi':
      case 'patchApi':
      case 'deleteApi':
        return 'handler';
      case 'workflow':
        return 'workflow';
      case 'transaction':
        return 'transaction';
      case 'step':
        return 'step';
      case 'storedProcedure':
        return 'storedProcedure';
      default:
        return undefined;
    }
  }
}

// helper function to determine the kind of DBOS method
export function parseDbosMethodInfo(node: tsm.MethodDeclaration): DbosDecoratorInfo | undefined {
  // Note, other DBOS method decorators (Scheduled, KafkaConsume, RequiredRole) modify runtime behavior
  //       of DBOS methods, but are not their own unique kind.
  //       Get/PostApi decorators are atypical in that they can be used on @Step/@Transaction/@Workflow
  //       methods as well as on their own.
  let handlerVersion: DbosDecoratorVersion | undefined = undefined;
  for (const decorator of node.getDecorators()) {
    const info = parseDbosDecoratorInfo(decorator);
    if (!info) {
      continue;
    }
    switch (info.kind) {
      case 'storedProcedure':
      case 'transaction':
      case 'workflow':
      case 'step':
      case 'initializer':
        return info;
      case 'handler':
        if (handlerVersion !== undefined) {
          throw new Error('Multiple handler decorators');
        }
        handlerVersion = info.version;
        break;
      default: {
        const _never: never = info.kind;
        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        throw new Error(`Unexpected DBOS method kind: ${info.kind}`);
      }
    }
  }
  return handlerVersion ? { kind: 'handler', version: handlerVersion } : undefined;
}

export function parseDecoratorArgument(node: tsm.Node): DecoratorArgument {
  switch (true) {
    case tsm.Node.isTrueLiteral(node):
      return true;
    case tsm.Node.isFalseLiteral(node):
      return false;
    case tsm.Node.isStringLiteral(node):
      return node.getLiteralValue();
    case tsm.Node.isNumericLiteral(node):
      return node.getLiteralValue();
    case tsm.Node.isArrayLiteralExpression(node):
      return node.getElements().map(parseDecoratorArgument);
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
