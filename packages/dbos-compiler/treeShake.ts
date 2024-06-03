import tsm from 'ts-morph';
import { getDbosMethodKind } from "./utility";

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

export function getProcMethodDeclarations(file: tsm.SourceFile) {
  // initialize set of declarations with all tx methods and their class declaration parents
  const declSet = new Set<tsm.Node>();
  for (const method of getProcMethods(file)) {
    declSet.add(method);
    const parent = method.getParentIfKind(tsm.SyntaxKind.ClassDeclaration);
    if (parent) { declSet.add(parent); }
  }

  // eslint-disable-next-line no-constant-condition
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

export function treeShake(project: tsm.Project) {

  removeUnusedFiles(project);

  // delete all workflow/communicator/init/handler methods
  for (const file of project.getSourceFiles()) {
    shakeFile(file);
  }
}

export function deAsync(project: tsm.Project) {
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
