import tsm from 'ts-morph';

// can be removed once TS 5.5 is released
// https://devblogs.microsoft.com/typescript/announcing-typescript-5-5-beta/#inferred-type-predicates
function isValid<T>(value: T | null | undefined): value is T { return !!value; }

interface DecoratorInfo {
  name: string;
  alias?: string;
  module?: string;
  args: tsm.Node[] | undefined;
}

// helper function to get the actual name (along with any alias) and module of a decorator
// from its import declaration
function getDecoratorInfo(node: tsm.Decorator): DecoratorInfo {
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

type DecoratorArgument = boolean | string | number | DecoratorArgument[] | Record<string, unknown>;

function parseDecoratorArgument(node: tsm.Node): DecoratorArgument {
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
        throw new Error(`Unexpected DBOS method kind: ${kind}`);
      }
    }
  }
  return isHandler ? "handler" : undefined;
}

export type IsolationLevel = "READ UNCOMMITTED" | "READ COMMITTED" | "REPEATABLE READ" | "SERIALIZABLE";
export interface TransactionConfig {
  isolationLevel: IsolationLevel;
  readOnly: boolean;
}

export function getStoredProcConfig(node: tsm.MethodDeclaration): TransactionConfig | undefined {
  const decorators = node.getDecorators().map(getDecoratorInfo);
  const procDecorator = decorators.find(d => getDbosDecoratorKind(d) === "storedProcedure");
  if (!procDecorator) { return undefined; }

  const arg0 = procDecorator.args?.[0];
  const configArg = arg0 ? parseDecoratorArgument(arg0) as Partial<TransactionConfig> : undefined;
  const readOnly = configArg?.readOnly ?? false;
  const isolationLevel = configArg?.isolationLevel ?? "SERIALIZABLE";
  return { isolationLevel, readOnly };
}
