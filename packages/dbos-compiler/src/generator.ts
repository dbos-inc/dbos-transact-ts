import fsp from 'node:fs/promises';
import path from 'node:path';
import tsm from 'ts-morph';
import { Liquid } from "liquidjs";
import type { TransactionConfig } from './utility.js';

const engine = new Liquid({
  root: path.resolve(__dirname, '..', 'templates'),
  extname: ".liquid"
});

async function render(file: string, ctx?: object): Promise<string> {
  return await engine.renderFile(file, ctx) as string;
}

function mapType(type: tsm.Type) {
  if (type.isString()) { return "TEXT"; }
  if (type.isNumber()) { return "INT"; }
  if (type.isBoolean()) { return "BOOLEAN"; }

  throw new Error(`Unsupported type: ${type.getText()}`);
}

export async function emitDbos({ createPath, dropPath }: { createPath: string, dropPath: string }) {
  await fsp.appendFile(createPath, await render("dbos.create.liquid"));
  await fsp.appendFile(dropPath, await render("dbos.drop.liquid"));
}

export async function emitMethod(method: tsm.MethodDeclaration, config: TransactionConfig, { createPath, dropPath }: { createPath: string, dropPath: string }, appVersion: string | undefined) {
  const methodName = method.getName();
  const className = method.getParentIfKindOrThrow(tsm.SyntaxKind.ClassDeclaration).getName();
  const moduleName = method.getSourceFile().getBaseNameWithoutExtension();
  const parameters = method.getParameters().slice(1).map(p => ({ name: p.getName(), type: mapType(p.getType()) }));

  const context = { ...config, methodName, className, moduleName, parameters, appVersion };
  await fsp.appendFile(createPath, await render("method.create.liquid", context));
  await fsp.appendFile(dropPath, await render("method.drop.liquid", context));
}

export async function emitModule(sourceFile: tsm.SourceFile, { createPath, dropPath }: { createPath: string, dropPath: string }, appVersion: string | undefined) {
  const results = sourceFile.getEmitOutput();
  const contents = results.getEmitSkipped() ? "" : results.getOutputFiles().map(f => f.getText()).join("\n");
  const moduleName = sourceFile.getBaseNameWithoutExtension();

  const context = { moduleName, contents, appVersion };
  await fsp.appendFile(createPath, await render("module.create.liquid", context));
  await fsp.appendFile(dropPath, await render("module.drop.liquid", context));
}
