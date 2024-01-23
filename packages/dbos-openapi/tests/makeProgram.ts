import ts from "typescript";
import path from "node:path";
import fs from "node:fs";

const sdkRepoRoot = path.join(__dirname, "../../..");
const dbosSDKModule = "node_modules/@dbos-inc/dbos-sdk/";

function readFile(fileName: string) {
  if (fileName.startsWith(dbosSDKModule)) {
    const $path = path.join(sdkRepoRoot, fileName.slice(dbosSDKModule.length));
    return fs.existsSync($path) ? fs.readFileSync($path, "utf-8") : undefined;
  }
  if (fileName.startsWith("node_modules/@dbos-inc/")) {
    return undefined;
  }
  if (fileName.startsWith("node_modules/")) {
    const $path = path.join(sdkRepoRoot, fileName);
    return fs.existsSync($path) ? fs.readFileSync($path, "utf-8") : undefined;
  }
  return undefined;
}

function readDirectory(directoryName: string) {
  if (directoryName.startsWith(dbosSDKModule)) {
    const $path = path.join(sdkRepoRoot, directoryName.slice(dbosSDKModule.length));
    return fs.readdirSync($path, { withFileTypes: true }).filter(f => f.isDirectory()).map(f => f.name);
  }
  if (directoryName.startsWith("node_modules/")) {
    const $path = path.join(sdkRepoRoot, directoryName);
    return fs.readdirSync($path, { withFileTypes: true }).filter(f => f.isDirectory()).map(f => f.name);
  }

  return undefined;
}

/*
 * Helper function to generate a TS program object from a string for testing TS Compiler related features
 */
export function makeTestTypescriptProgram(source: string): ts.Program {
  const inputFileName = "operation.ts";
  const sourceFile = ts.createSourceFile(inputFileName, source, ts.ScriptTarget.ESNext);
  const compilerHost: ts.CompilerHost = {
    getSourceFile: (fileName) => {
      return fileName === inputFileName ? sourceFile : undefined;
    },
    writeFile: () => { },
    getDefaultLibFileName: () => "lib.d.ts",
    useCaseSensitiveFileNames: () => ts.sys.useCaseSensitiveFileNames,
    getCanonicalFileName: fileName => fileName,
    getCurrentDirectory: () => "",
    getNewLine: () => ts.sys.newLine,
    fileExists: (fileName) => (fileName === inputFileName)
      ? true
      : readFile(fileName) !== undefined,
    readFile,
    directoryExists: (directoryName) => {
      if (directoryName === "node_modules") return true;
      if (directoryName === "node_modules/@dbos-inc") return true;
      if (directoryName === "node_modules/@dbos-inc/dbos-sdk") return true;
      return readDirectory(directoryName) !== undefined;
    },
    getDirectories: (path) => readDirectory(path) ?? []
  };
  return ts.createProgram([inputFileName], {}, compilerHost);
}
