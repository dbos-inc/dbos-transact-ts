import fs from "fs";
import path from "path";

/*
 * Use the node.js `fs` module to read the content of a file
 * Handles cases where:
 * - the file does not exist
 * - the file is not a valid file
 **/
export function readFileSync(path: string, encoding: BufferEncoding = "utf8"): string | Buffer {
  // First, check the file
  fs.stat(path, (error: NodeJS.ErrnoException | null, stats: fs.Stats) => {
    if (error) {
      throw new Error(`checking on ${path}. ${error.code}: ${error.errno}`);
    } else if (!stats.isFile()) {
      throw new Error(`config file ${path} is not a file`);
    }
  });

  // Then, read its content
  const fileContent: string = fs.readFileSync(path, { encoding } );
  return fileContent;
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export type ValuesOf<T> = T[keyof T];


export function createDirectory(path: string): string | undefined {
  return fs.mkdirSync(path, { recursive: true });
}

// Adapated and translated from from: https://github.com/junosuarez/find-root
export function findPackageRoot(start: string | string[]): string {
    if (typeof start === 'string') {
        if (!start.endsWith(path.sep)) {
            start += path.sep;
        }
        start = start.split(path.sep);
    }

    if (start.length === 0) {
        throw new Error('package.json not found in path');
    }

    start.pop();
    const dir = start.join(path.sep);

    try {
        if (fs.existsSync(path.join(dir, 'package.json'))) {
            return dir;
        }
    } catch (e) {
        // Error handling can be implemented here if necessary
    }

    return findPackageRoot(start);
}
