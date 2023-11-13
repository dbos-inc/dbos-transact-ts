import fs from "fs";
import path from 'path';
import { async as glob } from 'fast-glob'

/* A wrap around glob, mkdir and copyFile to copy files from one directory to another
 **/
export interface CopyOption {
  rename?: (basename: string) => string
}

const identity = (x: string) => x

export const copy = async (
  src: string,
  targets: string[],
  dest: string,
  { rename = identity }: CopyOption = {}
) => {

  if (targets.length === 0 || !dest) {
    throw new TypeError('`src` and `dest` are required')
  }

  const sourceFiles = await glob(targets, {
    cwd: src,
    dot: true,
    absolute: false,
    stats: false,
    ignore: ['**/node_modules/**', '**/dist/**']
  })

  return Promise.all(
    sourceFiles.map(async (p) => {
      const dirname = path.dirname(p)
      const basename = rename(path.basename(p))

      const from = path.resolve(src, p);
      const to = path.join(dest, dirname, basename);

      // Ensure the destination directory exists
      await fs.promises.mkdir(path.dirname(to), { recursive: true })

      return fs.promises.copyFile(from, to)
    })
  )
}

/*
 * Use the node.js `fs` module to read the content of a file
 * Handles cases where:
 * - the file does not exist
 * - the file is not a valid file
 **/
export function readFileSync(path: string): string {
  // First, check the file
  fs.stat(path, (error: NodeJS.ErrnoException | null, stats: fs.Stats) => {
    if (error) {
      throw new Error(`checking on ${path}. ${error.code}: ${error.errno}`);
    } else if (!stats.isFile()) {
      throw new Error(`config file ${path} is not a file`);
    }
  });

  // Then, read its content
  const fileContent: string = fs.readFileSync(path, "utf8");
  return fileContent;
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export type ValuesOf<T> = T[keyof T];
