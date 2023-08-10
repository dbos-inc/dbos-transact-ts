import fs from 'fs';
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
      throw(new Error(`checking on ${path}. ${error.code}: ${error.errno}`));
    } else if (!stats.isFile()) {
      throw(new Error(`config file ${path} is not a file`));
    }
  });

  // Then, read its content
  const fileContent: string = fs.readFileSync(path, 'utf8');
  return fileContent;
}

export const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

export type ValuesOf<T> = T[keyof T];