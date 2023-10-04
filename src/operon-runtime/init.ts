import { async as glob } from 'fast-glob'
import path from 'path'
import fs from 'fs'

interface CopyOption {
  cwd?: string
  rename?: (basename: string) => string
  parents?: boolean
}

const identity = (x: string) => x

export const copy = async (
  src: string | string[],
  dest: string,
  { cwd, rename = identity, parents = true }: CopyOption = {}
) => {
  const source = typeof src === 'string' ? [src] : src

  if (source.length === 0 || !dest) {
    throw new TypeError('`src` and `dest` are required')
  }

  console.log(source);
  const sourceFiles = await glob(source, {
    cwd,
    dot: true,
    absolute: false,
    stats: false,
    ignore: ['**/node_modules/**', '**/dist/**']
  })
  console.log(sourceFiles);


  return Promise.all(
    sourceFiles.map(async (p) => {
      const dirname = path.dirname(p)
      const basename = rename(path.basename(p))

      const from = cwd ? path.resolve(cwd, p) : p
      const to = parents
        ? path.join(dest, dirname, basename)
        : path.join(dest, basename)

      // Ensure the destination directory exists
      await fs.promises.mkdir(path.dirname(to), { recursive: true })

      return fs.promises.copyFile(from, to)
    })
  )
}

export async function init(dest: string) {
  const copySource = ["**"]
  const templatePath = path.resolve(__dirname, '..', '..', '..', 'examples', 'hello');
  console.log(templatePath);
  await copy(copySource, dest,
    {
      parents: true,
      cwd: templatePath
    });
}
