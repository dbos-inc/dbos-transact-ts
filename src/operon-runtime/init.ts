import { async as glob } from 'fast-glob'
import path from 'path'
import fs from 'fs'

interface CopyOption {
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

export async function init(dest: string) {
  const templatePath = path.resolve(__dirname, '..', '..', '..', 'examples', 'hello');
  const targets = ["**"]
  await copy(templatePath, targets, dest);
}
