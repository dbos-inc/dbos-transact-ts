import { async as glob } from 'fast-glob'
import path from 'path'
import fs from 'fs'
import { execSync } from 'child_process'
import { DBOSError } from '../error'

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

export async function init(appName: string) {

  if (fs.existsSync(appName)) {
    throw new DBOSError(`Directory ${appName} already exists, exiting...`);
  }

  const templatePath = path.resolve(__dirname, '..', '..', '..', 'examples', 'hello');
  const targets = ["**"]
  await copy(templatePath, targets, appName);

  const packageJsonName = path.resolve(appName, 'package.json');
  const packageJson: { name: string } = JSON.parse(fs.readFileSync(packageJsonName, 'utf-8')) as { name: string };
  packageJson.name = appName;
  fs.writeFileSync(packageJsonName, JSON.stringify(packageJson, null, 2), 'utf-8');
  execSync("npm i --no-fund", {cwd: appName, stdio: 'inherit'})
  execSync("npm uninstall --no-fund @dbos-inc/dbos-sdk", {cwd: appName, stdio: 'inherit'})
  execSync("npm install --no-fund --save @dbos-inc/dbos-sdk", {cwd: appName, stdio: 'inherit'})
  execSync("npm install --no-fund --save-dev @dbos-inc/dbos-cloud", {cwd: appName, stdio: 'inherit'})
  console.log("Application initialized successfully!")
}
