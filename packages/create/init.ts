import glob from 'fast-glob'
import path from 'path'
import fs from 'fs'
import { execSync } from 'child_process'
import validator from 'validator';
import { fileURLToPath } from 'url';

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

function isValidApplicationName(appName: string): boolean {
  if (appName.length < 3 || appName.length > 30) {
    return false;
  }
  return validator.matches(appName, "^[a-z0-9-_]+$");
}

const filesAllowedInEmpty = ['.gitignore', 'readme.md'];
const pathPrefixesAllowedInEmpty = ['.']; // Directories that start with 

function dirHasStuffInIt(pathName: string): boolean {
  if (!fs.existsSync(pathName)) {
    return false;
  }

  const files = fs.readdirSync(pathName);
  for (const file of files) {
      const fullPath = path.join(pathName, file);
      const isDirectory = fs.lstatSync(fullPath).isDirectory();

      if (isDirectory && !pathPrefixesAllowedInEmpty.some(prefix => file.startsWith(prefix))) {
          //console.log(`Directory ${pathName} is not sufficiently empty because it contains directory: ${file}`);
          return true;
      }

      // If it's not a directory, and not in the list of allowed files, return false
      if (!isDirectory && !filesAllowedInEmpty.includes(file.toLowerCase())) {
          //console.log(`Directory ${pathName} is not sufficiently empty because it contains file: ${file}`);
          return true;
      }
  }

  return false;
}

function loadGitignoreFile(filePath: string): Set<string> {
  if (!fs.existsSync(filePath)) return new Set();
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split(/\r?\n/);
  return new Set(lines);
}

function mergeGitIgnore(existingGISet: Set<string>, templateGISet: Set<string>): string {
    const resultSet = new Set(existingGISet);
    templateGISet.forEach(line => {
        if (!resultSet.has(line)) {
            resultSet.add(line);
        }
    });
    const joined = Array.from(resultSet).join('\n');
    return joined.replaceAll('\n#','\n\n#');
}

function mergeGitignoreFiles(existingFilePath: string, templateFilePath: string, outputFilePath: string): void {
    const existingSet = loadGitignoreFile(existingFilePath);
    const templateSet = loadGitignoreFile(templateFilePath);
    const resultContent = mergeGitIgnore(existingSet, templateSet);
    fs.writeFileSync(outputFilePath, resultContent);
    console.log(`Merged .gitignore files saved to ${outputFilePath}`);
}



export async function init(appName: string, templateName: string) {
  if (!isValidApplicationName(appName)) {
    throw new Error(`Invalid application name: ${appName}. Application name must be between 3 and 30 characters long and can only contain lowercase letters, numbers, hyphens and underscores. Exiting...`);
  }

  const __dirname = fileURLToPath(new URL('.', import.meta.url));
  const templatePath = path.resolve(__dirname, '..', 'templates', templateName);
  if (!fs.existsSync(templatePath)) {
    throw new Error(`Template does not exist: ${templateName}. Exiting...`);
  }

  if (dirHasStuffInIt(appName)) {
    throw new Error(`Directory ${appName} already exists, exiting...`);
  }

  const targets = ["**"]
  await copy(templatePath, targets, appName);
  mergeGitignoreFiles(path.join(appName, '.gitignore'), path.join(appName, 'gitignore.template'), path.join(appName, '.gitignore'));
  fs.rmSync(path.resolve(appName, 'gitignore.template'));

  const packageJsonName = path.resolve(appName, 'package.json');
  const packageJson: { name: string } = JSON.parse(fs.readFileSync(packageJsonName, 'utf-8')) as { name: string };
  packageJson.name = appName;
  fs.writeFileSync(packageJsonName, JSON.stringify(packageJson, null, 2), 'utf-8');
  execSync("npm install --no-fund --save @dbos-inc/dbos-sdk@latest", {cwd: appName, stdio: 'inherit'})
  execSync("npm ci --no-fund", {cwd: appName, stdio: 'inherit'})
  execSync("npm install --no-fund --save-dev @dbos-inc/dbos-cloud@latest", {cwd: appName, stdio: 'inherit'})
  console.log("Application initialized successfully!")
}
