import glob from 'fast-glob';
import path from 'path';
import fs from 'fs';
import { execSync } from 'child_process';
import validator from 'validator';
import { fileURLToPath } from 'url';
import { createTemplateFromGitHub } from './github-create.js';
import chalk from 'chalk';

interface CopyOption {
  rename?: (basename: string) => string;
}

const identity = (x: string) => x;

export const copy = async (src: string, targets: string[], dest: string, { rename = identity }: CopyOption = {}) => {
  if (targets.length === 0 || !dest) {
    throw new TypeError('`src` and `dest` are required');
  }

  const sourceFiles = await glob(targets, {
    cwd: src,
    dot: true,
    absolute: false,
    stats: false,
    ignore: ['package-lock.json', '**/node_modules/**', '**/dist/**'],
  });

  return Promise.all(
    sourceFiles.map(async (p) => {
      const dirname = path.dirname(p);
      const basename = rename(path.basename(p));

      const from = path.resolve(src, p);
      const to = path.join(dest, dirname, basename);

      // Ensure the destination directory exists
      await fs.promises.mkdir(path.dirname(to), { recursive: true });

      return fs.promises.copyFile(from, to);
    }),
  );
};

export function isValidApplicationName(appName: string): boolean | string {
  if (appName.length < 3 || appName.length > 30) {
    return 'Application name must be between 3 and 30 characters long';
  }
  if (!validator.matches(appName, '^[a-z0-9-_]+$')) {
    return 'Application name can only contain lowercase letters, numbers, hyphens and underscores';
  }
  return true;
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

    if (isDirectory && !pathPrefixesAllowedInEmpty.some((prefix) => file.startsWith(prefix))) {
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
  templateGISet.forEach((line) => {
    if (!resultSet.has(line)) {
      resultSet.add(line);
    }
  });
  const joined = Array.from(resultSet).join('\n');
  return joined.replaceAll('\n#', '\n\n#');
}

function updateLocalFileDependency(appName: string, deps?: Record<string, string>) {
  if (!deps) {
    return;
  }

  for (const [depName, depVersion] of Object.entries(deps)) {
    if (depName.startsWith('@dbos-inc/') && depVersion.startsWith('file:')) {
      // Install the latest version of DBOS dependency.
      execSync(`npm install --no-fund --save ${depName}@latest --loglevel=error`, {
        cwd: appName,
        stdio: 'inherit',
      });
    }
  }
}

function mergeGitignoreFiles(existingFilePath: string, templateFilePath: string, outputFilePath: string): void {
  const existingSet = loadGitignoreFile(existingFilePath);
  const templateSet = loadGitignoreFile(templateFilePath);
  const resultContent = mergeGitIgnore(existingSet, templateSet);
  fs.writeFileSync(outputFilePath, resultContent);
  console.log(`Merged .gitignore files saved to ${outputFilePath}`);
}

interface PackageJson {
  name: string;
  dependencies?: Record<string, string>;
}

export async function init(appName: string, templateName: string) {
  if (isValidApplicationName(appName) !== true) {
    throw new Error(
      `Invalid application name: ${appName}. Application name must be between 3 and 30 characters long and can only contain lowercase letters, numbers, hyphens and underscores. Exiting...`,
    );
  }

  const __dirname = fileURLToPath(new URL('.', import.meta.url));
  const templatePath = path.resolve(__dirname, '..', 'templates', templateName);
  const allTemplates = listTemplates();
  if (!allTemplates.includes(templateName)) {
    throw new Error(
      `Template does not exist: ${chalk.yellow(templateName)}. Please choose from: ${chalk.bold(allTemplates.join(', '))}. Exiting...`,
    );
  }

  if (dirHasStuffInIt(appName)) {
    throw new Error(`Directory ${chalk.yellow(appName)} already exists, please choose another name. Exiting...`);
  }

  if (DEMO_TEMPLATES.includes(templateName)) {
    // Download the template from the demo apps repository
    await createTemplateFromGitHub(appName, templateName);
  } else {
    // Copy the template from the local templates directory
    const targets = ['**'];
    await copy(templatePath, targets, appName);
    mergeGitignoreFiles(
      path.join(appName, '.gitignore'),
      path.join(appName, 'gitignore.template'),
      path.join(appName, '.gitignore'),
    );
    fs.rmSync(path.resolve(appName, 'gitignore.template'));
  }

  const packageJsonName = path.resolve(appName, 'package.json');
  const packageJson: PackageJson = JSON.parse(fs.readFileSync(packageJsonName, 'utf-8')) as PackageJson;
  packageJson.name = appName;
  fs.writeFileSync(packageJsonName, JSON.stringify(packageJson, null, 2), 'utf-8');
  updateLocalFileDependency(appName, packageJson.dependencies);
  execSync('npm i --no-fund --loglevel=error', { cwd: appName, stdio: 'inherit' });
  execSync('npm install --no-fund --save-dev @dbos-inc/dbos-cloud@latest', { cwd: appName, stdio: 'inherit' });
  console.log('Application initialized successfully!');
}

// Templates that will be downloaded through the demo apps repository
const DEMO_TEMPLATES = ['dbos-node-toolbox', 'dbos-node-starter', 'dbos-nextjs-starter'];

// Return a list of available templates
export function listTemplates(): string[] {
  const __dirname = fileURLToPath(new URL('.', import.meta.url));
  const templatePath = path.resolve(__dirname, '..', 'templates');
  const items = fs.readdirSync(templatePath, { withFileTypes: true });
  const templates = items.filter((item) => item.isDirectory()).map((folder) => folder.name);

  // Insert demo templates at the beginning of the list
  templates.unshift(...DEMO_TEMPLATES);
  return templates;
}
