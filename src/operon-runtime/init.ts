import path from 'path'
import fs from 'fs'
import { execSync } from 'child_process'
import { OperonError } from '../error'
import { copy } from '../utils'

export async function init(appName: string) {
  if (fs.existsSync(appName)) {
    throw new OperonError(`Directory ${appName} already exists, exiting...`);
  }

  const templatePath = path.resolve(__dirname, '..', '..', '..', 'examples', 'hello');
  const targets = ["**"]
  await copy(templatePath, targets, appName);

  const packageJsonName = path.resolve(appName, 'package.json');
  const content = fs.readFileSync(packageJsonName, 'utf-8');
  let updatedContent = content.replace('"name": "operon-hello"', `"name": "${appName}"`);
  updatedContent = updatedContent.replace('"@dbos-inc/operon": "../..",', ``);
  fs.writeFileSync(packageJsonName, updatedContent, 'utf-8');
  execSync("npm i", {cwd: appName, stdio: 'inherit'})
  execSync("npm install --save @dbos-inc/operon", {cwd: appName, stdio: 'inherit'})
}
