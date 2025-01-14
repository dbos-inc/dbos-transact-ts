#!/usr/bin/env node
import { Command } from 'commander';
import { init, isValidApplicationName, listTemplates } from './init.js';
import fs from 'fs'
import path from "path";
import { Package } from "update-notifier";
import { input, select } from "@inquirer/prompts";

const program = new Command();

////////////////////////
/* LOCAL DEVELOPMENT  */
////////////////////////

import { fileURLToPath } from 'url';
const __dirname = fileURLToPath(new URL('.', import.meta.url));
const packageJson = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "package.json")).toString()) as Package;
program.version(packageJson.version);

program
  .description('Init a DBOS application')
  .option('-n, --appName <application-name>', 'Application name')
  .option('-t, --template <template name>', 'Name of template application to copy')
  .action(async (options: { appName?: string, template?: string }, command: Command) => {
    if (command.args.length > 0) {
      throw new Error(`Unexpected arguments: ${command.args.join(',')}; Did you forget '--'?`);
    }
    let {appName, template} = options;
    if (appName || template) {
      appName = appName || 'dbos-hello-app';
      template = template || 'hello';
    }
    else {
      const templates = listTemplates();
      // TODO: add descriptions for each template.
      template = await select(
        {
          message: 'Choose a template to use:',
          choices: templates.map(t => ({ name: t, value: t })),
        });
      appName = await input(
        {
          message: 'What is the application/directory name to create?',
          default: template, // Default to the template name
          validate: isValidApplicationName,
        });
    }
    try {
      await init(appName, template);
    } catch (e) {
      console.error((e as Error).message);
      process.exit(1);
    }
  })
  .allowUnknownOption(false);

program.parse(process.argv);

