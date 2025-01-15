#!/usr/bin/env node

import { Command } from 'commander';
import { generateOpenApi } from "./openApi";
import YAML from 'yaml';
import fs from 'node:fs/promises';
import path from 'node:path';

const program = new Command();

////////////////////////
/* OpenAPI  */
////////////////////////

async function findTsFiles(directory: string): Promise<string[]> {
  const entries = await fs.readdir(directory, { withFileTypes: true });
  const tsFiles: string[] = [];

  for (const entry of entries) {
    const fullPath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      tsFiles.push(...await findTsFiles(fullPath));
    } else if (entry.isFile() && fullPath.endsWith('.ts')) {
      tsFiles.push(fullPath);
    }
  }

  return tsFiles;
}

async function processEntrypoints(entrypoints: string[]) {
  for (const entrypoint of entrypoints) {
    const stats = await fs.stat(entrypoint);
    let filesToProcess: string[];

    if (stats.isDirectory()) {
      filesToProcess = await findTsFiles(entrypoint);
    } else if (stats.isFile() && entrypoint.endsWith('.ts')) {
      filesToProcess = [entrypoint];
    } else {
      console.warn(`Skipping unsupported entrypoint: ${entrypoint}`);
      continue;
    }

    for (const file of filesToProcess) {
      const openapi = await generateOpenApi(file);
      if (openapi) {
        const filename = path.join(path.dirname(file), filesToProcess.length > 1 ? path.basename(file, '.ts')+'.yaml' : "openapi.yaml");
        const yaml = `# OpenApi specification generated for application\n\n` + YAML.stringify(openapi, { aliasDuplicateObjects: false });
        await fs.writeFile(filename, yaml, { encoding: 'utf-8' });
      }
      else {
        console.warn(`No OpenAPI generated for ${file}`);
      }
    }
  }
}

program
  .command("generate")
  .argument('<entrypoints...>', 'Specify entrypoints files / paths')
  .action(async (entrypoints: string[]) => {
    await processEntrypoints(entrypoints);
  });

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
