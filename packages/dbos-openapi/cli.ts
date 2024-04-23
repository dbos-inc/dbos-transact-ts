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

program
  .command("generate")
  .argument('<entrypoints...>', 'Specify entrypoints file path')
  .action(async (entrypoints: string[]) => {
    for (const entrypoint of entrypoints) {
      const openapi = await generateOpenApi(entrypoint);
      if (openapi) {
        const filename = path.join(path.dirname(entrypoint), "openapi.yaml");
        const yaml = `# OpenApi specification generated for application\n\n` + YAML.stringify(openapi, { aliasDuplicateObjects: false });
        await fs.writeFile(filename, yaml, { encoding: 'utf-8' });
      }
    }
  });

program.parse(process.argv);

// If no arguments provided, display help by default
if (!process.argv.slice(2).length) {
  program.outputHelp();
}
