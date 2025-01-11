import next from 'next';
import http, { IncomingMessage, ServerResponse } from 'http';
import path from 'path';
import fs from 'fs/promises';
import fg from 'fast-glob';

import { DBOS, parseConfigFile } from '@dbos-inc/dbos-sdk';
import { DBOSRuntime } from '@dbos-inc/dbos-sdk/dist/src/dbos-runtime/runtime';

// This is to handle files, in case entrypoints is not manually specified
export async function loadAllServerFiles() {
  const serverDir = path.resolve(__dirname, "dbos");

  const files = await fg(['**/*.ts', '**/*.js', '**/*.jsx', '**/*.tsx'], {
    cwd: serverDir,
    absolute: true,
  });

  console.log(`Files in ${serverDir}: ${files.length}`);

  for (const file of files) {
    if (file.endsWith('.d.ts')) continue;
    if (file.endsWith('.jsx')) continue;
    if (file.endsWith('.tsx')) continue;
    try {
      // Read the first few lines of the file
      const content = await fs.readFile(file, 'utf-8');
      const firstLine = content.split('\n')[0].trim();

      // Skip files with "use client"
      if (firstLine.startsWith('"use client"')) {
        continue;
      }

      // Dynamically load the file
      await import(file);
      console.log(`Loaded: ${file}`);
    } catch (error) {
      console.error(`Error loading ${file}:`, error);
    }
  }
}

const app = next({ dev: process.env.NODE_ENV !== 'production' });
const handle = app.getRequestHandler();

async function main() {
  const [_cfg, rtcfg] = parseConfigFile();

  if (rtcfg && rtcfg.entrypoints && rtcfg.entrypoints.length) {
    await DBOSRuntime.loadClasses(rtcfg.entrypoints);
  }
  else {
    await loadAllServerFiles();
  }
  await DBOS.launch();

  await app.prepare();

  const PORT = DBOS.runtimeConfig?.port ?? 3000;
  const ENV = process.env.NODE_ENV || 'development';

  http.createServer((req, res) => {
    handle(req, res as ServerResponse<IncomingMessage>);
  }).listen(PORT, () => {
    console.log(`🚀 Server is running on http://localhost:${PORT}`);
    console.log(`🌟 Environment: ${ENV}`);
  });
}

// Only start the server when this file is run directly from Node
if (require.main === module) {
  main().catch(console.log);
}

