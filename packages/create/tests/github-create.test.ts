import { afterEach, suite, test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { createTemplateFromGitHub } from '../github-create.js';

const realFetch = globalThis.fetch;

// Serve a fake demo-apps tree, answering each blob URL with the base64-encoded content GitHub returns.
function mockGitHub(templateName: string, files: Record<string, Buffer>) {
  const blobs = new Map<string, Buffer>();
  const tree = Object.entries(files).map(([name, content], i) => {
    const url = `https://api.github.com/blobs/${i}`;
    blobs.set(url, content);
    return { path: `typescript/${templateName}/${name}`, mode: '100644', type: 'blob', sha: `${i}`, url, size: 0 };
  });
  globalThis.fetch = (input: string | URL | Request) => {
    const url = input instanceof Request ? input.url : input.toString();
    const blob = blobs.get(url);
    const body = blob ? { content: blob.toString('base64'), encoding: 'base64' } : { tree, truncated: false };
    return Promise.resolve(new Response(JSON.stringify(body)));
  };
}

suite('createTemplateFromGitHub', async () => {
  afterEach(() => {
    globalThis.fetch = realFetch;
  });

  await test('writes text and binary template files byte for byte', async () => {
    const text = Buffer.from('# Café ☕\n', 'utf-8');
    // A favicon-like file: bytes that are not valid UTF-8
    const binary = Buffer.from(Array.from({ length: 256 }, (_, i) => i));
    mockGitHub('dbos-nextjs-starter', { 'README.md': text, 'app/favicon.ico': binary });

    const appDir = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'dbos-create-')), 'my-app');
    try {
      await createTemplateFromGitHub(appDir, 'dbos-nextjs-starter');
      assert.deepEqual(fs.readFileSync(path.join(appDir, 'README.md')), text);
      assert.deepEqual(fs.readFileSync(path.join(appDir, 'app/favicon.ico')), binary);
    } finally {
      fs.rmSync(path.dirname(appDir), { recursive: true, force: true });
    }
  });
}).catch(assert.fail);
