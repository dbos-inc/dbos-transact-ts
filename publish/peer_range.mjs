#!/usr/bin/env node
// The peer range core and enterprise each declare on the other, from the version being built.
//
// Releases accept any patch of the same minor, since the two repos share vX.Y tags but not commit heights.
// Previews and test builds accept anything, because an npm range never matches a prerelease with a different
// patch number; preview users are expected to install matching previews of both packages.
//
// With no argument, reads the version from package.json in the current directory, so it runs after the
// version is stamped.

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const VERSION = /^(\d+)\.(\d+)\.(\d+)(-.+)?$/;

export function peerRange(version) {
  const match = VERSION.exec(version);
  if (!match) throw new Error(`Cannot parse version "${version}"; expected X.Y.Z with an optional prerelease`);
  return match[4] ? '*' : `~${match[1]}.${match[2]}.0`;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const version = process.argv[2] ?? JSON.parse(readFileSync(join(process.cwd(), 'package.json'), 'utf8')).version;
    process.stdout.write(`${peerRange(version)}\n`);
  } catch (error) {
    console.error(`error: ${error.message}`);
    process.exit(1);
  }
}
