#!/usr/bin/env node
// Derive the npm version and dist-tag for the current commit from git tags and the branch name.
//
//   release/vX.Y branch:  X.Y.<commits since tag vX.Y>                   -> dist-tag "latest"
//   main:                 X.(Y+1).<commits since nearest vX.Y tag>-preview -> dist-tag "preview"
//   any other branch:     X.(Y+1).<height>-test.<short sha>               -> dist-tag "test"
//
// A three-part tag vX.Y.Z on a release branch sets the patch floor, so the next commit is X.Y.(Z+1).
//
// Prints `version=` and `dist_tag=` lines, and appends them to $GITHUB_OUTPUT when set.

import { execFileSync } from 'node:child_process';
import { appendFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

const RELEASE_BRANCH = /^release\/v(\d+)\.(\d+)$/;
const DESCRIBE = /^v(\d+)\.(\d+)(?:\.(\d+))?-(\d+)-g([0-9a-f]+)$/;

function git(args, cwd) {
  return execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
}

/**
 * @param {{ ref?: string, commit?: string, cwd?: string }} options
 *   ref: branch name to derive the version for; defaults to $GITHUB_REF_NAME, then the checked-out branch.
 *   commit: commit to version; defaults to HEAD.
 * @returns {{ version: string, distTag: string }}
 */
export function computeVersion({ ref, commit = 'HEAD', cwd } = {}) {
  const branch = ref ?? process.env.GITHUB_REF_NAME ?? git(['rev-parse', '--abbrev-ref', 'HEAD'], cwd);
  const described = git(['describe', '--tags', '--long', '--abbrev=7', '--match', 'v[0-9]*', commit], cwd);
  const match = DESCRIBE.exec(described);
  if (!match) {
    throw new Error(`Cannot parse git describe output "${described}"; expected a vX.Y tag in the commit's history`);
  }
  const [, tagMajor, tagMinor, tagPatch, heightStr, sha] = match;
  const major = Number(tagMajor);
  const minor = Number(tagMinor);
  const height = Number(heightStr);
  const patch = Number(tagPatch ?? 0) + height;

  const release = RELEASE_BRANCH.exec(branch);
  if (release) {
    if (Number(release[1]) !== major || Number(release[2]) !== minor) {
      throw new Error(`Branch ${branch} is versioned by tag v${major}.${minor}; the branch name and tag disagree`);
    }
    return { version: `${major}.${minor}.${patch}`, distTag: 'latest' };
  }
  const next = `${major}.${minor + 1}.${height}`;
  if (branch === 'main') {
    return { version: `${next}-preview`, distTag: 'preview' };
  }
  return { version: `${next}-test.${sha}`, distTag: 'test' };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const { version, distTag } = computeVersion();
    const lines = `version=${version}\ndist_tag=${distTag}\n`;
    process.stdout.write(lines);
    if (process.env.GITHUB_OUTPUT) {
      appendFileSync(process.env.GITHUB_OUTPUT, lines);
    }
  } catch (error) {
    console.error(`error: ${error.message}`);
    process.exit(1);
  }
}
