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
const DESCRIBE = /^(v\d+\.\d+(?:\.\d+)?)-(\d+)-g([0-9a-f]+)$/;
const TAG = /^v(\d+)\.(\d+)(?:\.(\d+))?$/;

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
  const release = RELEASE_BRANCH.exec(branch);
  // A release branch is versioned only by its own minor's tags: a commit with no changes since the previous
  // release carries that release's tag too, and must still build as the older line.
  const patterns = release
    ? ['--match', `v${release[1]}.${release[2]}`, '--match', `v${release[1]}.${release[2]}.*`]
    : ['--match', 'v[0-9]*'];
  const described = git(['describe', '--tags', '--long', '--abbrev=7', ...patterns, commit], cwd);
  const match = DESCRIBE.exec(described);
  if (!match) {
    throw new Error(`Cannot parse git describe output "${described}"; expected a vX.Y tag in the commit's history`);
  }
  const [, nearest, heightStr, sha] = match;
  const [major, minor, floor] = highestTagAt(git(['rev-list', '-n1', nearest], cwd), cwd, release);
  const height = Number(heightStr);
  const patch = floor + height;

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

// Several vX.Y[.Z] tags may share a commit, such as a release tag and a later patch-floor tag; the highest wins.
// On a release branch only that minor's tags are considered, matching the describe above.
function highestTagAt(commit, cwd, release) {
  const parsed = git(['tag', '--points-at', commit, '--list', 'v[0-9]*'], cwd)
    .split('\n')
    .map((tag) => TAG.exec(tag))
    .filter(Boolean)
    .map(([, major, minor, patch]) => [Number(major), Number(minor), Number(patch ?? 0)])
    .filter(([major, minor]) => !release || (major === Number(release[1]) && minor === Number(release[2])));
  parsed.sort((a, b) => a[0] - b[0] || a[1] - b[1] || a[2] - b[2]);
  return parsed[parsed.length - 1];
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
