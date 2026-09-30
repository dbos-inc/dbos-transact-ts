#!/usr/bin/env node
// Cut a release of every package in this repo from the tip of main, in one command.
//
//   node publish/make_release.mjs                    release the next minor version from main
//   node publish/make_release.mjs --version 6.0      release a specific version from main
//   node publish/make_release.mjs --patch 5.3        publish a patch from the existing release/v5.3 branch
//   ... --no-publish                                 tag and push only; skip running the publish workflow
//
// A minor release tags main as vX.Y, pushes the tag together with a new release/vX.Y branch, then runs
// the publish workflow on that branch and waits for it. Nothing is pushed until every check passes,
// and the tag and branch are pushed atomically, so a failure leaves the remote untouched.
// Versions are derived from the tag by publish/version.mjs, so no commit is needed on any branch.

import { execFileSync, spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { parseArgs } from 'node:util';
import { computeVersion } from './version.mjs';

const WORKFLOW = 'publish_npm.yml';
const VERSION = /^(\d+)\.(\d+)$/;

class ReleaseError extends Error {}

function git(args, options = {}) {
  return (execFileSync('git', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], ...options }) ?? '').trim();
}

function gh(args, options = {}) {
  return (
    execFileSync('gh', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'], ...options }) ?? ''
  ).trim();
}

function refExists(ref) {
  return spawnSync('git', ['rev-parse', '--verify', '--quiet', ref], { stdio: 'ignore' }).status === 0;
}

function remoteRefExists(ref) {
  return spawnSync('git', ['ls-remote', '--exit-code', 'origin', ref], { stdio: 'ignore' }).status === 0;
}

function main() {
  const { values } = parseArgs({
    options: {
      version: { type: 'string' },
      patch: { type: 'string' },
      'no-publish': { type: 'boolean', default: false },
    },
  });
  if (values.version && values.patch) {
    throw new ReleaseError('--version and --patch are mutually exclusive');
  }
  const requested = values.version ? parseVersion(values.version) : undefined;
  const patchBranch = values.patch ? `release/v${parseVersion(values.patch)}` : undefined;

  gh(['auth', 'status']);
  git(['fetch', '--tags', 'origin']);

  if (patchBranch) {
    const branch = patchBranch;
    if (!remoteRefExists(`refs/heads/${branch}`)) {
      throw new ReleaseError(`${branch} does not exist on origin`);
    }
    // A dispatched workflow runs the file as committed on that branch, so the branch must carry this scheme.
    if (
      spawnSync('git', ['cat-file', '-e', `origin/${branch}:publish/version.mjs`], { stdio: 'ignore' }).status !== 0
    ) {
      throw new ReleaseError(`${branch} predates tag-based versioning and cannot be patched with this script`);
    }
    const tagged = exactTag(`origin/${branch}`);
    if (tagged) {
      throw new ReleaseError(`${branch} has no commits since ${tagged}; nothing new to publish`);
    }
    const { version } = computeVersion({ ref: branch, commit: `origin/${branch}` });
    checkUnpublished(version);
    console.log(`Publishing ${version} from ${branch}`);
    if (!values['no-publish']) publish(branch);
    return;
  }

  checkReady();
  const previous = latestTag();
  const version = requested ?? guessNextVersion(previous);
  if (compareVersions(version, previous.replace(/^v/, '')) <= 0) {
    throw new ReleaseError(`Version ${version} is not greater than the latest release ${previous}`);
  }
  const tag = `v${version}`;
  const branch = `release/v${version}`;
  for (const ref of [`refs/tags/${tag}`, `refs/heads/${branch}`]) {
    if (refExists(ref)) throw new ReleaseError(`${ref} already exists locally`);
    if (remoteRefExists(ref)) throw new ReleaseError(`${ref} already exists on origin`);
  }

  createTag(tag, branch, version);
  try {
    git(['push', '--atomic', 'origin', `refs/tags/${tag}`, `HEAD:refs/heads/${branch}`], { stdio: 'inherit' });
  } catch (error) {
    // The atomic push left origin untouched, so deleting the local tag restores the starting state.
    git(['tag', '--delete', tag]);
    throw error;
  }
  console.log(`Pushed tag ${tag} and branch ${branch} at ${git(['rev-parse', '--short', 'HEAD'])}`);
  if (!values['no-publish']) publish(branch);
}

function parseVersion(input) {
  const match = VERSION.exec(input);
  if (!match) throw new ReleaseError(`Invalid version "${input}"; expected X.Y`);
  return `${Number(match[1])}.${Number(match[2])}`;
}

function compareVersions(a, b) {
  const [aMajor, aMinor] = a.split('.').map(Number);
  const [bMajor, bMinor] = b.split('.').map(Number);
  return aMajor - bMajor || aMinor - bMinor;
}

function checkReady() {
  if (git(['status', '--porcelain']) !== '') {
    throw new ReleaseError('Working tree is not clean');
  }
  if (git(['rev-parse', '--abbrev-ref', 'HEAD']) !== 'main') {
    throw new ReleaseError('Releases are cut from main; check out main first');
  }
  if (git(['rev-parse', 'HEAD']) !== git(['rev-parse', 'origin/main'])) {
    throw new ReleaseError('Local main differs from origin/main');
  }
  // Each release commit carries exactly one tag; a second tag would make versions ambiguous.
  const existing = exactTag('HEAD');
  if (existing) {
    throw new ReleaseError(`main has no commits since ${existing}; nothing to release`);
  }
}

// The v* tag sitting exactly on a commit, or undefined.
function exactTag(commit) {
  const result = spawnSync('git', ['describe', '--tags', '--exact-match', '--match', 'v[0-9]*', commit], {
    encoding: 'utf8',
  });
  return result.status === 0 ? result.stdout.trim() : undefined;
}

// Refuse a version that would republish or fall below what the root package already has on npm.
function checkUnpublished(version) {
  const [major, minor, patch] = version.split('.').map(Number);
  const root = git(['rev-parse', '--show-toplevel']);
  const name = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8')).name;
  const out = execFileSync('npm', ['view', name, 'versions', '--json'], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const parsed = JSON.parse(out);
  const pattern = new RegExp(`^${major}\\.${minor}\\.(\\d+)$`);
  const published = (Array.isArray(parsed) ? parsed : [parsed])
    .map((v) => pattern.exec(v))
    .filter(Boolean)
    .map((m) => Number(m[1]));
  if (published.length > 0 && Math.max(...published) >= patch) {
    const highest = `${major}.${minor}.${Math.max(...published)}`;
    throw new ReleaseError(`Version ${version} is not above ${highest}, which ${name} already has on npm`);
  }
}

// The newest vX.Y tag reachable from HEAD, which is what version.mjs derives versions from.
function latestTag() {
  return git(['describe', '--tags', '--abbrev=0', '--match', 'v[0-9]*']);
}

function guessNextVersion(previousTag) {
  const [major, minor] = previousTag.replace(/^v/, '').split('.').map(Number);
  return `${major}.${minor + 1}`;
}

// Tag locally and confirm the release branch would publish exactly X.Y.0 before anything is pushed.
function createTag(tag, branch, version) {
  git(['tag', '--annotate', tag, '--message', `Release ${version}`]);
  try {
    const computed = computeVersion({ ref: branch }).version;
    if (computed !== `${version}.0`) {
      throw new ReleaseError(`A build of ${branch} would be versioned ${computed}, not ${version}.0`);
    }
    checkUnpublished(computed);
  } catch (error) {
    git(['tag', '--delete', tag]);
    throw error;
  }
}

// Run the publish workflow on a branch and wait for it to finish.
function publish(branch) {
  const before = latestRun(branch);
  gh(['workflow', 'run', WORKFLOW, '--ref', branch]);
  let run;
  for (let attempt = 0; attempt < 60 && (run === undefined || run === before); attempt++) {
    sleep(2000);
    run = latestRun(branch);
  }
  if (run === undefined || run === before) {
    throw new ReleaseError(`Publish run for ${branch} did not start; check the Actions tab`);
  }
  console.log(`Publishing from ${branch}: ${gh(['run', 'view', String(run), '--json', 'url', '--jq', '.url'])}`);
  gh(['run', 'watch', String(run), '--exit-status'], { stdio: 'inherit' });
  console.log(`Published ${branch}`);
}

function latestRun(branch) {
  const runs = JSON.parse(
    gh(['run', 'list', '--workflow', WORKFLOW, '--branch', branch, '--limit', '1', '--json', 'databaseId']),
  );
  return runs.length > 0 ? runs[0].databaseId : undefined;
}

function sleep(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

try {
  main();
} catch (error) {
  // Failed git and gh commands have already written their own stderr.
  console.error(`error: ${error.message}`);
  process.exit(1);
}
