# Developing DBOS Transact

## Releasing

Every package in this repo is released together, from the tip of `main`, with one command:

```shell
node publish/make_release.mjs
```

This tags `main` as `vX.Y` (the next minor version), pushes the tag together with a new `release/vX.Y` branch, then runs the publish workflow on that branch and waits for it to finish.
Nothing is pushed until every check passes: clean working tree, on `main`, identical to `origin/main`, and at least one commit since the last release.
No commit is made on any branch.

To release a specific version, such as a major bump:

```shell
node publish/make_release.mjs --version 6.0
```

To publish a patch after merging backports into an existing release branch:

```shell
node publish/make_release.mjs --patch 5.3
```

Pass `--no-publish` to tag and push without running the publish workflow.
You need the [GitHub CLI](https://cli.github.com/) logged in with `repo` and `workflow` scopes, and permission to create branches and tags in this repo.

### Versions

`publish/version.mjs` derives the version of a build from the nearest `vX.Y` tag and the number of commits since it.
Run it on any checkout to see what a build of the current branch would be versioned.

| Branch         | Version                                  | npm dist-tag |
| -------------- | ---------------------------------------- | ------------ |
| `release/vX.Y` | `X.Y.<commits since tag vX.Y>`           | `latest`     |
| `main`         | `X.(Y+1).<commits since tag>-preview`    | `preview`    |
| anything else  | `X.(Y+1).<commits since tag>-test.<sha>` | `test`       |

The publish workflow runs on every push to `main`, so each merge publishes a preview.
Dispatching it manually on any other branch publishes a `test` build, which is how to try out the publish path without touching `latest`.

Release branches created before this scheme (`release/v5.2` and earlier) already have patch versions higher than the commit count since their tag.
To publish a patch from one of them, first tag its tip with the last version published from it, such as `v5.2.11`; later commits are then versioned `5.2.12` and up.
