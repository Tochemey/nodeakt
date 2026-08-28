# Maintainers

This document is for people with release rights on nodeakt. Day to day contribution rules live in [CONTRIBUTING.md](CONTRIBUTING.md); this guide covers the tasks only maintainers perform.

## Release runbook

Releases are tag driven and prepared by hand. There are no changesets, and nothing writes back to the repository on your behalf: the maintainer prepares the release commit locally, and pushing a `vX.Y.Z` tag is the single act that publishes it. The pipeline in [.github/workflows/release.yml](.github/workflows/release.yml) verifies, publishes to npm, and creates the GitHub release.

### Before you start

- You have push access to `main` and permission to create tags.
- The `NPM_TOKEN` repository secret is an npm automation token with publish rights on `@tochemey/nodeakt`. Everything else the pipeline needs (the GitHub token for the release, the OIDC token for npm provenance) is provided by the runner.
- `main` is green and holds exactly the commits you intend to ship.

### Cut a release

1. Choose the version. Follow semver: `patch` for fixes, `minor` for backward compatible features, `major` for breaking changes. For a preview, use a prerelease suffix such as `0.2.0-rc.1`.

2. Bump the `version` field in [package.json](package.json) to match.

3. Add a section to the top of [CHANGELOG.md](CHANGELOG.md). The heading must be exactly `## X.Y.Z` with no `v` prefix and nothing else on the line, because the pipeline extracts the release notes by matching that heading. Write the entries for a reader of the changelog, not for a reviewer:

   ```markdown
   ## 0.2.0

   ### Added
   - Distributed key/value store

   ### Fixed
   - Mailbox drain on shutdown
   ```

4. Commit both files together:

   ```sh
   git add package.json CHANGELOG.md
   git commit -m "chore: release v0.2.0"
   ```

5. Tag and push. The tag is the version with a `v` prefix:

   ```sh
   git tag v0.2.0
   git push origin main v0.2.0
   ```

That is the whole manual part. Pushing the tag triggers the pipeline.

### What the pipeline does

1. Runs typecheck, lint, and the test suite. A failure here stops the release before anything is published.
2. Verifies that `package.json` matches the tag. If they differ, the run aborts, which means step 2 or step 4 above was missed.
3. Builds and publishes to npm with provenance. The package is built fresh at publish time (through `prepublishOnly`), so `dist` is never committed. A plain tag publishes under the `latest` dist-tag; a prerelease tag (any version containing a hyphen, such as `v0.2.0-rc.1`) publishes under `next` and leaves `latest` untouched.
4. Creates the GitHub release from the `## X.Y.Z` section of `CHANGELOG.md`. If that section cannot be found, it falls back to notes generated from the commits and pull requests. Prerelease tags are marked as prereleases.

### If a release fails partway

- The pipeline never writes to the repository, so a failed run leaves `main` untouched.
- npm refuses to publish a version that already exists. If the npm step succeeded but a later step failed, do not reuse that version number; fix forward with the next patch.
- To retry a run that failed before publishing (a flaky test, say), fix the problem on `main`, then move the tag to the new commit:

  ```sh
  git push origin :refs/tags/v0.2.0   # delete the remote tag
  git tag -d v0.2.0                    # delete the local tag
  # commit the fix, then recreate and push the tag again
  ```
