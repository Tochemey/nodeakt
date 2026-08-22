# Contributing to nodeakt

Thank you for considering a contribution. This document is the practical guide: how to set up, how the project is verified, and what a change needs before it can merge.

Please also read the [code of conduct](CODE_OF_CONDUCT.md). Security issues follow the [security policy](SECURITY.md) instead of the public issue tracker.

## Prerequisites

- Node.js 22 or newer (CI tests 22, 24, and 26)
- pnpm 11 (the repository pins it through `devEngines`; npm and yarn will refuse to run here)

## Getting started

```sh
git clone https://github.com/Tochemey/nodeakt.git
cd nodeakt
pnpm install
```

`pnpm install` also installs the git hooks (through lefthook): a pre-commit hook runs Biome on staged files, applies safe fixes, and re-stages them. If a commit is rejected, the output names the file and rule.

Verify the setup:

```sh
pnpm typecheck && pnpm lint && pnpm test
```

## Everyday commands

| Command          | What it does                                                             |
|------------------|--------------------------------------------------------------------------|
| `pnpm test`      | Run the test suite (vitest)                                              |
| `pnpm typecheck` | Type-check with tsc; the build emits nothing, types must be clean        |
| `pnpm lint`      | Check formatting and lint rules (Biome)                                  |
| `pnpm fix`       | Apply safe Biome fixes                                                   |
| `pnpm coverage`  | Tests with coverage                                                      |
| `pnpm build`     | Build the publishable package (tsdown)                                   |
| `make bench`     | Run the benchmark suite (see [benchmark/README.md](benchmark/README.md)) |
| `make`           | List the runnable examples                                               |

## Coding guidelines

**TypeScript.** The codebase is strict-mode TypeScript, ESM only. `pnpm typecheck` must pass with no errors; Biome (2-space indent, 100-column lines, double quotes) is the single source of formatting truth, so never hand-format against it.

**Errors.** Failures are typed sentinel errors compared by identity. Asynchronous APIs reject with them, lifecycle hooks throw, and synchronous hot-path sends return `Error | null`. New APIs follow the same split.

**Comments.** Comments document behavior and constraints: what the code guarantees, what would break if changed. They never narrate the obvious, record history, or point at where an idea came from. Do not use em-dashes in comments or docs; use commas, colons, semicolons, or parentheses. Every source file starts with the MIT license header used across `src/`.

**Layout.** Leave a blank line after a statement block's closing brace before the next statement.

**Scope.** Keep changes surgical: touch what the change needs and nothing adjacent. Refactors ride in their own PRs, not inside features.

## Performance-sensitive code

The send paths (`tell`, `ask`, mailbox enqueue and drain) are measured in tens of millions of messages per second, where an innocent-looking change can cost double-digit percentages.

- Profile before optimizing; do not optimize on intuition.
- Any PR touching a hot path runs the benchmarks before and after (`make bench-baseline` at minimum) and quotes both numbers with the machine line in the PR description.
- A regression outside the reported noise band needs a justification or a fix before merge.

## Tests

Every behavior change comes with tests: a bug fix adds the test that fails without it, a feature tests its contract, including the failure paths. Tests live in `test/` and run single-threaded and deterministic; anything timing-dependent must not flake under a slow CI runner.

## Commits and pull requests

Commit messages and PR titles follow Conventional Commits and are enforced by CI:

```
<type>(optional scope): <description>

types: feat fix docs test refactor perf ci chore revert
```

Examples: `feat: add priority mailbox`, `fix(supervision): reset retry budget on success`.

Keep a PR to one logical change. The CI gate is typecheck, lint, and the test matrix on Node 22, 24, and 26; all of it must be green.

## Changesets

User-visible changes (features, fixes, behavior or API changes) include a changeset so the release notes write themselves:

```sh
pnpm changeset
```

Pick the bump (`patch` for fixes, `minor` for features; `major` is a maintainer decision) and write the summary for a reader of the changelog, not for the reviewer. Internal-only changes (docs, CI, refactors with no observable effect) do not need one.

Releases are cut by maintainers: every green push to `main` publishes a `nightly` dist-tag build automatically, and a stable release happens when a version tag is pushed, which folds the accumulated changesets into `CHANGELOG.md`.
