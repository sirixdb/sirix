# Contributing to SirixDB

Thank you for your interest in contributing to SirixDB! This guide will help you get started.

## Getting Started

1. **Fork** the repository on GitHub
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/<your-username>/sirix.git
   cd sirix
   ```
3. **Build** the project:
   ```bash
   ./gradlew build -x test
   ```

By default, `io.sirix` SNAPSHOT dependencies are resolved from the Sonatype
snapshot repository. If you are developing against a locally installed Sirix
or Brackit snapshot, explicitly opt in to the local Maven repository:

```bash
./gradlew -PuseMavenLocal=true build -x test
```

The opt-in only enables local artifacts in the `io.sirix` group; without the
property, artifacts in `~/.m2` are not considered.

### Requirements

- Java 25+ (with `--enable-preview`)
- Gradle 9.1+ (or use the included wrapper)
- GraalVM (optional, for native image builds)

### Common tasks

A `justfile` in the repo root wraps the commands below for convenience. It is
optional — every task is a plain `./gradlew` invocation you can run directly.
With [`just`](https://github.com/casey/just) installed, `just --list` shows them:

| Recipe | Underlying command |
| --- | --- |
| `just toolchains` | `./gradlew -q javaToolchains` — confirm a JDK 25 is detected |
| `just build` | `./gradlew build -x test` |
| `just test` | `./gradlew test` |
| `just format` | `./gradlew spotlessApply` |
| `just bench <Class>` | `./gradlew :sirix-benchmarks:jmh -Pjmh.includes=<Class>` |
| `just scale "<args>"` | `./gradlew :sirix-benchmarks:runScale -Pscale.args="<args>"` |

Benchmarks live in `sirix-benchmarks` (JMH). Override JMH `@Param` values with a
second argument, e.g. `just bench JsonWritePathBenchmark compressionPipeline=NONE`.

## Development Workflow

1. Create a branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Follow the [development strategy](docs/DEVELOPMENT.md) for the spec-first workflow and tests
3. Run tests:
   ```bash
   ./gradlew test
   ```
4. Format your code:
   ```bash
   ./gradlew spotlessApply
   ```
5. Commit and push your branch
6. Open a Pull Request against `main`

## Spec, mutation, exploration and learning

[How SirixDB is developed](docs/DEVELOPMENT.md) is the project's development strategy and explains
why each practice below exists: the specification as the source of truth, tests derived from the
requirements rather than from the code, behaviour-only tests, mutation testing, exploratory testing,
review against the written intent, and the benchmark protocol. Read it once before your first
change. What it asks of every pull request:

- State behaviour, invariants and error cases and cite the specification's requirement IDs in the
  issue, then in the PR, following the
  [spec-first workflow](docs/DEVELOPMENT.md#1-the-specification-is-the-source-of-truth). Derive the
  tests from those requirements **before writing implementation code, or independently of it** (the
  [test derivation policy](docs/DEVELOPMENT.md#2-tests-are-derived-from-the-requirements-not-from-the-code)).
  Map each requirement to its test in the PR template; for documentation or CI changes, name the
  observable acceptance checks instead of inventing application tests.
- Mutation-test changed logic: inspect the PIT artifact and resolve every surviving or uncovered
  changed-line mutant under
  [CONV-103](spec/CONV-conventions.md#conv-103--specification-before-implementation) — a behaviour
  test, a reviewed disposition with its reason and evidence, or removing the code. Never write a
  test to kill a mutant. Scope, local commands, reviewed dispositions, test associations and the
  blocking switch are in [Verification](docs/VERIFICATION.md#pr-mutation-testing).
- Explore beyond the happy path using adversarial inputs, property-based tests or an independent
  differential oracle. Record the cases, seeds and outcomes in the PR; promote any discovered
  failure to a deterministic regression.
- Give every bug a root-cause [write-up](docs/postmortems/TEMPLATE.md), a regression test that fails
  before the fix, and an actionable lesson. Link the evidence; explain why the old tests missed it
  and how the new check would catch it.
- Add each changelog entry and each lesson as **one fragment file** — in
  [`changelog.d/`](changelog.d/README.md) and [`docs/lessons.d/`](docs/lessons.d/README.md) — rather
  than editing `CHANGELOG.md` or `docs/LESSONS.md`. A release collects the fragments into those
  files. Two pull requests then never touch the same lines, so merging one no longer forces the
  others to rebase and revalidate. A pull-request check enforces this; the convention, the collector
  and what the check allows are documented in [`changelog.d/README.md`](changelog.d/README.md).

## Code Style

- **No star imports** — use explicit imports for every type
- **No inline fully-qualified class names** — import at the top of the file
- Use `final` for fields and local variables where possible
- Prefer primitive types over boxed types in performance-critical code
- Pre-size collections when the size is known
- Minimize object allocations in hot paths
- Use `StringBuilder` for string concatenation in loops

The project uses [Spotless](https://github.com/diffplug/spotless) for formatting, and CI enforces `spotlessCheck`. The check is ratcheted from `origin/main`, so only files changed since `main` are checked — legacy formatting is never your problem. Run `./gradlew spotlessApply` before committing, or install the `pre-commit` hook from the repo root (install instructions are in its header).

## Project Structure

```
bundles/
├── sirix-core/          # Core storage engine and versioning
├── sirix-query/         # Brackit JSONiq/XQuery integration + sirix-shell
├── sirix-rest-api/      # Vert.x REST server
├── sirix-kotlin-cli/    # Command-line interface (sirix-cli)
├── sirix-kotlin-api/    # Kotlin coroutine-based API
├── sirix-mcp/           # Model Context Protocol server for AI agents
├── sirix-examples/      # Runnable usage examples
└── sirix-benchmarks/    # JMH and scale benchmarks
```

## What to Contribute

- **Bug fixes** — check [open issues](https://github.com/sirixdb/sirix/issues) labeled `bug`
- **Documentation** — improvements to docs, examples, or inline comments
- **Tests** — behaviour tests for requirements that have none; coverage is a diagnostic here, not a target ([why](docs/DEVELOPMENT.md#5-coverage-is-a-diagnostic-not-a-target))
- **Features** — discuss in an issue first before starting large features

## Pull Request Guidelines

- Keep PRs focused — one logical change per PR
- Include tests for new functionality
- Ensure all existing tests pass (`./gradlew test`)
- Run `./gradlew spotlessApply` before submitting
- Write a clear PR description explaining what and why

## Reporting Bugs

Use [GitHub Issues](https://github.com/sirixdb/sirix/issues) with the bug report template. Include:

- Steps to reproduce
- Expected vs actual behavior
- Java version and OS
- Stack trace (if applicable)

## Questions?

- **[Discord](https://discord.gg/yC33wVpv7t)** — quick questions and chat
- **[Forum](https://sirix.discourse.group/)** — longer discussions
- **[GitHub Issues](https://github.com/sirixdb/sirix/issues)** — bug reports and feature requests

## License

By contributing, you agree that your contributions will be licensed under the [BSD 3-Clause License](LICENSE).
