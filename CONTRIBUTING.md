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

### Requirements

- Java 25+ (with `--enable-preview`)
- Gradle 9.1+ (or use the included wrapper)
- GraalVM (optional, for native image builds)

## Development Workflow

1. Create a branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Make your changes
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

## Code Style

- **No star imports** — use explicit imports for every type
- **No inline fully-qualified class names** — import at the top of the file
- Use `final` for fields and local variables where possible
- Prefer primitive types over boxed types in performance-critical code
- Pre-size collections when the size is known
- Minimize object allocations in hot paths
- Use `StringBuilder` for string concatenation in loops

The project uses [Spotless](https://github.com/diffplug/spotless) for formatting. Run `./gradlew spotlessApply` before committing.

## Project Structure

```
bundles/
├── sirix-core/          # Core storage engine and versioning
├── sirix-query/         # Brackit JSONiq/XQuery integration + sirix-shell
├── sirix-kotlin-cli/    # Command-line interface (sirix-cli)
├── sirix-rest-api/      # Vert.x REST server
└── sirix-xquery/        # XQuery support for XML
```

## What to Contribute

- **Bug fixes** — check [open issues](https://github.com/sirixdb/sirix/issues) labeled `bug`
- **Documentation** — improvements to docs, examples, or inline comments
- **Tests** — additional test coverage is always welcome
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
