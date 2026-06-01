# SirixDB Examples

Small, self-contained programs that show how to use the embedded SirixDB API. They write their
data to `~/sirix-data/` and clean up previous runs automatically.

| Example | What it shows |
|---------|---------------|
| [`ResourceTransactionUsage`](src/main/java/io/sirix/examples/ResourceTransactionUsage.java) | Create an XML database and resource, shred an XML document into it, move a subtree, commit, and serialize the result. |
| [`QueryUsage`](src/main/java/io/sirix/examples/QueryUsage.java) | Build a sample document and run JSONiq/XQuery queries against it via [Brackit](https://github.com/sirixdb/brackit), including secondary indexes. |

## Running

These are plain `main` methods, so the simplest way to run them is from your IDE (right-click →
Run). The Gradle project name is `:sirix-example`.

> **Prerequisite for `ResourceTransactionUsage`:** it reads `~/sirix-data/input.xml`. Drop any
> well-formed XML file there first, e.g.:
> ```bash
> mkdir -p ~/sirix-data
> echo '<root><a>1</a><b>2</b></root>' > ~/sirix-data/input.xml
> ```

For a zero-setup, copy-pasteable starting point, see the **As an Embedded Library** section of the
[root README](../../README.md#as-an-embedded-library).
