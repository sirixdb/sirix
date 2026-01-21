<p align="center"><img src="https://raw.githubusercontent.com/sirixdb/sirix/master/Circuit Technology Logo.png"/></p>

<h1 align="center">SirixDB</h1>
<h3 align="center">The database that remembers everything</h3>

<p align="center">
Every commit creates an efficient snapshot. Query any point in time. Diff any two revisions. Built for auditability.
</p>

<p align="center">
    <a href="https://github.com/sirixdb/sirix/actions"><img src="https://github.com/sirixdb/sirix/workflows/Java%20CI%20with%20Gradle/badge.svg" alt="CI Build Status"/></a>
    <a href="https://search.maven.org/search?q=g:io.sirix"><img src="https://img.shields.io/maven-central/v/io.sirix/sirix-core.svg" alt="Maven Central"/></a>
    <a href="http://makeapullrequest.com"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square" alt="PRs Welcome"/></a>
    <a href="#contributors-"><img src="https://img.shields.io/badge/all_contributors-23-orange.svg?style=flat-square" alt="All Contributors"/></a>
</p>

<p align="center">
<a href="https://sirix.io/docs/index.html"><b>Documentation</b></a> · <a href="https://discord.gg/yC33wVpv7t"><b>Discord</b></a> · <a href="https://sirix.discourse.group/"><b>Forum</b></a> · <a href="https://github.com/sirixdb/sirixdb-web-gui"><b>Web UI</b></a>
</p>

---

## Why SirixDB?

Most databases overwrite your data. When you update a record, the old value is gone. SirixDB takes a different approach: **every change creates a new revision**, and old revisions remain queryable forever.

This isn't just version control for your data—it's a fundamental rethinking of how databases should work:

- **Time-travel queries**: Query your data as it existed at any point in time
- **Efficient storage**: Revisions share unchanged data through copy-on-write semantics
- **Built-in audit trail**: Know exactly what changed, when, and reconstruct any historical state
- **Cryptographic integrity**: Merkle hash trees let you verify data hasn't been tampered with

<!-- TODO: Add architecture diagram showing how revisions share data -->

## Quick Start

### Using the CLI (Native Binaries)

SirixDB provides two CLI tools, both available as instant-startup native binaries:

| Binary | Module | Description |
|--------|--------|-------------|
| `sirix-cli` | sirix-kotlin-cli | Full-featured CLI for database operations |
| `sirix-shell` | sirix-query | Interactive JSONiq/XQuery shell |

Build native binaries with GraalVM:

```bash
# Build both CLIs as native binaries (requires GraalVM with native-image)
./gradlew :sirix-kotlin-cli:nativeCompile  # produces: sirix-cli
./gradlew :sirix-query:nativeCompile       # produces: sirix-shell

# Or run via JAR
./gradlew :sirix-kotlin-cli:run --args="-l /tmp/mydb create"
```

#### sirix-cli: Database Operations

**Create a database and store JSON:**
```bash
sirix-cli -l /tmp/mydb create json -r myresource -d '{"name": "Alice", "role": "admin"}'
```

**Query your data:**
```bash
sirix-cli -l /tmp/mydb query -r myresource
```

**Run a JSONiq query:**
```bash
sirix-cli -l /tmp/mydb query -r myresource 'jn:doc("mydb","myresource").name'
```

**Update and create a new revision:**
```bash
sirix-cli -l /tmp/mydb update -r myresource '{"role": "superadmin"}' -im as-first-child
```

**Query a previous revision:**
```bash
sirix-cli -l /tmp/mydb query -r myresource -rev 1
```

**View revision history:**
```bash
sirix-cli -l /tmp/mydb resource-history myresource
```

#### sirix-shell: Interactive Query Shell

The interactive shell provides a REPL for JSONiq/XQuery queries:

```bash
sirix-shell
> 1 + 1
2
> jn:store('mydb','resource','{"key": "value"}')
> jn:doc('mydb','resource').key
"value"
```

### Using the REST API

Start SirixDB with Docker:

```bash
git clone https://github.com/sirixdb/sirix.git
cd sirix
docker compose up
```

The REST API runs on `https://localhost:9443`. See [REST API documentation](https://sirix.io/docs/rest-api.html) for endpoints.

### As an Embedded Library

```xml
<dependency>
  <groupId>io.sirix</groupId>
  <artifactId>sirix-core</artifactId>
  <version>0.11.0-SNAPSHOT</version>
</dependency>
```

```java
var dbPath = Path.of("/tmp/mydb");

// Create database and resource
Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
try (var database = Databases.openJsonDatabase(dbPath)) {
    database.createResource(ResourceConfiguration.newBuilder("myresource").build());

    // Insert JSON data
    try (var session = database.beginResourceSession("myresource");
         var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"key\": \"value\"}"));
        wtx.commit();  // Creates revision 1
    }

    // Update and create revision 2
    try (var session = database.beginResourceSession("myresource");
         var wtx = session.beginNodeTrx()) {
        wtx.moveTo(2);  // Move to the "key" node
        wtx.setStringValue("updated value");
        wtx.commit();  // Creates revision 2
    }

    // Read from revision 1
    try (var session = database.beginResourceSession("myresource");
         var rtx = session.beginNodeReadOnlyTrx(1)) {  // Open revision 1
        rtx.moveTo(2);
        System.out.println(rtx.getValue());  // Prints: value
    }
}
```

## Time-Travel Queries with JSONiq

SirixDB uses [Brackit](https://github.com/sirixdb/brackit), a powerful JSONiq/XQuery processor with temporal extensions.

**Open a resource at a specific point in time:**
```xquery
jn:open('mydatabase','myresource', xs:dateTime('2024-01-15T10:30:00Z'))
```

**Find all changes to a node across revisions:**
```xquery
let $node := jn:doc('mydb','myresource').users[0]
for $version in jn:all-times($node)
where sdb:hash($version) ne sdb:hash(jn:previous($version))
return {"revision": sdb:revision($version), "data": $version}
```

**Diff between any two revisions:**
```xquery
jn:diff('mydb','myresource', 1, 5)
```

**Verify data integrity with Merkle hashes:**
```xquery
sdb:hash(jn:doc('mydb','myresource'))
```

See [Query documentation](https://sirix.io/docs/jsoniq.html) for the full JSONiq API.

## Web Interface

<!-- TODO: Add screenshot of the web UI showing revision history and diff viewer -->

The [SirixDB Web GUI](https://github.com/sirixdb/sirixdb-web-gui) provides:
- **Database Explorer**: Browse databases, resources, and navigate JSON/XML document trees
- **Query Editor**: Write and execute XQuery/JSONiq with Monaco Editor and syntax highlighting
- **Revision History**: Interactive timeline with diff viewer to explore version history
- **Modern Dark UI**: IDE-inspired interface designed for developers

**Try it locally:**
```bash
git clone https://github.com/sirixdb/sirixdb-web-gui.git
cd sirixdb-web-gui
docker compose -f docker-compose.demo.yml up
```
Then open `http://localhost:3000` (login: `admin`/`admin`)

Built with SolidJS, TypeScript, Tailwind CSS, and Monaco Editor.

## How It Works

SirixDB stores data in a persistent tree structure where revisions share unchanged nodes:

<!-- TODO: Add diagram showing copy-on-write and page sharing between revisions -->

**Key concepts:**

- **Copy-on-write**: Modified pages are written to new locations; unchanged pages are shared
- **No WAL needed**: Append-only writes ensure consistency without write-ahead logging
- **Page-level versioning**: Only changed page fragments are stored, not entire pages
- **Sliding snapshot algorithm**: Balances read performance with storage efficiency

This design means:
- Creating a new revision is O(changed nodes), not O(total nodes)
- Storage grows with changes, not with time
- Any revision can be read in O(log revisions) time

## Features

| Feature | Description |
|---------|-------------|
| **Bitemporal** | Track both valid time and transaction time |
| **Embeddable** | Use as a library (like SQLite) or via REST API |
| **JSON & XML** | Native support for both semi-structured formats |
| **Secondary indexes** | Path indexes, CAS indexes, name indexes |
| **Concurrent** | Multiple readers, single writer per resource |
| **Cryptographic hashes** | Merkle trees for tamper detection |
| **GraalVM native** | Compile to native binaries for instant startup |

## Use Cases

- **Audit logs**: Regulatory compliance requiring full history
- **Event sourcing**: Natural fit for append-only event stores
- **Document versioning**: Track changes to JSON/XML documents
- **Time-series with context**: Store data with full document history
- **Undo/redo**: Instant rollback to any previous state

## Documentation

- [Getting Started Guide](https://sirix.io/docs/index.html)
- [REST API Reference](https://sirix.io/docs/rest-api.html)
- [JSONiq Query Language](https://sirix.io/docs/jsoniq.html)
- [Architecture & Concepts](https://sirix.io/docs/concepts.html)

## Building from Source

```bash
git clone https://github.com/sirixdb/sirix.git
cd sirix
gradle build -x test
```

**Requirements:**
- Java 25+
- Gradle 9.1+ (or use included wrapper)

**JVM flags** (required for running):
```
--enable-preview
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
```

**Build native binaries** (requires GraalVM):
```bash
./gradlew :sirix-kotlin-cli:nativeCompile  # sirix-cli
./gradlew :sirix-query:nativeCompile       # sirix-shell
./gradlew :sirix-rest-api:nativeCompile    # REST API server
```

## Project Structure

```
bundles/
├── sirix-core/          # Core storage engine
├── sirix-query/         # Brackit JSONiq/XQuery integration + interactive shell (sirix-shell)
├── sirix-kotlin-cli/    # Command-line interface (sirix-cli)
├── sirix-rest-api/      # Vert.x REST server
└── sirix-xquery/        # XQuery support for XML
```

## Community

- **[Discord](https://discord.gg/yC33wVpv7t)** - Quick questions and chat
- **[Forum](https://sirix.discourse.group/)** - Discussions and support
- **[GitHub Issues](https://github.com/sirixdb/sirix/issues)** - Bug reports and features

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Contributors

SirixDB is maintained by Johannes Lichtenberger and the open source community.

The project originated from Treetank, a university research project by Dr. Marc Kramis, Dr. Sebastian Graf and many students.

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://github.com/yiss"><img src="https://avatars1.githubusercontent.com/u/12660796?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ilias YAHIA</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=yiss" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/BirokratskaZila"><img src="https://avatars1.githubusercontent.com/u/24469472?v=4?s=100" width="100px;" alt=""/><br /><sub><b>BirokratskaZila</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=BirokratskaZila" title="Documentation">📖</a></td>
    <td align="center"><a href="https://mrbuggysan.github.io/"><img src="https://avatars0.githubusercontent.com/u/9119360?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Andrei Buiza</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=MrBuggySan" title="Code">💻</a></td>
    <td align="center"><a href="https://www.linkedin.com/in/dmytro-bondar-330804103/"><img src="https://avatars0.githubusercontent.com/u/11942950?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Bondar Dmytro</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=Loniks" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/santoshkumarkannur"><img src="https://avatars3.githubusercontent.com/u/56201023?v=4?s=100" width="100px;" alt=""/><br /><sub><b>santoshkumarkannur</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=santoshkumarkannur" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/LarsEckart"><img src="https://avatars1.githubusercontent.com/u/4414802?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Lars Eckart</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=LarsEckart" title="Code">💻</a></td>
    <td align="center"><a href="http://www.hackingpalace.net"><img src="https://avatars1.githubusercontent.com/u/6793260?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jayadeep K M</b></sub></a><br /><a href="#projectManagement-kmjayadeep" title="Project Management">📆</a></td>
  </tr>
  <tr>
    <td align="center"><a href="http://keithkim.org"><img src="https://avatars0.githubusercontent.com/u/318225?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Keith Kim</b></sub></a><br /><a href="#design-karmakaze" title="Design">🎨</a></td>
    <td align="center"><a href="https://github.com/theodesp"><img src="https://avatars0.githubusercontent.com/u/328805?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Theofanis Despoudis</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=theodesp" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/Mrexsp"><img src="https://avatars3.githubusercontent.com/u/23698645?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Mario Iglesias Alarcón</b></sub></a><br /><a href="#design-Mrexsp" title="Design">🎨</a></td>
    <td align="center"><a href="https://twitter.com/_anmonteiro"><img src="https://avatars2.githubusercontent.com/u/661909?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Antonio Nuno Monteiro</b></sub></a><br /><a href="#projectManagement-anmonteiro" title="Project Management">📆</a></td>
    <td align="center"><a href="http://fultonbrowne.github.io"><img src="https://avatars1.githubusercontent.com/u/50185337?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Fulton Browne</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=FultonBrowne" title="Documentation">📖</a></td>
    <td align="center"><a href="https://twitter.com/felixrabe"><img src="https://avatars3.githubusercontent.com/u/400795?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Felix Rabe</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=felixrabe" title="Documentation">📖</a></td>
    <td align="center"><a href="https://twitter.com/ELWillis10"><img src="https://avatars3.githubusercontent.com/u/182492?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ethan Willis</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=ethanwillis" title="Documentation">📖</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/bark"><img src="https://avatars1.githubusercontent.com/u/223964?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Erik Axelsson</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=bark" title="Code">💻</a></td>
    <td align="center"><a href="https://se.rg.io/"><img src="https://avatars1.githubusercontent.com/u/976915?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Sérgio Batista</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=batista" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/chaensel"><img src="https://avatars2.githubusercontent.com/u/2786041?v=4?s=100" width="100px;" alt=""/><br /><sub><b>chaensel</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=chaensel" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/balajiv113"><img src="https://avatars1.githubusercontent.com/u/13016475?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Balaji Vijayakumar</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=balajiv113" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/FernandaCG"><img src="https://avatars3.githubusercontent.com/u/28972973?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Fernanda Campos</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=FernandaCG" title="Code">💻</a></td>
    <td align="center"><a href="https://joellau.github.io/"><img src="https://avatars3.githubusercontent.com/u/29514264?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Joel Lau</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=JoelLau" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/add09"><img src="https://avatars3.githubusercontent.com/u/38160880?v=4?s=100" width="100px;" alt=""/><br /><sub><b>add09</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=add09" title="Code">💻</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/EmilGedda"><img src="https://avatars2.githubusercontent.com/u/4695818?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Emil Gedda</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=EmilGedda" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/arohlen"><img src="https://avatars1.githubusercontent.com/u/49123208?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Andreas Rohlén</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=arohlen" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/marcinbieleckiLLL"><img src="https://avatars3.githubusercontent.com/u/26444765?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Marcin Bielecki</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=marcinbieleckiLLL" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/ManfredNentwig"><img src="https://avatars1.githubusercontent.com/u/164948?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Manfred Nentwig</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=ManfredNentwig" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/Raj-Datta-Manohar"><img src="https://avatars0.githubusercontent.com/u/25588557?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Raj</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=Raj-Datta-Manohar" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/mosheduminer"><img src="https://avatars.githubusercontent.com/u/47164590?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Moshe Uminer</b></sub></a><br /><a href="https://github.com/sirixdb/sirix/commits?author=mosheduminer" title="Code">💻</a></td>
  </tr>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

## Sponsors

Support SirixDB development on [Open Collective](https://opencollective.com/sirixdb/).

## License

[BSD 3-Clause License](LICENSE)
