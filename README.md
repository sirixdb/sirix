<p align="center"><img src="https://raw.githubusercontent.com/sirixdb/sirix/master/Circuit Technology Logo.png"/></p>

<h1 align="center">SirixDB</h1>
<h3 align="center">Query any revision as fast as the latest</h3>

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

## The Problem

You update a row in your database. The old value is gone.

To get history, you bolt on audit tables, change-data-capture, or event sourcing. Now you have two systems: one for current state, one for history. Querying the past means replaying events or scanning logs. Your "simple" audit requirement just became an infrastructure project.

Git solves this for files—but you can't query a Git repository. Event sourcing preserves history—but reconstructing past state means replaying from the beginning.

## The Solution

SirixDB is a database where **every revision is a first-class citizen**. Not an afterthought. Not a log you replay.

```java
// Query revision 1 - instant, not reconstructed
session.beginNodeReadOnlyTrx(1)

// Query by timestamp - which revision was current at 3am last Tuesday?
session.beginNodeReadOnlyTrx(Instant.parse("2024-01-15T03:00:00Z"))

// Both return the same thing: a readable snapshot, as fast as querying "now"
```

This works because SirixDB uses **structural sharing**: when you modify data, only changed pages are written. Unchanged data is shared between revisions via copy-on-write. Revision 1000 doesn't store 1000 copies—it stores the current state plus pointers to shared history.

**The result:**
- Storage: O(changes per revision), not O(total size × revisions)
- Read any page from any revision: O(N) page fragment reads, where N is the configurable snapshot window (default 3)
- No event replay, no log scanning—direct page access

## Bitemporal: Two Kinds of Time

Most databases (if they version at all) track one timeline: when data was written. SirixDB tracks two:

- **Transaction time**: When was this committed? (system-managed)
- **Valid time**: When was this true in the real world? (user-managed)

Why does this matter?

```
January 15: You record "Price = $100, valid from January 1"
January 20: You discover the price was actually $95 on January 1

After correction, you can ask:
  "What did we THINK the price was on Jan 16?"  →  $100 (transaction time)
  "What WAS the price on Jan 1?"                →  $95  (valid time)
```

Both questions have correct, different answers. Without bitemporal support, the correction destroys the audit trail.

## Core Properties

- **Append-only storage**: Data is never overwritten. New revisions write to new locations.
- **Structural sharing**: Unchanged pages and nodes are referenced between revisions via copy-on-write.
- **Snapshot isolation**: Readers see a consistent view; one writer per resource.
- **Embeddable**: Single JAR, no external dependencies. Or run as REST server.

## How Versioning Works

SirixDB stores data in a persistent tree structure where revisions share unchanged pages and nodes. Traditional databases overwrite data in place and use write-ahead logs for recovery. SirixDB takes a different approach:

### Physical Storage: Append-Only Log

All data is written sequentially to an append-only log. Nothing is ever overwritten.

```
Physical Log (append-only, sequential writes)
┌────────────────────────────────────────────────────────────────────────┐
│ [R1:Root] [R1:P1] [R1:P2] [R2:Root] [R2:P1'] [R3:Root] [R3:P2'] ...   │
└────────────────────────────────────────────────────────────────────────┘
     t=0      t=1     t=2      t=3      t=4       t=5       t=6    → time
```

### Logical Structure: Persistent Trie

Each revision has a root node in a trie. Unchanged pages are shared via references.

```
Revision Roots                    Page Trie (persistent, copy-on-write)
      │
      ▼
   [Rev 3] ─────────────────┬─────────────────┐
      │                     │                 │
   [Rev 2] ────────┬────────┤                 │
      │            │        │                 │
   [Rev 1] ───┐    │        │                 │
              │    │        │                 │
              ▼    ▼        ▼                 ▼
           [Root₁][Root₂][Root₃]          [Pages...]
              │      │      │
              ▼      ▼      ▼
            ┌────────────────────────────────────────┐
            │           Shared Page Pool             │
            │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐      │
            │  │ P1  │ │ P1' │ │ P2  │ │ P2' │ ...  │
            │  └──▲──┘ └──▲──┘ └──▲──┘ └──▲──┘      │
            │     │      │       │       │          │
            │   R1,R2    R3    R1,R3    R2          │
            │  (shared)       (shared)              │
            └────────────────────────────────────────┘
```

### Page Versioning Strategies

SirixDB supports multiple strategies for storing page versions, configurable per resource:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ FULL: Each page stores complete data                                    │
│                                                                         │
│   Rev1: [████████]  Rev2: [████████]  Rev3: [████████]                  │
│         (full)            (full)            (full)                      │
│                                                                         │
│   + Fast reads (no reconstruction)                                      │
│   - High storage cost                                                   │
├─────────────────────────────────────────────────────────────────────────┤
│ INCREMENTAL: Each page stores diff from previous revision               │
│                                                                         │
│   Rev1: [████████]  Rev2: [Δ←1]  Rev3: [Δ←2]  Rev4: [Δ←3]               │
│         (full)       (diff)       (diff)       (diff)                   │
│                                                                         │
│   + Minimal storage                                                     │
│   - Read cost grows: Rev4 = apply(Δ3, apply(Δ2, apply(Δ1, Rev1)))       │
├─────────────────────────────────────────────────────────────────────────┤
│ DIFFERENTIAL: Each page stores diff from a reference snapshot           │
│                                                                         │
│   Rev1: [████████]  Rev2: [Δ←1]  Rev3: [Δ←1]  Rev4: [Δ←1]               │
│         (full)       (diff)       (diff)       (diff)                   │
│                                                                         │
│   + Bounded read cost (max 1 diff to apply)                             │
│   - Diffs grow larger over time                                         │
├─────────────────────────────────────────────────────────────────────────┤
│ SLIDING SNAPSHOT: Periodic full snapshots + incremental diffs           │
│                                                                         │
│   Rev1: [████████]  Rev2: [Δ←1]  Rev3: [Δ←2]  Rev4: [████████]  Rev5:   │
│         (full)       (diff)       (diff)       (full)           [Δ←4]   │
│         ◄──────── window N=3 ────────►        ◄──── window ────►        │
│                                                                         │
│   + Bounded read cost (max N diffs)                                     │
│   + Bounded diff size (reset at each snapshot)                          │
│   = Best balance of storage vs read performance                         │
└─────────────────────────────────────────────────────────────────────────┘
```

When you modify data:
1. Only the affected pages are copied and modified (copy-on-write)
2. Unchanged pages are referenced from the new revision
3. The old revision remains intact and queryable

**Storage cost**: O(changed pages) per revision, not O(total document size).

**Read performance**: Opening a revision is O(1) by revision number or O(log R) by timestamp (binary search over R revisions). Each page read requires combining at most N page fragments, where N is the snapshot window size (configurable, default 3). Tree traversal to locate a node is O(log nodes), same as querying the latest revision.

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

The `-l` option specifies the database path. Each database can contain multiple resources.

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
# The context is set to the document root, so access fields directly
sirix-cli -l /tmp/mydb query -r myresource '.name'
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

    // Insert JSON data (creates revision 1)
    try (var session = database.beginResourceSession("myresource");
         var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"key\": \"value\"}"));
        wtx.commit();
    }

    // Update creates revision 2 (revision 1 remains unchanged)
    try (var session = database.beginResourceSession("myresource");
         var wtx = session.beginNodeTrx()) {
        wtx.moveTo(2);  // Move to the "key" node
        wtx.setStringValue("updated value");
        wtx.commit();
    }

    // Read from revision 1 - still accessible
    try (var session = database.beginResourceSession("myresource");
         var rtx = session.beginNodeReadOnlyTrx(1)) {
        rtx.moveTo(2);
        System.out.println(rtx.getValue());  // Prints: value
    }
}
```

## Time-Travel Queries

SirixDB extends JSONiq/XQuery (via [Brackit](https://github.com/sirixdb/brackit)) with temporal axis and functions.

### Access by Revision Number or Timestamp

```xquery
(: Open specific revision :)
jn:doc('mydb','myresource', 5)

(: Open by timestamp - returns revision valid at that instant :)
jn:open('mydb','myresource', xs:dateTime('2024-01-15T10:30:00Z'))
```

### Temporal Axis Functions

Navigate a node's history across revisions:

```xquery
(: Single-step navigation :)
jn:previous($node)       (: same node in the previous revision :)
jn:next($node)           (: same node in the next revision :)

(: Boundary access :)
jn:first($node)          (: node in the first revision :)
jn:last($node)           (: node in the most recent revision :)
jn:first-existing($node) (: revision where this node first appeared :)
jn:last-existing($node)  (: revision where this node last existed :)

(: Range navigation - returns sequences :)
jn:past($node)           (: node in all past revisions :)
jn:future($node)         (: node in all future revisions :)
jn:all-times($node)      (: node across all revisions :)

(: With includeSelf parameter :)
jn:past($node, true())   (: include current revision :)
jn:future($node, true()) (: include current revision :)
```

Example: iterate through all versions of a node:
```xquery
for $version in jn:all-times(jn:doc('mydb','myresource').users[0])
return {"rev": sdb:revision($version), "data": $version}
```

### Diff Between Revisions

```xquery
(: Structured diff between any two revisions :)
jn:diff('mydb','myresource', 1, 5)

(: Diff with optional parameters: startNodeKey, maxLevel :)
jn:diff('mydb','myresource', 1, 5, $nodeKey, 3)
```

For adjacent revisions, `jn:diff` reads directly from stored change tracking files. For non-adjacent revisions it computes the diff.

If hashes are enabled, you can also detect changes via hash comparison:
```xquery
(: Find which revisions changed a specific node - requires hashes enabled :)
let $node := jn:doc('mydb','myresource').config
for $v in jn:all-times($node)
let $prev := jn:previous($v)
where empty($prev) or sdb:hash($v) ne sdb:hash($prev)
return sdb:revision($v)
```

### Bitemporal Queries

Query both time dimensions (see [Bitemporal: Two Kinds of Time](#bitemporal-two-kinds-of-time) above for why this matters).

#### Configuring Valid Time Support

Configure a resource with valid time paths to enable automatic indexing and dedicated query functions:

```java
// Configure resource with valid time paths
var resourceConfig = ResourceConfiguration.newBuilder("employees")
    .validTimePaths("validFrom", "validTo")  // specify your JSON field names
    .buildPathSummary(true)
    .build();

database.createResource(resourceConfig);

// Or use conventional field names (_validFrom, _validTo)
var resourceConfig = ResourceConfiguration.newBuilder("employees")
    .useConventionalValidTimePaths()
    .build();
```

Via REST API, use query parameters when creating a resource:

```bash
# Custom valid time field names
curl -X PUT "https://localhost:9443/database/resource?validFromPath=validFrom&validToPath=validTo" \
  -H "Content-Type: application/json" \
  -d '[{"name": "Alice", "validFrom": "2024-01-01T00:00:00Z", "validTo": "2024-12-31T23:59:59Z"}]'

# Use conventional _validFrom/_validTo fields
curl -X PUT "https://localhost:9443/database/resource?useConventionalValidTime=true" \
  -H "Content-Type: application/json" \
  -d '[{"name": "Bob", "_validFrom": "2024-01-01T00:00:00Z", "_validTo": "2024-12-31T23:59:59Z"}]'
```

When valid time paths are configured, SirixDB automatically creates CAS indexes on the valid time fields for optimal query performance.

#### Valid Time Query Functions

```xquery
(: Get records valid at a specific point in time :)
jn:valid-at('mydb','myresource', xs:dateTime('2024-07-15T12:00:00Z'))

(: True bitemporal query: combine transaction time and valid time :)
(: "What records were known on Jan 20 and valid on July 15?" :)
jn:open-bitemporal('mydb','myresource',
    xs:dateTime('2024-01-20T10:00:00Z'),   (: transaction time - opens revision :)
    xs:dateTime('2024-07-15T12:00:00Z'))   (: valid time - filters via index :)

(: Extract valid time bounds from a node :)
let $record := jn:doc('mydb','myresource')[0]
return {
  "validFrom": sdb:valid-from($record),
  "validTo": sdb:valid-to($record)
}
```

#### Transaction Time Functions

```xquery
(: Transaction time: what did the database look like at a point in time? :)
jn:open('mydb','myresource', xs:dateTime('2024-01-15T10:30:00Z'))

(: Get the commit timestamp of current revision :)
sdb:timestamp(jn:doc('mydb','myresource'))

(: Open all revisions within a transaction time range :)
jn:open-revisions('mydb','myresource',
        xs:dateTime('2024-01-01T00:00:00Z'),
        xs:dateTime('2024-06-01T00:00:00Z'))
```

### Revision Metadata Functions

```xquery
(: Get revision number and timestamp :)
sdb:revision($node)              (: revision number of this node :)
sdb:timestamp($node)             (: commit timestamp as xs:dateTime :)
sdb:most-recent-revision($node)  (: latest revision number in resource :)

(: Get history of changes to a specific node :)
sdb:item-history($node)          (: all revisions where this node changed :)
sdb:is-deleted($node)            (: true if node was deleted in a later revision :)

(: Author tracking (if set during commit) :)
sdb:author-name($node)
sdb:author-id($node)

(: Commit with metadata :)
sdb:commit($doc)
sdb:commit($doc, "commit message")
sdb:commit($doc, "commit message", xs:dateTime('2024-01-15T10:30:00Z'))
```

### Merkle Hash Verification (Optional)

When enabled in resource configuration, SirixDB stores a hash for each node computed from its content and descendants. Use this for:
- Tamper detection
- Efficient change detection (compare subtree hashes instead of traversing)
- Data integrity verification

```xquery
sdb:hash(jn:doc('mydb','myresource'))           (: root hash :)
sdb:hash(jn:doc('mydb','myresource').users[0])  (: subtree hash :)
```

See [Query documentation](https://sirix.io/docs/jsoniq.html) for the full API.

## Web Interface

The [SirixDB Web GUI](https://github.com/sirixdb/sirixdb-web-gui) provides visualization of revision history and diffs:

```bash
git clone https://github.com/sirixdb/sirixdb-web-gui.git
cd sirixdb-web-gui
docker compose -f docker-compose.demo.yml up
```

Open `http://localhost:3000` (login: `admin`/`admin`)

## Architecture

### Storage Model

```
Database (directory)
└── Resource (single JSON or XML document with revision history)
    └── Revisions (numbered 1, 2, 3, ...)
        └── Pages (variable-size blocks containing node data)
```

- **Database**: Directory containing multiple resources
- **Resource**: One logical document with its complete revision history
- **Page**: Unit of I/O and versioning. Variable-size, immutable once written.

### Key Design Decisions

| Aspect | Design | Trade-off |
|--------|--------|-----------|
| Write pattern | Append-only | No in-place updates; simpler recovery; larger storage footprint |
| Consistency | Single writer per resource | No write conflicts; readers never blocked |
| Index updates | Synchronous | Queries always see current indexes |
| Node IDs | Stable across revisions | Enables tracking node identity through time |

### Indexes

- **Path index**: Index specific JSON paths for faster navigation
- **CAS index** (Content-and-Structure): Index values with type awareness
- **Name index**: Index object keys

## Comparison with Alternatives

| Feature | SirixDB | Postgres + Audit | Git + JSON | Event Sourcing | Datomic |
|---------|---------|------------------|------------|----------------|---------|
| Query past state | Direct page access | Scan audit log | Checkout + parse | Replay events | Direct segment access |
| Storage overhead | O(changes) | O(all writes) | O(file × revs) | O(all events) | O(changes) |
| Granularity | Node-level | Row-level | File-level | Event-level | Fact-level |
| Bitemporal | Built-in | Manual | No | Manual | Built-in |
| Embeddable | Yes | No | Yes | Varies | No |
| Query language | JSONiq/XQuery | SQL | None | Varies | Datalog |

## Building from Source

```bash
git clone https://github.com/sirixdb/sirix.git
cd sirix
./gradlew build -x test
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
├── sirix-core/          # Core storage engine and versioning
├── sirix-query/         # Brackit JSONiq/XQuery integration + sirix-shell
├── sirix-kotlin-cli/    # Command-line interface (sirix-cli)
├── sirix-rest-api/      # Vert.x REST server
└── sirix-xquery/        # XQuery support for XML
```

## Use Cases

- **Audit trails**: Regulatory requirements for complete data history (finance, healthcare)
- **Document versioning**: Track changes to configuration, contracts, or content
- **Debugging**: Query production state at the time a bug occurred
- **Temporal analytics**: Analyze how data evolved over time windows
- **Undo/restore**: Revert to or query any historical state

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
