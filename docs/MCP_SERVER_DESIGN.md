# SirixDB MCP server

`bundles/sirix-mcp` implements an embedded, stdio MCP server using Java MCP SDK
**2.1.0** (Jackson 3). It exposes SirixDB's revision history, queries, structural
diffs, named revision labels, and opt-in mutations. The MCP API surface exists;
HTTP transport, MCP resources, and prompts remain future work.

## Build and launch

Use JDK 25. Build a distribution containing the launcher and its dependencies:

```sh
./gradlew :sirix-mcp:installDist
bundles/sirix-mcp/build/install/sirix-mcp/bin/sirix-mcp --database-path /path/to/data
```

The generated launcher supplies the vector module, native access, and module-open
flags. The ordinary jar is not a standalone fat jar. The server reads JSON-RPC
from stdin and writes protocol responses to stdout; diagnostics and default audit
records go to stderr.

Example MCP host configuration:

```json
{
  "mcpServers": {
    "sirixdb": {
      "command": "/path/to/sirix/bundles/sirix-mcp/build/install/sirix-mcp/bin/sirix-mcp",
      "args": ["--database-path", "/path/to/data"]
    }
  }
}
```

Read-only is the default. `--read-write` enables writes. Alternatively, pass
`--config /path/to/config.json`; this loads the configuration directly (CLI
read-write and database-path overrides do not apply to that file):

```json
{
  "name": "sirixdb-mcp",
  "version": "1.0.0",
  "transport": "stdio",
  "databasePath": "/path/to/data",
  "readOnly": true,
  "allowDatabases": ["products"],
  "denyDatabases": ["secrets"],
  "allowResources": {"products": ["catalog"]},
  "maxResultSize": 100,
  "maxStringValueLength": 4096,
  "sanitizeOutput": true,
  "confirmDestructive": true,
  "auditLog": true,
  "auditLogPath": null
}
```

The configuration is a flat object, without a `server` wrapper. Only stdio is
implemented; `transport` is currently descriptive. Result limits must be positive.
The configured database path contains database directories. The configured root
may itself use a symlink, but database and resource names must be real names.
Every tool rejects symlinked database names and resource aliases before storage
access, returning a structured error with `code: "aliased_name"`. The same rule
applies to names used inside queries. Database and resource listings omit aliases;
allowlists and denylists apply to the real names.

## Tools

Eight tools are always registered:

| Tool | Parameters and behavior |
| --- | --- |
| `sirix_list_databases` | Lists accessible JSON databases. |
| `sirix_list_resources` | `database`; filters resource names by access policy. |
| `sirix_resource_info` | `database`, `resource`; revision head and timestamps. |
| `sirix_query` | `database`, `query`, optional `resource`, one optional historical selector, optional `offset`. |
| `sirix_history` | `database`, `resource`, optional `count` (default 20). |
| `sirix_diff` | `database`, `resource`, optional revision/snapshot endpoints and `offset`. |
| `sirix_list_snapshots` | `database`, `resource`; label-to-revision map. |
| `sirix_create_snapshot` | `database`, `resource`, `name`, optional `revision` (default head). |

Six tools are registered only in read-write mode:

| Tool | Parameters and behavior |
| --- | --- |
| `sirix_create_resource` | `database`, `resource`, content `data`, optional `format` (`json` default or `xml`), `overwrite` (default false), `message`. |
| `sirix_insert` | `database`, `resource`, JSON text `data`, optional `message`. |
| `sirix_update` | `database`, `resource`, `nodeKey`, string `value`, optional `message`. |
| `sirix_delete` | `database`, `resource`, `nodeKey`, optional `message`. |
| `sirix_revert` | `database`, `resource`, exactly one of `revision`/`snapshot`, optional `expected_head`, `message`. |
| `sirix_delete_snapshot` | `database`, `resource`, `name`; removes the label only. |

`sirix_query` is strictly read-only in both server modes. Parsed update syntax and resolved function calls are checked during compilation,
before optimization, evaluation or context-item lookup. The check covers legacy
write functions whose library metadata does not mark them as updating. Updating
expressions, including `jn:store`, `xn:store`, database drops and `sdb:commit`,
return an error with structured `code: "updating_query"` and the explicit write
tool names. No query writer is acquired. `sirix_query` always advertises the
read-only annotation. Dynamic evaluation, function lookup and external module imports
are rejected at the same boundary; they cannot establish a statically read-only
query. Calls through inline and locally declared functions are checked
through their definitions; read-only functions and JSON field access remain supported. There is no `sirix_read_node` or `sirix_execute_update_query`
tool. MCP tool annotations are host hints; the server does not implement an interactive
confirmation exchange, even when `confirmDestructive` is configured.

## Historical queries

A query without a selector uses the current state. Supply **exactly one** of:

- `revision`: positive, readable revision number;
- `snapshot`: existing label for this database/resource;
- `timestamp`: ISO-8601 instant selecting the last revision at or before that time.

A historical selector requires `resource`. Conflicting selectors, invalid
revisions, timestamps before the first commit, unknown labels, and pruned
revisions return errors. A pruned selector returns structured `code:
"revision_pruned"`; it never falls back to a retained revision.

The selector binds the context item `$$` to the selected resource and revision.
It is not an access restriction: `jn:doc` without a revision reads the current
head, and explicit revision arguments, temporal axes and diffs may read other
revisions. Queries may also name other authorized resources and databases.
Read-only enforcement, resource allow/deny checks and alias refusal apply to all
of these reads. The response's `revision` field identifies the context binding,
not the provenance of every returned value. For example:

```json
{
  "database": "products",
  "resource": "catalog",
  "query": "$$.name",
  "snapshot": "before-change"
}
```

Database and resource restrictions apply to the actual resources opened during
execution, including resources named inside query text. The request policy checks
database/resource opens and forbids writes. Temporal reader tasks reuse an already
authorized session and may visit its other readable revisions.
Prefetch failures propagate rather than becoming successful empty results, and
failed prefetches drain and close their unconsumed readers. Ordinary embedded
callers have no bound MCP policy.

Data changes use `sirix_create_resource`, `sirix_insert`, `sirix_update`,
`sirix_delete`, or `sirix_revert`. Each checks write access and the exact resource
before acquiring a writer. Resource creation validates the content and database
format before creating storage. JSON accepts object, array, string, number,
boolean and null roots; scalar roots use the engine's primitive insertion methods.
It creates the database if absent, and JSON/XML
formats must match an existing database's type. A denied name leaves existing
resources intact. Existing resources return structured `code: "resource_exists"`
unless authorized `overwrite: true` is supplied. Overwrite replaces only that
resource's content in a new revision, preserving its history, labels and sibling
resources. XML document types and external entities are not supported.

## Bounded results

Query results are an envelope in MCP `structuredContent`, mirrored as JSON text:

```json
{
  "items": ["<database-content>\nAlice\n</database-content>"],
  "truncated": false,
  "revision": 1
}
```

`items` contains serialized query values as strings. Each array is one value,
including an empty array; nested arrays retain their structure. Historical responses
include `revision`. `maxResultSize` bounds the number of items in a page;
`maxStringValueLength` bounds each serialized item. Serialization stops at the
limit and discards an oversized value in full, preserving valid JSON. These
bounds apply with output sanitization disabled too. A truncated response includes
`continuation.offset` and a hint: repeat the same query and selector at that offset,
or project/split an oversized value. This applies in both server modes. Pin a
revision when paging across concurrent writes. Query operators may themselves need
to materialize input (for example, sorting); output bounding does not change those
execution semantics.

Sanitization wraps complete query values in data delimiters. Suspected prompt
injection produces a separate warning. Delimiters are defense in depth; they are
not an authorization mechanism or a guarantee about an LLM's interpretation.

Diffs require `1 <= from_revision < to_revision <= head`, with both revisions
readable. Each endpoint accepts a revision or a snapshot, exclusively. Defaults
compare the preceding revision with the head; a resource with only one revision
and no explicit endpoints reports that no default diff is available. Same and
reversed pairs are errors, consistent with REST and `jn:diff`.

Diff structured responses contain `database`, `resource`, `old-revision`, `new-revision`,
`diffs`, and `truncated`. With sanitization enabled,
the text view wraps the complete JSON document in data delimiters; structured
content retains the JSON object. The server retains at most
one page of change tuples and emits complete operations, with a cumulative
serialized-operation character budget. Inserted/replaced JSON fragments retain their `data` payloads when they fit;
subtree serialization stops at the character limit and discards an oversized
operation in full. The traversal can still inspect the full comparison. A continuation
contains a change offset and guidance; repeat the same revision pair. An operation
that exceeds the character budget is omitted in full with guidance to query a
smaller projection.

## Labels and guarded revert

A snapshot is a label pointing at an existing readable revision. It does not
create a new revision, pin history against retention, create a writable branch,
or merge data. Labels persist per database in `.sirix-mcp-snapshots.json`:

```json
{
  "catalog": {
    "before-change": 12
  }
}
```

Labels are keyed only by real database and resource names. Persisted entries for
resource aliases are ignored, without migration or merging into real-name labels.
The registry loads a database's persisted labels lazily before its first lookup,
list, creation, or deletion after startup. Creating a label validates the actual
revision first. Reloading and then creating a label preserves the older labels.
Malformed persisted labels fail without being overwritten on retry, and failed
persistence restores the previous in-memory label values. Every use of a historical label revalidates readability, since retention can
prune its target later. Removing a label leaves revision data intact.

A safe mutation workflow reads the resource head, labels the baseline, makes a
change, inspects a diff, and optionally reverts. `sirix_revert` acquires the writer
before checking `expected_head`. A mismatch returns an MCP error with structured
`code: "stale_head"`, `expectedHead`, and `previousHead`, without committing. A
successful revert creates a forward revision and returns `revision`,
`previousHead`, and `targetRevision`. Reverting to the current head is a no-op
returning that head as both `revision` and `previousHead`.

These guards prevent an outdated revert decision from silently overwriting an
intervening commit. They do not implement branching, conflict resolution, or a
general multi-operation transaction API.

## Verification

`McpContractTest` exercises query → mutation → diff → guarded revert → cold reopen,
all selector forms, conflicting/invalid/pruned revisions, same/reversed diff
pairs, compile-time rejection of updating queries in both server modes,
execution-time read restrictions, JSON/XML resource creation and overwrite,
authorization before storage changes, and structured output bounds.
`JsonWriterLifecycleTest` covers embedded storeless object, array and primitive
updates as well as existing AUTO/EXPLICIT writer cleanup.
`McpServerE2ETest` covers SDK registration and the existing basic workflows.

```sh
./gradlew :sirix-mcp:test
```

Changes to read/query paths also require the work-budget suites listed in
[VERIFICATION.md](VERIFICATION.md).
