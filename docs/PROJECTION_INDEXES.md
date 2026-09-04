# Projection Indexes (experimental, analytical)

A fourth, columnar index type accelerates analytical queries — aggregates, filtered counts,
group-bys, count-distinct — over homogeneous record sets. A **projection index** extracts the
declared fields of every record under a root path into compact column-oriented leaf pages
(1024 rows per leaf: frame-of-reference numerics, per-leaf string dictionaries, presence
bitmaps), which the vectorized executor scans with SIMD kernels instead of walking the
document tree.

Persisted leaves are stored as **semantic segments** — the record-key column, one body per
column, one (FSST-compressed where beneficial) dictionary per string column — each its own
copy-on-write page addressed from a tiny per-leaf descriptor, so a single-column update
rewrites one segment page and unchanged segments are shared across revisions by reference.
Bit-packed segments come to roughly **5% of the in-memory size**, so the on-disk tax over the
versioned document store stays ~10%. Double columns store exact values in an order-preserving
encoding; value-exact consumers decline columns tainted by lossy decimal conversions
(fail-closed). A column declared `timestamp` or `date` stores the epoch instead of the text, which
requires every value to be exactly `YYYY-MM-DDTHH:MM:SS` or `YYYY-MM-DD`;
`-Dsirix.projection.temporalKinds=false` builds and serves it as an ordinary string column.

For the internals, see [`PROJECTION_INDEX_DEEP_DIVE.md`](PROJECTION_INDEX_DEEP_DIVE.md),
[`PROJECTION_INDEX_INCREMENTAL_MAINTENANCE.md`](PROJECTION_INDEX_INCREMENTAL_MAINTENANCE.md)
for exact row lookup and positional updates, and the storage-design notes in this directory.

## Creating a projection

Create one with JSONiq — the resource must be created with a path summary
(`buildPathSummary(true)`):

```xquery
(: store a record set :)
jn:store('mydb', 'sales.jn', '[
  {"age": 30, "active": true,  "dept": "Eng",   "city": "NYC"},
  {"age": 45, "active": false, "dept": "Sales", "city": "LA"},
  {"age": 52, "active": true,  "dept": "Eng",   "city": "NYC"}
]')
```

```xquery
(: project (age, active, dept, city) over the top-level array :)
let $doc := jn:doc('mydb', 'sales.jn')
let $stats := jn:create-projection-index($doc, '/[]',
    ('/[]/age', '/[]/active', '/[]/dept', '/[]/city'),
    ('long', 'boolean', 'string', 'string'))   (: also: 'double'/'decimal', 'timestamp'/'date' :)
return {"revision": sdb:commit($doc)}
```

Nested roots and nested columns are paths too — e.g. a record set under
`'/wrapper/records/[]'` with a column `'/wrapper/records/[]/address/city'`, or a descendant
pattern `'//records/[]'` spanning sibling subtrees. Missing fields are tracked per row in
presence bitmaps, so sparse data stays correct.

A field path may also end in an **array step**: `'/[]/genres/[]'` declares a `genres` column
over the *elements* of an array-valued field (a dictionary-encoded string set per row), so
membership predicates — `some $g in $r.genres[] satisfies $g eq "Drama"`, alone or inside a
larger filter — are answered from the projection instead of the tree.

### Building during the load (one pass)

`jn:create-projection-index` walks a *finished* resource, so it costs a second pass over the
corpus. Embedded callers can instead declare the projection up front and let the load itself
produce the rows — the definition is catalogued on the still-empty resource and the load feeds
the builder as it shreds, so the index is complete when the load commits:

```java
final var projection = new ProjectionSpec("/[]",
    List.of("/[]/age", "/[]/active", "/[]/dept", "/[]/city"),
    List.of("long", "boolean", "string", "string"),
    expectedRows);   // -1 when unknown; only sizes the value-dictionary election
store.create("mydb", "sales.jn", jsonReader, projection);
```

The spec uses exactly the vocabulary of the query form (same root path, field paths, and type
names), so the two cannot drift apart. A sequential shred feeds the builder through its own
change notifications; the parallel bulk importer feeds the same builder from the rows its build
workers extract, so both routes produce a byte-identical index — see
[`BULK_IMPORT.md`](BULK_IMPORT.md) for the load-side contract (armed builds, refusals, cost).

Until the load's final commit the projection's metadata slot holds the stale tombstone, so an
interrupted load leaves queries on the generic pipeline rather than serving them from a
half-filled index. One failure completes the LOAD but not the projection: a resource-wide value
dictionary that hits its byte budget mid-build abandons the projection rather than the load. That
prints `[proj] PROJECTION ABANDONED` on stderr (unconditionally — the warning alone is invisible
under the shipped log configuration), and the tombstone records the reason plus its remedy: give
the loader an expected-row-count hint so the oversized column is declined up front and the rest of
the projection still builds, or raise `-Dsirix.projection.globalDict.budgetBytes`. An abandoned
projection is then replaced, never repaired: `jn:create-projection-index` over the still-catalogued
definition fails loudly ("catalogued but its store is missing, stale, or unreadable"), so the route
is `jn:drop-projection-index`, `sdb:commit($doc)`, and only then create it again — the replacement
gets a new, empty projection tree.

## Querying

There is no separate scan function — eligible queries route through the projection
automatically once it is installed (compile through
`SirixCompileChain.createWithJsonStore(store, session)` to get the analytical executor).
Plain JSONiq does it:

```xquery
(: full-column aggregates — served from the numeric column, no tree walk :)
let $doc := jn:doc('mydb', 'sales.jn')
return {"sum": sum(for $r in $doc[] return $r.age),
        "min": min(for $r in $doc[] return $r.age),
        "max": max(for $r in $doc[] return $r.age)}

(: filtered count — conjunctive predicate over the age + active columns :)
let $doc := jn:doc('mydb', 'sales.jn')
return count(for $r in $doc[] where $r.age > 40 and $r.active return $r)

(: single- and multi-key group-by — dictionary-encoded group columns :)
let $doc := jn:doc('mydb', 'sales.jn')
for $r in $doc[]
let $d := $r.dept, $c := $r.city
group by $d, $c
return {"dept": $d, "city": $c, "count": count($r)}

(: count-distinct — answered from the union of per-leaf dictionaries :)
let $doc := jn:doc('mydb', 'sales.jn')
return count(for $r in $doc[] let $d := $r.dept group by $d return $d)
```

## Lifecycle, versioning, and maintenance

The index is written into the session's transaction — `sdb:commit($doc)` persists it, like
the other index-creation functions. Projection definitions are catalogued in the resource's
index set exactly like path/CAS/name indexes, so a resource can carry **several projections**
side by side (each in its own storage sub-tree), and queries **discover them through the
revision-scoped catalog and page layer** — after re-opening a database, analytical queries
use persisted projections automatically (decoded once per revision into a bounded in-memory
cache, sub-second per ~10M rows), with no re-creation call needed. Because discovery is
revision-scoped, uncommitted builds are invisible to other sessions, rollbacks need no
compensation, and time-travel queries only ever see projection data that was current at
their revision.

Update transactions maintain projections **incrementally** (wired through the
index-controller listener lifecycle, like the other index types): changes are attributed to
their records as they happen, and at commit time only the touched leaves are patched —
updated records are re-extracted in place, deleted records drop out, and new or moved records
are spliced at their document predecessor/successor anchors. First and middle inserts use
sparse exact locators; a proven high-key tail insert stays on the compact normal fence
backbone. The same catalogued projection therefore keeps serving without a re-creation call,
including record reordering, moves into or out of the projected set, replacing a record set
wholesale, and descendant-pattern record sets appearing or disappearing.

Maintenance has no dirty-record cliff and never scans or rebuilds the complete projection.
It updates only touched row groups, 32-physical-leaf order/fence chunks, 256-leaf Bloom
chunks, bounded per-column set summaries, sparse locators, and immutable global-dictionary
radix paths. An unresolvable or corrupt touched unit fails the owning transaction and requires
rollback. Calling `jn:create-projection-index` with a different shape creates an additional
projection.

Serving is memory-bounded, not all-or-nothing. Kernels that want a column's whole byte image get
it eagerly while the projection's worst-case resident size fits
`-Dsirix.projection.eagerMaterializeBytes` (default: the smaller of half the projection cache
budget and a quarter of the heap); above that the same kernels read through bounded 128-leaf
windows instead, and a column fill that would exceed the budget declines: the query re-enters the
windowed whole-leaf route where one exists, and otherwise falls back to the record path.
Declining is a routing decision, not a corruption signal — the index stays valid and keeps
serving everything else.

Uncommitted state is servable too: an executor constructed over an open write transaction
(`new SirixVectorizedExecutor(wtx, threads)`) answers unpredicated aggregates, group-bys and
count-distinct from the transaction's own state — pending maintenance is applied on read
(read-your-writes) and the leaves are read through the transaction log, uncached, so
committed readers keep their isolated snapshots.

The full function family matches the other index types:
`jn:find-projection-index($doc, $rootPath, $fields)` returns a projection's definition id
(or `-1`), and `jn:drop-projection-index($doc[, $idx-no])` drops one or all projections. A drop
removes the definition from the catalogue and leaves its immutable historical tree untouched, so
revisions committed before it keep being served; projection tree ids are never reused while their
physical reference exists, so a later same-shape creation gets a new, empty tree and cannot mistake
unmaintained pre-drop columns for current data.

## REST serving

Projection serving is also wired into the **REST API**: a resource-scoped query
(`GET /database/resource?query=...`) is compiled with a vectorized executor bound to the
request's resource and revision, so the same analytical queries are answered from the
projection over HTTP. Because the analytical detection captures source paths — not resource
identity — the REST layer applies a fail-closed serving gate built as an **allowlist**: the
executor is wired only when the query provably targets the request's own resource — every
`jn:doc` names exactly that database/resource with two string literals, every other function
call is a known-safe builtin or `xs:*` constructor (any prefixed function, unknown name,
function reference, or module import refuses), requests scoped to a `nodeId` subtree are
excluded, and with a pinned non-latest revision only pure context-item queries qualify.
Anything unprovable simply runs on the generic pipeline — the gate can cost performance,
never correctness. The index-management functions (`jn:create-projection-index`,
`jn:find-projection-index`, `jn:drop-projection-index`) work over REST like any other JSONiq
query.

## Current limits

Column types are `long`, `boolean`, `string`, `double`/`float`/`decimal` (stored exactly in
an order-preserving encoding; value-exact consumers decline columns tainted by lossy decimal
conversions), and array-element string sets. Columns are resolved by their **declared path
relative to the record set**, so a projection may declare fields nested at different depths and
on different branches below the root, and a trailing name that also occurs at some other path
under the record set is not ambiguous. The column *name* — the trailing object-key step, or for
a set column the field step before the array layer — is part of the projection's identity and
must be unique within one projection; a declared path that does not sit strictly under the
declared root falls back to name-only lookup and is rejected when that name also occurs
elsewhere under the record set. Queries that the projection cannot serve exactly
(unrepresentable values, non-covered predicates, ambiguous projection selection) fall back
to the regular pipeline automatically, so results are always identical with or without the
index.
