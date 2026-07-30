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
(fail-closed).

For the internals, see [`PROJECTION_INDEX_DEEP_DIVE.md`](PROJECTION_INDEX_DEEP_DIVE.md) and
the storage-design notes in this directory.

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
    ('long', 'boolean', 'string', 'string'))   (: also: 'double' / 'decimal' columns :)
return {"revision": sdb:commit($doc)}
```

Nested roots and nested columns are paths too — e.g. a record set under
`'/wrapper/records/[]'` with a column `'/wrapper/records/[]/address/city'`, or a descendant
pattern `'//records/[]'` spanning sibling subtrees. Missing fields are tracked per row in
presence bitmaps, so sparse data stays correct.

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
updated records are re-extracted in place, deleted records drop out, and new records append
to the tail — so the same catalogued projection keeps serving across updates with no
re-creation call — including replacing a record set wholesale (deleting the array drops its
rows, a fresh record set at the same path is picked up automatically) and, for
descendant-pattern roots, record sets appearing and disappearing.

Changes the incremental path cannot attribute exactly (subtree moves, unresolvable
structure, or more dirty records per transaction than
`-Dsirix.projection.maxIncrementalRecords`, default 100 000, where patching approaches
rebuild cost) degrade to an **automatic full rebuild inside the same commit** — the
projection stays exactly maintained, like the other index families, with no manual
intervention. Only an unexpected failure of both the incremental patch and the rebuild
tombstones the projection (a corruption valve: queries transparently use the regular
pipeline and re-running `jn:create-projection-index` rebuilds under the same definition);
calling it with a different shape creates an additional projection.

Uncommitted state is servable too: an executor constructed over an open write transaction
(`new SirixVectorizedExecutor(wtx, threads)`) answers unpredicated aggregates, group-bys and
count-distinct from the transaction's own state — pending maintenance is applied on read
(read-your-writes) and the leaves are read through the transaction log, uncached, so
committed readers keep their isolated snapshots.

The full function family matches the other index types:
`jn:find-projection-index($doc, $rootPath, $fields)` returns a projection's definition id
(or `-1`), and `jn:drop-projection-index($doc[, $idx-no])` drops one or all projections
(tombstoning the stored columns so a later same-shape re-creation rebuilds instead of
serving leftovers).

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

Column types are `long`, `boolean`, and `string` (floating-point columns are rejected rather
than silently degraded); columns are resolved by trailing field name, which must be unique
and unambiguous under the record set; queries that the projection cannot serve exactly
(unrepresentable values, non-covered predicates, ambiguous projection selection) fall back
to the regular pipeline automatically, so results are always identical with or without the
index.
