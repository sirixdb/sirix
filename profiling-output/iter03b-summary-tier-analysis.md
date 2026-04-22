# iter#03b — Data Blocks summary tier: formal verification gate

2026-04-22, Sirix perf campaign, branch `perf/umbra-ballpark-iter`.
STOP-BEFORE-CODE analysis.

## TL;DR — recommendation: **do not ship option (c) as proposed**

On the ScaleBenchMain dataset (the one the 5.15 s JVM / 4.72 s native
PGO cold-wall numbers were produced on), the projected 45 % cold-wall
reduction is **unreachable**. Summary-based leaf pruning would skip
essentially **0 %** of the 97,657 projection leaves under every shape
of the 9 bench queries, because the bench's synthetic data is uniformly
random over a small domain (`age ∈ [18, 65]`, `dept ∈ 8 values`). In
1024 row leaves that distribution gives `min ≈ 18 / max ≈ 65` on every
single leaf. The summary is structurally unable to prune anything.

Worse: the current `ProjectionIndexByteScan.evaluateLeafMask` path
**already does** the summary check (lines 344-350 + `zoneSkip` 374-383)
— the zone map is embedded in the leaf payload today. The "missing
lever" the iter#03 plan identified doesn't exist at the scan-time layer.

The pivot's only lever is at **install time**: defer reading the bulk
of each leaf at open until a query actually needs that leaf. But the
9 bench queries all need every leaf (each query's predicate/aggregate
touches 100 M rows spread across all 97,657 leaves). "Lazy hydrate"
would move the 2.5 s cost from `install` into `first-query`, not
eliminate it.

**The only way summary pruning could help on this bench is if the
test dataset were skewed** (e.g. `age` correlated with a leaf's
position in the sequence). That's not the configured bench.

Concrete outcomes for the three code variants considered:

| variant | persist-summary-only | lazy hydrate on demand | cold wall result (est) |
| --- | --- | --- | --- |
| A. Skip leaf bytes persist-only, read at query time | yes | yes | unchanged (~5.15 s) — 2.5 s move from install to first-query |
| B. Persist summary + skip hydrate only when `zoneSkip` | yes | partial | ~5.15 s (uniform data → 0 % skip) |
| C. No change, hydrate in parallel | no | no | ~3.9 s if 3-way parallel hydrate lands (different lever) |

**None of these reach the DuckDB ≤ 2 s target via the summary tier.**
The cold-wall budget is dominated by (a) DB-open + class-init (~2 s)
and (b) the single-threaded hydrate walk (2.5 s). Hydrate
parallelism — the iter#03 lever we explicitly chose NOT to take —
is the only remaining ≥ 10 % cold-wall lever. The summary tier is
a clean win ONLY on skewed data.

## Context — what the "summary tier" proposed

The iter#03b plan (per user prompt) proposed:

1. Add a 40 B per-leaf summary `{minValue, maxValue, validFlags, width}`
   persisted in a dedicated summary stream.
2. At install time, load ONLY summaries (3.9 MB vs 155 MB today).
3. At query time, consult summary first; skip the leaf entirely if the
   predicate can't match; lazy-hydrate the remaining leaves.

## Why it won't work on *this* bench

### Data distribution (from `ScaleBenchMain.GeneratedRecordsReader`)

```java
age    = 18 + rng.nextInt(48)              // uniform [18, 65]
dept   = DEPTS[rng.nextInt(8)]             // uniform 8-way
city   = CITIES[rng.nextInt(8)]            // uniform 8-way
active = rng.nextBoolean()                 // uniform bernoulli(0.5)
```

Per-leaf (1024 rows) min/max of `age`:

```
P(max_age < 41 in 1024 rows from uniform[18,65]) = (23/48)^1024  ≈ 0.0
P(min_age > 40 in 1024 rows from uniform[18,65]) = (25/48)^1024  ≈ 7.9 × 10^-291
```

Every single leaf satisfies `min ≤ 18 ≤ any query literal ≤ 65 ≤ max`.
Summary pruning cannot eliminate any leaf for any age predicate
(`filterCount`: `age > 40`; `compoundAndFilterCount`: `age > 30 AND
age < 50`). Same conclusion for `active` (boolean — both true and
false always present in 1024 samples at p≈0.5). Same conclusion for
`dept` (all 8 values always present when each has p=0.125 and n=1024).

### Summary check is already embedded in the hot path today

`ProjectionIndexByteScan.evaluateLeafMask` lines 344-350 read the
per-column min/max out of the leaf payload header and call
`zoneSkip(p, min, max)` which already returns `true` for the exact
conditions the user's plan documented:

```java
// ProjectionIndexByteScan.java:374-383
private static boolean zoneSkip(final ColumnPredicate p, final long min, final long max) {
  return switch (p.op) {
    case GT -> max <= p.longLit;
    case LT -> min >= p.longLit;
    case GE -> max <  p.longLit;
    case LE -> min >  p.longLit;
    case EQ -> p.longLit < min || p.longLit > max;
  };
}
```

This is the precise `operator → prune-condition` table the user's
analysis demanded. On the bench data it evaluates to `false` on
every leaf for every query. That does NOT change if we split the
summary out to its own stream — the numerical answer is the same.

### Lazy hydrate doesn't help when every query needs every leaf

The 9 bench queries:

| query | predicate | aggregate | leaves touched |
| --- | --- | --- | --- |
| filterCount | `age > 40 AND active` | count | all 97,657 |
| groupByDept | — | group by dept, count | all 97,657 |
| sumAge | — | sum(age) | all 97,657 |
| avgAge | — | avg(age) | all 97,657 |
| minMaxAge | — | min/max(age) | all 97,657 |
| groupBy2Keys | — | group by dept, city, count | all 97,657 |
| filterGroupBy | `active` | group by dept, count | all 97,657 |
| countDistinct | — | group by dept, distinct | all 97,657 |
| compoundAndFilter | `age>30 AND age<50 AND active` | count | all 97,657 |

None of the aggregates (sum/avg/min/max/groupBy/countDistinct) consult
a WHERE clause that the summary could satisfy — they HAVE to read
every row's value. Summary-based pruning is zero-for-all-9 here.

### The hydrate cost is CPU-bound, not I/O-bound

The 2.5 s cold hydrate walks 97,657 HOT leaves (~200 MB serialised).
That's 80 MB/s — an order of magnitude below NVMe sequential. The
hot loop is CPU:

- `HOTRangeCursor` key decode + sort-by-leafIndex.
- `HOTLeafPage.getValueSlice` per-chunk.
- `MemorySegment.copy` + `Arrays.copyOf` buffer grows.
- LZ4 / FSST decode in the page reader.

A "summary read" variant that reads only 3.9 MB of summaries instead
of 200 MB of full leaves **would** complete in ~10 ms: the I/O is
trivially fast. But the cost we're trying to delete is not I/O — it's
the CPU work of walking the HOT tree. That cost doesn't go away when
the payloads are smaller; the tree still has 97,657 entries to touch.

The cost only goes away if we STORE summaries in a separate, flatter
structure (a contiguous `long[][]` blob in one big HOT value or one
file-channel read) and NEVER visit the original per-leaf entries at
install. That is a larger re-architecture than "40 B per leaf in a
summary stream" — it requires **two** projection sub-trees: a
summary tree (~4 MB, one read) and a leaf tree (~200 MB, lazy).
The summary tree isn't a win unless the leaf tree is skipped, which
it's not on any of the 9 bench queries.

## Correctness note — if shipped anyway on *skewed* data

For datasets with skewed `age` (e.g. sorted or clustered), the
summary-pruning table is correct as stated. The formal operator →
prune-condition table (used by both current `zoneSkip` and any
summary-tier variant):

| op | skip iff |
| --- | --- |
| `>  L` | `max ≤ L` |
| `>= L` | `max <  L` |
| `<  L` | `min ≥ L` |
| `<= L` | `min >  L` |
| `=  L` | `L < min ∨ L > max` |

Conjunctive pruning: skip iff ANY predicate's `zoneSkip` holds (one
false predicate short-circuits the AND). `conjunctiveCountByGroup`
additionally needs the group column to be resolved after pruning — on
this code path the groupColumn is STRING_DICT, which has no numeric
zone map, so summary pruning never applies to the group column; it
applies only to the filter predicates. Correct behaviour preserved.

Group-cardinality aggregates (`groupByDept`, `groupBy2Keys`,
`countDistinct`) have NO WHERE clause, so the predicate array is
empty; zone skip is vacuously false; every leaf must be read. This
is correct with OR without the summary tier.

Tombstone handling: if a leaf is tombstoned, the current `readAll`
drops its entry from the output. The summary tier must do the same —
a tombstoned leaf's summary entry is removed when the tombstone
chunk is encountered. Matches existing `readAllViaCursor` semantics.

Adaptive encoding coexistence: a future FOR+BP compact numeric layer
(task #79 in MEMORY) leaves min/max untouched because they bracket
the leaf's entire value range — packed encoding is lossless over that
bracket. Summary tier safe to co-exist.

Backward compat: existing on-disk DBs don't have a separate summary
stream. Absence-fallback: when `loadSummaries(...)` returns null, the
code path must fall back to `readAll()` (today's hydrate). The fallback
check adds one null-test per open, no perf impact.

## What the next lever ACTUALLY is for the bench

The one remaining ≥ 10 % cold-wall lever on this bench is
**parallel hydrate** (option 2 in `iter03-coldwall-math.md`). The
single-threaded `readAllViaCursor` at 80 MB/s effective is the
bottleneck; the earlier parallel attempt died because HOT root fan-
out is 3-5 (so only 3-5 workers got work). Re-attempt with a 2-level
split (fan out one level deeper, then each worker owns a child
sub-tree) gives a realistic 3-5× speedup on an 8+ core machine.

Expected cold gain: 2.5 s → ~0.6 s = 37 % cold wall reduction
(vs 45 % if the summary tier were applicable on skewed data).

## Decision

**Do not implement the summary tier on this branch.** It is the
right design for a different dataset, and it's already partly
present (the zone-map lives in the leaf header). Shipping a
separate summary stream adds code + a persistence path WITHOUT
moving the cold-wall metric on the active bench.

Re-evaluate when the bench dataset is skewed or when we add
predicate queries whose literals lie outside each leaf's min/max.
Until then, the summary tier is structurally unable to deliver the
45 % cold-wall target.

No code changes land in this iter. No commits per the user's stated
constraint. The analysis answer is "the lever is mis-aimed; the
structural limit is the bench's data distribution".
