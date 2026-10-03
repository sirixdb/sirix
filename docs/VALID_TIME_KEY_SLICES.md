# Valid-time key slices

The valid-time index can answer closed and half-open point predicates without reading candidate
objects. Its sorted key sequence constructs JSON objects only when a consumer requests them.
`size()` counts keys; repeated iteration and early close do not close the borrowed revision transaction.

## Admission and exact fallbacks

| Source | Indexed predicate | Fallback |
| --- | --- | --- |
| `jn:valid-at`, `jn:open-bitemporal`, two-argument `jn:scan-valid-time-index` | Closed point containment | Original exact temporal predicate for rounded, clamped, open, duplicate, or lexically ambiguous bounds; existing scan routes when no interval index exists |
| `for $x in jn:open-bitemporal(C,R,T,P) where P lt xs:dateTime($x.vt) return ...` | Half-open end; inclusive `le`, reversed `gt/ge`, general comparisons, and the analogous start comparisons are supported | Original cast/comparison for exceptional records; original temporal scan plus comparison for a different configured field or absent index |
| `for $x in jn:doc/open(...)[] where xs:dateTime($x.vf) op P and P op xs:dateTime($x.vt) return ...` | All four combinations of inclusive/strict endpoints, either operand/conjunct order | Original full array scan and comparisons unless every member has two exact bounds and its array order agrees with key order |

The point must be the same invariant variable or `xs:dateTime` literal on both sides of a matched
predicate. Only `xs:dateTime` field casts are consumed. The bitemporal rule consumes the first
conjunct, preserving later conjuncts and their evaluation order. Positional/allowing-empty bindings,
other casts, mismatched points, and other source shapes retain their ordinary evaluation.

Brackit optimizes user-function bodies, so dynamic collection/resource/time parameters work inside
functions such as `local:slice`. After folding, an identity `for $x in <slice> return $x` pipeline
is removed, allowing direct `count()` calls to use the key count. Typed, positional, and nonidentity
loop bindings retain their original evaluation.

Transaction time is resolved at each evaluation. No revision is captured during compilation.
Timezone offsets in `xs:dateTime` arguments are preserved when converting to `Instant`.
Folded bitemporal comparisons retain their original operand type, comparison kind, and direction
independently of the source function's dateTime argument conversion. Their fallbacks filter the
original closed interval/CAS/linear source in its existing order.
Timezone-less comparison points retain Brackit's ordinary comparisons, as do non-singleton plain
FLWOR points. Computed field dereferences are not folded. Non-object array members evaluate the
original comparisons with empty field dereferences. Plain-FLWOR points are evaluated only on row
demand and at their original operand position; empty arrays never evaluate their point expression.
Reordered arrays retain the key-only bitemporal route, whose sorted keys preserve the closed source's
order, while plain FLWOR retains its document-order admission check.

## Index representation

The lower and upper RI-tree stores retain their `(fork, endpoint)` keys. `stabHalfOpen` excludes an
upper endpoint equal to the point, including at the equal fork. `startingAt` removes strict-start
ties. `forEachRef` enumerates every registration in one ordered-store scan, for index-wide
consistency assertions. These probes have counted scan bounds, not timing assertions.

A companion HOT tree stores revisioned postings for:

- object membership by immediate parent, removing candidate parent probes;
- intervals requiring exact verification;
- parents whose child node keys have been observed out of order.

The physical roots and key encoding are specified in
[HOT index specification §2.3.4](HOT_INDEX_SPECIFICATION.md#234-validtime-idxintervalvalidtimekeyserializerjava).
The order guard is conservative: rebuilding can reestablish orderedness after subsequent edits
restore it.

`IndexDef.needsValidTimeRebuild()` identifies obsolete catalog definitions, including formats whose
exactness evidence predates the empty-fraction lexical check. Opening a resource never
upgrades an obsolete valid-time catalog or adds a revision. Readers omit obsolete indexes from
discovery and use the ordinary exact query fallback, including historical reads, optimizer discovery and VIEW-authorized REST reads.
The explicit maintenance contract is documented on
[`JsonResourceSession.rebuildValidTimeIndexes()`](../bundles/sirix-core/src/main/java/io/sirix/api/json/JsonResourceSession.java).
No old index-layout reader is retained. Writer rebinding resolves the represented revision's
catalogue before listener creation and rebuilds obsolete definitions when reverting to an old
revision, so unchanged records and subsequent mutations maintain the current representation.

Every record carrying postings is registered in the interval tree, so a stab is the only candidate
source. Every duplicate-bound record is registered over
the whole domain and marked inexact, so the original field lookup and cast determine its answer
wherever a query could match it. Verification postings therefore flag refs the tree already yields
and are rescued for a strict endpoint at an exactly representable point only when the closed stab
also returns them: there a rounded endpoint such as `.000500Z` shares the point's millisecond, so
the half-open stab can skip the record and the strict-start tie removal can drop it, while a clamped start bound
can be dropped at the domain origin. The closed stab needs no union — the domain map is monotonic,
so it already returns a superset. Closed and strict stabs outside every interval read no candidate
object at all. Membership filters nested objects entirely from index postings. Exceptional residuals
run after the original closed predicate and only as each candidate is demanded. Iteration and
positional access can stop before a later malformed cast; counting evaluates all candidates that
need verification. Exact candidates require no field reads. A strict integer tie must not suppress
an original cast error. Membership, verification and order evidence is collected once per evaluation
and shared by plain-FLWOR admission and candidate filtering.

Direct and folded sequences probe the closed candidate set before expanding posting evidence. An
empty closed stab emits no interval or posting references. Nonempty strict slices reuse the closed
set for rounded/clamped rescue. Plain-FLWOR coverage and document-order admission still read their
evidence before considering a point stab.

## Verification plan

All Gradle commands use a private Maven-local directory. Do not use or modify `~/.m2` for this
change: it may contain a stale Brackit snapshot.

```bash
heavy ./gradlew --no-daemon -Dorg.gradle.jvmargs=-Xmx2g \
  -Dmaven.repo.local="$PWD/build/m2-private" --max-workers=2 \
  -PtestHeapMin=512m -PtestHeapMax=2g \
  :sirix-core:test --tests 'io.sirix.index.interval.*' \
  :sirix-query:test --tests 'io.sirix.query.function.jn.temporal.*' \
  --tests io.sirix.query.function.DateTimeToInstantTest \
  --tests io.sirix.query.budget.ValidTimeSliceWorkBudgetTest
```

Run the complete `:sirix-core:test :sirix-query:test` suites and the work-budget commands in
[VERIFICATION.md](VERIFICATION.md), with the same private Maven repository. On the shared laptop,
every Gradle or Maven invocation runs under the supplied memory-gated two-slot `heavy()` limiter;
other JVMs with heaps above 2 GB use it too. The full-suite run
uses a 512 MB initial/6 GB maximum test heap and a 2 GB Gradle heap.

The new budget decorates a real transaction. Before demand and during exact-key counting it permits
zero candidate moves, timestamp reads, and object-constructor child-pointer reads. The first `next()`
permits one object read. Direct function, direct FLWOR slice and plain `jn:doc(...)[]` FLWOR counts exercise the
query interface through the decorated cursor. A second case holds only sub-millisecond bounds, so every record carries a
verification posting. Closed, strict-start, strict-end, and combined strict stabs before and after
all intervals must return zero with zero candidate moves, timestamp reads, constructor reads, interval
references and posting references. `sirix.validTime.scanDiag` gates the emitted-reference counters
behind a static-final flag; both module test tasks provide it, and captures require the gate to be on.
Direct key and sequence consumers and folded bitemporal count/first-item queries use the decorated
cursor. Positive probes inside the intervals and at a rounded end tie must still verify the records
on demand in every mode and capture one membership plus one verification reference per record.
Repeated demand reuses candidates and posting evidence without further enumeration. A separate user-function count budget is retained but disabled pending the Brackit fix described below. A deliberate eager-materialization
mutation must fail this budget; ordinary result assertions alone cannot detect it.

The small inexact fixture remains at 64 records. The dedicated Test phase must also execute the
100,000-record variant and record its printed `moveTo`, `getFirstChildKey`, `getValue`, `intervalRefs`
and `postingRefs` counts,
all zero for each empty answer before and after the intervals in every mode and query route.
Printed modes use bit 1 for strict start and bit 2 for strict end. It is opt-in so ordinary CI retains
fixture-scale coverage:

```bash
SIRIX_VALID_TIME_LARGE_BUDGET=true heavy ./gradlew --no-daemon -Dorg.gradle.jvmargs=-Xmx2g \
  -Dmaven.repo.local="$PWD/build/m2-private" --max-workers=2 \
  -PtestHeapMin=512m -PtestHeapMax=2g :sirix-query:test \
  --tests '*ValidTimeSliceWorkBudgetTest.oneHundredThousandInexactIntervalsHaveZeroReadEmptyStab' --info
```

If the dedicated Test phase increases a JVM heap above 2 GB, wrap that invocation in the captured
`heavy()` limiter. This review phase adds the executable scenario; it does not claim a 100k result.

Correctness coverage includes exhaustive small RI-tree domains, hand-computed strict/inclusive
answers, reversed operators, dynamic resources and correlated points, nested objects, missing and
malformed fields, sub-millisecond endpoints, start bounds clamped below the domain origin, duplicate
bounds registered over the whole domain, changed precision with unchanged rounded keys, moved array
elements, and historical revisions. Existing incremental-maintenance fixtures also check the parent
postings after each update/delete/move and after reopening historical revisions, and that every
verification posting belongs to a registered interval.

## Brackit dependency

Full laziness through user functions, including SH1's `local:slice` in Q6/Q11, depends on the
separate Brackit UDF materialization fix. In the tested `1.0-alpha10-SNAPSHOT`,
`io.brackit.query.function.FunctionExpr.evaluate:109` calls `ExprUtil.materialize` after
return-type conversion. `FunctionConversionSequence` also iterates when counting even an
`item()*` result; the enclosing expression adds a flattening wrapper. Direct index calls avoid
these UDF wrappers. Predicate folding still saves timestamp reads within the function body,
but this change alone cannot provide demand-only object construction across its return boundary.

`ValidTimeSliceWorkBudgetTest.userFunctionCountDoesNotMaterializeTheSlice` is disabled explicitly
until Sirix consumes the corrected Brackit snapshot. Re-enable it when that dependency lands.
The active direct-call and direct-FLWOR budgets keep their zero-read bounds. The standalone
[UdfMaterializationRepro.java](bench/validtime-slice/UdfMaterializationRepro.java) needs only Brackit:
`count(probe:keys())` constructs zero items; the trivial UDF wrapper constructs all 64. It also
checks return conversion separately, so fixing only the explicit materialization is insufficient.

## Timing reproduction

[LatencyProbe.java](bench/validtime-slice/LatencyProbe.java) separates compilation, complete result
iteration, serialization, and TSV canonicalization. It aborts on any oracle mismatch before reporting
a timing. Build the normal query runtime classpath with `:sirix-query:printClickBenchRuntimeClasspath`
and compile the probe against it. Preserve class directories and the core jar per measured variant
so another build cannot change code under a running measurement.

Use the unmodified SH1 loader and its natural publication batching, with read-only inputs/oracles
under `/var/tmp/sirix-bitemporal/t25k` and `t100k`. Stores and output belong under the task-specific
campaign directory. Use the kit JVM flags: `--enable-preview --add-modules=jdk.incubator.vector
-Xms512m -Xmx2g -XX:MaxDirectMemorySize=1g` (and native-access permission for the storage backend).

```text
LatencyProbe <db-root> <oracle-dir> <out-dir> <repetitions> <query-numbers...>
```

Warm: one JVM, ten repetitions, discard the first; Q6 and Q11 use three repetitions, discard the
first. Cold: three fresh JVMs per query, one repetition each. Report medians of compile + execute +
serialize, with canonicalization separately. Store/resource opening is outside the measured interval.
These are cold-process measurements, not a forced OS-page-cache eviction. Also run the kit's twelve queries unchanged and byte-compare all twelve
TSVs at both tiers. Timing results are recorded only after these checks pass.


## Historical validation results (2026-10-03)

These historical figures measured implementation revision
`a17efd081a1a9f2928058785d7b4c7b0a6a7fb07` (`ship`) against
`79c7ab99e769300dffd1ab51d8f65ec8b9501818` (`baseline`). Both CSVs record that revision for every
row. They predate the subsequent correctness and posting-layout fixes and are not evidence for
current code. The dedicated Test phase must rebuild stores and rerun affected SH1 oracle checks
and measurements on the actual corrected revision before reporting current results.

The complete core suite passed: 11,957 tests, zero failures/errors, 76 existing skips. The complete
query suite passed: 1,818 tests, zero failures/errors, six skips (five existing skips and the explicitly
pending Brackit UDF budget). The focused interval/temporal and all work-budget commands also passed,
as did both module formatter checks. The deliberate eager-materialization mutation failed the active
budget at its zero-candidate-moves assertion before being restored.

Both historical implementation SH1 stores used the kit's natural 25-publication batching. Every TSV from the
unmodified twelve-query kit matched its oracle byte-for-byte at t25k and t100k. Each reported timing
repetition also checked its oracle. The historical t100k implementation store used 366,795,207 logical bytes versus
358,405,103 for the baseline (about 2.34% more for the additional revisioned postings).

The baseline is `79c7ab99e769300dffd1ab51d8f65ec8b9501818`. Measurements used frozen compiled
outputs per variant on an Intel Core i7-12700H, Oracle GraalVM 25.0.3+9.1, and the kit's 512 MB
initial/2 GB maximum JVM heap. Brackit was `1.0-alpha10-SNAPSHOT`, SHA-256
`8164d45e0a3aab8926d92f59c9996daafbfa0dc9c550d0ddbd2a4ee6fc877ca7`. This was a shared laptop;
other work, including part of this lane's full-suite validation, overlapped the timing campaign.
Treat the medians as observed workload measurements, not latency guarantees.

[DirectSliceProbe.java](bench/validtime-slice/DirectSliceProbe.java) counts a direct half-open slice
at the kit's latest publication and valid-time point. Its independent expected count is the sum
of the Q7 oracle's group counts: 97,212. With the same repetition/discard method, its total warm
median fell from **403.1 ms to 93.0 ms** and its fresh-process query median from **3,938.7 ms to
596.9 ms**. This removes candidate-object and timestamp-field work, but does not meet the study's
15 ms estimate. [All direct-slice repetitions](bench/validtime-slice/direct-slice-results.csv) are retained.

The end-to-end SH1 medians and every repetition are recorded in
[results.csv](bench/validtime-slice/results.csv). Q12 is measured on the baseline's original anti-join;
the separate membership change is not included, so its several-minute join dominates the saved slice
work. The Q6/Q11 path through `local:slice` still needs the upstream Brackit fix for full demand-only
materialization, as described above; timestamp-predicate folding already applies inside that body.

Median total query latency (milliseconds):

| Query | Before warm | After warm | Before cold | After cold |
| --- | ---: | ---: | ---: | ---: |
| Q4 | 1,272.9 | 574.8 | 6,923.7 | 4,711.0 |
| Q6 | 13,383.1 | 4,933.2 | 36,851.6 | 19,118.8 |
| Q7 | 780.6 | 247.8 | 4,300.0 | 3,033.1 |
| Q8 | 722.2 | 415.8 | 4,834.1 | 2,844.3 |
| Q9 | 769.4 | 377.3 | 4,616.3 | 3,177.2 |
| Q11 | 43,265.2 | 13,326.2 | 42,481.8 | 16,400.4 |
| Q12 | 314,039.0 | 249,531.7 | 307,634.4 | 216,514.9 |
