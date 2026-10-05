# Valid-time key slices

The valid-time index can answer exact closed and half-open point predicates without reading candidate
objects. Its sorted key sequence constructs exact-match JSON objects only when a consumer requests
them; exceptional candidates retain demand-time verification. `size()` counts exact keys and verifies
exceptional candidates; repeated iteration and early close do not close the borrowed revision transaction.

## Admission and exact fallbacks

| Source | Indexed predicate | Fallback |
| --- | --- | --- |
| `jn:valid-at`, two-argument `jn:scan-valid-time-index` | Closed point containment | Original exact temporal predicate for rounded, clamped, open, duplicate, or lexically ambiguous bounds; exact linear scan when no interval index applies |
| `jn:open-bitemporal(C,R,T,P)` | Half-open validity: `validFrom <= P < validTo` | Exact temporal verification for exceptional bounds; half-open linear scan when no interval index applies |
| `for $x in jn:open-bitemporal(C,R,T,P) where P lt xs:dateTime($x.vt) return ...` | Half-open source intersected with the residual; inclusive `le`, reversed `gt/ge`, general comparisons, and the analogous start comparisons are supported | Original cast/comparison for exceptional records; original temporal scan plus comparison for a different configured field or absent index |
| `for $x in jn:doc/open(...)[] where xs:dateTime($x.vf) op P and P op xs:dateTime($x.vt) return ...` | All four combinations of inclusive/strict endpoints, either operand/conjunct order | Original full array scan and comparisons unless every member has two exact bounds and its array order agrees with key order |

The point must be the same invariant variable or `xs:dateTime` literal on both sides of a matched
predicate. Only `xs:dateTime` field casts are consumed. The bitemporal rule consumes the first
conjunct, preserving later conjuncts and their evaluation order. Positional/allowing-empty bindings,
other casts, mismatched points, and other source shapes retain their ordinary evaluation.

The SH1 queries call `jn:open-bitemporal` directly; they need no `local:slice` wrapper or additional
strict-end comparison. The optimizer-only `open-bitemporal-slice` target is translated directly and
is absent from the public function registry, so query text cannot call it.

Brackit optimizes user-function bodies, so dynamic collection/resource/time parameters work inside
functions such as `local:slice`. An identity `for $x in <slice> return $x` pipeline over the public
function or a folded scan is removed, allowing `count()` calls to use the key count. Typed, positional, and nonidentity
loop bindings retain their original evaluation.

Transaction time is resolved at each evaluation. No revision is captured during compilation.
Timezone offsets in `xs:dateTime` arguments are preserved when converting to `Instant`.
Folded bitemporal comparisons retain their original operand type, comparison kind, and direction
independently of the source function's dateTime argument conversion. Their fallbacks filter the
original half-open interval or linear source in its existing order. Inclusive end residuals and
start residuals cannot include records excluded by the source's strict end.
Temporal fallbacks do not narrow with dateTime CAS indexes: their Brackit casts can omit bounds
accepted by the temporal predicate’s `Instant.parse`, so they provide no candidate coverage proof.
Timezone-less comparison points retain Brackit's ordinary comparisons, as do non-singleton plain
FLWOR points. Computed field dereferences are not folded. Non-object array members evaluate the
original comparisons with empty field dereferences. Plain-FLWOR points are evaluated only on row
demand and at their original operand position; empty documents, non-array roots, and empty arrays
supply no unboxed rows and never evaluate their point expression. Direct temporal functions retain
their original object-root and missing-resource semantics.
Reordered arrays retain the key-only bitemporal route, whose sorted keys preserve the temporal source's
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
The order guard is conservative: creating a fresh index can reestablish orderedness after
subsequent edits restore it.

`IndexDef.hasUnsupportedValidTimeFormat()` identifies incompatible catalog definitions. Readers
omit these indexes from discovery and use the ordinary exact query fallback, including historical
reads, optimizer discovery and VIEW-authorized REST reads. Reads do not change catalog bytes or add
revisions. Writer creation, listener binding and index creation reject incompatible definitions;
there is no migration API or automatic upgrade path. Create a fresh current-format database for
this representation. Writer rebinding resolves the represented revision's catalogue before listener
creation; reverting to an incompatible catalogue is rejected.

Every record carrying postings is registered in the interval tree, so a stab is the only candidate
source. Every duplicate-bound record is registered over
the whole domain and marked inexact, so the original field lookup and cast determine its answer
wherever a query could match it. Verification postings therefore flag refs the tree already yields
and are rescued for a strict endpoint at an exactly representable point only when the closed stab
also returns them: there a rounded endpoint such as `.000500Z` shares the point's millisecond, so
the half-open stab can skip the record and the strict-start tie removal can drop it, while a clamped start bound
can be dropped at the domain origin. The closed stab needs no union — the domain map is monotonic,
so it already returns a superset. Closed and strict stabs outside every interval read no candidate
object at all. Membership filters nested objects entirely from index postings. Retained built-in
temporal residuals run for every candidate after the original temporal predicate and only as each
candidate is demanded. Iteration and positional access can stop before a later malformed cast; counting evaluates all candidates that
need verification. Exact candidates require no field reads when no residual remains. A retained
temporal residual always filters exact candidates as well, and disables key-only known cardinality. Folded bitemporal
comparisons use the key-only sequence when the comparison matches the indexed bounds and all
selected candidates are exact; otherwise they retain the built-in comparison and reuse the selected
keys and evidence. Caller-supplied arbitrary predicates are not supported. Among source-valid records, a strict integer tie
must not suppress an original cast error. Candidate membership and verification use the existing compressed HOT
posting chunks and `NodeReferences.contains`, once per candidate chunk. Only matching candidate keys
are retained; unrelated posting references are never enumerated or copied into query collections.

Direct and folded sequences probe the closed candidate set before reading posting evidence. An
empty closed stab emits no interval or posting references and performs no posting lookups. Nonempty
strict slices reuse the closed set for rounded/clamped rescue. Plain-FLWOR coverage uses posting
cardinalities and compressed bitmap intersection without expanding references. Document-order
admission stops at the first live guard chunk; exceptional-bound admission stops at the first
intersection with the array's membership. Exceptional intervals in other cohorts do not prevent
key-only counts over exact array members. The read index controller retains up to 256 cohort admission
results, scoped to database, resource, immutable revision, definition identity, array key and length.
Controller eviction or collection of the owning session releases these proofs; entries retain no
readers or transactions. Every key-only factory declines writers and intent-log-backed serving
cursors before discovering indexes, including read-only wrappers over mutable readers. Historical
immutable readers remain eligible while their resource has an active writer. Mutable calls use the
exact current-document fallback, which refreshes held whole-array/object and object-field views.
Mutable positional slices refresh their members and cursor anchors while retaining their original
window. Direct scans and primitive-key requests also decline positional and object-field array
views: whole-storage-array postings cannot prove those views' membership. Key requests collect and
sort exact fallback matches, retaining the strict end mode. Repeated immutable queries reuse successful or declined admission without decoding whole membership
and verification postings; point conversion and endpoint selection remain per evaluation.

## Verification plan

All Gradle and Maven commands use a fresh, worktree-private Maven-local directory and a private
`TMPDIR`. Do not use or modify `~/.m2` for this change: it may contain a stale Brackit snapshot.
The example paths below must be empty at the start of a verification campaign.
Before running the commands, define the shared-host `heavy()` limiter captured in
[`measure.sh`](performance/cheap-first/measure.sh); its shell usage is described in the
[campaign reproduction instructions](performance/cheap-first/README.md#reproduction).

```bash
export TMPDIR="$PWD/build/validtime-verification/tmp"
mkdir -p "$TMPDIR" "$PWD/build/m2-private"
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
every Gradle or Maven invocation runs under the captured memory-gated two-slot `heavy()` limiter.
All runs, including the full suites, use two Gradle workers, a 512 MB initial/2 GiB maximum test
heap and a 2 GiB maximum Gradle heap.

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
on demand in every mode. Posting lookups and compressed chunks are counted separately from emitted
references: candidate checks emit zero posting references. An explicit scan is the positive control
for reference enumeration. Repeated demand reuses candidates and posting evidence without further
posting reads. Selective positive-stab budgets place an exact match first and last among 31 or 127 expired
inexact rows, exercising packed and bitmap postings, plus nested candidates outside the root cohort. Count performs zero object/timestamp
reads; exists and first demand construct one object, with work bounded by the three candidates.
Opt-in 100,000-row versions exercise both placements in the dedicated Test phase. Multi-chunk plain
FLWOR budgets put expired inexact nested records beside exact outer records with one match, first
and last. Their first admission proves nonzero validation work; repeated count/exists/first queries
decode only the two candidate evidence chunks. Packed and bitmap fixtures retain explicit reference
enumeration and cardinality controls. Separate executable checks cover warmed proofs across resource
and cohort boundaries, changed points, writer mutations, reordered arrays, dropped/rebuilt definitions,
reverts and reopened historical revisions. The two 100,000-row cohort variants are also opt-in.
`ValidTimeMutableSliceTest` reproduces both pending-edit wrong-result directions through real writers,
read-only cursor wrappers and intent-log reader facades. It checks direct count, exists, positional
demand, iteration and sorted keys, every strict/inclusive endpoint mode, structural edits, held
views, warmed slice members/anchors, immutable windows, object-field views, deferred empty points,
cast errors and historical readers with an active writer. Edited test transactions roll back before
closing.
User-function demand and return-type coverage is described under [Brackit dependency](#brackit-dependency).
A deliberate eager-materialization mutation must fail the count budget; ordinary result assertions
alone cannot detect it.

The small empty-stab inexact fixture remains at 64 records. The dedicated Test phase must execute
all five 100,000-record variants: empty stabs, selective positive stabs with the match first/last,
and plain-FLWOR cohorts with the match first/last. For empty stabs, record the printed `moveTo`,
`getFirstChildKey`, `getValue`, `intervalRefs` and `postingRefs` counts, all zero for each empty
answer before and after the intervals in every mode and query route.
Printed modes use bit 1 for strict start and bit 2 for strict end. It is opt-in so ordinary CI retains
fixture-scale coverage:

```bash
SIRIX_VALID_TIME_LARGE_BUDGET=true heavy ./gradlew --no-daemon -Dorg.gradle.jvmargs=-Xmx2g \
  -Dmaven.repo.local="$PWD/build/m2-private" --max-workers=2 \
  -PtestHeapMin=512m -PtestHeapMax=2g :sirix-query:test \
  --tests '*ValidTimeSliceWorkBudgetTest.oneHundredThousand*' --info
```

The Test phase must retain the captured heap limits and limiter for these opt-in scenarios;
their presence alone is not evidence of a 100k result.

Correctness coverage includes exhaustive small RI-tree domains, hand-computed strict/inclusive
answers, reversed operators, dynamic resources and correlated points, nested objects, missing and
malformed fields, sub-millisecond endpoints, start bounds clamped below the domain origin, duplicate
bounds registered over the whole domain, changed precision with unchanged rounded keys, moved array
elements, and historical revisions. Existing incremental-maintenance fixtures also check the parent
postings after each update/delete/move and after reopening historical revisions, and that every
verification posting belongs to a registered interval.

## Brackit dependency

The consumed `1.0-alpha10-SNAPSHOT` supports lazy UDF returns through `Sequence.isRepeatable()`
and `Sequence.knownSize()`. Sirix opts immutable valid-time key sequences into that protocol,
including folded user-function bodies in the laziness tests. SH1 Q6/Q11 use direct public calls.
For exact candidates, the known cardinality comes from index keys without constructing objects or reading timestamps. Inexact candidates and sequences
retaining a built-in temporal residual report an unknown cardinality. Mutable
views still use the existing fallback.

The five-argument scan overload returns its selected producer directly when the point is already a
captured `DateTime`. Expression-supplied points retain their deferred wrapper so empty sources and
point-evaluation errors keep their existing semantics.

Brackit preserves return-type validation and normalizes empty/singleton UDF results to their
existing scalar representation. That normalization may construct a singleton object, and unknown
cardinalities require bounded lookahead. Exact multi-item `item()*` returns need no lookahead:
count constructs zero objects, and the first requested item constructs one.

`ValidTimeSliceWorkBudgetTest.userFunctionCountDoesNotMaterializeTheSlice` is enabled and guards
two- and 64-row counts, first-item demand, independent readers after early close, and constrained
return-type errors. Direct-call and direct-FLWOR zero-read bounds remain unchanged. The standalone
[UdfMaterializationRepro.java](bench/validtime-slice/io/sirix/query/bench/validtime/UdfMaterializationRepro.java)
needs only Brackit and implements the same producer protocol: both the direct count and the trivial
UDF wrapper construct zero items. Unmarked producers retain Brackit's conservative eager behavior.

## Timing reproduction

[LatencyProbe.java](bench/validtime-slice/io/sirix/query/bench/validtime/LatencyProbe.java) separates compilation, complete result
iteration, serialization, and TSV canonicalization. It aborts on any oracle mismatch before reporting
a timing. Build the normal query runtime classpath with `:sirix-query:printClickBenchRuntimeClasspath`
and compile the probes against it with `javac -d <probe-classes>`. Their package is
`io.sirix.query.bench.validtime`; add `<probe-classes>` to the runtime classpath when launching them.
Preserve class directories and the core jar per measured variant
so another build cannot change code under a running measurement.

Use the unmodified SH1 loader and its natural publication batching, with read-only inputs/oracles
under `/var/tmp/sirix-bitemporal/t25k` and `t100k`. Stores and output belong under the task-specific
campaign directory. Use the kit JVM flags: `--enable-preview --add-modules=jdk.incubator.vector
-Xms512m -Xmx2g -XX:MaxDirectMemorySize=1g` (and native-access permission for the storage backend).

```text
io.sirix.query.bench.validtime.LatencyProbe <db-root> <oracle-dir> <out-dir> <repetitions> <query-numbers...>
```

Warm: one JVM, ten repetitions, discard the first; Q6 and Q11 use three repetitions, discard the
first. Cold: three fresh JVMs per query, one repetition each. Report medians of compile + execute +
serialize, with canonicalization separately. Store/resource opening is outside the measured interval.
These are cold-process measurements, not a forced OS-page-cache eviction. Also run the kit's
twelve direct-call queries and byte-compare all twelve TSVs at both tiers. Timing results are recorded only after these checks pass.


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

[DirectSliceProbe.java](bench/validtime-slice/io/sirix/query/bench/validtime/DirectSliceProbe.java) counts a direct half-open slice
at the kit's latest publication and valid-time point. Its independent expected count is the sum
of the Q7 oracle's group counts: 97,212. With the same repetition/discard method, its total warm
median fell from **403.1 ms to 93.0 ms** and its fresh-process query median from **3,938.7 ms to
596.9 ms**. This removes candidate-object and timestamp-field work, but does not meet the study's
15 ms estimate. [All direct-slice repetitions](bench/validtime-slice/direct-slice-results.csv) are retained.

The end-to-end SH1 medians and every repetition are recorded in
[results.csv](bench/validtime-slice/results.csv). Q12 is measured on the baseline's original anti-join;
the separate membership change is not included, so its several-minute join dominates the saved slice
work. At the measured revision, the Q6/Q11 path through `local:slice` lacked demand-only
materialization despite timestamp-predicate folding inside that body. The current dependency contract
is described under [Brackit dependency](#brackit-dependency).

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
