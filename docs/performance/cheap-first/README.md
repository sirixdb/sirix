# Cheap-first conjunctions (C(1))

The Sirix optimizer stably orders eligible residual conjuncts by static cost after join recognition
and index matching: field/literal comparisons, arithmetic, casts/functions, then nested pipelines.
Equal costs keep their order. Unknown calls and effectful binding initializers are barriers.
The pass sorts a maximal conjunction once and computes each term's cost once.

`-Dsirix.optimizer.cheapFirstConjuncts=false` disables the pass. It applies to Sirix compile chains;
a plain Brackit compile chain retains its existing behavior.

The pass proves binding sources using lexical variable identities, including function parameters,
prolog defaults and context declarations. Fresh rows opened by the final stock `BasicJsonDBStore`
and literal rows can be filtered before exposure, only within the selection's predicate branch.
A preceding selection does not make a lazy return fresh. Correlated opening arguments have already
been evaluated to produce the current row; they do not become inputs to its field predicates.
Every admitted document or index read requires the stock provider, including reads in defaults,
nested pipelines and composed row constructors. Custom document providers retain the original
order. A row reused through an inner loop, join or grouping is treated as captured. Other
captured inputs, including function parameters, are admitted only when the actual tuple value is
scalar. Globals are checked in the query context, separately from shadowing tuple bindings.
Admission runs for every conjunction evaluation and never iterates a sequence or traverses a
container. Opaque values, supplied stored
views and the global context item execute the original conjunction. Pure unbound defaults remain
lazy; unsafe defaults are barriers. A cheaper false conjunct can suppress a later dynamic error,
as permitted by XQuery predicate evaluation ordering.

Let materialisation and timestamp parsing are separate changes. This implementation adds no replay
buffer, memo lifetime, cursor sharing, view invalidation or query-context cache. It preserves main's
membership join implementation.

## Deterministic verification

The focused suite uses hand-computed answers and read counts. It covers the twelve value/general
comparison operators; arithmetic, function/cast and nested-pipeline ordering; disjunctions,
conditionals, castable and quantified terms; stable ties and unknown-call barriers; pure and
side-effectful scalar/default aliases; opaque scalar, object, nested object, member, array,
sequence and context inputs; captured aliases after root rebinding; reused query plans, shadowing,
lazy errors and prefix consumers; externally supplied stored memos and composites exposed earlier
in a lazy result, inner loop or join; function parameters shadowing globals; and custom document
providers returning changing fields through direct, nested, default and composed producers.
Q10's executable semantic plan and its hand-computed category counts and margins are identical with the pass enabled or disabled; Q10 is a control.

`BindingDependencyWorkTest` counts dependency visits on duplicated alias chains and checks exact
answers. Exhausting the per-term proof budget makes that term a reordering barrier. The budget
and its scope are owned by `BindingDependencies`; it does not cap total compilation time.

`CheapFirstConjunctWorkBudgetTest` decorates actual stored-document field accesses. On 100 rows,
one id matches and two timestamp conjuncts run on that survivor: 101 id evaluations (including
the projection), and 2 timestamp evaluations. Removing optimizer registration deliberately breaks
the 2-read budget with 101 timestamp evaluations. Restoring registration restores the positive
budget. The correlated temporal-open fixture processes two outer publication rows and scans
100 rows per open: 202 id evaluations, 4 timestamp evaluations and answer `1 1`. With the rule off, the
same answer requires at least 200 timestamp evaluations. Removing stage registration produces
202 timestamp evaluations against the 4-read budget. Both budgets use `FILE_CHANNEL` and
count field evaluations regardless of buffer-cache state. No wall-clock assertion or engine
hot-path counter was added.

The initial C(1)-only implementation's full-suite, focused and work-budget results are recorded
in [verification.json](verification.json), alongside the later admission-repair focused runs.
The recorded full query suite has two independently known codepoint-ordering failures whose repair
was assigned separately; their names and failure messages are in that evidence file. This is an
explicit suite exception, not a clean query-suite result. Full suites and timing oracles were not
rerun for the admission repairs. [budget-evidence.json](budget-evidence.json) records both killed
mutations and the restored positive budgets from the initial implementation.

SH1 was 12/12 oracle-exact at both t25k and t100k, loaded with the kit's natural batching. Every
timing execution also compared its canonical TSV bytes with the oracle.
[oracle-digests.csv](oracle-digests.csv) records the matching actual and expected SHA-256 digests
for all 24 final answers.

## Measurement protocol

The baseline was current main `28a95fe8efe4d0910a931475b1d61ee884d1b48f`. Stores were loaded with
`BitemporalSirixLoadMain` using the kit's natural batching and read-only tier event streams. The
standalone [LatencyProbe.java](LatencyProbe.java) compiles and fully consumes each query, records
compile/execute/serialize/setup separately, canonicalizes each answer, and compares its TSV bytes
with the independent oracle. Execute time is the metric below.

Warm Q1/Q2/Q3/Q10 use ten in-process executions, discarding the first; Q5 uses three, discarding the
first. Cold uses the first execution of three fresh JVMs per query. Here cold means a fresh JVM;
the filesystem cache is not dropped, and latest revision opens prime resource metadata before
timing. The JVM was Oracle GraalVM 25.0.3+9.1, Java HotSpot 25.0.3+9-LTS-jvmci-b01. These are
HotSpot Java 25 measurements, with preview/vector/native access enabled,
512 MB initial / 2 GB maximum heap and 1 GB maximum direct memory. The shared machine was an
Intel Core i7-12700H (20 logical CPUs, 32 GB RAM). They are not PGO/native-image
measurements. Shared-laptop timings are observational; exact work budgets guard the saved work.

## Results

| Query | Main warm ms | Cheap-first warm ms | Warm ratio | Main cold ms | Cheap-first cold ms |
|---|---:|---:|---:|---:|---:|
| Q1 | 436.271 | 124.601 | 3.50x | 2465.237 | 2034.275 |
| Q2 | 284.898 | 60.850 | 4.68x | 2170.982 | 1869.024 |
| Q3 | 1464.750 | 283.596 | 5.16x | 3598.582 | 2611.720 |
| Q5 | 8531.603 | 1434.584 | 5.95x | 15909.357 | 11399.992 |
| Q10 | 1104.009 | 1185.324 | 0.93x | 2979.880 | 3058.861 |

Q1/Q2/Q3/Q5 are served by the pass. Q3 still scans three times; each scan is cheaper. Q5's
correlated revision opens admit cheap-first filters on their newly produced rows. Q10's plan is
unchanged and is a control: its warm sample was 7.4% slower here, with no speedup claim.

[Raw samples](timings.csv), [setup and timing lines](timing-lines.txt), [median data](summary.json)
and [Brackit SHA-256 digests](dependencies.json) preserve the before/after evidence. The same
Brackit bytes were used in both measurements. These timings came from the C(1)-only implementation
before the later admission and compiler-work repairs, and contain no evidence from the archived
let-memo branch.

## Reproduction

Use a private Maven repository for every Gradle command; stale snapshots in `~/.m2` must not be used
or changed. On the shared laptop, every Gradle/Maven invocation, and every standalone JVM above
2 GB, runs under this memory-aware two-slot limiter:

```bash
heavy() { while :; do a=$(awk '/MemAvailable/{print int($2/1048576)}' /proc/meminfo); if [ "$a" -ge 6 ]; then for s in 1 2; do flock -n -E 75 /var/tmp/fm-heavy-jvm.$s.lock "$@"; rc=$?; [ $rc -ne 75 ] && return $rc; done; fi; sleep 30; done; }
```

Do not enable shell `errexit` around a bare call: the busy-slot exit code 75 must reach the retry.
Use `--no-daemon --max-workers=1 -Dorg.gradle.jvmargs=-Xmx2g` and explicit test minimum/maximum heaps
(`-PtestHeapMin=512m -PtestHeapMax=2g` for focused tests; 6g maximum for the full suites).

Run the five optimizer test classes (`CheapFirstConjunctTest`, `OpaqueConjunctTest`,
`ScalarDependencyTest`, `ConjunctInputTest`, `BindingDependencyWorkTest`) and
`CheapFirstConjunctWorkBudgetTest`, then all `sirix-core` and `sirix-query` tests, including the
Work budgets block in `docs/VERIFICATION.md`.
To repeat the mutation proof, temporarily disable the optimizer's stage registration, run only the
work-budget test, observe the 101-vs-2 failure, then restore the source and rerun.

The checked-in [measure.sh](measure.sh) and [classpath.init.gradle](classpath.init.gradle) build
the probe with bounded heaps, record revision/JVM/Brackit digests, measure the five queries and
check all twelve oracles at both tiers. Run the same command at baseline and feature revisions
in an isolated checkout, using labels `baseline` and `after`. First copy this directory to
ignored `build/cheap-first-probe` so the helper remains available when checking the baseline revision:

```bash
cp -r docs/performance/cheap-first build/cheap-first-probe
bash build/cheap-first-probe/measure.sh after \
  /var/tmp/sirix-bitemporal/sirix-ms-cheap-first-let-materialize
```

The stores must already be loaded using the kit command above. The helper does not remove them.
To invoke the probe directly, create a runtime classpath from `sirix-query`'s
`sourceSets.test.runtimeClasspath.asPath`, compile
the probe with `javac -J-Xmx512m --enable-preview --release 25 --add-modules jdk.incubator.vector`, and invoke:

```bash
java --enable-preview --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
  -Xms512m -Xmx2g -XX:MaxDirectMemorySize=1g -cp "probe:$QUERY_CLASSPATH" \
  LatencyProbe "$TASK_STORES/t100k" /var/tmp/sirix-bitemporal/t100k/oracle "$OUTPUT" 10 1 2 3 10
```

Run Q5 separately with `3 5`; run each cold query in three fresh processes with `1 QUERY_NUMBER`.
Arguments are database root, oracle directory, output directory, repetitions, query numbers. Use a
task-owned directory beneath `/var/tmp/sirix-bitemporal` for stores, never the input tiers. Run
`BitemporalSirixRunMain` and byte-compare all twelve TSVs with the t25k and t100k oracles. Record the
same dependency digests, including Brackit, before and after. Delete only task-owned scratch once
finished.
