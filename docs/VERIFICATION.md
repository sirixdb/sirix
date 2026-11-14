# Verifying correctness (especially of AI-generated code)

SirixDB is a temporal storage engine: a subtle bug does not just crash — it can silently corrupt
data and only surface revisions later. This document maps the verification layers the project
uses, what class of bug each layer catches, and how to run them. The layers are ordered by how
well they catch the characteristic failure mode of AI-generated (and, frankly, human) code:
*plausible-looking, locally correct, wrong about an invariant the author couldn't see*.

## Layer map

| Layer | What it proves | Where |
|---|---|---|
| Property-based tests (jqwik) | Round-trip fidelity and revision immutability hold for **arbitrary generated documents**, with automatic shrinking to a minimal counterexample | `sirix-core/src/test/java/io/sirix/property/JsonRoundTripPropertyTest.java` |
| Model-based oracle | Random operation sequences produce **semantically identical** results in SirixDB and a trivially-correct in-memory model | `sirix-core/src/test/java/io/sirix/property/JsonModelBasedOracleTest.java` |
| Structural-invariant fuzzing | Every mutation keeps sibling/parent/child links, childCount, descendantCount and hashes mutually consistent | `sirix-core/src/test/java/io/sirix/access/node/json/JsonStructuralFuzzTest.java` |
| Fixed-corpus sweeps | Adversarial shapes (control chars, astral pairs, 2^128 numbers, …) round-trip and serialize valid metadata | `JsonCorrectnessSweepTest`, `JsonUnicodeTest`, `JsonNumberEdgeCaseTest` |
| Concurrency invariant harnesses | Eviction watermark safety, slot-allocation exclusivity, guard/close protocol, cache weight accounting and page-to-key cache ownership under contention | `RevisionEpochTrackerWatermarkSafetyTest`, `ShardedPageCacheInvariantStressTest` |
| Crash injection / soak | Durability across simulated crashes; leak-free long-running bitemporal workloads | `crash/CrashRecoveryInjectionTest`, `stress/BitemporalSoakStressTest`, `stress.yml` workflow |
| Work-budget tests | A load or query does **no materially more work** than it should (leaves read, route taken, pages left pinned), where its answer would be identical either way | `sirix-query/src/test/java/io/sirix/query/budget/`, `sirix-core/.../index/projection/*WorkBudgetTest.java`; rules in `sirix-core/src/test/java/io/sirix/budget/README.md` |
| Mutation testing (PIT) | The tests **assert** on behavior instead of merely executing it — a surviving mutant is a code change no test noticed | `:sirix-core:pitest`, `verification.yml` workflow |
| REST authorization guards | Refused mutations through query entry points (execute/evaluate/serialize/paginate) and function calls preserve revision counts, pending edits, index catalogues, projection metadata and derived aggregate headers. Database-scoped roles use the full collection name; the permission contract is in the [OpenAPI specification](../bundles/sirix-rest-api/src/main/resources/openapi.yaml) | `sirix-rest-api/src/test/kotlin/io/sirix/rest/**` (`QueryMutationAuthorizationTest`, `PermissionCheckingQueryTest`); `:sirix-rest-api:offlineTest` selects tests needing no Docker or Keycloak, and `gradle.yml`'s `TestRestApi` job gates `:sirix-rest-api:test` |
| JUnit framework consistency | Query test classes do not mix standard JUnit 4 and Jupiter annotation markers, including inherited fixtures | `sirix-query/src/test/java/io/sirix/query/JUnitFrameworkConsistencyTest.java` (owns the guard's scope) |
| Error Prone + NullAway | Compile-time rejection of bug patterns and nullness-contract violations in `@NullMarked` code | Main CI, release, stress and deep verification; local `-PerrorProne` |
| SonarCloud / Checkstyle | Static analysis and style rules | [SonarCloud analysis](#sonarcloud-analysis), `checkstyle.xml` |

## Why these layers, specifically

**Property-based testing** states the invariant once ("any document shreds and serializes back to
itself"; "no commit ever changes an earlier revision") and lets the framework hunt for the
counterexample. You do not have to anticipate the edge case the code's author missed — which is
precisely the blind spot when the author is an LLM. On failure, jqwik shrinks to a minimal
counterexample and prints a seed; re-run with `@Property(seed = "...")` to reproduce.

**Model-based oracle testing** catches semantic divergence that structural checking cannot: an
operation that lands in the wrong place, drops a sibling, or mutates the wrong record still
produces a *well-formed* tree — only an independent, trivially-correct implementation notices the
document is now *wrong*. The oracle test replays the same random operations against SirixDB and a
plain `LinkedHashMap`/`ArrayList` model and compares serialization after every commit, plus every
historical revision at the end. It found a real bug on its first run (see below).

**Concurrency invariant harnesses** verify the properties the structures actually promise, using
a generation-stamped publication protocol so assertions are sound under concurrency (a checker
only asserts when it can prove the observed state spanned the whole read). Framework
linearizability checkers (Lincheck/jcstress) were evaluated and deliberately not adopted: the
custom concurrent structures here either expose *by-design eventually-consistent* reads
(`RevisionEpochTracker.minActiveRevision` is a lock-free scan) or hand out identity-based
resources (pages, buffers), both of which break the deterministic sequential specification those
tools require. The hand-rolled harnesses check exactly the contracts the code documents.

**Work-budget tests** cover the one regression class every layer above is blind to: the engine doing
more work for the same answer. A projection query that stops being served from a summary, a load
whose intent log stops spilling, a batch read that stops coalescing all return exactly what they
returned before, so result checks, oracles and invariants stay green while a large database slows
down or a large load dies. These tests capture counters around one operation — almost always figures
the engine already maintains — and assert a budget on them; the budget package's README owns the
exceptions and the rules for adding one. They deliberately assert no wall-clock time: a threshold on
a shared runner is flaky and cannot say what changed, where a counter is exact on any machine and
names the path that grew. The [budget README](../bundles/sirix-core/src/test/java/io/sirix/budget/README.md)
owns the inventory and its mutation evidence.

They are the first of two layers, not the whole of performance regression testing. A budget only
sees a change in the amount of counted work; it is blind to anything that makes the same work
slower, such as a slower call path or a native-image build setting, which is how two benchmark
queries once lost 150 ms each with every counter unchanged. That is the job of a separate, timed
baseline harness run on one known machine under the benchmark campaign's protocol (ClickBench's 43
queries, the 100M JSONBench numbers, later the bitemporal benchmark), which is deliberately not part
of these suites. A green budget suite means the engine did not start doing more work on the guarded
paths; it does not mean nothing got slower.

**Mutation testing** guards the tests themselves. AI-generated tests tend to assert too little —
they run the code and check something trivially true. PIT mutates the production code and reports
which mutants the tests fail to kill. Scope is deliberately curated (see the `pitest` block in
`bundles/sirix-core/build.gradle`); a whole-module run would take hours. Its `targetTests` must
contain only DETERMINISTIC tests: PIT requires a suite that is green without mutation, and a
randomly-seeded run (jqwik defaults, `JsonModelBasedOracleRandomTest`) can legitimately fail by
discovering a real, previously unknown bug — which aborts the whole mutation analysis. When a
random run finds a failing seed, add it to the fixed-seed regression list and PIT inherits the
coverage.

**Error Prone** rejects bug patterns at compile time (bad equals/format strings/self-assignment
and ~500 more); **NullAway** checks nullness contracts against the JSpecify annotations already
used in the codebase. Local builds opt in with `-PerrorProne`. The main PR/push CI workflow sets
`ORG_GRADLE_PROJECT_errorProne=true` only on its `Compile and package` job, which compiles all
main, test and JMH benchmark source sets. The release workflow runs the same gated compile
before importing signing credentials or publishing, covering tag pushes, release branches and
manually selected refs. The stress workflow enables the gate on its core test compile, including
manual runs against refs outside main CI's branch filters. Deep verification gates its dedicated
core and MCP compile with `-PerrorProne`. Each of these four workflows runs one gated compile;
other jobs, including Sonar, avoid repeating static analysis. Error Prone errors fail compilation;
NullAway also runs at `ERROR`, with `NullAway:OnlyNullMarked=true`. Every MCP main/test package
has a JSpecify `@NullMarked` package default. Legacy unmarked code, including core and query,
is outside NullAway checking, even where individual nullable contracts are annotated.
Test compilation models AssertJ/JUnit assertion checks without adding runtime guards.

`verifyNullMarkedPackages` checks package defaults for every Gradle Java compilation, including
main, test and JMH, using the JDK parser. It runs before gated Java compilation and from `check`.
A new `io.sirix` package must have its own `@NullMarked` `package-info.java` in each compilation
source group.
The checked-in baseline is `gradle/nullaway-unmarked-packages.txt`; entries may only be removed,
never added. The task rejects obsolete entries and any growth along the first-parent Git history,
including uncommitted additions (the first committed list establishes the baseline). Migrating a
legacy package requires marking all its compilation source groups and removing its entry.
Generated build sources are excluded. Run the gate with JDK 25 and a full-history checkout containing `origin/main`.
`StringConcatToTextBlock` alone is disabled because its generated-literal crash persists through
Error Prone 2.51.0 ([upstream bug](https://github.com/google/error-prone/issues/6208)).

## SonarCloud analysis

The `Run Sonarqube` job in [`gradle.yml`](../.github/workflows/gradle.yml) runs on pushes to
`main` and eligible pull requests targeting `main`, after both Linux core lanes and the query/Kotlin
lane succeed. The core lanes upload raw JaCoCo probes and test results; the query/Kotlin lane uploads
reports and test results. The scanner restores the same run's compiled modules and reports and never
reruns tests. The checkout matches the producing
lanes (the PR merge ref on pull requests), so coverage probes and analyzed bytecode use the same sources.
It waits for the quality gate, so a successful upload alone cannot make the check green.
The required quality-gate policy is **no new bugs or vulnerabilities on new code**;
configure its conditions in SonarCloud.
PR analysis uses GitHub Actions' automatic PR parameters and a full-history checkout.
Fork and Dependabot PRs skip this secret-dependent job; their normal CI still runs.
Never build untrusted PR code with secrets through `pull_request_target`.
Scheduled and release-branch CI do not run Sonar. Instrumentation, coverage reports, coverage uploads,
and the additional MCP/offline REST tests run only for Sonar-eligible events, using the shared
`Build` job's `sonar-enabled` output. The REST integration suite remains outside the
coverage handoff. The query lane also runs the MCP tests and the offline REST authorization tests
that the former serial Sonar build ran; offline REST needs neither Docker nor Keycloak, and its
JaCoCo execution data feeds both the REST and query reports. The analyzed module set is unchanged.

The core XML report merges both lanes' raw execution data, including the isolated test forks,
using `:sirix-core:jacocoMergedReport`. Merging probes before generating XML preserves the union of
branch coverage when both lanes visit the same method. Missing lane/fork data or module reports fails
the job instead of silently reducing coverage. The isolated forks' agent-handling
safety invariant is documented beside the fork-option copy in
[`bundles/sirix-core/build.gradle`](../bundles/sirix-core/build.gradle).

To check the analysis-only task graph locally after restoring coverage artifacts:

```bash
./gradlew :sirix-core:jacocoMergedReport sonar --dry-run
```

It must contain only the merge and scanner/resolver tasks, with no compilation or test tasks. When
changing this handoff, record Linux lane durations (including report generation), scanner duration, and end-to-end duration
from the workflow jobs API. Compare SonarCloud's `coverage` and `new_coverage` measures with the former
`build jacocoTestReport -x :sirix-rest-api:test` flow at the same source head; a different head is not
a coverage-equivalence check. Preserve the quality-gate wait and fork/Dependabot secret boundary.

Review issues against the low-latency requirements in `AGENTS.md`. Prefer clean code when it
costs no hot-path performance. Any rule exception must name the specific rule and scope, with
a short reason explaining the performance cost; do not weaken allocation or work-budget tests.

## PR mutation testing

`PIT changed-source mutation testing` in [gradle.yml](../.github/workflows/gradle.yml) runs
independently of the test lanes after `Build` and reuses that run's compiled output. It adds no
dependency to the test, Sonar or deploy lanes. Its **30-minute job limit** is below the longest
existing test lane (45 minutes); PIT processes share a **22-minute execution budget**, leaving
room for setup and report upload. A deadline kills the process group, preserves partial reports
and explicitly reports incomplete analysis. A large diff may need follow-up focused runs;
timeouts and missing evidence are never a clean score.

Scope comes from `git diff --diff-filter=ACMRT <base>...<head>`: added, modified and renamed
main Java/Kotlin sources only, excluding deleted files, tests, docs and package/module descriptors.
The checkout is the PR merge revision, matching `Build`. JVM `SourceFile` attributes map source
files to **exact compiled class names**, including nested/anonymous classes, extra top-level
classes and Kotlin file facades. No prefix glob or whole-module fallback broadens mutation scope.
Unmapped sources fail analysis explicitly (for example a file with no emitted bytecode); justify
these individually in the PR. New Gradle modules need an entry in `MODULES` in
[pr_mutation.py](../.github/scripts/pr_mutation.py).

Tests named `<Source>Test` or `<Source>Tests` in the source's package are preferred when every
changed source has one; otherwise PIT discovers the module's tests. The artifact records the
actual test filters. This focused selection can omit integration assertions; if a survivor is
already killed by a broader deterministic test, rerun with that test included and link the report.
Random jqwik/property exploration, the random model oracle, HOT structural properties and soak
suites are excluded from the PR mutation baseline; keep exploration in its normal test lane and
promote discovered seeds to deterministic regressions. The weekly curated core PIT task is unchanged.

The `pr-mutation` artifact contains source selection, exact targets and test filters, every
undetected mutant in `results.json`, HTML/XML reports, logs and `summary.md`. The job summary
shows killed/total separately from PIT's detected/total score (which includes timed-out mutants),
status counts and up to 100 undetected mutants per module; the artifact retains the full list.
Zero mutations, setup failures, malformed/missing XML and timeouts are incomplete, not 100%.
Inspect both `SURVIVED` and `NO_COVERAGE`; kill them or justify each with evidence in the PR.

The job runs on normal `pull_request` events, including forks and Dependabot, with only
`contents: read`, no secrets, no persisted checkout credentials and read-only Gradle caches.
Never use `pull_request_target` or add credentials to run untrusted PR code. Reports are artifacts
and job summaries, so reporting needs no PR write token.

**Rollout:** initially the job has `continue-on-error`; it remains visibly informational even
when survivors or incomplete analysis fail its analysis step. Once representative PRs have no
unresolved survivors/uncovered mutants or incomplete runs, maintainers can set the repository
Actions variable **`MUTATION_BLOCKING=true`** and require `PIT changed-source mutation testing`
in branch protection. That single switch removes job-level tolerance; the reporter already
rejects undetected mutants and incomplete runs. Review documented equivalent/unreachable-mutant
justifications before enabling; there is no silent survivor allowlist. Revert the variable to
restore informational rollout while investigating an infrastructure failure.

Local reproduction (JDK 25; compile the selected modules' `classes testClasses` first):

```bash
python3 -B -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation.py'
python3 .github/scripts/pr_mutation.py select --base origin/main --head HEAD --output /tmp/pr-mutation
python3 .github/scripts/pr_mutation.py run --output /tmp/pr-mutation --seconds 1320
# Add --blocking to reject undetected mutants and incomplete analysis locally.
```

For a broader deterministic test selection, reuse the artifact's `targets.txt` and run:

```bash
./gradlew :sirix-core:pitest -PprMutationModule=sirix-core \
  -PprMutationTargetsFile=/tmp/pr-mutation/sirix-core/targets.txt \
  -PprMutationTests='io.sirix.utils.NamePageHashTest*' \
  -PprMutationReportDir=/tmp/pr-mutation-expanded
```

The opt-in [Gradle configuration](../gradle/pr-mutation.gradle) uses the existing
[Gradle PIT plugin](https://gradle-pitest-plugin.solidsoft.info/) and versions. Its absence from
normal task graphs preserves existing test-lane timing.

## Running the layers

The normal core test task also runs `ValidTimeDisabledLookupCacheTest` in a separate JVM with
`sirix.hotLookupCache.maxEntries=0`, since the buffer manager reads that setting once at startup.
It checks fresh interval roots and all companion postings after truncation reuses a revision number,
and the surviving historical revision, under all four versioning types. Run it alone with
`./gradlew :sirix-core:validTimeDisabledLookupCacheTest`; enabled-cache tests retain their usual settings.
The normal core task also runs `GlobalValueDictionaryReadViewMissPathTest` in a separate JVM so
inline Mockito mocks in other suites cannot affect its allocation measurements. Run that suite alone
with `./gradlew :sirix-core:dictionaryReadViewAllocationTest`.

The normal core task also runs `StorageEpochStoreIdentityTest` and
`StorageEpochDifferentialTest` in a separate JVM with deterministic codec selection
(`sirix.codecBakeoff.probeInterval=1`). Run them alone with
`./gradlew :sirix-core:storageEpochStoreIdentityTest`. They compare rotating and
never-rotating transactions, including persisted page bytes in the identity suite
and seeded edits across storage, compression and dictionary lanes in the differential
suite, with cold-reopen checks.

```bash
# Generative + oracle + regression tests (fast, part of the normal test task)
./gradlew :sirix-core:test --tests 'io.sirix.property.*' \
                           --tests 'io.sirix.diff.JsonDiffSerializerStaleTupleRegressionTest'

# Concurrency invariant harnesses
./gradlew :sirix-core:test --tests 'io.sirix.access.trx.RevisionEpochTrackerWatermarkSafetyTest' \
                           --tests 'io.sirix.cache.ShardedPageCacheInvariantStressTest'

# HOT structural property test, default lane: runs with the normal test task, one test per index
# kind, every seed's stream under all four versioning types. A step is one operation of the stream,
# so a bulk posting run (the M and X operations) is checked once it has written every node key, not
# between them. After every operation it runs the structural validator, the full ordered slot walk
# against the reference and compares every one of the reference's keys with what the index answers.
# For CAS/VALIDTIME, the slot walk checks physical order but compares base chunks with the reference:
# delta slots must belong to a reference chunk or a base proven empty through the public posting
# view. Such empty bases can remain physical until a fold. The validator reuses resolved pages and
# decoded leaf keys only within one read-only pass; every later pass resolves current pages again.
# These representation and reuse rules apply to both lanes without changing their check cadence.
# A revert is followed by those same checks of the writer it rebound to the earlier revision, every
# commit checks the new revision that way through the reader, and every cold reopen re-checks every
# historical revision from disk. Nothing else is sampled;
# -Dsirix.hot.property.ops=N / .seeds=N / .seed=N resize or pin it.
./gradlew :sirix-core:test --tests 'io.sirix.index.hot.HOTStructuralPropertyTest.pathIndex'
# Heavy lane: every index kind with a longer seeded budget (each seed again under all four versioning
# types), sampling for reach instead. A mutation whose last handler merged or removed in place checks
# its touched key alone, except at zero-based stream positions divisible by 32. At those positions,
# after every other mutation handler, and after every successful revert, it also runs the structural
# validator, full ordered slot walk and a sampled lookup pass. The position counts all operations,
# including commit/revert/reopen; a bulk run still checks only after its last node key.
# Sampled writer and reader lookup passes target 16 keys using stride max(1, floor(size / min(16,
# size))) for a nonempty reference, rotating the offset modulo that stride each pass. Rounding can
# select more than 16 keys; stride 1 checks all keys. Every commit checks structure and slot order for
# the new revision, plus three rotating older-revision checks when an older revision exists (these
# can repeat). Every 8th commit requests exhaustive new-revision values and its logical iterator,
# where provided; other revision checks sample values. A cold reopen checks structure and
# slot order for every revision, all values for the newest, and sampled values for older revisions.
# Records each shrunk failure as a replayable stream whose header names the versioning, cadence and
# lane it failed under (-Dsirix.hot.property.kinds=CAS,PATH,NAME,VALIDTIME,PROJECTION narrows it)
./gradlew :sirix-core:test --tests 'io.sirix.index.hot.HOTStructuralPropertyTest.extendedBudgetAcrossEveryKind' \
                           -Dsirix.hot.property.collect=true -Dsirix.hot.property.heavy.seeds=8 \
                           -Dsirix.hot.property.heavy.ops=15000 -Dsirix.hot.property.failureDir=/tmp/hot-property
# Replay one recorded failure under the checks its header records; -Dhot.diag.validationDump=true
# makes the writer's own post-publication validator describe the offending node on stderr
./gradlew :sirix-core:test --tests 'io.sirix.index.hot.HOTStructuralPropertyTest.replayRecordedFailure' \
                           -Dsirix.hot.property.replayFile=/tmp/hot-property/hot-property-failures/failure-cas-1-full.txt \
                           -Dhot.diag.validationDump=true

# Work budgets (includes TemporalAggregateCatchUpWorkBudgetTest for lag 0, 1, 8, 16, 17 and folded lag 64)
# Add -Dsirix.workBudget.print=true -i to print every captured counter table
./gradlew :sirix-query:test --tests 'io.sirix.query.budget.*' \
  --tests 'io.sirix.query.json.IndexedArrayPositionTest' \
  --tests 'io.sirix.query.bench.bitemporal.BitemporalLoaderBudgetTest'
./gradlew :sirix-core:test --tests 'io.sirix.budget.*' \
                           --tests 'io.sirix.index.projection.*WorkBudgetTest' \
                           --tests 'io.sirix.access.trx.page.RefusedOverflowLeafTableTest'
./gradlew :sirix-mcp:test --tests 'io.sirix.mcp.*WorkBudgetTest' \
                          --tests 'io.sirix.mcp.BoundedSerializationAllocationBudgetTest'

# Mutation testing (report: bundles/sirix-core/build/reports/pitest/index.html)
./gradlew :sirix-core:pitest

# JUnit framework consistency (part of the normal query test task)
./gradlew :sirix-query:test --tests 'io.sirix.query.JUnitFrameworkConsistencyTest'

# Derived-resource crash boundaries (source publication, before/after derived beacon)
./gradlew :sirix-core:test --tests 'io.sirix.index.aggregate.DerivedIndexCrashRecoveryTest' \
                           -Dsirix.crash.run=true

# Static bug detection (same source sets as deep verification)
./gradlew :sirix-core:compileJava :sirix-core:compileTestJava \
          :sirix-mcp:compileJava :sirix-mcp:compileTestJava -PerrorProne
```

CI: the `Deep verification` workflow (`.github/workflows/verification.yml`) runs PIT and Error
Prone weekly and on demand; the soak workflow (`stress.yml`) runs the bitemporal leak-detecting
soak weekly and on demand.

## Bugs found by these layers (evidence they work)

* **Stale diff tuples after subtree removal** — updating a node and then removing one of its
  ancestors in the same transaction left an UPDATED tuple whose node key no longer resolves;
  `JsonDiffSerializer` then read from an unpositioned cursor and threw an NPE during commit (or
  silently wrote corrupt diff files for other node kinds). Found by `JsonModelBasedOracleTest`
  (seed `987654321`) on its first run; fixed in `JsonNodeTrxImpl.remove()` (descendant-tuple
  purge) and hardened in `JsonDiffSerializer` (unresolvable tuples are skipped). Regression
  coverage: `JsonDiffSerializerStaleTupleRegressionTest`.
* **Cache weight-accounting drift** — `ShardedPageCache.evictUnderPressure()` and `removePage()`
  subtracted page weight directly instead of releasing the recorded charge in `insertedWeights`;
  the next insert under the same reference then computed a zero delta and was never charged, so
  the tracked weight drifted toward zero and budget eviction stopped firing (unbounded memory
  growth under pressure). Found by `ShardedPageCacheInvariantStressTest` on its first run.
* **Error Prone first-run findings** — `ValueNodeDelegate` violated the equals/hashCode contract
  (equals compared the value array by content, hashCode hashed its identity);
  `EditScript.mChangeByNode` used an `IdentityHashMap` with boxed `Long` keys, so every
  `containsKey`/`get` for node keys beyond the autobox cache silently missed;
  `JsonNodeTrxImpl.moveToParentObjectKeyArrayOrDocumentRoot` tested the same node kind twice;
  `HOTTrieWriter.walkLeavesUntilFalseInstance` ignored its visitor's stop-verdict (visited every
  leaf regardless); `ConXPathAxisTest` swallowed XPath failures with no-op `getStackTrace()`
  calls.

## Checklist for new (AI-generated) changes

1. State the invariants first; review them in English before reviewing the diff.
2. If the change touches shred/serialize/mutate paths — extend or at least run the property and
   oracle tests; a new operation belongs in the oracle's operation mix.
3. If it touches concurrent structures — add an invariant harness with a provable claim window,
   not just "run it on 8 threads and hope".
4. Run `:sirix-core:pitest` scoped to the changed classes; a low kill rate means the new tests
   are decorative.
5. If it touches a read, load or query path — run the work-budget tests, and treat a broken budget
   as a regression until shown otherwise. A bound changes only with the evidence its README asks for.
6. Never accept a green build as proof when the same author (human or model) wrote both the code
   and its tests — the oracle/property layers exist precisely because they are independent.
