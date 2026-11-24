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
baseline harness run on one known machine under the
[benchmark protocol](DEVELOPMENT.md#the-benchmark-protocol) (ClickBench's 43
queries, the 100M JSONBench numbers, later the bitemporal benchmark), which is deliberately not part
of these suites. A green budget suite means the engine did not start doing more work on the guarded
paths; it does not mean nothing got slower.

**Mutation testing** guards the tests themselves. AI-generated tests tend to assert too little —
they run the code and check something trivially true. PIT mutates the production code and reports
which mutants the tests fail to kill. The weekly core task's scope is deliberately curated (see
the `pitest` block in `bundles/sirix-core/build.gradle`); a whole-module run would take hours.
The separate [PR lane](#pr-mutation-testing) and its
[scheduled full-scope report](#scheduled-full-scope-mutation-report) select changed sources.
Every task's `targetTests` must contain only DETERMINISTIC tests: PIT requires a suite that is
green without mutation, and a randomly-seeded run (jqwik defaults,
`JsonModelBasedOracleRandomTest`) can legitimately fail by
discovering a real, previously unknown bug — which aborts the whole mutation analysis. When a
random run finds a failing seed, add it to the fixed-seed regression list and PIT inherits the
coverage.

**Error Prone** rejects bug patterns at compile time (bad equals/format strings/self-assignment
and ~500 more); **NullAway** checks nullness contracts against the JSpecify annotations already
used in the codebase. Local builds opt in with `-PerrorProne`. The main PR/push CI workflow sets
`ORG_GRADLE_PROJECT_errorProne=true` on its `Compile and package` job, which compiles all
main, test and JMH benchmark source sets, and on `MutationTesting` so its compiler inputs match
the cached producer output (see [compilation reuse below](#pr-mutation-testing)). The release
workflow runs the same gated compile before importing signing credentials or publishing, covering
tag pushes, release branches and manually selected refs. The stress workflow enables the gate on
its core test compile, including
manual runs against refs outside main CI's branch filters. Deep verification gates its dedicated
core and MCP compile with `-PerrorProne`. The mutation consumer retains the gate when compilation
is needed; other jobs, including Sonar, do not enable it. Error Prone errors fail compilation;
NullAway also runs at `ERROR`, with `NullAway:OnlyNullMarked=true`. Every MCP main/test package
and the query shell's main/test packages have a JSpecify `@NullMarked` package default.
Legacy unmarked core and query packages are outside NullAway checking, even where individual
nullable contracts are annotated.
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
JaCoCo execution data feeds both the REST and query reports. The query lane also generates the
PIT plugin's JaCoCo XML and uploads it as `coverage-pit-tooling`. Sonar restores it to
`tools/pit-changed-lines/build/reports/jacoco/test/jacocoTestReport.xml`, the module's default
import location, and requires it alongside the engine reports before analysis.

The independent `Python mutation tooling` job discovers all `test_*.py` suites under
`.github/scripts/tests/`, covering mutation tooling and changelog/lesson fragments, with branch
coverage (85% minimum) and uploads `coverage-python`. Sonar requires this lane, downloads
its XML to `build/reports/python/python-coverage.xml`, verifies the report exists, and imports it
through `sonar.python.coverage.reportPaths`. The root Python test directory is explicitly
classified with `sonar.tests` and test-inclusion patterns that also preserve module Java/Kotlin
tests. Python production source remains in analysis; tests are analyzed as tests and do not count
toward production coverage. This small Python lane does not change the existing test lanes'
dependencies or run a local JVM.

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
independently of the test lanes after `Build` and the short `Python mutation tooling` job,
and reuses that run's compiled output. It adds no dependency to the test, Sonar or deploy lanes.
Its **30-minute job limit** is below the Linux query test lane (45 minutes); PIT processes share a **22-minute execution budget**, leaving
room for setup and report upload. Each module may use all the time still left except a
`MIN_MODULE_SECONDS` floor for every module after it. Modules run in name order; a module that
finishes early donates its remainder to those that follow. If the remaining budget cannot
provide that floor for every pending module, it is divided equally among them. A deadline kills the
launcher's process group and detached descendants, then waits for their termination before
artifact reads or the next module. Every invocation uses `--no-daemon`: its fresh, single-use
Gradle daemon is a descendant of that invocation's launcher. Daemon reuse was tried and dropped
because a reused daemon could perform work outside the terminating launcher's ownership, including
before publishing its identity during startup. The statuses of every class analysed before the kill
survive: the lane's controlling-JVM agent drains PIT 1.25.5 analysis units in completion order,
then its `STREAM` listener flushes each delivered class. Without that adapter, PIT waits for
earlier submitted units before invoking any result listener, even when later units have finished.
Result interceptors and listeners still run serially on the reporting thread; the module remains
incomplete analysis. The real-PIT `StreamingDeadlineTest` holds the leading unit open and verifies
that a later killed mutant reaches disk before terminating the run. A stream the reporter cannot
read leaves the deadline as the module's state
and is reported beside it, rather than replacing it. A large diff may need
follow-up focused runs; timeouts and missing evidence are never a clean score. Preserved statuses
still contradict equivalent claims and supersede out-of-lane claims when a test detected the
mutant. A preserved `SURVIVED` status from an unfinished run cannot authorize a disposition:
added matches are reported as not judged and contribute to neither dispositioned count,
including when valid XML was written before a failed or timed-out run ended.

Scope comes from `git diff --diff-filter=ACMRT <base>...<head>`: added, modified and renamed
main Java/Kotlin sources only, excluding deleted files, tests, docs and package/module descriptors.
Both diff revisions must be full hexadecimal commit IDs; option-like values are rejected
before Git runs. Diff paths must be canonical relative paths and selected sources must exist in
the head commit. Literal pathspecs follow `--`; source symlinks cannot escape the module. Every
subprocess uses an argument list with `shell=False`.

Changed **lines** come from a second diff, `git diff -U0 --merge-base <base> -- bundles/`, whose
base side is the merge base of `<base>` and the checked-out revision and whose new side is the
**working tree** - the exact content `Build` compiled and PIT mutates - so every mapped line
number describes the file that was actually mutated. Rename detection runs over the whole
`bundles/` diff rather than per path, so a renamed-and-edited file contributes only its edited
lines and a pure rename contributes none; deleted lines contribute no new-side line, and a deleted
file (`+++ /dev/null`, excluded from the selection anyway) contributes nothing. Hunks are
attributed through their `+++ b/<path>` header and kept only for already selected sources; a hunk
that precedes any file header, an unparsable hunk header and a path Git had to quote all fail
selection instead of silently reporting a file as untouched. `selection.json` records `modules`
(what to mutate), `changed_lines` (what to judge, one entry per selected source), and
`dispositions_base` (the dispositions file at the merge base, or null when absent); `run` refuses
a manifest that does not map every selected source rather than judging nothing.

The checkout is the PR merge revision, matching `Build`. JVM `SourceFile` attributes map source
files to **exact compiled class names**, including nested/anonymous classes, extra top-level
classes and Kotlin file facades. No prefix glob or whole-module fallback broadens mutation scope.
Unmapped sources fail analysis explicitly (for example a file with no emitted bytecode); justify
these individually in the PR. New Gradle modules need an entry in `MODULES` in
[pr_mutation.py](../.github/scripts/pr_mutation.py).

Test selection is resolved **per changed source** and is never widened to the module suite.
PIT's coverage phase has to execute every class in `targetTests`, and Sirix test classes open
databases during JUnit 5 discovery, so whole-module coverage collection can exhaust the budget
before mutation analysis starts. Explicit `LIFECYCLE_TESTS` associations select dedicated ownership,
worker-fence and callback regressions instead of unrelated package histories.

Test identities come from declared top-level Java/Kotlin type
names and packages, not source
filenames or directories. Discovery does not require compiled test classes. For a source `Foo`
in package `p` the first matching step wins:

1. `p.FooTest` or `p.FooTests`.
2. Any test in the module whose simple name starts with `Foo` - or with `Foo` minus a leading
   `Abstract` or trailing `Impl` - at a word boundary and ends with `Test`, `Tests` or `IT`
   (so `KeyValueLeafPage` reaches `KeyValueLeafPageSlotTest`, but `Page` never reaches
   `PagerTest`).
3. Tests in `p` and its subpackages ending with `Test`, `Tests` or `IT`, only when there are at
   most `PACKAGE_TEST_LIMIT` of them.
4. Otherwise the source is **unmapped**.

To select an existing behaviour test that the name-based rules miss, add an association in
[`.github/mutation-test-associations.toml`](../.github/mutation-test-associations.toml).
[CONV-204](../spec/CONV-conventions.md#conv-204--lane-test-associations) owns the schema,
declared-class ownership rules, validation, test-cap rules and PIT baseline exclusions. The file
also owns the reviewed association inventory. The runner augments selection with its
`CONTRACT_TESTS` release-contract mappings; those mappings count toward the same selection cap.

Mutants are generated **only on the PR's own lines**. The lane writes each module's changed-line
map and activates `+CHANGEDLINES(file[...])`, the interceptor in
[`tools/pit-changed-lines`](../tools/pit-changed-lines) on PIT's tool classpath, which removes
every mutant of the analysed classes whose source file and line that map does not list. The
feature takes that one parameter: PIT 1.25.5's parser reads a feature's first parameter and
silently ignores the rest, so the census location is derived (`census.jsonl`, beside the map)
rather than configured, and a test pins the parser behaviour that forced it. It runs
before any sampling - PIT sorts a compound interceptor pipeline by `InterceptorType` and runs its
`CLASSLIMIT` limit as a `FILTER`, and a test asserts that ordering against the pinned release - and
the lane configures no per-class limit at all, so every mutant on a changed line is analysed and
the changed-line counts are a complete measurement rather than a sample.
[CONV-205](../spec/CONV-conventions.md#conv-205--pr-mutation-lane-mutant-scope) owns these rules.

The interceptor records what it removed in `census.jsonl`, beside the map - identity, class,
source file and line, never a status. A record the reporter cannot read names the field at fault
and fails the module, because the interceptor wrote it. The reporter re-derives each census entry's attribution from its own changed-line
index and **fails the run** when one of them is on a changed line, so a defect in the interceptor
cannot silently drop a mutant the verdict reads. Dispositions entries are matched against the
census as well as the report, which keeps existence, staleness and re-identification after a
refactor judgeable without running a single unchanged-line mutant (CONV-202).

This replaced per-class sampling, which cost almost the whole budget on mutants no verdict reads:
on [PR 1353](https://github.com/sirixdb/sirix/pull/1353) 1,098 of 1,132 test-case executions served
unchanged-line mutants, and three surviving return-value mutants on unchanged `commit()` default
methods took 959 of them, while a killed mutant costs about 1.3 test cases. To obtain every mutant
of a class, including those on unchanged lines, omit `-PprMutationChangedLinesFile` from a
focused Gradle rerun using the artifact's `targets.txt`, as in the example below; `prMutationMaxMutations` still
limits such a rerun per class if you ask for it.

A source that resolves to nothing never changes the scope chosen for the other changed sources;
the union of their per-source selections is not truncated. Each source keeps the named, then
related, then package-fallback resolution rules; `PACKAGE_TEST_LIMIT` bounds package fallback,
and a source whose own candidate set exceeds `SOURCE_TEST_LIMIT` is marked `over-limit`,
including named matches and associated tests. The former union cap is removed so collecting
coverage once preserves the tests selected by the former partitions; it no longer bounds coverage
at the cost of dropping another source's tests. Selected names receive a
trailing `*` to include nested classes; other classes sharing that prefix can also match.
When any tests are selected, all changed classes remain in `targetClasses`; mutants without
coverage from those tests report `NO_COVERAGE`. A source without its own mapping may still be
covered by tests selected for another source. `results.json` records unresolved sources
(`unmapped` or `over-limit`) and every source's resolution kind; the job summary lists unresolved
sources. A module where nothing maps reports state `unmapped` instead of running PIT without a
baseline. This focused selection can omit integration assertions; if a survivor is
already killed by a broader deterministic test, rerun with that test included and link the report.
Random jqwik/property exploration, the random model oracle, HOT structural properties and soak
suites are excluded from the PR mutation baseline; keep exploration in its normal test lane and
promote discovered seeds to deterministic regressions. The weekly curated core PIT task is unchanged.

The `pr-mutation` artifact contains source selection with its changed-line map, exact targets and
test filters, every undetected mutant in `results.json`, HTML/XML reports, logs and `summary.md`.
The job summary shows, per module, the state, the number of mutated classes and test patterns, the
unmapped sources, the changed-line counts that decide the outcome (changed lines, mutants on them,
killed, survived, no coverage), the whole-report killed/total separately from PIT's detected/total
score (which includes timed-out mutants), status counts, up to 100 failing changed-line mutants
and - separately and never as a score - how many mutants the restriction removed, with a pointer to
the [scheduled full-scope report](#scheduled-full-scope-mutation-report) that measures them; the
artifact retains the full census. `results.json` holds `modules` (one
result per module, with that split under `changed` and `unchanged`), `dispositions` and
`associations`; each module's `changed` lists its `failing`, `dispositioned_equivalent` and
`dispositioned_out_of_lane` mutants. Each module's `unattributed` counts mutants whose source file
never mapped to a changed source (normally zero, since `targetClasses` holds only changed classes).
Setup failures, malformed XML, timeouts and unexplained empty or missing reports are incomplete,
not 100%; the census-explained empty-scope exception is defined by
[CONV-205](../spec/CONV-conventions.md#conv-205--pr-mutation-lane-mutant-scope).

### Blocking criterion: the PR's own lines

[CONV-202](../spec/CONV-conventions.md#conv-202--pr-mutation-verdict-with-dispositions)
owns the clean-run criterion, including reviewed dispositions; association validation is
specified by [CONV-204](../spec/CONV-conventions.md#conv-204--lane-test-associations).
A mutant sits on a changed line when its `<sourceFile>` and the package of its `<mutatedClass>`
match a changed source and its `<lineNumber>` is in that file's changed-line set; nested,
anonymous and extra top-level classes and Kotlin file facades are therefore attributed to the
file they were compiled from. Use the [disposition helpers below](#reviewed-dispositions) to
inspect claims alongside a local report.

Mutants on **unchanged** lines of the same classes are no longer analysed here: they are generated,
listed in the census and removed before any test runs. They were always advisory - pre-existing
test debt the PR did not introduce, never a reason to fail the job - and they were also where the
budget went, because an undetected mutant has to run every covering test to the end. On
[PR 1354](https://github.com/sirixdb/sirix/pull/1354) all 17 changed-line mutants were killed
while 348 survivors and 2,828 uncovered mutants sat on unchanged lines of the same classes; a
whole-class criterion leaves every larger PR red however completely its own change is tested. That
debt is now measured by the [scheduled full-scope report](#scheduled-full-scope-mutation-report)
and reduced in dedicated work, never by widening a PR. Disposition validation still applies to
entries matching censused mutants (CONV-202).

Incomplete analysis still fails on its own, independently of any line: a module that timed out,
errored, mapped to no test (`unmapped`), produced zero mutations, or attributed none of its
mutants to a changed source is never clean - an empty attribution judges nothing and must not
read as a pass. One case is not incomplete: a run that executed, generated mutants for its target
classes and had **none of them on a changed line**. Its census proves that, so the module is
complete with zero mutants judged and claims nothing - the case of a diff that touches only
comments, declarations or lines PIT does not mutate. Zero mutants with an empty census is still a
broken run. The Gradle `mutationThreshold`/`coverageThreshold` stay at 0, so PIT itself never
decides the outcome.

**Known limitation.** Sampling can no longer hide a changed line, since the lane applies no
per-class limit and judges every mutant it generates on one. What remains is that PIT generates no
mutant at all for some changed lines - a declaration, a line its own equivalence filters remove -
so a module can still report `0` mutants judged out of many changed lines. The summary prints a
module's changed-line count beside its changed-line mutant count and its census count, so that
case stays visible rather than silent. Mutant-level staleness of an entry whose mutant sits on an
unchanged line is also outside this lane's reach: the census proves the mutant still exists, and
whether it still survives is answered by a focused rerun omitting
`-PprMutationChangedLinesFile` or by the scheduled full-scope report.

### Scheduled full-scope mutation report

The debt the PR lane stopped analysing is measured once a week instead, by
`PIT full-scope unchanged-line report` in
[verification.yml](../.github/workflows/verification.yml) - the deep-verification workflow, whose
only triggers are its Sunday 04:00 UTC cron and a manual dispatch. It never runs for a pull
request and never decides an outcome;
[CONV-206](../spec/CONV-conventions.md#conv-206--scheduled-full-scope-mutation-report) owns the
rules.

It reuses the lane's selection and reporting with the changed-line restriction disabled and no
per-class limit: its base is the last first-parent commit on the default branch before the window
(`--window-days`, 7 by default), its head is `HEAD`, so its scope is the sources the branch changed
in that window, resolved by exactly the rules above. Its budget is its own and the PR lane's limits
are untouched. The job summary and the `full-scope-mutation` artifact carry, per class, the mutants
no test detected, and - advisory only - the dispositions entries this complete report proves stale,
which the PR lane's restricted scope cannot judge. A run that did not finish says so; an incomplete
report is not a clean debt report. The weekly curated `:sirix-core:pitest` job in the same workflow
is a different measurement (three correctness-critical classes against their own suites) and is
unchanged.

### Reviewed dispositions

Resolve mutants under [CONV-103](../spec/CONV-conventions.md#conv-103--specification-before-implementation).
For a reviewed disposition, edit
[`.github/mutation-dispositions.toml`](../.github/mutation-dispositions.toml), which includes an
example entry. [CONV-201](../spec/CONV-conventions.md#conv-201--mutation-disposition-record)
owns the schema, equivalence standard and counterfactual evidence requirements;
[CONV-202](../spec/CONV-conventions.md#conv-202--pr-mutation-verdict-with-dispositions)
owns the acceptance and re-confirmation rules. Review each added claim in the job summary under
[CONV-203](../spec/CONV-conventions.md#conv-203--disposition-review-abuse-limits-and-refactoring).

After compiling the selected modules and generating PIT reports, use the local helpers below.
Check and remap read the reports without changing files. Counterfactual temporarily applies a
patch and runs the supplied test command before restoring the sources; its supported patch
operations are defined by CONV-201. CI never runs these helpers, and none writes the
dispositions file.

```bash
# Judge entries against local PIT reports as the lane would (stale, re-confirmation, unconfirmed).
python3 .github/scripts/pr_mutation.py dispositions --check --base "$(git rev-parse origin/main)" \
  --report /tmp/pr-mutation/sirix-core/report/mutations.xml \
  --census /tmp/pr-mutation/sirix-core/census.jsonl
# Propose new identities for entries a rename, move or edit left stale; review before using.
python3 .github/scripts/pr_mutation.py dispositions --remap \
  --report /tmp/pr-mutation/sirix-core/report/mutations.xml
# Evidence for killed-out-of-lane: the test passes, fails with the injected mutant, and the
# sources are restored byte-identically (SHA-256 checked). Prints the [mutant.evidence] table.
python3 .github/scripts/pr_mutation.py counterfactual --patch mutant.patch -- \
  ./gradlew :sirix-core:test --tests 'io.sirix.SomeAllocationBudgetTest'
```

Pass `--sampled` to `--check` when the reports came from a rerun with a per-class limit, and
`--census /tmp/pr-mutation/sirix-core/census.jsonl` to judge entries whose mutants the lane
removed off the diff exactly as the lane judges them. Census validation failures are printed and
fail the checker; invalid evidence never proves a complete generated set. Valid census-only classes
are included when checking staleness.

Keep the lane artifact layout intact when checking its reports: `results.json` at the output
root, `<module>/report/mutations.xml`, and `<module>/census.jsonl`. The shared evidence loader
reads the matching module's completion and sampling state from that existing manifest. Failed,
timed-out, partial or unknown runs cannot authorize a disposition, and cannot prove a complete
generated set. Standalone XML without matching completion metadata is reported as unknown and
fails the check; its detected statuses still contradict or supersede entries, and remap can
still propose identities. A per-class mutant limit withholds completion-based judgements, including survivor
acceptance and absence-based staleness. Target membership is loaded together
with completeness from each module's saved `targets`, including classes that emitted no mutant;
it is never reconstructed from XML or census identities alone.

Run completion and loaded-evidence completeness are distinct. The helper keeps one module
value recording supplied reports/censuses, whether each was read completely and validly,
and recorded census failures. Report and census paths are resolved against the working
directory before deduplication and grouping, so mixed absolute/relative spellings share one
module value. Its named rejection facts control acceptance, both dispositioned counts, staleness,
empty-scope eligibility and status provenance for rule 1. Census violations reject all
those claims even when PIT exited zero; rule 4 still uses preserved statuses. A restricted run's nonempty report and census must both be supplied;
report-only or census-only input cannot prove an absent identity stale. A missing helper path
is not a supplied empty artifact. The lane can record a known zero count because it clears
artifacts before running; that saved zero count attests that a component contributes no missing
identities. Otherwise omitted components remain unproven. Recorded census failures still fail
the checker even when its census argument is omitted. Missing evidence alone leaves an unmatched
existing entry not judged, without failing it; a changed-line survivor still cannot be accepted
from incomplete evidence. `--sampled` withholds completion-based judgements as well.

Measured churn of pure refactors (PIT 1.25.5 on a four-method sample class whose six surviving
mutants were taken as reviewed entries): a line shift, an extract-method and a method reorder
touch no entry; a rename leaves two entries stale, each remapped to one candidate marked as
keeping its instruction indexes; an instance-to-static move leaves two stale with two
same-mutator candidates each, chosen by line; an edit inside a method requires re-confirming
that method's three entries, and remap re-identifies them. After applying the proposals no
entry fails in any of the six cases.

The reporter itself is mutation-tested with mutmut under the same rules; its per-mutant record
of equivalent survivors is
[`.github/scripts/tests/MUTATION_DISPOSITIONS.md`](../.github/scripts/tests/MUTATION_DISPOSITIONS.md),
including the limits of the historical aggregate after review corrections.

The job runs on normal `pull_request` events, including forks and Dependabot, with only
`contents: read`, no secrets, no persisted checkout credentials and read-only Gradle caches.
Never use `pull_request_target` or add credentials to run untrusted PR code. Reports are artifacts
and job summaries, so reporting needs no PR write token.

**Required or advisory:** the job is **advisory** by decision. It has `continue-on-error`, so it
reports without gating: its scope is the PR's own changed lines, a red result is normally a
test-quality finding for the author rather than a broken build, and the per-source test mapping
is a heuristic that can legitimately find no test. It stays visibly informational even when
the [CONV-202 verdict](../spec/CONV-conventions.md#conv-202--pr-mutation-verdict-with-dispositions)
fails its analysis step. Once representative PRs meet that criterion, maintainers can set the
repository Actions variable
**`MUTATION_BLOCKING=true`** and require `PIT changed-source mutation testing` in branch
protection. That single switch removes job-level tolerance; the reporter already applies the
CONV-202 verdict. Revert the variable to restore informational rollout while investigating an
infrastructure failure.

Local reproduction (JDK 25; compile the selected modules' `classes testClasses` first):

```bash
python3 -B -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'
python3 .github/scripts/pr_mutation.py select \
  --base "$(git rev-parse origin/main)" --head "$(git rev-parse HEAD)" --output /tmp/pr-mutation
python3 .github/scripts/pr_mutation.py run --output /tmp/pr-mutation --seconds 1320
# Add --blocking to enforce the clean-run criterion locally (CONV-202, CONV-204).
```

For a broader deterministic test selection, reuse the artifact's `targets.txt` and run:

```bash
./gradlew :sirix-core:pitest -PprMutationModule=sirix-core \
  -PprMutationTargetsFile=/tmp/pr-mutation/sirix-core/targets.txt \
  -PprMutationTests='io.sirix.utils.NamePageHashTest*' \
  -PprMutationReportDir=/tmp/pr-mutation-expanded
```

That rerun passes no `-PprMutationChangedLinesFile`, so it mutates every line of its targets.
Omitting the map is the sole control for unrestricted analysis. Add
`-PprMutationMaxMutations=<n>` to sample such a rerun per class; the lane itself passes the map
and applies no per-class limit.

The opt-in [Gradle configuration](../gradle/pr-mutation.gradle) uses the existing
[Gradle PIT plugin](https://gradle-pitest-plugin.solidsoft.info/) and versions, and adds
`tools/pit-changed-lines` to PIT's own `pitest` tool classpath. The existing query/Kotlin CI job
executes `:sirix-pit-changed-lines:test`, including its adversarial tests and ordering proof
against the pinned PIT release, without allocating another runner.

Each module is analysed by exactly one PIT invocation, so its slow test scope is covered once per
run. The single-use daemon described [above](#pr-mutation-testing) has a bounded heap
(`-Dorg.gradle.jvmargs=-Xmx3g`), rather than the workstation-sized heap in `gradle.properties`.
The PR mutation job uses the Build job's Error Prone compiler
inputs and restores its compiled output plus project-local Gradle task history. The existing
output cache remains shared as before; a separate PR-only history archive supplies `.gradle`
and `**/.gradle` only to the mutation consumer, so the other restores remain unchanged. Matching
inputs and history keep compilation up to date; this pull request's own CI task outcomes provide
the evidence of reuse and its effect, with no measured saving claimed here.

Follow-up outside this change: the core/query test, Sonar and REST output-cache consumers still
omit the producer's Error Prone setting, so their compiler inputs differ. Their restores and
compiler policy are unchanged. Platform lanes also keep their existing compiler/cache policy;
they currently compile cold rather than restore the Linux output archive. The scheduled mutation
job compiles its own output with matching inputs in the same job and restores no Build archive.

Minions reuse each module's final `test` task `opens`/`exports` and system properties, including
late native-image properties and values containing commas or whitespace. The
[Gradle configuration](../gradle/pr-mutation.gradle) owns the encoding for PIT's comma-separated
CLI and Java launcher argument file; those arguments remain intact across both transports.

## Running the layers

The normal core test task also runs `ValidTimeDisabledLookupCacheTest` in a separate JVM with
`sirix.hotLookupCache.maxEntries=0`, since the buffer manager reads that setting once at startup.
It checks fresh interval roots and all companion postings after truncation reuses a revision number,
and the surviving historical revision, under all four versioning types. Run it alone with
`./gradlew :sirix-core:validTimeDisabledLookupCacheTest`; enabled-cache tests retain their usual settings.
The normal core task also runs the allocation suites selected by
[`dictionaryReadViewAllocationTest`](../bundles/sirix-core/build.gradle) in a separate JVM so
inline Mockito mocks in other suites cannot affect their allocation measurements. Run that fork
alone with `./gradlew :sirix-core:dictionaryReadViewAllocationTest`. For only the HOT commit
allocation checks, add `--tests 'io.sirix.index.hot.HOTCommitAllocationBudgetTest'`; the
[HOT specification §2.6](HOT_INDEX_SPECIFICATION.md#26-partial-keys-and-routing) owns their allocation contract.

The normal core task also runs `StorageEpochStoreIdentityTest`,
`StorageEpochDifferentialTest` and `StorageEpochFsstInlineDecisionTest` in a separate JVM
with deterministic codec selection (`sirix.codecBakeoff.probeInterval=1`). Run them alone with
`./gradlew :sirix-core:storageEpochStoreIdentityTest`. They compare rotating and
never-rotating transactions, including persisted page bytes in the identity suite
and seeded edits across storage, compression and dictionary lanes in the differential
suite, with cold-reopen checks. The inline-decision suite pins the hash kind and checks that
FSST-encodable values make the same inline-or-carrier decision and keep the same table binding
across spilling epochs and the async-flush import mode; `-Dsirix.test.inlineDecision.seed=<n>`
adds one exploratory seed. The identity suite inherits the JVM hash default, so also run it with
`-Dsirix.hashType=ROLLING` when touching epoch or FSST code to cover the hashed configuration.

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
# Records each shrunk failure with its case configuration, including eviction and frame poisoning
# (-Dsirix.hot.property.kinds=CAS,PATH,NAME,VALIDTIME,PROJECTION narrows it)
./gradlew :sirix-core:test --tests 'io.sirix.index.hot.HOTStructuralPropertyTest.extendedBudgetAcrossEveryKind' \
                           -Dsirix.hot.property.collect=true -Dsirix.hot.property.heavy.seeds=8 \
                           -Dsirix.hot.property.heavy.ops=15000 -Dsirix.hot.property.failureDir=/tmp/hot-property
# Replay one recorded failure under its recorded checks and adversarial settings. The CaseConfig
# header parser in HOTStructuralPropertyTest owns the format and legacy defaults.
# -Dhot.diag.validationDump=true makes the writer's post-publication validator describe the node
./gradlew :sirix-core:test --tests 'io.sirix.index.hot.HOTStructuralPropertyTest.replayRecordedFailure' \
                           -Dsirix.hot.property.replayFile=/tmp/hot-property/hot-property-failures/failure-cas-1-full.txt \
                           -Dhot.diag.validationDump=true

# Work budgets (includes TemporalAggregateCatchUpWorkBudgetTest for lag 0, 1, 8, 16, 17 and folded lag 64)
# Add -Dsirix.workBudget.print=true -i to print every captured counter table
# The query test task also runs BitemporalLoaderBudgetTest in a clean JVM with blocking
# compilation, via :sirix-query:bitemporalLoaderAllocationTest; run that task alone to isolate it.
./gradlew :sirix-query:test --tests 'io.sirix.query.budget.*' \
  --tests 'io.sirix.query.json.IndexedArrayPositionTest'
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
6. A green build is weak evidence when one author (human or model) wrote both the code and its
   tests: check that every test traces to a requirement and would fail on a plausible wrong
   implementation. The oracle and property layers exist precisely because they are independent of
   that author. See the
   [development strategy](DEVELOPMENT.md#independence-without-mandatory-separate-authorship).
