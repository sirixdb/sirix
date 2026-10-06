# Runtime revision routing for CAS lookups

CAS routing preserves `jn:open`'s timestamp and `jn:doc`'s integer revision as an
expression evaluated for each input tuple. The timestamp lookup uses the resource
session's revision-by-instant lookup. Its bounded memo belongs to the query context,
not a cached plan, and is invalidated when the resource publishes another revision.
Revision arguments retain Brackit's original function conversion and cardinality
rules, including untyped atomic values and integer range checks.

Literal field equalities in a FLWOR `where` or array filter can select a CAS row
source, including a document whose root is an array. The full predicate stays in
place: the other conjuncts still filter the selected objects. At the evaluated
revision, routing rechecks the index catalogue and falls back to the original
source if the index is unavailable. Legacy path, name and CAS rewrites also retain
the revision operand and validate their compiled index definitions at execution.

Multiple matching rows retain array order. Dewey IDs supply document order when
enabled; otherwise the existing revisioned valid-time array evidence can certify
that sorted node keys have array order. Without either proof, the original array
source runs. This conservative fallback matters after inserts and moves. Single
point matches do not require an ordering proof.

The SH1 loader creates integer CAS paths `/[]/id` and `/[]/pid` on contracts and
`/[]/id` on products at E0, after automatic valid-time index creation. The
half-open valid-time rewrite and its residual handling remain independent.

## Verification

All Gradle and Java invocations run through the task's two-slot `heavy` limiter,
which admits a JVM only with at least 6 GiB available. This task uses an initially
empty private Maven repository at
`build/runtime-cas/m2-private`, never `~/.m2`. The resolved Brackit snapshot jar has
SHA-1 `c9dc857dedcfda2bc92c61a45796db58b58847e1` and a Maven build timestamp of
2026-10-06 15:22:13 UTC. It matches the [published checksum for snapshot 93](https://central.sonatype.com/repository/maven-snapshots/io/sirix/brackit/1.0-alpha10-SNAPSHOT/brackit-1.0-alpha10-20261006.152144-93.jar.sha1).

Gradle flags: `--no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx2g
-Dmaven.repo.local=<worktree>/build/runtime-cas/m2-private
-PtestHeapMin=256m -PtestHeapMax=2g`.

- `RuntimeRevisionCASTest`: historical filter and correlated FLWOR queries on all
  four versioning types, prolog and integer operands, pre-index revisions, and
  array order after insertion, function argument conversions and invalid inputs,
  with executable optimized-plan assertions.
- `CASLookupWorkBudgetTest`: 50 publication-row lookups among 1,000 objects read
  bounded candidate nodes and resolve only the two distinct instants; publishing
  another revision invalidates the memo.
- Full `:sirix-core:test`: 13,156 tests, 78 skipped, zero failures/errors.
- Full `:sirix-query:test`: 3,005 tests, 12 skipped, zero failures/errors. Its
  separate generic reader-lifetime fork also passed all 16 tests.
- All existing work budgets documented in `VERIFICATION.md` passed without
  changing their bounds. `spotlessJavaApply` completed for both modules;
  `:sirix-core:spotlessCheck :sirix-query:spotlessCheck` passed on the final tree.
- No-mistakes validation and shipping follow the committed implementation handoff.

## SH1 measurements

The baseline is commit `842f48e080ef3897c4fc421d05650dc2a7c35e33` with the original kit indexes.
The input is the campaign's t100k stream (234,884 events, 25 publications).
All generated stores and outputs stay under `build/runtime-cas/`. Authoritative
timing logs are `before-original.log` and `after-final.log`; every retained and
discarded repetition matched the oracle. The after run uses the final compiled
production classes from the successful full-suite build. A task-local
copy of the loader permits that output directory; loading logic and JVM flags
are unchanged. The before runtime uses an isolated overlay compiled from the initial commit's original versions of every changed production class.

Protocol: one warm process, ten repetitions for Q1/Q2/Q3/Q10 and three for Q5,
discard the first, report medians. Each repetition compiles, fully consumes,
serializes and canonicalizes the result. Every canonical TSV is byte-compared
with the campaign oracle before admitting a timing. JVM flags are `-Xms512m
-Xmx2g -XX:MaxDirectMemorySize=1g`, with preview and the vector module enabled.

| Query | Before, total ms | After, total ms | After, execute ms |
|---|---:|---:|---:|
| Q1 | 90.839 | 13.214 | 3.797 |
| Q2 | 63.895 | 6.921 | 2.212 |
| Q3 | 246.391 | 8.980 | 4.640 |
| Q5 | 1443.498 | 30.454 | 22.676 |
| Q10 | 1059.407 | 1059.288 | 1050.943 |

These baseline figures differ from the original profiling report because this
checkout already includes subsequent optimizer and valid-time work. Q10 has no
literal business-key equality; its joins and interval-overlap predicates remain
outside this CAS point-lookup rewrite.

The run does not reach a 1 ms total for Q1–Q3: query compilation alone takes
several milliseconds, and point-query execution is approximately 2–5 ms.
Q5 execution takes 28.720 and 16.632 ms in the two retained repetitions, a
22.676 ms execution median; its 30.454 ms total remains above the 10–25 ms
target. The total includes compilation, serialization and
canonicalization. Component medians are independent and need not sum to the
total median. These runs share the laptop with other workers. No timing is a
test assertion.

Mutation verification uses separately compiled overlay classes, leaving the
production source and running validation builds untouched. Removing CAS routing
keeps the answer but changes 50 indexed transactions to zero; bypassing the memo
keeps the answer but changes two revision-by-instant calls to 50. Substituting
the latest revision makes the historical value 20 instead of 10 on all four
versioning types. The healthy budget passes, and each mutation fails.

A separate before/after run also reproduced rejection of a valid untyped timestamp
when bypassing the original function conversion. Reusing Brackit's converter
passes all 13 revision tests, including numeric narrowing and invalid argument
checks.
