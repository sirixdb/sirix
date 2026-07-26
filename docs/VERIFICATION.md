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
| Concurrency invariant harnesses | Eviction watermark safety, slot-allocation exclusivity, guard/close protocol, cache weight accounting under contention | `RevisionEpochTrackerWatermarkSafetyTest`, `ShardedPageCacheInvariantStressTest` |
| Crash injection / soak | Durability across simulated crashes; leak-free long-running bitemporal workloads | `crash/CrashRecoveryInjectionTest`, `stress/BitemporalSoakStressTest`, `stress.yml` workflow |
| Mutation testing (PIT) | The tests **assert** on behavior instead of merely executing it — a surviving mutant is a code change no test noticed | `:sirix-core:pitest`, `verification.yml` workflow |
| Error Prone + NullAway | Compile-time rejection of almost-always-bug patterns and nullness-contract violations | `-PerrorProne`, `verification.yml` workflow |
| SonarQube / Checkstyle | Style and maintainability smells | `sonarqube.yml`, `checkstyle.xml` |

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
used in the codebase. Both are opt-in (`-PerrorProne`) so the everyday build is unchanged, and run
in CI via the `Deep verification` workflow.

## Running the layers

```bash
# Generative + oracle + regression tests (fast, part of the normal test task)
./gradlew :sirix-core:test --tests 'io.sirix.property.*' \
                           --tests 'io.sirix.diff.JsonDiffSerializerStaleTupleRegressionTest'

# Concurrency invariant harnesses
./gradlew :sirix-core:test --tests 'io.sirix.access.trx.RevisionEpochTrackerWatermarkSafetyTest' \
                           --tests 'io.sirix.cache.ShardedPageCacheInvariantStressTest'

# Mutation testing (report: bundles/sirix-core/build/reports/pitest/index.html)
./gradlew :sirix-core:pitest

# Static bug detection
./gradlew :sirix-core:compileJava :sirix-core:compileTestJava -PerrorProne
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
5. Never accept a green build as proof when the same author (human or model) wrote both the code
   and its tests — the oracle/property layers exist precisely because they are independent.
