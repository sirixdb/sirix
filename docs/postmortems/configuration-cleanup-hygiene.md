# Startup configuration and cleanup ownership failures

- Fix: [configuration/Cleaner/native-metadata change](https://github.com/sirixdb/sirix/pull/1351), architecture audit V08, V09 and V11.
- Scope: startup policy, resource ownership and native metadata. Row/page/query kernels and work-budget bounds are unchanged.

## Configuration: accepted settings did not establish the advertised policy

| Trigger and symptom | Root cause / missing oracle | Regression |
| --- | --- | --- |
| Set legacy metadata bytes and observe the same cache capacity. | The byte constructor ignored its argument while logging byte settings for a count-based cache. Tests did not assert admission and eviction at the requested limit. | `PageCacheConfigurationTest.legacyBytesSetAnEffectiveCountAndExplicitEntriesTakePrecedence`, `constructorEstimateUsesTheSameUnits`; real `IndirectPage` admission, retained count and eviction count. |
| Supply malformed, nonpositive or overflowing caps. | Parsing restored a default on failure and multiplied without checking overflow. An overflow can wrap back to a positive, apparently valid cap. | `ConfigurationPropertiesTest.explicitInvalidCapsFailWithThePropertyAndValue`, `binaryUnitBoundariesMatchAnArbitraryPrecisionOracle`, `AllocatorCapConfigurationTest.refusesInvalidAllocatorRequests`. |
| Initialize the process allocator, then request another allowance through a persisted database or pre-database cache access. | Cache sizing used the requested budget, or a hardcoded 2 GiB pre-database budget, instead of the allocator's first effective allowance. Log percentages also disagreed with the formulas. | Isolated `ConfigurationStartupTest` and `AllocatorColdStartupTest`: actual cache weights, odd allowance, persisted requests, captured effective-value logs, and two/three-argument reinitialization. |
| Override only the child threshold. | Child and descendant thresholds read the same key, so tests using only defaults missed the collision. | `SirixTranslatorThresholdTest.startupThresholdsAreIndependent` in default, independent-override and legacy-child-only JVMs. |
| Launch REST with an operator configuration. | `main` replaced its arguments with a developer's absolute path. | `MainTest.main forwards all launcher arguments`: immutable expected arguments observed at the Vert.x boundary; independent live HTTP configuration and write/read round trip. |
| Select global arenas or skip arena close without diagnostic opt-in. | Process-lifetime escape hatches were selectable as ordinary startup policy. | `SharedArenasTest.productionRejectsProcessLifetimeOverrides`, four isolated `SharedArenaStartupPolicyTest` modes, and `ArenaGenerationLifetimeTest` checking actual segment liveness. |

Public-API controls executed the audited baseline and the merged fix in fresh JVMs. Baseline controls accepted malformed caps, wrapped `17179869185G` to 1 GiB, admitted 12 metadata entries for a two-entry request, and sized caches from 2 GiB despite an effective 67,108,871-byte owner. Each control failed its independent contract assertion before the fix and passed afterward. These controls load the audited configuration/cache classes ahead of the current dependency classpath and exercise their public APIs; each startup-cap case uses a fresh JVM. The signed-wrap value is also a deterministic parameter of `explicitInvalidCapsFailWithThePropertyAndValue`.

The launcher regression was also run with the exact audited `Main.kt` through the existing mocked Vert.x boundary. The old entry point failed with `ArgumentsAreDifferent` because it supplied the developer config path; restoring the corrected entry point made the same test pass. The control never starts the old live server.

The metadata byte option now documents an estimate (1,000 bytes per entry, minimum one), not a retained-heap byte guarantee. The explicit entry option wins. Invalid explicit caps fail with their property and value; allocator requests are startup-only and limited to 1 TiB. Defaults remain 50,000 metadata entries, 50% record cache and 18.75% fragment cache of the effective allowance.

## Cleanup: sharing a service requires independent actions and clean ownership

A single synchronous Cleaner callback thread would serialize unrelated fallback actions. Normal close must remain primary, while abandoned owners must neither retain themselves nor block other fallback actions. `SirixCleanerTest` checks real collection, synchronous/one-shot close, propagated failure, 32 gated close races, and a deliberately blocked fallback while another completes. Writer, slab-lease and prefetch teardown regressions preserve caller ownership.

Explorative review found a second failure in the initial shared-service implementation: a plain `new Thread` factory inherited its creator's application thread locals. The real GC callback observed `registering-application-context`. `CleanerStartupTest.firstRegistrationDoesNotInheritApplicationThreadLocals` failed before replacing that factory:

```text
expected: <null> but was: <registering-application-context>
```

The same fresh-process test passes with the JDK's default Cleaner factory. Keep its isolated fork: a service initialized by another test would make the negative control vacuous.

PIT also exposed missing observations of normal-close deregistration. `WriterCleanerLifetimeTest.normalCloseDeregistersWhileTheWriterStaysReachable` holds the closed writer live, requires the actual registration to become collectible, and rejects a normal-close leak warning. It kills removal of both caller deregistration and its `Cleanable.clean()` call. Registration bookkeeping stays in the existing cold diagnostic state, preserving the writer's instance-field list.

## Native metadata: deletion left a stale consumer declaration

The removed allocator's `jlong(jint)` and `void*(jint)` descriptors remained in benchmark reachability metadata. The adjacent `jint(jint)` descriptor still belongs to `close`. A normalized JSON comparison proves exactly the two removals and preservation of every other value; the native-image build and 49 executable smoke checks verify the consumer. This is a declarative contract check, not a source-substring assertion.

## CI: daemon heap did not configure forked javac

The first Deep verification run exhausted javac's 512 MiB default while compiling query as an MCP dependency. The Gradle daemon's heap did not apply to its compiler worker. The shared Error Prone configuration now sets a 2 GiB compiler-worker cap for each Java source set, without changing ordinary builds or disabling analysis.

The repair was verified by forcing core, core tests, query, MCP main and MCP test compilation rather than accepting a cached core build. [The subsequent Deep verification run](https://github.com/sirixdb/sirix/actions/runs/43684989597) passed both Error Prone/NullAway and curated PIT on the repaired head.

## CI: diagnostics and isolated startup suites crossed execution boundaries

- Symptom / trigger: the allocator allowance warning corrupted CLI query and history stdout on
  Linux, macOS and Windows in [the PR run](https://github.com/sirixdb/sirix/actions/runs/43708637917).
- Invariant: command stdout contains only command results; enabled diagnostics go to stderr.
  This holds for XML/JSON queries, history dumps and other commands sharing core/query logging.
- Root cause: the new warning exposed shared main/test ConsoleAppenders targeting stdout. Earlier
  output tests had not exercised an allocator request differing from the effective allowance.
- Regression: `QueryTest` (all five cases) and `DumpResourceHistoryTest` failed locally before the
  fix. `ConsoleLoggingTest` executes all four packaged main/test Logback configurations and emits
  independent warning/info witnesses, requiring empty stdout and both witnesses on stderr.

The same run's PIT artifact records a shared 1,320-second deadline exhausted during module-wide
baseline coverage, before mutation results. Its baseline also ran `AllocatorColdStartupTest`,
`ConfigurationStartupTest`, `StorageEpochStoreIdentityTest`, `ValidTimeDisabledLookupCacheTest`
and the dictionary allocation suite outside their required forks.

- Invariant: a test excluded from the normal shared JVM must also be excluded from PIT's shared
  JVM. This includes allocator/configuration/Cleaner startup, all arena-policy modes, storage-epoch,
  disabled-cache and allocation suites; their dedicated Gradle tasks retain responsibility for them.
- Root cause: PIT copied normal JVM arguments/properties but ignored the normal test exclusions.
  Adding new first-initialization fixtures made that mismatch observable immediately.
- Regression: a focused real PIT invocation selecting `ConfigurationPropertiesTest*` together with
  `AllocatorColdStartupTest*` fails before mutation on the old configuration. Carrying the normal
  exclusions into PIT removes the incompatible fixture while preserving the ordinary mutation suite.
  No shared deadline or mutation target is widened or suppressed.
- Lesson: preserve output-channel and JVM-isolation contracts when adding diagnostics or reusing
  test suites; validate them through the real logging and mutation consumers.

The REST job failed before tests because downloading the unchanged Keycloak base image returned
`short read: expected 81209403 bytes but got 12582912: unexpected EOF`. This external download
failure requires no code change.

Local verification after the CI boundary fix:

- `ConsoleLoggingTest` passes against all four packaged configurations; the full Kotlin CLI suite
  has 24 tests, zero failures and one skip, and the Kotlin API suite has 12 tests, zero failures and
  three skips. The six previously failing CLI cases now pass.
- All seven dedicated core startup/policy tasks pass, as do both query threshold modes and the
  generic temporal-reader lifetime task. Diagnostic-skip runs retain two intentional policy skips.
- Focused PIT using `ConfigurationPropertiesTest*` and `PageCacheConfigurationTest*`, plus every
  normal excluded class pattern as an adversarial test selection, discovers only the two ordinary
  suites and kills all 24 parser mutants (zero uncovered). The initial parser-only selection left
  three positive-entry-count mutants uncovered; adding the existing cache consumer tests kills them.
- This verifies the baseline-exclusion repair, not completion of the full changed-source PIT lane.
  The full shared-deadline run and macOS/Windows execution were not repeated locally.

## Evidence quality and limits

- The first eviction fixture used `NamePage`, which ordinary metadata admission rejects. `IndirectPage` plus retained/evicted counts prevents rejected admission from producing a false result.
- Two constructor-boundary mutants survived because zero was tested on only one overload, or secondary default validation hid the missing guard. Valid overrides now make both zero guards independently observable.
- First-wins and cold-start fixtures need separate processes. PIT replay cannot safely reset the process allocator; isolated startup executions prove the first initialization contract.
- PIT excludes static initializers. Thresholds and lifetime policies therefore have separate startup JVM controls. An initial static-only MM scope produced no report; real generation-lifetime scope replaced it and killed all eight mutants.
- Query PIT initially lacked its JUnit 5 adapter. Those no-coverage reports were discarded and the corrected adapter runs exercised the cleanup callers.
- The defensive reachability-fence mutant remains deliberately retained: deterministic owner-retaining tests cannot reliably expose premature JIT/GC liveness. Actual fallback and reclamation are tested; no flaky timing assertion substitutes for that limitation.
- Full scoped results, individual retained-mutant dispositions, spec/test mapping and before/after observations belong in the linked PR. Timeouts and uncovered instructions remain incomplete evidence, not a perfect score. No budget was widened and no benchmark was run.

## Prevention

Apply the configuration, cleanup and compiler-worker lessons in [Lessons from failures](../LESSONS.md). For each future bug, identify the failing public contract, make its counterexample observable independently, and keep the regression in the appropriate startup or lifecycle lane.

Focused regression commands (the core task also runs its isolated startup forks):

```bash
./gradlew :sirix-core:test --tests io.sirix.utils.ConfigurationPropertiesTest \
  --tests io.sirix.cache.PageCacheConfigurationTest --tests io.sirix.utils.SirixCleanerTest \
  --tests io.sirix.access.trx.page.WriterCleanerLifetimeTest
./gradlew :sirix-query:test --tests io.sirix.query.SirixTranslatorThresholdTest \
  --tests io.sirix.query.json.JsonDBArrayScanTeardownTest
./gradlew :sirix-rest-api:offlineTest --tests io.sirix.rest.MainTest
```
