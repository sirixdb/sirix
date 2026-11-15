# Bug: allocation measurement warm-up and omitted feature lifecycle mutation tests

- Fix / issue: PR #1426; CI run 43765735357, checks 132971517834 and 132971518020.
- Symptom: macOS query allocation window four measured 1,360 bytes instead of zero. PIT completed but its changed-line verdict found six surviving and eight uncovered mutants.
- Trigger: run the interval allocation test in the full query fork; run the bounded PR mutation selector against the feature-word change.
- Behaviour and invariants specified before the repair:
  - Steady-state interval replacement/read/write allocates exactly zero bytes at both 40,000 and 80,000 operations. Warm-up and JVM compilation must finish outside measurement. The per-operation escaping-allocation control must fail the identical zero-byte assertions and show a positive slope. No bound may grow.
  - Cold persistent storage factory opens preload both revision data and revision index; a failure after mapping releases the owned native mapping and propagates the original failure. Applies to FILE_CHANNEL and MEMORY_MAPPED and their snapshot overloads.
  - A read-only opening snapshot must not create either missing data or revision files. Applies to both persistent backends and to independently called constructors/readers as well as ordinary resource opens.
  - Existing SPI implementations using legacy session factory, resource store and storage provider methods remain executable through snapshot overloads, returning usable storage/sessions and preserving input identity.
  - UberPage decoding consumes the complete owned page image in both legacy and feature-bearing encodings; trailing bytes are corruption.
  - The real bounded changed-line mutation verdict must pass without changing scope, thresholds or attribution. Existing lifecycle tests must participate through the existing naming convention.
- Independent regression oracles: committed revision counts and values; cold cache/index state; native arena liveness after injected external cache failure; filesystem absence after refusal; exact independently assembled page images; the zero-allocation contract and escaping positive control.
- Plausible wrong behaviours rejected: return null from an SPI adapter; omit preload; abandon a live mapping; create an empty file before refusing read-only access; silently accept a page tail; report PIT completion as a passing changed-line gate.
- Root cause:
  - The allocation harness ran beside unrelated tests with asynchronous compilation and only four measured warm-up windows. It had no independent compilation/profile boundary. In the measured one-identity alternating PUT/delete workload, the implementation reuses its primitive scratch arrays and its native free slot; once the first slab exists, `read`, `replace`, and `write` reach no heap allocation site. The isolated 1,360-byte event is consistent with runtime compilation/deoptimization or initialization work; its exact object attribution could not be recovered from the macOS HTML report. This is a measurement-setup defect being repaired, **not a claim that the reported failure was a harmless flake**. The fix uses a clean fork, blocking compilation and an explicit unmeasured warm-up, retaining strict zero-byte windows and all allocating rejection controls. macOS re-execution belongs to outer CI.
  - The source-to-test selector preferred exactly named `StoreFeaturesTest` over the related lifecycle suite; other changed interfaces had no direct executable coverage. Existing codec tests did not reject trailing bytes after the revision body. Existing factory tests observed final usability but not pre-return preload state or native cleanup on failure. Naming the lifecycle suite for the shared opening/recovery boundary and adding observable-contract tests closes all fourteen reported gaps without changing selection code, caps, thresholds or attribution.
- Why earlier tests missed it: isolated successful allocation windows were mistaken for a guaranteed warmed execution profile; a successful PIT engine exit and a narrow 16/16 derived-method run did not establish the full changed-line verdict. The new evidence includes the actual blocking wrapper and identifies each original blocker.
- Regression and lesson: executable tests and mutation accounting below; see the [CI gate lesson](../LESSONS.md).
- Rebase: all twelve commits were replayed onto `9a6e1f8995aa684a40372180754c970d10166289`. The only conflict was in `docs/LESSONS.md`; both branches' lessons were retained. The base adds record-page salt and changed-line mutation reporting; it introduces no lifecycle-fence commit to reconcile.

## Path retrace before verification

The ordinary query `test` task depends on `bitemporalLoaderAllocationTest`; it excludes the class only from the shared fork. The dedicated task includes the complete class (parser, model, failure cleanup, interval allocation, assertion rejection controls), inherits the standard fork options, and adds `-Xbatch`. Its warm-up calls the same `replaceRepeatedly` entry, ending with an empty relation after an even PUT/delete count. Subsequent measured and allocating windows therefore retain the original initial state, operation counts and assertions. Both paths still run the owner-close and exhausted-owner checks. There is no retry, window selection, changed bound or production hot-path change.

The renamed `InterruptedFirstCommitRecoveryStoreFeaturesTest` is a related test for the existing recovery source name. The selector now admits it alongside `InterruptedFirstCommitRecoveryTest`; no selector implementation, cap, mutation target or threshold changes. Every original lifecycle case is retained. The new tests exercise the existing factory/default methods rather than inspecting their source. Both backend factories still take the same bootstrap and committed paths; cache and native-scope assertions observe their pre-return state and cleanup. Default overload tests consume the returned real storage or session. Both independently encoded page images decode successfully before their one-byte tails are rejected.

The dedicated allocation task puts its HTML report beneath `reports/tests/test/bitemporal-loader`, matching the existing CI failure-artifact upload contract. A failing dependency therefore leaves a report available even though the shared query task does not execute. The initial original-source comparison (GraalVM JDK 25, compilation threshold scale 10) passed: window zero allocated 295,040 bytes, the final measured windows were zero, and the positive control was 640,000/1,280,000 bytes. That run does **not** reproduce the macOS failure or identify its exact allocated objects; the retained macOS report remains the before-fix failure evidence.

The final path retrace also moved the allocation-output hook from the shared task to the dedicated task. Excluding the class made the shared hook unreachable; the dedicated task now retains the same per-window CI diagnostics without a dead sibling hook.


## Mutation verification

The actual command `python3 .github/scripts/pr_mutation.py run --seconds 1320 --blocking --output build/ci-round7/pit`, run under the required heavy-command wrapper, exited **0**. The engine completed in 8m22s. All **28/28 changed-line mutants were killed**, with zero survivors and zero uncovered mutants. All fourteen original blockers are individually matched below by class, method descriptor, mutation description and bytecode index (the PageKind source line moved on rebase).

The full sample is **189: 79 killed, 42 surviving, 68 uncovered**. The 110 undetected mutants are on unchanged lines and remain advisory under the base branch's existing policy. They are not claimed as killed or waived. The union of 16 selected test patterns covers the blocking contracts even where an individual source still has no name-based mapping. No mutation policy exception is required.

| Class / method | Original line / status | Final line / status | Mutation |
|---|---|---|---|
| `io.sirix.io.IOStorage.getOpeningUberPage` | 381 / NO_COVERAGE | 381 / KILLED | replaced return value with null for io/sirix/io/IOStorage::getOpeningUberPage |
| `io.sirix.io.StorageType$3.getInstance` | 83 / NO_COVERAGE | 83 / KILLED | replaced return value with null for io/sirix/io/StorageType$3::getInstance |
| `io.sirix.io.StorageType$3.getInstance` | 92 / SURVIVED | 92 / KILLED | removed call to io/sirix/io/filechannel/FileChannelStorage::loadRevisionFileDataIntoMemory |
| `io.sirix.io.StorageType$3.getInstance` | 93 / SURVIVED | 93 / KILLED | removed call to io/sirix/io/filechannel/FileChannelStorage::loadRevisionIndex |
| `io.sirix.io.StorageType$4.getInstance` | 110 / NO_COVERAGE | 110 / KILLED | replaced return value with null for io/sirix/io/StorageType$4::getInstance |
| `io.sirix.io.StorageType$4.getInstance` | 119 / SURVIVED | 119 / KILLED | removed call to io/sirix/io/memorymapped/MMStorage::loadRevisionFileDataIntoMemory |
| `io.sirix.io.StorageType$4.getInstance` | 122 / SURVIVED | 122 / KILLED | removed call to io/sirix/io/memorymapped/MMStorage::close |
| `io.sirix.io.StorageProvider.createStorage` | 131 / NO_COVERAGE | 131 / KILLED | replaced return value with null for io/sirix/io/StorageProvider::createStorage |
| `io.sirix.io.memorymapped.MMStorage.createDirectoriesAndFile` | 379 / SURVIVED | 379 / KILLED | removed call to io/sirix/page/StoreFeatures::requireWritable |
| `io.sirix.page.PageKind$3.deserializePage` | 5468 / SURVIVED | 5476 / KILLED | removed call to io/sirix/page/PageKind::requireFullyConsumed |
| `io.sirix.access.xml.LocalXmlDatabaseFactory$1.create` | 69 / NO_COVERAGE | 69 / KILLED | replaced return value with null for io/sirix/access/xml/LocalXmlDatabaseFactory$1::create |
| `io.sirix.access.xml.LocalXmlDatabaseFactory$1.create` | 87 / NO_COVERAGE | 87 / KILLED | replaced return value with null for io/sirix/access/xml/LocalXmlDatabaseFactory$1::create |
| `io.sirix.access.ResourceStore.beginResourceSession` | 29 / NO_COVERAGE | 29 / KILLED | replaced return value with null for io/sirix/access/ResourceStore::beginResourceSession |
| `io.sirix.access.ResourceSessionFactory.create` | 22 / NO_COVERAGE | 22 / KILLED | replaced return value with null for io/sirix/access/ResourceSessionFactory::create |


Full mutation IDs, killing tests, reports, the original macOS failure, the rebase range-diff and source hashes are retained in [the evidence directory](evidence/store-feature-ci-gate/). The mutation-time hash manifest records the sources actually used for that run; subsequent Java formatting changed only test whitespace. Production hashes remain unchanged.

## Final local verification

All JVM commands ran through the required `heavy.sh` wrapper after a disk check; free space remained about 90 GB, above the 60 GB floor. Shared resource contention initially delayed verification; the wrapper's documented priority slot retained its memory/load/timing checks. No other jobs were stopped.

| Verification | Result |
|---|---|
| Focused feature lifecycle, SPI and codec regressions, after formatting | 105 cases, no failures or skips |
| Core work budgets (`:sirix-core:test`) | 325 cases, no failures or skips |
| Isolated dictionary / inline-slot allocation companion | 5 cases, no failures or skips |
| Query work budgets and indexed-array cases | 327 cases, no failures; 5 pre-existing 100,000-cohort skips |
| Dedicated loader suite | 5 cases, no failures or skips |
| MCP work / serialization budgets | 3 cases, no failures or skips |
| Selector Python tests | 58 passed |
| Core/query Java formatting checks and `git diff --check` | Passed |
| Actual blocking mutation wrapper | Exit 0; changed-line 28/28 killed; original blockers 14/14 killed |

The dedicated loader suite also passed with effective fork arguments `-Xbatch -XX:-UseJVMCICompiler -XX:CompileThresholdScaling=10`. Every nonallocating measured window, including windows four and five, was exactly **0 bytes**; controls were **640,000 bytes at N** and **1,280,000 bytes at 2N**. Its control also exercises rejection by the same zero-byte assertion. This independently exercises HotSpot C2 rather than relying only on the local Graal compiler. The exact macOS allocation event was not reproduced locally; its object attribution and the remote macOS rerun remain explicit limitations, not a waived check.

A development version of the new cleanup fixture initially returned a null synchronous cache view and failed before the intended injected bulk-load failure. It was corrected to delegate all normal cache operations to a real Caffeine cache. The final test proves that the original bulk-load exception propagates and the real mapped arena is closed; its close-removal mutant is killed.

Reproduce using the existing [work-budget commands](../VERIFICATION.md) and the dedicated task `:sirix-query:bitemporalLoaderAllocationTest`. For the additional C2 profile, pass `-I docs/postmortems/evidence/store-feature-ci-gate/allocation-stress.gradle.txt`. The original source comparison's temporary task is retained in the evidence directory; the remote before-fix failure is retained as `macos-before.html.gz`. Use the existing PR selector with this base and the full current head SHA, then run its unmodified `run --seconds 1320 --blocking` command. [The manifest](evidence/store-feature-ci-gate/manifest.json) retains counts and hashes; `blocker-accounting.json` retains every original mutation's method descriptor, bytecode index and killing test.

No production source changed in this repair. The feature-word implementation and prior pipeline fixes, including the content of `b548587677a616a4792fe4055647316973283ce9`, remain on the rebased history. No push, remote CI rerun, approval or pipeline transition was performed; the outer executor owns those actions.
