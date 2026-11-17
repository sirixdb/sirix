# Lessons from failures

Keep entries short and evidence-based: broken assumption, prevention, and fix/test evidence.
For each new bug, add a [write-up](postmortems/TEMPLATE.md), a failing-before regression and a
lesson here. Extend an existing lesson when the same failure mode recurs.

| Failure / broken assumption | Apply next time | Repository evidence |
| --- | --- | --- |
| `xn:store` read its resource name but dropped it when creating a database; tests passed `()` or read results back through the collection. | Exercise a shared parameter on every path of each sibling function and read the result back by that parameter, not by position. Test singleton cardinality independently of bare, sorted and lazy sequence representations. | [xn:store write-up](postmortems/xml-store-resource-name.md), `StoreResourceNameTest` |
| A shell option declared without a value could not run at all; mutation reviews recorded its unreachable lines as legacy instead of a defect. | Treat an unreachable public entry point as a bug to repair or remove; a mutation disposition must not explain away a user-visible command. Keep query line endings independent of native output framing, distinguish compiler normalization from serialization with a runtime-generated CR control, and use the same narrow delimiter grammar for splitting and blank-line classification. | [-iqf write-up](postmortems/shell-query-script-mode.md), `MainTest.queryScriptsRunEveryBlankLineSeparatedQueryInOneSessionAndReportFailuresInTheStatus` |
| A session-long query context treated only `QueryException` as query failure and had no failure path for pending updates or the writers an apply opened; the next query committed the failed query's changes or met a rollback-only writer. | When state outlives one unit of work, end every failed unit explicitly: discard its pending state and release what it opened, keeping caller-held resources. Test failures raised after side effects began, under each commit strategy, and assert the next unit's persisted result through the same retained query items. Rollback must invalidate their mutation-populated memos as well as restore storage; use a later failing operation for insert, replace and rename. Distinguish empty submissions from EOF at the shared input reader and test both interactive dispatches. Escape line terminators only when rendering reports; preserve diagnostic objects for debug logging and assert complete output lines. | [Shell failure write-up](postmortems/shell-query-failure-recovery.md), `MainTest.interactiveShellReturnsToThePromptAfterAnyQueryFailureWithoutApplyingItsUpdates`, `SirixQueryContextFailedApplyTest` |
| A repository regression treated valid CONV-204 additions as forbidden inventory changes. | Validate the complete configuration through its consumer, then assert required historical mappings individually; do not close an extensible inventory by whole-map equality. | [Timestamp CI association regression](postmortems/commit-timestamp-ci-associations.md), `AssociationTest.test_repository_files_are_valid_and_keep_the_former_contract_pairs` |
| A starvation test leaked its shortened JVM-wide retry deadline into a later retirement test. | Reset every static test override in common teardown, including failure paths; exercise the setter test before an existing default-behavior test in one JVM. | [Retry-deadline isolation](postmortems/2026-11-hot-writer-unheld-leaf-reads.md#review-follow-up-retry-deadline-test-isolation), `HOTIndexWriterLeafRetentionTest.restore`, `HOTLeafWriterGuardTest.abstractIndexWriterYieldsTheCoreToTheThreadRetiringTheLeaf` |
| Disposition validators combined schema, field rules and verdict precedence in complex methods. | Extract complete validation stages, preserving diagnostic order and first-match verdicts; verify aggregate errors and overlapping rules through public outputs. | [CodeFactor disposition refactor](postmortems/pr-mutation-codefactor-complexity.md) |
| Mutation review used helper counts, filenames, the first matching tier and only new-side patch paths as substitutes for behavioral contracts; later tests coupled empty selections and required pairs to the repository's changing configuration. | Assert emitted verdicts; share declaration-based ownership across validation and selection; exercise overlapping ranking tiers, literal-versus-declaration syntax, and both text and header-only Git patch effects at every consumer. Isolate CLI fixtures with an explicit root and assert required association pairs while permitting valid additions. Withdraw internal-only mutation kills. | [Disposition review write-up](postmortems/pr-mutation-disposition-review.md), [RAM CI fixture follow-up](postmortems/ram-storage-committed-revision-loss.md#ci-follow-up-mutation-tooling-fixture-isolation), focused reporter regressions |
| A conditional-write draft used inconsistent definitions of insert context, semantic modification, incarnation, mutation scope and response identity. | Trace each predicate through every insert direction, hash mode, lifecycle reset, indirect mutation boundary and response shape, including XML's separately stored attribute/namespace membership and records. Shared semantics do not imply mandatory exposure in every binding; preserve the approved release scope and honest planned evidence. | [Specification boundary write-up](postmortems/conditional-write-spec-boundaries.md) |
| Real-method static mocks can execute destructive lifecycle calls while registering a stub. | Default terminal-boundary mocks to no-op, opt in only test-owned resources to real calls, and assert shared resources still work after the mock closes. | [Native CI isolation regression](postmortems/native-ci-contracts.md#ci-follow-up-test-isolation-and-fixture-layout) |
| A package declaration alone does not satisfy the Java package/directory contract. | Keep standalone fixture directories aligned with packages and update every compiler input path when moving them. | [Native fixture layout](postmortems/native-ci-contracts.md#ci-follow-up-test-isolation-and-fixture-layout) |
| A native C smoke reached terminal shutdown, but the JVM regression bypassed its public adapters, leaving a removable delegation uncovered. | Exercise the public lifecycle path in the automatically selected JVM suite; substitute terminal process-owned resources at the drain boundary and assert real work and termination. Keep standalone fixtures packaged and workflow dependencies pinned. Rerun Spotless after the final test edit, including CI repairs. | [Native CI contracts](postmortems/native-ci-contracts.md), `StorageEngineWriterFactoryTest` |
| An isolated opening-work test counted independent derived-index rewind reads through a process-global counter. | Scope work observers to the operation's owning thread when opening starts background maintenance; use joined real reads on another thread and corrupt-primary recovery to verify isolation without weakening bounds. Document bounded primitive thread-lifetime ownership at a field-scoped analyzer suppression rather than resetting cumulative diagnostics per read. | [Beacon counter write-up](postmortems/beacon-opening-work-counter.md), `AbstractReaderBeaconWorkTest`, unchanged `derivedOpenReadsOneBeacon` |
| A subtree-local insertion treated its local root as the publication root, retaining redundant inner pages and stale ancestor heights while lookups stayed correct. | Propagate live height changes through the original spine; normalize only dirty blocks at commit when per-insert folding costs latency. Commit is still a per-page hot path: skip unchanged blocks before visitor setup, re-arm transient hints at every structural publication (including periodic consolidation after value-only dispatch), retain scratch across the actual writer-replacement lifecycle, and pin owned output plus publication costs separately. Audit paired classpaths as well as timing windows. Check routing, structural heights, insert/page allocation and commit cost independently; re-establish constructed path witnesses after normalization boundaries. | [Lost-spine write-up](postmortems/hot-lost-outer-spine.md), `HOTSubInsertSpineTest`, `HOTConstructedShapeIntegrationTest` |
| PR mutation selection assumed filenames were JVM class names; a timeout repair also allowed uncovered mutants to pass. | Resolve declared test identities before compilation at the shared discovery boundary; keep scope bounds separate from the [acceptance criterion](VERIFICATION.md#blocking-criterion-the-prs-own-lines). Test mismatched filenames across every lookup path and execute blocking/advisory report handling. | [Mutation scope write-up](postmortems/pr-mutation-test-identity.md), `SelectionTest` and `ReportingTest` in `test_pr_mutation.py` |
| JSON serialization assumed every result item matched a supported branch; unsupported items could leave the iterator on the same item. | Specify iterator progress and unsupported-type errors; test unsupported items alone and mixed into sequences. | [c9efc3a7b158](https://github.com/sirixdb/sirix/commit/c9efc3a7b158a12af7fc544c03b6113081ffe077), `JsonDBSerializerTest` and `QueryJsonSerializationTest` |
| Query mutation authorization relied on an updating-expression flag; ordinary function calls could mutate too. Assertions comparing absent aggregate headers also became vacuous after storage moved. Later, `xn:store` was found to lack the function-level check its siblings had gained; only REST's store decorators covered it. | Guard the actual mutation boundary in every sibling function; require the witness to exist, assert persisted state is unchanged on refusal, and include an authorized control. Verify the relevant JVM tests actually run in CI. Judge the impact of a missing check through the real deployment layers before calling it exploitable. | [36ee410f1f8d](https://github.com/sirixdb/sirix/commit/36ee410f1f8d0ff201bcb80170781f8d86b47550), `QueryMutationAuthorizationTest`, `PermissionCheckingQueryTest`, [xn:store authorization write-up](postmortems/xml-store-create-authorization.md), `StoreAuthorizationTest`, `QueryMutationAuthorizationTest.xmlStore*` |
| Correct timestamp results alone did not guard temporary allocations on the indexing path. | Pair a randomized independent JDK oracle with an allocation budget and a control that proves the budget detects the allocating implementation; retain precision and overflow cases. | [2c96b9196502](https://github.com/sirixdb/sirix/commit/2c96b91965024aaf9692974d661d39f3d7570d7c), `UtcTimestampParserTest`; [verification layers](VERIFICATION.md) |
| A successful argv-based subprocess call and passing tests did not prove revision arguments were safe, or that a new language's coverage reached Sonar. | Reject option-like revisions and hostile paths, check selected sources against the Git tree, test the boundary with a real hostile filename, and verify the language-specific coverage import. | [initial implementation](https://github.com/sirixdb/sirix/commit/48f3d9d2f04def3bd4bdc74ff9eed031c449a931), [write-up](postmortems/pr-mutation-validation.md) |
| Parsed or logged settings were treated as proof of an effective resource policy. | Assert actual cache admission/eviction and effective process allowance; include binary-unit overflow that wraps back to a positive long, precedence, and separate startup JVMs. | [Configuration write-up](postmortems/configuration-cleanup-hygiene.md), `ConfigurationPropertiesTest`, `PageCacheConfigurationTest`, `ConfigurationStartupTest`, `AllocatorColdStartupTest`, `SirixTranslatorThresholdTest` and `MainTest` |
| A shared cleanup daemon could inherit application context or serialize blocked actions; normal-close no-ops hid retained registrations. | Use the JDK context-free Cleaner factory, owner-independent actions and independent fallback dispatch; observe first-registration context and reclamation while a closed owner remains live. | [Cleanup write-up](postmortems/configuration-cleanup-hygiene.md), `CleanerStartupTest`, `SirixCleanerTest`, `WriterCleanerLifetimeTest` and caller teardown suites |
| Deleting an allocator did not remove its native reachability declarations. | Treat metadata as a consumer contract: compare normalized descriptor meaning, keep required close forms, and execute the native smoke consumer. | [Native metadata write-up](postmortems/configuration-cleanup-hygiene.md), normalized metadata comparison and `:sirix-query:nativeSmokeTest` |
| The Gradle daemon heap was mistaken for the Error Prone compiler-worker heap; a cached build masked the failing sequence. | Configure compiler resources at the shared gated task boundary and force the complete failing dependency chain to compile when validating a memory repair. | [Compiler write-up](postmortems/configuration-cleanup-hygiene.md), shared Error Prone configuration and forced core/query/MCP compilation |
| An allocator warning entered CLI result stdout, and PIT rediscovered tests requiring isolated startup JVMs. | Route shared console diagnostics to stderr; preserve normal test exclusions when configuring PIT's shared baseline. Exercise the actual consumers, including enabled warnings and an incompatible startup fixture. | [CI boundary write-up](postmortems/configuration-cleanup-hygiene.md), `ConsoleLoggingTest`, CLI query/history regressions and focused PIT baseline control |
| Initializing a salt in the shared serializer changed a disposable frame even when its caller refused the encoded result. | Identify the caller's publication boundary before adding serializer mutations; preserve the logical frame through post-encode refusal. | [Write-up](postmortems/record-page-salt-disposable-frame.md), `PageKindTest`, `AsyncSnapshotEncodedCacheTest.directIdentityCapacityFailurePreservesCacheAndFrame` |
| Neighboring outputs of an invertible PRNG exposed an erased page's salt and restored the parent-checksum oracle. | Match randomness to its threat model; execute the state-recovery attack and verify the cryptographic source against an independent vector while retaining allocation budgets. | [Write-up](postmortems/record-page-salt-predictability.md), `PageLayoutTest.neighboringSaltsCannotReconstructTheRemovedSalt`, `RecordPageSaltSourceTest` |
| Default-on random salts leaked into golden and differential byte fixtures; unnamed focused suites made PIT run the entire module baseline. | Disable randomness at byte-fixture configuration boundaries, including newly added differential arms; retain salted behavioral checks and use the mutation selector's documented class naming convention. | [CI write-up and FSST follow-up](postmortems/record-page-salt-ci-fixtures.md), `GoldenCompositePageTest`, `DerivedElisionSectionTest`, `StringRegionPerTagCompletenessTest`, `ResourceConfigurationTest`, `PageKindTest`, `PageLayoutTest`, `StorageEpochFsstInlineDecisionTest` |
| Sonar treated integer header arithmetic and an intentional thread-lifetime salt cache as new reliability issues. | Widen sink byte-count operands before arithmetic; document bounded thread-owned state with a scoped suppression when removal would defeat required reuse. Validate bytes and allocations through executable interfaces. | [Sonar write-up](postmortems/record-page-salt-sonar.md), `PageKindTest`, `RecordPageSaltAllocationTest` |
| Exact Python version pins did not verify artifact bytes or prevent source builds in the native-header workflow. | Pin transitive dependencies and published wheel hashes; enforce `--require-hashes` and `--only-binary :all:` at each install caller, and exercise pip's rejection paths. | [Native-header dependency write-up](postmortems/native-header-dependencies.md) |
| Unknown persisted discriminants, metadata tails and checksum-valid reserved fields were accepted; omitted optional settings desynchronized the reader. | Validate the semantic domain independently of checksums. Patch bytes independently of the writer, assert the actual discriminator in the refusal, and dispatch optional settings by name. | [Reader-boundary postmortem](postmortems/2026-11-page-reader-boundaries.md) |
| A focused JIT profile hid an `Enum.values()` array clone performed per page. | Avoid `values()` in per-page or per-record loops; use a direct lookup or prebuilt registry and verify allocation checks across compilation profiles. | [Reader allocation follow-up](postmortems/2026-11-page-reader-boundaries.md) |
| A newly merged legacy-envelope byte oracle inherited default-on random salts and misreported a salt change as an extension rewrite failure. | Pin deterministic mode only at the byte-oracle configuration boundary; retain a default-salted companion that checks records, salt preservation and extension refusal together. | [Integration write-up](postmortems/record-page-salt-reader-integration.md), `LegacyEnvelopeImagesTest`, `PageKindTest.optionalExtensionsPreserveSaltAndRecordsWhileRequiredEntriesRefuse` and `ResourceConfigurationTest` |
| RAM commits shared writer-owned pages; retirement, publication and cleanup depended on incomplete ownership or filesystem evidence. | Own independent stored payloads and caller-owned decoded pages; use reusable allocator scratch for hot reads. Check committed values and historical revisions after reopening writers, readers and sessions; publish timestamp metadata only at the committed beacon. Share ended-instance release and discard boundaries, consult remembered source/derived ownership, clear stores and known caches after quiescence, remove discarded IDs, and keep admission pending after incomplete discovery. Retain source proof until derived retirement, rename before deletion using bounded garbage names, report remaining paths, exclude garbage at shared tool preflight, and bind retries to exact old paths and proof UUIDs. Keep known paths until absence is proved; enumerate the parent after inconclusive lookups and test denied traversal plus actual derived siblings. Probe fixture filename limits before exercising retirement, retaining the original case where supported. | [RAM commit write-up](postmortems/ram-storage-committed-revision-loss.md), [retirement and read scratch](postmortems/ram-storage-committed-revision-loss.md#review-follow-up-retirement-and-read-scratch), [cleanup admission and proof](postmortems/ram-storage-committed-revision-loss.md#review-follow-up-failed-cleanup-admission-and-proof), [retained sessions and close retries](postmortems/ram-storage-committed-revision-loss.md#review-follow-up-retained-sessions-and-close-retries), [publication and inaccessible paths](postmortems/ram-storage-committed-revision-loss.md#review-follow-up-publication-and-inaccessible-known-paths), [remembered removal ownership](postmortems/ram-storage-committed-revision-loss.md#review-follow-up-remembered-ownership-on-removal), [long-name fixture](postmortems/ram-long-name-test-filesystem-limit.md), [derived-name cleanup](postmortems/ram-long-name-derived-cleanup.md); `RAMStorageCommittedRevisionTest`, `RAMStorageContractTest`, `DatabasesInMemoryLifetimeTest` (AC12), TXN-615 memory, permission, publication and retry regressions |
| Revision zero was treated as proof that a resource held nothing, even after the root gained active feature metadata. | Before auto-reinitializing a store, prove both data and root metadata are disposable; distinguish unsupported features from corrupted beacons. | [write-up](postmortems/store-feature-recovery.md), `InterruptedFirstCommitRecoveryStoreFeaturesTest.bootstrapRecoveryCannotEraseActiveFeatures` and its guard-removal control |
| Opening a reader was assumed to be side-effect free, and a derived-store runtime failure was assumed to justify rebuilding. | Check compatibility before preload can heal records; preserve unsupported-feature failures at every destructive recovery owner. Test cold caches, damaged but salvageable records, and indirect parent opens with byte-preservation assertions. | [write-up](postmortems/store-feature-recovery.md), `InterruptedFirstCommitRecoveryStoreFeaturesTest.readOnlySalvageDoesNotHealRevisionRecords`, `damagedRevisionToolRefusalPreservesBothFiles`, `parentOpenPreservesUnsupportedDerivedSibling` |
| Repair gates covered a reported writer and exception, while file creation, spill cleanup, unrelated derived failures and hidden tool inputs remained reachable. | Apply the [open-time compatibility contract](DISK_FORMAT.md#uberpage-store-feature-words) at every mutation owner, and assert file absence and orphan preservation as well as bytes. Execute the fault fixture and validate its precondition before interpreting mutation results. | [write-up](postmortems/store-feature-recovery.md#review-round-2-all-open-time-mutation-owners), `InterruptedFirstCommitRecoveryStoreFeaturesTest.unsupportedOpenPreservesMissingFilesAndSpills`, `unsupportedDerivedDamageNeverTriggersRebuild`, `toolsRefuseHiddenDerivedFeaturesBeforeOpeningParents` |
| A compatibility inventory was added to every database-handle open, although its results were discarded. | Keep resource checks at the target resource boundary and full inventory checks in tools; assert zero resource I/O for cold and reused database handles, with an actual resource read as a positive control. | [write-up](postmortems/store-feature-recovery.md#review-round-3-database-handles-must-not-scan-resources), `InterruptedFirstCommitRecoveryStoreFeaturesTest.databaseHandlesDoNotReadResourceBeacons` |
| Independent safety probes at every layer duplicated beacon reads and allocations during one resource open. | Establish and retain one opening uber-page snapshot, pass it through nested initialization, and count decoder work for both mapped and channel backends; keep explicit index refresh separate from initialization. | [write-up](postmortems/store-feature-recovery.md#review-round-4-reuse-the-opening-snapshot), `InterruptedFirstCommitRecoveryStoreFeaturesTest.resourceOpenReadsOneBeacon`, `standaloneToolOpenReadsOneBeacon`, `derivedOpenReadsOneBeacon`, `revisionIndexRefreshUsesCurrentBeacon` |
| Future required metadata was classified as corruption, so redundant-copy selection silently opened an older compatible store root. | Give unsupported formats a distinct outcome and propagate it from either beacon; pair valid future-primary fixtures with a compatible secondary and retain a corrupt-primary recovery control. Run existing format-contract checks too when changing exception classification. | [write-up](postmortems/store-feature-recovery.md#review-round-6-unsupported-metadata-is-not-corruption), `InterruptedFirstCommitRecoveryStoreFeaturesTest.unsupportedBeaconNeverFallsBack`, `unsupportedDerivedBeaconRemainsUntouched`, `corruptPrimaryStillUsesCompatibleSecondary` |
| End-to-end refusals masked independent writer/reader guards, while manual mutation controls did not satisfy the requested PIT evidence. | Run actual bounded PIT, exercise independently callable boundaries and pair refusal cases with successful cleanup controls; preserve every remaining mutation with its exact site, rationale and raw report. | [PIT evidence and individual catalog](postmortems/store-feature-pit-evidence.md), `InterruptedFirstCommitRecoveryStoreFeaturesTest.publishedReadOnlyFeaturesRefuseDirectPageWriters`, `rawReaderChecksFeaturesBeforeSalvageAndOnExplicitRefresh`, `readOnlyContextCannotRecreateADeletedDataFile` |
| Open-time compatibility changed recovery ordering and test fixture contracts; static mock setup leaked a real database handle. | Cover missing/malformed settings alongside damaged binary state, reuse successful compatibility snapshots across rebuild checks, assert rebuilt persisted state rather than maintainer existence, use real value objects in lifecycle fixtures, and intercept static resource opens without executing them during stubbing. Run the existing global-cache replacement test after resource-boundary mocks. | [CI fixture/recovery write-up](postmortems/store-feature-ci.md), base cleanup/derived regressions and `InterruptedFirstCommitRecoveryStoreFeaturesTest` followed by `HOTStructuralChildEvictionTest` |
| A bounded mutation run omitted lifecycle tests, and an allocation check assumed a shared JVM had finished warming. | Name integration suites for the existing source-to-test mapping; verify the changed-line verdict and measure zero-allocation paths in a clean, warmed JVM with allocating rejection controls. | [CI gate write-up](postmortems/store-feature-ci-gate.md), `InterruptedFirstCommitRecoveryStoreFeaturesTest`, `BitemporalLoaderBudgetTest` |
| Temporal functions assumed their `json-item()` argument was an object or array; stored numbers, strings, booleans, `null` and field names failed with a `ClassCastException`. The initial spec also generalized same-kind field replacement to type changes. | Test every admitted item class and constructed items; dispatch on node identity. Assert keys and literal histories separately for same-kind field updates, type changes and array replacements before promising history continuity. | [Write-up](postmortems/temporal-functions-value-items.md), [spec](TEMPORAL_FUNCTIONS_SPEC.md), `TemporalNavigationTest` |
| Passing runtime tests and high coverage did not satisfy Sonar's maintainability gate for temporal items. | Inspect the failing gate metric and individual rules; isolate exception assertions and justify necessary covariance or marker inheritance at the narrow analyzer boundary without changing hot-path representation. | [CI follow-up](postmortems/temporal-functions-value-items.md#ci-follow-up-sonar-maintainability-gate), `TemporalNavigationTest`, `TemporalSirixJsonItemStreamTest` |
| Storage-epoch replay lost a page's FSST binding, and epoch materialization judged raw string sizes, changing inline placement despite equal values. | A replay must reproduce the original read, not only its combine. Every route into a size-based placement decision must apply all canonical encoding passes in the same order before consuming append capacity. Pin geometry-changing settings (hash kind) in differential tests, and compare decisions and bindings, not only values. Pair isolated regressions with fast boundary tests selected by the actual PR mutation lane. | [FSST epoch carrier write-up](postmortems/2026-11-fsst-epoch-carrier-spill.md), `StorageEpochFsstInlineDecisionTest`, `StorageEpochStoreIdentityTest` under both hash kinds, `VersioningTypeFsstMetadataCopyTest`, `BulkAdoptionDiagnosticsTest` |
| PIT split inherited property values at commas/whitespace, missed late test properties, and exhausted its heap on a large baseline fixture. | Exercise the real launcher with commas, whitespace, late properties and the largest selected deterministic oracle; configure coverage and mutation JVMs at their shared boundary. | [write-up and reproduction](postmortems/pr-fastpath-ci.md) |
| Passing focused PIT runs did not establish that the full changed-source gate could finish or cover its whole-source inventory. | Inspect the full inventory, baseline coverage and mutation-unit distribution before proposing a bounded-gate repair; make scope/budget policy changes explicit human decisions. | [deadline diagnosis and limitations](postmortems/pr-fastpath-ci.md#follow-up-shared-execution-deadline) |
| An interval test expected five rapid commits to have distinct millisecond timestamps; PIT exposed the then-valid last-tie result. | Use explicit ordering fixtures under the current [timestamp and lookup contracts](../spec/VER-versioning-bitemporal.md#ver-201--timestamp-precision-and-persistence), and assert exact revision IDs. | `OpenRevisionsTest`; [write-up and reproduction](postmortems/pr-fastpath-ci.md#follow-up-revision-interval-baseline-flake) |
| A refusal wrapper covered only the sequential compiler while the parallel strategy admitted the same SPI. | Enumerate every physical compiler, function interception and public factory; test refusals and admitted controls through each, including outer count semantics and cold reopen. | `VectorizedReleaseCorrectnessTest`; [parallel grouping write-up](postmortems/parallel-grouping-refusal.md), [count-wrapped grouping write-up](postmortems/count-wrapped-grouping-refusal.md) |
| Bounded PIT naming heuristics omitted a compiler/refusal contract; its parallel cases also omitted the non-grouping return. | Associate integration contracts at the shared selection boundary, preserve matches and caps, include ordinary-path controls, and verify real mutant outcomes. | `SelectionTest` release associations, `ordinaryPipelineRetainsRowsAcrossReopen`; [write-up](postmortems/pr-fastpath-mutation-selection.md) |
| Long allocation-test loops warmed OSR but left normal helper entry cold; late VM string materialization contaminated zero-byte windows. | Warm the helper with repeated short calls before measurement; retain strict N/2N checks and an escaping-allocation control, and diagnose transient bursts with allocation stacks. | `BitemporalLoaderBudgetTest`; [warmup write-up](postmortems/bitemporal-allocation-warmup.md) |
| A deferred accumulator's last type marker was mistaken for proof that every counted value was numeric; a later number erased an earlier string marker. | Test invalid values before, between and after valid ones, including cold reopen; refuse summary serving when persisted metadata cannot certify the required type invariant. | `VectorizedReleaseCorrectnessTest.mixedStringPlacementPreservesGenericError`; [purity write-up](postmortems/path-summary-numeric-purity.md) |
| A seek returned the first entry of the leaf a key was routed to although the key sorted below that leaf: routing by discriminative bits says nothing about the leaves before it. The tests probed stored keys and leaf-boundary gaps only. | Compare every seek and range with an independent sorted reference over keys that are absent in every way: one bit of a stored key flipped, truncated and zero-padded forms, numeric neighbours. Assert that the probes reach each geometry (routed below, above, inside the leaf), and make a condition state exactly the case its comment argues for. | [Lower-bound postmortem](postmortems/2026-11-hot-lower-bound-below-leaf.md), `HOTTrieReaderBoundContractTest`, `HOTTrieReaderBoundPropertyTest`, `HOTTrieReaderCasRangeRegressionTest` |
| HOT factories, readers and structural guards disagreed on the minimum indirect fanout. | Derive construction, decode and publication checks from I10; exercise invalid bounds in both layouts and keep budget fixtures within the legal fanout domain. When tightening a selector, check every caller and legacy test: deletion must handle removal/collapse before selecting a legal type. | [Cardinality write-up](postmortems/2026-11-hot-indirect-cardinality.md), `HOTIndirectCardinalityTest`, `NodeUpgradeManagerTest` |
| HOT sibling reads outlived cache-owned frames; pair-only retention also missed surviving children used by recompression. Short benchmark warm-ups obscured the cost of the first fix. | Derive retention scope from every downstream consumer under [FMT-304](../spec/FMT-disk-format.md#fmt-304--hot-mutation-leaf-lifetime). Retire leaves after resolution captures them, poison or reuse released frames, and check the full result against a model; verify that restoring unsafe reads fails the regression. Price hot-path changes after compilation settles, using a fixture that accommodates the schedule consumed by the real harness. | [Root causes and evidence](postmortems/2026-11-hot-writer-unheld-leaf-reads.md), `HOTIncrementalInsertLeafRetentionTest`, `HOTIndexWriterLeafRetentionTest`, `HOTStructuralPropertyTest.mutationsSurviveLeafEviction`; benchmark schedules: `HotWriterSiblingReadBenchmark` class Javadoc |
| A balanced extra guard made an inclusive-zero duplicate-scan mutant invisible to behavioral tests; removed assertions also left three unused imports. | Use a remaining-slot countdown to express reverse traversal without a special inclusive-zero index. Keep behavioral mutation oracles, audit all changed Java imports with the committed Checkstyle rules, and rerun the actual PIT lane without exemptions. | [CI follow-up](postmortems/2026-11-hot-writer-unheld-leaf-reads.md#ci-follow-up-reverse-scan-boundary-and-unused-imports) |
| MCP treated identical head content as a no-op while REST/core recorded the revert event. | Test persisted revision counts, returned metadata and historical content across every interface and target representation. | [Revert parity write-up](postmortems/2026-11-revert-head-parity.md), MCP `ToolHandlersIntegrationTest.Revert`, REST `RevertHandlerTest` |
| Shared benchmark setup required optional absent-probe geometries unavailable in a one-entry leaf. | Apply each probe requirement only to its consuming stage and run a smallest-corpus smoke check through the real harness. | [Benchmark write-up](postmortems/2026-11-hot-benchmark-small-corpus.md), `:sirix-benchmarks:verifyHotSmallCorpus` |
| HOT runtime regressions passed while large validation/seek methods and opaque assertion helpers failed Sonar's maintainability gate. | Extract cold validation stages without changing failure priority, work or allocation; never split a hot seek or per-insert kernel for the analyzer, keep its measured shape under a narrow reasoned S3776 suppression; isolate throwing operations and name delegated assertion helpers explicitly. Verify with the analyzer as well as behavioral oracles; count-source-only changes can expose the same inherited query traversal complexity. | [CI write-up](postmortems/2026-11-hot-sonar-maintainability.md), [query follow-up](postmortems/hash-default-consumers.md#ci-follow-up-unchanged-traversal-complexity), HOT cardinality/footprint/detector/bound suites and MCP integration tests |
| A dangerous option was safe only because every caller happened to disable it: `DiffFactory.Builder.hashKind` hard-coded `ROLLING` regardless of the diffed resource, and the hash shortcut was avoided only because each caller separately derived `DiffOptimized` from the resource's hash kind. Changing the hash default would have made the unsafe combination the common case. | Put the gate on the state that makes the option valid, not on each caller's discipline: derive the default from the resource and refuse the shortcut when the resource stores no hashes. Test the direction a caller must not be able to force, not only the happy path. | [Write-up](postmortems/hash-default-consumers.md), `DiffFactoryHashKindTest`, `JsonDiffHashEquivalenceTest`, `XmlDiffHashEquivalenceTest` |
| A field that exists only under one configuration was read as if it were always maintained: stored descendant counts are gated on `hashType != NONE`, and four planner size heuristics read `getDescendantCount()` as a document-size signal, so a hashless resource looked empty and lost its index route, its parallel axis and every cost-based annotation. | When a planner heuristic reads a stored statistic, check whether that statistic is configuration-gated and give it an unconditional fallback. Guard it with a test that asserts the decision input, since a plan change leaves results exact and no result test notices. | [Write-up](postmortems/hash-default-consumers.md), `DocumentNodeCountTest`, `DocumentNodeCount` |
| Query/REST metadata concealed core Caffeine node constructors missing from a shared image. | Test the smallest supported classpath from C; distinguish node constructors from cache factory fields and exercise weighted/async variants. | [Core metadata write-up](postmortems/native-core-caffeine-metadata.md), `tests/native/run-smoke.sh` and constructor omissions |
| A persisted serializer class name was loadable on the JVM but unreachable in a core-only native image. | Round-trip resource creation and reopen with only core metadata; keep built-in serializer/handler registrations in core. | [Resource configuration write-up](postmortems/native-core-resource-configuration.md), `CoreSmoke` |
| Daemon status and interruption did not terminate custom snapshot workers during isolate teardown. | Use actual teardown laggard diagnostics; explicitly drain coordinators before their serialization pool, with running and queued dependency work in the regression. | [Snapshot executor write-up](postmortems/native-snapshot-executor-teardown.md), `SnapshotExecutorShutdownTest` and native shutdown omission |

- Inspect the final changed-source mutation inventory after documentation edits: Java comment cleanup can add unrelated whole-class baselines to a bounded gate. Defer that cleanup instead of weakening mutation scope or deadlines. [Evidence](postmortems/pr-fastpath-comment-scope.md).
- **Drain lexical state at EOF:** streaming presentation adapters must preserve the last token
  without requiring a trailing delimiter. Test one-character chunks and EOF in every deferred
  state, then compare the complete ANSI-stripped payload with the input.
  Evidence: [shell final-token regression](postmortems/shell-stream-final-token.md).
  Keep escape rules specific to the serialized language: XML attribute quotes are not
  escaped by backslashes. ANSI golden tests must check style transitions as well as payloads.

- **Preserve a PrintStream's declared encoding when adapting it:** use its `charset()` rather
  than assuming the JVM default. Test the adapter with a non-default Unicode encoding and
  a Windows code page. Evidence: [shell output encoding](postmortems/shell-output-encoding.md).

- **Do not label line accumulation as syntactic continuation:** a blank-line shell protocol
  cannot infer whether the preceding query is incomplete. Check prompts and spacing together
  in a real terminal transcript. Evidence: [shell continuation prompt](postmortems/shell-continuation-prompt.md).

- **Infer shell context only from supported executed forms:** a literal call in arbitrary source
  may be skipped or nested in a mutation that is not an updating expression. Keep uncertain
  expressions neutral and test both stores with actual commits and skipped/deferred accesses.
  Literal-name inference must also respect parser decoding. Decline entity/character
  references before metadata lookup when exact decoding is unsupported.
  Diff colouring needs the same rule at the output boundary. Check skipped/deferred calls,
  mixed results and direct controls for both stores and both operation representations.
  Evidence: [shell revision inference](postmortems/shell-revision-inference.md) and
  [shell diff inference](postmortems/shell-diff-inference.md) and
  [encoded prompt names](postmortems/shell-encoded-names.md).

- **Test lexical precedence at overlapping forms:** an opening parenthesis can follow both an
  expression keyword and a function name. Assert rendered styles for both, including callable
  keyword built-ins, the demo's `count(...)`, `last()`, `replace(...)` and incomplete/multiline input.
  Evidence: [shell keyword precedence](postmortems/shell-keyword-precedence.md).

- **Distinguish XML document contexts from patch strings.** Follow the quoting rules in
  the [shell presentation contract](shell-presentation.md). Check payloads and styles
  across markup terminators, write boundaries, finish/reuse and mixed formats.
  Evidence appears in [XML content quoting](postmortems/shell-xml-content-quotes.md).

- **Cover every serialized diff operation in the palette contract.** Structural JSON
  replace and scalar update both represent modifications. Pair the operation golden
  with an actual structural replacement rendered through the terminal entry point.
  Evidence appears in [JSON replacement colour](postmortems/shell-json-replace-colour.md).

- **Exercise process and streaming boundaries explicitly.** Keep subprocess argv
  portable and set the encoding of any byte golden. Pair process smoke tests with
  in-process CLI assertions for mutation coverage. Assert exact buffer boundaries
  across overloads and reuse; make shared cursor-reset postconditions explicit.
  Keep Unicode output goldens independent of native argv conversion, and keep
  empty-buffer decisions at callers that admit empty buffers.
  Evidence: [shell CI boundaries](postmortems/shell-ci-boundaries.md) and
  [CI follow-up](postmortems/shell-ci-follow-up.md).
  Keep process termination at the entry point and expose command status separately
  so mutation tests can assert batch failures without terminating their JVM.
  Evidence: [CLI mutation coverage](postmortems/shell-cli-mutation-coverage.md).

### Lexical language boundaries

Recognize diff operation positions before matching operation spellings. Check neutral
attribute names and paths as well as coloured verbs, including qualified and hyphenated
names from actual serialized diffs. Evidence: [XML operation positions](postmortems/shell-xml-operation-positions.md).

Preserve JSON versus XQuery patch context through the output adapter. A backslash is
literal in XQuery strings, and either quote may delimit them. Check styles after strings
as well as payload preservation: the real diff must restore the next operation's colour.
Exercise both quote forms, doubled delimiters, split writes and JSON controls.
Evidence: [XML patch quoting](postmortems/shell-xml-patch-quotes.md).

The input scanner needs the same language distinction: JSONiq single-quoted strings
use literal backslashes, while double-quoted strings retain escapes. Validate the value
with the executable parser and the subsequent token styles independently, across every
multiline prefix boundary. Check both diff selectors and keep unsupported prompt names
neutral. Evidence: [input backslashes](postmortems/shell-input-backslashes.md).

- **Verify mutation coverage with CI's automatic test selection.** An exact-name
  test match takes precedence over package fallback. Keep renderer unit regressions
  in its matching test class; a broader focused run can otherwise hide surviving
  mutants in the default lane. Public exception diagnostics also need boundary and
  accessor coverage: a positive-wait integration failure cannot reject a constructor
  that wrongly refuses zero or an accessor that loses the page key. Evidence:
  [word-buffer mutation scope](postmortems/shell-pit-word-buffer-scope.md) and
  [HOT retirement diagnostics](postmortems/2026-11-hot-writer-unheld-leaf-reads.md#ci-follow-up-retirement-diagnostic-coverage) and
  [RAM lifetime contracts](postmortems/ram-storage-committed-revision-loss.md#ci-follow-up-executable-contracts-and-mutation-selection).
  Keep child-JVM lifetime and memory tests in a separate ordinary-CI suite: children load
  unmutated classes and cannot observe PIT instrumentation in their parent. Verify the full
  bounded lane and use counterfactual evidence for effects observable only in clean processes.
  For cross-process exclusion, let a child hold the real OS lock while the parent retries cleanup;
  assert refusal and later recovery for both original paths and tombstones. Parent-side operations
  remain observable under PIT even though the independent lock holder is unmutated.
  [Ownership-retry follow-up](postmortems/ram-storage-committed-revision-loss.md#ci-follow-up-cleanup-retry-must-respect-independent-ownership).
  [Forked-contract follow-up](postmortems/ram-storage-committed-revision-loss.md#ci-follow-up-isolate-forked-lifetime-contracts-from-pit).
  Writer-lifecycle regressions must also distinguish historical update targets from the latest
  committed head, and caller-held edits from apply-created state, under both commit strategies.
  [XML snapshot and ownership coverage](postmortems/shell-query-failure-recovery.md#ci-follow-up-xml-snapshot-and-writer-ownership-coverage).
  Cross-class timestamp suites also need explicit associations to writer entry points;
  exercise a commit after reverting content so an older represented root cannot stand in
  for the latest committed predecessor. CLI fixtures must own their repository metadata
  as well as their output manifest. Evidence:
  [timestamp mutation follow-up](postmortems/commit-timestamp-ci-associations.md#follow-up-timestamp-mutation-scope-and-revert-coverage).

- **Check semantic state beyond scratch-capacity boundaries.** A lossless streaming
  renderer can still lose container context and style keys as values. Exercise
  both sides of each growth boundary, mixed parent containers and retained-buffer
  reuse; bound exceptional growth separately from steady-state allocation.
  Evidence: [JSON container depth](postmortems/shell-json-container-depth.md).

- **Keep historical size bounds out of sampling populations.** A node-key frontier
  retains deleted keys and can make a stride skip every surviving value. Use a
  stored live count for sampling, retaining stride 1 when unavailable; keep the
  frontier for size gating. Test deletion-heavy histories after cold reopen with
  known surviving values and a hashed control through collection and registration.
  Evidence: [histogram deletion regression](postmortems/hash-default-consumers.md#follow-up-deleted-keys-are-not-a-sampling-population).

- **Assert positive selection for every supported index kind.** CAS coverage and
  no-index cases cannot detect a broken NAME lookup. Use persisted definitions as
  the oracle for NAME/PATH/CAS priority, unmatched and empty paths, revisions and
  cached lookups. Evidence: [statistics mutation coverage](postmortems/hash-default-consumers.md#ci-follow-up-name-index-selection-lacked-assertions).
- Consume resolved HOT boundary heights during each load: a later load may clear an earlier PageReference. Allocation comparison factories may retain temporary arrays on another JVM; keep exact production output budgets independent of that control (postmortems/hot-ci-boundary-heights.md).
- Extract complete cold HOT decisions at shared boundaries when complexity grows;
  helper extraction changes bytecode, so retain pinned allocation budgets and
  recapture compiled shape before claiming latency neutrality. Evidence:
  [HOT complexity follow-up](postmortems/hot-codefactor-complexity.md).
- Split update lifecycle orchestration at whole-loop boundaries, preserving writer ownership,
  cleanup order and failure suppression without adding per-operation calls. Evidence:
  [XML apply complexity](postmortems/shell-query-failure-recovery.md#ci-follow-up-xml-apply-complexity).
- **Bound retained pages by ownership.** A full-fanout node can produce additional private leaves
  during repeated merges. Exclude those replacements at the shared acquisition boundary and
  check capacity before taking a guard. Exercise full fanout and count live frames to catch guards
  leaked on consumed replacements. Evidence: [HOT retention follow-up](postmortems/2026-11-hot-writer-unheld-leaf-reads.md#review-follow-up-private-replacements-exceeded-the-retention-bound).
- **Replay the failure's environment as well as its operations.** Store eviction and frame-poisoning
  settings in the case configuration shared by generation, shrinking and replay. Check actual
  eviction and released bytes, including cleanup after failure, rather than accepting a green replay
  with its trigger disabled. Evidence: [HOT replay follow-up](postmortems/2026-11-hot-writer-unheld-leaf-reads.md#review-follow-up-replay-lost-the-eviction-trigger).

<a id="commit-attempt-state"></a>

- **Distinguish attempt state from committed state.** Retrying must derive metadata from the original
  committed predecessor and the retry input, while repeated serialization within an attempt remains
  stable. Exercise a failure after assignment, both increasing and decreasing replacement inputs,
  the synchronous and asynchronous recovery paths, and warm as well as cold read-back. Repeat the
  failure after each metadata publication boundary: an index append before the final flush can
  survive a failed attempt even when the root timestamp itself is recomputed correctly. Keep effective timestamps from other timelines as mapping metadata, not custom commit input:
  monotonic adjustment can place them ahead of a regressed clock. Verify dependent maintenance and
  folds with a controlled backward clock step. Verify formatted metadata through its actual
  serializers and handlers, including zero and trailing fractional groups.
  Recompile merged test sources after a unit-bearing API rename: newly integrated fixtures must
  migrate both accessors and their independent numeric oracles.
  Evidence: [commit timestamp retry](postmortems/commit-timestamp-retry.md).

- **Give public reader metadata a focused behavior contract.** The mutation selector's package
  fallback can pull unrelated integration suites for a one-getter change; JUnit 5 executes them
  again during mutation discovery. Verify exact historical/current metadata through the public
  reader, then check the selector's emitted scope without widening its limits or excluding tests.
  Keep independent clock and history contracts separately executable, retaining both in ordinary tests.
  Evidence: [timestamp CI associations](postmortems/commit-timestamp-ci-associations.md#follow-up-focused-path-summary-timestamp-contract).
| Async hardening's background path was safe while two inline ownership routes were not; `isClosed` was mistaken for recovered storage. | Derive failure tests for every ownership transfer, including paired construction/submission and hardening faults. Fence workers, release independent owners, and require marker recovery before offset reuse; test a second session with warm caches. | [write-up](postmortems/async-commit-ownership-recovery.md), `NodeTransactionAbortLifecycleTest`, `AsyncFlushFailurePathTest` |

- Test worker ownership independently of operating-system I/O cancellation: inject a known cancellation failure at the storage boundary for fence/interrupt assertions and keep file-backed correctness comparisons separate. In multi-stage hardening, rethrow the accumulated first failure after retirement, outside finally. See [the CI postmortem](postmortems/lifecycle-ci-portability.md).

- Establish who can perform an executor fence before adding it to close. The [hook contract](hooks.md) supersedes the callback-initiated teardown in the historical [self-join](postmortems/scheduler-close-self-join.md) and [lock-ordering](postmortems/scheduler-close-lock-ordering.md) fixes. Recheck open state under the ownership lock before recovery handling, and test external session close and rollback as well as transaction close, including RAM backends where a spurious marker blocks admission. Check both embedded and keyed root representations against the same published object; a valid inline page must not hide a broken storage key. See `NodeTransactionAbortLifecycleTest`.

- Keep owner registrations alive until their final release obligation is discharged, not merely until payload close publishes CLOSED. Serialize registration removal and permit release at one boundary; test a second closer in the window after reader cleanup and fencing, while another user keeps the resource alive. See [the owner-registration postmortem](postmortems/node-owner-admission-release.md) and `lateConcurrentCloseRetainsOwnerUntilPermitRelease`.

- Include setup in the lifecycle lock graph: never wait for a writer permit while retaining a monitor that the old owner's close needs. Revalidate closing/open state after acquiring admission, before allocating any reader or writer. Test same-session queued node/page admission with both owner close and session close. See [the admission-cycle postmortem](postmortems/writer-admission-monitor-cycle.md).

- Guard lifecycle reentry at the database boundary, including nested callbacks and sibling handles/sessions. Use the [database notification contract](hooks.md) rather than the superseded writer-borrowing design in [the callback postmortem](postmortems/commit-callback-reentrancy.md).

- Test async notification timing with held durability and delivery barriers, and assert identities and reported failures outside the dispatcher. The earlier final-close Error oracle is historical; see [the current error policy](hooks.md) and [callback completion postmortem](postmortems/callback-completion-reader-ownership.md).

- Post-commit hooks must not share a mutable writer cursor with a lock-free successor. Open a separate read-only cursor pinned to the committed revision for each hook, close it at dispatch, and test navigation on B while the writer updates A. Do not add row locking to compensate for shared callback ownership; remove borrowing-only reservation checks and fence job lifetime at teardown. See [the revision-view postmortem](postmortems/post-commit-revision-views.md).

- An identity ownership snapshot supplies membership, not teardown order. Fence every writer/callback source before closing any ordinary direct/shared reader, then refresh reader membership. Retain reader dependencies if a node owner refuses its fence; force and assert adverse initial reader-before-writer ordering in regressions, including interruption and cleanup errors. See [the session reader fence postmortem](postmortems/session-reader-fence-order.md).

- Exercise cross-session readers and destructive history changes against the [current notification lifetime boundary](hooks.md). The resource-shared completion fence in [the earlier postmortem](postmortems/shared-callback-history-lifetime.md) is historical.

- Check enclosing writer execution across sibling sessions before retirement waits, including raw page truncation. Cover both pre-publication hooks and post-publication readers; see [the current retirement contract](hooks.md) and [original regression](postmortems/resource-execution-prehook-lifetime.md).

- Registration does not guarantee notification progress. Test raw page publication followed by sequential sibling-session cleanup, awaiting dispatcher results independently of the publishing call; see [the current delivery contract](hooks.md) and [historical idle publication defect](postmortems/idle-page-publication-dispatch.md).

## Database notification ownership

Reject lifecycle reentry before database-handle admission, not only inside resource/session cleanup.
Keep synchronous pre-publication veto hooks separate from asynchronous after-durability notifications.
Bound callback drain, drop only unopened queued work, and retain live reader/backend dependencies on
close timeout. Recovery after truncation must select an existing durable revision, even if a node cursor still names discarded history. Resolve dispatcher read views from durable roots even while writer pending roots exist.
Exercise the public wrapper and nested database scopes in deterministic interleavings; never treat
cancellation or a deadline as proof that a callback released its readers.
See [postmortem](postmortems/database-notification-dispatch.md) and [contract](hooks.md).

## Resource bootstrap and notification drain

A session close can hide inside resource bootstrap, removal or derived-resource repair. Establish
notification quiescence before the outer handle/database monitor, including eager work behind session
open. Never restrict already permitted callback metadata reads to compensate for reversed lock order.
Keep registration serialized and move mutating initialization outside admission monitors.
See [postmortem](postmortems/bootstrap-notification-monitor-order.md).

## Mutation gates precede notification retirement

Trace outer collection/store authorization gates before moving lifecycle waits. Any path needing
both a mutation gate and notification retirement must acquire the gate first, then drain outside
admission monitors. Reject callback mutation at gate entry itself; keep independent metadata reads
ungated. Include held-outer-gate regressions alongside callback-drain tests.
See [postmortem](postmortems/mutation-retirement-lock-order.md).

## Separate quiescence from exclusive ownership

Notification retirement pauses callback admission and drains existing readers; it must not serve as
an exclusive lock between lifecycle owners. Trace execution/history ownership and worker joins in
both directions. Destructive maintenance must fence hardening and append workers at the shared
page boundary, including raw entry and captured predecessors. Test scheduled admission refusal
separately from a user-hook veto so temporary maintenance cannot disable later ticks.
See [the ownership postmortem](postmortems/storage-maintenance-retirement-ownership.md).

## Deferred publications still own their history

A deferred callback that opens its view later still depends on the original committed pages.
Destructive admission must account for deferred as well as executing work; a reused revision number
is not the same publication. Bound the combined active, queued and deferred retention budget, and
check saturation with a latched consumer while proving durable commits finish. Never turn overflow
into caller-thread delivery or claim replay that is not implemented.
See [the deferred-publication postmortem](postmortems/deferred-notification-history.md).
