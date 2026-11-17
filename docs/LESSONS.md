# Lessons from failures

Keep entries short and evidence-based: broken assumption, prevention, and fix/test evidence.
For each new bug, add a [write-up](postmortems/TEMPLATE.md), a failing-before regression and a
lesson here. Extend an existing lesson when the same failure mode recurs.

| Failure / broken assumption | Apply next time | Repository evidence |
| --- | --- | --- |
| An isolated opening-work test counted independent derived-index rewind reads through a process-global counter. | Scope work observers to the operation's owning thread when opening starts background maintenance; use joined real reads on another thread and corrupt-primary recovery to verify isolation without weakening bounds. Document bounded primitive thread-lifetime ownership at a field-scoped analyzer suppression rather than resetting cumulative diagnostics per read. | [Beacon counter write-up](postmortems/beacon-opening-work-counter.md), `AbstractReaderBeaconWorkTest`, unchanged `derivedOpenReadsOneBeacon` |
| PR mutation selection assumed filenames were JVM class names; a timeout repair also allowed uncovered mutants to pass. | Resolve declared test identities before compilation at the shared discovery boundary; keep scope bounds separate from the [acceptance criterion](VERIFICATION.md#blocking-criterion-the-prs-own-lines). Test mismatched filenames across every lookup path and execute blocking/advisory report handling. | [Mutation scope write-up](postmortems/pr-mutation-test-identity.md), `SelectionTest` and `ReportingTest` in `test_pr_mutation.py` |
| JSON serialization assumed every result item matched a supported branch; unsupported items could leave the iterator on the same item. | Specify iterator progress and unsupported-type errors; test unsupported items alone and mixed into sequences. | [c9efc3a7b158](https://github.com/sirixdb/sirix/commit/c9efc3a7b158a12af7fc544c03b6113081ffe077), `JsonDBSerializerTest` and `QueryJsonSerializationTest` |
| Query mutation authorization relied on an updating-expression flag; ordinary function calls could mutate too. Assertions comparing absent aggregate headers also became vacuous after storage moved. | Guard the actual mutation boundary; require the witness to exist, assert persisted state is unchanged on refusal, and include an authorized control. Verify the relevant JVM tests actually run in CI. | [36ee410f1f8d](https://github.com/sirixdb/sirix/commit/36ee410f1f8d0ff201bcb80170781f8d86b47550), `QueryMutationAuthorizationTest`, `PermissionCheckingQueryTest` |
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
| An interval test expected five rapid commits to have distinct millisecond timestamps; PIT exposed the valid last-tie result. | Use explicit timestamps for ordering and tie fixtures, and assert exact revision IDs against the floor lookup contract. | `OpenRevisionsTest`; [write-up and reproduction](postmortems/pr-fastpath-ci.md#follow-up-revision-interval-baseline-flake) |
| A refusal wrapper covered only the sequential compiler while the parallel strategy admitted the same SPI. | Enumerate every physical compiler, function interception and public factory; test refusals and admitted controls through each, including outer count semantics and cold reopen. | `VectorizedReleaseCorrectnessTest`; [parallel grouping write-up](postmortems/parallel-grouping-refusal.md), [count-wrapped grouping write-up](postmortems/count-wrapped-grouping-refusal.md) |
| Bounded PIT naming heuristics omitted a compiler/refusal contract; its parallel cases also omitted the non-grouping return. | Associate integration contracts at the shared selection boundary, preserve matches and caps, include ordinary-path controls, and verify real mutant outcomes. | `SelectionTest` release associations, `ordinaryPipelineRetainsRowsAcrossReopen`; [write-up](postmortems/pr-fastpath-mutation-selection.md) |
| Long allocation-test loops warmed OSR but left normal helper entry cold; late VM string materialization contaminated zero-byte windows. | Warm the helper with repeated short calls before measurement; retain strict N/2N checks and an escaping-allocation control, and diagnose transient bursts with allocation stacks. | `BitemporalLoaderBudgetTest`; [warmup write-up](postmortems/bitemporal-allocation-warmup.md) |
| A deferred accumulator's last type marker was mistaken for proof that every counted value was numeric; a later number erased an earlier string marker. | Test invalid values before, between and after valid ones, including cold reopen; refuse summary serving when persisted metadata cannot certify the required type invariant. | `VectorizedReleaseCorrectnessTest.mixedStringPlacementPreservesGenericError`; [purity write-up](postmortems/path-summary-numeric-purity.md) |
| A seek returned the first entry of the leaf a key was routed to although the key sorted below that leaf: routing by discriminative bits says nothing about the leaves before it. The tests probed stored keys and leaf-boundary gaps only. | Compare every seek and range with an independent sorted reference over keys that are absent in every way: one bit of a stored key flipped, truncated and zero-padded forms, numeric neighbours. Assert that the probes reach each geometry (routed below, above, inside the leaf), and make a condition state exactly the case its comment argues for. | [Lower-bound postmortem](postmortems/2026-11-hot-lower-bound-below-leaf.md), `HOTTrieReaderBoundContractTest`, `HOTTrieReaderBoundPropertyTest`, `HOTTrieReaderCasRangeRegressionTest` |
| HOT factories, readers and structural guards disagreed on the minimum indirect fanout. | Derive construction, decode and publication checks from I10; exercise invalid bounds in both layouts and keep budget fixtures within the legal fanout domain. When tightening a selector, check every caller and legacy test: deletion must handle removal/collapse before selecting a legal type. | [Cardinality write-up](postmortems/2026-11-hot-indirect-cardinality.md), `HOTIndirectCardinalityTest`, `NodeUpgradeManagerTest` |
| MCP treated identical head content as a no-op while REST/core recorded the revert event. | Test persisted revision counts, returned metadata and historical content across every interface and target representation. | [Revert parity write-up](postmortems/2026-11-revert-head-parity.md), MCP `ToolHandlersIntegrationTest.Revert`, REST `RevertHandlerTest` |
| Shared benchmark setup required optional absent-probe geometries unavailable in a one-entry leaf. | Apply each probe requirement only to its consuming stage and run a smallest-corpus smoke check through the real harness. | [Benchmark write-up](postmortems/2026-11-hot-benchmark-small-corpus.md), `:sirix-benchmarks:verifyHotSmallCorpus` |
| HOT runtime regressions passed while large validation/seek methods and opaque assertion helpers failed Sonar's maintainability gate. | Extract cold validation stages without changing failure priority, work or allocation; never split a hot seek or per-insert kernel for the analyzer, keep its measured shape under a narrow reasoned S3776 suppression; isolate throwing operations and name delegated assertion helpers explicitly. Verify with the analyzer as well as behavioral oracles; count-source-only changes can expose the same inherited query traversal complexity. | [CI write-up](postmortems/2026-11-hot-sonar-maintainability.md), [query follow-up](postmortems/hash-default-consumers.md#ci-follow-up-unchanged-traversal-complexity), HOT cardinality/footprint/detector/bound suites and MCP integration tests |
| A dangerous option was safe only because every caller happened to disable it: `DiffFactory.Builder.hashKind` hard-coded `ROLLING` regardless of the diffed resource, and the hash shortcut was avoided only because each caller separately derived `DiffOptimized` from the resource's hash kind. Changing the hash default would have made the unsafe combination the common case. | Put the gate on the state that makes the option valid, not on each caller's discipline: derive the default from the resource and refuse the shortcut when the resource stores no hashes. Test the direction a caller must not be able to force, not only the happy path. | [Write-up](postmortems/hash-default-consumers.md), `DiffFactoryHashKindTest`, `JsonDiffHashEquivalenceTest`, `XmlDiffHashEquivalenceTest` |
| A field that exists only under one configuration was read as if it were always maintained: stored descendant counts are gated on `hashType != NONE`, and four planner size heuristics read `getDescendantCount()` as a document-size signal, so a hashless resource looked empty and lost its index route, its parallel axis and every cost-based annotation. | When a planner heuristic reads a stored statistic, check whether that statistic is configuration-gated and give it an unconditional fallback. Guard it with a test that asserts the decision input, since a plan change leaves results exact and no result test notices. | [Write-up](postmortems/hash-default-consumers.md), `DocumentNodeCountTest`, `DocumentNodeCount` |

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
  mutants in the default lane. Evidence: [word-buffer mutation scope](postmortems/shell-pit-word-buffer-scope.md).

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
### Logical codec bounds

When codec input uses reusable scratch, test every truncated frame length while
leaving the complete valid frame in the backing array. Run the same adversarial
contract against every output overload: bounds against array capacity alone can
silently consume stale bytes. Require exact input consumption and bound decoded
output before any writes. See the [native zero-run postmortem](postmortems/2026-11-17-zero-run-native-frame-bounds.md).

### Cold paths can change warm compilation

When adding a cold codec, include its setup work in fresh-process warm lookup
benchmarks. Heap-backed foreign-memory scalar accesses can alter process-wide
receiver profiles and slow unchanged native lookup methods. Keep heap parsing
on array accessors and native FFM paths native-only; verify compiled shape as
well as allocations. See the [leaf-codec postmortem](postmortems/2026-11-17-leaf-codec-receiver-profiles.md).

### Measure allocation at each cold entry point

A zero-allocation warm lookup does not establish an allocation-free codec.
Compare raw and encoded arms for encoder selection, decode into existing storage,
full/compact materialization, image creation and uncached selection. Reuse input
arrays **and** parsing cursors. A native view eliminated in a standalone
helper may still allocate in a larger caller; use JFR stacks to verify whole
entry points and remove avoidable view construction instead of assuming escape
analysis will remove it. When instrumentation changes the result, disable escape analysis for the allocation-site comparison. Gate every fresh fork, rather than only the mean. Pass owning base segments and primitive offsets through an adapter instead of replacing checked FFM arguments with raw addresses. Compare unchanged main with owned-result fixtures. When substituting scratch for an exact array, test every logical
truncation after a valid longer frame and verify previously published images
remain independent. See the [codec allocation postmortem](postmortems/2026-11-17-leaf-codec-allocation.md).

### Move conditional native resources with their loader

A loader refactor must update native-image `typeReached` resource conditions as
well as Java calls. JVM tests do not apply those conditions. Pin the resource's
actual loading type in a packaging regression, and keep downcall descriptors in
sync with adapter signatures. See the [resource reachability postmortem](postmortems/2026-11-17-native-codec-resource-reachability.md).

### Production read ownership and retention

A direct decoder comparison can hide a production regression when raw reads
transfer decompression storage and encoded reads allocate a second destination.
Exercise the public reader as well as the real byte-handler ownership route;
remove an identity-pipeline copy when the decoder always owns its output. Assert live allocator bytes and
cache charge, then force mutation and verify lazy promotion invalidates the old
stamp and releases its allocation. Include failure after decode, such as a
truncated side-reference trailer. See the [retained-frame postmortem](postmortems/2026-11-17-leaf-codec-retained-frames.md).

### Codec preference is a separate boundary

A smaller encoding does not automatically justify a more expensive decoder.
Use the specified shared margin for that choice, and test margin-minus-one,
margin and margin-plus-one for both decoder choice and raw fallback, with framing
included. Measure codec lengths independently and assert the emitted codec as
well as the round trip. See the [preference-margin postmortem](postmortems/2026-11-17-leaf-codec-preference-margin.md).

### Codec CI contracts

Keep exact packing oracles on decoded logical frames and checksum oracles on stored wire
frames. Compression fixtures used across platforms must compress with every supported
codec set; explicitly exercise missing-native-codec behavior. Verify native staging names,
fallback directories and losing-publication cleanup through filesystem behavior, and
separate functionally equivalent decoder-routing mutants from missing correctness tests.
Keep encoded-frame expanders strict when callers already route raw frames: accepting raw
scratch adds a redundant copy route that functional assertions cannot distinguish. Upload
reports from all test tasks, including allocation forks, and print complete failures there.
See the [leaf codec CI postmortem](postmortems/2026-11-17-leaf-codec-ci.md).

- **Do not depend on escape analysis to remove codec accessor objects.** A heap segment's
  `heapBase()` returns an `Optional` per call. Reuse the writer's cached buffer view when
  accessing its heap array, and validate allocation gates on HotSpot with escape analysis
  disabled as well as the workstation compiler. Exercise both process settings through
  the shared writer, and document intentional thread-lifetime scratch at the field.
  Evidence: [leaf codec CI follow-up](postmortems/2026-11-17-leaf-codec-ci.md#follow-up-hotspot-allocation-writer-mode-coverage-and-sonar-lifetime).
