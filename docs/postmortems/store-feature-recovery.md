# Bug: bootstrap repair could discard active store metadata

- Fix / issue: store-level feature words; `AbstractReader.isEmptyBootstrapBeacon` and `InterruptedFirstCommitRecovery`.
- Symptom: a resource with revision-zero beacons, active feature words and a missing revision record could be reinitialized as feature-free. The store's requirements must survive recovery even when no data revision is reachable.
- Trigger: write checksum-valid revision-zero beacons containing a read-only-compatible feature bit, remove the revisions-file records, and reopen with cold caches.
- Root cause: the old emptiness proof equated revision zero with an empty store. Feature metadata invalidates that equivalence. Repair now requires a revision-zero beacon **and empty feature words** for every nonzero slot. A valid but unsupported feature is also kept separate from corruption, so it cannot cause fallback to an older beacon or bootstrap repair.
- Why tests missed it: earlier fixtures only carried a revision count in their uber pages; none could represent durable metadata independent of the revision frontier.
- Regression test: `InterruptedFirstCommitRecoveryStoreFeaturesTest.bootstrapRecoveryCannotEraseActiveFeatures`, for FILE_CHANNEL and MEMORY_MAPPED. The test asserts refusal and byte-for-byte preservation of the data file. An isolated compiled-class control removes only `&& page.getFeatures().isEmpty()` while keeping the rest of the implementation and tests unchanged; both backend cases fail because opening unexpectedly succeeds. Both pass with the predicate restored.
- Lesson: prove metadata as well as data is disposable before replacing a root; see [Lessons](../LESSONS.md). The existing `.commit`-marker truncation protocol is unchanged. Its beacon-slot copy preserves all feature words, while explicit rollback retains the committed words when changing the revision count.

## Review regressions: compatibility before repair or rebuild

- Symptom: a read-only-compatible store could rewrite a damaged revision record during metadata preload, and an unsupported derived resource could be deleted and recreated when its parent opened.
- Invariants: establish compatibility before any repair; unknown read-only-compatible requirements prohibit healing while still permitting tail-log reads. Unsupported features must bypass every destructive corruption-recovery boundary.
- Affected paths: cold metadata-cache and revision-index preload, subsequent file-channel revision reads, direct tool opens, backup source preflight, restore verification, and derived-resource opening through both beacon and session-constructor refusal. Both beacon choices and all three feature words are covered by tool refusal.
- Error cases and oracle: a valid committed beacon with a salvageable missing revision record must leave both binary files unchanged when read-only or refused by a tool; an unsupported derived sibling must remain byte-identical while its parent retains its ordinary query fallback. Feature-free salvage and supported derived opens must continue to work.
- Root cause: metadata preload used a repair-capable reader before the tool policy was checked, and the derived rebuild boundary classified every runtime exception as corruption. Existing tests used intact revision files, warm metadata caches, or directly opened resources; they did not combine these states with compatibility refusal.
- Regression tests: `InterruptedFirstCommitRecoveryStoreFeaturesTest.readOnlySalvageDoesNotHealRevisionRecords`, `damagedRevisionToolRefusalPreservesBothFiles`, and `parentOpenPreservesUnsupportedDerivedSibling`. Their expected bytes come from the persisted-state preservation contract, not implementation text.
- Lesson: compatibility refusal is a distinct outcome at every repair/rebuild owner, including implicit writes on read paths; see [Lessons](../LESSONS.md).
- Verification status for review round 1: the focused Gradle command was queued through the supplied `heavy.sh` wrapper for about 20 minutes without starting Gradle because shared heavy-command slots remained occupied. It was stopped without touching other jobs. The four selected test classes and five guard-removal mutation controls in `build/review-fixes/verification.gradle` remain unexecuted; no passing or mutation-kill result is claimed.

## Review round 2: all open-time mutation owners

- Broken invariant: no resource file may be created, healed, cleaned, truncated or rebuilt before its committed beacon establishes compatibility for that operation. Tool inventory includes hidden derived resources, even though the public resource listing hides them.
- Root cause: the first review guarded one implicit writer and one exception type, leaving file creation and spill cleanup before the checks, generic derived failures outside that exception path, and a filtered tool inventory.
- Specification: one read-only beacon probe supplies the feature words without touching revision files or repairing anything. Database inventory probes every persisted directory; resource/storage opens apply readability before initialization, missing-file creation and recovery require writability, read-only spill cleanup is skipped, and derived rebuilding requires writability before structural/snapshot checks and again at the destructive boundary. Backup and restore inspect the physical resource inventory before opening any parent or creating a target.
- Error cases: missing revision files remain absent for incompatible/read-only stores; orphan spills remain present; unsupported derived stores survive incomplete directory structure and damaged revision metadata; every unknown word in a hidden sibling refuses backup and restore. Assertions compare directory entries and persisted bytes against their pre-open state. Feature-free bootstrap recovery, salvage and orphan cleanup remain supported.
- Test gaps: previous fixtures covered torn records but not absent files or orphan spills, and derived refusal but not an earlier/later unrelated exception. Tool tests only put features in publicly listed resources.

- The database-open inventory introduced here was removed in review round 3 below; the current open and tool-inventory contract is owned by [DISK_FORMAT.md](../DISK_FORMAT.md#uberpage-store-feature-words). If a derived resource's settings or beacons cannot establish compatibility, it is retained and the source fallback applies.
- Regression map: `unsupportedOpenPreservesMissingFilesAndSpills` and `xmlOpenPreservesUnsupportedFilesAndSpills` cover file absence; `readOnlyOpenPreservesOrphanSpills` covers cleanup; `unsupportedDerivedDamageNeverTriggersRebuild` covers missing logs, missing revision metadata and an independently checked invalid frontier; `toolsRefuseHiddenDerivedFeaturesBeforeOpeningParents` covers all three unknown words in hidden siblings. `featureFreeDerivedSnapshotDamageStillRebuilds` retains the supported repair control.
- Verification: the focused Gradle invocation completed successfully with **109 tests, zero failures, errors or skips**, plus the test task's existing auxiliary allocation/identity/cache checks. The selected work-budget class passed without changing bounds. Selected classes were `StoreFeaturesLifecycleTest`, `InterruptedFirstCommitRecoveryTest`, `CrashRecoveryOrderingTest`, `BackupRestoreTest`, `FileChannelStorageTest`, `MemoryMappedStorageTest`, `ProjectionSortedRunSpillTest` and `IndexCatalogueResolutionWorkBudgetTest`.
- Mutation evidence: all **seven controlled variants were killed**: returning empty probe words, removing file-creation authorization in each backend, unconditionally cleaning spills, removing both derived rebuild probes, filtering hidden stores from inventory, and removing the read-only healing guard. Failures were behavioral assertion failures: altered directory entries/bytes, wrong refusal type, or missing tool refusal. There are no surviving generated variants; this is targeted mutation evidence, not a claim of full legacy-class PIT coverage. The generator and reports are under `build/review-round2/verification.gradle` and `build/review-round2/*/results`; the successful command log is `build/review-round2/verification-3.log`.
- Verification setup corrections: the first attempt failed before compilation because a Groovy variable shadowed `source`; the harness now calls `setSource`. The next attempt compiled production code and passed 101 tests, but eight derived cases failed during setup because public resource creation reserves hidden names. Fixtures now persist an ordinary resource through the public API, close it, move its directory to the hidden sibling location, and deserialize its configuration before injecting faults. The inherited round-one test's direct hidden-resource open was replaced with the storage-open interface, keeping the compatibility-refusal assertion. The corrected suite passes, and the derived guard-removal control changes the directory snapshot as expected.
- Lesson: a regression must reach its intended boundary; validate the malformed-state precondition and exercise the fixture before interpreting mutation results. No repository-wide test or lint suite, pipeline control, commit or push was run by this phase.

## Review round 3: database handles must not scan resources

- Symptom: opening or reusing a database handle parsed settings and read beacons for every resource, although no resource was opened.
- Invariant: both JSON and XML database handle opens, including owner reuse, perform no resource-file I/O; compatibility checks belong to opening a target resource or to a tool that actually inspects the complete inventory.
- Root cause: round 2 broadened the resource compatibility boundary into a database-wide scan. Its ordinary-open mode discarded all results and swallowed failures, so the added work provided no protection. Existing preservation tests checked safety, not handle-open work.
- Specification and regression oracle: remove the ordinary scan and its boolean mode; retain a strictly named tool-only inventory check for backup and both restore preflights. With sixteen feature-free stores, cold and reused handles must open zero resource-file channels. A subsequent real resource read must open a resource channel, proving the external I/O observation is active. No clock assertion or budget widening is needed.
- Cleanup ownership: database handle opening has no spill cleanup. `LocalDatabase.beginResourceSession` cleans only its target, after probing that resource; unsupported/read-only opens retain their files and spills. Existing byte/existence tests remain required.
- Lesson: measure the public operation's work independently of its successful result, and delete modes whose results do not enforce an invariant; see [Lessons](../LESSONS.md).

- Regression: `InterruptedFirstCommitRecoveryStoreFeaturesTest.databaseHandlesDoNotReadResourceBeacons` passed for JSON and XML with sixteen real feature-free resources alternating both file backends. It checks new-owner and reused-handle opens, then performs a successful resource read to prove the real `FileChannel.open` observation is active. The zero-channel bound is stronger than zero beacon reads for these unopened stores.
- Verification: **77 focused tests passed, zero failures/errors/skips**, plus the existing auxiliary allocation/identity/cache tasks. Selected classes were `StoreFeaturesLifecycleTest`, `BackupRestoreTest`, `ProjectionSortedRunSpillTest` and `IndexCatalogueResolutionWorkBudgetTest`; no bounds changed. The supplied heavy-command wrapper and disk check were used; no benchmarks, repository-wide tests, lint or pipeline-control phases were run.
- Mutation evidence: **2/2 controlled variants killed**. Reintroducing the tool inventory scan on ordinary database opening produced **16 resource-channel opens instead of zero**, for both JSON and XML. Removing the strict inventory policy allowed hidden unsupported stores through backup, failing both backend cases. All four failures were behavioral assertion failures, with no generated survivors; this is targeted evidence rather than full-class PIT coverage. Logs and the reproducer are `build/review-round3/verification-2.log`, `build/review-round3/verification.gradle` and its two `*/results` directories.
- Verification setup correction: the first attempt compiled production code but the new test did not compile because wildcard reader cleanup can throw `Exception`. Declaring that exception on the test fixes the fixture's generic API use; the corrected invocation passed. No product change was needed after production compilation.

## Review round 4: reuse the opening snapshot

- R1 status: the ordinary database-wide scan and its no-op mode are already absent at this round's starting commit. The zero-resource-I/O regression remains the oracle.
- Broken invariant: a healthy physical resource open must read its committed beacon once, retain that uber page, and reuse it through storage initialization and session creation. Direct constructors and standalone tool opens establish their own snapshot; independent backup/restore inventory checks remain separate operations.
- Root cause: each nesting layer independently probed compatibility, while cache preload, index preload and session initialization also re-read the same beacon. No layer owned and passed the opening snapshot. Byte-preservation tests could not reveal the duplicate reads.
- Specification: retain the opening uber page on storage, pass it through derived-resource/session factories and optional storage initialization arguments, seed readers with the established feature words, and supply the known page to initial metadata preload. Later revision-index refresh keeps its fresh-read path. Structural and rebuild guards use the derived open's same snapshot; bootstrap repair still requires the existing proof that all committed data and feature metadata are disposable.
- Regression oracle: count canonical verified-beacon reads, covering both channel and mapped representations. Healthy JSON/XML resource opens, both file backends, nested derived opens and standalone tool opens must read one committed beacon; logical session reuse adds none. A source open with a derived sibling reads two beacons for two stores, and an explicit later index refresh must observe a newly committed revision. Fixtures materialize revision records first so necessary damaged-record salvage is not confused with redundant compatibility reads. Existing refusal, byte-preservation, marker-recovery and feature-free repair cases remain required.
- Verification: **139 focused tests passed, zero failures/errors/skips**, plus the existing auxiliary allocation/identity/cache tasks. Coverage included feature lifecycle, interrupted-first-commit and marker recovery, backup/restore, both storage backends, spill cleanup, sparse revision history, truncation and `IndexCatalogueResolutionWorkBudgetTest`; no budget bounds changed. The disk check and supplied heavy-command wrapper were used. No repository-wide suite, lint, benchmark or pipeline-control phase was run.
- Mutation evidence: **9/9 controlled variants killed**, all by behavioral assertion failures. Reloading either backend's opening page produced four beacon reads instead of one; dropping either preload snapshot produced two; dropping derived or tool context added a beacon read; discarding the seeded feature words changed the revision-file bytes; reusing the opening snapshot for later index refresh missed a committed revision; removing derived writability guards changed the directory snapshot. There are no surviving generated variants. This is targeted mutation evidence, not full-class PIT coverage. Reproducer and reports: `build/review-round4/verification.gradle`, `build/review-round4/verification.log` and its nine `*/results` directories.

## Review round 6: unsupported metadata is not corruption

- Symptom and trigger: a checksum-valid primary containing an unknown required entry, version or flag was rejected by the codec, then replaced by an older compatible secondary at open. Feature words earlier in the rejected payload could consequently disappear from the opening snapshot.
- Invariant and scope: checksum-valid unsupported metadata in either selected beacon must propagate as an unsupported-store failure; checksum and structural corruption alone permit fallback. The shared decoder/selection boundary owns this distinction for ordinary file and mapped opens, derived opens, standalone tools and backup/restore inventory. No consumer-specific recovery is added.
- Root cause: the codec used the same generic exception family for unsupported formats and malformed data, while beacon selection caught both as corruption. Earlier tests asserted codec rejection and beacon fallback separately, without combining a future primary with an older compatible secondary.
- Regression oracle: independently framed/checksummed future payloads (required tag alone, required tag after incompat words, unknown version, unknown flags and unknown incompat words) must refuse direct and tool opens without changing resource entries or bytes. A derived sibling remains unavailable and unchanged while its parent keeps its existing fallback; backup and restore refuse before creating a target. A corrupt primary with a valid secondary still reads successfully; a corrupt primary with an unsupported secondary must propagate the unsupported outcome.
- Lesson: preserve semantic refusal across redundant-copy selection, not just across repair boundaries; see [Lessons](../LESSONS.md).

- PIT follow-up specification: after a feature is published in an already-open session, direct page-writer creation must consult the committed words; independently opened read-only storage must refuse its own writer. A healthy read-only derived sibling is excluded from the parent's writable index participant. Raw readers must establish compatibility before salvage, pure probes must distinguish zero beacons from nonzero corruption, failed storage opens must release their owned storage, and compatible resource opens must still clean orphan spills. These are public state/side-effect or ownership contracts, not source-shape assertions.

- Verification: the initial focused run passed **115 tests** (codec, feature lifecycle, first-commit/marker recovery, backup/restore and the selected work budget); the strengthened focused run passed **93 tests**, including compatible spill cleanup. Both had zero failures/errors/skips and ran the core task's required auxiliary checks. The pre-fix codec control failed all four direct/derived backend cases with behavioral assertions; the fixed cases passed. No budget bounds changed.
- Real PIT evidence: three bounded PIT 1.25.5 runs completed. Initial: 339 kills by failing tests, 3 timeouts, 1 memory error, 185 survivors and 155 uncovered; supplement: 90 kills by failing tests, 49 survivors and 6 uncovered; file helpers: 10 kills by failing tests and one unchanged arena-lifetime survivor. The latest result for each of **688 mutation identities** is 356 kills by failing tests, 3 timeouts, 1 memory error, 174 survivors and 154 uncovered. All 40 uber-codec and 14 feature-policy mutants were killed. Every remaining survivor/uncovered site is listed with its class, method, line, mutation and rationale in [the PIT catalog](store-feature-pit-evidence.md), alongside scope and SPI/legacy coverage limits. This is not full-class legacy coverage; timeout/memory detections are not kills by failing tests.
- PIT-driven regression additions isolated the committed-root writer guard, raw-reader lazy compatibility, direct backend writer refusal, healthy read-only derived admission, pure-probe zero/partial/corrupt regions, failed-open ownership, and absent-data-file creation. The existing compatible orphan-cleanup control was included in PIT. These closed previously masked mutation sites without changing additional production logic. Raw reports, logs, final focused JUnit output, the failing legacy control, scope lists and source digests are preserved under `docs/postmortems/evidence/store-feature-round6/`; the catalog includes PR-description-ready evidence and must accompany the PR's remaining-mutant explanations.

## Test phase: align the legacy version-rejection oracle

- Fix / issue: test-phase follow-up to review round 6's unsupported-format classification.
- Symptom: the focused suite failed `GoldenFormatTest.unknownPageVersionsAreRejectedBeforeDecoding`: expected `IllegalStateException`, observed `UnsupportedStoreFeatureException`.
- Trigger: serialize an uber page, replace its version byte with `127`, then deserialize through `PagePersister`.
- Root cause: the round-6 fix correctly separated unsupported metadata from corruption, but the older golden-format assertion still named the corruption exception. Broadening the product exception back would reintroduce unsafe redundant-beacon fallback.
- Why tests missed it: the new lifecycle and codec checks asserted the distinct unsupported outcome; the older format suite was not included in their final focused verification.
- Regression test: retain the existing hostile version fixture and require `UnsupportedStoreFeatureException` specifically. The initial focused run failed this assertion before the test-only repair; the repaired suite passed alongside the documented core/query/MCP work-budget block. Reproduce with `./gradlew :sirix-core:test --tests io.sirix.format.GoldenFormatTest`.
- Lesson: when an intentional exception classification changes, execute both the new lifecycle regression and existing format-contract checks. No production logic changes in this repair; no new production mutants are introduced.


## Format baseline: refusals through repair boundaries

- Fix / issue: FMT-102/FMT-401 review findings R1/R2; FMT-402 evidence correction R3.
- Symptom: a foreign-format derived resource could be deleted and rebuilt, or a foreign-format
  revisions file truncated while recovering an interrupted bootstrap. Layout, uber and ordinary
  page versions also used different diagnostics. A timestamp arithmetic helper asserted only its
  own output rather than executing the importer.
- Trigger: open a valid-time source whose derived sibling names `binaryEncoding=V7`, or patch either
  file's layout version with a valid checksum; alternatively retain only bootstrap beacons and a
  foreign revisions header with no valid revision records. Exercise FILE_CHANNEL and MEMORY_MAPPED.
- Root cause: format refusals lacked a shared non-repairable classification. Derived fallback
  checked feature words without validating file formats; bootstrap proof validated only the data
  header before interpreting revisions records. The violated invariant is that no repair may
  reinterpret, delete or truncate foreign-format bytes. Recovery additionally needs each nonzero
  file identity to match its role and the configured resource UUID.
- Why tests missed it: direct helper calls bypassed real page decoding and repair owners; bootstrap
  cases had no foreign revisions header, and derived feature tests omitted foreign settings and
  superblocks. A second test-only timestamp implementation could pass without an engine adjustment.
- Regression tests and independent oracle: `FormatRepairBoundaryTest` opens real stores with
  checksum-valid foreign versions, mismatched UUIDs/roles/strides and missing derived settings.
  FMT-102/FMT-401 require refusal and byte-for-byte unchanged data, revisions and derived settings;
  these tests reject rebuilding or truncating the selected settings, superblock and uber-page
  images; they do not establish full page-envelope protection through repair or recovery.
  `FormatVersionBaselineTest` decodes
  foreign uber and revision-root envelopes through cold store opens, including a compatible
  secondary beacon; the observable oracle is the specified plain refusal and unchanged files.
  `HistoryPreservingExportImportTest` imports exported timestamps directly and compares with the
  fixture's known source timestamps, preserving all other history assertions.
- Verification: the pre-fix failing sequences were traced through the repair branches, without a
  counterfactual execution in this review phase. The focused run executed 131 tests: 130 passed,
  with one test-only refusal-count error (20 fixtures were incorrectly counted as 36). After
  correcting that count, all nine `FormatVersionBaselineTest` tests passed on the targeted rerun.
  Thus every selected test passed across the initial run and rerun, including both golden suites,
  `FormatRepairBoundaryTest`, history round-trip, binary-version, bootstrap and store-feature
  recovery suites. The attached startup tasks also passed. The first compilation required moving
  the repair fixture into the access package to use its package-private derived-resource builder.
  Logs: `build/review-format-verification.log`, `build/review-format-baseline-verification.log`;
  initial JUnit XML: `build/review-format-initial-results/`. Reproduce the full selection with
  `./gradlew :sirix-core:test --tests io.sirix.format.FormatVersionBaselineTest --tests io.sirix.access.FormatRepairBoundaryTest --tests io.sirix.format.HistoryPreservingExportImportTest --tests io.sirix.format.GoldenFormatTest --tests io.sirix.format.GoldenCompositePageTest --tests io.sirix.BinaryEncodingVersionStrictTest --tests io.sirix.access.InterruptedFirstCommitRecoveryTest --tests io.sirix.access.InterruptedFirstCommitRecoveryStoreFeaturesTest`.
  The outer pipeline owns mutation, work-budget, lint and broader verification phases.
- Lesson: extend the existing compatibility-before-repair lesson in [LESSONS.md](../LESSONS.md):
  version refusals need the same repair immunity as required-feature refusals, and every persistent
  file must establish its identity before interpreting records as evidence that deletion is safe.

## Deferred repair ownership: documentation scope correction

- Findings: R4 and review-1 identify the same remaining ownership gap. Ordinary opens refuse foreign
  versions before mutation on both persistent backends, but that evidence does not cover derived
  rebuilding, torn-record healing from the tail log, orphan-spill deletion, crash-leftover truncation
  or interrupted-first-commit recovery. The mapped bootstrap case can truncate a foreign
  revision-zero root when its directory record is torn.
- Root cause of the overstatement: the earlier regression matrix tested foreign settings,
  superblocks and uber pages through selected repair paths, and page envelopes through ordinary
  opens. Combining a foreign envelope with repair-triggering state exposes mutations before
  envelope decoding. Passing those separate fixture groups did not prove the combined invariant.
- Decision: retain delivered production checks and every behavior test; record the remaining
  gap in FMT-102/FMT-401 and scope factual claims to ordinary opens. The normative MUST requirements
  remain the decided target, and FMT-402's three timestamp gaps remain unchanged.
- Mandatory follow-up: task `sirix-repair-format-gate`, before any second format version exists.
  It needs side-effect-free discovery of affected records and envelopes and a resource-bound proof
  required by every repair entry point, replacing the bare opening snapshot across factories and
  reaching direct reader and writer APIs. This documentation round adds no production behavior.
- Verification: 28 focused tests passed with zero failures, errors or skips:
  `FormatVersionBaselineTest` (9), `GoldenFormatTest` (16), `GoldenCompositePageTest` (2) and
  `BinaryEncodingVersionStrictTest` (1). The run used `:sirix-core:test` with those four selections
  and excluded unrelated startup tasks; output is in `build/review-format-doc-scope.log`.
  Production sources, test bodies, golden pins and the FMT-102/FMT-401 normative paragraphs were
  unchanged in this round; FMT-402 was unchanged in full.
- Lesson: document the boundary exercised by a regression matrix. Separate version and recovery
  tests cannot establish version refusal before recovery side effects; see [LESSONS.md](../LESSONS.md).
