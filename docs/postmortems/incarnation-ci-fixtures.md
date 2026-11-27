# Bug: FMT-209 fixtures and crash recording disagreed with their contracts

- Fix / issue: PR #1507, CI run `45880112993`; the four non-index lanes and PIT baseline.
- Symptom: frozen page-history reads failed on missing incarnation metadata; direct repair
  accepted a supposedly unknown feature; the truncate recording saw no removal on macOS and
  could not copy a live lock on Windows. ARM also reported 1,048 bytes against the envelope
  allocation test's 1,024-byte bound.
- Trigger: run the existing non-index suite on Linux, macOS, ARM or Windows, or collect PIT
  baseline coverage for `FormatRepairBoundaryTest`.
- Root cause:
  - The frozen pre-freeze archive predates FMT-209's mandatory settings field. Its page images
    remain a valid independent reader oracle, but its settings are not the current schema.
  - The repair fixture used readOnlyCompat bit 0 after FMT-209 assigned and implemented that
    bit. The writer's acceptance was correct; the unknown-bit test input was wrong.
    The isolated tool-reader fixture retained the same stale literal after the other refusal
    fixtures were corrected. Targeted Test validation reproduced its failure on both persistent
    backends. It now uses the shared unassigned-bit fixture constant; its exception and whole-file
    preservation assertions are unchanged. The oracle is FMT-204/209's feature registry, not the
    engine's implementation of known bits.
  - The new recording test registered a lexical temporary path while database open resolves
    symlinks. macOS's temporary-directory alias therefore bypassed the recording provider.
    Both older recording tests already resolve their temporary roots with `toRealPath()`.
  - Both live snapshots copied the process-held database `.lock`, which Windows prevents
    reading. A crash snapshot needs persisted database content, not the process lock.
  - The allocation harness warmed a different loop from the measured loop, did not initialize
    its allocation probe before warmup, and ran with asynchronous compilation beside unrelated
    tests. The generic decoder exercised by this check has no executable change in this PR.
    A 1,048-byte total for 100,000 decodes suggests fixed measurement overhead rather than a
    per-page allocation; the original interval did not isolate that distinction.
  - PIT's name-based selection omitted the existing incarnation and last-revert conformance
    suites. After baseline repair, the first completed extension unit reported 14 surviving
    and four uncovered mutants. The lane now associates those existing requirement-derived
    suites with all eight lifecycle and page-format producers/consumers through its existing
    association file; no new tests or broader mutation scope are introduced for this omission.
  - The associated tests still accepted arbitrary runtime failures for malformed settings,
    omitted temporary-file force failures, and did not assert all extension stream boundaries
    or unsigned flag diagnostics. Those checks now assert the existing interface contracts.
    A validation-only helper also returned an unused identity, and restore redundantly tested
    directory type before checking for a settings file. Both redundant pieces are removed.
  - The final broad run also omitted the existing read-only healing/orphan-spill suite from
    reader and database selections. Both boundaries now select that suite. The recreation
    fixture built a new configuration on every creation, so it could not distinguish a draw
    at resource creation from one at configuration construction; it now reuses the original
    configuration across all three creations for both JSON and XML.
- Invariants and sibling audit: every fixture opened as a current resource has required settings;
  unknown-bit refusals use unassigned bits in both beacons and on both persistent backends;
  recording roots match canonical resource paths in all three recording tests; both live
  snapshots omit only the root process lock; all three envelope inputs retain the original
  operation count, checksum oracle and allocation bound.
- Regression tests and independent oracles:
  - `LegacyPageReadersFixtureTest` checks the frozen three revisions, appends a fourth, compares
    old page bytes, and reopens both original and new data. Only extracted settings receive a
    deterministic test identity; neither the archive nor production reader gets a fallback.
  - `FormatRepairBoundaryTest.directRepairRequiresWritableFeaturesAndCompatibleHistoricalRoots`
    uses FMT-204/209's unassigned readOnlyCompat bit 1. It still rejects incompatible and foreign
    historical roots, allows ordinary read-only discovery, and proves refusal changes no files.
  - `IncarnationIdTruncateCrashOrderTest` still requires a recorded removal, a new identity
    before that removal, and cold-openable copies at the original or target head. It additionally
    asserts that neither snapshot contains the live lock before its verification open.
  - `PageEnvelopeAllocationTest` measures the same warmed workload method in the existing
    `dictionaryReadViewAllocationTest` fork with `-Xbatch`. All three independently encoded wire
    cases keep their checksums and the 1,024-byte bound; allocating per decode would fail it.
  - `LastRevertRevisionEntryTest` uses independently specified block bytes to check complete
    block consumption and refusal without touching following body bytes on both array and
    segment inputs. It also checks the actual unsigned invalid flag and rejects nonpositive
    writer input. Consuming the body as a length/payload or reporting another byte would fail.
  - `IncarnationIdConformanceTest` requires field-specific `SirixIOException` refusals through
    deserialize, open, rewrite and both publishers, with unchanged settings afterward. Restore
    also includes a copied stray regular file, which must not be interpreted as a resource.
  - `ResourceConfigurationTest` injects a temporary-file force failure at the external channel
    boundary while real writes and close still reach a real file. Both publication APIs must
    propagate that failure and preserve the real persisted and live identities before rename.
- Path retrace before verification: fixture extraction precedes the settings overlay and both
  database opens; feature patching changes both slots and leaves the incompat case's read-only
  word zero; the canonical recording path is used for creation, observation and both snapshot
  destinations; the snapshot helper still copies settings, page files, indexes and markers;
  the isolated allocation task remains a prerequisite of ordinary core tests and their coverage
  report, with the envelope class excluded only from the shared fork. The mutation associations
  augment existing selections for configuration, creation, truncate, restore, transaction revert,
  revision-root cloning, extension framing and page dispatch; ordinary test execution is unchanged.
  The JSON-member validator still validates through the same string parser but no longer returns
  an unused value. Every reader still obtains its unsigned bits from the string parser. Restore
  checks settings-file presence for every entry: regular files cannot contain that child path;
  eligible resource directories retain publication followed by the existing verification open.
  No alias or unused predicate remains after these removals.
- Verification: the focused pre-fix run reproduced three failures in 71 cases (the archive and
  both repair backends). After the correction, all 141 focused cases and 20 isolated allocation
  cases passed. The archive's SHA-256 remains the value recorded in its provenance file.
  Expanded verification exercised 539 core cases, including 369 passing work-budget cases.
  Its only two failures came from the new stray-file fixture initially assuming that a live
  backup copied such files; backup intentionally omits them. Moving the file to restore's
  documented cold-copy input fixed the fixture, and all 44 incarnation cases then passed.
  The remaining expanded cases passed, including the new framing and temporary-force cases.
  Sequential query/MCP verification passed 322 query cases (five existing skips), five isolated
  loader-allocation cases and three MCP budget cases. Core, query and MCP compiled with Error
  Prone enabled. An earlier overlapping core rebuild invalidated MCP's compilation classpath;
  the sequential verification completed successfully without MCP source changes.
  The truncate recording also passed with `TMPDIR` set to a symlink inside the worktree,
  exercising macOS's path-alias condition on Linux. Native macOS, Windows and ARM verification
  remains with the outer executor's CI lanes.
  The complete changed-line PIT run tested 105 mutants: 102 killed and three survivors. Adding
  the existing read-only suite to the reader/database associations and reusing the creation
  configuration resolved those final gaps. All 83 focused lifecycle cases passed; the focused
  PIT rerun killed all six mutants in its two classes, including exact identity matches for all
  three broad-run survivors. No mutation dispositions, scope limits or budget changes were added.
  A subsequent fresh PIT run over all 61 changed target classes, using the six requirement
  contract suites and the same changed-line map with no per-class limit, killed all 105 of 105
  mutants in 2m 45s. Its report is `build/ci-diagnosis/mutation-contracts/report/mutations.xml`.
  The existing 44 disposition entries validate without record errors after compiling the local
  CLI inventory; none was edited. The native platform CI rerun remains with the outer executor.
  Evidence and commands are retained under `build/ci-diagnosis/`, including the complete broad
  report, the focused lifecycle report and their separate logs.
  The later targeted Test-phase run reproduced two failures in
  `InterruptedFirstCommitRecoveryStoreFeaturesTest.isolatedToolRefusesUnknownFeaturesWithoutChangingBytes`
  before correcting its remaining bit-0 literal. Both persistent-backend cases and the complete
  selected core conformance/work-budget run passed afterward; no production behavior or assertion
  was changed by that fixture correction.
- Lesson: fixtures must continue to express the contract they claim to exercise as the format
  grows; crash observers must use the engine's path identity and omit process-only locks.
  Measure allocation after warming both probe and workload in the existing isolated fork.
  See [LESSONS.md](../LESSONS.md) and the fragment for this fix.
- Vision check: retains strict single-format refusal, frozen page-history evidence and unchanged
  work/allocation bounds. The two production simplifications preserve the same accepted inputs,
  settings and identities while removing unused work. No compatibility path or default changes.
