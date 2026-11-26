# Bug: discovery treated a live revision hash as a tombstone

- Fix / issue: review R1 on `fm/sirix-fmt-one-format`.
- Symptom: opening a resource could delete an orphan spill despite a foreign envelope in
  a checksum-valid live revision inside the bounded discovery window.
- Trigger: commit revisions 1–3, patch revision 1's envelope to foreign version 1, retain its
  positive root offset, set its directory hash to `PRUNED_REVISION_TAG`, and recompute the
  record checksum. Both persistent metadata loaders accept the record as live.
- Root cause: discovery separately recognized either prune marker as a tombstone, bypassing
  the canonical decoder's checksum and two-marker classification. A hash word can equal the
  prune tag without the offset naming a tombstone. The duplicate classification contradicted
  FMT-105 and weakened FMT-401's live-envelope discovery guarantee.
- Fix: complete directory records use the same `storedRevisionRecord` boundary as ordinary
  reads, which calls `IOStorage.decodeRevisionRecord`; missing/torn records use the existing
  non-healing tail-log decoder. Discovery skips only the resulting validated tombstones.
  Invalid identities still refuse; ordinary live records still have their envelopes checked.
- Why tests missed it: existing foreign-history fixtures used ordinary hash values, while
  tombstone tests carried both prune markers. Neither exercised a positive offset paired with
  the prune tag as a hash.
- Regression test: `FormatRepairBoundaryTest.fmt401Criterion3ChecksLiveRootsWhoseHashMatchesPruneTag`
  derives its refusal oracle from accepted criterion 3 / FMT-401 and the two-marker contract
  in FMT-105. It covers revision 1 and latest revision 3, directory and tail-log representations,
  ordinary opens and direct repair, both persistent backends, explicit orphan preservation,
  whole-resource content and unchanged data/revisions byte arrays. In the original directory
  path the either-marker branch bypasses the required envelope check; the assertion rejects
  that successful open. The final focused verification also runs this regression against an
  isolated compilation of the original reader, without reverting the fixed production source;
  both backend cases must fail there and pass with the corrected reader.
- Lesson: [canonical tombstone classification](../lessons.d/format-tombstone-discovery.md),
  collected into [LESSONS.md](../LESSONS.md).

## Commit-message rationale for the removed latest-root checks

The separate latest-root envelope checks removed by `e50f294df` remain subsumed by
`requireCompatibleRevisionRoots`: its inclusive window ends at the supplied latest revision
(or prospective truncate frontier), and every decoded live record now reaches the envelope
check, including a positive-offset record whose hash equals the prune tag. Reintroducing
those duplicate checks would not add coverage. The separate `root.requireReadable` calls
remain necessary and present so a pruned latest revision reports its typed retention refusal.
The new regression covers the latest root through both ordinary discovery and direct repair.

## Verification scope

Run `:sirix-core:test` with only `FormatRepairBoundaryTest`, `SparseRevisionHistoryTest` and
`FormatDiscoveryWorkBudgetTest` selected. Existing sparse-history tests cover valid tombstones
and corrupt records; the existing work budget keeps its unchanged bounds, cold start and
positive controls. PIT selectors, sampling, timeouts, targets and the reviewed disposition
remain unchanged; the outer executor owns mutation validation.

## Focused verification evidence

Evidence is worktree-local under `build/format-tombstone-review/`. The isolated original
reader fails the new regression on both persistent backends because the expected unsupported
version exception is not thrown. With the fixed reader both backend invocations pass, covering
all four historical/latest and directory/tail-log combinations per backend, including direct
repair and byte preservation. The 84 existing selected cases passed, including four unchanged
format-discovery work-budget cases and 14 sparse-history cases.

The initial fixed run also exposed an incomplete supplementary MEMORY_MAPPED tail-log fixture:
its old ring entry was not guaranteed to exist. The fixture now explicitly writes its revision,
reserved word, offset and timestamp as well as both checksums. No production change was
needed; only the new regression was rerun after this fixture correction. The successful
recheck and both expected original-reader failures are recorded in `regression-recheck.log`;
the initial results for the unchanged tests are preserved in `initial-fixed-results/`.

The local verification init script compiles the original reader into an isolated classpath for
the counterfactual check. It does not revert production sources or change repository build
configuration, CI or PIT settings. The fixed regression recheck reports `BUILD SUCCESSFUL`.
