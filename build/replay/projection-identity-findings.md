# Gate 6 blocker: projection identity import

Checkpoint: 2026-10-05 03:29 Europe/Berlin. Worker rule 5 requires a stop after a
repeated obstacle. No JVM or pipeline remains running. Production JsonResourceCopy
still uses its existing allocator; none of the private import work is routed there.

## Retained, tested history change

JsonReplayHistory imports exact source RECORD_TO_REVISIONS records and its own frontier
through a shared immutable record-page walk. It includes histories for deleted identities,
revert epochs, and mapped history suffixes. It does not infer history from document PUTs.
JsonReplayPageWalk factors the existing document walker without numeric-frontier scans.
The importer now requires matching storeNodeHistory settings.

The original history regression failed 24/24 because the copied history frontier was zero
instead of five. With the fix, derived-1.log passed all 24 history cases plus all 279 prior
import/epoch cases (303 pass); its other 24 failures were the new projection fixture trying
the ordinary creation API on an empty destination. This exact history-only production
change has been restored after saving the later experimental projection changes. No new
validation was run after that restoration; git diff --check passes. The full generated
matrix has not been rerun after factoring the walker/history import.

## Reproducer

The reproducer was promoted to
bundles/sirix-core/src/test/java/io/sirix/access/trx/node/json/JsonIdentityDerivedIndexTest.java;
run that test with the current memory-gated validation runner. The superseded auxiliary
copy was removed during CodeFactor cleanup. It covers all 24 version/hash/Dewey configurations,
full copy and a suffix beginning at source revision 3, every cold historical revision,
and initial/later failure injection followed by retry.

Source history: an array of two records with score/dept/validFrom/validTo fields; update
one record and reverse the two record identities; append a third record; delete a record
and rename a surviving field; revert to revision 1; no-op commit. Destination has matching
NAME/PATH/CAS/VALIDTIME definitions and a projection declared with the existing
createProjectionIndexesAtLoadStart API. The oracle derives expected query keys, numeric
projection rows in document order, and interval stab results directly from the document.

Fixture corrections made before investigating production: ordinary projection creation
rejects an empty root (use the load-start API); the load-start API requires the concrete
JsonIndexController type; NAME index id is obtained from IndexDefs.createNameIdxDef,
not assumed to be physical zero. These corrections are retained in the reproducer.

## Confirmed production mismatch and failed integration attempts

1. derived-4.log: history 24/24 passes; projection oracle fails 24/24 after initial import.
   Expected projection record order [2,7], actual [7,2]. JsonIdentityDelta PUTs are unordered,
   but the projection load listener observes their hash-map order. Its append-only lifecycle
   also rejects general non-monotonic persistent identity order; sorting by numeric key is
   not a valid fix because source document order may differ (especially suffix snapshots).

2. Initial projection rebuild after aborting an armed load (derived-5.log) fails with
   `Projection index 0 is not virgin; full reset/rebuild is forbidden, use incremental maintenance`.
   The load already initialized its physical tree. Do not weaken requireVirginTreeForInitialBuild.

3. Saved experiment in build/replay/repro/projection-epoch-attempt.patch retained logical
   initial index definitions, rolled back only the fresh bootstrap setup to abort load owners,
   rebuilt indexes over the final snapshot, and bracketed later target-state transitions with
   beforeStructuralChange(0)/afterStructuralChange(0), reseeding imported path classes.
   derived-6.log: 327 tests, 28 failures, zero errors. The 24 projection cases fail with
   `Projection index 0 cannot reach the end of its moved record interval`. A single-move
   StructuralRange represented by old first/last record endpoints cannot describe an arbitrary
   final permutation/membership transition once the document has been relinked.
   Four pre-existing primitive index snapshot tests also regress: ordinary array-value CAS
   keys {6,10} become empty when indexes are rebuilt through the complete-tree visitor instead
   of the original notifications. This is an additional builder-path follow-up to reproduce
   independently; it has not been diagnosed as a source bug yet.

The failed projection production changes were removed from the live source tree and saved
as an exact patch against 4c0f97a83 (it also contains the small history importer hook/config
hunks; those are already retained in the history checkpoint). At this checkpoint the
reproducer stayed under build/replay/repro rather than adding a knowingly failing test
to the standard suite; the promoted test now covers the subsequent production repairs.
No work bounds or oracle expectations were relaxed.

## Required next seam / resume work

A projection import lifecycle must accept explicit old/final record memberships and final
order for a whole identity epoch; it cannot replay unordered PUT notifications through an
append-only loader or label the entire permutation as one ordinary subtree move. Initial
index declarations must retain logical configuration and cleanly retire only their own
uncommitted build state. Existing committed history and initial/later rollback/retry must
remain sound. Primitive notifications that already pass should be preserved while the CAS
full-builder discrepancy is investigated. An initial completed-tree projection build is
valid only on a genuinely virgin tree; later imports need bounded incremental maintenance.

Gate 6 remains incomplete. Gate 7 must still remove whole-document graph/path scans and
add the requested record/identity/staging/ancestor/sidecar/page budgets. Gate 8 full core/query,
all exact verification budgets, formatting/final acceptance, alternating paired bootstrap
latency, prerequisite tombstone rebase and production routing remain pending. No no-mistakes
run, push, PR, or done handoff is authorized by this incomplete checkpoint.
