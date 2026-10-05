# Identity delta replay implementation and verification plan

Started 2026-10-04 on fm/sirix-replay-identity-delta from 71be74062.
Design authority: /home/johannes/IdeaProjects/firstmate/data/sirix-diff-replay-design-review/report.md,
recommendation B and its ordered migration/acceptance plan. The report remains read-only.

## Current checkpoint (2026-10-05 22:55 Berlin)

Isolation and assigned branch verified on resume. Inbox 005 sets the stop at
2026-10-06 04:00 Berlin; no validation round may extend past 03:40. The private
Maven repository build/replay/m2 and heavy runner remain in use.

Gates 1–7 are complete. Projection epoch import is committed in bac72b467;
incremental path-record/cache maintenance in 76d48c70d; bounded graph validation
and mutation-proven work budgets in bc85f8315. Gate-7 final validation passed
604/604 cases, including all 144 generated configurations, forged invalid graphs,
index/path oracles and every existing core/projection budget. Formatting passes.
The projection-identity-import key is resolved.

Independent source repairs include f6688ece7 (CAS cursor restoration) and
56052a8cd (pipelined JSON sidecar preparation/publication). The latter passes
242/242 final cases, including all generated histories and work budgets; the old
async cache errors no longer occur. Details and failed/passing logs appear below.

Commit fa459887e routes complete public revision copies only through typed
identity deltas and removes the old private key-sorted allocator. Snapshot subtree
copying and public diff behavior stay intact. Initial public-copy selection passes
160/160 cases, including all 16 R16 modes; the FULL/DIFFERENTIAL/INCREMENTAL R1–R18
matrix and 17 new suffix/preflight cases now pass (177 tests per remaining mode).
SLIDING_SNAPSHOT passed 160 tests in the initial route selection.

Gate 8 remains: rebase the separately owned tombstone fix when it lands, complete
full core/query suites (query fork 2 GiB), the exact work-budget block,
formatting and pinned paired latency acceptance. Inbox 006 confirms the prerequisite landed: origin/main is now af9f20e5a. Rebase at
the next clean point after the current validation process finishes. The full core run
passes 13,870 tests with zero failures/errors and 77 skips (1,119 suites archived).
The query suite is still running and has one outdated hash-history expectation:
revisions 4 and 5 now correctly change the selected object's hash when its left and
right sibling links change. The existing computeHash contract explicitly includes
those links; actual output preserves the same JSON and adds exactly these revisions.
Formatting remains part of this running job. No benchmark campaign, no-mistakes run,
push or done handoff yet.

## Contract and concrete implementation

- Keep BasicJsonDiff, its public JSON format, compact fragments and HASHED early stop independent.
- Introduce io.sirix.service.json.replay.JsonReplayManifest (protocol version 1),
  JsonReplayRecord (logical payload and final topology), JsonIdentityDelta (PUT/DELETE),
  and JsonIdentityDeltaReader (authoritative document-page comparison).
- Manifest v1 is an internal typed value: protocol version, exact source resource identity,
  base and target revision, mapped destination revision, base/target allocation frontier,
  source configuration contract (Dewey/hash), complete changed-key set and final records.
  It is NOT the public diff JSON. Initially no replay sidecar is emitted or trusted:
  missing, corrupt, legacy, cumulative and post-revert presentation sidecars all take
  the same authoritative committed-storage path. A durable cache is optional later.
- Add a private transaction import implementation in access.trx.node.json, reached by
  an explicit import entry point on InternalJsonNodeTrx. Stage detached logical records in the
  transaction intent log without calling public insert/move methods. Record identity
  creation and final link installation are separate phases. Normal bulk factory paths
  remain allocation-only; explicit replacement/existence checks belong to import.
- Finalize names, path summaries, indexes, hashes and stored Dewey IDs explicitly.
  Never copy foreign resource-local dictionary/path/index identifiers blindly. Preserve
  source node revision metadata for full-history copy; define an explicit offset for
  supported history suffixes. Reject unsupported contracts before modifying destination.
- Import is one locked compound epoch: threshold/scheduled commits cannot publish staged
  records. Validate target closure, compatible parents, ordered reciprocal links,
  acyclicity, counts and allocation bounds before publication. Failure rolls back the
  current epoch; prior successfully copied revisions remain the advertised history.
- JsonResourceCopy chooses identity replay only after shadow/oracle validation passes.
  Existing snapshot-only subtree copying retains its normal allocation semantics.
- Authoritative delta discovery walks populated indirect document-page references,
  skips only equal durable immutable references plus fragment chains, resolves complete
  logical records through StorageEngineReader for every versioning type, and compares
  exact identity-sensitive records. No numeric frontier scan; no content-hash identity
  shortcut. Boundary/count/hash changes are ordinary changed records.

## Ordered migration gates (keep current)

1. [done: contract and regressions pinned] Pin public behavior and promote R16 across Dewey/recompute/versioning;
   define typed protocol without routing production replay to it.
2. [done: initial seam/oracle] Independent full-snapshot test reconstruction and private import seam;
   explicit keys/gaps/frontier, stage-failure rollback tests. Rebase onto the separate
   tombstone/recreate/restore fix when firstmate reports it landed; do not duplicate it.
3. [done: initial shadow validation] Authoritative delta discovery; shadow hook compares delta-applied graph to
   separately enumerated full target snapshot before enabling production routing.
4. [done: explicit epochs and failure validation] Commit/revert/rollback/async epoch coverage; exact manifest base validation.
5. [done: fixed seeds, shrinking and sidecar independence] Fixed-seed shrinkable operation streams across all four versioning types,
   hash NONE/ROLLING/POSTORDER, Dewey on/off, auto-commit and KEEP_OPEN/async modes;
   sidecar present/missing/corrupt/diffs-disabled, cold reopened history.
6. [done: complete derived-index identity epochs and independent queried oracles] Oracle compares keys, kind, name/scalar, parent and ordered child/sibling
   links, counts, frontier, revision metadata, hashes, stored Dewey IDs, queried indexes
   and path summaries. Include equal-value swaps, later parents, deleted-key restore,
   sparse reservations, replacement survivors and empty/no-op revisions.
7. [done: bounded transition/path work and mutation-proven counters] Work budgets: record visits, created identities, staged records, ancestor
   work, sidecar reads, fallback page visits. Guard append unchanged prefixes, no-op
   page reuse and sparse gaps; preserve every existing bound.
8. [pending] Full core/query suites, exact docs/VERIFICATION.md work-budget block,
   formatting and pinned alternating paired latency campaign; then commit/handoff.
   Firstmate starts no-mistakes after the first done handoff; pipeline owns push.

## Verification environment and commands

Every Gradle/Maven JVM and benchmark runs via build/replay/run.sh and the required
memory gate (MemAvailable >= 6 GiB, two /var/tmp/fm-heavy-jvm.* flock slots).
Fresh empty private Maven repository: build/replay/m2 (created before first build).
Private Gradle home: build/replay/gradle-home; shared dependency cache read-only:
/home/johannes/.gradle/caches. Scratch: build/replay/tmp. Never alter ~/.m2.
Full query tests use -PtestHeapMax=2g. Preserve command logs and test XML summaries.
No Gradle stop, foreign process kills, worktree administration or power changes.

Run the report workload (4096 initial primitives, append 4096, no-op revision), with
FILE_CHANNEL/ROLLING/Dewey off plus extended sparse/deep/move/restore scenarios.
Measure source append+commit, public compact/materialized diff, authoritative replay
read and end-to-end copy separately. taskset -c 0-11; matched alternating forks,
identical inputs/JVM/index configuration, warmups, raw samples and work/allocation
counters. 95% paired bootstrap, 5000 draws; confirmed regression lower bound >1.05.
Compare to current replay 89-111 ms workload medians; no wall-clock test assertions.
From Monday 2026-10-05 no new campaign after 05:00 Berlin; benchmark JVMs done by 06:00.

## Evidence

- Isolation: pwd -P and git top-level both /home/johannes/.treehouse/sirix-cdde48/20/sirix.
- Assigned branch created; no-mistakes doctor passed (daemon running, codex runnable).
- At setup there were no production edits or validation runs. Later evidence is recorded below.

## Captain steering, acknowledged 2026-10-05 00:02 Berlin

Tonight's stop is now 04:00 Berlin (02:00 UTC), superseding the benchmark-only 06:00
stop. No build/test/benchmark/pipeline round may be started unless it can finish before
03:40 Berlin. Between 03:40 and 03:55 preserve unfinished work in a local WIP commit,
stop own jobs, report paused with resume notes, and remain idle pending firstmate.
Current baseline has not acquired a limiter slot; no new JVM has started in this lane.

## Stop checkpoint (2026-10-05 00:03 Berlin)

The lane's last declared state was paused waiting for the JVM limiter when inbox 001
arrived. Its explicit instruction to already-paused lanes is to stay paused and start
nothing new. Cancelled this lane's queued baseline before any JVM was launched and
preserved a local WIP checkpoint; no validation results or production-readiness claim.

Implemented but uncompiled: R16 regression under VersioningType enum with internal
Dewey/recompute loops; JsonReplayRecord, JsonReplayManifest, JsonIdentityDelta; detached
payload construction and separate link installation in JsonReplayNodeFactory.
Existing JsonResourceCopy and public diff are unchanged. No tombstone code duplicated.

Resume at migration gate 1/2. The narrower existing InternalJsonNodeTrx is the chosen
import seam (prefer it over expanding public JsonNodeTrx). Build the independent
snapshot oracle and atomic import epoch before authoritative page-delta routing.
Unresolved implementation detail: names participate in node hashes; reconstruct a
validated logical name namespace (including collision assignments) and path-summary
namespace/metadata, rather than blindly copying foreign local identifiers. Index
DELETE/INSERT notifications must bracket final topology publication, with path stats
and projection/valid-time integration verified. JsonReplayNodeFactory is only a draft
helper, not a wired importer. No import epoch, failure hook, page walker, derived-state
maintenance, oracle, work budgets or benchmark acceptance has been implemented yet.

No no-mistakes run started; doctor only. No push. Baseline log is empty (limiter wait).
After firstmate resumes: inspect inbox, rebase the prerequisite tombstone fix when it
lands, continue implementation, then run the required acceptance sequence. Do not
represent this WIP commit as task completion or hand it off as the first done gate.

## Resume clarification (inbox 002, 2026-10-05 00:05 Berlin)

Firstmate explicitly resumed normal work until the 03:40 validation cutoff. Continue
on top of WIP 24128d1af; perform save-and-park between 03:40 and 03:55 Berlin.

Baseline evidence: build/replay/baseline.log and baseline-results/*.xml. Four versioning
invocations of R16 fail with the expected temporary allocation parent -1 exception;
each invocation stops at its first Dewey/recompute case, so this run alone does not
claim all sixteen combinations. Five BasicJsonDiffWorkBudgetTest invocations pass.

## Gate 2 implementation and first oracle run

Private InternalJsonNodeTrx.importRevision owns a clean locked epoch, suppresses public
mutation sidecars, stages detached records, links them, validates the graph, rebuilds
logical name/path namespaces, notifies indexes and commits. Failure checkpoints exist
at identities-staged, links-installed, derived-state-finalized and before-publish.
The complete path namespace is currently rebuilt; production replay is NOT routed here.
Changed-path and bounded graph validation remain later work once the oracle is sound.

First focused run: 29 invocations, 13 pass and 16 fail. Eight HashType.NONE R16 snapshots
across Dewey/versioning combinations pass after cold reopen, including frontier 1,000,000;
all four staging rollback/retry tests (with colliding names Aa/BB) and dirty-writer refusal
pass. ROLLING/POSTORDER snapshots fail the independent descendant-count invariant.
A diagnostic shadow comparison of ALL copied fields against the source now runs before
that invariant check to distinguish importer drift from pre-existing source corruption.
Suspect source path: insertSubtreeInternal repairs only the selected last root after a
skipped-root bulk append; AbstractNodeHashing.postorderAdd does not maintain descendant
counts. Do not weaken the validator or copy acceptance to make this green.

Source performance paths remain unchanged except new cold import methods. Full suites,
index integration, allocation budgets, delta discovery and benchmarking have not run.

History-suffix mapping is explicit: source revision S maps to destination 1; later
revisions retain the same offset. Predecessors before S become unavailable (-1), and
last modification before S maps to snapshot boundary 1. Document-root sentinel metadata
remains its engine-defined zero. Full history from source revision 1 preserves metadata.

Manifest source identity is ResourceConfiguration.resourceUuid, not merely the resource
path or reusable numeric resource id. Import validates both the UUID and exact epoch.
No legacy format fallback is promised (captain explicitly waived old database formats).

The queued second diagnostic adds all fused/ordinary payload kinds, an overflow Unicode
string, and a name-hash collision whose earlier binding has been deleted. The independent
oracle now checks document name/path keys and path-summary topology, allocation frontier,
references, all statistics values/trust flags, HLL and persisted page-presence state.
R1-R18 test configurations can be selected with -Dsirix.replay.versioning=<VersioningType>;
the default stays SLIDING_SNAPSHOT, and copied resources use the source versioning type.
This matrix support has not yet been run across all versions.

R16 now has sixteen separately reported versioning/Dewey/recompute invocations. The
report's small-edit R18 work budget is promoted with its original bounds, extended to
both Dewey modes. Primitive index snapshot coverage independently checks expected node
keys after cold reopen for NAME, unrestricted/selective PATH, and unrestricted/selective
CAS indexes. Source and staged document fields are compared before separately validating
the immutable source's graph invariants in the second diagnostic.

## Gate-2 stop checkpoint (2026-10-05 01:07 Berlin)

Second diagnostic finished: compilation passed; 37 tests ran, 21 passed and 16 failed.
All R16 staged/source field comparisons pass, then immutable SOURCE validation fails:
ROLLING array key 1 stores 2 descendants instead of 3; POSTORDER object key 5 stores
0 instead of 1. Both failures cover all four versioning types and both Dewey modes.
This repeats the first run's obstacle, so the worker brief requires a blocker handoff
and stop. Full evidence and source-repair leads: build/replay/source-count-findings.md.

The 21 passing invocations include all added payload/name-collision and primitive-index
checks, all four fault rollback/retry checkpoints, dirty-writer refusal and eight NONE
R16 cases. The newly expanded public R16 matrix and promoted R18 test compiled but have
not run in their new form. Existing baseline public-diff budgets passed five invocations.
Formatting, full suites and performance acceptance are pending; no production switch,
no tombstone duplication, no push, no no-mistakes pipeline. The diagnostic job finished.
Preserve a local WIP commit and await firstmate resolution before continuing gate 2.

## Source repair authorized (inbox 003, 2026-10-05 01:11 Berlin)

Firstmate resolved source-descendant-counts and directed an independently reviewable
source repair as the first commit before replay. The new writer-local JsonHashingMutation
captures changed boundaries/ancestors before link surgery, repairs every new forest root,
and propagates final hash/count contributions after insertion, move or removal. ROLLING
does not walk the unchanged child prefix. Auto-commit revisions use incremental maintenance
because a final repair cannot fix already-published intermediate revisions.

Source-fix run 1: compilation passed, unchanged snapshot oracle 37/37 and public-diff
budgets 7/7 pass. Direct source tests validate canonical hashes (including final local
links/counts) and structural counts across all versions/hash/Dewey modes, live and cold
reopened, including intermediate auto-commits. Failures exposed an invalid NONE test
assumption (leaf getHash can compute a local value lazily) and a pre-existing primitive-first
skipped-root left-sibling ordering defect. Correct the assumption and normalize the initial
sibling position after the first primitive in all three shredders; retain exact expected
JSON ordering. Revalidate before committing/reordering the isolated source fix.

## Source follow-up results (2026-10-05 02:19 Berlin)

Full core: 13,161 tests, five failures, no errors, 77 skipped. The failures were three
unneeded boundary reads with NONE/diffs disabled and two old reversed left-sibling order
expectations. Those are corrected without changing work bounds. Additional direct
regressions produced 32 failures of 48 before fixes: eight ROLLING shared-container
renames lost their subtree hash; all 24 malformed-forest cases could publish partial
state. The repaired focused suite passes 267/267. The independent source repair is
committed before both replay WIPs; raw logs/XML are retained under build/replay.

## Gate 2 restored and verified (2026-10-05 02:23 Berlin)

The unchanged original snapshot oracle and 24 new full-snapshot reference transitions
pass together: 61/61 invocations (import-restore.log, import-restore-results/). The new
transitions import four source epochs: initial state, deletion, restoration after revert,
and subsequent allocation. They compare every cold-reopened revision and path summary
across all four versioning types, all hash modes and both Dewey settings, under a target
threshold of one with KEEP_OPEN_ASYNC_FLUSH. The private persistRecord staging seam
safely replaces a deleted identity; it does not use the old allocator's defective fresh
native creation route. That separate worker's fix is still not duplicated and must be
rebased when it lands.

Proceed to gate 3: pair authoritative document tries, compare complete reconstructed
logical slots, and validate the resulting delta against the independently enumerated
full-snapshot reference. Public JsonResourceCopy stays on its old path until the later
acceptance gates pass. No production routing switch is included in this checkpoint.

## Gate 3 implementation (2026-10-05 02:24 Berlin)

JsonDocumentDeltaWalk pairs immutable indirect tries, aligns different trie heights
through virtual zero-offset ancestors, and skips only equal durable resource/offset and
fragment-chain references. Changed leaves resolve through getRecordPage and guarded
logical-slot bitmaps, including overflow slots. JsonIdentityDeltaReader.between validates
consecutive committed same-resource epochs and restores both cursor positions. It never
reads presentation sidecars or scans the numeric allocation frontier.

The shadow regression compares the exact PUT/DELETE set against JsonReplaySnapshotOracle,
a separate full-tree enumeration, then compares every imported revision and path summary
against the original source after cold reopen. Cases include R16, no-op and frontier-only
commits, a trillion-key gap with trie growth, value/name updates and sparse deletion.
The draft is now in production sources but remains uncalled by JsonResourceCopy. Compile
and shadow validation are next; later acceptance gates remain pending.

## Sparse-source follow-up (2026-10-05 02:29 Berlin)

First shadow run compiled; 68/92 tests passed. All 24 new cases failed while building the
SOURCE fixture, before delta discovery: a trillion-key jump aliased an existing record,
and renaming later found NUMBER_VALUE at the array's key 1. KeyedTrieWriter only grew
when pageKey equaled the next capacity boundary, assuming sequential allocation. It now
grows repeatedly until the key fits and rejects invalid offsets before narrowing them.
SparseDocumentIdentityTest directly checks jumps across multiple levels (through 2^52),
ordered links/values/counts/frontier and cold history on all 24 configurations. This is
a separate follow-up to explicit-key import, unrelated to the other lane's tombstone fix.
The fix and shadow run need validation; retain the first failure in delta-shadow-1-results/.

## Gate 3 first green (2026-10-05 02:34 Berlin)

The sparse growth fix is a separate commit 0067f38b3. Its rerun passes 126/126 invocations:
24 direct sparse-source cases, ten keyed-trie integrations, 85 import/shadow cases and
seven public diff budgets. The authoritative PUT/DELETE sets match independently
enumerated complete snapshots for all 24 configurations. Add direct authoritative
restoration comparison and real overflow-reference replacement/deletion before closing
this gate. Existing manual snapshot-field and path-summary assertions remain intact.

## Gate 3 expanded shadow verification (2026-10-05 02:41 Berlin)

116/116 pass in delta-shadow-3.log and delta-shadow-3-results/: 109 import/oracle cases
plus seven public-diff budgets. Deleted-key restoration now compares authoritative and
reference deltas exactly. Oversized strings are verified to have real overflow references;
all 24 configurations copy inline → overflow → inline → overflow → deletion → no-op,
then compare every cold-reopened graph and path summary. Formatting applied successfully.
Production copy is still unchanged; epoch acceptance is next.

## Gate 4 validation (2026-10-05 02:51 Berlin)

Manifest construction now rejects impossible suffix offsets, non-adjacent transition
epochs and nonzero initial frontiers. Import preflight already enforces the exact
source UUID/path/revision/configuration and current destination base/frontier.

`JsonIdentityEpochTest` adds 170 cases: all 24 version/hash/Dewey configurations under
KEEP_OPEN, KEEP_OPEN_ASYNC_FLUSH and KEEP_OPEN_ASYNC_COMMIT; four injected failures
after an earlier successful import, rollback/retry and cold committed history; actual
bulk intermediate revisions and writer replacement, source rollback/no-op/revert;
malformed manifests, duplicate/wrong-source epochs, dirty/uncommitted input, and
intervening destination commits/reverts. A deterministic competing commit uses the
scheduler's transaction lock and cannot publish staged records.

`epoch-1.log`: 207/207 pass. Expanded `epoch-2.log`: 279/279 pass in 43 seconds,
including all 109 existing import/oracle cases. XML retained in matching result
directories. Formatting applied through the memory-gated runner. Production copy
is unchanged. No benchmark campaign, full query run or final acceptance claim.

## Generated oracle source follow-ups (2026-10-05 03:00 Berlin)

The first two generated configurations shrank two source-only failures before replay:
(1) reserve a multi-billion identity range then move/delete a newly created sparse
object: `acquireGuardForNode` consulted only the predecessor reader, which cannot
resolve the new writer-only page; (2) insertion then moving a field to a later parent
at a count threshold: the move retained a flyweight across writer replacement.

The guard now resolves the authoritative writer page/intent log first and guards that
frame; durable pages use their exact reference with scoped lifetime. Both real move
entry points reacquire the moved record only when the commit check replaced the writer.
Neither correction touches native tombstone replacement or the legacy replay allocator.

`JsonStructuralEpochRegressionTest` exercises the minimized source-only cases across
24 version/hash/Dewey configurations, three commit modes, zero/positive thresholds and
first/left/right move positions (432 scenarios), validating canonical graphs live and
in every cold historical revision. `generated-1.log` is the initial red shrink evidence;
`generated-2.log` passes 26/26 reported invocations: the 24 source matrix invocations and
two generated configurations (both fixed seeds, sidecars present/missing/corrupt and
diffs disabled). Formatting applied. Full generated matrix remains the next validation.

Expected pipelined public-sidecar serialization cache misses were logged before durable
publication; authoritative replay does not read them. No production routing switch.

## Gate 5 fixed-seed oracle (2026-10-05 03:05 Berlin)

`JsonIdentityGraphGeneratedTest` passes all 144 configurations: four versioning types,
three hashes, both Dewey modes, three open commit modes and zero/positive thresholds.
Each runs two retained fixed seeds, 28 state-relative operations, every committed
intermediate revision, and three destination histories for present/missing/corrupt
public sidecars; the second seed disables source diff generation. Cold independent
oracles compare complete identities, payload, ordered topology, counts, canonical
hashes, frontier, Dewey bytes/order, revision metadata and complete path summaries.
The bounded shrinker produced two-operation reproducers for the source fixes above.

Streams include every insertion position, fused/ordinary nodes, equal-value identity
swaps, descendant changes, moves, replacement, ancestor deletion, gaps/reservations,
empty/no-op revisions, rollback and deleted-identity restoration. Resumed branches
reserve disjoint high-water ranges: native reuse of a tombstoned slot remains the
parallel prerequisite's regression, to incorporate on rebase.

`generated-3.log` completes in 3m33s with the entire generated matrix and all selected
core `*WorkBudgetTest` classes green, without widened bounds. XML retained in
`generated-3-results/`. Gate 6 now adds queried primitive/projection/valid-time indexes
and exact node-history records. The importer currently omits node-history index
maintenance; do not route production copy before correcting and validating it.

## Gate 6 history checkpoint and projection blocker (2026-10-05 03:29 Berlin)

Added authoritative RECORD_TO_REVISIONS import, including deleted identities and suffix
boundaries. The document/page walker is shared with history discovery. Full history
regression: 24/24 failed before; 24/24 pass after, with all 279 prior import/epoch cases
still passing (derived-1.log). Configuration validation now includes storeNodeHistory.

The independently queried projection oracle found unordered initial rows and the existing
listener's inability to express a batch permutation as one moved interval. Bootstrap
rebuild experiments also exposed an ordinary-value CAS builder discrepancy. Details,
all intermediate validation results and the exact saved experiment are recorded in
projection-identity-findings.md. The production experiment has been backed out while
the tested history importer remains. Reproducer is retained outside the normal suite;
no production copy switch and no claim of final acceptance. Stop and await firstmate.

## CAS complete-tree source regression (2026-10-05 18:11 Berlin)

Reproduced on plain origin/main 71be74062 in this same worktree: all four versioning
cases miss ordinary array string keys {6,10}. Evidence: cas-main-1.log and XML under
cas-main-1-results/. Returned to fm/sirix-replay-identity-delta before production edits.
The CAS visitor moved the shared traversal cursor to the primitive parent without
restoring it; subsequent builders visited the parent and silently omitted the value.
Restore the cursor before processing each value. Validate multiple CAS definitions,
primitive types, nested/root arrays and other shared visitors, then commit separately.

Inbox 005 acknowledged: captain's stop is 2026-10-06 04:00 Berlin; start no validation
that cannot finish before 03:40, preserve work and retire own jobs by 03:55.

CAS fix validation: 129/129 invocations pass (cas-fixed-1.log and cas-fixed-1-results/),
including eight new versioned regressions, existing CAS suites and all 109 import cases.
Spotless Java apply passes; no work bound changed. The source fix is committed independently.

## Projection epoch seam implementation (2026-10-05 18:17 Berlin)

JsonIndexController.completeInitialIdentityImport builds only after the final graph/path
namespace exists. A preflight-proven fresh document epoch retires its uncommitted index
declarations by rollback; definitions are captured locally, and failure restores both
primitive declarations and owned projection load-start declarations in the new empty
writer. No committed tree is reset and requireVirginTreeForInitialBuild is unchanged.

ProjectionIndexChangeListener.beginIdentityImport captures explicit old affected record
memberships and structural keys whose parent/sibling/path changed. Completion collects
final memberships, removes old memberships in batches of at most 256, invalidates only
changed local labels, mints final labels and installs the rows in document order through
the existing incremental row-group editor. Ordinary PUT notifications do not enter
the append-only loader. Relocated containing subtrees discover their projected roots;
unchanged prefixes are not enumerated merely because an ancestor count/hash changed.

First validation queued through heavy(): projection-epoch-1.log (derived-index, import,
history and epoch oracles). Must pass before closing projection-identity-import.
Additional multi-batch and changed-container coverage is planned before gate 7.

Projection epoch run 1: 315/327 pass. All 12 failures are copied valid-time intervals
with Dewey IDs enabled; the source's independently queried indexes pass. Every
projection assertion reached passes. These failures reveal the valid-time listener's
ordinary contiguous-event assumption when replay PUT/DELETE keys arrive unordered.
Evidence retained in projection-epoch-1.log and projection-epoch-1-results/.

JsonValidTimeIndexListener now captures complete old intervals for changed old/final
objects before staging, suppresses primitive events during the epoch, and reconciles
exact final intervals once per affected object. The second run also includes a
360-row two-container permutation, deletion and restoration across maintenance batch
boundaries, full and suffix snapshots, all 24 configurations. It is queued under
projection-epoch-2.log; the prior 303 import/history/epoch cases remain in the selection.

Extended JsonIdentityGraphGeneratedTest with JsonIdentityIndexOracle: every generated
copy declares NAME/PATH/CAS/projection indexes; each cold revision compares all queried
memberships, value postings, ordered projected identities, field presence, scalar values
and representation proofs against independent source document walks. The streams and
seeds are unchanged. This added oracle is not in run 2 and still requires validation.

While run 2 remains queued for the shared limiter, strengthened its nested-container
fixture to include removal and re-entry of a whole record set by renaming its array
field, including the last instance of a path class (seven source revisions). Identity
epoch capture treats logical name changes as changed subtree membership even when a
path class is renamed in place and keeps its numeric PCR. The fixture also declares
a sorted covering view and independently checks its persisted keys/values after cold
reopen through ProjectionIdentityEpochOracle. No virgin-tree guard is changed.

Filtered PATH/CAS membership also depends on path names, even for descendants whose
document record stays identical. The importer now brackets additional unchanged
descendant index entries under renamed/reparented nodes, with a visited set to skip
overlapping subtrees. This runs only when a filtered PATH/CAS definition exists; it
does not stage those records or alter their name counts. The nested-container test
also queries all postings of filtered indexes, so stale entries cannot hide behind
a query filter after the selected array is renamed away. Still awaiting run 2.


## Gate 7 review notes while the gate 6 validation queues

The paired document/history walkers already skip identical durable regions. Per-epoch
full scans still exist in JsonReplayGraphValidator.validate, JsonReplayPaths.rebuild,
and PathSummaryReader.reloadAfterImport. Count cache reconstruction as real replay
work; merely changing path-record persistence would leave a hidden whole-namespace scan.
The index listeners' pathSummaryImported invalidation may also reseed matching PCRs.
Maintain independent full source/target snapshot validation in tests while bounding the
production import path. JsonIdentityDelta has a package-private constructor and only
committed authoritative readers create it; preserve that trust boundary if validation
uses induction from the exact base epoch. Do not claim reciprocal link checks alone
prove sibling reachability or acyclicity. No gate 7 implementation or acceptance yet.


Gate 6 oracle refinement during the run-3 limiter wait: generated copies now query
PATH and CAS postings per source path as well as globally, and numeric-column safety
checks respect the persisted format's sticky conservative flags. A new 24-configuration
empty-document fixture bootstraps declarations before any record set exists, creates
and removes the whole document, then restores its identities from history. The focused
run includes that fixture; generated fixed-seed operation streams are unchanged.


Gate 6 run 3: 375 invocations, 351 pass and 24 multi-batch permutation cases fail.
Valid-time reconciliation, all four initial/later failure checkpoints, prior graph/epoch/
history tests and the 24 empty-document bootstrap/restoration configurations pass.
The multi-batch removal phase re-extracted retained rows from the final document,
where a not-yet-drained row may already be deleted. It now holds a committed base reader
and base path summary only while draining old membership batches. Final row installation
still uses final document state, and metadata keeps the writer epoch. Run-3 XML retained
in build/replay/projection-epoch-3-results. This correction awaits its own rerun.


Gate 6 accepted (2026-10-05 20:44 Berlin): 375 focused cases, 144 full generated
configurations, 51 existing core/projection work budgets and formatting are green.
The source CAS visitor repair remains the independent f6688ece7 commit. The new epoch
seam does not weaken requireVirginTreeForInitialBuild, use an unordered append loader,
or model an epoch as one subtree move. Old/final membership edits are bounded batches;
retained old rows use the committed base until removal completes. Gate 7 follows.

## Gate 7 implementation order

1. JsonReplayPaths.importChanges reuses JsonReplayPageWalk over committed PATH_SUMMARY
   tries; it compares complete logical records/statistics, imports only differences and
   maps revision metadata. PathSummaryReader.applyImportedChanges updates just replaced
   cache entries and affected cached path matches. No initial source path-reader build
   is needed merely to enumerate records. Logical-path cache invalidation must include
   unchanged descendants of a renamed/reparented path node.
2. Retain JsonReplayGraphValidator as the independent full oracle; add a bounded import
   validator around the sealed authoritative-delta/base-epoch contract. Document the
   inductive proof and validate affected links/records rather than claiming local
   reciprocal links alone prove global acyclicity.
3. Add gated replay counters and non-vacuous, mutation-checked budgets for changed-page
   visits, records, created identities, staging, ancestors, sidecars and bookkeeping.
   Include no-op, append beside large untouched regions and sparse frontier fixtures.
   Account for writer/cache lifecycle work as well as the helper's explicit loops.


Gate 7 path substep: path-delta-1 passes 377/377 focused and two-configuration generated
cases, with formatting. JsonReplayPaths now compares authoritative path tries directly,
including logical names, topology, revision metadata, frontier and serialized statistics.
Only changed path records are staged. The writer-owned path reader repairs exact QName/
child mappings and cached path matches for affected logical subtrees; unchanged descendant
expressions beneath a rename cannot reuse their old PathNode cache. Statistics-only
changes preserve cached PCR matches. Projection epoch memoization is always invalidated,
while path membership reseeding occurs only when the namespace changed. No full source
path reader or full post-import cache reconstruction is needed by this substep. The
remaining graph validator is still a full walk; work counters and budgets remain pending.
Evidence: build/replay/path-delta-1.log and path-delta-1-results.


## Gate 7 bounded transition and budgets (2026-10-05 evening)

`JsonReplayTransitionValidator` checks exact staged payloads, all old/new boundaries,
compatible kinds and Dewey order, memoized parent chains, affected child lists and
count contributions. Initial snapshots retain full graph validation. No-op deltas
skip document validation; pure appends retain the already-proven base prefix and
validate only newly created suffix identities. General permutations may walk affected
sibling lists but never their unchanged subtrees. This is an incremental inductive
proof over a validated base plus a complete authoritative delta, not a claim that
reciprocal pointers alone prove reachability or acyclicity.

Eight forged-delta regressions (four versioning types) reject reciprocal disconnected
sibling rings and parent cycles with hashes, Dewey IDs and stored child counts disabled;
rollback preserves the prior revision and valid retry succeeds. `transition-2` passes
191 selected cases (109 import, 24 derived index, 48 projection epoch, two generated,
eight forged graphs), plus formatting. Full history/epoch suites and 144 generated
configurations are included in the final gate-7 selection, not yet rerun at this point.

`ReplayWorkDiagnostics` is static-final gated (`sirix.replay.workDiag`, false in
production). Record work counts actual node cursor moves AND storage lookup/prepare
calls, including nested calls and writer reinitialization. Path steps include cached
moves. Created/staged document identities, ancestry hops, presentation sidecar attempts,
authoritative indirect/leaf resolutions and pending-diff map operations/entry visits
are separately exposed through EngineWorkCounters. The diagnostic pending map is used
only with the gate enabled. Existing bounds are unchanged.

Measured across FULL/DIFFERENTIAL/INCREMENTAL/SLIDING_SNAPSHOT in
`replay-budget-scout-2`: append beside 4096/16384 unchanged leaves takes 2158 record
visits, 12 authoritative pages, three creations, six stages and five ancestry hops.
Appending beyond a trillion-key reservation takes 2153 visits and 18 pages. No-op
and frontier-only epochs take 29 visits, six path moves and zero stages/pages/ancestry/
sidecar reads. Whole-path-schema writer initialization is included, so these figures
are specifically for one array PCR, not a claim that unrelated writer construction
is constant for arbitrary schemas. R8 costs exactly three map operations per move.

Mutation evidence: restoring full graph traversal fails all 12 replay fixtures
(27776/113792 append visits for 4096/16384 prefixes; 193 visits for small no-op).
Restoring R8 map scans fails all six fixtures (1648/25024/395008 operations for
32/128/512 moves). Disabling immutable-region pruning fails all 12 page budgets
(small no-op six pages instead of zero; larger append 26/74 instead of <=20).
All three mutations were restored immediately after their runs; logs/results live
under `build/replay/replay-budget-mutation-{1,2}*`. Deep ancestry fixture and restored
healthy validation are the current pending run. No benchmark or production routing yet.


Restored healthy budgets pass 26/26 (`replay-budget-healthy-1`). Shared-ancestry mutation
fails all eight depth/version fixtures: 2673/10961 hops at depths 32/96 instead of
97/161 (`replay-budget-mutation-3`). Validator restored. Gate-7 final selection now
runs every identity epoch/history/index test, all 144 generated configurations,
forged graph cases, the exact core/projection work-budget selection and formatting.


## Gate 8 execution design (before production routing)

Route only full-resource history copies through InternalJsonNodeTrx.importRevision;
initial state uses the source snapshot and subsequent epochs use the authoritative
reader. Keep allocating subtree snapshots and BasicJsonDiff's public shape/HASHED
behavior. Reject unsupported historical subtree placement before changing the target.
Run JsonBulkInsertDiffRegressionTest under each sirix.replay.versioning value (its
existing harness chooses matching source/target configuration and R16 additionally
expands Dewey/recompute modes explicitly), then full core/query and exact budgets.
Rebase on the separately owned tombstone/restore fix when it lands; no duplicate fix.

Latency acceptance must use the limiter, taskset 0-11, identical JVM/configuration and
alternating matched baseline/candidate forks. Retain fork/iteration raw nanoseconds,
allocation deltas and separate untimed work diagnostics. Include source append+commit,
public compact/materialized and no-op diffs, authoritative replay read and complete
copy. The reference fixture is 4096 initial + 4096 appended primitives + no-op.
Additional fixtures cover large unchanged siblings, equal-valued moves, deep nesting,
sparse reservations and deleted-identity restoration. Bootstrap matched fork blocks
5000 times with a fixed seed, 95% intervals; lower ratio bound >1.05 confirms a
regression. Report both fresh baseline/candidate medians and the report's 89–111 ms
copy context. No latency claim is made from work-budget results alone.

Gate-7 final selection passed: 604 tests, zero failures/errors/skips, formatting clean.
Evidence: gate-7-final.log/results; all mutations restored. The generated matrix also
reproduces a pre-existing pipelined source sidecar failure: serialization opens an
unpublished new revision before hardenAndPublish. Identity replay stays correct on
that cache miss. Prepare a separate source regression/repair before routing production.


## Source follow-up: pipelined presentation sidecars

Generated KEEP_OPEN_ASYNC_COMMIT histories exposed missing public sidecars because
JsonDiffSerializer opened revision N before background hardening published N.
`async-sidecar-red-1` reproduces the omission in all eight version/Dewey cases.
Borrowing the writer after commitWritePages is too late: that step retires its read
buffers (`async-sidecar-fixed-1`). The final design prepares immutable JSON and its
path while the frozen writer still owns live pages, restores both borrowed cursors,
keeps pending-map/ingest state on failed phase 1, and clears it only after phase 1
succeeds. A callback owning only Path/String publishes atomically after hardening;
no writer, mutable map or path reader crosses the async boundary. XML has no sidecar
preparer; synchronous behavior keeps its existing durable reader path.

`async-sidecar-fixed-2` passes selected serializer/diff/ingest budgets, async frontier
ordering, all eight regression cases, and a held-hardening publication test proving
no file before durability and no successor-value leakage. Extending insert ordinal
coverage and rerunning the generated matrix before the independent fix commit.

Async source follow-up final validation: 242 tests, zero failures/errors/skips,
including all 144 generated configurations and all core/projection budgets; formatting
passes. No cache-serialization errors occur in async-sidecar-final.log. Final XML is
archived in async-sidecar-final-results. Independent fix ready to commit.


Production routing now uses the validated identity protocol exclusively for complete
history copies. The key-sorted fragment allocator and public-sidecar replay scheduler
are removed from JsonResourceCopy; allocating snapshot copying and the public explicit
copyNodeWithKey API remain. Public diff methods retain behavior and JSON shape.
`production-copy-1` passes the historical regressions including all 16 R16
version/Dewey/recompute combinations, snapshot copying and public diff budgets.
Full versioning matrix and new history-suffix/preflight integration cases follow.
The separate async source repair is committed in 56052a8cd (242 final tests green).


Production-copy matrix passes: FULL, DIFFERENTIAL and INCREMENTAL each 177/177,
zero failures/errors/skips; the initial SLIDING_SNAPSHOT selection passed 160/160.
Each run includes R16's own all-version/Dewey/recompute expansion. New 17-case public
integration coverage verifies suffix revision metadata/identity, source document or
sole-value cursors, and rejection of partial-history placement or dirty destinations
without discarding their state. Formatting and git diff --check pass. Production
route ready for its own commit; full core/query suites follow under the same limiter.


## Gate 8 benchmark preparation (2026-10-05 22:30 Berlin)

The standalone harness and exporter are prepared under build/replay/latency. They have
not yet been compiled or timed. The exporter snapshots candidate classes before a
reversible in-worktree baseline source restore, automatically restores the candidate
on exit, and verifies that exported classpaths contain no live module build output.
No extra worktree is created. Alternating matched fork blocks, fixed-seed 5000-draw
bootstrap, current-thread allocation deltas and separate diagnostic forks are wired.
The captain's explicit resume/keep-working order allows normal work in this evening's
window; benchmark admission ends 03:30 Berlin with a five-minute per-fork timeout to
respect the 03:40 validation cutoff. No benchmark JVM has started.

Memory contract: the transaction intent log bounds resident staged page buffers and
prevents intermediate publication. The detached typed delta and validator scratch maps
scale with changed identities (the initial snapshot with the initial document); they
are not a constant-total-heap or streaming-manifest claim. Unchanged document regions
are excluded by durable page identity and append/no-op work budgets.


## Main prerequisite and full-suite checkpoint (2026-10-05 22:55 Berlin)

Inbox 006 is acknowledged. The tombstone/restore-key prerequisite is on origin/main
at af9f20e5a, alongside the query namespace repair. Rebase after full-core-query-1
finishes, remove the generated oracle's disjoint-frontier workaround, and enable the
upstream disabled later-created-parent fixture; retire its resolved limitation entry.
Final full suites and budgets must run on that combined branch.

The first broad core run passes 13,870 tests, zero failures/errors, 77 skipped;
full-core-query-1-results/core contains all 1,119 suites and a JSON summary. Query
currently has one failure at JsonIntegrationTest.test's hash-history assertion:
expected revisions 1/2/3, actual 1/2/3/4/5 with identical object JSON at 3/4/5.
Revisions 4/5 insert its left/right neighbors. ObjectNode.computeHash and the
architecture report explicitly include those sibling links. The source hash repair
corrects their formerly stale hashes; update this expectation and retain the exact
JSON and revision checks. No production hash contract change is called for.

The root Gradle test configuration forwards sirix.* properties, confirming the prior
versioning runs used their requested modes. A prepared test-only patch adds an
explicit forceRecompute switch around both historical copy entry points, temporarily
removing and then restoring every presentation cache. Run that additional matrix under
all four versioning types, recording the actual fork configuration in test output.
The patch is retained at build/replay/recompute-matrix.patch and is not applied while
the current full-suite process is active.
