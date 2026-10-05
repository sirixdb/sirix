# Identity delta replay implementation and verification plan

Started 2026-10-04 on fm/sirix-replay-identity-delta from 71be74062.
Design authority: /home/johannes/IdeaProjects/firstmate/data/sirix-diff-replay-design-review/report.md,
recommendation B and its ordered migration/acceptance plan. The report remains read-only.

## Current checkpoint (2026-10-05 02:51 Berlin)

Source hash/count repair is the first commit 90d4fdd87b7ed3776087d105c87d16620f9dbb02;
its focused suite passes 267/267. Sparse trie growth is a separate follow-up, 0067f38b3,
with 24 source-only cases through 2^52. See their verification/finding files below.

Gates 1–4 now have the typed contract, snapshot/import seam and authoritative page delta
in shadow validation. Latest focused run: 116/116 (109 import/oracle cases plus seven
public diff budgets), including real overflow references, restoration after revert,
trie growth, no-op/frontier-only epochs and cold history. Gate 4 now passes 279/279 with exact epoch rejection, later-stage rollback/retry,
concurrent commit serialization and source bulk/async/revert/rollback epochs. Continue
generated shrinking, derived-index integration, work budgets and final
full suites/latency in order. JsonResourceCopy remains unchanged.

The prerequisite legacy allocator/tombstone fix has not appeared in the local origin/main
ref or inbox; do not duplicate it and rebase when it lands. No production routing switch,
performance acceptance, push or no-mistakes pipeline. The 03:40 validation cutoff and
03:40–03:55 save/park window remain in force.

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
5. [in progress] Fixed-seed shrinkable operation streams across all four versioning types,
   hash NONE/ROLLING/POSTORDER, Dewey on/off, auto-commit and KEEP_OPEN/async modes;
   sidecar present/missing/corrupt/diffs-disabled, cold reopened history.
6. [pending] Oracle compares keys, kind, name/scalar, parent and ordered child/sibling
   links, counts, frontier, revision metadata, hashes, stored Dewey IDs, queried indexes
   and path summaries. Include equal-value swaps, later parents, deleted-key restore,
   sparse reservations, replacement survivors and empty/no-op revisions.
7. [pending] Work budgets: record visits, created identities, staged records, ancestor
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
