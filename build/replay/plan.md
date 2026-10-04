# Identity delta replay implementation and verification plan

Started 2026-10-04 on fm/sirix-replay-identity-delta from 71be74062.
Design authority: /home/johannes/IdeaProjects/firstmate/data/sirix-diff-replay-design-review/report.md,
recommendation B and its ordered migration/acceptance plan. The report remains read-only.

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

1. [in progress] Pin public behavior and promote R16 across Dewey/recompute/versioning;
   define typed protocol without routing production replay to it.
2. [pending] Independent full-snapshot test reconstruction and private import seam;
   explicit keys/gaps/frontier, stage-failure rollback tests. Rebase onto the separate
   tombstone/recreate/restore fix when firstmate reports it landed; do not duplicate it.
3. [pending] Authoritative delta discovery; shadow hook compares delta-applied graph to
   separately enumerated full target snapshot before enabling production routing.
4. [pending] Commit/revert/rollback/async epoch coverage; exact manifest base validation.
5. [pending] Fixed-seed shrinkable operation streams across all four versioning types,
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
- No production edits or validation runs yet. Report establishes R16 on current base.

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
