# Bug: backups omit the hidden derived resources

- Fix / issue: [#1382](https://github.com/sirixdb/sirix/issues/1382); spec
  [TXN-613](../../spec/TXN-transactions-durability.md#txn-613--backups-include-derived-resources).
- Symptom: `BackupManager.backupDatabase` produced a backup directory with no
  `resources/<name>$derived` entry. A database restored from it had no temporal-aggregate data:
  queries that had been served from the aggregate index fell back to a full scan until the
  maintainer rebuilt the index from the source log, and the restore reported nothing about it. The
  summary, the CLI output and `docs/BACKUP.md` all claimed a complete backup.
- Trigger: back up any database with a resource that declares a temporal aggregate
  (`BackupDerivedResourceTest.createSourceDatabaseWithTemporalAggregate`): a projection index, a
  `TemporalAggregateDeclarations.declare` on it, and a drained maintainer. The backup's
  `derivedResources()` is empty and `resources/records$derived` does not exist in the target.
- Root cause: the backup enumerated resources with `Database.listResources()`, whose contract is the
  *public* listing — it deliberately filters out `DerivedIndexMaintainer.isDerivedResourceName`
  entries, because a derived resource is internal and must not appear to callers. The backup used
  that listing as if it were "every resource whose files have to be copied". Those are two different
  sets: hiding a resource from callers says nothing about whether its bytes are part of the
  database's durable state. The restore side had the mirrored gap: its copy pass walked the whole
  tree, so a derived resource *present* in a backup was restored, but its verification pass used the
  same public listing and so verified and reported only the visible resources.
- Why tests missed it: `BackupRestoreTest` built its fixture with
  `ResourceConfiguration.newBuilder(name).build()` — no valid-time configuration, hence no derived
  resource to lose. Every assertion was over the resources the test had created by name, so no test
  ever asked "is anything else on disk that a backup must carry?". The derived-index tests, in turn,
  never went through backup or restore. Neither side's oracle could see the other's omission.
- Regression test: `BackupDerivedResourceTest` (sirix-core) and
  `BackupRestoreTemporalAggregateTest` (sirix-query). Fails-before arm: the fixed `BackupManager`
  with only its two behavioural decisions reverted (`quiesceDerivedResource` returning "no derived
  resource", the restore's derived pass skipped), compiled into a separate output directory ahead of
  the build output, so both arms differ in behaviour alone. Before: 0/4 pass —
  `derivedResources=[]`, no `records$derived` in the backup, a truncated derived store restores
  clean, no refusal while maintenance is held. After: 4/4 pass.
- Lesson: see [Lessons](../LESSONS.md) (see the [fragment convention](../../changelog.d/README.md)).

## Copy-boundary and restore-verification follow-up

- Symptom: a hidden sibling rebuilt after the database metadata copy can reuse an allocated ID on
  the next create; a paused truncation can leave derived mappings newer than the copied source;
  damaged restart metadata can be silently rebuilt during restore.
- Trigger: remove the derived sibling before backup; or pause a caught-up maintainer and truncate
  its source; or corrupt the derived temporal-aggregate page while retaining its revision root.
- Root cause: the database allocation counter was copied before resource membership settled, a
  history lock was treated as proof of a valid frontier, and restore validated only the root rather
  than the metadata the next open consumes. These are three incomplete copy-boundary invariants.
- Fix: reconcile the target counter against all copied settings; refuse pending rewinds under the
  existing lock order; validate structure, frontier and mappings with a tool-opened detached derived
  reader and private caches before opening any source. Both backup and restore check the source-head
  bound. Private caches ensure verification reads the copied bytes rather than warm source pages.
- Why tests missed it: the original tests used stable metadata, did not truncate a paused source,
  and damaged the whole derived file rather than a referenced metadata page below an intact root.
- Regression tests: `BackupDerivedResourceTest.backupReservesIdsForRebuiltDerivedResources`,
  `backupRefusesAPendingDerivedRewind`, `restoreRefusesDerivedMappingsBeyondTheSourceHead`,
  `restoreRefusesDamagedDerivedRestartMetadata`, `restoreRefusesIncompleteDerivedStructure`.
  Their independent oracle is TXN-613: unique future IDs, mappings bounded by the copied source,
  and rejection without repair of unreadable derived state. Counterfactual execution and mutation
  evidence remain for the outer verification phases. The review round passed all 8 tests in
  `:sirix-core:test --tests io.sirix.backup.BackupDerivedResourceTest`, with attached startup,
  allocation, storage-epoch and disabled-cache test tasks excluded.
- Vision check: preserves durable historical derived state and refuses unverifiable copies, matching
  “History is the data model” and “A fast answer is the same answer”; introduces no new default.

## Verification cache pressure ownership

- Symptom: an online backup or restore leaves allocator pressure evicting closed verification
  caches instead of the live manager's caches, risking allocation stalls or exhaustion.
- Trigger: verify a copied derived resource while another buffer manager owns the process-wide
  allocator pressure handler, then request pressure eviction after the verifier closes.
- Root cause: `BufferManagerImpl` construction combined private cache creation with implicit global
  handler registration. The short-lived verifier needed the caches but did not own that registration.
- Fix: the verification factory constructs isolated caches without registering a pressure handler;
  the shared backup/restore verifier uses that factory. Ordinary constructors retain their existing
  registration behavior, and verification never swaps or restores global handlers.
- Why tests missed it: backup tests checked copied bytes and corruption refusals, but never exercised
  the live cache's pressure eviction after verification or while its temporary caches remained open.
- Regression: `BackupPressureHandlerTest` checks real live metadata cache eviction through the
  installed pressure callback after backup, successful restore and failed restore, plus while
  verification caches remain open and after they close. Its independent oracle is TXN-613's live
  cache ownership invariant; the wrong handler leaves the live page cached. No timing bound is used.
  Focused execution passed all 4 cases plus the existing 8 `BackupDerivedResourceTest` cases in
  `:sirix-core:test`, with only those two classes selected and attached startup, allocation,
  storage-epoch and disabled-cache tasks excluded. Counterfactual and mutation runs belong to the
  outer verification phases.
- Vision check: preserves the existing allocation-pressure path and private verification caches,
  supporting “Speed is part of correctness” without changing live-manager defaults.

## Reusing a failed copy's target

- Symptom: retrying a backup at its cleaned target rejects valid reissued history using stale
  offsets and hashes from the first copy's derived resource.
- Trigger: copy and verify the first resource, fail on a second resource's busy writer, truncate and
  reissue the first resource with larger aggregate contents under the same revision numbers, then
  retry at the same target path.
- Root cause: the verifier's private page caches still sat above normal storage opens, which used
  path-keyed revision caches/indexes and superblock validation. The temporary session also updated
  the global revision epoch. Private page caches did not isolate the whole verification open.
- Fix: the shared verifier uses a session-free tool reader with read-only channels, uncached
  configuration, a private revision-record cache and fresh superblock checks. The existing complete
  revision-index validation is shared with normal storage opens, and frontier/mapping acceptance is
  unchanged. No storage-provider registry, session registry, writer lock registry or global epoch
  is entered. Normal resource readers and writers retain their session ownership and cached opens.
- Why tests missed it: every prior backup test used a fresh target and did not replace the physical
  history after a partial copy had populated path-keyed metadata.
- Regression: `BackupDerivedResourceTest.backupRetriesACleanedTargetAfterHistoryReissue` exercises
  partial success, writer refusal, cleanup, reissue with twenty groups instead of one, retry and
  restore. TXN-613 is the independent oracle: the retry must accept the new bytes and report their
  source and derived revision heads. Reusing stale offsets/hashes must fail this behavior test.
  Focused execution passed all 21 cases across `BackupDerivedResourceTest`,
  `BackupPressureHandlerTest`, `NodeStorageEngineReaderTest` and
  `NodeStorageEngineWriterTruncateToRevisionIntegrationTest`, using only those class filters on
  `:sirix-core:test` and excluding attached startup/allocation/storage-epoch/disabled-cache tasks.
  Mutation and counterfactual runs remain with the outer verification phases.
- Vision check: verification reads the actual durable history and refuses actual corruption without
  retaining identity-bound state from earlier copies; no live read/cache default changes.

## Missing committed beacons during isolated verification

- Symptom: a derived store with both committed beacon slots zeroed can be accepted as revision 0,
  then silently rebuilt when the restored source opens.
- Trigger: zero bytes 4096–12287 in a populated derived backup while retaining its superblock,
  revision records and all pages, including the readable revision-0 root.
- Root cause: the isolated reader bypassed the disk storage constructors' existing guard against
  treating a nonempty file as a fresh bootstrap store. Root and mapping checks could then accept
  the real but unrelated revision-0 state.
- Fix: FileChannel storage, memory-mapped storage and the isolated reader call the same committed
  beacon guard. Valid committed revision-0 stores remain accepted; empty-store bootstrap and
  conservative first-commit recovery retain their previous behavior. Verification retains strict
  feature checks, fresh UUID-checked superblocks, complete revision-index validation and private
  metadata before checking the derived root and mappings.
- Why tests missed it: earlier damage tests truncated pages or damaged referenced metadata while
  keeping a valid beacon; none removed both beacons while preserving a readable revision-0 root.
- Regression: `BackupDerivedResourceTest.restoreRefusesDerivedStoresWithZeroedBeacons` changes only
  the beacon slots, asserts the other persisted bytes remain intact, and requires refusal with
  target cleanup under TXN-613. Accepting the bootstrap probe and reporting success violates its
  independent oracle. Focused execution passed all 28 cases across `BackupDerivedResourceTest`,
  `BackupPressureHandlerTest` and `InterruptedFirstCommitRecoveryTest`, with only those class filters
  on `:sirix-core:test` and attached startup/allocation/storage-epoch/disabled-cache tasks excluded.
  Counterfactual and mutation verification remain with the outer phases.
- Vision check: refuses unverifiable durable state without silently replacing historical derived
  data, preserving the existing disk-engine acceptance rule and live cache ownership.

## CI test selection and tool-reader ownership

The first blocking mutation run selected name-related tests but omitted the existing TXN-613
`BackupDerivedResourceTest`, `BackupRestoreTest` and `BackupPressureHandlerTest` contracts.
The new isolated verification path consequently appeared uncovered even though its regressions
ran in the ordinary test lane. Explicit CONV-204 associations now connect the cross-cutting
backup contract to its reader, configuration, storage, derived-maintainer and cache entry points.
The CLI change also moved the existing non-null-list assertions and added assertions for the
derived list. Their per-mutant equivalence records must follow the compiled identities and
explain the relevant immutable-list or public-command input contract.

Sonar's reliability gate rejected the isolated tool open's catch-only channel cleanup. The
reader's ownership must be explicit on all exits: until the fully initialized result is returned,
`finally` blocks close the reader and each acquired channel, including constructor and
revision-index failures. Once ownership transfers, the backup/restore verification caller keeps
the reader open and closes it after verification. The existing damaged-store and successful-copy
regressions exercise those error and success paths; no test asserts on source spelling.

### Interrupted quiescence cleanup

The new cancellation regression reproduced a masked refusal before the fix:
`interruptedBackupPreservesInterruptionAndCanBeRetried` failed because writer rollback threw
`SirixThreadedException` from its interruptible lifecycle lock, replacing the original derived
quiescence `SirixIOException` and skipping writer close. Interruption during writer I/O could also
close a live channel. The initial fix deferred interruption through writer cleanup, but left the
enclosing lifetimes exposed; see [the complete cancellation follow-up](#cancellation-across-the-complete-backup-lifetime)
for its replacement and regression. The operative cleanup invariant is owned by
[TXN-613](../../spec/TXN-transactions-durability.md#txn-613--backups-include-derived-resources).
The pending-rewind refusal also checks that another thread can acquire the history lock while
maintenance is paused.

The CI follow-up keeps the original writer implementation and its direct session access: tool readers keep configuration internally rather than adding a public accessor or changing
posting-index readers. It removes the unnecessary configuration API and the fallback that copied a sibling without its installed maintainer. The derived summary now
comes directly from verification of the copied bytes. Frontier validation still requires a
matching durable mapping, and one full mapping-bound check also bounds that frontier.

Local contract verification passed 43 core cases plus 6 CLI cases, including both document
models, file-channel and mapped stores, each unsupported feature word, retained revision-0 pages
with erased beacons, actual Linux descriptor ownership, pending-rewind lock release, and the
interruption regression's successful retry. All 363 core work-budget cases and 11 selected
allocation checks passed. The deterministic interruption test was observed
failing before its cleanup fix. SonarCloud analysis needs the CI credential; its local rerun is
not claimed.

The final local changed-line PIT verdict completed both modules with no failing mutants, no
uncovered mutants, valid test associations and no failing dispositions: 107 core mutants
(99 killed, 6 timed out, 2 runtime errors detected by PIT) and 9 CLI mutants (5 killed, 4
documented equivalent Kotlin assertions). The local selection omitted four source files
restored byte-for-byte to the PR base, matching the reduced diff the outer executor will commit.
The full reports and failed-before/fixed-after logs are archived locally under
`build/ci-pr1499/`; no mutation threshold, CI deadline or work bound was widened.

## Cancellation across the complete backup lifetime

- Symptom: cancelling a backup during derived quiescence can retain its database reference after
  writer cleanup, preventing the application's last handle from releasing OS database ownership.
- Trigger: hold the derived history lock, admit the backup's real source writer, interrupt its wait,
  then close the application handles after the backup returns.
- Root cause: writer cleanup restored interruption before the enclosing session and database
  lifetimes closed. Their notification retirement rejects an interrupted caller. The test replaced
  those lifetimes with a no-op close and a mocked database, hiding the leaked ownership.
- Fix: one primitive flag owned by the outer backup records interruption. Lock-wait cancellation
  and teardown defer it before closing each enclosing lifetime; the outermost finally cleans the
  target and restores the flag once, including when teardown itself throws. No intermediate close
  restores it, and writer close remains guaranteed if rollback throws.
- Regression: `BackupDerivedResourceTest.interruptedBackupPreservesInterruptionAndCanBeRetried`
  uses real source/session/database ownership. The JVM's lock-owner observation establishes the
  cancellation boundary without sleeps. It checks the caller's restored interruption, absence of
  a partial target, acquisition of the released OS ownership lock after application close, and
  reopening and retrying a backup. TXN-613 supplies the independent cleanup and ownership oracle.
  A leaked backup reference leaves the ownership lock held and fails the test. Focused execution
  passed all 17 tests across `BackupDerivedResourceTest` and `BackupRestoreTest`, with only those
  class filters on `:sirix-core:test` and all attached verification forks excluded. Mutation and
  counterfactual validation remain with the outer phases.
- Vision check: cancellation releases the engine lifetimes it acquired and preserves the caller's
  cancellation signal without changing durable data or live read defaults.


## CI cancellation-path follow-up

The next hosted PIT run completed analysis, but cancellation coverage left the outer restoration's
success and cleanup-error paths unobserved. It also showed that explicit writer rollback/close was
redundant: the owning resource session already aborts and closes every transaction on teardown.
Keeping two cleanup owners obscured which lifetime actually released the writer semaphore.

TXN-613 already requires release of all backup-owned lifetimes and restoration of interruption even
when cleanup fails. The missing cases now execute the public backup API with filesystem-boundary
faults: an interrupted resource copy, failed target deletion (runtime exception or error), and an
interrupt arriving after successful metadata publication. Their independent oracles are the caller's
interrupt flag, the preserved copy error, a usable source and backup, writer admission, and actual
OS database ownership after application handles close. These extend the original real-lifetime
quiescence-cancellation test; no test examines implementation source or private state.

The copier now lets its session own writer teardown, captures interruption before closing that
session and the database, and restores the deferred flag through one shared outer-boundary helper.
Keeping target cleanup and restoration in that helper also avoids javac duplicating a cleanup-error
handler into the successful-return path, where cleanup is skipped and that handler is unreachable.
The final restoration does not need to clear an interrupt already left by target cleanup: leaving
that flag set already preserves it. The failure sequence therefore releases the derived history
lock, aborts/closes the session-owned writer, closes the database, attempts target cleanup, and only
then restores the deferred signal. Normal successful copying uses those same session/database
lifetimes and keeps the copied revisions unchanged.

Both Sonar reliability reports are analyzer limitations at deliberate ownership boundaries, rather
than leaks or swallowed interruption. S2142 cannot follow the deferred flag to the outer backup
boundary; immediately re-interrupting the catch would reinstate the reviewed cleanup bug. S2095
cannot follow conditional channel transfer into the returned reader. Narrow method-level
suppressions explain these contracts. The existing real-handle tool-reader tests cover each failed
acquisition, handler initialization, revision validation and successful caller-owned close; the
same tool open serves both backup and restore verification. Other Sonar rules remain enabled.

Vision check: this removes duplicate cleanup while preserving immutable source history and copied
derived data; it changes no engine default or query/read hot path.


Local verification of this follow-up passed 34 focused tests across `BackupDerivedResourceTest`,
`BackupRestoreTest`, `BackupManagerInMemoryResourcesTest`, `ToolReaderOwnershipTest` and
`BackupPressureHandlerTest`. The final complete changed-source PIT reports detected all 113 core
mutants (109 killed, two timed out, two runtime errors) and accepted the CLI's four existing
per-mutant equivalence records beside five killed mutants. Neither module has an uncovered or
undispositioned surviving mutant. Test associations are valid.

The full local runner initially returned failure only because the global disposition checker
could not find the unrelated REST classes in this worktree. After `:sirix-rest-api:classes` built
those inputs, `pr_mutation.py dispositions --check` passed against both saved complete reports and
censuses, with the original PR base; no registry entry was changed. Java formatting and
`git diff --check` passed. Evidence is under `build/ci-pr1499-followup/`, including the raw runner
result and the final successful disposition check. Authenticated SonarCloud reanalysis is left to
the outer executor; no local Sonar credential was available, and no hosted rerun is claimed.


## Canonical paths in cancellation fault injection

- Symptom: hosted macOS `BackupDerivedResourceTest.interruptedCopyReleasesOwnershipEvenWhenTargetCleanupFails`
  returned successfully in all three cases instead of throwing the injected copy/cleanup failure.
- Trigger: the temporary source path passes through macOS's `/var` symlink; database ownership
  resolves it to `/private/var` before listing resources.
- Root cause: the filesystem fault injector matched resource-copy paths against the lexical input
  prefix. The copier received the canonical resource path, so the fault never fired. This was a
  test-boundary identity error, not a production lifetime or eviction failure.
- Requirement and oracle: TXN-613 requires cancellation to preserve the original refusal and
  interruption while releasing writer/session/database ownership, including failed target cleanup.
  The test observes those errors, the interrupt flag, subsequent writer admission and OS lock
  acquisition. A backup that swallows cancellation or retains ownership must fail these assertions.
- Fix: resolve the expected source-resource prefix with `toRealPath()` before installing the
  filesystem mock. Cover all three failure modes through both direct and noncanonical `.` source
  paths, without needing symlink privileges on Windows. Target cleanup and successful publication
  use the supplied target path and do not pass through database ownership canonicalization.
- Regression: the noncanonical cases reproduce the missing fault locally with the old matcher.
  Run `./gradlew :sirix-core:test --tests 'io.sirix.backup.BackupDerivedResourceTest'`.
- Lesson: identify filesystem fault targets in the same path representation as the real boundary;
  exercise noncanonical inputs without weakening the behavior oracle.
- Vision check: the correction preserves cancellation and immutable-history assertions across
  platforms; no production behavior, defaults, work budgets or mutation scope changes.

The other failure in hosted run 45717237032 was external to this head's source: its checkout was
synthetic merge 4c1a1345c (head 5d5173c5 merged into newer base bf3e95c7), but mutation selection
compared dispositions with the event base 7f8fb8e7. Fourteen REST dispositions from that newer
base appeared new without a corresponding REST source change in this PR's mutation scope.
Both hosted mutation modules completed; core detected all 113 changed-line mutants and the CLI's
four survivors had accepted dispositions. The disposition helper passes for the stated head/base
using both hosted reports and censuses. No registry waiver or lane-selection change is warranted;
the outer executor must obtain fresh hosted evidence with consistent merge/base revisions.

Local verification passed 401 selected core tests (364 budget cases and 37 backup/ownership
cases), the isolated allocation/startup/ownership tasks, and `spotlessJavaCheck`. The default
workstation-sized run was killed with exit 137 during budgets; the complete retry passed with
`-Dorg.gradle.jvmargs=-Xmx1g -PtestHeapMin=256m -PtestHeapMax=1g
-Dsirix.allocator.maxSize=512M --max-workers=1`, without changing the selected tests or any work bound.

The complete blocking PIT rerun also passed with Error Prone enabled and a local 512 MiB
allocator cap: core detected all 113 mutants (109 killed, two timed out, two runtime errors);
CLI killed five and accepted its four existing equivalent mutants. Neither module has an
uncovered or unresolved surviving changed-line mutant. The disposition helper passed with both
complete reports and censuses; no disposition, selection setting or mutation time budget changed.
Evidence, including the three failing noncanonical cases before correction and the final reports,
is under `build/ci-pr1499-canonical-paths/`. Hosted CI refresh remains the outer executor's task.
