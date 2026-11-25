# Bug: in-memory storage lost committed revisions

- Fix / issue: [#1463](https://github.com/sirixdb/sirix/issues/1463); fixed by the commit that adds this write-up.
- Symptom: on an `IN_MEMORY` resource a node inserted and committed successfully was gone once
  the writer was reopened (`moveTo(key)` returned `false`). The session reported revision 0 after the
  commit, a reader of the same session saw nothing, a new resource session started empty, and
  `beginNodeReadOnlyTrx(revision)` on an older revision had no revision root to read. JSON and XML
  resources were both affected. Found by the lifecycle-fences work (#1353) as a no-fault control
  that failed on unchanged production code.
- Trigger: create a database and an `IN_MEMORY` resource, insert one value node, commit, close the
  writer and open another writer on the same session.
- Root cause: `RAMStorage` met none of the storage contract's ownership rules.
  1. It stored the writer's page instances themselves instead of their encoding. Two effects
     follow. `UberPageFormat.write` is what clears an uber page's `isBootstrap` flag, so a page
     that was never encoded stayed "bootstrap": every commit republished revision 0, and each new
     writer started from the bootstrap revision again. The writer's commit cleanup also closes and
     reuses the pages it wrote, and those were the store's only copies, so readers could be handed
     closed pages. `returnsSharedPages() == true` existed only to stop callers releasing these
     shared instances; it could not stop the writer.
  2. It never recorded revision roots: `readRevisionRootPage`, `getRevisionFileData` and the
     revision-timestamp lookups read a map that nothing filled, and the revision index stayed empty.
  3. Its state belonged to the `RAMStorage` instance, and each resource session builds its own
     storage. When the last user session closed, the database-owned shared session closed with it,
     and the next session (including the first one after `createResource`, whose bootstrap session
     had already closed) opened an empty store.
- Fix: the store now keeps exactly the payload a file-backed writer would append (page encoding,
  key-value leaf page cache, byte-handler pipeline, page hash set on the reference). It records each
  revision root and serves the uber page in the file storages' beacon framing. Reads go through
  `AbstractReader`'s checksum and decode path and return a fresh, caller-owned page, so
  `returnsSharedPages()` is `false` like every other backend. Page keys start at
  `IOStorage.DATA_REGION_START`, so they never collide with the beacon offsets the writer re-reads
  as pages on close. File-backed read and write paths are unchanged, and `truncateTo` still
  refuses.
- Lifetime, removal, teardown failures and backup exclusions are specified in
  [TXN-615](../../spec/TXN-transactions-durability.md#txn-615--in-memory-state-lives-with-the-open-database)
  (captain decisions of 20 November 2026).
- Why tests missed it: no test committed through a real `IN_MEMORY` resource and read the value
  back; the only RAM tests drove `RAMStorage` directly with an uber page, or pinned the
  shared-instance behaviour itself (`DurableBatchReadFailureReleaseTest` asserted `assertSame` on a
  re-read page). The class carried a `// FIXME: broken` comment.
- Regression test: `RAMStorageCommittedRevisionTest` (JSON and XML: reopened writer, same-session
  reader, new session, later commit leaves earlier revisions unchanged, revert, a 1,500-commit
  history read back through a new session, recreated resource starts empty). All eight cases of its
  first four methods failed on main (`expected <1> but was <0>` and `moveTo` false); all pass after
  the fix.
  `RAMStorageContractTest` pins the storage-level contract, including a key-value leaf page that the
  writer closes after the write and that still reads back intact under the empty, Deflate and LZ4
  pipelines. The lifetime suites are mapped to acceptance criteria in
  [TXN-615](../../spec/TXN-transactions-durability.md#txn-615--in-memory-state-lives-with-the-open-database).
  In the initial lifetime regression run, 22 of 28 cases failed before the lifetime change;
  the other 6 guarded behaviour the code already had. Run with
  `./gradlew :sirix-core:test --tests 'io.sirix.io.ram.*' --tests '*InMemory*'`. The mutation
  results are in the PR notes.
- Found while implementing TXN-615: a plain recursive delete of a resource directory can remove
  every file (settings included) and then fail on the directory itself, when its parent is not
  writable. The leftover then has no settings proving it was in-memory state, so it has to be
  treated as persistent and shows up again. The failed-cleanup acceptance criterion (AC12) caught
  it. Retirement now renames the directory to a reserved tombstone name first, which is atomic:
  either the name is free at once, or nothing changed and the proof is intact.
- Lesson: a storage backend's correctness is defined by the round trip (commit, reopen, read the
  value) through the public API, not by its own methods returning without error. A "shared pages"
  exception in an ownership contract needs the *writer's* side checked too. Cleanup that must stay
  retryable has to keep its own proof until the last step; see [LESSONS.md](../LESSONS.md).

## Review follow-up: retirement and read scratch

- Symptoms and triggers: a valid 220-character resource name could not be renamed on a filesystem
  with 255-byte filenames; a successful rename followed by permission-denied deletion incorrectly
  let final close succeed; a partially deleted tombstone without settings blocked backup/restore;
  each RAM page read through the byte-array decoder allocated automatic native staging memory.
- Root causes: retirement tied garbage names to user-name length and represented both name release
  and footprint removal as one boolean; tool preflight treated retired garbage as a resource;
  RAM decoding used the cold byte-array bridge rather than the allocator scratch boundary.
- Fix: use bounded `$retired-<uuid>` names and return the actual remaining path. Creation can
  proceed after a rename, cleanup reports the tombstone, and only a failed derived rename keeps
  its source proof. Skip tombstones at shared tool preflight and backup reporting. Decode RAM
  payloads inside a READ scratch lease, retaining the byte-handler fallback and page ownership.
- Why tests missed it: ordinary names, failed renames and complete settings did not exercise these
  independent failure boundaries. Value assertions alone also cannot expose native staging churn.
- Regression oracles: TXN-615 clauses 3/5 require removal and empty recreation even with long names;
  clause 7 requires a failed final close after successful rename and failed deletion, with admission
  released and cleanup retryable; clause 10 requires persistent content to survive backup/restore
  despite incomplete tombstones. Tests are named in TXN-615's review-regressions entry. Existing
  RAMStorageContractTest and RAMStorageCommittedRevisionTest retain their ownership/value oracles
  unchanged. Focused verification is recorded by the review phase; mutation and broader budget
  gates remain with the outer executor.
- Lesson: distinguish name release from garbage deletion, probe only actual resources, and use the
  allocator's bounded scratch for hot decoding; see [LESSONS.md](../LESSONS.md).

## Review follow-up: failed cleanup admission and proof

- Trigger and symptoms: forced database removal after denied tombstone deletion left a closed
  instance registered and its lock held; source removal or hidden-name creation could release
  settings before retiring a derived sibling; a denied initial directory listing exposed old RAM
  resources once permissions returned; retaining source settings on derived-rename failure also
  retained decoded source caches.
- Root causes: forced close bypassed the ended-instance ownership-release boundary; derived
  ordering and cache discard were enforced only on some retirement paths; a failed scan was logged
  without preserving the fact that resource admission had not yet been established.
- Fix: forced removal uses the same ownership release as final-handle close when the instance has
  ended. Shared retirement retires the derived sibling before releasing its source name. Shared
  state discard clears caches and RAM even while proof is retained. A pending-scan flag gates the
  four resource-admission paths until scanning succeeds, without rescanning live state thereafter.
- Regression oracles: TXN-615 clauses 7/8 and AC9/AC12 require reopen and empty recreation after
  failed forced removal; clause 6 requires old RAM names to remain absent on each admission path
  after an unreadable initial scan. Clauses 7/8 require a derived original name to be retired before
  source-name reuse, even after a previous failed rename. Tests use public API results, committed
  content, POSIX permissions and persisted directory fixtures; the latter model the reserved derived
  namespace and its readable source settings, not implementation text.
- Why tests missed it: tests exercised final-handle close and successful initial scans, without
  crossing forced removal, source/derived deletion order or scan-permission transitions.
- Test limitation: no new public-only test directly measures decoded-cache retention while source
  settings remain. That guarantee is checked at the shared discard boundary; internal counters or
  cache object assertions would not satisfy the requested public-behavior constraint. Mutation and
  broader verification remain assigned to the outer executor.
- Lesson: every lifecycle exit must release ended ownership, preserve proof until its dependents
  retire, and treat incomplete discovery as pending admission; see [LESSONS.md](../LESSONS.md).

## Review follow-up: retained sessions and close retries

- Symptoms and triggers: retaining closed sessions retained the RAM store's complete payload and
  revision arrays after its registry entry was removed. A failed directory scan or unreadable
  settings prevented cache release. Failed renames left discarded names addressable by ID.
  Repeated close returned without retrying leftover deletion.
- Root causes: registry lifetime was mistaken for object reachability; cache release depended on
  rediscovering identities from disk; name/ID removal depended on rename success; ended handles
  discarded the authority needed to finish their own cleanup.
- Fix: remove each RAM store from the registry before clearing its arrays, after users quiesce.
  Remember known RAM resource configurations at session admission, including derived storage;
  final teardown clears their caches independently of discovery. Shared logical discard removes
  name/ID mappings even while settings remain. Remember exact pending cleanup paths and original
  proof UUIDs, and retry only those paths under lifecycle admission. Retry is disk-only, checks
  incarnation identity, preserves derived-before-source ordering, and never clears a newer store.
- Regression oracles: TXN-615 AC13's existing small-heap child now has additional modes that keep
  closed sessions reachable through the entire workload, after both resource removal and final
  close (`closedSessionsDoNotRetainCommittedHistories`). Clauses 5/7/8 require both metadata lookup
  directions to forget hidden resources (`discardedHiddenResourcesHaveNoNameOrId`). Clause 7
  requires repeated close to report still-denied cleanup and succeed after permissions return,
  for failed renames, failed tombstone deletion and failed directory listing. The newer-instance
  regression commits and reads a replacement value while an old handle retries close.
- Why tests missed it: the memory child retained open databases as its control, but did not retain
  closed sessions; permission tests reopened the database instead of retrying the ended handle.
- Test limitation: the failed-scan regression verifies known footprint cleanup and the ended-handle
  behavior publicly. It does not directly measure native cache retention for missing directories
  or unreadable settings; those release paths are reviewed against the remembered identity map.
  Mutation and broader budget validation remain assigned to the outer executor.
- Lesson: remove registry ownership and retained payloads together; separate logical state release
  from filesystem discovery and keep retry authority bound to old identities, not reusable names.

## Review follow-up: publication and inaccessible known paths

- Symptoms and triggers: root writing appended a timestamp-index entry before catalogue persistence
  could fail. Retrying the same revision appended another entry, so an in-between timestamp selected
  the failed attempt and an exact retry timestamp returned a nonexistent revision. Separately, a
  resources directory with read permission but no search permission could be listed while child
  existence checks returned unknown; final close silently dropped those known cleanup targets.
- Root causes: physical root serialization was mistaken for commit publication, and `Files.exists`
  was mistaken for a two-valued absence test at the cleanup boundary.
- Fix: stage revision locators separately, prefer committed records on reads, and append the index
  and committed locator once at uber-beacon publication. Preserve physical reads of provisional
  roots for storage clients, without exposing them through timestamp lookup. Clear both record
  arrays on store discard. Written root locators remain available throughout publication so a
  physical reader crossing a committed-array resize never loses its fallback locator. Retain known
  cleanup paths unless `Files.notExists` proves absence.
- Regression oracles: TXN-503 and VER-202 require failed commits to contribute no timestamp entry.
  `RAMStorageCommittedRevisionTest.failedCommitDoesNotPublishATimestampEntry` uses a directory at
  the revision catalogue's output path to make public JSON/XML commits fail after root writing;
  rollback/retry checks timestamps 100/200/250/300/400, historical values, history and absent revision
  3. No new injection seam or implementation-shape assertion is needed. TXN-615 clause 7 requires
  denied traversal to make close fail and repeated close to finish once permissions return;
  `failedTraversalRemainsPendingUntilRepeatedClose` exercises POSIX read-without-search permission.
- Why tests missed it: successful histories never revisited an unacknowledged revision number,
  and existing directory permission tests denied listing or writing rather than search permission.
- Lesson: publish commit metadata at the commit boundary, and distinguish an unobservable path
  from a path proven absent. Mutation and broader gates remain assigned to the outer executor.

## Review follow-up: remembered ownership on removal

- Trigger and symptom: commit RAM history, retain the closed session, make persisted settings
  unreadable, remove the resource and recreate its name. Removal's disk-only classification selected
  persistent deletion, skipping RAM-store discard. Recreation replaced the registry entry while the
  retained session still held the old payloads. Derived removal used the same incomplete dispatch.
- Root cause: session registration remembered ownership, but resource removal did not consult that
  proof. The final-close fix had not covered the shared source/derived removal boundary.
- Fix: share remembered-source recognition between registration and removal; remembered resources
  and remembered-source siblings use retirement even if settings reads fail. Unremembered paths
  still require readable settings. Preserve unknown derived paths as proof dependencies; positively
  absent paths finish retirement without blocking source-name reuse. No new ownership state is added.
- Regression oracles: TXN-615 clauses 6/7/8 require failed-rename proof retention and empty name reuse
  for JSON/XML, including derived siblings (`knownOwnershipSurvivesUnreadableSettings`). AC13's
  existing heap bound now covers corrupt settings, repeated removal/recreation of one name and
  retained closed sessions (`unreadableSettingsRemovalReleasesMemory`). Corrupting the resource's
  persisted settings is an external-boundary fault, not an implementation-text assertion. The
  missing-directory test guards the newly reachable retirement path's name-reuse behavior.
- Why tests missed it: failed-discovery checks covered final close, and the removal memory child
  kept settings readable. Neither combined removal, unreadable settings and retained sessions.
- Lesson: ownership already established by the open instance must reach every state-discard path;
  rereading persisted proof must not downgrade a known RAM resource to persistent deletion.
  Mutation and broader gates remain assigned to the outer executor.

## CI follow-up: executable contracts and mutation selection

The changed-source lane exposed missing contract evidence rather than another committed-value
failure. Cleanup retry assertions had reopened the database, which could perform cleanup itself;
they did not prove repeated close completed the old instance's pending work. Storage discard tests
had not retained the provider, unknown-key cases missed the allocated-array boundary, and revision
read-back alone could not detect quadratic metadata allocation. The CLI omission notice had no
output test. The byte-handler matrix covered leaves but not non-leaf roots, missing the legacy
Deflate metadata stream path. Some integration contracts also lacked explicit mutation-lane associations.

TXN-615 clauses 3, 6, 7, 8 and 10 remain the oracles. The added tests exercise direct and facade close,
old-handle retry beside a new live instance, retained providers and neighbouring resources,
unknown-key boundaries, and quiet/verbose backup output. The revision append allocation budget
allows 8 KiB per small revision, including staging and publication, and rejects repeated copies of
full history without a timing assertion. `InMemoryResourcesTest` checks nameless/unknown paths and
settings-free tombstones through the classifier's public interface. Explicit lane associations
retain the cross-class ownership and reader contracts.

The synthetic revision-root reference also redundantly carried a checksum copied from the same
private immutable payload store. It supplied no independent integrity boundary and was removed;
`read(PageReference, ...)` still verifies checksums supplied by a caller. Revision-file metadata
still exposes the recorded payload hash.

Verification commands: focused `:sirix-core:test` for `RAMStorageContractTest`,
`InMemoryResourcesTest` and `DatabasesInMemoryLifetimeTest.repeatedCloseFinishesOnlyTheEndedInstancesCleanup`;
`:sirix-kotlin-cli:test --tests '*BackupTest'`; and the bounded changed-source PIT lane.

Local repair verification passed 26 focused core cases, four CLI output cases, the eight query
cancellation cases both with and without coverage, and 326 core work-budget cases. After extending
the root/leaf pipeline matrix, focused PIT killed all 43 sampled changed-line mutants in RAM storage
and resource classification. CLI PIT resolved all five changed-line mutants: three killed, two
compiler-generated null checks accepted as equivalent because the final record guarantees a
nonnull list. The full core mutation run passed its baseline but exceeded its 1,200-second module
share; its partial output recorded 24 killed changed-line mutants, including the direct/facade
cleanup retries. That run is incomplete, not a green full-lane result. The original query CI failure
was temporary-directory deletion after successful assertions and did not reproduce locally.
CodeFactor's diagnostic page returned HTTP 403, leaving its findings unclassified.

## CI follow-up: mutation tooling fixture isolation

- Fix / issue: PR 1469's `Python mutation tooling` failures at
  `ad0d86119e9ee6889d6912bd0e5d28b50e66b40d`.
- Symptom: two reporter tests failed after adding valid RAM associations and dispositions,
  preventing the coverage report and dependent mutation job from running.
- Trigger: run `python3 -B -m unittest discover -s .github/scripts/tests -p 'test_pr_mutation*.py'`
  with the new repository configuration; both reported failures reproduce locally.
- Root cause: the empty-selection CLI fixture set only its output directory, so both direct
  `main()` and module-entrypoint calls read the real repository's dispositions without its
  merge-base manifest. The repository association test required equality with the four former
  release-contract pairs, incorrectly forbidding valid additions under CONV-204.
- Required behavior and oracle: an empty fixture with no dispositions or associations must
  exit zero; valid additional associations must be accepted while every former release-contract
  pair remains present. Both CLI entry forms now receive the fixture's explicit root. The
  association test validates the entire file through its consumer and checks membership for
  each required pair, allowing additions both for other classes and for an existing class.
  Missing required pairs and invalid ownership/schema remain failures; no production validation
  or configuration is removed.
- Why tests missed it: the repository formerly had no dispositions and exactly the four
  association pairs, so the accidental configuration dependencies agreed with the fixtures.
- Regression tests: `CommandLineTest.test_run_cli_skips_empty_selection_and_module_entrypoint_exits_zero`
  and `AssociationTest.test_repository_files_are_valid_and_keep_the_former_contract_pairs`.
  The initial run failed both tests; the repaired tests exercise the real CLI and normalized
  association consumer rather than implementation text.
- Verification: all 156 `test_pr_mutation*.py` tests pass under branch coverage; coverage is
  99%, exceeding the CI job's 85% floor, and XML generation succeeds. Reports are under
  `build/ci-diagnostics/python-tooling/`. No production logic changed in this repair.
  CodeFactor still reports three issues but supplies no GitHub annotations; its diagnostic
  page returns HTTP 403, so that separate finding remains unresolved.
- Lesson: [isolate fixtures and preserve semantic association contracts](../LESSONS.md).

## CI follow-up: isolate forked lifetime contracts from PIT

- Fix / issue: PR 1469, PIT module timeout and SonarCloud reliability findings.
- Symptom: the core changed-source mutation run exceeded its 1,200-second share; SonarCloud
  reported S3077 on the four volatile array fields in `RAMStorage.Store`.
- Trigger: PIT selected `DatabasesInMemoryLifetimeTest` by its production-name prefix and
  repeatedly ran its abrupt-exit and bounded-heap child processes for parent-JVM mutants.
- Root cause: a child JVM loads unmutated classes, so repeated child work cannot detect a
  mutation in the parent. The tests mixed in-process contracts with process-isolated contracts.
  The array warning also did not account for acquire/release slot access, locked replacement,
  and the beacon's immutable snapshot publication.
- Required behavior: every existing lifetime assertion continues to run in ordinary CI;
  process-isolated tests do not consume the bounded parent-JVM mutation lane. Keep RAM's
  publication semantics and direct array reads. All six child-launching tests, both child main
  classes, and their exclusive helpers move unchanged to `InMemoryLifetimeForkedJvmTest`;
  in-process lifecycle tests remain selected. Four field-scoped S3077 suppressions document
  the publication discipline without adding atomic-array indirection.
- Why tests missed it: ordinary passing tests establish lifecycle behavior but say nothing
  about whether their child processes can observe PIT's parent-only instrumentation. Earlier
  focused mutation runs did not establish full-lane completion.
- Regression evidence: execute both lifetime suites unchanged, then the actual bounded PR
  lane; record any out-of-lane memory kills with the CONV-203 counterfactual helper.
- Extraction verification: both lifetime suites pass all 46 cases (28 in-process, 18 forked),
  without skips. The forked suite takes 135 seconds and the in-process suite 5.5 seconds locally.
  Evidence is under `build/ci-lifetime-split/`; the unchanged full lane is run with
  `python3 .github/scripts/pr_mutation.py run --output build/ci-lifetime-split/pr-mutation
  --seconds 1320 --blocking`.
- The first full core run completed in 17m20s, within its 1,200-second share. It detected
  52 of 53 changed-line mutants and reported no changed-line survivors. The one uncovered
  mutant identified pending initial discovery as a contract only the extracted abrupt-exit test
  exercised. The spec already requires refusal while discovery remains pending and recovery
  before every admission path; `pendingInitialScanBlocksAdmissionUntilDiscoverySucceeds`
  now checks those behaviors in-process, with a committed persistent neighbour as its oracle,
  across JSON/XML and existence, listing, session opening and resource creation. Skipping
  discovery would admit the resource instead of raising the required error. The unchanged
  forked test retains the abrupt-exit/RAM-leftover assertions. No uncovered mutant is dispositioned.
- Final verification: the 36 in-process cases pass without skips, including all eight pending-scan
  admissions; together with the unchanged 18 forked cases, all 54 lifetime cases pass. The final
  bounded lane exits zero: core completes in 17m14s against its 1,200-second share and detects
  all 53 changed-line mutants, with no survivors or uncovered cases. CLI completes with three
  changed-line kills and its two existing equivalence dispositions. No new out-of-lane
  dispositions are needed; the sampled store-clearing and cleanup-retry mutants are detected
  in the lane. Reports and commands are under `build/ci-lifetime-split/pr-mutation-final/`.
  SonarCloud must analyze the updated fields on the next CI revision; no remote scan is claimed.
- Lesson: [match mutation selection to the process that observes mutations](../LESSONS.md).

## CI follow-up: cleanup retry must respect independent ownership

- Fix / issue: PR 1469, failed PIT job
  [137202797551](https://github.com/sirixdb/sirix/actions/runs/45020284859/job/137202797551).
- Symptom: the complete bounded report has one changed-line survivor: negating the directory
  absence check in `Databases.retryInMemoryCleanup` bypasses OS lock acquisition for an existing
  database. All other changed-line mutants were detected or already dispositioned.
- Root cause: retry coverage exercised no owner and a newer in-process owner, but never an
  independent OS owner. Both original directories and tombstones were removable without the
  tests noticing that ownership had been bypassed. Production already acquires the lock correctly;
  this repair strengthens its executable contract without changing runtime behavior.
- Required behavior and independent oracle: TXN-101 and TXN-615 clause 7 / AC12 require refusal
  while another process owns the database. `DatabaseOwnershipTest.repeatedCloseRespectsAnotherProcessesOwnership`
  creates committed JSON/XML content, forces final-close cleanup to leave either an original
  directory or a tombstone, restores permissions and uses the existing `lock-only` child to hold
  the real OS lock. Parent close must throw `SirixDatabaseLockException` and preserve the footprint;
  after the child releases ownership, close must delete it and a further close must succeed.
- Why tests missed it: successful cleanup and in-process generation isolation do not establish
  cross-process exclusion. This child only holds the lock; the parent executes the cleanup under
  test and therefore observes PIT instrumentation, unlike child-only storage/memory contracts.
- Regression evidence: the targeted contract, an injected lock-bypass counterfactual and the
  bounded lane are recorded under `build/ci-pit-fix/`.
  The four JSON/XML × original/tombstone cases pass without skips. The CONV-203
  counterfactual helper reports baseline exit 0 and injected exit 1: all four cases reject
  the lock bypass because the required `SirixDatabaseLockException` is absent. It restores
  `Databases.java` byte-identically (SHA-256
  `42f97c91c8cb6d158cf1cbe8ae4a9135c2e73c9050ee9a3d345231430df862e1`).
  The unchanged automatic PIT selection also marks the exact reported mutant (instruction
  index 50, `NegateConditionalsMutator` in `retryInMemoryCleanup`) KILLED by the new contract's
  JSON/tombstone case. No association, disposition, sampling limit or deadline was changed.
  The full local run is nevertheless incomplete: core exceeded its unchanged 1,200-second
  module share. The partial XML and extracted identity in `build/ci-pit-fix/reported-mutant.json`
  retain the kill evidence; they do not establish a clean full-lane verdict. CI must rerun the
  bounded lane on the updated test. CLI completes with three changed-line kills, its two existing
  accepted equivalence dispositions and no failures. The overall bounded command exits 1 solely
  because core timed out. Compilation and Spotless apply succeed; `git diff --check` is clean.
- Lesson: exercise independent ownership at the retry boundary for both cleanup representations;
  [extend the existing RAM ownership lesson](../LESSONS.md).
