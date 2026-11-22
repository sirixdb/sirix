# Bug: IN_MEMORY commits disappear before timestamp readback

- Fix / issue: test finding T-IN-MEMORY on `fm/sirix-commit-ts-micros`; VER-101/102/201.
- Symptom: committing a JSON value returns successfully but reports head 0, and opening revision 1
  fails before its timestamp or content can be read.
- Trigger: create an IN_MEMORY resource, open a writer, insert a string and commit a custom
  microsecond timestamp; close and reopen writers and resource sessions.
- Root cause: RAM storage bypassed page serialization, leaving the uber page in bootstrap state,
  never registering revision roots, and retaining transaction-owned page objects which cleanup
  clears or closes. Independently, releasing the last user session closed the shared resource
  session, discarding the bootstrap and all later RAM data because no backing file exists.
  RAM now retains encoded page snapshots and revision-root keys, decodes independently owned pages
  through the existing reader, and publishes the actual uber-page key. Its shared resource session
  remains owned by the database until database close. Cached record-page encodings are reused;
  structural pages use the configured segment or stream compression interface.
- Why tests missed it: the timestamp conformance fixture selected FILE_CHANNEL only. RAM's existing
  regression checked refusal of unsupported truncation without committing or reading a value.
  Timestamp tests could pass while an entire supported backend never exposed a user revision.
- Regression test: `RAMStorageCommitTest` derives its head/content oracle from VER-101/102 and uses
  literal VER-201 instants one microsecond apart plus independently specified epoch-microsecond
  history values. It checks reopened writers, immutable historical content, exact time lookup,
  and a new resource session, with empty, LZ4 and stream-only Deflate pipelines. The original
  focused Gradle run failed with `expected: <1> but was: <0>` (exit 1). Registering roots and clearing
  bootstrap state alone still failed; retaining the shared session then exposed missing document
  content (exit 1), proving that metadata and lifetime fixes alone were insufficient.
- Mutation verification: the final focused PIT run completed with exit 0: 35 of 40 mutants killed,
  four survivors and one uncovered mutant, all five on unchanged lines (`truncate`, `createReader`,
  and `getByteHandler`). Every mutant on a changed RAM code line was killed. A separate single-site
  inversion of the IN_MEMORY session-retention predicate made all three compression fixtures fail
  at head 0 (Gradle exit 1); the original source was restored in `finally`. Mutation associations
  select the regression for both changed production classes. The storage-level test additionally
  executes the fluent writer contract, checks valid published page keys, verifies snapshot ownership
  after buffer reuse and original-page mutation, and checks absent-revision diagnostics after full
  truncation. SDK reads invalidate this database's page caches and check named JSON field content.
- Final verification: Gradle exit 0; 334 selected core cases across 37 classes passed with no skips,
  failures or errors. This includes all five RAM cases, the prescribed core work-budget selectors,
  and `DatabaseOwnershipTest.handlesShareWriterAndLatestViewUntilLastClose` plus
  `closingLastUserSessionAllowsResourceRemovalAndRecreation`. The restored session source hash
  matches its pre-mutation hash. No complete repository suite or other pipeline phase was run.
- Lesson: [In-memory publication owns its snapshots and lifetime](../LESSONS.md#in-memory-publication).

Focused verification:

```sh
./gradlew :sirix-core:test --tests 'io.sirix.io.ram.*'
```

Local runs bound the Gradle/test heaps and use a temporary init script to skip unrelated attached
startup suites and keep test temporary data inside the worktree. No linter, formatter or static
analysis is run. Closing the database still ends the in-memory data lifetime; this fix adds no
cross-process persistence or rollback support.
