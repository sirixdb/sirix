# Bug: lifecycle regression retained the old RAM beacon identity oracle

- Fix / issue: lifecycle-fences integration with the RAM committed-revision fix (#1463).
- Symptom: `RAMStorageTruncateToTest.publishedRootReferenceReadsTheLatestRootByItsStorageKey`
  failed with expected key 8192 and actual key 4096, before checking revision contents.
- Trigger: run the lifecycle regression partition after rebasing onto the serialized RAM backend.
- Root cause: the older regression required writer/reader beacon-key equality and shared page
  object identity. The merged RAM contract stores owned encoded bytes and returns caller-owned
  decoded pages, using the existing redundant beacon reader. Either valid beacon must expose the
  committed revision; selecting the same physical copy or returning the writer's object is not
  required. See [TXN-615](../../spec/TXN-transactions-durability.md#txn-615--in-memory-state-lives-with-the-open-database)
  and [the RAM ownership fix](ram-storage-committed-revision-loss.md).
- Why tests missed it: lifecycle and RAM changes had been validated separately; the integrated
  regression still encoded the earlier RAM representation rather than the publication contract.
- Regression test: the same test stages revision count 1, publishes revision count 2, and checks
  count 2 through the embedded read root, the returned reader reference, and the writer reference.
  It uses real scratch storage. This rejects a stale root or a key resolving to the staged page
  without requiring shared mutable objects. Run `:sirix-core:test --tests
  'io.sirix.io.ram.RAMStorageTruncateToTest'`; the original assertion failed in this test phase,
  and the repaired class passed in the subsequent core work-budget run.
- Lesson: after integrating a storage ownership change, rederive storage test oracles from
  committed content and verify every supported reference, rather than retaining representation
  assumptions. See [lessons](../LESSONS.md).
