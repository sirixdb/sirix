# Cache publication and writable HOT copies need different representations

- The compact-file reader suite found two crossed contracts. Its offset
  oracle still expected an allocated int array for the packed image.
  The test that exercised writable copies stopped at that stale assertion.
  The cache-charge test also counted the slot bytes without the on-frame
  directory.
- An independent read image is useful in the complete cache, but the older
  `copyForRead` API also provides writable capacity. Reusing it to preserve
  the packed body would make those copies reject mutation.
- The complete cache now calls a read-only copy operation. The existing
  copy API restores ordinary slots and a writable directory. Compact
  images keep their direct directory, and the cache charges both the slots
  and the retained wire metadata.
- `HOTCompactFragmentReadTest.compactImageRejectsMutationAndCopiesRestoreWritableCapacity`
  checks the compact source, its writable copies and their independent
  mutations. The adjacent cache-charge test asserts the directory's extra
  retained bytes explicitly.
- See [the copy-contract lesson](../LESSONS.md#distinguish-writable-copies-from-cache-images).
