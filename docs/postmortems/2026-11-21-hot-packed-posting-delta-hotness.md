# Bug: packed posting frames made hot posting chunks look cold

- Fix / issue: S1 leaf codec change (FMT-210..217). `HOTIndexWriter.loadChunkView`
  and `ChunkView.readBase` now use `HOTLeafPage.ordinaryValueLength` and
  `copyOrdinaryValueInto`.
- Symptom: after a commit, a valid-time index lost postings. `HOTIndexWriter.remove`
  returned false for a posting that had been indexed ("the posting to remove must
  be registered"). Later reads missed the posting or kept a removed one. This is
  silent data loss in a CAS or VALIDTIME index under posting deltas.
- Trigger: `HOTFrontierSplitHalfCanonicalTest.frontierStreamAcrossDeltaGeometries`
  at hot bytes 512 and `HOTValidTimeCorrectionStreamTest.correctionStreamAcrossDeltaGeometries`
  at hot bytes 256, both with fold bound 64, under every versioning type. A chunk
  whose base posting reached the hot threshold received deltas. The commit then
  wrote the leaf as a packed body whose base posting is a frame-of-reference
  (FMT-214), and a later add or remove on the same chunk followed.
- Root cause: the hot/cold rule ("a cold base cannot have live deltas; only a
  fold changes the base") compared the stored byte length of the base posting
  (`HOTLeafPage.refLength`) with `hotChunkBytes`. The packed writer stores a hot
  posting as a frame of about 5 + (n - 1) bytes in place of 2 + 8n bytes. So
  after a commit, the same logical chunk read as cold, its live deltas were
  ignored, and the operation took the direct path against the base alone. A
  remove of a delta-added posting found nothing, and an add bypassed the
  deltas. The invariant that broke: decisions about a posting must depend on
  the posting, not on its stored representation. The decoder (`readBase`)
  copied the encoded bytes for the same reason.
- Why tests missed it: the leaf-codec tests checked the framing and every read
  path, but not the writer's size-dependent decisions on a re-read packed
  leaf. The PR's verification ran a targeted core subset (463 tests) that
  omitted these two main-branch stream tests. Only the full `:sirix-core:test`
  task contains them, and that run was skipped once behind failing allocation
  forks and timed out once.
- Regression test: the two stream tests above, run with `./gradlew :sirix-core:test
  --tests '*HOTFrontierSplitHalfCanonicalTest' --tests '*HOTValidTimeCorrectionStreamTest'`.
  Before the fix: 8 failures at fold bound 64. The same tree with packed bodies
  disabled passes, and main passes. After the fix: 32/32 and 25/25.
- Lesson: when a value gains a second stored representation, audit every
  consumer that reads its length or bytes for a decision, not only its
  decoders. See [LESSONS.md](../LESSONS.md#make-size-decisions-on-the-logical-value).
