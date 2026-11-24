# Bug: an automatic arena per compact HOT-leaf decode

- Fix / issue: this pull request; the defect was found while fixing the HOT leaf-codec gate's
  allocation fixture (PR #1454's evidence round).
- Symptom: every compact HOT-leaf fragment decode allocated Java heap for the *ownership* of its
  native image. `GcOwnedNativeMemory.allocate` is `Arena.ofAuto().allocate(used, 8)`, so each decode
  created an arena: a 24-byte `jdk.internal.foreign.ArenaImpl` whenever escape analysis did not
  remove it, plus a session and a `Cleaner` registration. Measured on the `compactMaterialize`
  benchmark: 11 of 12 JMH forks at 994.5 B/op and one at 1018.4 — the arena object survives or not
  per compilation, so two legs of *identical* code allocate differently — and about 209 B/op for the
  arena itself, with ±1 B/op of jitter that follows GC timing, because the `Cleaner` runs when the
  collector does. A fragment decode is per element on a read path (`readHOTLeafFragments` is the
  default for every HOT fragment chain whose byte handler supports memory segments), where
  `AGENTS.md` forbids per-element allocation.
- Trigger: any read of a versioned HOT projection slot that walks a fragment chain. No input is
  special; the cost is on every decode.
- Root cause: the image needs native storage with a lifetime independent of the borrowed read
  buffer, and the original change (#1289) gave it one *automatic arena per image* rather than a
  pooled block, so the garbage collector became the owner of memory whose owner was already known —
  the decoded `HOTLeafPage`, which releases its frame exactly once from `releaseMemory()`. Paying a
  Java allocation and GC-timed bookkeeping per element for an ownership decision the page already
  makes is the defect.
- Why tests missed it: no test asserted anything about the storage acquisition. The suite checked
  the decode's results and, in two places, that the image's segment was exactly its packed size —
  which the arena satisfied — but nothing bounded the allocations of the path. The work-budget
  suites count engine work (reads, slots, records), not heap bytes, and the only allocation budgets
  were on other paths. A JMH allocation profile found it, and only because an A/A comparison of two
  legs of identical code disagreed.
- Regression test:
  `:sirix-core:dictionaryReadViewAllocationTest --tests 'io.sirix.budget.CompactImageStorageAllocationBudgetTest'`
  bounds a warmed lease/release cycle at 0 bytes (100,000 rounds, smallest of three legs) and keeps
  the automatic arena as the positive control, which allocates; a third case ties the bound to the
  decode by asserting that a compact fragment decode leases exactly one block and returns it on
  close. `io.sirix.page.CompactImageOwnershipTest` covers the lifetime: one block per image, a
  guarded image keeps its block until the last guard goes, a closed image whose block has been
  re-leased can no longer certify an optimistic reader's stamp (with the pool's poison-on-release
  switch on, so a stale read is certain rather than lucky), and the pool's refusal still answers the
  read. `io.sirix.cache.CompactImageStoreTest` covers the pool: the class table holds every legal
  image size with under an eighth of slack, a released block is leased again rather than carved, a
  release of an unleased block is refused, and eight threads never receive the same block twice.
  On the parent commit the new tests fail (no lease exists to count); the two existing exact-size
  assertions in `HOTCompactFragmentReadTest` and `HOTCompactFragmentBatchReadTest` are now bounds on
  the slack instead, because a pooled block is its image rounded up to a class.
- Lesson: when a decoder knows exactly when its memory dies, it must own that memory explicitly; an
  automatic arena per element turns a known lifetime into GC work and makes the path's allocation
  depend on whether escape analysis fires. See
  [the lesson](../LESSONS.md) collected from `docs/lessons.d/compact-image-decode-arena.md`
  (see the [fragment convention](../../changelog.d/README.md)).

## Was the decrypted-page reclaim the same cause? No.

The flake investigation of `DerivedIndexMaintainerTest`
(`firstmate/data/sirix-derived-index-maintainer-flake/diagnosis.md`) listed, as one of three
hypotheses, that a GC-reclaimed automatically managed copy of a decrypted page could be freed while
the decoder still read it. That hypothesis is not this defect, and its own investigation retracted
it:

- Different site. The decrypted-page copy is `AbstractReader.nativeCopyOf`, one arena per *page* on
  the byte-array branch; the defect here is one arena per *decoded fragment image*. Nothing is shared
  but the `Arena.ofAuto()` idiom.
- The shape is not mechanically possible. An automatic arena frees its region when the segment and
  every segment derived from it become unreachable. The decoder holds the segment across the whole
  decode (it is the `BytesIn` source), so there is no point at which a read happens through a
  segment that is unreachable.
- The evidence pointed elsewhere. The investigation's write-side and read-side dumps showed
  `frameEqualsDisk=true`, a writer hash covering exactly those bytes, and the *same* 719-byte
  ciphertext decrypting to entropy once and to a valid `HOT_LEAF_PAGE` twice, in one thread,
  microseconds apart. That places the fault in the decrypt computation, not in the ownership of any
  buffer. It remains open there and is not affected by this change.

What this change does touch is the general rule the flake round was probing: an image whose storage
can be recycled needs a reader protocol that detects the recycle. The compact image has one — the
page's optimistic stamp — and `releaseMemory()` publishes `closed` and an odd stamp binding before
it calls the releaser, so the block cannot reach a second owner while a reader can still certify
reads of the first. That ordering is now an asserted invariant rather than an incidental one.
