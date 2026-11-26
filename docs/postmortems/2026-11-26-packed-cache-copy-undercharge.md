# Bug: a cached packed HOT leaf was charged its packed bytes, not the frame it retains

- Fix / issue: this change on `fm/sirix-fp-s1-leaf-codec`; review finding review-4 on
  [PR 1454](https://github.com/sirixdb/sirix/pull/1454).
- Symptom: `HOTLeafPage.getActualMemorySize` reported the packed body's size for a leaf whose
  `slotMemory` is a slice of a larger allocation the page owns and releases. For 128 seven-byte
  projection keys with distinct 255-byte values the packed body is 34,048 bytes — directory
  `128 × 2` plus `128 × (1 + 7 + 1 + 255)` slot bytes — and `copyForCache` asks the allocator for
  exactly that, so `ShardedPageCache` was charged 34,048. `FrameSlotAllocator.allocate` serves the
  request from its 65,536-byte size class and returns the whole slot
  (`region.asSlice(offset, slotSize).reinterpret(slotSize, …)`), which the page's releaser frees in
  full. 31,488 retained native bytes per leaf therefore sat outside every budget: the cache admitted
  roughly twice the leaves its configured weight allowed, and eviction returned memory nothing had
  counted. Expected: the charge is what the page retains.
- Trigger: any read that publishes a packed leaf into the page cache —
  `NodeStorageEngineReader` taking `rawHead.copyForCache()` — with a packed body whose size is not
  already a size-class boundary. The worse the rounding, the larger the gap; a 34,048-byte body sits
  near the middle of the 32 KiB–64 KiB step.
- Root cause: the page's reported size answered "how many bytes do I read through" instead of "how
  many bytes do I retain", and for a packed body those differ by whatever the allocator rounded up.
  The two are only the same when the page's memory IS its allocation. The page already records which
  case it is in: `stampBaseSegment` is documented as "the segment as returned by the allocator", is
  set at every site that takes ownership of a frame (`copyInternal` preserving a packed body,
  `PageKind.retainPackedHOTLeaf` for both its transferred decompression frame and its copied
  allocator frame, and `deserializeHOTLeaf`'s zero-copy slice of a transferred frame), and is left
  null exactly where the page owns an exact-size GC-managed image. `publishSlotMemory` even clears it
  with the comment "the new segment IS the allocation". `getActualMemorySize` simply never read it,
  so the field that already modelled the invariant was not consulted by the one method that needed
  it.
- Sibling sites: the same read fixes all of them, because they all maintain the same record. The
  finding named `HOTLeafPage.copyInternal` (packed cache-copy allocation),
  `NodeStorageEngineReader` (the production cache-copy caller), and `PageKind`'s transferred
  decompression frame and copied allocator frame. `deserializeHOTLeaf`'s ordinary zero-copy branch
  was undercharging identically and was not named: it keeps `slotMemory` as a mid-buffer slice of a
  decompression frame whose ownership it transfers, so an ordinary encoded read was charged its slot
  bytes and retained the whole frame. Compact images keep exact accounting: `retainPackedHOTLeaf`'s
  compact branch passes `null`, and the GC-owned path reports its packed bytes as before.
- Why tests missed it: the cache-charge test in this area covers the compact image
  (`aCachedCompactImageIsChargedItsPackedBytesPlusItsFixedPerPageHeap`), which is the one packed
  shape that genuinely owns exact-size memory, so it agreed with the code. Nothing exercised
  `copyForCache` against the cache, and no test compared a charge with the allocator's own answer —
  the existing assertions derive their expectation from the page's own accessors, so a page that
  under-reports its retained bytes satisfies both sides. The gap also only appears when the body size
  is not a size-class boundary, which the small fixtures (counts up to 512 with values under 31
  bytes) never produce.
- Regression test:
  `HOTCompactFragmentReadTest.aCachedPackedCopyIsChargedTheAllocatorFrameItRetains` builds the
  reviewer's leaf through the page writer, reads it back with `FileChannelReader.readHOTLeafFragment`,
  takes `copyForCache()`, puts it in a `ShardedPageCache` and asserts the weight. The oracle is the
  allocator itself — the test asks `allocate(bodyBytes)` what it hands out and reads the segment's
  size, which is 65,536 against a 34,048-byte body — so the expectation never comes from
  `getActualMemorySize`. Command:
  `./gradlew --max-workers=2 :sirix-core:test --tests 'io.sirix.io.filechannel.HOTCompactFragmentReadTest'`.
  Before the fix the charge is 34,048 plus per-page heap and the assertion fails; after it the charge
  is 65,536 plus per-page heap.
- Lesson: a memory charge must come from the allocation a component releases, not the view it reads,
  and when the code already carries a field naming that allocation the accounting must read it.
  Recorded as a fragment in [`docs/lessons.d/hot-leaf-codec.md`](../lessons.d/hot-leaf-codec.md);
  [`../LESSONS.md`](../LESSONS.md) is the permanent destination (see the
  [fragment convention](../../changelog.d/README.md)).
