# Packed HOT writer must bound its source slots

- The integrated HOT test run found one failure in
  `PageKindTest.aSlotThatRunsPastTheUsedBytesIsRefusedNotWritten`.
  The new packed path wrote a page after reading beyond the source leaf's
  declared used bytes. The ordinary writer refused the same source.
- The second six-byte slot starts at byte eight of a 12-byte live region.
  The allocation is 64 KiB, so the offending field reads still succeed
  when checked against the allocation.
- Packed format selection gathered slot lengths and bytes before the
  ordinary writer's validation. Packing could turn an invalid source into
  an apparently valid smaller image and expose stale values.
- The independent fixture now tests both complete and sparse writes. For
  packed frames it checks that selection refuses the source before emitting
  the envelope. The writer bounds headers and payloads against the logical
  used region before reading them and checks their combined live size.
- See [the source-bounds lesson](../LESSONS.md#check-logical-bounds-before-packing).
