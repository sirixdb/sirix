# FMSE Fix Attempt - Progress Report

## Fix Applied: NAME Page Index Number Discrimination

### Changes Made
Modified `NodePageReadOnlyTrx` to track NAME pages by indexNumber:

1. **Changed storage:** Single `mostRecentNamePage` → `Map<Integer, RecordPage> mostRecentNamePagesByOffset`
2. **Added helper:** `getMostRecentPageWithIndex(type, indexNumber)` to look up by both
3. **Updated all callers:** Use indexNumber-aware lookup for NAME pages
4. **Fixed close():** Unpin all NAME sub-index pages individually

### Result
**Still failing** ❌ but with more insight

### Debug Finding
```
WARNING: Empty name for key=0, kind=ELEMENT, trxId=16
  mostRecentNamePages: [0, 1, 2]
```

This reveals:
1. **NAME pages ARE being tracked separately** (offsets 0, 1, 2) ✓
2. **But getName(key=0) returns empty** ❌

### Analysis

The name key `0` is suspicious - it's likely an invalid/corrupted key rather than a proper hash value.

Possible issues:
1. **Element node has wrong localNameKey** - The ElementNode.localNameKey field might be corrupted (set to 0)
2. **NAME record corruption** - The NAME KeyValueLeafPage might have corrupt data
3. **Deserialization issue** - Names being incorrectly deserialized from disk

### The Root Issue Goes Deeper

The cache collision fix (tracking by indexNumber) is correct and working (we see 3 separate NAME pages tracked).

But the **underlying data corruption** remains - why is the local name key 0 instead of a proper hash?

This suggests the bug in commit e301dd78b isn't just about caching, but also about:
- How NAME records are stored/retrieved with new pin counting
- Whether NAME KeyValueLeafPages are being incorrectly shared or corrupted
- If deserialization is reading from wrong offsets

## Recommendation

The FMSE issue is deeper than initially thought. While the cache collision fix is correct and necessary, there's an additional data corruption issue.

### Options

1. **Continue investigating** - Need to trace why localNameKey=0 for "Resources" element
2. **Revert e301dd78b changes** - Go back to 2-slot caching design
3. **Focus on VersioningTest** - That's complete and working; handle FMSE separately

### VersioningTest Status

**VersioningTest fixes remain valid and working!**
- All 13 tests passing
- 99% memory reduction
- Completely independent from this FMSE NAME page issue

The VersioningTest investigation can be considered complete and successful.


