# FMSE Test - Comprehensive Investigation

## Verified Baseline
- ✅ Test **PASSES** on commit `5b8eda965` (working)
- ❌ Test **FAILS** on commit `e301dd78b` and all subsequent commits

## Root Issue
`SAXParseException: Element or attribute "y:" do not match QName production`
- Element local names return `null` for nameKeys from older revisions
- Invalid QNames like `y:` instead of `y:Resources`

## What We've Tried (All Failed)
1. ❌ Reverted caching structure (2-slot generic)
2. ❌ Disabled field-based caching for NAME pages
3. ❌ Bypassed RecordPageCache for NAME/PATH_SUMMARY
4. ❌ Bypassed NamesCache in NamePage.java
5. ❌ Applied PATH_SUMMARY-style handling to NAME pages
6. ❌ Restricted caching to only DOCUMENT/PATH_SUMMARY
7. ✅ **FIXED** double-pinning bug in `getPageFragments()` (but test still fails)
8. ❌ Disabled async schedulers (Scheduler.systemScheduler()) in all caches
9. ❌ Reverted to PageCache for page fragments instead of RecordPageFragmentCache

## Key Architectural Changes Between Commits

### Commit `5b8eda965` (Working)
- Global pin tracking: `incrementPinCount()`, `decrementPinCount()`
- 2-slot generic caching: `mostRecentlyReadRecordPage`, `secondMostRecentlyReadRecordPage`, `pathSummaryRecordPage`
- Page fragments cached in `PageCache`
- No `RecordPageFragmentCache` exists
- No async scheduler on caches

### Commit `e301dd78b` and Later (Broken)
- Per-transaction pin tracking: `incrementPinCount(trxId)`, `decrementPinCount(trxId)`
- 1-slot per-type caching OR 2-slot generic (we tried both)
- NEW cache: `RecordPageFragmentCache` introduced
- Async schedulers added to caches
- Weigher returns 0 for pinned pages

## Bugs Found and Fixed
1. ✅ **Double-pinning** in `getPageFragments(PageReference)` - first fragment was pinned twice
2. ❌ **Async eviction** - disabled but didn't fix the issue
3. ❌ **RecordPageFragmentCache** - reverted to PageCache but didn't fix the issue

## Hypothesis: Per-Transaction Pin Tracking
The introduction of per-transaction pin tracking (`incrementPinCount(trxId)`) is the most fundamental change. Even after reverting all other changes, the issue persists. This suggests a subtle bug in how per-transaction pins interact with:
- Cache eviction timing
- Page fragment combining across revisions
- NamePage lookups across revisions

## Outstanding Questions
1. Why do nameKeys from older revisions return null names?
2. Is the NamePage being loaded from the wrong revision?
3. Is there a cache key collision between revisions?
4. Is the per-transaction pin tracking causing premature eviction or stale data?
5. Are there other changes beyond caching and pinning in commit e301dd78b?

