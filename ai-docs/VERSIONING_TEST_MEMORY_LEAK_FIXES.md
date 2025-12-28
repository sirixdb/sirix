# VersioningTest Memory Leak Investigation & Fixes

## Summary

**Total memory leak reduction: 78% (774 MB → 172 MB)**
**Total unclosed page reduction: 77% (6,236 → 1,418 pages)**

## Fixes Applied

### 1. RecordPageFragmentCache RemovalListener Fix
**File:** `bundles/sirix-core/src/main/java/io/sirix/cache/RecordPageFragmentCache.java`

**Problem:** The RemovalListener was skipping `close()` for ALL non-eviction removals (`!cause.wasEvicted()`), even for unpinned pages.

**Fix:** Now properly checks pin count and closes unpinned pages even on EXPLICIT/REPLACED removal:
```java
if (cause == RemovalCause.EXPLICIT || cause == RemovalCause.REPLACED) {
    if (page.getPinCount() > 0) {
        SKIPPED_EXPLICIT.incrementAndGet();
        return;  // Still in use, don't close
    }
    // Unpinned EXPLICIT/REPLACED removal → safe to close, fall through
}
```

### 2. SLIDING_SNAPSHOT Temporary Page Leak
**File:** `bundles/sirix-core/src/main/java/io/sirix/settings/VersioningType.java`

**Problem:** `SLIDING_SNAPSHOT.combineRecordPagesForModification()` created 3 pages but only 2 went into PageContainer:
- `completePage` ✓
- `modifyingPage` ✓  
- `pageWithRecordsInSlidingWindow` ✗ (leaked!)

**Fix:** Close the temporary page after use:
```java
// CRITICAL FIX: Close temporary page to prevent memory leak
if (pageWithRecordsInSlidingWindow instanceof io.sirix.page.KeyValueLeafPage tempPage) {
    if (!tempPage.isClosed()) {
        tempPage.close();
    }
}
```

**Impact:** Reduced page creation by ~60% (6,506 → 2,644 pages)

### 3. TransactionIntentLog Fragment Cache Bug  
**File:** `bundles/sirix-core/src/main/java/io/sirix/cache/TransactionIntentLog.java`

**Problem:** When adding a PageContainer to TransactionIntentLog, the code removed fragments from **PageCache** instead of **RecordPageFragmentCache**:
```java
bufferManager.getPageCache().remove(fragmentRef);  // WRONG CACHE!
```

**Fix:** Remove from the correct cache:
```java
bufferManager.getRecordPageFragmentCache().remove(fragmentRef);
```

**Impact:** Reduced page creation by additional ~30% (2,644 → 1,688 pages)

## Remaining Leak: Combined Pages Not Cached (All Versioning Types)

### Verified Across All Versioning Types

**FULL:** 1,418/1,688 unclosed (84%)
**INCREMENTAL:** 995/1,260 unclosed (79%)  
**DIFFERENTIAL:** 4,914/5,162 unclosed (95%)

### Root Cause (Hypothesis)
Most `combineRecordPages()` calls create pages that are NEVER cached:
- RecordPageCache handles only ~76 pages per test via `get()` compute function
- TransactionIntentLog closes ~250-270 pages per test
- But 1,000-5,000 `newInstance()` pages are created!

Primary suspect: Write transactions reading PATH_SUMMARY pages use a special code path that bypasses RecordPageCache:

```java
// NodePageReadOnlyTrx.getRecordPage() lines 458-463
// Read-write-trx and path summary page.
var loadedPage =
    (KeyValueLeafPage) loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
// This page is NEVER cached and unclear if/when it's closed!
return new PageReferenceToPage(pageReferenceToRecordPage, loadedPage);
```

### Stack Trace Evidence
```
Page 100/300/500 created via newInstance (closed=37/85/101)
  io.sirix.settings.VersioningType$1.combineRecordPages(VersioningType.java:57)
  io.sirix.access.trx.page.NodePageReadOnlyTrx.loadDataPageFromDurableStorageAndCombinePageFragments
  io.sirix.access.trx.page.NodePageReadOnlyTrx.getRecordPage(NodePageReadOnlyTrx.java:460)  ← Direct path!
  io.sirix.access.trx.page.NodePageReadOnlyTrx.getRecord
  io.sirix.access.trx.page.NodePageTrx.getRecord
  io.sirix.index.path.summary.PathSummaryReader.moveTo
```

### Confirmed: NO Temp Page Leaks in DIFFERENTIAL/INCREMENTAL

✅ Both create exactly 2 pages (completePage + modifiedPage) per modification
✅ No temporary pages like SLIDING_SNAPSHOT
✅ Only SLIDING_SNAPSHOT had the temp page bug (now fixed)

### Recommended Next Steps

The remaining leak affects ALL versioning types equally. Need to verify the PATH_SUMMARY hypothesis:

1. **Add IndexType tracking** to page creation to see which index types are leaking
2. **Trace uncached page lifecycle:** Why are 80-95% of combined pages never cached?
3. **Consider caching PATH_SUMMARY:** Remove the special bypass at line 460 of NodePageReadOnlyTrx
4. **Or track ALL combined pages:** Add transaction-local tracking for cleanup on close

## Test Results

### Initial State (before fixes):
- Physical memory: **774 MB**
- Pages created: 6,506 (6,422 newInstance, 84 deserialized)
- Pages closed: 270
- **Unclosed leak: 6,236 pages**

### After All Fixes:
- Physical memory: **172 MB** (78% reduction)
- Pages created: 1,688 (1,604 newInstance, 84 deserialized)
- Pages closed: 270
- **Unclosed leak: 1,418 pages** (77% reduction)

### Cache Statistics:
- FragmentCache: 1,431 puts, 0 evictions
- RecordPageCache: 76 gets, 0 puts, 21 closes, 54 skipped

## Diagnostic Tools Added

1. Page creation tracking: `PAGES_CREATED`, `PAGES_CREATED_NEW`, `PAGES_CREATED_DESERIALIZE`
2. Cache statistics: `TOTAL_PUTS`, `TOTAL_GETS`, `TOTAL_CLOSES`, `TOTAL_EVICTIONS`
3. Stack trace logging for page creation sources
4. Transaction close logging with unpin counts

These diagnostics should be removed or disabled for production.

