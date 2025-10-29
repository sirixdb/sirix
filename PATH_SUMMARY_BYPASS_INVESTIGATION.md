# PATH_SUMMARY Bypass Investigation - Diagnostic Results

## Key Finding: The TIL Hypothesis Was WRONG!

### What We Discovered

Running `VersioningTest.testFull1` with diagnostics revealed:

```
[PATH_SUMMARY-DECISION] Taking BYPASS path:
  - Write transaction detected (trxIntentLog != null)
  - IndexType: PATH_SUMMARY
  - TIL contains 11 total pages, 0 are PATH_SUMMARY
```

**Critical Observation:** The TIL has **ZERO PATH_SUMMARY pages** when the bypass is taken!

### What This Means

1. **The bypass is NOT about reading modified pages from TIL**
   - The documentation claimed PATH_SUMMARY pages in TIL couldn't be read from cache
   - But diagnostics show: TIL has 0 PATH_SUMMARY pages
   - The `getFromBufferManager` is NEVER called for PATH_SUMMARY in write transactions

2. **The REAL reason for the bypass** (from code comments):
   ```
   // REQUIRED BYPASS for PATH_SUMMARY in write transactions:
   // - PathSummaryReader.pathNodeMapping caches PathNode objects during initialization
   // - Normal caching allows pages to be evicted while pathNodeMapping references them
   // - PathSummaryReader.moveTo() doesn't reload missing nodes, just returns false
   // - Results in "Failed to move to nodeKey" errors in VersioningTest
   // - Bypass keeps pages alive (swizzled, not cached) preventing eviction
   ```

3. **The problem is about PAGE EVICTION, not TIL isolation!**
   - PathSummaryReader caches PathNode objects with zero-copy MemorySegment references
   - If the underlying page is evicted from cache, those MemorySegments become invalid
   - PathSummaryReader.moveTo() doesn't reload from disk, just returns false
   - This causes test failures

### The Bypass Mechanism

**Current code (line 455):**
```java
if (trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {
    // Use normal cache
    return new PageReferenceToPage(pageReferenceToRecordPage,
                                   getFromBufferManager(indexLogKey, pageReferenceToRecordPage));
}

// Take bypass for PATH_SUMMARY in write transactions
var loadedPage = loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
return new PageReferenceToPage(pageReferenceToRecordPage, loadedPage);
```

**What happens:**
- Read-only transactions: Use normal cache (can evict pages safely)
- Write transactions: Bypass cache entirely (pages stay "swizzled" in PageReferences)

### Why It Leaks

The bypass creates pages that are:
1. **Not in cache** (bypass avoids RecordPageCache)
2. **Swizzled in PageReferences** (kept alive via setPage())
3. **Referenced by mostRecentPathSummaryPage field**
4. **Never properly closed** when replaced (clear() is a no-op)

Result: Pages accumulate in memory without cleanup.

### Questions to Answer

1. **Why are there NO PATH_SUMMARY pages in TIL?**
   - Are PATH_SUMMARY pages read-only during write transactions?
   - Or are modifications stored differently?

2. **Why only write transactions need the bypass?**
   - Read-only transactions can use cache safely
   - What's different about write transaction page lifetime?

3. **Is the pathNodeMapping really holding references across evictions?**
   - Need to verify the zero-copy issue
   - Check if PathSummaryReader re-initializes pathNodeMapping

### Next Steps

1. Add diagnostics to PathSummaryWriter to see when PATH_SUMMARY pages are modified
2. Check if pathNodeMapping is re-created or reused across operations
3. Test if removing the bypass actually causes failures
4. Investigate the page lifecycle difference between read-only and write transactions

### Diagnostic Commands Used

```bash
./gradlew :sirix-core:test --tests "*VersioningTest.testFull1" \
  -Dsirix.debug.path.summary=true --rerun-tasks
```

Output shows bypass is always taken for PATH_SUMMARY in write transactions, and TIL contains 0 PATH_SUMMARY pages every time.

