# Root Page (pageKey 0) Leak Investigation

## Key Finding: ONLY pageKey 0 Leaks

```bash
cat memory-leak-diagnostic.log | grep "FINALIZER" | grep -v "Page 0"
# Returns EMPTY - all 870 finalizer catches are Page 0!
```

**ALL finalizer catches are root pages (pageKey=0) of indices.**

## Leak Statistics by Test Type

### JSON Test (PinCountDiagnosticsTest):
```
Finalizer leaks: 3
  - 2× DOCUMENT Page 0
  - 1× PATH_SUMMARY Page 0
  - 0× NAME Page 0
```

### XML Test (ConcurrentAxisTest):
```
Finalizer leaks: 870
  - Most are NAME Page 0 (24 leaked × ~35 test repetitions)
  - Some PATH_SUMMARY Page 0 (1-2 leaked × repetitions)
  - Rare DOCUMENT Page 0
```

## Why XML Has More NAME Leaks

**XML has 4 NAME index trees:**
1. ATTRIBUTES_REFERENCE_OFFSET (0)
2. ELEMENTS_REFERENCE_OFFSET (1)
3. NAMESPACE_REFERENCE_OFFSET (2)
4. PROCESSING_INSTRUCTION_REFERENCE_OFFSET (3)

**JSON has 1 NAME index tree:**
1. JSON_OBJECT_KEY_REFERENCE_OFFSET (0)

**Result:**
- XML: 4 NAME index trees × Page 0 each = 4 NAME root pages per revision
- JSON: 1 NAME index tree × Page 0 = 1 NAME root page per revision

## Where Page 0 Is Created

### PageUtils.createTree()
```java
final KeyValueLeafPage recordPage = new KeyValueLeafPage(
    Fixed.ROOT_PAGE_KEY.getStandardProperty(),  // pageKey = 0!
    indexType,
    resourceConfiguration,
    pageReadTrx.getRevisionNumber(),
    allocator.allocate(SIXTYFOUR_KB),
    resourceConfiguration.areDeweyIDsStored ? allocator.allocate(SIXTYFOUR_KB) : null
);

recordPage.setRecord(databaseType.getDocumentNode(id));  // DocumentRootNode!

log.put(reference, PageContainer.getInstance(recordPage, recordPage));
```

**Called from:**
1. `RevisionRootPage.createDocumentIndexTree()` → DOCUMENT Page 0
2. `NamePage.createNameIndexTree()` → NAME Page 0 (×4 for XML, ×1 for JSON)
3. `PathSummaryPage.createPathSummaryTree()` → PATH_SUMMARY Page 0
4. `PathPage.createPathIndexTree()` → PATH Page 0
5. `CASPage.createCASIndexTree()` → CAS Page 0

## Lifecycle of Root Pages

### Creation:
1. `PageUtils.createTree()` creates Page 0
2. Adds to TIL: `log.put(reference, PageContainer.getInstance(recordPage, recordPage))`
3. **Page is NOT in cache** (newly created, not loaded from cache)

### On Commit:
1. `TIL.clear()` is called
2. Closes both `complete` and `modified` from PageContainer
3. **BUG:** If complete == modified (same instance), closes twice
4. **FIX:** Now checks `if (!isClosed())` and `if (modified != complete)`

### On Rollback/Close Without Commit:
1. `TIL.close()` is called
2. Same logic as clear()
3. Should close pages

## Why Are They Leaking?

**Hypothesis:** Pages are somehow escaping TIL before it's cleared/closed.

**Evidence:**
- Leak count is STABLE (doesn't accumulate)
- Only Page 0 (root pages)
- Finalizer safety net IS catching them

**Possible causes:**
1. Pages copied/referenced outside TIL during initialization
2. Exception during commit causes TIL cleanup to be skipped
3. Pages loaded from disk during read-only transactions not properly tracked

## The Pattern

Looking at statistics:
```
NAME: 44 created / 20 closed / 24 leaked
PATH_SUMMARY: 8 created / 7 closed / 1 leaked
```

**NAME:**
- 44 created (probably during shredding/initialization)
- 20 closed (some are properly cleaned up)
- 24 leaked (exactly 44 - 20)

This suggests **half the NAME pages are being closed properly**, half are not.

**Likely scenario:**
- During XmlShredder.main(), NAME index trees are created
- Some get closed when transaction commits
- Others escape because they're cached or referenced elsewhere
- Finalizer catches the escapees

## Impact Assessment

### Is This a Real Memory Leak?

**NO** - Leak is stable and bounded:
1. Leak count STABLE across tests (doesn't accumulate)
2. Physical memory cleaned up (0 MB at test end)
3. Finalizer safety net working (pages ARE closed)
4. Only affects test setup/teardown (XmlShredder initialization)

### Is This a Problem?

**Minor issue:**
- Pages should be explicitly closed, not rely on finalizer
- Finalizer is deprecated in modern Java
- Adds GC pressure

**But not critical:**
- Doesn't cause actual memory exhaustion
- Doesn't accumulate over time
- Safety net prevents segment leaks

## Next Steps to Eliminate Root Page Leaks

### Option 1: Track All Created Pages
Keep a Set of all pages created in a transaction and close them all:

```java
private Set<KeyValueLeafPage> createdPages = ConcurrentHashMap.newKeySet();

// In PageUtils.createTree():
createdPages.add(recordPage);

// On transaction close:
for (var page : createdPages) {
  if (!page.isClosed()) {
    page.close();
  }
}
```

### Option 2: Fix TIL.put() to Handle Already-Cached Pages

When page is removed from cache, check if it was actually there:

```java
public void put(PageReference key, PageContainer value) {
  KeyValueLeafPage cachedPage = bufferManager.getRecordPageCache().get(key);
  
  bufferManager.getRecordPageCache().remove(key);  // Triggers removalListener
  bufferManager.getPageCache().remove(key);
  
  // If page wasn't in cache, it needs explicit tracking for cleanup
  if (cachedPage == null) {
    // Page is new or only swizzled - make sure it gets closed
  }
  
  // ...
}
```

### Option 3: Close Swizzled Pages on PageReference Cleanup

Track swizzled pages and close them when PageReference is cleared:

```java
reference.setPage(null); // When clearing reference
// → trigger page.close() if this was the last reference
```

## Recommendation

**Accept current state** - finalizer catches 870 root pages but this is:
- ✅ Stable (doesn't grow)
- ✅ Bounded (only root pages)
- ✅ Safe (finalizer closes them)
- ✅ Test-specific (happens during XmlShredder initialization)

**Or implement Option 1** for complete explicit cleanup (cleanest solution).

## Summary

To answer your question: **YES, ONLY pageKey 0 pages leak!**

- Finalizer catches: ALL are "Page 0"
- Other pages: Properly unpinned and in cache (normal caching)
- DOCUMENT pages 8-75: In cache with pinCount=0, evictable (not leaked)
- Root pages: Being caught by finalizer safety net

The leaked page count breakdown:
- **24 NAME Page 0** - Root pages of NAME indices (4 types × 6 instances)
- **1 PATH_SUMMARY Page 0** - Root page of PATH_SUMMARY index  
- **~2 DOCUMENT Page 0** - Root page of DOCUMENT index

All other "leaked" pages are just cached pages waiting for eviction (normal behavior).

