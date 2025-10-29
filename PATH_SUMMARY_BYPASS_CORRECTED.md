# PATH_SUMMARY Bypass - CORRECTED UNDERSTANDING

## Critical Correction

PathNode objects do **NOT** contain MemorySegment references. They are regular Java objects with:
- Delegates (NodeDelegate, StructNodeDelegate, NameNodeDelegate)
- Primitive fields (int references, int level)
- Regular object references (QNm name, PathNode links, etc.)

## What the Bypass Actually Does

```java
// Bypass code (line 515-516)
var loadedPage = loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
```

Looking at `loadDataPageFromDurableStorageAndCombinePageFragments` (line 658-691):
1. Calls `getPageFragments(pageReference)` - loads from **durable storage** (disk/cache)
2. Calls `versioningApproach.combineRecordPages()` - combines page fragments
3. Returns combined page

**The bypass loads from DISK, not from TIL!**

## So Why Does It Work?

The key insight: **PATH_SUMMARY pages are rarely re-read after initialization!**

### The Architecture

1. **PathSummaryReader Initialization** (line 110-156 in PathSummaryReader.java):
   ```java
   if (pathSummaryData == null || pageReadTrx.hasTrxIntentLog()) {
       // Load ALL PATH_SUMMARY nodes into pathNodeMapping array
       while (axis.hasNext()) {
           pathNodeMapping[(int) pathNode.getNodeKey()] = pathNode;
       }
   }
   ```
   - Loads all existing PATH_SUMMARY nodes
   - Stores them in `pathNodeMapping` array
   - For write transactions: does NOT cache this (line 152-154)

2. **PathSummaryWriter Modifications**:
   ```java
   // PathSummaryWriter calls this after EVERY modification
   pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
   ```
   - Every modification immediately updates `pathNodeMapping`
   - Modified PathNode objects come from `pageTrx.prepareRecordForModification()` (which uses TIL)
   - `pathNodeMapping` always has the latest data

3. **PathSummaryReader Reads** (moveTo, line 424-472):
   ```java
   public boolean moveTo(long nodeKey) {
       if (!init && nodeKey != 0) {
           final PathNode node = pathNodeMapping[(int) nodeKey];  // From cache!
           if (node != null) {
               currentNode = node;
               return true;  // Most reads return here!
           }
       }
       // Only falls through to load from page if not in cache
       newNode = pageReadTrx.getRecord(nodeKey, IndexType.PATH_SUMMARY, 0);
   }
   ```
   - First checks `pathNodeMapping`
   - Only loads from page if not found (rare after initialization)

## Why the Bypass Might Not Be Needed for Correctness

Since `putMapping()` keeps `pathNodeMapping` up-to-date, and most reads hit the cache, **the bypass may not be needed for correctness at all**!

The bypass loads from disk (not TIL), but that's okay because:
1. Initial load: Gets committed data from disk → correct
2. Modifications: `putMapping()` updates cache with TIL data → correct  
3. Subsequent reads: Hit cache → correct

Pages are only re-loaded from disk during initialization, when there are no TIL modifications yet.

## Then Why Does the Bypass Exist?

Looking at the code comments (line 501-506):
```
// REQUIRED BYPASS for PATH_SUMMARY in write transactions:
// - PathSummaryReader.pathNodeMapping caches PathNode objects during initialization
// - Normal caching allows pages to be evicted while pathNodeMapping references them
// - PathSummaryReader.moveTo() doesn't reload missing nodes, just returns false
// - Results in "Failed to move to nodeKey" errors in VersioningTest
// - Bypass keeps pages alive (swizzled, not cached) preventing eviction
```

**Hypothesis**: The bypass might be preventing a different issue - perhaps related to **page lifecycle during initialization**, not zero-copy references.

## What We Need to Understand

1. **What happens without the bypass?**
   - Pages go into RecordPageCache
   - Can be evicted
   - But PathNode objects in `pathNodeMapping` are deserialized copies, not references to page data
   - So eviction shouldn't break anything...

2. **Unless...** there's something about how PathSummaryReader is initialized across multiple revisions?

3. **Or...** the bypass is actually **not needed** and can be safely removed?

## Diagnostic Evidence

1. **Bypass loads from disk, not TIL**:
   - `loadDataPageFromDurableStorageAndCombinePageFragments` → `getPageFragments` → `pageReader.read()`
   - This reads from disk/cache, not TIL

2. **pathNodeMapping is kept up-to-date**:
   - `putMapping()` called 14 times in PathSummaryWriter
   - Updates cache with PathNode objects from TIL

3. **Most reads hit the cache**:
   - Diagnostic shows many `[PATH_SUMMARY-MOVETO] Found in pathNodeMapping`
   - Few `Loading from page` after initialization

## Next Question

**Can we test removing the bypass?**

If `pathNodeMapping` is always kept up-to-date, and PathNode objects are deserialized copies (not references to page data), then using normal caching should work fine:

```java
// Instead of bypass
return new PageReferenceToPage(pageReferenceToRecordPage,
                               getFromBufferManager(indexLogKey, pageReferenceToRecordPage));
```

This would:
- Cache pages normally
- Allow eviction
- pathNodeMapping still works (has deserialized copies)
- putMapping() still updates cache
- No memory leak (pages can be evicted)

## Conclusion

Without MemorySegment references, the zero-copy theory is wrong. The bypass loads from disk (not TIL), so it's not about TIL isolation either.

The bypass might be:
1. **Legacy code** that's no longer needed
2. **Working around a different issue** we haven't identified yet
3. **Actually causing the memory leak** without providing any benefit

We should TEST removing the bypass to see if tests still pass.

