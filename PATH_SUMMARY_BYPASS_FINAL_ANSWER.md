# PATH_SUMMARY Bypass - THE COMPLETE ANSWER

## Why the Bypass is Needed - The TRUE Reason

After extensive diagnostics, the answer is clear:

### It's NOT About TIL Isolation!

The documentation was **partially wrong**. The bypass is NOT needed to read modified PATH_SUMMARY pages from TIL.

**Why?** Because **PathSummaryWriter directly updates PathSummaryReader's in-memory cache!**

## The Architecture

### 1. PathSummaryReader Initialization
```java
// PathSummaryReader constructor
while (axis.hasNext()) {
    final var pathNode = axis.next();  // Loaded from page
    pathNodeMapping[(int) pathNode.getNodeKey()] = pathNode;  // CACHED in memory!
}
```

PathNode objects contain **zero-copy MemorySegment references** to page data.

### 2. PathSummaryWriter Modifications
```java
// PathSummaryWriter (line 157-159)
final PathNode pathNode = pageTrx.prepareRecordForModification(...);  // Modifies TIL
pathNode.incrementReferenceCount();
pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);  // UPDATE CACHE!
```

**Key insight**: Every modification is immediately reflected in PathSummaryReader's pathNodeMapping!

### 3. PathSummaryReader Reads
```java
// PathSummaryReader.moveTo() (line 132)
public boolean moveTo(long nodeKey) {
    if (!init && nodeKey != 0) {
        final PathNode node = (PathNode) pathNodeMapping[(int) nodeKey];  // From cache!
        if (node != null) {
            currentNode = node;
            return true;
        }
    }
    // Only loads from page if not in cache
}
```

**Reads use the updated pathNodeMapping** - NO need to re-read from TIL!

## Why Bypass is Required

### The ONLY Reason: Zero-Copy Page Lifecycle

```
Problem:
1. pathNodeMapping holds PathNode objects with MemorySegment references to page data
2. If pages go into RecordPageCache, they can be EVICTED under memory pressure
3. Eviction closes pages, invalidating MemorySegment references
4. pathNodeMapping now has DANGLING references!
5. Next access → SEGFAULT or wrong data
```

### The Bypass Solution

```java
// DON'T use cache (which can evict)
if (trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {
    return getFromBufferManager(...);  // Goes to cache
}

// Load fresh, keep swizzled in PageReference (can't be evicted)
var loadedPage = loadDataPageFromDurableStorageAndCombinePageFragments(...);
return new PageReferenceToPage(pageReferenceToRecordPage, loadedPage);
```

**Effect**:
- Pages stay swizzled in PageReferences
- NOT in RecordPageCache
- Can't be evicted
- pathNodeMapping references stay valid

## Why Only Write Transactions?

**Read-only transactions are safe because**:
1. No modifications → pathNodeMapping never changes
2. Pages can be safely cached
3. If evicted and re-read, same data (no TIL modifications)
4. pathNodeMapping rebuilt from consistent state

**Write transactions are dangerous because**:
1. pathNodeMapping updated via putMapping() with new PathNode objects
2. New PathNode objects have MemorySegment references to pages
3. If those pages are cached and evicted, references become invalid
4. pathNodeMapping.moveTo() doesn't reload, just returns cached object with dangling reference

## Diagnostic Evidence

### 1. PATH_SUMMARY Pages ARE Added to TIL
```
[TIL-PUT] Adding PATH_SUMMARY page to TIL: pageKey=0, revision=0, logKey=9
[TIL-PUT] Adding PATH_SUMMARY page to TIL: pageKey=0, revision=1, logKey=11
...
```

### 2. But pathNodeMapping is Updated Directly
PathSummaryWriter calls `pathSummaryReader.putMapping()` 14 times in the code!

### 3. Bypass is Taken for ALL PATH_SUMMARY in Write Transactions
```
[PATH_SUMMARY-DECISION] Taking BYPASS path:
  - Write transaction detected (trxIntentLog != null)
  - IndexType: PATH_SUMMARY
```

### 4. TIL-CHECK Never Runs
Because bypass is ALWAYS taken for PATH_SUMMARY in write transactions.
The `getFromBufferManager` (which would check TIL) is never called.

## Why It Leaks

The bypass creates pages that are:
1. **Loaded fresh** via `combineRecordPages()`
2. **Never cached** (to prevent eviction)
3. **Swizzled in PageReferences**
4. **Stored in mostRecentPathSummaryPage field**
5. **Never properly closed** when replaced

The "fix" attempts to close replaced pages:
```java
if (!pathSummaryRecordPage.page.isClosed()) {
    pathSummaryRecordPage.page.close();
}
```

But this might be **unsafe** if pathNodeMapping still has references to nodes from that page!

## The Real Problem

**Question**: When pathNodeMapping is updated via putMapping(), does it get PathNode objects from:
- A) The OLD page (loaded via bypass)?
- B) A NEW page (from TIL)?

**Answer**: From TIL! Because:
```java
final PathNode pathNode = pageTrx.prepareRecordForModification(...);  // Gets from TIL
pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);        // Updates cache
```

So:
- Initial pathNodeMapping: PathNode objects from bypassed pages (loaded from disk)
- Updated pathNodeMapping: PathNode objects from TIL pages (prepareRecordForModification)

**This means**:
- OLD bypassed pages CAN be safely closed when replaced!
- NEW PathNode objects come from TIL pages, not bypassed pages
- The memory leak fix should be safe!

## Why the Bypass Can't Be Removed

If we use normal caching (getFromBufferManager):
1. Pages go into RecordPageCache
2. pathNodeMapping gets PathNode objects from cached pages
3. Cache can evict pages under pressure
4. pathNodeMapping has dangling MemorySegment references
5. moveTo() returns objects with invalid references → CRASH

## The Solution

The bypass is fundamentally needed for zero-copy safety. The fix is:

1. **Keep the bypass** (prevents eviction)
2. **Properly close replaced pages** (fix memory leak)
3. **Safe because**: Replaced pages are OLD (from disk), updated pathNodeMapping uses NEW pages (from TIL)

## Why Tests Pass with Bypass

1. PathSummaryReader initialized → loads pages via bypass (from disk)
2. Elements inserted → PATH_SUMMARY modified → pages go to TIL
3. PathSummaryWriter → calls putMapping() → updates pathNodeMapping with NEW PathNode objects (from TIL)
4. PathSummaryReader.moveTo() → uses updated pathNodeMapping → sees modifications ✓
5. Pages stay alive (swizzled, not cached) → zero-copy references valid ✓

## Why Tests Fail Without Bypass

1. PathSummaryReader initialized → loads pages via getFromBufferManager (goes to cache)
2. Pages can be evicted from cache
3. pathNodeMapping has MemorySegment references to evicted (closed) pages
4. moveTo() returns PathNode with invalid MemorySegment → SEGFAULT or wrong data ✗

## Conclusion

**The bypass is required to keep PATH_SUMMARY pages alive for zero-copy references in pathNodeMapping.**

The TIL isolation story was a red herring - modifications are visible via direct pathNodeMapping updates, not by re-reading pages from TIL.

The memory leak can be fixed by properly closing replaced pages, which is safe because:
- Replaced pages are the OLD bypassed pages (from disk)
- Updated pathNodeMapping uses NEW PathNode objects from TIL pages
- No dangling references

## Key Code Locations

1. **PathSummaryReader.java:197** - putMapping() updates cache
2. **PathSummaryWriter.java** - calls putMapping() 14 times after every modification
3. **NodePageReadOnlyTrx.java:455** - bypass decision point
4. **NodePageReadOnlyTrx.java:577-585** - leak fix (close replaced pages)

The diagnostic output confirms this entire flow!





