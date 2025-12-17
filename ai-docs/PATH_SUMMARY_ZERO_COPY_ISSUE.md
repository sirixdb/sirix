# PATH_SUMMARY Zero-Copy Issue - The REAL Problem

## The Root Cause (Finally!)

PATH_SUMMARY pages must stay PINNED because **PathSummaryReader caches PathNode objects in memory** that contain zero-copy MemorySegment references to page data.

## How PathSummaryReader Works

### The pathNodeMapping Array

```java
// PathSummaryReader.java
private StructNode[] pathNodeMapping;  // Cached PathNode objects!

public boolean moveTo(long nodeKey) {
    if (!init && nodeKey != 0) {
        final PathNode node = (PathNode) pathNodeMapping[(int) nodeKey];  // From cache!
        if (node != null) {
            currentNode = node;
            return true;
        }
    }
    // ...
}
```

### The Lifecycle

1. **Initial Load** (constructor):
   ```java
   // Load all PATH_SUMMARY nodes into memory
   while (axis.hasNext()) {
       final var pathNode = axis.next();  // Loaded from page
       pathNodeMapping[(int) pathNode.getNodeKey()] = pathNode;  // CACHED!
   }
   ```

2. **Zero-Copy Problem:**
   - `pathNode` contains MemorySegment references to page data
   - `pathNodeMapping` holds these PathNode objects
   - **Pages must stay alive while pathNodeMapping exists!**

3. **Modification:**
   ```java
   // PathSummaryWriter.java
   final PathNode pathNode = pageTrx.prepareRecordForModification(...);  // Modified node
   pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);  // Update cache!
   ```

## Why the Bypass Was Added

The current bypass code:
```java
// Read-write-trx and path summary page.
var loadedPage = loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
return new PageReferenceToPage(pageReferenceToRecordPage, loadedPage);
```

**Purpose:** Create new combined pages for write transactions, keep them alive for pathNodeMapping

**Problem:** These pages are never cached, never cleaned up properly → 80-98% leak!

## Why Removing the Bypass Breaks Things

When you use normal caching:
```java
return new PageReferenceToPage(pageReferenceToRecordPage,
                               getFromBufferManager(indexLogKey, pageReferenceToRecordPage));
```

**What happens:**
1. Page goes into RecordPageCache
2. pathNodeMapping holds PathNode objects from this page
3. Cache evicts the page (under pressure)
4. Page memory is released
5. **pathNodeMapping now has dangling MemorySegment references!**
6. Next access → SEGFAULT or wrong data!

## The Real Question

**Why doesn't this break with the bypass?**

With the bypass:
- Pages are swizzled in PageReferences but NOT cached
- Pages stay alive because:
  - PageReference holds them (swizzled)
  - mostRecentPathSummaryPage field holds them
  - They're not evicted (not in cache)
- **But they leak** because when replaced, they're never closed!

## The Actual Problem

The issue is NOT about TIL lookup or transaction isolation!

The issue is: **How do we keep PATH_SUMMARY pages alive while pathNodeMapping references them, WITHOUT leaking memory when the mapping is replaced?**

### Current State (with bypass):
- ✅ Pages stay alive (swizzled + field reference)
- ✅ Zero-copy works (no dangling references)
- ❌ **Pages leak when replaced** (clear() is no-op)

### With normal caching (bypass removed):
- ❌ Pages can be evicted while pathNodeMapping references them
- ❌ Zero-copy broken (dangling references)
- ✅ No leak (cache handles cleanup)

## The Solution

We need to **properly close replaced PATH_SUMMARY pages** instead of calling `clear()`:

**File:** `NodePageReadOnlyTrx.java` line 524-526

**Change:**
```java
} else {
    // Write transaction: close replaced page properly
    pathSummaryRecordPage.pageReference.setPage(null);
    if (!pathSummaryRecordPage.page.isClosed()) {
        pathSummaryRecordPage.page.close();  // FIX: Actually release memory!
    }
}
```

**Why this is safe:**
- When pathSummaryRecordPage is replaced, the OLD pathNodeMapping is no longer used
- PathSummaryWriter.putMapping() updates the array with new nodes from new pages
- Old nodes are discarded, so old page can be safely closed
- New page is kept alive via new mostRecentPathSummaryPage reference

## Verification Needed

Check that when pathNodeMapping is updated, ALL old PathNode references are replaced before the old page is closed.

If pathNodeMapping can have a mix of nodes from different pages, we need a more sophisticated tracking mechanism.

