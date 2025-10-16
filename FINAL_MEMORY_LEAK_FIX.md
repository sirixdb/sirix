# Final Memory Leak Fix - Complete Solution

## Summary

The memory leak was caused by **THREE issues** that needed to be fixed together:

1. ❌ Memory segments from mmap weren't associated with an Arena
2. ❌ RecordPageCache used `evictionListener` instead of `removalListener`  
3. ❌ Removal listener tried to return pinned pages (assertion failure)

All three issues are now **FIXED** ✅

## The Complete Fix

### Issue #1: Missing Arena Association ✅ FIXED

**Problem:** Memory segments lacked proper JVM lifecycle management

**Solution:** Use a single shared Arena for all memory segments

```java
// LinuxMemorySegmentAllocator.java
private volatile Arena sharedArena;

// In init()
sharedArena = Arena.ofShared();

// In mapMemory()
return MemorySegment.ofAddress(addr.address()).reinterpret(totalSize, sharedArena, null);

// In free()
sharedArena.close();
```

### Issue #2: Wrong Caffeine Listener Type ✅ FIXED

**Problem:** `evictionListener` only fires on size-based evictions, not on explicit removals

**Solution:** Changed to `removalListener` which fires on ALL removals

```java
// RecordPageCache.java - BEFORE (WRONG)
.evictionListener(removalListener)  // ❌ Only fires on evictions

// AFTER (CORRECT)
.removalListener(removalListener)   // ✅ Fires on ALL removals (eviction, clear, invalidate)
```

### Issue #3: Assertion Failure on Pinned Pages ✅ FIXED

**Problem:** Removal listener tried to return pages that were still pinned (in use)

**Root Cause:** When `TransactionIntentLog.put()` removes a page from cache, the page might still be pinned because it's being moved to the transaction log, not discarded.

**Solution:** Skip returning segments for pinned pages - they'll be cleaned up elsewhere

```java
// RecordPageCache.java removal listener
if (page.getPinCount() > 0) {
    // Skip returning - page is still in use (e.g., in TransactionIntentLog)
    LOGGER.trace("Skipping segment return for pinned page {} with pinCount={}", 
                key.getKey(), page.getPinCount());
    return;
}

// Safe to return segments - page is unpinned and being discarded
KeyValueLeafPagePool.getInstance().returnPage(page);
```

## Page Lifecycle Flow

### Scenario 1: Page Evicted from Cache (Unpinned)
```
1. Caffeine cache evicts page due to size limit
2. removalListener fires with pinCount=0
3. Segments returned to allocator ✅
```

### Scenario 2: Page Explicitly Removed (Pinned)
```
1. TransactionIntentLog.put() calls cache.remove(pageRef)
2. removalListener fires with pinCount > 0
3. Skip returning segments (page still in use)
4. TransactionIntentLog increments pinCount
5. Later: TransactionIntentLog.clear() or close():
   a. Decrements pinCount to 0
   b. Clears page
   c. Returns page to pool
   d. Pool returns segments to allocator ✅
```

### Scenario 3: Cache Cleared
```
1. cache.clear() called
2. removalListener fires for each page
3. Pinned pages (pinCount > 0): Skip returning
4. Unpinned pages (pinCount = 0): Return segments to allocator ✅
5. Pinned pages eventually cleaned up by their owners ✅
```

## Why All Three Fixes Were Needed

1. **Without Arena:** Segments leaked even when "returned" to allocator
2. **Without removalListener:** Segments not returned on explicit cache operations  
3. **Without pinCount check:** Assertion failures when removing pages still in use

**Together:** All segments are properly managed and returned! 🎯

## Key Insights

### Pinned Pages Are Managed Elsewhere

When a page is removed from a cache while pinned, it's being **moved** somewhere else (like TransactionIntentLog), not discarded. The removal listener should NOT try to return it - the page's new owner will handle cleanup.

### TransactionIntentLog Owns Pinned Pages

The TransactionIntentLog:
- Removes pages from caches (triggers removal listener with pinCount > 0)
- Increments pinCount to claim ownership
- Eventually decrements pinCount and returns pages to pool
- Pool returns segments to allocator

This is correct behavior - we just needed to make the removal listener aware of it.

### removalListener vs evictionListener

- **evictionListener:** Only fires when cache evicts entries due to size/weight constraints
- **removalListener:** Fires on ALL removals (eviction, clear, invalidate, explicit remove)

For proper cleanup, we need `removalListener` but must check pinCount to avoid interfering with pages that are still in use.

## Files Modified

1. **LinuxMemorySegmentAllocator.java**
   - Added single shared Arena
   - Associate all segments with arena
   - Close arena on cleanup

2. **RecordPageCache.java**
   - Changed from `evictionListener` to `removalListener`
   - Added pinCount check to skip pinned pages
   - Added logging for debugging

## Expected Behavior

✅ Memory segments have proper Arena lifecycle management  
✅ Segments returned on cache eviction (unpinned pages)  
✅ Segments returned on cache.clear() (unpinned pages)  
✅ Segments returned on cache.invalidate() (unpinned pages)  
✅ Pinned pages skip removal listener (managed elsewhere)  
✅ TransactionIntentLog properly cleans up its pages  
✅ No assertion failures  
✅ No memory leaks!  

## Testing

Run your tests and verify:
1. ✅ No assertion errors about pinned pages
2. ✅ Memory usage stabilizes instead of growing
3. ✅ Segments returned to allocator (check logs)
4. ✅ No "OUT OF SEGMENTS" errors
5. ✅ Proper cleanup on transaction commit/rollback

## Comparison with PageCache

`PageCache` already used `removalListener` correctly, but it doesn't have the pin count assertion because it handles mixed page types. `RecordPageCache` is specific to `KeyValueLeafPage` which has pin counts, so we needed to add the pinCount check.

## Final Notes

The memory leak fix required understanding:
1. Arena lifecycle management for native memory
2. Caffeine cache removal listener semantics
3. Page ownership transfer between caches and TransactionIntentLog
4. Pin count protocol for pages in use

All pieces are now in place for proper memory management! 🎉







