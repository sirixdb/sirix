# Memory Leak Root Cause and Complete Fix

## Summary

The memory leak was caused by **TWO critical bugs**:

1. **Missing Arena association** - Memory segments from mmap weren't associated with an Arena
2. **Wrong Caffeine listener type** - RecordPageCache used `evictionListener` instead of `removalListener`

## Bug #1: Missing Arena Association (Fixed)

### Problem
Memory segments created from raw `mmap` system calls had no associated Java `Arena`, preventing proper lifecycle management.

### Solution
- Use a **single shared Arena** for all memory segments (efficient!)
- Associate all mmap'd segments with the shared arena using `MemorySegment.ofAddress(...).reinterpret(size, sharedArena, null)`
- Close the shared arena during cleanup to release all resources

### Changes in LinuxMemorySegmentAllocator.java

**Added:**
```java
// Single shared Arena for all memory segments - much more efficient than one per segment
private volatile Arena sharedArena;
```

**In init():**
```java
// Create a single shared Arena for ALL memory segments
sharedArena = Arena.ofShared();
```

**In mapMemory():**
```java
// Associate the native address with the shared arena for proper lifecycle management
return MemorySegment.ofAddress(addr.address()).reinterpret(totalSize, sharedArena, null);
```

**In free():**
```java
// Close the shared arena to release all associated resources
if (sharedArena != null) {
    sharedArena.close();
    sharedArena = null;
}
```

## Bug #2: RecordPageCache Used Wrong Listener Type (CRITICAL FIX!)

### Problem

`RecordPageCache` registered a `RemovalListener` as an **evictionListener** instead of a **removalListener**:

```java
// WRONG - only fires on size-based evictions
.evictionListener(removalListener)
```

This meant:
- ✅ Segments returned when entries evicted due to size
- ❌ Segments **NOT returned** when `cache.clear()` is called
- ❌ Segments **NOT returned** when `cache.invalidate(key)` is called
- ❌ Segments **NOT returned** on explicit removals

**This caused a massive memory leak** because segments were only returned during natural evictions, not during explicit cache operations.

### Solution

Changed to use `removalListener` (like PageCache already does correctly):

```java
// CORRECT - fires on ALL removals (eviction, clear, invalidate, etc.)
.removalListener(removalListener)
```

### Changes in RecordPageCache.java

**Before (WRONG):**
```java
public RecordPageCache(final int maxWeight) {
    final RemovalListener<PageReference, KeyValueLeafPage> removalListener =
        (PageReference key, KeyValueLeafPage page, RemovalCause cause) -> {
          assert cause.wasEvicted();  // ❌ Only handles evictions!
          // ...
          KeyValueLeafPagePool.getInstance().returnPage(page);
        };

    cache = Caffeine.newBuilder()
                    // ...
                    .evictionListener(removalListener)  // ❌ WRONG - only fires on evictions
                    .build();
}
```

**After (CORRECT):**
```java
public RecordPageCache(final int maxWeight) {
    final RemovalListener<PageReference, KeyValueLeafPage> removalListener =
        (PageReference key, KeyValueLeafPage page, RemovalCause _) -> {
          // Handle ALL removals (eviction, invalidate, clear) - not just evictions
          // ...
          KeyValueLeafPagePool.getInstance().returnPage(page);
        };

    cache = Caffeine.newBuilder()
                    // ...
                    .removalListener(removalListener)  // ✅ CORRECT - fires on ALL removals
                    .build();
}
```

## Why This Combination Was Deadly

1. **Without Arena**: Segments leaked even when returned to allocator
2. **Without proper listener**: Segments weren't even returned on explicit cache operations
3. **Together**: Memory leaked from two different directions!

## Comparison with PageCache

PageCache was already doing it correctly:
```java
// PageCache (CORRECT implementation)
.removalListener(removalListener)  // ✅ Handles all removals
```

RecordPageCache was doing it wrong:
```java
// RecordPageCache (BUG - now fixed)
.evictionListener(removalListener)  // ❌ Only handles evictions
```

## Expected Behavior After Fix

With both fixes applied:

1. ✅ All memory segments are properly associated with a single shared Arena
2. ✅ Segments are returned to allocator on cache eviction
3. ✅ Segments are returned to allocator on `cache.clear()`
4. ✅ Segments are returned to allocator on `cache.invalidate(key)`
5. ✅ Segments are returned to allocator on explicit removals
6. ✅ Arena cleanup releases all JVM-tracked resources
7. ✅ No memory leaks!

## Testing Recommendations

1. **Run your tests** - Memory should stabilize instead of growing
2. **Monitor native memory** - Should see proper cleanup
3. **Check logs** - Look for "LOW ON SEGMENTS" warnings (should be rare/none)
4. **Test explicit clears** - Cache clearing should now properly return segments
5. **Test transaction cleanup** - Verify segments released when transactions close

## Key Takeaways

1. **Always use Arena.ofShared()** for native memory segments in multi-threaded contexts
2. **Use removalListener, not evictionListener** when you need to handle ALL removal scenarios
3. **Test explicit cleanup paths** (clear, invalidate) not just natural evictions
4. **One shared Arena is much more efficient** than one per segment

## Files Modified

1. `LinuxMemorySegmentAllocator.java` - Added Arena support with single shared arena
2. `RecordPageCache.java` - Fixed listener type from evictionListener to removalListener
3. `ARENA_MEMORY_LEAK_FIX.md` - Documentation about Arena fix
4. `MEMORY_LEAK_ROOT_CAUSE_FIX.md` - This comprehensive fix documentation





