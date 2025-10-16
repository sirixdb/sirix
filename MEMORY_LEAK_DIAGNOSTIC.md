# Memory Leak Diagnostic Guide

## Key Question: Are segments actually being returned to the allocator?

The allocation/deallocation flow should be:

1. **Allocation**: `borrowPage()` → `segmentAllocator.allocate()` → takes segment from pool
2. **Usage**: Page lives in cache (PageCache, RecordPageCache, TransactionIntentLog)
3. **Return**: Cache eviction → `returnPage()` → `returnSegmentsToAllocator()` → `segmentAllocator.release()` → segment goes back to pool

## Potential Issues:

### 1. **PinCount Preventing Eviction** (MOST LIKELY)
Pages with `pinCount > 0` have zero weight in Caffeine cache:
```java
.weigher((_, value) -> value.getPinCount() > 0 ? 0 : value.getUsedSlotsSize())
```

If pinCount is never decremented, pages accumulate with **zero weight** and are **never evicted**, causing:
- Pages held indefinitely in cache
- Segments never returned to allocator
- Continuous growth as new pages are created

**Check**: Are pages being properly unpinned after use?

### 2. **Allocator Pool Exhaustion**
The allocator pre-allocates a fixed number of segments. If segments aren't returned:
```
OutOfMemoryError: No preallocated segments available for size: 65536
```

This suggests segments are being taken but not returned to pools.

### 3. **Strong References Holding Pages**
If pages are referenced outside the cache, they won't be GC'd even when evicted.

## Debugging Steps:

1. **Check allocator pool sizes**:
   ```java
   LinuxMemorySegmentAllocator.getInstance().getPoolSizes()  // Add this method
   ```
   
2. **Monitor pinCount**:
   - Add logging to `incrementPinCount()` / `decrementPinCount()`
   - Track if pinCount ever reaches 0

3. **Verify eviction is happening**:
   - Add logging to cache removal listeners
   - Check if `returnSegmentsToAllocator()` is being called

4. **Check segment return**:
   - Add logging to `segmentAllocator.release()`
   - Verify segments are actually going back to pools

## Most Likely Root Cause:

**Pages are pinned but never unpinned**, preventing cache eviction and segment return.

Look for code patterns like:
```java
page.incrementPinCount();
// ... use page ...
// MISSING: page.decrementPinCount();
```

Or:
```java
// Page added to TransactionIntentLog with pinCount++
// But not decremented when log is cleared/closed
```








