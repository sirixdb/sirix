# Complete MemorySegment Memory Leak Fix - Final Summary

## Critical Issue Found: Pages Pinned Forever

**THE ROOT CAUSE**: Pages were being pinned when added to cache but never unpinned, preventing cache eviction and segment return.

## All Fixes Applied:

### 1. **CRITICAL: Removed Permanent Page Pinning** ✅
**File**: `NodePageReadOnlyTrx.java` lines 711, 725

**Problem**: 
```java
((KeyValueLeafPage) page).incrementPinCount();  // ← Pinned forever!
resourceBufferManager.getPageCache().put(pageReference, page);
```

**Impact**:
- Pages with `pinCount > 0` have **zero weight** in Caffeine cache
- Zero-weight pages are **NEVER EVICTED**
- Segments never returned to allocator
- **Memory grows forever** as new pages are created

**Fix**: Removed the `incrementPinCount()` calls. Pages are only pinned during active use (e.g., in `unpinPageFragments`) and properly unpinned after.

### 2. **Fixed Pre-allocated Page Leak** (100MB+) ✅
**File**: `KeyValueLeafPagePool.java`

**Problem**: Pool pre-allocated 700 pages with 1400 segments that were never used.

**Fix**: Removed `preAllocatePages()` call from initialization.

### 3. **Fixed Double-Free in resizeMemorySegment()** ✅
**File**: `KeyValueLeafPage.java` line 557

**Problem**: When segments were shared between `slotMemory` and `deweyIdMemory`, resizing would update only one reference, leaving the other pointing to freed memory.

**Fix**: Check if segments are shared and update both references.

### 4. **Fixed OverflowPage Memory Leak** ✅
**Files**: `PageKind.java` line 664, `KeyValueLeafPage.java` line 1245

**Problem**: Using `Arena.global().allocate()` which **never frees memory**.

**Fix**: Changed `OverflowPage` to use `byte[]` instead of `MemorySegment`, avoiding the global arena leak.

### 5. **Fixed PageCache Not Returning Segments** ✅
**File**: `PageCache.java` line 29

**Problem**: PageCache called `page.clear()` instead of `returnPage()`.

**Fix**: Changed to call `KeyValueLeafPagePool.getInstance().returnPage()`.

### 6. **Added Double-Free Protection** ✅
**File**: `KeyValueLeafPage.java`

**Added**: `segmentsReturned` flag + `returnSegmentsToAllocator()` idempotent method to prevent double-free.

### 7. **Removed Dangerous Memory Clearing** ✅
**File**: `KeyValueLeafPage.java` `clear()` method

**Problem**: Clearing segments after they were returned caused JVM crashes (SIGSEGV).

**Fix**: Removed memory clearing - allocator already clears segments on allocation.

### 8. **Fixed RecordPageCache Scheduler** ✅
**File**: `RecordPageCache.java` line 34

**Problem**: Missing scheduler reference causing compilation error.

**Fix**: Used `Scheduler.systemScheduler()` explicitly.

## Expected Results:

**Before Fixes**:
- ~100MB+ leaked at initialization (pre-allocated pages)
- Pages accumulated in cache with pinCount > 0
- No cache eviction → no segment return
- Memory grows continuously
- Eventual `OutOfMemoryError: No preallocated segments available`
- JVM crashes from clearing freed segments

**After Fixes**:
- ✅ No pre-allocation leak
- ✅ Pages evicted normally when unpinned
- ✅ Segments returned to allocator pools
- ✅ Segments properly recycled
- ✅ Memory usage stays constant
- ✅ No crashes

## Verification:

1. **Check memory is stable** - run tests and monitor with JFR/VisualVM
2. **Check allocator pools** - segments should be returned and reused
3. **Check cache eviction** - pages with pinCount=0 should be evicted
4. **No crashes** - no more SIGSEGV from clearing freed memory

## Key Insight:

The pin count system was the bottleneck:
- PinCount > 0 → zero weight → never evicted → segments never returned
- The async page loading was incorrectly pinning pages forever
- By removing those incorrect pins, normal cache eviction can work

##Files Modified:

1. `KeyValueLeafPage.java` - segment lifecycle management
2. `KeyValueLeafPagePool.java` - removed pre-allocation
3. `PageCache.java` - proper segment return on eviction
4. `RecordPageCache.java` - scheduler fix
5. `NodePageReadOnlyTrx.java` - removed permanent pinning
6. `OverflowPage.java` - use byte[] instead of MemorySegment  
7. `PageKind.java` - OverflowPage serialization fixes

## Testing Recommendations:

```bash
# Run with memory profiling
./gradlew :sirix-core:test --tests "JsonShredderTest.testShredderAndTraverseChicago" \
  -Djava.util.logging.config.file=logging.properties

# Monitor with JFR
java -XX:StartFlightRecording=filename=test.jfr ...

# Check for:
# - Stable memory usage (no growth)
# - No "OutOfMemoryError: No preallocated segments"
# - No SIGSEGV crashes
```




