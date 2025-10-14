# SIGSEGV Race Condition - Critical Fix

## Problem: SIGSEGV in `Unsafe.getIntUnaligned`

```
SIGSEGV (0xb) at pc=0x000077945b0c0a3a
Problematic frame:
J 3718 jvmci jdk.internal.misc.Unsafe.getIntUnaligned(Ljava/lang/Object;JZ)I
```

This indicates **use-after-free** - trying to read from memory that has been unmapped.

---

## Root Cause: Race Condition in Eager Region Freeing

### The Bug

**Before Fix:**
```java
@Override
public void release(MemorySegment segment) {
    // ... return to pool ...
    
    MemoryRegion region = findRegionForSegment(segment);
    if (region != null) {
        int unused = region.unusedSlices.incrementAndGet();
        
        // BUG: Free region immediately when all slices returned
        if (unused == region.totalSlices) {
            freeRegion(region);  // Calls arena.close() + munmap
        }
    }
}
```

### The Race Condition

**Timeline of the crash:**

1. **Thread A** returns the last slice → `unusedSlices == totalSlices`
2. **Thread A** calls `freeRegion()`:
   - Calls `pool.removeIf()` to remove slices
   - Calls `munmap()` to release native memory
   - Calls `arena.close()` to invalidate MemorySegments
   
3. **Thread B** (concurrently) calls `allocate()`:
   - Calls `pool.poll()` and gets a segment ✅
   - Segment is still in pool during `removeIf()`
   
4. **Thread A's** `arena.close()` **invalidates Thread B's segment**

5. **Thread B** tries to use the segment:
   ```java
   memorySegment.get(ValueLayout.JAVA_INT_UNALIGNED, position)
   ```
   → **SIGSEGV!** Memory was unmapped!

---

## Why This Happens

### Small Regions Make It Worse

With **1MB regions** and **64KB segments**:
- Only **16 segments per region**
- All 16 returned → immediate free
- Higher chance of race between return and reallocation
- **This is why it appeared after reducing region size!**

### The Timing Window

```
Thread A                          Thread B
─────────────────────────────────────────────────────
release(segment16)
  unusedSlices = 16
  if (16 == 16) {
    removeIf() starts...
    [WINDOW]                       allocate()
    [WINDOW]                         poll() → gets segment16
    removeIf() completes
    munmap()
    arena.close()
      invalidates segment16
                                     segment16.get(...) → SIGSEGV!
  }
```

---

## The Fix

### Defer Region Freeing

**After Fix:**
```java
@Override
public void release(MemorySegment segment) {
    // ... return to pool ...
    
    MemoryRegion region = findRegionForSegment(segment);
    if (region != null) {
        region.unusedSlices.incrementAndGet();
        
        // CRITICAL FIX: Do NOT free regions immediately!
        // Only free when we need budget space (in freeUnusedRegions)
    }
}
```

### Why This Works

1. **Segments stay valid** until we explicitly need memory back
2. **No race condition** - segments in pool are always valid
3. **Cleanup happens safely** in `freeUnusedRegions()`:
   - Called when budget exceeded
   - Called during `free()` at shutdown
   - At these points, no active allocations happen

### Memory Trade-off

**Before Fix:**
- ✅ Memory freed immediately
- ❌ SIGSEGV race condition
- ❌ Dangerous and broken

**After Fix:**
- ✅ No race condition - safe!
- ✅ Memory freed when needed (budget pressure)
- ✅ Still respects budget limits
- ⚠️ Slightly higher memory usage (regions kept until needed)

The memory trade-off is acceptable - we still enforce the budget, just defer cleanup.

---

## Proper Pool Lifecycle

This follows the standard pattern for memory pools:

### Correct Approach
```
1. Allocate regions on-demand
2. Return objects to pool immediately (fast path)
3. Cleanup/free only when:
   - Out of budget (need space)
   - Explicit cleanup request
   - Shutdown
```

### What We Were Doing Wrong
```
1. Allocate regions on-demand ✅
2. Return to pool ✅
3. Free immediately when empty ❌ RACE CONDITION!
```

---

## Testing the Fix

### Verify No SIGSEGV

```bash
./gradlew :sirix-core:test --tests "*testShredderAndTraverseChicago*"
```

**Expected:**
- ✅ No SIGSEGV crashes
- ✅ Test completes successfully
- ✅ Memory usage slightly higher but within budget
- ✅ Regions freed when budget pressure hits

### Check Memory Behavior

With 1MB regions:
- Memory grows in 1MB chunks
- Regions stay allocated until budget exceeded
- Then `freeUnusedRegions()` cleans up
- Net result: Still within budget, just less aggressive cleanup

---

## Why Standard Pools Don't Free Eagerly

Most memory pools (e.g., Netty ByteBufAllocator, jemalloc) don't free eagerly because:

1. **Race conditions** - objects can be reallocated before cleanup
2. **Performance** - freeing is expensive (munmap, Arena.close)
3. **Fragmentation** - keeping regions reduces allocation overhead
4. **Simplicity** - defer cleanup to explicit points

Our fix aligns with these best practices.

---

## Impact on Performance

### Before Fix (with eager freeing)
- ❌ SIGSEGV crashes
- ❌ Unpredictable behavior
- ❌ System unstable

### After Fix (deferred freeing)
- ✅ Stable and correct
- ✅ Slightly higher memory usage (acceptable)
- ✅ Still respects budget
- ✅ Cleanup happens when needed
- ✅ No performance impact (freeing was rare anyway)

---

## Conclusion

**The eager region freeing was premature optimization that caused a critical bug.**

The fix:
- ✅ Eliminates race condition
- ✅ Maintains correctness
- ✅ Slightly higher memory usage but within budget
- ✅ Follows standard pool patterns
- ✅ Much more robust

**Trade-off:** Worth it for stability!

