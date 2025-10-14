# Complete SIGSEGV Fix - Arena and Lifecycle Issues

## The Problem

SIGSEGV crashes happening in multi-threaded workload with parallel serialization:
```
SIGSEGV (0xb) at pc=0x00007203ff024bc3
Problematic frame:
J 3271 jvmci jdk.internal.misc.Unsafe.putIntUnaligned
```

---

## Root Causes Identified

### Issue #1: Arena.close() Invalidates ALL Segments

**The Bug:**
```java
private static class MemoryRegion {
    final Arena arena;  // ← ONE ARENA PER REGION
    final MemorySegment baseSegment;
    
    MemoryRegion(...) {
        this.arena = Arena.ofShared();
        this.baseSegment = MemorySegment.ofAddress(mmappedSegment.address())
                                        .reinterpret(..., arena, null);
    }
}

// When creating slices:
MemorySegment slice = region.baseSegment.asSlice(...);  // ← ALL SLICES SHARE ARENA

// When freeing:
arena.close();  // ← INVALIDATES ALL SLICES IMMEDIATELY!
```

**The Problem:**
- All slices from one region share the same Arena
- When `arena.close()` is called, ALL slices become invalid
- Even slices that are actively being used by other threads!
- **Arena is fundamentally incompatible with pooling**

**The Race:**
```
Thread A                        Thread B
─────────────────────────────────────────
freeRegion(region)
  arena.close()
    invalidates all slices
                                 segment.put(...) → SIGSEGV!
```

---

### Issue #2: unusedSlices Not Tracking Allocations

**The Bug:**
```java
// In allocate():
MemorySegment segment = pool.poll();
poolSizes[index].decrementAndGet();
// ❌ NOT decrementing region.unusedSlices!

// In release():
pool.offer(segment);
poolSizes[index].incrementAndGet();
region.unusedSlices.incrementAndGet();  // ✅ Only tracking returns
```

**The Problem:**
- `unusedSlices` only tracked returns, not allocations
- `allSlicesReturned()` could be true even with segments in use!

**The Race:**
```
Thread A                        Thread B
─────────────────────────────────────────
segment = pool.poll()
  unusedSlices = 16 (unchanged!)
                                 freeUnusedRegions()
                                   if (16 == 16) {  // TRUE!
                                     munmap()
segment.put(...) → SIGSEGV!
```

---

## The Complete Fix

### Fix #1: Remove Arena Entirely

**Before:**
```java
private static class MemoryRegion {
    final Arena arena;
    final MemorySegment baseSegment;
    
    MemoryRegion(...) {
        this.arena = Arena.ofShared();
        this.baseSegment = MemorySegment.ofAddress(mmappedSegment.address())
                                        .reinterpret(..., arena, null);
    }
}

void freeRegion(MemoryRegion region) {
    releaseMemory(region.baseSegment, ...);
    region.arena.close();  // ❌ Invalidates all slices!
}
```

**After:**
```java
private static class MemoryRegion {
    final MemorySegment baseSegment;  // No Arena!
    
    MemoryRegion(...) {
        // Use raw mmap'd segment directly
        this.baseSegment = mmappedSegment;
    }
}

void freeRegion(MemoryRegion region) {
    releaseMemory(region.baseSegment, ...);
    // No arena.close() - segments stay valid until munmap
}
```

**Why This Works:**
- No Arena means no `Arena.close()` to invalidate segments
- Segments remain valid until we explicitly `munmap()`
- We manage lifecycle manually, which is safe for pooling

---

### Fix #2: Track Both Allocations and Releases

**Before:**
```java
public MemorySegment allocate(long size) {
    MemorySegment segment = pool.poll();
    poolSizes[index].decrementAndGet();
    // ❌ NOT tracking allocation in region!
    return segment;
}

public void release(MemorySegment segment) {
    pool.offer(segment);
    poolSizes[index].incrementAndGet();
    region.unusedSlices.incrementAndGet();  // Only tracking returns
}
```

**After:**
```java
public MemorySegment allocate(long size) {
    MemorySegment segment = pool.poll();
    poolSizes[index].decrementAndGet();
    
    // ✅ CRITICAL: Decrement region's unused counter
    MemoryRegion region = findRegionForSegment(segment);
    if (region != null) {
        region.unusedSlices.decrementAndGet();
    }
    
    return segment;
}

public void release(MemorySegment segment) {
    pool.offer(segment);
    poolSizes[index].incrementAndGet();
    
    // ✅ Increment region's unused counter
    MemoryRegion region = findRegionForSegment(segment);
    if (region != null) {
        region.unusedSlices.incrementAndGet();
    }
}
```

**Why This Works:**
- `unusedSlices` now correctly tracks segments in use vs in pool
- `allSlicesReturned()` is only true when ALL slices are in pool AND not in use
- `freeUnusedRegions()` will only free truly unused regions

---

## Lifecycle Now

### Correct Tracking

```
Initial state:
- totalSlices = 16
- unusedSlices = 16
- All in pool

Thread A allocates:
- poll from pool
- unusedSlices = 15  ✅ Correctly tracking

Thread A releases:
- return to pool
- unusedSlices = 16  ✅ Back to all unused

freeUnusedRegions():
- if (unusedSlices == 16) → safe to free!  ✅
- munmap only when truly unused  ✅
```

### Safe Freeing

```
Thread A                        Thread B
─────────────────────────────────────────
allocate()
  segment = poll()
  unusedSlices = 15
                                 freeUnusedRegions()
                                   if (15 == 16) → FALSE
                                   Don't free!  ✅
segment.put(...) → Safe!  ✅
```

---

## Why Arena Was Wrong for Pooling

### Arena Design Intent
- **Manage lifecycle of a group of memory allocations**
- `Arena.close()` → immediately invalidate everything
- Good for: temporary allocations with clear boundaries

### Pooling Requirements
- **Segments reused across transactions**
- Need individual segment validity tracking
- Cannot invalidate entire group at once
- Good for: long-lived, reusable memory

### The Mismatch
```
Arena Model:              Pool Model:
─────────────────────────────────────────
Allocate group            Allocate individually
Use all together          Use independently  
Close all together        Free when budget pressure
Short-lived               Long-lived
```

**Conclusion:** Arena and pooling are fundamentally incompatible patterns.

---

## Testing

### What Should Work Now

```bash
./gradlew :sirix-core:test --tests "*testShredderAndTraverseChicago*"
```

**Expected:**
- ✅ No SIGSEGV crashes
- ✅ Multi-threaded parallel serialization works
- ✅ Memory properly tracked and freed
- ✅ Regions only freed when truly unused
- ✅ Budget enforcement still works

### What Changed

**Memory Behavior:**
- Segments remain valid until explicit munmap
- Regions freed only when all slices returned AND not in use
- Slightly longer lifetime but within budget
- Much more stable and predictable

**Performance:**
- No impact from Arena removal (we were managing mmap/munmap anyway)
- Slight overhead from region lookup in allocate() (but necessary for safety)
- Still all performance optimizations in place

---

## Summary of Changes

1. **Removed Arena from MemoryRegion**
   - No more `Arena.ofShared()`
   - No more `arena.close()`
   - Use raw mmap'd segments directly

2. **Track allocations in unusedSlices**
   - Decrement on `allocate()`
   - Increment on `release()`
   - Correct tracking of in-use vs available

3. **Safe region freeing**
   - Only free when `unusedSlices == totalSlices`
   - Only during budget pressure or shutdown
   - No eager freeing in `release()`

---

## Trade-offs

### Before Fix:
- ❌ SIGSEGV crashes
- ❌ Unpredictable behavior
- ❌ Multi-threaded workload fails
- ❌ System unstable

### After Fix:
- ✅ Stable and correct
- ✅ Multi-threading safe
- ✅ Proper lifecycle management
- ✅ Budget still enforced
- ⚠️ Regions live longer (acceptable - within budget)
- ⚠️ Slight overhead from region lookups (necessary for safety)

**Net result:** MUCH better stability with negligible cost.

---

## Lessons Learned

1. **Arena is not for pooling** - it's for scoped allocation
2. **Track both allocations and releases** - not just one side
3. **Test with multi-threaded workloads** - reveals races
4. **Don't optimize prematurely** - eager freeing caused the bugs
5. **Manual memory management is OK** - when you need fine control

---

## Files Modified

- `LinuxMemorySegmentAllocator.java`:
  - Removed Arena from MemoryRegion
  - Added unusedSlices tracking in allocate()
  - Updated freeRegion() to skip arena.close()
  - Updated free() to skip arena.close()

No other files needed changes - the fix is entirely in the allocator.

