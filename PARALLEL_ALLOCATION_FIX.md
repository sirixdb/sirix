# Parallel Allocation Fix - Pool Still Empty Error

## Problem: Pool Empty After Allocation

Error occurring in parallel serialization:
```
java.lang.IllegalStateException: Pool still empty after region allocation!
at io.sirix.cache.LinuxMemorySegmentAllocator.allocate(LinuxMemorySegmentAllocator.java:252)
```

---

## Root Cause: Insufficient Segments for Parallel Workload

### The Math Problem

With **1MB regions** and **large segment sizes**:

| Segment Size | Slices per 1MB Region |
|--------------|----------------------|
| 4KB | 256 slices ✅ |
| 8KB | 128 slices ✅ |
| 16KB | 64 slices ✅ |
| 32KB | 32 slices ✅ |
| 64KB | 16 slices ⚠️ |
| 128KB | **8 slices** ❌ |
| 256KB | **4 slices** ❌ |

### The Race in Parallel Serialization

**Scenario:** 16 threads in ForkJoinPool.commonPool processing pages in parallel

```
Time  Thread  Action                          Pool State
──────────────────────────────────────────────────────────
T0    All     Call allocate(128KB)            Pool: empty
T1    T1      synchronized(pool) - wins       Pool: empty
T2    T1      allocateNewRegion()             Pool: adding 8 slices
T3    T1      pool.poll() → success           Pool: 7 slices
T3    T2-T8   poll() → success                Pool: 0 slices
T4    T9      synchronized(pool) - wins       Pool: empty
T5    T9      allocateNewRegion()             Pool: adding 8 slices
T6    T9      poll() → success                Pool: 7 slices
T6    T10-16  poll() → success                Pool: 0 slices
T7    T17     poll() → null
T8    T17     synchronized(pool)
T9    T17     allocateNewRegion()             Pool: adding 8 slices
T10   T18     (waiting for lock)
T11   T17     poll() → success                Pool: 7 slices
T12   T18     (gets lock, double-checks)
T13   T18     poll() → might get one OR...
              OTHER THREADS DRAIN IT!
T14   T18     poll() → null ❌
              "Pool still empty!"
```

The problem: **8 slices per region isn't enough for 16+ parallel threads!**

---

## The Solution: Batch Allocate for Parallelism

### Estimate Parallelism

```java
int parallelism = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
```

For a typical laptop:
- 8 cores = 16 threads
- With buffer: 16-32 segments needed

### Allocate Multiple Regions

```java
int slicesPerRegion = (int) (regionSize / segmentSize);
int regionsToAllocate = Math.max(1, (parallelism + slicesPerRegion - 1) / slicesPerRegion);

for (int r = 0; r < regionsToAllocate; r++) {
    // mmap and slice region
    // Add all slices to pool
}
```

**Example:** 128KB segments, 16 threads needed
- 1MB / 128KB = 8 slices per region
- 16 threads / 8 slices = 2 regions
- **Allocate 2 regions = 16 slices** ✅

---

## Before vs After

### Before (Single Region)
```
Pool empty, 16 threads waiting
Thread 1: Allocate 1 region = 8 slices
Threads 1-8: Get segments ✅
Threads 9-16: Pool empty! ❌
```

### After (Multi-Region)
```
Pool empty, 16 threads waiting
Thread 1: Allocate 2 regions = 16 slices  
Threads 1-16: Get segments ✅
All satisfied!
```

---

## Impact

### Memory Usage
With parallelism = 16:

| Segment Size | Regions Allocated | Memory per Batch |
|--------------|-------------------|------------------|
| 4KB | 1 (256 > 16) | 1 MB |
| 64KB | 1 (16 ≥ 16) | 1 MB |
| 128KB | 2 (8 < 16) | 2 MB |
| 256KB | 4 (4 < 16) | 4 MB |

**Net result:** Allocate enough upfront to avoid multiple synchronization points.

### Performance
- ✅ No "pool still empty" errors
- ✅ Parallel threads satisfied in one batch
- ✅ Less lock contention (allocate once vs many times)
- ⚠️ Slightly more memory per allocation (but still within budget)

---

## Why This Works

1. **Synchronized allocation** - only one thread allocates at a time
2. **Batch allocation** - allocate for all parallel threads at once
3. **Double-check locking** - fast path when segments available
4. **Conservative estimate** - better to over-allocate than under-allocate

---

## Alternative Considered: Increase Region Size

We could have increased region size back to 4MB or 64MB, but:
- ❌ Would use too much memory on laptop (freezing issue returns)
- ❌ All-or-nothing approach

Current solution (batch small regions) is better:
- ✅ Gradual allocation (still 1MB per region)
- ✅ Allocate exactly what's needed for parallelism
- ✅ Better memory usage than pre-allocation

---

## Testing

The Chicago test should now:
- ✅ Complete without "Pool still empty" errors
- ✅ Allocate 2-4 regions at once for large segments
- ✅ Single region for small segments (sufficient slices)
- ✅ Handle parallel workloads correctly

---

## Files Modified

- `LinuxMemorySegmentAllocator.java`:
  - Updated `allocateNewRegion()` to accept `minSegmentsNeeded`
  - Allocate multiple regions based on parallelism
  - Added logging for debugging

---

## Commit

`ff7afb5cd` - Fix parallel allocation race (double-check locking)
`[next]` - Fix batch allocation for parallel workloads

