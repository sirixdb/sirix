# Virtual Memory Leak Fix - Complete Solution

## The Critical Bug

### Symptom
Memory growing from 10GB to 28GB of 32GB RAM, swap running full.

### Root Cause
```java
// Old code:
totalMappedBytes.addAndGet(regionSize);        // On allocation
totalMappedBytes.addAndGet(-freedBytes);       // On madvise free
if (totalMappedBytes.get() + regionSize > budget) { ... }  // Budget check
```

**The Problem:**
- `totalMappedBytes` tracked physical memory
- madvise freed physical → `totalMappedBytes` decremented
- Budget check used `totalMappedBytes` for virtual allocation decision
- Virtual address space kept growing!

**Timeline:**
```
Allocate 100 regions: virtual=200MB, physical=200MB, totalMappedBytes=200MB
madvise 50 regions:   virtual=200MB, physical=100MB, totalMappedBytes=100MB ← Bug!
Budget check:         100MB < 1GB → Can allocate more!
Allocate 100 more:    virtual=400MB, physical=200MB, totalMappedBytes=200MB
Repeat indefinitely → Virtual grows to 28GB!
```

---

## The Solution

### 1. Separate Virtual and Physical Tracking

```java
private final AtomicLong totalVirtualBytes = new AtomicLong(0);   // Virtual address space
private final AtomicLong totalPhysicalBytes = new AtomicLong(0);  // Physical RAM
```

**Virtual:** Only grows on NEW mmap, never decremented on madvise
**Physical:** Decremented on madvise, incremented on reuse

### 2. Region Reuse with Freed Queues

```java
@SuppressWarnings("unchecked")
private final ConcurrentLinkedQueue<MemoryRegion>[] freedRegionsByPool = 
    new ConcurrentLinkedQueue[SEGMENT_SIZES.length];
```

**Per-pool queues:**
- Pool 4 freed regions only in `freedRegionsByPool[4]`
- O(1) exact-match reuse (no unsuitable regions)
- Lock-free ConcurrentLinkedQueue

### 3. Region State Tracking

```java
final AtomicBoolean isPhysicallyMapped;  // In MemoryRegion

// On free:
isPhysicallyMapped.set(false);
freedRegionsByPool[poolIndex].offer(region);

// On reuse:
if (region.isPhysicallyMapped.compareAndSet(false, true)) {
    // Reuse successful, only ONE thread wins
}
```

---

## Implementation Details

### allocateNewRegion() - Reuse First

```java
// TRY TO REUSE: O(1) poll from freed queue
MemoryRegion candidate = freedRegionsByPool[poolIndex].poll();
if (candidate != null && candidate.isPhysicallyMapped.compareAndSet(false, true)) {
    region = candidate;
    // Virtual stays same!
    LOGGER.info("REUSING region (no new mmap)");
}

if (region == null) {
    // Allocate NEW
    region = new MemoryRegion(...);
    totalVirtualBytes.addAndGet(regionSize);  // Virtual grows ONLY here
}

// Re-slice and add to pool
totalPhysicalBytes.addAndGet(regionSize);  // Physical allocated
```

### freeRegion() - Queue for Reuse

```java
// madvise to release physical
releasePhysicalMemory(region.baseSegment, region.byteSize());

// Mark as freed and queue
region.isPhysicallyMapped.set(false);
freedRegionsByPool[region.poolIndex].offer(region);

// Update ONLY physical (virtual unchanged!)
totalPhysicalBytes.addAndGet(-freedBytes);
```

### Budget Check - Use Virtual

```java
long currentVirtual = totalVirtualBytes.get();

if (currentVirtual + regionSize > maxBufferSize.get()) {
    freeUnusedRegionsForBudget(regionSize);
    
    // Can proceed if we have reusable regions (virtual won't grow)
    if (totalVirtualBytes.get() + regionSize > maxBufferSize.get() && 
        freedRegionsByPool[index].isEmpty()) {
        throw OOM;  // No reuse possible, would exceed virtual budget
    }
}
```

---

## Why This Fixes the Leak

### Virtual Memory Tracking

```
Operation              Virtual    Physical   totalVirtual  totalPhysical
─────────────────────────────────────────────────────────────────────────
Allocate NEW region    +200MB     +200MB     +200MB        +200MB
madvise free           200MB      0MB        200MB         -200MB  ✓
Reuse region           200MB      +200MB     200MB         +200MB  ✓
```

**Virtual only grows on NEW mmap → Leak fixed!**

### Budget Enforcement

```java
if (totalVirtualBytes + regionSize > budget && !canReuse) {
    throw OOM;  // Prevents virtual growth beyond budget
}
```

Virtual is now properly constrained!

---

## Thread Safety

### Lock-Free Operations
- `freedRegionsByPool[i].poll()` - wait-free ✓
- `freedRegionsByPool[i].offer()` - wait-free ✓
- `isPhysicallyMapped.compareAndSet()` - atomic ✓
- `totalVirtualBytes.addAndGet()` - atomic ✓

### Race Prevention
```java
// Multiple threads try to reuse same region:
Thread A: compareAndSet(false, true) → TRUE (wins)
Thread B: compareAndSet(false, true) → FALSE (loses, must allocate new)
```

Only ONE thread can reuse each region ✓

---

## Performance

### Reuse Path
```java
region = freedRegionsByPool[poolIndex].poll();  // O(1), no lock
```

**vs alternatives:**
- Global queue with filtering: O(n) ❌
- Synchronized iteration: O(n) + lock ❌
- Per-pool queue: O(1), lock-free ✓

**Optimal!**

---

## Expected Behavior

### Memory Pattern

```
Initial:
- Virtual: 0 MB, Physical: 0 MB, Active: 0, Freed: 0

After initial load:
- Virtual: 500 MB, Physical: 500 MB, Active: 250, Freed: 0

After freeing 100 regions:
- Virtual: 500 MB (stable!), Physical: 300 MB, Active: 250, Freed: 100

After reusing 50 regions:
- Virtual: 500 MB (stable!), Physical: 400 MB, Active: 250, Freed: 50

Steady state:
- Virtual: 500-600 MB (bounded!)
- Physical: fluctuates based on usage
- No 28GB leak!
```

### Logs to Watch

```
"NEW mmap for pool X" ← Initial allocation
"REUSING region for pool X" ← Steady state
"Virtual budget pressure" ← If hitting limit
```

---

## Files Modified

- `LinuxMemorySegmentAllocator.java` - Complete rewrite of tracking and reuse logic

---

## Commits

`[current]` - Fix critical virtual memory leak with proper region reuse

---

## Testing

Run Chicago test and monitor:
- Virtual memory should stabilize < 1GB
- No swap usage
- See "REUSING" messages in logs
- activeRegions.size() should stabilize
- Performance should be good (madvise + reuse both fast)

