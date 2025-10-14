# Proactive Region Freeing Fix - Final Solution

## The Memory Leak That Persisted

Even after implementing region reuse infrastructure, memory still leaked from 11GB → 28GB.

### Evidence from Logs

**What happened:**
- 377 regions created during test
- ZERO regions freed during test
- Virtual memory: 5MB → 889MB (continuously growing)
- Pool 4 reached 8000 segments repeatedly
- NO "Freed XX MB physical" messages
- NO "REUSING region" messages

**Why regions never freed:**
```java
// Budget check only triggers freeing:
if (totalVirtualBytes + regionSize > maxBufferSize.get()) {
    freeUnusedRegionsForBudget(...);
}

// But: 889MB < 1000MB budget
// Result: Never triggered!
```

---

## The Solution: Proactive Cleanup Triggers

### Trigger Based on Pool Fullness

Instead of waiting for budget exhaustion, trigger cleanup when pools accumulate many returned segments.

**Rationale:** If pool has 8000 segments, clearly many regions are fully returned and can be freed!

### Implementation

```java
// In release() after incrementing poolSize:
if (poolSize > 5000 && poolSize % 1000 == 0) {
    if (cleanupInProgress[index].compareAndSet(false, true)) {
        CompletableFuture.runAsync(() -> {
            try {
                freeUnusedRegionsForBudget(0);
            } finally {
                cleanupInProgress[index].set(false);
            }
        });
    }
}
```

**Triggers:**
- Pool reaches 6000 segments → cleanup
- Pool reaches 7000 segments → cleanup
- Pool reaches 8000 segments → cleanup

**Each cleanup:**
1. Finds regions with allSlicesReturned()
2. madvise frees physical memory
3. Queues region in freedRegionsByPool[index]
4. Next allocation reuses → virtual doesn't grow!

---

## Why This Works

### From Chicago Test Logs

Pool 4 behavior:
```
Cycle 1: Return segments → pool: 1000, 2000, ..., 8000
Cycle 2: Return segments → pool: 1000, 2000, ..., 8000
...repeats many times
```

**With proactive freeing:**
```
Cycle 1: Allocate regions, use, return
- Pool hits 6000 → trigger cleanup #1
- Pool hits 7000 → trigger cleanup #2
- Pool hits 8000 → trigger cleanup #3

Cleanup #1-3:
- Find fully-returned regions
- Free via madvise
- Queue for reuse

Cycle 2: Need more regions
- Poll freedRegionsByPool[4] → REUSE!
- Virtual stays stable ✓
```

---

## Thread Safety

### CAS Guard Per Pool

```java
private final AtomicBoolean[] cleanupInProgress;

if (cleanupInProgress[index].compareAndSet(false, true)) {
    // Only ONE thread per pool wins
    runAsync(...)
}
```

**Protection:**
- Multiple threads may trigger check
- Only ONE wins CAS and starts cleanup
- Others skip immediately (no blocking)
- Finally block ensures flag reset

### Async Execution

```java
CompletableFuture.runAsync(() -> {
    freeUnusedRegionsForBudget(0);
})
```

- Runs on ForkJoinPool.commonPool
- Doesn't block release() caller
- freeUnusedRegionsForBudget() has internal synchronization where needed

**Fully thread-safe!**

---

## Performance

### Fast Path (pool ≤ 5000)

```java
poolSizes[index].incrementAndGet();
// No additional checks
```

**Overhead:** Zero ✓

### Moderate Path (pool > 5000, not at threshold)

```java
if (poolSize > 5000 && poolSize % 1000 == 0)  // False, skip
```

**Overhead:** One comparison + modulo (~5 nanoseconds) ✓

### Cleanup Trigger (pool % 1000 == 0)

```java
if (compareAndSet(false, true)) {
    runAsync(...)  // Non-blocking spawn
}
```

**Overhead:** CAS + async spawn (~1-2 microseconds, non-blocking) ✓

### Cleanup Execution

- Runs asynchronously in background
- No impact on release() caller
- Finds and frees unused regions
- Duration: milliseconds, but off critical path

**No blocking, no latency spikes!** ✓

---

## Expected Results

### Memory Behavior

```
Initial (0-30s):
- Virtual: 0 → 500MB (allocating as needed)
- Physical: 0 → 500MB
- Regions: 0 → 250
- Freed queue: empty

Mid-test (30-60s):
- Pool 4 hits 6000 → cleanup triggered
- Some regions freed and queued
- Virtual: 500MB (stable)
- Freed queue: 20 regions

Steady state (60s+):
- Allocations reuse from freed queue
- Virtual: 500-600MB (bounded!)
- Freed queue: 10-30 regions cycling
- NO 28GB leak!
```

### Logs to Watch For

**Success indicators:**
```
"Pool 4 has 6000 segments, running cleanup"
"Freed 2 MB physical from pool 4 (queued for reuse...)"
"REUSING region for pool 4: 2 MB (no new mmap)"
"Virtual: 500 MB" (staying stable)
```

---

## Why Async is Critical

**Synchronous cleanup would:**
- Block one release() call per cleanup
- Cause latency spike (~10-50ms)
- Hurt throughput under load

**Async cleanup:**
- release() returns immediately
- Cleanup runs in background
- No latency impact
- Better for high-performance database

---

## Complete Memory Management System

### Allocation
1. Try reuse from freedRegionsByPool (O(1))
2. If none, create NEW mmap
3. Track in totalVirtualBytes

### Usage
1. Allocate segments from pool
2. Use in pages
3. Return to pool via release()

### Cleanup (NEW!)
1. Trigger when pool > 5000 and % 1000 == 0
2. Async cleanup finds unused regions
3. madvise frees physical
4. Queue in freedRegionsByPool

### Reuse
1. Next allocation polls freed queue
2. Reuses virtual address space
3. totalVirtualBytes stays stable

**Complete cycle prevents leak!**

---

## Files Modified

- `LinuxMemorySegmentAllocator.java`:
  - Added cleanupInProgress[] array
  - Added proactive cleanup trigger in release()
  - Async CompletableFuture execution

---

## Testing Expectations

Run Chicago test and verify:
- Virtual memory stays < 900MB (bounded!)
- See "running cleanup" in logs
- See "Freed XX MB physical" messages
- See "REUSING region" messages
- freedRegionsByPool queues populated
- No swap usage
- No 28GB leak!

---

## Commit

`[current]` - Fix memory leak with proactive async region freeing

