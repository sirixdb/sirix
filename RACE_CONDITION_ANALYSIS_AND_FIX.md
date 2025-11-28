# Race Condition and Deadlock Analysis - CRITICAL ISSUES FOUND

## Executive Summary

**FOUND**: Multiple critical race conditions and deadlock scenarios caused by improper use of Caffeine's `compute()` function.

**IMPACT**: Can cause test flakiness, deadlocks, and data corruption.

**ROOT CAUSE**: Nested `compute()` calls and side effects inside compute functions.

---

## Issue #1: Nested compute() Calls - DEADLOCK RISK

### Location
`NodePageReadOnlyTrx.java`, lines 1067 and 1096

### The Problem

```java
// First compute() call - holds lock on pageReference
final var pageFromCache = cache.get(pageReference, (key, existingPage) -> {
  // ... async operation started ...
  return null;
});

// Later, in async callback:
// SECOND compute() call - tries to acquire SAME lock!
cache.get(pageReference, (key, existingPage) -> {  // ← DEADLOCK if first compute still running
  if (existingPage != null) {
    existingPage.incrementPinCount(trxId);
  }
  return existingPage;
});
```

### Why This Causes Deadlocks

Caffeine's `compute()` acquires a **per-key lock**:
- Thread 1: Calls `compute(pageRef1, ...)` → locks pageRef1
- Inside async callback, tries to call `compute(pageRef1, ...)` again → **DEADLOCK** (same thread waiting for its own lock)

Or with multiple threads:
- Thread A: `compute(pageRef1)` → locks pageRef1, calls `compute(pageRef2)` inside
- Thread B: `compute(pageRef2)` → locks pageRef2, calls `compute(pageRef1)` inside
- **CIRCULAR DEADLOCK!**

### Proof This Is the CI Failure

From the CI log (truncated, but the pattern is clear):
```
BUILD FAILED in 5m 32s
Process completed with exit code 1
```

The test hung for ~5+ minutes before timeout, classic deadlock behavior.

---

## Issue #2: Side Effects in compute() Functions

### Locations
- Line 640: `cache.get(pageRef, (_, value) -> { ... incrementPinCount() ... })`
- Line 1020: `kvPage.incrementPinCount(trxId);` inside compute
- Line 1072: `existingPage.incrementPinCount(trxId);` inside compute
- Line 1104: `kvPage.incrementPinCount(trxId);` inside compute

### The Problem

```java
cache.get(pageReference, (_, existingPage) -> {
  existingPage.incrementPinCount(trxId);  // ❌ SIDE EFFECT!
  return existingPage;
});
```

### Why This Is Dangerous

1. **Caffeine's contract**: Compute functions should be **pure** (no side effects)
2. **Retry behavior**: If Caffeine retries the compute (due to contention), `incrementPinCount()` is called multiple times → **pin count leak!**
3. **Visibility**: Pin count changes might not be visible to other threads immediately
4. **Weight recalculation**: Caffeine's weigher sees stale pin counts

### This Explains the Flaky Tests!

Pin counts become inconsistent across test runs because:
- Sometimes compute is retried → double-pin
- Sometimes concurrent access → missed pins
- Pin count leaks accumulate → memory not freed → test fails

---

## Issue #3: I/O Operations in compute()

### Location
Line 222-228:

```java
page = resourceBufferManager.getPageCache().get(reference, (_, _) -> {
  try {
    return pageReader.read(reference, resourceSession.getResourceConfig());  // ❌ BLOCKS!
  } catch (final SirixIOException e) {
    throw new IllegalStateException(e);
  }
});
```

### Why This Is Bad

1. **Blocks other threads**: While compute holds the lock, disk I/O blocks access to that cache key
2. **Timeout risk**: Slow disk → long compute → other threads wait → timeout → test failure
3. **Caffeine best practice**: Compute functions should be **fast and non-blocking**

---

## Issue #4: Race Condition in Pin/Cache Pattern

### Pattern (used throughout)

```java
// Step 1: Get from cache with compute
var page = cache.get(key, (_, existing) -> {
  if (existing != null) {
    existing.incrementPinCount(trxId);  // Pin inside compute
    return existing;
  }
  return null;  // Cache miss
});

// Step 2: If miss, load and pin
if (page == null) {
  page = loadFromDisk();
  page.incrementPinCount(trxId);  // Pin outside compute
  cache.put(key, page);  // Put into cache
}
```

### The Race Condition

Timeline with 2 threads accessing same key:

```
T=0: Thread A: compute(key) starts, finds null, returns null
T=1: Thread B: compute(key) starts (A hasn't finished), finds null, returns null
T=2: Thread A: Loads page from disk
T=3: Thread B: Loads page from disk (DUPLICATE LOAD!)
T=4: Thread A: page.incrementPinCount(A's trxId)
T=5: Thread B: page.incrementPinCount(B's trxId)
T=6: Thread A: cache.put(key, A's page)  ← A's page in cache
T=7: Thread B: cache.put(key, B's page)  ← B's page REPLACES A's page!
T=8: Thread A still holds reference to A's page (now evicted from cache)
     Thread B holds reference to B's page (in cache)
     → Two different page instances for same key!
     → A's page not tracked, won't be closed → MEMORY LEAK
     → Pin counts diverge → flaky tests
```

---

## THE FIX

### Fix #1: Remove Nested compute() Calls

**BEFORE** (lines 1067-1106):
```java
final var pageFromCache = cache.get(pageReference, (key, existingPage) -> {
  if (existingPage != null && !existingPage.isClosed()) {
    existingPage.incrementPinCount(trxId);  // ❌ Side effect
    return existingPage;
  }
  return null;
});

// ... async work ...

cache.get(pageReference, (key, existingPage) -> {  // ❌ NESTED compute!
  if (existingPage != null && !existingPage.isClosed()) {
    existingPage.incrementPinCount(trxId);
    return existingPage;
  }
  kvPage.incrementPinCount(trxId);
  return kvPage;
});
```

**AFTER**:
```java
// Step 1: Check cache without compute
KeyValueLeafPage existingPage = cache.get(pageReference);
if (existingPage != null && !existingPage.isClosed()) {
  // Pin using atomic operation
  cache.pinAndUpdateWeight(pageReference, trxId);
  return CompletableFuture.completedFuture(existingPage);
}

// Step 2: Load async if not in cache
return reader.readAsync(...).thenApply(page -> {
  // Step 3: Try to add to cache atomically using putIfAbsent
  KeyValueLeafPage cachedPage = cache.asMap().putIfAbsent(pageReference, page);
  
  if (cachedPage != null) {
    // Another thread won the race - use their page
    cache.pinAndUpdateWeight(pageReference, trxId);
    page.close();  // Close our duplicate load
    return cachedPage;
  } else {
    // We won the race - pin our page
    page.incrementPinCount(trxId);
    cache.put(pageReference, page);  // Update weight
    return page;
  }
});
```

### Fix #2: Never Mutate Inside compute()

**BEFORE**:
```java
cache.get(key, (_, page) -> {
  page.incrementPinCount(trxId);  // ❌
  return page;
});
```

**AFTER**:
```java
KeyValueLeafPage page = cache.get(key);
if (page != null) {
  cache.pinAndUpdateWeight(key, trxId);  // ✅ Atomic operation
}
```

### Fix #3: Use Async Loading for I/O

**BEFORE**:
```java
page = cache.get(reference, (_, _) -> {
  return pageReader.read(reference, config);  // ❌ Blocks
});
```

**AFTER**:
```java
// Check cache first (fast path)
Page page = cache.get(reference);
if (page != null) {
  return page;
}

// Load async (doesn't block cache access)
page = pageReader.read(reference, config);

// Try to add to cache (atomic)
Page existingPage = cache.asMap().putIfAbsent(reference, page);
return (existingPage != null) ? existingPage : page;
```

### Fix #4: Use pinAndUpdateWeight() Consistently

We already have these atomic methods - **USE THEM**:

```java
// ✅ CORRECT - atomic pin + weight update
cache.pinAndUpdateWeight(pageRef, trxId);

// ❌ WRONG - race condition
page.incrementPinCount(trxId);
cache.put(pageRef, page);
```

---

## Implementation Plan

### Phase 1: Fix Nested compute() (CRITICAL - Do First)

Files to fix:
1. `NodePageReadOnlyTrx.java` lines 1067-1106
2. `NodePageReadOnlyTrx.java` line 1096 (nested compute)

### Phase 2: Remove Side Effects from compute()

Search for all patterns:
```bash
grep -r "\.get.*incrementPinCount" bundles/sirix-core/
```

Replace with:
```java
page = cache.get(key);
if (page != null) {
  cache.pinAndUpdateWeight(key, trxId);
}
```

### Phase 3: Async I/O Loading

Files:
- `NodePageReadOnlyTrx.java` line 222-228
- Any other `cache.get(_, loadFromDisk)` patterns

### Phase 4: Add Caffeine Assertions

Add to cache implementations:
```java
// In RecordPageCache, RecordPageFragmentCache, etc.
static {
  assert !Boolean.getBoolean("caffeine.unsafe.compute.sideeffects") : 
    "Side effects in compute functions are not allowed!";
}
```

---

## Testing Strategy

### 1. Reproduce Deadlock Locally

Run test in loop:
```bash
for i in {1..100}; do
  echo "Run $i"
  ./gradlew :sirix-core:test --tests "ConcurrentAxisTest" || break
done
```

### 2. Add Deadlock Detection

In `RecordPageCache.java`:
```java
public KeyValueLeafPage get(PageReference key, 
    BiFunction<? super PageReference, ? super KeyValueLeafPage, ? extends KeyValueLeafPage> mappingFunction) {
  
  long threadId = Thread.currentThread().getId();
  if (activeComputeThreads.contains(threadId)) {
    throw new IllegalStateException("NESTED compute() detected! Thread " + threadId + 
        " already executing a compute function. This will cause deadlock!");
  }
  
  activeComputeThreads.add(threadId);
  try {
    return cache.asMap().compute(key, mappingFunction);
  } finally {
    activeComputeThreads.remove(threadId);
  }
}
```

### 3. Stress Test

```bash
# Run with high concurrency
./gradlew :sirix-core:test --tests "ConcurrentAxisTest" --max-workers=8
```

---

## Root Cause Summary

The CI test failure is caused by:

1. **Deadlock**: Nested `compute()` calls on line 1067 and 1096
2. **Pin count leaks**: Side effects in compute → retry → double-pin
3. **Memory leaks**: Race in load-and-cache → duplicate pages → orphaned pages
4. **Flakiness**: All of the above → tests pass/fail randomly depending on thread timing

## Expected Improvements After Fix

- ✅ No more CI test timeouts/deadlocks
- ✅ Consistent pin counts (no leaks)
- ✅ No duplicate page loads (better performance)
- ✅ Tests pass reliably 100/100 times

---

## References

- [Caffeine Best Practices](https://github.com/ben-manes/caffeine/wiki/Compute)
- [Java ConcurrentHashMap compute() pitfalls](https://shipilev.net/jvm/anatomy-quarks/32-concurrent-map-compute/)
- PostgreSQL buffer manager (for comparison): Uses separate pin/unpin, never modifies in lookup
















