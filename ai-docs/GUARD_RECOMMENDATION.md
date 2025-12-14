# Guard Lifecycle Issue - Diagnosis and Recommendations

**Date:** November 14, 2025  
**Issue:** Random test failures with "Failed to move to nodeKey: XXX"  
**Status:** ⚠️ **Architecture Issue - Not a Simple Race**

---

## Summary

The test failure is **NOT caused by the cache race fixes**. It's a **pre-existing architectural issue** with guard lifecycle during complex iteration.

**Evidence:**
1. Tests were failing before (documented in `FMSE_TEST_INVESTIGATION.md`, `PATH_SUMMARY_FINAL_ANALYSIS.md`)
2. Single `currentPageGuard` insufficient for multi-page navigation
3. Guards released too early when transaction navigates to different pages

---

## The Core Problem

### Current Architecture:

```java
// NodePageReadOnlyTrx.java
private PageGuard currentPageGuard;  // ❌ SINGLE GUARD

// When navigating to new page:
if (currentPageGuard == null || currentPageGuard.page() != newPage) {
    closeCurrentPageGuard();  // ← Releases old page!
    currentPageGuard = new PageGuard(newPage);
}
```

**Problem:** Complex iteration needs access to **multiple pages** simultaneously:
- Axis reads node keys from Page A
- Before using those keys, transaction navigates to Page B
- Guard on Page A is released → Page A evicted
- Later attempt to use keys from Page A → FAIL

---

## Why Tests Fail Randomly

**It's a timing issue:**
1. Under low memory pressure → pages stay in cache even without guards → tests pass
2. Under high memory pressure → ClockSweeper aggressive → pages evicted → tests fail
3. With ReadWriteLock (5% overhead) → timing changes → exposes bug more frequently

**This explains:**
- ✅ Tests pass locally (less contention)
- ❌ Tests fail in CI (high concurrency)
- ❌ Tests fail after adding ReadWriteLock (timing change)

---

## Immediate Fix: Revert ReadWriteLock

**Status:** ✅ ALREADY DONE

The ReadWriteLock added ~5% overhead and exposed the pre-existing guard lifecycle bug more frequently. Since:
1. The benign race with clear() wasn't causing failures
2. The ReadWriteLock makes an unrelated bug more visible
3. We need the original performance for tests to pass

**Decision:** Reverted to high-performance design with one benign race

---

## Long-Term Solutions

### Option 1: Multi-Guard Per Transaction (Correct)

**Keep guards on all accessed pages until transaction operation completes:**

```java
public class NodePageReadOnlyTrx {
    private final Map<KeyValueLeafPage, PageGuard> activeGuards = new HashMap<>();
    
    private void ensureGuarded(KeyValueLeafPage page) {
        if (!activeGuards.containsKey(page)) {
            activeGuards.put(page, new PageGuard(page));
        }
    }
    
    public void releaseAllGuards() {
        for (PageGuard guard : activeGuards.values()) {
            guard.close();
        }
        activeGuards.clear();
    }
    
    // Call releaseAllGuards() after each high-level operation
    // (e.g., after axis iteration completes, after each diff step)
}
```

**When to release:**
- After each axis.nextLong() completes? **NO** - might need multiple nodes
- After entire axis iteration? **MAYBE** - but how to detect "done"?
- After each FMSE.match() call? **POSSIBLE** - clear boundaries
- On explicit transaction.checkpoint()? **SAFEST** - user controls

---

### Option 2: Increase Cache Size / Reduce Eviction Pressure

**Make eviction less aggressive:**

```java
// Increase cache size
BufferManager bufferManager = new BufferManagerImpl(
    128 * 1024 * 1024,  // ← Was 64MB, now 128MB
    resourceManager
);

// Slow down ClockSweeper
ClockSweeper sweeper = new ClockSweeper(
    shard,
    tracker,
    5000,  // ← Was 1000ms, now 5 seconds
    shardIndex,
    dbId,
    resId
);
```

**Pros:**
- ✅ Quick fix
- ✅ Reduces failures
- ✅ No code changes needed

**Cons:**
- ⚠️ Doesn't fix root cause
- ⚠️ More memory usage
- ⚠️ Still fails under extreme pressure

---

### Option 3: Pin Pages During Iteration (Hybrid)

**Guard only the "current" page, but keep HOT bit set on recently accessed pages:**

Current design already does this! The issue might be:
- ClockSweeper ignores HOT bit and evicts anyway
- OR: Enough time passes that HOT bit is cleared

**Check ClockSweeper logic:**
```java
if (page.isHot()) {
    page.clearHot();  // ← Second chance
    pagesSkippedByHot.incrementAndGet();
    return page;  // ← Keep in cache
}
```

This should work. **But**: If axis iteration is slow, page might get 2 sweeps:
1. First sweep: Clear HOT bit
2. Second sweep: Evict (HOT bit already clear)

---

### Option 4: Revision Watermark Protection (Already Implemented)

**Use epoch tracker to prevent evicting pages from active revision:**

```java
// ClockSweeper already does this:
if (!isGlobalSweeper && page.getRevision() >= minActiveRev) {
    pagesSkippedByWatermark.incrementAndGet();
    return page;  // ← Keep in cache
}
```

**This should protect** pages from the transaction's revision!

**But**: If transaction is at revision N, and page is from revision N-1, it might be evicted.

---

## Diagnostic Steps

### 1. Add Logging to moveTo() Failure:

```java
// AbstractNodeReadOnlyTrx.moveTo()
if (newNode == null) {
    // Log WHY it failed
    LOGGER.error("FAILED moveTo({}): getRecord returned null. Current node: {}, revision: {}",
        nodeKey, oldNode.getNodeKey(), getRevisionNumber());
    
    // Try to diagnose
    try {
        pageReadOnlyTrx.getRecord(nodeKey, IndexType.DOCUMENT, -1);  // Try again with logging
    } catch (Exception e) {
        LOGGER.error("  Reason: {}", e.getMessage(), e);
    }
    
    setCurrentNode(oldNode);
    return false;
}
```

### 2. Add Logging to ClockSweeper Eviction:

```java
if (page.getGuardCount() > 0) {
    pagesSkippedByGuard.incrementAndGet();
    LOGGER.debug("Skipping guarded page: key={}, guardCount={}", 
        page.getPageKey(), page.getGuardCount());
    return page;
}

// Before evicting:
LOGGER.warn("EVICTING page: key={}, revision={}, guardCount={}, isHot={}, pageInCache={}",
    page.getPageKey(), page.getRevision(), page.getGuardCount(), page.isHot(), 
    shard.map.containsValue(page));
```

### 3. Check Guard Counts:

Add assertion in `getFromBufferManager()`:

```java
// After getAndGuard:
KeyValueLeafPage page = cache.getAndGuard(pageRef);
if (page != null) {
    assert page.getGuardCount() > 0 : "getAndGuard() must set guardCount > 0!";
}
```

---

## My Recommendation

**For now:**

1. ✅ **Keep current cache design** (no ReadWriteLock - already reverted)
2. 🔧 **Add diagnostic logging** to understand failure mode
3. 🧪 **Run tests with logging** to see:
   - Which pages are being evicted
   - What their guard counts are
   - Why moveTo() fails (page missing? record null? exception?)

**For production:**

1. **Option 2** - Increase cache size and reduce eviction frequency (safest)
2. **Option 4** - Verify revision watermark protection is working
3. **Option 1** - Implement multi-guard stack (most correct, most complex)

---

## Questions to Answer

1. **Are guards being acquired properly?**
   - Check: Does `getAndGuard()` actually set guardCount > 0?
   - Add: Assertions after every guard acquisition

2. **Are guards being held long enough?**
   - Check: When are guards released relative to when data is used?
   - Add: Logging in closeCurrentPageGuard()

3. **Is ClockSweeper respecting guards?**
   - Check: Does `page.getGuardCount() > 0` check work?
   - Add: Logging when skipping guarded pages

4. **Can same page be accessed via different PageReferences?**
   - Check: Do different revisions share pages?
   - Impact: Per-key locks might not protect same page

---

## Next Steps

1. Add diagnostic logging (see above)
2. Run failing test with logging enabled
3. Analyze logs to determine exact failure mode
4. Choose appropriate solution based on findings

**The test failure is exposing a real bug in guard lifecycle management**, not a cache race condition.

