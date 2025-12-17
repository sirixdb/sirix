# Guard Lifecycle Bug: Pages Evicted During Iteration

**Date:** November 14, 2025  
**Status:** ⚠️ **CRITICAL BUG IDENTIFIED**

---

## The Problem

**Test Failure:** `Failed to move to nodeKey: 1024` in FMSE diff tests

**Root Cause:** Guards are released too early during complex iteration, allowing ClockSweeper to evict pages that are still needed.

---

## Bug Timeline

### Step-by-Step Failure:

```
1. Axis.hasNext() is called
   ├─> nextKey() is called
   ├─> DescendantAxis.nextKey() called
   ├─> cursor.getFirstChildKey() 
   │   └─> Reads from MemorySegment on Page A
   │   └─> currentPageGuard protects Page A ✓
   └─> Returns key = 1024

2. hasNext() returns true

3. ❌ BEFORE nextLong() IS CALLED:
   FMSE code calls other axis operations or navigates elsewhere
   ├─> Transaction navigates to node on Page B
   ├─> getRecordPage() fetches Page B
   ├─> closeCurrentPageGuard()  
   │   └─> Releases guard on Page A!  ❌
   └─> currentPageGuard = new PageGuard(Page B)

4. ⚠️  Page A is now UNGUARDED (guardCount = 0)

5. ClockSweeper runs:
   ├─> Finds Page A with guardCount = 0
   ├─> Evicts Page A
   ├─> page.reset() - clears all data
   └─> map.remove(refA)

6. Caller: axis.nextLong()
   ├─> Tries to moveTo(1024)
   ├─> Calls getRecord(1024, DOCUMENT, -1)
   ├─> getRecordPage() loads page for node 1024
   ├─> BUT: Page was reset or wrong version loaded
   └─> ❌ Returns null → "Failed to move to nodeKey: 1024"
```

---

## Root Cause Analysis

### The Fundamental Issue:

**Single `currentPageGuard` is insufficient for multi-page iteration**

```java
// NodePageReadOnlyTrx.java
private PageGuard currentPageGuard;  // ❌ ONLY ONE GUARD!

private void closeCurrentPageGuard() {
    if (currentPageGuard != null) {
        currentPageGuard.close();  // Releases guard
        currentPageGuard = null;
    }
}
```

**Problem:**
- Guards protect **one page at a time**
- Complex iteration spans **multiple pages**
- When transaction moves to new page, old guard is released
- Old page becomes eligible for eviction
- But caller still needs data from old page!

---

## Why This Happens

### Axis Iteration Pattern:

```java
// DescendantAxis.nextKey()
protected long nextKey() {
    NodeCursor cursor = getCursor();
    
    // Read structural data from CURRENT node (Page A guarded)
    if (cursor.hasFirstChild()) {
        long key = cursor.getFirstChildKey();  // ← Reads from MemorySegment
        
        if (cursor.hasRightSibling()) {
            rightSiblingKeyStack.add(cursor.getRightSiblingKey());  // ← More reads!
        }
        
        return key;  // ← Returns 1024
    }
    // ...
}
```

**The issue:**
1. `getFirstChildKey()` reads `FIRST_CHILD_KEY_HANDLE.get(segment, 0L)` - from MemorySegment
2. Value is returned (e.g., 1024)
3. **Value is copied to Java long** - should be safe from eviction
4. Later, `moveTo(1024)` is called
5. `moveTo()` calls `getRecord(1024)` which loads the page containing node 1024
6. **But**: If that page was evicted and reset, getRecord() returns null

---

## Why moveTo() Fails

### The moveTo() Flow:

```java
// AbstractNodeReadOnlyTrx.moveTo() - line 239-268
public boolean moveTo(final long nodeKey) {
    final N oldNode = currentNode;
    DataRecord newNode;
    
    try {
        // Fetch the node
        newNode = pageReadOnlyTrx.getRecord(nodeKey, IndexType.DOCUMENT, -1);
    } catch (final SirixIOException | UncheckedIOException | IllegalArgumentException e) {
        newNode = null;
    }
    
    if (newNode == null) {  // ❌ Page was evicted!
        setCurrentNode(oldNode);
        return false;
    }
    //...
}
```

**Why getRecord() returns null:**

Option 1: **Page evicted and not reloaded properly**
- ClockSweeper evicted the page
- getRecordPage() tries to load it
- Cache lookup returns null (evicted)
- Disk load fails or returns wrong page

Option 2: **Page reset but still in cache**
- ClockSweeper called page.reset()
- Page still in cache but data cleared
- getValue() reads reset page → returns null

---

## The Real Bug: Missing Re-Guard After Navigation

Looking at line 748-757 in NodePageReadOnlyTrx:

```java
// Wrap the already-guarded page (guard was acquired by getAndGuard())
if (currentPageGuard == null || currentPageGuard.page() != recordPageFromBuffer) {
    closeCurrentPageGuard();  // ❌ Releases old page!
    currentPageGuard = PageGuard.fromAcquired(recordPageFromBuffer);
} else {
    // Same page as current guard - release the extra guard we just acquired
    recordPageFromBuffer.releaseGuard();
}
```

**The bug:**
- Every time we fetch a different page, we close the old guard
- But axis iterators may have stored node keys from old pages
- Those node keys become inaccessible when pages are evicted

---

## Solutions

### Option 1: Multi-Guard Stack (Correct But Complex)

**Concept:** Keep guards on ALL pages accessed during iteration

```java
private final Deque<PageGuard> guardStack = new ArrayDeque<>();

private void acquireGuard(KeyValueLeafPage page) {
    // Check if already guarded
    for (PageGuard guard : guardStack) {
        if (guard.page() == page) {
            return; // Already guarded
        }
    }
    
    // Acquire new guard
    guardStack.push(new PageGuard(page));
}

private void releaseAllGuards() {
    while (!guardStack.isEmpty()) {
        guardStack.pop().close();
    }
}
```

**Pros:**
- ✅ Correct - pages can't be evicted while iteration active
- ✅ Handles multi-page iteration properly

**Cons:**
- ⚠️ Complex - need to track all guarded pages
- ⚠️ When to release? (after iteration? after each axis? unclear)
- ⚠️ Memory overhead (more guards held)

---

### Option 2: Node Keys Are Self-Contained (Current Assumption - BROKEN)

**Assumption:** Once you have a node key (long value), you can always navigate to it via `moveTo()`.

**Why it's broken:**
- moveTo() loads the page from disk
- But if records were deleted or pages corrupted, load fails
- This shouldn't happen in read-only transactions!

**If this assumption is correct**, the bug is elsewhere:
- Page eviction corrupting data
- Wrong page being loaded
- Disk corruption
- Or: nodes being deleted during diff (shouldn't happen in read-only)

---

### Option 3: Increase ClockSweeper Interval (Workaround)

**Make eviction less aggressive:**

```java
// Slow down ClockSweeper
ClockSweeper sweeper = new ClockSweeper(
    shard,
    epochTracker,
    5000,  // ← Was 1000ms, now 5000ms (5x slower)
    shardIndex,
    databaseId,
    resourceId
);
```

**Pros:**
- ✅ Quick fix
- ✅ Reduces eviction pressure

**Cons:**
- ⚠️ Doesn't fix root cause
- ⚠️ Just makes bug less frequent
- ⚠️ Worse cache efficiency

---

### Option 4: Fix ClockSweeper Guard Check (Check if this is the issue)

**Theory:** The `getGuardCount() > 0` check in ClockSweeper happens **inside compute()**, but there's still a window.

Let me check the compute() implementation again:

```java
// ClockSweeper - line 134-183
shard.map.compute(ref, (k, page) -> {
    if (page == null) return null;
    
    if (page.isHot()) {
        page.clearHot();
        return page;
    }
    
    if (page.getGuardCount() > 0) {  // ← Check
        return page;
    }
    
    // Evict
    page.reset();  // ← Use
    return null;
});
```

**This SHOULD be atomic** - compute() holds per-key lock.

**But wait** - what if the page is accessed by a DIFFERENT PageReference key?

In Sirix, the same logical page might be accessible via different PageReference keys (due to versioning). If so:
- Thread A: compute(refA, ...) - holds lock for refA
- Thread B: compute(refB, ...) - holds lock for refB
- Both reference the SAME page object!
- Thread A: checks guardCount=0, evicts
- Thread B: acquires guard on same page
- **RACE!**

Let me check if this is possible.

