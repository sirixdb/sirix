# Correct Guard Lifecycle - The Right Solution

**Date:** November 14, 2025  
**Problem:** Holding guards until close is too late (memory bloat)

---

## The Key Insight

**Node keys are primitives (long) - they're COPIED from MemorySegments!**

```java
// Reading a node key:
long key = node.getFirstChildKey();
// ↓
// Calls: FIRST_CHILD_KEY_HANDLE.get(segment, 0L)
// ↓
// Returns Java long (primitive) - COPIED from MemorySegment
// ↓
// Original page can now be evicted safely!
```

**This means:**
- Once we have the node key (long value), the page can be released
- `moveTo(key)` will reload the page from cache/disk if needed
- **We don't need to hold the page!**

---

## The Real Bug

If node keys are self-contained primitives, why does `moveTo()` fail?

**Hypothesis 1: Page Reset Without Removal**
```java
// ClockSweeper:
page.reset();  // Clears data
return null;  // Remove from cache

// But what if removal fails?
// compute() returns null but map entry stays?
```

**Check:** This shouldn't happen - `compute()` returning `null` removes the entry atomically.

**Hypothesis 2: Wrong Page Loaded**
```java
// moveTo(1024):
getRecord(1024, DOCUMENT, -1)
  → Loads page for nodeKey 1024
  → But loads WRONG page (cache collision? wrong revision?)
  → Returns null
```

**Check:** PageReference equals/hashCode includes revision? Let me verify.

**Hypothesis 3: Deleted Node**
```java
// Node 1024 was deleted between hasNext() and next()
// This SHOULDN'T happen in read-only transactions!
```

---

## Correct Guard Lifecycle

### Single "Current Page" Guard (Original Design Was Correct!)

```java
public class NodePageReadOnlyTrx {
    // Guard ONLY the page containing current cursor position
    private PageGuard currentPageGuard;
    
    void fetchPage(PageReference ref) {
        KeyValueLeafPage page = cache.getAndGuard(ref);
        
        // Release OLD page guard (we're moving to new page)
        if (currentPageGuard == null || currentPageGuard.page() != page) {
            closeCurrentPageGuard();  // ← Release old page
            currentPageGuard = PageGuard.fromAcquired(page);
        } else {
            page.releaseGuard();  // Same page, release extra
        }
    }
    
    void close() {
        closeCurrentPageGuard();  // Release just the current page
    }
}
```

**Why this is correct:**
1. ✅ Guard protects current cursor position
2. ✅ Old pages can be evicted (we have their node keys as primitives)
3. ✅ moveTo(key) reloads pages if needed
4. ✅ Low memory overhead (one guard at a time)

**Why it was failing:**
- Bug in moveTo() implementation
- OR stale mostRecent references
- NOT a guard lifecycle issue!

---

## The Actual Fix Needed

### Don't Trust mostRecent Fields - Always Validate

```java
// Check mostRecent
var cachedPage = getMostRecentPage(...);
if (cachedPage != null) {
    var page = cachedPage.page();
    
    // ✅ VALIDATE: Fetch from cache to ensure still valid
    var validPage = cache.get(cachedPage.pageReference);
    
    if (validPage == page && !page.isClosed()) {
        // ✅ Still valid - acquire guard
        if (currentPageGuard == null || currentPageGuard.page() != page) {
            closeCurrentPageGuard();
            currentPageGuard = new PageGuard(page);
        }
        return page;
    } else {
        // ❌ Stale - clear and reload
        setMostRecentPage(type, index, null);
        cachedPage = null;
    }
}
```

**Key change:** Use `cache.get()` not `cache.getAndGuard()` for validation (lighter weight)

---

## Revised Architecture (Correct)

### Principle: Guard Only Active Cursor Position

```java
┌──────────────────────────────────────────────┐
│ Transaction has ONE guard on current page    │
├──────────────────────────────────────────────┤
│ When moving to new page:                     │
│   1. Fetch new page with guard               │
│   2. Release guard on old page               │  ← Old page evictable
│   3. Update currentPageGuard                 │
├──────────────────────────────────────────────┤
│ Data accessed from old pages:                │
│   - Node keys (long) - primitive, copied     │  ← Safe after release
│   - Strings, names - interned/cached         │  ← Safe after release
│   - References to nodes - NO! (must moveTo)  │  ← Reload on demand
└──────────────────────────────────────────────┘
```

**This matches database cursor semantics:**
- PostgreSQL: Pins current buffer only
- MySQL: Locks current page only
- LeanStore: Guards current frame only

---

## What About Complex Iteration?

**Scenario:**
```java
hasNext() {
    long childKey = rtx.getFirstChildKey();  // From Page A
    return childKey;  // Primitive long - COPIED
}

// Guard on Page A released here (cursor moves)

nextLong() {
    moveTo(childKey);  // ← Reloads Page A from cache/disk
}
```

**This SHOULD work because:**
1. `childKey` is a primitive (not pointing to MemorySegment)
2. `moveTo()` reloads the page containing that node
3. If page was evicted, it's loaded from disk
4. If node doesn't exist, moveTo() returns false

**If it doesn't work, the bug is in moveTo(), NOT in guard lifecycle!**

---

## Action Plan

1. **REVERT multi-guard to single-guard** (too conservative)
2. **KEEP the mostRecent validation fixes** (those were correct)
3. **CHANGE getAndGuard() to get()** for mostRecent validation (lighter)
4. **ADD logging to moveTo()** to see WHY it fails

---

## The Right Fix

```java
// mostRecent validation (LIGHTWEIGHT):
var cachedPage = getMostRecentPage(...);
if (cachedPage != null) {
    var page = cachedPage.page();
    
    // Validate page is still in cache (no guard needed yet)
    var cached = cache.get(cachedPage.pageReference);
    
    if (cached == page && !page.isClosed()) {
        // Valid - NOW acquire guard
        if (currentPageGuard == null || currentPageGuard.page() != page) {
            closeCurrentPageGuard();
            currentPageGuard = new PageGuard(page);  // Regular guard acquisition
        }
        return page;
    } else {
        // Stale - clear and reload
        setMostRecentPage(type, index, null);
    }
}

// Fetch from cache:
KeyValueLeafPage page = cache.getAndGuard(ref);  // Atomic guard acquisition

if (page != null) {
    // Transfer guard
    if (currentPageGuard == null || currentPageGuard.page() != page) {
        closeCurrentPageGuard();
        currentPageGuard = PageGuard.fromAcquired(page);
    } else {
        page.releaseGuard();  // Already guarded
    }
}
```

**Key difference:** 
- Validation uses `cache.get()` (no guard)
- Guard acquired only when actually using the page
- Guard released when moving to different page
- **One guard at a time** (like original design)

Should I revert to single-guard with lightweight validation?

