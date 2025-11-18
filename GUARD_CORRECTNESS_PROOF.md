# Guard Correctness Proof - Formal Verification

**Date:** November 14, 2025  
**Goal:** Prove guard management is correct with 100% certainty  
**Status:** 🔍 **UNDER REVIEW**

---

## Invariants That Must Hold

### I1: Guard Before Use
**Every page access MUST have an active guard at the moment of access**

### I2: Single Owner
**Each guard acquisition MUST have exactly one corresponding release**

### I3: No Use-After-Release
**Pages MUST NOT be accessed after their guard is released**

### I4: Eviction Safety
**ClockSweeper MUST NOT evict pages with guardCount > 0**

### I5: Version Consistency
**PageGuard version check MUST detect frame reuse**

---

## Current Architecture Analysis

### Guard Storage:

```java
// NodePageReadOnlyTrx.java
private PageGuard currentPageGuard;  // ❌ SINGLE GUARD ONLY
```

**PROBLEM:** This violates I3 (No Use-After-Release)

**Proof by counterexample:**
```
1. hasNext() fetches Page A, stores nodeKeys from Page A
2. currentPageGuard protects Page A ✓
3. hasNext() returns true
4. Between hasNext() and next():
   - Some code navigates to Page B
   - closeCurrentPageGuard() releases Page A  ❌
   - currentPageGuard = new PageGuard(Page B)
5. ClockSweeper evicts Page A (guardCount = 0)
6. next() tries to moveTo(key from Page A)
7. moveTo() loads Page A - but it was reset! ❌
```

**Violation:** We're using data from Page A (node keys) after releasing its guard.

---

## The Architectural Flaw

### Current Model (Broken):

```
┌─────────────────────────────────────────┐
│ Transaction has ONE guard at a time     │
├─────────────────────────────────────────┤
│ When navigating to new page:           │
│   1. Release guard on current page      │  ← ❌ Page becomes evictable
│   2. Acquire guard on new page          │
└─────────────────────────────────────────┘

Problem: Caller might still need old page!
```

### Correct Model (Needed):

```
┌─────────────────────────────────────────┐
│ Transaction has MULTIPLE guards         │
├─────────────────────────────────────────┤
│ When fetching page:                     │
│   1. Check if already guarded           │
│   2. If not, acquire new guard          │
│   3. Add to guard set                   │
├─────────────────────────────────────────┤
│ Release guards:                         │
│   - After high-level operation completes│
│   - Or on transaction close             │
└─────────────────────────────────────────┘
```

---

## Current Code Paths - Systematic Review

### Path 1: getRecordPage() → getFromBufferManager()

**Lines 696-753:**
```java
KeyValueLeafPage page = cache.getAndGuard(ref);  // ← Guard acquired

if (currentPageGuard == null || currentPageGuard.page() != page) {
    closeCurrentPageGuard();  // ← Releases OLD guard
    currentPageGuard = PageGuard.fromAcquired(page);
} else {
    page.releaseGuard();  // ← Releases extra guard
}
```

**Analysis:**
- ✅ Guard acquired atomically
- ⚠️ Old guard released → old page evictable
- ⚠️ If caller had references to old page data → VIOLATION

**Status:** ⚠️ **UNSAFE if caller keeps old page data**

---

### Path 2: getRecordPage() → mostRecentPage check

**Lines 502-533:**
```java
var guardedPage = cache.getAndGuard(cachedPage.pageReference);

if (guardedPage == page && !page.isClosed()) {
    if (currentPageGuard == null || currentPageGuard.page() != page) {
        closeCurrentPageGuard();  // ← Releases old guard
        currentPageGuard = PageGuard.fromAcquired(page);
    } else {
        page.releaseGuard();  // ← Releases extra
    }
}
```

**Analysis:**
- ✅ Validates page still in cache
- ✅ Guards atomically
- ⚠️ Releases old guard → same problem as Path 1

**Status:** ⚠️ **UNSAFE if caller keeps old page data**

---

### Path 3: getInMemoryPageInstance() - Swizzled

**Lines 1014-1026:**
```java
var guardedPage = cache.getAndGuard(pageReference);

if (guardedPage == kvLeafPage && !kvLeafPage.isClosed()) {
    if (currentPageGuard == null || currentPageGuard.page() != kvLeafPage) {
        closeCurrentPageGuard();  // ← Releases old guard
        currentPageGuard = PageGuard.fromAcquired(kvLeafPage);
    } else {
        kvLeafPage.releaseGuard();
    }
}
```

**Analysis:** Same pattern, same issue.

**Status:** ⚠️ **UNSAFE if caller keeps old page data**

---

## The Root Problem: Who Owns The Data?

### Current Unclear Ownership:

```java
// Axis code:
hasNext() {
    long key = rtx.getFirstChildKey();  // ← Reads from MemorySegment on Page A
    return key;  // ← Returns Java long (copied from MemorySegment)
}

// Later:
nextLong() {
    moveTo(key);  // ← Page A might be evicted by now!
}
```

**Question:** Is `key` safe after guard on Page A is released?

**Answer:** 
- ✅ **YES** if `key` is a Java primitive (copied from MemorySegment)
- ❌ **NO** if we need to re-access Page A to use `key`

**In Sirix:**
- `getFirstChildKey()` returns `long` - primitive, copied ✓
- `moveTo(key)` must look up the node again - needs page access!
- **If Page A was evicted, moveTo() will reload it** ✓
- **BUT**: If Page A was *reset* (not removed), moveTo() gets corrupted data ❌

---

## The Real Bug: Page Reset vs Eviction

### ClockSweeper Does:

```java
page.incrementVersion();  // V1 → V2
page.reset();  // ❌ Clears data but keeps page in cache!
return null;  // Remove from cache
```

**But what if:**
1. Page is reset
2. Page removal from cache fails (race condition)
3. Page stays in cache but with reset data
4. Later getAndGuard() finds reset page in cache
5. Returns page with NO data!

Let me check the ClockSweeper code again carefully.

