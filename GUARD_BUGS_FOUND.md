# Guard Management Bugs Found

## Bug #1: mostRecentPage May Have Stale/Unguarded Pages

**Location:** `NodePageReadOnlyTrx.getMostRecentPage()` (lines 489-508)

**The Bug:**
```java
// Get from local field (NOT from cache!)
var cachedPage = getMostRecentPage(indexLogKey.getIndexType(), ...);
if (cachedPage != null) {
    var page = cachedPage.page();
    
    // ❌ This page might not have a guard anymore!
    // The page is in our local field, but its guard might have been released
    
    // Acquire guard for cached page
    if (currentPageGuard == null || currentPageGuard.page() != page) {
        closeCurrentPageGuard();
        currentPageGuard = new PageGuard(page);  // ← Acquires NEW guard
    }
}
```

**Problem:**
- mostRecentDocumentPage field holds a page reference
- That page was guarded when first fetched
- But guard was closed when we navigated to a different page
- Page became unguarded → eligible for eviction
- Page might have been evicted and reset
- Now we try to acquire a NEW guard on a reset page!

**Impact:** Accessing stale/reset pages

---

## Bug #2: PATH_SUMMARY mostRecent Page Also Unguarded

**Location:** Lines 475-485

```java
if (indexLogKey.getIndexType() == IndexType.PATH_SUMMARY && isMostRecentlyReadPathSummaryPage(indexLogKey)) {
    var page = pathSummaryRecordPage.page();  // ← From local field
    assert !page.isClosed();  // ← Might FAIL if evicted!
    
    // Acquire guard for PATH_SUMMARY page
    if (currentPageGuard == null || currentPageGuard.page() != page) {
        closeCurrentPageGuard();
        currentPageGuard = new PageGuard(page);  // ← NEW guard on potentially stale page
    }
}
```

---

## Bug #3: Swizzled Pages Without Guards

**Location:** `getInMemoryPageInstance()` lines 974-1005

```java
Page page = pageReferenceToRecordPage.getPage();  // Swizzled page

if (page != null) {
    var kvLeafPage = ((KeyValueLeafPage) page);
    
    // Acquire guard for swizzled page
    if (currentPageGuard == null || currentPageGuard.page() != kvLeafPage) {
        closeCurrentPageGuard();
        currentPageGuard = new PageGuard(kvLeafPage);  // ← NEW guard
    }
}
```

**Problem:** Swizzled pages might not have guards initially!

---

## Root Cause

**The fundamental issue:** `mostRecentXXXPage` fields hold page references WITHOUT maintaining guards.

**Flow:**
1. First access → fetch page via `getFromBufferManager()` → guard acquired
2. Store in `mostRecentDocumentPage` → page reference kept
3. Navigate to different page → `closeCurrentPageGuard()` releases guard
4. Second access → `getMostRecentPage()` returns stale page → NO GUARD!
5. Try to acquire new guard → but page might be evicted/reset

---

## The Fix

### Option A: mostRecentPage fields must maintain guards

**Keep a guard for EACH mostRecentXXXPage:**

```java
private PageGuard mostRecentDocumentPageGuard;
private PageGuard mostRecentChangedNodesPageGuard;
// ... one guard per mostRecent field

private void setMostRecentPage(IndexType type, int index, RecordPage page) {
    // Release old guard
    switch (type) {
        case DOCUMENT:
            if (mostRecentDocumentPageGuard != null) {
                mostRecentDocumentPageGuard.close();
            }
            mostRecentDocumentPageGuard = page != null ? new PageGuard(page.page()) : null;
            mostRecentDocumentPage = page;
            break;
        // ... similar for other types
    }
}
```

**Complexity:** Need ~10 additional guard fields

---

### Option B: Don't use mostRecentPage fields - always fetch from cache

**Remove the optimization, always use cache:**

```java
// DELETE getMostRecentPage() check
// Always go through getFromBufferManager() which uses getAndGuard()
```

**Pros:**
- ✅ Simpler - no stale references
- ✅ Cache handles guards correctly
- ✅ Fewer bugs

**Cons:**
- ⚠️ Slightly slower (cache lookup every time)
- ⚠️ Loses "hot page" optimization

---

### Option C: Check if page is still in cache before using mostRecent

**Validate mostRecent pages are still cached:**

```java
var cachedPage = getMostRecentPage(...);
if (cachedPage != null) {
    var page = cachedPage.page();
    
    // ✅ Verify page is still in cache AND guard it atomically
    var guardedPage = resourceBufferManager.getRecordPageCache().getAndGuard(cachedPage.pageReference);
    
    if (guardedPage == page) {
        // Same instance - safe to use
        if (currentPageGuard == null || currentPageGuard.page() != page) {
            closeCurrentPageGuard();
            currentPageGuard = PageGuard.fromAcquired(page);
        } else {
            // Already guarded - release extra
            page.releaseGuard();
        }
        return new PageReferenceToPage(cachedPage.pageReference, page);
    } else {
        // Different instance or null - mostRecent is stale
        setMostRecentPage(indexLogKey.getIndexType(), indexLogKey.getIndexNumber(), null);
        cachedPage = null;
        // Fall through to reload
    }
}
```

---

## Recommendation: Fix Option C (Safest)

This validates mostRecent pages are still valid before using them and acquires guards atomically.

