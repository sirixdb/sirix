# Bypass vs Cache - Detailed Diagnostic Comparison

## The Failure Point

**Test:** Initializing PathSummaryReader at revision 4

### WITH BYPASS (Works ✅)
```
[PATH_SUMMARY-INIT] Initializing pathNodeMapping: hasTrxIntentLog=true, revision=4

[PATH_SUMMARY-DECISION] Using bypass (write trx):
  - recordPageKey: 0
  - revision: (not shown, but asking for latest before revision 4)
  
[PATH_SUMMARY-BYPASS] -> Loaded from disk: pageKey=0, revision=3
```

**Result:** Loads pageKey=0 **revision 3**

### WITHOUT BYPASS (Fails ❌)
```
[PATH_SUMMARY-INIT] Initializing pathNodeMapping: hasTrxIntentLog=true, revision=4

[PATH_SUMMARY-DECISION] Using normal cache (read-only trx):
  - recordPageKey: 0
  - revision: 3  ← Requesting revision 3
  
[PATH_SUMMARY-CACHE-LOOKUP] Looking up in cache: PageRef(key=79312, logKey=-15), requestedRevision=3
[PATH_SUMMARY-CACHE-LOOKUP] -> Cache HIT: pageKey=0, revision=2, pinCount=2  ← Got revision 2!

ERROR: AssertionError at line 539: assert value == null || !value.isClosed()
```

**Result:** Asks for revision 3, gets **revision 2** from cache, then hits closed page assertion

## The Key Questions

### Question 1: Why Does Cache Return Wrong Revision?

pageKey=0 modification history:
```
[TIL-PUT] Adding PATH_SUMMARY page to TIL: pageKey=0, revision=0, logKey=9
[TIL-PUT] Adding PATH_SUMMARY page to TIL: pageKey=0, revision=1, logKey=11
[TIL-PUT] Adding PATH_SUMMARY page to TIL: pageKey=0, revision=2, logKey=12
(No modification at revision 3)
```

**Observation:** pageKey=0 was NOT modified at revision 3

**Your point:** If not modified, returning revision 2 is correct!

**But:** The bypass loads "revision 3" from disk. How?

### Question 2: What Does combinePageFragments Actually Do?

The bypass calls:
```java
loadDataPageFromDurableStorageAndCombinePageFragments(pageReference)
  ↓
List<KeyValuePage> pages = getPageFragments(pageReference)
  ↓
Page completePage = versioningApproach.combineRecordPages(pages, ...)
```

**Hypothesis:** `combinePageFragments` might:
1. Load the base page (revision 2 for pageKey=0)
2. Load additional fragment pages from disk
3. Combine them to reconstruct what pageKey=0 looks like at revision 3
4. Set the combined page's revision to 3 (even if base wasn't modified)

### Question 3: Why Does "revision=2" Cause a Problem?

**Possible answers:**
1. The PathSummaryReader expects a specific revision number for validation?
2. The page at revision 2 has DIFFERENT DATA than revision 3 (from other fragments)?
3. The closed page assertion is unrelated to the revision issue?

## What We Need to Test

Let me check:
1. What does `combinePageFragments` actually return for revision 3 of pageKey=0?
2. Is the returned page's data different from revision 2?
3. Why does the assertion fail about closed pages?

## The Closed Page Mystery

The assertion failure is:
```
assert value == null || !value.isClosed()
```

This means the cache returned a **CLOSED** page. But our fix should prevent caching closed pages!

**Possible explanation:**
1. Page 0 revision 2 was loaded and cached
2. Later, page 0 revision 2 was replaced and closed (but still in cache!)
3. Next request for page 0 revision 3 uses same PageRef(key=79312)
4. Cache returns the CLOSED revision 2 page
5. Assertion fails!

This suggests the real problem is: **We're closing and leaving pages in cache**, not a revision mismatch issue!

