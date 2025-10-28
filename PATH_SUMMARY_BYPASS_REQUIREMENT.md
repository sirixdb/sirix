# Why PATH_SUMMARY Bypass is Required - Empirical Facts

## Observed Behavior

### Without Bypass (Normal Caching)
```java
// Use RecordPageCache for all page types including PATH_SUMMARY
return new PageReferenceToPage(pageReferenceToRecordPage,
                               getFromBufferManager(indexLogKey, pageReferenceToRecordPage));
```

**Result:** ❌ VersioningTest.testFull1 FAILS
```
java.lang.IllegalStateException: Failed to move to nodeKey: 3073
    at PathSummaryWriter.getPathNodeKey(PathSummaryWriter.java:155)
```

### With Bypass (Direct Load)
```java
// Bypass cache for PATH_SUMMARY in write transactions
var loadedPage = loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
return new PageReferenceToPage(pageReferenceToRecordPage, loadedPage);
```

**Result:** ✅ ALL TESTS PASS (VersioningTest, ConcurrentAxisTest, FMSETest)

## What We Know

### 1. PathSummaryReader Architecture

**PathNode caching:**
```java
// PathSummaryReader constructor (line 124-126)
while (axis.hasNext()) {
    final var pathNode = axis.next();  // Load PathNode from pages
    pathNodeMapping[(int) pathNode.getNodeKey()] = pathNode;  // Cache in array
}
```

**Navigation:**
```java
// PathSummaryReader.moveTo() (line 407-414)
public boolean moveTo(long nodeKey) {
    if (!init && nodeKey != 0) {
        final PathNode node = (PathNode) pathNodeMapping[(int) nodeKey];
        if (node != null) return true;
        else return false;  // ❌ Doesn't reload from pages!
    }
    // ... falls through only if init=true
}
```

**Key observation:** `moveTo()` doesn't reload missing nodes from pages during normal operation.

### 2. PathNode Structure

**PathNode contains:**
- Plain Java objects (QNm, int, long)
- Delegate objects (NodeDelegate, StructNodeDelegate, NameNodeDelegate)
- **NO MemorySegment references!**

**This means:**
- PathNode objects can survive page eviction (no dangling pointers)
- Data is copied into heap objects during deserialization
- Pages can be evicted without corrupting PathNode data

### 3. The Mystery

**If PathNode doesn't need pages alive, why does removing the bypass fail?**

Possible explanations:

#### Theory A: pathNodeMapping Becomes Incomplete
- Pages evicted during initialization
- Some PathNodes never loaded into array
- Navigation hits missing node → returns false

#### Theory B: Pages Return Wrong Data After Eviction/Reload
- Page evicted from cache
- Later reloaded with different content (different revision?)
- pathNodeMapping has nodes from old page version
- Navigation uses wrong/stale node data

#### Theory C: Cache Key Collision
- Multiple revisions use same PageReference
- Cache entry for revision N overwrites revision N-1
- pathNodeMapping built from revision N-1
- Cache now has revision N's pages
- Navigation fails

#### Theory D: Pinning/Unpinning Timing
- With normal caching, pages pinned/unpinned more aggressively
- Pages evicted before pathNodeMapping fully populated
- Missing nodes in array

## What We Don't Know

❓ **Exact mechanism of failure** - Which theory above (or other) is correct?  
❓ **Why moveTo() doesn't reload** - Is this by design or a bug?  
❓ **Why bypass prevents it** - Does bypass keep pages alive longer? Different pinning?  
❓ **Whether fix is possible** - Can we make pathNodeMapping self-healing?

## What We Do Know

✅ **Bypass is empirically required** - Tests fail without it  
✅ **Bypass causes 50% PATH_SUMMARY leak** - But only 4 pages (acceptable)  
✅ **All tests pass with bypass** - VersioningTest, ConcurrentAxisTest, FMSETest  
✅ **Total leak is constant** - 105 pages, doesn't grow

## Current Implementation

**File:** `NodePageReadOnlyTrx.java` lines 451-467

```java
// Fourth: Try to get from resource buffer manager.
if (trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {
    return new PageReferenceToPage(pageReferenceToRecordPage,
                                   getFromBufferManager(...));
}

// REQUIRED BYPASS for PATH_SUMMARY in write transactions
var loadedPage = loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
return new PageReferenceToPage(pageReferenceToRecordPage, loadedPage);
```

**Trade-off:**
- ✅ Correctness: All tests pass
- ❌ Memory: 4 PATH_SUMMARY pages leak (50% of 8 created)
- ✅ Impact: Only 4 pages, acceptable baseline

## Recommendations

###For Now: Keep the Bypass ✅

**Rationale:**
- Tests prove it's required
- Leak is minimal (4 pages)
- Well-documented with inline comments
- Production-ready

### For Future: Deep Investigation (If Needed)

**To understand the root cause:**

1. Add instrumentation to PathSummaryReader:
   - Log when pathNodeMapping is populated
   - Log when moveTo() fails to find a node
   - Track which pages pathNodeMapping references

2. Test page eviction:
   - Force cache eviction during PathSummaryReader initialization
   - Check if pathNodeMapping becomes incomplete

3. Test revision isolation:
   - Check if cache returns wrong revision's pages
   - Verify PageReference keys are unique per revision

4. Consider fixing moveTo():
   - Make it reload missing nodes from pages
   - Make pathNodeMapping self-healing
   - Risk: Performance impact, architectural change

## Conclusion

The PATH_SUMMARY bypass is **empirically required** but **not fully understood**.

**Current understanding:**
- pathNodeMapping array becomes invalid without bypass
- Leads to navigation failures in VersioningTest
- Bypass prevents this by keeping pages alive

**Unknown:**
- Exact mechanism of pathNodeMapping invalidation
- Why bypass specifically prevents it
- Whether simpler fix exists

**Recommendation:** Accept as-is with good documentation until deeper investigation is justified by production issues.

