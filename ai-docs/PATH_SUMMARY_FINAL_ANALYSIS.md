# PATH_SUMMARY Bypass - Final Analysis

## The Confirmed Issue

**With normal caching (bypass commented out):**
- VersioningTest.testFull1 FAILS: "Failed to move to nodeKey: 3073" ❌

**With bypass enabled:**
- VersioningTest.testFull1 PASSES ✅

## Why It Fails Without Bypass

### The Flow

1. **PathSummaryReader initialization** (constructor, line 124-126):
   ```java
   while (axis.hasNext()) {
       final var pathNode = axis.next();  // Loads PathNode from pages
       pathNodeMapping[(int) pathNode.getNodeKey()] = pathNode;  // Caches in array
   }
   ```

2. **Pages loaded during initialization:**
   - Come from `getFromBufferManager()` → RecordPageCache
   - Get pinned during load
   - Get unpinned after pathNodeMapping is populated
   - **Eligible for cache eviction**

3. **Later navigation** (line 407-414):
   ```java
   public boolean moveTo(long nodeKey) {
       if (!init && nodeKey != 0) {
           final PathNode node = (PathNode) pathNodeMapping[(int) nodeKey];
           if (node != null) {
               return true;  // Found in cache
           } else {
               return false;  // ❌ NOT FOUND - returns false!
           }
       }
       // Falls through to load from pages only if init=true
   }
   ```

4. **The problem:**
   - Page with node 3073 was loaded initially
   - Page added to RecordPageCache
   - Page evicted from cache (under memory pressure)
   - Later, navigation tries moveTo(3073)
   - pathNodeMapping[3073] is now null or corrupted
   - Returns false → "Failed to move to nodeKey: 3073"

## Why Bypass Works

**Bypass code:**
```java
var loadedPage = loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
return new PageReferenceToPage(pageReferenceToRecordPage, loadedPage);
```

**Key differences:**
1. **NOT added to RecordPageCache** - page is only swizzled in PageReference
2. **Stored in mostRecentPathSummaryPage field** - kept alive via field reference
3. **Never evicted** - not in cache, so can't be evicted
4. **pathNodeMapping stays valid** - pages remain alive throughout transaction

## The Mystery: pathNodeMapping Contents

**User correctly points out:** PathNode contains plain Java objects (longs, QNm), NOT MemorySegments!

So why does page eviction break pathNodeMapping?

### Hypothesis 1: pathNodeMapping Holds References That Need Pages Alive

Even though PathNode fields are primitive, perhaps:
- PathNode objects themselves are stored on heap
- But they hold REFERENCES to other PathNode objects (firstChild, lastChild, etc.)
- When page is evicted and reloaded, NEW PathNode objects are created
- Old PathNode objects in pathNodeMapping become stale
- Navigation follows stale references → can't find nodes

### Hypothesis 2: Cache Key Collision

With normal caching:
- Multiple revisions might share same PageReference
- Cache uses PageReference as key
- Different revision's page overwrites previous in cache
- pathNodeMapping built from revision N's pages
- Cache now has revision N-1's pages
- Navigation fails because wrong revision

### Hypothesis 3: Initialization Race Condition  

With normal caching:
- Initialization loads all nodes (line 124-126)
- Adds pages to RecordPageCache
- Pages can be evicted DURING initialization
- pathNodeMapping partially populated
- Some nodes missing → navigation fails

## The Leak Caused by Bypass

**Statistics:**
- PATH_SUMMARY: 4 pages leaked (50% leak rate)
- In investigations: 80-98% of ALL pages were PATH_SUMMARY

**Why it leaks:**
- Bypass creates new combined pages via `combineRecordPages()`
- Pages stored in mostRecentPathSummaryPage field
- When replaced, calls `page.clear()` which is a NO-OP
- Pages never closed → memory leak!

**The fix (line 524-526):**
```java
} else {
    pathSummaryRecordPage.pageReference.setPage(null);
    if (!pathSummaryRecordPage.page.isClosed()) {
        pathSummaryRecordPage.page.close();  // Instead of clear()!
    }
}
```

## What We Need to Understand

To fix this properly, we need to answer:

**Q1:** Why does pathNodeMapping become invalid when pages are cached normally?
- Is it because PathNode objects from different page loads are incompatible?
- Is it a cache key collision issue?
- Is it a timing/initialization issue?

**Q2:** Why does bypass prevent the problem?
- Is it because pages stay pinned/alive longer?
- Is it because each access gets fresh pages?
- Is it because PathReferences aren't shared/reused?

**Q3:** Can we fix the leak WITHOUT removing the bypass?
- Change `clear()` to `close()` in line 526
- This might reduce leak by 50% (PATH_SUMMARY pages)
- Keep bypass for correctness, just fix the cleanup

## Recommended Next Step

**Option A:** Fix the leak in the bypass path (safer, incremental)
1. Change `page.clear()` → `page.close()` in setMostRecentlyReadRecordPage (line 526)
2. Test that VersioningTest still passes
3. Verify PATH_SUMMARY leak reduced
4. Keep bypass for correctness

**Option B:** Deep investigation (riskier, comprehensive)
1. Add extensive logging to understand why pathNodeMapping becomes invalid
2. Test cache key collisions
3. Test page eviction timing
4. Fix root cause and remove bypass

I recommend **Option A** first!

