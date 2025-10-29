# PATH_SUMMARY Bypass Investigation - Current Status

## What the Diagnostic Investigation Achieved

### 1. We Understand What the Bypass Does

**The Bypass (lines 473-491):**
```java
// For PATH_SUMMARY in write transactions:
var loadedPage = loadDataPageFromDurableStorageAndCombinePageFragments(pageReference);
return loadedPage;  // Return WITHOUT putting in RecordPageCache
```

**Normal Path (other pages):**
```java
var result = getFromBufferManager(indexLogKey, pageReference);
  ↓
RecordPageCache.get(pageReference, ...)  // Uses cache
```

### 2. We Fixed the Major Leak Source

**Problem Found:** Bypassed pages were being accidentally cached via `getInMemoryPageInstance` (line 692)

**Fixed:** Don't cache PATH_SUMMARY from write transactions (line 689)

**Fixed:** Close replaced bypassed pages (lines 582-589)

**Fixed:** Close final bypassed page on transaction close (lines 999-1001)

## Current Leak Status

Running **all** VersioningTests:
```
Total finalizer leaks: 1230
  - NAME: 900 (73%)
  - PATH_SUMMARY: 199 (16%)
  - DOCUMENT: 131 (11%)
```

Running **only** VersioningTest.testFull1:
```
Total finalizer leaks: 0
  - PATH_SUMMARY: 0 ✅
```

## What This Means

✅ **For testFull1**: Our fixes completely eliminated PATH_SUMMARY leaks

❌ **For other VersioningTests**: Still have PATH_SUMMARY leaks (plus NAME and DOCUMENT leaks)

## Why Do We Still Have Leaks in Other Tests?

Possible reasons:
1. **Different versioning types** (testIncremental, testSlidingSnapshot) may have different code paths
2. **NAME pages** are the biggest leak (900) - separate issue from PATH_SUMMARY
3. **Read-only transactions** might need similar fixes
4. **Multiple transactions** in sequence might accumulate some pages

## The Bypass Question - Still Unclear

**Your point:** If data hasn't been modified, getting it from cache with same PageReference is correct.

**My confusion:** Why did removing the bypass cause "Failed to move to nodeKey: 3073"?
- Cache returned revision 2 when asking for revision 4
- But if no modification, that should be fine
- Yet it failed...

**What we need to understand:**
- Does `combinePageFragments` create DIFFERENT data for different revisions even if the base page wasn't modified?
- Or is there something else going on?

## Next Steps

1. **Investigate why bypass removal failed** - understand the actual requirement
2. **Fix remaining NAME page leaks** (900 leaks - much bigger problem!)
3. **Check other VersioningTests** for PATH_SUMMARY leak sources
4. **Document the final understanding** of why bypass is architecturally required

## Summary

Diagnostic investigation was very helpful:
- ✅ Found and fixed major leak source (accidental caching)
- ✅ Eliminated leaks for testFull1
- ⚠️ Still have leaks in other tests
- ❓ Still unclear on exact bypass requirement

The diagnostics showed WHERE the leaks were but we still need to understand WHY the bypass is fundamentally required.

