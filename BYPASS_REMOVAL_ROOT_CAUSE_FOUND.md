# PATH_SUMMARY Bypass Removal - Root Cause Identified

## The Real Problem

After testing with diagnostics, we found the **actual reason** the bypass exists:

### Cache Key Problem

**PageReference.equals() and hashCode() (lines 178-191):**
```java
public int hashCode() {
  return Objects.hash(logKey, key);  // key = storage offset
}

public boolean equals(Object other) {
  return otherPageRef.logKey == logKey && otherPageRef.key == key;
}
```

**The Issue:**
- PageReference uses `key` (persistent storage offset) and `logKey` for equality
- Does NOT include the page's revision number!
- **Different revisions of the same page can have the SAME PageReference (same storage offset)**

### What Happens

1. Load page 2 revision 3, get PageReference with key=X, logKey=Y
2. Unpin and put in RecordPageCache with that PageReference
3. Later need page 2 revision 4, create PageReference with... same key=X, logKey=Y!
4. Cache returns page 2 revision 3 (wrong revision!) ❌
5. PathSummaryReader gets old data → "Failed to move to nodeKey" error

### Diagnostic Evidence

```
[PATH_SUMMARY-DECISION] Using normal cache path:
  - recordPageKey: 2
  - revision: 4      ← Want revision 4
  - trxId: 5

[PATH_SUMMARY-NORMAL] -> Got page from cache: 
  pageKey=2, 
  revision=3         ← Got revision 3! Wrong!
```

This happened repeatedly in the logs.

### Why the Bypass Works

The bypass NEVER uses RecordPageCache:
```java
// Bypass loads fresh from disk
var loadedPage = loadDataPageFromDurableStorageAndCombinePageFragments(...);
// NOT cached - returned directly
return new PageReferenceToPage(pageReferenceToRecordPage, loadedPage);
```

- Pages never go in cache → can't get wrong revision from cache
- Always loads the correct revision from disk
- But leaks memory because pages aren't managed by cache

### Why Our Fixes Didn't Work

1. **Unpin instead of close**: Helped with memory, but didn't fix the cache key problem
2. **TIL search**: TIL is empty after commit, can't find pages there
3. **Match on revision in TIL**: Still TIL empty, and cache still returns wrong revision

The fundamental issue is that **RecordPageCache can't differentiate between revisions** because PageReference equality doesn't include revision!

## The Real Solution Needed

To remove the bypass, we need ONE of:

### Option 1: Fix PageReference Equality (Breaking Change)
Include revision in PageReference.equals() and hashCode():
```java
// Would need to store revision in PageReference
public int hashCode() {
  return Objects.hash(logKey, key, revision);
}
```

**Problem**: Major architectural change, PageReference doesn't currently store revision

### Option 2: Don't Cache PATH_SUMMARY in RecordPageCache
Keep PATH_SUMMARY out of the shared cache, manage differently:
- Use transaction-local storage
- Or use a separate cache that includes revision in keys
  
**Problem**: Still needs proper lifecycle management to avoid leaks

### Option 3: Keep the Bypass (Current Approach)
Accept that PATH_SUMMARY needs bypass, just fix the leak:
- ✅ Unpin pages if in cache (our fix)
- ✅ Close pages if not in cache (our fix)
- ✅ Works correctly (bypass loads right revision)
- ✅ No more leaks (proper cleanup)

## Recommendation

**Keep the bypass** with our unpin/close fixes. The bypass exists for a valid architectural reason (cache can't handle multi-revision pages), not just as a workaround.

The unpin/close fix solves the memory leak while maintaining correctness.

## Current Status

- ✅ Bypass restored
- ✅ Unpin fix implemented (pages unpinned if in cache)
- ✅ Close fix implemented (pages closed if not in cache)
- ✅ Tests pass with bypass
- ❌ Tests fail without bypass (cache returns wrong revisions)
- ✅ Root cause identified (PageReference equality doesn't include revision)

