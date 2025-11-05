# PATH_SUMMARY Bypass - Simple Explanation

## What Does the Bypass Do?

### Normal Path (Used by Most Pages)
```java
// 4th step in getRecordPage()
return getFromBufferManager(indexLogKey, pageReferenceToRecordPage);
  ↓
RecordPageCache.get(pageReference, (key, cachedValue) -> {
  if (cachedValue == null) {
    // Load from disk
    return loadFromDisk();
  }
  return cachedValue;  // Return from cache
});
```

**Normal path**: Check cache → if miss, load from disk → put in cache → return

### Bypass Path (Used by PATH_SUMMARY in Write Transactions)
```java
// Lines 473-491 in NodePageReadOnlyTrx.java
if (trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {
  // Use normal path
} else {
  // BYPASS: Load directly from disk, DON'T use cache
  var loadedPage = loadDataPageFromDurableStorageAndCombinePageFragments(pageReference);
  return loadedPage;  // Return without caching
}
```

**Bypass path**: Load from disk → return **WITHOUT caching**

## Why Bypass?

You're absolutely right - if data hasn't been modified, the same PageReference should work fine.

Let me trace through what the diagnostic showed when we removed the bypass:

### The Failure

```
[PATH_SUMMARY-INIT] Initializing pathNodeMapping: hasTrxIntentLog=true, revision=4
[PATH_SUMMARY-DECISION] Using normal cache path:
  - recordPageKey: 1
  - revision: 4
[PATH_SUMMARY-NORMAL] -> Got page from cache: pageKey=1, pinCount=1, revision=2
[PATH_SUMMARY-INIT] -> Loaded 2048 nodes into pathNodeMapping

ERROR: Failed to move to nodeKey: 3073
```

## The Current Leak Status

Running all VersioningTests:
- **Total leaks: 1230**
  - NAME: 900 (biggest problem!)
  - PATH_SUMMARY: 199  
  - DOCUMENT: 131

Our PATH_SUMMARY fixes helped but didn't eliminate all leaks.

## Where Are We?

✅ **Fixed:** Bypassed PATH_SUMMARY pages being accidentally cached
✅ **Fixed:** Replaced bypassed pages being closed
✅ **Fixed:** Final bypassed page being closed on transaction close

❌ **Not fixed:** Still 199 PATH_SUMMARY leaks across all tests
❌ **Not fixed:** 900 NAME page leaks
❌ **Not fixed:** 131 DOCUMENT page leaks

## What Do We Need to Understand?

1. **What exactly does the bypass prevent?**
   - Is it about getting wrong revision from cache?
   - Is it about something else entirely?

2. **Why do we still have 199 PATH_SUMMARY leaks?**
   - Are they from read-only transactions?
   - Are they from different code paths we didn't fix?

3. **Are the leaks acceptable or do we need to fix them all?**

Let me check where the remaining PATH_SUMMARY leaks come from.





