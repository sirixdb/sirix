# PATH_SUMMARY Bypass Removal - Test Results

## What We Did

Disabled the PATH_SUMMARY bypass by changing line 457:
```java
// Before:
if (trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {

// After (testing):
if (true || trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {
```

This forces PATH_SUMMARY to use normal caching instead of the bypass.

## Test Result: FAILED ✗

Assertion error at `NodePageReadOnlyTrx.java:573`:
```java
assert value == null || !value.isClosed();
```

## Root Cause Identified!

### The Sequence of Events

1. **Page 0 loaded into cache**:
   ```
   [PATH_SUMMARY-NORMAL] -> Got page from cache: pageKey=0, pinCount=1, revision=2
   ```

2. **Page 1 replaces Page 0**:
   ```
   [PATH_SUMMARY-REPLACE] Replacing old pathSummaryRecordPage: oldPageKey=0, newPageKey=1, trxIntentLog=true
   [PATH_SUMMARY-REPLACE]   -> Write trx: Closing old page 0
   ```

3. **PathSummaryReader re-initializes** (after commit):
   ```
   [PATH_SUMMARY-INIT] Initializing pathNodeMapping: hasTrxIntentLog=true, revision=3
   ```

4. **Tries to read Page 0 from cache**:
   ```
   [PATH_SUMMARY-DECISION] Taking NORMAL CACHE path:
     - recordPageKey: 0
     - revision: 2
     - trxId: 2
   [PATH_SUMMARY-TIL-CHECK] Checking TIL for page: recordPageKey=0, pageRef=-15, existsInTIL=false
   ```

5. **Cache returns CLOSED Page 0 → Assertion fails!**

## Why the Bypass is Needed

The bypass is NOT about MemorySegment references or TIL isolation.

**The REAL reason**:

When using normal caching, replaced PATH_SUMMARY pages are closed (to fix memory leak), but they remain in the cache. Later reads get the closed page from cache → assertion failure.

### The Problem Chain

1. **Normal cache path**: Pages go into `RecordPageCache`
2. **Leak fix**: When replaced, we close the old page (line 585: `page.close()`)
3. **Page still in cache**: But we don't remove it from the cache!
4. **Later read**: PathSummaryReader re-initializes, tries to read the same page
5. **Gets closed page**: Cache returns the closed page
6. **Assertion fails**: `assert !value.isClosed()` fails

### Why Bypass Works

With the bypass:
1. Pages are loaded fresh via `loadDataPageFromDurableStorageAndCombinePageFragments`
2. **NOT put in RecordPageCache** → can't be evicted
3. Swizzled in PageReferences
4. When replaced, closed (or used to call `clear()`)
5. **Never read from cache again** → no closed page problem
6. **But leaks** because closed pages are never released

## The TIL Mystery Solved

Diagnostic output shows:
```
[TIL-PUT] Adding PATH_SUMMARY page to TIL: pageKey=0, revision=0, logKey=9
[PATH_SUMMARY-TIL-CHECK] Checking TIL for page: recordPageKey=0, pageRef=-15, existsInTIL=false
```

**Why is existsInTIL=false even though we PUT it?**

The TIL uses `PageReference.getLogKey()` to retrieve pages. The diagnostic shows `pageRef=-15`, which is NOT a valid logKey (should be 0-17 based on TIL-PUT messages).

This means:
1. When PathSummaryReader reads, it gets a PageReference from somewhere
2. That PageReference doesn't have the correct logKey set
3. So `trxIntentLog.get(pageRef)` returns null (line 567: checks `key.getLogKey()`)
4. TIL can't find the page even though it's there!

## The Actual Issues

There are TWO problems preventing bypass removal:

### Problem 1: Closed Pages in Cache
- Pages are closed but not removed from cache
- Later reads get closed pages → assertion failure
- **Fix**: Remove pages from cache when closing them

### Problem 2: TIL Lookup Fails
- PageReferences don't have correct logKey
- TIL.get() can't find PATH_SUMMARY pages even though they're in TIL
- **Fix**: Ensure PathSummaryReader gets PageReferences with correct logKeys

## What This Means

The bypass is a **workaround** for two bugs:
1. Not removing closed pages from cache
2. TIL lookup not working for PATH_SUMMARY

Fixing these bugs would allow bypass removal and eliminate the memory leak!

## Next Steps

To remove the bypass, we need to:

1. **Remove pages from cache when closing them**:
   ```java
   // In setMostRecentlyReadRecordPage()
   pathSummaryRecordPage.pageReference.setPage(null);
   if (!pathSummaryRecordPage.page.isClosed()) {
       pathSummaryRecordPage.page.close();
       // FIX: Remove from cache!
       resourceBufferManager.getRecordPageCache().remove(pathSummaryRecordPage.pageReference);
   }
   ```

2. **Fix TIL lookup** to use correct PageReferences with valid logKeys

OR:

3. **Use a different approach**: Keep bypass but properly clean up pages (current approach with leak fix)

## Conclusion

The bypass exists because:
- ❌ NOT because of MemorySegment references (PathNodes don't have them)
- ❌ NOT because of TIL isolation (putMapping keeps cache updated)
- ✅ **Because closed pages stay in cache and cause assertion failures**
- ✅ **Because TIL lookup doesn't work correctly for PATH_SUMMARY**

The diagnostics revealed the exact failure mode and the root cause!

