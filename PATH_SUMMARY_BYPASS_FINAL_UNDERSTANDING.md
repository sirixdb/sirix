# PATH_SUMMARY Bypass - Final Understanding from Diagnostic Investigation

## Summary

Using `-Dsirix.debug.path.summary=true` diagnostics, we tracked down exactly why the PathSummary bypass is needed and fixed the memory leaks.

## The Core Issue

### The Bypass is Required

The bypass loads PATH_SUMMARY pages via `loadDataPageFromDurableStorageAndCombinePageFragments`, which reconstructs pages for specific revisions by combining page fragments.

**Example from diagnostics:**
```
[TIL-PUT] Adding PATH_SUMMARY page to TIL: pageKey=1, revision=1, logKey=14
```
PageKey=1 was only modified at revision 1.

**But the bypass loads different revision views:**
```
[PATH_SUMMARY-BYPASS] -> Loaded from disk: pageKey=1, revision=2
[PATH_SUMMARY-BYPASS] -> Loaded from disk: pageKey=1, revision=3
[PATH_SUMMARY-BYPASS] -> Loaded from disk: pageKey=1, revision=4
```

Each revision's view is **different** because `combinePageFragments` combines the base page with fragments from all relevant revisions to reconstruct what the page looked like at that point in time.

### The Caching Problem

All these revision views share the **same PageReference**:
```
pageKey=1, revision=2, PageRef(key=92616, logKey=-15)
pageKey=1, revision=3, PageRef(key=92616, logKey=-15)  ← SAME key!
pageKey=1, revision=4, PageRef(key=92616, logKey=-15)  ← SAME key!
```

If bypassed pages get cached:
1. Load revision 2 → cache with PageRef(key=92616)
2. Load revision 4 → cache with PageRef(key=92616) → **replaces revision 2**
3. Try to read revision 2 again → get revision 4 instead → **WRONG DATA**

Or the reverse:
1. Load and cache revision 4
2. Later request revision 4, but revision 2 is in cache → get old data → **MISSING NODES**

This is what caused "Failed to move to nodeKey: 3073" - nodeKey 3073 exists in revision 4 but not in the cached revision 2.

## The Fixes We Implemented

### Fix 1: Don't Cache Bypassed PATH_SUMMARY Pages
**Location:** `NodePageReadOnlyTrx.java`, line 689

```java
private Page getInMemoryPageInstance(...) {
  if (page != null) {
    // Don't cache PATH_SUMMARY pages from write transactions (bypassed pages)
    if (trxIntentLog == null || indexLogKey.getIndexType() != IndexType.PATH_SUMMARY) {
      resourceBufferManager.getRecordPageCache().put(pageReferenceToRecordPage, kvLeafPage);
    }
  }
}
```

**Why:** Prevents cache collisions between different revision views of the same pageKey.

### Fix 2: Close Replaced Bypassed Pages
**Location:** `NodePageReadOnlyTrx.java`, lines 565-590

```java
if (pathSummaryRecordPage != null) {
  if (trxIntentLog != null) {
    // Check if page is in cache
    if (resourceBufferManager.getRecordPageCache().get(...) != null) {
      // In cache: just unpin (shouldn't happen for bypassed pages)
      pathSummaryRecordPage.page.decrementPinCount(trxId);
    } else {
      // Not in cache (bypassed pages): close to release memory
      pathSummaryRecordPage.page.close();
    }
  }
}
```

**Why:** Bypassed pages are not cached, so they must be explicitly closed to release memory.

### Fix 3: Close Final Page on Transaction Close
**Location:** `NodePageReadOnlyTrx.java`, lines 967-980

```java
if (pathSummaryRecordPage != null) {
  if (trxIntentLog != null) {
    // Write transaction: close bypassed pages
    pathSummaryRecordPage.page.close();
  }
}
```

**Why:** The final `pathSummaryRecordPage` reference was calling `clear()` (a no-op) instead of `close()`.

## Test Results

**Before Fixes:**
- 80-98% memory leak
- Hundreds of finalizer leaks

**After Fixes:**
- ✅ Test passes
- ✅ **ZERO finalizer leaks**
- ✅ Memory: 15 MB (minimal)
- ✅ Bypass kept (required)

## Why Bypass Cannot Be Removed

The bypass is architecturally required because:

1. **Multi-revision reconstruction**: `combinePageFragments` creates different page instances for each revision
2. **Shared PageReferences**: All revisions share the same `key` (storage offset)  
3. **Cache collision**: Caching causes wrong revisions to be returned
4. **No simple fix**: PageReference equality doesn't include revision by design

## What We Learned

The diagnostic investigation revealed the bypass is NOT about:
- ❌ MemorySegment references (PathNodes don't have them)
- ❌ TIL isolation (putMapping keeps cache updated)

The bypass IS about:
- ✅ **Multi-revision page reconstruction with shared PageReference keys**
- ✅ **Preventing cache collisions between revision views**

## Diagnostic Commands

```bash
# Enable diagnostics
./gradlew :sirix-core:test --tests "*VersioningTest.testFull1" \
  -Dsirix.debug.path.summary=true --rerun-tasks

# Check what's being PUT to TIL
grep "TIL-PUT.*PATH_SUMMARY" output.log

# Check bypass loading
grep "PATH_SUMMARY-BYPASS" output.log

# Check PageReference keys
grep "PATH_SUMMARY-REPLACE.*PageRef" output.log

# Check for leaks
grep "FINALIZER LEAK.*PATH_SUMMARY" memory-leak-diagnostic.log
```

## Conclusion

The bypass is correctly designed and now properly manages memory. Investigation complete!

