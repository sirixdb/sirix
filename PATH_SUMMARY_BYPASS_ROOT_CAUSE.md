# PATH_SUMMARY Bypass - ROOT CAUSE IDENTIFIED

## Diagnostic Evidence

### Run 1: Bypass Decision Point
```
[PATH_SUMMARY-DECISION] Taking BYPASS path:
  - TIL contains 11 total pages, 0 are PATH_SUMMARY
```

### Run 2: TIL Modifications
```
[TIL-PUT] Adding PATH_SUMMARY page to TIL: pageKey=0, revision=0, logKey=9
[TIL-PUT] Adding PATH_SUMMARY page to TIL: pageKey=0, revision=1, logKey=11
[TIL-PUT] Adding PATH_SUMMARY page to TIL: pageKey=0, revision=2, logKey=12
[TIL-PUT] Adding PATH_SUMMARY page to TIL: pageKey=1, revision=1, logKey=14
[TIL-PUT] Adding PATH_SUMMARY page to TIL: pageKey=2, revision=1, logKey=17
...
```

## The Complete Picture

### Timeline of Events

1. **Transaction Starts**
   - TIL is created
   - Some pages added (DOCUMENT, NAME, etc.) - 11 pages total
   - **0 PATH_SUMMARY pages** in TIL at this point

2. **PathSummaryReader Initializes** (EARLY)
   - Needs to read existing PATH_SUMMARY structure
   - Takes **BYPASS** path (write transaction detected)
   - Loads PATH_SUMMARY pages from disk
   - Caches PathNode objects in `pathNodeMapping` array
   - TIL still has 0 PATH_SUMMARY pages

3. **Elements Are Inserted** (LATER)
   - `wtx.insertElement(qname)` called
   - PATH_SUMMARY needs to be updated (new paths)
   - PathSummaryWriter modifies PATH_SUMMARY pages
   - Modified pages are **PUT into TIL** at logKeys 9, 11, 12, 14, 17...

4. **Subsequent PATH_SUMMARY Reads**
   - PathSummaryReader needs to see the modifications
   - Takes **BYPASS** path again
   - **PROBLEM**: Bypass loads from disk, NOT from TIL!
   - Should see modified version, but bypass doesn't check TIL

## Why Bypass Exists - TWO REASONS

### Reason 1: TIL Isolation (Confirmed)
**The documentation was CORRECT**, but timing matters:

- PATH_SUMMARY pages are modified and stored in TIL
- PathSummaryReader needs to see those modifications
- Normal cache path (getFromBufferManager) doesn't check TIL
- Would return stale committed data from RecordPageCache

**However**, the bypass ALSO doesn't get TIL data! It loads from disk!

### Reason 2: Zero-Copy Page Lifecycle (Also True)
From code comments:
```
// REQUIRED BYPASS for PATH_SUMMARY in write transactions:
// - PathSummaryReader.pathNodeMapping caches PathNode objects during initialization
// - Normal caching allows pages to be evicted while pathNodeMapping references them
```

The pathNodeMapping holds PathNode objects with MemorySegment references to page data.
If pages go into cache, they can be evicted, invalidating those references.

## The Bypass Doesn't Actually Solve Reason 1!

**Critical Discovery:**

The bypass does this:
```java
var loadedPage = loadDataPageFromDurableStorageAndCombinePageFragments(pageReferenceToRecordPage);
```

This loads from **durable storage** (disk), NOT from TIL!

So the bypass:
- ✅ Solves Reason 2 (keeps pages alive, not cached, preventing eviction)
- ❌ DOESN'T solve Reason 1 (still doesn't get TIL modifications!)

## How Does It Actually Work Then?

There must be another mechanism! Let me check what `combinePageFragments` does...

Perhaps:
1. The bypass loads the base page from disk
2. Then combines it with fragments
3. **And fragments might include TIL modifications?**

OR:

1. PathSummaryWriter directly updates `pathNodeMapping` when it modifies paths
2. So PathSummaryReader doesn't need to re-read pages to see modifications
3. The bypass is only needed to keep the INITIAL pages alive

## What Needs Investigation

1. **What does `loadDataPageFromDurableStorageAndCombinePageFragments` actually return?**
   - Does it somehow include TIL modifications?
   - Or does it only return committed data?

2. **Does PathSummaryWriter update PathSummaryReader.pathNodeMapping directly?**
   - Check `PathSummaryWriter.putMapping()`
   - This would bypass the need to re-read pages

3. **When are PATH_SUMMARY pages actually READ after being modified?**
   - Maybe they're only written, not re-read in the same transaction?

## Why It Leaks

The bypass:
1. Creates pages via `combineRecordPages()`
2. Never puts them in cache (to avoid eviction)
3. Keeps them swizzled in PageReferences
4. Stores mostRecentPathSummaryPage reference
5. When replaced, calls `clear()` which is a no-op
6. **Pages leak until GC**

## Next Steps

1. Check if PathSummaryWriter.putMapping() updates PathSummaryReader's cache
2. Add diagnostics to see if PATH_SUMMARY pages are READ after being PUT to TIL
3. Verify what `combinePageFragments` actually returns
4. Test if the bypass can be replaced with proper TIL lookup

## Key Insight

The bypass might be solving TWO problems:
1. Keep pages alive for zero-copy references (prevents eviction)
2. Avoid stale cache data (but doesn't actually get TIL data either!)

The REAL solution might be:
- PathSummaryWriter directly updates PathSummaryReader.pathNodeMapping
- So re-reading pages from TIL/disk isn't actually needed
- The bypass just keeps the INITIAL pages alive

Need to verify this hypothesis!

