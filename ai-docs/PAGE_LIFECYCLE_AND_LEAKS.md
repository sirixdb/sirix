# Page Lifecycle and Leak Sources

## Complete Page Lifecycle Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PAGE CREATION                                │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
         ┌───────────────────────────────────────────┐
         │  5 Creation Paths (see below)             │
         │  - newInstance()                          │
         │  - deserializePage() (disk load)          │
         │  - createTree() (initial tree)            │
         │  - combineRecordPagesForModification()    │
         │    * Creates TWO pages (complete+modify)  │
         └───────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          PAGE STORAGE                                │
└─────────────────────────────────────────────────────────────────────┘
                                 │
         ┌───────────────────────┴────────────────────────┐
         │                                                 │
         ▼                                                 ▼
┌──────────────────────┐                    ┌──────────────────────────┐
│   PageContainer      │                    │   Direct Cache           │
│   (TIL storage)      │                    │   (RecordPageCache)      │
├──────────────────────┤                    ├──────────────────────────┤
│ - complete: Page     │                    │ PageReference -> Page    │
│ - modify: Page       │                    │                          │
│ (TWO pages!)         │                    │ - ClockSweeper eviction  │
│                      │                    │ - Guard protection       │
└──────────────────────┘                    └──────────────────────────┘
         │                                                 │
         ▼                                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         PAGE ACCESS                                  │
└─────────────────────────────────────────────────────────────────────┘
         │
         ├─► NodeStorageEngineReader.getRecordPage()
         │   ├─► Checks: mostRecent* cache (per-index-type)
         │   ├─► Checks: RecordPageCache (shared)
         │   ├─► Loads:  From disk if miss
         │   └─► Guards: Acquires PageGuard (guardCount++)
         │
         ├─► NodeStorageEngineWriter.prepareRecordPage()
         │   ├─► Checks: TIL (TransactionIntentLog)
         │   ├─► Checks: pageContainerCache
         │   └─► Creates: PageContainer if needed (2 pages!)
         │
         └─► PathSummaryReader.getPathSummaryPage()
             └─► Special bypass logic for write transactions
┌─────────────────────────────────────────────────────────────────────┐
│                         PAGE RELEASE                                 │
└─────────────────────────────────────────────────────────────────────┘
         │
         ├─► Guard release: PageGuard.close() → guardCount--
         │   └─► When guardCount == 0: Page eligible for eviction
         │
         ├─► Reader close: NodeStorageEngineReader.close()
         │   ├─► closeCurrentPageGuard() → release active guard
         │   └─► closeMostRecentPageIfOrphaned() → close cached pages
         │
         ├─► Writer commit: NodeStorageEngineWriter.commit()
         │   ├─► TIL.putAll() → move to cache
         │   └─► TIL.clear() → close all entries
         │
         └─► Cache eviction: ClockSweeper (background)
             ├─► Checks: guardCount == 0 && version
             └─► Calls: page.close() → memory release
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         PAGE CLEANUP                                 │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
                   KeyValueLeafPage.close()
                   ├─► Check: guardCount == 0 (required!)
                   ├─► Set: isClosed = true
                   └─► Release: Memory segments to allocator
                                 │
                                 ▼
                   ┌──────────────────────────┐
                   │  GC runs finalizer       │
                   │  (if page leaked)        │
                   └──────────────────────────┘
                                 │
                                 ▼
                   finalize() checks isClosed
                   ├─► If false: FINALIZER LEAK CAUGHT!
                   └─► Logs: Stack trace (if DEBUG_MEMORY_LEAKS)
```

## Leak Sources by Location

### 1. Reader Transaction Leaks

**Location:** `NodeStorageEngineReader.getRecordPage()` (line 480)

**Flow:**
```
getRecordPage(indexLogKey)
  ├─► getFromBufferManager()
  │   ├─► recordPageCache.getAndGuard(ref) → acquires guard
  │   │   OR
  │   ├─► loadDataPageFromDurableStorageAndCombinePageFragments(ref)
  │   └─► closeCurrentPageGuard() → releases previous guard
  │
  └─► setMostRecentlyReadRecordPage() → caches page locally
```

**Leak Points:**
- ❌ `getAndGuard()` called but guard never released (missing `closeCurrentPageGuard()`)
- ❌ `mostRecent*Page` reference held after reader closes
- ❌ `loadDataPageFromDurableStorageAndCombinePageFragments()` fragments not closed

**Fix Pattern:**
```java
// BEFORE: Potential leak
KeyValueLeafPage page = cache.getAndGuard(ref);
// ... use page ...
// ❌ Forget to release guard

// AFTER: Guaranteed release
try (PageGuard guard = new PageGuard(page)) {
    KeyValueLeafPage p = guard.page();
    // ... use page ...
}  // ✅ Auto-release
```

### 2. Writer Transaction Leaks (PageContainer)

**Location:** `NodeStorageEngineWriter.combineRecordPagesForModification()` (line 739)

**Flow:**
```
combineRecordPagesForModification()
  ├─► Creates: completePage = new KeyValueLeafPage(...)
  ├─► Creates: modifyPage = new KeyValueLeafPage(...)
  └─► Stores: PageContainer.getInstance(completePage, modifyPage)
                                 │
                                 ▼
                  Stored in TIL (TransactionIntentLog)
                                 │
                                 ▼
                  On commit: TIL.putAll() → cache
                  On abort:  TIL.clear() → must close!
```

**Leak Points:**
- ❌ **TWO pages created per container** → 2x leak potential
- ❌ TIL.clear() doesn't close all entries
- ❌ Transaction aborted without cleanup
- ❌ Exception during commit → TIL entries orphaned

**Fix Pattern:**
```java
// Ensure PageContainer cleanup
PageContainer container = PageContainer.getInstance(completePage, modifyPage);
try {
    // ... use container ...
    til.put(ref, container);
} catch (Exception e) {
    // ✅ CRITICAL: Clean up on failure
    container.close();  // Closes BOTH pages
    throw e;
}

// On transaction end
til.clear();  // Must call close() on all entries
```

### 3. Disk Load Leaks (Fragment Combining)

**Location:** `NodeStorageEngineReader.loadDataPageFromDurableStorageAndCombinePageFragments()` (line 879)

**Flow:**
```
loadDataPageFromDurableStorageAndCombinePageFragments(ref)
  ├─► Load fragment 0 (most recent)
  ├─► Load fragment 1 (older)
  ├─► Load fragment 2 (even older)
  │   ...
  └─► Combine into single page
      ├─► Fragment 0: Kept and returned
      └─► Fragments 1+: Should be closed after combining!
```

**Leak Points:**
- ❌ Older fragments (1, 2, ...) not closed after combining
- ❌ Exception during combine → all fragments leaked
- ❌ Fragment cache evicts page while still referenced

**Fix Pattern:**
```java
List<KeyValueLeafPage> fragments = new ArrayList<>();
try {
    // Load all fragments
    for (PageFragmentKey key : keys) {
        var fragment = loadFragment(key);
        fragments.add(fragment);
    }
    
    // Combine fragments 
    var combinedPage = combineFragments(fragments);
    
    // ✅ Close all fragments except the result
    for (int i = 1; i < fragments.size(); i++) {
        fragments.get(i).close();
    }
    
    return combinedPage;  // Fragment 0 is reused
} catch (Exception e) {
    // ✅ Clean up ALL fragments on error
    fragments.forEach(KeyValueLeafPage::close);
    throw e;
}
```

### 4. Tree Initialization Leaks

**Location:** `PageUtils.createTree()` (line 72)

**Flow:**
```
createTree(indexType, pageRef, reader, til)
  ├─► Create: recordPage = new KeyValueLeafPage(pageKey=0, ...)
  ├─► Add root node: recordPage.setRecord(documentRoot)
  └─► Store: til.put(pageRef, PageContainer.getInstance(recordPage, recordPage))
                                             ^^^^^^^^^^^^  ^^^^^^^^^^^^
                                             Same page used twice!
```

**Leak Points:**
- ❌ **Page 0 created for each index** (DOCUMENT, PATH_SUMMARY, etc.)
- ❌ Same page instance used for both complete AND modify → double reference
- ❌ TIL cleanup must handle this special case

**Fix Pattern:**
```java
// Initial tree uses same page for both roles
var container = PageContainer.getInstance(recordPage, recordPage);
til.put(ref, container);

// On TIL.clear()
for (PageContainer pc : entries) {
    if (pc.complete == pc.modify) {
        // ✅ Only close once!
        pc.complete.close();
    } else {
        // ✅ Close both
        pc.complete.close();
        pc.modify.close();
    }
}
```

### 5. Path Summary Bypass Leaks

**Location:** `NodeStorageEngineReader.getRecordPage()` PATH_SUMMARY path (line 571)

**Flow:**
```
getRecordPage(indexLogKey) where indexType == PATH_SUMMARY
  ├─► Check: trxIntentLog != null (write transaction)
  └─► Bypass cache: Load directly from disk
      ├─► loadDataPageFromDurableStorageAndCombinePageFragments()
      ├─► Store in pathSummaryRecordPage (local field)
      └─► ❌ NOT in RecordPageCache!
                                 │
                                 ▼
      On reader close: Must close explicitly
                       (Cache won't do it!)
```

**Leak Points:**
- ❌ Bypassed pages not in cache → no automatic eviction
- ❌ `pathSummaryRecordPage` field not cleared → hard reference
- ❌ Reader closed without closing bypassed page

**Fix Pattern:**
```java
// On reader close
if (pathSummaryRecordPage != null && trxIntentLog != null) {
    // ✅ Bypassed page must be closed explicitly
    if (!pathSummaryRecordPage.page.isClosed()) {
        // Remove from cache if present (testing scenario)
        resourceBufferManager.getRecordPageCache().remove(pathSummaryRecordPage.pageReference);
        pathSummaryRecordPage.page.close();
    }
}
// ✅ Clear reference
pathSummaryRecordPage = null;
```

## Leak Detection by Pattern

### Pattern A: Missing Guard Release

**Symptom:**
```
FINALIZER LEAK CAUGHT: Page 42 (DOCUMENT) revision=1 (instance=123456789)
  Created at:
    io.sirix.page.KeyValueLeafPage.<init>(KeyValueLeafPage.java:216)
    io.sirix.page.KeyValueLeafPage.newInstance(KeyValueLeafPage.java:1022)
    ...NodeStorageEngineReader.getFromBufferManager(...)
```

**Diagnosis:** Page acquired with guard but guard never released.

**Solution:** Use try-with-resources for all PageGuard usage.

### Pattern B: PageContainer Not Closed

**Symptom:**
```
FINALIZER LEAK CAUGHT: Page 55 (DOCUMENT) revision=0 (instance=111111111)
FINALIZER LEAK CAUGHT: Page 55 (DOCUMENT) revision=0 (instance=222222222)
  Created at:
    ...NodeStorageEngineWriter.combineRecordPagesForModification(...)
```

**Diagnosis:** TWO pages with same key/revision → PageContainer pair both leaked.

**Solution:** Ensure TIL.clear() closes all PageContainer entries.

### Pattern C: Fragment Leak

**Symptom:**
```
FINALIZER LEAK CAUGHT: Page 100 (DOCUMENT) revision=1 (instance=333333333)
  Created at:
    ...PageKind.deserializePage(PageKind.java:115)
    ...loadDataPageFromDurableStorageAndCombinePageFragments(...)
```

**Diagnosis:** Old fragment loaded but not closed after combining.

**Solution:** Close all fragments except the first one after combine.

### Pattern D: Most Recent Page Leak

**Symptom:**
```
FINALIZER LEAK CAUGHT: Page 7 (DOCUMENT) revision=1 (instance=444444444)
  Created at:
    ...NodeStorageEngineReader.getRecordPage(...)
```

**Diagnosis:** Page cached in `mostRecent*Page` field, reader closed without cleanup.

**Solution:** Ensure `closeMostRecentPageIfOrphaned()` called for all cached pages.

## Instrumentation Points for Debugging

### Add to KeyValueLeafPage Constructor

```java
if (DEBUG_MEMORY_LEAKS) {
    this.creationStackTrace = Thread.currentThread().getStackTrace();
    
    // Track creation context
    StackTraceElement[] stack = creationStackTrace;
    for (int i = 0; i < Math.min(stack.length, 15); i++) {
        String methodName = stack[i].getMethodName();
        if (methodName.equals("combineRecordPagesForModification")) {
            LOGGER.info("[CREATE-CONTAINER] Page {} ({}) rev={} instance={} (PAIR MEMBER)",
                recordPageKey, indexType, revision, System.identityHashCode(this));
            break;
        } else if (methodName.equals("loadDataPageFromDurableStorageAndCombinePageFragments")) {
            LOGGER.info("[CREATE-FRAGMENT] Page {} ({}) rev={} instance={} (FRAGMENT)",
                recordPageKey, indexType, revision, System.identityHashCode(this));
            break;
        } else if (methodName.equals("createTree")) {
            LOGGER.info("[CREATE-TREE] Page {} ({}) rev={} instance={} (INITIAL)",
                recordPageKey, indexType, revision, System.identityHashCode(this));
            break;
        }
    }
}
```

### Add to PageContainer

```java
public void close() {
    LOGGER.debug("[CONTAINER-CLOSE] Closing PageContainer: complete={}, modify={}, sameInstance={}",
        System.identityHashCode(complete), 
        System.identityHashCode(modify),
        complete == modify);
    
    if (complete == modify) {
        // Only close once
        complete.close();
    } else {
        complete.close();
        modify.close();
    }
}
```

### Add to TIL.clear()

```java
public void clear() {
    LOGGER.debug("[TIL-CLEAR] Clearing {} entries", map.size());
    int containerCount = 0;
    int singleCount = 0;
    
    for (PageContainer pc : map.values()) {
        if (pc.complete == pc.modify) {
            singleCount++;
        } else {
            containerCount++;
        }
        pc.close();  // Must implement!
    }
    
    LOGGER.debug("[TIL-CLEAR] Closed {} containers (2 pages each), {} single pages",
        containerCount, singleCount);
    map.clear();
}
```

## Quick Reference: Where Pages Live

| Location | Type | Cleanup Responsibility | Leak Risk |
|----------|------|------------------------|-----------|
| `PageContainer` (TIL) | Complete + Modify | TIL.clear() on commit/abort | ⚠️ HIGH (2 pages) |
| `RecordPageCache` | Cached pages | ClockSweeper eviction | ✅ LOW (automatic) |
| `RecordPageFragmentCache` | Fragments | ClockSweeper eviction | ⚠️ MEDIUM (fragments) |
| `mostRecent*Page` fields | Recent pages | Reader.close() | ⚠️ MEDIUM (per-reader) |
| `pathSummaryRecordPage` | Bypassed page | Reader.close() explicit | ⚠️ HIGH (bypassed!) |
| `pageContainerCache` | Writer cache | Writer cleanup | ⚠️ HIGH (containers) |

## Summary: Top 3 Leak Sources

1. **PageContainer cleanup** (2 pages per container)
   - Fix: `TIL.clear()` must close all entries
   - Location: `NodeStorageEngineWriter.combineRecordPagesForModification()`

2. **Reader mostRecent* fields** (per-index-type caching)
   - Fix: `closeMostRecentPageIfOrphaned()` for all fields
   - Location: `NodeStorageEngineReader.close()`

3. **Fragment combining** (older fragments not closed)
   - Fix: Close fragments 1+ after combining
   - Location: `loadDataPageFromDurableStorageAndCombinePageFragments()`


