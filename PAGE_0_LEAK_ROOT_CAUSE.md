# Page 0 (Root Page) Leak - Root Cause Analysis

## Current Status

### Improvements Made:
- **Pin count leaks:** 5 → 0 (100% fixed ✅)
- **Unclosed pages:** 104 → ~90 (13% improvement ✅)
- **Finalizer catches:** 870 → 952 (9% worse ⚠️)

### Remaining Issue:
**~950 Page 0 instances** still being caught by finalizer instead of explicitly closed.

## Root Cause Hypothesis

The issue is with pages that are **removed from cache while still pinned**:

### The Flow:

1. **Transaction loads Page 0**
   ```java
   page = cache.get(ref, () -> loadFromDisk());  // Pins page, adds to cache
   page.incrementPinCount(trxId);  // pinCount = 1
   ```

2. **Page modified → moved to TIL**
   ```java
   TIL.put(ref, PageContainer.getInstance(page, page));
   // calls: cache.remove(ref)
   // Removal listener sees: pinCount = 1
   // Decision: Skip closing (page is pinned, will be in TIL)
   // Result: Page removed from cache, NOT closed
   ```

3. **Transaction continues, unpins page**
   ```java
   // Later in transaction:
   unpinAllPagesForTransaction(trxId);
   // Scans cache - but page is NO LONGER in cache!
   // Page is in TIL, not in cache
   // Result: Page NOT unpinned
   ```

4. **Transaction commits**
   ```java
   TIL.clear() {
     for (PageContainer : list) {
       complete.close();  // Tries to close
       modified.close();  // Same instance, already closed
     }
   }
   ```

5. **But if close() fails or page escapes TIL**
   - Page has pinCount > 0 still
   - Not in cache
   - Not reachable except through weak references
   - → Finalizer catches it

## The Problem

Our `unpinAllPagesForTransaction()` only scans **pages in cache**:

```java
private void unpinAllPagesForTransaction(int transactionId) {
  // Scan RecordPageCache
  for (var entry : resourceBufferManager.getRecordPageCache().asMap().entrySet()) {
    // ...
  }
  
  // Missing: Pages that were REMOVED from cache (in TIL)
}
```

**Pages in TIL are NOT in cache**, so they don't get unpinned!

## The Solution

We need to ALSO unpin pages when they're added to TIL:

### Option 1: Unpin in TIL.put()

```java
public void put(PageReference key, PageContainer value) {
  // Before removing from cache, unpin the page
  KeyValueLeafPage cachedPage = bufferManager.getRecordPageCache().get(key);
  if (cachedPage instanceof KeyValueLeafPage kvp) {
    // Unpin for ALL transactions that have it pinned
    // Or track which transaction is adding to TIL and unpin for that one
  }
  
  bufferManager.getRecordPageCache().remove(key);
  // ...
}
```

### Option 2: Track Pages Removed from Cache While Pinned

```java
// In NodePageTrx or somewhere
private Set<KeyValueLeafPage> pagesRemovedWhilePinned = ConcurrentHashMap.newKeySet();

// When removing from cache:
if (page.getPinCount() > 0) {
  pagesRemovedWhilePinned.add(page);
}

// On close:
for (var page : pagesRemovedWhilePinned) {
  // Unpin and close
}
```

### Option 3: Unpin Pages Before Commit

```java
public UberPage commit(...) {
  // Before serializing, unpin all pages for this transaction
  unpinAllPagesForTransaction(trxId);
  
  // Then serialize and clear TIL
  parallelSerializationOfKeyValuePages();
  log.clear();  // Pages now have pinCount=0, can be closed
}
```

## Evidence

The fact that finalizer catches are happening proves:
1. Pages are allocated (memory segments assigned)
2. Pages are NOT explicitly closed
3. Pages become unreachable (no strong references)
4. Finalizer catches them and closes them

The 24 NAME pages leaked (44 created - 20 closed) suggests:
- Half are properly closed (in TIL, cleaned up)
- Half escape cleanup (maybe not in TIL? Or TIL.clear() doesn't close them?)

## Next Investigation

Need to determine:
1. Are the leaked Page 0 instances actually in TIL when commit happens?
2. Or are they created somewhere else (Names.fromStorage())?
3. Do they have pinCount > 0 when finalizer catches them?

To debug, we could add logging to:
- PageUtils.createTree() - log when Page 0 created
- TIL.put() - log when Page 0 added
- TIL.clear() - log when trying to close Page 0
- Finalizer - we already log this

This would show the full lifecycle and where they escape.

