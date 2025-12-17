# Why Only PATH_SUMMARY Needs Special TIL Handling

## The Key Difference

**PATH_SUMMARY uses a SEPARATE READER that bypasses the write transaction's TIL-aware caching.**

## How DOCUMENT Pages Work (No Problem)

### Writing DOCUMENT Pages:
```java
// In NodeTrx (write transaction)
NodeTrx.insertElement(...)
  → NodePageTrx.createRecord(..., IndexType.DOCUMENT, ...)
    → prepareRecordPage(recordPageKey, index, DOCUMENT)
      → pageContainerCache.computeIfAbsent(..., _ -> log.get(reference))  // Checks TIL!
```

### Reading DOCUMENT Pages:
```java
// Same NodeTrx instance
NodeTrx.moveTo(nodeKey)
  → NodeTrx.getNode()
    → NodePageTrx.getRecord(recordKey, IndexType.DOCUMENT, ...)
      → prepareRecordPage(recordPageKey, index, DOCUMENT)
        → pageContainerCache.computeIfAbsent(..., _ -> log.get(reference))  // Gets from TIL!
```

**Result:** ✅ Reads and writes both use `NodePageTrx.prepareRecordPage()` → checks `pageContainerCache` → gets from TIL

---

## How PATH_SUMMARY Works (Problem!)

### Writing PATH_SUMMARY Pages:
```java
// In PathSummaryWriter (inside NodeTrx)
PathSummaryWriter.insertPathAsFirstChild(...)
  → pageTrx.prepareRecordForModification(..., IndexType.PATH_SUMMARY, ...)
    → NodePageTrx.prepareRecordPage(recordPageKey, index, PATH_SUMMARY)
      → pageContainerCache.computeIfAbsent(..., _ -> log.get(reference))  // Stores in TIL!
```

### Reading PATH_SUMMARY Pages:
```java
// PathSummaryReader (separate instance!)
PathSummaryReader.moveTo(pathNodeKey)
  → PathSummaryReader.currentNode = pageReadTrx.getRecord(...)
    → PageReadOnlyTrx.getRecord(recordKey, IndexType.PATH_SUMMARY, ...)
      → getRecordPage(indexLogKey)  
        → getFromBufferManager(indexLogKey, pageRef)
          → RecordPageCache.get(...)  // MISSES TIL! Gets old committed data ❌
```

**Result:** ❌ Writes go to TIL via NodePageTrx, but reads use PathSummaryReader → PageReadOnlyTrx → RecordPageCache → STALE DATA!

---

## Why PATH_SUMMARY is Unique

### 1. Separate Reader Architecture

**DOCUMENT:**
- `NodeReadOnlyTrx` and `NodeTrx` are the SAME instance in write transactions
- Both reads and writes go through NodePageTrx
- TIL-aware caching works correctly

**PATH_SUMMARY:**
- `PathSummaryReader` is a SEPARATE instance from `PathSummaryWriter`
- Writer uses `NodePageTrx` (write path)
- Reader uses `PageReadOnlyTrx` (read path)
- TIL-aware caching is BYPASSED!

### 2. Read-Modify-Write Pattern

**DOCUMENT:**
```java
// Typically write-only in a transaction
wtx.insertElement(qname);  // Write only
```

**PATH_SUMMARY:**
```java
// Read-modify-write pattern
wtx.insertElement(qname);
  → PathSummaryWriter.getPathNodeKey(qname)  
    → PathSummaryReader.moveTo(...)           // READ first
      → axis.forEach(...)                      // Traverse existing tree
    → PathSummaryWriter.insertPathAsFirstChild(...)  // WRITE after
```

PATH_SUMMARY needs to READ the existing path tree structure BEFORE it can determine where to insert new nodes. This read-modify-write pattern exposes the TIL isolation issue.

### 3. Metadata vs. Data

**DOCUMENT pages:** Contain user data (elements, text, attributes)
- Accessed via `NodeTrx` which has TIL-aware caching

**PATH_SUMMARY pages:** Contain metadata about the document structure
- Accessed via `PathSummaryReader` which uses lower-level PageReadOnlyTrx
- Optimization: separate reader allows fast path queries without node transaction overhead
- Side-effect: Loses TIL awareness!

---

## Why Other Page Types Don't Have This Problem

### NAME Pages
- Only accessed during node creation (write-only)
- No need to read NAME pages after modifying them in same transaction
- Lookups use cached `NamePage` object, not low-level page reads

### CAS/PATH Index Pages
- Similar to NAME - mostly write-only during transaction
- Reads happen at commit time or after commit

### CHANGED_NODES Pages  
- Internal tracking, not exposed via reader API
- Accessed only through NodePageTrx (TIL-aware)

---

## The Fix

PATH_SUMMARY needs TIL lookup in `PageReadOnlyTrx.getFromBufferManager()`:

```java
private Page getFromBufferManager(IndexLogKey indexLogKey, PageReference pageRef) {
    // SPECIAL CASE: PATH_SUMMARY in write transactions
    if (trxIntentLog != null && indexLogKey.getIndexType() == IndexType.PATH_SUMMARY) {
        PageContainer container = trxIntentLog.get(pageRef);
        if (container != null) {
            // Found modified version in TIL!
            return container.getModifiedAsUnorderedKeyValuePage();
        }
    }
    
    // Normal path for other types or cache miss
    return resourceBufferManager.getRecordPageCache().get(pageRef, ...);
}
```

This makes PathSummaryReader TIL-aware without requiring it to use NodePageTrx.

---

## Summary

**PATH_SUMMARY is the ONLY page type that:**
1. Has a separate reader (PathSummaryReader) using PageReadOnlyTrx
2. Uses read-modify-write pattern within same transaction  
3. Exposes modified pages to the reader API during transaction

All other page types either:
- Use the same transaction instance for reads/writes (DOCUMENT)
- Don't need reads after writes in same transaction (NAME, CAS, PATH)
- Are internal and use TIL-aware access (CHANGED_NODES)

Therefore, only PATH_SUMMARY needs the special TIL check in the read path!

