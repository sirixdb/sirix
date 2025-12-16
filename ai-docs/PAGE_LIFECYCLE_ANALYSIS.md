# Complete Page Lifecycle Analysis

## Page Creation Points

### 1. Loaded from Disk
```java
page = pageReader.read(ref, config);
```
**Ownership:** Should go into cache → closed by removal listener

### 2. Combined Pages
```java
completePage = combineRecordPages(fragments, ...);
```
**Ownership:** ??? This is the leak!

### 3. New Pages (Write Txn)
```java
page = firstPage.newInstance(recordPageKey, indexType, pageReadTrx);
```
**Ownership:** Goes into TIL → closed by TIL.clear()

---

## Page Storage Locations

### Location 1: Cache (RecordPageCache)
- Entry point: `cache.put(ref, page)`
- Exit point: Removal listener calls `page.close()`
- ✅ **Proper cleanup**

### Location 2: TransactionIntentLog (TIL)
- Entry point: `til.put(ref, PageContainer(complete, modified))`
- Exit point: `til.clear()` calls `complete.close()` and `modified.close()`
- ✅ **Proper cleanup** (supposedly)

### Location 3: Swizzled in PageReference
- Entry point: `ref.setPage(page)`
- Exit point: **??? NONE! This is the leak!**
- ❌ **NO cleanup**

---

## The Leak: Swizzled Combined Pages

### Flow:
```java
// 1. Load fragments from cache
fragments = getPageFragments(ref);  // Fragments in cache, guarded

// 2. Combine into new page
combinedPage = combineRecordPages(fragments, ...);  // NEW page created

// 3. Swizzle into reference
ref.setPage(combinedPage);  // ← Swizzled but NOT in cache!

// 4. Release fragment guards
for (frag : fragments) {
    frag.releaseGuard();
}

// 5. Return combined page
return combinedPage;  // Caller uses it

// ??? When is combinedPage closed?
// - Not in cache (removal listener won't see it)
// - Not in TIL (this is read-only transaction)
// - Just sitting in ref.page field
// - Eventually GC'd → finalizer catches it ❌
```

---

## The Bug

**Combined pages should be added to cache but aren't!**

Why? Let's check the code path.

