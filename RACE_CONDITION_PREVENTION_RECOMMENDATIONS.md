# Race Condition Prevention - Best Practices and Recommendations

## Critical Rules for Caffeine Cache Usage

### ❌ NEVER DO THIS

```java
// 1. NEVER mutate inside compute()
cache.get(key, (k, page) -> {
  page.incrementPinCount(trxId);  // ❌ SIDE EFFECT!
  return page;
});

// 2. NEVER nest compute() calls
cache.get(key1, (k1, v1) -> {
  cache.get(key2, (k2, v2) -> {  // ❌ DEADLOCK RISK!
    return v2;
  });
  return v1;
});

// 3. NEVER do I/O inside compute()
cache.get(key, (k, v) -> {
  return pageReader.read(...);  // ❌ BLOCKS OTHER THREADS!
});

// 4. NEVER use compute() then cache.put()
cache.get(key, (k, v) -> {
  if (v == null) {
    v = loadFromDisk();
  }
  return v;
});
cache.put(key, v);  // ❌ RACE: Another thread might have added different value!
```

### ✅ ALWAYS DO THIS

```java
// 1. Pin/unpin using atomic operations
cache.pinAndUpdateWeight(key, trxId);    // ✅ ATOMIC
cache.unpinAndUpdateWeight(key, trxId);  // ✅ ATOMIC

// 2. Use putIfAbsent to avoid races
KeyValueLeafPage myPage = loadFromDisk();
KeyValueLeafPage cachedPage = cache.asMap().putIfAbsent(key, myPage);
if (cachedPage != null) {
  myPage.close();  // Another thread won, use theirs
  return cachedPage;
}
return myPage;  // We won

// 3. Check cache first, load separately
KeyValueLeafPage page = cache.get(key);
if (page == null) {
  page = loadFromDisk();
  KeyValueLeafPage existing = cache.asMap().putIfAbsent(key, page);
  if (existing != null) {
    page.close();
    page = existing;
  }
}
cache.pinAndUpdateWeight(key, trxId);
return page;

// 4. Use async for I/O
CompletableFuture<Page> future = CompletableFuture.supplyAsync(() -> 
  pageReader.read(reference, config)
);
```

---

## Specific Fixes Needed

### Fix #1: NodePageReadOnlyTrx.java Line 1067-1110

**CURRENT CODE (BROKEN)**:
```java
final var pageFromCache = resourceBufferManager.getRecordPageFragmentCache()
    .get(pageReference, (key, existingPage) -> {
      if (existingPage != null && !existingPage.isClosed()) {
        existingPage.incrementPinCount(trxId);  // ❌
        return existingPage;
      }
      return null;
    });

// ... later in async callback ...

resourceBufferManager.getRecordPageFragmentCache()
    .get(pageReference, (key, existingPage) -> {  // ❌ NESTED!
      if (existingPage != null && !existingPage.isClosed()) {
        existingPage.incrementPinCount(trxId);  // ❌
        return existingPage;
      }
      kvPage.incrementPinCount(trxId);  // ❌
      return kvPage;
    });
```

**FIXED CODE**:
```java
// Step 1: Quick cache check (no compute)
KeyValueLeafPage cachedFragment = resourceBufferManager
    .getRecordPageFragmentCache()
    .get(pageReference);

if (cachedFragment != null && !cachedFragment.isClosed()) {
  // Found in cache - pin atomically
  resourceBufferManager.getRecordPageFragmentCache()
      .pinAndUpdateWeight(pageReference, trxId);
  return CompletableFuture.completedFuture(cachedFragment);
}

// Step 2: Load async
return reader.readAsync(pageReference, resourceConfig)
    .thenApply(loadedPage -> {
      var kvPage = (KeyValueLeafPage) loadedPage;
      
      // Step 3: Try to add to cache atomically
      KeyValueLeafPage existing = resourceBufferManager
          .getRecordPageFragmentCache()
          .asMap()
          .putIfAbsent(pageReference, kvPage);
      
      if (existing != null && !existing.isClosed()) {
        // Another thread loaded it - use theirs
        kvPage.close();
        resourceBufferManager.getRecordPageFragmentCache()
            .pinAndUpdateWeight(pageReference, trxId);
        return existing;
      } else {
        // We won - pin our page
        kvPage.incrementPinCount(trxId);
        resourceBufferManager.getRecordPageFragmentCache()
            .put(pageReference, kvPage);  // Update weight
        return kvPage;
      }
    });
```

### Fix #2: NodePageReadOnlyTrx.java Line 640-657

**CURRENT CODE (BROKEN)**:
```java
final KeyValueLeafPage recordPageFromBuffer =
    resourceBufferManager.getRecordPageCache().get(pageReferenceToRecordPage, (_, value) -> {
      if (value != null && value.isClosed()) {
        value = null;
      }
      
      if (value != null) {
        // ... just return cached value
        return value;
      }
      
      // Load from disk or TIL
      // ...
    });
```

**ISSUE**: While this doesn't have nested compute, it does I/O inside compute (loading from disk/TIL).

**FIXED CODE**:
```java
// Step 1: Check cache
KeyValueLeafPage recordPageFromBuffer = resourceBufferManager
    .getRecordPageCache()
    .get(pageReferenceToRecordPage);

// Step 2: Handle closed pages
if (recordPageFromBuffer != null && recordPageFromBuffer.isClosed()) {
  resourceBufferManager.getRecordPageCache().remove(pageReferenceToRecordPage);
  recordPageFromBuffer = null;
}

// Step 3: If not in cache, load (outside compute)
if (recordPageFromBuffer == null) {
  // Check TIL first
  if (trxIntentLog != null) {
    var container = trxIntentLog.get(pageReferenceToRecordPage);
    if (container != null) {
      var modifiedPage = container.getModified();
      if (modifiedPage instanceof KeyValueLeafPage kvPage) {
        recordPageFromBuffer = kvPage;
      }
    }
  }
  
  // Load from disk if not in TIL
  if (recordPageFromBuffer == null) {
    recordPageFromBuffer = (KeyValueLeafPage) loadDataPageFromDurableStorageAndCombinePageFragments(
        pageReferenceToRecordPage);
  }
  
  // Try to cache atomically
  if (recordPageFromBuffer != null) {
    KeyValueLeafPage existing = resourceBufferManager.getRecordPageCache()
        .asMap()
        .putIfAbsent(pageReferenceToRecordPage, recordPageFromBuffer);
    if (existing != null && !existing.isClosed()) {
      // Another thread cached it - use theirs
      recordPageFromBuffer = existing;
    }
  }
}

// Step 4: Pin the page
if (recordPageFromBuffer != null) {
  resourceBufferManager.getRecordPageCache()
      .pinAndUpdateWeight(pageReferenceToRecordPage, trxId);
}
```

### Fix #3: NodePageReadOnlyTrx.java Line 1015-1030

**CURRENT CODE (BROKEN)**:
```java
page = resourceBufferManager.getRecordPageFragmentCache().get(pageReferenceWithKey, (_, value) -> {
  var kvPage = (value != null) ? value :
      (KeyValueLeafPage) pageReader.read(pageReferenceWithKey, resourceSession.getResourceConfig());
  
  kvPage.incrementPinCount(trxId);  // ❌ SIDE EFFECT!
  
  return kvPage;
});
```

**FIXED CODE**:
```java
// Step 1: Check cache
KeyValueLeafPage page = resourceBufferManager.getRecordPageFragmentCache()
    .get(pageReferenceWithKey);

// Step 2: Load if not cached
if (page == null) {
  page = (KeyValueLeafPage) pageReader.read(pageReferenceWithKey, 
      resourceSession.getResourceConfig());
  
  // Try to cache atomically
  KeyValueLeafPage existing = resourceBufferManager.getRecordPageFragmentCache()
      .asMap()
      .putIfAbsent(pageReferenceWithKey, page);
  
  if (existing != null && !existing.isClosed()) {
    // Another thread loaded it - use theirs
    page = existing;
  }
}

// Step 3: Pin (outside compute)
page.incrementPinCount(trxId);
resourceBufferManager.getRecordPageFragmentCache()
    .put(pageReferenceWithKey, page);  // Update weight

// Diagnostic logging
if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && page.getPageKey() == 0) {
  io.sirix.cache.DiagnosticLogger.log("FRAGMENT Page 0 loaded: " + page.getIndexType() + 
      " rev=" + page.getRevision() + " (instance=" + System.identityHashCode(page) + 
      ", pinsAfterLoad=" + page.getPinCount() + ")");
}
```

### Fix #4: NodePageReadOnlyTrx.java Line 222-228 (I/O in compute)

**CURRENT CODE (BROKEN)**:
```java
page = resourceBufferManager.getPageCache().get(reference, (_, _) -> {
  try {
    return pageReader.read(reference, resourceSession.getResourceConfig());  // ❌ BLOCKS!
  } catch (final SirixIOException e) {
    throw new IllegalStateException(e);
  }
});
```

**FIXED CODE**:
```java
// Step 1: Check cache
page = resourceBufferManager.getPageCache().get(reference);

// Step 2: Load if not cached (outside compute)
if (page == null) {
  try {
    page = pageReader.read(reference, resourceSession.getResourceConfig());
  } catch (final SirixIOException e) {
    throw new IllegalStateException(e);
  }
  
  // Step 3: Cache atomically
  if (page != null) {
    Page existing = resourceBufferManager.getPageCache()
        .asMap()
        .putIfAbsent(reference, page);
    if (existing != null) {
      page = existing;  // Use the one that was cached
    }
  }
}

// Step 4: Swizzle
if (page != null) {
  reference.setPage(page);
}
```

---

## Code Review Checklist

Before committing cache-related code, verify:

- [ ] No `incrementPinCount()` or `decrementPinCount()` inside `compute()` functions
- [ ] No nested `cache.get(_, computeFunction)` calls
- [ ] No I/O operations (disk read, network) inside `compute()` functions
- [ ] No `cache.put()` after `cache.get(_, computeFunction)` on same key
- [ ] Use `pinAndUpdateWeight()` / `unpinAndUpdateWeight()` for atomic pin/unpin
- [ ] Use `putIfAbsent()` to handle concurrent loads
- [ ] Check if page is closed after getting from cache
- [ ] Close duplicate pages loaded by losing thread in race

---

## Testing Requirements

### 1. Stress Test

Run each test 100 times:
```bash
for i in {1..100}; do 
  echo "Run $i"
  ./gradlew :sirix-core:test --tests "ConcurrentAxisTest" || exit 1
done
echo "✅ All 100 runs passed!"
```

### 2. Concurrent Test

Run with maximum parallelism:
```bash
./gradlew :sirix-core:test --tests "*Concurrent*" --max-workers=16 --parallel
```

### 3. Memory Leak Check

```bash
# Run test with memory diagnostics
java -Xmx512m \
     -Dsirix.debug.memory.leaks=true \
     -Dsirix.debug.path.summary=true \
     -jar sirix-core/build/libs/sirix-core-test.jar \
     --tests "ConcurrentAxisTest"

# Check output for:
# - "FINALIZED LEAK CAUGHT" messages (should be 0)
# - Pin count warnings (should be 0)
# - Unclosed page warnings (should be 0)
```

### 4. Deadlock Detection

Add to JVM args:
```bash
-Dcaffeine.unsafe.compute.enabled=false  # Fail fast on unsafe usage
```

---

## Monitoring in Production

### Add Metrics

```java
// In RecordPageCache
private static final AtomicLong computeCalls = new AtomicLong();
private static final AtomicLong computeRetries = new AtomicLong();
private static final AtomicLong nestedComputeDetected = new AtomicLong();

public KeyValueLeafPage get(PageReference key, BiFunction<...> mappingFunction) {
  computeCalls.incrementAndGet();
  
  // Detect nested compute
  if (isInCompute.get()) {
    nestedComputeDetected.incrementAndGet();
    LOGGER.error("NESTED COMPUTE DETECTED! This will cause deadlock!");
  }
  
  isInCompute.set(true);
  try {
    return cache.asMap().compute(key, mappingFunction);
  } finally {
    isInCompute.remove();
  }
}

// Export metrics via JMX or similar
```

### Add Assertions (Dev/Test Only)

```java
// In RecordPageCache constructor
if (Boolean.getBoolean("sirix.debug.cache.strict")) {
  // Wrap compute to detect violations
  this.safeCompute = (key, fn) -> {
    checkNoNestedCompute();
    checkNoSideEffects(fn);
    return cache.asMap().compute(key, fn);
  };
}
```

---

## Summary of Changes Needed

### Files to Modify

1. **NodePageReadOnlyTrx.java**
   - Fix lines 222-228 (I/O in compute)
   - Fix lines 640-657 (I/O in compute)
   - Fix lines 1015-1030 (pin in compute)
   - Fix lines 1067-1110 (nested compute + pin in compute)

2. **RecordPageCache.java**
   - Add nested compute detection (optional, for safety)
   - Document that `get(key, BiFunction)` should NOT be used with side effects

3. **RecordPageFragmentCache.java**
   - Same as RecordPageCache

4. **Test Infrastructure**
   - Add stress test runner (100 iterations)
   - Add concurrent test config
   - Add memory leak assertions

### Estimated Impact

- **Lines changed**: ~150 lines
- **Files affected**: 3-4 files
- **Test time**: 2-3 hours for full validation
- **Risk**: Low (changes make code MORE thread-safe)

### Success Criteria

- ✅ ConcurrentAxisTest passes 100/100 times
- ✅ No deadlocks detected
- ✅ No pin count leaks (all transactions unpin all pages)
- ✅ No memory leaks (all pages closed)
- ✅ CI tests pass consistently

---

## Long-Term Recommendations

1. **Deprecate `get(key, BiFunction)` Method**
   - Too easy to misuse
   - Replace with explicit `getOrLoad(key, Supplier<V>)` that handles caching internally

2. **Add Static Analysis**
   - PMD rule: Detect `cache.get(_, lambda)` with side effects
   - SpotBugs rule: Detect nested cache operations

3. **Caffeine Alternatives**
   - Consider explicit `LoadingCache` instead of manual compute
   - Or use simple `ConcurrentHashMap` with explicit synchronization

4. **Documentation**
   - Add Javadoc warnings to all cache `get(BiFunction)` methods
   - Link to this document from cache interfaces

5. **Training**
   - Share these learnings with team
   - Add to coding guidelines
   - Code review checklist









