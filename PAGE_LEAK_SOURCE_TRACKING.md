# Page Leak Source Tracking Guide

## Summary

This document explains how to track down where leaked pages come from when "FINALIZER LEAK CAUGHT" warnings appear. The system has built-in stack trace capturing, but it must be enabled with a system property.

## How Leak Detection Works

### 1. Finalizer-Based Detection

In `KeyValueLeafPage.java`, the `finalize()` method detects pages that weren't explicitly closed:

```java
protected void finalize() {
  if (!isClosed) {
    PAGES_FINALIZED_WITHOUT_CLOSE.incrementAndGet();
    
    // Track by type and pageKey for detailed leak analysis
    if (indexType != null) {
      FINALIZED_BY_TYPE.computeIfAbsent(indexType, _ -> new AtomicLong(0)).incrementAndGet();
    }
    FINALIZED_BY_PAGE_KEY.computeIfAbsent(recordPageKey, _ -> new AtomicLong(0)).incrementAndGet();
    
    // Print leak information with creation stack trace
    StringBuilder leakMsg = new StringBuilder();
    leakMsg.append(String.format("FINALIZER LEAK CAUGHT: Page %d (%s) revision=%d (instance=%d) - not closed explicitly!",
        recordPageKey, indexType, revision, System.identityHashCode(this)));
    
    if (creationStackTrace != null) {
      leakMsg.append("\n  Created at:");
      // Skip first 2 frames (getStackTrace, constructor), show next 8 frames
      for (int i = 2; i < Math.min(creationStackTrace.length, 10); i++) {
        StackTraceElement frame = creationStackTrace[i];
        leakMsg.append(String.format("\n    %s.%s(%s:%d)",
            frame.getClassName(), frame.getMethodName(),
            frame.getFileName(), frame.getLineNumber()));
      }
    }
    
    LOGGER.warn(leakMsg.toString());
  }
}
```

### 2. Stack Trace Capturing

Stack traces are captured in the constructor, but **only when `DEBUG_MEMORY_LEAKS` is enabled**:

```java
// DIAGNOSTIC: Capture creation stack trace for leak tracing
if (DEBUG_MEMORY_LEAKS) {
  this.creationStackTrace = Thread.currentThread().getStackTrace();
  long created = PAGES_CREATED.incrementAndGet();
  PAGES_BY_TYPE.computeIfAbsent(indexType, _ -> new AtomicLong(0)).incrementAndGet();
  boolean addedToLivePages = ALL_LIVE_PAGES.add(this);
  // ... tracking code ...
} else {
  this.creationStackTrace = null;
}
```

## Enabling Stack Trace Tracking

### Option 1: Via Gradle Test Command

```bash
cd bundles/sirix-core
./gradlew test --tests "YourTestClass" -Dsirix.debug.memory.leaks=true
```

### Option 2: Via build.gradle (Permanent)

In `bundles/sirix-core/build.gradle`, modify the test block:

```groovy
test {
    useJUnitPlatform()
    jvmArgs = [
        '--add-opens', 'java.base/sun.nio.ch=ALL-UNNAMED',
        '--add-opens', 'java.base/java.nio=ALL-UNNAMED',
    ]
    // Enable leak diagnostics by default
    systemProperty "sirix.debug.memory.leaks", "true"  // ← Change from "false" to "true"
    systemProperty "sirix.debug.leak.diagnostics", System.getProperty("sirix.debug.leak.diagnostics", "false")
    systemProperty "sirix.debug.pin.counts", System.getProperty("sirix.debug.pin.counts", "false")
    
    testLogging {
        showStandardStreams = true
        exceptionFormat = 'full'
    }
}
```

### Option 3: Via IDE (IntelliJ/Eclipse)

Add VM option to test configuration:
```
-Dsirix.debug.memory.leaks=true
```

## Where Pages Come From (5 Creation Paths)

### Path 1: New Page Creation via `newInstance()`

**Location:** `KeyValueLeafPage.java:1009-1030`

```java
public <C extends KeyValuePage<DataRecord>> C newInstance(@NonNegative long recordPageKey,
    @NonNull IndexType indexType, @NonNull StorageEngineReader pageReadTrx) {
  ResourceConfiguration config = pageReadTrx.getResourceSession().getResourceConfig();
  MemorySegmentAllocator allocator = OS.isWindows() 
      ? WindowsMemorySegmentAllocator.getInstance() 
      : LinuxMemorySegmentAllocator.getInstance();
  
  MemorySegment slotMemory = allocator.allocate(SIXTYFOUR_KB);
  MemorySegment deweyIdMemory = config.areDeweyIDsStored 
      ? allocator.allocate(SIXTYFOUR_KB) 
      : null;
  
  return (C) new KeyValueLeafPage(
      recordPageKey,
      indexType,
      config,
      pageReadTrx.getRevisionNumber(),
      slotMemory,
      deweyIdMemory
  );
}
```

**When:** Creating new pages during tree expansion.

### Path 2: Disk Load via `PageKind.deserializePage()`

**Location:** `PageKind.java:80-127`

```java
public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
    final SerializationType type) {
  final long recordPageKey = Utils.getVarLong(source);
  final int revision = source.readInt();
  final IndexType indexType = IndexType.getType(source.readByte());
  
  // Allocate memory for deserialized page
  MemorySegmentAllocator allocator = OS.isWindows() 
      ? WindowsMemorySegmentAllocator.getInstance() 
      : LinuxMemorySegmentAllocator.getInstance();
  
  MemorySegment slotMemory = allocator.allocate(slotsMemorySize);
  MemorySegment deweyIdMemory = (deweyIdsMemorySize > 0) 
      ? allocator.allocate(deweyIdsMemorySize) 
      : null;
  
  var page = new KeyValueLeafPage(
      recordPageKey,
      revision,
      indexType,
      resourceConfig,
      areDeweyIDsStored,
      recordPersister,
      references,
      slotMemory,
      deweyIdMemory,
      lastSlotIndex,
      lastDeweyIdIndex
  );
  // ... deserialize data into page ...
}
```

**When:** Loading pages from persistent storage.

### Path 3: Tree Initialization via `PageUtils.createTree()`

**Location:** `PageUtils.java:72-96`

```java
public static void createTree(final DatabaseType databaseType, @NonNull PageReference reference,
    final IndexType indexType, final StorageEngineReader pageReadTrx, final TransactionIntentLog log) {
  final ResourceConfiguration resourceConfiguration = pageReadTrx.getResourceSession().getResourceConfig();
  
  final MemorySegmentAllocator allocator = OS.isWindows() 
      ? WindowsMemorySegmentAllocator.getInstance()
      : LinuxMemorySegmentAllocator.getInstance();
  
  final KeyValueLeafPage recordPage = new KeyValueLeafPage(
      Fixed.ROOT_PAGE_KEY.getStandardProperty(),  // Always page key 0
      indexType,
      resourceConfiguration,
      pageReadTrx.getRevisionNumber(),
      allocator.allocate(SIXTYFOUR_KB),
      resourceConfiguration.areDeweyIDsStored ? allocator.allocate(SIXTYFOUR_KB) : null
  );

  // Create document root node
  final SirixDeweyID id = resourceConfiguration.areDeweyIDsStored ? SirixDeweyID.newRootID() : null;
  recordPage.setRecord(databaseType.getDocumentNode(id));

  log.put(reference, PageContainer.getInstance(recordPage, recordPage));
}
```

**When:** Creating a new index tree (initial page 0).

### Path 4: Copy-on-Write via `NodeStorageEngineWriter.combineRecordPagesForModification()`

**Location:** `NodeStorageEngineWriter.java:739-779`

```java
// When reference.getKey() == Constants.NULL_ID_LONG (new page needed)
final MemorySegmentAllocator allocator = OS.isWindows() 
    ? WindowsMemorySegmentAllocator.getInstance() 
    : LinuxMemorySegmentAllocator.getInstance();

// Create PAIR of pages (complete + modify)
final KeyValueLeafPage completePage = new KeyValueLeafPage(
    recordPageKey,
    indexType,
    getResourceSession().getResourceConfig(),
    pageRtx.getRevisionNumber(),
    allocator.allocate(SIXTYFOUR_KB),
    getResourceSession().getResourceConfig().areDeweyIDsStored 
        ? allocator.allocate(SIXTYFOUR_KB) 
        : null
);

final KeyValueLeafPage modifyPage = new KeyValueLeafPage(
    recordPageKey,
    indexType,
    getResourceSession().getResourceConfig(),
    pageRtx.getRevisionNumber(),
    allocator.allocate(SIXTYFOUR_KB),
    getResourceSession().getResourceConfig().areDeweyIDsStored 
        ? allocator.allocate(SIXTYFOUR_KB) 
        : null
);

pageContainer = PageContainer.getInstance(completePage, modifyPage);
```

**When:** Copy-on-write for modifications (creates TWO pages per container).

### Path 5: Copy-on-Write via `NodePageTrx.combineRecordPagesForModification()`

**Location:** `NodePageTrx.java:739-779`

Same pattern as Path 4, but from a different transaction type.

**When:** Copy-on-write during node page transactions.

## What to Look For in Enabled Logs

With `DEBUG_MEMORY_LEAKS=true`, you'll see stack traces like:

```
FINALIZER LEAK CAUGHT: Page 55 (DOCUMENT) revision=1 (instance=1612231729) - not closed explicitly!
  Created at:
    io.sirix.page.KeyValueLeafPage.<init>(KeyValueLeafPage.java:756)
    io.sirix.access.trx.page.NodeStorageEngineWriter.combineRecordPagesForModification(NodeStorageEngineWriter.java:756)
    io.sirix.access.trx.page.NodeStorageEngineWriter.prepareRecordPage(NodeStorageEngineWriter.java:450)
    io.sirix.access.trx.node.xml.XmlNodeTrxImpl.insertChild(XmlNodeTrxImpl.java:285)
    io.sirix.axis.concurrent.ConcurrentAxisTest.testAxisOperations(ConcurrentAxisTest.java:123)
```

This tells you:
1. **Which page:** Key 55, DOCUMENT index, revision 1
2. **Creation path:** Created in `combineRecordPagesForModification()` (Path 4)
3. **Call chain:** From `insertChild()` during test

## Common Leak Patterns

### Pattern 1: PageContainer Leaks

**Symptom:** BOTH complete and modify pages leak with same page key.

**Cause:** `PageContainer.getInstance(completePage, modifyPage)` created but never closed.

**Fix:** Ensure `PageContainer.close()` is called or that the TIL properly cleans up.

### Pattern 2: Deserialization Leaks

**Symptom:** Multiple revisions of same page key leak.

**Cause:** Pages loaded from disk via `dereferenceRecordPageForModification()` not closed.

**Fix:** Ensure loaded pages are released after use.

### Pattern 3: Guard-Protected Leaks

**Symptom:** Leak with message "GUARD PROTECTED: Attempted to close guarded page".

**Cause:** Cache eviction trying to close page still in use (guardCount > 0).

**Fix:** Guards must be released before page can be closed.

### Pattern 4: TIL Leaks

**Symptom:** Pages created during transaction never finalized.

**Cause:** `TransactionIntentLog.clear()` not called or doesn't close all entries.

**Fix:** Ensure TIL cleanup on transaction commit/abort.

## Analysis Workflow

1. **Enable debug flag:**
   ```bash
   ./gradlew test --tests "FMSETest" -Dsirix.debug.memory.leaks=true > leak-trace.log 2>&1
   ```

2. **Collect leak patterns:**
   ```bash
   grep -A 10 "FINALIZER LEAK CAUGHT" leak-trace.log | grep -E "(CAUGHT|Created at|io.sirix)" > leak-summary.txt
   ```

3. **Group by creation path:**
   ```bash
   # Count leaks by creation method
   grep "Created at" -A 1 leak-summary.txt | grep "io.sirix" | sort | uniq -c | sort -rn
   ```

4. **Identify hotspots:**
   - If most leaks from `combineRecordPagesForModification()`: PageContainer cleanup issue
   - If most from `deserializePage()`: Loaded page cleanup issue
   - If most from `createTree()`: Initial page 0 cleanup issue

5. **Cross-reference with guard logs:**
   ```bash
   grep "GUARD PROTECTED" leak-trace.log
   ```

6. **Check TIL cleanup:**
   ```bash
   grep "TIL.clear()" leak-trace.log -A 5
   ```

## Next Steps for Your Investigation

Based on your logs showing leaks without stack traces:

1. **Re-run with debug enabled:**
   ```bash
   cd /home/johannes/.cursor/worktrees/sirix/FuQIP
   ./gradlew :bundles:sirix-core:test --tests "io.sirix.diff.algorithm.FMSETest" \
     -Dsirix.debug.memory.leaks=true > fmse-with-stacks.log 2>&1
   ```

2. **Analyze the new log:**
   ```bash
   grep -A 10 "FINALIZER LEAK CAUGHT" fmse-with-stacks.log > leak-analysis.txt
   ```

3. **Look for patterns:**
   - Are leaks from `combineRecordPagesForModification()`? → PageContainer issue
   - Are leaks from `deserializePage()`? → Loaded page cleanup issue
   - Are leaks from concurrent test methods? → Reader lifecycle issue

4. **Focus cleanup efforts:**
   - Most leaks from one method → targeted fix
   - Leaks spread across paths → systemic cleanup issue (TIL or cache)

## Key Files to Check

- **Page creation:** `KeyValueLeafPage.java` (constructors at lines 214, 297, 1009)
- **Container creation:** `NodeStorageEngineWriter.java:739-779`, `NodePageTrx.java:739-779`
- **Page loading:** `PageKind.java:80-127`
- **Tree initialization:** `PageUtils.java:72-96`
- **Cleanup:** `TransactionIntentLog.java` (clear method), `NodeStorageEngineReader.java` (close methods)

## References

- `DEBUG_MEMORY_LEAKS` flag: `KeyValueLeafPage.java:57-58`
- Stack trace capture: `KeyValueLeafPage.java:242-243, 329`
- Finalizer detection: `KeyValueLeafPage.java:1066-1102`
- Build config: `bundles/sirix-core/build.gradle:63`


