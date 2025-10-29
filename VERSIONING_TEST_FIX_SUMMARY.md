# VersioningTest Memory Pool Exhaustion - Fix Summary

## Issues Identified

### 1. ✅ Memory Allocator Bug (FIXED)
**Problem:** The `LinuxMemorySegmentAllocator` had a critical bug in memory accounting.

**Root Cause:**
- The `borrowedSegments` set tracked segments that were EVER borrowed
- When segments were released via `madvise(MADV_DONTNEED)`, they were NOT removed from the set
- On subsequent borrows of the same segment:
  - Physical memory was NOT incremented (already in `borrowedSegments`)
  - On release, physical memory WAS decremented
  - This caused memory counter to become inaccurate over time

**Fix Applied:**
```java
// In LinuxMemorySegmentAllocator.release():
if (result == 0) {
  // Successfully released physical memory
  long newPhysical = physicalMemoryBytes.addAndGet(-size);
  
  // Remove from borrowed set so next allocation counts as first borrow
  // (since physical memory was actually released via madvise)
  borrowedSegments.remove(address);
}
```

**Location:** `bundles/sirix-core/src/main/java/io/sirix/cache/LinuxMemorySegmentAllocator.java:530`

### 2. ✅ Test Hygiene with Global BufferManager (FIXED)
**Problem:** With the global BufferManager, caches persist across tests, leading to memory buildup.

**Root Cause:**
- Previously, each test had its own BufferManager instance that was garbage collected
- Now, a single global BufferManager serves all tests
- Caches weren't being cleared between tests

**Fix Applied:**
```java
@AfterEach
public void tearDown() {
  // Clear caches BEFORE closing to release pinned pages
  try {
    io.sirix.access.Databases.getGlobalBufferManager().clearAllCaches();
  } catch (Exception e) {
    // Ignore if not initialized
  }
  database.close();
}
```

**Location:** `bundles/sirix-core/src/test/java/io/sirix/settings/VersioningTest.java:60-69`

## Remaining Issues

### ⚠️ Pin Count Leaks (PARTIAL)

**Problem:** Pages in TransactionIntentLog (TIL) are not being unpinned.

**Root Cause** (from existing documentation):
- `unpinAllPagesForTransaction()` only scans pages in cache
- Pages that were moved to TIL (during write transactions) are not in cache
- Therefore, those pages don't get unpinned on transaction close

**Evidence:**
```
RecordPageFragmentCache: 300 SIZE evictions, 261 EXPLICIT skipped (pinned)
```
- 261 pages remained pinned and couldn't be evicted
- This blocks memory segment release
- Eventually leads to pool exhaustion

**Existing Fix Attempts:**
According to `PAGE_0_LEAK_ROOT_CAUSE.md` and `PINNING_ARCHITECTURE_FIX_FINAL.md`, there were previous attempts to fix this, but the issue persists.

## Test Results

### Before Fixes:
- `testDifferential1`: **FAILED** with `OutOfMemoryError: Memory pool exhausted for size class 131072`
- Physical memory at shutdown: ~4103 MB (not properly released)
- Pinned pages: 261

### After Fixes:
- `testDifferential1`: **PASSED** (when run individually)
- Physical memory at shutdown: ~1519 MB (improved)
- Pinned pages: 138 (improved, but still high)

### Full Test Suite:
- Still experiencing issues with some tests
- Memory pool exhaustion still occurs in `testSlidingSnapshot1`

## Recommendations

### Short-term (Workarounds):

1. **Reduce Cache Sizes for Tests**
   - Modify test setup to use smaller `maxSegmentAllocationSize`
   - This leaves more memory segments available for page operations
   
   ```java
   @BeforeEach
   public void setUp() {
     XmlTestHelper.deleteEverything();
     var config = new DatabaseConfiguration(XmlTestHelper.PATHS.PATH1.getFile())
                      .setMaxSegmentAllocationSize(2L * (1L << 30)); // 2GB instead of 8GB
     Databases.createXmlDatabase(config);
     database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile());
   }
   ```

2. **Skip Problematic Tests Temporarily**
   - Mark `testSlidingSnapshot1` with `@Disabled` until pinning is fixed
   - Or run tests with increased memory: `-Xmx16g`

### Long-term (Proper Fixes):

1. **Fix TIL Unpinning**
   - Extend `unpinAllPagesForTransaction()` to also scan TIL
   - Ensure pages are unpinned before being moved to TIL
   - See: `PAGE_0_LEAK_ROOT_CAUSE.md` for detailed analysis

2. **Improve Cache Eviction**
   - Implement more aggressive eviction when memory pressure is high
   - Consider unpinning pages earlier in transaction lifecycle
   - Add memory pressure notifications to allocator

3. **Better Test Isolation**
   - Consider test-specific buffer manager configurations
   - Add explicit memory cleanup checkpoints in long tests
   - Monitor and assert on memory usage in tests

## Files Modified

1. `bundles/sirix-core/src/main/java/io/sirix/cache/LinuxMemorySegmentAllocator.java`
   - Line 530: Added `borrowedSegments.remove(address)` after successful madvise

2. `bundles/sirix-core/src/test/java/io/sirix/settings/VersioningTest.java`
   - Lines 60-69: Updated `tearDown()` to clear caches before closing database

## Conclusion

The memory allocator bug has been fixed, which improves memory accounting accuracy. The test hygiene issue has been addressed by clearing caches in tearDown(). However, the underlying pinning issue (pages in TIL not being unpinned) remains and is the root cause of continued test failures.

The proper fix requires updating the transaction close logic to unpin TIL pages, which is a more invasive change to the core transaction management code.

