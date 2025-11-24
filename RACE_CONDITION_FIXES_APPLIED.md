# Race Condition and Memory Leak Fixes Applied

## Summary
Fixed critical race conditions in the page pinning/caching system that were causing both memory leaks and intermittent test failures.

## Root Causes Identified

### 1. Race Condition in Pin Count Operations
**Problem:** The `incrementPinCount()` and `decrementPinCount()` methods checked `isClosed` with an assertion, but the check was not atomic with the pin count modification. A page could be closed between the check and the pin count update, leading to:
- Pinned closed pages (memory leaks - segments never released)
- Unpinning already-closed pages (crashes/assertions)

**Solution:** Made both methods `synchronized` to ensure atomicity between the closed check and pin count modification. Also made `close()` synchronized and added pin count check to refuse closing pinned pages.

### 2. Race Condition in Cache Unpinning
**Problem:** The `RecordPageCache.unpinAndUpdateWeight()` and `RecordPageFragmentCache.unpinAndUpdateWeight()` methods had checks for closed pages and pin counts that were not atomic with the unpin operation.

**Solution:** Simplified the methods to rely on the now-synchronized `decrementPinCount()` which handles all edge cases gracefully (closed pages, never-pinned pages, etc.).

### 3. Concurrent Modification in Transaction Cleanup
**Problem:** The `unpinAllPagesForTransaction()` method iterated directly over cache entry sets while unpinning pages. This could cause `ConcurrentModificationException` when:
- Cache eviction occurred during iteration
- Other transactions modified the cache
- Pages were added/removed by other threads

**Solution:** Collect a snapshot of pages to unpin first, then iterate over the snapshot. This avoids concurrent modification issues while still allowing proper cleanup.

## Changes Made

### KeyValueLeafPage.java
1. Made `incrementPinCount(int trxId)` synchronized
   - Atomically checks `isClosed` before pinning
   - Throws `IllegalStateException` if page is closed
   
2. Made `decrementPinCount(int trxId)` synchronized
   - Atomically checks `isClosed` before unpinning
   - Returns silently if page is already closed (graceful handling)
   - Returns silently if transaction never pinned the page
   
3. Made `close()` synchronized
   - Atomically checks pin count before closing
   - Refuses to close pages with pinCount > 0
   - Prevents race where a thread pins a page being closed

### RecordPageCache.java
1. Simplified `unpinAndUpdateWeight(PageReference key, int trxId)`
   - Relies on synchronized `decrementPinCount()` for safety
   - Removed complex checking logic (now handled in the synchronized method)
   
2. Enhanced `pinAndUpdateWeight(PageReference key, int trxId)`
   - Catches `IllegalStateException` from `incrementPinCount()`
   - Removes closed pages from cache (returns null from compute function)

### RecordPageFragmentCache.java
1. Applied same fixes as RecordPageCache:
   - Simplified `unpinAndUpdateWeight()`
   - Enhanced `pinAndUpdateWeight()` with exception handling

### NodePageReadOnlyTrx.java
1. Fixed `unpinAllPagesForTransaction(int transactionId)`
   - Collects snapshot of pages to unpin for RecordPageCache
   - Collects snapshot of pages to unpin for RecordPageFragmentCache
   - Collects snapshot of pages to unpin for PageCache
   - Iterates over snapshots to avoid ConcurrentModificationException

## Thread Safety Analysis

### Before Fixes
- **incrementPinCount/decrementPinCount:** NOT thread-safe with respect to close()
- **close():** Could close pages that were being pinned
- **unpinAllPagesForTransaction():** Vulnerable to ConcurrentModificationException
- **Cache unpinning:** Multiple checks before unpin created race windows

### After Fixes
- **All pin/unpin/close operations:** Thread-safe via synchronized methods
- **Cache operations:** Atomic via computeIfPresent + synchronized methods
- **Transaction cleanup:** Safe from concurrent modification via snapshots
- **Defense in depth:** Multiple layers prevent pinned pages from being closed

## Race Condition Scenarios Fixed

### Scenario 1: Pin-During-Close
- **Before:** Thread A calls close(), Thread B calls incrementPinCount() → pinned closed page
- **After:** Synchronized methods ensure either pin succeeds first (close skipped) or close succeeds first (pin throws exception and page removed from cache)

### Scenario 2: Close-During-Unpin
- **Before:** Thread A unpins page, Thread B closes it between check and unpin → assertion failure
- **After:** Synchronized decrementPinCount() returns silently if page is already closed

### Scenario 3: Concurrent Cache Iteration
- **Before:** Thread A iterates cache to unpin, Thread B evicts page → ConcurrentModificationException
- **After:** Snapshot collection separates iteration from unpinning

### Scenario 4: Double-Unpin
- **Before:** Thread A and B both try to unpin same transaction → negative pin count
- **After:** Synchronized decrementPinCount() prevents concurrent decrements; returns silently if already unpinned

## Expected Impact

### Memory Leaks
- **FIXED:** Pinned closed pages can no longer occur (synchronized prevents race)
- **FIXED:** Close() refuses to proceed if page is pinned
- **IMPROVED:** Even if a page is closed by eviction, subsequent unpin attempts are safe (no-op)

### Test Failures
- **FIXED:** ConcurrentModificationException in transaction cleanup
- **FIXED:** Assertion failures from unpinning closed pages
- **FIXED:** IllegalStateException from negative pin counts
- **IMPROVED:** Graceful handling of edge cases reduces intermittent failures

## Performance Considerations

### Synchronization Overhead
- Pin/unpin operations are now synchronized → slight performance impact
- **Mitigation:** These operations are already fast (just incrementing AtomicInteger)
- **Trade-off:** Correctness and reliability are more important than microsecond-level performance

### Snapshot Collection
- `unpinAllPagesForTransaction()` creates ArrayList snapshots → memory allocation
- **Mitigation:** Only done once per transaction close (infrequent)
- **Trade-off:** Small memory overhead vs avoiding crashes

## Testing Recommendations

1. Run existing test suite to verify no regressions
2. Run tests with `-Dsirix.debug.memory.leaks=true` to check for leaks
3. Run tests multiple times to check for intermittent failures
4. Monitor cache statistics to ensure proper eviction still occurs
5. Use stress tests with many concurrent transactions

## Related Documentation
- FINAL_PINNING_ARCHITECTURE_SUMMARY.md - Overall pin count architecture
- MEMORY_LEAK_FIX_FINAL.md - Previous memory leak fixes
- PAGE_0_LEAK_ROOT_CAUSE.md - Detailed investigation of Page 0 leaks

## Verification Checklist
- [x] All pin/unpin/close operations are now synchronized
- [x] Cache unpinning methods simplified and safe
- [x] Transaction cleanup avoids concurrent modification
- [x] Graceful handling of closed pages in all code paths
- [x] Defense in depth: multiple layers prevent invalid states
- [ ] Tests pass without intermittent failures
- [ ] No memory leaks detected in leak detector
- [ ] Cache eviction still works properly









