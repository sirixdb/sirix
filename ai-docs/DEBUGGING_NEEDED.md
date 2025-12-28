# Debugging Needed: JSON Tests Hang

## Status: Partial Success

- ✅ XML tests pass (81/81)
- ❌ JSON tests hang (timeout after 60s)
- ❌ JVM crashes seen (SIGSEGV - use-after-free)

## Symptoms

### Test: `JsonNodeTrxRemoveTest.removeObjectKeyAsFirstChild`
- Starts normally (128 ClockSweeper threads start)
- Hangs after ~60 seconds
- No progress, just timeout
- No error message, just silence

## Potential Root Causes

### 1. Guard Leak
**Theory**: Guards never released, guardCount stays > 0 forever
-  All pages have guardCount > 0
- ClockSweeper can't evict anything
- Memory fills up
- System hangs

**How to test**:
- Add logging in closeCurrentPageGuard() to count closes
- Add logging in PageGuard constructor to count acquisitions
- Compare counts - should match

### 2. Missing Guard Paths
**Theory**: Some code paths access pages without acquiring guards
- ClockSweeper evicts page while being accessed
- Use-after-free → SIGSEGV crash or hang

**Known guard locations**:
- ✅ `setMostRecentlyReadRecordPage()`
- ✅ `getFromBufferManager()`
- ✅ `getMostRecentPage()` path
- ✅ `getInMemoryPageInstance()` (swizzled)
- ✅ PATH_SUMMARY cached access

**Possibly missing**:
- ❓ Fragment access in `combineRecordPages()`?
- ❓ TIL (TransactionIntentLog) page access?
- ❓ Index page access?
- ❓ Write transaction page preparation?

### 3. Concurrent Access Without Synchronization
**Theory**: Multiple threads accessing same PageReference.guardCount
- Race conditions in acquire/release
- guardCount goes negative or gets stuck

**Check**: AtomicInteger should prevent this, but verify no races

### 4. ClockSweeper Deadlock
**Theory**: Sweeper thread deadlocks while holding shard lock
- Tries to evict page
- Calls page.reset() or page.close()
- Those methods try to acquire something ClockSweeper already holds
- Deadlock

**Check**: Add logging around shard.evictionLock

### 5. Infinite Version Retry Loop
**Theory**: FrameReusedException thrown repeatedly
- Try to access page
- Version changes (evicted)
- Retry
- Version changes again (evicted again)
- Infinite loop

**Check**: Add retry counter, fail after N retries

## Immediate Actions Needed

### Option A: Add Comprehensive Logging
```java
// In PageGuard constructor:
LOGGER.debug("GUARD ACQUIRE: page={}, guardCount={}", page.getPageKey(), ref.getGuardCount());

// In PageGuard.close():
LOGGER.debug("GUARD RELEASE: page={}, guardCount={}", page.getPageKey(), ref.getGuardCount());

// In ClockSweeper.sweep():
if (ref.getGuardCount() > 0) {
  LOGGER.debug("Skipping page {} - guardCount={}", page.getPageKey(), ref.getGuardCount());
}
```

### Option B: Disable ClockSweeper Temporarily
Comment out `startClockSweepers()` to see if hang persists without eviction.
- If hang stops → guards/eviction issue
- If hang continues → different problem (maybe I/O or locking)

### Option C: Revert to Caffeine Only
Go back to commit `730a9e89e` (Caffeine with guard checks, no ShardedPageCache/ClockSweeper).
- Caffeine's eviction is proven to work
- Guards added in eviction listeners
- Can debug guard consistency without ClockSweeper complexity

## Recommendation

**Option B first** (disable ClockSweeper):
- Quick test to isolate the problem
- If it's sweeper-related, we know guards need more work
- If it's not, we know it's a different issue

Then **Option A** (logging) to find the specific issue.

## Current Branch State

Branch is NOT production ready due to JSON test hangs.

Need to either:
1. Fix the guard leaks/missing paths
2. Or revert to simpler approach and build up gradually

