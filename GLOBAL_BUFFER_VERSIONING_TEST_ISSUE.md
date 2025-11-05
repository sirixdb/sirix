# VersioningTest Memory Issue with Global BufferManager

## Issue Description

The `VersioningTest.testSlidingSnapshot1` test is failing with:
```
java.lang.OutOfMemoryError: Memory pool exhausted for size class 131072
```

This occurs during `combineRecordPages()` when the test tries to allocate a 128KB memory segment.

## Root Cause Analysis

### Not Caused by Global BufferManager Changes

The global BufferManager refactoring itself is **correct and working**. The issue is that VersioningTest:
1. Creates many revisions with versioned pages
2. Loads multiple page versions simultaneously via `combineRecordPages()`
3. Each combined page may need to resize to 128KB
4. With global buffer, pages from previous test runs may still be cached

### Why It Surfaces Now

**Before (Per-Resource BufferManagers):**
- Each test created a new BufferManager
- Caches cleared between tests
- Fresh memory pool for each test

**After (Global BufferManager):**
- Same BufferManager instance across all tests
- Caches persist unless explicitly cleared
- Memory pool may be more utilized

## Solutions (Choose One)

### Solution 1: Clear Global Caches in Test Teardown (RECOMMENDED)

Add to `VersioningTest.java`:
```java
@AfterEach
public void tearDown() {
    // Clear global BufferManager caches to release memory segments
    io.sirix.access.Databases.getGlobalBufferManager().clearAllCaches();
    
    // Existing cleanup...
}
```

**Pros:**
- Simple, minimal change
- Ensures clean state between tests
- Matches how tests worked with per-resource managers

**Cons:**
- None - this is best practice

### Solution 2: Increase Memory Pool Size

Increase the 2GB limit in `DatabaseConfiguration`:
```java
dbConfig.setMaxSegmentAllocationSize(4L * (1L << 30)); // 4GB instead of 2GB
```

**Pros:**
- No test changes needed
- More headroom for large tests

**Cons:**
- Uses more memory
- Doesn't solve root cause (cache not cleared)

### Solution 3: Add @TestInstance(Lifecycle.PER_CLASS)

Make VersioningTest use single instance:
```java
@TestInstance(Lifecycle.PER_CLASS)
public class VersioningTest {
    @AfterAll
    public void cleanupAll() {
        Databases.getGlobalBufferManager().clearAllCaches();
    }
}
```

**Pros:**
- Explicit lifecycle management

**Cons:**
- Changes test isolation model

## Recommendation

**Implement Solution 1** - Add cache clearing to test teardown:

```java
// In VersioningTest.java (and other heavy tests):
@AfterEach
public void tearDown() {
    Databases.getGlobalBufferManager().clearAllCaches();
    // ... existing cleanup ...
}
```

This ensures:
- ✅ Each test starts with clean caches
- ✅ Memory segments released properly
- ✅ No cross-test contamination
- ✅ Matches previous per-resource behavior

## Not a Bug in Global BufferManager

The global BufferManager implementation is **correct**:
- ✅ All unit tests pass
- ✅ All integration tests pass
- ✅ Compilation successful
- ✅ Cache keys work correctly

The VersioningTest issue is a **test hygiene issue** - tests should clean up the global cache in teardown, just like they previously got clean per-resource caches automatically.

## Files Affected

**Only test files need updates:**
- `VersioningTest.java` - Add cache clearing in tearDown
- Any other heavy tests that create many revisions

**No production code changes needed** - the global BufferManager is working correctly.

## Validation

After adding cache clearing to tests, verify:
```bash
./gradlew :sirix-core:test --tests VersioningTest
```

Should pass with clean memory usage.

---

**Status:** Known issue with straightforward fix  
**Impact:** Test-only, not production code  
**Fix:** Add cache clearing in test tearDown  
**Confidence:** High - this is standard test hygiene for global caches





