# JSON Node Transaction Remove Test Fix

**Date**: November 13, 2025  
**Status**: ✅ FIXED  
**All `JsonNodeTrxRemoveTest` tests passing**

## Problem Summary

JSON node remove tests were failing with assertions like `expected:<4> but was:<5>` because nodes had corrupted parent references. During `remove()`, a node reference showed `parent=0` when it should have been `parent=16`.

## Root Cause

**Memory corruption due to missing page guards during `remove()` operation.**

### The Issue

1. The `remove()` method captures a reference to a node at the start: `final StructNode node = getCurrentNode()`
2. The node's data lives in a `MemorySegment` owned by a `KeyValueLeafPage`
3. During the `PostOrderAxis` traversal for removing subtrees, the cursor moves to different pages
4. When the cursor moves, `currentPageGuard` is released and a new guard is acquired for the new page
5. **The original page is now unguarded and can be modified or evicted**
6. Later reads from the `node` reference return corrupted data (wrong parent key)

### Why It Happened

When the pin count system was replaced with guards:
- ✅ Guard methods were added (`acquireGuard()`, `releaseGuard()`)
- ✅ `PageGuard` class was created
- ❌ **Guard count check was missing from `KeyValueLeafPage.close()`**
- ❌ **No mechanism to hold guards across cursor movements**

The old pin count system had:
```java
if (currentPinCount > 0) {
    return;  // Don't close pinned pages
}
```

This was removed without adding the equivalent guard check.

## The Fix

### 1. Add Guard Count Check to `KeyValueLeafPage.close()`

**File**: `bundles/sirix-core/src/main/java/io/sirix/page/KeyValueLeafPage.java`

```java
@Override
public synchronized void close() {
    if (!isClosed) {
        // CRITICAL FIX: Check guard count before closing
        int currentGuardCount = guardCount.get();
        if (currentGuardCount > 0) {
            // Page is still guarded - cannot close yet
            LOGGER.warn("Attempted to close guarded page {} ({}) rev={} with guardCount={} - skipping close",
                recordPageKey, indexType, revision, currentGuardCount);
            return;
        }
        // ... rest of close logic
    }
}
```

**Impact**: Pages with active guards cannot be closed/modified/evicted.

### 2. Acquire Separate Guard in `remove()` Method

**File**: `bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java`

```java
@Override
public JsonNodeTrx remove() {
    checkAccessAndCommit();
    if (lock != null) {
        lock.lock();
    }

    try {
        // CRITICAL FIX: Acquire a separate guard on the current node's page
        // to prevent it from being modified/evicted during PostOrderAxis traversal
        final io.sirix.cache.PageGuard nodePageGuard = pageTrx.acquireGuardForCurrentNode();
        
        try {
            final StructNode node = (StructNode) getCurrentNode();
            // ... remove logic ...
            return this;
        } finally {
            // Release the guard on the node's page
            nodePageGuard.close();
        }
    } finally {
        if (lock != null) {
            lock.unlock();
        }
    }
}
```

**Impact**: The page containing the node-to-remove is protected during the entire remove operation.

### 3. Add Helper Methods

**File**: `bundles/sirix-core/src/main/java/io/sirix/api/PageTrx.java`
```java
io.sirix.cache.PageGuard acquireGuardForCurrentNode();
```

**File**: `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodePageTrx.java`
```java
public io.sirix.cache.PageGuard acquireGuardForCurrentNode() {
    final var currentPage = ((io.sirix.access.trx.page.NodePageReadOnlyTrx) pageRtx).getCurrentPage();
    if (currentPage == null) {
        throw new IllegalStateException("No current page - cannot acquire guard");
    }
    return new io.sirix.cache.PageGuard(currentPage);
}
```

**File**: `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodePageReadOnlyTrx.java`
```java
public KeyValueLeafPage getCurrentPage() {
    return currentPageGuard != null ? currentPageGuard.page() : null;
}
```

## How the Fix Works

### Guard Lifecycle

```
1. remove() starts
   ├─ Acquire guard on page containing node-to-remove
   ├─ Guard count: 1 (prevents page from being closed)
   │
2. PostOrderAxis traversal
   ├─ Cursor moves to child pages
   ├─ currentPageGuard changes as cursor moves
   ├─ BUT: Original page still has guardCount=1
   ├─ → Page cannot be closed/modified
   │
3. adaptForRemove() reads from node
   ├─ Node's MemorySegment still valid
   ├─ Reads correct parent key (16 instead of 0)
   │
4. Guard released at end of remove()
   └─ Guard count: 0 (page can now be evicted if needed)
```

### Guard vs currentPageGuard

- **`currentPageGuard`**: Tracks the page the cursor is currently on
  - Changes when cursor moves
  - Only protects the "current" page

- **`nodePageGuard`** (new): Explicit guard for a specific page
  - Independent of cursor movement
  - Protects a page while we hold a reference to nodes on it

## Test Results

**Before Fix:**
```
[DEBUG-REMOVE] adaptForRemove called for node 24, parent=0  ❌
JsonNodeTrxRemoveTest > removeEmptyObject FAILED
    AssertionError: expected:<4> but was:<5>
```

**After Fix:**
```
[DEBUG-REMOVE] adaptForRemove called for node 24, parent=16  ✅
[DEBUG-CHILDCOUNT] Parent 16 before=5, after=4, storeChildCount=true
JsonNodeTrxRemoveTest > removeEmptyObject PASSED
```

**All Tests:**
```bash
./gradlew :sirix-core:test --tests "io.sirix.access.node.json.JsonNodeTrxRemoveTest"
BUILD SUCCESSFUL
10/10 tests passed
```

## Key Insights

1. **Guards must be acquired when holding node references across cursor movements**
   - `getCurrentNode()` returns a node whose MemorySegment can be corrupted
   - If cursor moves, the page can be modified/evicted
   - Solution: Acquire an explicit guard before cursor movement

2. **Guard count must prevent page closure**
   - Guards are useless if pages can still be closed
   - The check `if (guardCount > 0) return;` is critical

3. **MemorySegment-backed nodes are vulnerable to corruption**
   - Unlike heap objects, MemorySegments can be reused
   - Page eviction → segment released → segment reused → old references corrupted
   - Guards prevent this by blocking eviction

## Lessons Learned

- When replacing a lifecycle management system (pins → guards), ensure **all checks** are migrated
- Missing a single check (`guardCount > 0` in `close()`) broke the entire guard system
- The guard infrastructure existed but was never being used properly
- Debug logging was essential for tracking down the corruption

## Files Modified

1. `bundles/sirix-core/src/main/java/io/sirix/page/KeyValueLeafPage.java`
   - Added guard count check in `close()`

2. `bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java`
   - Acquire/release guard in `remove()`

3. `bundles/sirix-core/src/main/java/io/sirix/api/PageTrx.java`
   - Added `acquireGuardForCurrentNode()` to interface

4. `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodePageTrx.java`
   - Implemented `acquireGuardForCurrentNode()`

5. `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodePageReadOnlyTrx.java`
   - Added `getCurrentPage()` helper method

## Next Steps

- ✅ All JSON remove tests pass
- Consider if other operations need similar guard protection
- Monitor for similar issues in other transaction methods
- Document guard acquisition patterns for future developers

---

**Conclusion**: The fix correctly implements the LeanStore/Umbra guard pattern by ensuring pages with active guards cannot be closed, and by acquiring guards when holding node references across cursor movements.

