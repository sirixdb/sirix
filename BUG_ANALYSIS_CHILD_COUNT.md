# Child Count Bug - Root Cause Analysis

Date: November 12, 2025  
Test: `JsonNodeTrxRemoveTest.removeEmptyObject`  
Error: `expected:<4> but was:<5>`

---

## Root Cause: Node Creation Bug

### The Problem

Node 24 (empty object `{}`) is created with **wrong parent**:
- **Expected parent**: Node 16 (the "tada" array)
- **Actual parent**: Node 0 (document root)

### Evidence

```
[DEBUG-REMOVE] adaptForRemove called for node 24, parent=0
[DEBUG-REMOVE] Retrieved parent node 0 for modification  
[DEBUG-CHILDCOUNT] Parent 0 before=1, after=0
```

Node 24 has `parent=0` when it should have `parent=16`.

### Why This Causes Test Failure

1. Remove test expects 4 children in array (nodes 17, 20, 23, 25)
2. But node 24 is a child of root (node 0), not array (node 16)
3. After "removing" node 24, it decrements document root's child count
4. Array still has 5 children (including node 24 which shouldn't be there)
5. Test expects 4 children → FAIL

---

## The Real Bug

The issue is NOT in:
- ❌ Guard implementation (works correctly)
- ❌ Child count decrement (works correctly)
- ❌ TIL retrieval (works correctly)
- ❌ Removal logic (works correctly)

The issue IS in:
- ✅ **Node insertion** - `insertObjectAsRightSibling()` sets wrong parent

---

## Where It Broke

- **First appeared**: Commit `b1420f4c6` ("Remove pinning infrastructure")
- **Affects**: JSON document creation
- **Impact**: Wrong parent keys assigned during insertion

---

## Hypothesis

When pinning was removed, something in the node creation/insertion logic broke. Possibly:
1. Fragment combining creates nodes with wrong parents
2. PageContainer caching returns stale parent references
3. Node serialization/deserialization corrupted

---

## Next Steps

1. Compare `adaptForInsert()` between working (`test-cache-changes-incrementally`) and broken (`refactor/buffer-manager-guards`)
2. Check if `prepareRecordForModification` returns correct parent during insertion
3. Trace through `insertObjectAsRightSibling()` to see where parent key gets set wrong

---

## Impact on Guard Implementation

**Guards are NOT the problem!** They work correctly. The bug is in node insertion logic that was broken when pinning was removed.

Guards can be shipped independently once insertion bug is fixed.












