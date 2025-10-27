# FMSE Bug - ROOT CAUSE CONFIRMED ✅

## The Bug

FMSETest fails because **element local names are lost during serialization**, producing invalid QNames like "y:/" instead of "y:Resources".

## Root Cause

**Multiple NAME indexes incorrectly share a single cache slot!**

### Problem Details

1. **NAME indexes use multiple offsets:**
   - `indexNumber=0` → ATTRIBUTES_REFERENCE_OFFSET
   - `indexNumber=1` → ELEMENTS_REFERENCE_OFFSET
   - `indexNumber=2` → NAMESPACE_REFERENCE_OFFSET
   - `indexNumber=3` → PROCESSING_INSTRUCTION_REFERENCE_OFFSET

2. **Each offset has its own KeyValueLeafPage** storing different name data

3. **All map to `IndexType.NAME`** when calling `getRecord(key, IndexType.NAME, indexNumber)`

4. **All cached in SINGLE `mostRecentNamePage` slot!**

### The Bug Scenario

```
Serialization workflow:
1. Read element name "Resources" (IndexType.NAME, indexNumber=1)
   → Loads ELEMENTS NAME page into mostRecentNamePage ✓

2. Read attribute name (IndexType.NAME, indexNumber=0)  
   → Loads ATTRIBUTES NAME page into mostRecentNamePage
   → ❌ OVERWRITES element page!

3. Read next element name
   → Uses cached mostRecentNamePage  
   → ❌ But it's the ATTRIBUTES page, not ELEMENTS page!
   → Returns wrong/empty data
   → Element serialized as "y:/" instead of "y:Resources"
```

### Evidence

**Original XML:**
```xml
<data key="d0"><y:Resources/></data>
```

**Serialized (broken):**
```xml
<data key="d0"><y:/></data>
```

The local name "Resources" is completely missing because serializer read from wrong NAME page.

## Breaking Commit

**e301dd78b:** "Fix: Per-transaction pin tracking and global BufferManager"

This commit changed from having 2 general-purpose cache slots to 8 per-IndexType slots:
- `mostRecentDocumentPage`
- `mostRecentNamePage` ← **BUG: Should track indexNumber too!**
- `mostRecentPathPage`
- etc.

**The fix didn't account for NAME having multiple sub-indexes (offsets).**

## The Fix

Need to track **both IndexType AND indexNumber** for cache key, not just IndexType.

### Option 1: Separate slots per NAME offset
```java
private RecordPage mostRecentElementNamePage;  // NAME offset 1
private RecordPage mostRecentAttributeNamePage;  // NAME offset 0
private RecordPage mostRecentNamespacePage;  // NAME offset 2
private RecordPage mostRecentPINamePage;  // NAME offset 3
```

### Option 2: Use composite key (IndexType + indexNumber)
```java
private Map<IndexLogKey, RecordPage> mostRecentPageByKey;
```

### Option 3: Revert to 2-slot design (original approach)
Keep `mostRecentlyReadRecordPage` and `secondMostRecentlyReadRecordPage` that can handle any index type/number combination.

## Impact

- **VersioningTest:** Not affected (doesn't use NAME index extensively)
- **PathSummaryTest:** Not affected (uses PATH_SUMMARY index)
- **FMSETest:** BROKEN - Heavy use of element/attribute names during diff/merge operations
- **Any XML operations with namespaces:** Potentially affected

## Recommendation

Fix the NAME page caching bug separately from VersioningTest memory leak fixes.

The VersioningTest fixes are correct and working. The FMSE issue is a distinct bug in NAME page caching that needs its own investigation and fix.


