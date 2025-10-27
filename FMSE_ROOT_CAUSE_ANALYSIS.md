# FMSE Test Failure - Root Cause Analysis

## The Bug

FMSETest serializes XML incorrectly, producing invalid QNames.

### Example

**Original XML:**
```xml
<data key="d0">
  <y:Resources/>
</data>
```

**Serialized (broken):**
```xml
<data key="d0"><y:/></data>
```

**The element local name "Resources" is completely lost!**

## Breaking Commit

**e301dd78b:** "Fix: Per-transaction pin tracking and global BufferManager"

### What Changed

1. **Pin counting:** Changed from global `pinCount` to per-transaction `pinCountByTrx`
   - `incrementPinCount()` → `incrementPinCount(trxId)`  
   - `decrementPinCount()` → `decrementPinCount(trxId)`

2. **Page caching refactor:** Changed from 2 slots to 8 per-index-type slots:
   - Before: `mostRecentlyReadRecordPage`, `secondMostRecentlyReadRecordPage`
   - After: `mostRecentDocumentPage`, `mostRecentNamePage`, `mostRecentPathPage`, etc.

3. **NAME page handling:** NAME pages now cached in separate slot

## Root Cause Hypothesis

The bug appears to be in **NAME page caching**:

1. **NamePage.getRawName()** retrieves name strings from NAME index KeyValueLeafPages
2. **Serializer calls `rawNameForKey(localNameKey)`** to get element names
3. **NAME pages** are now cached in `mostRecentNamePage` slot
4. **Problem:** NAME page data appears corrupted or stale

### Possible Causes

1. **Wrong NAME page being cached** - Multiple NAME pages for different offsets (attributes, elements, namespaces, PIs) might be conflicting in single `mostRecentNamePage` slot

2. **Premature unpinning** - NAME page being unpinned when replaced as mostRecent, then evicted before serialization completes

3. **Cross-transaction corruption** - NAME page from one transaction leaking into another via cache

4. **Fragment pinning issue** - NAME index uses KeyValueLeafPages loaded as fragments, new `incrementPinCount(trxId)` might break fragment handling

## Evidence

- **Commit 5b8eda965:** FMSE passes, uses old pin counting
- **Commit e301dd78b:** FMSE fails, uses per-transaction pin counting
- **Error location:** XML serialization when retrieving element local names
- **Symptom:** rawNameForKey returns empty/corrupt data for valid name keys

## Next Investigation Steps

1. Check if NAME pages are being evicted too early
2. Verify NAME page isn't being shared incorrectly across NAME index offsets (elements vs attributes vs namespaces)
3. Add debug logging to see what rawNameForKey returns for the "Resources" element
4. Check if Names.getRawName() is reading from wrong page/offset

## Impact on VersioningTest Fixes

**VersioningTest fixes are independent and working correctly.**

The FMSE issue is a NAME page caching bug from an earlier commit in this branch. 
Today's VersioningTest fixes don't interact with NAME page handling and all 13 VersioningTests pass.


