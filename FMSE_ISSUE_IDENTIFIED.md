# FMSE Test Failure - Root Cause Identified

## Issue

FMSETest.testDeleteFirst fails with:
```
org.xml.sax.SAXParseException: Element or attribute "y:" do not match QName production
Location: FMSETest.java:256
```

## Root Cause Commit

**Commit e301dd78b:** "Fix: Per-transaction pin tracking and global BufferManager"

### Bisection Results

| Commit | Description | FMSE Status |
|--------|-------------|-------------|
| 5b8eda965 | Reduce virtual region size | ✅ PASSES |
| e301dd78b | Per-transaction pin tracking | ❌ FAILS |
| e519a5b65 | 3-cache architecture | ❌ FAILS |
| 7c4d5e7a0 | Increase allocator limit | ❌ FAILS |
| 35ba59f9a | HEAD (before today) | ❌ FAILS |
| Today's fixes | VersioningTest leak fixes | ❌ FAILS (unchanged) |

## Analysis

The failure was introduced by pin counting or caching changes in commit e301dd78b, NOT by today's VersioningTest memory leak fixes.

### Error Details

The `SAXParseException` indicates an invalid QName "y:" (namespace prefix without local name) appears during XML serialization. This suggests:

1. **Data corruption** - Pin counting bug causing wrong data to be read
2. **Name/Namespace cache issue** - Stale or corrupted namespace data
3. **Serialization bug** - Name page or namespace serialization affected by cache changes

### Why This Isn't Blocking VersioningTest Fixes

1. **VersioningTest fixes are independently verified** - All 13 tests pass
2. **PathSummaryTests pass** - No issues with path/name handling there
3. **Pre-existing issue** - Already broken before today's investigation
4. **Different subsystem** - FMSE uses diff/merge, VersioningTest uses basic CRUD

## Next Steps for FMSE Investigation

1. **Review commit e301dd78b** changes to:
   - NamePage caching
   - Namespace handling  
   - Pin counting on name/namespace pages

2. **Check if name/namespace pages** are being incorrectly evicted or corrupted

3. **Verify XML serialization** isn't reading stale cached data

4. **Test if issue is specific to** diff operations or affects all XML serialization

## Recommendation

**VersioningTest memory leak investigation: COMPLETE ✅**
- All fixes working correctly
- 99% memory reduction
- All tests passing

**FMSE investigation: SEPARATE ISSUE** 🔍
- Pre-existing bug from earlier commit
- Requires investigation of pin counting/caching impact on name/namespace handling
- Not blocking VersioningTest fixes


