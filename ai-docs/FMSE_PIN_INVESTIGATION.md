# FMSE Test - Pin Tracking Investigation

## Verified Facts
1. ✅ Test PASSES on commit `5b8eda965` (working)
2. ❌ Test FAILS on commit `e301dd78b` (broken - has compilation errors, but likely fails if fixed)
3. ❌ Test FAILS on current HEAD with all attempted fixes

## Key Changes Between Working and Broken Commits
1. **Caching structure**: 2-slot generic → 1-slot per-type
2. **Pin tracking**: Global `incrementPinCount()` → Per-transaction `incrementPinCount(trxId)`

## What We've Tried (All Failed)
1. ❌ Reverted to 2-slot generic caching
2. ❌ Disabled field-based caching for NAME pages
3. ❌ Bypassed RecordPageCache for NAME/PATH_SUMMARY
4. ❌ Bypassed NamesCache in NamePage.java
5. ❌ Applied PATH_SUMMARY-style handling to NAME pages
6. ❌ Restricted caching to only DOCUMENT/PATH_SUMMARY
7. ✅ **FIXED**: Double-pinning bug in `getPageFragments()` (was pinning first fragment twice)
   - But test still fails after this fix

## Double-Pinning Bug (Fixed but didn't solve FMSE)
**Location**: `NodePageReadOnlyTrx.getPageFragments(PageReference)`

**Problem**:
```java
// Line 699: Pinned the first page
firstPage.incrementPinCount(trxId);
// Line 703: Called second overload which pinned it AGAIN
return getPageFragments(pageReference, indexType); // This pins all fragments including first
```

**Fix**: Removed the pin at line 699, letting the second overload handle all pinning.

**Result**: Test still fails, so this wasn't the root cause (but was still a real bug).

## Root Cause Hypothesis
The per-transaction pin tracking introduced in commit `e301dd78b` may have a subtle bug related to:
- Page eviction timing
- Cache key collisions
- Transaction lifecycle mismatches
- Fragment combining with per-transaction pins

The fact that even reverting the caching structure didn't help suggests the issue is specifically with the **per-transaction pin tracking mechanism** itself, not the caching structure.

## Next Steps
1. Add detailed logging for NAME page fragment loading and pinning
2. Compare pin count behavior between working and broken commits
3. Check if pages are being evicted prematurely due to per-transaction pin accounting

