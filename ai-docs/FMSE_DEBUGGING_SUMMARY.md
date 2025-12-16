# FMSE Test Failure - Debugging Summary

## Issue
FMSE test fails with: `Element or attribute "y:" do not match QName production`
- Element local names return `null`
- NameKey `20897285` consistently returns `null` for ELEMENT nodes
- Produces invalid QNames like `y:` instead of `y:Resources`

## Root Cause
Records from older revisions have nameKeys that existed in their original revision's NamePage, but the current revision's NamePage doesn't contain those entries (likely because elements were deleted in later revisions).

## What We've Tried (All Failed)

### 1. Disabled Field-Based Caching for NAME Pages ❌
- Made NAME pages skip the mostRecent/secondMostRecent cache slots
- Result: Issue persists

### 2. Applied PATH_SUMMARY-Style Special Handling to NAME Pages ❌
- NAME pages always bypass buffer manager
- Always load directly with fragment combining  
- Result: Issue persists

### 3. Bypassed NamesCache ❌
- Disabled the `Cache<NamesCacheKey, Names> namesCache` 
- Always call `Names.fromStorage()` directly
- Result: Issue persists

### 4. Removed All Field-Based Caching Except DOCUMENT/PATH_SUMMARY ❌
- Removed mostRecent slots for NAME, PATH, CAS, CHANGED_NODES, etc.
- Only DOCUMENT and PATH_SUMMARY keep field-based caching
- Result: Issue persists

## Key Finding
**The issue is NOT caching-related.** Even with all caches bypassed, name lookups still return `null`.

## The Real Problem
When `getName()` is called, it goes through:
1. `NodePageReadOnlyTrx.namePage` (set once at transaction creation to current revision)
2. `NamePage.getName()` → `getNames()` → `Names.fromStorage()`
3. `Names.fromStorage()` loops from `1` to `maxNodeKey` from the **current** NamePage
4. Records from older revisions may have nameKeys >= current maxNodeKey or pointing to deleted/modified entries

## Between Working and Broken Commits
Commit `5b8eda965` (working) → `e301dd78b` (broken):
- Changed from global `incrementPinCount()` to per-transaction `incrementPinCount(trxId)`
- Simplified from 2-slot caching (mostRecent + secondMostRecent) to 1-slot per index type
- This is a red herring - the caching changes don't cause the NAME lookup issue

## Next Steps Needed
The solution requires ensuring that when records from older revisions are accessed, their name lookups use the NamePage from their **original revision**, not the current one. This is a fundamental architecture issue, not a caching bug.

Possible approaches:
1. **Pass record revision to getName()**: Modify API to accept revision number and load that revision's NamePage
2. **Never delete name entries**: Keep all name entries forever (mark inactive but don't remove)
3. **Investigate what changed before commit 5b8eda965**: The bug may have been introduced even earlier

