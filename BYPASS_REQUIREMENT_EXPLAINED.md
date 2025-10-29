# WHY THE BYPASS IS NEEDED - Diagnostic Comparison

## The Diagnostic Comparison Results

### Test Scenario
Initializing PathSummaryReader at revision 4 in a write transaction

### WITH BYPASS - Works ✅

```
[PATH_SUMMARY-INIT] Initializing pathNodeMapping: revision=4
[PATH_SUMMARY-BYPASS] -> Loaded from disk: pageKey=0, revision=3
```
- Asks for pageKey=0 
- Bypass loads from disk → gets **revision 3**
- Initialization succeeds

### WITHOUT BYPASS - Fails ❌

```
[PATH_SUMMARY-INIT] Initializing pathNodeMapping: revision=4
[PATH_SUMMARY-CACHE-LOOKUP] requestedRevision=3
[PATH_SUMMARY-CACHE-LOOKUP] -> Cache HIT: pageKey=0, revision=2
AssertionError: assert value == null || !value.isClosed()
```
- Asks for pageKey=0, revision 3
- Cache returns **revision 2** 
- Page is CLOSED
- Assertion fails!

## The Root Cause: TWO Problems

### Problem 1: Cache Returns Wrong Revision

**Fact from diagnostics:**
- pageKey=0 was only modified at revisions 0, 1, 2
- pageKey=0 was NOT modified at revision 3
- All revisions use same PageReference: `PageRef(key=79312, logKey=-15)`

**What happens:**
1. Revision 2: Load pageKey=0 → cache with PageRef(79312)
2. Revision 3: Request pageKey=0 → PageRef(79312) → cache returns revision 2
3. **But we ASKED for revision 3!**

**Why this matters:**
Even though pageKey=0 wasn't modified, the bypass loads "revision 3" from disk. This suggests `combinePageFragments` is doing something more than just returning the last modified version.

### Problem 2: Closed Pages Stay in Cache

**Our fix attempts to solve this:**
```java
if (resourceBufferManager.getRecordPageCache().get(pageReference) != null) {
  // Page is in cache: just unpin it
  page.decrementPinCount(trxId);
} else {
  // Not in cache: close it
  page.close();
}
```

**The bug:** We unpin but DON'T remove from cache. Later:
1. Cache still has the page (pinCount=0)
2. Something closes the page elsewhere
3. Next read gets CLOSED page from cache → assertion fails!

## The Question You Asked

**"How can cache return wrong revisions?"**

The cache uses PageReference as the key. PageReference equality:
```java
public boolean equals(Object other) {
  return otherPageRef.logKey == logKey && otherPageRef.key == key;
}
```

**All revisions of pageKey=0 have:**
- Same `key` = 79312 (persistent storage offset)
- Same `logKey` = -15 (not set for read transactions)

**Therefore:**
- Revision 2 page cached with PageRef(79312, -15)
- Revision 3 requests with PageRef(79312, -15)
- Cache returns revision 2 (same key!)

## The Real Answer

The bypass is needed because:

1. **Multi-revision views:** `combinePageFragments` creates revision-specific views
2. **Shared PageReferences:** All revisions share the same PageReference key
3. **Cache collision:** Cache can't differentiate between revisions
4. **Wrong data:** Cache returns old revision when new revision requested

**Your insight is correct** - IF the data is identical, returning an old revision should be fine.

**BUT:** The diagnostic shows the bypass loads "revision 3" while cache returns "revision 2", suggesting the data IS different (combined from multiple fragments perhaps).

## Next Step

We need to understand: **Does combinePageFragments create different data for revision 3 vs revision 2 of pageKey=0, even though pageKey=0 wasn't modified at revision 3?**

If YES: Cache is wrong, bypass is needed
If NO: Something else is wrong with our logic

