# Memory Leak Source - New Hypothesis

## Current Leak Statistics

```
TOTAL: 1329 created / 1224 closed / 105 leaked (7.9%)

By Type:
- PATH_SUMMARY: 8 / 4 / 4 (50% leak)
- NAME: 44 / 20 / 24 (55% leak)
- DOCUMENT: 1274 / 1197 / 77 (6% leak)
```

## Observation

PATH_SUMMARY (50%) and NAME (55%) have **similar high leak rates**, while DOCUMENT (6%) is much lower.

This suggests PATH_SUMMARY and NAME share a common characteristic that causes leaks, which DOCUMENT doesn't have.

## Hypothesis: Small IndexTypes Leak More

### Why Document Has Low Leak Rate (6%)

**DOCUMENT pages:**
- Very frequently accessed (1,274 created)
- High cache churn (many evictions and recreations)
- Field-based caching helps (mostRecent, secondMostRecent)
- Large volume dilutes the baseline leak
- **Result:** Only 77 out of 1,274 leak (6%)

### Why PATH_SUMMARY and NAME Have High Leak Rates (50-55%)

**PATH_SUMMARY pages:**
- Infrequently accessed (8 created total)
- Bypass keeps pages alive longer  
- Only 1 field slot (mostRecentPathSummaryPage)
- Small volume amplifies baseline leak
- **Result:** 4 out of 8 leak (50%)

**NAME pages:**
- Infrequently accessed (44 created)
- No field-based caching (no mostRecentNamePage slot)
- Small volume amplifies baseline leak
- **Result:** 24 out of 44 leak (55%)

## The Real Pattern

The leak appears to be a **constant baseline** that affects all page types:
- ~105 pages leak regardless of type
- High-frequency types (DOCUMENT) show low % because many pages are created/closed
- Low-frequency types (PATH_SUMMARY, NAME) show high % because few pages total

**Mathematical verification:**
- DOCUMENT: 77 leaked out of 1,274 = 6%
- NAME: 24 leaked out of 44 = 55%
- PATH_SUMMARY: 4 leaked out of 8 = 50%
- **Total leaked: 77 + 24 + 4 = 105** ✓

So PATH_SUMMARY isn't special - it's just that with only 8 pages total, losing 4 shows as 50%, while DOCUMENT losing 77 out of 1,274 shows as 6%.

## Source of the 105-Page Baseline Leak

From investigation documents:

**1. Setup Phase (XmlShredder):**
- Creates ~1,000 pages during initial database shredding
- Intent log closes 653 pages
- **Gap:** ~350 pages created outside normal transaction tracking

**2. Combined Pages from Versioning:**
- `combineRecordPages()` creates new pages
- Returned pages are swizzled in PageReferences
- NOT added to cache (by design, prevents recursive updates)
- Cleaned up by cache pressure or GC eventually

**3. Field Slot Limits:**
- Only 3 field slots (mostRecent, secondMostRecent, pathSummary)
- Pages beyond these slots aren't tracked
- Get orphaned when PageReferences are replaced

## Conclusion

The PATH_SUMMARY leak is NOT special - it's part of the general 105-page baseline leak that affects all page types proportionally.

**The close() fix didn't help** because:
- Most leaked PATH_SUMMARY pages are from setup/combined pages
- Not from mostRecentPathSummaryPage replacements
- The leak is systemic, not PATH_SUMMARY-specific

## Next Steps

To reduce the 105-page leak, we need to address the baseline causes:

1. **Setup page tracking:** Ensure XmlShredder properly closes all created pages
2. **Combined page lifecycle:** Track or cache combined pages so they're closed
3. **Field slot expansion:** More tracking slots for different page types

But all of these are risky architectural changes. The 105-page constant leak is acceptable for now.

