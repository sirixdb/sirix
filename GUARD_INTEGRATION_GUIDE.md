# PageGuard Integration Guide

## Status: Infrastructure Complete, Integration Pending

All guard infrastructure is in place:
- ✅ `PageGuard` class
- ✅ `FrameReusedException`
- ✅ Guard count on `PageReference`
- ✅ Version counter on `KeyValueLeafPage`
- ✅ `currentPageGuard` field in `NodePageReadOnlyTrx`
- ✅ `closeCurrentPageGuard()` helper method

## Remaining Work: Integrate Guards into Page Access

### Critical Insight from Discussion

**Key principle**: Guards stay open only while cursor is on the SAME PAGE.
- Multiple `moveTo()` calls on nodes from same page → reuse guard
- `moveTo()` to node on different page → close old guard, acquire new guard

### Integration Points in NodePageReadOnlyTrx

#### 1. Main Page Fetching Method: `getRecordPage()`

**Current**: Returns page without guard
```java
private PageReferenceToPage getRecordPage(IndexLogKey indexLogKey) {
  // Loads page from cache or disk
  return new PageReferenceToPage(ref, page);
}
```

**Needed**: Acquire guard when loading a new page
```java
private PageReferenceToPage getRecordPage(IndexLogKey indexLogKey) {
  // Load page...
  
  // Check if same page as current guard
  if (currentPageGuard != null && currentPageGuard.page() == page) {
    // Reuse existing guard
    return new PageReferenceToPage(ref, page);
  }
  
  // Different page - close old guard, acquire new
  closeCurrentPageGuard();
  currentPageGuard = new PageGuard(ref, page);
  return new PageReferenceToPage(ref, page);
}
```

#### 2. Fragment Loading: `loadDataPageFragments()`

**Current**: Loads fragments without guards  
**Needed**: Guard each fragment temporarily, close after combining

```java
private List<KeyValuePage<DataRecord>> loadDataPageFragments(...) {
  List<Page> fragments = new ArrayList<>();
  
  for (PageFragmentKey fragmentKey : ...) {
    try (PageGuard guard = new PageGuard(fragmentRef, fragment)) {
      fragments.add(guard.page());
    } // Guard auto-released after adding to list
  }
  
  return fragments;
}
```

#### 3. Most Recent Page Management

**Current**: Stores pages in `mostRecent*` fields
**Issue**: These pages might need guards if they're accessed later

**Options**:
a) Don't cache in `mostRecent*` fields - always reload and guard
b) Store guards in `mostRecent*` fields (complex lifecycle)
c) Only use `mostRecent*` for page identity check, always reacquire guard

**Recommendation**: Option C - simplest and safest

#### 4. Close Method

**Current**: Already calls `closeCurrentPageGuard()`
**Status**: ✅ Done

### Testing Strategy

1. **Start simple**: Add guards to one code path (e.g., document page reads)
2. **Test**: Run basic read tests, verify no guard leaks
3. **Expand**: Add to fragment loading, other page types
4. **Stress test**: Concurrent reads, long scans

### Pragmatic Phased Approach

**Phase 1** (Minimum Viable):
- Add guard acquisition in `getRecordPage()` for DOCUMENT pages only
- Keep old behavior for other page types
- Test basic functionality

**Phase 2** (Full Integration):
- Extend to all page types (PATH_SUMMARY, NAME, etc.)
- Handle fragment guards properly
- Remove all TODO comments

**Phase 3** (Optimization):
- Profile guard acquisition overhead
- Optimize hot paths
- Consider caching strategies

### Known Challenges

1. **Fragment Chains**: Fragments need temporary guards during combining
2. **PATH_SUMMARY Bypass**: Special case for write transactions
3. **Most Recent Caching**: Complex interaction with guard lifecycle
4. **Exception Safety**: Ensure guards close on all error paths

### Success Criteria

- ✅ No manual pin/unpin calls (already done)
- ✅ Guards auto-close via try-with-resources
- ✅ No guard leaks (verified by diagnostic tools)
- ✅ Performance acceptable (< 5% overhead vs pinning)

## Next Steps

1. Implement guard acquisition in `getRecordPage()` (start with DOCUMENT type)
2. Test with basic integration tests
3. Expand to other page types
4. Add guard leak detection to tests
5. Performance validation

## See Also

- `REFACTOR_STATUS.md` - Overall progress
- `PageGuard.java` - Guard implementation
- `RevisionEpochTracker.java` - Epoch tracker for eviction
- `ClockSweeper.java` - Eviction algorithm

