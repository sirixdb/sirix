# JSON move last-child follow-up

The bulk replay investigation found a separate source-side move defect: cached last-child links were not maintained. It is recorded here with the original reproduction. The shared move pointer updates are addressed in this review round because replay now uses the same native moves for final placement.

Reproduction with a JSON write transaction:

1. Commit `[[[0]]]` as revision 1 (outer array 1, array 2, nested array 3, number 4).
2. Position on array 2 and call `insertArrayAsLeftSibling()` (new array 5).
3. Call `moveSubtreeToFirstChild(3)` while positioned on array 5.
4. Position on array 5 and call `insertNumberValueAsLastChild(9)` (number 6).
5. Set number 4 to 100, remove array 2, and commit.

Expected revision 2: `[[[100],9]]`. Observed revision 2: `[[9]]`.

This occurred before revision copy ran. Evidence is in `build/review-verification/focused-r5-r8-final.log`, in the original `recomputedReplacementMovesRetainedChildrenBeforeRemovingTheirParent` setup. The replay regression now keeps another child in the old container and inserts the new value relative to the moved node, so it independently establishes valid source state.

`JsonNodeTrxImpl.adaptForMove` updates the former parent's first-child pointer and neighboring sibling pointers, but does not update its last-child pointer when detaching the last child. `processMoveAsFirstChild` does not initialize the destination's last-child pointer when attaching to an empty container. `processMoveAsRightSibling` likewise needs to maintain the destination parent's last-child pointer when attaching at the end.

`JsonBulkInsertDiffRegressionTest.movingOnlyChildSupportsLastChildAppendAndRemovalOfItsOldParent` executes the original reproduction and checks source links after each move and append. `movesMaintainBothEndsOfOrdinaryAndFusedChildChains` covers first-child, right-sibling, and delegated left-sibling moves for ordinary and fused parents with both Dewey configurations. Revision-copy assertions now compare last-child links as well as first-child, parent, sibling, content, and allocation identity. The correction adds constant-size pointer updates; no work-budget bounds were changed.

The named-field reorder fixtures also exposed a same-parent first-child move rebuilding its unchanged path-summary entry. It attempted to reuse the entry after removing its final reference and threw `ClassCastException` on a `DeletedNode`. First-child moves now use the same parent-change guard as right-sibling moves; delegated left-sibling moves share those paths. A move between different parents can also resolve to the same path-summary entry. `PathSummaryWriter.adaptPathForChangedNode` now leaves that shared entry intact when the selected destination is the original entry. The failing evidence is in `build/review-verification/focused-r9-r11-final.log` and `build/review-verification/reproduce-r12-deletes.log`.
