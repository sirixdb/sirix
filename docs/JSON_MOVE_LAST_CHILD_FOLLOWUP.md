# JSON move last-child follow-up

The child-chain invariant belongs to `JsonNodeTrxImpl.adaptForMove` and its destination helpers
in [JsonNodeTrxImpl.java](../bundles/sirix-core/src/main/java/io/sirix/access/trx/node/json/JsonNodeTrxImpl.java).
Regression coverage is in `movingOnlyChildSupportsLastChildAppendAndRemovalOfItsOldParent`
and `movesMaintainBothEndsOfOrdinaryAndFusedChildChains` in
[JsonBulkInsertDiffRegressionTest.java](../bundles/sirix-core/src/test/java/io/sirix/diff/JsonBulkInsertDiffRegressionTest.java).
Path-summary reuse is guarded at `PathSummaryWriter.adaptPathForChangedNode` in
[PathSummaryWriter.java](../bundles/sirix-core/src/main/java/io/sirix/index/path/summary/PathSummaryWriter.java).
