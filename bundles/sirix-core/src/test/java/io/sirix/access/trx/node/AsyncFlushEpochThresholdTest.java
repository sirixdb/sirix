/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class AsyncFlushEpochThresholdTest {

  @Test
  void capsEveryLargeAsyncFlushEpoch() {
    final int cap = AfterCommitState.MAX_ASYNC_FLUSH_NODE_COUNT;

    // maxNodeCount == 0 — the beginNodeTrx(AfterCommitState) overload — asks for no COMMIT
    // cadence, but the async-flush epoch bounds intent-log MEMORY and stays armed at the full
    // epoch size. (It used to resolve to 0, which shouldRotateIntermediateEpoch dead-gated
    // behind maxNodeCount > 0: the log then grew unbounded for the whole import.)
    assertEquals(cap, threshold(0, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(1, threshold(1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap - 1, threshold(cap - 1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, threshold(cap, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, threshold(cap + 1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, threshold(4 * cap, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, threshold(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
  }

  @ParameterizedTest
  @EnumSource(value = AfterCommitState.class, mode = EnumSource.Mode.EXCLUDE, names = "KEEP_OPEN_ASYNC_FLUSH")
  void preservesConfiguredThresholdForEveryOtherCommitMode(final AfterCommitState afterCommitState) {
    assertEquals(Integer.MAX_VALUE, threshold(Integer.MAX_VALUE, afterCommitState));
  }

  @Test
  void rejectsInvalidInputs() {
    assertThrows(IllegalArgumentException.class, () -> threshold(-1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertThrows(NullPointerException.class, () -> threshold(1, null));
  }

  private static int threshold(final int maxNodeCount, final AfterCommitState afterCommitState) {
    return AbstractNodeTrxImpl.autoCommitNodeCountThreshold(maxNodeCount, afterCommitState);
  }
}
