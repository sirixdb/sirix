/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class AsyncFlushPrimingThresholdTest {

  @Test
  void capsOnlyLargeAsyncFlushEpochs() {
    final int cap = AbstractNodeTrxImpl.MAX_ASYNC_FLUSH_PRIMING_NODE_COUNT;

    assertEquals(0, initialThreshold(0, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(1, initialThreshold(1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap - 1, initialThreshold(cap - 1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, initialThreshold(cap, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, initialThreshold(cap + 1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, initialThreshold(4 * cap, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, initialThreshold(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
  }

  @ParameterizedTest
  @EnumSource(value = AfterCommitState.class, mode = EnumSource.Mode.EXCLUDE,
      names = "KEEP_OPEN_ASYNC_FLUSH")
  void preservesConfiguredThresholdForEveryOtherCommitMode(final AfterCommitState afterCommitState) {
    assertEquals(Integer.MAX_VALUE, initialThreshold(Integer.MAX_VALUE, afterCommitState));
  }

  @Test
  void rejectsInvalidInputs() {
    assertThrows(IllegalArgumentException.class,
        () -> initialThreshold(-1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertThrows(NullPointerException.class,
        () -> initialThreshold(1, null));
  }

  private static int initialThreshold(final int maxNodeCount, final AfterCommitState afterCommitState) {
    return AbstractNodeTrxImpl.initialAutoCommitNodeCountThreshold(maxNodeCount, afterCommitState);
  }
}
