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
  void startsLargeAsyncFlushTransactionsWithAColdEpoch() {
    final int cap = AfterCommitState.MAX_ASYNC_FLUSH_PRIMING_NODE_COUNT;

    assertEquals(0, initialThreshold(0, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(1, initialThreshold(1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, initialThreshold(cap, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, initialThreshold(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
  }

  @Test
  void capsEveryLargeSteadyAsyncFlushEpoch() {
    final int cap = AfterCommitState.MAX_ASYNC_FLUSH_NODE_COUNT;

    assertEquals(0, steadyThreshold(0, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(1, steadyThreshold(1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap - 1, steadyThreshold(cap - 1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, steadyThreshold(cap, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, steadyThreshold(cap + 1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, steadyThreshold(4 * cap, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertEquals(cap, steadyThreshold(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
  }

  @ParameterizedTest
  @EnumSource(value = AfterCommitState.class, mode = EnumSource.Mode.EXCLUDE,
      names = "KEEP_OPEN_ASYNC_FLUSH")
  void preservesConfiguredThresholdForEveryOtherCommitMode(final AfterCommitState afterCommitState) {
    assertEquals(Integer.MAX_VALUE, initialThreshold(Integer.MAX_VALUE, afterCommitState));
    assertEquals(Integer.MAX_VALUE, steadyThreshold(Integer.MAX_VALUE, afterCommitState));
  }

  @Test
  void rejectsInvalidInputs() {
    assertThrows(IllegalArgumentException.class,
        () -> initialThreshold(-1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertThrows(NullPointerException.class,
        () -> initialThreshold(1, null));
    assertThrows(IllegalArgumentException.class,
        () -> steadyThreshold(-1, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH));
    assertThrows(NullPointerException.class,
        () -> steadyThreshold(1, null));
  }

  private static int initialThreshold(final int maxNodeCount, final AfterCommitState afterCommitState) {
    return AbstractNodeTrxImpl.initialAutoCommitNodeCountThreshold(maxNodeCount, afterCommitState);
  }

  private static int steadyThreshold(final int maxNodeCount, final AfterCommitState afterCommitState) {
    return AbstractNodeTrxImpl.steadyAutoCommitNodeCountThreshold(maxNodeCount, afterCommitState);
  }
}
