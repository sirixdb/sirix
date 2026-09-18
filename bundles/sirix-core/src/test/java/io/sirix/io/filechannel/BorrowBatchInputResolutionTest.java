/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.filechannel;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Batch-input borrowing follows the overflow-input switch unless it is set itself: an explicit
 * {@code sirix.filechannel.borrowBatchInput} always wins, an unset one inherits
 * {@code sirix.io.borrowOverflowInput}, and with neither set input is borrowed.
 */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
final class BorrowBatchInputResolutionTest {

  @ParameterizedTest(name = "batch={0}, overflow={1} -> {2}")
  @CsvSource(nullValues = "unset", value = {"unset, unset, true", "unset, true, true", "unset, false, false",
      "unset, '', true", "true, unset, true", "true, false, true", "'', false, true", "false, unset, false",
      "false, true, false", "false, false, false"})
  void anExplicitBatchSwitchWinsAndAnUnsetOneFollowsTheOverflowSwitch(final @Nullable String batchInput,
      final @Nullable String overflowInput, final boolean expected) {
    final String previousBatch = System.getProperty(FileChannelReader.BORROW_BATCH_INPUT);
    final String previousOverflow = System.getProperty(FileChannelReader.BORROW_OVERFLOW_INPUT);
    try {
      set(FileChannelReader.BORROW_BATCH_INPUT, batchInput);
      set(FileChannelReader.BORROW_OVERFLOW_INPUT, overflowInput);
      assertEquals(expected, FileChannelReader.batchInputBorrowingEnabled());
    } finally {
      set(FileChannelReader.BORROW_BATCH_INPUT, previousBatch);
      set(FileChannelReader.BORROW_OVERFLOW_INPUT, previousOverflow);
    }
  }

  private static void set(final String property, final @Nullable String value) {
    if (value == null) {
      System.clearProperty(property);
    } else {
      System.setProperty(property, value);
    }
  }
}
