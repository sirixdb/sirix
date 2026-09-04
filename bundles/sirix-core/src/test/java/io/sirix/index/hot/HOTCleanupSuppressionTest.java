/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@DisplayName("HOT fresh-page cleanup suppression")
final class HOTCleanupSuppressionTest {

  @Test
  @DisplayName("bulk-build cleanup preserves an OOME primary and permits later sibling cleanup")
  void bulkBuildCleanupPreservesPrimaryAndContinues() {
    assertSuppressionFailureIsContained(HOTBulkBuilder::addSuppressedSafely);
  }

  @Test
  @DisplayName("incremental cleanup preserves an OOME primary and permits later sibling cleanup")
  void incrementalCleanupPreservesPrimaryAndContinues() {
    assertSuppressionFailureIsContained(HOTIncrementalInsert::addSuppressedSafely);
  }

  private static void assertSuppressionFailureIsContained(final BiConsumer<Throwable, Throwable> suppressor) {
    final OutOfMemoryError primary = new OutOfMemoryError("primary allocation failure");

    // Throwable.addSuppressed(null) throws. The cleanup helper must absorb that secondary failure so
    // the caller's sibling loop can keep running and retain the exact original throwable.
    assertDoesNotThrow(() -> suppressor.accept(primary, null));

    final AssertionError laterSiblingFailure = new AssertionError("later sibling cleanup failure");
    assertDoesNotThrow(() -> suppressor.accept(primary, laterSiblingFailure));
    assertDoesNotThrow(() -> suppressor.accept(primary, primary));

    assertEquals(1, primary.getSuppressed().length);
    assertSame(laterSiblingFailure, primary.getSuppressed()[0]);
  }
}
