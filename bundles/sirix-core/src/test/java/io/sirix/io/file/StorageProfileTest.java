/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.file;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

final class StorageProfileTest {

  @Test
  void unknownRawWritesSuppressTheOverallRatioButPreserveTheKnownSubset() {
    StorageProfile.record("StorageProfileCounterTest", 200, 100);
    StorageProfile.recordUnknownRaw("StorageProfileCounterTest", 60);

    final PrintStream originalOut = System.out;
    final ByteArrayOutputStream captured = new ByteArrayOutputStream();
    try (PrintStream replacement = new PrintStream(captured, true, StandardCharsets.UTF_8)) {
      System.setOut(replacement);
      StorageProfile.dump();
    } finally {
      System.setOut(originalOut);
    }

    final String report = captured.toString(StandardCharsets.UTF_8);
    assertTrue(report.contains("StorageProfileCounterTest"));
    assertTrue(report.contains("Overall compression ratio: unavailable"), report);
    assertTrue(report.contains("Known-subset compression ratio: 0.500"), report);
  }
}
