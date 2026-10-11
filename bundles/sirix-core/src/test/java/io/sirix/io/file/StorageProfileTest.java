/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.file;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.sirix.index.IndexType;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PathPage;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

final class StorageProfileTest {

  @Test
  void hotLeafLabelsDistinguishTemporalAndProjectionBytes() {
    for (final IndexType type : new IndexType[] {IndexType.CAS, IndexType.VALIDTIME, IndexType.PROJECTION}) {
      try (HOTLeafPage leaf = new HOTLeafPage(1, 1, type)) {
        assertEquals("HOTLeafPage:" + type.name(), StorageProfile.pageKind(leaf));
      }
    }
    assertEquals("PathPage", StorageProfile.pageKind(new PathPage()));
  }

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
