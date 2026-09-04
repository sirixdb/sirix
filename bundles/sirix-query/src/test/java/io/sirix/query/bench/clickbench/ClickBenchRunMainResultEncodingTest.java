/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.clickbench;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ClickBenchRunMainResultEncodingTest {

  @Test
  void finiteDoublesRetainEveryBinary64Bit() {
    final double[] values = {1920.0001, 1920.0008, Math.nextUp(1.0), Double.MIN_VALUE, Double.MAX_VALUE, -0.0};

    for (final double value : values) {
      final JsonElement encoded = ClickBenchRunMain.canonicalCell(new JsonPrimitive(value));
      assertEquals(Double.doubleToRawLongBits(value), Double.doubleToRawLongBits(encoded.getAsDouble()),
          () -> "binary64 value did not round-trip: " + value + " as " + encoded);
    }

    final JsonElement lower = ClickBenchRunMain.canonicalCell(new JsonPrimitive(1920.0001));
    final JsonElement upper = ClickBenchRunMain.canonicalCell(new JsonPrimitive(1920.0008));
    assertNotEquals(lower.toString(), upper.toString());
  }

  @Test
  void integerTokensKeepAllDigits() {
    final JsonPrimitive wide = new JsonPrimitive(435090932899640449L);
    assertEquals(wide, ClickBenchRunMain.canonicalCell(wide));
  }

  @Test
  void markerCannotBlessAFormerRoundedResultDirectory(@TempDir final Path temporaryDirectory) throws Exception {
    final Path legacy = Files.createDirectory(temporaryDirectory.resolve("legacy"));
    Files.writeString(legacy.resolve("q00.jsonl"), "[1920.0]\n");
    assertThrows(IOException.class, () -> ClickBenchRunMain.prepareDumpDirectory(legacy));

    final Path fresh = temporaryDirectory.resolve("fresh");
    ClickBenchRunMain.prepareDumpDirectory(fresh);
    assertEquals(ClickBenchRunMain.RESULT_FORMAT_VERSION + '\n',
        Files.readString(fresh.resolve(ClickBenchRunMain.RESULT_FORMAT_MARKER)));
    ClickBenchRunMain.prepareDumpDirectory(fresh);

    final Path wrongVersion = Files.createDirectory(temporaryDirectory.resolve("wrong-version"));
    Files.writeString(wrongVersion.resolve(ClickBenchRunMain.RESULT_FORMAT_MARKER), "legacy-v1\n");
    assertThrows(IOException.class, () -> ClickBenchRunMain.prepareDumpDirectory(wrongVersion));
  }

  @Test
  void reusedRunInvalidatesOnlySelectedFinalAndPartialResults(@TempDir final Path temporaryDirectory) throws Exception {
    ClickBenchRunMain.prepareDumpDirectory(temporaryDirectory);
    final Path selected = temporaryDirectory.resolve("q00.jsonl");
    final Path selectedPartial = temporaryDirectory.resolve("q00.jsonl.partial");
    final Path unselected = temporaryDirectory.resolve("q01.jsonl");
    Files.writeString(selected, "[\"stale\"]\n");
    Files.writeString(selectedPartial, "[\"partial\"]");
    Files.writeString(unselected, "[\"diagnostic\"]\n");

    ClickBenchRunMain.invalidateSelectedDumps(temporaryDirectory, Set.of(0));

    assertFalse(Files.exists(selected));
    assertFalse(Files.exists(selectedPartial));
    assertTrue(Files.exists(unselected));
  }

  @Test
  void failedDumpCannotPublishStaleOrPartialRows(@TempDir final Path temporaryDirectory) throws Exception {
    ClickBenchRunMain.prepareDumpDirectory(temporaryDirectory);
    final Path result = temporaryDirectory.resolve("q03.jsonl");
    final Path partial = temporaryDirectory.resolve("q03.jsonl.partial");
    Files.writeString(result, "[\"stale\"]\n");

    assertThrows(RuntimeException.class, () -> ClickBenchRunMain.dump(temporaryDirectory, 3, "1 {"));

    assertFalse(Files.exists(result));
    assertFalse(Files.exists(partial));
  }

  @Test
  void completedDumpIsAtomicallyPublished(@TempDir final Path temporaryDirectory) throws Exception {
    ClickBenchRunMain.prepareDumpDirectory(temporaryDirectory);

    assertEquals(2, ClickBenchRunMain.dump(temporaryDirectory, 3, "1 1920.0008"));
    assertEquals("[1]\n[1920.0008]\n", Files.readString(temporaryDirectory.resolve("q03.jsonl")));
    assertFalse(Files.exists(temporaryDirectory.resolve("q03.jsonl.partial")));
  }
}
