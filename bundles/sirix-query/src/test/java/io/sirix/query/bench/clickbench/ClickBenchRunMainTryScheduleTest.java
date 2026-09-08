package io.sirix.query.bench.clickbench;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickBenchRunMainTryScheduleTest {
  @TempDir
  Path directory;

  @Test
  void everyQueryGetsTheSameTryCount() {
    final double[][] timings = ClickBenchRunMain.initializeTimings(3);
    assertEquals(43, timings.length);
    assertEquals(129, Arrays.stream(timings).mapToInt(row -> row.length).sum());
    assertTrue(Arrays.stream(timings).allMatch(row -> row.length == 3));
    assertTrue(Arrays.stream(timings).flatMapToDouble(Arrays::stream).allMatch(Double::isNaN));
  }

  @Test
  void anEmptyScheduleIsRejected() {
    for (final int tries : new int[] {0, -1}) {
      assertThrows(IllegalArgumentException.class, () -> ClickBenchRunMain.initializeTimings(tries));
    }
  }

  @Test
  void theResultJsonCarriesEveryAttemptAndClaimsNoRigProvenance() throws IOException {
    final double[][] timings = ClickBenchRunMain.initializeTimings(3);
    for (final double[] row : timings) {
      for (int attempt = 0; attempt < row.length; attempt++) {
        row[attempt] = attempt + 1;
      }
    }
    final Path output = directory.resolve("result.json");
    ClickBenchRunMain.writeResultsJson(output, timings, -1, 100, null);
    final JsonObject result = JsonParser.parseString(Files.readString(output)).getAsJsonObject();
    assertEquals(3, result.getAsJsonArray("result").get(2).getAsJsonArray().size());
    assertEquals(2., result.getAsJsonArray("result").get(2).getAsJsonArray().get(1).getAsDouble());
    // rank.py ranks only a leg whose rig.scope states the publication regime, so a bare harness
    // result must not carry one: absent provenance refuses rather than passes.
    assertFalse(result.has("rig"));
  }
}
