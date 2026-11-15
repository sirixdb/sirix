package io.sirix.query;

import io.sirix.query.compiler.translator.SirixTranslator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class SirixTranslatorThresholdTest {
  @Test
  void startupThresholdsAreIndependent() {
    final String mode = System.getProperty("sirix.test.threshold.case", "defaults");
    final int expectedChild = mode.equals("defaults")
        ? 100_000
        : 17;
    final int expectedDescendant = mode.equals("independent")
        ? 31
        : -1;
    assertEquals(expectedChild, SirixTranslator.CHILD_THRESHOLD);
    assertEquals(expectedDescendant, SirixTranslator.DESCENDANT_THRESHOLD);
  }
}
