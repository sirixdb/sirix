package io.sirix.utils;

import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static io.sirix.utils.StringComparisonOracle.compareStrings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class StringComparisonsTest {
  @Test
  void supplementaryCharactersFollowBmpAndMatchTheInterpreter() {
    assertTrue(StringComparisons.compareCodePoints("！", "𐐀") < 0);
    final String[] values = {"", "a", "ab", "！", "𐐀", "𐐀a", "\uD800", "\uDC00", "\uFFFF", "😀"};
    for (final String left : values) {
      for (final String right : values) {
        assertEquals(Integer.signum(compareStrings(left, right)),
            Integer.signum(StringComparisons.compareCodePoints(left, right)));
      }
    }
  }

  @Test
  void sharedPrefixesAndAllUnicodePlanesMatchTheInterpreter() {
    final SplittableRandom random = new SplittableRandom(0xC011A710);
    for (int iteration = 0; iteration < 2_000; iteration++) {
      final String prefix = value(random);
      final String left = prefix + value(random);
      final String right = iteration % 7 == 0
          ? left
          : prefix + value(random);
      assertEquals(Integer.signum(compareStrings(left, right)),
          Integer.signum(StringComparisons.compareCodePoints(left, right)));
    }
  }

  @Test
  void nullOperandsAreRejected() {
    assertThrows(NullPointerException.class, () -> StringComparisons.compareCodePoints(null, ""));
    assertThrows(NullPointerException.class, () -> StringComparisons.compareCodePoints("", null));
  }

  private static String value(final SplittableRandom random) {
    final StringBuilder value = new StringBuilder();
    final int length = random.nextInt(24);
    for (int i = 0; i < length; i++) {
      value.appendCodePoint(random.nextInt(Character.MAX_CODE_POINT + 1));
    }
    return value.toString();
  }
}
