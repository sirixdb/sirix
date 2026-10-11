package io.sirix.query.function.jn.temporal;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ValidTimeRangeKeysTest {
  @Test
  void collectionAndStrictTieRemovalMatchAnIndependentOrderedSet() {
    final int maximum = 100003;
    final ValidTimeRangeKeys keys = new ValidTimeRangeKeys(maximum);
    final TreeSet<Long> expected = new TreeSet<>();
    final Random random = new Random(964261);
    for (int operation = 0; operation < 20000; operation++) {
      final long key = operation % 4 == 0
          ? maximum
          : random.nextInt(maximum + 1);
      if (operation % 3 == 0) {
        keys.remove(key);
        expected.remove(key);
      } else {
        keys.accept(key);
        expected.add(key);
      }
    }
    keys.accept(0);
    expected.add(0L);
    assertArrayEquals(expected.stream().mapToLong(Long::longValue).toArray(), keys.sortedKeys());
  }

  @Test
  void invalidDomainsAndReferencesFailBeforeAliasingAnotherKey() {
    assertThrows(IllegalArgumentException.class, () -> new ValidTimeRangeKeys(-1));
    assertThrows(IllegalArgumentException.class, () -> new ValidTimeRangeKeys(ValidTimeRangeKeys.MAXIMUM_KEY + 1));
    final ValidTimeRangeKeys keys = new ValidTimeRangeKeys(64);
    assertThrows(IllegalStateException.class, () -> keys.accept(-1));
    assertThrows(IllegalStateException.class, () -> keys.accept(65));
    assertThrows(IllegalStateException.class, () -> keys.accept(1L << 48));
    assertThrows(IllegalStateException.class, () -> keys.remove(65));
    assertArrayEquals(new long[0], keys.sortedKeys());
  }
}
