package io.sirix.query.function.jn.temporal;

import java.util.function.LongConsumer;

/** Exact sorted key collection for a bounded, compact immutable storage-key domain. */
final class ValidTimeRangeKeys implements LongConsumer {
  static final int MAXIMUM_KEY = (1 << 24) - 1;
  private final int maximum;
  private final long[] words;

  ValidTimeRangeKeys(final int maximum) {
    if (maximum < 0 || maximum > MAXIMUM_KEY) {
      throw new IllegalArgumentException("Dense storage-key domain out of range: " + maximum);
    }
    this.maximum = maximum;
    words = new long[(maximum >>> 6) + 1];
  }

  @Override
  public void accept(final long key) {
    check(key);
    words[(int) (key >>> 6)] |= 1L << key;
  }

  void remove(final long key) {
    check(key);
    words[(int) (key >>> 6)] &= ~(1L << key);
  }

  private void check(final long key) {
    if (key < 0 || key > maximum) {
      throw new IllegalStateException("Interval reference exceeds the revision's storage-key domain: " + key);
    }
  }

  long[] sortedKeys() {
    int count = 0;
    for (final long word : words) {
      count += Long.bitCount(word);
    }
    final long[] keys = new long[count];
    int output = 0;
    for (int index = 0; index < words.length; index++) {
      long word = words[index];
      final long base = (long) index << 6;
      while (word != 0) {
        keys[output++] = base + Long.numberOfTrailingZeros(word);
        word &= word - 1;
      }
    }
    return keys;
  }
}
