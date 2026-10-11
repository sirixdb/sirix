package io.sirix.query.function.jn.temporal;

import java.util.Arrays;

/** Bounded radix sorting of nonnegative 48-bit storage keys, skipping constant bytes. */
final class ValidTimeKeySort {
  private static final int MIN_RADIX_KEYS = 4096;
  private static final int MAX_RADIX_KEYS =
      (int) Math.max(MIN_RADIX_KEYS, Math.min(1L << 22, Runtime.getRuntime().maxMemory() / 1024));

  private ValidTimeKeySort() {}

  @SuppressWarnings("ReferenceEquality") // Buffer identity determines whether sorted keys need copying back.
  static void sort(final long[] keys) {
    if (keys.length < MIN_RADIX_KEYS || keys.length > MAX_RADIX_KEYS || keys[0] < 0) {
      Arrays.sort(keys);
      return;
    }
    final long first = keys[0];
    long varying = 0;
    for (final long key : keys) {
      varying |= first ^ key;
    }
    if (varying == 0) {
      return;
    }
    if (varying >>> 48 != 0) {
      Arrays.sort(keys);
      return;
    }
    final int[] shifts = new int[6];
    int passes = 0;
    for (int shift = 0; shift < 48; shift += 8) {
      if ((varying >>> shift & 255L) != 0) {
        shifts[passes++] = shift;
      }
    }
    final int[] histogram = new int[passes << 8];
    for (final long key : keys) {
      for (int pass = 0; pass < passes; pass++) {
        histogram[(pass << 8) + (int) (key >>> shifts[pass] & 255L)]++;
      }
    }
    // Support is at most min(32 MiB, heap / 128), including when storage keys are widely spread.
    long[] source = keys;
    long[] target = new long[keys.length];
    final int[] positions = new int[256];
    for (int pass = 0; pass < passes; pass++) {
      int position = 0;
      for (int bucket = 0; bucket < 256; bucket++) {
        positions[bucket] = position;
        position += histogram[(pass << 8) + bucket];
      }
      final int shift = shifts[pass];
      for (final long key : source) {
        target[positions[(int) (key >>> shift & 255L)]++] = key;
      }
      final long[] swap = source;
      source = target;
      target = swap;
    }
    if (source != keys) {
      System.arraycopy(source, 0, keys, 0, keys.length);
    }
  }
}
