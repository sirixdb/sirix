/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

/**
 * Writes the US-ASCII prefix of a {@link String} straight into a key buffer.
 *
 * <p>
 * Index keys are built one per indexed node, so {@code String.getBytes(UTF_8)} on that path is a
 * {@code byte[]} per node that exists only to be copied into the destination and dropped. Field
 * names and the overwhelming majority of indexed values are ASCII, where UTF-8 is one byte per char
 * and the intermediate array buys nothing.
 * </p>
 *
 * <p>
 * Callers check {@link #isAsciiPrefix} first and fall back to {@code getBytes} otherwise, so this
 * never has to reproduce UTF-8's multi-byte forms or {@code getBytes}' replacement of unpaired
 * surrogates — the bytes are identical to the prefix {@code getBytes} would have produced, by
 * construction.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
final class AsciiKeyBytes {

  private AsciiKeyBytes() {
    throw new AssertionError("no instances");
  }

  /**
   * Whether the first {@code upTo} chars of {@code s} are all US-ASCII.
   *
   * <p>
   * HFT: accumulates every char into one {@code int} and tests the accumulator once, instead of
   * branching per char. The branchless form is a plain OR reduction over the string's backing array,
   * which C2 can unroll and vectorize; the early-exit form cannot be, and this loop runs once per key
   * serialized. Chars are non-negative, so an accumulator below {@code 0x80} proves every
   * contributing char was.
   *
   * @param s the string
   * @param upTo number of leading chars to test; must not exceed {@code s.length()}
   * @return {@code true} if every one of them is below {@code 0x80}
   */
  static boolean isAsciiPrefix(final String s, final int upTo) {
    int accumulator = 0;
    for (int i = 0; i < upTo; i++) {
      accumulator |= s.charAt(i);
    }
    return accumulator < 0x80;
  }

  /**
   * Write the first {@code n} chars of {@code s} as one byte each.
   *
   * <p>
   * HFT: {@link String#getBytes(int, int, byte[], int)} writes exactly the low byte of each char —
   * the same bytes this used to produce with a {@code charAt} loop — but for a Latin-1-coded string,
   * which every ASCII string is, it bottoms out in {@code System.arraycopy} over the backing array
   * rather than one bounds-checked store per char. Deprecated since 1.1 for being charset-blind,
   * which is precisely the contract wanted here: the caller has already established that the first
   * {@code n} chars are ASCII, so truncation to eight bits is lossless.
   *
   * @param s the string, whose first {@code n} chars must be US-ASCII
   * @param n number of chars to write
   * @param dest destination buffer
   * @param offset offset to write at
   * @return {@code n}, the number of bytes written
   */
  @SuppressWarnings("deprecation")
  static int writeAsciiPrefix(final String s, final int n, final byte[] dest, final int offset) {
    s.getBytes(0, n, dest, offset);
    return n;
  }
}
