/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

/**
 * Writes the US-ASCII prefix of a {@link String} straight into a key buffer.
 *
 * <p>Index keys are built one per indexed node, so {@code String.getBytes(UTF_8)} on that path is a
 * {@code byte[]} per node that exists only to be copied into the destination and dropped. Field
 * names and the overwhelming majority of indexed values are ASCII, where UTF-8 is one byte per
 * char and the intermediate array buys nothing.</p>
 *
 * <p>Callers check {@link #isAsciiPrefix} first and fall back to {@code getBytes} otherwise, so
 * this never has to reproduce UTF-8's multi-byte forms or {@code getBytes}' replacement of
 * unpaired surrogates — the bytes are identical to the prefix {@code getBytes} would have
 * produced, by construction.</p>
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
   * @param s the string
   * @param upTo number of leading chars to test; must not exceed {@code s.length()}
   * @return {@code true} if every one of them is below {@code 0x80}
   */
  static boolean isAsciiPrefix(final String s, final int upTo) {
    for (int i = 0; i < upTo; i++) {
      if (s.charAt(i) >= 0x80) {
        return false;
      }
    }
    return true;
  }

  /**
   * Write the first {@code n} chars of {@code s} as one byte each.
   *
   * @param s the string, whose first {@code n} chars must be US-ASCII
   * @param n number of chars to write
   * @param dest destination buffer
   * @param offset offset to write at
   * @return {@code n}, the number of bytes written
   */
  static int writeAsciiPrefix(final String s, final int n, final byte[] dest, final int offset) {
    for (int i = 0; i < n; i++) {
      dest[offset + i] = (byte) s.charAt(i);
    }
    return n;
  }
}
