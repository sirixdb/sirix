package io.sirix.utils;

import static java.util.Objects.requireNonNull;

/** Allocation-free string comparison under XQuery's default Unicode codepoint collation. */
public final class StringComparisons {
  private StringComparisons() {
    throw new AssertionError("no instances");
  }

  /**
   * Returns a negative value, zero, or a positive value as left orders before, with, or after right.
   */
  public static int compareCodePoints(final String left, final String right) {
    requireNonNull(left, "left");
    requireNonNull(right, "right");
    final int length = Math.min(left.length(), right.length());
    for (int i = 0; i < length;) {
      final int leftCodePoint = left.codePointAt(i);
      final int rightCodePoint = right.codePointAt(i);
      if (leftCodePoint != rightCodePoint) {
        return leftCodePoint - rightCodePoint;
      }
      i += Character.charCount(leftCodePoint);
    }
    return left.length() - right.length();
  }
}
