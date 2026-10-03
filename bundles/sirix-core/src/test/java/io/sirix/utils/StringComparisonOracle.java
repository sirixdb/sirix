package io.sirix.utils;

import io.brackit.query.atomic.Str;

/** The interpreter's string collation, kept independent of the served comparators. */
public final class StringComparisonOracle {
  private StringComparisonOracle() {
    throw new AssertionError("no instances");
  }

  public static int compareStrings(final String left, final String right) {
    return new Str(left).atomicCmpInternal(new Str(right));
  }
}
