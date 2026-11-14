package io.sirix.utils;

import java.util.Locale;

/** Checked startup configuration parsing. An explicit invalid cap must never become a default. */
public final class ConfigurationProperties {
  private ConfigurationProperties() {}

  /** Read a positive byte cap, using the default only when the property is absent. */
  public static long positiveBytes(final String property, final long defaultValue) {
    if (defaultValue <= 0) {
      throw new IllegalArgumentException("Default for " + property + " must be positive: " + defaultValue);
    }
    final String value = System.getProperty(property);
    return value == null ? defaultValue : parsePositiveBytes(property, value);
  }

  /** Parse bytes or binary K/KB, M/MB and G/GB units, rejecting malformed or overflowing caps. */
  public static long parsePositiveBytes(final String property, final String value) {
    if (value == null) {
      throw invalid(property, null, "positive bytes or K/KB, M/MB, G/GB units", null);
    }
    String numeric = value.trim().toUpperCase(Locale.ROOT);
    long multiplier = 1;
    if (numeric.endsWith("B")) {
      numeric = numeric.substring(0, numeric.length() - 1);
    }
    if (numeric.endsWith("G")) {
      multiplier = 1L << 30;
      numeric = numeric.substring(0, numeric.length() - 1);
    } else if (numeric.endsWith("M")) {
      multiplier = 1L << 20;
      numeric = numeric.substring(0, numeric.length() - 1);
    } else if (numeric.endsWith("K")) {
      multiplier = 1L << 10;
      numeric = numeric.substring(0, numeric.length() - 1);
    }
    try {
      final long bytes = Math.multiplyExact(Long.parseLong(numeric.trim()), multiplier);
      if (bytes > 0) {
        return bytes;
      }
    } catch (final NumberFormatException | ArithmeticException failure) {
      throw invalid(property, value, "positive bytes within the signed 64-bit range", failure);
    }
    throw invalid(property, value, "positive bytes", null);
  }

  /** Read a positive entry count, using the default only when the property is absent. */
  public static int positiveInt(final String property, final int defaultValue) {
    if (defaultValue <= 0) {
      throw new IllegalArgumentException("Default for " + property + " must be positive: " + defaultValue);
    }
    final String value = System.getProperty(property);
    if (value == null) {
      return defaultValue;
    }
    try {
      final int count = Integer.parseInt(value.trim());
      if (count > 0) {
        return count;
      }
    } catch (final NumberFormatException failure) {
      throw invalid(property, value, "an entry count in [1, 2147483647]", failure);
    }
    throw invalid(property, value, "an entry count in [1, 2147483647]", null);
  }

  private static IllegalArgumentException invalid(final String property, final String value,
      final String expected, final Throwable cause) {
    return new IllegalArgumentException("Invalid -D" + property + "=" + value + ": expected " + expected, cause);
  }
}
