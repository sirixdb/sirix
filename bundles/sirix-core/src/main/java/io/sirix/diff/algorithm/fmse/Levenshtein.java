/**
 * SimMetrics - SimMetrics is a java library of Similarity or Distance Metrics, e.g. Levenshtein
 * Distance, that provide float based similarity measures between String Data. All metrics return
 * consistant measures rather than unbounded similarity scores.
 *
 * Copyright (C) 2005 Sam Chapman - Open Source Release v1.1
 *
 * Please Feel free to contact me about this library, I would appreciate knowing quickly what you
 * wish to use it for and any criticisms/comments upon the SimMetric library.
 *
 * email: s.chapman@dcs.shef.ac.uk www: http://www.dcs.shef.ac.uk/~sam/ www:
 * http://www.dcs.shef.ac.uk/~sam/stringmetrics.html
 *
 * address: Sam Chapman, Department of Computer Science, University of Sheffield, Sheffield, S.
 * Yorks, S1 4DP United Kingdom,
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if
 * not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307 USA
 */

package io.sirix.diff.algorithm.fmse;


import static java.util.Objects.requireNonNull;

/**
 * Implements the levenstein distance function.
 *
 * Date: 24-Mar-2004 Time: 10:54:06
 *
 * @author Sam Chapman <a href="http://www.dcs.shef.ac.uk/~sam/">Website</a>,
 *         <a href="mailto:sam@dcs.shef.ac.uk">Email</a>.
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class Levenshtein {

  /** Minimum length of strings to compare. */
  private static final int MIN = 4;

  /** Maximum length of strings to compare. */
  private static final int MAX = 50;

  /** A constant for calculating the estimated timing cost. */
  private static final float ESTIMATEDTIMINGCONST = 1.8e-4f;

  /**
   * Get the description.
   *
   * @return the string identifier for the metric
   */
  public static String getShortDescriptionString() {
    return "Levenshtein";
  }

  /**
   * returns the long string identifier for the metric.
   *
   * @return the long string identifier for the metric
   */
  public static String getLongDescriptionString() {
    return "Implements the basic Levenshtein algorithm providing a similarity measure between two strings";
  }

  /**
   * Get the estimated time in milliseconds it takes to perform a similarity timing.
   *
   * @param first first string
   * @param second second string
   *
   * @return the estimated time in milliseconds taken to perform the similarity measure
   */
  public static float getSimilarityTimingEstimated(final String first, final String second) {
    final float str1Length = first.length();
    final float str2Length = second.length();
    return (str1Length * str2Length) * ESTIMATEDTIMINGCONST;
  }

  /**
   * Gets the similarity of the two strings using levenstein distance if string lengths are between
   * {@link Levenshtein#MIN} and {@link Levenshtein#MAX}. Otherwise string equality is used whereas
   * {@code 0} is returned if the strings aren't equal and {@code 1} if they are equal.
   *
   * @param first first string
   * @param second second string
   * @return a value between {@code 0} and {@code 1}. {@code 0} denotes that the strings are
   *         completely different, {@code 1} denotes that the strings are equal
   * @throws NullPointerException if {@code pFirst} or {@code pSecond} is {@code null}
   */
  public static float getSimilarity(final String first, final String second) {
    requireNonNull(first);
    requireNonNull(second);
    if (first.equals(second)) {
      return 1f;
    }

    final int firstLength = first.length();
    final int secondLength = second.length();
    if (firstLength > MAX | secondLength > MAX | firstLength < MIN | secondLength < MIN) {
      return 0f;
    }

    final float levenshteinDistance = getUnNormalisedSimilarity(first, second);

    // Convert into zero to one and return value.
    final float maxLen = Math.max(firstLength, secondLength);

    // Actual / possible levenshtein distance to get 0-1 range.
    final float norm = 1f - (levenshteinDistance / maxLen);

    assert norm >= 0f && norm <= 1f;
    return norm;
  }

  /**
   * Implements the levenstein distance function using two-row optimization.
   * Only two rows of the DP matrix are kept in memory at any time,
   * reducing allocation from O(n*m) to O(m).
   *
   * @param s first string
   * @param t second string to compare
   * @return the levenstein distance between given strings
   */
  private static float getUnNormalisedSimilarity(final String s, final String t) {
    final int n = s.length();
    final int m = t.length();
    if (n == 0) {
      return m;
    }
    if (m == 0) {
      return n;
    }

    // Two-row optimization: only keep previous and current rows
    float[] prev = new float[m + 1];
    float[] curr = new float[m + 1];

    // Initialize first row
    for (int j = 0; j <= m; j++) {
      prev[j] = j;
    }

    for (int i = 1; i <= n; i++) {
      curr[0] = i;
      final char si = s.charAt(i - 1);
      for (int j = 1; j <= m; j++) {
        // Inlined cost function: 0 if chars match, 1 otherwise
        final float cost = (si == t.charAt(j - 1)) ? 0.0f : 1.0f;
        curr[j] = min3(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost);
      }
      // Swap rows
      final float[] temp = prev;
      prev = curr;
      curr = temp;
    }

    return prev[m];
  }

  /**
   * Get the minimum of three numbers.
   *
   * @param x first number
   * @param y second number
   * @param z third number
   * @return the {@code minimum} of the three specified numbers
   */
  private static float min3(final float x, final float y, final float z) {
    return Math.min(x, Math.min(y, z));
  }
}
