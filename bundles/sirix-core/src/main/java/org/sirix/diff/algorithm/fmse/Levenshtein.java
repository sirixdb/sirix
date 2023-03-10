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

package org.sirix.diff.algorithm.fmse;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.diff.algorithm.fmse.utils.SubCost01;
import org.sirix.diff.algorithm.fmse.utils.SubstitutionCost;

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
   * The private cost function used in the levenstein distance.
   */
  private static final SubstitutionCost COSTFUNC = new SubCost01();

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
  public static float getSimilarityTimingEstimated(final String first,
      @NonNull final String second) {
    // timed millisecond times with string lengths from 1 + 50 each increment
    // 0 0.31 1.12 2.4 4.41 6.77 11.28 14.5 24.33 31.29 43.6 51 54.5 67.67 68 78
    // 88.67 101.5 109 117.5
    // 140.5 148.5 156 180 187.5 219 203 250 250 312 297 328 297 359 360 406 453
    // 422 437 469 500 516 578
    // 578 578 609 672 656 688 766 765 781 829 843 875 891 984 954 984 1078
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
  public static float getSimilarity(final String first, @NonNull final String second) {
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
    // ================================================

    // Get the max possible levenshtein distance score for string.
    final float maxLen = firstLength > secondLength
        ? firstLength
        : secondLength;

    // Actual / possible levenshtein distance to get 0-1 range.
    final float norm = 1f - (levenshteinDistance / maxLen);

    assert norm >= 0f && norm <= 1f;
    return norm;
  }

  /**
   * Implements the levenstein distance function.
   *
   * Copy character from string1 over to string2 (cost 0) Delete a character in string1 (cost 1)
   * Insert a character in string2 (cost 1) Substitute one character for another (cost 1)
   *
   * <pre>
   * D(i - 1, j - 1) + d(si, tj) // subst/copy D(i,j) = min D(i-1,j)+1 //insert
   *                             // D(i,j-1)+1 //delete
   * </pre>
   *
   * <pre>
   * d(i,j) is a function whereby d(c,d)=0 if c=d, 1 else.
   * </pre>
   *
   * @param s first string
   * @param t second string to compare
   * @return the levenstein distance between given strings
   */
  private static float getUnNormalisedSimilarity(final String s, final String t) {
    assert s != null;
    assert t != null;
    final float[][] d; // matrix
    final int n; // length of s
    final int m; // length of t
    int i; // iterates through s
    int j; // iterates through t
    float cost; // cost

    // Step 1
    n = s.length();
    m = t.length();
    if (n == 0) {
      return m;
    }
    if (m == 0) {
      return n;
    }
    d = new float[n + 1][m + 1];

    // Step 2
    for (i = 0; i <= n; i++) {
      d[i][0] = i;
    }
    for (j = 0; j <= m; j++) {
      d[0][j] = j;
    }

    // Step 3
    for (i = 1; i <= n; i++) {
      // Step 4
      for (j = 1; j <= m; j++) {
        // Step 5
        cost = COSTFUNC.getCost(s, i - 1, t, j - 1);

        // Step 6
        d[i][j] = min3(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + cost);
      }
    }

    // Step 7
    return d[n][m];
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
