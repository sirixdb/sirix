/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.diff.algorithm.fmse;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.ArrayList;
import java.util.List;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.utils.Pair;

/**
 * Useful functions.
 */
public final class Util {

  /**
   * Private constructor.
   */
  private Util() {
    throw new AssertionError("May not be instantiated!");
  }

  /**
   * Longest common subsequence algorithm. cf. E. Myers. An O(ND) difference algorithm and its
   * variations. Algorithmica, 1(2): 251-266, 1986
   *
   * @param <T> Element to find commonSubSequence for
   * @param first first list
   * @param second second list
   * @param cmp function to compare the items in both lists (equality)
   * @return lcs, the items in the pairs are equal and taken from list x and list y.
   */
  public static <T> List<Pair<T, T>> longestCommonSubsequence(@NonNull final List<T> first,
      @NonNull final List<T> second, @NonNull final NodeComparator<T> cmp) {

    if (first == null || second == null) {
      return new ArrayList<>();
    }

    if (first.size() == 0 && second.size() == 0) {
      return new ArrayList<>();
    }

    final List<T> x = checkNotNull(first);
    final List<T> y = checkNotNull(second);
    final int n = x.size();
    final int m = y.size();
    final int max = n + m;

    final int v[] = new int[2 * max + 1];
    final List<List<Pair<T, T>>> common = new ArrayList<>(2 * max + 1);
    for (int i = 0; i <= 2 * max; i++) {
      v[i] = 0;
      common.add(i, new ArrayList<Pair<T, T>>());
    }

    for (int i = 0; i <= max; i++) {
      for (int j = -i; j <= i; j += 2) {
        int idxX;
        if (j == -i || j != i && v[max + j - 1] < v[max + j + 1]) {
          // System.err.printf("Array index: %d\n", max + j + 1);
          idxX = v[max + j + 1];
          common.set(max + j, new ArrayList<>(common.get(max + j + 1)));
        } else {
          idxX = v[max + j - 1] + 1;
          common.set(max + j, new ArrayList<>(common.get(max + j - 1)));
        }
        int idxY = idxX - j;
        while (idxX < n && idxY < m && cmp.isEqual(x.get(idxX), y.get(idxY))) {
          common.get(max + j).add(new Pair<>(x.get(idxX), y.get(idxY)));
          idxX++;
          idxY++;
        }

        v[max + j] = idxX;
        if (idxX >= n && idxY >= m) {
          return common.get(max + j);
        }
      }
    }
    throw new IllegalStateException("We should never get to this point!");
  }

  /**
   * Calculates the similarity of two strings. This is done by comparing the frequency of each
   * character occurs in both strings.
   * 
   * @param first first string
   * @param second second string
   * @return similarity of a and b, a value in [0, 1]
   */
  public static float quickRatio(final String first, final String second) {
    if ((first.isEmpty() && second.isEmpty()) || (first.equals(second))) {
      return 1;
    }

    float matches = 0;
    // Use a sparse array to reduce the memory usage
    // for unicode characters.
    final int x[][] = new int[256][];
    for (char c : second.toCharArray()) {
      if (x[c >> 8] == null) {
        x[c >> 8] = new int[256];
      }
      x[c >> 8][c & 0xFF]++;
    }

    for (char c : first.toCharArray()) {
      final int n = (x[c >> 8] == null) ? 0 : x[c >> 8][c & 0xFF]--;
      if (n > 0) {
        matches++;
      }
    }

    return (float) (2d * matches / (first.length() + second.length()));
  }

}
