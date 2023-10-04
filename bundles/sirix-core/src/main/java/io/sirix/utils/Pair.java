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
package io.sirix.utils;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * A Pair of objects.
 *
 * @param <U> first reference
 * @param <V> second reference
 */
public class Pair<U, V> {
  /** The first reference. */
  private final U first;

  /** The second reference. */
  private final V second;

  /**
   * Constructs the pair.
   *
   * @param first first reference
   * @param second second reference
   */
  public Pair(@Nullable final U first, @Nullable final V second) {
    this.first = first;
    this.second = second;
  }

  /**
   * Get the first reference.
   *
   * @return first reference
   */
  public U getFirst() {
    return first;
  }

  /**
   * Get the second reference.
   *
   * @return second reference
   */
  public V getSecond() {
    return second;
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Pair))
      return false;

    @SuppressWarnings("unchecked")
    final Pair<U, V> otherPair = (Pair<U, V>) other;

    return Objects.equals(first, otherPair.first) && Objects.equals(second, otherPair.second);
  }

  @Override
  public String toString() {
    return new StringBuilder("first: ").append(first.toString())
                                       .append(" second: ")
                                       .append(second.toString())
                                       .toString();
  }
}
