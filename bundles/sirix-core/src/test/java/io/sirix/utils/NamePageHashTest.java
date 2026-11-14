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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class NamePageHashTest {

  @Test
  public void testGenerateHashCodes() {
    final int[] hashes = {70, 25};
    assertArrayEquals(hashes, NamePageHash.generateOffsets("foo".hashCode()));
  }

  @Test
  public void testEveryBitHasExactlyItsDocumentedOffset() {
    assertArrayEquals(new int[] {0, 0}, NamePageHash.generateOffsets(0));
    assertArrayEquals(new int[] {127, 127}, NamePageHash.generateOffsets(-1));
    assertArrayEquals(new int[] {0, 0}, NamePageHash.generateOffsets(Integer.MIN_VALUE));
    assertArrayEquals(new int[] {127, 127}, NamePageHash.generateOffsets(Integer.MAX_VALUE));
    for (int bit = 0; bit < Integer.SIZE; bit++) {
      final int first = bit < 7
          ? 1 << bit
          : 0;
      final int second = bit >= 7 && bit < 14
          ? 1 << (bit - 7)
          : 0;
      assertArrayEquals(new int[] {first, second}, NamePageHash.generateOffsets(1 << bit));
    }
  }

  @Test
  public void testOffsetsAgainstUnsignedDivisionOracle() {
    // Fixed exploration stream: independent arithmetic oracle, reproducible under PIT.
    int key = 0x13579bdf;
    for (int sample = 0; sample < 4096; sample++) {
      key = key * 1664525 + 1013904223;
      final int first = Integer.remainderUnsigned(key, 128);
      final int second = Integer.remainderUnsigned(Integer.divideUnsigned(key, 128), 128);
      assertArrayEquals(new int[] {first, second}, NamePageHash.generateOffsets(key));
    }
  }

  @Test
  public void testGenerateHashForString() {
    final int hash = "foo".hashCode();
    assertEquals(hash, NamePageHash.generateHashForString("foo"));
  }

}
