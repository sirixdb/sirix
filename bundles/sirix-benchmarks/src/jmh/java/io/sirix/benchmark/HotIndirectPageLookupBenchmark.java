/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.benchmark;

import io.sirix.page.HOTIndirectPage;
import io.sirix.page.PageReference;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for {@link HOTIndirectPage#findChildIndex(byte[])} on a SpanNode-style
 * compound node with SparsePartialKeys (the equality-preferred routing path at
 * {@code HOTIndirectPage.findChildSpanNode}).
 *
 * <p>Goal: isolate the cost of the {@code (denseKey & sparseKey) == sparseKey} SIMD
 * subset search plus the scalar equality-preferred follow-up scan so we can measure
 * the win from candidate-bit-iteration over the {@code matchMask} returned by
 * {@link io.sirix.index.hot.SparsePartialKeys#search(int)}.</p>
 *
 * <p>Parameters:</p>
 * <ul>
 *   <li>{@code numChildren} — node fanout (drives both the SparsePartialKeys SIMD
 *       width and the length of the equality-preferred scan).</li>
 *   <li>{@code discBitCount} — number of discriminative bits in the routing mask;
 *       selects the SparsePartialKeys storage tier (byte / short / int).</li>
 * </ul>
 *
 * <p>Two methods:</p>
 * <ul>
 *   <li>{@code lookupFound} — search keys whose dense partial key exactly equals one
 *       of the stored partials, so the equality scan terminates somewhere in the
 *       middle of the array (best case for the new bit-iteration, worst case for the
 *       scalar scan when the match index is large).</li>
 *   <li>{@code lookupMissingEquality} — search keys whose dense partial is outside the
 *       stored set but is a superset of {@code 0}, so the equality scan completes
 *       all {@code numChildren} iterations and falls back to {@code subsetPick}
 *       (the absolute worst case for the scalar scan).</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :sirix-benchmarks:jmh -Pjmh.includes="HotIndirectPageLookupBenchmark"
 * </pre>
 *
 * @author Johannes Lichtenberger
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
    jvmArgs = {"--add-modules=jdk.incubator.vector", "--enable-preview", "--enable-native-access=ALL-UNNAMED"})
public class HotIndirectPageLookupBenchmark {

  /** Node fanout. All values are powers of 2 so {@code idx & (numChildren-1)} cycles cleanly. */
  @Param({"4", "8", "16", "32"})
  public int numChildren;

  /**
   * Number of discriminative bits in the routing mask. 8 → byte-tier SparsePartialKeys,
   * 16 → short-tier, 24 → int-tier. Must satisfy {@code numChildren <= 2^discBitCount}.
   */
  @Param({"8", "16", "24"})
  public int discBitCount;

  private HOTIndirectPage page;
  private byte[][] foundKeys;
  private byte[][] missingKeys;
  private int idx;

  @Setup(Level.Trial)
  public void setUp() {
    if (numChildren > (1L << discBitCount)) {
      throw new IllegalStateException(
          "numChildren=" + numChildren + " exceeds 2^" + discBitCount);
    }

    final long bitMask = (discBitCount == 64) ? -1L : ((1L << discBitCount) - 1L);

    final int[] partials = new int[numChildren];
    final PageReference[] children = new PageReference[numChildren];
    for (int i = 0; i < numChildren; i++) {
      partials[i] = i; // distinct partials in [0, numChildren) ⊂ [0, 2^discBitCount)
      children[i] = new PageReference();
    }

    // createMultiNode supports 1..32 children and installs SparsePartialKeys
    // → both findChildMultiNode and findChildSpanNode converge on the same
    // equality-preferred routing path we want to benchmark.
    page = HOTIndirectPage.createMultiNode(
        /* pageKey */ 1L,
        /* revision */ 1,
        /* initialBytePos */ 0,
        bitMask,
        partials,
        children,
        /* height */ 0);

    foundKeys = new byte[numChildren][];
    missingKeys = new byte[numChildren][];
    for (int i = 0; i < numChildren; i++) {
      foundKeys[i] = makeKey(i);
      // A partial >= numChildren is guaranteed not stored. We also OR in a high bit
      // so the densePartialKey has multiple subset matches against stored partials —
      // this forces the scalar equality scan to run to completion before returning
      // subsetPick (the actual worst case for the current implementation).
      missingKeys[i] = makeKey(numChildren + i);
    }
  }

  /**
   * Build a 16-byte key whose dense partial key (extracted via {@code Long.compress}
   * with the contiguous low-{@code discBitCount} mask placed at the LSB end of the
   * 8-byte BE window at byte 0) equals {@code partial}. The mask occupies the lowest
   * {@code ceil(discBitCount/8)} bytes of that window — i.e., bytes {@code 7, 6, ...}
   * counting from the end.
   */
  private byte[] makeKey(int partial) {
    final byte[] key = new byte[16];
    int remaining = discBitCount;
    int value = partial;
    int byteIdx = 7;
    while (remaining > 0) {
      final int bitsThisByte = Math.min(8, remaining);
      final int byteVal = value & ((1 << bitsThisByte) - 1);
      key[byteIdx] = (byte) byteVal;
      value >>>= bitsThisByte;
      remaining -= bitsThisByte;
      byteIdx--;
    }
    return key;
  }

  @Benchmark
  public int lookupFound() {
    final int i = (idx++) & (numChildren - 1);
    return page.findChildIndex(foundKeys[i]);
  }

  @Benchmark
  public int lookupMissingEquality() {
    final int i = (idx++) & (numChildren - 1);
    return page.findChildIndex(missingKeys[i]);
  }
}
