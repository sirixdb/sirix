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

import io.sirix.index.hot.NodeReferencesSerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
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
import org.openjdk.jmh.infra.Blackhole;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

/**
 * Measures reassembling one HOT index chunk: decoding its stored node keys and merging them into a
 * result bitmap.
 *
 * <p>Two implementations are compared. {@code materializeThenMerge} is what the index scan and
 * point-lookup paths did before: copy the slot value to the heap, {@code deserialize} it into a
 * {@link NodeReferences} (allocating a {@link Roaring64Bitmap}, which that constructor then clones),
 * and walk a {@link LongIterator} to re-add every key. {@code mergeInPlace} reads the keys straight
 * off the page view via {@code NodeReferencesSerializer.mergeChunkInto}.
 *
 * <h2>Methodology — read before trusting a result</h2>
 *
 * <p>The destination bitmap is allocated once in {@link Setup} and <b>cleared</b> per invocation
 * rather than allocated per invocation. That is load-bearing, not tidiness: a freshly allocated
 * destination costs O(cardinality) itself and, above a few dozen keys, dominates the very
 * difference being measured. It will flatten a real gap to ~1.0x and read as "no improvement" —
 * which is exactly how an earlier hand-rolled harness mis-reported this and nearly buried the
 * packed-path result.
 *
 * <p>The {@code cardinality} sweep straddles {@code PACKED_THRESHOLD} (64), where the encoding
 * flips from the packed form (a dense array of big-endian longs, readable with no decoding) to the
 * Roaring form (which must go through Roaring's own decoder). The two sides behave differently and
 * that boundary is the point of the sweep — it is where the optimisation stops paying.
 *
 * <p>Allocation per operation is the primary signal, so run with the GC profiler:
 * <pre>{@code
 * ./gradlew :sirix-benchmarks:jmhJar
 * java --enable-preview --add-modules jdk.incubator.vector \
 *      --add-opens java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED \
 *      -jar bundles/sirix-benchmarks/build/libs/*-jmh.jar \
 *      ".*NodeReferencesChunkMerge.*" -prof gc
 * }</pre>
 *
 * <p>Measured on this benchmark (2 forks, -prof gc): the packed path is 2.1–6.9x faster and
 * allocates 2.0–2.8x less, while at 256 keys the two are within noise on time and identical on
 * allocation. That result is why the Roaring branch does <em>not</em> carry a reused scratch
 * bitmap — one was tried and measured 48 B/op <em>worse</em>.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
@Fork(2)
public class NodeReferencesChunkMergeBenchmark {

  /** Straddles PACKED_THRESHOLD (64): 1..64 use the packed form, 256+ the Roaring form. */
  @Param({"1", "4", "16", "64", "256", "4096"})
  public int cardinality;

  /** The chunk's stored bytes, as the heap array the OLD path consumed. */
  private byte[] encodedHeap;

  /** The same bytes as an off-heap view, as the NEW path consumes them off the page. */
  private MemorySegment encodedView;

  private Arena arena;

  /** Reused destination — see the methodology note on the class javadoc. */
  private Roaring64Bitmap dest;

  private static final long HIGH_BITS = 7L << 16;

  @Setup(Level.Trial)
  public void setUp() {
    final NodeReferences refs = new NodeReferences();
    for (int i = 0; i < cardinality; i++) {
      // Spread across the 16-bit space a chunk stores, deterministically.
      refs.addNodeKey((i * 7919L) & 0xFFFFL);
    }
    encodedHeap = NodeReferencesSerializer.serialize(refs);

    arena = Arena.ofShared();
    encodedView = arena.allocate(encodedHeap.length, 1);
    MemorySegment.copy(MemorySegment.ofArray(encodedHeap), 0, encodedView, 0, encodedHeap.length);

    dest = new Roaring64Bitmap();
  }

  @Setup(Level.Invocation)
  public void clearDest() {
    dest.clear();
  }

  /** The pre-change path: heap copy, full materialization, iterator walk. */
  @Benchmark
  public void materializeThenMerge(final Blackhole bh) {
    if (!NodeReferencesSerializer.isTombstone(encodedHeap, 0, encodedHeap.length)) {
      final NodeReferences refs = NodeReferencesSerializer.deserialize(encodedHeap);
      final Roaring64Bitmap bitmap = refs.getNodeKeys();
      if (!bitmap.isEmpty()) {
        final LongIterator it = bitmap.getLongIterator();
        while (it.hasNext()) {
          dest.add(HIGH_BITS | (it.next() & 0xFFFFL));
        }
      }
    }
    bh.consume(dest);
  }

  /** The current path: keys read in place off the page view. */
  @Benchmark
  public void mergeInPlace(final Blackhole bh) {
    if (!NodeReferencesSerializer.isTombstone(encodedView)) {
      NodeReferencesSerializer.mergeChunkInto(encodedView, HIGH_BITS, dest);
    }
    bh.consume(dest);
  }
}
