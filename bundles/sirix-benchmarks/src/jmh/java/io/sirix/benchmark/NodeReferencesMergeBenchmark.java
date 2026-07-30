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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.concurrent.TimeUnit;

/**
 * What it costs to fold one more node key into an existing index reference set — the step
 * {@code HOTLeafPage.mergeWithNodeRefs} runs once per indexed node.
 *
 * <p>Small sets never reach this code: a packed bucket under {@code PACKED_THRESHOLD} is merged
 * by a bespoke single-bit path that rewrites the sorted key array and allocates nothing else.
 * Past that threshold the bucket switches to Roaring and every subsequent insert pays the full
 * round trip — deserialize the whole set, merge, serialize the whole set — so the per-insert cost
 * grows with the set and building an index of {@code n} nodes under one path is quadratic. That
 * shape is the reason this benchmark exists; the arms below measure how much of it is avoidable.
 *
 * <p>The two arms differ by exactly one defensive copy. {@link NodeReferences#getNodeKeys()} hands
 * out the live bitmap and {@link NodeReferences#addNodeKey} mutates in place, so a
 * {@code NodeReferences} built around someone else's bitmap has to clone it or the two sets would
 * overwrite each other. A bitmap that has just been deserialized has no other owner, and cloning
 * it there buys nothing but an O(set) copy and a duplicate allocation on the hot path — which is
 * what {@link NodeReferences#owning} exists to skip.
 *
 * <p>{@code distribution} is not decoration. Roaring stores a dense consecutive range as a single
 * run and a scattered one as a 8 KiB bitmap container, so the clone can cost nearly nothing or
 * copy kilobytes for the same cardinality. Measuring only consecutive keys makes the copy look
 * free and is how you would talk yourself out of a real win: an index over one path of a
 * repeating record collects every k-th node key, which is the {@code STRIDED} arm, not the
 * {@code CONSECUTIVE} one.
 *
 * <p>Measured on a 4-core machine, average time per merge in microseconds:
 * <pre>
 *   distribution   setSize   adopting   copying   copying costs
 *   CONSECUTIVE       4096     10.862    13.466       1.24x
 *   CONSECUTIVE      32768      3.063     4.554       1.49x
 *   STRIDED           4096      9.030    10.331       1.14x
 *   STRIDED          32768      8.889    13.959       1.57x
 *   SPARSE            4096      8.954    11.862       1.32x
 *   SPARSE           32768     28.331    41.585       1.47x
 * </pre>
 * The copy costs most where it copies most — a large scattered set spanning many containers —
 * and the consecutive rows show why the distribution parameter matters: at 32768 the whole set
 * is one run container, so the entire merge is cheaper than the 4096 case that sits right on
 * Roaring's array-to-bitmap conversion threshold.
 *
 * <p>Note the module's Gradle config sets warmup and measurement iterations and overrides the
 * annotations on this class; the figures above came from
 * <pre>./gradlew :sirix-benchmarks:jmh -Pjmh.includes=NodeReferencesMergeBenchmark \
 *     -Pjmh.warmupIterations=5 -Pjmh.iterations=10</pre>
 * The plugin's defaults (2 warmup, 3 measurement) leave error bars wider than the effect.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class NodeReferencesMergeBenchmark {

  /** How the node keys already in the set are laid out. */
  public enum Distribution {
    /** Every key in a contiguous block — Roaring collapses it to one run container. */
    CONSECUTIVE,
    /** Every fifth key, as an index over one path of a five-field record collects. */
    STRIDED,
    /** Keys scattered over a wide range, forcing several containers. */
    SPARSE
  }

  /** Cardinality of the set being merged into. */
  @Param({ "4096", "32768" })
  public int setSize;

  @Param({ "CONSECUTIVE", "STRIDED", "SPARSE" })
  public Distribution distribution;

  /** The serialized reference set already stored in the leaf. */
  private byte[] existing;

  /** The serialized single-key value being merged in, as the index writer supplies it. */
  private byte[] incoming;

  @Setup
  public void setUp() {
    final Roaring64Bitmap bitmap = new Roaring64Bitmap();
    for (int i = 0; i < setSize; i++) {
      bitmap.add(keyAt(i));
    }
    existing = NodeReferencesSerializer.serialize(NodeReferences.owning(bitmap));

    // A key the set does not already hold, so the merge always does work.
    final NodeReferences single = new NodeReferences();
    single.addNodeKey(keyAt(setSize) + 1);
    incoming = NodeReferencesSerializer.serialize(single);
  }

  private long keyAt(final int i) {
    return switch (distribution) {
      case CONSECUTIVE -> i;
      case STRIDED -> (long) i * 5;
      // Spread across several 2^16 Roaring containers rather than one.
      case SPARSE -> ((long) (i % 64) << 16) + (long) (i / 64) * 7;
    };
  }

  /** The merge as it stands: the deserialized bitmap is adopted, not copied. */
  @Benchmark
  public byte[] mergeAdoptingBitmap() {
    final NodeReferences existingRefs = NodeReferencesSerializer.deserialize(existing);
    final NodeReferences newRefs =
        NodeReferencesSerializer.deserialize(incoming, 0, incoming.length);
    NodeReferencesSerializer.merge(existingRefs, newRefs);
    return NodeReferencesSerializer.serialize(existingRefs);
  }

  /**
   * The same merge with the defensive copy the deserializer used to take, so the difference
   * between the two arms is the copy and nothing else.
   */
  @Benchmark
  public byte[] mergeCopyingBitmap() {
    final NodeReferences existingRefs =
        new NodeReferences(NodeReferencesSerializer.deserialize(existing).getNodeKeys());
    final NodeReferences newRefs =
        new NodeReferences(
            NodeReferencesSerializer.deserialize(incoming, 0, incoming.length).getNodeKeys());
    NodeReferencesSerializer.merge(existingRefs, newRefs);
    return NodeReferencesSerializer.serialize(existingRefs);
  }
}
