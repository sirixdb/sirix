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

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.io.SerializationBufferPool;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.LE;
import io.sirix.node.PooledBytesOut;
import io.sirix.node.PooledGrowingSegment;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NodeFieldLayout;
import io.sirix.page.PageKind;
import io.sirix.page.SerializationType;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
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
import org.openjdk.jmh.infra.Blackhole;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Serializing and deserializing one {@link KeyValueLeafPage} — the encode path that dominates
 * ingest CPU.
 *
 * <p>A warm 176 MB shred spends around a fifth of its application samples inside
 * {@code writeEncodedBody} alone, with the codecs and the foreign-memory access checks behind it
 * making up most of the rest. That is the single largest lever on ingest latency, and it is also
 * the one hardest to measure from the outside: an end-to-end shred on a shared 4-core box carries
 * roughly 7% run-to-run variance, which swallows anything smaller than a large win, and CPU
 * profiles of it are muddied by inlining — collapse four byte loads into one int load and the
 * callee's samples do not vanish, they migrate into the caller's self time and make the caller
 * look worse.
 *
 * <p>Hence this: the same work, in isolation, where JMH's error bars can resolve a couple of
 * percent. Judge encode-path changes here first, then confirm the direction end to end.
 *
 * <p>The page is built from synthetic records with genuine delta-varint structural keys, in the
 * shape a DFS shred produces — sibling runs, a share of NULL links, and a realistic spread of
 * parent keys — so the structural-key column and hash elision both engage as they would on real
 * data. {@code slots} spans a nearly-full page down to a sparse one, since per-slot costs and
 * per-page costs scale differently.
 *
 * <p>Crucially the page <em>mixes kinds</em>: containers interleaved with their primitive leaves,
 * which is what a JSON document produces and what a page of 1024 consecutive node keys therefore
 * holds. A single-kind page is not a conservative simplification of that, it is a different
 * benchmark — every per-slot kind dispatch in the encoder becomes perfectly predictable, one
 * offset-table template covers the whole page, and both flatter the encoder in ways real data
 * never will. The kinds here differ in field count, in whether they carry a pathNodeKey, and in
 * whether they carry a hash, so the branchy per-slot paths are exercised from both sides.
 *
 * <p>Run with:
 * <pre>./gradlew :sirix-benchmarks:jmh -Pjmh.includes=PageSerializeBenchmark \
 *     -Pjmh.warmupIterations=5 -Pjmh.iterations=10</pre>
 * The module's Gradle config overrides the iteration annotations below; its defaults (2 warmup,
 * 3 measurement) leave the error bars wider than the effects worth chasing.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
public class PageSerializeBenchmark {

  private static final long NULL = Fixed.NULL_NODE_KEY.getStandardProperty();

  private static final int OBJECT_KIND_ID = 24;
  private static final int ARRAY_KIND_ID = 25;
  private static final int BOOLEAN_VALUE_KIND_ID = 27;
  private static final int NUMBER_VALUE_KIND_ID = 28;
  private static final int NULL_VALUE_KIND_ID = 29;
  private static final int STRING_VALUE_KIND_ID = 30;

  /**
   * One repeat of the record shape a JSON document produces: a container, then the primitive
   * leaves hanging off it. Walked cyclically so a page carries every kind in a stable ratio.
   */
  private static final int[] KIND_CYCLE = {
      OBJECT_KIND_ID, STRING_VALUE_KIND_ID, NUMBER_VALUE_KIND_ID, STRING_VALUE_KIND_ID,
      BOOLEAN_VALUE_KIND_ID, NUMBER_VALUE_KIND_ID, ARRAY_KIND_ID, NUMBER_VALUE_KIND_ID,
      NUMBER_VALUE_KIND_ID, STRING_VALUE_KIND_ID, NULL_VALUE_KIND_ID, NUMBER_VALUE_KIND_ID
  };

  private static final long PAGE_KEY = 7L;

  @Param({ "1000", "256" })
  public int slots;

  private ResourceConfiguration config;
  private KeyValueLeafPage page;

  /** A pre-serialized copy of the same page, so the deserialize arm has stable input. */
  private byte[] serialized;

  @Setup
  public void setUp() {
    Allocators.getInstance().init(2L * 1024 * 1024 * 1024);
    config = new ResourceConfiguration.Builder("bench").build();

    final byte[][] records = new byte[slots][];
    final int[] kinds = new int[slots];
    final Random rnd = new Random(0x5E71A1);
    int total = 0;
    for (int slot = 0; slot < slots; slot++) {
      kinds[slot] = KIND_CYCLE[slot % KIND_CYCLE.length];
      records[slot] = record(kinds[slot], slot, rnd);
      total += records[slot].length;
    }

    page = new KeyValueLeafPage(PAGE_KEY, 0, IndexType.DOCUMENT, config, false, null,
        new LinkedHashMap<>(), Allocators.getInstance().allocate(4096 + total), null, -1);
    for (int slot = 0; slot < slots; slot++) {
      page.setSlotWithNodeKind(MemorySegment.ofArray(records[slot]), slot, kinds[slot]);
    }

    serialized = encodeOnce();

    // The encode caches its result on the page and every later call short-circuits to a memcpy of
    // it, so the benchmark arm drops the cache per invocation. That only measures the encode if
    // encoding twice is the same as encoding once — and it is not obviously so: the write path
    // strips fields out of the slotted page in place and writes them back afterwards, and it
    // rebuilds the region table and the FSST symbol table each time. Check rather than assume; a
    // second pass that diverged would make every number below meaningless.
    final byte[] again = encodeOnce();
    if (!Arrays.equals(serialized, again)) {
      throw new IllegalStateException("re-encoding the same page did not reproduce its bytes ("
          + serialized.length + " vs " + again.length + " bytes) — the write path is not"
          + " idempotent, so dropping the cache per invocation does not measure the encode");
    }
  }

  /** Drop the serialization cache and encode, returning the body after the page-kind id. */
  private byte[] encodeOnce() {
    final PooledGrowingSegment pooled = SerializationBufferPool.INSTANCE.acquire();
    try {
      final BytesOut<?> sink = encodeInto(pooled);
      final BytesIn<?> src = sink.bytesForRead();
      src.readByte(); // pageKind id, consumed by the caller in production too
      final byte[] body = new byte[(int) (sink.writePosition() - 1)];
      src.read(body);
      return body;
    } finally {
      SerializationBufferPool.INSTANCE.release(pooled);
    }
  }

  private BytesOut<?> encodeInto(final PooledGrowingSegment pooled) {
    page.setCompressedSegment(null);
    final BytesOut<?> sink = new PooledBytesOut(pooled);
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
    return sink;
  }

  /**
   * The write path: everything {@code writeEncodedBody} does, plus the page envelope.
   *
   * <p>Sink and buffer lifecycle mirror {@code NodeStorageEngineWriter.serializeKeyValuePage},
   * pooled segment included — an elastic buffer would fold its own growth and off-heap allocation
   * into every measurement, and neither happens during a commit.
   */
  @Benchmark
  public void serialize(final Blackhole blackhole) {
    final PooledGrowingSegment pooled = SerializationBufferPool.INSTANCE.acquire();
    try {
      blackhole.consume(encodeInto(pooled).writePosition());
    } finally {
      SerializationBufferPool.INSTANCE.release(pooled);
    }
  }

  /**
   * The read path: decompress, rebuild the directory, and expand every record's offset table
   * while re-injecting whatever the writer stripped. Included because the same per-slot access
   * patterns appear on both sides, and a page is read far more often than it is written.
   */
  @Benchmark
  public void deserialize(final Blackhole blackhole) {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    sink.write(serialized);
    final KeyValueLeafPage read = (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE
        .deserializePage(config, sink.bytesForRead(), SerializationType.DATA);
    blackhole.consume(read.getPageKey());
    read.close();
  }

  private static long nodeKey(final int slot) {
    return (PAGE_KEY << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
  }

  /**
   * Build one record of {@code kindId} sitting at {@code slot}.
   *
   * <p>Fields are emitted in the order {@link NodeFieldLayout} declares them for the kind, since
   * the encoder locates a field by its offset-table index rather than by name.
   *
   * <p>The link pattern makes a DFS shred's shape: every group of twelve slots is one container
   * followed by its leaves, so the leaves share a parent and form one sibling chain with NULL at
   * both ends. That is what the structural-key column is built to exploit, and a page of
   * independent random links would understate it. Hashes are zero on most slots — containers get a
   * real one — which is the ratio hash elision actually sees.
   */
  private static byte[] record(final int kindId, final int slot, final Random rnd) {
    final long nodeKey = nodeKey(slot);
    final int within = slot % KIND_CYCLE.length;
    final long groupStart = nodeKey(slot - within);
    final long parentKey = within == 0 ? nodeKey(Math.max(0, slot - KIND_CYCLE.length)) : groupStart;
    final long rightSib = within == KIND_CYCLE.length - 1 ? NULL : nodeKey + 1;
    final long leftSib = within <= 1 ? NULL : nodeKey - 1;
    final long hash = rnd.nextLong() | 1L;
    final RecordBuilder b = new RecordBuilder(kindId, nodeKey);

    switch (kindId) {
      case OBJECT_KIND_ID -> b.delta(parentKey).delta(rightSib).delta(leftSib)
          .delta(nodeKey + 1).delta(nodeKey + KIND_CYCLE.length - 1)
          .signed(0).signed(1).hash(hash).signed(KIND_CYCLE.length - 1)
          .signed(KIND_CYCLE.length - 1);
      case ARRAY_KIND_ID -> b.delta(parentKey).delta(rightSib).delta(leftSib)
          .delta(NULL).delta(NULL).delta(3 + (slot % 4))
          .signed(0).signed(1).hash(hash).signed(0).signed(0);
      case BOOLEAN_VALUE_KIND_ID -> b.delta(parentKey).delta(rightSib).delta(leftSib)
          .signed(0).signed(1).raw((byte) (slot & 1));
      case NUMBER_VALUE_KIND_ID -> b.delta(parentKey).delta(rightSib).delta(leftSib)
          .signed(0).signed(1).number(rnd.nextInt(1 << 20));
      case NULL_VALUE_KIND_ID -> b.delta(parentKey).delta(rightSib).delta(leftSib)
          .signed(0).signed(1);
      case STRING_VALUE_KIND_ID -> b.delta(parentKey).delta(rightSib).delta(leftSib)
          .signed(0).signed(1).string(rnd.nextInt(24));
      default -> throw new IllegalArgumentException("no record shape for kindId " + kindId);
    }
    return b.build();
  }

  /**
   * Assembles a {@code [kindId][offsetTable: fieldCount bytes][data region]} record.
   *
   * <p>Each {@code add}-style call appends one field to the data region and records where it
   * started, so the offset table is a by-product of writing the fields rather than something kept
   * in step by hand. {@link #build()} checks the field count against {@link NodeFieldLayout} — a
   * record one field short still looks well-formed to the encoder and would quietly change which
   * bytes it strips.
   */
  private static final class RecordBuilder {

    private final byte[] data = new byte[256];
    private final MemorySegment seg = MemorySegment.ofArray(data);
    private final int[] offsets = new int[32];
    private final int kindId;
    private final long nodeKey;
    private int fields;
    private int cursor;

    RecordBuilder(final int kindId, final long nodeKey) {
      this.kindId = kindId;
      this.nodeKey = nodeKey;
    }

    /** A node-key-valued field, stored as a delta-varint against this record's own key. */
    RecordBuilder delta(final long value) {
      offsets[fields++] = cursor;
      cursor += DeltaVarIntCodec.writeDeltaToSegment(seg, cursor, value, nodeKey);
      return this;
    }

    /** A small signed field — revisions and counts. */
    RecordBuilder signed(final int value) {
      offsets[fields++] = cursor;
      cursor += DeltaVarIntCodec.writeSignedToSegment(seg, cursor, value);
      return this;
    }

    /** The fixed-width rolling hash. */
    RecordBuilder hash(final long value) {
      offsets[fields++] = cursor;
      seg.set(LE.LONG, cursor, value);
      cursor += NodeFieldLayout.HASH_WIDTH;
      return this;
    }

    /** A single opaque byte, such as a boolean's value. */
    RecordBuilder raw(final byte value) {
      offsets[fields++] = cursor;
      data[cursor++] = value;
      return this;
    }

    /** A number payload: {@code [numberType][numberData]}, here always a signed-varint int. */
    RecordBuilder number(final int value) {
      offsets[fields++] = cursor;
      data[cursor++] = 2; // NUMBER_TYPE_INTEGER
      cursor += DeltaVarIntCodec.writeSignedToSegment(seg, cursor, value);
      return this;
    }

    /** A string payload: {@code [isCompressed][length varint][bytes]}, filled with ASCII. */
    RecordBuilder string(final int length) {
      offsets[fields++] = cursor;
      data[cursor++] = 0; // not compressed
      cursor += DeltaVarIntCodec.writeSignedToSegment(seg, cursor, length);
      for (int i = 0; i < length; i++) {
        data[cursor++] = (byte) ('a' + ((i + length) % 26));
      }
      return this;
    }

    byte[] build() {
      final int expected = NodeFieldLayout.fieldCountForKind(kindId);
      if (fields != expected) {
        throw new IllegalStateException("kindId " + kindId + " wants " + expected
            + " fields, record supplies " + fields);
      }
      final byte[] record = new byte[1 + expected + cursor];
      record[0] = (byte) kindId;
      for (int f = 0; f < expected; f++) {
        if (offsets[f] > 0xFF) {
          throw new IllegalStateException("field " + f + " of kindId " + kindId + " starts at "
              + offsets[f] + ", past the one-byte offset table");
        }
        record[1 + f] = (byte) offsets[f];
      }
      System.arraycopy(data, 0, record, 1 + expected, cursor);
      return record;
    }
  }
}
