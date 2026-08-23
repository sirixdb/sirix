package io.sirix.index.path.summary;

import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Per-{@link PathNode} value statistics: lazily-allocated value-object held
 * directly on the owning {@link PathNode}, allocated only on the first
 * recorded observation. Persisted inline as the trailing block of the
 * {@link io.sirix.node.NodeKind#PATH} record serialization when the resource
 * is configured with {@code withPathStatistics == true}.
 *
 * <p>Lifted out of {@link PathNode}'s field set so the node carries a single
 * nullable reference (8 B) instead of 11 always-present primitives + lazy
 * heap blobs. Empty-state PathNodes that never see an analytical query pay
 * only that one reference.
 *
 * <p>Read/written by {@link PathSummaryWriter} at commit time; read by the
 * vectorized executor's aggregate-short-circuit fast paths at query time.
 */
public final class PathStats {

  public static final long EMPTY_MIN = Long.MAX_VALUE;
  public static final long EMPTY_MAX = Long.MIN_VALUE;

  public long count;
  public long nullCount;
  public long sum;
  public long min = EMPTY_MIN;
  public long max = EMPTY_MAX;
  public byte @Nullable [] minBytes;
  public byte @Nullable [] maxBytes;
  public @Nullable HyperLogLogSketch hll;
  public boolean minDirty;
  public boolean maxDirty;

  /**
   * Fractional remainder of {@link #sum}, carrying what the integral accumulator cannot.
   *
   * <p>{@link #sum} holds the integral part and this the rest, so a column of {@code 17.125} sums
   * to {@code 17 + 0.125} rather than to {@code 17}. Numbers used to reach the statistics through
   * {@code Number.longValue()}, which truncates on the way IN — the loss happened before anything
   * could detect it, and a sum served from the summary was silently wrong for every non-integral
   * column.
   *
   * <p>Maintained but never SERVED: any non-integral observation also sets {@link #doubleTyped},
   * and the reader declines every value aggregate on a double-typed column — deliberately, because
   * a double sum depends on the order the values were added, so the summary's answer and the scan's
   * would differ even when both are arithmetically defensible. This exists so the accumulator is
   * not silently wrong if that policy is ever relaxed, not because anything reads it today.
   */
  public double sumFraction;

  /**
   * Set when the sum can no longer be reproduced exactly, after which readers must not serve
   * {@code sum}/{@code avg} from these statistics.
   *
   * <p>Needed because floating-point addition is not invertible: subtracting a double on delete
   * does not restore the value the accumulator held before it was added, so a maintained double
   * sum drifts away from the true one. Same reasoning as {@link #minDirty} — the honest move is to
   * record that the aggregate is no longer trustworthy and let the query fall back to the scan.
   */
  public boolean sumDirty;

  /**
   * Set once any observation arrived as a floating-point value, whatever its magnitude.
   *
   * <p>Stickiness is the point: {@code 1.0E16} is integral, so folding it into the long
   * accumulator looks lossless — but the interpreter sums that column with double arithmetic and
   * rounds at every step, landing on {@code 1.0E16} where an exact integer sum lands on
   * {@code 10000000000000002}. The summary is not entitled to be "more accurate" than the answer
   * it is standing in for, so a column that was ever floating-point declines value aggregates.
   */
  public boolean doubleTyped;

  /**
   * Set when the value COUNT can no longer be trusted, after which nothing may be served from
   * these statistics.
   *
   * <p>Moving a subtree re-parents records without re-attributing their observations: the summary
   * adapts its structure, but the values stay counted under the path they left. Recomputing the
   * affected paths would mean walking the moved subtree, so the statistics instead record that
   * they are stale and let queries fall back to the scan -- the same bargain {@link #minDirty}
   * already makes for the bounds.
   */
  public boolean countDirty;

  /**
   * Leaf pages on which this path was observed; {@code null} until the first merge.
   *
   * <p><b>Guarded by {@code this}.</b> Every read, write and serialization of this bitmap holds the
   * {@code PathStats} monitor, and the field is private so that discipline cannot be bypassed from
   * outside. The async snapshot flush genuinely shares it across threads:
   * {@code KeyValueLeafPage.deepCopy()} copies the {@code records[]} ARRAY but not the records, so
   * the serialization copy and the live page hold the SAME {@link PathNode}, and the background
   * snapshot-append thread serializes this bitmap while the ingest thread keeps merging page keys
   * into it.
   *
   * <p>{@link RoaringBitmap} tolerates neither half of that. {@code runOptimize()} REWRITES the
   * container in place: it sizes a run container from a run count and then re-walks the array to
   * fill it, so a concurrent {@code add} that creates a run overruns the fill
   * ({@code ArrayIndexOutOfBoundsException: Index N out of bounds for length N}) while one that
   * merges two runs silently under-fills it, leaving a container whose declared run count exceeds
   * what was written. Independently of {@code runOptimize},
   * {@code serializedSizeInBytes()} and {@code serialize()} are two separate walks, so an add
   * between them makes the length prefix disagree with the bytes that follow and corrupts the
   * record for every later reader.
   */
  private @Nullable RoaringBitmap pageKeys;

  public PathStats() {
  }

  public synchronized boolean isEmpty() {
    return count == 0L && nullCount == 0L && sum == 0L && sumFraction == 0.0d && !sumDirty
        && !doubleTyped && !countDirty
        && min == EMPTY_MIN && max == EMPTY_MAX
        && minBytes == null && maxBytes == null && hll == null
        && !minDirty && !maxDirty && pageKeys == null;
  }

  /**
   * Serialize this record to {@code sink}. Mirrors the legacy inline encoding
   * previously embedded in {@link io.sirix.node.NodeKind#PATH}.
   *
   * <p>This is the frozen V0 layout. {@code sumFraction} and the three flags below sit between
   * {@code maxDirty} and the page-key trailer; any future rearrangement requires a new resource
   * encoding version before resources using that layout are written.
   */
  public void writeTo(final BytesOut<?> sink) {
    sink.writeLong(count);
    sink.writeLong(nullCount);
    sink.writeLong(sum);
    sink.writeLong(min);
    sink.writeLong(max);
    writeOptionalBytes(sink, minBytes);
    writeOptionalBytes(sink, maxBytes);
    final HyperLogLogSketch hllRef = hll;
    if (hllRef == null) {
      sink.writeInt(-1);
    } else {
      final byte[] hllBytes = hllRef.serialize();
      sink.writeInt(hllBytes.length);
      sink.write(hllBytes);
    }
    sink.writeBoolean(minDirty);
    sink.writeBoolean(maxDirty);
    sink.writeDouble(sumFraction);
    sink.writeBoolean(sumDirty);
    sink.writeBoolean(doubleTyped);
    sink.writeBoolean(countDirty);
    writePageKeysTo(sink);
  }

  /**
   * Write the optional page-key trailer under the monitor, so no merge can land between
   * {@code runOptimize()}, the length prefix and the payload those two agreed on.
   *
   * <p>Nothing is copied: the flush path stays allocation-free -- a defensive clone here would put
   * an unbounded, bitmap-sized allocation on every snapshot flush -- and the monitor is held only
   * for this one trailer, so the ingest thread can at worst wait out a single bitmap's
   * serialization.
   */
  private synchronized void writePageKeysTo(final BytesOut<?> sink) {
    final RoaringBitmap pageKeysRef = pageKeys;
    if (pageKeysRef == null) {
      sink.writeInt(-1);
      return;
    }
    pageKeysRef.runOptimize();
    sink.writeInt(pageKeysRef.serializedSizeInBytes());
    try (final DataOutputStream out = new DataOutputStream(sink.outputStream())) {
      pageKeysRef.serialize(out);
    } catch (final IOException e) {
      throw new UncheckedIOException("PathStats pageKeys serialize failed", e);
    }
  }

  /**
   * Merge every key {@code keys} yields into the presence bitmap, allocating it on first use.
   * Holds the monitor for the whole batch so a flush observes the batch as a unit.
   */
  synchronized void mergePageKeys(final IntIterator keys) {
    RoaringBitmap bitmap = pageKeys;
    if (bitmap == null) {
      bitmap = new RoaringBitmap();
      pageKeys = bitmap;
    }
    while (keys.hasNext()) {
      bitmap.add(keys.nextInt());
    }
  }

  /** Replace the presence bitmap; the caller hands over ownership of {@code bitmap}. */
  synchronized void setPageKeys(final @Nullable RoaringBitmap bitmap) {
    pageKeys = bitmap;
  }

  /**
   * The live presence bitmap, or {@code null}. Safe only for callers reading a revision no ingest
   * thread is still mutating; anything else must use {@link #pageKeysToArray()}, which snapshots
   * under the monitor.
   */
  synchronized @Nullable RoaringBitmap pageKeys() {
    return pageKeys;
  }

  /** A snapshot of the presence bitmap taken under the monitor, or {@code null} if unset. */
  synchronized int @Nullable [] pageKeysToArray() {
    final RoaringBitmap bitmap = pageKeys;
    return bitmap == null ? null : bitmap.toArray();
  }

  /**
   * Convenience: writes either the supplied non-null stats or an empty-state
   * trailer when {@code stats == null}. Avoids allocating a throwaway empty
   * {@link PathStats} on the hot serialize path for nodes that never recorded
   * a value.
   */
  public static void writeOrEmpty(final BytesOut<?> sink, final @Nullable PathStats stats) {
    if (stats == null) {
      EMPTY.writeTo(sink);
    } else {
      stats.writeTo(sink);
    }
  }

  /**
   * Read a record produced by {@link #writeTo(BytesOut)} from {@code source}.
   *
   * <p>Tolerates a record that stops before the optional trailing presence-bitmap field, which is
   * the only shape variation within V0.
   */
  public static PathStats readFrom(final BytesIn<?> source) {
    final PathStats s = new PathStats();
    s.count = source.readLong();
    s.nullCount = source.readLong();
    s.sum = source.readLong();
    s.min = source.readLong();
    s.max = source.readLong();
    s.minBytes = readOptionalBytes(source);
    s.maxBytes = readOptionalBytes(source);
    final int hllLen = source.readInt();
    if (hllLen >= 0) {
      final byte[] hllBytes = new byte[hllLen];
      source.read(hllBytes, 0, hllLen);
      s.hll = HyperLogLogSketch.deserialize(hllBytes);
    }
    s.minDirty = source.readBoolean();
    s.maxDirty = source.readBoolean();
    s.sumFraction = source.readDouble();
    s.sumDirty = source.readBoolean();
    s.doubleTyped = source.readBoolean();
    s.countDirty = source.readBoolean();
    final int bitmapLen = readOptionalIntLength(source);
    if (bitmapLen > 0) {
      final byte[] bmBytes = new byte[bitmapLen];
      source.read(bmBytes, 0, bitmapLen);
      final RoaringBitmap bm = new RoaringBitmap();
      try {
        bm.deserialize(new DataInputStream(new ByteArrayInputStream(bmBytes)));
      } catch (final IOException e) {
        throw new UncheckedIOException("PathStats pageKeys deserialize failed", e);
      }
      s.pageKeys = bm;
    } else if (bitmapLen == 0) {
      // Empty bitmap was explicitly serialised — preserve it (a completed scan
      // that proved the nameKey is present nowhere).
      s.pageKeys = new RoaringBitmap();
    }
    // bitmapLen == -1 → leave pageKeys null (legacy / absent).
    return s;
  }

  /**
   * Convenience: reads a record from {@code source} and returns {@code null}
   * if the parsed stats are in the empty default state. Lets the caller keep
   * the lazy-allocation property for nodes whose serialised trailer is
   * effectively empty.
   */
  public static @Nullable PathStats readFromOrNullIfEmpty(final BytesIn<?> source) {
    final PathStats s = readFrom(source);
    return s.isEmpty() ? null : s;
  }

  /** Shared empty-state instance used as a write-side stand-in for null. */
  private static final PathStats EMPTY = new PathStats();

  private static void writeOptionalBytes(final BytesOut<?> sink, final byte @Nullable [] bytes) {
    if (bytes == null) {
      sink.writeInt(-1);
    } else {
      sink.writeInt(bytes.length);
      if (bytes.length > 0) {
        sink.write(bytes);
      }
    }
  }

  private static byte @Nullable [] readOptionalBytes(final BytesIn<?> source) {
    final int length = source.readInt();
    if (length < 0) {
      return null;
    }
    final byte[] bytes = new byte[length];
    if (length > 0) {
      source.read(bytes, 0, length);
    }
    return bytes;
  }

  /**
   * Read an {@code int} length prefix, treating end-of-stream as {@code -1}.
   * Used for the optional trailing presence-bitmap field that older on-disk
   * records may not contain.
   */
  private static int readOptionalIntLength(final BytesIn<?> source) {
    try {
      return source.readInt();
    } catch (final RuntimeException eof) {
      // Bytes-based backends throw a RuntimeException on underflow rather
      // than a checked EOFException — treat either as "legacy record, field
      // not present" and fall back to the null-equivalent sentinel.
      return -1;
    }
  }
}
