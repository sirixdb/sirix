package io.sirix.index.path.summary;

import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
  public @Nullable RoaringBitmap pageKeys;

  public PathStats() {
  }

  public boolean isEmpty() {
    return count == 0L && nullCount == 0L && sum == 0L && min == EMPTY_MIN && max == EMPTY_MAX
        && minBytes == null && maxBytes == null && hll == null
        && !minDirty && !maxDirty && pageKeys == null;
  }

  /**
   * Serialize this record to {@code sink}. Mirrors the legacy inline encoding
   * previously embedded in {@link io.sirix.node.NodeKind#PATH} so the on-disk
   * layout is preserved byte-for-byte.
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
    final RoaringBitmap pageKeysRef = pageKeys;
    if (pageKeysRef == null) {
      sink.writeInt(-1);
    } else {
      pageKeysRef.runOptimize();
      final ByteArrayOutputStream baos =
          new ByteArrayOutputStream(Math.max(16, pageKeysRef.serializedSizeInBytes()));
      try {
        pageKeysRef.serialize(new DataOutputStream(baos));
      } catch (final IOException e) {
        throw new UncheckedIOException("PathStats pageKeys serialize failed", e);
      }
      final byte[] bmBytes = baos.toByteArray();
      sink.writeInt(bmBytes.length);
      sink.write(bmBytes);
    }
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
   * Tolerant of legacy on-disk records that stop before the optional trailing
   * presence-bitmap field.
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
