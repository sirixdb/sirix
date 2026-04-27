package io.sirix.index.path.summary;

import org.jspecify.annotations.Nullable;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/**
 * Off-page per-{@link io.sirix.index.path.summary.PathNode path-node} statistics
 * holder.
 *
 * <p>Lifted out of {@link PathNode} so the flyweight {@link PathNode} can stay narrow
 * and slot-fixed-width on the slotted page. Variable-length blobs (HLL sketch,
 * presence bitmap, min/max byte buffers) live here, keyed by {@code pathNodeKey}
 * inside {@link PathStatsRegistry} on the owning {@link io.sirix.page.PathSummaryPage}.
 *
 * <p>All fields are read/written by {@link io.sirix.index.path.summary.PathSummaryWriter}
 * at commit time and consumed by the vectorized executor's aggregate-short-circuit
 * fast paths at query time.
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
   * Decoded HLL byte length from the on-disk format. {@code -1} sentinel means absent.
   */
  static byte @Nullable [] readOptionalBytes(final ByteBuffer buf) {
    final int len = buf.getInt();
    if (len < 0) {
      return null;
    }
    final byte[] out = new byte[len];
    buf.get(out);
    return out;
  }

  static void writeOptionalBytes(final ByteBuffer buf, final byte @Nullable [] bytes) {
    if (bytes == null) {
      buf.putInt(-1);
    } else {
      buf.putInt(bytes.length);
      buf.put(bytes);
    }
  }

  /**
   * Serialize this stats record into a fresh {@code byte[]}. Format mirrors the
   * legacy inline encoding in {@code NodeKind.PATH.serialize} so on-disk layout
   * is preserved across the move from inline-on-PathNode to off-page registry.
   */
  public byte[] toBytes() {
    final byte[] minBytesLocal = minBytes;
    final byte[] maxBytesLocal = maxBytes;
    final byte[] hllSerialized = hll == null ? null : hll.serialize();
    byte[] bitmapSerialized = null;
    if (pageKeys != null) {
      pageKeys.runOptimize();
      final ByteArrayOutputStream baos = new ByteArrayOutputStream(
          Math.max(16, pageKeys.serializedSizeInBytes()));
      try {
        pageKeys.serialize(new DataOutputStream(baos));
      } catch (final IOException e) {
        throw new UncheckedIOException("PathStats pageKeys serialize failed", e);
      }
      bitmapSerialized = baos.toByteArray();
    }
    final int sz = 8 + 8 + 8 + 8 + 8                                 // 5 longs
        + 4 + (minBytesLocal == null ? 0 : minBytesLocal.length)     // minBytes
        + 4 + (maxBytesLocal == null ? 0 : maxBytesLocal.length)     // maxBytes
        + 4 + (hllSerialized == null ? 0 : hllSerialized.length)     // hll
        + 1 + 1                                                       // minDirty + maxDirty
        + 4 + (bitmapSerialized == null ? 0 : bitmapSerialized.length); // pageKeys
    final ByteBuffer buf = ByteBuffer.allocate(sz);
    buf.putLong(count);
    buf.putLong(nullCount);
    buf.putLong(sum);
    buf.putLong(min);
    buf.putLong(max);
    writeOptionalBytes(buf, minBytesLocal);
    writeOptionalBytes(buf, maxBytesLocal);
    writeOptionalBytes(buf, hllSerialized);
    buf.put((byte) (minDirty ? 1 : 0));
    buf.put((byte) (maxDirty ? 1 : 0));
    writeOptionalBytes(buf, bitmapSerialized);
    return buf.array();
  }

  public static PathStats fromBytes(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final PathStats s = new PathStats();
    s.count = buf.getLong();
    s.nullCount = buf.getLong();
    s.sum = buf.getLong();
    s.min = buf.getLong();
    s.max = buf.getLong();
    s.minBytes = readOptionalBytes(buf);
    s.maxBytes = readOptionalBytes(buf);
    final byte[] hllBytes = readOptionalBytes(buf);
    if (hllBytes != null) {
      s.hll = HyperLogLogSketch.deserialize(hllBytes);
    }
    s.minDirty = buf.get() != 0;
    s.maxDirty = buf.get() != 0;
    final byte[] bitmapBytes = readOptionalBytes(buf);
    if (bitmapBytes != null) {
      final RoaringBitmap bm = new RoaringBitmap();
      try {
        bm.deserialize(new DataInputStream(new ByteArrayInputStream(bitmapBytes)));
      } catch (final IOException e) {
        throw new UncheckedIOException("PathStats pageKeys deserialize failed", e);
      }
      s.pageKeys = bm;
    }
    return s;
  }
}
