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
 * Per-{@link PathNode} value statistics: lazily-allocated value-object held directly on the owning
 * {@link PathNode}, allocated only on the first recorded observation. Persisted inline as the
 * trailing block of the {@link io.sirix.node.NodeKind#PATH} record serialization when the resource
 * is configured with {@code withPathStatistics == true}.
 *
 * <p>
 * Lifted out of {@link PathNode}'s field set so the node carries a single nullable reference (8 B)
 * instead of 11 always-present primitives + lazy heap blobs. Empty-state PathNodes that never see
 * an analytical query pay only that one reference.
 *
 * <p>
 * Read/written by {@link PathSummaryWriter} at commit time; read by the vectorized executor's
 * aggregate-short-circuit fast paths at query time.
 */
public final class PathStats {

  public static final long EMPTY_MIN = Long.MAX_VALUE;
  public static final long EMPTY_MAX = Long.MIN_VALUE;

  public long count;
  public long nullCount;
  /**
   * Low (unsigned) half of the 128-bit integral accumulator, and the exact {@code long} total
   * whenever {@link #sumFitsLong()} holds.
   */
  public long sum;
  /**
   * High (signed) half of the 128-bit integral accumulator.
   *
   * <p>
   * The persisted accumulator is 128 bits wide so that what it holds is a function of the OBSERVED
   * VALUES and nothing else — not of how many flushes the ingestion happened to take, nor of how a
   * bulk load happened to chunk the input. A 64-bit accumulator that drops an overflowing addend
   * answers {@code [M, M, -M]} one way when a flush lands after the second value and another way when
   * it does not, so the same document loaded two supported ways would persist different statistics
   * and decline in different places. Here every intermediate is exact and {@link #sumFitsLong()} asks
   * the only question a reader cares about: is the TRUE total representable as the {@code long} that
   * {@code sum}/{@code avg} would be served from?
   */
  public long sumHi;
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
   * <p>
   * {@link #sum} holds the integral part and this the rest, so a column of {@code 17.125} sums to
   * {@code 17 + 0.125} rather than to {@code 17}. Numbers used to reach the statistics through
   * {@code Number.longValue()}, which truncates on the way IN — the loss happened before anything
   * could detect it, and a sum served from the summary was silently wrong for every non-integral
   * column.
   *
   * <p>
   * Maintained but never SERVED: any non-integral observation also sets {@link #doubleTyped}, and the
   * reader declines every value aggregate on a double-typed column — deliberately, because a double
   * sum depends on the order the values were added, so the summary's answer and the scan's would
   * differ even when both are arithmetically defensible. This exists so the accumulator is not
   * silently wrong if that policy is ever relaxed, not because anything reads it today.
   */
  public double sumFraction;

  /**
   * Set when the sum can no longer be reproduced exactly, after which readers must not serve
   * {@code sum}/{@code avg} from these statistics.
   *
   * <p>
   * Needed because floating-point addition is not invertible: subtracting a double on delete does not
   * restore the value the accumulator held before it was added, so a maintained double sum drifts
   * away from the true one. Same reasoning as {@link #minDirty} — the honest move is to record that
   * the aggregate is no longer trustworthy and let the query fall back to the scan.
   */
  public boolean sumDirty;

  /**
   * Set once any observation arrived as a floating-point value, whatever its magnitude.
   *
   * <p>
   * Stickiness is the point: {@code 1.0E16} is integral, so folding it into the long accumulator
   * looks lossless — but the interpreter sums that column with double arithmetic and rounds at every
   * step, landing on {@code 1.0E16} where an exact integer sum lands on {@code 10000000000000002}.
   * The summary is not entitled to be "more accurate" than the answer it is standing in for, so a
   * column that was ever floating-point declines value aggregates.
   */
  public boolean doubleTyped;

  /**
   * Set when the value COUNT can no longer be trusted, after which nothing may be served from these
   * statistics.
   *
   * <p>
   * Moving a subtree re-parents records without re-attributing their observations: the summary adapts
   * its structure, but the values stay counted under the path they left. Recomputing the affected
   * paths would mean walking the moved subtree, so the statistics instead record that they are stale
   * and let queries fall back to the scan -- the same bargain {@link #minDirty} already makes for the
   * bounds.
   */
  public boolean countDirty;

  /**
   * Leaf pages on which this path was observed; {@code null} until the first merge.
   *
   * <p>
   * <b>Guarded by {@code this}.</b> Every read, write and serialization of this bitmap holds the
   * {@code PathStats} monitor, and the field is private so that discipline cannot be bypassed from
   * outside. The async snapshot flush genuinely shares it across threads:
   * {@code KeyValueLeafPage.deepCopy()} copies the {@code records[]} ARRAY but not the records, so
   * the serialization copy and the live page hold the SAME {@link PathNode}, and the background
   * snapshot-append thread serializes this bitmap while the ingest thread keeps merging page keys
   * into it.
   *
   * <p>
   * {@link RoaringBitmap} tolerates neither half of that. {@code runOptimize()} REWRITES the
   * container in place: it sizes a run container from a run count and then re-walks the array to fill
   * it, so a concurrent {@code add} that creates a run overruns the fill
   * ({@code ArrayIndexOutOfBoundsException: Index N out of bounds for length N}) while one that
   * merges two runs silently under-fills it, leaving a container whose declared run count exceeds
   * what was written. Independently of {@code runOptimize}, {@code serializedSizeInBytes()} and
   * {@code serialize()} are two separate walks, so an add between them makes the length prefix
   * disagree with the bytes that follow and corrupts the record for every later reader.
   */
  private @Nullable RoaringBitmap pageKeys;

  /**
   * Record version written as the leading marker of every {@link #writeTo(BytesOut)} record.
   *
   * <p>
   * V1 widened the integral accumulator to 128 bits by inserting {@link #sumHi} after {@link #sum};
   * see {@code docs/DISK_FORMAT.md}. This is the per-record version mechanism the format contract
   * names for a record that has to evolve independently of the page-wide
   * {@code BinaryEncodingVersion}.
   */
  public static final int RECORD_VERSION = 1;

  /**
   * Leading discriminator: {@code -RECORD_VERSION}, occupying the slot a V0 record used for
   * {@code count}.
   *
   * <p>
   * A V0 record carries no version field, so it has to be recognised by shape. {@code count} is the
   * first field it writes and is never negative — every decrement is guarded by {@code count > 0} and
   * every merge folds in a non-negative batch count — so a negative leading long cannot be a V0
   * record and unambiguously marks a versioned one.
   *
   * <p>
   * <b>Accepted limitation, deliberately not closed:</b> the marker versions this RECORD while the
   * page-wide {@code BinaryEncodingVersion} stays V0, so the break is one-directional. This build
   * reads a V0 record correctly and refuses a newer-than-known marker with a diagnostic, but an OLDER
   * build opening a resource written by this one WITH path statistics has no version byte to fail on:
   * it reads the marker as {@code count = -1} and every field after it shifted by eight bytes.
   * SirixDB has no users to downgrade, so that is an accepted risk rather than a defect — recorded
   * here and in {@code docs/DISK_FORMAT.md} so it stays a decision rather than a surprise.
   */
  private static final long VERSION_MARKER = -RECORD_VERSION;

  public PathStats() {}

  /**
   * Whether the exact integral total is representable as a {@code long} — the high half is nothing
   * but the sign extension of the low half.
   */
  public boolean sumFitsLong() {
    return sumFitsLong(sumHi, sum);
  }

  /** Whether the 128-bit value {@code (hi, lo)} is representable as a signed {@code long}. */
  public static boolean sumFitsLong(final long hi, final long lo) {
    return hi == (lo >> 63);
  }

  /**
   * High half of {@code (hi, lo) + (addHi, addLo)}, given the already-computed low half
   * {@code resultLo = lo + addLo}.
   *
   * <p>
   * The single source of truth for the 128-bit carry rule, shared with {@link PathStatsAccumulator}
   * so the batch accumulator and the persisted one cannot drift. Two adds and an unsigned compare —
   * no allocation, no boxing, nothing that must not sit on a per-value ingestion path.
   */
  public static long addCarry(final long hi, final long addHi, final long lo, final long resultLo) {
    return hi + addHi + (Long.compareUnsigned(resultLo, lo) < 0
        ? 1L
        : 0L);
  }

  /**
   * High half of {@code (hi, lo) - (subHi, subLo)}, borrowing out of the low half.
   *
   * <p>
   * A subtraction primitive rather than "negate and add", because on two's-complement 64-bit integers
   * negation is not total: {@code -Long.MIN_VALUE} is {@code Long.MIN_VALUE}, so cancelling an
   * observation of that value by adding its negation ADDS it a second time. That is not a corner that
   * can be waved off now that representability is derived from the exact total — the doubled value
   * can land back inside {@code long} range, where nothing marks it and the wrong sum is served.
   * Subtracting the sign-extended pair directly has no such hole.
   */
  public static long subBorrow(final long hi, final long subHi, final long lo, final long subLo) {
    return hi - subHi - (Long.compareUnsigned(lo, subLo) < 0
        ? 1L
        : 0L);
  }

  public synchronized boolean isEmpty() {
    return countAndSumLanesEmpty() && boundLanesEmpty() && sketchAndTrailerEmpty();
  }

  /**
   * The counting and summing lanes, including the 128-bit sum's high word and the flags that mark
   * either untrustworthy. Callers hold this record's monitor (see {@link #isEmpty()}), so the split
   * helpers are deliberately unsynchronized rather than redundantly re-entering it.
   */
  private boolean countAndSumLanesEmpty() {
    return count == 0L && nullCount == 0L && sum == 0L && sumHi == 0L && sumFraction == 0.0d && !sumDirty
        && !doubleTyped && !countDirty;
  }

  /** The numeric and byte-valued bounds, with their dirty flags. */
  private boolean boundLanesEmpty() {
    return min == EMPTY_MIN && max == EMPTY_MAX && minBytes == null && maxBytes == null && !minDirty && !maxDirty;
  }

  /** The distinct-value sketch and the optional page-key trailer. */
  private boolean sketchAndTrailerEmpty() {
    return hll == null && pageKeys == null;
  }

  /**
   * Serialize this record to {@code sink}. Mirrors the legacy inline encoding previously embedded in
   * {@link io.sirix.node.NodeKind#PATH}.
   *
   * <p>
   * V1 layout: {@code [i64 -RECORD_VERSION][i64 count][i64 nullCount][i64 sum][i64 sumHi][i64 min]
   * [i64 max]} then the byte bounds, the HLL blob, the dirty flags, {@code sumFraction}, the three
   * trailing flags and the optional page-key trailer. {@code sumFraction} and the flags sit between
   * {@code maxDirty} and that trailer; any rearrangement needs another {@link #RECORD_VERSION} bump
   * before resources using it are written.
   */
  public void writeTo(final BytesOut<?> sink) {
    sink.writeLong(VERSION_MARKER);
    sink.writeLong(count);
    sink.writeLong(nullCount);
    sink.writeLong(sum);
    sink.writeLong(sumHi);
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
   * <p>
   * Nothing is copied: the flush path stays allocation-free -- a defensive clone here would put an
   * unbounded, bitmap-sized allocation on every snapshot flush -- and the monitor is held only for
   * this one trailer, so the ingest thread can at worst wait out a single bitmap's serialization.
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
   * Merge every key {@code keys} yields into the presence bitmap, allocating it on first use. Holds
   * the monitor for the whole batch so a flush observes the batch as a unit.
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
    return bitmap == null
        ? null
        : bitmap.toArray();
  }

  /**
   * Convenience: writes either the supplied non-null stats or an empty-state trailer when
   * {@code stats == null}. Avoids allocating a throwaway empty {@link PathStats} on the hot serialize
   * path for nodes that never recorded a value.
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
   * <p>
   * Reads V1 and MIGRATES V0 in place: a V0 record has no {@link #sumHi}, but its {@code sum} is by
   * construction a value that fitted a {@code long}, so sign-extending it reconstructs the exact
   * 128-bit accumulator. A V0 record whose true total had left {@code long} range carries a partial
   * sum with {@code sumDirty} already set, and {@code sumDirty} is never cleared, so the migrated
   * record stays exactly as unservable as it was — nothing is silently promoted to trusted.
   *
   * <p>
   * Tolerates a record that stops before the optional trailing presence-bitmap field, which is the
   * only shape variation within a version.
   */
  public static PathStats readFrom(final BytesIn<?> source) {
    final PathStats s = new PathStats();
    final long leading = source.readLong();
    if (leading < 0L) {
      if (leading != VERSION_MARKER) {
        throw new IllegalStateException("Unknown PathStats record version " + (-leading)
            + " — written by a newer SirixDB; this build understands version " + RECORD_VERSION);
      }
      s.count = source.readLong();
      s.nullCount = source.readLong();
      s.sum = source.readLong();
      s.sumHi = source.readLong();
    } else {
      s.count = leading;
      s.nullCount = source.readLong();
      s.sum = source.readLong();
      s.sumHi = s.sum >> 63;
    }
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
   * Convenience: reads a record from {@code source} and returns {@code null} if the parsed stats are
   * in the empty default state. Lets the caller keep the lazy-allocation property for nodes whose
   * serialised trailer is effectively empty.
   */
  public static @Nullable PathStats readFromOrNullIfEmpty(final BytesIn<?> source) {
    final PathStats s = readFrom(source);
    return s.isEmpty()
        ? null
        : s;
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
   * Read an {@code int} length prefix, treating end-of-stream as {@code -1}. Used for the optional
   * trailing presence-bitmap field that older on-disk records may not contain.
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
