/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * Segmented persistence codec for projection leaves
 * (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3): splits one raw leaf into
 * <em>semantic segments</em> — the record-key column, one body per column
 * (presence + encoded values, flags at the head), and one dictionary per
 * string column — each destined for its own CoW-versioned
 * {@code ProjectionSegmentPage}, addressed from a {@link LeafDescriptor} slot
 * value. Per-segment encodings are byte-compatible with
 * {@link ProjectionIndexLeafCodec}'s compact streams (same delta/FOR record
 * keys, FOR bit-packed numerics, packed dict-ids, marker-byte presence — the
 * primitives are shared), regrouped so that
 *
 * <ul>
 *   <li>a query reading column {@code c} fetches {@code BODY(c)} (+
 *       {@code DICT(c)} for string predicates) and nothing else;</li>
 *   <li>a single-column in-place update re-encodes one body segment, and the
 *       byte-shift cascade of the monolithic form is contained inside that
 *       column by construction;</li>
 *   <li>{@link #assembleRaw} reconstructs the raw scan form
 *       <b>byte-identically</b> (same guarantee as
 *       {@link ProjectionIndexLeafCodec#decode}), presence, unrepresentable
 *       and integrality provenance included.</li>
 * </ul>
 *
 * <h2>Segment id scheme</h2>
 *
 * {@code 0 = KEYS}, {@code 3c+1 = BODY(c)}, {@code 3c+2 = DICT(c)} — capped
 * at {@link LeafDescriptor#MAX_COLUMNS} columns by the 8-bit id space of the
 * HOT side-map composite key.
 *
 * <h2>Per-segment wire form</h2>
 *
 * Every segment is self-describing: {@code int "PIXS"; byte version; byte
 * segKind}, then the kind-specific payload:
 *
 * <pre>
 *   KEYS:  long firstRecordKey; long lastRecordKey;
 *          [rowCount &gt; 0] byte mode; long base; byte width; packed keys
 *   BODY:  byte colFlags;                       // provenance TRUTH (5.1-7)
 *          [rowCount &gt; 0] long min; long max;   // zone-map truth
 *          presence marker byte [+ words];
 *          NUMERIC: long base; byte width; packed values
 *          BOOLEAN: words verbatim
 *          STRING:  byte idWidth; packed dict-ids
 *   DICT:  int dictSize; int[dictSize] lens; concatenated UTF-8
 * </pre>
 *
 * <p>An empty leaf ({@code rowCount == 0}) still emits KEYS (fences) and one
 * BODY per column (flag truth) — no DICT segments; the descriptor stays the
 * bounds authority.
 *
 * <p>Integrity: {@link #assembleRaw} verifies every segment's exact
 * {@code byteLen} and XXH3-64 {@code contentHash} against the descriptor
 * before parsing — segment pages persist behind bare offset keys, so the
 * descriptor hash is their only checksum. A mismatch throws (callers fail
 * soft to the generic pipeline and negative-cache, §4). The hash doubles as
 * the maintenance write-path no-op comparator (§3).
 */
public final class ProjectionIndexSegmentCodec {

  /** Leading magic of every segment ("PIXS" little-endian). */
  public static final int SEGMENT_MAGIC = 0x53584950;

  /** Segment layout version; bumped on any wire change. */
  public static final byte SEGMENT_VERSION = 1;

  /** Segment kind tags. */
  public static final byte SEG_KIND_KEYS = 0;
  public static final byte SEG_KIND_BODY = 1;
  public static final byte SEG_KIND_DICT = 2;

  /** Fixed per-segment header size: magic + version + segKind. */
  public static final int SEGMENT_HEADER_BYTES = 6;

  /** XXH3-64 for descriptor content hashes (zero-allocation, shared instance). */
  private static final LongHashFunction XX3 = LongHashFunction.xx3();

  private ProjectionIndexSegmentCodec() {
  }

  /** Segment id of the record-key segment. */
  public static int keysSegmentId() {
    return 0;
  }

  /** Segment id of column {@code c}'s body segment. */
  public static int bodySegmentId(final int column) {
    return 3 * column + 1;
  }

  /** Segment id of column {@code c}'s dictionary segment (STRING_DICT columns only). */
  public static int dictSegmentId(final int column) {
    return 3 * column + 2;
  }

  /** Column index owning segment id {@code segmentId} (BODY/DICT), or -1 for KEYS. */
  public static int columnOfSegment(final int segmentId) {
    return segmentId == 0 ? -1 : (segmentId - 1) / 3;
  }

  /** XXH3-64 content hash as stored in descriptor entries. */
  public static long contentHash(final byte[] segment) {
    return XX3.hashBytes(segment);
  }

  /**
   * One encoded leaf: the descriptor plus parallel arrays of segment ids and segment bytes
   * (ascending id order — KEYS, then per column BODY [, DICT]).
   */
  public record EncodedLeaf(byte[] descriptor, byte[] segmentIds, byte[][] segments) {
  }

  /** Resolves a segment's bytes by id — the storage layer's read hook. */
  @FunctionalInterface
  public interface SegmentResolver {
    byte @Nullable [] segment(int segmentId);
  }

  // ==================== encode ====================

  /**
   * Encode a raw leaf payload into descriptor + segments.
   *
   * @throws IllegalStateException when {@code rawPayload} is not a valid raw leaf
   *         (propagated from {@link ProjectionIndexLeafPage#deserialize})
   */
  public static EncodedLeaf encode(final byte[] rawPayload) {
    final ProjectionIndexLeafPage page = ProjectionIndexLeafPage.deserialize(rawPayload);
    final int rowCount = page.getRowCount();
    final int columnCount = page.getColumnCount();
    if (columnCount > LeafDescriptor.MAX_COLUMNS) {
      throw new IllegalStateException("columnCount " + columnCount + " exceeds MAX_COLUMNS="
          + LeafDescriptor.MAX_COLUMNS);
    }

    final int maxSegments = 1 + 2 * columnCount;
    final byte[] segIds = new byte[maxSegments];
    final byte[][] segments = new byte[maxSegments][];
    final byte[] entryFlags = new byte[maxSegments];
    final long[] entryMins = new long[maxSegments];
    final long[] entryMaxs = new long[maxSegments];
    int segCount = 0;

    // KEYS segment.
    {
      final ByteArrayOutputStream out = newSegmentStream(SEG_KIND_KEYS);
      ProjectionIndexLeafCodec.putLongLE(out, page.firstRecordKey());
      ProjectionIndexLeafCodec.putLongLE(out, page.lastRecordKey());
      if (rowCount > 0) {
        ProjectionIndexLeafCodec.encodeRecordKeys(out, page.recordKeys(), rowCount);
      }
      segIds[segCount] = (byte) keysSegmentId();
      segments[segCount] = out.toByteArray();
      segCount++;
    }

    final byte[] kinds = new byte[columnCount];
    for (int c = 0; c < columnCount; c++) {
      kinds[c] = page.columnKind(c);

      byte flags = page.columnUnrepresentable(c) ? ProjectionIndexLeafPage.COLUMN_FLAG_UNREPRESENTABLE : 0;
      if (page.columnNumericNonIntegral(c)) {
        flags |= ProjectionIndexLeafPage.COLUMN_FLAG_NON_INTEGRAL;
      }

      // BODY segment.
      final ByteArrayOutputStream body = newSegmentStream(SEG_KIND_BODY);
      body.write(flags);
      if (rowCount > 0) {
        ProjectionIndexLeafCodec.putLongLE(body, page.columnMin(c));
        ProjectionIndexLeafCodec.putLongLE(body, page.columnMax(c));
        ProjectionIndexLeafCodec.encodePresence(body, page.presenceColumnBits(c), rowCount);
        switch (kinds[c]) {
          case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG ->
              ProjectionIndexLeafCodec.encodeForBitPacked(body, page.numericColumn(c), rowCount);
          case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> {
            final long[] bits = page.booleanColumnBits(c);
            final int words = (rowCount + 63) >>> 6;
            for (int w = 0; w < words; w++) {
              ProjectionIndexLeafCodec.putLongLE(body, bits[w]);
            }
          }
          case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT ->
              encodeDictIds(body, page.stringDictionary(c), page.stringDictIdColumn(c), rowCount);
          default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
        }
      }
      segIds[segCount] = (byte) bodySegmentId(c);
      segments[segCount] = body.toByteArray();
      entryFlags[segCount] = flags;
      entryMins[segCount] = rowCount > 0 ? page.columnMin(c) : 0L;
      entryMaxs[segCount] = rowCount > 0 ? page.columnMax(c) : 0L;
      segCount++;

      // DICT segment (string columns with rows only).
      if (kinds[c] == ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT && rowCount > 0) {
        final ByteArrayOutputStream dict = newSegmentStream(SEG_KIND_DICT);
        encodeDictEntries(dict, page.stringDictionary(c));
        segIds[segCount] = (byte) dictSegmentId(c);
        segments[segCount] = dict.toByteArray();
        segCount++;
      }
    }

    final int[] byteLens = new int[segCount];
    final long[] hashes = new long[segCount];
    for (int i = 0; i < segCount; i++) {
      byteLens[i] = segments[i].length;
      hashes[i] = contentHash(segments[i]);
    }
    final byte[] descriptor = LeafDescriptor.serialize(rowCount, page.firstRecordKey(), page.lastRecordKey(),
        kinds, segCount, segIds, byteLens, hashes, entryFlags, entryMins, entryMaxs);

    final byte[] idsTrimmed = new byte[segCount];
    System.arraycopy(segIds, 0, idsTrimmed, 0, segCount);
    final byte[][] segsTrimmed = new byte[segCount][];
    System.arraycopy(segments, 0, segsTrimmed, 0, segCount);
    return new EncodedLeaf(descriptor, idsTrimmed, segsTrimmed);
  }

  private static ByteArrayOutputStream newSegmentStream(final byte segKind) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream(256);
    ProjectionIndexLeafCodec.putIntLE(out, SEGMENT_MAGIC);
    out.write(SEGMENT_VERSION);
    out.write(segKind);
    return out;
  }

  /** Dict-id stream only (the dictionary itself lives in the DICT segment). */
  private static void encodeDictIds(final ByteArrayOutputStream out, final byte[][] dict, final int[] ids,
      final int rowCount) {
    final int dictSize = dictSizeOf(dict);
    final int width = dictSize <= 1 ? 0 : ProjectionIndexLeafCodec.widthOf(dictSize - 1L);
    out.write(width);
    if (width > 0) {
      final ProjectionIndexLeafCodec.BitWriter bw = new ProjectionIndexLeafCodec.BitWriter(out);
      for (int i = 0; i < rowCount; i++) {
        bw.write(ids[i], width);
      }
      bw.flush();
    }
  }

  private static void encodeDictEntries(final ByteArrayOutputStream out, final byte[][] dict) {
    final int dictSize = dictSizeOf(dict);
    ProjectionIndexLeafCodec.putIntLE(out, dictSize);
    for (int i = 0; i < dictSize; i++) {
      ProjectionIndexLeafCodec.putIntLE(out, dict[i].length);
    }
    for (int i = 0; i < dictSize; i++) {
      out.write(dict[i], 0, dict[i].length);
    }
  }

  private static int dictSizeOf(final byte[][] dict) {
    int dictSize = 0;
    while (dictSize < dict.length && dict[dictSize] != null) {
      dictSize++;
    }
    return dictSize;
  }

  // ==================== assemble (decode) ====================

  /**
   * Reassemble the raw scan form from a descriptor and its segments, byte-identically to the
   * original {@link ProjectionIndexLeafPage#serialize()} output. Verifies each segment's
   * byteLen + contentHash against the descriptor before parsing.
   *
   * @throws IllegalStateException on any missing segment, length/hash mismatch, or malformed
   *         segment bytes — corruption is caught here, at fill time, never mid-kernel
   */
  public static byte[] assembleRaw(final byte[] descriptor, final SegmentResolver resolver) {
    LeafDescriptor.validate(descriptor);
    final int rowCount = LeafDescriptor.rowCount(descriptor);
    final int columnCount = LeafDescriptor.columnCount(descriptor);
    final byte[] kinds = new byte[columnCount];
    for (int c = 0; c < columnCount; c++) {
      kinds[c] = LeafDescriptor.kind(descriptor, c);
    }

    // KEYS.
    final ProjectionIndexLeafCodec.Cursor keys =
        openSegment(descriptor, resolver, keysSegmentId(), SEG_KIND_KEYS);
    final long firstRecordKey = keys.readLong();
    final long lastRecordKey = keys.readLong();
    final long[] recordKeys =
        rowCount > 0 ? ProjectionIndexLeafCodec.decodeRecordKeys(keys, rowCount) : new long[0];

    final long[] columnMin = new long[columnCount];
    final long[] columnMax = new long[columnCount];
    final long[][] numericCols = new long[columnCount][];
    final long[][] booleanCols = new long[columnCount][];
    final int[][] dictIdCols = new int[columnCount][];
    final byte[][][] dicts = new byte[columnCount][][];
    final boolean[] unrep = new boolean[columnCount];
    final boolean[] nonIntegral = new boolean[columnCount];
    final long[][] presence = new long[columnCount][];
    final int presWords = rowCount > 0 ? (rowCount + 63) >>> 6 : 0;

    for (int c = 0; c < columnCount; c++) {
      final ProjectionIndexLeafCodec.Cursor body =
          openSegment(descriptor, resolver, bodySegmentId(c), SEG_KIND_BODY);
      final byte flags = body.readByte();
      unrep[c] = (flags & ProjectionIndexLeafPage.COLUMN_FLAG_UNREPRESENTABLE) != 0;
      nonIntegral[c] = (flags & ProjectionIndexLeafPage.COLUMN_FLAG_NON_INTEGRAL) != 0;

      final long[] bits = new long[Math.max(presWords, (ProjectionIndexLeafPage.MAX_ROWS + 63) >>> 6)];
      presence[c] = bits;
      if (rowCount == 0) {
        continue;
      }
      columnMin[c] = body.readLong();
      columnMax[c] = body.readLong();
      decodePresence(body, bits, presWords, rowCount);
      switch (kinds[c]) {
        case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG -> {
          final long base = body.readLong();
          final int width = body.readByte() & 0xFF;
          final long[] values = new long[rowCount];
          if (width == 0) {
            Arrays.fill(values, base);
          } else {
            final ProjectionIndexLeafCodec.BitReader br = new ProjectionIndexLeafCodec.BitReader(body);
            for (int i = 0; i < rowCount; i++) {
              values[i] = base + br.read(width);
            }
            br.align();
          }
          numericCols[c] = values;
        }
        case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> {
          final long[] boolBits = new long[presWords];
          for (int w = 0; w < presWords; w++) {
            boolBits[w] = body.readLong();
          }
          booleanCols[c] = boolBits;
        }
        case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> {
          final ProjectionIndexLeafCodec.Cursor dictCur =
              openSegment(descriptor, resolver, dictSegmentId(c), SEG_KIND_DICT);
          final int dictSize = dictCur.readInt();
          final int[] lens = new int[dictSize];
          for (int i = 0; i < dictSize; i++) {
            lens[i] = dictCur.readInt();
          }
          final byte[][] dict = new byte[Math.max(16, dictSize)][];
          for (int i = 0; i < dictSize; i++) {
            dict[i] = dictCur.readBytes(lens[i]);
          }
          dicts[c] = dict;
          final int width = body.readByte() & 0xFF;
          final int[] ids = new int[rowCount];
          if (width > 0) {
            final ProjectionIndexLeafCodec.BitReader br = new ProjectionIndexLeafCodec.BitReader(body);
            for (int i = 0; i < rowCount; i++) {
              ids[i] = (int) br.read(width);
            }
            br.align();
          }
          dictIdCols[c] = ids;
        }
        default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
      }
    }

    final ProjectionIndexLeafPage page = ProjectionIndexLeafPage.reconstruct(kinds, rowCount,
        firstRecordKey, lastRecordKey, recordKeys, columnMin, columnMax,
        numericCols, booleanCols, dictIdCols, dicts, presence, unrep, nonIntegral);
    return page.serialize();
  }

  /**
   * Column-scoped provenance primitive (5.1-7): the flags byte from a BODY segment's bytes —
   * segment TRUTH, as opposed to the descriptor's mirror. Validates the segment header.
   */
  public static byte bodySegmentFlags(final byte[] bodySegment) {
    checkSegmentHeader(bodySegment, SEG_KIND_BODY);
    return bodySegment[SEGMENT_HEADER_BYTES];
  }

  // ==================== internals ====================

  /**
   * Resolve, verify (length + hash against the descriptor), and open a segment positioned
   * after its header.
   */
  private static ProjectionIndexLeafCodec.Cursor openSegment(final byte[] descriptor,
      final SegmentResolver resolver, final int segmentId, final byte expectedKind) {
    final int entry = LeafDescriptor.entryIndexOf(descriptor, segmentId);
    if (entry < 0) {
      throw new IllegalStateException("Missing descriptor entry for segmentId=" + segmentId);
    }
    final byte[] segment = resolver.segment(segmentId);
    if (segment == null) {
      throw new IllegalStateException("Missing segment bytes for segmentId=" + segmentId
          + " (descriptor lists " + LeafDescriptor.entryByteLen(descriptor, entry) + " bytes)");
    }
    if (segment.length != LeafDescriptor.entryByteLen(descriptor, entry)) {
      throw new IllegalStateException("Segment length mismatch for segmentId=" + segmentId + ": "
          + segment.length + " != descriptor " + LeafDescriptor.entryByteLen(descriptor, entry));
    }
    if (contentHash(segment) != LeafDescriptor.entryContentHash(descriptor, entry)) {
      throw new IllegalStateException("Segment content-hash mismatch for segmentId=" + segmentId
          + " — corrupted segment page or dangling side-map reference");
    }
    checkSegmentHeader(segment, expectedKind);
    return new ProjectionIndexLeafCodec.Cursor(segment, SEGMENT_HEADER_BYTES);
  }

  private static void checkSegmentHeader(final byte[] segment, final byte expectedKind) {
    if (segment.length < SEGMENT_HEADER_BYTES
        || ProjectionIndexLeafCodec.getIntLE(segment, 0) != SEGMENT_MAGIC) {
      throw new IllegalStateException("Not a projection segment (missing PIXS magic)");
    }
    if (segment[4] != SEGMENT_VERSION) {
      throw new IllegalStateException("Unknown segment version " + segment[4] + " (expected "
          + SEGMENT_VERSION + ")");
    }
    if (segment[5] != expectedKind) {
      throw new IllegalStateException("Segment kind mismatch: " + segment[5] + " != expected " + expectedKind);
    }
  }

  private static void decodePresence(final ProjectionIndexLeafCodec.Cursor in, final long[] bits,
      final int presWords, final int rowCount) {
    final int mode = in.readByte() & 0xFF;
    switch (mode) {
      case 0 -> {
        for (int w = 0; w < presWords; w++) {
          bits[w] = ProjectionIndexLeafCodec.expectedFullWord(w, presWords, rowCount);
        }
      }
      case 1 -> { /* all-missing — words stay zero */ }
      case 2 -> {
        for (int w = 0; w < presWords; w++) {
          bits[w] = in.readLong();
        }
      }
      default -> throw new IllegalStateException("Bad presence marker " + mode);
    }
  }
}
