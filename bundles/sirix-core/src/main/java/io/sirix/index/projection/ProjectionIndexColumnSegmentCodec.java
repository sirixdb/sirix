/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.utils.FSSTCompressor;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Segmented persistence codec for projection leaves
 * (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3): splits one raw leaf into
 * <em>semantic segments</em> — the record-key column, one body per column
 * (presence + encoded values, flags at the head), and one dictionary per
 * string column — each destined for its own CoW-versioned
 * {@link io.sirix.page.OverflowPage} (referenced segments) or the descriptor's own inline region
 * (small segments), addressed from a {@link RowGroupDescriptor} slot
 * value. Per-segment encodings are byte-compatible with
 * {@link ProjectionIndexRowGroupCodec}'s compact streams (same delta/FOR record
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
 *       {@link ProjectionIndexRowGroupCodec#decode}), presence, unrepresentable
 *       and integrality provenance included.</li>
 * </ul>
 *
 * <h2>Segment id scheme</h2>
 *
 * {@code 0 = KEYS}, {@code 3c+1 = BODY(c)}, {@code 3c+2 = DICT(c)} — capped
 * at {@link RowGroupDescriptor#MAX_COLUMNS} columns by the 16-bit id space of the
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
public final class ProjectionIndexColumnSegmentCodec {

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

  /**
   * Hybrid inline policy (docs/PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md §3.3): a segment whose
   * encoded length is {@code <=} {@link #inlineMaxColumnSegmentBytes} is eligible to live inline in
   * the owning {@link RowGroupDescriptor} slot instead of a side-map {@link io.sirix.page.OverflowPage}
   * — the projection analogue of {@link io.sirix.page.KeyValueLeafPage} keeping a small record in
   * the slot heap. {@link #inlineMaxTotalBytes} caps the total inlined per leaf (smallest-first;
   * the rest spill to pages) so the descriptor slot stays small and the HOT trie shallow. Setting
   * {@code inlineMaxColumnSegmentBytes=0} disables inlining → the pre-hybrid all-referenced layout
   * (escape hatch / A-B baseline). Read from the system property once at class load; deterministic
   * for a given value, so identical re-encodes classify identically and the maintenance no-op hash
   * stays stable (5.2-n). Production reads the immutable defaults; the per-thread
   * {@link #INLINE_POLICY} override exists only so tests can exercise both storage classes without
   * a shared-global race (each test thread sees its own value even under parallel execution).
   */
  private static final int DEFAULT_INLINE_MAX_SEGMENT_BYTES =
      Integer.getInteger("sirix.projection.inlineMaxColumnSegmentBytes", 192);

  /** Per-leaf inline budget; see {@link #DEFAULT_INLINE_MAX_SEGMENT_BYTES}. */
  private static final int DEFAULT_INLINE_MAX_TOTAL_BYTES =
      Integer.getInteger("sirix.projection.inlineMaxTotalBytes", 512);

  /**
   * Per-thread inline-policy override ({@code [maxColumnSegmentBytes, maxTotalBytes]}), or {@code null}
   * to use the defaults — a test-only seam ({@link #setInlinePolicyForTesting} /
   * {@link #clearInlinePolicyForTesting}). Thread-local, so a test toggling the policy can never
   * bleed into a concurrently-running encode on another thread. Encoding always runs on the caller
   * thread (single-threaded per write transaction), so the override is seen by the encode it wraps.
   */
  private static final ThreadLocal<int[]> INLINE_POLICY = new ThreadLocal<>();

  /** Test seam: set this thread's inline policy. Prefer a try/finally with {@link #clearInlinePolicyForTesting}. */
  static void setInlinePolicyForTesting(final int maxColumnSegmentBytes, final int maxTotalBytes) {
    INLINE_POLICY.set(new int[] {maxColumnSegmentBytes, maxTotalBytes});
  }

  /** Test seam: restore this thread to the default inline policy. */
  static void clearInlinePolicyForTesting() {
    INLINE_POLICY.remove();
  }

  /** XXH3-64 for descriptor content hashes (zero-allocation, shared instance). */
  private static final LongHashFunction XX3 = LongHashFunction.xx3();

  private ProjectionIndexColumnSegmentCodec() {
  }

  /**
   * Per-column segment slots in the id scheme; {@link RowGroupDescriptor#MAX_COLUMNS} is derived
   * from this and the 16-bit id space so the invariant lives in one place.
   */
  public static final int SEGMENTS_PER_COLUMN = 3;

  /** Segment id of the record-key segment. */
  public static int keysColumnSegmentId() {
    return 0;
  }

  /** Segment id of column {@code c}'s body segment. */
  public static int bodyColumnSegmentId(final int column) {
    return SEGMENTS_PER_COLUMN * checkColumn(column) + 1;
  }

  /** Segment id of column {@code c}'s dictionary segment (STRING_DICT columns only). */
  public static int dictColumnSegmentId(final int column) {
    return SEGMENTS_PER_COLUMN * checkColumn(column) + 2;
  }

  private static int checkColumn(final int column) {
    if (column < 0 || column >= RowGroupDescriptor.MAX_COLUMNS) {
      // Without this check a column past the cap would produce a segment id past the 16-bit sub-id
      // space, colliding with another column's segment — a hash mismatch found only at later assembly.
      throw new IllegalArgumentException("column out of range [0, " + RowGroupDescriptor.MAX_COLUMNS + "): " + column);
    }
    return column;
  }

  /** Column index owning segment id {@code columnSegmentId} (BODY/DICT), or -1 for KEYS. */
  public static int columnOfColumnSegment(final int columnSegmentId) {
    return columnSegmentId == 0 ? -1 : (columnSegmentId - 1) / 3;
  }

  /** XXH3-64 content hash as stored in descriptor entries. */
  public static long contentHash(final byte[] segment) {
    return XX3.hashBytes(segment);
  }

  /**
   * One encoded leaf: the descriptor plus parallel arrays of segment ids and segment bytes
   * (ascending id order — KEYS, then per column BODY [, DICT]).
   *
   * <p><b>Aliasing contract (HFT, no defensive copies):</b> the accessors expose the codec's
   * internal arrays. The descriptor embeds each segment's content hash at encode time, so
   * mutating any returned array de-synchronises bytes from their recorded hash and poisons
   * every later assembly with a spurious corruption error. Treat all three as immutable.
   */
  public record EncodedRowGroup(byte[] descriptor, int[] columnSegmentIds, byte[][] segments) {
  }

  /** Resolves a segment's bytes by id — the storage layer's read hook. */
  @FunctionalInterface
  public interface SegmentResolver {
    byte @Nullable [] segment(int columnSegmentId);
  }

  // ==================== encode ====================

  /**
   * Encode a raw leaf payload into descriptor + segments.
   *
   * @throws IllegalStateException when {@code rawPayload} is not a valid raw leaf
   *         (propagated from {@link ProjectionIndexRowGroupPage#deserialize})
   */
  public static @Nullable EncodedRowGroup encode(final byte @Nullable [] rawPayload) {
    if (rawPayload == null) {
      // Null-in/null-out mirrors ProjectionIndexRowGroupCodec.encode — an absent leaf stays absent.
      return null;
    }
    final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(rawPayload);
    final int rowCount = page.getRowCount();
    final int columnCount = page.getColumnCount();
    if (columnCount > RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalStateException("columnCount " + columnCount + " exceeds MAX_COLUMNS="
          + RowGroupDescriptor.MAX_COLUMNS);
    }

    final int maxColumnSegments = 1 + 2 * columnCount;
    final int[] columnSegmentIds = new int[maxColumnSegments];
    final byte[][] segments = new byte[maxColumnSegments][];
    final byte[] entryFlags = new byte[maxColumnSegments];
    final long[] entryMins = new long[maxColumnSegments];
    final long[] entryMaxs = new long[maxColumnSegments];
    int columnSegmentCount = 0;

    // KEYS segment.
    {
      final ByteArrayOutputStream out = newColumnSegmentStream(SEG_KIND_KEYS);
      ProjectionIndexRowGroupCodec.putLongLE(out, page.firstRecordKey());
      ProjectionIndexRowGroupCodec.putLongLE(out, page.lastRecordKey());
      if (rowCount > 0) {
        ProjectionIndexRowGroupCodec.encodeRecordKeys(out, page.recordKeys(), rowCount);
      }
      columnSegmentIds[columnSegmentCount] = keysColumnSegmentId();
      segments[columnSegmentCount] = out.toByteArray();
      columnSegmentCount++;
    }

    final byte[] kinds = new byte[columnCount];
    for (int c = 0; c < columnCount; c++) {
      kinds[c] = page.columnKind(c);

      byte flags = page.columnUnrepresentable(c) ? ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE : 0;
      if (page.columnNumericNonIntegral(c)) {
        flags |= ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL;
      }
      if (page.columnPureDoubleSource(c)) {
        flags |= ProjectionIndexRowGroupPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE;
      }

      // BODY segment.
      final ByteArrayOutputStream body = newColumnSegmentStream(SEG_KIND_BODY);
      body.write(flags);
      if (rowCount > 0) {
        ProjectionIndexRowGroupCodec.putLongLE(body, page.columnMin(c));
        ProjectionIndexRowGroupCodec.putLongLE(body, page.columnMax(c));
        ProjectionIndexRowGroupCodec.encodePresence(body, page.presenceColumnBits(c), rowCount);
        switch (kinds[c]) {
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG ->
              ProjectionIndexRowGroupCodec.encodeForBitPacked(body, page.numericColumn(c), rowCount);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE ->
              ProjectionIndexRowGroupCodec.encodeForBitPackedDouble(body, page.numericColumn(c), rowCount);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> {
            final long[] bits = page.booleanColumnBits(c);
            final int words = (rowCount + 63) >>> 6;
            for (int w = 0; w < words; w++) {
              ProjectionIndexRowGroupCodec.putLongLE(body, bits[w]);
            }
          }
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT ->
              ProjectionIndexRowGroupCodec.encodeDictIds(body, page.stringDictionary(c),
                  page.stringDictIdColumn(c), rowCount);
          default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
        }
      }
      columnSegmentIds[columnSegmentCount] = bodyColumnSegmentId(c);
      segments[columnSegmentCount] = body.toByteArray();
      entryFlags[columnSegmentCount] = flags;
      // Empty leaf: mirror the zone-map sentinel pair (min > max = "no present value"), the
      // same discipline appendRow initialises — a fabricated [0, 0] would defeat descriptor
      // pruning and read as "possibly contains 0".
      entryMins[columnSegmentCount] = rowCount > 0 ? page.columnMin(c) : Long.MAX_VALUE;
      entryMaxs[columnSegmentCount] = rowCount > 0 ? page.columnMax(c) : Long.MIN_VALUE;
      columnSegmentCount++;

      // DICT segment (string columns with rows only).
      if (kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT && rowCount > 0) {
        final ByteArrayOutputStream dict = newColumnSegmentStream(SEG_KIND_DICT);
        encodeDictColumnSegmentPayload(dict, page.stringDictionary(c));
        columnSegmentIds[columnSegmentCount] = dictColumnSegmentId(c);
        segments[columnSegmentCount] = dict.toByteArray();
        columnSegmentCount++;
      }
    }

    final int[] byteLens = new int[columnSegmentCount];
    final long[] hashes = new long[columnSegmentCount];
    for (int i = 0; i < columnSegmentCount; i++) {
      byteLens[i] = segments[i].length;
      hashes[i] = contentHash(segments[i]);
    }
    final boolean[] inline = classifyInline(byteLens, columnSegmentCount);
    final byte[] descriptor = RowGroupDescriptor.serialize(rowCount, page.firstRecordKey(), page.lastRecordKey(),
        kinds, columnSegmentCount, columnSegmentIds, byteLens, hashes, entryFlags, entryMins, entryMaxs, inline, segments);

    final int[] idsTrimmed = new int[columnSegmentCount];
    System.arraycopy(columnSegmentIds, 0, idsTrimmed, 0, columnSegmentCount);
    final byte[][] segsTrimmed = new byte[columnSegmentCount][];
    System.arraycopy(segments, 0, segsTrimmed, 0, columnSegmentCount);
    return new EncodedRowGroup(descriptor, idsTrimmed, segsTrimmed);
  }

  /**
   * Classify each segment inline vs referenced by size, smallest-first under the per-leaf budget
   * (docs/PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md §3.3). Deterministic — same byteLens in
   * always produce the same classification — so the maintenance no-op hash is stable across
   * identical re-encodes (5.2-n). {@code columnSegmentCount} is tiny (≤ ~169), so the O(columnSegmentCount²)
   * smallest-first scan is free on the build path and needs no allocation beyond the result.
   */
  static boolean[] classifyInline(final int[] byteLens, final int columnSegmentCount) {
    final boolean[] inline = new boolean[columnSegmentCount];
    final int[] policy = INLINE_POLICY.get();
    final int maxColumnSegment = policy != null ? policy[0] : DEFAULT_INLINE_MAX_SEGMENT_BYTES;
    if (maxColumnSegment <= 0) {
      return inline; // inlining disabled → all referenced (pre-hybrid layout)
    }
    int remaining = policy != null ? policy[1] : DEFAULT_INLINE_MAX_TOTAL_BYTES;
    while (true) {
      int best = -1;
      for (int i = 0; i < columnSegmentCount; i++) {
        if (!inline[i] && byteLens[i] <= maxColumnSegment && byteLens[i] <= remaining
            && (best < 0 || byteLens[i] < byteLens[best])) {
          best = i;
        }
      }
      if (best < 0) {
        return inline; // nothing more fits the budget → the rest spill to pages
      }
      inline[best] = true;
      remaining -= byteLens[best];
    }
  }

  private static ByteArrayOutputStream newColumnSegmentStream(final byte segKind) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream(256);
    ProjectionIndexRowGroupCodec.putIntLE(out, SEGMENT_MAGIC);
    out.write(SEGMENT_VERSION);
    out.write(segKind);
    return out;
  }

  // ==================== assemble (decode) ====================

  /**
   * Reassemble the raw scan form from a descriptor and its segments, byte-identically to the
   * original {@link ProjectionIndexRowGroupPage#serialize()} output. Verifies each segment's
   * byteLen + contentHash against the descriptor before parsing.
   *
   * @throws IllegalStateException on any missing segment, length/hash mismatch, or malformed
   *         segment bytes — corruption is caught here, at fill time, never mid-kernel
   */
  public static byte[] assembleRaw(final byte[] descriptor, final SegmentResolver resolver) {
    RowGroupDescriptor.validate(descriptor);
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final int columnCount = RowGroupDescriptor.columnCount(descriptor);
    final byte[] kinds = new byte[columnCount];
    for (int c = 0; c < columnCount; c++) {
      kinds[c] = RowGroupDescriptor.kind(descriptor, c);
    }
    // Resolve every inline segment's data offset once (O(columnSegmentCount)) instead of a per-segment
    // prefix walk, so a whole-leaf assembly stays O(columnSegmentCount) in the inline bookkeeping.
    final int[] inlineOffsets = RowGroupDescriptor.inlineOffsets(descriptor);

    // KEYS.
    final ProjectionIndexRowGroupCodec.Cursor keys =
        openColumnSegment(descriptor, resolver, keysColumnSegmentId(), SEG_KIND_KEYS, inlineOffsets);
    final long firstRecordKey = keys.readLong();
    final long lastRecordKey = keys.readLong();
    final long[] recordKeys =
        rowCount > 0 ? ProjectionIndexRowGroupCodec.decodeRecordKeys(keys, rowCount) : new long[0];

    final long[] columnMin = new long[columnCount];
    final long[] columnMax = new long[columnCount];
    final long[][] numericCols = new long[columnCount][];
    final long[][] booleanCols = new long[columnCount][];
    final int[][] dictIdCols = new int[columnCount][];
    final byte[][][] dicts = new byte[columnCount][][];
    final byte[] columnFlags = new byte[columnCount];
    final long[][] presence = new long[columnCount][];
    final int presWords = rowCount > 0 ? (rowCount + 63) >>> 6 : 0;

    for (int c = 0; c < columnCount; c++) {
      final ProjectionIndexRowGroupCodec.Cursor body =
          openColumnSegment(descriptor, resolver, bodyColumnSegmentId(c), SEG_KIND_BODY, inlineOffsets);
      columnFlags[c] = body.readByte();

      final long[] bits = new long[Math.max(presWords, (ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6)];
      presence[c] = bits;
      if (rowCount == 0) {
        continue;
      }
      columnMin[c] = body.readLong();
      columnMax[c] = body.readLong();
      ProjectionIndexRowGroupCodec.decodePresenceInto(body, bits, presWords, rowCount);
      switch (kinds[c]) {
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE ->
            numericCols[c] = ProjectionIndexRowGroupCodec.decodeForBitPackedColumn(body, rowCount);
        case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN ->
            booleanCols[c] = ProjectionIndexRowGroupCodec.decodeBooleanWords(body, presWords);
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
          final ProjectionIndexRowGroupCodec.Cursor dictCur =
              openColumnSegment(descriptor, resolver, dictColumnSegmentId(c), SEG_KIND_DICT, inlineOffsets);
          dicts[c] = decodeDictColumnSegmentPayload(dictCur);
          dictIdCols[c] = ProjectionIndexRowGroupCodec.decodePackedIds(body, rowCount);
        }
        default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
      }
    }

    final byte[] direct = writeRawDirect(rowCount, columnCount, kinds, firstRecordKey,
        lastRecordKey, recordKeys, columnMin, columnMax, numericCols, booleanCols, dictIdCols,
        dicts, columnFlags, presence, presWords);
    if (verifyDirectAssembly) {
      final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.reconstruct(kinds, rowCount,
          firstRecordKey, lastRecordKey, recordKeys, columnMin, columnMax,
          numericCols, booleanCols, dictIdCols, dicts, presence, columnFlags);
      final byte[] viaPage = page.serialize();
      if (!Arrays.equals(direct, viaPage)) {
        throw new IllegalStateException("Direct raw assembly diverged from the page-based path ("
            + direct.length + " vs " + viaPage.length + " bytes) — layout drift");
      }
    }
    return direct;
  }

  /**
   * Cross-check switch for {@link #writeRawDirect}: when set, every assembly ALSO runs the
   * historical reconstruct-then-serialize path and fails loudly on any byte difference.
   * Off in production (the direct writer is the hydrate hot path); package-private and
   * mutable so {@code ProjectionColumnScanParityTest#directAssemblyMatchesPageSerialization}
   * can pin the parity in CI — the system property remains a manual diagnostic override.
   */
  static volatile boolean verifyDirectAssembly =
      Boolean.getBoolean("sirix.projection.verifyDirectAssembly");

  /**
   * Single-buffer raw-form writer — byte-identical to
   * {@code ProjectionIndexRowGroupPage.reconstruct(...).serialize()} but with the exact output
   * size precomputed and every array bulk-copied ({@code LongBuffer.put(long[])} is an
   * intrinsified memcpy), instead of a page object, a growing {@code ByteArrayOutputStream},
   * and per-value {@code putLong} calls. Measured 2-3x on the hydrate assemble phase, which
   * dominates cold-open cost.
   */
  private static byte[] writeRawDirect(final int rowCount, final int columnCount,
      final byte[] kinds, final long firstRecordKey, final long lastRecordKey,
      final long[] recordKeys, final long[] columnMin, final long[] columnMax,
      final long[][] numericCols, final long[][] booleanCols, final int[][] dictIdCols,
      final byte[][][] dicts, final byte[] columnFlags, final long[][] presence,
      final int presWords) {
    // ---- exact size ----
    int size = 8 + 16 + columnCount;                      // header
    if (rowCount > 0) {
      size += rowCount * 8;                               // record keys
      for (int c = 0; c < columnCount; c++) {
        size += 16;                                       // min/max
        switch (kinds[c]) {
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
               ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE -> size += rowCount * 8;
          case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> size += presWords * 8;
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
            final byte[][] dict = dicts[c];
            int dictSize = 0;
            int dictBytes = 0;
            while (dictSize < dict.length && dict[dictSize] != null) {
              dictBytes += dict[dictSize].length;
              dictSize++;
            }
            size += 4 + dictSize * 4 + dictBytes + rowCount * 4;
          }
          default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
        }
      }
    }
    size += columnCount + columnCount * presWords * 8 + 9; // presence tail + footer
    final byte[] out = new byte[size];
    final ByteBuffer bb = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
    // ---- header ----
    bb.putInt(rowCount);
    bb.putInt(columnCount);
    bb.putLong(firstRecordKey);
    bb.putLong(lastRecordKey);
    bb.put(kinds, 0, columnCount);
    if (rowCount > 0) {
      putLongsBulk(bb, recordKeys, rowCount);
      for (int c = 0; c < columnCount; c++) {
        bb.putLong(columnMin[c]);
        bb.putLong(columnMax[c]);
        switch (kinds[c]) {
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
               ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE ->
              putLongsBulk(bb, numericCols[c], rowCount);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN ->
              putLongsBulk(bb, booleanCols[c], presWords);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
            final byte[][] dict = dicts[c];
            int dictSize = 0;
            while (dictSize < dict.length && dict[dictSize] != null) {
              dictSize++;
            }
            bb.putInt(dictSize);
            for (int i = 0; i < dictSize; i++) {
              bb.putInt(dict[i].length);
            }
            for (int i = 0; i < dictSize; i++) {
              bb.put(dict[i], 0, dict[i].length);
            }
            final IntBuffer ib = bb.asIntBuffer();
            ib.put(dictIdCols[c], 0, rowCount);
            bb.position(bb.position() + rowCount * 4);
          }
          default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
        }
      }
    }
    // ---- presence tail ----
    bb.put(columnFlags, 0, columnCount);
    if (presWords > 0) {
      for (int c = 0; c < columnCount; c++) {
        putLongsBulk(bb, presence[c], presWords);
      }
    }
    bb.putInt(columnCount + columnCount * presWords * 8); // tailLen
    bb.put(ProjectionIndexRowGroupPage.PRESENCE_TAIL_VERSION);
    bb.putInt(ProjectionIndexRowGroupPage.PRESENCE_TAIL_MAGIC);
    if (bb.position() != size) {
      throw new IllegalStateException("Direct raw assembly size drift: wrote " + bb.position()
          + " of a computed " + size + " bytes");
    }
    return out;
  }

  /** Bulk little-endian long copy — {@code LongBuffer.put(long[])} intrinsifies to memcpy. */
  private static void putLongsBulk(final ByteBuffer bb, final long[] values, final int count) {
    final LongBuffer lb = bb.asLongBuffer();
    lb.put(values, 0, count);
    bb.position(bb.position() + count * 8);
  }

  /**
   * Decode ONE column's BODY segment into a {@link ProjectionColumnStore.ColumnSlice}
   * (P5b stage 2) — the column-pruned alternative to {@link #assembleRaw}: verifies the
   * segment's byteLen + XXH3-64 against the descriptor entry, then parses flags, zone map,
   * presence, and values with the exact decoders the assembler uses. String columns are
   * rejected (the column path never slices them — dict ids without their DICT segment are
   * meaningless).
   *
   * @throws IllegalStateException on verification or parse failure — callers decline to the
   *         eager whole-leaf path
   */
  static ProjectionColumnStore.ColumnSlice decodeBodySlice(final byte[] descriptor,
      final byte[] bodyColumnSegment, final int col) {
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final byte kind = RowGroupDescriptor.kind(descriptor, col);
    final int bodyId = bodyColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor body =
        openColumnSegment(descriptor, id -> id == bodyId ? bodyColumnSegment : null, bodyId, SEG_KIND_BODY, null);
    final byte flags = body.readByte();
    final int presWords = rowCount > 0 ? (rowCount + 63) >>> 6 : 0;
    final long[] presence = new long[presWords];
    if (rowCount == 0) {
      return new ProjectionColumnStore.ColumnSlice(0, flags, Long.MAX_VALUE, Long.MIN_VALUE,
          presence, null, null);
    }
    final long min = body.readLong();
    final long max = body.readLong();
    ProjectionIndexRowGroupCodec.decodePresenceInto(body, presence, presWords, rowCount);
    return switch (kind) {
      case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
           ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE ->
          new ProjectionColumnStore.ColumnSlice(rowCount, flags, min, max, presence,
              ProjectionIndexRowGroupCodec.decodeForBitPackedColumn(body, rowCount), null);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN ->
          new ProjectionColumnStore.ColumnSlice(rowCount, flags, min, max, presence, null,
              ProjectionIndexRowGroupCodec.decodeBooleanWords(body, presWords));
      default -> throw new IllegalStateException("Column " + col + " (kind " + kind
          + ") is not sliceable");
    };
  }

  /**
   * Column-scoped provenance primitive (5.1-7): the flags byte from a BODY segment's bytes —
   * segment TRUTH, as opposed to the descriptor's mirror. Validates the segment header.
   */
  public static byte bodyColumnSegmentFlags(final byte[] bodyColumnSegment) {
    checkColumnSegmentHeader(bodyColumnSegment, SEG_KIND_BODY);
    if (bodyColumnSegment.length < SEGMENT_HEADER_BYTES + 1) {
      throw new IllegalStateException("Truncated BODY segment: no flags byte after the header");
    }
    return bodyColumnSegment[SEGMENT_HEADER_BYTES];
  }

  // ==================== internals ====================

  /**
   * Resolve, verify (length + hash against the descriptor), and open a segment positioned
   * after its header.
   */
  private static ProjectionIndexRowGroupCodec.Cursor openColumnSegment(final byte[] descriptor,
      final SegmentResolver resolver, final int columnSegmentId, final byte expectedKind,
      final int @Nullable [] inlineOffsets) {
    // Hybrid: an inline segment's bytes live in the descriptor itself — resolve them there and
    // never touch the (page) resolver, so a referenced-segment resolver stays oblivious to inline.
    // The single entryIndexOf here is reused by verifyColumnSegment (no second lookup), and when the
    // caller precomputed inlineOffsets for the whole assembly the inline slice is an O(1) lookup
    // instead of a per-segment prefix walk.
    final int entry = RowGroupDescriptor.entryIndexOf(descriptor, columnSegmentId);
    final byte[] segment;
    if (entry >= 0 && RowGroupDescriptor.entryIsInline(descriptor, entry)) {
      final int off = inlineOffsets != null
          ? inlineOffsets[entry]
          : RowGroupDescriptor.inlineDataOffset(descriptor, entry);
      segment = RowGroupDescriptor.inlineColumnSegmentBytesAt(descriptor, entry, off);
    } else {
      segment = resolver.segment(columnSegmentId);
    }
    verifyColumnSegment(descriptor, segment, columnSegmentId, expectedKind, entry);
    return new ProjectionIndexRowGroupCodec.Cursor(segment, SEGMENT_HEADER_BYTES);
  }

  /**
   * Full segment verification against its descriptor entry — exact byteLen, XXH3-64 content
   * hash, and the PIXS header — without opening a cursor. The byte-level column cache
   * ({@code ProjectionColumnStore#columnBytes}) verifies once at fill; the fused fold
   * kernels then trust the cached bytes.
   *
   * @throws IllegalStateException on any mismatch (callers decline through the established
   *         fail-soft flow)
   */
  static void verifyColumnSegment(final byte[] descriptor, final byte @Nullable [] segment,
      final int columnSegmentId, final byte expectedKind) {
    verifyColumnSegment(descriptor, segment, columnSegmentId, expectedKind,
        RowGroupDescriptor.entryIndexOf(descriptor, columnSegmentId));
  }

  /** {@link #verifyColumnSegment} for a caller that already resolved the descriptor entry index. */
  static void verifyColumnSegment(final byte[] descriptor, final byte @Nullable [] segment,
      final int columnSegmentId, final byte expectedKind, final int entry) {
    if (entry < 0) {
      throw new IllegalStateException("Missing descriptor entry for columnSegmentId=" + columnSegmentId);
    }
    if (segment == null) {
      throw new IllegalStateException("Missing segment bytes for columnSegmentId=" + columnSegmentId
          + " (descriptor lists " + RowGroupDescriptor.entryByteLen(descriptor, entry) + " bytes)");
    }
    if (segment.length != RowGroupDescriptor.entryByteLen(descriptor, entry)) {
      throw new IllegalStateException("Segment length mismatch for columnSegmentId=" + columnSegmentId + ": "
          + segment.length + " != descriptor " + RowGroupDescriptor.entryByteLen(descriptor, entry));
    }
    if (contentHash(segment) != RowGroupDescriptor.entryContentHash(descriptor, entry)) {
      throw new IllegalStateException("Segment content-hash mismatch for columnSegmentId=" + columnSegmentId
          + " — corrupted segment page or dangling side-map reference");
    }
    checkColumnSegmentHeader(segment, expectedKind);
  }

  /** DICT payload modes: raw entry stream vs FSST-compressed entries behind a symbol table. */
  private static final byte DICT_MODE_RAW = 0;
  private static final byte DICT_MODE_FSST = 1;

  /**
   * DICT segment payload (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.7): a mode byte, then
   * either the raw entry stream (count, lengths, concatenated UTF-8 — byte-compatible with
   * the monolithic codec's dictionary half) or, for high-cardinality dictionaries that pass
   * {@code FSSTCompressor}'s existing gates AND actually compress, a per-segment symbol table
   * followed by per-entry FSST streams. FSST lives in the PERSISTED form only — decode
   * restores plain UTF-8 dictionary bytes, so the raw scan form (and every kernel comparing
   * dictionary bytes raw) is untouched. Training input is the dictionary in interning order
   * (deterministic), so identical re-encodes hash identically — the carry-forward no-op
   * contract (5.2-n) holds.
   */
  private static void encodeDictColumnSegmentPayload(final ByteArrayOutputStream out, final byte[][] dict) {
    final int dictSize = ProjectionIndexRowGroupCodec.dictSizeOf(dict);
    int totalBytes = 0;
    for (int i = 0; i < dictSize; i++) {
      totalBytes += dict[i].length;
    }
    if (dictSize >= FSSTCompressor.MIN_SAMPLES_FOR_TABLE
        && totalBytes >= FSSTCompressor.MIN_TOTAL_BYTES_FOR_TABLE) {
      final List<byte[]> entries = new ArrayList<>(dictSize);
      for (int i = 0; i < dictSize; i++) {
        entries.add(dict[i]);
      }
      final byte[] table = FSSTCompressor.buildSymbolTable(entries);
      if (table != null && FSSTCompressor.isCompressionBeneficial(entries, table)) {
        // Parse the symbol table ONCE and encode every entry against the parsed form — the
        // same lazy-parsed-table discipline as KeyValueLeafPage's per-page FSST wiring; the
        // byte[]-table overloads re-parse per call, an O(dictSize × tableLen) waste.
        final byte[][] parsedSymbols = FSSTCompressor.parseSymbolTable(table);
        out.write(DICT_MODE_FSST);
        ProjectionIndexRowGroupCodec.putIntLE(out, table.length);
        out.write(table, 0, table.length);
        ProjectionIndexRowGroupCodec.putIntLE(out, dictSize);
        for (int i = 0; i < dictSize; i++) {
          final byte[] encoded = FSSTCompressor.encode(dict[i], parsedSymbols);
          ProjectionIndexRowGroupCodec.putIntLE(out, encoded.length);
          out.write(encoded, 0, encoded.length);
        }
        return;
      }
    }
    out.write(DICT_MODE_RAW);
    ProjectionIndexRowGroupCodec.encodeDictEntries(out, dict);
  }

  /** Inverse of {@link #encodeDictColumnSegmentPayload}; restores plain UTF-8 dictionary bytes. */
  private static byte[][] decodeDictColumnSegmentPayload(final ProjectionIndexRowGroupCodec.Cursor in) {
    final int mode = in.readByte() & 0xFF;
    if (mode == DICT_MODE_RAW) {
      return ProjectionIndexRowGroupCodec.decodeDictEntries(in);
    }
    if (mode != DICT_MODE_FSST) {
      throw new IllegalStateException("Unknown DICT segment mode " + mode
          + " — written by a newer version");
    }
    final int tableLen = in.readInt();
    final byte[] table = in.readBytes(tableLen);
    // Parse once, decode all entries with the parsed symbols (KeyValueLeafPage discipline).
    final byte[][] parsedSymbols = FSSTCompressor.parseSymbolTable(table);
    final int dictSize = in.readInt();
    final byte[][] dict = new byte[Math.max(16, dictSize)][];
    for (int i = 0; i < dictSize; i++) {
      final int encLen = in.readInt();
      final byte[] encoded = in.readBytes(encLen);
      dict[i] = FSSTCompressor.decode(encoded, parsedSymbols);
    }
    return dict;
  }

  private static void checkColumnSegmentHeader(final byte[] segment, final byte expectedKind) {
    if (segment.length < SEGMENT_HEADER_BYTES
        || ProjectionIndexRowGroupCodec.getIntLE(segment, 0) != SEGMENT_MAGIC) {
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
}
