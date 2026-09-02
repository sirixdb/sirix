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
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Segmented persistence codec for projection leaves (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md
 * §2.3): splits one raw leaf into <em>semantic segments</em> — the record-key column, one body per
 * column (presence + encoded values, flags at the head), and one dictionary per string column —
 * each destined for its own CoW-versioned segment slot, addressed from a
 * {@link RowGroupDescriptor}. The slot keeps a small payload in its HOT value and spills a larger
 * payload to an {@link io.sirix.page.OverflowPage}; descriptor bytes never duplicate segment bytes.
 * Per-segment encodings use the package-local primitives in
 * {@link ProjectionIndexRowGroupCodec} (delta/FOR record keys, FOR bit-packed numerics, packed
 * dict-ids, and marker-byte presence). That helper owns no persisted row-group envelope; this class
 * owns the one supported format, in which
 *
 * <ul>
 * <li>a query reading column {@code c} fetches {@code BODY(c)} (+ {@code DICT(c)} for string
 * predicates) and nothing else;</li>
 * <li>a single-column update re-encodes only that column's changed segments;</li>
 * <li>{@link #assembleRaw} reconstructs the raw scan form <b>byte-identically</b>, including
 * presence, unrepresentable, and integrality provenance.</li>
 * </ul>
 *
 * <h2>Segment id scheme</h2>
 *
 * {@code 0 = KEYS}, {@code 4c+1 = BODY(c)}, {@code 4c+2 = DICT(c)}, {@code 4c+3 = SET_COUNTS(c)},
 * {@code 4c+4 = STRING_BLOOM(c)} — capped at {@link RowGroupDescriptor#MAX_COLUMNS} columns by the
 * 16-bit id space of the HOT side-map composite key.
 *
 * <h2>Per-segment wire form</h2>
 *
 * Every segment is self-describing: {@code int "PIXS"; byte version; byte
 * segKind}, then the kind-specific payload:
 *
 * <pre>
 *   KEYS:  long firstRecordKey; long lastRecordKey;
 *          [rowCount &gt; 0] byte mode; long base; byte width; packed keys;
 *          byte orderExceptionKind; [kind=DENSE] long[ceil(rowCount/64)] bits;
 *          order-label lane, one of the three interchangeable forms in
 *          {@link ProjectionIndexRowGroupCodec#encodeOrderLabels} (legacy offsets, synthesized runs,
 *          front-coded), discriminated by the sign of its leading int32
 *   BODY:  byte colFlags;                       // provenance TRUTH (5.1-7)
 *          [rowCount &gt; 0] long min; long max;   // zone-map truth
 *          presence marker byte [+ words];
 *          NUMERIC: long base; byte width; packed values
 *          BOOLEAN: words verbatim
 *          STRING:  byte idWidth; packed dict-ids
 *   DICT:  int dictSize; int[dictSize] lens; concatenated UTF-8
 * </pre>
 *
 * <p>
 * An empty leaf ({@code rowCount == 0}) still emits KEYS (fences) and one BODY per column (flag
 * truth) — no DICT segments; the descriptor stays the bounds authority.
 *
 * <p>
 * Integrity: {@link #assembleRaw} verifies every segment's exact {@code byteLen} and XXH3-64
 * {@code contentHash} against the descriptor before parsing — segment pages persist behind bare
 * offset keys, so the descriptor hash is their only checksum. A mismatch throws (callers fail soft
 * to the generic pipeline and negative-cache, §4). The hash doubles as the maintenance write-path
 * no-op comparator (§3).
 */
public final class ProjectionIndexColumnSegmentCodec {

  /** Leading magic of every segment ("PIXS" little-endian). */
  public static final int SEGMENT_MAGIC = 0x53584950;

  /** Segment layout version; bumped on any wire change. */
  public static final byte SEGMENT_VERSION = 0;

  /** Segment kind tags. */
  public static final byte SEG_KIND_KEYS = 0;
  public static final byte SEG_KIND_BODY = 1;
  public static final byte SEG_KIND_DICT = 2;

  /**
   * Per-value ROW COUNTS for a set column: the values, each with the number of rows on this leaf
   * whose set contains it.
   *
   * <p>
   * Its own segment kind rather than a section of {@link #SEG_KIND_DICT} for one reason: SIZE. The
   * segment-slot policy stores a small payload directly in its HOT slot, so it costs no overflow-page
   * fetch. A dictionary can spill while the counts beside a compact value list remain inline.
   * Splitting them is what turns a membership count from a dictionary scan into a tiny slot read.
   *
   * <p>
   * Emitted only while it fits the segment-slot inline threshold — past that it would buy nothing and
   * cost a page of its own, so it is not written and the reader falls back to scanning.
   */
  public static final byte SEG_KIND_SET_COUNTS = 3;

  /**
   * Per-leaf BLOOM FINGERPRINT over a string column's distinct values — the equality analogue of the
   * numeric zone map for UNSORTED string data.
   *
   * <p>
   * Why min/max cannot do this job: the descriptor's zone pair prunes only when the column is
   * clustered, and a leaf of ~1024 arbitrary titles spans nearly the whole collation range, so a
   * string-equality literal zone-tests as "possible" on every leaf. The fingerprint answers the
   * question the zone map cannot: "can THIS value be on this leaf at all?" — probabilistically, with
   * no false negatives. A miss skips the leaf's BODY and DICT fetch AND decode entirely; a false
   * positive costs exactly what every leaf cost before this segment existed.
   *
   * <p>
   * Sized at ~10 bits per distinct value (3 probes of one 64-bit hash), clamped to [64 B, 2 KiB] of
   * filter words: ~1 % false-positive rate at full occupancy. On the movies corpus a title-equality
   * one-shot reads ~4 MB of fingerprints instead of ~55 MB of dictionary + id segments.
   *
   * <p>
   * This is an optional acceleration segment within the sole segment-slot format. When a column has
   * no fingerprint entry, the scan conservatively keeps the row group.
   */
  public static final byte SEG_KIND_STRING_BLOOM = 4;

  /**
   * Per-leaf DICT-ENTRY HASHES: {@link ProjectionIndexByteScan#fnv1a64} of every dictionary entry, in
   * dict-id order, as a fixed-width {@code long} array.
   *
   * <p>
   * Why a segment of its own rather than a section of {@link #SEG_KIND_DICT}: a
   * {@code COUNT(DISTINCT s)} fold consumes the dictionary ONLY as 64-bit content identities (dict
   * ids are leaf-local, so the set member has to be the entry's hash). On a HIGH-CARDINALITY column
   * the per-leaf dictionaries barely dedupe, so that fold's first touch fetched and FSST-decoded the
   * whole dictionary chain — hundreds of MB — to derive eight bytes per entry. Precomputing them at
   * build time turns the operand's fill into (packed ids + 8 B/entry): no dictionary bytes fetched,
   * no FSST decode, no hashing loop. A leaf caps at {@link ProjectionIndexRowGroupPage#MAX_ROWS}
   * rows, hence at that many entries, so the segment is at most ~8 KiB.
   *
   * <p>
   * Stored in a segment-id region DISJOINT from the per-column stride (see
   * {@link #dictHashColumnSegmentId}). Every populated {@code STRING_DICT} column in the sole format
   * carries this segment; absence is corruption, not an older-format fallback.
   */
  public static final byte SEG_KIND_DICT_HASHES = 5;

  /** Fixed per-segment header size: magic + version + segKind. */
  public static final int SEGMENT_HEADER_BYTES = 6;

  /**
   * Byte offset of a BODY segment's presence marker for a leaf WITH rows: the segment header, the
   * column flags byte, then the {@code min}/{@code max} zone mirrors. Kept beside the header size
   * because it is the same kind of fact — a layout position the decoders consume implicitly through
   * {@link #decodeBodySlice} and that {@link #bodyAllPresent} reads directly.
   */
  static final int BODY_PRESENCE_MARKER_OFFSET = SEGMENT_HEADER_BYTES + Byte.BYTES + 2 * Long.BYTES;

  /**
   * Whether a VERIFIED BODY segment of a leaf with rows encodes every row present — the encoder's
   * marker {@code 0} ({@code 1} = all missing, {@code 2} = explicit words). Reads ONE byte instead of
   * decoding the payload, which is what lets a column's all-present bitset be built from a pass over
   * its BODY chain without ever holding the decoded column.
   *
   * @throws IllegalStateException when the segment is too short to carry a marker — a rowless leaf's
   *         BODY (flags only) must never be asked; the caller consults the row count first
   */
  static boolean bodyAllPresent(final byte[] bodyColumnSegment) {
    if (bodyColumnSegment.length <= BODY_PRESENCE_MARKER_OFFSET) {
      throw new IllegalStateException("BODY segment of " + bodyColumnSegment.length
          + " bytes carries no presence marker (rowless leaf, or a truncated segment)");
    }
    final int marker = bodyColumnSegment[BODY_PRESENCE_MARKER_OFFSET] & 0xFF;
    if (marker > 2) {
      throw new IllegalStateException("Bad presence marker " + marker);
    }
    return marker == 0;
  }

  /** Whether decoded presence words mark every one of {@code rowCount} rows present. */
  static boolean allPresent(final long[] presenceWords, final int rowCount) {
    final int words = (rowCount + 63) >>> 6;
    for (int w = 0; w < words; w++) {
      if (presenceWords[w] != ProjectionIndexRowGroupCodec.expectedFullWord(w, words, rowCount)) {
        return false;
      }
    }
    return true;
  }

  /** XXH3-64 for descriptor content hashes (zero-allocation, shared instance). */
  private static final LongHashFunction XX3 = LongHashFunction.xx3();

  private ProjectionIndexColumnSegmentCodec() {}

  /**
   * Per-column segment slots in the id scheme; {@link RowGroupDescriptor#MAX_COLUMNS} is derived from
   * this and the 16-bit id space so the invariant lives in one place.
   */
  public static final int SEGMENTS_PER_COLUMN = 4;

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

  /** Segment id of column {@code c}'s {@link #SEG_KIND_SET_COUNTS} segment. */
  public static int setCountsColumnSegmentId(final int column) {
    return SEGMENTS_PER_COLUMN * checkColumn(column) + 3;
  }

  /**
   * Segment id of column {@code c}'s {@link #SEG_KIND_STRING_BLOOM} segment. Sub-id 4 takes the
   * fourth slot of the per-column stride ({@code SEGMENTS_PER_COLUMN} = 4; body/dict/set-counts take
   * 1..3). The V0 projection layout assigns this stride from its first emitted resource.
   */
  public static int bloomColumnSegmentId(final int column) {
    return SEGMENTS_PER_COLUMN * checkColumn(column) + 4;
  }

  /**
   * Id slots a column reserves in the 16-bit sub-id space: the {@link #SEGMENTS_PER_COLUMN} of its
   * contiguous stride plus one in the {@link #DICT_HASH_SEGMENT_BASE} region.
   * {@link RowGroupDescriptor#MAX_COLUMNS} derives from this, so the two regions can never overlap.
   */
  public static final int SEGMENT_ID_SLOTS_PER_COLUMN = SEGMENTS_PER_COLUMN + 1;

  /**
   * First id of the DICT-HASH region, which sits ABOVE every per-column stride id
   * ({@code SEGMENTS_PER_COLUMN · (MAX_COLUMNS - 1) + 4}) with slack to spare.
   *
   * <p>
   * A disjoint region rather than a fifth stride slot for one reason: widening the stride RENUMBERS
   * every existing segment id, so a projection written before the change would be read at shifted ids
   * — body bytes fetched where a dictionary lives. Appending a region leaves all four existing
   * formulas stable. It costs only a tighter column cap.
   */
  public static final int DICT_HASH_SEGMENT_BASE = SEGMENTS_PER_COLUMN * RowGroupDescriptor.MAX_COLUMNS + 8;

  /**
   * Segment id of column {@code c}'s {@link #SEG_KIND_DICT_HASHES} segment. Entries in this region
   * sort ABOVE every stride id, so the encoder emits them in one trailing pass to keep the
   * descriptor's ascending-id invariant.
   */
  public static int dictHashColumnSegmentId(final int column) {
    return DICT_HASH_SEGMENT_BASE + checkColumn(column);
  }

  private static int checkColumn(final int column) {
    if (column < 0 || column >= RowGroupDescriptor.MAX_COLUMNS) {
      // Without this check a column past the cap would produce a segment id past the 16-bit sub-id
      // space, colliding with another column's segment — a hash mismatch found only at later assembly.
      throw new IllegalArgumentException("column out of range [0, " + RowGroupDescriptor.MAX_COLUMNS + "): " + column);
    }
    return column;
  }

  /** Column index owning segment id {@code columnSegmentId} (any kind), or -1 for KEYS. */
  public static int columnOfColumnSegment(final int columnSegmentId) {
    if (columnSegmentId == 0) {
      return -1;
    }
    return columnSegmentId >= DICT_HASH_SEGMENT_BASE
        ? columnSegmentId - DICT_HASH_SEGMENT_BASE
        : (columnSegmentId - 1) / SEGMENTS_PER_COLUMN;
  }

  /** XXH3-64 content hash as stored in descriptor entries. */
  public static long contentHash(final byte[] segment) {
    return XX3.hashBytes(segment);
  }

  /**
   * One encoded leaf: the descriptor plus parallel arrays of segment ids and segment bytes (ascending
   * id order — KEYS, then per column BODY [, DICT]).
   *
   * <p>
   * <b>Aliasing contract (HFT, no defensive copies):</b> the accessors expose the codec's internal
   * arrays. The descriptor embeds each segment's content hash at encode time, so mutating any
   * returned array de-synchronises bytes from their recorded hash and poisons every later assembly
   * with a spurious corruption error. Treat all three as immutable.
   */
  public record EncodedRowGroup(byte[] descriptor, int[] columnSegmentIds, byte[][] segments) {
  }

  /**
   * One independently encoded column. Segment ids are already remapped to the owning projection's
   * real column ordinal and sorted ascending; no KEYS segment is present.
   */
  record EncodedColumn(int column, int rowCount, byte columnKind, int[] columnSegmentIds, byte[][] segments,
      long[] contentHashes, byte[] entryFlags, long[] entryMins, long[] entryMaxs) {
  }

  /**
   * Owner-confined scratch for a stream of row-group encodes.
   *
   * <p>
   * The segment staging stream is retained at its high-water mark, while the expensive FSST matcher
   * arrays are created lazily; both are reused across every dictionary and row group owned by the
   * builder. The atomic claim is not a sharing mechanism: it fails closed if a caller accidentally
   * submits the same workspace concurrently. Published descriptors and segments never alias this
   * state.
   */
  static final class EncodeWorkspace {
    private final AtomicBoolean inUse = new AtomicBoolean();
    /**
     * One grow-only staging stream for every segment emitted by this workspace.
     *
     * <p>
     * The stream is reset between segments, but its backing array is retained at the high-water mark.
     * Every published segment is detached through {@link ByteArrayOutputStream#toByteArray()} before
     * the next reset, so neither later segment writes nor later row-group encodes can mutate an
     * {@link EncodedRowGroup} that has already escaped this workspace.
     */
    private final ByteArrayOutputStream segmentOutput = new ByteArrayOutputStream(256);
    private FSSTCompressor.Workspace fsstWorkspace;

    void claim() {
      if (!inUse.compareAndSet(false, true)) {
        throw new IllegalStateException("Projection encode workspace is already in use");
      }
    }

    private FSSTCompressor.Workspace fsstWorkspace() {
      if (fsstWorkspace == null) {
        fsstWorkspace = new FSSTCompressor.Workspace();
      }
      return fsstWorkspace;
    }

    /** Reset the owner-confined staging stream and start one self-describing segment. */
    private ByteArrayOutputStream beginSegment(final byte segmentKind) {
      segmentOutput.reset();
      ProjectionIndexRowGroupCodec.putIntLE(segmentOutput, SEGMENT_MAGIC);
      segmentOutput.write(SEGMENT_VERSION);
      segmentOutput.write(segmentKind);
      return segmentOutput;
    }

    void release() {
      try {
        if (fsstWorkspace != null) {
          fsstWorkspace.clear();
        }
      } finally {
        // Volatile release for accidental cross-thread sequential hand-off. Workspace-pool
        // hand-off additionally crosses the pool monitor.
        inUse.set(false);
      }
    }
  }

  /** Retains at most eight reusable workspaces; streaming owners bypass this monitor. */
  private static final int ENCODE_WORKSPACE_POOL_SIZE =
      Math.min(8, Math.max(1, Runtime.getRuntime().availableProcessors()));
  private static final ArrayDeque<EncodeWorkspace> ENCODE_WORKSPACE_POOL = new ArrayDeque<>(ENCODE_WORKSPACE_POOL_SIZE);

  private static EncodeWorkspace acquireEncodeWorkspace() {
    synchronized (ENCODE_WORKSPACE_POOL) {
      final EncodeWorkspace workspace = ENCODE_WORKSPACE_POOL.pollFirst();
      if (workspace != null) {
        return workspace;
      }
    }
    return new EncodeWorkspace();
  }

  private static void releaseEncodeWorkspace(final EncodeWorkspace workspace) {
    synchronized (ENCODE_WORKSPACE_POOL) {
      if (ENCODE_WORKSPACE_POOL.size() < ENCODE_WORKSPACE_POOL_SIZE) {
        ENCODE_WORKSPACE_POOL.addFirst(workspace);
      }
    }
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
   * @throws IllegalStateException when {@code rawPayload} is not a valid raw leaf (propagated from
   *         {@link ProjectionIndexRowGroupPage#deserialize})
   */
  public static @Nullable EncodedRowGroup encode(final byte @Nullable [] rawPayload) {
    return encodeWithWorkspace(rawPayload);
  }

  /** Owner-confined form used by streaming projection builders. */
  static @Nullable EncodedRowGroup encode(final byte @Nullable [] rawPayload, final EncodeWorkspace workspace) {
    return encodeWithWorkspace(rawPayload, workspace);
  }

  /**
   * Owner-confined SEGMENT-SLOT form for a live builder-owned leaf.
   *
   * <p>
   * The page is borrowed for this synchronous call only. The returned descriptor, id array and every
   * segment are newly allocated encoder outputs and do not alias the page or the workspace. This is
   * the load path's allocation boundary: it avoids serialising the whole row group merely to
   * deserialize it again before producing the independently owned column segments.
   */
  static EncodedRowGroup encode(final ProjectionIndexRowGroupPage page, final EncodeWorkspace workspace) {
    if (page == null) {
      throw new NullPointerException("page must not be null");
    }
    if (workspace == null) {
      throw new NullPointerException("workspace must not be null");
    }
    workspace.claim();
    try {
      return encodeClaimed(page, workspace);
    } finally {
      workspace.release();
    }
  }

  /**
   * Encode a one-column maintenance page and remap its segment ids to {@code owningColumn}.
   *
   * <p>
   * The ordinary leaf encoder is reused so BODY/DICT/SET_COUNTS/BLOOM/DICT_HASH bytes stay defined by
   * one implementation. KEYS is discarded: the update-only path preserves the existing key segment
   * and publishes only this record's returned column segments.
   * </p>
   */
  static EncodedColumn encodeColumn(final ProjectionIndexRowGroupPage page, final int owningColumn,
      final EncodeWorkspace workspace) {
    checkColumn(owningColumn);
    if (page == null || page.getColumnCount() != 1) {
      throw new IllegalArgumentException("column maintenance encoding requires a one-column page");
    }
    final EncodedRowGroup encoded = encode(page, workspace);
    final int segmentCount = encoded.columnSegmentIds().length - 1;
    if (segmentCount < 1 || encoded.columnSegmentIds()[0] != keysColumnSegmentId()) {
      throw new IllegalStateException("one-column encoding did not emit KEYS followed by a BODY segment");
    }
    final int[] ids = new int[segmentCount];
    final byte[][] segments = new byte[segmentCount][];
    final long[] hashes = new long[segmentCount];
    final byte[] flags = new byte[segmentCount];
    final long[] mins = new long[segmentCount];
    final long[] maxs = new long[segmentCount];
    for (int i = 0; i < segmentCount; i++) {
      final int sourceId = encoded.columnSegmentIds()[i + 1];
      final int remappedId = remapSingleColumnSegmentId(sourceId, owningColumn);
      ids[i] = remappedId;
      segments[i] = encoded.segments()[i + 1];
      final int sourceEntry = RowGroupDescriptor.entryIndexOf(encoded.descriptor(), sourceId);
      hashes[i] = RowGroupDescriptor.entryContentHash(encoded.descriptor(), sourceEntry);
      flags[i] = RowGroupDescriptor.entryColFlags(encoded.descriptor(), sourceEntry);
      mins[i] = RowGroupDescriptor.entryMin(encoded.descriptor(), sourceEntry);
      maxs[i] = RowGroupDescriptor.entryMax(encoded.descriptor(), sourceEntry);
    }
    return new EncodedColumn(owningColumn, page.getRowCount(), page.columnKind(0), ids, segments, hashes, flags, mins,
        maxs);
  }

  private static int remapSingleColumnSegmentId(final int sourceId, final int owningColumn) {
    if (sourceId == bodyColumnSegmentId(0))
      return bodyColumnSegmentId(owningColumn);
    if (sourceId == dictColumnSegmentId(0))
      return dictColumnSegmentId(owningColumn);
    if (sourceId == setCountsColumnSegmentId(0))
      return setCountsColumnSegmentId(owningColumn);
    if (sourceId == bloomColumnSegmentId(0))
      return bloomColumnSegmentId(owningColumn);
    if (sourceId == dictHashColumnSegmentId(0))
      return dictHashColumnSegmentId(owningColumn);
    throw new IllegalStateException("unexpected segment " + sourceId + " in one-column encoding");
  }

  /**
   * Replace several columns' descriptor entries in one merge and one set of output arrays.
   * Replacements must be sorted by strictly ascending owning column. The output bitmap is cleared and
   * receives only columns whose complete descriptor entry set (including pruning mirrors) changed.
   * No carried segment bytes are fetched or copied.
   */
  static byte[] spliceColumns(final byte[] priorDescriptor, final EncodedColumn[] replacements,
      final int replacementCount, final long[] actuallyChangedColumnWords) {
    RowGroupDescriptor.validate(priorDescriptor);
    if (replacements == null || actuallyChangedColumnWords == null) {
      throw new NullPointerException("replacement columns and changed-column output are required");
    }
    if (replacementCount <= 0 || replacementCount > replacements.length) {
      throw new IllegalArgumentException("replacementCount out of range: " + replacementCount);
    }
    final int columnCount = RowGroupDescriptor.columnCount(priorDescriptor);
    final int requiredWords = (columnCount + Long.SIZE - 1) >>> 6;
    if (actuallyChangedColumnWords.length < requiredWords) {
      throw new IllegalArgumentException("changed-column output is too short for " + columnCount + " columns");
    }
    Arrays.fill(actuallyChangedColumnWords, 0L);

    int previousColumn = -1;
    int replacementSegmentCount = 0;
    for (int replacementIndex = 0; replacementIndex < replacementCount; replacementIndex++) {
      final EncodedColumn replacement = replacements[replacementIndex];
      if (replacement == null) {
        throw new NullPointerException("replacement column " + replacementIndex + " is required");
      }
      final int column = replacement.column();
      if (column <= previousColumn || column >= columnCount) {
        throw new IllegalArgumentException(
            "replacement columns must be unique and ascending within the descriptor: " + column);
      }
      if (replacement.rowCount() != RowGroupDescriptor.rowCount(priorDescriptor)
          || replacement.columnKind() != RowGroupDescriptor.kind(priorDescriptor, column)) {
        throw new IllegalArgumentException("replacement column " + column + " shape does not match its row group");
      }
      validateEncodedColumn(replacement);
      replacementSegmentCount = Math.addExact(replacementSegmentCount, replacement.columnSegmentIds().length);
      if (descriptorColumnChanged(priorDescriptor, replacement)) {
        actuallyChangedColumnWords[column >>> 6] |= 1L << (column & 63);
      }
      previousColumn = column;
    }

    final int priorCount = RowGroupDescriptor.columnSegmentCount(priorDescriptor);
    int replacedPriorSegments = 0;
    for (int priorIndex = 0; priorIndex < priorCount; priorIndex++) {
      final int owner = columnOfColumnSegment(RowGroupDescriptor.entryColumnSegmentId(priorDescriptor, priorIndex));
      if (owner >= 0 && replacementIndexForColumn(replacements, replacementCount, owner) >= 0) {
        replacedPriorSegments++;
      }
    }
    final int nextCount = Math.addExact(priorCount - replacedPriorSegments, replacementSegmentCount);
    final int[] ids = new int[nextCount];
    final int[] lengths = new int[nextCount];
    final long[] hashes = new long[nextCount];
    final byte[] flags = new byte[nextCount];
    final long[] mins = new long[nextCount];
    final long[] maxs = new long[nextCount];

    // Each independently encoded local-dictionary column has one trailing DICT_HASH id. Merge the
    // ordinary stride and trailing hash region separately; both sources are monotonic within each
    // region, so this remains O(prior entries + replacement entries) without a heap or sort scratch.
    int priorIndex = 0;
    int out = 0;
    for (int region = 0; region < 2; region++) {
      final int lowerBound = region == 0 ? 0 : DICT_HASH_SEGMENT_BASE;
      final int upperBound = region == 0 ? DICT_HASH_SEGMENT_BASE : Integer.MAX_VALUE;
      int replacementIndex = 0;
      int replacementSegmentIndex = 0;
      while (true) {
        int replacementId = Integer.MAX_VALUE;
        EncodedColumn replacement = null;
        while (replacementIndex < replacementCount) {
          replacement = replacements[replacementIndex];
          final int[] replacementIds = replacement.columnSegmentIds();
          while (replacementSegmentIndex < replacementIds.length
              && replacementIds[replacementSegmentIndex] < lowerBound) {
            replacementSegmentIndex++;
          }
          if (replacementSegmentIndex < replacementIds.length
              && replacementIds[replacementSegmentIndex] < upperBound) {
            replacementId = replacementIds[replacementSegmentIndex];
            break;
          }
          replacementIndex++;
          replacementSegmentIndex = 0;
        }

        int priorId = Integer.MAX_VALUE;
        while (priorIndex < priorCount) {
          final int candidate = RowGroupDescriptor.entryColumnSegmentId(priorDescriptor, priorIndex);
          if (candidate >= upperBound) {
            break;
          }
          final int owner = columnOfColumnSegment(candidate);
          if (candidate < lowerBound
              || owner >= 0 && replacementIndexForColumn(replacements, replacementCount, owner) >= 0) {
            priorIndex++;
            continue;
          }
          priorId = candidate;
          break;
        }

        if (priorId == Integer.MAX_VALUE && replacementId == Integer.MAX_VALUE) {
          break;
        }
        if (priorId == replacementId) {
          throw new IllegalStateException("replacement column segment collides with carried entry " + priorId);
        }
        if (replacementId < priorId) {
          final byte[] segment = replacement.segments()[replacementSegmentIndex];
          ids[out] = replacementId;
          lengths[out] = segment.length;
          hashes[out] = replacement.contentHashes()[replacementSegmentIndex];
          flags[out] = replacement.entryFlags()[replacementSegmentIndex];
          mins[out] = replacement.entryMins()[replacementSegmentIndex];
          maxs[out] = replacement.entryMaxs()[replacementSegmentIndex];
          replacementSegmentIndex++;
        } else {
          ids[out] = priorId;
          lengths[out] = RowGroupDescriptor.entryByteLen(priorDescriptor, priorIndex);
          hashes[out] = RowGroupDescriptor.entryContentHash(priorDescriptor, priorIndex);
          flags[out] = RowGroupDescriptor.entryColFlags(priorDescriptor, priorIndex);
          mins[out] = RowGroupDescriptor.entryMin(priorDescriptor, priorIndex);
          maxs[out] = RowGroupDescriptor.entryMax(priorDescriptor, priorIndex);
          priorIndex++;
        }
        out++;
      }
    }
    if (out != nextCount) {
      throw new IllegalStateException("descriptor splice emitted " + out + " entries, expected " + nextCount);
    }

    final byte[] kinds = new byte[columnCount];
    for (int column = 0; column < columnCount; column++) {
      kinds[column] = RowGroupDescriptor.kind(priorDescriptor, column);
    }
    final byte[] next = RowGroupDescriptor.serialize(RowGroupDescriptor.rowCount(priorDescriptor),
        RowGroupDescriptor.firstRecordKey(priorDescriptor), RowGroupDescriptor.lastRecordKey(priorDescriptor), kinds,
        nextCount, ids, lengths, hashes, flags, mins, maxs);
    RowGroupDescriptor.validate(next);
    return next;
  }

  private static boolean descriptorColumnChanged(final byte[] priorDescriptor, final EncodedColumn replacement) {
    final int[] replacementIds = replacement.columnSegmentIds();
    for (int replacementIndex = 0; replacementIndex < replacementIds.length; replacementIndex++) {
      final int priorEntry = RowGroupDescriptor.entryIndexOf(priorDescriptor, replacementIds[replacementIndex]);
      if (priorEntry < 0
          || RowGroupDescriptor.entryByteLen(priorDescriptor, priorEntry) != replacement.segments()[replacementIndex].length
          || RowGroupDescriptor.entryContentHash(priorDescriptor,
              priorEntry) != replacement.contentHashes()[replacementIndex]
          || RowGroupDescriptor.entryColFlags(priorDescriptor, priorEntry) != replacement.entryFlags()[replacementIndex]
          || RowGroupDescriptor.entryMin(priorDescriptor, priorEntry) != replacement.entryMins()[replacementIndex]
          || RowGroupDescriptor.entryMax(priorDescriptor, priorEntry) != replacement.entryMaxs()[replacementIndex]) {
        return true;
      }
    }
    final int column = replacement.column();
    int priorOwned = RowGroupDescriptor.entryIndexOf(priorDescriptor, bodyColumnSegmentId(column)) >= 0 ? 1 : 0;
    priorOwned += RowGroupDescriptor.entryIndexOf(priorDescriptor, dictColumnSegmentId(column)) >= 0 ? 1 : 0;
    priorOwned += RowGroupDescriptor.entryIndexOf(priorDescriptor, setCountsColumnSegmentId(column)) >= 0 ? 1 : 0;
    priorOwned += RowGroupDescriptor.entryIndexOf(priorDescriptor, bloomColumnSegmentId(column)) >= 0 ? 1 : 0;
    priorOwned += RowGroupDescriptor.entryIndexOf(priorDescriptor, dictHashColumnSegmentId(column)) >= 0 ? 1 : 0;
    return priorOwned != replacementIds.length;
  }

  private static int replacementIndexForColumn(final EncodedColumn[] replacements, final int replacementCount,
      final int column) {
    int low = 0;
    int high = replacementCount - 1;
    while (low <= high) {
      final int middle = (low + high) >>> 1;
      final int candidate = replacements[middle].column();
      if (candidate == column) {
        return middle;
      }
      if (candidate < column) {
        low = middle + 1;
      } else {
        high = middle - 1;
      }
    }
    return -1;
  }

  private static void validateEncodedColumn(final EncodedColumn encoded) {
    final int count = encoded.columnSegmentIds().length;
    if (count == 0 || encoded.segments().length != count || encoded.contentHashes().length != count
        || encoded.entryFlags().length != count
        || encoded.entryMins().length != count || encoded.entryMaxs().length != count) {
      throw new IllegalArgumentException("encoded column arrays are not aligned");
    }
    int previous = -1;
    boolean bodySeen = false;
    for (int i = 0; i < count; i++) {
      final int id = encoded.columnSegmentIds()[i];
      if (id <= previous || columnOfColumnSegment(id) != encoded.column() || encoded.segments()[i] == null) {
        throw new IllegalArgumentException("invalid encoded segment id " + id + " for column " + encoded.column());
      }
      bodySeen |= id == bodyColumnSegmentId(encoded.column());
      previous = id;
    }
    if (!bodySeen) {
      throw new IllegalArgumentException("encoded column has no BODY segment");
    }
  }

  static byte expectedSegmentKind(final int columnSegmentId) {
    if (columnSegmentId == keysColumnSegmentId())
      return SEG_KIND_KEYS;
    if (columnSegmentId >= DICT_HASH_SEGMENT_BASE
        && columnSegmentId < DICT_HASH_SEGMENT_BASE + RowGroupDescriptor.MAX_COLUMNS) {
      return SEG_KIND_DICT_HASHES;
    }
    if (columnSegmentId < 1 || columnSegmentId > SEGMENTS_PER_COLUMN * RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalArgumentException("unknown projection column segment id " + columnSegmentId);
    }
    return switch ((columnSegmentId - 1) % SEGMENTS_PER_COLUMN) {
      case 0 -> SEG_KIND_BODY;
      case 1 -> SEG_KIND_DICT;
      case 2 -> SEG_KIND_SET_COUNTS;
      case 3 -> SEG_KIND_STRING_BLOOM;
      default -> throw new AssertionError();
    };
  }

  private static @Nullable EncodedRowGroup encodeWithWorkspace(final byte @Nullable [] rawPayload) {
    if (rawPayload == null) {
      // Null denotes an absent row group at this optional encoding boundary.
      return null;
    }
    final EncodeWorkspace workspace = acquireEncodeWorkspace();
    try {
      return encodeWithWorkspace(rawPayload, workspace);
    } finally {
      releaseEncodeWorkspace(workspace);
    }
  }

  private static @Nullable EncodedRowGroup encodeWithWorkspace(final byte @Nullable [] rawPayload,
      final EncodeWorkspace workspace) {
    if (rawPayload == null) {
      return null;
    }
    if (workspace == null) {
      throw new NullPointerException("workspace must not be null");
    }
    workspace.claim();
    try {
      return encodeClaimed(ProjectionIndexRowGroupPage.deserialize(rawPayload), workspace);
    } finally {
      workspace.release();
    }
  }

  /** Encode body entered only while {@code workspace} is exclusively claimed by the caller. */
  private static EncodedRowGroup encodeClaimed(final ProjectionIndexRowGroupPage page,
      final EncodeWorkspace workspace) {
    final int rowCount = page.getRowCount();
    final int columnCount = page.getColumnCount();
    if (columnCount > RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalStateException(
          "columnCount " + columnCount + " exceeds MAX_COLUMNS=" + RowGroupDescriptor.MAX_COLUMNS);
    }

    // KEYS once, then per column: BODY, an optional DICT, an optional SET_COUNTS, an optional BLOOM,
    // and an optional DICT_HASHES. Sized for the maximum a column can emit — under-sizing this
    // overflows the parallel arrays below rather than dropping a segment, which is how the SET_COUNTS
    // addition first showed up.
    final int maxColumnSegments = 1 + SEGMENT_ID_SLOTS_PER_COLUMN * columnCount;
    final int[] columnSegmentIds = new int[maxColumnSegments];
    final byte[][] segments = new byte[maxColumnSegments][];
    final byte[] entryFlags = new byte[maxColumnSegments];
    final long[] entryMins = new long[maxColumnSegments];
    final long[] entryMaxs = new long[maxColumnSegments];
    int columnSegmentCount = 0;

    // KEYS segment.
    {
      final ByteArrayOutputStream out = newColumnSegmentStream(SEG_KIND_KEYS, workspace);
      ProjectionIndexRowGroupCodec.putLongLE(out, page.firstRecordKey());
      ProjectionIndexRowGroupCodec.putLongLE(out, page.lastRecordKey());
      if (rowCount > 0) {
        ProjectionIndexRowGroupCodec.encodeRecordKeys(out, page.recordKeys(), rowCount);
      }
      final long[] orderExceptionBits = page.orderExceptionBits();
      if (orderExceptionBits == null) {
        out.write(ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_NONE);
      } else {
        out.write(ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_DENSE);
        final int orderWords = (rowCount + 63) >>> 6;
        for (int word = 0; word < orderWords; word++) {
          ProjectionIndexRowGroupCodec.putLongLE(out, orderExceptionBits[word]);
        }
      }
      ProjectionIndexRowGroupCodec.encodeOrderLabels(out, page);
      columnSegmentIds[columnSegmentCount] = keysColumnSegmentId();
      segments[columnSegmentCount] = out.toByteArray();
      columnSegmentCount++;
    }

    final byte[] kinds = new byte[columnCount];
    for (int c = 0; c < columnCount; c++) {
      kinds[c] = page.columnKind(c);

      byte flags = page.columnUnrepresentable(c)
          ? ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE
          : 0;
      if (page.columnNumericNonIntegral(c)) {
        flags |= ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL;
      }
      if (page.columnPureDoubleSource(c)) {
        flags |= ProjectionIndexRowGroupPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE;
      }

      // BODY segment.
      final ByteArrayOutputStream body = newColumnSegmentStream(SEG_KIND_BODY, workspace);
      body.write(flags);
      if (rowCount > 0) {
        ProjectionIndexRowGroupCodec.putLongLE(body, page.columnMin(c));
        ProjectionIndexRowGroupCodec.putLongLE(body, page.columnMax(c));
        ProjectionIndexRowGroupCodec.encodePresence(body, page.presenceColumnBits(c), rowCount);
        switch (kinds[c]) {
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
              ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL,
              ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP, ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
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
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> ProjectionIndexRowGroupCodec.encodeDictIds(body,
              page.stringDictionarySize(c), page.stringDictIdColumn(c), rowCount);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
            // Counts are ids in their own right — bit-packed to the widest count on the leaf — and
            // the element run reuses the dictionary width. Two packed runs, no lengths beyond the
            // counts themselves.
            final int[] counts = page.stringSetCountColumn(c);
            int maxCount = 0;
            for (int r = 0; r < rowCount; r++) {
              if (counts[r] > maxCount) {
                maxCount = counts[r];
              }
            }
            ProjectionIndexRowGroupCodec.encodePackedIds(body, counts, rowCount, maxCount);
            ProjectionIndexRowGroupCodec.encodeDictIds(body, page.stringDictionarySize(c), page.stringSetIdColumn(c),
                page.stringSetLength(c));
          }
          default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
        }
      }
      columnSegmentIds[columnSegmentCount] = bodyColumnSegmentId(c);
      segments[columnSegmentCount] = body.toByteArray();
      entryFlags[columnSegmentCount] = flags;
      // Empty leaf: mirror the zone-map sentinel pair (min > max = "no present value"), the
      // same discipline appendRow initialises — a fabricated [0, 0] would defeat descriptor
      // pruning and read as "possibly contains 0".
      entryMins[columnSegmentCount] = rowCount > 0
          ? page.columnMin(c)
          : Long.MAX_VALUE;
      entryMaxs[columnSegmentCount] = rowCount > 0
          ? page.columnMax(c)
          : Long.MIN_VALUE;
      columnSegmentCount++;

      // DICT segment (string columns with rows only).
      if ((kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
          || kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) && rowCount > 0) {
        final ByteArrayOutputStream dict = newColumnSegmentStream(SEG_KIND_DICT, workspace);
        encodeDictColumnSegmentPayload(dict, page, c, kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET
            ? valueRowCounts(page.stringDictionarySize(c), page.stringSetCountColumn(c), page.stringSetIdColumn(c),
                rowCount)
            : null, workspace);
        columnSegmentIds[columnSegmentCount] = dictColumnSegmentId(c);
        segments[columnSegmentCount] = dict.toByteArray();
        columnSegmentCount++;

        // SET_COUNTS segment: the values with their per-value ROW counts, emitted only while the
        // dedicated segment slot can keep them inline so a membership count reads no overflow page.
        if (kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
          final byte[] counts = encodeSetCountsPayload(page, c, valueRowCounts(page.stringDictionarySize(c),
              page.stringSetCountColumn(c), page.stringSetIdColumn(c), rowCount), workspace);
          if (counts != null) {
            columnSegmentIds[columnSegmentCount] = setCountsColumnSegmentId(c);
            segments[columnSegmentCount] = counts;
            columnSegmentCount++;
          }
        }

        // STRING_BLOOM segment: equality fingerprint over the leaf's distinct values.
        columnSegmentIds[columnSegmentCount] = bloomColumnSegmentId(c);
        segments[columnSegmentCount] = encodeStringBloomPayload(page, c, workspace);
        columnSegmentCount++;
      }
    }

    // DICT_HASHES, in a TRAILING pass: their ids live above every stride id, and descriptor entries
    // must ascend. STRING_DICT only — it is the one kind whose per-row ids a distinct fold reads as
    // content identities; a STRING_SET column's dictionary is consumed as membership values (bytes),
    // so hashes would cost a page per leaf and answer nothing.
    for (int c = 0; c < columnCount; c++) {
      if (kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT && rowCount > 0) {
        columnSegmentIds[columnSegmentCount] = dictHashColumnSegmentId(c);
        segments[columnSegmentCount] = encodeDictHashesPayload(page, c);
        columnSegmentCount++;
      }
    }

    final int[] byteLens = new int[columnSegmentCount];
    final long[] hashes = new long[columnSegmentCount];
    for (int i = 0; i < columnSegmentCount; i++) {
      byteLens[i] = segments[i].length;
      hashes[i] = contentHash(segments[i]);
    }
    final byte[] descriptor = RowGroupDescriptor.serialize(rowCount, page.firstRecordKey(), page.lastRecordKey(), kinds,
        columnSegmentCount, columnSegmentIds, byteLens, hashes, entryFlags, entryMins, entryMaxs);

    final int[] idsTrimmed = new int[columnSegmentCount];
    System.arraycopy(columnSegmentIds, 0, idsTrimmed, 0, columnSegmentCount);
    final byte[][] segsTrimmed = new byte[columnSegmentCount][];
    System.arraycopy(segments, 0, segsTrimmed, 0, columnSegmentCount);
    return new EncodedRowGroup(descriptor, idsTrimmed, segsTrimmed);
  }

  /** Filter-size clamp: [512, 16384] bits = [64 B, 2 KiB] of words. */
  private static final int BLOOM_MIN_BITS = 512;
  private static final int BLOOM_MAX_BITS = 16384;

  /**
   * Build the {@link #SEG_KIND_STRING_BLOOM} payload over the leaf's distinct dictionary values.
   * Layout after the shared segment header: {@code int mBits; long[mBits/64] words}.
   */
  private static byte[] encodeStringBloomPayload(final ProjectionIndexRowGroupPage page, final int column,
      final EncodeWorkspace workspace) {
    final int count = page.stringDictionarySize(column);
    int mBits = Integer.highestOneBit(Math.max(1, count * 10));
    if (mBits < count * 10) {
      mBits <<= 1; // next power of two at ~10 bits/value
    }
    mBits = Math.max(BLOOM_MIN_BITS, Math.min(BLOOM_MAX_BITS, mBits));
    final long[] words = new long[mBits >>> 6];
    final int mask = mBits - 1;
    for (int i = 0; i < count; i++) {
      final long h = fnv64(page.stringDictionaryEntryBacking(column, i), page.stringDictionaryEntryOffset(column, i),
          page.stringDictionaryEntryLength(column, i));
      words[(int) ((h & mask) >>> 6)] |= 1L << (h & 63);
      final long h2 = h >>> 21;
      words[(int) ((h2 & mask) >>> 6)] |= 1L << (h2 & 63);
      final long h3 = h >>> 42;
      words[(int) ((h3 & mask) >>> 6)] |= 1L << (h3 & 63);
    }
    final ByteArrayOutputStream out = newColumnSegmentStream(SEG_KIND_STRING_BLOOM, workspace);
    ProjectionIndexRowGroupCodec.putIntLE(out, mBits);
    for (final long w : words) {
      ProjectionIndexRowGroupCodec.putLongLE(out, w);
    }
    return out.toByteArray();
  }

  /**
   * Build the {@link #SEG_KIND_DICT_HASHES} payload: {@code int dictSize; long[dictSize] hashes},
   * hashes in dict-id order.
   *
   * <p>
   * The hash is {@link ProjectionIndexByteScan#fnv1a64} over the entry's PLAIN UTF-8 bytes — the same
   * function, over the same bytes, that a reader would apply to the decoded DICT entry. Identity is
   * therefore bit-for-bit identical whether a fold reads this optional acceleration segment or
   * hashes decoded dictionary entries.
   */
  private static byte[] encodeDictHashesPayload(final ProjectionIndexRowGroupPage page, final int column) {
    final int dictSize = page.stringDictionarySize(column);
    final byte[] out = new byte[SEGMENT_HEADER_BYTES + Integer.BYTES + dictSize * Long.BYTES];
    ProjectionIndexRowGroupCodec.putIntLEAt(out, 0, SEGMENT_MAGIC);
    out[4] = SEGMENT_VERSION;
    out[5] = SEG_KIND_DICT_HASHES;
    ProjectionIndexRowGroupCodec.putIntLEAt(out, SEGMENT_HEADER_BYTES, dictSize);
    int off = SEGMENT_HEADER_BYTES + Integer.BYTES;
    for (int i = 0; i < dictSize; i++) {
      ProjectionIndexRowGroupCodec.putLongLEAt(out, off,
          ProjectionIndexByteScan.fnv1a64(page.stringDictionaryEntryBacking(column, i),
              page.stringDictionaryEntryOffset(column, i), page.stringDictionaryEntryLength(column, i)));
      off += Long.BYTES;
    }
    return out;
  }

  /**
   * Decode a {@link #SEG_KIND_DICT_HASHES} segment into {@code dictId -> hash}, verifying it against
   * its descriptor entry exactly as every other segment read does.
   *
   * @param dictHashColumnSegment the segment's bytes, or {@code null} for a leaf that has none
   * @return the hashes, or {@code null} when this leaf carries no such segment (the caller falls back
   *         to hashing decoded dictionary entries)
   * @throws IllegalStateException on verification or parse failure
   */
  static long @Nullable [] decodeDictHashes(final byte[] descriptor, final byte @Nullable [] dictHashColumnSegment,
      final int col) {
    final int segId = dictHashColumnSegmentId(col);
    if (RowGroupDescriptor.entryIndexOf(descriptor, segId) < 0) {
      return null;
    }
    final ProjectionIndexRowGroupCodec.Cursor in = openColumnSegment(descriptor, id -> id == segId
        ? dictHashColumnSegment
        : null, segId, SEG_KIND_DICT_HASHES);
    final int dictSize = in.readInt();
    final byte[] buf = in.buffer();
    final int base = in.position();
    if (dictSize < 0 || base + (long) dictSize * Long.BYTES > buf.length) {
      throw new IllegalStateException(
          "Truncated DICT_HASHES segment for column " + col + ": dictSize=" + dictSize + " len=" + buf.length);
    }
    final long[] hashes = new long[dictSize];
    for (int i = 0; i < dictSize; i++) {
      hashes[i] = ProjectionIndexRowGroupCodec.getLongLE(buf, base + i * Long.BYTES);
    }
    return hashes;
  }

  /**
   * Probe a {@link #SEG_KIND_STRING_BLOOM} payload for {@code literalUtf8}. No false negatives:
   * {@code false} PROVES the value is absent from the leaf; {@code true} means "fetch and check".
   * Defensive on malformed input — anything unexpected answers {@code true}, i.e. no pruning.
   */
  public static boolean bloomMayContain(final byte[] segment, final byte[] literalUtf8) {
    return bloomMayContainHash(segment, bloomHash(literalUtf8));
  }

  /**
   * The literal's fingerprint hash, hoisted so a scan over N leaves hashes ONCE and probes N times —
   * the per-leaf work is three word loads and three bit tests.
   */
  public static long bloomHash(final byte[] literalUtf8) {
    return fnv64(literalUtf8);
  }

  /** Probe with a pre-computed {@link #bloomHash}. Same no-false-negative contract. */
  public static boolean bloomMayContainHash(final byte[] segment, final long h) {
    return segment == null || bloomMayContainHashAt(segment, 0, segment.length, h);
  }

  private static boolean bloomMayContainHashAt(final byte[] a, final int off, final int len, final long h) {
    if (!bloomSegmentIsWellFormedAt(a, off, len)) {
      return true;
    }
    final int mBits = ProjectionIndexRowGroupCodec.getIntLE(a, off + SEGMENT_HEADER_BYTES);
    final int base = off + SEGMENT_HEADER_BYTES + Integer.BYTES;
    final int mask = mBits - 1;
    return bloomBit(a, base, h & mask) && bloomBit(a, base, (h >>> 21) & mask) && bloomBit(a, base, (h >>> 42) & mask);
  }

  /** Structural validation shared by direct and block probes; malformed evidence always keeps. */
  private static boolean bloomSegmentIsWellFormedAt(final byte[] a, final int off, final int len) {
    if (off < 0 || len < SEGMENT_HEADER_BYTES + Integer.BYTES || off + (long) len > a.length
        || ProjectionIndexRowGroupCodec.getIntLE(a, off) != SEGMENT_MAGIC || a[off + Integer.BYTES] != SEGMENT_VERSION
        || a[off + Integer.BYTES + 1] != SEG_KIND_STRING_BLOOM) {
      return false;
    }
    final int mBits = ProjectionIndexRowGroupCodec.getIntLE(a, off + SEGMENT_HEADER_BYTES);
    return mBits >= BLOOM_MIN_BITS && mBits <= BLOOM_MAX_BITS && Integer.bitCount(mBits) == 1
        && len == SEGMENT_HEADER_BYTES + Integer.BYTES + (mBits >>> 3);
  }

  private static boolean bloomBit(final byte[] segment, final int base, final long bit) {
    final long word = ProjectionIndexRowGroupCodec.getLongLE(segment, base + (int) ((bit >>> 6) << 3));
    return (word & 1L << (bit & 63)) != 0;
  }

  // ==================== fingerprint BLOCK ====================
  // One contiguous blob per string column concatenating every leaf's fingerprint segment.
  // Motivation (measured): the per-leaf fingerprint chain is ~2 KiB pages STRIDED between the fat
  // BODY/DICT pages, so a cold chain fetch degenerates to one scattered pread per leaf and cost
  // MORE than the pruning saved (S6 381 -> ~950 ms). As one blob the same bytes are one
  // sequential read. The per-leaf segments remain the WRITTEN truth; the block is a derived
  // acceleration — deleted on incremental maintenance, rebuilt by the next full build — and
  // readers without it fall back to the chain.

  /** Block header: magic + version + leafCount, then (leafCount+1) int offsets, then payloads. */
  private static final int BLOOM_BLOCK_MAGIC = 0x50424C4D; // "PBLM"
  private static final byte BLOOM_BLOCK_VERSION = 0;
  private static final int BLOOM_BLOCK_HEADER_BYTES = Integer.BYTES + 1 + Integer.BYTES;

  /**
   * Largest structurally valid contiguous fingerprint block for {@code leafCount} leaves.
   *
   * <p>
   * Deferred Bloom-chunk readers use this before fetching a referenced PIXB payload. The PIXB marker
   * is untrusted; without this independent format bound a corrupt length could make an optional
   * pruning acceleration retain an arbitrarily large overflow page. Every non-empty leaf can
   * contribute at most one maximum-sized {@code STRING_BLOOM} segment.
   * </p>
   */
  static int maxBloomBlockBytes(final int leafCount) {
    if (leafCount < 0 || leafCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      throw new IllegalArgumentException("leafCount out of range: " + leafCount);
    }
    final long maxSegmentBytes = SEGMENT_HEADER_BYTES + Integer.BYTES + (BLOOM_MAX_BITS >>> 3);
    final long bytes = BLOOM_BLOCK_HEADER_BYTES + (leafCount + 1L) * Integer.BYTES + leafCount * maxSegmentBytes;
    if (bytes > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) bytes;
  }

  /** Cheap marker-length gate used before a deferred Bloom-chunk payload is read. */
  static boolean bloomBlockLengthCouldBeWellFormed(final int byteLength, final int leafCount) {
    if (leafCount < 0 || leafCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      return false;
    }
    final long minimum = BLOOM_BLOCK_HEADER_BYTES + (leafCount + 1L) * Integer.BYTES;
    return byteLength >= minimum && byteLength <= maxBloomBlockBytes(leafCount);
  }

  /**
   * Concatenate per-leaf fingerprint segments (index = rowGroupId - 1; {@code null} = the leaf has
   * none, e.g. rowless) into one block. Returns {@code null} when NO leaf carries a fingerprint —
   * writing an all-empty block would cost a slot and prove nothing.
   */
  public static byte @Nullable [] encodeBloomBlock(final byte @Nullable [] @Nullable [] perLeaf, final int leafCount) {
    int total = 0;
    boolean any = false;
    for (int i = 0; i < leafCount; i++) {
      final byte[] seg = perLeaf[i];
      if (seg != null) {
        total += seg.length;
        any = true;
      }
    }
    if (!any) {
      return null;
    }
    final byte[] block = new byte[BLOOM_BLOCK_HEADER_BYTES + (leafCount + 1) * Integer.BYTES + total];
    ProjectionIndexRowGroupCodec.putIntLEAt(block, 0, BLOOM_BLOCK_MAGIC);
    block[Integer.BYTES] = BLOOM_BLOCK_VERSION;
    ProjectionIndexRowGroupCodec.putIntLEAt(block, Integer.BYTES + 1, leafCount);
    int payload = 0;
    final int tableBase = BLOOM_BLOCK_HEADER_BYTES;
    final int payloadBase = tableBase + (leafCount + 1) * Integer.BYTES;
    for (int i = 0; i < leafCount; i++) {
      ProjectionIndexRowGroupCodec.putIntLEAt(block, tableBase + i * Integer.BYTES, payload);
      final byte[] seg = perLeaf[i];
      if (seg != null) {
        System.arraycopy(seg, 0, block, payloadBase + payload, seg.length);
        payload += seg.length;
      }
    }
    ProjectionIndexRowGroupCodec.putIntLEAt(block, tableBase + leafCount * Integer.BYTES, payload);
    return block;
  }

  /** Leaf count a block declares, or {@code -1} when it is not a well-formed block. */
  public static int bloomBlockLeafCount(final byte @Nullable [] block) {
    if (block == null || block.length < BLOOM_BLOCK_HEADER_BYTES
        || ProjectionIndexRowGroupCodec.getIntLE(block, 0) != BLOOM_BLOCK_MAGIC
        || block[Integer.BYTES] != BLOOM_BLOCK_VERSION) {
      return -1;
    }
    final int leafCount = ProjectionIndexRowGroupCodec.getIntLE(block, Integer.BYTES + 1);
    if (leafCount < 0 || block.length < BLOOM_BLOCK_HEADER_BYTES + (leafCount + 1L) * Integer.BYTES) {
      return -1;
    }
    return leafCount;
  }

  /**
   * Exact structural validation for a contiguous fingerprint block. Empty slices are valid (they mean
   * "no evidence, keep"); every non-empty slice must be one exact STRING_BLOOM segment. This gate is
   * used before a persisted block is trusted, so an offset table corrupted into unrelated payload
   * bytes can never manufacture negative evidence.
   */
  public static boolean bloomBlockIsWellFormed(final byte @Nullable [] block, final int expectedLeafCount) {
    if (expectedLeafCount < 0 || bloomBlockLeafCount(block) != expectedLeafCount) {
      return false;
    }
    final int tableBase = BLOOM_BLOCK_HEADER_BYTES;
    final long payloadBaseLong = tableBase + (expectedLeafCount + 1L) * Integer.BYTES;
    if (payloadBaseLong > block.length) {
      return false;
    }
    final int payloadBase = (int) payloadBaseLong;
    final int payloadBytes = block.length - payloadBase;
    int previous = ProjectionIndexRowGroupCodec.getIntLE(block, tableBase);
    if (previous != 0) {
      return false;
    }
    for (int i = 0; i < expectedLeafCount; i++) {
      final int end = ProjectionIndexRowGroupCodec.getIntLE(block, tableBase + (i + 1) * Integer.BYTES);
      if (end < previous || end > payloadBytes) {
        return false;
      }
      if (end != previous && !bloomSegmentIsWellFormedAt(block, payloadBase + previous, end - previous)) {
        return false;
      }
      previous = end;
    }
    return previous == payloadBytes;
  }

  static byte @Nullable [] @Nullable [] copyBloomBlockSlices(final byte @Nullable [] block,
      final int expectedLeafCount) {
    if (!bloomBlockIsWellFormed(block, expectedLeafCount)) {
      return null;
    }
    final byte[][] slices = new byte[expectedLeafCount][];
    final int tableBase = BLOOM_BLOCK_HEADER_BYTES;
    final int payloadBase = tableBase + (expectedLeafCount + 1) * Integer.BYTES;
    for (int i = 0; i < expectedLeafCount; i++) {
      final int start = ProjectionIndexRowGroupCodec.getIntLE(block, tableBase + i * Integer.BYTES);
      final int end = ProjectionIndexRowGroupCodec.getIntLE(block, tableBase + (i + 1) * Integer.BYTES);
      if (end > start) {
        slices[i] = Arrays.copyOfRange(block, payloadBase + start, payloadBase + end);
      }
    }
    return slices;
  }

  /**
   * Probe leaf {@code i} of a validated block ({@link #bloomBlockLeafCount} {@code >= i+1}) with a
   * pre-computed {@link #bloomHash}. A leaf without a fingerprint (empty slice) answers {@code true}
   * — no evidence, no prune.
   */
  public static boolean bloomBlockMayContainHash(final byte[] block, final int i, final long h) {
    final int leafCount = bloomBlockLeafCount(block);
    if (i < 0 || i >= leafCount) {
      return true;
    }
    final int tableBase = BLOOM_BLOCK_HEADER_BYTES;
    final int off = ProjectionIndexRowGroupCodec.getIntLE(block, tableBase + i * Integer.BYTES);
    final int end = ProjectionIndexRowGroupCodec.getIntLE(block, tableBase + (i + 1) * Integer.BYTES);
    final long payloadBase = tableBase + (leafCount + 1L) * Integer.BYTES;
    if (off < 0 || end <= off || payloadBase + end > block.length) {
      return true;
    }
    return bloomMayContainHashAt(block, (int) payloadBase + off, end - off, h);
  }

  /**
   * Allocation-free probe after one successful {@link #bloomBlockIsWellFormed} gate.
   *
   * <p>
   * The chunk pruner validates the whole block once, then probes up to 256 leaves. Re-running the
   * magic, offset, and segment-shape checks for every leaf doubled the hot work without increasing
   * safety: the payload is immutable for the duration of the call and was just hash-verified.
   * </p>
   */
  static boolean bloomBlockMayContainHashValidated(final byte[] block, final int leafIndex, final int leafCount,
      final long hash) {
    final int tableBase = BLOOM_BLOCK_HEADER_BYTES;
    final int off = ProjectionIndexRowGroupCodec.getIntLE(block, tableBase + leafIndex * Integer.BYTES);
    final int end = ProjectionIndexRowGroupCodec.getIntLE(block, tableBase + (leafIndex + 1) * Integer.BYTES);
    if (end == off) {
      return true;
    }
    final int segmentOffset = tableBase + (leafCount + 1) * Integer.BYTES + off;
    final int mBits = ProjectionIndexRowGroupCodec.getIntLE(block, segmentOffset + SEGMENT_HEADER_BYTES);
    final int wordsOffset = segmentOffset + SEGMENT_HEADER_BYTES + Integer.BYTES;
    final int mask = mBits - 1;
    return bloomBit(block, wordsOffset, hash & mask) && bloomBit(block, wordsOffset, (hash >>> 21) & mask)
        && bloomBit(block, wordsOffset, (hash >>> 42) & mask);
  }

  /**
   * Locate leaf {@code leafIndex}'s fingerprint WORDS in a validated block, packed as
   * {@code (mBits << 32) | wordsOffset}, or {@link #NO_FINGERPRINT} when the leaf carries none (no
   * evidence — every probe answers "may contain"). One location serves any number of probes through
   * {@link #bloomWordsMayContainHash}: a pass that prices MANY literals against the same leaf (a
   * disjunction of equalities, a planner ranking candidate group values) pays the offset-table walk
   * once per leaf instead of once per literal.
   */
  static long bloomBlockLeafWords(final byte[] block, final int leafIndex, final int leafCount) {
    final int tableBase = BLOOM_BLOCK_HEADER_BYTES;
    final int off = ProjectionIndexRowGroupCodec.getIntLE(block, tableBase + leafIndex * Integer.BYTES);
    final int end = ProjectionIndexRowGroupCodec.getIntLE(block, tableBase + (leafIndex + 1) * Integer.BYTES);
    if (end == off) {
      return NO_FINGERPRINT;
    }
    final int segmentOffset = tableBase + (leafCount + 1) * Integer.BYTES + off;
    final int mBits = ProjectionIndexRowGroupCodec.getIntLE(block, segmentOffset + SEGMENT_HEADER_BYTES);
    final int wordsOffset = segmentOffset + SEGMENT_HEADER_BYTES + Integer.BYTES;
    return ((long) mBits << 32) | wordsOffset;
  }

  /**
   * Locate a stand-alone fingerprint segment's words (the per-leaf chain path), packed like
   * {@link #bloomBlockLeafWords}; a malformed or absent segment is {@link #NO_FINGERPRINT}.
   */
  static long bloomSegmentWords(final byte[] segment) {
    if (segment == null || !bloomSegmentIsWellFormedAt(segment, 0, segment.length)) {
      return NO_FINGERPRINT;
    }
    final int mBits = ProjectionIndexRowGroupCodec.getIntLE(segment, SEGMENT_HEADER_BYTES);
    return ((long) mBits << 32) | (SEGMENT_HEADER_BYTES + Integer.BYTES);
  }

  /** {@link #bloomBlockLeafWords} / {@link #bloomSegmentWords} answer for a leaf without evidence. */
  static final long NO_FINGERPRINT = -1L;

  /** Probe words located by {@link #bloomBlockLeafWords} or {@link #bloomSegmentWords}. */
  static boolean bloomWordsMayContainHash(final byte[] bytes, final long packedWords, final long hash) {
    final int mask = (int) (packedWords >>> 32) - 1;
    final int wordsOffset = (int) packedWords;
    return bloomBit(bytes, wordsOffset, hash & mask) && bloomBit(bytes, wordsOffset, (hash >>> 21) & mask)
        && bloomBit(bytes, wordsOffset, (hash >>> 42) & mask);
  }

  /** FNV-1a 64 over the value bytes — the fingerprint's one hash; probes derive from it. */
  private static long fnv64(final byte[] bytes) {
    return fnv64(bytes, 0, bytes.length);
  }

  private static long fnv64(final byte[] bytes, final int offset, final int length) {
    long h = 0xcbf29ce484222325L;
    final int end = offset + length;
    for (int i = offset; i < end; i++) {
      h = (h ^ (bytes[i] & 0xFF)) * 0x100000001b3L;
    }
    return h;
  }

  private static ByteArrayOutputStream newColumnSegmentStream(final byte segKind, final EncodeWorkspace workspace) {
    return workspace.beginSegment(segKind);
  }

  // ==================== assemble (decode) ====================

  /**
   * Reassemble the raw scan form from a descriptor and its segments, byte-identically to the original
   * {@link ProjectionIndexRowGroupPage#serialize()} output. Verifies each segment's byteLen +
   * contentHash against the descriptor before parsing.
   *
   * @throws IllegalStateException on any missing segment, length/hash mismatch, or malformed segment
   *         bytes — corruption is caught here, at fill time, never mid-kernel
   */
  public static byte[] assembleRaw(final byte[] descriptor, final SegmentResolver resolver) {
    RowGroupDescriptor.validate(descriptor);
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final int columnCount = RowGroupDescriptor.columnCount(descriptor);
    final byte[] kinds = new byte[columnCount];
    for (int c = 0; c < columnCount; c++) {
      kinds[c] = RowGroupDescriptor.kind(descriptor, c);
    }
    // KEYS.
    final ProjectionIndexRowGroupCodec.Cursor keys =
        openColumnSegment(descriptor, resolver, keysColumnSegmentId(), SEG_KIND_KEYS);
    final long firstRecordKey = keys.readLong();
    final long lastRecordKey = keys.readLong();
    final long[] recordKeys = rowCount > 0
        ? ProjectionIndexRowGroupCodec.decodeRecordKeys(keys, rowCount)
        : new long[0];
    final byte orderKind = keys.readByte();
    final long[] orderExceptionBits;
    if (orderKind == ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_NONE) {
      orderExceptionBits = null;
    } else if (orderKind == ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_DENSE && rowCount > 0) {
      orderExceptionBits = new long[(rowCount + 63) >>> 6];
      for (int word = 0; word < orderExceptionBits.length; word++) {
        orderExceptionBits[word] = keys.readLong();
      }
    } else {
      throw new IllegalStateException("unknown projection order-exception kind " + orderKind);
    }
    final ProjectionIndexRowGroupCodec.OrderLabels orderLabels =
        ProjectionIndexRowGroupCodec.decodeOrderLabels(keys, rowCount);
    ProjectionIndexRowGroupPage.validateOrderMetadata(rowCount, firstRecordKey, lastRecordKey, recordKeys,
        orderExceptionBits);

    final long[] columnMin = new long[columnCount];
    final long[] columnMax = new long[columnCount];
    final long[][] numericCols = new long[columnCount][];
    final long[][] booleanCols = new long[columnCount][];
    final int[][] dictIdCols = new int[columnCount][];
    final byte[][][] dicts = new byte[columnCount][][];
    final int[][] setCountCols = new int[columnCount][];
    final int[][] setElemCols = new int[columnCount][];
    final byte[] columnFlags = new byte[columnCount];
    final long[][] presence = new long[columnCount][];
    final int presWords = rowCount > 0
        ? (rowCount + 63) >>> 6
        : 0;

    for (int c = 0; c < columnCount; c++) {
      final ProjectionIndexRowGroupCodec.Cursor body =
          openColumnSegment(descriptor, resolver, bodyColumnSegmentId(c), SEG_KIND_BODY);
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
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
            ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
            ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL,
            ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP, ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
          numericCols[c] = ProjectionIndexRowGroupCodec.decodeForBitPackedColumn(body, rowCount);
        case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN ->
          booleanCols[c] = ProjectionIndexRowGroupCodec.decodeBooleanWords(body, presWords);
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
          final ProjectionIndexRowGroupCodec.Cursor dictCur =
              openColumnSegment(descriptor, resolver, dictColumnSegmentId(c), SEG_KIND_DICT);
          dicts[c] = decodeDictColumnSegmentPayload(dictCur);
          dictIdCols[c] = ProjectionIndexRowGroupCodec.decodePackedIds(body, rowCount);
        }
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
          final ProjectionIndexRowGroupCodec.Cursor dictCur =
              openColumnSegment(descriptor, resolver, dictColumnSegmentId(c), SEG_KIND_DICT);
          dicts[c] = decodeDictColumnSegmentPayload(dictCur);
          // Counts first, then the flat element run whose length is their sum — the same order the
          // encoder writes, and the reason no separate total is stored.
          final int[] counts = ProjectionIndexRowGroupCodec.decodePackedIds(body, rowCount);
          int total = 0;
          for (int r = 0; r < rowCount; r++) {
            total += counts[r];
          }
          setCountCols[c] = counts;
          setElemCols[c] = ProjectionIndexRowGroupCodec.decodePackedIds(body, total);
        }
        default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
      }
    }

    final byte[] direct = writeRawDirect(rowCount, columnCount, kinds, firstRecordKey, lastRecordKey, recordKeys,
        orderExceptionBits, orderLabels.bytes(), orderLabels.offsets(), columnMin, columnMax, numericCols, booleanCols,
        dictIdCols, dicts, setCountCols, setElemCols, columnFlags, presence, presWords);
    if (verifyDirectAssembly) {
      final ProjectionIndexRowGroupPage page =
          ProjectionIndexRowGroupPage.reconstruct(kinds, rowCount, firstRecordKey, lastRecordKey, recordKeys,
              orderExceptionBits, orderLabels.bytes(), orderLabels.offsets(), orderLabels.bytes().length, columnMin,
              columnMax, numericCols, booleanCols, dictIdCols, dicts, setCountCols, setElemCols, presence, columnFlags);
      final byte[] viaPage = page.serialize();
      if (!Arrays.equals(direct, viaPage)) {
        throw new IllegalStateException("Direct raw assembly diverged from the page-based path (" + direct.length
            + " vs " + viaPage.length + " bytes) — layout drift");
      }
    }
    return direct;
  }

  /**
   * Cross-check switch for {@link #writeRawDirect}: when set, every assembly ALSO runs the historical
   * reconstruct-then-serialize path and fails loudly on any byte difference. Off in production (the
   * direct writer is the hydrate hot path); package-private and mutable so
   * {@code ProjectionColumnScanParityTest#directAssemblyMatchesPageSerialization} can pin the parity
   * in CI — the system property remains a manual diagnostic override.
   */
  static volatile boolean verifyDirectAssembly = Boolean.getBoolean("sirix.projection.verifyDirectAssembly");

  /**
   * Single-buffer raw-form writer — byte-identical to
   * {@code ProjectionIndexRowGroupPage.reconstruct(...).serialize()} but with the exact output size
   * precomputed and every array bulk-copied ({@code LongBuffer.put(long[])} is an intrinsified
   * memcpy), instead of a page object, a growing {@code ByteArrayOutputStream}, and per-value
   * {@code putLong} calls. Measured 2-3x on the hydrate assemble phase, which dominates cold-open
   * cost.
   */
  /**
   * The exact byte length {@link #writeRawDirect} will produce, computed before a single byte is
   * written so the payload can be built into one right-sized array with no growth or copy.
   */
  private static int rawDirectByteSize(final int rowCount, final int columnCount, final byte[] kinds,
      final byte[][][] dicts, final int[][] setElemCols, final int presWords, final boolean hasOrderExceptions,
      final int orderLabelBytes) {
    int size = 8 + 16 + columnCount + 1; // header + order-exception kind
    size += Integer.BYTES + (rowCount + 1) * Integer.BYTES + orderLabelBytes;
    if (rowCount > 0) {
      size += rowCount * 8; // record keys
      if (hasOrderExceptions) {
        size += presWords * 8; // sparse document-order exception bitmap
      }
      for (int c = 0; c < columnCount; c++) {
        size += 16; // min/max
        size += switch (kinds[c]) {
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
              ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
              ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL,
              ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP, ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
            rowCount * 8;
          case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> presWords * 8;
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> dictionaryByteSize(dicts[c]) + rowCount * 4;
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET ->
            dictionaryByteSize(dicts[c]) + rowCount * 4 + setElemCols[c].length * 4;
          default -> throw new IllegalStateException("Unknown column kind " + kinds[c]);
        };
      }
    }
    return size + columnCount + columnCount * presWords * 8 + 9; // presence tail + footer
  }

  /** Writes what {@link #dictionaryByteSize} measures: count, per-entry lengths, then the entries. */
  private static void putDictionary(final ByteBuffer bb, final byte[][] dict) {
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
  }

  /** Bulk int write through an {@link IntBuffer} view, advancing the backing buffer past it. */
  private static void putIntsBulk(final ByteBuffer bb, final int[] values, final int count) {
    final IntBuffer ib = bb.asIntBuffer();
    ib.put(values, 0, count);
    bb.position(bb.position() + count * 4);
  }

  /** Entry count, per-entry lengths, and the entries themselves. */
  private static int dictionaryByteSize(final byte[][] dict) {
    int dictSize = 0;
    int dictBytes = 0;
    while (dictSize < dict.length && dict[dictSize] != null) {
      dictBytes += dict[dictSize].length;
      dictSize++;
    }
    return 4 + dictSize * 4 + dictBytes;
  }

  private static byte[] writeRawDirect(final int rowCount, final int columnCount, final byte[] kinds,
      final long firstRecordKey, final long lastRecordKey, final long[] recordKeys, final long[] orderExceptionBits,
      final byte[] orderLabelBytes, final int[] orderLabelOffsets, final long[] columnMin, final long[] columnMax,
      final long[][] numericCols, final long[][] booleanCols, final int[][] dictIdCols, final byte[][][] dicts,
      final int[][] setCountCols, final int[][] setElemCols, final byte[] columnFlags, final long[][] presence,
      final int presWords) {
    final int size = rawDirectByteSize(rowCount, columnCount, kinds, dicts, setElemCols, presWords,
        orderExceptionBits != null, orderLabelBytes.length);
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
    }
    if (orderExceptionBits == null) {
      bb.put(ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_NONE);
    } else {
      bb.put(ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_DENSE);
      putLongsBulk(bb, orderExceptionBits, presWords);
    }
    bb.putInt(orderLabelBytes.length);
    putIntsBulk(bb, orderLabelOffsets, rowCount + 1);
    bb.put(orderLabelBytes);
    if (rowCount > 0) {
      for (int c = 0; c < columnCount; c++) {
        bb.putLong(columnMin[c]);
        bb.putLong(columnMax[c]);
        switch (kinds[c]) {
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
              ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
              ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL,
              ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP, ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
            putLongsBulk(bb, numericCols[c], rowCount);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> putLongsBulk(bb, booleanCols[c], presWords);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
            putDictionary(bb, dicts[c]);
            putIntsBulk(bb, dictIdCols[c], rowCount);
          }
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
            putDictionary(bb, dicts[c]);
            // Same order the page's own serialize writes: counts, then the flat element run.
            putIntsBulk(bb, setCountCols[c], rowCount);
            putIntsBulk(bb, setElemCols[c], setElemCols[c].length);
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
      throw new IllegalStateException(
          "Direct raw assembly size drift: wrote " + bb.position() + " of a computed " + size + " bytes");
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
   * Decode ONE column's BODY segment into a {@link ProjectionColumnStore.ColumnSlice} (P5b stage 2) —
   * the column-pruned alternative to {@link #assembleRaw}: verifies the segment's byteLen + XXH3-64
   * against the descriptor entry, then parses flags, zone map, presence, and values with the exact
   * decoders the assembler uses. String columns are rejected HERE — their dict ids are meaningless
   * without the DICT segment, so they go through {@link #decodeStringSlice}, which takes both.
   *
   * @throws IllegalStateException on verification or parse failure — callers decline to the eager
   *         whole-leaf path
   */
  static ProjectionColumnStore.ColumnSlice decodeBodySlice(final byte[] descriptor, final byte[] bodyColumnSegment,
      final int col) {
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final byte kind = RowGroupDescriptor.kind(descriptor, col);
    final int bodyId = bodyColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor body = openColumnSegment(descriptor, id -> id == bodyId
        ? bodyColumnSegment
        : null, bodyId, SEG_KIND_BODY);
    final byte flags = body.readByte();
    final int presWords = rowCount > 0
        ? (rowCount + 63) >>> 6
        : 0;
    final long[] presence = new long[presWords];
    if (rowCount == 0) {
      return new ProjectionColumnStore.ColumnSlice(0, flags, Long.MAX_VALUE, Long.MIN_VALUE, presence, null, null, null,
          null, null);
    }
    final long min = body.readLong();
    final long max = body.readLong();
    ProjectionIndexRowGroupCodec.decodePresenceInto(body, presence, presWords, rowCount);
    return switch (kind) {
      case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
          ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP,
          ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
        new ProjectionColumnStore.ColumnSlice(rowCount, flags, min, max, presence,
            ProjectionIndexRowGroupCodec.decodeForBitPackedColumn(body, rowCount), null, null, null, null);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> new ProjectionColumnStore.ColumnSlice(rowCount, flags,
          min, max, presence, null, ProjectionIndexRowGroupCodec.decodeBooleanWords(body, presWords), null, null, null);
      default -> throw new IllegalStateException("Column " + col + " (kind " + kind + ") is not body-only sliceable");
    };
  }

  /**
   * Decode a STRING_DICT column's BODY + DICT segments into one slice — what lets a string equality
   * run column-sliced instead of hydrating whole leaves. Same verification and header discipline as
   * {@link #decodeBodySlice}; the ids come from the BODY (width byte + packed ids, the raw form's own
   * stream) and the dictionary from the DICT segment ({@code null} only for a rowless leaf, which
   * writes no DICT segment at all).
   *
   * @throws IllegalStateException on verification or parse failure — callers decline to the eager
   *         whole-leaf path
   */
  static ProjectionColumnStore.ColumnSlice decodeStringSlice(final byte[] descriptor, final byte[] bodyColumnSegment,
      final byte @Nullable [] dictColumnSegment, final int col) {
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final byte kind = RowGroupDescriptor.kind(descriptor, col);
    if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
      throw new IllegalStateException("Column " + col + " (kind " + kind + ") is not STRING_DICT");
    }
    final int bodyId = bodyColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor body = openColumnSegment(descriptor, id -> id == bodyId
        ? bodyColumnSegment
        : null, bodyId, SEG_KIND_BODY);
    final byte flags = body.readByte();
    final int presWords = rowCount > 0
        ? (rowCount + 63) >>> 6
        : 0;
    final long[] presence = new long[presWords];
    if (rowCount == 0) {
      return new ProjectionColumnStore.ColumnSlice(0, flags, Long.MAX_VALUE, Long.MIN_VALUE, presence, null, null, null,
          null, null);
    }
    if (dictColumnSegment == null) {
      throw new IllegalStateException("Column " + col + " has rows but no DICT segment bytes");
    }
    final long min = body.readLong();
    final long max = body.readLong();
    ProjectionIndexRowGroupCodec.decodePresenceInto(body, presence, presWords, rowCount);
    final int[] ids = ProjectionIndexRowGroupCodec.decodePackedIds(body, rowCount);
    final int dictId = dictColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor dict = openColumnSegment(descriptor, id -> id == dictId
        ? dictColumnSegment
        : null, dictId, SEG_KIND_DICT);
    final FlatDict flat = decodeFlatDictColumnSegmentPayload(dict);
    return new ProjectionColumnStore.ColumnSlice(rowCount, flags, min, max, presence, null, null, ids, flat.bytes(),
        flat.offsets());
  }

  /**
   * Decode a STRING_DICT column's BODY beside a PRECOMPUTED {@link #SEG_KIND_DICT_HASHES} segment —
   * the distinct-identity fill: per-row dict ids plus {@code dictId -> content hash}, with NO
   * dictionary bytes fetched or decoded at all.
   *
   * <p>
   * The resulting slice answers identity questions only. Its {@code dictBytes}/{@code dictOffsets}
   * are {@code null} by construction, so a consumer that needs the strings themselves
   * (materialization, comparison, string-length) must not be handed one — the store hands it out
   * through a distinct accessor for exactly that reason.
   *
   * @throws IllegalStateException on verification or parse failure
   */
  static ProjectionColumnStore.ColumnSlice decodeStringIdentitySlice(final byte[] descriptor,
      final byte[] bodyColumnSegment, final long[] dictHashes, final int col) {
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final byte kind = RowGroupDescriptor.kind(descriptor, col);
    if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
      throw new IllegalStateException("Column " + col + " (kind " + kind + ") is not STRING_DICT");
    }
    final int bodyId = bodyColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor body = openColumnSegment(descriptor, id -> id == bodyId
        ? bodyColumnSegment
        : null, bodyId, SEG_KIND_BODY);
    final byte flags = body.readByte();
    final int presWords = rowCount > 0
        ? (rowCount + 63) >>> 6
        : 0;
    final long[] presence = new long[presWords];
    if (rowCount == 0) {
      return new ProjectionColumnStore.ColumnSlice(0, flags, Long.MAX_VALUE, Long.MIN_VALUE, presence, null, null, null,
          null, null);
    }
    final long min = body.readLong();
    final long max = body.readLong();
    ProjectionIndexRowGroupCodec.decodePresenceInto(body, presence, presWords, rowCount);
    final int[] ids = ProjectionIndexRowGroupCodec.decodePackedIds(body, rowCount);
    return new ProjectionColumnStore.ColumnSlice(rowCount, flags, min, max, presence, null, null, ids, null, null, null,
        dictHashes);
  }

  /**
   * Decode ONE {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_SET} column into a slice.
   *
   * <p>
   * Same discipline as {@link #decodeStringSlice}, with the set's shape on top: per-row element
   * COUNTS, then the flat element run whose length is their sum. The counts are what turn a flat run
   * back into rows, and storing them rather than offsets keeps the packed width down — most rows hold
   * a handful of elements while an offset grows with the leaf.
   *
   * @throws IllegalStateException on verification or parse failure
   */
  static ProjectionColumnStore.ColumnSlice decodeStringSetSlice(final byte[] descriptor, final byte[] bodyColumnSegment,
      final byte @Nullable [] dictColumnSegment, final int col) {
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final byte kind = RowGroupDescriptor.kind(descriptor, col);
    if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
      throw new IllegalStateException("Column " + col + " (kind " + kind + ") is not STRING_SET");
    }
    final int bodyId = bodyColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor body = openColumnSegment(descriptor, id -> id == bodyId
        ? bodyColumnSegment
        : null, bodyId, SEG_KIND_BODY);
    final byte flags = body.readByte();
    final int presWords = rowCount > 0
        ? (rowCount + 63) >>> 6
        : 0;
    final long[] presence = new long[presWords];
    if (rowCount == 0) {
      return new ProjectionColumnStore.ColumnSlice(0, flags, Long.MAX_VALUE, Long.MIN_VALUE, presence, null, null, null,
          null, null);
    }
    if (dictColumnSegment == null) {
      throw new IllegalStateException("Column " + col + " has rows but no DICT segment bytes");
    }
    final long min = body.readLong();
    final long max = body.readLong();
    ProjectionIndexRowGroupCodec.decodePresenceInto(body, presence, presWords, rowCount);
    final int[] counts = ProjectionIndexRowGroupCodec.decodePackedIds(body, rowCount);
    int total = 0;
    for (int r = 0; r < rowCount; r++) {
      total += counts[r];
    }
    final int[] elems = ProjectionIndexRowGroupCodec.decodePackedIds(body, total);
    final int dictId = dictColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor dict = openColumnSegment(descriptor, id -> id == dictId
        ? dictColumnSegment
        : null, dictId, SEG_KIND_DICT);
    final FlatDict flat = decodeFlatDictColumnSegmentPayload(dict);
    return new ProjectionColumnStore.ColumnSlice(rowCount, flags, min, max, presence, null, null, elems, flat.bytes(),
        flat.offsets(), counts);
  }

  /**
   * Decode one leaf's KEYS segment into its document-ordered per-row record keys, verified against
   * the descriptor like every other segment read. The sparse order bitmap is validated in place and
   * is never materialised on this keys-only path. Empty for a rowless leaf.
   *
   * @throws IllegalStateException on verification or parse failure
   */
  static long[] decodeKeysSlice(final byte[] descriptor, final byte[] keysColumnSegment) {
    return decodeKeysView(descriptor, keysColumnSegment).recordKeys();
  }

  /**
   * Decode and validate KEYS without allocating the optional dense exception bitmap. The returned
   * view retains the already-loaded segment and reads a requested membership bit directly from it.
   */
  static KeysView decodeKeysView(final byte[] descriptor, final byte[] keysColumnSegment) {
    final int rowCount = RowGroupDescriptor.rowCount(descriptor);
    final int keysId = keysColumnSegmentId();
    final ProjectionIndexRowGroupCodec.Cursor in = openColumnSegment(descriptor, id -> id == keysId
        ? keysColumnSegment
        : null, keysId, SEG_KIND_KEYS);
    final long firstRecordKey = in.readLong();
    final long lastRecordKey = in.readLong();
    final long[] recordKeys = rowCount > 0
        ? ProjectionIndexRowGroupCodec.decodeRecordKeys(in, rowCount)
        : EMPTY_KEYS;
    final byte orderKind = in.readByte();
    final boolean dense;
    if (orderKind == ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_NONE) {
      dense = false;
    } else if (orderKind == ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_DENSE && rowCount > 0) {
      dense = true;
    } else {
      throw new IllegalStateException("unknown projection order-exception kind " + orderKind);
    }
    final byte[] segment = in.buffer();
    final int orderBitsOffset = in.position();
    final int orderBitsBytes = dense
        ? ((rowCount + 63) >>> 6) * Long.BYTES
        : 0;
    in.skip(orderBitsBytes);
    final ProjectionIndexRowGroupCodec.OrderLabelLane orderLabels =
        ProjectionIndexRowGroupCodec.decodeOrderLabelLane(in, rowCount);
    if (in.position() != segment.length) {
      throw new IllegalStateException("projection KEYS order metadata has an invalid length");
    }
    validateKeysView(rowCount, firstRecordKey, lastRecordKey, recordKeys, segment, orderBitsOffset, dense);
    return new KeysView(firstRecordKey, lastRecordKey, recordKeys, segment, orderBitsOffset, dense, orderLabels);
  }

  /** Decode KEYS together with a materialised bitmap for membership/order rewrites only. */
  static KeysSlice decodeKeysAndOrderSlice(final byte[] descriptor, final byte[] keysColumnSegment) {
    final KeysView view = decodeKeysView(descriptor, keysColumnSegment);
    final long[] orderExceptionBits;
    if (!view.dense()) {
      orderExceptionBits = null;
    } else {
      final int words = (view.recordKeys().length + 63) >>> 6;
      orderExceptionBits = new long[words];
      for (int word = 0; word < words; word++) {
        orderExceptionBits[word] =
            ProjectionIndexRowGroupCodec.getLongLE(view.segment(), view.orderBitsOffset() + word * Long.BYTES);
      }
    }
    final int[] orderLabelOffsets = new int[view.recordKeys().length + 1];
    view.orderLabels().copyOffsetsInto(orderLabelOffsets);
    return new KeysSlice(view.firstRecordKey(), view.lastRecordKey(), view.recordKeys(), orderExceptionBits,
        view.orderLabels().materializeLabelBytes(), orderLabelOffsets);
  }

  private static void validateKeysView(final int rowCount, final long firstRecordKey, final long lastRecordKey,
      final long[] recordKeys, final byte[] segment, final int orderBitsOffset, final boolean dense) {
    long expectedFirst = Long.MAX_VALUE;
    long expectedLast = Long.MIN_VALUE;
    long previousNormal = Long.MIN_VALUE;
    boolean sawException = false;
    long exceptionWord = 0L;
    for (int row = 0; row < rowCount; row++) {
      final long recordKey = recordKeys[row];
      if (recordKey < 0) {
        throw new IllegalStateException("projection KEYS contains a negative document node key");
      }
      if (dense && (row & 63) == 0) {
        exceptionWord = ProjectionIndexRowGroupCodec.getLongLE(segment, orderBitsOffset + (row >>> 6) * Long.BYTES);
      }
      final boolean exception = dense && (exceptionWord & (1L << (row & 63))) != 0L;
      if (exception) {
        sawException = true;
        continue;
      }
      if (recordKey <= previousNormal) {
        throw new IllegalStateException("projection normal routing backbone is not strictly increasing");
      }
      if (expectedFirst == Long.MAX_VALUE) {
        expectedFirst = recordKey;
      }
      expectedLast = recordKey;
      previousNormal = recordKey;
    }
    if (dense) {
      final int tailBits = rowCount & 63;
      if (tailBits != 0) {
        final int lastWord = (rowCount - 1) >>> 6;
        final long bits = ProjectionIndexRowGroupCodec.getLongLE(segment, orderBitsOffset + lastWord * Long.BYTES);
        if ((bits & (-1L << tailBits)) != 0L) {
          throw new IllegalStateException("projection order-exception bitmap sets bits beyond rowCount");
        }
      }
      if (!sawException) {
        throw new IllegalStateException("dense projection order bitmap contains no exceptions");
      }
    }
    if (firstRecordKey != expectedFirst || lastRecordKey != expectedLast) {
      throw new IllegalStateException("projection normal fence does not match its KEYS rows");
    }
  }

  private static final long[] EMPTY_KEYS = new long[0];

  record KeysSlice(long firstRecordKey, long lastRecordKey, long[] recordKeys, long[] orderExceptionBits,
      byte[] orderLabelBytes, int[] orderLabelOffsets) {
    boolean orderExceptionAt(final int row) {
      if (row < 0 || row >= recordKeys.length) {
        throw new IndexOutOfBoundsException("projection row out of range: " + row);
      }
      return orderExceptionBits != null && (orderExceptionBits[row >>> 6] & (1L << (row & 63))) != 0;
    }
  }

  record KeysView(long firstRecordKey, long lastRecordKey, long[] recordKeys, byte[] segment, int orderBitsOffset,
      boolean dense, ProjectionIndexRowGroupCodec.OrderLabelLane orderLabels) {
    boolean orderExceptionAt(final int row) {
      if (row < 0 || row >= recordKeys.length) {
        throw new IndexOutOfBoundsException("projection row out of range: " + row);
      }
      return dense && (ProjectionIndexRowGroupCodec.getLongLE(segment, orderBitsOffset + (row >>> 6) * Long.BYTES)
          & (1L << (row & 63))) != 0L;
    }

    int compareOrderLabelAt(final int row, final byte[] label) {
      if (row < 0 || row >= recordKeys.length || label == null) {
        throw new IndexOutOfBoundsException("projection row out of range: " + row);
      }
      return orderLabels.compareAt(row, label);
    }

    byte[] copyOrderLabelAt(final int row) {
      if (row < 0 || row >= recordKeys.length) {
        throw new IndexOutOfBoundsException("projection row out of range: " + row);
      }
      return orderLabels.copyAt(row);
    }
  }

  /**
   * Column-scoped provenance primitive (5.1-7): the flags byte from a BODY segment's bytes — segment
   * TRUTH, as opposed to the descriptor's mirror. Validates the segment header.
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
   * Resolve, verify (length + hash against the descriptor), and open a segment positioned after its
   * header.
   */
  private static ProjectionIndexRowGroupCodec.Cursor openColumnSegment(final byte[] descriptor,
      final SegmentResolver resolver, final int columnSegmentId, final byte expectedKind) {
    final int entry = RowGroupDescriptor.entryIndexOf(descriptor, columnSegmentId);
    final byte[] segment = resolver.segment(columnSegmentId);
    verifyColumnSegment(descriptor, segment, columnSegmentId, expectedKind, entry);
    return new ProjectionIndexRowGroupCodec.Cursor(segment, SEGMENT_HEADER_BYTES);
  }

  /**
   * Full segment verification against its descriptor entry — exact byteLen, XXH3-64 content hash, and
   * the PIXS header — without opening a cursor. The byte-level column cache
   * ({@code ProjectionColumnStore#columnBytes}) verifies once at fill; the fused fold kernels then
   * trust the cached bytes.
   *
   * @throws IllegalStateException on any mismatch (callers decline through the established fail-soft
   *         flow)
   */
  static void verifyColumnSegment(final byte[] descriptor, final byte @Nullable [] segment, final int columnSegmentId,
      final byte expectedKind) {
    verifyColumnSegment(descriptor, segment, columnSegmentId, expectedKind,
        RowGroupDescriptor.entryIndexOf(descriptor, columnSegmentId));
  }

  /** {@link #verifyColumnSegment} for a caller that already resolved the descriptor entry index. */
  static void verifyColumnSegment(final byte[] descriptor, final byte @Nullable [] segment, final int columnSegmentId,
      final byte expectedKind, final int entry) {
    validateColumnSegmentShape(descriptor, segment, columnSegmentId, expectedKind, entry);
    if (contentHash(segment) != RowGroupDescriptor.entryContentHash(descriptor, entry)) {
      throw new IllegalStateException("Segment content-hash mismatch for columnSegmentId=" + columnSegmentId
          + " — corrupted segment page or dangling side-map reference");
    }
  }

  /**
   * Allocation-free publication gate for codec-owned bytes. It checks descriptor alignment, the
   * self-describing segment header, and pruning mirrors without hashing the segment a second time;
   * the encoder already computed the descriptor hash while producing this immutable-by-contract
   * object. Persisted reads use {@link #verifyColumnSegment} and do re-hash untrusted bytes.
   */
  static void validateColumnSegmentShape(final byte[] descriptor, final byte @Nullable [] segment,
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
    checkColumnSegmentHeader(segment, expectedKind);
    verifyDescriptorMirror(descriptor, segment, columnSegmentId, entry);
  }

  /**
   * Cross-check the descriptor fields used for no-segment-I/O pruning against the authoritative
   * KEYS/BODY bytes. The write boundary invokes this before publication; readers repeat it whenever
   * they fetch a segment, turning a stale mirror into attributed corruption rather than a wrong
   * answer.
   */
  private static void verifyDescriptorMirror(final byte[] descriptor, final byte[] segment,
      final int columnSegmentId, final int entry) {
    if (columnSegmentId == keysColumnSegmentId()) {
      final int minimum = SEGMENT_HEADER_BYTES + 2 * Long.BYTES;
      if (segment.length < minimum) {
        throw new IllegalStateException("Truncated KEYS segment: " + segment.length + " < " + minimum);
      }
      final long first = ProjectionIndexRowGroupCodec.getLongLE(segment, SEGMENT_HEADER_BYTES);
      final long last = ProjectionIndexRowGroupCodec.getLongLE(segment, SEGMENT_HEADER_BYTES + Long.BYTES);
      if (first != RowGroupDescriptor.firstRecordKey(descriptor)
          || last != RowGroupDescriptor.lastRecordKey(descriptor)) {
        throw new IllegalStateException("KEYS fences [" + first + ", " + last
            + "] do not match descriptor fences [" + RowGroupDescriptor.firstRecordKey(descriptor) + ", "
            + RowGroupDescriptor.lastRecordKey(descriptor) + "]");
      }
      return;
    }
    if (expectedSegmentKind(columnSegmentId) != SEG_KIND_BODY) {
      return;
    }
    if (segment.length < SEGMENT_HEADER_BYTES + 1) {
      throw new IllegalStateException("Truncated BODY segment: no flags byte after the header");
    }
    final byte flags = segment[SEGMENT_HEADER_BYTES];
    if (flags != RowGroupDescriptor.entryColFlags(descriptor, entry)) {
      throw new IllegalStateException("BODY flags " + (flags & 0xFF) + " do not match descriptor flags "
          + (RowGroupDescriptor.entryColFlags(descriptor, entry) & 0xFF) + " for columnSegmentId="
          + columnSegmentId);
    }
    if (RowGroupDescriptor.rowCount(descriptor) == 0) {
      return;
    }
    final int minimum = SEGMENT_HEADER_BYTES + 1 + 2 * Long.BYTES;
    if (segment.length < minimum) {
      throw new IllegalStateException("Truncated BODY segment: " + segment.length + " < " + minimum);
    }
    final long min = ProjectionIndexRowGroupCodec.getLongLE(segment, SEGMENT_HEADER_BYTES + 1);
    final long max = ProjectionIndexRowGroupCodec.getLongLE(segment, SEGMENT_HEADER_BYTES + 1 + Long.BYTES);
    if (min != RowGroupDescriptor.entryMin(descriptor, entry)
        || max != RowGroupDescriptor.entryMax(descriptor, entry)) {
      throw new IllegalStateException("BODY zone map [" + min + ", " + max
          + "] does not match descriptor zone map [" + RowGroupDescriptor.entryMin(descriptor, entry) + ", "
          + RowGroupDescriptor.entryMax(descriptor, entry) + "] for columnSegmentId=" + columnSegmentId);
    }
  }

  /** DICT payload modes: raw entry stream vs FSST-compressed entries behind a symbol table. */
  private static final byte DICT_MODE_RAW = 0;
  private static final byte DICT_MODE_FSST = 1;

  /**
   * A DICT segment carrying, beside each value, the number of ROWS on this leaf whose set contains
   * it. Written only for {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_SET} columns.
   *
   * <p>
   * This is what lets a bare {@code count(... satisfies $g eq lit)} skip the BODY segment entirely.
   * The BODY holds a per-row cardinality and the flat element run — the bulk of the column — and the
   * current path fetches all of it and visits every element. With a per-value row count the same
   * answer is: find the literal in this leaf's dictionary, read its count, add it to the running
   * total. Measured on the movies corpus the dictionary is 41 entries against 6.2M elements, so the
   * counts cost a few hundred bytes per leaf and replace a scan of the whole column.
   *
   * <p>
   * ROWS, not occurrences. A record listing the same genre twice must count once, and the encoder is
   * the only place that can tell — it sees each row's whole element run, so it counts distinct values
   * per row. Deriving this later from occurrence counts would be wrong for exactly the data that
   * makes the optimisation attractive, and wrong in the direction of over-counting.
   */
  private static final byte DICT_MODE_RAW_ROW_COUNTS = 2;

  /** {@link #DICT_MODE_RAW_ROW_COUNTS}, with the dictionary FSST-compressed. */
  private static final byte DICT_MODE_FSST_ROW_COUNTS = 3;

  /**
   * DICT segment payload (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.7): a mode byte, then either
   * the raw entry stream (count, lengths, concatenated UTF-8 — byte-compatible with the monolithic
   * codec's dictionary half) or, for high-cardinality dictionaries that pass {@code FSSTCompressor}'s
   * existing gates AND actually compress, a per-segment symbol table followed by per-entry FSST
   * streams. FSST lives in the PERSISTED form only — decode restores plain UTF-8 dictionary bytes, so
   * the raw scan form (and every kernel comparing dictionary bytes raw) is untouched. Training input
   * is the dictionary in interning order (deterministic), so identical re-encodes hash identically —
   * the carry-forward no-op contract (5.2-n) holds.
   */
  /**
   * ROWS on this leaf whose set contains each dictionary value, indexed by dict id.
   *
   * <p>
   * Rows, not occurrences: a record listing the same genre twice must count once. The encoder is the
   * only place that can tell, because it is the only place that sees a row's whole element run —
   * which is why this is computed here rather than derived later from occurrence counts, where the
   * duplicate is already indistinguishable from two records.
   *
   * <p>
   * The {@code lastRow} marker is the dedup: a value already credited to this row has the row's index
   * parked in its slot, so a repeat is a comparison rather than a set lookup.
   */
  static long[] valueRowCounts(final int dictSize, final int[] setCounts, final int[] setElems, final int rowCount) {
    if (dictSize == 0 || setCounts == null || setElems == null) {
      return null;
    }
    final long[] counts = new long[dictSize];
    final int[] lastRow = new int[dictSize];
    java.util.Arrays.fill(lastRow, -1);
    int cursor = 0;
    for (int row = 0; row < rowCount; row++) {
      final int n = setCounts[row];
      for (int k = 0; k < n; k++) {
        final int id = setElems[cursor + k];
        if (id >= 0 && id < dictSize && lastRow[id] != row) {
          lastRow[id] = row;
          counts[id]++;
        }
      }
      cursor += n;
    }
    return counts;
  }

  private static void encodeDictColumnSegmentPayload(final ByteArrayOutputStream out,
      final ProjectionIndexRowGroupPage page, final int column, final long @Nullable [] rowCounts,
      final EncodeWorkspace encodeWorkspace) {
    final int dictSize = page.stringDictionarySize(column);
    int totalBytes = 0;
    for (int i = 0; i < dictSize; i++) {
      totalBytes += page.stringDictionaryEntryLength(column, i);
    }
    if (dictSize >= FSSTCompressor.MIN_SAMPLES_FOR_TABLE && totalBytes >= FSSTCompressor.MIN_TOTAL_BYTES_FOR_TABLE) {
      final FSSTCompressor.Workspace fsstWorkspace = encodeWorkspace.fsstWorkspace();
      final boolean flat = page.stringDictionaryIsSlabBacked(column);
      final List<byte[]> legacyEntries = flat
          ? List.of()
          : new DictionaryRangeList(page, column, dictSize);
      final byte[] table = flat
          ? FSSTCompressor.buildSymbolTable(page.stringDictionaryFlatBacking(column),
              page.stringDictionaryFlatOffsets(column), page.stringDictionaryFlatLengths(column), dictSize,
              fsstWorkspace)
          : FSSTCompressor.buildSymbolTable(legacyEntries, fsstWorkspace);
      if (table.length > 0) {
        // This table exists for one segment only. Parse it directly rather than retaining both the
        // table and its parsed form in the generic thread-local identity rings. The workspace keeps
        // one matcher hot across training, trial, and every dictionary entry.
        final byte[][] parsedSymbols = FSSTCompressor.parseSymbolTable(table);
        final boolean beneficial = flat
            ? FSSTCompressor.isCompressionBeneficial(page.stringDictionaryFlatBacking(column),
                page.stringDictionaryFlatOffsets(column), page.stringDictionaryFlatLengths(column), dictSize,
                parsedSymbols, fsstWorkspace)
            : FSSTCompressor.isCompressionBeneficial(legacyEntries, parsedSymbols, fsstWorkspace);
        if (beneficial) {
          out.write(rowCounts == null
              ? DICT_MODE_FSST
              : DICT_MODE_FSST_ROW_COUNTS);
          ProjectionIndexRowGroupCodec.putIntLE(out, table.length);
          out.write(table, 0, table.length);
          ProjectionIndexRowGroupCodec.putIntLE(out, dictSize);
          for (int i = 0; i < dictSize; i++) {
            final byte[] encoded = FSSTCompressor.encode(page.stringDictionaryEntryBacking(column, i),
                page.stringDictionaryEntryOffset(column, i), page.stringDictionaryEntryLength(column, i), parsedSymbols,
                fsstWorkspace);
            ProjectionIndexRowGroupCodec.putIntLE(out, encoded.length);
            out.write(encoded, 0, encoded.length);
          }
          writeRowCounts(out, rowCounts, dictSize);
          return;
        }
      }
    }
    out.write(rowCounts == null
        ? DICT_MODE_RAW
        : DICT_MODE_RAW_ROW_COUNTS);
    ProjectionIndexRowGroupCodec.encodeDictEntries(out, page, column);
    writeRowCounts(out, rowCounts, dictSize);
  }

  /**
   * Allocation-free entry view for legacy/deserialised dictionaries. The list object is one small
   * cold-path adapter; {@link #get} returns the already-owned legacy entry and never copies bytes.
   */
  private static final class DictionaryRangeList extends AbstractList<byte[]> {
    private final ProjectionIndexRowGroupPage page;
    private final int column;
    private final int size;

    private DictionaryRangeList(final ProjectionIndexRowGroupPage page, final int column, final int size) {
      this.page = page;
      this.column = column;
      this.size = size;
    }

    @Override
    public byte[] get(final int index) {
      final byte[] backing = page.stringDictionaryEntryBacking(column, index);
      final int offset = page.stringDictionaryEntryOffset(column, index);
      final int length = page.stringDictionaryEntryLength(column, index);
      if (offset != 0 || length != backing.length) {
        throw new IllegalStateException("legacy dictionary entry " + index + " is not an owned whole array");
      }
      return backing;
    }

    @Override
    public int size() {
      return size;
    }
  }

  /**
   * Append the per-value row counts, bit-packed to the widest count on the leaf.
   *
   * <p>
   * A leaf holds at most {@code MAX_ROWS} rows, so a count needs at most 10 bits and usually fewer —
   * writing longs here would cost more than the dictionary it annotates.
   */
  private static void writeRowCounts(final ByteArrayOutputStream out, final long @Nullable [] counts,
      final int dictSize) {
    if (counts == null) {
      return;
    }
    long max = 0;
    for (int i = 0; i < dictSize; i++) {
      if (counts[i] > max) {
        max = counts[i];
      }
    }
    final int width = max == 0
        ? 0
        : 64 - Long.numberOfLeadingZeros(max);
    out.write(width);
    if (width > 0) {
      final ProjectionIndexRowGroupCodec.BitWriter bw = new ProjectionIndexRowGroupCodec.BitWriter(out);
      for (int i = 0; i < dictSize; i++) {
        bw.write(counts[i], width);
      }
      bw.flush();
    }
  }

  /**
   * Largest SET_COUNTS payload worth writing.
   *
   * <p>
   * Tied to the segment-slot inline cap, not chosen freely: past it the segment spills to a page —
   * and a page fetch is exactly what this segment exists to avoid. A column whose values do
   * not fit is better served by the scanning path, so nothing is written and the reader falls back.
   */
  private static final int MAX_SET_COUNTS_PAYLOAD_BYTES =
      ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES - SEGMENT_HEADER_BYTES;

  /**
   * {@code [short valueCount] ( [short len][len bytes value][short rowCount] )*}
   *
   * <p>
   * Values inline rather than hashed: a hash collision would over-count silently, and the whole point
   * of this segment is to answer without reading anything else that could confirm it. At these sizes
   * the bytes cost less than the doubt.
   *
   * @return the complete segment, or {@code null} when header + payload would exceed the segment
   *         slot's inline bound
   */
  private static byte[] encodeSetCountsPayload(final ProjectionIndexRowGroupPage page, final int column,
      final long @Nullable [] counts, final EncodeWorkspace workspace) {
    if (counts == null) {
      return null;
    }
    final int dictSize = page.stringDictionarySize(column);
    if (dictSize == 0 || dictSize > 0xFFFF) {
      return null;
    }
    int size = 2;
    for (int i = 0; i < dictSize; i++) {
      final int length = page.stringDictionaryEntryLength(column, i);
      if (length < 0 || length > MAX_SET_COUNTS_PAYLOAD_BYTES - size - 4) {
        return null;
      }
      size += 4 + length;
    }
    final ByteArrayOutputStream out = newColumnSegmentStream(SEG_KIND_SET_COUNTS, workspace);
    putShortLE(out, dictSize);
    for (int i = 0; i < dictSize; i++) {
      final int length = page.stringDictionaryEntryLength(column, i);
      putShortLE(out, length);
      out.write(page.stringDictionaryEntryBacking(column, i), page.stringDictionaryEntryOffset(column, i), length);
      // A leaf holds at most MAX_ROWS rows, so a row count always fits a short.
      putShortLE(out, (int) Math.min(counts[i], 0xFFFF));
    }
    return out.toByteArray();
  }

  private static void putShortLE(final ByteArrayOutputStream out, final int v) {
    out.write(v & 0xFF);
    out.write((v >>> 8) & 0xFF);
  }

  /** Read the counts {@link #writeRowCounts} wrote; {@code null} when the mode carries none. */
  private static int[] readRowCounts(final ProjectionIndexRowGroupCodec.Cursor in, final int dictSize) {
    final int width = in.readByte() & 0xFF;
    // int, not long: a leaf holds at most MAX_ROWS rows, so a per-value row count cannot exceed it
    // and the existing int unpacker serves.
    final int[] counts = new int[dictSize];
    if (width > 0) {
      ProjectionIndexRowGroupCodec.unpackIntsInto(in, dictSize, width, counts);
    }
    return counts;
  }

  /** Inverse of {@link #encodeDictColumnSegmentPayload}; restores plain UTF-8 dictionary bytes. */
  /**
   * Rows on this leaf whose set contains {@code literal}, from an inline SET_COUNTS payload.
   *
   * @return the count, or {@code -1} when the payload is absent or unreadable
   */
  public static long setCountFor(final byte[] descriptor, final byte @Nullable [] segment, final int col,
      final byte[] literal) {
    if (segment == null) {
      return -1;
    }
    final int segId = setCountsColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor in = openColumnSegment(descriptor, id -> id == segId
        ? segment
        : null, segId, SEG_KIND_SET_COUNTS);
    final int values = readShortU(in);
    for (int i = 0; i < values; i++) {
      final int len = readShortU(in);
      final byte[] value = in.readBytes(len);
      final int count = readShortU(in);
      if (java.util.Arrays.equals(value, literal)) {
        return count;
      }
    }
    return 0; // the leaf lists its values exhaustively, so absence here is a real zero
  }

  /** Little-endian unsigned short, mirroring {@link #putShortLE}. */
  private static int readShortU(final ProjectionIndexRowGroupCodec.Cursor in) {
    final int lo = in.readByte() & 0xFF;
    final int hi = in.readByte() & 0xFF;
    return lo | (hi << 8);
  }

  /** A leaf's dictionary beside the per-value ROW counts, when the segment carries them. */
  public record DictWithRowCounts(byte[][] dict, int @Nullable [] rowCounts) {
  }

  /**
   * Decode a DICT segment INCLUDING its per-value row counts.
   *
   * <p>
   * This is the whole point of putting the counts in the dictionary segment: a membership count needs
   * the dictionary anyway (to resolve the literal to an id) and nothing else. The BODY segment — a
   * per-row cardinality plus the flat element run, and the bulk of the column — is never fetched.
   *
   * @return dictionary and counts; {@code rowCounts} is {@code null} for a segment written before the
   *         counts existed or for a scalar string column, and the caller falls back
   */
  static DictWithRowCounts decodeDictWithRowCounts(final byte[] descriptor, final byte @Nullable [] dictColumnSegment,
      final int col) {
    if (dictColumnSegment == null) {
      return null;
    }
    final int dictId = dictColumnSegmentId(col);
    final ProjectionIndexRowGroupCodec.Cursor in = openColumnSegment(descriptor, id -> id == dictId
        ? dictColumnSegment
        : null, dictId, SEG_KIND_DICT);
    final int mode = in.readByte() & 0xFF;
    final byte[][] dict;
    final boolean hasCounts;
    switch (mode) {
      case DICT_MODE_RAW -> {
        dict = ProjectionIndexRowGroupCodec.decodeDictEntries(in);
        hasCounts = false;
      }
      case DICT_MODE_RAW_ROW_COUNTS -> {
        dict = ProjectionIndexRowGroupCodec.decodeDictEntries(in);
        hasCounts = true;
      }
      case DICT_MODE_FSST, DICT_MODE_FSST_ROW_COUNTS -> {
        dict = decodeFsstDictEntries(in);
        hasCounts = mode == DICT_MODE_FSST_ROW_COUNTS;
      }
      default -> throw new IllegalStateException("Unknown DICT segment mode " + mode);
    }
    if (!hasCounts) {
      return new DictWithRowCounts(dict, null);
    }
    return new DictWithRowCounts(dict, readRowCounts(in, ProjectionIndexRowGroupCodec.dictSizeOf(dict)));
  }

  /**
   * A leaf's dictionary as ONE flat byte run: entry {@code i} is
   * {@code bytes[offsets[i] .. offsets[i + 1])}, and {@code offsets.length - 1} is the exact entry
   * count (no null-padded tail, unlike the {@code byte[][]} form).
   *
   * <p>
   * This is what a {@link ProjectionColumnStore.ColumnSlice} carries. Every slice-dict consumer is
   * (array, offset, length)-shaped already — {@code fnv1a64}, {@code hasFourByteUtf8},
   * {@code Arrays.compareUnsigned}, {@code new String(...)} — so the per-entry {@code byte[]} the old
   * form allocated was pure intermediate. On a high-cardinality column, where the per-leaf dictionary
   * is nearly as large as the leaf itself, that intermediate WAS the cost of a column fill.
   */
  record FlatDict(byte[] bytes, int[] offsets) {
  }

  /** Empty dictionary — a leaf whose column holds no entry at all. */
  private static final FlatDict EMPTY_FLAT_DICT = new FlatDict(new byte[0], new int[1]);

  /**
   * Per-thread expansion buffer for FSST dictionaries: the decode writes every entry of a leaf into
   * it and then copies ONE exactly-sized run out. Reused across leaves, so a fill allocates one
   * right-sized array per leaf instead of three per dictionary entry.
   */
  private static final ThreadLocal<byte[]> FLAT_DICT_SCRATCH = ThreadLocal.withInitial(() -> new byte[1 << 16]);

  /** {@link #decodeDictColumnSegmentPayload}, decoded into the flat form. */
  private static FlatDict decodeFlatDictColumnSegmentPayload(final ProjectionIndexRowGroupCodec.Cursor in) {
    final int mode = in.readByte() & 0xFF;
    return switch (mode) {
      case DICT_MODE_RAW, DICT_MODE_RAW_ROW_COUNTS ->
        // Zero copy: the raw wire form IS the flat form, so the offsets index the segment itself.
        new FlatDict(in.buffer(), ProjectionIndexRowGroupCodec.decodeFlatDictEntries(in));
      case DICT_MODE_FSST, DICT_MODE_FSST_ROW_COUNTS -> decodeFlatFsstDictEntries(in);
      default -> throw new IllegalStateException("Unknown DICT segment mode " + mode + " — written by a newer version");
    };
  }

  /** {@link #decodeFsstDictEntries}, expanding into one flat run instead of an array per entry. */
  private static FlatDict decodeFlatFsstDictEntries(final ProjectionIndexRowGroupCodec.Cursor in) {
    final int tableLen = in.readInt();
    final byte[] table = in.readBytes(tableLen);
    final byte[][] parsedSymbols = FSSTCompressor.parsedFor(table);
    final int dictSize = in.readInt();
    if (dictSize < 0) {
      throw new IllegalStateException("Negative dictionary size " + dictSize);
    }
    if (dictSize == 0) {
      return EMPTY_FLAT_DICT;
    }
    final byte[] src = in.buffer();
    final int[] offsets = new int[dictSize + 1];
    byte[] scratch = FLAT_DICT_SCRATCH.get();
    int pos = 0;
    for (int i = 0; i < dictSize; i++) {
      final int len = in.readInt();
      if (len < 0) {
        throw new IllegalStateException("Negative dictionary entry length " + len + " at " + i);
      }
      final int off = in.position();
      if (off + len > src.length) {
        throw new IllegalStateException("Dictionary entry " + i + " overruns the segment");
      }
      in.skip(len);
      // One capacity check per ENTRY against the worst-case expansion, so the decode's byte loop
      // carries none. Growth is geometric and the buffer is per thread, so it settles after the
      // first few leaves and never allocates again.
      final int worstCase = FSSTCompressor.maxDecodedLength(len);
      if (scratch.length - pos < worstCase) {
        scratch = Arrays.copyOf(scratch, Math.max(scratch.length << 1, pos + worstCase));
        FLAT_DICT_SCRATCH.set(scratch);
      }
      pos = FSSTCompressor.decodeInto(src, off, len, parsedSymbols, scratch, pos);
      offsets[i + 1] = pos;
    }
    return new FlatDict(Arrays.copyOf(scratch, pos), offsets);
  }

  private static byte[][] decodeDictColumnSegmentPayload(final ProjectionIndexRowGroupCodec.Cursor in) {
    final int mode = in.readByte() & 0xFF;
    // The *_ROW_COUNTS modes are the same dictionary with a counts table appended. Readers that
    // only want the entries accept them and simply stop before the counts — otherwise writing the
    // counts would break every existing consumer of a set column's dictionary.
    return switch (mode) {
      case DICT_MODE_RAW, DICT_MODE_RAW_ROW_COUNTS -> ProjectionIndexRowGroupCodec.decodeDictEntries(in);
      case DICT_MODE_FSST, DICT_MODE_FSST_ROW_COUNTS -> decodeFsstDictEntries(in);
      default -> throw new IllegalStateException("Unknown DICT segment mode " + mode + " — written by a newer version");
    };
  }

  /** The FSST-compressed dictionary entries, with the symbol table parsed once. */
  private static byte[][] decodeFsstDictEntries(final ProjectionIndexRowGroupCodec.Cursor in) {
    final int tableLen = in.readInt();
    final byte[] table = in.readBytes(tableLen);
    final byte[][] parsedSymbols = FSSTCompressor.parsedFor(table);
    final int dictSize = in.readInt();
    final byte[][] dict = new byte[Math.max(16, dictSize)][];
    for (int i = 0; i < dictSize; i++) {
      final int len = in.readInt();
      final byte[] encoded = in.readBytes(len);
      dict[i] = FSSTCompressor.decode(encoded, parsedSymbols);
    }
    return dict;
  }


  private static void checkColumnSegmentHeader(final byte[] segment, final byte expectedKind) {
    if (segment.length < SEGMENT_HEADER_BYTES || ProjectionIndexRowGroupCodec.getIntLE(segment, 0) != SEGMENT_MAGIC) {
      throw new IllegalStateException("Not a projection segment (missing PIXS magic)");
    }
    if (segment[4] != SEGMENT_VERSION) {
      throw new IllegalStateException("Unknown segment version " + segment[4] + " (expected " + SEGMENT_VERSION + ")");
    }
    if (segment[5] != expectedKind) {
      throw new IllegalStateException("Segment kind mismatch: " + segment[5] + " != expected " + expectedKind);
    }
  }
}
