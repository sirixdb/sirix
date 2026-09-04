/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import java.util.Arrays;
import java.util.Objects;

/**
 * The projection leaf <em>descriptor</em> ("PIXD"): the tiny HOT slot value of the
 * segment-directory storage layout (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3). It carries the
 * raw-form header (rowCount, columnCount, kinds, record-key fences) plus one fixed-size entry per
 * segment: {@code columnSegmentId}, exact {@code byteLen}, XXH3-64 {@code contentHash} (the
 * write-path no-op comparator AND the read-path integrity check — segment pages persist behind bare
 * offset keys with no checksum of their own), and the per-column stats mirror (flags, min, max) for
 * descriptor-only pruning decisions.
 *
 * <p>
 * <b>Mirror discipline (5.2-k):</b> flags/min/max here are a cache of the segment truth (the BODY
 * segment carries the authoritative copies). Pruning may consult the mirror; provenance gates must
 * read segment bytes.
 *
 * <p>
 * Wire layout (all little-endian, fixed offsets — the readers are positional and allocation-free
 * for the scan hot path):
 *
 * <pre>
 *   int    MAGIC = "PIXD"                        [offset 0]
 *   byte   VERSION = 0                           [offset 4]
 *   int    rowCount                              [offset 5]
 *   short  columnCount                           [offset 9]
 *   long   firstRecordKey                        [offset 11]
 *   long   lastRecordKey                         [offset 19]
 *   byte[columnCount] kinds                      [offset 27]
 *   short  columnSegmentCount                              [offset 27 + columnCount]
 *   per entry (ENTRY_BYTES = 31):
 *     short columnSegmentId; int byteLen; long contentHash; byte colFlags; long min; long max
 * </pre>
 *
 * <p>
 * Segment bytes live in exactly one adjacent segment slot. That slot keeps payloads up to the
 * storage threshold inline in the HOT leaf and spills larger payloads to an {@link OverflowPage};
 * the descriptor never carries segment bytes. Consequently there is one descriptor shape and one
 * mutation path, while the slot layer remains free to choose its physical inline/overflow encoding.
 *
 * <p>
 * A zero-length slot value is the leaf tombstone; a descriptor with {@code rowCount == 0} is a live
 * empty leaf (deletes can legitimately empty a mid-store leaf) — the two are distinct states by
 * design (5.1-4).
 */
public final class RowGroupDescriptor {

  /** Leading magic ("PIXD" little-endian). */
  public static final int MAGIC = 0x44584950;

  /**
   * Wire-format version, and there is exactly ONE — the current one, like
   * {@link io.sirix.BinaryEncodingVersion} and {@link ProjectionIndexMetadata}'s. The byte exists so
   * that a future format change can be REJECTED rather than misread, not so two formats can coexist:
   * {@link #validate} refuses any other value outright, so a changed payload fails loudly instead of
   * being read at shifted offsets.
   *
   * <p>
   * It starts at 0 rather than carrying a history. Earlier values existed only within this codebase's
   * own development — the entry's columnSegmentId widening from 1 to 2 bytes, the descriptor being
   * reduced to a zone map — and no resource written with them exists, so numbering as though a
   * migration path had to be preserved would document a compatibility guarantee this project does not
   * make. A zero here is unambiguous because {@link #isDescriptor} gates on the magic first: a
   * zero-filled buffer is rejected as "not a descriptor", never read as a version-0 one.
   *
   * <p>
   * Bump it when the payload's shape changes. That is what makes such a change safe: an old
   * descriptor is refused, instead of its bytes being read at shifted offsets.
   */
  public static final byte VERSION = 0;

  /**
   * Column cap imposed by the 16-bit columnSegmentId space of the HOT side-map composite key: every
   * column claims {@link ProjectionIndexColumnSegmentCodec#SEGMENT_ID_SLOTS_PER_COLUMN} ids — the
   * four of its contiguous stride (body, dict, set-counts, bloom) plus one in the disjoint
   * {@link ProjectionIndexColumnSegmentCodec#DICT_HASH_SEGMENT_BASE} region — and the largest of them
   * must stay {@code ≤ MAX_OVERFLOW_PAGE_REF_SUB_ID}. Derived — not restated — from the id-scheme
   * constants so the invariant has a single authority (a further segment kind automatically tightens
   * this cap). With a 16-bit sub-id this is {@code (65535-10)/5 = 13105} columns (was 84 at 8 bits);
   * the on-disk entry columnSegmentId field is 2 bytes to match.
   *
   * <p>
   * Tightening it does NOT renumber anything: the four stride formulas are unchanged, so a projection
   * written under the wider cap is still read at the ids it was written with.
   */
  public static final int MAX_COLUMNS =
      (HOTLeafPage.MAX_OVERFLOW_PAGE_REF_SUB_ID - 10) / ProjectionIndexColumnSegmentCodec.SEGMENT_ID_SLOTS_PER_COLUMN;

  /**
   * Fixed size of one segment entry (2-byte columnSegmentId + int byteLen + long hash + byte flags +
   * 2 longs).
   */
  public static final int ENTRY_BYTES = 2 + 4 + 8 + 1 + 8 + 8;

  /**
   * Upper bound on one projection segment or serialized descriptor. This is the PROJECTION's own
   * domain limit, not a page-layer one: {@link OverflowPage} deliberately imposes no ceiling (a node
   * record spilled there is unbounded), so bounding a projection segment is this layer's job. It
   * fails a runaway encode loudly at the producer instead of at some later assembly.
   */
  public static final int MAX_SEGMENT_BYTES = 16 * 1024 * 1024;

  /**
   * Smallest structurally possible descriptor: fixed head through the kinds offset (zero columns)
   * plus the columnSegmentCount short. Cheap plausibility floor for slice-level readers that only
   * need head fields without a full {@link #validate}.
   */
  public static final int MIN_BYTES = 27 + 2;

  private static final int OFF_ROW_COUNT = 5;
  private static final int OFF_COLUMN_COUNT = 9;
  private static final int OFF_FIRST_KEY = 11;
  private static final int OFF_LAST_KEY = 19;
  private static final int OFF_KINDS = 27;

  private RowGroupDescriptor() {}

  // ==================== write ====================

  /**
   * Serialize the sole descriptor shape. Entry arrays are parallel, {@code columnSegmentCount}
   * entries each; entries must be sorted by ascending {@code columnSegmentId} (binary-searchable,
   * deterministic bytes). Segment payloads are stored in their adjacent segment slots, never in this
   * byte array.
   */
  public static byte[] serialize(final int rowCount, final long firstRecordKey, final long lastRecordKey,
      final byte[] kinds, final int columnSegmentCount, final int[] columnSegmentIds, final int[] byteLens,
      final long[] contentHashes, final byte[] colFlags, final long[] mins, final long[] maxs) {
    checkSerializeShape(rowCount, kinds, columnSegmentCount, columnSegmentIds, byteLens, contentHashes, colFlags, mins,
        maxs);
    final int entriesEnd = OFF_KINDS + kinds.length + 2 + columnSegmentCount * ENTRY_BYTES;
    if (entriesEnd > MAX_SEGMENT_BYTES) {
      throw new IllegalArgumentException(
          "descriptor of " + entriesEnd + " bytes exceeds the projection segment ceiling " + MAX_SEGMENT_BYTES);
    }
    final byte[] out = new byte[entriesEnd];
    putIntLE(out, 0, MAGIC);
    out[4] = VERSION;
    putIntLE(out, OFF_ROW_COUNT, rowCount);
    putShortLE(out, OFF_COLUMN_COUNT, (short) kinds.length);
    putLongLE(out, OFF_FIRST_KEY, firstRecordKey);
    putLongLE(out, OFF_LAST_KEY, lastRecordKey);
    System.arraycopy(kinds, 0, out, OFF_KINDS, kinds.length);
    int pos = OFF_KINDS + kinds.length;
    putShortLE(out, pos, (short) columnSegmentCount);
    writeColumnSegmentEntries(out, pos + 2, columnSegmentCount, columnSegmentIds, byteLens, contentHashes, colFlags,
        mins, maxs);
    return out;
  }

  /** Range and index-alignment guards for {@link #serialize} — everything checkable up front. */
  private static void checkSerializeShape(final int rowCount, final byte[] kinds, final int columnSegmentCount,
      final int[] columnSegmentIds, final int[] byteLens, final long[] contentHashes, final byte[] colFlags,
      final long[] mins, final long[] maxs) {
    Objects.requireNonNull(kinds, "kinds");
    Objects.requireNonNull(columnSegmentIds, "columnSegmentIds");
    Objects.requireNonNull(byteLens, "byteLens");
    Objects.requireNonNull(contentHashes, "contentHashes");
    Objects.requireNonNull(colFlags, "colFlags");
    Objects.requireNonNull(mins, "mins");
    Objects.requireNonNull(maxs, "maxs");
    if (kinds.length > MAX_COLUMNS) {
      throw new IllegalArgumentException("columnCount " + kinds.length + " exceeds MAX_COLUMNS=" + MAX_COLUMNS);
    }
    if (rowCount < 0 || rowCount > ProjectionIndexRowGroupPage.MAX_ROWS) {
      throw new IllegalArgumentException("rowCount out of range: " + rowCount);
    }
    if (columnSegmentCount < 0 || columnSegmentCount > 0xFFFF) {
      throw new IllegalArgumentException("columnSegmentCount out of range: " + columnSegmentCount);
    }
    if (columnSegmentIds.length < columnSegmentCount || byteLens.length < columnSegmentCount
        || contentHashes.length < columnSegmentCount || colFlags.length < columnSegmentCount
        || mins.length < columnSegmentCount || maxs.length < columnSegmentCount) {
      throw new IllegalArgumentException("entry array shorter than columnSegmentCount=" + columnSegmentCount
          + ": columnSegmentIds=" + columnSegmentIds.length + " byteLens=" + byteLens.length + " contentHashes="
          + contentHashes.length + " colFlags=" + colFlags.length + " mins=" + mins.length + " maxs=" + maxs.length);
    }
    for (int i = 0; i < columnSegmentCount; i++) {
      if (byteLens[i] < ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES || byteLens[i] > MAX_SEGMENT_BYTES) {
        throw new IllegalArgumentException("byteLen out of range at entry " + i + ": " + byteLens[i]);
      }
    }
  }

  /**
   * Write the fixed-size entry table at {@code pos}, enforcing the two invariants every reader relies
   * on: each id fits the 16-bit entry field, and the entries ascend strictly by id (which is what
   * makes {@link #entryIndexOf} a binary search and the write-path merge-joins monotonic).
   */
  private static void writeColumnSegmentEntries(final byte[] out, final int pos, final int columnSegmentCount,
      final int[] columnSegmentIds, final int[] byteLens, final long[] contentHashes, final byte[] colFlags,
      final long[] mins, final long[] maxs) {
    int entryPos = pos;
    int prevId = -1;
    for (int i = 0; i < columnSegmentCount; i++) {
      final int id = columnSegmentIds[i];
      if (id < 0 || id >= HOTLeafPage.MAX_OVERFLOW_PAGE_REF_SUB_ID) {
        throw new IllegalArgumentException("columnSegmentId out of the slot-addressable range [0, "
            + HOTLeafPage.MAX_OVERFLOW_PAGE_REF_SUB_ID + "): " + id);
      }
      if (id <= prevId) {
        throw new IllegalArgumentException(
            "segment entries must be sorted by ascending columnSegmentId: " + id + " after " + prevId);
      }
      prevId = id;
      putShortLE(out, entryPos, (short) id);
      putIntLE(out, entryPos + 2, byteLens[i]);
      putLongLE(out, entryPos + 6, contentHashes[i]);
      out[entryPos + 14] = colFlags[i];
      putLongLE(out, entryPos + 15, mins[i]);
      putLongLE(out, entryPos + 23, maxs[i]);
      entryPos += ENTRY_BYTES;
    }
  }

  // ==================== positional readers (allocation-free) ====================

  /** {@code true} iff {@code value} starts with the descriptor magic. */
  public static boolean isDescriptor(final byte[] value) {
    return value != null && value.length >= 4 && ProjectionIndexRowGroupCodec.getIntLE(value, 0) == MAGIC;
  }

  /**
   * Structural validation: magic, version, plausible counts, exact length. Throws
   * {@link IllegalStateException} on corruption; unknown versions are refused before any positional
   * reader can interpret an incompatible payload.
   */
  public static void validate(final byte[] d) {
    if (!isDescriptor(d)) {
      throw new IllegalStateException("Not a leaf descriptor (missing PIXD magic)");
    }
    if (d.length < OFF_KINDS || d[4] != VERSION) {
      throw new IllegalStateException("Unknown leaf-descriptor version " + (d.length > 4
          ? d[4]
          : "<missing>") + " (expected " + VERSION + ") or truncated header");
    }
    final int rowCount = ProjectionIndexRowGroupCodec.getIntLE(d, OFF_ROW_COUNT);
    final int columnCount = getShortLE(d, OFF_COLUMN_COUNT) & 0xFFFF;
    if (rowCount < 0 || rowCount > ProjectionIndexRowGroupPage.MAX_ROWS || columnCount > MAX_COLUMNS) {
      throw new IllegalStateException("Corrupt leaf descriptor: rowCount=" + rowCount + " columnCount=" + columnCount);
    }
    final int segCountOff = OFF_KINDS + columnCount;
    if (d.length < segCountOff + 2) {
      throw new IllegalStateException("Corrupt leaf descriptor: truncated before columnSegmentCount");
    }
    final int columnSegmentCount = getShortLE(d, segCountOff) & 0xFFFF;
    final int entriesEnd = segCountOff + 2 + columnSegmentCount * ENTRY_BYTES;
    if (d.length < entriesEnd) {
      throw new IllegalStateException("Corrupt leaf descriptor: truncated entry table (length " + d.length + " < "
          + entriesEnd + ", columnSegmentCount=" + columnSegmentCount + ")");
    }
    int previousId = -1;
    for (int i = 0; i < columnSegmentCount; i++) {
      final int id = entryColumnSegmentId(d, i);
      if (id <= previousId) {
        throw new IllegalStateException("Corrupt leaf descriptor: segment ids are not strictly ascending at entry " + i
            + " (" + id + " after " + previousId + ")");
      }
      previousId = id;
      final int byteLen = entryByteLen(d, i);
      if (byteLen < ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES || byteLen > MAX_SEGMENT_BYTES) {
        throw new IllegalStateException("Corrupt leaf descriptor: byteLen=" + byteLen + " at entry " + i);
      }
    }
    if (d.length != entriesEnd) {
      throw new IllegalStateException("Corrupt leaf descriptor: length " + d.length + " != expected " + entriesEnd
          + " (columnSegmentCount=" + columnSegmentCount + ")");
    }
    validateCanonicalSchema(d, rowCount, columnCount, columnSegmentCount);
  }

  /**
   * Validate the sole encoder's semantic entry schema without allocating. Framing alone is not
   * enough: descriptor-only pruning must never accept a leaf missing KEYS/BODY truth or carrying an
   * undeclared acceleration segment that the physical walk would otherwise ignore.
   */
  private static void validateCanonicalSchema(final byte[] descriptor, final int rowCount, final int columnCount,
      final int segmentCount) {
    if (rowCount == 0) {
      requireSentinelPair("record-key fence", firstRecordKey(descriptor), lastRecordKey(descriptor));
    } else {
      final long first = firstRecordKey(descriptor);
      final long last = lastRecordKey(descriptor);
      if (first > last && (first != Long.MAX_VALUE || last != Long.MIN_VALUE)) {
        throw new IllegalStateException(
            "Corrupt leaf descriptor: invalid record-key fences [" + first + ", " + last + "]");
      }
    }

    int entry = requireEntry(descriptor, 0, ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(), "KEYS");
    requireNonBodyMirror(descriptor, 0, "KEYS");
    final int minimumKeysBytes =
        ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES + 2 * Long.BYTES + 1 + 2 * Integer.BYTES;
    requireMinimumBytes(descriptor, 0, minimumKeysBytes, "KEYS");

    for (int column = 0; column < columnCount; column++) {
      final byte kind = kind(descriptor, column);
      requireKnownColumnKind(kind, column);

      final int bodyEntry = entry;
      entry = requireEntry(descriptor, entry, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(column),
          "BODY(" + column + ")");
      requireMinimumBytes(descriptor, bodyEntry,
          ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES + 1 + (rowCount == 0
              ? 0
              : 2 * Long.BYTES + 1),
          "BODY(" + column + ")");
      final int unknownFlags =
          entryColFlags(descriptor, bodyEntry) & ~(ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE
              | ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL
              | ProjectionIndexRowGroupPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE);
      if (unknownFlags != 0) {
        throw new IllegalStateException(
            "Corrupt leaf descriptor: BODY(" + column + ") has unknown flags 0x" + Integer.toHexString(unknownFlags));
      }
      if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE
          && (entryColFlags(descriptor, bodyEntry) & ProjectionIndexRowGroupPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE) != 0) {
        throw new IllegalStateException(
            "Corrupt leaf descriptor: BODY(" + column + ") asserts double provenance for kind " + kind);
      }
      final long min = entryMin(descriptor, bodyEntry);
      final long max = entryMax(descriptor, bodyEntry);
      if (rowCount == 0) {
        requireSentinelPair("BODY(" + column + ") zone map", min, max);
      } else if (min > max && (min != Long.MAX_VALUE || max != Long.MIN_VALUE)) {
        throw new IllegalStateException(
            "Corrupt leaf descriptor: BODY(" + column + ") has invalid zone map [" + min + ", " + max + "]");
      }

      final boolean localString = kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
          || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET;
      if (rowCount > 0 && localString) {
        final int dictEntry = entry;
        entry = requireEntry(descriptor, entry, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(column),
            "DICT(" + column + ")");
        requireNonBodyMirror(descriptor, dictEntry, "DICT(" + column + ")");

        if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET && entry < segmentCount
            && entryColumnSegmentId(descriptor,
                entry) == ProjectionIndexColumnSegmentCodec.setCountsColumnSegmentId(column)) {
          final int countsEntry = entry++;
          requireNonBodyMirror(descriptor, countsEntry, "SET_COUNTS(" + column + ")");
          if (entryByteLen(descriptor, countsEntry) > ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES) {
            throw new IllegalStateException("Corrupt leaf descriptor: SET_COUNTS(" + column + ") has "
                + entryByteLen(descriptor, countsEntry) + " bytes and cannot be inline in its segment slot");
          }
        }

        final int bloomEntry = entry;
        entry = requireEntry(descriptor, entry, ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(column),
            "BLOOM(" + column + ")");
        requireNonBodyMirror(descriptor, bloomEntry, "BLOOM(" + column + ")");
      }
    }

    if (rowCount > 0) {
      for (int column = 0; column < columnCount; column++) {
        if (kind(descriptor, column) != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
          continue;
        }
        final int hashEntry = entry;
        entry = requireEntry(descriptor, entry, ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(column),
            "DICT_HASHES(" + column + ")");
        requireNonBodyMirror(descriptor, hashEntry, "DICT_HASHES(" + column + ")");
      }
    }

    if (entry != segmentCount) {
      throw new IllegalStateException("Corrupt leaf descriptor: unexpected segment id "
          + entryColumnSegmentId(descriptor, entry) + " at entry " + entry);
    }
  }

  /** Return the entry immediately after {@code entryIndex}, or throw on a missing/wrong entry. */
  private static int requireEntry(final byte[] descriptor, final int entryIndex, final int expectedId,
      final String name) {
    final int count = columnSegmentCount(descriptor);
    if (entryIndex >= count || entryColumnSegmentId(descriptor, entryIndex) != expectedId) {
      final String actual = entryIndex >= count
          ? "<missing>"
          : Integer.toString(entryColumnSegmentId(descriptor, entryIndex));
      throw new IllegalStateException(
          "Corrupt leaf descriptor: expected " + name + " segment id " + expectedId + " but found " + actual);
    }
    if (expectedId >= HOTLeafPage.MAX_OVERFLOW_PAGE_REF_SUB_ID) {
      throw new IllegalStateException(
          "Corrupt leaf descriptor: segment id " + expectedId + " cannot be encoded as slotKind=id+1");
    }
    return entryIndex + 1;
  }

  private static void requireKnownColumnKind(final byte kind, final int column) {
    if (kind < ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG
        || kind > ProjectionIndexRowGroupPage.MAX_COLUMN_KIND) {
      throw new IllegalStateException("Corrupt leaf descriptor: unknown column kind " + kind + " at " + column);
    }
  }

  private static void requireNonBodyMirror(final byte[] descriptor, final int entry, final String name) {
    if (entryColFlags(descriptor, entry) != 0 || entryMin(descriptor, entry) != 0L
        || entryMax(descriptor, entry) != 0L) {
      throw new IllegalStateException("Corrupt leaf descriptor: " + name + " carries BODY-only mirror fields");
    }
  }

  private static void requireMinimumBytes(final byte[] descriptor, final int entry, final int minimum,
      final String name) {
    if (entryByteLen(descriptor, entry) < minimum) {
      throw new IllegalStateException("Corrupt leaf descriptor: " + name + " has " + entryByteLen(descriptor, entry)
          + " bytes, expected at least " + minimum);
    }
  }

  private static void requireSentinelPair(final String name, final long min, final long max) {
    if (min != Long.MAX_VALUE || max != Long.MIN_VALUE) {
      throw new IllegalStateException("Corrupt leaf descriptor: empty " + name + " is [" + min + ", " + max
          + "] instead of the canonical sentinel pair");
    }
  }

  public static int rowCount(final byte[] d) {
    return ProjectionIndexRowGroupCodec.getIntLE(d, OFF_ROW_COUNT);
  }

  public static int columnCount(final byte[] d) {
    return getShortLE(d, OFF_COLUMN_COUNT) & 0xFFFF;
  }

  public static long firstRecordKey(final byte[] d) {
    return ProjectionIndexRowGroupCodec.getLongLE(d, OFF_FIRST_KEY);
  }

  public static long lastRecordKey(final byte[] d) {
    return ProjectionIndexRowGroupCodec.getLongLE(d, OFF_LAST_KEY);
  }

  public static byte kind(final byte[] d, final int column) {
    return d[OFF_KINDS + column];
  }

  /**
   * Whether two descriptors declare the SAME encoding for every column.
   *
   * <p>
   * Every leaf of one projection must, and the store depends on that hard enough to read the kinds
   * from leaf 0 alone and dispatch an entire query on them. When maintenance broke the invariant —
   * writing one leaf's descriptor as {@code STRING_DICT} beside a payload that was still
   * {@code STRING_GLOBAL} — nothing noticed until a decode four rounds later reported the column as
   * corrupt (tasks #45, #50). Checking costs one range comparison because the kinds are contiguous,
   * so the invariant can be VERIFIED instead of assumed.
   * </p>
   *
   * <p>
   * Descriptors that disagree about how many columns they have disagree, full stop: they cannot be
   * describing the same projection.
   * </p>
   *
   * @param a one descriptor; never {@code null}
   * @param b the other; never {@code null}
   * @return whether both declare identical kinds for identically many columns
   */
  public static boolean kindsAgree(final byte[] a, final byte[] b) {
    final int columns = columnCount(a);
    if (columns != columnCount(b)) {
      return false;
    }
    return Arrays.equals(a, OFF_KINDS, OFF_KINDS + columns, b, OFF_KINDS, OFF_KINDS + columns);
  }

  public static int columnSegmentCount(final byte[] d) {
    return getShortLE(d, OFF_KINDS + columnCount(d)) & 0xFFFF;
  }

  private static int entriesOffset(final byte[] d) {
    return OFF_KINDS + columnCount(d) + 2;
  }

  /**
   * Index of the entry for {@code columnSegmentId}, or {@code -1} when absent. Entries are sorted
   * ascending by columnSegmentId (serialize enforces it), so this binary-searches: the write-side
   * carry-forward loops call this once per encoded segment, and at wide-table segment counts (up to
   * {@code 4·MAX_COLUMNS} ≈ 65k) a linear scan would make those loops quadratic.
   */
  public static int entryIndexOf(final byte[] d, final int columnSegmentId) {
    final int base = entriesOffset(d);
    int lo = 0;
    int hi = columnSegmentCount(d) - 1;
    while (lo <= hi) {
      final int mid = (lo + hi) >>> 1;
      final int id = getShortLE(d, base + mid * ENTRY_BYTES) & 0xFFFF;
      if (id == columnSegmentId) {
        return mid;
      }
      if (id < columnSegmentId) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return -1;
  }

  public static int entryColumnSegmentId(final byte[] d, final int entryIndex) {
    return getShortLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES) & 0xFFFF;
  }

  /** The segment's exact payload length in its adjacent segment slot. */
  public static int entryByteLen(final byte[] d, final int entryIndex) {
    return ProjectionIndexRowGroupCodec.getIntLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 2);
  }

  public static long entryContentHash(final byte[] d, final int entryIndex) {
    return ProjectionIndexRowGroupCodec.getLongLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 6);
  }

  public static byte entryColFlags(final byte[] d, final int entryIndex) {
    return d[entriesOffset(d) + entryIndex * ENTRY_BYTES + 14];
  }

  public static long entryMin(final byte[] d, final int entryIndex) {
    return ProjectionIndexRowGroupCodec.getLongLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 15);
  }

  public static long entryMax(final byte[] d, final int entryIndex) {
    return ProjectionIndexRowGroupCodec.getLongLE(d, entriesOffset(d) + entryIndex * ENTRY_BYTES + 23);
  }

  // ==================== little-endian primitives ====================

  private static void putShortLE(final byte[] b, final int off, final short v) {
    b[off] = (byte) v;
    b[off + 1] = (byte) (v >>> 8);
  }

  private static short getShortLE(final byte[] b, final int off) {
    return (short) ((b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8));
  }

  static void putIntLE(final byte[] b, final int off, final int v) {
    b[off] = (byte) v;
    b[off + 1] = (byte) (v >>> 8);
    b[off + 2] = (byte) (v >>> 16);
    b[off + 3] = (byte) (v >>> 24);
  }


  static void putLongLE(final byte[] b, final int off, final long v) {
    putIntLE(b, off, (int) v);
    putIntLE(b, off + 4, (int) (v >>> 32));
  }

}
