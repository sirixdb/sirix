package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;
import io.sirix.utils.ToStringHelper;

import java.util.Arrays;
import java.util.Objects;

/**
 * The resource's segment-dictionary directory: which document-page key ranges form a segment, and
 * for every {@code (segment, slot)} the sealed dictionary generation that decodes its ids.
 *
 * <p>
 * ONE record per resource at the fixed key {@link #DIRECTORY_KEY} of the projection-value-dictionary
 * sub-trie. A resource that never bound the segment lane has either nothing or a plain dictionary
 * header at that key, so readers dispatch on the record's type rather than on a flag anywhere else:
 * {@code instanceof SegmentDictionaryDirectoryNode} is the whole discovery protocol, and the
 * {@code NamePage} format is untouched. The lane reserves keys {@code 1..1023} when it binds, which
 * leaves the directory alone in page 0 of the sub-trie ({@code pageKey = recordKey >>> 10}); a
 * rewrite of the directory therefore never copies a value block.
 * </p>
 *
 * <p>
 * A <b>segment</b> is a range of document page keys: segment {@code s} holds the pages
 * {@code segmentStart(s) .. segmentStart(s + 1) - 1} (the last one is open-ended). Boundaries are
 * decided once by the thread that adopts pages in page-key order and are never moved; a reader maps
 * a page to its segment by binary search. A <b>slot</b> is the projection column index of the load
 * that wrote the segment, and each slot lists the path-class tags it covers — a tag belongs to at
 * most one slot within a segment, verified here. Slot tables are per segment on purpose: a later
 * load may bind its columns differently, and a page's ids must resolve through the assignment that
 * was in force when it was written.
 * </p>
 *
 * <p>
 * Every array handed out is the record's own; callers must not modify it.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentDictionaryDirectoryNode implements DataRecord {

  /** The fixed key of the directory record in the projection-value-dictionary sub-trie. */
  public static final long DIRECTORY_KEY = 1L;

  /** Keys the lane reserves at bind so the directory is alone in the sub-trie's page 0. */
  public static final long RESERVED_KEYS = 1023L;

  /** Bounds a deserializer trusts before sizing arrays. */
  public static final int MAX_SEGMENTS = 1 << 24;

  public static final int MAX_SLOTS_PER_SEGMENT = 1 << 16;

  public static final int MAX_TAGS_PER_SLOT = 1 << 20;

  private static final int[] NO_TAGS = new int[0];

  /**
   * One segment's slot table: three parallel arrays indexed by slot. A slot with no dictionary in
   * this segment (a non-string column, or a string column that minted nothing) has no tags, header
   * key 0 and entry count 0.
   */
  public static final class SlotTable {

    /** The table of a segment with no slots at all — the open segment before its seal. */
    public static final SlotTable EMPTY = new SlotTable(new int[0][], new long[0], new int[0]);

    private final int[][] tagsBySlot;

    private final long[] headerKeys;

    private final int[] entryCounts;

    private SlotTable(final int[][] tagsBySlot, final long[] headerKeys, final int[] entryCounts) {
      this.tagsBySlot = tagsBySlot;
      this.headerKeys = headerKeys;
      this.entryCounts = entryCounts;
    }

    /**
     * Takes ownership of the arrays; the caller must not touch them again.
     *
     * @param tagsBySlot per slot its tags, each strictly ascending; an empty array for a slot
     *        without a dictionary
     * @param headerKeys per slot the sealed generation's header key, 0 for none
     * @param entryCounts per slot the sealed generation's entry count, 0 for none
     */
    public static SlotTable takeOwnership(final int[][] tagsBySlot, final long[] headerKeys,
        final int[] entryCounts) {
      if (tagsBySlot == null || headerKeys == null || entryCounts == null) {
        throw new NullPointerException("slot table arrays must not be null");
      }
      final int slots = tagsBySlot.length;
      if (slots > MAX_SLOTS_PER_SEGMENT || headerKeys.length != slots || entryCounts.length != slots) {
        throw new IllegalArgumentException("slot table arrays disagree on the slot count: " + slots + "/"
            + headerKeys.length + "/" + entryCounts.length);
      }
      int totalTags = 0;
      for (int slot = 0; slot < slots; slot++) {
        final int[] tags = tagsBySlot[slot];
        if (tags == null) {
          throw new NullPointerException("slot " + slot + " has null tags; use an empty array");
        }
        if (tags.length > MAX_TAGS_PER_SLOT) {
          throw new IllegalArgumentException("slot " + slot + " lists " + tags.length + " tags");
        }
        for (int i = 0; i < tags.length; i++) {
          if (tags[i] < 0 || (i > 0 && tags[i] <= tags[i - 1])) {
            throw new IllegalArgumentException("slot " + slot + "'s tags must be non-negative and strictly ascending");
          }
        }
        totalTags = Math.addExact(totalTags, tags.length);
        final long headerKey = headerKeys[slot];
        final int entryCount = entryCounts[slot];
        if (headerKey < 0 || entryCount < 0 || (headerKey == 0) != (entryCount == 0)) {
          throw new IllegalArgumentException("slot " + slot + " has header key " + headerKey + " with entry count "
              + entryCount + "; a sealed dictionary has both or neither");
        }
        if (tags.length == 0 && headerKey != 0) {
          throw new IllegalArgumentException("slot " + slot + " has a dictionary but covers no tag");
        }
      }
      // A tag in two slots of one segment would give one value two ids; merge every slot's tags and
      // look for a duplicate once, at construction, so no reader has to.
      if (totalTags > 1) {
        final int[] all = new int[totalTags];
        int n = 0;
        for (final int[] tags : tagsBySlot) {
          System.arraycopy(tags, 0, all, n, tags.length);
          n += tags.length;
        }
        Arrays.sort(all);
        for (int i = 1; i < all.length; i++) {
          if (all[i] == all[i - 1]) {
            throw new IllegalArgumentException("tag " + all[i] + " is covered by two slots of one segment");
          }
        }
      }
      return new SlotTable(tagsBySlot, headerKeys, entryCounts);
    }

    /** Number of slots (the highest projection column index the segment's load dictionary-encoded, plus one). */
    public int slotCount() {
      return headerKeys.length;
    }

    /** The tags slot {@code slot} covers, strictly ascending; empty for a slot without a dictionary. */
    public int[] tags(final int slot) {
      return slot < 0 || slot >= tagsBySlot.length
          ? NO_TAGS
          : tagsBySlot[slot];
    }

    /** Header key of the slot's sealed generation, or 0 (also for a slot index beyond the table). */
    public long headerKey(final int slot) {
      return slot < 0 || slot >= headerKeys.length
          ? 0L
          : headerKeys[slot];
    }

    /** Entry count of the slot's sealed generation, or 0. */
    public int entryCount(final int slot) {
      return slot < 0 || slot >= entryCounts.length
          ? 0
          : entryCounts[slot];
    }

    /** The slot covering {@code tag}, or {@code -1}. */
    public int slotOfTag(final int tag) {
      for (int slot = 0; slot < tagsBySlot.length; slot++) {
        if (Arrays.binarySearch(tagsBySlot[slot], tag) >= 0) {
          return slot;
        }
      }
      return -1;
    }
  }

  private final long nodeKey;

  /** First document page key of every segment, strictly ascending, {@code [0] == 0}. */
  private final long[] segmentStarts;

  /** Index-aligned with {@link #segmentStarts}. */
  private final SlotTable[] slotTables;

  private SegmentDictionaryDirectoryNode(final long nodeKey, final long[] segmentStarts,
      final SlotTable[] slotTables) {
    this.nodeKey = nodeKey;
    this.segmentStarts = segmentStarts;
    this.slotTables = slotTables;
  }

  /**
   * Takes ownership of the arrays; the caller must not touch them again.
   *
   * @param nodeKey must be {@link #DIRECTORY_KEY}
   * @param segmentStarts first page key of every segment, strictly ascending, starting at 0
   * @param slotTables one table per segment, {@link SlotTable#EMPTY} for a segment not yet sealed
   */
  public static SegmentDictionaryDirectoryNode takeOwnership(final long nodeKey, final long[] segmentStarts,
      final SlotTable[] slotTables) {
    if (nodeKey != DIRECTORY_KEY) {
      throw new IllegalArgumentException("the segment dictionary directory lives at key " + DIRECTORY_KEY + ", not "
          + nodeKey);
    }
    if (segmentStarts == null || slotTables == null) {
      throw new NullPointerException("directory arrays must not be null");
    }
    if (segmentStarts.length == 0 || segmentStarts.length > MAX_SEGMENTS) {
      throw new IllegalArgumentException("a segment directory holds 1.." + MAX_SEGMENTS + " segments, not "
          + segmentStarts.length);
    }
    if (slotTables.length != segmentStarts.length) {
      throw new IllegalArgumentException("directory has " + segmentStarts.length + " segments but "
          + slotTables.length + " slot tables");
    }
    if (segmentStarts[0] != 0L) {
      throw new IllegalArgumentException("segment 0 must start at page key 0, not " + segmentStarts[0]);
    }
    for (int s = 1; s < segmentStarts.length; s++) {
      if (segmentStarts[s] <= segmentStarts[s - 1]) {
        throw new IllegalArgumentException("segment starts must ascend strictly, at segment " + s);
      }
    }
    for (int s = 0; s < slotTables.length; s++) {
      if (slotTables[s] == null) {
        throw new NullPointerException("segment " + s + " has a null slot table; use SlotTable.EMPTY");
      }
    }
    return new SegmentDictionaryDirectoryNode(nodeKey, segmentStarts, slotTables);
  }

  /** How many segments exist, the open one included. */
  public int segmentCount() {
    return segmentStarts.length;
  }

  /**
   * First document page key of segment {@code segment}.
   *
   * @throws IndexOutOfBoundsException if {@code segment} is not in {@code 0..segmentCount()-1} — the
   *         directory is the read side's map from page to dictionary, so a wrong segment must fail
   *         here rather than resolve against another segment's dictionary
   */
  public long segmentStart(final int segment) {
    return segmentStarts[Objects.checkIndex(segment, segmentStarts.length)];
  }

  /**
   * The segment holding document page {@code pageKey}: the last segment whose start does not exceed
   * it. Binary search; the hot callers cache per page.
   *
   * @param pageKey a document page key, at least 0
   */
  public int segmentOf(final long pageKey) {
    if (pageKey < 0L) {
      throw new IllegalArgumentException("negative page key " + pageKey);
    }
    int low = 0;
    int high = segmentStarts.length - 1;
    while (low < high) {
      final int mid = (low + high + 1) >>> 1;
      if (segmentStarts[mid] <= pageKey) {
        low = mid;
      } else {
        high = mid - 1;
      }
    }
    return low;
  }

  /**
   * The slot table of segment {@code segment}.
   *
   * @throws IndexOutOfBoundsException if {@code segment} is not in {@code 0..segmentCount()-1}
   */
  public SlotTable slots(final int segment) {
    return slotTables[Objects.checkIndex(segment, slotTables.length)];
  }

  /**
   * Header key of {@code (segment, slot)}'s sealed generation, or 0 when there is none (a slot the
   * segment does not have answers 0 too: the slot table is lenient, the segment index is not).
   *
   * @throws IndexOutOfBoundsException if {@code segment} is not in {@code 0..segmentCount()-1}
   */
  public long headerKey(final int segment, final int slot) {
    return slots(segment).headerKey(slot);
  }

  /**
   * Entry count of {@code (segment, slot)}'s sealed generation, or 0.
   *
   * @throws IndexOutOfBoundsException if {@code segment} is not in {@code 0..segmentCount()-1}
   */
  public int entryCount(final int segment, final int slot) {
    return slots(segment).entryCount(slot);
  }

  /** The first page key of every segment, the record's own array. */
  public long[] segmentStarts() {
    return segmentStarts;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.SEGMENT_DICTIONARY_DIRECTORY;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return 0;
  }

  @Override
  public int getPreviousRevisionNumber() {
    return 0;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return null;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return null;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this)
                         .add("nodeKey", nodeKey)
                         .add("segments", segmentStarts.length)
                         .toString();
  }
}
