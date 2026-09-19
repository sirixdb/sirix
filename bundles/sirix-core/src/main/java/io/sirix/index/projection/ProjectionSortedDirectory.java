/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * Sparse, revisioned fence hierarchy over sorted covering leaves.
 *
 * <p>
 * Each directory entry is the first key of one child plus its four-byte child id. The hierarchy
 * uses the same bounded, prefix-compressed leaf format as the data. Only the selected path is read
 * for a seek; the data leaves remain separate copy-on-write blobs. A newly built hierarchy becomes
 * visible only when its small root header is published at the end of the owning transaction. The
 * header also records the key layout, how many rows are held under the reserved unencodable key,
 * and how many rows have no value in the aggregated last key field.
 * </p>
 *
 * <p>
 * Header versions follow the projection formats' rule: backward-readable, not forward-readable. A
 * version-3 header, written before the missing-aggregate count existed, still parses and reports
 * that count as {@link #MISSING_AGGREGATE_ROWS_UNKNOWN}; such a view keeps the behaviour it had
 * before the count was added and gains an exact one when it is rebuilt or its leaf summaries are
 * backfilled. A header carrying the count is written at version 4, which an earlier release rejects.
 * </p>
 */
final class ProjectionSortedDirectory {

  /** Blob side-page owner keys must remain below 2^47. */
  static final long HEADER_SLOT = ProjectionSortedLeafStore.LEAF_SLOT_BASE + (1L << 32);
  /** Empty lower bound: a range starting at the view's first key. */
  static final byte[] NO_BOUND = new byte[0];
  private static final byte[] UNENCODABLE_PREFIX = {ProjectionSortKeyCodec.UNENCODABLE};
  private static final byte[] EMPTY_PAYLOAD = new byte[0];
  private static final byte[][] NO_KEYS = new byte[0][];
  private static final int MAGIC = 0x31445350; // PSD1
  /** Header carrying the missing-aggregate count; {@link #VERSION_WITHOUT_MISSING_AGGREGATE} lacks it. */
  private static final byte VERSION = 4;
  private static final byte VERSION_WITHOUT_MISSING_AGGREGATE = 3;
  private static final int FIXED_HEADER_BYTES = 39;
  private static final int FIXED_HEADER_BYTES_WITHOUT_MISSING_AGGREGATE = 31;
  /** Rows without a value in the aggregated last key field, for a view written before they were counted. */
  static final long MISSING_AGGREGATE_ROWS_UNKNOWN = -1;
  private static final int MAX_HEIGHT = 8;
  /** First capacity of a range's leaf-id array; it doubles up to the caller's maximum. */
  private static final int INITIAL_RANGE_IDS = 64;

  private ProjectionSortedDirectory() {}

  static @Nullable Accessor open(final StorageEngineReader reader, final int indexNumber) {
    Objects.requireNonNull(reader, "reader");
    final byte[] header = ProjectionIndexHOTStorage.readBlob(reader, indexNumber, HEADER_SLOT);
    return header == null
        ? null
        : new Accessor(reader, indexNumber, header);
  }

  /**
   * Parsed, validated root header shared by readers and the transaction's editor.
   *
   * <p>
   * {@code missingAggregateRows} is {@link #MISSING_AGGREGATE_ROWS_UNKNOWN} for a header written
   * before the count existed, and a nonnegative exact count otherwise. A header serializes at the
   * version that matches that knowledge, so the field's presence on disk <em>is</em> the knowledge
   * and an unknown count is never written as a number a reader could trust.
   * </p>
   */
  private record Header(int height, int rootId, int nodeCount, int dataLeafCount, int maxLeafId, long unencodableRows,
      long missingAggregateRows, ProjectionSortKeyCodec.Layout layout) {

    static Header parse(final byte[] header) {
      if (header.length < Integer.BYTES + 1 || getInt(header, 0) != MAGIC) {
        throw new IllegalStateException("invalid sorted projection directory header");
      }
      final byte version = header[4];
      final int fixedBytes = version == VERSION
          ? FIXED_HEADER_BYTES
          : version == VERSION_WITHOUT_MISSING_AGGREGATE
              ? FIXED_HEADER_BYTES_WITHOUT_MISSING_AGGREGATE
              : -1;
      if (fixedBytes < 0 || header.length < fixedBytes + 1
          || header.length != fixedBytes + (header[fixedBytes - 1] & 0xFF)) {
        throw new IllegalStateException("invalid sorted projection directory header");
      }
      final int height = header[5] & 0xFF;
      final int rootId = getInt(header, 6);
      final int nodeCount = getInt(header, 10);
      final int dataLeafCount = getInt(header, 14);
      final int maxLeafId = getInt(header, 18);
      final long unencodableRows = getLong(header, 22);
      final long missingAggregateRows = version == VERSION
          ? getLong(header, 30)
          : MISSING_AGGREGATE_ROWS_UNKNOWN;
      if (height > MAX_HEIGHT || nodeCount < 0 || dataLeafCount < 0 || (rootId == 0) != (dataLeafCount == 0)
          || (dataLeafCount == 0) != (height == 0) || rootId > nodeCount || rootId < 0 || maxLeafId < dataLeafCount
          || unencodableRows < 0 || version == VERSION && missingAggregateRows < 0) {
        throw new IllegalStateException("invalid sorted projection directory dimensions");
      }
      final ProjectionSortKeyCodec.Layout layout;
      try {
        layout = new ProjectionSortKeyCodec.Layout(Arrays.copyOfRange(header, fixedBytes, header.length));
      } catch (final IllegalArgumentException invalid) {
        throw new IllegalStateException("invalid sorted projection key layout", invalid);
      }
      return new Header(height, rootId, nodeCount, dataLeafCount, maxLeafId, unencodableRows, missingAggregateRows,
          layout);
    }

    byte[] serialize() {
      final boolean counted = missingAggregateRows != MISSING_AGGREGATE_ROWS_UNKNOWN;
      final int fixedBytes = counted
          ? FIXED_HEADER_BYTES
          : FIXED_HEADER_BYTES_WITHOUT_MISSING_AGGREGATE;
      final byte[] fields = layout.toBytes();
      final byte[] header = new byte[fixedBytes + fields.length];
      putInt(header, 0, MAGIC);
      header[4] = counted
          ? VERSION
          : VERSION_WITHOUT_MISSING_AGGREGATE;
      header[5] = (byte) height;
      putInt(header, 6, rootId);
      putInt(header, 10, nodeCount);
      putInt(header, 14, dataLeafCount);
      putInt(header, 18, maxLeafId);
      putLong(header, 22, unencodableRows);
      if (counted) {
        putLong(header, 30, missingAggregateRows);
      }
      header[fixedBytes - 1] = (byte) fields.length;
      System.arraycopy(fields, 0, header, fixedBytes, fields.length);
      return header;
    }
  }

  /**
   * Directory row whose fence is at or immediately below {@code key}; the first row below the
   * minimum.
   */
  private static int atOrBelow(final ProjectionSortedLeaf node, final byte[] key) {
    final int lower = node.lowerBound(key);
    return lower == node.rowCount()
        ? lower - 1
        : lower == 0 || node.compareRowKey(lower, key) == 0
            ? lower
            : lower - 1;
  }

  static final class Accessor {
    private final StorageEngineReader reader;
    private final int indexNumber;
    private final int height;
    private final int nodeCount;
    private final int dataLeafCount;
    private final int maxLeafId;
    private final long unencodableRows;
    private final long missingAggregateRows;
    private final ProjectionSortKeyCodec.Layout layout;
    private final @Nullable ProjectionSortedLeaf root;

    private Accessor(final StorageEngineReader reader, final int indexNumber, final byte[] header) {
      final Header parsed = Header.parse(header);
      this.reader = reader;
      this.indexNumber = indexNumber;
      this.height = parsed.height();
      this.nodeCount = parsed.nodeCount();
      this.dataLeafCount = parsed.dataLeafCount();
      this.maxLeafId = parsed.maxLeafId();
      this.unencodableRows = parsed.unencodableRows();
      this.missingAggregateRows = parsed.missingAggregateRows();
      this.layout = parsed.layout();
      this.root = parsed.rootId() == 0
          ? null
          : readNode(parsed.rootId());
    }

    int dataLeafCount() {
      return dataLeafCount;
    }

    /** Rows kept under the reserved unencodable key; while any exist the view is not servable. */
    long unencodableRows() {
      return unencodableRows;
    }

    /**
     * Rows whose aggregated last key field has no value, or {@link #MISSING_AGGREGATE_ROWS_UNKNOWN}
     * for a view written before they were counted.
     */
    long missingAggregateRows() {
      return missingAggregateRows;
    }

    /**
     * Whether the header states that some row has no aggregate value. Only then can a leaf without a
     * group summary be explained by such a row, which is what lets the summaries route prove that the
     * full-key walk cannot serve a range either. A zero count and an unknown one both answer
     * {@code false} and leave every route exactly as it was: with a zero count a missing summary can
     * only be an unsummarized revision, and with an unknown count it may be either.
     */
    boolean holdsRowsWithoutAggregateValues() {
      return missingAggregateRows > 0;
    }

    ProjectionSortKeyCodec.Layout layout() {
      return layout;
    }

    /**
     * Data leaf at or immediately below {@code key}. Below the minimum, the first leaf is returned;
     * above the maximum, the last. An empty directory returns zero.
     */
    int findLeafId(final byte[] key) {
      Objects.requireNonNull(key, "key");
      ProjectionSortedLeaf node = root;
      if (node == null) {
        return 0;
      }
      for (int depth = height; depth > 0; depth--) {
        final int childId = node.intPayloadAt(atOrBelow(node, key));
        if (childId < 1) {
          throw new IllegalStateException("sorted projection directory names a nonpositive child id");
        }
        if (depth == 1) {
          if (childId > maxLeafId) {
            throw new IllegalStateException("sorted projection directory names an absent data leaf");
          }
          return childId;
        }
        node = readNode(childId);
      }
      throw new IllegalStateException("sorted projection directory has no data level");
    }

    /** Position at the first key at or after {@code key}, ready for an in-order range scan. */
    Cursor seek(final byte[] key) {
      Objects.requireNonNull(key, "key");
      final Cursor cursor = new Cursor();
      if (root == null) {
        return cursor;
      }
      ProjectionSortedLeaf node = root;
      for (int level = 0; level < height; level++) {
        cursor.nodes[level] = node;
        final int position = atOrBelow(node, key);
        cursor.positions[level] = position;
        final int childId = node.intPayloadAt(position);
        if (level == height - 1) {
          cursor.loadLeaf(childId);
        } else {
          node = readNode(childId);
        }
      }
      cursor.row = cursor.leaf.lowerBound(key);
      if (cursor.row == cursor.leaf.rowCount()) {
        cursor.moveToNextLeaf();
      }
      return cursor;
    }

    Cursor first() {
      return seek(NO_BOUND);
    }

    /** Walk live data-leaf IDs in key order, without reading the data leaves themselves. */
    LeafCursor leaves() {
      return new LeafCursor(NO_BOUND, null);
    }

    /**
     * Walk the data leaves that can hold keys in {@code [from, upperExclusive)}: from the leaf at or
     * below {@code from} up to the last leaf whose first key is below {@code upperExclusive}.
     */
    LeafCursor leaves(final byte[] from, final byte @Nullable [] upperExclusive) {
      return new LeafCursor(Objects.requireNonNull(from, "from"), upperExclusive);
    }

    /** Physical ids of {@link #leaves(byte[], byte[])} in key order, or null beyond {@code maximum}. */
    int @Nullable [] leafIds(final byte[] from, final byte @Nullable [] upperExclusive, final int maximum) {
      final LeafCursor cursor = leaves(from, upperExclusive);
      int[] ids = new int[Math.max(1, Math.min(INITIAL_RANGE_IDS, maximum))];
      int count = 0;
      while (cursor.id() != 0) {
        if (count == maximum) {
          return null;
        }
        if (count == ids.length) {
          ids = Arrays.copyOf(ids, (int) Math.min(maximum, (long) ids.length << 1));
        }
        ids[count++] = cursor.id();
        cursor.advance();
      }
      return count == ids.length
          ? ids
          : Arrays.copyOf(ids, count);
    }

    /**
     * Leaves of {@link #leaves(byte[], byte[])}, counted up to {@code cap}; the whole view needs no
     * walk.
     */
    int leafCount(final byte[] from, final byte @Nullable [] upperExclusive, final int cap) {
      if (from.length == 0 && upperExclusive == null) {
        return Math.min(dataLeafCount, cap);
      }
      final LeafCursor cursor = leaves(from, upperExclusive);
      int count = 0;
      while (count < cap && cursor.id() != 0) {
        count++;
        cursor.advance();
      }
      return count;
    }

    final class LeafCursor {
      private final ProjectionSortedLeaf[] nodes = new ProjectionSortedLeaf[height];
      private final int[] positions = new int[height];
      private final byte @Nullable [] upperExclusive;
      private final boolean complete;
      private int id;
      private int visited;

      private LeafCursor(final byte[] from, final byte @Nullable [] upperExclusive) {
        this.upperExclusive = upperExclusive;
        this.complete = from.length == 0 && upperExclusive == null;
        if (root == null) {
          return;
        }
        if (from.length == 0) {
          nodes[0] = root;
          descend(0, root.intPayloadAt(0));
        } else {
          ProjectionSortedLeaf node = root;
          for (int level = 0; level < height; level++) {
            nodes[level] = node;
            final int position = atOrBelow(node, from);
            positions[level] = position;
            final int childId = node.intPayloadAt(position);
            if (level == height - 1) {
              if (childId < 1 || childId > maxLeafId) {
                throw new IllegalStateException("sorted directory names an absent data leaf");
              }
              id = childId;
            } else {
              node = readNode(childId);
            }
          }
        }
        visited = 1;
        if (!belowUpper()) {
          id = 0;
        }
      }

      int id() {
        return id;
      }

      /** First tuple of the current data leaf, available from the directory without reading it. */
      int firstKeyLength() {
        if (id == 0) {
          throw new IllegalStateException("sorted leaf cursor is exhausted");
        }
        return nodes[height - 1].keyLength(positions[height - 1]);
      }

      void copyFirstKeyTo(final byte[] target) {
        firstKeyLength();
        nodes[height - 1].copyKeyTo(positions[height - 1], target);
      }

      boolean advance() {
        if (id == 0) {
          return false;
        }
        for (int level = height - 1; level >= 0; level--) {
          final int next = positions[level] + 1;
          if (next < nodes[level].rowCount()) {
            positions[level] = next;
            descend(level, nodes[level].intPayloadAt(next));
            if (++visited > dataLeafCount) {
              throw new IllegalStateException("sorted directory contains more leaves than declared");
            }
            if (!belowUpper()) {
              id = 0;
              return false;
            }
            return true;
          }
        }
        if (complete && visited != dataLeafCount) {
          throw new IllegalStateException("sorted directory contains fewer leaves than declared");
        }
        id = 0;
        return false;
      }

      private boolean belowUpper() {
        return upperExclusive == null || nodes[height - 1].compareRowKey(positions[height - 1], upperExclusive) < 0;
      }

      private void descend(final int level, final int child) {
        int childId = child;
        for (int down = level + 1; down < height; down++) {
          final ProjectionSortedLeaf node = readNode(childId);
          nodes[down] = node;
          positions[down] = 0;
          childId = node.intPayloadAt(0);
        }
        if (childId < 1 || childId > maxLeafId) {
          throw new IllegalStateException("sorted directory names an absent data leaf");
        }
        id = childId;
      }
    }

    /** A cursor owns only the directory path and current leaf; advancing never scans leaf IDs. */
    final class Cursor {
      private final ProjectionSortedLeaf[] nodes = new ProjectionSortedLeaf[height];
      private final int[] positions = new int[height];
      private @Nullable ProjectionSortedLeaf leaf;
      private int leafId;
      private int row;
      private @Nullable ProjectionSortedLeaf lastSkippedLeaf;
      private int lastSkippedRow;

      boolean isValid() {
        return leaf != null;
      }

      int leafId() {
        requirePosition();
        return leafId;
      }

      int row() {
        requirePosition();
        return row;
      }

      byte[] copyKey() {
        requirePosition();
        return leaf.copyKey(row);
      }

      int keyLength() {
        requirePosition();
        return leaf.keyLength(row);
      }

      void copyKeyTo(final byte[] target) {
        requirePosition();
        leaf.copyKeyTo(row, target);
      }

      int payloadLength() {
        requirePosition();
        return leaf.payloadLength(row);
      }

      void copyPayloadTo(final byte[] target, final int offset) {
        requirePosition();
        leaf.copyPayloadTo(row, target, offset);
      }

      /** Advance to the next key; return false after the last key. */
      boolean advance() {
        if (leaf == null) {
          return false;
        }
        if (++row < leaf.rowCount()) {
          return true;
        }
        return moveToNextLeaf();
      }

      /** Skip the remainder of the current key prefix without visiting each matching row. */
      boolean skipPrefix(final byte[] prefix, final int length) {
        return skipPrefix(prefix, length, false);
      }

      /** Skip a prefix and retain its final key for a grouped maximum. */
      boolean skipPrefixCapturingLast(final byte[] prefix, final int length) {
        return skipPrefix(prefix, length, true);
      }

      int lastSkippedKeyLength() {
        if (lastSkippedLeaf == null) {
          throw new IllegalStateException("no skipped key was captured");
        }
        return lastSkippedLeaf.keyLength(lastSkippedRow);
      }

      void copyLastSkippedKeyTo(final byte[] target) {
        if (lastSkippedLeaf == null) {
          throw new IllegalStateException("no skipped key was captured");
        }
        lastSkippedLeaf.copyKeyTo(lastSkippedRow, target);
      }

      private boolean skipPrefix(final byte[] prefix, final int length, final boolean captureLast) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.checkFromIndexSize(0, length, prefix.length);
        requirePosition();
        lastSkippedLeaf = null;
        while (true) {
          final int low = leaf.firstNonPrefixRowAfter(row, prefix, length);
          if (captureLast) {
            lastSkippedLeaf = leaf;
            lastSkippedRow = low - 1;
          }
          if (low < leaf.rowCount()) {
            row = low;
            return true;
          }
          if (!moveToLastPrefixLeaf(prefix, length) && !moveToNextLeaf()) {
            return false;
          }
          if (!leaf.keyHasPrefix(row, prefix, length)) {
            return true;
          }
        }
      }

      /**
       * Jump to the last later leaf whose fence has the current prefix. Every intervening key belongs to
       * that prefix by sorted order. The destination still needs reading: it can also contain the next
       * group, and its last matching row supplies a captured maximum.
       */
      private boolean moveToLastPrefixLeaf(final byte[] prefix, final int length) {
        int selectedLevel = -1;
        int selectedPosition = -1;
        for (int level = height - 1; level >= 0; level--) {
          final ProjectionSortedLeaf node = nodes[level];
          final int next = positions[level] + 1;
          if (next == node.rowCount()) {
            continue;
          }
          if (!node.keyHasPrefix(next, prefix, length)) {
            break;
          }
          selectedLevel = level;
          selectedPosition = node.firstNonPrefixRowAfter(next, prefix, length) - 1;
          if (selectedPosition + 1 < node.rowCount()) {
            break;
          }
        }
        if (selectedLevel < 0) {
          return false;
        }
        positions[selectedLevel] = selectedPosition;
        int childId = nodes[selectedLevel].intPayloadAt(selectedPosition);
        for (int down = selectedLevel + 1; down < height; down++) {
          final ProjectionSortedLeaf child = readNode(childId);
          nodes[down] = child;
          final int position = child.firstNonPrefixRowAfter(0, prefix, length) - 1;
          positions[down] = position;
          childId = child.intPayloadAt(position);
        }
        loadLeaf(childId);
        return true;
      }

      private void loadLeaf(final int id) {
        if (id < 1 || id > maxLeafId) {
          throw new IllegalStateException("sorted projection directory names an absent data leaf");
        }
        final ProjectionSortedLeaf next = ProjectionSortedLeafStore.read(reader, indexNumber, id);
        if (next == null) {
          throw new IllegalStateException("missing sorted projection data leaf " + id);
        }
        leaf = next;
        leafId = id;
        row = 0;
      }

      private boolean moveToNextLeaf() {
        for (int level = height - 1; level >= 0; level--) {
          final ProjectionSortedLeaf node = nodes[level];
          final int nextPosition = positions[level] + 1;
          if (nextPosition == node.rowCount()) {
            continue;
          }
          positions[level] = nextPosition;
          int childId = node.intPayloadAt(nextPosition);
          for (int down = level + 1; down < height; down++) {
            final ProjectionSortedLeaf child = readNode(childId);
            nodes[down] = child;
            positions[down] = 0;
            childId = child.intPayloadAt(0);
          }
          loadLeaf(childId);
          return true;
        }
        leaf = null;
        leafId = 0;
        row = 0;
        return false;
      }

      private void requirePosition() {
        if (leaf == null) {
          throw new IllegalStateException("sorted projection cursor is exhausted");
        }
      }
    }

    private ProjectionSortedLeaf readNode(final int nodeId) {
      if (nodeId < 1 || nodeId > nodeCount) {
        throw new IllegalStateException("sorted projection directory node id is outside its header range");
      }
      final byte[] bytes = ProjectionIndexHOTStorage.readBlob(reader, indexNumber, HEADER_SLOT + nodeId);
      if (bytes == null) {
        throw new IllegalStateException("missing sorted projection directory node " + nodeId);
      }
      return ProjectionSortedLeaf.open(bytes);
    }
  }

  /**
   * Transaction-confined, path-local maintenance of data leaves and their sparse fences. One
   * {@link #apply} pass groups its sorted edits by target leaf: every touched leaf is rewritten once,
   * its group summary re-encoded once, and each touched bounds chunk copied and published once.
   */
  static final class Editor {
    private final ProjectionIndexHOTStorage storage;
    private final ProjectionSortKeyCodec.Layout layout;
    private final int[] pathIds = new int[MAX_HEIGHT];
    private final int[] pathPositions = new int[MAX_HEIGHT];
    private final ProjectionSortedLeaf[] pathNodes = new ProjectionSortedLeaf[MAX_HEIGHT];
    private int height;
    private int rootId;
    private int nodeHighWater;
    private int activeLeafCount;
    private int leafHighWater;
    private long unencodableRows;
    /** {@link #MISSING_AGGREGATE_ROWS_UNKNOWN} for an uncounted view and for one aggregating nothing. */
    private long missingAggregateRows;

    Editor(final ProjectionIndexHOTStorage storage) {
      this.storage = Objects.requireNonNull(storage, "storage");
      final byte[] bytes = storage.getBlob(HEADER_SLOT);
      if (bytes == null) {
        throw new IllegalStateException("sorted projection directory is absent");
      }
      final Header header = Header.parse(bytes);
      height = header.height();
      rootId = header.rootId();
      nodeHighWater = header.nodeCount();
      activeLeafCount = header.dataLeafCount();
      leafHighWater = header.maxLeafId();
      unencodableRows = header.unencodableRows();
      missingAggregateRows = header.missingAggregateRows();
      layout = header.layout();
    }

    ProjectionSortKeyCodec.Layout layout() {
      return layout;
    }

    /**
     * Rows whose aggregated last key field has no value, or {@link #MISSING_AGGREGATE_ROWS_UNKNOWN}
     * while this view has never been counted.
     */
    long missingAggregateRows() {
      return missingAggregateRows;
    }

    /**
     * Publish an exact count recomputed from every live leaf, upgrading a view written before the
     * count existed. The caller must have visited every leaf of the current directory; a count that
     * disagrees with the data only costs or spares an accelerated route, never changes a result.
     */
    void publishMissingAggregateRows(final long rows) {
      if (rows < 0) {
        throw new IllegalArgumentException("missing-aggregate row count must be nonnegative: " + rows);
      }
      if (rows == missingAggregateRows) {
        return;
      }
      missingAggregateRows = rows;
      storage.putBlob(HEADER_SLOT, currentHeader().serialize());
    }

    /**
     * Clear every {@code keys[positions[i]]}, {@code i < count}, that the transaction's view does not
     * hold exactly. The keys are visited in key order, so each data leaf they fall into is located and
     * read once however many of them it holds. Reorders {@code positions[0, count)}.
     */
    void dropAbsent(final byte[][] keys, final int[] positions, final int count) {
      Objects.requireNonNull(keys, "keys");
      Objects.checkFromIndexSize(0, count, positions.length);
      if (rootId == 0) {
        for (int i = 0; i < count; i++) {
          keys[positions[i]] = null;
        }
        return;
      }
      IntArrays.quickSort(positions, 0, count, (left, right) -> Arrays.compareUnsigned(keys[left], keys[right]));
      ProjectionSortedLeaf leaf = null;
      byte[] fence = null;
      for (int i = 0; i < count; i++) {
        final byte[] key = Objects.requireNonNull(keys[positions[i]], "key");
        if (leaf == null || !below(key, fence)) {
          leaf = readLeaf(locate(key));
          fence = upperFence();
        }
        final int row = leaf.lowerBound(key);
        if (row == leaf.rowCount() || leaf.compareRowKey(row, key) != 0) {
          keys[positions[i]] = null;
        }
      }
    }

    void insert(final byte[] key, final byte[] payload) {
      Objects.requireNonNull(key, "key");
      Objects.requireNonNull(payload, "payload");
      apply(NO_KEYS, 0, new byte[][] {key}, new byte[][] {payload}, 1);
    }

    void remove(final byte[] key) {
      Objects.requireNonNull(key, "key");
      apply(new byte[][] {key}, 1, NO_KEYS, null, 0);
    }

    /**
     * Remove and insert exact rows in one pass. Both key runs must be strictly ascending; a key may not
     * be both removed and inserted. {@code insertionPayloads == null} inserts empty payloads.
     */
    void apply(final byte[][] removals, final int removalCount, final byte[][] insertions,
        final byte @Nullable [][] insertionPayloads, final int insertionCount) {
      requireAscending(removals, removalCount);
      requireAscending(insertions, insertionCount);
      if (insertionPayloads != null && insertionPayloads.length < insertionCount) {
        throw new IllegalArgumentException("insertion payloads must cover every insertion");
      }
      if (removalCount == 0 && insertionCount == 0) {
        return;
      }
      final Header before = currentHeader();
      final ProjectionSortedLeafBounds.Updater bounds = new ProjectionSortedLeafBounds.Updater(storage);
      // An uncounted view stays uncounted: one pass sees only its own edits, never the whole view.
      final boolean counted = missingAggregateRows != MISSING_AGGREGATE_ROWS_UNKNOWN;
      long missingAggregateDelta = 0;
      for (int i = 0; i < removalCount; i++) {
        if (ProjectionSortKeyCodec.isUnencodable(removals[i], removals[i].length)) {
          unencodableRows--;
        } else if (counted && layout.lastFieldMissing(removals[i], removals[i].length)) {
          missingAggregateDelta--;
        }
      }
      for (int i = 0; i < insertionCount; i++) {
        if (ProjectionSortKeyCodec.isUnencodable(insertions[i], insertions[i].length)) {
          unencodableRows++;
        } else if (counted && layout.lastFieldMissing(insertions[i], insertions[i].length)) {
          missingAggregateDelta++;
        }
      }
      if (unencodableRows < 0) {
        throw new IllegalStateException("sorted projection removes more unencodable rows than it holds");
      }
      if (counted) {
        final long updated = missingAggregateRows + missingAggregateDelta;
        if (updated < 0) {
          throw new IllegalStateException(
              "sorted projection removes more rows without an aggregate value than it holds");
        }
        missingAggregateRows = updated;
      }
      int removal = 0;
      int insertion = 0;
      while (removal < removalCount || insertion < insertionCount) {
        if (rootId == 0) {
          if (removal < removalCount) {
            throw new IllegalStateException("sorted projection directory is empty");
          }
          bootstrap(insertions, insertionPayloads, insertion, insertionCount - insertion, bounds);
          break;
        }
        final byte[] next = removal < removalCount
            && (insertion == insertionCount || Arrays.compareUnsigned(removals[removal], insertions[insertion]) < 0)
                ? removals[removal]
                : insertions[insertion];
        final int leafId = locate(next);
        final byte[] fence = upperFence();
        int removalEnd = removal;
        while (removalEnd < removalCount && below(removals[removalEnd], fence)) {
          removalEnd++;
        }
        int insertionEnd = insertion;
        while (insertionEnd < insertionCount && below(insertions[insertionEnd], fence)) {
          insertionEnd++;
        }
        rewriteLeaf(leafId, removals, removal, removalEnd, insertions, insertionPayloads, insertion, insertionEnd,
            bounds);
        removal = removalEnd;
        insertion = insertionEnd;
      }
      bounds.flush();
      if (!before.equals(currentHeader())) {
        storage.putBlob(HEADER_SLOT, currentHeader().serialize());
      }
    }

    /** Apply one leaf's edits in a single rewrite, then replace its directory entry with the result. */
    private void rewriteLeaf(final int leafId, final byte[][] removals, final int removalFrom, final int removalTo,
        final byte[][] insertions, final byte @Nullable [][] insertionPayloads, final int insertionFrom,
        final int insertionTo, final ProjectionSortedLeafBounds.Updater bounds) {
      final ProjectionSortedLeaf oldLeaf = readLeaf(leafId);
      final int removed = removalTo - removalFrom;
      final int inserted = insertionTo - insertionFrom;
      ProjectionSortedLeaf single = null;
      if (removed == 1 && inserted == 0 && oldLeaf.rowCount() > 1) {
        single = oldLeaf.withRemoved(removals[removalFrom]);
      } else if (removed == 0 && inserted == 1) {
        single = oldLeaf.withInserted(insertions[insertionFrom], payloadAt(insertionPayloads, insertionFrom));
      }
      if (single != null) {
        ProjectionSortedLeafStore.write(storage, leafId, single, layout, bounds);
        if (oldLeaf.compareRowKey(0, single.copyKey(0)) != 0) {
          replaceChild(new byte[][] {single.copyKey(0)}, new int[] {leafId}, 1);
        }
        return;
      }
      final int oldRows = oldLeaf.rowCount();
      final byte[][] keys = new byte[oldRows + inserted][];
      final byte[][] payloads = new byte[oldRows + inserted][];
      int count = 0;
      int row = 0;
      int removal = removalFrom;
      int insertion = insertionFrom;
      while (row < oldRows || insertion < insertionTo) {
        final int comparison = row == oldRows
            ? 1
            : insertion == insertionTo
                ? -1
                : oldLeaf.compareRowKey(row, insertions[insertion]);
        if (comparison == 0) {
          throw new IllegalArgumentException("sorted projection leaf already contains the key");
        }
        if (comparison < 0) {
          if (removal < removalTo && oldLeaf.compareRowKey(row, removals[removal]) == 0) {
            removal++;
          } else {
            keys[count] = oldLeaf.copyKey(row);
            payloads[count++] = oldLeaf.copyPayload(row);
          }
          row++;
        } else {
          keys[count] = insertions[insertion];
          payloads[count++] = payloadAt(insertionPayloads, insertion);
          insertion++;
        }
      }
      if (removal != removalTo) {
        throw new IllegalStateException("sorted projection leaf does not contain the key");
      }
      if (count == 0) {
        ProjectionSortedLeafStore.remove(storage, leafId, bounds);
        activeLeafCount--;
        replaceChild(NO_KEYS, new int[0], 0);
        return;
      }
      final ProjectionSortedLeaf[] leaves = pack(keys, payloads, count);
      final byte[][] firstKeys = new byte[leaves.length][];
      final int[] ids = new int[leaves.length];
      for (int i = 0; i < leaves.length; i++) {
        ids[i] = i == 0
            ? leafId
            : allocateLeafId();
        ProjectionSortedLeafStore.write(storage, ids[i], leaves[i], layout, bounds);
        firstKeys[i] = leaves[i].copyKey(0);
      }
      activeLeafCount += leaves.length - 1;
      if (leaves.length > 1 || oldLeaf.compareRowKey(0, firstKeys[0]) != 0) {
        replaceChild(firstKeys, ids, leaves.length);
      }
    }

    /** Build a directory for an empty view from the remaining sorted insertions. */
    private void bootstrap(final byte[][] insertions, final byte @Nullable [][] insertionPayloads, final int from,
        final int count, final ProjectionSortedLeafBounds.Updater bounds) {
      final byte[][] keys = Arrays.copyOfRange(insertions, from, from + count);
      final byte[][] payloads = new byte[count][];
      for (int i = 0; i < count; i++) {
        payloads[i] = payloadAt(insertionPayloads, from + i);
      }
      final ProjectionSortedLeaf[] leaves = pack(keys, payloads, count);
      byte[][] entryKeys = new byte[leaves.length][];
      int[] entryIds = new int[leaves.length];
      for (int i = 0; i < leaves.length; i++) {
        entryIds[i] = allocateLeafId();
        ProjectionSortedLeafStore.write(storage, entryIds[i], leaves[i], layout, bounds);
        entryKeys[i] = leaves[i].copyKey(0);
      }
      activeLeafCount = leaves.length;
      height = 0;
      int entries = leaves.length;
      do {
        final byte[][] payloadsOfIds = idPayloads(entryIds, entries);
        final ProjectionSortedLeaf[] nodes = pack(entryKeys, payloadsOfIds, entries);
        final byte[][] parentKeys = new byte[nodes.length][];
        final int[] parentIds = new int[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
          parentIds[i] = allocateNodeId();
          writeNode(parentIds[i], nodes[i]);
          parentKeys[i] = nodes[i].copyKey(0);
        }
        entryKeys = parentKeys;
        entryIds = parentIds;
        entries = nodes.length;
        growHeight();
      } while (entries > 1);
      rootId = entryIds[0];
    }

    /**
     * Replace the located child entry at the lowest directory level by {@code count} entries (none
     * removes it), splitting or removing nodes upward as needed.
     */
    private void replaceChild(byte[][] keys, int[] ids, int count) {
      for (int level = height - 1; level >= 0; level--) {
        final int nodeId = pathIds[level];
        final ProjectionSortedLeaf node = pathNodes[level];
        final int position = pathPositions[level];
        final byte[] oldFirst = node.copyKey(0);
        if (count == 1 && node.intPayloadAt(position) == ids[0]) {
          if (node.compareRowKey(position, keys[0]) == 0) {
            return;
          }
          final ProjectionSortedLeaf rewritten = node.withReplaced(position, keys[0], intPayload(ids[0]));
          if (rewritten != null) {
            writeNode(nodeId, rewritten);
            if (position != 0) {
              return;
            }
            keys = new byte[][] {rewritten.copyKey(0)};
            ids = new int[] {nodeId};
            continue;
          }
        }
        final int total = node.rowCount() - 1 + count;
        if (total == 0) {
          storage.tombstoneBlob(HEADER_SLOT + nodeId);
          keys = NO_KEYS;
          ids = new int[0];
          count = 0;
          continue;
        }
        final byte[][] entryKeys = new byte[total][];
        final byte[][] entryPayloads = new byte[total][];
        int at = 0;
        for (int i = 0; i < node.rowCount(); i++) {
          if (i == position) {
            for (int j = 0; j < count; j++) {
              entryKeys[at] = keys[j];
              entryPayloads[at++] = intPayload(ids[j]);
            }
          } else {
            entryKeys[at] = node.copyKey(i);
            entryPayloads[at++] = intPayload(node.intPayloadAt(i));
          }
        }
        final ProjectionSortedLeaf[] nodes = pack(entryKeys, entryPayloads, total);
        keys = new byte[nodes.length][];
        ids = new int[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
          ids[i] = i == 0
              ? nodeId
              : allocateNodeId();
          writeNode(ids[i], nodes[i]);
          keys[i] = nodes[i].copyKey(0);
        }
        count = nodes.length;
        if (count == 1 && Arrays.equals(oldFirst, keys[0])) {
          return;
        }
      }
      if (count == 0) {
        rootId = 0;
        height = 0;
        return;
      }
      while (count > 1) {
        final ProjectionSortedLeaf[] nodes = pack(keys, idPayloads(ids, count), count);
        final byte[][] parentKeys = new byte[nodes.length][];
        final int[] parentIds = new int[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
          parentIds[i] = allocateNodeId();
          writeNode(parentIds[i], nodes[i]);
          parentKeys[i] = nodes[i].copyKey(0);
        }
        keys = parentKeys;
        ids = parentIds;
        count = nodes.length;
        growHeight();
      }
      rootId = ids[0];
    }

    private int locate(final byte[] key) {
      int id = rootId;
      for (int level = 0; level < height; level++) {
        final ProjectionSortedLeaf node = readNode(id);
        pathIds[level] = id;
        pathNodes[level] = node;
        final int position = atOrBelow(node, key);
        pathPositions[level] = position;
        id = node.intPayloadAt(position);
      }
      if (id < 1 || id > leafHighWater) {
        throw new IllegalStateException("sorted projection directory names an absent data leaf");
      }
      return id;
    }

    /** Smallest directory key above the located leaf's range, or null when it is the last leaf. */
    private byte @Nullable [] upperFence() {
      for (int level = height - 1; level >= 0; level--) {
        final ProjectionSortedLeaf node = pathNodes[level];
        final int next = pathPositions[level] + 1;
        if (next < node.rowCount()) {
          return node.copyKey(next);
        }
      }
      return null;
    }

    private Header currentHeader() {
      return new Header(height, rootId, nodeHighWater, activeLeafCount, leafHighWater, unencodableRows,
          missingAggregateRows, layout);
    }

    private void growHeight() {
      if (height == MAX_HEIGHT) {
        throw new IllegalStateException("sorted projection directory exceeds eight levels");
      }
      height++;
    }

    private ProjectionSortedLeaf readNode(final int id) {
      if (id < 1 || id > nodeHighWater) {
        throw new IllegalStateException("sorted projection directory node id is outside its header range");
      }
      final byte[] bytes = storage.getBlob(HEADER_SLOT + id);
      if (bytes == null) {
        throw new IllegalStateException("missing sorted projection directory node " + id);
      }
      return ProjectionSortedLeaf.open(bytes);
    }

    private ProjectionSortedLeaf readLeaf(final int id) {
      final ProjectionSortedLeaf leaf = ProjectionSortedLeafStore.read(storage, id);
      if (leaf == null) {
        throw new IllegalStateException("missing sorted projection data leaf " + id);
      }
      return leaf;
    }

    private int allocateLeafId() {
      if (leafHighWater == Integer.MAX_VALUE) {
        throw new IllegalStateException("sorted projection exhausted data leaf ids");
      }
      return ++leafHighWater;
    }

    private int allocateNodeId() {
      if (nodeHighWater == Integer.MAX_VALUE) {
        throw new IllegalStateException("sorted projection exhausted directory node ids");
      }
      return ++nodeHighWater;
    }

    private void writeNode(final int id, final ProjectionSortedLeaf node) {
      storage.putBlob(HEADER_SLOT + id, node.encodedBytes());
    }

    private static boolean below(final byte[] key, final byte @Nullable [] fence) {
      return fence == null || Arrays.compareUnsigned(key, fence) < 0;
    }

    private static byte[] payloadAt(final byte @Nullable [][] payloads, final int index) {
      return payloads == null
          ? EMPTY_PAYLOAD
          : Objects.requireNonNull(payloads[index], "payload");
    }

    private static void requireAscending(final byte[][] keys, final int count) {
      Objects.checkFromIndexSize(0, count, keys.length);
      for (int i = 0; i < count; i++) {
        Objects.requireNonNull(keys[i], "key");
        if (i > 0 && Arrays.compareUnsigned(keys[i - 1], keys[i]) >= 0) {
          throw new IllegalArgumentException("sorted projection edits must be strictly ascending");
        }
      }
    }
  }

  private static byte[] intPayload(final int value) {
    final byte[] payload = new byte[Integer.BYTES];
    putInt(payload, 0, value);
    return payload;
  }

  private static byte[][] idPayloads(final int[] ids, final int count) {
    final byte[][] payloads = new byte[count][];
    for (int i = 0; i < count; i++) {
      payloads[i] = intPayload(ids[i]);
    }
    return payloads;
  }

  /**
   * Encode {@code keys[0, count)} into the fewest balanced bounded pages that fit. Materialization is
   * reserved for a multi-edit rewrite or a split; single edits copy within one bounded page.
   */
  private static ProjectionSortedLeaf[] pack(final byte[][] keys, final byte[][] payloads, final int count) {
    for (int pieces = Math.max(1,
        (count + ProjectionSortedLeaf.MAX_ROWS - 1) / ProjectionSortedLeaf.MAX_ROWS); pieces <= count; pieces++) {
      final ProjectionSortedLeaf[] pages = new ProjectionSortedLeaf[pieces];
      final int base = count / pieces;
      final int larger = count % pieces;
      int from = 0;
      boolean fits = true;
      for (int piece = 0; piece < pieces && fits; piece++) {
        final int size = base + (piece >= pieces - larger
            ? 1
            : 0);
        pages[piece] = ProjectionSortedLeaf.encode(keys, payloads, from, size);
        fits = pages[piece] != null;
        from += size;
      }
      if (fits) {
        return pages;
      }
    }
    throw new IllegalArgumentException("sorted projection row exceeds a bounded page");
  }

  /** Initial-build writer; row keys must arrive in strict global sort order. */
  static final class Builder {
    private final ProjectionIndexHOTStorage storage;
    private final ProjectionSortKeyCodec.Layout layout;
    /** Only a view that aggregates its last key field ever consults the missing-aggregate count. */
    private final boolean countsMissingAggregates;
    private final ProjectionSortedLeafBounds.Builder bounds;
    private byte[][] keys = new byte[256][];
    private int[] ids = new int[256];
    private int count;
    private long unencodableRows;
    private long missingAggregateRows;
    private byte @Nullable [] lastKey;
    private boolean finished;

    Builder(final ProjectionIndexHOTStorage storage, final ProjectionSortKeyCodec.Layout layout) {
      this.storage = Objects.requireNonNull(storage, "storage");
      this.layout = Objects.requireNonNull(layout, "layout");
      this.countsMissingAggregates = layout.groupsByLastLong();
      if (storage.getBlob(HEADER_SLOT) != null) {
        throw new IllegalStateException("sorted projection directory already exists");
      }
      bounds = new ProjectionSortedLeafBounds.Builder(storage);
    }

    int append(final ProjectionSortedLeaf leaf) {
      Objects.requireNonNull(leaf, "leaf");
      if (finished) {
        throw new IllegalStateException("sorted projection directory is finished");
      }
      final byte[] first = leaf.copyKey(0);
      if (lastKey != null && Arrays.compareUnsigned(lastKey, first) >= 0) {
        throw new IllegalArgumentException("sorted projection data leaves overlap or arrive out of order");
      }
      if (count == Integer.MAX_VALUE) {
        throw new IllegalStateException("sorted projection exhausted data leaf ids");
      }
      if (count == keys.length) {
        final int capacity = keys.length <= Integer.MAX_VALUE / 2
            ? keys.length << 1
            : Integer.MAX_VALUE;
        keys = Arrays.copyOf(keys, capacity);
        ids = Arrays.copyOf(ids, capacity);
      }
      final int leafId = count + 1;
      missingAggregateRows += ProjectionSortedLeafStore.write(storage, leafId, leaf, layout, bounds);
      keys[count] = first;
      ids[count] = leafId;
      count++;
      final int lastRow = leaf.rowCount() - 1;
      lastKey = leaf.copyKey(lastRow);
      // Unencodable rows sort after every ordinary key, so only a trailing run of a leaf holds them.
      for (int row = lastRow; row >= 0 && leaf.keyHasPrefix(row, UNENCODABLE_PREFIX, 1); row--) {
        unencodableRows++;
      }
      return leafId;
    }

    void finish() {
      if (finished) {
        throw new IllegalStateException("sorted projection directory is already finished");
      }
      int height = 0;
      int nextNodeId = 1;
      int levelCount = count;
      byte[][] levelKeys = keys;
      int[] levelIds = ids;
      while (levelCount > 0 && (levelCount > 1 || height == 0)) {
        final byte[][] parentKeys = new byte[levelCount][];
        final int[] parentIds = new int[levelCount];
        int parentCount = 0;
        for (int from = 0; from < levelCount;) {
          int entries = Math.min(ProjectionSortedLeaf.MAX_ROWS, levelCount - from);
          ProjectionSortedLeaf page;
          do {
            final byte[][] pageKeys = Arrays.copyOfRange(levelKeys, from, from + entries);
            final byte[][] payloads = new byte[entries][];
            for (int i = 0; i < entries; i++) {
              payloads[i] = intPayload(levelIds[from + i]);
            }
            page = ProjectionSortedLeaf.encode(pageKeys, payloads, entries);
            if (page == null) {
              entries >>>= 1;
            }
          } while (page == null && entries > 0);
          if (page == null || nextNodeId == Integer.MAX_VALUE) {
            throw new IllegalStateException("sorted projection directory entry exceeds a bounded node");
          }
          storage.putBlob(HEADER_SLOT + nextNodeId, page.encodedBytes());
          parentKeys[parentCount] = levelKeys[from];
          parentIds[parentCount] = nextNodeId++;
          parentCount++;
          from += entries;
        }
        levelKeys = parentKeys;
        levelIds = parentIds;
        levelCount = parentCount;
        height++;
        if (height > MAX_HEIGHT) {
          throw new IllegalStateException("sorted projection directory exceeds eight levels");
        }
      }
      final Header header = new Header(height, count == 0
          ? 0
          : levelIds[0], nextNodeId - 1, count, count, unencodableRows, countsMissingAggregates
              ? missingAggregateRows
              : MISSING_AGGREGATE_ROWS_UNKNOWN, layout);
      bounds.finish();
      storage.putBlob(HEADER_SLOT, header.serialize());
      finished = true;
    }
  }

  private static int getInt(final byte[] bytes, final int at) {
    return (bytes[at] & 0xFF) | (bytes[at + 1] & 0xFF) << 8 | (bytes[at + 2] & 0xFF) << 16
        | (bytes[at + 3] & 0xFF) << 24;
  }

  private static long getLong(final byte[] bytes, final int at) {
    return (getInt(bytes, at) & 0xFFFF_FFFFL) | (long) getInt(bytes, at + Integer.BYTES) << 32;
  }

  private static void putInt(final byte[] bytes, final int at, final int value) {
    bytes[at] = (byte) value;
    bytes[at + 1] = (byte) (value >>> 8);
    bytes[at + 2] = (byte) (value >>> 16);
    bytes[at + 3] = (byte) (value >>> 24);
  }

  private static void putLong(final byte[] bytes, final int at, final long value) {
    putInt(bytes, at, (int) value);
    putInt(bytes, at + Integer.BYTES, (int) (value >>> 32));
  }
}
