/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
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
 * visible only when its small root header is published at the end of the owning transaction.
 * </p>
 */
final class ProjectionSortedDirectory {

  /** Blob side-page owner keys must remain below 2^47. */
  static final long HEADER_SLOT = ProjectionSortedLeafStore.LEAF_SLOT_BASE + (1L << 32);
  private static final int MAGIC = 0x31445350; // PSD1
  private static final byte VERSION = 2;
  private static final int HEADER_BYTES = 22;

  private ProjectionSortedDirectory() {}

  static @Nullable Accessor open(final StorageEngineReader reader, final int indexNumber) {
    Objects.requireNonNull(reader, "reader");
    final byte[] header = ProjectionIndexHOTStorage.readBlob(reader, indexNumber, HEADER_SLOT);
    return header == null
        ? null
        : new Accessor(reader, indexNumber, header);
  }

  static final class Accessor {
    private final StorageEngineReader reader;
    private final int indexNumber;
    private final int height;
    private final int nodeCount;
    private final int dataLeafCount;
    private final int maxLeafId;
    private final @Nullable ProjectionSortedLeaf root;

    private Accessor(final StorageEngineReader reader, final int indexNumber, final byte[] header) {
      if (header.length != HEADER_BYTES || getInt(header, 0) != MAGIC || header[4] != VERSION) {
        throw new IllegalStateException("invalid sorted projection directory header");
      }
      this.reader = reader;
      this.indexNumber = indexNumber;
      this.height = header[5] & 0xFF;
      final int rootId = getInt(header, 6);
      this.nodeCount = getInt(header, 10);
      this.dataLeafCount = getInt(header, 14);
      this.maxLeafId = getInt(header, 18);
      if (height > 8 || nodeCount < 0 || dataLeafCount < 0 || (rootId == 0) != (dataLeafCount == 0)
          || (dataLeafCount == 0) != (height == 0) || rootId > nodeCount || maxLeafId < dataLeafCount) {
        throw new IllegalStateException("invalid sorted projection directory dimensions");
      }
      this.root = rootId == 0
          ? null
          : readNode(rootId);
    }

    int dataLeafCount() {
      return dataLeafCount;
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
        final int lower = node.lowerBound(key);
        final int index = lower == node.rowCount()
            ? lower - 1
            : lower == 0 || node.compareRowKey(lower, key) == 0
                ? lower
                : lower - 1;
        final int childId = node.intPayloadAt(index);
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
        final int lower = node.lowerBound(key);
        final int position = lower == node.rowCount()
            ? lower - 1
            : lower == 0 || node.compareRowKey(lower, key) == 0
                ? lower
                : lower - 1;
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
      return seek(new byte[0]);
    }

    /** Walk live data-leaf IDs in key order, without reading the data leaves themselves. */
    LeafCursor leaves() {
      return new LeafCursor();
    }

    final class LeafCursor {
      private final ProjectionSortedLeaf[] nodes = new ProjectionSortedLeaf[height];
      private final int[] positions = new int[height];
      private int id;
      private int visited;

      private LeafCursor() {
        if (root != null) {
          nodes[0] = root;
          descend(0, root.intPayloadAt(0));
          visited = 1;
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
            return true;
          }
        }
        if (visited != dataLeafCount) {
          throw new IllegalStateException("sorted directory contains fewer leaves than declared");
        }
        id = 0;
        return false;
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

  /** Transaction-confined, path-local maintenance of data leaves and their sparse fences. */
  static final class Editor {
    private final ProjectionIndexHOTStorage storage;
    private final int[] pathIds = new int[8];
    private final int[] pathPositions = new int[8];
    private int height;
    private int rootId;
    private int nodeHighWater;
    private int activeLeafCount;
    private int leafHighWater;

    private boolean childRemoved;
    private byte @Nullable [] childFirst;
    private int splitChildId;
    private byte @Nullable [] splitChildFirst;

    Editor(final ProjectionIndexHOTStorage storage) {
      this.storage = Objects.requireNonNull(storage, "storage");
      final byte[] header = storage.getBlob(HEADER_SLOT);
      if (header == null || header.length != HEADER_BYTES || getInt(header, 0) != MAGIC || header[4] != VERSION) {
        throw new IllegalStateException("sorted projection directory is absent or invalid");
      }
      height = header[5] & 0xFF;
      rootId = getInt(header, 6);
      nodeHighWater = getInt(header, 10);
      activeLeafCount = getInt(header, 14);
      leafHighWater = getInt(header, 18);
      if (height > pathIds.length || nodeHighWater < 0 || activeLeafCount < 0 || leafHighWater < activeLeafCount
          || (rootId == 0) != (activeLeafCount == 0) || (rootId == 0) != (height == 0) || rootId > nodeHighWater) {
        throw new IllegalStateException("invalid sorted projection directory dimensions");
      }
    }

    void insert(final byte[] key, final byte[] payload) {
      Objects.requireNonNull(key, "key");
      Objects.requireNonNull(payload, "payload");
      if (rootId == 0) {
        final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(new byte[][] {key}, new byte[][] {payload}, 1);
        if (leaf == null) {
          throw new IllegalArgumentException("sorted projection row exceeds a bounded leaf");
        }
        final int leafId = allocateLeafId();
        ProjectionSortedLeafStore.write(storage, leafId, leaf);
        final int nodeId = allocateNodeId();
        writeNode(nodeId, oneEntry(key, leafId));
        rootId = nodeId;
        height = 1;
        activeLeafCount = 1;
        publishHeader();
        return;
      }
      final int leafId = locate(key);
      final ProjectionSortedLeaf oldLeaf = readLeaf(leafId);
      final ProjectionSortedLeaf updated = oldLeaf.withInserted(key, payload);
      childRemoved = false;
      splitChildId = 0;
      splitChildFirst = null;
      if (updated != null) {
        ProjectionSortedLeafStore.write(storage, leafId, updated);
        childFirst = updated.copyKey(0);
        if (!Arrays.equals(oldLeaf.copyKey(0), childFirst)) {
          propagate();
        }
        return;
      }
      final int count = oldLeaf.rowCount() + 1;
      final byte[][] keys = new byte[count][];
      final byte[][] payloads = new byte[count][];
      final int insertedAt = oldLeaf.lowerBound(key);
      if (insertedAt < oldLeaf.rowCount() && oldLeaf.compareRowKey(insertedAt, key) == 0) {
        throw new IllegalArgumentException("sorted projection leaf already contains the key");
      }
      for (int i = 0, source = 0; i < count; i++) {
        if (i == insertedAt) {
          keys[i] = key;
          payloads[i] = payload;
        } else {
          keys[i] = oldLeaf.copyKey(source);
          payloads[i] = oldLeaf.copyPayload(source++);
        }
      }
      final ProjectionSortedLeaf[] halves = splitRows(keys, payloads, count);
      ProjectionSortedLeafStore.write(storage, leafId, halves[0]);
      splitChildId = allocateLeafId();
      ProjectionSortedLeafStore.write(storage, splitChildId, halves[1]);
      activeLeafCount++;
      childFirst = halves[0].copyKey(0);
      splitChildFirst = halves[1].copyKey(0);
      propagate();
      publishHeader();
    }

    void remove(final byte[] key) {
      Objects.requireNonNull(key, "key");
      if (rootId == 0) {
        throw new IllegalStateException("sorted projection directory is empty");
      }
      final int leafId = locate(key);
      final ProjectionSortedLeaf oldLeaf = readLeaf(leafId);
      final ProjectionSortedLeaf updated = oldLeaf.withRemoved(key);
      splitChildId = 0;
      splitChildFirst = null;
      if (updated != null) {
        ProjectionSortedLeafStore.write(storage, leafId, updated);
        childRemoved = false;
        childFirst = updated.copyKey(0);
        if (!Arrays.equals(oldLeaf.copyKey(0), childFirst)) {
          propagate();
        }
        return;
      }
      ProjectionSortedLeafStore.remove(storage, leafId);
      activeLeafCount--;
      childRemoved = true;
      childFirst = null;
      propagate();
      publishHeader();
    }

    private int locate(final byte[] key) {
      int id = rootId;
      for (int level = 0; level < height; level++) {
        final ProjectionSortedLeaf node = readNode(id);
        pathIds[level] = id;
        final int lower = node.lowerBound(key);
        final int position = lower == node.rowCount()
            ? lower - 1
            : lower == 0 || node.compareRowKey(lower, key) == 0
                ? lower
                : lower - 1;
        pathPositions[level] = position;
        id = node.intPayloadAt(position);
      }
      if (id < 1 || id > leafHighWater) {
        throw new IllegalStateException("sorted projection directory names an absent data leaf");
      }
      return id;
    }

    private void propagate() {
      for (int level = height - 1; level >= 0; level--) {
        final int nodeId = pathIds[level];
        final ProjectionSortedLeaf oldNode = readNode(nodeId);
        final int position = pathPositions[level];
        final byte[] oldFirst = oldNode.copyKey(0);
        ProjectionSortedLeaf rewritten;
        if (childRemoved) {
          rewritten = oldNode.withRemoved(oldNode.copyKey(position));
        } else if (splitChildId == 0) {
          final byte[] nextFirst = Objects.requireNonNull(childFirst, "childFirst");
          if (oldNode.compareRowKey(position, nextFirst) == 0) {
            return;
          }
          rewritten = oldNode.withReplaced(position, nextFirst, intPayload(oldNode.intPayloadAt(position)));
        } else {
          rewritten = null;
        }
        if (rewritten != null) {
          writeNode(nodeId, rewritten);
          childRemoved = false;
          splitChildId = 0;
          splitChildFirst = null;
          childFirst = rewritten.copyKey(0);
          if (Arrays.equals(oldFirst, childFirst)) {
            return;
          }
          continue;
        }
        if (childRemoved && oldNode.rowCount() == 1) {
          storage.tombstoneBlob(HEADER_SLOT + nodeId);
          childFirst = null;
          splitChildId = 0;
          splitChildFirst = null;
          continue;
        }
        final int capacity = oldNode.rowCount() + (childRemoved
            ? -1
            : splitChildId == 0
                ? 0
                : 1);
        final byte[][] keys = new byte[capacity][];
        final byte[][] payloads = new byte[capacity][];
        int count = 0;
        for (int i = 0; i < oldNode.rowCount(); i++) {
          if (i == position) {
            if (!childRemoved) {
              keys[count] = Objects.requireNonNull(childFirst, "childFirst");
              payloads[count++] = intPayload(oldNode.intPayloadAt(i));
              if (splitChildId != 0) {
                keys[count] = Objects.requireNonNull(splitChildFirst, "splitChildFirst");
                payloads[count++] = intPayload(splitChildId);
              }
            }
          } else {
            keys[count] = oldNode.copyKey(i);
            payloads[count++] = intPayload(oldNode.intPayloadAt(i));
          }
        }
        rewritten = ProjectionSortedLeaf.encode(keys, payloads, count);
        childRemoved = false;
        if (rewritten != null) {
          writeNode(nodeId, rewritten);
          childFirst = rewritten.copyKey(0);
          splitChildId = 0;
          splitChildFirst = null;
          if (Arrays.equals(oldFirst, childFirst)) {
            return;
          }
          continue;
        }
        final ProjectionSortedLeaf[] halves = splitRows(keys, payloads, count);
        writeNode(nodeId, halves[0]);
        splitChildId = allocateNodeId();
        writeNode(splitChildId, halves[1]);
        childFirst = halves[0].copyKey(0);
        splitChildFirst = halves[1].copyKey(0);
      }
      if (childRemoved) {
        rootId = 0;
        height = 0;
      } else if (splitChildId != 0) {
        if (height == pathIds.length) {
          throw new IllegalStateException("sorted projection directory exceeds eight levels");
        }
        final int newRootId = allocateNodeId();
        writeNode(newRootId,
            ProjectionSortedLeaf.encode(
                new byte[][] {Objects.requireNonNull(childFirst, "childFirst"),
                    Objects.requireNonNull(splitChildFirst, "splitChildFirst")},
                new byte[][] {intPayload(rootId), intPayload(splitChildId)}, 2));
        rootId = newRootId;
        height++;
      }
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
      if (node == null) {
        throw new IllegalStateException("sorted projection directory entry exceeds a bounded node");
      }
      storage.putBlob(HEADER_SLOT + id, node.encodedBytes());
    }

    private void publishHeader() {
      final byte[] header = new byte[HEADER_BYTES];
      putInt(header, 0, MAGIC);
      header[4] = VERSION;
      header[5] = (byte) height;
      putInt(header, 6, rootId);
      putInt(header, 10, nodeHighWater);
      putInt(header, 14, activeLeafCount);
      putInt(header, 18, leafHighWater);
      storage.putBlob(HEADER_SLOT, header);
    }
  }

  private static ProjectionSortedLeaf oneEntry(final byte[] key, final int id) {
    return ProjectionSortedLeaf.encode(new byte[][] {key}, new byte[][] {intPayload(id)}, 1);
  }

  private static byte[] intPayload(final int value) {
    final byte[] payload = new byte[Integer.BYTES];
    putInt(payload, 0, value);
    return payload;
  }

  /** Materialization is reserved for a page split; ordinary changes copy within one bounded page. */
  private static ProjectionSortedLeaf[] splitRows(final byte[][] keys, final byte[][] payloads, final int count) {
    final int middle = count >>> 1;
    for (int displacement = 0; displacement <= middle; displacement++) {
      for (int sign = 0; sign < 2; sign++) {
        final int cut = middle + (sign == 0
            ? displacement
            : -displacement);
        if (cut < 1 || cut >= count) {
          continue;
        }
        final ProjectionSortedLeaf left =
            ProjectionSortedLeaf.encode(Arrays.copyOfRange(keys, 0, cut), Arrays.copyOfRange(payloads, 0, cut), cut);
        if (left == null) {
          continue;
        }
        final ProjectionSortedLeaf right = ProjectionSortedLeaf.encode(Arrays.copyOfRange(keys, cut, count),
            Arrays.copyOfRange(payloads, cut, count), count - cut);
        if (right != null) {
          return new ProjectionSortedLeaf[] {left, right};
        }
      }
    }
    throw new IllegalArgumentException("sorted projection row exceeds a bounded page");
  }

  /** Initial-build writer; row keys must arrive in strict global sort order. */
  static final class Builder {
    private final ProjectionIndexHOTStorage storage;
    private final ProjectionSortedLeafBounds.Builder bounds;
    private byte[][] keys = new byte[256][];
    private int[] ids = new int[256];
    private int count;
    private byte @Nullable [] lastKey;
    private boolean finished;

    Builder(final ProjectionIndexHOTStorage storage) {
      this.storage = Objects.requireNonNull(storage, "storage");
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
      ProjectionSortedLeafStore.write(storage, leafId, leaf, bounds);
      keys[count] = first;
      ids[count] = leafId;
      count++;
      lastKey = leaf.copyKey(leaf.rowCount() - 1);
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
              payloads[i] = new byte[Integer.BYTES];
              putInt(payloads[i], 0, levelIds[from + i]);
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
        if (height > 8) {
          throw new IllegalStateException("sorted projection directory exceeds eight levels");
        }
      }
      final byte[] header = new byte[HEADER_BYTES];
      putInt(header, 0, MAGIC);
      header[4] = VERSION;
      header[5] = (byte) height;
      putInt(header, 6, count == 0
          ? 0
          : levelIds[0]);
      putInt(header, 10, nextNodeId - 1);
      putInt(header, 14, count);
      putInt(header, 18, count);
      bounds.finish();
      storage.putBlob(HEADER_SLOT, header);
      finished = true;
    }
  }

  private static int getInt(final byte[] bytes, final int at) {
    return (bytes[at] & 0xFF) | (bytes[at + 1] & 0xFF) << 8 | (bytes[at + 2] & 0xFF) << 16
        | (bytes[at + 3] & 0xFF) << 24;
  }

  private static void putInt(final byte[] bytes, final int at, final int value) {
    bytes[at] = (byte) value;
    bytes[at + 1] = (byte) (value >>> 8);
    bytes[at + 2] = (byte) (value >>> 16);
    bytes[at + 3] = (byte) (value >>> 24);
  }
}
