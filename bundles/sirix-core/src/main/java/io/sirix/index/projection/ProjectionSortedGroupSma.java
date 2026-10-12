/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.hot.reader.HOTReadIntent;
import io.sirix.index.hot.cache.HOTMiniPageCache.ReadScope;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Revisioned upper SMA level: one exact min/max per logical group, above the per-chunk SMAs. Each
 * node is an independent inline blob. Updates touch only affected group paths; interior tuple edits
 * leave this tree byte-identical. Queries traverse groups, never enumerate data chunks. Missing
 * values or unusually wide group keys decline this optional acceleration.
 */
final class ProjectionSortedGroupSma {
  static final long HEADER_SLOT = ProjectionSortedLeafStore.LEAF_SLOT_BASE + (5L << 32);
  private static final int MAGIC = 0x414D5347;
  private static final int LIMIT = ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES;
  private static final int MAX_GROUP_KEY = 128;

  private ProjectionSortedGroupSma() {}

  /** An active edit owns its captured nodes until it republishes or retires the tree. */
  static boolean invalidate(final ProjectionIndexHOTStorage storage, final boolean preserveOwnedNodes) {
    final byte[] header = storage.getBlob(HEADER_SLOT);
    if (header == null)
      return false;
    if (preserveOwnedNodes) {
      storage.tombstoneBlob(HEADER_SLOT);
    } else {
      new Tree(storage, Header.open(header), false).dropAll();
    }
    return true;
  }

  /**
   * The group level is built and kept only while it is coarser than the chunk level: at most one
   * group per data chunk, i.e. the average group holds at least as many rows as the average chunk.
   * With more groups than chunks, every per-chunk summary already lists its groups at the same or a
   * finer granularity, so this tree would be a second copy of the summaries that costs a node edit
   * per inserted row and is carried forward with every revision, for no read gain. Below the bound
   * the tree is dropped and grouped queries take the exact summary and bounds routes.
   */
  static boolean coarserThanChunks(final long groups, final long chunks) {
    return groups <= chunks;
  }

  /** The number of groups the published tree holds, or -1 when the view has no group SMA. */
  static long publishedGroupCount(final StorageEngineReader reader, final int index) {
    final byte[] bytes = ProjectionIndexHOTStorage.readBlob(reader, index, HEADER_SLOT, HOTReadIntent.SELECTIVE);
    return bytes == null
        ? -1
        : Header.open(bytes).groups;
  }

  private record Header(int root, int height, int highWater, long groups) {
    static final int BYTES = 24;

    static Header open(final byte[] bytes) {
      if (bytes.length != BYTES || integer(bytes, 0) != MAGIC)
        throw new IllegalStateException("invalid group SMA header");
      final Header h = new Header(integer(bytes, 4), integer(bytes, 8), integer(bytes, 12), longValue(bytes, 16));
      if (h.root < 0 || h.root > h.highWater || h.highWater < 0 || h.height < 0 || h.height > 32
          || (h.root == 0) != (h.height == 0) || h.groups < 0 || (h.root == 0) != (h.groups == 0))
        throw new IllegalStateException("invalid group SMA dimensions");
      return h;
    }

    byte[] bytes() {
      final byte[] bytes = new byte[BYTES];
      putInt(bytes, 0, MAGIC);
      putInt(bytes, 4, root);
      putInt(bytes, 8, height);
      putInt(bytes, 12, highWater);
      putLong(bytes, 16, groups);
      return bytes;
    }
  }

  static @Nullable List<ProjectionSortedGroupScan.Group> topK(final StorageEngineReader reader, final int index,
      final byte[] prefix, final byte @Nullable [] upper, final int limit, final ProjectionSortedGroupScan.Order order,
      final long divisor, final boolean minOnly, final ReadScope scope) {
    try {
      final byte[] bytes =
          ProjectionIndexHOTStorage.readBlob(reader, index, HEADER_SLOT, HOTReadIntent.SELECTIVE, scope);
      if (bytes == null)
        return null;
      ProjectionSortedGroupSummary.observeRead(0);
      final Header header = Header.open(bytes);
      final Query query = new Query(reader, index, prefix, upper, limit, order, divisor, minOnly, scope);
      if (header.root != 0)
        query.visit(header.root, header.height);
      return ProjectionSortedGroupScan.finish(query.winners, query.minima, query.maxima, query.scores, query.retained,
          limit);
    } finally {
      scope.finish(false);
    }
  }

  private static final class Query {
    private final StorageEngineReader reader;
    private final int index;
    private final byte[] prefix;
    private final byte @Nullable [] upper;
    private final ProjectionSortedGroupScan.Order order;
    private final long divisor;
    private final boolean minOnly;
    private final ReadScope scope;
    private final byte[][] winners;
    private final long[] minima;
    private final long[] maxima;
    private final long[] scores;
    private final byte[] payload = new byte[16];
    private int retained;

    Query(final StorageEngineReader reader, final int index, final byte[] prefix, final byte @Nullable [] upper,
        final int limit, final ProjectionSortedGroupScan.Order order, final long divisor, final boolean minOnly,
        final ReadScope scope) {
      this.reader = reader;
      this.index = index;
      this.prefix = prefix;
      this.upper = upper;
      this.order = order;
      this.divisor = divisor;
      this.minOnly = minOnly;
      this.scope = scope;
      winners = new byte[limit + 1][];
      minima = new long[limit + 1];
      maxima = new long[limit + 1];
      scores = new long[limit + 1];
    }

    void visit(final int id, final int height) {
      ProjectionSortedGroupSummary.observeRead(-id);
      final ProjectionSortedLeaf node =
          node(ProjectionIndexHOTStorage.readBlob(reader, index, HEADER_SLOT + id, HOTReadIntent.SELECTIVE, scope));
      int row = node.lowerBound(prefix);
      if (height > 1 && row > 0 && (row == node.rowCount() || node.compareRowKey(row, prefix) > 0))
        row--;
      for (; row < node.rowCount(); row++) {
        if (upper != null && node.compareRowKey(row, upper) >= 0)
          break;
        if (height > 1) {
          visit(node.intPayloadAt(row), height - 1);
        } else {
          if (!node.keyHasPrefix(row, prefix, prefix.length))
            break;
          if (node.payloadLength(row) != 16)
            throw new IllegalStateException("invalid group SMA payload");
          node.copyPayloadTo(row, payload, 0);
          final long min = longValue(payload, 0);
          final long max = longValue(payload, 8);
          if (min > max)
            throw new IllegalStateException("inverted group SMA");
          final byte[] key = node.copyKey(row);
          retained = ProjectionSortedGroupScan.offer(key, prefix.length, key.length, min, minOnly
              ? min
              : max, order, divisor, winners, minima, maxima, scores, retained);
        }
      }
    }
  }

  /** Streaming initial fold. Only the current group and bounded leaf staging are retained. */
  static final class Builder {
    private final Tree tree;
    private final byte[] payload = new byte[16];
    private byte @Nullable [] group;
    private long min;
    private long max;
    private boolean enabled;

    Builder(final ProjectionIndexHOTStorage storage, final ProjectionSortKeyCodec.Layout layout) {
      tree = new Tree(storage, new Header(0, 0, 0, 0), true);
      enabled = layout.groupsByLastLong();
    }

    void appendSummary(final @Nullable ProjectionSortedLeaf summary) {
      if (!enabled)
        return;
      if (summary == null) {
        enabled = false;
        return;
      }
      for (int row = 0; row < summary.rowCount(); row++) {
        if (summary.keyLength(row) > MAX_GROUP_KEY) {
          enabled = false;
          return;
        }
        summary.copyPayloadTo(row, payload, 0);
        if (group == null || summary.compareRowKey(row, group) != 0) {
          flushGroup();
          group = summary.copyKey(row);
          min = longValue(payload, 0);
        }
        max = longValue(payload, Long.BYTES);
      }
    }

    private void flushGroup() {
      if (group != null)
        tree.set(group, extrema(min, max));
    }

    /**
     * Publish the tree when its groups are coarser than the {@code chunks} data leaves; else drop it.
     */
    void finish(final int chunks) {
      if (enabled) {
        flushGroup();
        if (coarserThanChunks(tree.groups, chunks)) {
          tree.publish();
          return;
        }
      }
      // An incomplete or declined optional build must not leave unreachable blobs behind.
      tree.dropAll();
    }
  }

  /** Snapshot the group SMA before summary writes invalidate it; publish after data maintenance. */
  static final class Update {
    private final ProjectionIndexHOTStorage storage;
    private final ProjectionSortKeyCodec.Layout layout;
    private final Tree tree;
    private @Nullable TreeMap<byte[], GroupChange> groups;
    private boolean pendingRemoval;
    private byte @Nullable [] pendingKey;
    private boolean enabled = true;
    private boolean finished;

    private Update(final ProjectionIndexHOTStorage storage, final ProjectionSortKeyCodec.Layout layout,
        final Header header) {
      this.storage = storage;
      this.layout = layout;
      tree = new Tree(storage, header, true);
    }

    static @Nullable Update begin(final ProjectionIndexHOTStorage storage, final ProjectionSortKeyCodec.Layout layout,
        final byte[][] removals, final int removed, final byte[][] insertions, final int inserted) {
      final byte[] header = storage.getBlob(HEADER_SLOT);
      if (header == null)
        return null;
      final Update update = new Update(storage, layout, Header.open(header));
      if (removed == 1 && inserted == 0) {
        update.pendingKey = removals[0];
        update.pendingRemoval = true;
      } else if (removed == 0 && inserted == 1) {
        update.pendingKey = insertions[0];
      } else {
        update.collect(removals, removed, true);
        update.collect(insertions, inserted, false);
      }
      return update;
    }

    private void collect(final byte[][] keys, final int count, final boolean removal) {
      for (int i = 0; i < count; i++) {
        collectKey(keys[i], removal);
        if (!enabled)
          return;
      }
    }

    private void collectKey(final byte[] key, final boolean removal) {
      final int end = layout.lastFieldOffset(key, key.length);
      if (end < 0 || end > MAX_GROUP_KEY || key[end] != ProjectionSortKeyCodec.PRESENT) {
        enabled = false;
        return;
      }
      final byte[] group = Arrays.copyOf(key, end);
      if (groups == null)
        groups = new TreeMap<>(Arrays::compareUnsigned);
      GroupChange change = groups.get(group);
      if (change == null) {
        change = new GroupChange();
        groups.put(group, change);
      }
      if (key.length != end + 17) {
        // Unsupported shape: this group keeps the exact post-edit rescan.
        change.shaped = false;
        change.refresh = true;
        return;
      }
      final long value = ProjectionSortedGroupScan.readOrderedLong(key, end + 1);
      if (removal) {
        change.removed = true;
      } else {
        change.min = Math.min(change.min, value);
        change.max = Math.max(change.max, value);
      }
      // Strictly interior edits cannot change either endpoint, including duplicate values.
      // Endpoints and new groups need a refresh: from the inserted values alone when nothing was
      // removed from the group (exact: extrema of a union), otherwise from an exact rescan.
      if (!change.refresh && !tree.isInterior(group, value)) {
        change.refresh = true;
      }
    }

    /** What one edit did to one group; decides between an exact combine and an exact rescan. */
    private static final class GroupChange {
      boolean refresh;
      boolean removed;
      boolean shaped = true;
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
    }

    /**
     * A single edit strictly inside its original chunk cannot alter the enclosing group's extrema. Use
     * that already validated leaf before any mutation; every other case retains the global proof.
     * Multi-key edits collect all proofs up front, so one edit cannot remove another's bounds.
     */
    void observeOriginalLeaf(final ProjectionSortedLeaf leaf) {
      if (finished)
        throw new IllegalStateException("group SMA proof belongs to a completed edit");
      final byte[] key = pendingKey;
      if (key == null)
        return;
      final ProjectionSortedDataBlock block = leaf.dataBlock();
      if (block != null && block.hasSingleDictionaryEntry()) {
        final int end = layout.lastFieldOffset(key, key.length);
        if (end >= 0 && end <= MAX_GROUP_KEY && key.length == end + 17 && key[end] == ProjectionSortKeyCodec.PRESENT
            && leaf.keyLength(0) == key.length && leaf.keyHasPrefix(0, key, end)) {
          final long value = ProjectionSortedGroupScan.readOrderedLong(key, end + 1);
          if (value > block.minimum() && value < block.maximum()) {
            pendingKey = null;
            return;
          }
        }
      }
      resolvePending();
    }

    private void resolvePending() {
      final byte[] key = pendingKey;
      if (key != null) {
        pendingKey = null;
        collectKey(key, pendingRemoval);
      }
    }

    /** Only the editor holding this proof may retain the unchanged global extrema. */
    boolean preservesExtrema(final ProjectionIndexHOTStorage owner) {
      if (owner != storage || finished) {
        throw new IllegalStateException("group SMA proof belongs to another or completed edit");
      }
      resolvePending();
      if (!enabled || groups == null) {
        return enabled;
      }
      for (final GroupChange change : groups.values()) {
        if (change.refresh) {
          return false;
        }
      }
      return true;
    }

    /**
     * Refresh the changed groups, then keep the tree only while its groups are still coarser than the
     * view's {@code chunks} data leaves; a tree that fell below that bound is dropped whole.
     */
    void finish(final int chunks) {
      final boolean unchanged = preservesExtrema(storage);
      finished = true;
      if (!enabled) {
        tree.dropAll();
        return;
      }
      final Map<byte[], GroupChange> changedGroups = groups;
      final boolean touched;
      if (!unchanged && changedGroups != null) {
        touched = true;
        if (!refresh(changedGroups)) {
          return;
        }
      } else {
        touched = false;
      }
      if (!coarserThanChunks(tree.groups, chunks)) {
        tree.dropAll();
        return;
      }
      if (touched) {
        tree.publish();
      }
    }

    /** Apply every collected group change to the tree; {@code false} after invalidating the tree. */
    private boolean refresh(final Map<byte[], GroupChange> changedGroups) {
      final ProjectionSortedDirectory.Accessor directory =
          ProjectionSortedDirectory.open(storage.getStorageEngineReader(), storage.getIndexNumber());
      if (directory == null || directory.unencodableRows() != 0 || directory.holdsRowsWithoutAggregateValues()) {
        tree.dropAll();
        return false;
      }
      for (final var entry : changedGroups.entrySet()) {
        final GroupChange change = entry.getValue();
        if (!change.refresh)
          continue;
        final byte[] group = entry.getKey();
        if (change.shaped && !change.removed && change.min <= change.max) {
          // Insert-only change of a well-formed group: the new extrema are the extrema of the old rows
          // and the inserted values, exactly; no rows need to be read. An absent group has no rows.
          final long[] old = tree.extrema(group);
          final long min = old == null
              ? change.min
              : Math.min(old[0], change.min);
          final long max = old == null
              ? change.max
              : Math.max(old[1], change.max);
          tree.set(group, extrema(min, max));
          continue;
        }
        final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.scanFrom(group);
        if (!cursor.isValid()) {
          tree.set(group, null);
          continue;
        }
        byte[] key = new byte[cursor.keyLength()];
        cursor.copyKeyTo(key);
        if (!ProjectionSortKeyCodec.startsWith(key, key.length, group)) {
          tree.set(group, null);
          continue;
        }
        if (key.length != group.length + 17 || key[group.length] != ProjectionSortKeyCodec.PRESENT) {
          tree.dropAll();
          return false;
        }
        final long min = ProjectionSortedGroupScan.readOrderedLong(key, group.length + 1);
        cursor.skipPrefixCapturingLast(group, group.length);
        key = new byte[cursor.lastSkippedKeyLength()];
        cursor.copyLastSkippedKeyTo(key);
        if (key.length != group.length + 17 || key[group.length] != ProjectionSortKeyCodec.PRESENT) {
          tree.dropAll();
          return false;
        }
        tree.set(group, extrema(min, ProjectionSortedGroupScan.readOrderedLong(key, group.length + 1)));
      }
      return true;
    }
  }

  /** Bounded B+tree with stable ids, local splits and removal of empty nodes. */
  private static final class Tree {
    private final ProjectionIndexHOTStorage storage;
    /**
     * Right-path node cache, one node per level, private to this build or edit and never shared with
     * another transaction or revision. The edit path uses it too: sorted group order edits the same
     * leaf node many times in a row, and every read of it was a blob read and decode. Writes stay
     * immediate, so page images are exactly what they were.
     */
    private final int @Nullable [] cachedIds;
    private final ProjectionSortedLeaf @Nullable [] cachedNodes;
    private int root;
    private int height;
    private int highWater;
    /** Groups currently held; every {@link #set} keeps it exact. */
    private long groups;
    /** What the last leaf-level edit did to the group count: +1 inserted, -1 removed, 0 otherwise. */
    private int lastDelta;

    Tree(final ProjectionIndexHOTStorage storage, final Header header, final boolean cachePath) {
      this.storage = storage;
      cachedIds = cachePath
          ? new int[33]
          : null;
      cachedNodes = cachePath
          ? new ProjectionSortedLeaf[33]
          : null;
      root = header.root;
      height = header.height;
      highWater = header.highWater;
      groups = header.groups;
    }

    // Paired cache arrays are allocated together; a matching cached ID has a populated node.
    @SuppressWarnings("NullAway")
    private ProjectionSortedLeaf readNode(final int id, final int level) {
      if (cachedIds != null && cachedIds[level] == id)
        return cachedNodes[level];
      final ProjectionSortedLeaf leaf = node(storage.getBlob(HEADER_SLOT + id));
      cacheNode(id, level, leaf);
      return leaf;
    }

    // Paired cache arrays are allocated together, so a non-null ID array implies a node array.
    @SuppressWarnings("NullAway")
    private void cacheNode(final int id, final int level, final ProjectionSortedLeaf leaf) {
      if (cachedIds != null) {
        cachedIds[level] = id;
        cachedNodes[level] = leaf;
      }
    }

    private void writeNode(final int id, final int level, final ProjectionSortedLeaf leaf) {
      storage.putBlob(HEADER_SLOT + id, leaf.encodedBytes());
      cacheNode(id, level, leaf);
    }

    // Paired cache arrays are allocated together, so a non-null ID array implies a node array.
    @SuppressWarnings("NullAway")
    private void removeNode(final int id, final int level) {
      storage.tombstoneBlob(HEADER_SLOT + id);
      if (cachedIds != null && cachedIds[level] == id) {
        cachedIds[level] = 0;
        cachedNodes[level] = null;
      }
    }

    void publish() {
      storage.putBlob(HEADER_SLOT, new Header(root, height, highWater, groups).bytes());
    }

    /** Tombstone every node and the header: the view has no group SMA afterwards. */
    void dropAll() {
      if (root != 0) {
        dropReachable(root, height);
      }
      storage.tombstoneBlob(HEADER_SLOT);
      if (cachedIds != null) {
        Arrays.fill(cachedIds, 0);
        Arrays.fill(cachedNodes, null);
      }
      root = 0;
      height = 0;
      groups = 0;
    }

    /** Retired id gaps cost no work; recursion retains at most one bounded node per tree level. */
    private void dropReachable(final int id, final int level) {
      if (id <= 0 || id > highWater || level <= 0) {
        throw new IllegalStateException("invalid group SMA retirement path");
      }
      final ProjectionSortedLeaf leaf = readNode(id, level);
      if (level > 1) {
        for (int row = 0; row < leaf.rowCount(); row++) {
          dropReachable(leaf.intPayloadAt(row), level - 1);
        }
      } else {
        for (int row = 0; row < leaf.rowCount(); row++) {
          if (leaf.payloadLength(row) != 2 * Long.BYTES) {
            throw new IllegalStateException("invalid group SMA retirement leaf");
          }
        }
      }
      removeNode(id, level);
    }

    /** The stored [min, max] of a group, or {@code null} when the group has no entry. */
    long @Nullable [] extrema(final byte[] group) {
      if (root == 0)
        return null;
      int id = root;
      for (int level = height; level > 0; level--) {
        final ProjectionSortedLeaf leaf = readNode(id, level);
        int row = leaf.lowerBound(group);
        if (level > 1) {
          if (row > 0 && (row == leaf.rowCount() || leaf.compareRowKey(row, group) > 0))
            row--;
          id = leaf.intPayloadAt(row);
        } else {
          if (row == leaf.rowCount() || leaf.compareRowKey(row, group) != 0)
            return null;
          final byte[] payload = leaf.copyPayload(row);
          if (payload.length != 16)
            throw new IllegalStateException("invalid group SMA payload");
          final long min = longValue(payload, 0);
          final long max = longValue(payload, 8);
          if (min > max)
            throw new IllegalStateException("inverted group SMA");
          return new long[] {min, max};
        }
      }
      throw new IllegalStateException("invalid group SMA height");
    }

    boolean isInterior(final byte[] group, final long value) {
      if (root == 0)
        return false;
      int id = root;
      for (int level = height; level > 0; level--) {
        final ProjectionSortedLeaf leaf = readNode(id, level);
        int row = leaf.lowerBound(group);
        if (level > 1) {
          if (row > 0 && (row == leaf.rowCount() || leaf.compareRowKey(row, group) > 0))
            row--;
          id = leaf.intPayloadAt(row);
        } else {
          if (row == leaf.rowCount() || leaf.compareRowKey(row, group) != 0)
            return false;
          final byte[] payload = leaf.copyPayload(row);
          if (payload.length != 16)
            throw new IllegalStateException("invalid group SMA payload");
          final long min = longValue(payload, 0);
          final long max = longValue(payload, 8);
          if (min > max)
            throw new IllegalStateException("inverted group SMA");
          return value > min && value < max;
        }
      }
      throw new IllegalStateException("invalid group SMA height");
    }

    int allocate() {
      if (highWater == Integer.MAX_VALUE)
        throw new IllegalStateException("group SMA ids exhausted");
      return ++highWater;
    }

    // A bounded group key and its fixed-size extrema payload always fit a singleton leaf.
    @SuppressWarnings("NullAway")
    void set(final byte[] key, final byte @Nullable [] value) {
      if (root == 0) {
        if (value == null)
          return;
        root = allocate();
        height = 1;
        groups = 1;
        writeNode(root, height, ProjectionSortedLeaf.encode(new byte[][] {key}, new byte[][] {value}, 1));
        return;
      }
      lastDelta = 0;
      final List<Child> children = edit(root, height, key, value);
      groups += lastDelta;
      if (children.isEmpty()) {
        root = 0;
        height = 0;
        if (groups != 0)
          throw new IllegalStateException("group SMA count survived its last node");
      } else if (children.size() == 1)
        root = children.getFirst().id;
      else {
        final byte[][] keys = new byte[children.size()][];
        final byte[][] payloads = new byte[children.size()][];
        for (int i = 0; i < children.size(); i++) {
          keys[i] = children.get(i).key;
          payloads[i] = childPayload(children.get(i).id);
        }
        root = allocate();
        height++;
        if (height > 32)
          throw new IllegalStateException("group SMA height exceeded");
        final ProjectionSortedLeaf newRoot = ProjectionSortedLeaf.encode(keys, payloads, keys.length);
        if (newRoot == null || newRoot.encodedBytes().length > LIMIT)
          throw new IllegalStateException("group SMA root exceeds bound");
        writeNode(root, height, newRoot);
      }
    }

    private List<Child> edit(final int id, final int level, final byte[] key, final byte @Nullable [] value) {
      final ProjectionSortedLeaf old = readNode(id, level);
      int position = old.lowerBound(key);
      if (level == 1) {
        final boolean present = position < old.rowCount() && old.compareRowKey(position, key) == 0;
        lastDelta = value == null
            ? (present
                ? -1
                : 0)
            : (present
                ? 0
                : 1);
        if ((present && value != null && Arrays.equals(old.copyPayload(position), value))
            || (!present && value == null))
          return List.of(new Child(id, old.copyKey(0)));
        if (value == null && old.rowCount() == 1) {
          removeNode(id, level);
          return List.of();
        }
        // Copy a fitting node's encoded rows directly. Its stable id, capacity and split policy
        // stay unchanged; only the former per-row key/value materialization disappears.
        final ProjectionSortedLeaf changed = value == null
            ? old.withRemoved(key)
            : present
                ? old.withReplaced(position, key, value)
                : old.withInserted(key, value);
        if (changed != null && changed.encodedBytes().length <= LIMIT) {
          writeNode(id, level, changed);
          return List.of(new Child(id, changed.copyKey(0)));
        }
      }
      final ArrayList<byte[]> keys;
      final ArrayList<byte[]> payloads;
      if (level == 1) {
        keys = new ArrayList<>(old.rowCount() + 1);
        payloads = new ArrayList<>(old.rowCount() + 1);
        final boolean present = position < old.rowCount() && old.compareRowKey(position, key) == 0;
        for (int row = 0; row <= old.rowCount(); row++) {
          if (row == position && value != null) {
            keys.add(key);
            payloads.add(value);
          }
          if (row < old.rowCount() && !(row == position && present)) {
            keys.add(old.copyKey(row));
            payloads.add(old.copyPayload(row));
          }
        }
      } else {
        if (position > 0 && (position == old.rowCount() || old.compareRowKey(position, key) > 0))
          position--;
        final List<Child> changed = edit(old.intPayloadAt(position), level - 1, key, value);
        if (changed.size() == 1 && changed.getFirst().id == old.intPayloadAt(position)
            && old.compareRowKey(position, changed.getFirst().key) == 0) {
          return List.of(new Child(id, old.copyKey(0)));
        }
        keys = new ArrayList<>(old.rowCount() + 1);
        payloads = new ArrayList<>(old.rowCount() + 1);
        for (int row = 0; row < old.rowCount(); row++) {
          if (row == position) {
            for (final Child child : changed) {
              keys.add(child.key);
              payloads.add(childPayload(child.id));
            }
          } else {
            keys.add(old.copyKey(row));
            payloads.add(old.copyPayload(row));
          }
        }
      }
      if (keys.isEmpty()) {
        removeNode(id, level);
        return List.of();
      }
      final byte[][] keyArray = keys.toArray(byte[][]::new);
      final byte[][] payloadArray = payloads.toArray(byte[][]::new);
      final ArrayList<Child> result = new ArrayList<>(2);
      for (int from = 0; from < keys.size();) {
        int low = 1;
        int high = Math.min(ProjectionSortedLeaf.MAX_ROWS, keys.size() - from);
        // Avoid repeatedly encoding smaller prefixes when the whole edited node still fits.
        ProjectionSortedLeaf best = ProjectionSortedLeaf.encode(keyArray, payloadArray, from, high);
        if (best != null && best.encodedBytes().length <= LIMIT) {
          low = high + 1;
        } else {
          best = null;
          high--;
        }
        while (low <= high) {
          final int count = (low + high) >>> 1;
          final ProjectionSortedLeaf candidate = ProjectionSortedLeaf.encode(keyArray, payloadArray, from, count);
          if (candidate != null && candidate.encodedBytes().length <= LIMIT) {
            best = candidate;
            low = count + 1;
          } else
            high = count - 1;
        }
        if (best == null)
          throw new IllegalStateException("group SMA entry exceeds bound");
        final int target = from == 0
            ? id
            : allocate();
        writeNode(target, level, best);
        result.add(new Child(target, best.copyKey(0)));
        from += best.rowCount();
      }
      return result;
    }
  }

  @SuppressWarnings("ArrayRecordComponent") // Edit-path carrier; fences are compared explicitly, never through record
                                            // equality.
  private record Child(int id, byte[] key) {
  }

  private static ProjectionSortedLeaf node(final byte @Nullable [] bytes) {
    if (bytes == null || bytes.length > LIMIT)
      throw new IllegalStateException("missing or oversized group SMA node");
    return ProjectionSortedLeaf.open(bytes);
  }

  private static byte[] childPayload(final int id) {
    final byte[] bytes = new byte[4];
    putInt(bytes, 0, id);
    return bytes;
  }

  private static byte[] extrema(final long min, final long max) {
    final byte[] bytes = new byte[16];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) (min >>> (i * 8));
      bytes[8 + i] = (byte) (max >>> (i * 8));
    }
    return bytes;
  }

  private static int integer(final byte[] bytes, final int at) {
    return (bytes[at] & 255) | (bytes[at + 1] & 255) << 8 | (bytes[at + 2] & 255) << 16 | (bytes[at + 3] & 255) << 24;
  }

  private static long longValue(final byte[] bytes, final int at) {
    return (integer(bytes, at) & 0xffffffffL) | (long) integer(bytes, at + 4) << 32;
  }

  private static void putInt(final byte[] bytes, final int at, final int value) {
    for (int i = 0; i < 4; i++)
      bytes[at + i] = (byte) (value >>> (i * 8));
  }

  private static void putLong(final byte[] bytes, final int at, final long value) {
    for (int i = 0; i < 8; i++)
      bytes[at + i] = (byte) (value >>> (i * 8));
  }
}
