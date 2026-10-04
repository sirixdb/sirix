package io.sirix.access.trx.node.json;

import io.sirix.access.trx.node.HashType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.PostOrderAxis;
import io.sirix.index.IndexType;
import io.sirix.node.BytesOut;
import io.sirix.node.interfaces.StructNode;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 * Writer-local hash/count maintenance for a structural mutation. Capture only changed boundary
 * records and their ancestors before changing links, then propagate their final contributions.
 * ROLLING never reads the unchanged child prefix. Scratch storage is reused between mutations.
 */
final class JsonHashingMutation {
  private static final long PRIME = 77081L;
  private static final int WIDTH = 9;
  private static final int KEY = 0;
  private static final int OLD_PARENT = 1;
  private static final int OLD_BASE = 2;
  private static final int OLD_HASH = 3;
  private static final int OLD_COUNT = 4;
  private static final int HASH_DELTA = 5;
  private static final int COUNT_DELTA = 6;
  private static final int NEW_PARENT = 7;
  private static final int PENDING = 8;

  private final StorageEngineWriter writer;
  private final InternalJsonNodeReadOnlyTrx cursor;
  private final HashType hashType;
  private final BytesOut<?> bytes;
  private final Long2IntOpenHashMap offsets = new Long2IntOpenHashMap(16);
  private final LongArrayList states = new LongArrayList(16 * WIDTH);
  private final LongArrayList ready = new LongArrayList(16);

  JsonHashingMutation(final StorageEngineWriter writer, final InternalJsonNodeReadOnlyTrx cursor,
      final HashType hashType, final BytesOut<?> bytes) {
    this.writer = writer;
    this.cursor = cursor;
    this.hashType = hashType;
    this.bytes = bytes;
    offsets.defaultReturnValue(-1);
  }

  boolean begin() {
    if (hashType == HashType.NONE) {
      return false;
    }
    // A large earlier move may have grown the map. Clearing its entire backing array on
    // every subsequent scalar insert would make those inserts scale with that old move.
    for (int offset = 0; offset < states.size(); offset += WIDTH) {
      offsets.remove(states.getLong(offset + KEY));
    }
    states.clear();
    ready.clear();
    return true;
  }

  void capturePath(long key) {
    while (key >= 0 && !offsets.containsKey(key)) {
      final StructNode node = writer.getRecord(key, IndexType.DOCUMENT, -1);
      final long parent = node.getParentKey();
      final long hash = node.getHash();
      final long ownHash = key == 0 && node.getFirstChildKey() == -1 && hash == 0
          ? 0
          : node.computeHash(bytes);
      final int offset = states.size();
      offsets.put(key, offset);
      states.add(key);
      states.add(parent);
      states.add(ownHash);
      states.add(hash);
      states.add(node.getDescendantCount());
      states.add(0);
      states.add(0);
      states.add(-1);
      states.add(0);
      key = parent;
    }
  }

  void captureNeighborhood(final long key) {
    final StructNode node = writer.getRecord(key, IndexType.DOCUMENT, -1);
    final long left = node.getLeftSiblingKey();
    final long right = node.getRightSiblingKey();
    final long first = node.getFirstChildKey();
    capturePath(key);
    capturePath(left);
    capturePath(right);
    // Moving to first child changes the former first child's left link.
    capturePath(first);
  }

  void captureSubtree(final long key) {
    final long saved = cursor.getNodeKey();
    cursor.moveTo(key);
    final var axis = new DescendantAxis(cursor, IncludeSelf.YES);
    while (axis.hasNext()) {
      capturePath(axis.nextLong());
    }
    cursor.moveTo(saved);
  }

  void addNewLeaf(final long key) {
    recompute(key);
    addNewContribution(key);
  }

  void addNewSubtree(final long key) {
    final long saved = cursor.getNodeKey();
    if (saved != key) {
      cursor.moveTo(key);
    }
    if (cursor.hasFirstChild()) {
      final var axis = new PostOrderAxis(cursor, IncludeSelf.YES);
      while (axis.hasNext()) {
        recompute(axis.nextLong());
      }
    } else {
      // Primitive forests are common: do not allocate one axis/iterator per appended value.
      recompute(key);
    }
    addNewContribution(key);
    if (cursor.getNodeKey() != saved) {
      cursor.moveTo(saved);
    }
  }

  private void addNewContribution(final long key) {
    final StructNode node = writer.getRecord(key, IndexType.DOCUMENT, -1);
    contribute(node.getParentKey(), node.getHash(), node.getDescendantCount() + 1);
  }

  private void contribute(final long parent, final long hash, final long count) {
    final int offset = offsets.get(parent);
    if (offset >= 0) {
      states.set(offset + HASH_DELTA, states.getLong(offset + HASH_DELTA) + hash);
      states.set(offset + COUNT_DELTA, states.getLong(offset + COUNT_DELTA) + count);
    }
  }

  /** Finish in final-tree postorder, including changes of a boundary record's parent. */
  void finish() {
    for (int offset = 0; offset < states.size(); offset += WIDTH) {
      contribute(states.getLong(offset + OLD_PARENT), -states.getLong(offset + OLD_HASH),
          -states.getLong(offset + OLD_COUNT) - 1);
      final var record = writer.getRecord(states.getLong(offset + KEY), IndexType.DOCUMENT, -1);
      if (record instanceof StructNode node) {
        final long parent = node.getParentKey();
        states.set(offset + NEW_PARENT, parent);
        final int parentOffset = offsets.get(parent);
        if (parentOffset >= 0) {
          states.set(parentOffset + PENDING, states.getLong(parentOffset + PENDING) + 1);
        }
      }
    }
    for (int offset = 0; offset < states.size(); offset += WIDTH) {
      if (states.getLong(offset + PENDING) == 0) {
        ready.add(offset);
      }
    }
    int completed = 0;
    while (!ready.isEmpty()) {
      final int offset = (int) ready.removeLong(ready.size() - 1);
      completed++;
      final long key = states.getLong(offset + KEY);
      final var record = writer.getRecord(key, IndexType.DOCUMENT, -1);
      if (!(record instanceof StructNode)) {
        continue;
      }
      final StructNode node = writer.prepareRecordForModification(key, IndexType.DOCUMENT, -1);
      final long descendants = states.getLong(offset + OLD_COUNT) + states.getLong(offset + COUNT_DELTA);
      if (descendants < 0) {
        throw new IllegalStateException("Negative descendant count after structural mutation at " + key);
      }
      node.setDescendantCount(descendants);
      if (hashType == HashType.ROLLING) {
        node.setHash(states.getLong(offset + OLD_HASH) - states.getLong(offset + OLD_BASE)
            + states.getLong(offset + HASH_DELTA) * PRIME + node.computeHash(bytes));
        writer.persistRecord(node, IndexType.DOCUMENT, -1);
      } else {
        recompute(key);
      }
      final StructNode finished = writer.getRecord(key, IndexType.DOCUMENT, -1);
      final long parent = states.getLong(offset + NEW_PARENT);
      contribute(parent, finished.getHash(), descendants + 1);
      final int parentOffset = offsets.get(parent);
      if (parentOffset >= 0) {
        final long pending = states.getLong(parentOffset + PENDING) - 1;
        states.set(parentOffset + PENDING, pending);
        if (pending == 0) {
          ready.add(parentOffset);
        }
      }
    }
    if (completed != offsets.size()) {
      throw new IllegalStateException("Cyclic structural hash dependencies");
    }
  }

  /** Rebuild one new node after its children, or one POSTORDER boundary/ancestor. */
  private void recompute(final long key) {
    final StructNode original = writer.getRecord(key, IndexType.DOCUMENT, -1);
    long child = original.getFirstChildKey();
    long descendants = 0;
    long aggregate = 0;
    long factor = 1;
    while (child >= 0) {
      final StructNode node = writer.getRecord(child, IndexType.DOCUMENT, -1);
      descendants += node.getDescendantCount() + 1;
      if (hashType == HashType.ROLLING) {
        aggregate += node.getHash() * PRIME;
      } else {
        aggregate = aggregate * PRIME + node.getHash();
        factor *= PRIME;
      }
      child = node.getRightSiblingKey();
    }
    final StructNode node = writer.prepareRecordForModification(key, IndexType.DOCUMENT, -1);
    node.setDescendantCount(descendants);
    node.setHash(node.computeHash(bytes) * factor + aggregate);
    writer.persistRecord(node, IndexType.DOCUMENT, -1);
  }
}
