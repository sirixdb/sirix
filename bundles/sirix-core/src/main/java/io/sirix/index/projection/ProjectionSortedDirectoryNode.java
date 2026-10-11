/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.HOTReadIntent;
import io.sirix.cache.HOTMiniPageCache.ReadScope;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * Direct small directory node, with separate ordinary HOT key records only when two full fences
 * cannot fit together. An oversized separator alone may use the existing blob overflow path. No
 * reader reconstructs a combined PSL1 leaf from these key records.
 */
final class ProjectionSortedDirectoryNode {
  static final long SEPARATOR_SLOT_BASE = ProjectionSortedLeafStore.LEAF_SLOT_BASE + (4L << 32);
  private final ProjectionSortedLeaf leaf;
  private final byte @Nullable [][] separatorKeys;

  private ProjectionSortedDirectoryNode(final ProjectionSortedLeaf leaf, final byte @Nullable [][] keys) {
    this(leaf, keys, true);
  }

  private ProjectionSortedDirectoryNode(final ProjectionSortedLeaf leaf, final byte @Nullable [][] keys,
      final boolean validateChildren) {
    this.leaf = Objects.requireNonNull(leaf, "leaf");
    separatorKeys = keys;
    final int payloadBytes = keys == null
        ? Integer.BYTES
        : 2 * Integer.BYTES;
    if (validateChildren) {
      for (int i = 0; i < leaf.rowCount(); i++) {
        if (leaf.payloadLength(i) != payloadBytes || leaf.payloadIntAt(i, 0) <= 0) {
          throw new IllegalStateException("invalid sorted directory child reference");
        }
      }
    }
    if (keys != null) {
      for (int i = 0; i < leaf.rowCount(); i++) {
        if ((validateChildren && leaf.payloadIntAt(i, Integer.BYTES) <= 0)
            || (i > 0 && Arrays.compareUnsigned(keys[i - 1], keys[i]) >= 0)) {
          throw new IllegalStateException("invalid sorted directory separator references");
        }
      }
    }
  }

  static ProjectionSortedDirectoryNode direct(final ProjectionSortedLeaf leaf) {
    return new ProjectionSortedDirectoryNode(leaf, null);
  }

  static ProjectionSortedDirectoryNode indirect(final ProjectionSortedLeaf leaf, final byte[][] keys) {
    if (keys.length != leaf.rowCount() || keys.length > 2) {
      throw new IllegalArgumentException("an indirect node must hold one or two separators");
    }
    return new ProjectionSortedDirectoryNode(leaf, keys);
  }

  static ProjectionSortedDirectoryNode open(final byte[] bytes, final ProjectionIndexHOTStorage storage) {
    final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.open(bytes);
    if (leaf.payloadLength(0) == Integer.BYTES) {
      return direct(leaf);
    }
    final byte[][] keys = new byte[leaf.rowCount()][];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = requireKey(storage.getBlob(separatorSlot(leaf, i)));
    }
    return indirect(leaf, keys);
  }

  static ProjectionSortedDirectoryNode openReadOnly(final byte[] bytes, final StorageEngineReader reader,
      final int indexNumber, final HOTReadIntent intent, final @Nullable ReadScope scope) {
    final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.openDirectoryReadOnly(bytes);
    if (leaf.payloadLength(0) == Integer.BYTES) {
      return new ProjectionSortedDirectoryNode(leaf, null, false);
    }
    if (leaf.rowCount() > 2) {
      throw new IllegalArgumentException("an indirect node must hold one or two separators");
    }
    final byte[][] keys = new byte[leaf.rowCount()][];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = requireKey(
          ProjectionIndexHOTStorage.readBlob(reader, indexNumber, separatorSlot(leaf, i), intent, scope)).clone();
    }
    return new ProjectionSortedDirectoryNode(leaf, keys, false);
  }

  ProjectionSortedLeaf leaf() {
    return leaf;
  }

  long retainedMetadataBytes() {
    long bytes = 32;
    if (separatorKeys != null) {
      bytes += (16L + 8L * separatorKeys.length + 7) & ~7L;
      for (final byte[] key : separatorKeys) {
        bytes += (16L + key.length + 7) & ~7L;
      }
    }
    return bytes;
  }

  private static byte[] requireKey(final byte @Nullable [] key) {
    if (key == null) {
      throw new IllegalStateException("missing sorted directory separator");
    }
    return key;
  }

  private static long separatorSlot(final ProjectionSortedLeaf leaf, final int row) {
    if (leaf.payloadLength(row) != 2 * Integer.BYTES || leaf.payloadIntAt(row, Integer.BYTES) <= 0) {
      throw new IllegalStateException("invalid sorted directory separator id");
    }
    return SEPARATOR_SLOT_BASE + leaf.payloadIntAt(row, Integer.BYTES);
  }

  void retireSeparators(final ProjectionIndexHOTStorage storage) {
    if (separatorKeys != null) {
      for (int i = 0; i < leaf.rowCount(); i++) {
        storage.tombstoneBlob(separatorSlot(leaf, i));
      }
    }
  }

  int rowCount() {
    return leaf.rowCount();
  }

  int intPayloadAt(final int row) {
    return leaf.payloadIntAt(row, 0);
  }

  int keyLength(final int row) {
    return separatorKeys == null
        ? leaf.keyLength(row)
        : separatorKeys[row].length;
  }

  byte[] copyKey(final int row) {
    return separatorKeys == null
        ? leaf.copyKey(row)
        : separatorKeys[row].clone();
  }

  void copyKeyTo(final int row, final byte[] target) {
    if (separatorKeys == null) {
      leaf.copyKeyTo(row, target);
    } else {
      System.arraycopy(separatorKeys[row], 0, target, 0, separatorKeys[row].length);
    }
  }

  int compareRowKey(final int row, final byte[] key) {
    return separatorKeys == null
        ? leaf.compareRowKey(row, key)
        : Arrays.compareUnsigned(separatorKeys[row], key);
  }

  int lowerBound(final byte[] key) {
    if (separatorKeys == null) {
      return leaf.lowerBound(key);
    }
    int low = 0;
    int high = leaf.rowCount();
    while (low < high) {
      final int middle = (low + high) >>> 1;
      if (compareRowKey(middle, key) < 0) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  boolean keyHasPrefix(final int row, final byte[] prefix, final int length) {
    if (separatorKeys == null) {
      return leaf.keyHasPrefix(row, prefix, length);
    }
    final byte[] key = separatorKeys[row];
    return key.length >= length && Arrays.equals(key, 0, length, prefix, 0, length);
  }

  int firstNonPrefixRowAfter(final int row, final byte[] prefix, final int length) {
    if (separatorKeys == null) {
      return leaf.firstNonPrefixRowAfter(row, prefix, length);
    }
    int at = row + 1;
    while (at < rowCount() && keyHasPrefix(at, prefix, length)) {
      at++;
    }
    return at;
  }

  @Nullable
  ProjectionSortedDirectoryNode withReplaced(final int row, final byte[] key, final byte[] payload) {
    if (separatorKeys != null) {
      return null;
    }
    final ProjectionSortedLeaf replacement = leaf.withReplaced(row, key, payload);
    return replacement != null && replacement.encodedBytes().length <= ProjectionSortedChunks.MAX_BYTES
        ? direct(replacement)
        : null;
  }

  byte[] encodedBytes() {
    return leaf.encodedBytes();
  }
}
