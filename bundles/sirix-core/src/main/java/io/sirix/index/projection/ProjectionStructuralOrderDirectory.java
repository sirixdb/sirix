/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.settings.Fixed;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongFunction;

final class ProjectionStructuralOrderDirectory {
  static final long BASE = 1L << 50;
  static final long NODE_KEY_LIMIT = 1L << 48;
  static final long LIMIT = BASE + NODE_KEY_LIMIT;

  private static final long NULL_NODE_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();
  private static final byte FORMAT_VERSION = 1;
  private static final int FORMAT_HEADER_BYTES = 1;
  private static final int MAX_LOCAL_LABEL_BYTES = 1 << 14;
  private static final int MAX_ANCESTORS = 1 << 15;
  private static final int MAX_FULL_LABEL_DIVISIONS = 1 << 16;

  private ProjectionStructuralOrderDirectory() {
  }

  static Accessor open(final ProjectionIndexHOTStorage storage) {
    return new Accessor(Objects.requireNonNull(storage));
  }

  static void initialize(final NodeReadOnlyTrx rtx, final ProjectionIndexHOTStorage storage) {
    Objects.requireNonNull(rtx);
    final Accessor accessor = open(storage);
    if (!(rtx instanceof NodeCursor cursor)) {
      throw new IllegalArgumentException("projection structural ordering requires a node cursor");
    }

    final long restoreNodeKey = rtx.getNodeKey();
    try {
      if (!cursor.moveToDocumentRoot()) {
        throw new IllegalStateException("document root is unavailable while initializing projection order");
      }
      final long documentRootKey = cursor.getNodeKey();
      while (true) {
        accessor.putIfAbsent(cursor.getNodeKey(), cursor.getLeftSiblingKey());
        if (cursor.moveToFirstChild()) {
          continue;
        }

        while (cursor.getNodeKey() != documentRootKey && !cursor.hasRightSibling()) {
          if (!cursor.moveToParent()) {
            throw new IllegalStateException("structural preorder lost its parent while initializing projection order");
          }
        }
        if (cursor.getNodeKey() == documentRootKey) {
          return;
        }
        if (!cursor.moveToRightSibling()) {
          throw new IllegalStateException("structural preorder lost its right sibling while initializing projection order");
        }
      }
    } finally {
      if (!cursor.moveTo(restoreNodeKey)) {
        throw new IllegalStateException("failed to restore the node cursor after initializing projection order");
      }
    }
  }

  static boolean ownsSlot(final long slotKey) {
    return slotKey >= BASE && slotKey < LIMIT;
  }

  private static long slotKey(final long nodeKey) {
    validateNodeKey(nodeKey, "node");
    return BASE + nodeKey;
  }

  private static void validateNodeKey(final long nodeKey, final String role) {
    if (nodeKey < 0 || nodeKey >= NODE_KEY_LIMIT) {
      throw new IllegalArgumentException(role + " key is outside the projection structural-order namespace: " + nodeKey);
    }
  }

  private static void validateOptionalNodeKey(final long nodeKey, final String role) {
    if (nodeKey != NULL_NODE_KEY) {
      validateNodeKey(nodeKey, role);
    }
  }

  static final class Accessor {
    private final ProjectionIndexHOTStorage storage;

    private Accessor(final ProjectionIndexHOTStorage storage) {
      this.storage = storage;
    }

    SirixDeweyID putIfAbsent(final long nodeKey, final long leftSiblingKey) {
      validateNodeKey(nodeKey, "node");
      validateOptionalNodeKey(leftSiblingKey, "left sibling");
      if (nodeKey == leftSiblingKey) {
        throw new IllegalArgumentException("a structural node cannot be its own left sibling");
      }

      final SirixDeweyID existing = localLabel(nodeKey);
      if (existing != null) {
        return existing;
      }

      final SirixDeweyID localLabel;
      if (leftSiblingKey == NULL_NODE_KEY) {
        localLabel = firstLocalLabel();
      } else {
        localLabel = SirixDeweyID.newBetween(requireLocalLabel(leftSiblingKey, "left sibling"), null);
      }
      putLocalLabel(nodeKey, localLabel);
      return localLabel;
    }

    SirixDeweyID relabel(final long nodeKey, final long leftSiblingKey, final long rightSiblingKey) {
      validateNodeKey(nodeKey, "node");
      validateOptionalNodeKey(leftSiblingKey, "left sibling");
      validateOptionalNodeKey(rightSiblingKey, "right sibling");
      if (nodeKey == leftSiblingKey || nodeKey == rightSiblingKey) {
        throw new IllegalArgumentException("a structural node cannot be its own sibling");
      }
      if (leftSiblingKey != NULL_NODE_KEY && leftSiblingKey == rightSiblingKey) {
        throw new IllegalArgumentException("left and right structural siblings must be distinct");
      }

      final SirixDeweyID left = leftSiblingKey == NULL_NODE_KEY
          ? null
          : requireLocalLabel(leftSiblingKey, "left sibling");
      final SirixDeweyID right = rightSiblingKey == NULL_NODE_KEY
          ? null
          : requireLocalLabel(rightSiblingKey, "right sibling");
      if (left != null && right != null && left.compareTo(right) >= 0) {
        throw new IllegalStateException("structural sibling labels are not strictly ordered");
      }

      final SirixDeweyID existing = localLabel(nodeKey);
      if (existing != null && (left == null || left.compareTo(existing) < 0)
          && (right == null || existing.compareTo(right) < 0)) {
        return existing;
      }

      final SirixDeweyID replacement = left == null && right == null
          ? firstLocalLabel()
          : SirixDeweyID.newBetween(left, right);
      putLocalLabel(nodeKey, replacement);
      return replacement;
    }

    void remove(final long nodeKey) {
      storage.tombstoneStructuralOrderSlot(slotKey(nodeKey));
    }

    SirixDeweyID fullLabel(final long nodeKey, final LongFunction<ImmutableNode> nodeLookup) {
      validateNodeKey(nodeKey, "node");
      Objects.requireNonNull(nodeLookup);

      long[] ancestry = new long[Math.min(16, MAX_ANCESTORS)];
      int depth = 1;
      ancestry[0] = nodeKey;
      long tortoise = nodeKey;
      long hare = parentKey(nodeKey, nodeLookup);
      int power = 1;
      int cycleLength = 1;
      while (hare != NULL_NODE_KEY) {
        if (tortoise == hare) {
          throw new IllegalStateException("cycle in projection structural ancestry at node " + hare);
        }
        if (depth == MAX_ANCESTORS) {
          throw new IllegalStateException("projection structural ancestry exceeds " + MAX_ANCESTORS + " nodes");
        }
        if (depth == ancestry.length) {
          ancestry = Arrays.copyOf(ancestry, Math.min(MAX_ANCESTORS, Math.multiplyExact(depth, 2)));
        }
        ancestry[depth++] = hare;
        if (power == cycleLength) {
          tortoise = hare;
          power = Math.multiplyExact(power, 2);
          cycleLength = 0;
        }
        hare = parentKey(hare, nodeLookup);
        cycleLength++;
      }

      int divisionCount = 1;
      int initialCapacity = Math.max(16, Math.min(MAX_FULL_LABEL_DIVISIONS, Math.addExact(1, depth * 2)));
      int[] divisions = new int[initialCapacity];
      divisions[0] = 1;
      for (int ancestryIndex = depth - 1; ancestryIndex >= 0; ancestryIndex--) {
        final long ancestorKey = ancestry[ancestryIndex];
        final SirixDeweyID localLabel = requireLocalLabel(ancestorKey, "ancestor");
        final int[] localDivisions = localLabel.getDivisionValues();
        final int suffixLength = localDivisions.length - 1;
        final int required = Math.addExact(divisionCount, suffixLength);
        if (required > MAX_FULL_LABEL_DIVISIONS) {
          throw new IllegalStateException(
              "projection structural label exceeds " + MAX_FULL_LABEL_DIVISIONS + " divisions");
        }
        if (required > divisions.length) {
          int capacity = divisions.length;
          while (capacity < required) {
            capacity = Math.min(MAX_FULL_LABEL_DIVISIONS, Math.multiplyExact(capacity, 2));
          }
          divisions = Arrays.copyOf(divisions, capacity);
        }
        System.arraycopy(localDivisions, 1, divisions, divisionCount, suffixLength);
        divisionCount = required;
      }

      final SirixDeweyID fullLabel = new SirixDeweyID(divisionCount, divisions);
      if (fullLabel.toBytes().length > ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES) {
        throw new IllegalStateException("projection structural label exceeds the bounded order-label lane");
      }
      return fullLabel;
    }

    private static long parentKey(final long nodeKey, final LongFunction<ImmutableNode> nodeLookup) {
      if (nodeKey == NULL_NODE_KEY) {
        return NULL_NODE_KEY;
      }
      validateNodeKey(nodeKey, "ancestor");
      final ImmutableNode node = nodeLookup.apply(nodeKey);
      if (node == null) {
        throw new IllegalStateException("missing structural node " + nodeKey + " while resolving projection order");
      }
      if (node.getNodeKey() != nodeKey) {
        throw new IllegalStateException(
            "structural node lookup returned " + node.getNodeKey() + " for requested node " + nodeKey);
      }
      final long parentKey = node.getParentKey();
      validateOptionalNodeKey(parentKey, "parent");
      if (parentKey == nodeKey) {
        throw new IllegalStateException("structural node " + nodeKey + " is its own parent");
      }
      return parentKey;
    }

    private SirixDeweyID requireLocalLabel(final long nodeKey, final String role) {
      final SirixDeweyID localLabel = localLabel(nodeKey);
      if (localLabel == null) {
        throw new IllegalStateException("missing projection structural-order label for " + role + " node " + nodeKey);
      }
      return localLabel;
    }

    private SirixDeweyID localLabel(final long nodeKey) {
      final byte[] encoded = storage.getStructuralOrderSlot(slotKey(nodeKey));
      if (encoded == null) {
        return null;
      }
      if (encoded.length <= FORMAT_HEADER_BYTES || encoded[0] != FORMAT_VERSION
          || encoded.length - FORMAT_HEADER_BYTES > MAX_LOCAL_LABEL_BYTES) {
        throw new IllegalStateException("invalid projection structural-order encoding for node " + nodeKey);
      }

      final SirixDeweyID localLabel;
      try {
        localLabel = new SirixDeweyID(encoded, FORMAT_HEADER_BYTES, encoded.length - FORMAT_HEADER_BYTES);
      } catch (final RuntimeException exception) {
        throw new IllegalStateException("invalid projection structural-order label for node " + nodeKey, exception);
      }
      final int[] divisions = localLabel.getDivisionValues();
      final byte[] canonical = localLabel.toBytes();
      if (divisions.length < 2 || divisions.length > MAX_FULL_LABEL_DIVISIONS || divisions[0] != 1
          || localLabel.getLevel() != 1 || containsInvalidLocalDivision(divisions)
          || !Arrays.equals(canonical, 0, canonical.length,
              encoded, FORMAT_HEADER_BYTES, encoded.length)) {
        throw new IllegalStateException("invalid projection structural-order label for node " + nodeKey);
      }
      return localLabel;
    }

    private void putLocalLabel(final long nodeKey, final SirixDeweyID localLabel) {
      final int[] divisions = localLabel.getDivisionValues();
      final byte[] payload = localLabel.toBytes();
      if (divisions.length < 2 || divisions.length > MAX_FULL_LABEL_DIVISIONS || divisions[0] != 1
          || localLabel.getLevel() != 1 || containsInvalidLocalDivision(divisions) || payload.length == 0
          || payload.length > MAX_LOCAL_LABEL_BYTES) {
        throw new IllegalStateException("projection structural-order label exceeds its bounded local encoding");
      }
      final byte[] encoded = new byte[Math.addExact(FORMAT_HEADER_BYTES, payload.length)];
      encoded[0] = FORMAT_VERSION;
      System.arraycopy(payload, 0, encoded, FORMAT_HEADER_BYTES, payload.length);
      storage.putStructuralOrderSlot(slotKey(nodeKey), encoded);
    }

    private static SirixDeweyID firstLocalLabel() {
      return SirixDeweyID.newRootID().getNewChildID();
    }

    private static boolean containsInvalidLocalDivision(final int[] divisions) {
      for (int index = 1; index < divisions.length; index++) {
        if (divisions[index] < 2) {
          return true;
        }
      }
      return false;
    }
  }
}
