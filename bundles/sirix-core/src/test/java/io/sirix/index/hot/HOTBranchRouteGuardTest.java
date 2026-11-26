/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two route questions a mutation asks before it extends a node's mask or publishes a frontier
 * block (FMT-305): whether every node a branch insert could extend would still name its
 * discriminative key bytes, and whether a freshly assembled block really routes a part's key to
 * that part — which now takes one step per level, because such a block may be a small nest.
 */
final class HOTBranchRouteGuardTest {

  private static final byte[] VALUE = {7, 7, 7, 7};

  /** Past the last extraction position, so a node naming an early byte cannot take it. */
  private static final int LATE_BYTE = 300;

  private static final int LATE_BIT = LATE_BYTE * Byte.SIZE;

  /** The bytes the fixtures branch on; every key carries a zero in each of them. */
  private static final int[] BRANCH_BYTES = {10, 11, LATE_BYTE - 3, LATE_BYTE - 2, LATE_BYTE};

  private final AtomicLong pageKeys = new AtomicLong(1);

  private final List<HOTLeafPage> leaves = new ArrayList<>();

  @Test
  void aRouteWhoseEveryNodeCanStillNameItsBytesIsAccepted() {
    try {
      assertTrue(routeCanName(1, nodeNear(), nodeNear(), nodeNear()),
          "every node names a byte a few before the new one, so each stays inside one window");
    } finally {
      close();
    }
  }

  /**
   * The node just below the insertion depth is inside the window a branch insert may extend, so a
   * node there that could not name the new byte declines the whole route.
   */
  @Test
  void aRouteIsDeclinedWhenTheNodeBelowTheInsertCannotNameTheBit() {
    try {
      assertFalse(routeCanName(1, nodeNear(), nodeNear(), nodeFar()),
          "the node one below the insert would have to name a byte past the extraction range");
    } finally {
      close();
    }
  }

  /** A capacity cascade carries a split bit upwards, so the root is inside the window as well. */
  @Test
  void aRouteIsDeclinedWhenTheRootCannotNameTheBit() {
    try {
      assertFalse(routeCanName(0, nodeFar(), nodeNear(), nodeNear()), "the root cannot name the new byte");
    } finally {
      close();
    }
  }

  /**
   * The window ends one below the insertion depth: a node deeper than that is never extended by a
   * branch insert at that depth, so its mask is not this question's business.
   */
  @Test
  void aNodeDeeperThanTheWindowIsNotAsked() {
    try {
      assertTrue(routeCanName(0, nodeNear(), nodeNear(), nodeFar()),
          "the node two below the insert is outside the window a branch insert may extend");
    } finally {
      close();
    }
  }

  /** A bit the node already names leaves its mask unchanged, so its span cannot grow. */
  @Test
  void aNodeThatAlreadyNamesTheBitIsAccepted() {
    try {
      assertTrue(routeCanName(1, nodeNaming(LATE_BYTE - 3, LATE_BYTE), nodeNear(), nodeNear()),
          "the new bit is one of this node's own, so nothing joins its mask");
    } finally {
      close();
    }
  }

  @Test
  void aBlockOfOneNodeRoutesEachPartsKeyToThatPart() {
    try {
      final byte[] zero = keyWithBits(0, 0);
      final byte[] one = keyWithBits(1, 0);
      final PageReference[] parts = {leafRef(zero), leafRef(one)};
      final HOTIndirectPage block = HOTBulkBuilder.assembleIndirect(new int[] {bitOf(10)}, new int[] {0, 1}, parts, 1,
          1, IndexType.NAME, pageKeys::getAndIncrement);
      assertTrue(AbstractHOTIndexWriter.blockRoutesToPart(block, zero, parts[0]));
      assertTrue(AbstractHOTIndexWriter.blockRoutesToPart(block, one, parts[1]));
      assertFalse(AbstractHOTIndexWriter.blockRoutesToPart(block, zero, parts[1]),
          "the key routes to the other part, so this part's canonical path is not the one assembled");
    } finally {
      close();
    }
  }

  /**
   * A block whose bits no single node can name is assembled over levels, so the walk has to follow
   * the route through every level it built before it can say the part was reached.
   */
  @Test
  void aBlockAssembledOverLevelsRoutesEachPartsKeyThroughItsLevels() {
    try {
      final byte[][] keys = new byte[4][];
      final PageReference[] parts = new PageReference[keys.length];
      for (int index = 0; index < keys.length; index++) {
        keys[index] = lateKey((index & 2) == 0
            ? 0
            : 1, index & 1);
        parts[index] = leafRef(keys[index]);
      }
      final HOTIndirectPage block = HOTBulkBuilder.assembleIndirect(new int[] {bitOf(10), LATE_BIT},
          new int[] {0, 1, 2, 3}, parts, 1, 1, IndexType.NAME, pageKeys::getAndIncrement);
      assertTrue(block.getHeight() > 1, "the fixture must really be a nest, or this asserts one level");
      for (int index = 0; index < keys.length; index++) {
        assertTrue(AbstractHOTIndexWriter.blockRoutesToPart(block, keys[index], parts[index]),
            "part " + index + " must be reached through the levels of its own block");
      }
      assertFalse(AbstractHOTIndexWriter.blockRoutesToPart(block, keys[0], parts[3]),
          "a route that ends under another level is not this part's route");
      assertFalse(AbstractHOTIndexWriter.blockRoutesToPart(block, keys[0], parts[1]),
          "reaching the right level is not reaching the right part");
    } finally {
      close();
    }
  }

  /**
   * A key no child's partial subsumes is not routed by the block at all. Routing answers
   * {@code NOT_FOUND} there, which is not a child to compare against the part.
   */
  @Test
  void aKeyNoChildSubsumesReachesNoPart() {
    try {
      // Both columns branch and neither child carries the all-zero partial, so a key with both bits
      // clear is subsumed by no child.
      final PageReference[] parts = {leafRef(keyWithBits(0, 1)), leafRef(keyWithBits(1, 0))};
      final HOTIndirectPage block = HOTBulkBuilder.assembleIndirect(new int[] {bitOf(10), bitOf(11)},
          new int[] {0b01, 0b10}, parts, 1, 1, IndexType.NAME, pageKeys::getAndIncrement);
      final byte[] unrouted = keyWithBits(0, 0);
      assertTrue(block.findChildIndex(unrouted, unrouted.length) < 0, "the fixture must really route nowhere");
      assertFalse(AbstractHOTIndexWriter.blockRoutesToPart(block, unrouted, parts[0]));
    } finally {
      close();
    }
  }

  private boolean routeCanName(final int insertDepth, final HOTIndirectPage... pathNodes) {
    final PageReference[] pathRefs = new PageReference[pathNodes.length];
    for (int depth = 0; depth < pathNodes.length; depth++) {
      pathRefs[depth] = new PageReference();
      pathRefs[depth].setPage(pathNodes[depth]);
    }
    final HOTLeafPage leaf = new HOTLeafPage(pageKeys.getAndIncrement(), 1, IndexType.NAME);
    leaves.add(leaf);
    final PageReference leafRef = new PageReference();
    leafRef.setPage(leaf);
    final AbstractHOTIndexWriter.LeafNavigationResult route = new AbstractHOTIndexWriter.LeafNavigationResult(leaf,
        leafRef, pathNodes, pathRefs, new int[pathNodes.length], pathNodes.length);
    return AbstractHOTIndexWriter.routeCanNameBranchBit(route, insertDepth, LATE_BIT);
  }

  /** A node naming bytes a few before the late one: the late byte joins its eight-byte window. */
  private HOTIndirectPage nodeNear() {
    return nodeNaming(LATE_BYTE - 3, LATE_BYTE - 2);
  }

  /** A node naming early bytes: the late byte is neither inside its window nor nameable beside it. */
  private HOTIndirectPage nodeFar() {
    return nodeNaming(10, 11);
  }

  private HOTIndirectPage nodeNaming(final int firstByte, final int secondByte) {
    final byte[] low = baseKey();
    final byte[] high = baseKey();
    high[firstByte] = (byte) 0x80;
    high[secondByte] = (byte) 0x80;
    final PageReference[] children = {leafRef(low), leafRef(high)};
    return HOTBulkBuilder.assembleIndirect(new int[] {bitOf(firstByte), bitOf(secondByte)}, new int[] {0b00, 0b11},
        children, 1, 1, IndexType.NAME, pageKeys::getAndIncrement);
  }

  private PageReference leafRef(final byte[] key) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKeys.getAndIncrement(), 1, IndexType.NAME);
    assertTrue(leaf.put(key, VALUE), "the fixture leaf must accept its key");
    leaves.add(leaf);
    final PageReference reference = new PageReference();
    reference.setPage(leaf);
    return reference;
  }

  private static int bitOf(final int keyByte) {
    return keyByte * Byte.SIZE;
  }

  /** A key whose leading bits in bytes ten and eleven are set as asked. */
  private static byte[] keyWithBits(final int tenth, final int eleventh) {
    final byte[] key = baseKey();
    key[10] = highBit(tenth);
    key[11] = highBit(eleventh);
    return key;
  }

  /** A key whose leading bits in byte ten and in the late byte are set as asked. */
  private static byte[] lateKey(final int tenth, final int late) {
    final byte[] key = baseKey();
    key[10] = highBit(tenth);
    key[LATE_BYTE] = highBit(late);
    return key;
  }

  private static byte[] baseKey() {
    final byte[] key = new byte[LATE_BYTE + 2];
    Arrays.fill(key, (byte) 'k');
    for (final int branchByte : BRANCH_BYTES) {
      key[branchByte] = 0x00;
    }
    return key;
  }

  private static byte highBit(final int set) {
    return set == 0
        ? 0x00
        : (byte) 0x80;
  }

  private void close() {
    for (final HOTLeafPage leaf : leaves) {
      leaf.close();
    }
    leaves.clear();
  }
}
