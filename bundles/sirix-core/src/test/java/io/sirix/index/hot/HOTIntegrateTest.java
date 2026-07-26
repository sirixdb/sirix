/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIncrementalInsert.BiNode;
import io.sirix.index.hot.HOTMalformedSubtreeDetector.MalformedSubtree;
import io.sirix.index.hot.HOTMalformedSubtreeDetector.PageResolver;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verification of {@link HOTIncrementalInsert#integrate} — step 4 of the faithful incremental
 * port ({@code docs/HOT_INCREMENTAL_PORT_PLAN.md} §3 step 4): the spine integration of a
 * {@link BiNode}, including the capacity cascade.
 *
 * <p>Each scenario builds a canonical trie with {@link HOTBulkBuilder}, overflows one leaf via
 * {@link HOTIncrementalInsert#splitLeafPage} (re-using an existing key, so the key multiset is
 * preserved), integrates the resulting {@code BiNode}, and checks the rebuilt trie two ways:
 * structurally clean ({@link HOTMalformedSubtreeDetector} reports nothing) and routing-correct
 * (every key still PEXT-descends to a leaf that contains it). The four integration cases —
 * direct new root, addEntry into a not-full node, single-level cascade, and a multi-level
 * cascade — are each exercised.
 */
@DisplayName("HOTIncrementalInsert.integrate — spine integration + capacity cascade")
final class HOTIntegrateTest {

  private static final PageResolver RESOLVER = PageReference::getPage;
  private static final byte[] VALUE = nodeRef(7L);

  @Test
  @DisplayName("a leaf-root index: integrate at depth 0 installs a fresh 2-entry root")
  void integratesDirectNewRoot() {
    final AtomicLong allocator = new AtomicLong(1);
    final List<Long> keys = randomKeys(200, 11L);
    final HOTBulkBuilder.BuildResult built = build(keys, allocator);
    assertTrue(built.rootPage() instanceof HOTLeafPage, "200 keys must fit a single leaf page");

    final HOTLeafPage rootLeaf = (HOTLeafPage) built.rootPage();
    final BiNode biNode = HOTIncrementalInsert.splitLeafPage(rootLeaf, rootLeaf.getKey(0), VALUE,
        1, IndexType.CAS, allocator::getAndIncrement);
    final PageReference rootRef = built.rootReference();
    final PageReference newRoot = HOTIncrementalInsert.integrate(new HOTIndirectPage[0],
        new PageReference[] {rootRef}, new int[0], 0, biNode, 1, allocator::getAndIncrement)
        .rootRef();

    assertSame(rootRef, newRoot, "depth-0 integration re-points the index-root reference");
    assertCleanAndRoutes(newRoot, keys, "direct-new-root");
    closeAll(newRoot.getPage(), rootLeaf);
    System.out.println("[integrate] direct new root — clean");
  }

  @Test
  @DisplayName("a not-full node: integrate folds the BiNode in via addEntry")
  void integratesViaAddEntry() {
    final AtomicLong allocator = new AtomicLong(1);
    final List<Long> keys = randomKeys(5_000, 22L);
    final HOTBulkBuilder.BuildResult built = build(keys, allocator);
    final HOTIndirectPage root = (HOTIndirectPage) built.rootPage();

    // splitIndirect yields a not-full compound node with leaf children — the addEntry target.
    final BiNode rootSplit = HOTIncrementalInsert.splitIndirect(root, 1,
        allocator::getAndIncrement);
    final PageReference halfRef = rootSplit.left().getPage() instanceof HOTIndirectPage
        ? rootSplit.left() : rootSplit.right();
    final PageReference orphanHalfRef = halfRef == rootSplit.left()
        ? rootSplit.right() : rootSplit.left();
    final HOTIndirectPage half = (HOTIndirectPage) halfRef.getPage();
    assertTrue(half.getNumChildren() < HOTIndirectPage.MAX_NODE_ENTRIES, "half is not full");

    final int slot = firstMultiEntryLeafSlot(half);
    assertTrue(slot >= 0, "a splitIndirect half must have a splittable leaf child");
    final HOTLeafPage leaf = (HOTLeafPage) half.getChildReference(slot).getPage();
    final TreeSet<String> subtreeKeys = collectKeys(half);

    final BiNode biNode = HOTIncrementalInsert.splitLeafPage(leaf, leaf.getKey(0), VALUE, 1,
        IndexType.CAS, allocator::getAndIncrement);
    // integrate folds the BiNode in via addEntry — a clean canonical fold.
    final PageReference newRoot = HOTIncrementalInsert.integrate(new HOTIndirectPage[] {half},
        new PageReference[] {halfRef, half.getChildReference(slot)},
        new int[] {slot}, 1, biNode, 1, allocator::getAndIncrement).rootRef();
    assertSame(halfRef, newRoot, "addEntry re-points the parent's reference, the root reference");
    assertCleanAndRoutesKeys(newRoot, subtreeKeys, "addEntry");
    assertEquals(subtreeKeys, collectKeys(newRoot.getPage()), "addEntry preserves the key set");
    closeAll(newRoot.getPage(), orphanHalfRef.getPage(), leaf);
    System.out.println("[integrate] addEntry into a not-full node — clean");
  }

  @Test
  @DisplayName("a full root: integrate cascades — split the root, integrate, grow a new root")
  void integratesViaCascadeToNewRoot() {
    final AtomicLong allocator = new AtomicLong(1);
    final List<Long> keys = randomKeys(5_000, 33L);
    final HOTBulkBuilder.BuildResult built = build(keys, allocator);
    final HOTIndirectPage root = (HOTIndirectPage) built.rootPage();
    assertEquals(HOTIndirectPage.MAX_NODE_ENTRIES, root.getNumChildren(), "root is full");
    assertEquals(1, root.getHeight(), "root is height 1 (leaf children)");

    final int slot = firstMultiEntryLeafSlot(root);
    assertTrue(slot >= 0, "a full height-1 root must have a splittable leaf child");
    final HOTLeafPage leaf = (HOTLeafPage) root.getChildReference(slot).getPage();
    final BiNode biNode = HOTIncrementalInsert.splitLeafPage(leaf, leaf.getKey(0),
        VALUE, 1, IndexType.CAS, allocator::getAndIncrement);
    // The full root cascades — splitIndirect + addEntry into a half + a fresh root.
    final PageReference newRoot = HOTIncrementalInsert.integrate(new HOTIndirectPage[] {root},
        new PageReference[] {built.rootReference(), root.getChildReference(slot)},
        new int[] {slot}, 1, biNode, 1, allocator::getAndIncrement).rootRef();
    final HOTIndirectPage newRootPage = (HOTIndirectPage) newRoot.getPage();
    assertEquals(2, newRootPage.getNumChildren(), "the cascade grows a fresh 2-entry root");
    assertEquals(root.getHeight() + 1, newRootPage.getHeight(), "the trie height grows by one");
    assertCleanAndRoutes(newRoot, keys, "cascade-new-root");
    assertEquals(hexSet(keys), collectKeys(newRoot.getPage()), "the cascade preserves the keys");
    closeAll(newRoot.getPage());
    System.out.println("[integrate] cascade to a new root — clean");
  }

  @Test
  @DisplayName("a two-level full trie: the cascade propagates mid -> root -> new root")
  void integratesViaMultiLevelCascade() {
    final AtomicLong allocator = new AtomicLong(1);
    final List<Long> keys = randomKeys(50_000, 44L);
    final HOTBulkBuilder.BuildResult built = build(keys, allocator);
    final HOTIndirectPage root = (HOTIndirectPage) built.rootPage();
    assertEquals(2, root.getHeight(), "50k keys must yield a height-2 root");

    assertEquals(HOTIndirectPage.MAX_NODE_ENTRIES, root.getNumChildren(), "root is full");
    // Pick a full mid child and a splittable leaf inside it, so the cascade propagates
    // mid -> root -> new root (a not-full mid would absorb the BiNode without cascading).
    int midSlot = -1;
    int leafSlot = -1;
    for (int ms = 0; ms < root.getNumChildren() && leafSlot < 0; ms++) {
      if (root.getChildReference(ms).getPage() instanceof HOTIndirectPage mid
          && mid.getNumChildren() == HOTIndirectPage.MAX_NODE_ENTRIES) {
        final int ls = firstMultiEntryLeafSlot(mid);
        if (ls >= 0) {
          midSlot = ms;
          leafSlot = ls;
        }
      }
    }
    assertTrue(leafSlot >= 0, "a height-2 full trie must have a full mid with a splittable leaf");
    final HOTIndirectPage mid = (HOTIndirectPage) root.getChildReference(midSlot).getPage();
    final HOTLeafPage leaf = (HOTLeafPage) mid.getChildReference(leafSlot).getPage();

    final BiNode biNode = HOTIncrementalInsert.splitLeafPage(leaf, leaf.getKey(0),
        VALUE, 1, IndexType.CAS, allocator::getAndIncrement);
    // The cascade folds the leaf split through the full mid and the full root via addEntry at
    // each level — a clean two-level cascade.
    final PageReference newRoot = HOTIncrementalInsert.integrate(
        new HOTIndirectPage[] {root, mid},
        new PageReference[] {built.rootReference(), root.getChildReference(midSlot),
            mid.getChildReference(leafSlot)},
        new int[] {midSlot, leafSlot}, 2, biNode, 1, allocator::getAndIncrement).rootRef();
    assertEquals(root.getHeight() + 1, ((HOTIndirectPage) newRoot.getPage()).getHeight(),
        "a full-path cascade grows the height by exactly one");
    assertCleanAndRoutes(newRoot, keys, "multi-level-cascade");
    assertEquals(hexSet(keys), collectKeys(newRoot.getPage()), "the cascade preserves the keys");
    closeAll(newRoot.getPage());
    System.out.println("[integrate] multi-level cascade — clean");
  }

  // ======================================================================
  // Verification helpers.
  // ======================================================================

  private static void assertCleanAndRoutes(final PageReference root, final List<Long> keys,
      final String label) {
    final List<MalformedSubtree> malformed = HOTMalformedSubtreeDetector.detect(root, RESOLVER);
    assertTrue(malformed.isEmpty(), label + ": malformed subtree(s) " + malformed);
    for (final long key : keys) {
      assertRoutes(root, beKey(key), label);
    }
  }

  private static void assertCleanAndRoutesKeys(final PageReference root,
      final TreeSet<String> hexKeys, final String label) {
    final List<MalformedSubtree> malformed = HOTMalformedSubtreeDetector.detect(root, RESOLVER);
    assertTrue(malformed.isEmpty(), label + ": malformed subtree(s) " + malformed);
    for (final String hex : hexKeys) {
      assertRoutes(root, HexFormat.of().parseHex(hex), label);
    }
  }

  private static void assertRoutes(final PageReference root, final byte[] key,
      final String label) {
    Page page = root.getPage();
    int depth = 0;
    while (page instanceof HOTIndirectPage indirect) {
      if (++depth > 64) {
        fail(label + ": descent for " + HexFormat.of().formatHex(key) + " exceeded depth 64");
      }
      final int childIndex = indirect.findChildIndex(key);
      assertTrue(childIndex >= 0, label + ": routing NOT_FOUND for "
          + HexFormat.of().formatHex(key));
      page = indirect.getChildReference(childIndex).getPage();
    }
    assertTrue(page instanceof HOTLeafPage, label + ": descent did not reach a leaf");
    assertTrue(((HOTLeafPage) page).findEntry(key) >= 0,
        label + ": " + HexFormat.of().formatHex(key) + " routed to a leaf without it");
  }

  // ======================================================================
  // Leaf selection + page / key utilities.
  // ======================================================================

  /** The first slot of a {@code >= 2}-entry (so splittable) leaf child of {@code node}, or
   *  {@code -1} if none. */
  private static int firstMultiEntryLeafSlot(final HOTIndirectPage node) {
    for (int i = 0; i < node.getNumChildren(); i++) {
      if (node.getChildReference(i).getPage() instanceof HOTLeafPage leaf
          && leaf.getEntryCount() >= 2) {
        return i;
      }
    }
    return -1;
  }

  private static HOTBulkBuilder.BuildResult build(final List<Long> sortedKeys,
      final AtomicLong allocator) {
    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(sortedKeys.size());
    for (final long key : sortedKeys) {
      entries.add(new HOTBulkBuilder.Entry(beKey(key), VALUE));
    }
    return HOTBulkBuilder.build(entries, 1, IndexType.CAS, allocator::getAndIncrement);
  }

  private static List<Long> randomKeys(final int count, final long seed) {
    final TreeSet<Long> set = new TreeSet<>(Long::compareUnsigned);
    final Random random = new Random(seed);
    while (set.size() < count) {
      set.add(random.nextLong());
    }
    return new ArrayList<>(set);
  }

  private static TreeSet<String> collectKeys(final Page page) {
    final TreeSet<String> keys = new TreeSet<>();
    collectKeysInto(page, keys);
    return keys;
  }

  private static void collectKeysInto(final Page page, final TreeSet<String> out) {
    if (page instanceof HOTLeafPage leaf) {
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        out.add(HexFormat.of().formatHex(leaf.getKey(i)));
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        collectKeysInto(indirect.getChildReference(i).getPage(), out);
      }
    }
  }

  private static TreeSet<String> hexSet(final List<Long> keys) {
    final TreeSet<String> set = new TreeSet<>();
    for (final long key : keys) {
      set.add(HexFormat.of().formatHex(beKey(key)));
    }
    return set;
  }

  private static void closeAll(final Page... roots) {
    final Set<HOTLeafPage> leaves = Collections.newSetFromMap(new IdentityHashMap<>());
    for (final Page root : roots) {
      collectLeaves(root, leaves);
    }
    for (final HOTLeafPage leaf : leaves) {
      leaf.close();
    }
  }

  private static void collectLeaves(final Page page, final Set<HOTLeafPage> out) {
    if (page instanceof HOTLeafPage leaf) {
      out.add(leaf);
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference reference = indirect.getChildReference(i);
        if (reference != null && reference.getPage() != null) {
          collectLeaves(reference.getPage(), out);
        }
      }
    }
  }

  private static void assertSame(final PageReference expected, final PageReference actual,
      final String message) {
    assertTrue(expected == actual, message);
  }

  private static byte[] nodeRef(final long bit) {
    final NodeReferences references = new NodeReferences();
    references.getNodeKeys().add(bit);
    final byte[] out = new byte[NodeReferencesSerializer.computeSerializedSize(references)];
    NodeReferencesSerializer.serialize(references, out, 0);
    return out;
  }

  private static byte[] beKey(final long value) {
    final byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) (value >>> (56 - 8 * i));
    }
    return bytes;
  }
}
