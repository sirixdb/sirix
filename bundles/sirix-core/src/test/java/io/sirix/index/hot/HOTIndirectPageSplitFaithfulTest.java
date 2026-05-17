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
import java.util.Arrays;
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
 * Verification of {@link HOTIncrementalInsert#splitIndirect} and
 * {@link HOTIncrementalInsert#addEntry} — step 2 of the faithful incremental port
 * ({@code docs/HOT_INCREMENTAL_PORT_PLAN.md} §3 step 2).
 *
 * <p>The two primitives are exercised over canonical tries produced by {@link HOTBulkBuilder}
 * (correct by {@code HOT_FORMAL_FOUNDATION.md} Theorem 1). Each result is checked two ways:
 * <ul>
 *   <li><b>structurally</b> — {@link HOTMalformedSubtreeDetector} reports zero malformed
 *       subtrees (I3 / I4 / I5 / I7 / I8 / I11);</li>
 *   <li><b>by routing</b> — every key PEXT-descends from the rebuilt root to a leaf that
 *       actually contains it, and the key multiset is exactly preserved (cross-check against
 *       the {@code HOTBulkBuilder} input — {@code HOT_INCREMENTAL_SPLIT_VERIFICATION.md}
 *       Theorem IV corollary).</li>
 * </ul>
 *
 * <p>Both the SingleMask layout (8-byte keys) and the MultiMask layout (40-byte keys, disc
 * bits spanning &gt; 8 bytes) are covered — asserted explicitly so a coverage regression fails.
 */
@DisplayName("HOTIncrementalInsert.splitIndirect / addEntry — faithful indirect-node split")
final class HOTIndirectPageSplitFaithfulTest {

  /** Resolver for in-memory (swizzled) pages. */
  private static final PageResolver RESOLVER = PageReference::getPage;

  /** A non-tombstone {@code NodeReferences} value (node key {@code 7}) shared by every entry. */
  private static final byte[] VALUE = nodeRef(7L);

  // ======================================================================
  // splitIndirect.
  // ======================================================================

  @Test
  @DisplayName("splitting a canonical indirect root yields two clean, correctly-routing halves")
  void splitIndirectProducesCleanHalves() {
    int checked = 0;
    boolean sawSingleMask = false;
    boolean sawMultiMask = false;
    for (final Workload workload : Workload.values()) {
      for (final int size : workload.sizes) {
        for (int seed = 0; seed < 3; seed++) {
          final AtomicLong allocator = new AtomicLong(1);
          final List<byte[]> keys = workload.generate(size, seed);
          final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(entries(keys), 1,
              IndexType.CAS, allocator::getAndIncrement);
          if (!(built.rootPage() instanceof HOTIndirectPage root)) {
            closeLeavesOf(built.rootPage());
            continue;
          }
          if (root.getLayoutType() == HOTIndirectPage.LayoutType.MULTI_MASK) {
            sawMultiMask = true;
          } else {
            sawSingleMask = true;
          }
          final String label = workload + " size=" + size + " seed=" + seed;

          final BiNode split = HOTIncrementalInsert.splitIndirect(root, 1,
              allocator::getAndIncrement);
          final HOTIndirectPage materialized = materialize(split, allocator);
          final PageReference rootRef = swizzle(materialized);

          assertClean(rootRef, label);
          assertRoutesAll(rootRef, keys, label);
          assertEquals(hexSet(keys), collectKeys(materialized),
              label + ": split must preserve the exact key set");

          closeLeavesOf(materialized);
          checked++;
        }
      }
    }
    assertTrue(sawSingleMask, "no SingleMask root exercised — coverage gap");
    assertTrue(sawMultiMask, "no MultiMask root exercised — coverage gap");
    System.out.println("[splitIndirect] " + checked + " canonical roots split — clean");
  }

  @Test
  @DisplayName("a half can itself be split — stale disc bits are dropped across generations")
  void splitIndirectIsRepeatable() {
    int checked = 0;
    for (final Workload workload : Workload.values()) {
      final AtomicLong allocator = new AtomicLong(1);
      final List<byte[]> keys = workload.generate(workload.sizes[workload.sizes.length - 1], 0);
      final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(entries(keys), 1,
          IndexType.CAS, allocator::getAndIncrement);
      if (!(built.rootPage() instanceof HOTIndirectPage root)) {
        closeLeavesOf(built.rootPage());
        continue;
      }
      final BiNode first = HOTIncrementalInsert.splitIndirect(root, 1, allocator::getAndIncrement);
      // Re-split whichever half is itself a multi-child compound node.
      for (final PageReference halfRef : List.of(first.left(), first.right())) {
        if (halfRef.getPage() instanceof HOTIndirectPage half && half.getNumChildren() >= 2) {
          final BiNode second = HOTIncrementalInsert.splitIndirect(half, 1,
              allocator::getAndIncrement);
          final HOTIndirectPage materialized = materialize(second, allocator);
          final PageReference rootRef = swizzle(materialized);
          final String label = workload + " re-split";
          assertClean(rootRef, label);
          assertRoutesAll(rootRef, collectKeyBytes(half), label);
          checked++;
        }
      }
      closeLeavesOf(materialize(first, allocator));
    }
    assertTrue(checked > 0, "no half was re-split — coverage gap");
    System.out.println("[splitIndirect] " + checked + " re-splits — clean");
  }

  // ======================================================================
  // addEntry.
  // ======================================================================

  @Test
  @DisplayName("addEntry folds a leaf-page split into a not-full node, preserving the key set")
  void addEntryIntegratesLeafSplit() {
    int checked = 0;
    boolean sawMultiMask = false;
    for (final Workload workload : Workload.values()) {
      for (final int size : new int[] {700, 5_000}) {
        final AtomicLong allocator = new AtomicLong(1);
        final List<byte[]> keys = workload.generate(size, 1);
        final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(entries(keys), 1,
            IndexType.CAS, allocator::getAndIncrement);
        if (!(built.rootPage() instanceof HOTIndirectPage root) || root.getHeight() != 1) {
          closeLeavesOf(built.rootPage());
          continue; // need a height-1 root so the halves' children are leaf pages
        }
        // splitIndirect produces a not-full compound node with leaf children — the addEntry target.
        final BiNode split = HOTIncrementalInsert.splitIndirect(root, 1,
            allocator::getAndIncrement);
        final PageReference targetRef = split.left().getPage() instanceof HOTIndirectPage
            ? split.left() : split.right();
        final PageReference orphanHalf = targetRef == split.left() ? split.right() : split.left();
        if (!(targetRef.getPage() instanceof HOTIndirectPage target)) {
          continue;
        }
        if (target.getLayoutType() == HOTIndirectPage.LayoutType.MULTI_MASK) {
          sawMultiMask = true;
        }
        final int slot = firstLeafChildWithEntries(target, 2);
        final HOTLeafPage leaf = (HOTLeafPage) target.getChildReference(slot).getPage();
        final TreeSet<String> before = collectKeys(target);

        // splitLeafPage with an existing key OR-merges values — the key set is unchanged.
        final BiNode leafSplit = HOTIncrementalInsert.splitLeafPage(leaf, leaf.getKey(0), VALUE, 1,
            IndexType.CAS, allocator::getAndIncrement);
        final HOTIndirectPage integrated = HOTIncrementalInsert.addEntry(target, leafSplit, slot,
            1, allocator::getAndIncrement);
        final PageReference rootRef = swizzle(integrated);
        final String label = workload + " size=" + size;

        assertEquals(target.getNumChildren() + 1, integrated.getNumChildren(),
            label + ": addEntry adds exactly one child");
        assertClean(rootRef, label);
        assertRoutesAll(rootRef, toByteList(before), label);
        assertEquals(before, collectKeys(integrated),
            label + ": addEntry of a duplicate-key leaf split must preserve the key set");

        closeAll(integrated, orphanHalf.getPage(), leaf);
        checked++;
      }
    }
    assertTrue(checked > 0, "no addEntry case ran — coverage gap");
    assertTrue(sawMultiMask, "no MultiMask addEntry target exercised — coverage gap");
    System.out.println("[addEntry] " + checked + " leaf-split integrations — clean");
  }

  @Test
  @DisplayName("addEntry accepts a brand-new in-range key — it routes to the rebuilt node")
  void addEntryAcceptsFreshKey() {
    int checked = 0;
    for (final int size : new int[] {700, 5_000, 12_000}) {
      final AtomicLong allocator = new AtomicLong(1);
      final List<byte[]> keys = Workload.RANDOM8.generate(size, 2);
      final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(entries(keys), 1,
          IndexType.CAS, allocator::getAndIncrement);
      if (!(built.rootPage() instanceof HOTIndirectPage root) || root.getHeight() != 1) {
        closeLeavesOf(built.rootPage());
        continue;
      }
      final BiNode split = HOTIncrementalInsert.splitIndirect(root, 1, allocator::getAndIncrement);
      final PageReference targetRef = split.left().getPage() instanceof HOTIndirectPage
          ? split.left() : split.right();
      final PageReference orphanHalf = targetRef == split.left() ? split.right() : split.left();
      if (!(targetRef.getPage() instanceof HOTIndirectPage target)) {
        continue;
      }
      final int slot = firstLeafChildWithEntries(target, 2);
      final HOTLeafPage leaf = (HOTLeafPage) target.getChildReference(slot).getPage();
      final byte[] freshKey = freshKeyInside(leaf);
      if (freshKey == null) {
        closeAll(materialize(split, allocator));
        continue;
      }
      final TreeSet<String> expected = collectKeys(target);
      expected.add(hex(freshKey));

      final BiNode leafSplit = HOTIncrementalInsert.splitLeafPage(leaf, freshKey, VALUE, 1,
          IndexType.CAS, allocator::getAndIncrement);
      final HOTIndirectPage integrated = HOTIncrementalInsert.addEntry(target, leafSplit, slot, 1,
          allocator::getAndIncrement);
      final PageReference rootRef = swizzle(integrated);
      final String label = "fresh-key size=" + size;

      assertClean(rootRef, label);
      assertEquals(expected, collectKeys(integrated),
          label + ": the new key joins the node's key set");
      assertRoutesAll(rootRef, toByteList(expected), label);

      closeAll(integrated, orphanHalf.getPage(), leaf);
      checked++;
    }
    assertTrue(checked > 0, "no fresh-key addEntry case ran — coverage gap");
    System.out.println("[addEntry] " + checked + " fresh-key integrations — clean");
  }

  // ======================================================================
  // Key workloads.
  // ======================================================================

  /** Adversarial key workloads — chosen to exercise both the SingleMask and MultiMask layouts. */
  private enum Workload {
    /** 8-byte big-endian {@code 0..n-1} — disc bits in the low bytes, SingleMask. */
    ASCENDING8(new int[] {600, 5_000, 20_000}),
    /** 8-byte big-endian random longs — disc bits across all 8 bytes, SingleMask. */
    RANDOM8(new int[] {600, 5_000, 20_000}),
    /**
     * 40-byte keys: an 8-value byte-0 prefix plus a far-out random tail at bytes 30-39 (bytes
     * 1-29 are zero). The root block exhausts the 7 byte-0 BiNodes then must reach the byte-30
     * tail to fill its 32 children — disc bits span &gt; 8 bytes, forcing the MultiMask layout.
     */
    WIDE_SPAN(new int[] {700, 4_000});

    private final int[] sizes;

    Workload(final int[] sizes) {
      this.sizes = sizes;
    }

    List<byte[]> generate(final int size, final int seed) {
      final Random random = new Random((ordinal() * 0x9E3779B9L) ^ (size * 131L) ^ seed);
      return switch (this) {
        case ASCENDING8 -> {
          final List<byte[]> keys = new ArrayList<>(size);
          for (long i = 0; i < size; i++) {
            keys.add(beKey(i));
          }
          yield keys; // already strictly ascending
        }
        case RANDOM8 -> {
          final TreeSet<Long> set = new TreeSet<>(Long::compareUnsigned);
          while (set.size() < size) {
            set.add(random.nextLong());
          }
          final List<byte[]> keys = new ArrayList<>(size);
          for (final long k : set) {
            keys.add(beKey(k));
          }
          yield keys;
        }
        case WIDE_SPAN -> {
          final TreeSet<byte[]> set = new TreeSet<>(Arrays::compareUnsigned);
          while (set.size() < size) {
            final byte[] key = new byte[40];
            key[0] = (byte) random.nextInt(8);          // 8 top-level byte-0 groups
            for (int i = 30; i < 40; i++) {             // far-out random tail; bytes 1-29 zero
              key[i] = (byte) random.nextInt(256);
            }
            set.add(key);
          }
          yield new ArrayList<>(set);
        }
      };
    }
  }

  // ======================================================================
  // Verification helpers.
  // ======================================================================

  /** Assert the trie rooted at {@code rootRef} has zero malformed subtrees. */
  private static void assertClean(final PageReference rootRef, final String label) {
    final List<MalformedSubtree> malformed = HOTMalformedSubtreeDetector.detect(rootRef, RESOLVER);
    assertTrue(malformed.isEmpty(), label + ": malformed subtree(s) " + malformed);
  }

  /** Assert every key PEXT-descends from {@code rootRef} to a leaf that contains it. */
  private static void assertRoutesAll(final PageReference rootRef, final List<byte[]> keys,
      final String label) {
    for (final byte[] key : keys) {
      Page page = rootRef.getPage();
      int depth = 0;
      while (page instanceof HOTIndirectPage indirect) {
        if (++depth > 64) {
          fail(label + ": descent for " + hex(key) + " exceeded depth 64");
        }
        final int childIndex = indirect.findChildIndex(key);
        assertTrue(childIndex >= 0,
            label + ": routing returned NOT_FOUND for " + hex(key));
        page = indirect.getChildReference(childIndex).getPage();
      }
      assertTrue(page instanceof HOTLeafPage,
          label + ": descent for " + hex(key) + " did not reach a leaf");
      final HOTLeafPage leaf = (HOTLeafPage) page;
      assertTrue(leaf.findEntry(key) >= 0,
          label + ": " + hex(key) + " routed to leaf " + leaf.getPageKey() + " without it");
    }
  }

  /** Materialize a virtual {@link BiNode} as a standalone 2-entry compound node. */
  private static HOTIndirectPage materialize(final BiNode biNode, final AtomicLong allocator) {
    return HOTIndirectPage.createBiNode(allocator.getAndIncrement(), 1,
        biNode.discriminativeBitIndex(), biNode.left(), biNode.right(), biNode.height());
  }

  /** Index of the first child of {@code node} that is a leaf page with {@code >= min} entries. */
  private static int firstLeafChildWithEntries(final HOTIndirectPage node, final int min) {
    for (int i = 0; i < node.getNumChildren(); i++) {
      if (node.getChildReference(i).getPage() instanceof HOTLeafPage leaf
          && leaf.getEntryCount() >= min) {
        return i;
      }
    }
    throw new IllegalStateException("no leaf child with >= " + min + " entries");
  }

  /** A key strictly between two adjacent keys of {@code leaf}, or {@code null} if none exists. */
  private static byte[] freshKeyInside(final HOTLeafPage leaf) {
    for (int i = 1; i < leaf.getEntryCount(); i++) {
      final long lo = decodeBe(leaf.getKey(i - 1));
      final long hi = decodeBe(leaf.getKey(i));
      final long mid = lo + ((hi - lo) >>> 1);
      if (Long.compareUnsigned(lo, mid) < 0 && Long.compareUnsigned(mid, hi) < 0) {
        return beKey(mid);
      }
    }
    return null;
  }

  // ======================================================================
  // Key / page utilities.
  // ======================================================================

  private static List<HOTBulkBuilder.Entry> entries(final List<byte[]> sortedKeys) {
    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(sortedKeys.size());
    for (final byte[] key : sortedKeys) {
      entries.add(new HOTBulkBuilder.Entry(key, VALUE));
    }
    return entries;
  }

  private static TreeSet<String> collectKeys(final Page page) {
    final TreeSet<String> keys = new TreeSet<>();
    collectKeysInto(page, keys);
    return keys;
  }

  private static void collectKeysInto(final Page page, final TreeSet<String> out) {
    if (page instanceof HOTLeafPage leaf) {
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        out.add(hex(leaf.getKey(i)));
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        collectKeysInto(indirect.getChildReference(i).getPage(), out);
      }
    }
  }

  private static List<byte[]> collectKeyBytes(final Page page) {
    final List<byte[]> keys = new ArrayList<>();
    collectKeyBytesInto(page, keys);
    return keys;
  }

  private static void collectKeyBytesInto(final Page page, final List<byte[]> out) {
    if (page instanceof HOTLeafPage leaf) {
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        out.add(leaf.getKey(i));
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        collectKeyBytesInto(indirect.getChildReference(i).getPage(), out);
      }
    }
  }

  private static TreeSet<String> hexSet(final List<byte[]> keys) {
    final TreeSet<String> set = new TreeSet<>();
    for (final byte[] key : keys) {
      set.add(hex(key));
    }
    return set;
  }

  private static List<byte[]> toByteList(final Set<String> hexKeys) {
    final List<byte[]> keys = new ArrayList<>(hexKeys.size());
    for (final String h : hexKeys) {
      keys.add(HexFormat.of().parseHex(h));
    }
    return keys;
  }

  /** Close every leaf page reachable from each root, each exactly once (identity-deduplicated). */
  private static void closeAll(final Page... roots) {
    final Set<HOTLeafPage> leaves = Collections.newSetFromMap(new IdentityHashMap<>());
    for (final Page root : roots) {
      collectLeafPages(root, leaves);
    }
    for (final HOTLeafPage leaf : leaves) {
      leaf.close();
    }
  }

  private static void closeLeavesOf(final Page page) {
    closeAll(page);
  }

  private static void collectLeafPages(final Page page, final Set<HOTLeafPage> out) {
    if (page instanceof HOTLeafPage leaf) {
      out.add(leaf);
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference reference = indirect.getChildReference(i);
        if (reference != null && reference.getPage() != null) {
          collectLeafPages(reference.getPage(), out);
        }
      }
    }
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

  private static long decodeBe(final byte[] key) {
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value = (value << 8) | (key[i] & 0xFF);
    }
    return value;
  }

  private static PageReference swizzle(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  private static String hex(final byte[] bytes) {
    return HexFormat.of().formatHex(bytes);
  }
}
