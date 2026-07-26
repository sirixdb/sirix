/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIncrementalInsert.DescentAnalysis;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verification of {@link HOTIncrementalInsert#analyzeDescent} — step 3 of the faithful
 * incremental port ({@code docs/HOT_INCREMENTAL_PORT_PLAN.md} §3 step 3): the post-processing
 * of Binna's {@code searchForInsert} that yields the mismatch bit β and the insert depth d*.
 *
 * <p>The descent path is walked over canonical tries built by {@link HOTBulkBuilder}. Each
 * analysis is checked against independently-recomputed properties:
 * <ul>
 *   <li><b>mismatch bit</b> — β must equal {@code msdb(key, residentKey)} <em>and</em> the
 *       maximum {@code msdb} over <em>every</em> key of the landed leaf (the resident is the
 *       longest-prefix neighbour — analyzed from two keys, here cross-checked against all);</li>
 *   <li><b>insert depth</b> — with the trie condition (I11) the path MSBs strictly increase, so
 *       d* must sit exactly at the crossover: {@code pathNodes[d*].MSB} more significant than β,
 *       {@code pathNodes[d*+1].MSB} not — and the affected child is the slot taken at d*.</li>
 * </ul>
 */
@DisplayName("HOTIncrementalInsert.analyzeDescent — mismatch bit + insert depth")
final class HOTDescentAnalysisTest {

  /** A non-tombstone {@code NodeReferences} value shared by every entry. */
  private static final byte[] VALUE = nodeRef(7L);

  @Test
  @DisplayName("a key already in the trie is reported present, with no mismatch bit or depth")
  void detectsExistingKeys() {
    final AtomicLong allocator = new AtomicLong(1);
    final List<Long> keys = randomKeys(8_000, 42L);
    final HOTBulkBuilder.BuildResult built = build(keys, allocator);
    assertTrue(built.rootPage() instanceof HOTIndirectPage, "8k keys must yield an indirect root");

    int checked = 0;
    for (int i = 0; i < keys.size(); i += 53) {
      final byte[] key = beKey(keys.get(i));
      final Descent descent = walk(built.rootReference(), key);
      final DescentAnalysis analysis = HOTIncrementalInsert.analyzeDescent(descent.pathNodes(),
          descent.pathChildIndices(), descent.pathDepth(), descent.leaf(), key);

      assertTrue(analysis.keyAlreadyPresent(), "an existing key must be reported present");
      assertEquals(-1, analysis.mismatchBit(), "a present key has no mismatch bit");
      assertEquals(-1, analysis.insertDepth(), "a present key has no insert depth");
      assertEquals(-1, analysis.affectedChildIndex(), "a present key has no affected child");
      checked++;
    }
    closeLeaves(built.rootPage());
    assertTrue(checked > 0, "no existing key sampled — coverage gap");
    System.out.println("[analyzeDescent] " + checked + " existing keys — present");
  }

  @Test
  @DisplayName("a fresh key: β is the longest-prefix neighbour's msdb; d* is the node owning β")
  void analyzesFreshKeys() {
    final AtomicLong allocator = new AtomicLong(1);
    final TreeSet<Long> keySet = new TreeSet<>(Long::compareUnsigned);
    final Random random = new Random(2026_05_17L);
    while (keySet.size() < 30_000) {
      keySet.add(random.nextLong());
    }
    final List<Long> keys = new ArrayList<>(keySet);
    final HOTBulkBuilder.BuildResult built = build(keys, allocator);
    assertTrue(built.rootPage() instanceof HOTIndirectPage, "30k keys must yield an indirect root");

    int checked = 0;
    int minDepth = Integer.MAX_VALUE;
    int maxDepth = Integer.MIN_VALUE;
    // Fresh keys = bit-flips of existing keys at a spread of absolute positions, so the
    // mismatch bit (hence the insert depth) lands at a range of tree levels.
    for (int i = 0; i < keys.size(); i += 311) {
      final long base = keys.get(i);
      for (final int absBit : new int[] {2, 9, 18, 27, 36, 45, 54, 62}) {
        final long flipped = base ^ (1L << (63 - absBit));
        if (keySet.contains(flipped)) {
          continue;
        }
        final int depth = checkFreshKey(built.rootReference(), beKey(flipped));
        minDepth = Math.min(minDepth, depth);
        maxDepth = Math.max(maxDepth, depth);
        checked++;
      }
    }
    closeLeaves(built.rootPage());
    assertTrue(checked > 0, "no fresh key analyzed — coverage gap");
    System.out.println("[analyzeDescent] " + checked + " fresh keys — clean; insert depth "
        + minDepth + ".." + maxDepth);
  }

  @Test
  @DisplayName("the insert-depth loop stops at the path node whose block owns β, at every level")
  void computesInsertDepthAtEveryLevel() {
    // A synthetic descent path with controlled MSBs — the trie condition (I11) requires them
    // strictly increasing down the path. analyzeDescent reads only getMostSignificantBitIndex().
    final int[] msbs = {4, 12, 20, 28};
    final HOTIndirectPage[] pathNodes = new HOTIndirectPage[msbs.length];
    for (int i = 0; i < msbs.length; i++) {
      pathNodes[i] = HOTIndirectPage.createBiNode(i + 1, 1, msbs[i],
          new PageReference(), new PageReference(), 1);
      assertEquals(msbs[i], pathNodes[i].getMostSignificantBitIndex(), "controlled path MSB");
    }
    final int[] pathChildIndices = {0, 1, 0, 1};

    // A one-entry leaf holding the all-zero key: a probe key whose only set bit is β then has
    // msdb(probe, 0) == β exactly, so the mismatch bit is fully controlled.
    final HOTLeafPage leaf = new HOTLeafPage(99L, 1, IndexType.CAS);
    leaf.put(beKey(0L), VALUE);

    // β just below / equal to / just above each path MSB — exercises the loop's `<` boundary.
    final int[] betas = {2, 8, 12, 13, 20, 21, 28, 29, 40};
    final int[] expectedInsertDepth = {0, 0, 0, 1, 1, 2, 2, 3, 3};
    for (int t = 0; t < betas.length; t++) {
      final int beta = betas[t];
      final byte[] probe = beKey(1L << (63 - beta)); // the key whose single set bit is β
      final DescentAnalysis analysis = HOTIncrementalInsert.analyzeDescent(pathNodes,
          pathChildIndices, pathNodes.length, leaf, probe);

      assertEquals(beta, analysis.mismatchBit(),
          "β must be msdb(probe, all-zero resident) for β=" + beta);
      assertEquals(expectedInsertDepth[t], analysis.insertDepth(),
          "insert depth for β=" + beta + " over path MSBs " + Arrays.toString(msbs));
      assertEquals(pathChildIndices[expectedInsertDepth[t]], analysis.affectedChildIndex(),
          "affected child is the slot at d* for β=" + beta);
    }
    leaf.close();
    System.out.println("[analyzeDescent] insert depth verified at d* = 0..3");
  }

  @Test
  @DisplayName("when the index root is a single leaf page the insert depth is -1")
  void handlesLeafRootIndex() {
    final AtomicLong allocator = new AtomicLong(1);
    final TreeSet<Long> keySet = new TreeSet<>(Long::compareUnsigned);
    final Random random = new Random(7L);
    while (keySet.size() < 100) {
      keySet.add(random.nextLong());
    }
    final List<Long> keys = new ArrayList<>(keySet);
    final HOTBulkBuilder.BuildResult built = build(keys, allocator);
    assertTrue(built.rootPage() instanceof HOTLeafPage, "100 keys must fit a single leaf page");

    long freshValue;
    do {
      freshValue = random.nextLong();
    } while (keySet.contains(freshValue));
    final byte[] fresh = beKey(freshValue);

    final Descent descent = walk(built.rootReference(), fresh);
    assertEquals(0, descent.pathDepth(), "a leaf-root index has no compound node on the path");
    final DescentAnalysis analysis = HOTIncrementalInsert.analyzeDescent(descent.pathNodes(),
        descent.pathChildIndices(), descent.pathDepth(), descent.leaf(), fresh);

    assertFalse(analysis.keyAlreadyPresent(), "the fresh key must not be reported present");
    assertEquals(-1, analysis.insertDepth(), "no compound node on the path ⇒ insert depth -1");
    assertEquals(-1, analysis.affectedChildIndex(), "no compound node ⇒ no affected child");
    assertNotNull(analysis.residentKey(), "a non-empty leaf yields a resident key");
    assertTrue(analysis.mismatchBit() >= 0, "a fresh key against a non-empty leaf has a β");
    assertEquals(analysis.mismatchBit(),
        HOTBulkBuilder.msdb(fresh, analysis.residentKey()), "β = msdb(key, residentKey)");

    closeLeaves(built.rootPage());
    System.out.println("[analyzeDescent] leaf-root index — insert depth -1");
  }

  // ======================================================================
  // Per-fresh-key property checks.
  // ======================================================================

  /** Analyze a fresh key's descent and assert every defining property; returns the insert depth. */
  private static int checkFreshKey(final PageReference rootRef, final byte[] key) {
    final Descent descent = walk(rootRef, key);
    final HOTIndirectPage[] pathNodes = descent.pathNodes();
    final int pathDepth = descent.pathDepth();
    assertTrue(pathDepth >= 1, "an indirect-root trie has at least one compound node on the path");

    // Precondition — the trie condition (I11): path MSBs strictly increase down the tree.
    for (int d = 1; d < pathDepth; d++) {
      assertTrue(pathNodes[d - 1].getMostSignificantBitIndex()
              < pathNodes[d].getMostSignificantBitIndex(),
          "canonical trie must satisfy I11 (MSBs strictly increase down the path)");
    }

    final DescentAnalysis analysis = HOTIncrementalInsert.analyzeDescent(pathNodes,
        descent.pathChildIndices(), pathDepth, descent.leaf(), key);
    assertFalse(analysis.keyAlreadyPresent(), "a fresh key must not be reported present");

    // (1) β = msdb(key, residentKey), and the resident is a key of the landed leaf.
    final byte[] resident = analysis.residentKey();
    assertNotNull(resident, "a fresh key against a non-empty leaf has a resident key");
    assertEquals(analysis.mismatchBit(), HOTBulkBuilder.msdb(key, resident),
        "the mismatch bit must be msdb(key, residentKey)");
    assertTrue(leafContains(descent.leaf(), resident),
        "the resident key must be an entry of the landed leaf");

    // (2) longest-prefix property: no leaf key shares a longer prefix than the resident.
    assertEquals(maxLeafMsdb(descent.leaf(), key), analysis.mismatchBit(),
        "β must be the maximum msdb over every key of the landed leaf");

    // (3) d* sits exactly at the β crossover (path MSBs increase, so this pins it uniquely).
    final int dStar = analysis.insertDepth();
    assertTrue(dStar >= 0 && dStar < pathDepth, "insert depth must be a valid path index");
    if (dStar >= 1) {
      assertTrue(pathNodes[dStar].getMostSignificantBitIndex() < analysis.mismatchBit(),
          "the node at d* must have an MSB more significant than β");
    }
    if (dStar + 1 < pathDepth) {
      assertTrue(pathNodes[dStar + 1].getMostSignificantBitIndex() >= analysis.mismatchBit(),
          "the node below d* must not out-rank β — d* cannot descend further");
    }

    // (4) the affected child is the slot the key descended through at d*.
    assertEquals(descent.pathChildIndices()[dStar], analysis.affectedChildIndex(),
        "the affected child index must be the slot taken at d*");
    return dStar;
  }

  // ======================================================================
  // Descent walk + helpers.
  // ======================================================================

  /** A walked descent path: the compound nodes, the slot chosen at each, and the landed leaf. */
  private record Descent(HOTIndirectPage[] pathNodes, int[] pathChildIndices, int pathDepth,
                         HOTLeafPage leaf) {}

  /** Walk {@code key} from {@code rootRef} to its leaf over a swizzled in-memory trie. */
  private static Descent walk(final PageReference rootRef, final byte[] key) {
    final List<HOTIndirectPage> nodes = new ArrayList<>();
    final List<Integer> slots = new ArrayList<>();
    Page page = rootRef.getPage();
    while (page instanceof HOTIndirectPage indirect) {
      int childIndex = indirect.findChildIndex(key);
      if (childIndex < 0) {
        childIndex = 0;
      }
      nodes.add(indirect);
      slots.add(childIndex);
      page = indirect.getChildReference(childIndex).getPage();
    }
    final HOTIndirectPage[] pathNodes = nodes.toArray(new HOTIndirectPage[0]);
    final int[] pathChildIndices = new int[slots.size()];
    for (int i = 0; i < slots.size(); i++) {
      pathChildIndices[i] = slots.get(i);
    }
    return new Descent(pathNodes, pathChildIndices, pathNodes.length, (HOTLeafPage) page);
  }

  /** The maximum {@code msdb(key, leafKey)} over every key of {@code leaf} (key is fresh). */
  private static int maxLeafMsdb(final HOTLeafPage leaf, final byte[] key) {
    int max = -1;
    for (int i = 0; i < leaf.getEntryCount(); i++) {
      max = Math.max(max, HOTBulkBuilder.msdb(key, leaf.getKey(i)));
    }
    return max;
  }

  private static boolean leafContains(final HOTLeafPage leaf, final byte[] key) {
    for (int i = 0; i < leaf.getEntryCount(); i++) {
      if (Arrays.equals(leaf.getKey(i), key)) {
        return true;
      }
    }
    return false;
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

  private static void closeLeaves(final Page page) {
    final Set<HOTLeafPage> leaves = new HashSet<>();
    collectLeaves(page, leaves);
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
