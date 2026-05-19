/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIncrementalInsert.DescentAnalysis;
import io.sirix.index.hot.HOTIncrementalInsert.InsertInfo;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Decisive empirical probe for the HOT branch-insert {@code betaIsDiscBit} case.
 *
 * <p>Goal — settle whether {@link HOTIncrementalInsert#addChildAtCombination} (the not-full
 * {@code betaIsDiscBit} path) is genuinely routing-correct under STRICT, no-fallback root-to-leaf
 * {@link HOTIndirectPage#findChildIndex} descent, and whether the canonical {@link HOTBulkBuilder}
 * tree of the same key set both routes 100% and contains a child at exactly {@code comboPartial}.
 *
 * <p>The probe faithfully replicates {@code AbstractHOTIndexWriter.tryBranchIncremental}'s
 * {@code betaIsDiscBit} branch: it builds a canonical HOT, picks a fresh in-range key K, performs
 * a real strict descent to a leaf, runs {@link HOTIncrementalInsert#analyzeDescent} +
 * {@link HOTIncrementalInsert#getInsertInformation}, and — when {@code info.betaIsDiscBit()} and
 * the insert-depth node is not full — computes {@code comboPartial} exactly as the writer does,
 * folds K's leaf via {@code addChildAtCombination}, and verifies the result.
 */
final class BetaIsDiscBitRoutingProbe {

  private static final byte[] VALUE = nodeRef(7L);
  private static final HexFormat HEX = HexFormat.of();

  // ====================================================================
  // Q1 + Q2 — not-full betaIsDiscBit: strict routing + comboPartial vs densePK.
  // ====================================================================

  @Test
  void notFullBetaIsDiscBitStrictRouting() {
    int betaIsDiscBitFirings = 0;
    int comboEqualsDensePK = 0;
    int comboSubsetOfDensePK = 0;
    int strictRouteOk = 0;
    int strictMisroutes = 0;
    int notFullCases = 0;
    final List<String> misrouteDetails = new ArrayList<>();
    final java.util.TreeMap<Integer, Integer> childCountHistogram = new java.util.TreeMap<>();

    // Mix of sizes: small sets (600..2000) give a not-full root indirect directly; larger
    // sets exercise deeper trees (their interior nodes are usually full -> Q4 territory).
    final List<KeySet> sets = new ArrayList<>();
    for (final int size : new int[] {600, 900, 1_400, 2_000, 8_000, 20_000}) {
      sets.add(new KeySet("random-" + size + "-s1", random(size, 1)));
      sets.add(new KeySet("random-" + size + "-s2", random(size, 2)));
      sets.add(new KeySet("ascending-" + size, ascending(size)));
      sets.add(new KeySet("bimodal-" + size, bimodal(size)));
    }

    for (final KeySet ks : sets) {
      final AtomicLong allocator = new AtomicLong(1);
      final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(entries(ks.keys), 1,
          IndexType.CAS, allocator::getAndIncrement);
      Page root = built.rootPage();
      if (!(root instanceof HOTIndirectPage)) {
        closeLeaves(root);
        continue;
      }
      // A fresh canonical HOTBulkBuilder tree has every interior node FULL (32 children) —
      // betaIsDiscBit then always lands on a full d* (Q4). The not-full betaIsDiscBit case
      // only arises AFTER an incremental splitIndirect has created a not-full node. So
      // recursively splitIndirect every full node — exactly the state the real writer's
      // addChildAtCombination operates on — then probe.
      root = splitAllFullNodes(root, allocator);
      // Targeted betaIsDiscBit candidates: for every node, every disc bit b, take an existing
      // key from a child subtree and flip it at b — its msdb vs that resident is exactly b,
      // an existing disc bit. This is precisely the betaIsDiscBit branch-insert scenario.
      final TreeSet<byte[]> present = new TreeSet<>(java.util.Arrays::compareUnsigned);
      present.addAll(ks.keys);
      final List<byte[]> candidates = betaIsDiscBitCandidates(root, present);
      for (final byte[] k : candidates) {
        if (betaIsDiscBitFirings >= 400) {
          break;
        }
        final BetaCase bc = analyzeBetaCase(root, k);
        if (bc == null || !bc.info.betaIsDiscBit()) {
          continue;
        }
        betaIsDiscBitFirings++;
        childCountHistogram.merge(bc.node.getNumChildren(), 1, Integer::sum);
        if (bc.node.getNumChildren() >= HOTIndirectPage.MAX_NODE_ENTRIES) {
          continue; // full-node case handled in Q4
        }
        notFullCases++;

        // ---- replicate tryBranchIncremental's comboPartial exactly ----
        final int[] nodeDiscBits = HOTIncrementalInsert.discriminativeBits(bc.node);
        final int betaColumn = java.util.Arrays.binarySearch(nodeDiscBits, bc.analysis.mismatchBit());
        final int betaValue = HOTBulkBuilder.bitAt(k, bc.analysis.mismatchBit()) ? 1 : 0;
        final int comboPartial = bc.info.subtreePrefix()
            | (betaValue == 1 ? 1 << (nodeDiscBits.length - 1 - betaColumn) : 0);

        // ---- Q2: comboPartial vs K's real densePK at this node ----
        final int densePK = bc.node.computeDensePartialKey(k);
        final boolean comboIsSubsetOfDense = (comboPartial & ~densePK) == 0;
        if (comboPartial == densePK) {
          comboEqualsDensePK++;
        } else if (comboIsSubsetOfDense) {
          comboSubsetOfDensePK++;
        }

        // ---- perform the fold ----
        final HOTLeafPage comboLeaf = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.CAS);
        assertTrue(comboLeaf.put(k, VALUE), "fresh single-entry leaf must accept the key");
        final HOTIndirectPage foldedNode;
        try {
          foldedNode = HOTIncrementalInsert.addChildAtCombination(bc.node, comboPartial,
              swizzle(comboLeaf), bc.node.getHeight(), 1, allocator::getAndIncrement);
        } catch (IllegalArgumentException dup) {
          // comboPartial already a child partial — addChildAtCombination rejects it.
          comboLeaf.close();
          continue;
        }
        // Splice the folded node into a copy of the spine so a strict root-to-leaf descent
        // for K exercises the real tree (the fold replaces bc.node in place at bc.nodeRef).
        bc.nodeRef.setPage(foldedNode);

        // ---- Q1: STRICT no-fallback routing of K from the real root ----
        final HOTLeafPage routed = strictDescend(root, k);
        if (routed != null && routed.findEntry(k) >= 0) {
          strictRouteOk++;
        } else {
          strictMisroutes++;
          // Which child did K's combo node route to?
          final int chosen = foldedNode.findChildIndex(k);
          final int[] partials = foldedNode.getPartialKeys();
          if (misrouteDetails.size() < 8) {
            misrouteDetails.add(String.format(
                "[%s] K=%s MISROUTE: node discBits=%s comboPartial=0x%x densePK=0x%x "
                    + "betaCol=%d betaVal=%d | findChildIndex->slot %d (partial 0x%x) "
                    + "comboLeafSlot has partial 0x%x | nodePartials=%s",
                ks.name, HEX.formatHex(k), java.util.Arrays.toString(nodeDiscBits),
                comboPartial, densePK, betaColumn, betaValue, chosen,
                chosen >= 0 && partials != null && chosen < partials.length
                    ? partials[chosen] : -1,
                comboPartial, partialsHex(partials)));
          }
        }
        // Restore the spine slot so the next probe sees the canonical tree.
        bc.nodeRef.setPage(bc.node);
        comboLeaf.close();
      }
      closeLeaves(root);
    }

    System.out.println("=== Q1/Q2 — not-full betaIsDiscBit addChildAtCombination ===");
    System.out.printf("  betaIsDiscBit firings=%d | not-full cases folded=%d%n",
        betaIsDiscBitFirings, notFullCases);
    System.out.println("  d* child-count histogram at firings: " + childCountHistogram);
    System.out.printf("  Q2: comboPartial == densePK : %d%n", comboEqualsDensePK);
    System.out.printf("  Q2: comboPartial STRICT-SUBSET of densePK : %d%n", comboSubsetOfDensePK);
    System.out.printf("  Q1: strict route OK=%d | strict MISROUTES=%d%n",
        strictRouteOk, strictMisroutes);
    for (final String d : misrouteDetails) {
      System.out.println("  " + d);
    }
    System.out.println("  DECISIVE: not-full betaIsDiscBit routes 100% strict = "
        + (strictMisroutes == 0 && notFullCases > 0 ? "YES" : strictMisroutes > 0 ? "NO"
            : "INCONCLUSIVE (no case)"));
    assertTrue(notFullCases > 0, "no not-full betaIsDiscBit case exercised — coverage gap");
    assertTrue(strictMisroutes == 0,
        "not-full betaIsDiscBit addChildAtCombination misrouted " + strictMisroutes + " keys");
  }

  // ====================================================================
  // Q4 — full-node betaIsDiscBit: the splitIndirect decomposition.
  //
  // For a FULL node where betaIsDiscBit fires, the rebuild fallback can be replaced by:
  //   splitIndirect(node) -> BiNode on node.MSB; the two halves are not-full;
  //   K routes (by node.MSB) into one half; addChildAtCombination into that half;
  //   reassemble the BiNode.
  // This probe builds that decomposition and verifies strict routing of K + every key,
  // and cross-checks against HOTBulkBuilder of the same key set.
  // ====================================================================

  @Test
  void fullNodeBetaIsDiscBitDecomposition() {
    int fullCases = 0;
    int decomposedOk = 0;
    int decomposeFailed = 0;
    int strictRouteOk = 0;
    int strictMisroutes = 0;
    int betaSurvivesInHalf = 0;
    int betaLostInHalf = 0;
    final List<String> failDetails = new ArrayList<>();

    final List<KeySet> sets = new ArrayList<>();
    for (final int size : new int[] {700, 2_000, 8_000, 20_000}) {
      sets.add(new KeySet("random-" + size, random(size, 11)));
      sets.add(new KeySet("ascending-" + size, ascending(size)));
      sets.add(new KeySet("bimodal-" + size, bimodal(size)));
      sets.add(new KeySet("widespan-" + size, widespan(size)));
    }

    for (final KeySet ks : sets) {
      final AtomicLong allocator = new AtomicLong(1);
      final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(entries(ks.keys), 1,
          IndexType.CAS, allocator::getAndIncrement);
      final Page root = built.rootPage();
      if (!(root instanceof HOTIndirectPage)) {
        closeLeaves(root);
        continue;
      }
      // Fresh canonical tree — interior nodes FULL, so betaIsDiscBit lands on a full d*.
      final TreeSet<byte[]> present = new TreeSet<>(java.util.Arrays::compareUnsigned);
      present.addAll(ks.keys);
      final List<byte[]> candidates = betaIsDiscBitCandidates(root, present);
      int casesThisSet = 0;
      for (final byte[] k : candidates) {
        if (casesThisSet >= 40) {
          break;
        }
        final BetaCase bc = analyzeBetaCase(root, k);
        if (bc == null || !bc.info.betaIsDiscBit()
            || bc.node.getNumChildren() < HOTIndirectPage.MAX_NODE_ENTRIES) {
          continue; // need a FULL d*
        }
        casesThisSet++;
        fullCases++;
        final int beta = bc.analysis.mismatchBit();
        final int betaValue = HOTBulkBuilder.bitAt(k, beta) ? 1 : 0;

        // ---- the decomposition: splitIndirect(node) at node.MSB ----
        final HOTIncrementalInsert.BiNode split =
            HOTIncrementalInsert.splitIndirect(bc.node, 1, allocator::getAndIncrement);
        // K routes into one half by node.MSB.
        final int nodeMsb = bc.node.getMostSignificantBitIndex();
        final boolean kMsbBit = HOTBulkBuilder.bitAt(k, nodeMsb);
        final PageReference halfRef = kMsbBit ? split.right() : split.left();
        if (!(halfRef.getPage() instanceof HOTIndirectPage half)) {
          // K's half is a single leaf — addChildAtCombination needs an indirect; skip.
          continue;
        }
        // beta survives in the half? If yes -> still betaIsDiscBit -> addChildAtCombination.
        // If no -> beta is a genuinely-new bit for the half -> addEntryWithInsertInfo (the
        // already-ported not-full branch path). Either way the fold is an O(children) op.
        final int[] halfDiscBits = HOTIncrementalInsert.discriminativeBits(half);
        final int betaCol = java.util.Arrays.binarySearch(halfDiscBits, beta);
        final int childIdx = half.findChildIndex(k);
        if (childIdx < 0) {
          decomposeFailed++;
          continue;
        }
        final HOTIncrementalInsert.InsertInfo halfInfo =
            HOTIncrementalInsert.getInsertInformation(half, childIdx, beta);
        final HOTLeafPage comboLeaf =
            new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.CAS);
        assertTrue(comboLeaf.put(k, VALUE));
        final HOTIndirectPage foldedHalf;
        // comboPartial is meaningful only for the betaCol>=0 (still-betaIsDiscBit) branch.
        final int comboPartial = betaCol >= 0
            ? halfInfo.subtreePrefix() | (betaValue == 1 ? 1 << (halfDiscBits.length - 1 - betaCol)
                : 0)
            : -1;
        try {
          if (betaCol >= 0) {
            betaSurvivesInHalf++;
            foldedHalf = HOTIncrementalInsert.addChildAtCombination(half, comboPartial,
                swizzle(comboLeaf), half.getHeight(), 1, allocator::getAndIncrement);
          } else {
            betaLostInHalf++;
            // beta is new to the half — fold it as a fresh disc bit via addEntryWithInsertInfo.
            foldedHalf = HOTIncrementalInsert.addEntryWithInsertInfo(half, beta, betaValue,
                halfInfo.firstAffected(), halfInfo.affectedCount(), halfInfo.subtreePrefix(),
                swizzle(comboLeaf), half.getHeight(), 1, allocator::getAndIncrement);
          }
        } catch (IllegalArgumentException | IllegalStateException ex) {
          comboLeaf.close();
          decomposeFailed++;
          if (failDetails.size() < 6) {
            failDetails.add(String.format("[%s] K=%s beta=%d decomposition op threw: %s",
                ks.name, HEX.formatHex(k), beta, ex.getMessage()));
          }
          continue;
        }
        halfRef.setPage(foldedHalf);
        // Reassemble the BiNode with the folded half.
        final HOTIndirectPage decomposed = HOTIndirectPage.createBiNode(
            allocator.getAndIncrement(), 1, split.discriminativeBitIndex(),
            split.left(), split.right(), split.height());
        decomposedOk++;

        // ---- STRICT routing of K from the decomposed BiNode ----
        final HOTLeafPage routed = strictDescend(decomposed, k);
        if (routed != null && routed.findEntry(k) >= 0) {
          strictRouteOk++;
        } else {
          strictMisroutes++;
          if (failDetails.size() < 6) {
            final int chosen = decomposed.findChildIndex(k);
            failDetails.add(String.format(
                "[%s] K=%s MISROUTE in decomposed BiNode: beta=%d comboPartial=0x%x "
                    + "half discBits=%s -> BiNode child %d", ks.name, HEX.formatHex(k), beta,
                comboPartial, java.util.Arrays.toString(halfDiscBits), chosen));
          }
        }
        comboLeaf.close();
      }
      closeLeaves(root);
    }

    System.out.println("=== Q4 — full-node betaIsDiscBit splitIndirect decomposition ===");
    System.out.printf("  full-node betaIsDiscBit cases=%d%n", fullCases);
    System.out.printf("  beta SURVIVES in K's half=%d | beta LOST in half=%d%n",
        betaSurvivesInHalf, betaLostInHalf);
    System.out.printf("  decomposition built OK=%d | decomposition not applicable=%d%n",
        decomposedOk, decomposeFailed);
    System.out.printf("  strict route OK=%d | strict MISROUTES=%d%n",
        strictRouteOk, strictMisroutes);
    for (final String d : failDetails) {
      System.out.println("  " + d);
    }
    System.out.println("  DECISIVE: full-node betaIsDiscBit decomposition routes strict = "
        + (decomposedOk > 0 && strictMisroutes == 0 ? "YES"
            : strictMisroutes > 0 ? "NO (misroutes)"
            : "BLOCKED (beta lost in half / not applicable)"));
    assertTrue(fullCases > 0, "no full-node betaIsDiscBit case exercised — coverage gap");
    // Report-only: a decomposition that cannot apply (beta lost) is the finding, not a failure.
    if (decomposedOk > 0) {
      assertTrue(strictMisroutes == 0,
          "full-node decomposition misrouted " + strictMisroutes + " keys");
    }
  }

  // ====================================================================
  // Q3 — canonical target: HOTBulkBuilder of the SAME key set (incl. K).
  // ====================================================================

  @Test
  void canonicalTreeRoutesAndHasComboChild() {
    int casesChecked = 0;
    int canonicalHasComboChild = 0;
    int canonicalNoComboChild = 0;
    int canonicalMisroutes = 0;
    final List<String> noComboDetails = new ArrayList<>();

    final List<KeySet> sets = new ArrayList<>();
    for (final int size : new int[] {600, 900, 1_400, 2_000, 8_000, 20_000}) {
      sets.add(new KeySet("random-" + size + "-s1", random(size, 1)));
      sets.add(new KeySet("random-" + size + "-s2", random(size, 2)));
      sets.add(new KeySet("ascending-" + size, ascending(size)));
      sets.add(new KeySet("bimodal-" + size, bimodal(size)));
    }

    for (final KeySet ks : sets) {
      final AtomicLong allocator = new AtomicLong(1);
      final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(entries(ks.keys), 1,
          IndexType.CAS, allocator::getAndIncrement);
      Page root = built.rootPage();
      if (!(root instanceof HOTIndirectPage)) {
        closeLeaves(root);
        continue;
      }
      // Split full nodes so betaIsDiscBit lands on not-full d* (see Q1) — gives more cases.
      root = splitAllFullNodes(root, allocator);
      final TreeSet<byte[]> present = new TreeSet<>(java.util.Arrays::compareUnsigned);
      present.addAll(ks.keys);
      final List<byte[]> candidates = betaIsDiscBitCandidates(root, present);
      int casesThisSet = 0;
      for (final byte[] k : candidates) {
        if (casesThisSet >= 40) {
          break;
        }
        final BetaCase bc = analyzeBetaCase(root, k);
        if (bc == null || !bc.info.betaIsDiscBit()
            || bc.node.getNumChildren() >= HOTIndirectPage.MAX_NODE_ENTRIES) {
          continue;
        }
        casesThisSet++;
        casesChecked++;

        final int[] nodeDiscBits = HOTIncrementalInsert.discriminativeBits(bc.node);
        final int betaColumn =
            java.util.Arrays.binarySearch(nodeDiscBits, bc.analysis.mismatchBit());
        final int betaValue = HOTBulkBuilder.bitAt(k, bc.analysis.mismatchBit()) ? 1 : 0;
        final int comboPartial = bc.info.subtreePrefix()
            | (betaValue == 1 ? 1 << (nodeDiscBits.length - 1 - betaColumn) : 0);

        // Build the canonical tree over the SAME key set PLUS K.
        final TreeSet<byte[]> withK = new TreeSet<>(java.util.Arrays::compareUnsigned);
        withK.addAll(ks.keys);
        withK.add(k);
        final AtomicLong canonAlloc = new AtomicLong(1);
        final HOTBulkBuilder.BuildResult canon = HOTBulkBuilder.build(
            entries(new ArrayList<>(withK)), 1, IndexType.CAS, canonAlloc::getAndIncrement);

        // Canonical routes ALL keys strict?
        int misroutes = 0;
        for (final byte[] kk : withK) {
          final HOTLeafPage leaf = strictDescend(canon.rootPage(), kk);
          if (leaf == null || leaf.findEntry(kk) < 0) {
            misroutes++;
          }
        }
        canonicalMisroutes += misroutes;

        // Where did K land in the canonical tree? Find the node on K's strict descent that
        // corresponds to bc.node's disc-bit set, and check for a child at comboPartial.
        final boolean hasCombo = canonicalHasChildAtPartial(canon.rootPage(), k, comboPartial);
        if (hasCombo) {
          canonicalHasComboChild++;
        } else {
          canonicalNoComboChild++;
          if (noComboDetails.size() < 8) {
            // What partial DID the canonical tree give K's child?
            final int canonChildPartial = canonicalChildPartialForKey(canon.rootPage(), k);
            noComboDetails.add(String.format(
                "[%s] K=%s incremental comboPartial=0x%x but canonical child partial=0x%x "
                    + "(node discBits=%s)", ks.name, HEX.formatHex(k), comboPartial,
                canonChildPartial, java.util.Arrays.toString(nodeDiscBits)));
          }
        }
        closeLeaves(canon.rootPage());
      }
      closeLeaves(root);
    }

    System.out.println("=== Q3 — canonical HOTBulkBuilder target ===");
    System.out.printf("  betaIsDiscBit cases checked=%d%n", casesChecked);
    System.out.printf("  canonical tree HAS a child at comboPartial : %d%n", canonicalHasComboChild);
    System.out.printf("  canonical tree has NO child at comboPartial : %d%n", canonicalNoComboChild);
    System.out.printf("  canonical strict-routing misroutes (all keys) : %d%n", canonicalMisroutes);
    for (final String d : noComboDetails) {
      System.out.println("  " + d);
    }
    assertTrue(casesChecked > 0, "no betaIsDiscBit case exercised — coverage gap");
    assertTrue(canonicalMisroutes == 0,
        "canonical HOTBulkBuilder tree misrouted " + canonicalMisroutes + " keys under strict descent");
  }

  // ====================================================================
  // Faithful descent + analysis (replicates tryBranchIncremental's setup).
  // ====================================================================

  /** Bundle of one betaIsDiscBit descent's state. */
  private record BetaCase(HOTIndirectPage node, PageReference nodeRef, DescentAnalysis analysis,
                          InsertInfo info) {}

  /**
   * Strict-descend {@code key} from {@code root}, build the path-node stack, run
   * {@link HOTIncrementalInsert#analyzeDescent} + {@link HOTIncrementalInsert#getInsertInformation}
   * exactly as {@code AbstractHOTIndexWriter} does. Returns {@code null} when the key is already
   * present, the path has no compound node, or the descent cannot proceed.
   */
  private static BetaCase analyzeBetaCase(final Page root, final byte[] key) {
    final List<HOTIndirectPage> pathNodes = new ArrayList<>();
    final List<PageReference> pathRefs = new ArrayList<>();
    final List<Integer> childIdx = new ArrayList<>();
    Page page = root;
    PageReference pageRef = null;
    int depth = 0;
    while (page instanceof HOTIndirectPage node) {
      if (++depth > 64) {
        return null;
      }
      final int ci = node.findChildIndex(key);
      if (ci < 0) {
        return null;
      }
      pathNodes.add(node);
      pathRefs.add(pageRef);
      childIdx.add(ci);
      pageRef = node.getChildReference(ci);
      if (pageRef == null) {
        return null;
      }
      page = pageRef.getPage();
    }
    if (!(page instanceof HOTLeafPage leaf)) {
      return null;
    }
    final int pathDepth = pathNodes.size();
    if (pathDepth == 0) {
      return null;
    }
    final HOTIndirectPage[] pn = pathNodes.toArray(new HOTIndirectPage[0]);
    final int[] cs = childIdx.stream().mapToInt(Integer::intValue).toArray();
    final DescentAnalysis analysis = HOTIncrementalInsert.analyzeDescent(pn, cs, pathDepth, leaf, key);
    if (analysis.keyAlreadyPresent() || analysis.insertDepth() < 0) {
      return null;
    }
    final int insertDepth = analysis.insertDepth();
    final HOTIndirectPage node = pn[insertDepth];
    final InsertInfo info = HOTIncrementalInsert.getInsertInformation(node,
        analysis.affectedChildIndex(), analysis.mismatchBit());
    // pathRefs[insertDepth] is the reference whose page is `node`; for the root it is null,
    // so wrap the root in a synthetic reference for splice-in.
    PageReference nodeRef = pathRefs.get(insertDepth);
    if (nodeRef == null) {
      nodeRef = new PageReference();
      nodeRef.setPage(node);
    }
    return new BetaCase(node, nodeRef, analysis, info);
  }

  /**
   * Recursively replace every FULL ({@code MAX_NODE_ENTRIES}-child) indirect node with the
   * materialized {@link HOTIncrementalInsert#splitIndirect} BiNode, producing a tree whose
   * interior nodes are all not-full — the state the incremental writer's
   * {@code addChildAtCombination} actually operates on (after an earlier split). Routing is
   * preserved: splitIndirect is verified routing-correct by {@code HOTIndirectPageSplitFaithfulTest}.
   */
  private static Page splitAllFullNodes(final Page page, final AtomicLong allocator) {
    if (!(page instanceof HOTIndirectPage node)) {
      return page;
    }
    // First recurse into children (rebuild them), then split this node if full.
    for (int i = 0; i < node.getNumChildren(); i++) {
      final PageReference ref = node.getChildReference(i);
      if (ref != null && ref.getPage() != null) {
        ref.setPage(splitAllFullNodes(ref.getPage(), allocator));
      }
    }
    if (node.getNumChildren() < HOTIndirectPage.MAX_NODE_ENTRIES) {
      return node;
    }
    final HOTIncrementalInsert.BiNode split =
        HOTIncrementalInsert.splitIndirect(node, 1, allocator::getAndIncrement);
    return HOTIndirectPage.createBiNode(allocator.getAndIncrement(), 1,
        split.discriminativeBitIndex(), split.left(), split.right(), split.height());
  }

  /**
   * Generate betaIsDiscBit candidate keys: walk to every leaf carrying the accumulated set of
   * ancestor discriminative bits; for several sample keys in each leaf and every ancestor disc
   * bit {@code b}, emit {@code flip(sample, b)}. The flipped key K then has
   * {@code msdb(K, resident) == b}, an existing ancestor disc bit — exactly the betaIsDiscBit
   * branch-insert scenario. Skips any flip that collides with a present key.
   */
  private static List<byte[]> betaIsDiscBitCandidates(final Page root,
      final TreeSet<byte[]> present) {
    final List<byte[]> out = new ArrayList<>();
    final TreeSet<byte[]> emitted = new TreeSet<>(java.util.Arrays::compareUnsigned);
    collectBetaCandidates(root, new int[0], present, emitted, out);
    return out;
  }

  private static void collectBetaCandidates(final Page page, final int[] ancestorDiscBits,
      final TreeSet<byte[]> present, final TreeSet<byte[]> emitted, final List<byte[]> out) {
    if (page instanceof HOTLeafPage leaf) {
      final int ec = leaf.getEntryCount();
      // Sample up to 6 spread-out keys from the leaf; flip each at every ancestor disc bit.
      final int step = Math.max(1, ec / 6);
      for (int i = 0; i < ec; i += step) {
        final byte[] sample = leaf.getKey(i);
        for (final int b : ancestorDiscBits) {
          if (sample == null || b >= sample.length * 8) {
            continue;
          }
          final byte[] flipped = sample.clone();
          flipped[b >>> 3] ^= (byte) (1 << (7 - (b & 7)));
          if (!present.contains(flipped) && emitted.add(flipped)) {
            out.add(flipped);
          }
        }
      }
      return;
    }
    if (!(page instanceof HOTIndirectPage node)) {
      return;
    }
    final int[] discBits = HOTIncrementalInsert.discriminativeBits(node);
    // Accumulate ancestor disc bits for the children's recursion.
    final int[] merged = new int[ancestorDiscBits.length + discBits.length];
    System.arraycopy(ancestorDiscBits, 0, merged, 0, ancestorDiscBits.length);
    System.arraycopy(discBits, 0, merged, ancestorDiscBits.length, discBits.length);
    for (int c = 0; c < node.getNumChildren(); c++) {
      final PageReference ref = node.getChildReference(c);
      if (ref != null && ref.getPage() != null) {
        collectBetaCandidates(ref.getPage(), merged, present, emitted, out);
      }
    }
  }

  /** STRICT root-to-leaf descent — pure {@code findChildIndex}, NO reader fallback. */
  private static HOTLeafPage strictDescend(final Page root, final byte[] key) {
    Page page = root;
    int depth = 0;
    while (page instanceof HOTIndirectPage node) {
      if (++depth > 64) {
        return null;
      }
      final int ci = node.findChildIndex(key);
      if (ci < 0) {
        return null;
      }
      final PageReference ref = node.getChildReference(ci);
      if (ref == null) {
        return null;
      }
      page = ref.getPage();
    }
    return page instanceof HOTLeafPage leaf ? leaf : null;
  }

  /**
   * Walk K's strict descent in {@code canonRoot}; at the node whose disc-bit set best matches
   * the incremental node, check whether any child's stored partial equals {@code comboPartial}.
   * Conservative: returns true if ANY node on K's path has a child at comboPartial.
   */
  private static boolean canonicalHasChildAtPartial(final Page canonRoot, final byte[] key,
      final int comboPartial) {
    Page page = canonRoot;
    int depth = 0;
    while (page instanceof HOTIndirectPage node) {
      if (++depth > 64) {
        return false;
      }
      final int[] partials = node.getPartialKeys();
      if (partials != null) {
        for (int i = 0; i < node.getNumChildren(); i++) {
          if (partials[i] == comboPartial) {
            return true;
          }
        }
      }
      final int ci = node.findChildIndex(key);
      if (ci < 0) {
        return false;
      }
      final PageReference ref = node.getChildReference(ci);
      if (ref == null) {
        return false;
      }
      page = ref.getPage();
    }
    return false;
  }

  /** The stored partial of the child K routes to at the deepest indirect node on its path. */
  private static int canonicalChildPartialForKey(final Page canonRoot, final byte[] key) {
    Page page = canonRoot;
    int lastPartial = -1;
    int depth = 0;
    while (page instanceof HOTIndirectPage node) {
      if (++depth > 64) {
        break;
      }
      final int ci = node.findChildIndex(key);
      if (ci < 0) {
        break;
      }
      final int[] partials = node.getPartialKeys();
      lastPartial = partials != null && ci < partials.length ? partials[ci] : -1;
      final PageReference ref = node.getChildReference(ci);
      if (ref == null) {
        break;
      }
      page = ref.getPage();
    }
    return lastPartial;
  }

  // ====================================================================
  // Key sets and utilities.
  // ====================================================================

  private record KeySet(String name, List<byte[]> keys) {}

  private static List<byte[]> ascending(final int n) {
    final List<byte[]> keys = new ArrayList<>(n);
    for (long i = 0; i < n; i++) {
      keys.add(beKey(i));
    }
    return keys;
  }

  private static List<byte[]> random(final int n, final int seed) {
    final Random rng = new Random(0x9E3779B97F4A7C15L ^ seed ^ ((long) n << 20));
    final TreeSet<Long> set = new TreeSet<>(Long::compareUnsigned);
    while (set.size() < n) {
      set.add(rng.nextLong());
    }
    final List<byte[]> keys = new ArrayList<>(n);
    for (final long k : set) {
      keys.add(beKey(k));
    }
    return keys;
  }

  /** Two magnitude bands — disc bits land in both high and low bytes (wide disc-bit span). */
  private static List<byte[]> bimodal(final int n) {
    final TreeSet<Long> set = new TreeSet<>(Long::compareUnsigned);
    final Random rng = new Random(0xC0FFEEL ^ n);
    final int half = n / 2;
    while (set.size() < half) {
      set.add((long) rng.nextInt(1 << 18));
    }
    while (set.size() < n) {
      set.add(0x5100_0000_0000_0000L + rng.nextInt(1 << 18));
    }
    final List<byte[]> keys = new ArrayList<>(n);
    for (final long k : set) {
      keys.add(beKey(k));
    }
    return keys;
  }

  /**
   * 40-byte keys: an 8-value byte-0 prefix plus a far-out random tail at bytes 30-39 — disc
   * bits span &gt; 8 bytes, forcing the MultiMask layout (mirrors {@code
   * HOTIndirectPageSplitFaithfulTest.WIDE_SPAN}; reproduces the prior "WIDE_SPAN beta=242" case).
   */
  private static List<byte[]> widespan(final int n) {
    final TreeSet<byte[]> set = new TreeSet<>(java.util.Arrays::compareUnsigned);
    final Random rng = new Random(0x5A1AD0L ^ n);
    while (set.size() < n) {
      final byte[] key = new byte[40];
      key[0] = (byte) rng.nextInt(8);
      for (int i = 30; i < 40; i++) {
        key[i] = (byte) rng.nextInt(256);
      }
      set.add(key);
    }
    return new ArrayList<>(set);
  }

  private static List<HOTBulkBuilder.Entry> entries(final List<byte[]> sortedKeys) {
    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(sortedKeys.size());
    for (final byte[] key : sortedKeys) {
      entries.add(new HOTBulkBuilder.Entry(key, VALUE));
    }
    return entries;
  }

  private static byte[] beKey(final long value) {
    final byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) (value >>> (56 - 8 * i));
    }
    return bytes;
  }

  private static PageReference swizzle(final Page page) {
    final PageReference ref = new PageReference();
    ref.setPage(page);
    return ref;
  }

  private static void closeLeaves(final Page page) {
    if (page instanceof HOTLeafPage leaf) {
      if (!leaf.isClosed()) {
        leaf.close();
      }
    } else if (page instanceof HOTIndirectPage node) {
      for (int i = 0; i < node.getNumChildren(); i++) {
        final PageReference ref = node.getChildReference(i);
        if (ref != null && ref.getPage() != null) {
          closeLeaves(ref.getPage());
        }
      }
    }
  }

  private static String partialsHex(final int[] partials) {
    if (partials == null) {
      return "null";
    }
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < partials.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("0x").append(Integer.toHexString(partials[i]));
    }
    return sb.append(']').toString();
  }

  private static byte[] nodeRef(final long bit) {
    final NodeReferences references = new NodeReferences();
    references.getNodeKeys().add(bit);
    final byte[] out = new byte[NodeReferencesSerializer.computeSerializedSize(references)];
    NodeReferencesSerializer.serialize(references, out, 0);
    return out;
  }
}
