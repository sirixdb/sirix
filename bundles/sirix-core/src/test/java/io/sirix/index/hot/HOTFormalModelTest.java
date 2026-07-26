/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Clean-room executable verification of {@code docs/HOT_FORMAL_FOUNDATION.md}.
 *
 * <p>This is <b>not production code</b> and depends on nothing in the HOT writer. It is a
 * reference model that implements the foundation's algorithm directly — the binary Patricia
 * trie {@code R(S)}, the {@code bulkBuild} compression, sparse-path partial encoding, the
 * eight-invariant validator, and {@code descend} routing — and then property-tests the
 * theorems over adversarial key sets.
 *
 * <ul>
 *   <li><b>V1 — {@link #v1ConstructionSoundness()}</b>: executable form of <b>Theorem 1</b>
 *       (and, via the I6 check, Theorem 2): for every adversarial key set,
 *       {@code validate(bulkBuild(S))} is empty.</li>
 *   <li><b>V2 — {@link #v2LexOrder()}</b>: the built HOT's leaves, enumerated in child-index
 *       order, concatenate to {@code S} in sorted order and each is a contiguous lex block —
 *       the property range scans depend on.</li>
 *   <li><b>V3 — {@link #v3SubtreeRebuildIsCanonical()}</b>: executable form of <b>Binna
 *       Lemma 3</b>: every indirect subtree of {@code bulkBuild(S)} is structurally identical
 *       to {@code bulkBuild} of that subtree's keys alone — subtree-local rebuild is
 *       canonical, the formal license for detect-and-rebuild.</li>
 * </ul>
 *
 * <p>If a property fails, either the model or the foundation has a hole — both are worth
 * finding before any writer change.
 */
@DisplayName("HOT formal-foundation executable verification")
final class HOTFormalModelTest {

  /** Key width in bits; bit position 0 is the most significant bit. */
  private static final int W = 64;
  /** Leaf-page capacity. Deliberately small so modest key sets produce multi-level trees. */
  private static final int C = 4;
  /** Maximum indirect fanout. */
  private static final int K = 4;

  private static boolean bitAt(long key, int pos) {
    return ((key >>> (W - 1 - pos)) & 1L) != 0L;
  }

  // ======================================================================
  // R(S) — the binary Patricia trie (foundation §3), over a sorted distinct long[].
  // ======================================================================

  private sealed interface RNode permits RLeaf, RBranch {
    int lo();

    int hi();
  }

  private record RLeaf(int lo, int hi) implements RNode {}

  private record RBranch(int lo, int hi, int beta, RNode left, RNode right) implements RNode {}

  private static int size(RNode r) {
    return r.hi() - r.lo() + 1;
  }

  /** Most significant differing bit position of two distinct keys (0 = MSB). */
  private static int msdb(long a, long b) {
    return Long.numberOfLeadingZeros(a ^ b);
  }

  private static RNode buildR(long[] keys, int lo, int hi) {
    if (lo == hi) {
      return new RLeaf(lo, hi);
    }
    final int beta = msdb(keys[lo], keys[hi]);
    // keys sorted + all agree above beta ⟹ a clean 0→1 transition at beta exists in (lo,hi].
    int m = lo + 1;
    while (m <= hi && !bitAt(keys[m], beta)) {
      m++;
    }
    return new RBranch(lo, hi, beta, buildR(keys, lo, m - 1), buildR(keys, m, hi));
  }

  // ======================================================================
  // The HOT — bulkBuild (foundation §3.1/§3.2).
  // ======================================================================

  private sealed interface HNode permits HLeaf, HIndirect {}

  private record HLeaf(long[] keys) implements HNode {}

  private record HIndirect(int[] discBits, int[] partials, HNode[] children) implements HNode {}

  private static HNode bulkBuild(long[] sortedDistinctKeys) {
    return bulk(buildR(sortedDistinctKeys, 0, sortedDistinctKeys.length - 1), sortedDistinctKeys);
  }

  private static HNode bulk(RNode r, long[] keys) {
    if (size(r) <= C) {
      return new HLeaf(Arrays.copyOfRange(keys, r.lo(), r.hi() + 1));
    }
    // r has > C keys, so it is an RBranch. Form a compound block: greedily expand the
    // frontier (the block's exit points) by splitting the largest expandable frontier node
    // until the block has K children or no frontier node can be expanded. Any frontier is
    // invariant-correct (Theorem 1); SMHP only picks the height-minimal one.
    final RBranch root = (RBranch) r;
    final List<RNode> fnodes = new ArrayList<>();
    final List<List<int[]>> fpaths = new ArrayList<>(); // path = list of {beta, side}
    fnodes.add(root.left());
    fpaths.add(new ArrayList<>(List.of(new int[] {root.beta(), 0})));
    fnodes.add(root.right());
    fpaths.add(new ArrayList<>(List.of(new int[] {root.beta(), 1})));

    while (fnodes.size() < K) {
      int idx = -1;
      int best = -1;
      for (int i = 0; i < fnodes.size(); i++) {
        if (fnodes.get(i) instanceof RBranch rb && size(rb) > best) {
          best = size(rb);
          idx = i;
        }
      }
      if (idx < 0) {
        break; // no expandable frontier node
      }
      final RBranch rb = (RBranch) fnodes.get(idx);
      final List<int[]> base = fpaths.get(idx);
      final List<int[]> pl = new ArrayList<>(base);
      pl.add(new int[] {rb.beta(), 0});
      final List<int[]> pr = new ArrayList<>(base);
      pr.add(new int[] {rb.beta(), 1});
      fnodes.set(idx, rb.left());
      fpaths.set(idx, pl);
      fnodes.add(idx + 1, rb.right());
      fpaths.add(idx + 1, pr);
    }

    // discBits = sorted unique bit positions of the block's BiNodes.
    final TreeSet<Integer> bits = new TreeSet<>();
    for (final List<int[]> path : fpaths) {
      for (final int[] step : path) {
        bits.add(step[0]);
      }
    }
    final int[] discBits = bits.stream().mapToInt(Integer::intValue).toArray();
    final int m = discBits.length;

    // partial[i] = sparse-path encoding: OR of the densePK-weight of each block BiNode the
    // path to child i takes on the 1-side. densePK packs discBits[0] at the most significant
    // position, so discBits[j] has weight 1 << (m-1-j).
    final int[] partials = new int[fnodes.size()];
    final HNode[] children = new HNode[fnodes.size()];
    for (int i = 0; i < fnodes.size(); i++) {
      int p = 0;
      for (final int[] step : fpaths.get(i)) {
        if (step[1] == 1) {
          final int j = Arrays.binarySearch(discBits, step[0]);
          p |= 1 << (m - 1 - j);
        }
      }
      partials[i] = p;
      children[i] = bulk(fnodes.get(i), keys);
    }
    return new HIndirect(discBits, partials, children);
  }

  /** Dense partial key: the key's bits at the mask positions, packed MSB-first. */
  private static int densePK(long key, int[] discBits) {
    int d = 0;
    for (final int b : discBits) {
      d = (d << 1) | (bitAt(key, b) ? 1 : 0);
    }
    return d;
  }

  /** descend — Binna's routing: pure highest-index subset match (no exact-match branch). */
  private static HLeaf descend(HNode node, long key) {
    HNode n = node;
    while (n instanceof HIndirect ind) {
      final int d = densePK(key, ind.discBits());
      int best = -1;
      for (int i = 0; i < ind.partials().length; i++) {
        if ((d & ind.partials()[i]) == ind.partials()[i]) {
          best = i; // highest index wins
        }
      }
      if (best < 0) {
        return new HLeaf(new long[0]); // unroutable — provokes an I6 failure
      }
      n = ind.children()[best];
    }
    return (HLeaf) n;
  }

  // ======================================================================
  // The validator — the eight invariants of foundation §2.
  // ======================================================================

  private static List<String> validate(HNode root, long[] allKeys) {
    final List<String> v = new ArrayList<>();

    // I1 — leaves pairwise key-disjoint, and together exactly S.
    final List<HLeaf> leaves = new ArrayList<>();
    collectLeaves(root, leaves);
    final Set<Long> seen = new HashSet<>();
    for (final HLeaf lf : leaves) {
      for (final long k : lf.keys()) {
        if (!seen.add(k)) {
          v.add("I1: key " + Long.toHexString(k) + " appears in more than one leaf");
        }
      }
    }
    if (seen.size() != allKeys.length) {
      v.add("I1: leaf key count " + seen.size() + " != |S| " + allKeys.length);
    }

    validateIndirect(root, Integer.MIN_VALUE, v);

    // I6 — every stored key descends to the leaf that holds it.
    for (final long k : allKeys) {
      final HLeaf reached = descend(root, k);
      boolean found = false;
      for (final long x : reached.keys()) {
        if (x == k) {
          found = true;
          break;
        }
      }
      if (!found) {
        v.add("I6: key " + Long.toHexString(k) + " descends to a leaf that does not hold it");
      }
    }
    return v;
  }

  private static void validateIndirect(HNode node, int parentTopDisc, List<String> v) {
    if (!(node instanceof HIndirect ind)) {
      return;
    }
    final int[] disc = ind.discBits();
    final int[] partials = ind.partials();
    final HNode[] children = ind.children();

    // I11 — most significant disc bit strictly less significant than the parent's.
    if (parentTopDisc != Integer.MIN_VALUE && disc[0] <= parentTopDisc) {
      v.add("I11: indirect topDisc " + disc[0] + " <= parent topDisc " + parentTopDisc);
    }
    // I4 — first partial is zero.
    if (partials[0] != 0) {
      v.add("I4: partials[0] = 0x" + Integer.toHexString(partials[0]) + " != 0");
    }
    // I3 — partials distinct.  I7 — partials strictly ascending.
    for (int i = 1; i < partials.length; i++) {
      if (partials[i] == partials[i - 1]) {
        v.add("I3: duplicate partial 0x" + Integer.toHexString(partials[i]));
      }
      if (Integer.compareUnsigned(partials[i], partials[i - 1]) <= 0) {
        v.add("I7: partials not ascending at " + (i - 1) + "->" + i + ": 0x"
            + Integer.toHexString(partials[i - 1]) + " vs 0x" + Integer.toHexString(partials[i]));
      }
    }
    // I5 — every bit of partials[i] set in every subtree key's dense PK.
    // I8 — child subtree minima strictly ascending.
    long prevMin = Long.MIN_VALUE;
    boolean prevMinSet = false;
    for (int i = 0; i < children.length; i++) {
      final List<Long> keys = new ArrayList<>();
      collectKeys(children[i], keys);
      for (final long k : keys) {
        final int d = densePK(k, disc);
        if ((d & partials[i]) != partials[i]) {
          v.add("I5: indirect child[" + i + "] partial 0x" + Integer.toHexString(partials[i])
              + " not subset of densePK 0x" + Integer.toHexString(d) + " of key "
              + Long.toHexString(k));
        }
      }
      long min = Long.MAX_VALUE;
      for (final long k : keys) {
        min = Math.min(min, k);
      }
      if (prevMinSet && Long.compareUnsigned(min, prevMin) <= 0) {
        v.add("I8: child[" + i + "] min " + Long.toHexString(min)
            + " <= child[" + (i - 1) + "] min " + Long.toHexString(prevMin));
      }
      prevMin = min;
      prevMinSet = true;
      validateIndirect(children[i], disc[0], v);
    }
  }

  private static void collectLeaves(HNode n, List<HLeaf> out) {
    if (n instanceof HLeaf lf) {
      out.add(lf);
    } else if (n instanceof HIndirect ind) {
      for (final HNode c : ind.children()) {
        collectLeaves(c, out);
      }
    }
  }

  private static void collectKeys(HNode n, List<Long> out) {
    if (n instanceof HLeaf lf) {
      for (final long k : lf.keys()) {
        out.add(k);
      }
    } else if (n instanceof HIndirect ind) {
      for (final HNode c : ind.children()) {
        collectKeys(c, out);
      }
    }
  }

  private static boolean structurallyEqual(HNode a, HNode b) {
    if (a instanceof HLeaf la && b instanceof HLeaf lb) {
      return Arrays.equals(la.keys(), lb.keys());
    }
    if (a instanceof HIndirect ia && b instanceof HIndirect ib) {
      if (!Arrays.equals(ia.discBits(), ib.discBits()) || !Arrays.equals(ia.partials(), ib.partials())
          || ia.children().length != ib.children().length) {
        return false;
      }
      for (int i = 0; i < ia.children().length; i++) {
        if (!structurallyEqual(ia.children()[i], ib.children()[i])) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  // ======================================================================
  // Adversarial key-set generators. bulkBuild consumes a sorted key set, so what
  // matters is the value DISTRIBUTION, not insertion order.
  // ======================================================================

  private interface Gen {
    void fill(int n, Random r, Set<Long> out);
  }

  private static final Gen[] GENERATORS = {
      // dense low — values 0..n-1
      (n, r, out) -> { for (int i = 0; i < n; i++) out.add((long) i); },
      // dense high — values shifted into the top bits
      (n, r, out) -> { for (int i = 0; i < n; i++) out.add(((long) i) << 50); },
      // uniform random over the full 64-bit domain (straddles the sign bit)
      (n, r, out) -> { while (out.size() < n) out.add(r.nextLong()); },
      // bimodal — two far-apart clusters
      (n, r, out) -> {
        while (out.size() < n) {
          out.add((out.size() & 1) == 0 ? r.nextInt(1 << 16) : 0x6000_0000_0000_0000L + r.nextInt(1 << 16));
        }
      },
      // common high prefix — differ only in the low bits (the CAS value/nodeKey case)
      (n, r, out) -> { while (out.size() < n) out.add(0x1234_5678_9ABC_0000L | (r.nextLong() & 0xFFFFL)); },
      // sparse — a handful of set bits, so R(S) is deep and unbalanced
      (n, r, out) -> {
        while (out.size() < n) {
          long k = 0;
          for (int b = 0; b < 4; b++) {
            k |= 1L << r.nextInt(64);
          }
          out.add(k);
        }
      },
  };

  private static long[] sortedDistinct(Set<Long> set) {
    final long[] a = set.stream().mapToLong(Long::longValue).toArray();
    unsignedSort(a);
    return a;
  }

  /**
   * Sort by unsigned (bit-string lexicographic) order — the order the model assumes
   * everywhere ({@code msdb}/{@code bitAt} treat keys as bit strings, MSB first). Flipping
   * the sign bit makes signed sort agree with unsigned sort.
   */
  private static void unsignedSort(long[] a) {
    for (int i = 0; i < a.length; i++) {
      a[i] ^= Long.MIN_VALUE;
    }
    Arrays.sort(a);
    for (int i = 0; i < a.length; i++) {
      a[i] ^= Long.MIN_VALUE;
    }
  }

  // ======================================================================
  // V1 — construction soundness (Theorem 1 + Theorem 2 via I6).
  // ======================================================================

  @Test
  @DisplayName("V1: validate(bulkBuild(S)) is empty for every adversarial key set")
  void v1ConstructionSoundness() {
    final int[] sizes = {2, 3, 5, 8, 16, 50, 150, 500};
    int checked = 0;
    final List<String> failures = new ArrayList<>();
    for (int g = 0; g < GENERATORS.length; g++) {
      for (final int n : sizes) {
        for (int seed = 0; seed < 100; seed++) {
          final Set<Long> set = new HashSet<>();
          GENERATORS[g].fill(n, new Random((((long) g) << 40) ^ (((long) n) << 20) ^ seed), set);
          if (set.size() < 2) {
            continue;
          }
          final long[] keys = sortedDistinct(set);
          final HNode hot = bulkBuild(keys);
          final List<String> v = validate(hot, keys);
          checked++;
          if (!v.isEmpty() && failures.size() < 10) {
            failures.add("gen=" + g + " n=" + keys.length + " seed=" + seed + " -> " + v.get(0));
          }
        }
      }
    }
    assertTrue(failures.isEmpty(),
        "construction violated invariants in " + checked + " trials:\n" + String.join("\n", failures));
    System.out.println("[V1] " + checked + " adversarial key sets — 0 invariant violations");
  }

  // ======================================================================
  // V2 — lex order: in-order leaves concatenate to sorted S in contiguous blocks.
  // ======================================================================

  @Test
  @DisplayName("V2: in-order HOT leaves reproduce S in sorted, contiguous blocks")
  void v2LexOrder() {
    int checked = 0;
    final List<String> failures = new ArrayList<>();
    for (int g = 0; g < GENERATORS.length; g++) {
      for (final int n : new int[] {2, 8, 50, 300}) {
        for (int seed = 0; seed < 60; seed++) {
          final Set<Long> set = new HashSet<>();
          GENERATORS[g].fill(n, new Random((((long) g) << 33) ^ seed), set);
          if (set.size() < 2) {
            continue;
          }
          final long[] keys = sortedDistinct(set);
          final List<HLeaf> leaves = new ArrayList<>();
          collectLeaves(bulkBuild(keys), leaves);
          final List<Long> flat = new ArrayList<>();
          for (final HLeaf lf : leaves) {
            for (final long k : lf.keys()) {
              flat.add(k);
            }
          }
          checked++;
          for (int i = 0; i < keys.length && failures.size() < 10; i++) {
            if (flat.get(i) != keys[i]) {
              failures.add("gen=" + g + " n=" + keys.length + " seed=" + seed
                  + ": leaf order != sorted order at index " + i);
              break;
            }
          }
        }
      }
    }
    assertTrue(failures.isEmpty(), "lex-order violations:\n" + String.join("\n", failures));
    System.out.println("[V2] " + checked + " key sets — leaf order == sorted order in all");
  }

  // ======================================================================
  // V3 — subtree-rebuild canonicity (Binna Lemma 3).
  // ======================================================================

  @Test
  @DisplayName("V3: every indirect subtree equals bulkBuild of its own keys (Lemma 3)")
  void v3SubtreeRebuildIsCanonical() {
    int checkedSubtrees = 0;
    final List<String> failures = new ArrayList<>();
    for (int g = 0; g < GENERATORS.length; g++) {
      for (final int n : new int[] {8, 16, 80, 400}) {
        for (int seed = 0; seed < 60; seed++) {
          final Set<Long> set = new HashSet<>();
          GENERATORS[g].fill(n, new Random((((long) g) << 29) ^ seed), set);
          if (set.size() < 2) {
            continue;
          }
          final HNode hot = bulkBuild(sortedDistinct(set));
          final List<HNode> subtrees = new ArrayList<>();
          collectIndirects(hot, subtrees);
          for (final HNode sub : subtrees) {
            final List<Long> keys = new ArrayList<>();
            collectKeys(sub, keys);
            final long[] arr = keys.stream().mapToLong(Long::longValue).toArray();
            unsignedSort(arr);
            final HNode rebuilt = bulkBuild(arr);
            checkedSubtrees++;
            if (!structurallyEqual(sub, rebuilt) && failures.size() < 10) {
              failures.add("gen=" + g + " n=" + n + " seed=" + seed
                  + ": subtree of " + keys.size() + " keys != bulkBuild of those keys");
            }
            // a rebuilt subtree must itself be invariant-clean in isolation
            final List<String> v = validate(rebuilt, arr);
            if (!v.isEmpty() && failures.size() < 10) {
              failures.add("gen=" + g + " seed=" + seed + ": rebuilt subtree invalid -> " + v.get(0));
            }
          }
        }
      }
    }
    assertTrue(failures.isEmpty(), "subtree-rebuild violations:\n" + String.join("\n", failures));
    System.out.println("[V3] " + checkedSubtrees + " subtrees — each equals bulkBuild of its keys");
  }

  private static void collectIndirects(HNode n, List<HNode> out) {
    if (n instanceof HIndirect ind) {
      out.add(n);
      for (final HNode c : ind.children()) {
        collectIndirects(c, out);
      }
    }
  }
}
