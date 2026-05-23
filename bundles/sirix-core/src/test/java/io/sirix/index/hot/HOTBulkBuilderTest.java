/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verification of {@link HOTBulkBuilder} against the eight-invariant oracle
 * {@link HOTInvariantValidator} and a routing oracle (descend each key and assert it lands in
 * the leaf that actually holds it).
 *
 * <p>This is the executable form of Theorem 1 / Theorem 2 from {@code
 * docs/HOT_FORMAL_FOUNDATION.md}: {@code HOTBulkBuilder.build} of any adversarial key set must
 * produce a HOT with zero invariant violations, and every stored key must PEXT-route
 * (descend via {@link HOTIndirectPage#findChildIndex}) from the root to the leaf that holds it.
 * The adversarial key-set generators mirror {@code HOTFormalModelTest} — ascending, descending,
 * uniform random, bimodal, common-prefix, and sparse distributions — plus variable-length keys
 * and tombstone entries, which the model (fixed 64-bit keys) does not exercise.
 *
 * <p>The validator needs a {@link StorageEngineReader}; since every page produced by
 * {@code HOTBulkBuilder} is swizzled into its {@link PageReference} via
 * {@link PageReference#setPage}, a lenient Mockito stub suffices — its {@code loadHOTPage}
 * returns {@code null}, and the validator falls through to the in-memory page.
 */
@DisplayName("HOTBulkBuilder — invariant and routing verification")
final class HOTBulkBuilderTest {

  /** Tombstone value: a {@code NodeReferences} tagged 0xFE (foundation: tombstones are keys). */
  private static final byte[] TOMBSTONE = {(byte) 0xFE};

  // ======================================================================
  // Adversarial key-set generators (mirrors HOTFormalModelTest), over 8-byte BE keys.
  // ======================================================================

  private interface Gen {
    void fill(int n, Random r, Set<Long> out);
  }

  private static final Gen[] GENERATORS = {
      // dense low — values 0..n-1
      (n, r, out) -> { for (int i = 0; i < n; i++) out.add((long) i); },
      // dense high — values shifted into the top bits
      (n, r, out) -> { for (int i = 0; i < n; i++) out.add(((long) i) << 40); },
      // descending dense — same key set as dense-low but generated high-to-low (the sorted
      // input is identical; this exercises that build() is insertion-order-independent)
      (n, r, out) -> { for (int i = n - 1; i >= 0; i--) out.add((long) i); },
      // uniform random over the full 64-bit domain (straddles the sign bit)
      (n, r, out) -> { while (out.size() < n) out.add(r.nextLong()); },
      // bimodal — two far-apart clusters
      (n, r, out) -> {
        while (out.size() < n) {
          out.add((out.size() & 1) == 0
              ? r.nextInt(1 << 16)
              : 0x6000_0000_0000_0000L + r.nextInt(1 << 16));
        }
      },
      // common high prefix — differ only in the low bits (the CAS value/nodeKey case)
      (n, r, out) -> {
        while (out.size() < n) out.add(0x1234_5678_9ABC_0000L | (r.nextLong() & 0xFFFFL));
      },
      // sparse — a handful of set bits, so R(S) is deep and unbalanced
      (n, r, out) -> {
        while (out.size() < n) {
          long k = 0;
          for (int b = 0; b < 4; b++) k |= 1L << r.nextInt(64);
          out.add(k);
        }
      },
  };

  // ======================================================================
  // V1 — construction soundness: validate(build(S)) is empty for adversarial key sets.
  // ======================================================================

  @Test
  @DisplayName("V1: zero invariant violations for every adversarial key set")
  void v1ConstructionSoundness() {
    // Sizes deliberately exceed HOTLeafPage.MAX_ENTRIES (512) so multi-level tries arise.
    final int[] sizes = {2, 3, 8, 64, 512, 513, 2_000, 8_000, 20_000};
    int checked = 0;
    int multiLevelTries = 0;
    final List<String> failures = new ArrayList<>();
    for (int g = 0; g < GENERATORS.length; g++) {
      for (final int n : sizes) {
        for (int seed = 0; seed < 25; seed++) {
          final Set<Long> set = new HashSet<>();
          GENERATORS[g].fill(n, new Random((((long) g) << 40) ^ (((long) n) << 20) ^ seed), set);
          if (set.size() < 2) {
            continue;
          }
          final List<HOTBulkBuilder.Entry> entries = entriesFromLongs(set, seed);
          final HOTBulkBuilder.BuildResult result = HOTBulkBuilder.build(
              entries, /*revision*/ 1, IndexType.CAS, new AtomicLong()::getAndIncrement);
          checked++;
          if (treeHeight(result.rootPage()) >= 3) {
            multiLevelTries++;
          }
          final String failure = checkAll(result, entries, "gen=" + g + " n=" + entries.size()
              + " seed=" + seed);
          if (failure != null && failures.size() < 12) {
            failures.add(failure);
          }
          closeLeaves(result.rootPage());
        }
      }
    }
    assertTrue(failures.isEmpty(),
        "construction violated invariants in " + checked + " trials:\n"
            + String.join("\n", failures));
    // Sanity: the large key sets must actually exercise multi-level (indirect-of-indirect)
    // tries, otherwise V1 would only test shallow structure.
    assertTrue(multiLevelTries > 0, "no multi-level tries were exercised — coverage gap");
    System.out.println("[V1] " + checked + " adversarial key sets — 0 invariant violations ("
        + multiLevelTries + " multi-level tries)");
  }

  // ======================================================================
  // V2 — variable-length keys with large values forcing byte-capacity leaf cuts.
  // ======================================================================

  @Test
  @DisplayName("V2: variable-length keys and byte-overflow leaf cuts validate clean")
  void v2VariableLengthKeysAndByteOverflow() {
    int checked = 0;
    final List<String> failures = new ArrayList<>();
    for (int seed = 0; seed < 40; seed++) {
      final Random r = new Random(0x5151_0000L ^ seed);
      final int n = 200 + r.nextInt(2_000);
      // Distinct variable-length keys (1..40 bytes); large values (up to ~600 bytes) so leaves
      // overflow on byte capacity well before MAX_ENTRIES — exercises data-determined leaf fill.
      final Set<String> seenHex = new HashSet<>();
      final List<byte[]> keyList = new ArrayList<>();
      while (keyList.size() < n) {
        final byte[] k = new byte[1 + r.nextInt(40)];
        r.nextBytes(k);
        if (seenHex.add(java.util.HexFormat.of().formatHex(k))) {
          keyList.add(k);
        }
      }
      keyList.sort(java.util.Arrays::compareUnsigned);
      final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(keyList.size());
      for (int i = 0; i < keyList.size(); i++) {
        // Sprinkle tombstones (every 7th key) — they are keys and must be placed like any entry.
        if (i % 7 == 3) {
          entries.add(new HOTBulkBuilder.Entry(keyList.get(i), TOMBSTONE));
        } else {
          final byte[] v = new byte[1 + r.nextInt(600)];
          r.nextBytes(v);
          entries.add(new HOTBulkBuilder.Entry(keyList.get(i), v));
        }
      }
      final HOTBulkBuilder.BuildResult result = HOTBulkBuilder.build(
          entries, /*revision*/ 3, IndexType.PATH, new AtomicLong(1000)::getAndIncrement);
      checked++;
      final String failure = checkAll(result, entries, "varlen seed=" + seed + " n=" + n);
      if (failure != null && failures.size() < 12) {
        failures.add(failure);
      }
      closeLeaves(result.rootPage());
    }
    assertTrue(failures.isEmpty(), "variable-length violations:\n" + String.join("\n", failures));
    System.out.println("[V2] " + checked + " variable-length / byte-overflow key sets — clean");
  }

  // ======================================================================
  // V3 — small key sets: a single entry, and key sets that fit one leaf page.
  // ======================================================================

  @Test
  @DisplayName("V3: degenerate sizes (1 entry, single-page key sets) build correctly")
  void v3DegenerateSizes() {
    // Single entry — root is a leaf page.
    final List<HOTBulkBuilder.Entry> one = List.of(
        new HOTBulkBuilder.Entry(beKey(42L), new byte[] {1, 2, 3}));
    final HOTBulkBuilder.BuildResult single = HOTBulkBuilder.build(
        one, 1, IndexType.NAME, new AtomicLong()::getAndIncrement);
    assertTrue(single.rootPage() instanceof HOTLeafPage, "single-entry root must be a leaf");
    assertEquals(1, single.leafCount());
    assertEquals(0, single.indirectCount());
    assertEquals(null, checkAll(single, one, "single-entry"));
    closeLeaves(single.rootPage());

    // A handful of keys that all fit one leaf page — still a leaf root.
    final List<HOTBulkBuilder.Entry> few = new ArrayList<>();
    for (long k = 0; k < 50; k++) {
      few.add(new HOTBulkBuilder.Entry(beKey(k), new byte[] {(byte) k}));
    }
    final HOTBulkBuilder.BuildResult fewResult = HOTBulkBuilder.build(
        few, 1, IndexType.NAME, new AtomicLong()::getAndIncrement);
    assertTrue(fewResult.rootPage() instanceof HOTLeafPage, "50-entry root must be a leaf");
    assertEquals(null, checkAll(fewResult, few, "fifty-entries"));
    closeLeaves(fewResult.rootPage());
    System.out.println("[V3] degenerate sizes — single entry + single-page key sets clean");
  }

  // ======================================================================
  // V4 — determinism: the same key set always produces a structurally identical HOT.
  // ======================================================================

  @Test
  @DisplayName("V4: build is a deterministic function of the key set")
  void v4Determinism() {
    for (int g = 0; g < GENERATORS.length; g++) {
      final Set<Long> set = new HashSet<>();
      GENERATORS[g].fill(3_000, new Random(g), set);
      if (set.size() < 2) {
        continue;
      }
      final List<HOTBulkBuilder.Entry> a = entriesFromLongs(set, 0);
      final List<HOTBulkBuilder.Entry> b = entriesFromLongs(set, 0);
      final HOTBulkBuilder.BuildResult ra = HOTBulkBuilder.build(
          a, 1, IndexType.CAS, new AtomicLong()::getAndIncrement);
      final HOTBulkBuilder.BuildResult rb = HOTBulkBuilder.build(
          b, 1, IndexType.CAS, new AtomicLong()::getAndIncrement);
      assertTrue(structurallyEqual(ra.rootPage(), rb.rootPage()),
          "gen=" + g + ": build is not deterministic");
      assertEquals(ra.leafCount(), rb.leafCount(), "gen=" + g + " leaf count differs");
      assertEquals(ra.indirectCount(), rb.indirectCount(), "gen=" + g + " indirect count differs");
      closeLeaves(ra.rootPage());
      closeLeaves(rb.rootPage());
    }
    System.out.println("[V4] build is deterministic across all generators");
  }

  // ======================================================================
  // V5 — MultiMask layout: keys whose discriminative bits span more than 8 bytes.
  // ======================================================================

  @Test
  @DisplayName("V5: keys spanning >8 byte positions exercise the MultiMask layout")
  void v5MultiMaskLayout() {
    // Keys engineered so a single compound node's discriminative bits span well past an 8-byte
    // window: each key is a zero-filled 28-byte array with one 0x80 bit at a distinct byte. The
    // pairwise MSDBs land at bytes 0..27, so the root compound node — once its block expands —
    // captures discriminative bits across the whole 28-byte range, forcing the MultiMask layout
    // (and its chunked computeMultiMaskPartialKey routing decode).
    final int span = 28;
    for (int variant = 0; variant < 8; variant++) {
      final Random r = new Random(0x7E57_0000L ^ variant);
      final List<HOTBulkBuilder.Entry> entries = new ArrayList<>();
      // Many keys per byte position so leaves overflow and indirect structure is forced.
      for (int bytePos = 0; bytePos < span; bytePos++) {
        for (int low = 0; low < 24; low++) {
          final byte[] key = new byte[span + 2];
          key[bytePos] = (byte) 0x80;
          // A distinct low tail keeps keys unique without disturbing the wide MSDB spread.
          key[span] = (byte) (low >>> 8);
          key[span + 1] = (byte) low;
          entries.add(new HOTBulkBuilder.Entry(key,
              (low % 9 == 4) ? TOMBSTONE : beKey(r.nextLong())));
        }
      }
      entries.sort((a, b) -> java.util.Arrays.compareUnsigned(a.key(), b.key()));
      final HOTBulkBuilder.BuildResult result = HOTBulkBuilder.build(
          entries, /*revision*/ 2, IndexType.CAS, new AtomicLong()::getAndIncrement);
      assertEquals(null, checkAll(result, entries, "multimask variant=" + variant));
      assertTrue(countMultiMaskIndirects(result.rootPage()) > 0,
          "variant=" + variant + ": expected the MultiMask layout to be exercised");
      closeLeaves(result.rootPage());
    }
    System.out.println("[V5] MultiMask layout exercised and validated clean");
  }

  // ======================================================================
  // Oracle: run the full validator + descent routing oracle + key-set preservation.
  // ======================================================================

  /**
   * Returns {@code null} if the built HOT passes every check, otherwise a one-line failure
   * description. Checks: (1) all eight {@link HOTInvariantValidator} invariants; (2) every
   * stored key descends to the leaf that holds it (I6, against the production routing); (3)
   * the leaf key set equals the input key set exactly (no key lost, no key invented).
   */
  private static String checkAll(final HOTBulkBuilder.BuildResult result,
      final List<HOTBulkBuilder.Entry> entries, final String label) {
    final PageReference rootRef = result.rootReference();
    assertNotNull(rootRef.getPage(), label + ": root page not swizzled");

    // (1) Eight-invariant validator.
    final HOTInvariantValidator.Result validation =
        HOTInvariantValidator.validate(rootRef, Mockito.mock(StorageEngineReader.class));
    if (!validation.isOk()) {
      return label + " -> invariant: " + validation.violations().get(0);
    }

    // (2) Routing: every stored key must descend to the leaf that holds it.
    for (final HOTBulkBuilder.Entry e : entries) {
      final HOTLeafPage routed = descend(rootRef.getPage(), e.key());
      if (routed == null) {
        return label + " -> I6: key " + hex(e.key()) + " did not route to any leaf";
      }
      if (routed.findEntry(e.key()) < 0) {
        return label + " -> I6: key " + hex(e.key()) + " routed to leaf "
            + routed.getPageKey() + " which does not hold it";
      }
    }

    // (3) Key-set preservation: leaves hold exactly the input keys.
    final Set<String> inputKeys = new HashSet<>(entries.size() * 2);
    for (final HOTBulkBuilder.Entry e : entries) {
      inputKeys.add(hex(e.key()));
    }
    final Set<String> leafKeys = new HashSet<>(entries.size() * 2);
    collectLeafKeys(rootRef.getPage(), leafKeys);
    if (!leafKeys.equals(inputKeys)) {
      return label + " -> key-set: leaf keys (" + leafKeys.size() + ") != input keys ("
          + inputKeys.size() + ")";
    }
    return null;
  }

  /** Descend the production routing ({@link HOTIndirectPage#findChildIndex}) to a leaf. */
  private static HOTLeafPage descend(final Page root, final byte[] key) {
    Page page = root;
    int guard = 0;
    while (page instanceof HOTIndirectPage indirect) {
      if (++guard > HOTInvariantValidator.DEFAULT_MAX_HEIGHT) {
        return null;
      }
      final int idx = indirect.findChildIndex(key);
      if (idx < 0) {
        return null;
      }
      final PageReference childRef = indirect.getChildReference(idx);
      if (childRef == null || childRef.getPage() == null) {
        return null;
      }
      page = childRef.getPage();
    }
    return page instanceof HOTLeafPage leaf ? leaf : null;
  }

  private static void collectLeafKeys(final Page page, final Set<String> out) {
    if (page instanceof HOTLeafPage leaf) {
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        out.add(hex(leaf.getKey(i)));
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference ref = indirect.getChildReference(i);
        if (ref != null && ref.getPage() != null) {
          collectLeafKeys(ref.getPage(), out);
        }
      }
    }
  }

  /**
   * Release the off-heap {@code MemorySegment} of every leaf page in a built subtree. The
   * builder allocates real 64 KiB leaf pages; a test that builds thousands of trees must free
   * them or it transiently exhausts the off-heap budget.
   */
  private static void closeLeaves(final Page page) {
    if (page instanceof HOTLeafPage leaf) {
      leaf.close();
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference ref = indirect.getChildReference(i);
        if (ref != null && ref.getPage() != null) {
          closeLeaves(ref.getPage());
        }
      }
    }
  }

  /** Structural equality of two built HOT subtrees (used by the determinism check). */
  private static boolean structurallyEqual(final Page a, final Page b) {
    if (a instanceof HOTLeafPage la && b instanceof HOTLeafPage lb) {
      if (la.getEntryCount() != lb.getEntryCount()) {
        return false;
      }
      for (int i = 0; i < la.getEntryCount(); i++) {
        if (!java.util.Arrays.equals(la.getKey(i), lb.getKey(i))
            || !java.util.Arrays.equals(la.getValue(i), lb.getValue(i))) {
          return false;
        }
      }
      return true;
    }
    if (a instanceof HOTIndirectPage ia && b instanceof HOTIndirectPage ib) {
      if (ia.getNumChildren() != ib.getNumChildren()
          || !java.util.Arrays.equals(ia.getPartialKeys(), ib.getPartialKeys())
          || ia.getLayoutType() != ib.getLayoutType()) {
        return false;
      }
      for (int i = 0; i < ia.getNumChildren(); i++) {
        if (!structurallyEqual(ia.getChildReference(i).getPage(),
            ib.getChildReference(i).getPage())) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  // ======================================================================
  // Helpers.
  // ======================================================================

  /**
   * Build a sorted, distinct entry list from a set of long keys. Keys are 8-byte big-endian
   * (so unsigned-lexicographic order on the bytes equals unsigned order on the longs). Every
   * 5th entry is a tombstone; the rest get a small deterministic value derived from the key.
   */
  private static List<HOTBulkBuilder.Entry> entriesFromLongs(final Set<Long> set,
      final int valueSalt) {
    final long[] longs = set.stream().mapToLong(Long::longValue).toArray();
    // Sort unsigned (flip the sign bit so signed sort agrees with unsigned).
    for (int i = 0; i < longs.length; i++) {
      longs[i] ^= Long.MIN_VALUE;
    }
    java.util.Arrays.sort(longs);
    for (int i = 0; i < longs.length; i++) {
      longs[i] ^= Long.MIN_VALUE;
    }
    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(longs.length);
    for (int i = 0; i < longs.length; i++) {
      final byte[] key = beKey(longs[i]);
      if (i % 5 == 2) {
        entries.add(new HOTBulkBuilder.Entry(key, TOMBSTONE));
      } else {
        entries.add(new HOTBulkBuilder.Entry(key,
            beKey(longs[i] * 0x9E37_79B9_7F4A_7C15L + valueSalt)));
      }
    }
    return entries;
  }

  /** Count indirect pages using the {@code MULTI_MASK} layout in a built HOT subtree. */
  private static int countMultiMaskIndirects(final Page page) {
    if (!(page instanceof HOTIndirectPage indirect)) {
      return 0;
    }
    int count = indirect.getLayoutType() == HOTIndirectPage.LayoutType.MULTI_MASK ? 1 : 0;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      final PageReference ref = indirect.getChildReference(i);
      if (ref != null && ref.getPage() != null) {
        count += countMultiMaskIndirects(ref.getPage());
      }
    }
    return count;
  }

  /** Height of a built HOT subtree: 1 for a lone leaf, 1 + max child height for an indirect. */
  private static int treeHeight(final Page page) {
    if (!(page instanceof HOTIndirectPage indirect)) {
      return 1;
    }
    int max = 0;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      final PageReference ref = indirect.getChildReference(i);
      if (ref != null && ref.getPage() != null) {
        max = Math.max(max, treeHeight(ref.getPage()));
      }
    }
    return 1 + max;
  }

  /** 8-byte big-endian encoding of a long. */
  private static byte[] beKey(final long v) {
    final byte[] b = new byte[8];
    for (int i = 0; i < 8; i++) {
      b[i] = (byte) (v >>> (56 - 8 * i));
    }
    return b;
  }

  private static String hex(final byte[] b) {
    return java.util.HexFormat.of().formatHex(b);
  }
}
