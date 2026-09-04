/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import io.sirix.index.projection.ProjectionIndexScan.PredicateTree;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Many literals, one evidence walk: {@link ProjectionColumnStore#applyBloomPruneMany} must narrow
 * every mask exactly as {@link ProjectionColumnStore#applyBloomPrune} does one literal at a time, a
 * tree of string equalities over one column must price them in ONE walk (counted in fetches), and
 * {@link ProjectionColumnScan#zoneStabSorted} must mark exactly the leaves whose zone admits each
 * value. The any-k group rewrite plans on these three answers; a mask mixed up between two literals
 * would choose the wrong groups — cheap, but the recursive aggregate would still be exact — and a
 * batched tree mask mixed up would drop rows, so parity is asserted bit for bit.
 */
final class MultiLiteralEvidenceTest {

  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};

  private static final int LEAVES = 70; // > one 64-bit word, so word boundaries are crossed
  private static final int ROWS = 32;

  private record Fixture(ProjectionColumnStore store, ColumnSegmentFetcher fetcher, AtomicInteger fetchedSegments) {
  }

  /**
   * Column 0 is {@code leaf * 100 + r} (every leaf its own zone, ranges [L*100, L*100+31]); column 1
   * holds {@code "t-L-r"} plus the shared value {@code "every"} on even leaves — so literals have
   * home sets of one leaf, half the leaves, or none.
   */
  private static Fixture buildFixture() {
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(LEAVES);
    long nextOffset = 1_000;
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
      long recordKey = leaf * 100_000L + 1;
      for (int r = 0; r < ROWS; r++) {
        final String title = r == 0 && (leaf & 1) == 0
            ? "every"
            : "t-" + leaf + "-" + r;
        page.appendRow(recordKey++, new long[] {leaf * 100L + r, 0L}, new boolean[] {false, false},
            new String[] {null, title}, new boolean[] {true, true}, new boolean[] {false, false},
            new boolean[] {false, false}, new boolean[] {false, false});
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final int[] idArr = new int[encoded.columnSegmentIds().length];
      final long[] offArr = new long[idArr.length];
      for (int i = 0; i < idArr.length; i++) {
        idArr[i] = encoded.columnSegmentIds()[i];
        offArr[i] = nextOffset;
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1 + encoded.segments()[i].length;
      }
      directories.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), idArr, offArr, new byte[idArr.length][]));
    }
    final AtomicInteger fetched = new AtomicInteger();
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        if (wanted[i] != Constants.NULL_ID_LONG) {
          out[i] = segmentsByOffset.get(wanted[i]);
          if (out[i] != null) {
            fetched.incrementAndGet();
          }
        }
      }
      return out;
    };
    return new Fixture(new ProjectionColumnStore(directories), fetcher, fetched);
  }

  private static final String[] LITERALS =
      {"t-0-1", "t-5-17", "every", "t-63-3", "t-64-3", "t-69-31", "absent-1", "t-9-99", "t-33-0", "nope"};

  private static long[] hashes() {
    final long[] h = new long[LITERALS.length];
    for (int i = 0; i < h.length; i++) {
      h[i] = ProjectionIndexColumnSegmentCodec.bloomHash(LITERALS[i].getBytes(StandardCharsets.UTF_8));
    }
    return h;
  }

  private static int kept(final long[] m) {
    int bits = 0;
    for (final long w : m) {
      bits += Long.bitCount(w);
    }
    return bits;
  }

  private static boolean bit(final long[] m, final int leaf) {
    return (m[leaf >>> 6] & 1L << (leaf & 63)) != 0;
  }

  @Test
  @DisplayName("applyBloomPruneMany narrows every mask exactly like one applyBloomPrune per literal (chain path)")
  void manyLiteralsAgreeWithOneAtATime() {
    final Fixture f = buildFixture();
    final long[] hashes = hashes();
    final long[][] single = new long[hashes.length][];
    long singleDropped = 0;
    for (int j = 0; j < hashes.length; j++) {
      single[j] = ProjectionColumnScan.allKeptMask(LEAVES);
      singleDropped += f.store().applyBloomPrune(1, hashes[j], single[j], f.fetcher());
    }
    final long[][] many = new long[hashes.length][];
    for (int j = 0; j < hashes.length; j++) {
      many[j] = ProjectionColumnScan.allKeptMask(LEAVES);
    }
    final long manyDropped = f.store().applyBloomPruneMany(1, hashes, many, f.fetcher());
    assertEquals(singleDropped, manyDropped, "the same bits are cleared in total");
    for (int j = 0; j < hashes.length; j++) {
      assertArrayEquals(single[j], many[j], "literal " + LITERALS[j] + " keeps the same leaves either way");
    }
    // Ground truth on the evidence's no-false-negative side: the home leaves survive.
    assertTrue(bit(many[0], 0), "t-0-1 lives on leaf 0");
    assertTrue(bit(many[1], 5), "t-5-17 lives on leaf 5");
    assertTrue(bit(many[3], 63) && bit(many[4], 64), "leaves on both sides of the word boundary survive");
    assertTrue(bit(many[5], 69), "the last leaf survives");
    for (int leaf = 0; leaf < LEAVES; leaf += 2) {
      assertTrue(bit(many[2], leaf), "'every' lives on every even leaf: " + leaf);
    }
    assertTrue(kept(many[6]) < LEAVES / 2 && kept(many[9]) < LEAVES / 2, "absent literals are pruned somewhere");
    // A mask already narrowed stays narrowed: bits are only ever cleared, never set.
    final long[] narrowed = new long[(LEAVES + 63) >>> 6];
    narrowed[0] = 1L << 5;
    f.store().applyBloomPruneMany(1, new long[] {hashes[1]}, new long[][] {narrowed}, f.fetcher());
    assertEquals(1, kept(narrowed), "t-5-17 on a mask that only kept leaf 5");
    assertThrows(IllegalArgumentException.class,
        () -> f.store().applyBloomPruneMany(1, hashes, new long[hashes.length - 1][], f.fetcher()),
        "hashes and masks must pair up");
  }

  @Test
  @DisplayName("A tree of string equalities over one column is priced in ONE fingerprint walk")
  void orOfEqualitiesPricesInOneWalk() {
    final Fixture f = buildFixture();
    final ColumnPredicate[] leaves = new ColumnPredicate[6];
    final byte[] program = new byte[2 * leaves.length - 1];
    for (int i = 0; i < leaves.length; i++) {
      leaves[i] = ColumnPredicate.stringEq(1, LITERALS[i].getBytes(StandardCharsets.UTF_8));
    }
    // Left-deep OR chain: 0 1 OR 2 OR 3 OR ...
    int at = 0;
    program[at++] = 0;
    for (int i = 1; i < leaves.length; i++) {
      program[at++] = (byte) i;
      program[at++] = PredicateTree.OP_OR;
    }
    final PredicateTree tree = PredicateTree.of(leaves, program);
    final long[] batched = ProjectionColumnScan.predicateKeepMask(f.store(), new ColumnPredicate[0], tree, f.fetcher());
    // The fingerprint chain is one segment per leaf, fetched ONCE for all six literals.
    assertEquals(LEAVES, f.fetchedSegments().get(), "six equalities over one column fetch the chain once");
    // Parity with the union of single-literal masks.
    final long[] expected = new long[(LEAVES + 63) >>> 6];
    final Fixture g = buildFixture();
    for (final ColumnPredicate leaf : leaves) {
      final long[] one = ProjectionColumnScan.allKeptMask(LEAVES);
      g.store().applyBloomPrune(1, ProjectionIndexColumnSegmentCodec.bloomHash(leaf.stringLitBytes), one, g.fetcher());
      for (int w = 0; w < expected.length; w++) {
        expected[w] |= one[w];
      }
    }
    assertArrayEquals(expected, batched, "the batched OR equals the union of the single-literal masks");
    assertTrue(bit(batched, 0) && bit(batched, 5) && bit(batched, 63) && bit(batched, 64) && bit(batched, 69),
        "every home leaf survives the union");
    // Mixed trees: a numeric zone leaf AND a string equality — the string leaf still prices alone.
    final PredicateTree mixed = PredicateTree.of(
        new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.GE, 6_400L),
            ColumnPredicate.stringEq(1, "t-64-3".getBytes(StandardCharsets.UTF_8))},
        new byte[] {0, 1, PredicateTree.OP_AND});
    final long[] and = ProjectionColumnScan.predicateKeepMask(buildFixture().store(), new ColumnPredicate[0], mixed,
        buildFixture().fetcher());
    assertTrue(and != null && bit(and, 64), "leaf 64 survives the conjunction");
    for (int leaf = 0; leaf < 64; leaf++) {
      assertTrue(!bit(and, leaf), "the zone excludes leaf " + leaf);
    }
  }

  @Test
  @DisplayName("A leaf the program references twice is folded from its ORIGINAL evidence both times")
  void repeatedLeafReferenceDoesNotAliasItsMask() {
    final Fixture f = buildFixture();
    // (col0 >= 6400 AND title = 'every') OR col0 >= 6400 == col0 >= 6400: leaves 64..69. An
    // implementation that folds the first reference in place and then reads it again answers with
    // the CONJUNCTION (even leaves 64, 66, 68 only) — three leaves silently dropped.
    final ColumnPredicate[] leaves = {ColumnPredicate.numeric(0, Op.GE, 6_400L),
        ColumnPredicate.stringEq(1, "every".getBytes(StandardCharsets.UTF_8))};
    final PredicateTree tree =
        PredicateTree.of(leaves, new byte[] {0, 1, PredicateTree.OP_AND, 0, PredicateTree.OP_OR});
    final long[] keep = ProjectionColumnScan.predicateKeepMask(f.store(), new ColumnPredicate[0], tree, f.fetcher());
    assertTrue(keep != null);
    assertEquals(6, kept(keep), "col0 >= 6400 alone decides: six leaves");
    for (int leaf = 64; leaf < LEAVES; leaf++) {
      assertTrue(bit(keep, leaf), "leaf " + leaf + " must survive (col0 >= 6400 OR ...)");
    }
    // The mirror image: 0 OR (0 AND 1), the repeated reference on the right.
    final PredicateTree mirror =
        PredicateTree.of(leaves, new byte[] {0, 0, 1, PredicateTree.OP_AND, PredicateTree.OP_OR});
    assertArrayEquals(keep, ProjectionColumnScan.predicateKeepMask(buildFixture().store(), new ColumnPredicate[0],
        mirror, buildFixture().fetcher()));
  }

  @Test
  @DisplayName("zoneStabSorted marks exactly the leaves whose zone admits each value")
  void zoneStabbingIsExactOnDisjointZones() {
    final Fixture f = buildFixture();
    // Values: inside leaf 0, inside leaf 5 (twice: 517 and 531 = its last row), a gap value between
    // leaves (leaf 63's range ends at 6331; 6350 is in no zone), leaf 64's first, leaf 69's last.
    final long[] values = {0L, 517L, 531L, 6_350L, 6_400L, 6_931L};
    final int[] home = {0, 5, 5, -1, 64, 69};
    final long[][] keeps = new long[values.length][(LEAVES + 63) >>> 6];
    final long set = ProjectionColumnScan.zoneStabSorted(f.store(), 0, values, keeps);
    assertEquals(5, set, "one leaf per in-zone value, none for the gap value");
    for (int j = 0; j < values.length; j++) {
      for (int leaf = 0; leaf < LEAVES; leaf++) {
        assertEquals(leaf == home[j], bit(keeps[j], leaf), "value " + values[j] + " on leaf " + leaf);
      }
    }
    assertEquals(0, f.fetchedSegments().get(), "zone stabbing reads descriptors only — no segment fetch");
    // Masks are OR-ed into: a pre-set bit survives.
    final long[][] preset = new long[1][(LEAVES + 63) >>> 6];
    preset[0][1] = 1L << (69 - 64);
    ProjectionColumnScan.zoneStabSorted(f.store(), 0, new long[] {0L}, preset);
    assertTrue(bit(preset[0], 0) && bit(preset[0], 69), "stabbing only sets bits");
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionColumnScan.zoneStabSorted(f.store(), 0, new long[] {5L, 5L}, new long[2][2]),
        "values must be strictly ascending");
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionColumnScan.zoneStabSorted(f.store(), 1, new long[] {5L}, new long[1][2]),
        "a string column has no numeric zone");
  }

  @Test
  @DisplayName("Zone pruning through the memoized index answers like the descriptor walk")
  void zonePruneUsesTheMemoizedIndex() {
    final Fixture f = buildFixture();
    // col0 >= 6400 keeps leaves 64..69; col0 < 100 keeps leaf 0; both are exact on disjoint zones.
    final long[] ge = ProjectionColumnScan.predicateKeepMask(f.store(),
        new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.GE, 6_400L)}, null, f.fetcher());
    assertEquals(6, kept(ge));
    for (int leaf = 64; leaf < LEAVES; leaf++) {
      assertTrue(bit(ge, leaf), "leaf " + leaf + " kept by >= 6400");
    }
    final long[] lt = ProjectionColumnScan.predicateKeepMask(f.store(),
        new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.LT, 100L)}, null, f.fetcher());
    assertEquals(1, kept(lt));
    assertTrue(bit(lt, 0));
    assertEquals(0, f.fetchedSegments().get(), "zone pruning never fetches a segment");
  }

  private static long[] copy(final long[] m) {
    return Arrays.copyOf(m, m.length);
  }
}
