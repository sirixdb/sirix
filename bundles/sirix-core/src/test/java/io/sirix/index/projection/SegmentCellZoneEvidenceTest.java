/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Descriptor-level evidence over a {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_SEGMENT}
 * column. Its leaves carry a zone over packed {@code (segment, id)} cells, and a leaf never
 * straddles a segment, so the zone is a containment test for a cell exactly as a global column's is
 * for an id: {@link ProjectionColumnScan#zoneStabSorted} must admit a cell only into the leaves of
 * its own segment whose range covers it, and a resolved equality
 * ({@link ColumnPredicate#segmentScopedEquality}) must prune leaves from the descriptors BEFORE any
 * column bytes are fetched — the fetch count is the witness, because a slice-level skip answers the
 * same rows after reading the whole column.
 *
 * <p>
 * Three segments, several leaves each, more leaves than a mask word holds. Every expectation is
 * computed from the cells the leaves ACTUALLY hold after conversion, so nothing here assumes the
 * order in which a segment's dictionary minted its ids.
 * </p>
 */
final class SegmentCellZoneEvidenceTest {

  private static final int SEGMENTS = 3;

  private static final int LEAVES_PER_SEGMENT = 24; // 72 leaves: the masks cross a word boundary

  private static final int LEAVES = SEGMENTS * LEAVES_PER_SEGMENT;

  private static final int ROWS = 32;

  /** A value every EVEN leaf of segments 0 and 2 holds; segment 1 never sees it. */
  private static final String SHARED = "every";

  /** Segment 2's last leaf holds nothing but {@link #SHARED}: the one collapsed zone. */
  private static final int COLLAPSED_LEAF = 2 * LEAVES_PER_SEGMENT + LEAVES_PER_SEGMENT - 1;

  /** One dictionary per segment: the same bytes get the same id, a new value the next one. */
  private static final class SegmentEncoder implements GlobalValueDictionaryEncoder {
    private final Map<String, Integer> ids = new HashMap<>();

    @Override
    public int intern(final byte[] source, final int offset, final int length) {
      return intern(new String(source, offset, length, StandardCharsets.UTF_8));
    }

    @Override
    public int intern(final String value) {
      return ids.computeIfAbsent(value, v -> ids.size() + 1);
    }

    int idOf(final String value) {
      final Integer id = ids.get(value);
      return id == null
          ? -1
          : id;
    }
  }

  private record Fixture(ProjectionColumnStore store, ColumnSegmentFetcher fetcher, AtomicInteger fetches,
      long[][] cellsByLeaf, SegmentEncoder[] encoders) {

    int segmentOf(final int leaf) {
      return leaf / LEAVES_PER_SEGMENT;
    }

    /** The zone rule, spelled out over the data: does the leaf's cell range cover {@code cell}? */
    boolean zoneAdmits(final int leaf, final long cell) {
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (final long c : cellsByLeaf[leaf]) {
        min = Math.min(min, c);
        max = Math.max(max, c);
      }
      return cell >= min && cell <= max;
    }

    boolean holds(final int leaf, final long cell) {
      for (final long c : cellsByLeaf[leaf]) {
        if (c == cell) {
          return true;
        }
      }
      return false;
    }

    /**
     * The literal's cell in every segment, {@link ColumnPredicate#SEGMENT_LITERAL_ABSENT} where
     * unknown.
     */
    long[] literalCells(final String value) {
      final long[] cells = new long[SEGMENTS];
      for (int segment = 0; segment < SEGMENTS; segment++) {
        final int id = encoders[segment].idOf(value);
        cells[segment] = id < 0
            ? ColumnPredicate.SEGMENT_LITERAL_ABSENT
            : ProjectionIndexRowGroupPage.packSegmentCell(segment, id);
      }
      return cells;
    }
  }

  private static String valueOf(final int leaf, final int row) {
    if (leaf == COLLAPSED_LEAF) {
      return SHARED;
    }
    final int segment = leaf / LEAVES_PER_SEGMENT;
    final int inSegment = leaf % LEAVES_PER_SEGMENT;
    if (row == 0 && segment != 1 && (inSegment & 1) == 0) {
      return SHARED;
    }
    return "v-" + segment + "-" + inSegment + "-" + row;
  }

  private static Fixture buildFixture() {
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(LEAVES);
    final long[][] cellsByLeaf = new long[LEAVES][];
    final SegmentEncoder[] encoders = new SegmentEncoder[SEGMENTS];
    for (int segment = 0; segment < SEGMENTS; segment++) {
      encoders[segment] = new SegmentEncoder();
    }
    long nextOffset = 1_000;
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      final int segment = leaf / LEAVES_PER_SEGMENT;
      final ProjectionIndexRowGroupPage page =
          new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT});
      long recordKey = leaf * 100_000L + 1;
      for (int r = 0; r < ROWS; r++) {
        page.appendRow(recordKey++, new long[] {0L}, new boolean[] {false}, new String[] {valueOf(leaf, r)},
            new boolean[] {true}, new boolean[] {false}, new boolean[] {false});
      }
      page.convertStringDictColumnToSegment(0, encoders[segment], segment);
      // The lane is sized to capacity; only the first rowCount slots are cells.
      cellsByLeaf[leaf] = Arrays.copyOf(page.numericColumn(0), page.getRowCount());
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
    final AtomicInteger fetches = new AtomicInteger();
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        if (wanted[i] != Constants.NULL_ID_LONG) {
          out[i] = segmentsByOffset.get(wanted[i]);
          if (out[i] != null) {
            fetches.incrementAndGet();
          }
        }
      }
      return out;
    };
    final ProjectionColumnStore store = new ProjectionColumnStore(directories);
    assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT, store.columnKind(0),
        "the fixture must present a segment-scoped column, or nothing below tests the segment arms");
    return new Fixture(store, fetcher, fetches, cellsByLeaf, encoders);
  }

  private static boolean bit(final long[] m, final int leaf) {
    return (m[leaf >>> 6] & 1L << (leaf & 63)) != 0;
  }

  private static int kept(final long[] m) {
    int bits = 0;
    for (final long w : m) {
      bits += Long.bitCount(w);
    }
    return bits;
  }

  @Test
  @DisplayName("zoneStabSorted admits a cell only into leaves of its own segment whose range covers it")
  void stabbingIsSegmentIsolatedContainment() {
    final Fixture f = buildFixture();
    // Cells to stab: the shared value in segments 0 and 2, a value private to one leaf of segment 1,
    // and the same ID as that private value re-packed into segments 0 and 2 — a stab that ignored
    // the segment bits would admit those into segment 1's leaves.
    final int privateId = f.encoders[1].idOf(valueOf(LEAVES_PER_SEGMENT + 5, 7));
    assertTrue(privateId > 0, "the fixture's private value was minted");
    final long[] values = {ProjectionIndexRowGroupPage.packSegmentCell(0, f.encoders[0].idOf(SHARED)),
        ProjectionIndexRowGroupPage.packSegmentCell(0, privateId),
        ProjectionIndexRowGroupPage.packSegmentCell(1, privateId),
        ProjectionIndexRowGroupPage.packSegmentCell(2, privateId),
        ProjectionIndexRowGroupPage.packSegmentCell(2, f.encoders[2].idOf(SHARED))};
    Arrays.sort(values);
    final int words = (LEAVES + 63) >>> 6;
    final long[][] keeps = new long[values.length][words];
    final long set = ProjectionColumnScan.zoneStabSorted(f.store(), 0, values, keeps);
    assertEquals(0, f.fetches().get(), "stabbing reads descriptors, never column bytes");

    long expectedSet = 0;
    for (int j = 0; j < values.length; j++) {
      final int segment = ProjectionIndexRowGroupPage.segmentOfCell(values[j]);
      for (int leaf = 0; leaf < LEAVES; leaf++) {
        final boolean expected = f.zoneAdmits(leaf, values[j]);
        assertEquals(expected, bit(keeps[j], leaf), "value " + j + " (segment " + segment + ") at leaf " + leaf);
        if (expected) {
          expectedSet++;
          assertEquals(segment, f.segmentOf(leaf),
              "a cell stabbed a leaf of another segment: leaf " + leaf + " for segment " + segment);
        }
        if (f.holds(leaf, values[j])) {
          assertTrue(bit(keeps[j], leaf), "a leaf holding the cell must be admitted: leaf " + leaf);
        }
      }
    }
    assertEquals(expectedSet, set, "the reported bit count");
    // The whole point, pinned by name: the shared value lives on the even leaves of segments 0 and 2
    // (and the collapsed leaf) — twelve leaves in segment 0, thirteen in segment 2 — and the private
    // value's id admits SOME leaf in its own segment and, packed into segments 0 and 2, only leaves
    // of THOSE segments (containment there is a coincidence of ids, never a cross-segment leak).
    final int sharedIn0 =
        Arrays.binarySearch(values, ProjectionIndexRowGroupPage.packSegmentCell(0, f.encoders[0].idOf(SHARED)));
    final int sharedIn2 =
        Arrays.binarySearch(values, ProjectionIndexRowGroupPage.packSegmentCell(2, f.encoders[2].idOf(SHARED)));
    assertTrue(kept(keeps[sharedIn0]) >= LEAVES_PER_SEGMENT / 2, "the shared value's even leaves in segment 0");
    assertTrue(kept(keeps[sharedIn2]) >= LEAVES_PER_SEGMENT / 2 + 1, "segment 2's even leaves plus the collapsed one");
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      if (f.segmentOf(leaf) == 1) {
        assertFalse(bit(keeps[sharedIn0], leaf) || bit(keeps[sharedIn2], leaf),
            "segment 1 never held the shared value and must not be stabbed for it: leaf " + leaf);
      }
    }
    final int privateIn1 = Arrays.binarySearch(values, ProjectionIndexRowGroupPage.packSegmentCell(1, privateId));
    assertTrue(bit(keeps[privateIn1], LEAVES_PER_SEGMENT + 5), "the private value's home leaf is admitted");
  }

  @Test
  @DisplayName("a segment-scoped equality prunes leaves from the descriptors, before any column bytes are read")
  void anEqualityPrunesAtTheDescriptor() {
    final Fixture f = buildFixture();
    final long[] literal = f.literalCells(SHARED);
    assertEquals(ColumnPredicate.SEGMENT_LITERAL_ABSENT, literal[1], "segment 1 provably lacks the value");
    final ColumnPredicate eq = ColumnPredicate.segmentScopedEquality(0, Op.EQ, literal);
    final long[] keep = ProjectionColumnScan.predicateKeepMask(f.store(), new ColumnPredicate[] {eq}, f.fetcher());
    assertNotNull(keep, "an equality over a segment column must prune on the descriptor zones");
    assertEquals(0, f.fetches().get(), "descriptor-level pruning must not fetch a single column segment");
    int keptLeaves = 0;
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      final int segment = f.segmentOf(leaf);
      final boolean expected =
          literal[segment] != ColumnPredicate.SEGMENT_LITERAL_ABSENT && f.zoneAdmits(leaf, literal[segment]);
      assertEquals(expected, bit(keep, leaf), "leaf " + leaf + " of segment " + segment);
      if (expected) {
        keptLeaves++;
      }
      // Soundness, independently of the zone rule: a dropped leaf holds no matching row.
      if (!bit(keep, leaf)) {
        assertFalse(literal[segment] != ColumnPredicate.SEGMENT_LITERAL_ABSENT && f.holds(leaf, literal[segment]),
            "a leaf holding the literal was pruned: leaf " + leaf);
      }
      if (segment == 1) {
        assertFalse(bit(keep, leaf), "every leaf of the segment that lacks the value is dropped: leaf " + leaf);
      }
    }
    assertEquals(keptLeaves, kept(keep));
    assertTrue(keptLeaves > 0 && keptLeaves < LEAVES, "the prune is neither empty nor total: " + keptLeaves);
    assertTrue(bit(keep, COLLAPSED_LEAF), "the collapsed leaf holds only the literal and stays");
  }

  @Test
  @DisplayName("a segment-scoped inequality prunes exactly the zones collapsed onto the literal's cell")
  void anInequalityPrunesOnlyCollapsedZones() {
    final Fixture f = buildFixture();
    final ColumnPredicate ne = ColumnPredicate.segmentScopedEquality(0, Op.NE, f.literalCells(SHARED));
    final long[] keep = ProjectionColumnScan.predicateKeepMask(f.store(), new ColumnPredicate[] {ne}, f.fetcher());
    assertNotNull(keep, "the collapsed leaf gives the NE rule a positive witness");
    assertEquals(0, f.fetches().get(), "descriptor-level pruning must not fetch a single column segment");
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      assertEquals(leaf != COLLAPSED_LEAF, bit(keep, leaf), "leaf " + leaf);
    }
    // A value no segment holds: NE keeps everything, so nothing is pruned and the mask stays null.
    final ColumnPredicate neAbsent = ColumnPredicate.segmentScopedEquality(0, Op.NE, f.literalCells("nowhere"));
    assertNull(ProjectionColumnScan.predicateKeepMask(f.store(), new ColumnPredicate[] {neAbsent}, f.fetcher()),
        "NE against a value no segment holds prunes nothing");
    // ... and EQ against it prunes every leaf.
    final ColumnPredicate eqAbsent = ColumnPredicate.segmentScopedEquality(0, Op.EQ, f.literalCells("nowhere"));
    final long[] none =
        ProjectionColumnScan.predicateKeepMask(f.store(), new ColumnPredicate[] {eqAbsent}, f.fetcher());
    assertNotNull(none);
    assertEquals(0, kept(none), "EQ against a value no segment holds keeps no leaf");
    assertEquals(0, f.fetches().get());
  }

  @Test
  @DisplayName("a per-value verdict predicate over the same column still takes no descriptor-level prune")
  void aVerdictPredicateIsNotZonePruned() {
    final Fixture f = buildFixture();
    // A containment verdict is per VALUE; the cell range says nothing about it, and the arm must not
    // pretend otherwise — the mask stays null and the slices decide.
    final byte[] literal = SHARED.getBytes(StandardCharsets.UTF_8);
    final SegmentCellVerdicts verdicts =
        new SegmentCellVerdicts(cell -> Boolean.TRUE, Op.STR_CONTAINS, literal, SEGMENTS);
    final ColumnPredicate contains = ColumnPredicate.segmentCellVerdict(0, Op.STR_CONTAINS, literal, verdicts);
    assertNull(ProjectionColumnScan.predicateKeepMask(f.store(), new ColumnPredicate[] {contains}, f.fetcher()));
    assertEquals(0, f.fetches().get());
  }

  @Test
  @DisplayName("zoneStabSorted still refuses a kind whose long lane is not a value space")
  void stabbingRefusesAPerLeafDictionaryColumn() {
    final Map<Long, byte[]> unused = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(1);
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT});
    page.appendRow(1L, new long[] {0L}, new boolean[] {false}, new String[] {"x"}, new boolean[] {true},
        new boolean[] {false}, new boolean[] {false});
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(page.serialize());
    final int[] idArr = encoded.columnSegmentIds().clone();
    final long[] offArr = new long[idArr.length];
    for (int i = 0; i < idArr.length; i++) {
      offArr[i] = 1_000L + i;
      unused.put(offArr[i], encoded.segments()[i]);
    }
    directories.add(new RowGroupDirectory(1, encoded.descriptor(), idArr, offArr, new byte[idArr.length][]));
    final ProjectionColumnStore store = new ProjectionColumnStore(directories);
    final IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> ProjectionColumnScan.zoneStabSorted(store, 0, new long[] {1L}, new long[1][1]));
    assertTrue(refused.getMessage().contains("segment-string"), refused.getMessage());
  }
}
