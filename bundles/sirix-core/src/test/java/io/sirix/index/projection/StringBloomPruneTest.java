/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The string-fingerprint prune ({@link ProjectionIndexColumnSegmentCodec#SEG_KIND_STRING_BLOOM}): a
 * string-equality one-shot must fetch the BODY+DICT chains ONLY for leaves whose fingerprint admits
 * the literal, and the answer must be byte-identical to the unpruned scan at every selectivity —
 * per-leaf-common, single-leaf-rare, and absent ({@code validate-with-a-rare-literal}: a
 * wrong-answer bug once hid behind a common literal).
 *
 */
final class StringBloomPruneTest {

  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};

  private static final int LEAVES = 8;
  private static final int ROWS = 64;

  private record Fixture(ProjectionColumnStore store, ColumnSegmentFetcher fetcher, AtomicInteger fetchedSegments) {
  }

  /**
   * Leaf {@code L} holds titles {@code "t-L-0" .. "t-L-63"} — every value names its leaf, so a
   * literal's true home leaf set is known exactly.
   */
  private static Fixture buildFixture() {
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(LEAVES);
    long nextOffset = 1_000;
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
      long recordKey = leaf * 100_000L + 1;
      for (int r = 0; r < ROWS; r++) {
        page.appendRow(recordKey++, new long[] {leaf * 1_000L + r, 0L}, new boolean[] {false, false},
            new String[] {null, "t-" + leaf + "-" + r}, new boolean[] {true, true}, new boolean[] {false, false},
            new boolean[] {false, false}, new boolean[] {false, false});
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final List<Integer> ids = new ArrayList<>();
      final List<Long> offsets = new ArrayList<>();
      for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
        ids.add(encoded.columnSegmentIds()[i]);
        offsets.add(nextOffset);
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1 + encoded.segments()[i].length;
      }
      final int[] idArr = new int[ids.size()];
      final long[] offArr = new long[ids.size()];
      for (int i = 0; i < ids.size(); i++) {
        idArr[i] = ids.get(i);
        offArr[i] = offsets.get(i);
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

  private static long count(final Fixture f, final String literal) {
    return ProjectionColumnScan.conjunctiveCount(f.store(),
        new ColumnPredicate[] {ColumnPredicate.stringEq(1, literal.getBytes(StandardCharsets.UTF_8))}, f.fetcher());
  }

  @Test
  @DisplayName("Counts agree at every selectivity, and a rare literal fetches only its home leaf")
  void rareLiteralFetchesOneLeaf() {
    // Selectivity sweep: one row on one leaf; absent everywhere; every leaf's first row.
    final Fixture pruned = buildFixture();
    assertEquals(1, count(pruned, "t-5-17"), "single-leaf literal");
    final int afterRare = pruned.fetchedSegments().get();
    // The fingerprint chain is LEAVES fetches; the surviving leaf adds its BODY + DICT. False
    // positives can admit a few extra leaves — the bound proves pruning fired, not luck.
    assertTrue(afterRare <= LEAVES + 2 * 3, "rare literal fetched " + afterRare + " segments — pruning did not fire");

    assertEquals(0, count(pruned, "t-9-99"), "absent literal");
    assertEquals(0, count(pruned, "nope"), "absent literal, different shape");

    // Fresh fixture so fetch accounting starts clean; a per-leaf-present literal keeps all.
    final Fixture common = buildFixture();
    long total = 0;
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      total += count(common, "t-" + leaf + "-0");
    }
    assertEquals(LEAVES, total, "one hit per leaf");
  }

  @Test
  @DisplayName("The fingerprint itself: every stored value admitted, absents overwhelmingly rejected")
  void bloomRoundTrip() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
    for (int r = 0; r < ROWS; r++) {
      page.appendRow(r + 1, new long[] {r, 0L}, new boolean[2], new String[] {null, "v" + r},
          new boolean[] {true, true}, new boolean[2], new boolean[2], new boolean[2]);
    }
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(page.serialize());
    byte[] bloom = null;
    for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
      if (encoded.columnSegmentIds()[i] == ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(1)) {
        bloom = encoded.segments()[i];
      }
    }
    assertTrue(bloom != null, "string column must emit a fingerprint segment");
    for (int r = 0; r < ROWS; r++) {
      assertTrue(ProjectionIndexColumnSegmentCodec.bloomMayContain(bloom, ("v" + r).getBytes(StandardCharsets.UTF_8)),
          "no false negatives permitted: v" + r);
    }
    int falsePositives = 0;
    for (int r = 0; r < 1_000; r++) {
      if (ProjectionIndexColumnSegmentCodec.bloomMayContain(bloom, ("absent-" + r).getBytes(StandardCharsets.UTF_8))) {
        falsePositives++;
      }
    }
    assertTrue(falsePositives < 100,
        "false-positive rate implausibly high: " + falsePositives + "/1000 — sizing or probing is broken");
  }
}
