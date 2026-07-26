/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.page;

import io.sirix.node.DeltaVarIntCodec;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Measures how many unique per-record offset tables a typical DFS-shredded KVL page
 * actually contains — the precondition for the "per-kind offset-table template" idea.
 *
 * <p>The test simulates a page of 500 ObjectNode-style records (10 fields each) where
 * the varint widths of the structural delta fields follow realistic DFS-order
 * distributions. If most slots end up with one of only a few unique offset tables,
 * dedup saves (FIELD_COUNT - 1) bytes × (slotCount - templateCount) bytes per page.
 *
 * <p>Also reports savings assuming a 32-KB page with 500 slots and compares against
 * the claimed 5-15 % page-size reduction goal.
 */
final class OffsetTableRedundancyProbe {

  /** 10 fields for ObjectNode (matches {@code NodeFieldLayout.OBJECT_FIELD_COUNT}). */
  private static final int OBJECT_FIELD_COUNT = 10;

  @Test
  void measureRedundancyOnDfsObjectNodePage() {
    final int slotsPerPage = 500;
    final PageStats stats = simulateDfsObjectNodePage(slotsPerPage);

    System.out.println("=== Offset-table redundancy on simulated DFS ObjectNode page ===");
    System.out.printf("Slots:                       %d%n", slotsPerPage);
    System.out.printf("Unique offset tables:        %d%n", stats.uniqueTemplates);
    System.out.printf("Bytes in offset tables:      %d  (FIELD_COUNT=%d, 1 byte/entry)%n",
        stats.offsetTableBytes, OBJECT_FIELD_COUNT);
    System.out.printf("Bytes for per-slot template id: %d  (1 byte × slots)%n", slotsPerPage);
    System.out.printf("Bytes for template table:    %d  (uniqueTemplates × FIELD_COUNT)%n",
        stats.uniqueTemplates * OBJECT_FIELD_COUNT);
    System.out.printf("After dedup:                 %d bytes (%.1f%% of baseline)%n",
        stats.afterDedup(slotsPerPage), 100.0 * stats.afterDedup(slotsPerPage) / stats.offsetTableBytes);
    System.out.printf("Savings per page:            %d bytes%n",
        stats.offsetTableBytes - stats.afterDedup(slotsPerPage));

    // Assume 32 KB fully-packed page, estimate offset-table fraction and page-level savings.
    final int estimatedPageBytes = 32 * 1024;
    final double pageReductionPct = 100.0 * (stats.offsetTableBytes - stats.afterDedup(slotsPerPage))
        / estimatedPageBytes;
    System.out.printf("Est. page size reduction:    %.2f%% of a 32 KB page%n", pageReductionPct);

    // Precondition for the refactor: save at least 10% of offset-table bytes.
    assertTrue(
        stats.afterDedup(slotsPerPage) < stats.offsetTableBytes * 0.5,
        "Dedup must cut offset-table size by >=50% to be worth the format bump; got "
            + stats.afterDedup(slotsPerPage) + " / " + stats.offsetTableBytes);
  }

  @Test
  void measureRedundancyOnUniformRecordPage() {
    // All 500 slots are exact twins — worst-case variance model where every record
    // has the same widths. Expected: 1 template, maximal savings.
    final int slotsPerPage = 500;
    final long[][] tables = new long[slotsPerPage][];
    for (int i = 0; i < slotsPerPage; i++) {
      tables[i] = typicalObjectOffsetTable(1, 1, 1, 1, 1, 1, 1);
    }
    final PageStats stats = compute(tables);
    System.out.println("=== Uniform offset-table page (bound) ===");
    System.out.printf("Unique templates: %d (expect 1)%n", stats.uniqueTemplates);
    System.out.printf("Savings: %d bytes (expect ~%d)%n",
        stats.offsetTableBytes - stats.afterDedup(slotsPerPage),
        (OBJECT_FIELD_COUNT - 1) * slotsPerPage - OBJECT_FIELD_COUNT);
    assertTrue(stats.uniqueTemplates == 1);
  }

  // ==================== Simulation ====================

  /**
   * Generates a realistic offset table for each slot assuming the varint widths of
   * parentKey, rightSib, leftSib, firstChild, lastChild, prevRev, lastModRev are
   * drawn from a distribution that matches a DFS shred of bench JSON records.
   *
   * <p>Bench data: 10M records in a single array; each record is a 5-field JSON
   * object. Parent keys are typically 1–2 bytes after zig-zag. Sibling links are
   * null or ±1 (1 byte). Revisions are 1 byte. So most slots produce the *same*
   * offset table, with a few variations for edge slots (first record, last record
   * in an array, etc.).
   */
  private static PageStats simulateDfsObjectNodePage(final int slotCount) {
    final long[][] tables = new long[slotCount][];
    final java.util.Random rnd = new java.util.Random(0xDF5_1EAFL);
    for (int i = 0; i < slotCount; i++) {
      // Pick varint widths based on the realistic shape.
      final int parentW = pickWidth(rnd, 90, 9, 1);       // 90% 1B, 9% 2B, 1% 3B
      final int rightSibW = pickWidth(rnd, 95, 4, 1);     // mostly null or ±1 (1B)
      final int leftSibW = pickWidth(rnd, 95, 4, 1);
      final int firstChildW = pickWidth(rnd, 95, 4, 1);
      final int lastChildW = pickWidth(rnd, 95, 4, 1);
      final int prevRevW = 1;                             // revision = 1 fits in 1B
      final int lastModRevW = 1;
      tables[i] = typicalObjectOffsetTable(parentW, rightSibW, leftSibW, firstChildW,
          lastChildW, prevRevW, lastModRevW);
    }
    return compute(tables);
  }

  private static int pickWidth(final java.util.Random rnd, final int pct1B, final int pct2B, final int pct3B) {
    final int r = rnd.nextInt(100);
    if (r < pct1B) return 1;
    if (r < pct1B + pct2B) return 2;
    if (r < pct1B + pct2B + pct3B) return 3;
    return 4;
  }

  /**
   * Build an offset table for an ObjectNode given the varint widths of each
   * structural field. Fixed-size fields (hash = 8B) are at known offsets.
   */
  private static long[] typicalObjectOffsetTable(final int parentW, final int rightSibW,
      final int leftSibW, final int firstChildW, final int lastChildW,
      final int prevRevW, final int lastModRevW) {
    final long[] offsets = new long[OBJECT_FIELD_COUNT];
    int pos = 0;
    offsets[0] = pos; pos += parentW;       // parentKey
    offsets[1] = pos; pos += rightSibW;     // rightSib
    offsets[2] = pos; pos += leftSibW;      // leftSib
    offsets[3] = pos; pos += firstChildW;   // firstChild
    offsets[4] = pos; pos += lastChildW;    // lastChild
    offsets[5] = pos; pos += prevRevW;      // prevRev
    offsets[6] = pos; pos += lastModRevW;   // lastModRev
    offsets[7] = pos; pos += 8;             // hash (fixed 8B)
    offsets[8] = pos; pos += 1;             // childCount (1B varint typical)
    offsets[9] = pos;                        // descendantCount
    return offsets;
  }

  // ==================== Stats computation ====================

  private static final class PageStats {
    final int uniqueTemplates;
    final int offsetTableBytes;

    PageStats(final int uniqueTemplates, final int offsetTableBytes) {
      this.uniqueTemplates = uniqueTemplates;
      this.offsetTableBytes = offsetTableBytes;
    }

    /** Bytes after dedup: template table + 1 byte template id per slot. */
    int afterDedup(final int slots) {
      return uniqueTemplates * OBJECT_FIELD_COUNT + slots;
    }
  }

  private static PageStats compute(final long[][] tables) {
    final Map<String, Integer> seen = new LinkedHashMap<>();
    for (final long[] table : tables) {
      seen.computeIfAbsent(key(table), _ -> seen.size());
    }
    return new PageStats(seen.size(), tables.length * OBJECT_FIELD_COUNT);
  }

  private static String key(final long[] table) {
    final StringBuilder sb = new StringBuilder();
    for (final long off : table) {
      sb.append(off).append(',');
    }
    return sb.toString();
  }

  // Force at least one reference to DeltaVarIntCodec so the test file participates in
  // the same package reachability as the runtime write path (keeps future link checks
  // honest if someone accidentally deletes unused imports).
  @Test
  void linkCheck() {
    final Map<String, Long> sizes = new HashMap<>();
    sizes.put("delta", (long) DeltaVarIntCodec.computeDeltaEncodedWidth(42L, 0L));
    assertTrue(sizes.get("delta") >= 1L);
  }
}
