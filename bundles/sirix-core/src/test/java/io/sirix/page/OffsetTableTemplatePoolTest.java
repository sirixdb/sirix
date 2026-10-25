/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link OffsetTableTemplatePool} — the per-page offset-table template codec used by
 * the slotted page {@link io.sirix.BinaryEncodingVersion} on disk for no-LZ4 storage parity.
 *
 * <p>
 * Covers:
 * <ol>
 * <li>Build/expand round trip on narrow (FIELD_COUNT &le; 7) offset tables.</li>
 * <li>Build/expand round trip on wide (FIELD_COUNT &gt; 7) offset tables.</li>
 * <li>Abort when &gt; 255 unique templates on a single page.</li>
 * <li>Single-template degenerate case (one record on a page).</li>
 * </ol>
 *
 * <p>
 * The synthetic test page simulates the slotted-page heap layout produced by
 * {@code ObjectKeyNode.writeNewRecord}: each record is {@code [kindId]
 * [offsetTable] [data]}. We never run a full {@link KeyValueLeafPage} serializer here; the pool
 * works directly on the slotted-page memory and the tests exercise the build/expand sides in
 * isolation.
 */
@DisplayName("OffsetTableTemplatePool")
final class OffsetTableTemplatePoolTest {

  @ParameterizedTest
  @CsvSource({"0, false, 0", "0, true, 0", "0, false, 32", "0, true, 32", "1, false, 0", "1, true, 0", "1, false, 32",
      "1, true, 32", "2, false, 0", "2, true, 0", "2, false, 32", "2, true, 32"})
  void collidingKeysRetainTheirOwnTemplates(final int fixture, final boolean reverse, final int padding) {
    // Eight-field tables whose high byte cancels the kind byte in the wide-key hash.
    // Repeated alternation also exercises lookup after a collision replaced the map entry.
    // The other fixtures cover a wide/narrow collision and a same-kind wide collision.
    final int[] kinds = switch (fixture) {
      case 0 -> new int[] {48, 49};
      case 1 -> new int[] {48, 29};
      case 2 -> new int[] {52, 52};
      default -> throw new AssertionError(fixture);
    };
    final int[][] patterns = switch (fixture) {
      case 0 -> new int[][] {{0, 1, 2, 3, 4, 5, 6, 8}, {0, 1, 2, 3, 4, 5, 6, 9}};
      case 1 -> new int[][] {{216, 98, 26, 36, 83, 138, 158, 219}, {0, 1, 2, 3, 4}};
      case 2 ->
        new int[][] {{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, {21, 133, 77, 140, 187, 131, 77, 165, 9, 9, 10, 11}};
      default -> throw new AssertionError(fixture);
    };
    assertCollidingTemplatesRoundTrip(kinds, patterns, reverse, padding);
  }

  private static void assertCollidingTemplatesRoundTrip(final int[] kinds, final int[][] patterns,
      final boolean reverse, final int padding) {
    final int count = 8;
    final int[] kindIds = new int[count];
    final int[] heapOffsets = new int[count];
    int heapSize = 0;
    for (int i = 0; i < count; i++) {
      final int pattern = (i + (reverse
          ? 1
          : 0)) & 1;
      kindIds[i] = kinds[pattern];
      heapOffsets[i] = heapSize;
      heapSize += 1 + patterns[pattern].length;
    }
    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.HEAP_START + heapSize + padding);
      for (int i = 0; i < count; i++) {
        final int pattern = (i + (reverse
            ? 1
            : 0)) & 1;
        writeRecordWithPattern(page, heapOffsets[i], kindIds[i], patterns[pattern].length, patterns[pattern]);
      }
      final Long2IntOpenHashMap map = new Long2IntOpenHashMap();
      map.defaultReturnValue(-1);
      final byte[] templates = new byte[OffsetTableTemplatePool.MAX_TEMPLATES * 18];
      final byte[] slotIds = new byte[count];
      final OffsetTableTemplatePool.BuildResult result =
          OffsetTableTemplatePool.build(page, count, kindIds, heapOffsets, templates, slotIds, map);
      assertEquals(2, result.templateCount);
      final int[] templateOffsets = new int[result.templateCount + 1];
      OffsetTableTemplatePool.parseTemplateOffsets(templates, result.templatesByteLength, result.templateCount,
          templateOffsets);
      final MemorySegment expanded = arena.allocate(16);
      for (int i = 0; i < count; i++) {
        final int templateId = slotIds[i] & 0xFF;
        final int[] expected = patterns[(i + (reverse
            ? 1
            : 0)) & 1];
        assertEquals(kindIds[i], OffsetTableTemplatePool.templateKindId(templates, templateOffsets, templateId));
        assertEquals(expected.length,
            OffsetTableTemplatePool.expandTemplateTo(templates, templateOffsets, templateId, expanded, 0));
        for (int field = 0; field < expected.length; field++) {
          assertEquals(expected[field], expanded.get(ValueLayout.JAVA_BYTE, field) & 0xFF,
              "slot " + i + ", field " + field);
        }
      }
    }
  }

  @Test
  @DisplayName("empty page builds to zero templates")
  void emptyPage() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment page = arena.allocate(PageLayout.HEAP_START + 64);
      final Long2IntOpenHashMap map = new Long2IntOpenHashMap();
      map.defaultReturnValue(-1);
      final byte[] templates = new byte[64];
      final byte[] slotIds = new byte[16];
      final OffsetTableTemplatePool.BuildResult r =
          OffsetTableTemplatePool.build(page, 0, new int[0], new int[0], templates, slotIds, map);
      assertEquals(0, r.templateCount);
      assertEquals(0, r.templatesByteLength);
    }
  }

  @Test
  @DisplayName("all-identical OBJECT_NAMED_OBJECT records build to a single template")
  void uniformObjectKeyPage() {
    // Phase 4: migrated from legacy OBJECT_KEY (kindId 26, fc=10) to fused OBJECT_NAMED_OBJECT
    // (kindId 52, fc=12). Cat-1: physical layout shift (kindId + fieldCount) is the direct
    // logical consequence of the OBJECT_KEY → fused-named refactor.
    try (Arena arena = Arena.ofConfined()) {
      final int slotCount = 50;
      final int kindId = 52; // OBJECT_NAMED_OBJECT
      final int fc = NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT; // 12
      final int perRecord = 1 + fc + 5; // nodeKind + offsetTable + 5 bytes data
      final MemorySegment page = arena.allocate(PageLayout.HEAP_START + perRecord * slotCount);

      final int[] kindIds = new int[slotCount];
      final int[] heapOffs = new int[slotCount];
      for (int i = 0; i < slotCount; i++) {
        kindIds[i] = kindId;
        heapOffs[i] = i * perRecord;
        writeDummyRecord(page, heapOffs[i], kindId, fc, 0x42); // all offset tables = 0x42
      }

      final Long2IntOpenHashMap map = new Long2IntOpenHashMap();
      map.defaultReturnValue(-1);
      final byte[] templates = new byte[256 * (2 + 16)];
      final byte[] slotIds = new byte[slotCount];
      final OffsetTableTemplatePool.BuildResult r =
          OffsetTableTemplatePool.build(page, slotCount, kindIds, heapOffs, templates, slotIds, map);

      assertEquals(1, r.templateCount);
      assertEquals(2 + fc, r.templatesByteLength);
      // All slots map to template 0.
      for (int i = 0; i < slotCount; i++) {
        assertEquals(0, slotIds[i] & 0xFF);
      }
      // Template 0 header: kindId + fc.
      assertEquals(kindId, templates[0] & 0xFF);
      assertEquals(fc, templates[1] & 0xFF);
      // Template 0 body: 0x42 × fc.
      for (int i = 0; i < fc; i++) {
        assertEquals(0x42, templates[2 + i] & 0xFF);
      }
    }
  }

  @Test
  @DisplayName("mixed OBJECT_NAMED_OBJECT / ELEMENT records produce two templates")
  void mixedKindTwoTemplates() {
    // Phase 4: migrated from legacy OBJECT_KEY to fused OBJECT_NAMED_OBJECT (Cat-1).
    try (Arena arena = Arena.ofConfined()) {
      final int slotCount = 20;
      // 10 OBJECT_NAMED_OBJECT (fc=12) + 10 ELEMENT (fc=15); ELEMENT exercises the wide-key path.
      final int[] kindIds = new int[slotCount];
      final int[] heapOffs = new int[slotCount];
      final int objectKeyId = 52;
      final int elementId = 1;
      final int fcOk = NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;
      final int fcEl = NodeFieldLayout.ELEMENT_FIELD_COUNT;

      int off = 0;
      final int perObjectKey = 1 + fcOk;
      final int perElement = 1 + fcEl;
      for (int i = 0; i < 10; i++) {
        kindIds[i] = objectKeyId;
        heapOffs[i] = off;
        off += perObjectKey;
      }
      for (int i = 10; i < 20; i++) {
        kindIds[i] = elementId;
        heapOffs[i] = off;
        off += perElement;
      }
      final MemorySegment page = arena.allocate(PageLayout.HEAP_START + off);
      for (int i = 0; i < 10; i++) {
        writeDummyRecord(page, heapOffs[i], objectKeyId, fcOk, 0x11);
      }
      for (int i = 10; i < 20; i++) {
        writeDummyRecord(page, heapOffs[i], elementId, fcEl, 0x22);
      }

      final Long2IntOpenHashMap map = new Long2IntOpenHashMap();
      map.defaultReturnValue(-1);
      final byte[] templates = new byte[256 * (2 + 16)];
      final byte[] slotIds = new byte[slotCount];
      final OffsetTableTemplatePool.BuildResult r =
          OffsetTableTemplatePool.build(page, slotCount, kindIds, heapOffs, templates, slotIds, map);

      assertEquals(2, r.templateCount);
      // First 10 slots → template 0 (OBJECT_NAMED_OBJECT), next 10 → template 1 (ELEMENT).
      for (int i = 0; i < 10; i++) {
        assertEquals(0, slotIds[i] & 0xFF);
      }
      for (int i = 10; i < 20; i++) {
        assertEquals(1, slotIds[i] & 0xFF);
      }
    }
  }

  @Test
  @DisplayName("expand round-trips every template to its original bytes")
  void expandRoundTrip() {
    // Phase 4: migrated from legacy OBJECT_KEY (fc=10) to fused OBJECT_NAMED_OBJECT (fc=12).
    // Patterns extended from 10 to 12 elements (Cat-1 — direct consequence of the field-count
    // increase, not a relaxation).
    try (Arena arena = Arena.ofConfined()) {
      final int slotCount = 30;
      final int kindId = 52;
      final int fc = NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;
      final int perRecord = 1 + fc + 3;
      final MemorySegment page = arena.allocate(PageLayout.HEAP_START + perRecord * slotCount);

      final int[] kindIds = new int[slotCount];
      final int[] heapOffs = new int[slotCount];
      // 3 distinct offset-table patterns, round-robin across slots.
      final int[][] patterns = {{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22},
          {1, 2, 3, 4, 5, 5, 5, 5, 5, 5, 5, 5}};
      for (int i = 0; i < slotCount; i++) {
        kindIds[i] = kindId;
        heapOffs[i] = i * perRecord;
        writeRecordWithPattern(page, heapOffs[i], kindId, fc, patterns[i % 3]);
      }

      final Long2IntOpenHashMap map = new Long2IntOpenHashMap();
      map.defaultReturnValue(-1);
      final byte[] templates = new byte[256 * (2 + 16)];
      final byte[] slotIds = new byte[slotCount];
      final OffsetTableTemplatePool.BuildResult r =
          OffsetTableTemplatePool.build(page, slotCount, kindIds, heapOffs, templates, slotIds, map);
      assertEquals(3, r.templateCount);

      final int[] templateOffsets = new int[r.templateCount + 1];
      OffsetTableTemplatePool.parseTemplateOffsets(templates, r.templatesByteLength, r.templateCount, templateOffsets);

      // Expand each slot's template into a scratch segment and verify bytes match.
      try (Arena scratchArena = Arena.ofConfined()) {
        final MemorySegment scratch = scratchArena.allocate(fc);
        for (int i = 0; i < slotCount; i++) {
          scratch.fill((byte) 0);
          OffsetTableTemplatePool.expandTemplateTo(templates, templateOffsets, slotIds[i] & 0xFF, scratch, 0L);
          final int[] expected = patterns[i % 3];
          for (int k = 0; k < fc; k++) {
            final int got = scratch.get(ValueLayout.JAVA_BYTE, k) & 0xFF;
            assertEquals(expected[k], got, "slot=" + i + " byte=" + k);
          }
        }
      }
    }
  }

  @Test
  @DisplayName(">256 unique templates triggers abort")
  void tooManyTemplatesAborts() {
    // Phase 4: migrated from legacy OBJECT_KEY to fused OBJECT_NAMED_OBJECT (Cat-1).
    try (Arena arena = Arena.ofConfined()) {
      final int slotCount = 300;
      final int kindId = 52;
      final int fc = NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;
      final int perRecord = 1 + fc + 0;
      final MemorySegment page = arena.allocate(PageLayout.HEAP_START + perRecord * slotCount);
      final int[] kindIds = new int[slotCount];
      final int[] heapOffs = new int[slotCount];
      for (int i = 0; i < slotCount; i++) {
        kindIds[i] = kindId;
        heapOffs[i] = i * perRecord;
        // Each record has a unique offset-table bit pattern (i is used as varying byte).
        writeDummyRecord(page, heapOffs[i], kindId, fc, i & 0xFF);
        // Distinguish pattern 0..255 by additionally flipping byte 1.
        page.set(ValueLayout.JAVA_BYTE, PageLayout.HEAP_START + heapOffs[i] + 2, (byte) ((i >>> 8) & 0xFF));
      }

      final Long2IntOpenHashMap map = new Long2IntOpenHashMap();
      map.defaultReturnValue(-1);
      final byte[] templates = new byte[256 * (2 + 16)];
      final byte[] slotIds = new byte[slotCount];
      final OffsetTableTemplatePool.BuildResult r =
          OffsetTableTemplatePool.build(page, slotCount, kindIds, heapOffs, templates, slotIds, map);

      assertFalse(r.isDedupEnabled(), "should abort when > 255 unique templates");
      assertEquals(-1, r.templateCount);
    }
  }

  @Test
  @DisplayName("single-record page builds one template")
  void singleRecord() {
    // Phase 4: migrated from legacy OBJECT_KEY to fused OBJECT_NAMED_OBJECT (Cat-1).
    try (Arena arena = Arena.ofConfined()) {
      final int kindId = 52;
      final int fc = NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;
      final MemorySegment page = arena.allocate(PageLayout.HEAP_START + 1 + fc);
      writeDummyRecord(page, 0, kindId, fc, 0x7E);

      final Long2IntOpenHashMap map = new Long2IntOpenHashMap();
      map.defaultReturnValue(-1);
      final byte[] templates = new byte[64];
      final byte[] slotIds = new byte[1];
      final OffsetTableTemplatePool.BuildResult r =
          OffsetTableTemplatePool.build(page, 1, new int[] {kindId}, new int[] {0}, templates, slotIds, map);

      assertTrue(r.isDedupEnabled());
      assertEquals(1, r.templateCount);
      assertEquals(0, slotIds[0] & 0xFF);
      assertEquals(kindId, templates[0] & 0xFF);
      assertEquals(fc, templates[1] & 0xFF);
    }
  }

  private static void writeDummyRecord(final MemorySegment page, final int heapOff, final int kindId, final int fc,
      final int patternByte) {
    final long base = PageLayout.HEAP_START + heapOff;
    page.set(ValueLayout.JAVA_BYTE, base, (byte) kindId);
    for (int i = 0; i < fc; i++) {
      page.set(ValueLayout.JAVA_BYTE, base + 1 + i, (byte) patternByte);
    }
  }

  private static void writeRecordWithPattern(final MemorySegment page, final int heapOff, final int kindId,
      final int fc, final int[] pattern) {
    final long base = PageLayout.HEAP_START + heapOff;
    page.set(ValueLayout.JAVA_BYTE, base, (byte) kindId);
    for (int i = 0; i < fc; i++) {
      page.set(ValueLayout.JAVA_BYTE, base + 1 + i, (byte) pattern[i]);
    }
  }
}
