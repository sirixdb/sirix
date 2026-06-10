/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Per-page dedup of per-record offset tables for disk-format compression.
 *
 * <p>Each record on the slotted page heap carries a fixed-width offset
 * table (one byte per field, FIELD_COUNT bytes total — see
 * {@link NodeFieldLayout}). In a typical DFS-shredded page, the varint
 * widths for parentKey / rightSib / leftSib / ... are drawn from a narrow
 * distribution (often identical across hundreds of adjacent records), so
 * most slots produce the same offset table bytes.
 *
 * <p>The {@link OffsetTableRedundancyProbe} test measures 85% template
 * dedup on realistic DFS pages. Replacing FIELD_COUNT inline bytes per
 * slot with a 1-byte templateId cuts the offset-table footprint by
 * {@code (FIELD_COUNT - 1) / FIELD_COUNT} per deduplicated slot.
 *
 * <h2>Wire format (on-disk only — in-memory slotted page is unchanged)</h2>
 * <pre>
 * byte   templateCount    // 0..255 unique (kindId, offsetTable) pairs on page
 * // for each template i in [0, templateCount):
 *   byte  kindId           // node kind byte, 1..127
 *   byte  fieldCount       // FIELD_COUNT for this kind (redundant with kindId
 *                          // but self-describing — tolerates future kindId
 *                          // reuse without schema migration)
 *   byte[fieldCount] bytes // the offset table bytes
 * // followed by: per-slot templateId (1 byte each), {@code populatedCount} total
 * </pre>
 *
 * <p>When {@code templateCount == 0} the page uses legacy inline-offset-table
 * encoding (no dedup). This happens when {@code populatedCount == 0} or
 * when the dedup didn't pay — rare in practice.
 *
 * <p>Saving per page: {@code (FIELD_COUNT - 1) × (populatedCount - templateCount)
 * - 2} bytes (minus 2-byte template table overhead for fieldCount/kindId
 * that gets paid once per template). For 256-slot pages with 1 template
 * and FIELD_COUNT=10, that's {@code 9 × 255 - 2 = 2293 bytes}. At 850K
 * pages on the 100M-record bench, that's ~2 GiB of on-disk compression —
 * a large fraction of what LZ4 buys us on the same input.
 *
 * <h2>HFT-grade constraints</h2>
 * <ul>
 *   <li>Zero allocation on serialize/deserialize hot paths. Templates
 *       encode/decode into caller-supplied scratch buffers.</li>
 *   <li>Map uses fastutil's primitive {@code Long2IntOpenHashMap} so
 *       the template-hash lookup is a {@code long} key (kindId{@code <<}56
 *       {@code |} offset bytes) — no boxing, no allocation.</li>
 *   <li>Template ids fit in a single byte (0..255). Pages with more than
 *       256 unique offset tables fall back to inline encoding.</li>
 *   <li>Max FIELD_COUNT across all node kinds is 15 (ELEMENT) — offset-table
 *       packed into a 64-bit key plus kindId in the top byte works for all
 *       but ELEMENT; there we use a composite (kindId, byte[]) lookup.</li>
 * </ul>
 */
public final class OffsetTableTemplatePool {

  /**
   * Upper bound on templateCount. STRICTLY 255, not 256: the count itself is ONE BYTE on the
   * wire — a page with exactly 256 unique templates would serialize the count as (byte) 256 ==
   * 0x00 and the reader would mis-frame the body as the non-dedup inline layout, making
   * legitimately-written data unreadable. (Slot template IDs 0..254 still fit one byte each.)
   */
  public static final int MAX_TEMPLATES = 255;

  /**
   * Width of the templateId field per slot (1 byte). Keep as a constant so
   * call sites don't hard-code the literal.
   */
  public static final int TEMPLATE_ID_WIDTH = 1;

  /**
   * Magic byte pattern prefixing the template-pool header on disk, used by
   * the deserialize path to distinguish "dedup enabled" from "no region
   * table at position N". Unused in V0 (where offset tables are inline);
   * only written in V1+.
   */
  public static final byte MAGIC_V1 = (byte) 0xA7;

  /** Max field count any NodeKind uses today — sized for ELEMENT = 15. */
  private static final int MAX_FIELD_COUNT = 16;

  /**
   * Builds a pool over the populated slots of a page, returning the count of
   * unique (kindId, offsetTable) pairs discovered. Fills {@code outTemplates}
   * with packed template bytes and {@code outSlotIds} with per-slot ids.
   *
   * <p>Caller must size {@code outTemplates} for the worst case: {@code
   * populatedCount × (2 + MAX_FIELD_COUNT)}. Typically only a handful are
   * needed.
   *
   * @param page slotted-page memory
   * @param populatedCount number of populated slots
   * @param slotKindIds scratch array of length ≥ populatedCount — filled
   *     with each slot's nodeKindId in bitmap walk order
   * @param heapOffsets scratch array of length ≥ populatedCount — filled
   *     with each slot's heapOffset in bitmap walk order
   * @param outTemplates caller-sized scratch byte[] to receive the concat
   *     of per-template (kindId, fieldCount, bytes)
   * @param outSlotIds scratch byte[] of length ≥ populatedCount — receives
   *     per-slot templateId
   * @return header object carrying templateCount, templatesByteLength and
   *     a sentinel {@code templateCount == -1} when the dedup aborted
   *     (too many unique tables — caller falls back to inline)
   */
  public static BuildResult build(final MemorySegment page, final int populatedCount,
      final int[] slotKindIds, final int[] heapOffsets,
      final byte[] outTemplates, final byte[] outSlotIds,
      final Long2IntOpenHashMap templateMap) {
    if (populatedCount == 0) {
      return new BuildResult(0, 0);
    }
    templateMap.clear();
    int uniqueCount = 0;
    int templatesOff = 0;
    for (int slotIdx = 0; slotIdx < populatedCount; slotIdx++) {
      final int kindId = slotKindIds[slotIdx];
      final int fc = NodeFieldLayout.fieldCountForKind(kindId);
      if (fc < 0 || fc > MAX_FIELD_COUNT) {
        // Unknown kind or over-wide table — disable dedup. The PageKind
        // serializer will fall back to inline encoding for this whole page.
        return new BuildResult(-1, 0);
      }
      // Key = kindId(8 bits) | offsetTableBytesPacked(56 bits). For FIELD_COUNT ≤ 7
      // this is exact; for FIELD_COUNT > 7 we spill to a byte-wise compare.
      final long recordBase = PageLayout.HEAP_START + heapOffsets[slotIdx];
      // Byte 0 of the record is the kindId; offset table is at recordBase+1.
      final long offsetTableStart = recordBase + 1;
      long key;
      if (fc <= 7) {
        key = packOffsetTableKey(page, offsetTableStart, fc, kindId);
      } else {
        // Fall back to a stable hash for wide tables (rare — ELEMENT).
        key = hashWideKey(page, offsetTableStart, fc, kindId);
      }
      int templateId = templateMap.get(key);
      if (templateId == templateMap.defaultReturnValue()) {
        // Collision check for wide tables: verify bytes match any existing
        // template with this hash. For 1-byte kindIds the packed key is
        // exact so we can skip the walk.
        if (fc > 7) {
          final int existing = locateWideTemplate(page, offsetTableStart, kindId, fc,
              outTemplates, templatesOff);
          if (existing >= 0) {
            templateMap.put(key, existing);
            outSlotIds[slotIdx] = (byte) existing;
            continue;
          }
        }
        if (uniqueCount >= MAX_TEMPLATES) {
          // Too many distinct tables — abort dedup.
          return new BuildResult(-1, 0);
        }
        templateId = uniqueCount++;
        templateMap.put(key, templateId);
        outTemplates[templatesOff++] = (byte) kindId;
        outTemplates[templatesOff++] = (byte) fc;
        MemorySegment.copy(page, ValueLayout.JAVA_BYTE, offsetTableStart,
            outTemplates, templatesOff, fc);
        templatesOff += fc;
      }
      outSlotIds[slotIdx] = (byte) templateId;
    }
    return new BuildResult(uniqueCount, templatesOff);
  }

  /**
   * Look for a wide-table template whose bytes match what's at
   * {@code offsetTableStart} in {@code page}. Linear scan — only fires on
   * hash-collision fallback for FIELD_COUNT > 7 kinds (ELEMENT, PI).
   *
   * @return existing template id, or -1 if not present
   */
  private static int locateWideTemplate(final MemorySegment page, final long offsetTableStart,
      final int kindId, final int fc, final byte[] templates, final int templatesLen) {
    int idx = 0;
    int tid = 0;
    while (idx < templatesLen) {
      final int existingKind = templates[idx] & 0xFF;
      final int existingFc = templates[idx + 1] & 0xFF;
      if (existingKind == kindId && existingFc == fc) {
        boolean equal = true;
        for (int i = 0; i < fc; i++) {
          if (templates[idx + 2 + i]
              != page.get(ValueLayout.JAVA_BYTE, offsetTableStart + i)) {
            equal = false;
            break;
          }
        }
        if (equal) {
          return tid;
        }
      }
      idx += 2 + existingFc;
      tid++;
    }
    return -1;
  }

  private static long hashWideKey(final MemorySegment page, final long offsetTableStart,
      final int fc, final int kindId) {
    long h = 1469598103934665603L ^ kindId; // FNV seed + kindId
    for (int i = 0; i < fc; i++) {
      h ^= page.get(ValueLayout.JAVA_BYTE, offsetTableStart + i) & 0xFFL;
      h *= 1099511628211L;
    }
    return h;
  }

  /**
   * Pack a narrow (FIELD_COUNT ≤ 7) offset table + kindId into a 64-bit
   * key: {@code kindId(8) << 56 | b0 | b1<<8 | b2<<16 | ...}.
   */
  public static long packOffsetTableKey(final MemorySegment page, final long offsetTableStart,
      final int fc, final int kindId) {
    long key = ((long) kindId & 0xFFL) << 56;
    for (int i = 0; i < fc; i++) {
      final long b = page.get(ValueLayout.JAVA_BYTE, offsetTableStart + i) & 0xFFL;
      key |= b << (i * 8);
    }
    return key;
  }

  /**
   * Expand a templateId back into an offset-table byte sequence and write
   * to {@code target} at {@code targetOff}.
   *
   * @param templates per-page template pool bytes
   * @param templateOffsets parsed offsets of each templateId into
   *     {@code templates} (built by {@link #parseTemplateOffsets})
   * @param templateId the id to expand
   * @param target destination segment
   * @param targetOff destination offset (absolute)
   * @return the FIELD_COUNT of the expanded template (bytes written)
   */
  public static int expandTemplateTo(final byte[] templates, final int[] templateOffsets,
      final int templateId, final MemorySegment target, final long targetOff) {
    final int start = templateOffsets[templateId];
    final int fc = templates[start + 1] & 0xFF;
    MemorySegment.copy(templates, start + 2, target, ValueLayout.JAVA_BYTE, targetOff, fc);
    return fc;
  }

  /**
   * Parse {@code templates} (length {@code templatesLen}) into an array of
   * {@code templateCount + 1} offsets. {@code templateOffsets[i]} is the
   * start of template {@code i} in {@code templates}; the sentinel
   * {@code templateOffsets[templateCount] == templatesLen} makes length
   * computations branch-free.
   *
   * @param templates per-page template pool bytes
   * @param templatesLen number of valid bytes in {@code templates}
   * @param templateCount number of templates
   * @param templateOffsets scratch array of length ≥ templateCount+1
   */
  public static void parseTemplateOffsets(final byte[] templates, final int templatesLen,
      final int templateCount, final int[] templateOffsets) {
    int off = 0;
    for (int i = 0; i < templateCount; i++) {
      templateOffsets[i] = off;
      // off = kindId(1) + fc(1) + fcBytes
      final int fc = templates[off + 1] & 0xFF;
      off += 2 + fc;
    }
    templateOffsets[templateCount] = off;
    if (off != templatesLen) {
      throw new IllegalStateException(
          "template pool length mismatch: parsed=" + off + " expected=" + templatesLen);
    }
  }

  /**
   * Read kindId of template {@code templateId} (its first byte).
   */
  public static int templateKindId(final byte[] templates, final int[] templateOffsets,
      final int templateId) {
    return templates[templateOffsets[templateId]] & 0xFF;
  }

  /**
   * Read fieldCount of template {@code templateId} (its second byte).
   */
  public static int templateFieldCount(final byte[] templates, final int[] templateOffsets,
      final int templateId) {
    return templates[templateOffsets[templateId] + 1] & 0xFF;
  }

  /**
   * Read the offset-table byte at field index {@code fieldIndex} of template
   * {@code templateId}. The returned value is the offset of that field's data
   * from the start of the record's <em>data region</em> (i.e. after the
   * 1-byte kindId and the {@code fieldCount}-byte offset table).
   *
   * <p>Used by the hash-elision / column-extraction path in {@link PageKind}
   * to find the in-data-region position of a specific semantic field in
   * {@code O(1)} without re-parsing the live record bytes.
   */
  public static int templateFieldOffset(final byte[] templates, final int[] templateOffsets,
      final int templateId, final int fieldIndex) {
    final int templateStart = templateOffsets[templateId];
    final int fc = templates[templateStart + 1] & 0xFF;
    if (fieldIndex < 0 || fieldIndex >= fc) {
      throw new IndexOutOfBoundsException(
          "fieldIndex=" + fieldIndex + " fc=" + fc + " templateId=" + templateId);
    }
    return templates[templateStart + 2 + fieldIndex] & 0xFF;
  }

  /**
   * Compute the on-disk width of field {@code fieldIndex} as it appears in the
   * record's data region. The width is derived from the offset table:
   * {@code offset[fieldIndex + 1] - offset[fieldIndex]} for non-terminal fields;
   * for the last field the caller passes {@code dataRegionBytes} (the total
   * data-region size for this specific slot) and the width becomes
   * {@code dataRegionBytes - offset[fieldIndex]}.
   *
   * <p>Widths are uniform within a template (that's the definition of
   * template grouping) <em>except</em> for the final field when record lengths
   * differ between records of the same template due to variable-length trailing
   * payloads (e.g. STRING value bytes). Non-final structural fields like
   * parentKey / pathNodeKey / prev+lastModRev are always template-uniform, so
   * this helper's width value is safe to cache per-template once.
   */
  public static int templateFieldWidth(final byte[] templates, final int[] templateOffsets,
      final int templateId, final int fieldIndex, final int dataRegionBytes) {
    final int templateStart = templateOffsets[templateId];
    final int fc = templates[templateStart + 1] & 0xFF;
    if (fieldIndex < 0 || fieldIndex >= fc) {
      throw new IndexOutOfBoundsException(
          "fieldIndex=" + fieldIndex + " fc=" + fc + " templateId=" + templateId);
    }
    final int thisOff = templates[templateStart + 2 + fieldIndex] & 0xFF;
    if (fieldIndex + 1 < fc) {
      final int nextOff = templates[templateStart + 2 + fieldIndex + 1] & 0xFF;
      return nextOff - thisOff;
    }
    return dataRegionBytes - thisOff;
  }

  /**
   * Carries results of a {@link #build} call. {@code templateCount == -1}
   * when dedup aborted (caller falls back to legacy inline-offset encoding).
   */
  public static final class BuildResult {
    public final int templateCount;
    public final int templatesByteLength;

    public BuildResult(final int templateCount, final int templatesByteLength) {
      this.templateCount = templateCount;
      this.templatesByteLength = templatesByteLength;
    }

    public boolean isDedupEnabled() {
      return templateCount >= 0;
    }
  }

  private OffsetTableTemplatePool() {}
}
