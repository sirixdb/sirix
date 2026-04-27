/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import java.util.concurrent.atomic.LongAdder;

/**
 * Diagnostic byte-count aggregator for the per-section serialized size of
 * a {@link KeyValueLeafPage}. Activated via {@code -Dsirix.pageSectionDiag=true}
 * from {@link PageKind}; off by default.
 *
 * <p>Sections tracked:
 * <ul>
 *   <li>{@code headerBitmap}: fixed 160-byte page header + bitmap prefix.</li>
 *   <li>{@code encodedBody}: the compact-dir + template pool + slotIds +
 *       compressed-heap body.</li>
 *   <li>{@code regionTable}: PAX region table (number/string/struct/DeweyID
 *       payloads). This is where NumberRegion / StringRegion dictionaries and
 *       value arrays live, so expect it to dominate for columnar workloads.</li>
 *   <li>{@code overlong}: overlong-entries bitmap + references.</li>
 *   <li>{@code fsst}: FSST symbol table (small).</li>
 * </ul>
 *
 * <p>A shutdown hook prints a cumulative summary ordered by absolute bytes.
 * The summary is printed to {@code System.out} so it's captured by stdout
 * logging from the bench runner.
 *
 * <p>HFT-grade: per-record path uses only {@link LongAdder} additions and one
 * pageCount increment. No allocation on the hot path.
 */
public final class PageSectionDiag {

  private static final LongAdder PAGE_COUNT = new LongAdder();
  private static final LongAdder HEADER_BITMAP_BYTES = new LongAdder();
  private static final LongAdder ENCODED_BODY_BYTES = new LongAdder();
  private static final LongAdder REGION_TABLE_BYTES = new LongAdder();
  private static final LongAdder OVERLONG_BYTES = new LongAdder();
  private static final LongAdder FSST_BYTES = new LongAdder();
  private static final LongAdder COMPACT_DIR_BYTES = new LongAdder();
  private static final LongAdder TEMPLATE_POOL_BYTES = new LongAdder();
  private static final LongAdder COMPRESSED_HEAP_BYTES = new LongAdder();
  private static final LongAdder HASH_ELISION_PAGES = new LongAdder();
  private static final LongAdder HASH_ELISION_BYTES_SAVED = new LongAdder();
  private static final LongAdder PARENT_KEY_COLUMN_PAGES = new LongAdder();
  private static final LongAdder PARENT_KEY_COLUMN_BYTES_SAVED = new LongAdder();
  private static final LongAdder PARENT_KEY_COLUMN_CANDIDATE_PAGES = new LongAdder();
  private static final LongAdder PARENT_KEY_COLUMN_RAW_BYTES = new LongAdder();
  private static final LongAdder PARENT_KEY_COLUMN_ENCODED_BYTES = new LongAdder();

  // Per-codec selection counters (pages for which each codec was chosen as
  // smallest). Exercised by the write path's pick-smallest logic between
  // ZeroRunByteCodec (0), ByteRunCodec (2), and SirixLZ77Codec (3).
  private static final LongAdder CODEC_ZERORUN_PAGES = new LongAdder();
  private static final LongAdder CODEC_BYTERUN_PAGES = new LongAdder();
  private static final LongAdder CODEC_LZ77_PAGES = new LongAdder();
  private static final LongAdder CODEC_ZERORUN_BYTES = new LongAdder();
  private static final LongAdder CODEC_BYTERUN_BYTES = new LongAdder();
  private static final LongAdder CODEC_LZ77_BYTES = new LongAdder();

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(PageSectionDiag::dumpStats,
        "page-section-diag-dump"));
  }

  private PageSectionDiag() { throw new AssertionError(); }

  /**
   * Accumulate per-section byte counts for the encodedBody breakdown
   * (compactDir, templatePool + slotIds, compressedHeap incl. length+codec).
   */
  public static void recordEncodedBody(final long compactDir, final long templatePool,
      final long compressedHeap) {
    COMPACT_DIR_BYTES.add(compactDir);
    TEMPLATE_POOL_BYTES.add(templatePool);
    COMPRESSED_HEAP_BYTES.add(compressedHeap);
  }

  /**
   * Record activation of the hash-elision structural encoder on a single
   * page along with the number of pre-compression bytes stripped.
   */
  public static void recordHashElision(final long bytesSaved) {
    HASH_ELISION_PAGES.increment();
    HASH_ELISION_BYTES_SAVED.add(bytesSaved);
  }

  /**
   * Record activation of the parent-key column extractor on a single page
   * along with the number of pre-compression bytes it displaced from the
   * heap body.
   */
  public static void recordParentKeyColumn(final long bytesSaved) {
    PARENT_KEY_COLUMN_PAGES.increment();
    PARENT_KEY_COLUMN_BYTES_SAVED.add(bytesSaved);
  }

  /**
   * Record a parent-key column candidate (a page with at least one slot
   * whose kind has a parent-key field) regardless of whether the column
   * ultimately paid off. Used to diagnose why the column fails to activate.
   */
  public static void recordParentKeyColumnCandidate(final long rawStrippedBytes,
      final long encodedColumnBytes) {
    PARENT_KEY_COLUMN_CANDIDATE_PAGES.increment();
    PARENT_KEY_COLUMN_RAW_BYTES.add(rawStrippedBytes);
    PARENT_KEY_COLUMN_ENCODED_BYTES.add(encodedColumnBytes);
  }

  /** Record that ZeroRunByteCodec (codec=0) was chosen for this page. */
  public static void recordCodecZeroRun(final long encodedBytes) {
    CODEC_ZERORUN_PAGES.increment();
    CODEC_ZERORUN_BYTES.add(encodedBytes);
  }

  /** Record that ByteRunCodec (codec=2) was chosen for this page. */
  public static void recordCodecByteRun(final long encodedBytes) {
    CODEC_BYTERUN_PAGES.increment();
    CODEC_BYTERUN_BYTES.add(encodedBytes);
  }

  /** Record that SirixLZ77Codec (codec=3) was chosen for this page. */
  public static void recordCodecLz77(final long encodedBytes) {
    CODEC_LZ77_PAGES.increment();
    CODEC_LZ77_BYTES.add(encodedBytes);
  }

  /**
   * Accumulate one page's per-section byte counts.
   *
   * @param headerBitmap bytes written for the 160-byte header + bitmap prefix
   * @param encodedBody bytes written for the compact-dir + template pool + compressed heap
   * @param regionTable bytes written for the PAX region table
   * @param overlong bytes written for the overlong-entries bitmap + references
   * @param fsst bytes written for the FSST symbol table
   */
  public static void record(final long headerBitmap, final long encodedBody,
      final long regionTable, final long overlong, final long fsst) {
    PAGE_COUNT.increment();
    HEADER_BITMAP_BYTES.add(headerBitmap);
    ENCODED_BODY_BYTES.add(encodedBody);
    REGION_TABLE_BYTES.add(regionTable);
    OVERLONG_BYTES.add(overlong);
    FSST_BYTES.add(fsst);
  }

  private static void dumpStats() {
    final long pages = PAGE_COUNT.sum();
    if (pages == 0) return;
    final long hb = HEADER_BITMAP_BYTES.sum();
    final long eb = ENCODED_BODY_BYTES.sum();
    final long rt = REGION_TABLE_BYTES.sum();
    final long ov = OVERLONG_BYTES.sum();
    final long fsst = FSST_BYTES.sum();
    final long total = hb + eb + rt + ov + fsst;
    final String fmt =
        "[PageSectionDiag] pages=%,d total=%,d B (%.1f MB)  headerBitmap=%,d (%.1f%%)"
            + "  encodedBody=%,d (%.1f%%)  regionTable=%,d (%.1f%%)"
            + "  overlong=%,d (%.1f%%)  fsst=%,d (%.1f%%)%n";
    System.out.printf(fmt,
        pages, total, total / (1024.0 * 1024.0),
        hb, pct(hb, total),
        eb, pct(eb, total),
        rt, pct(rt, total),
        ov, pct(ov, total),
        fsst, pct(fsst, total));
    final long cd = COMPACT_DIR_BYTES.sum();
    final long tp = TEMPLATE_POOL_BYTES.sum();
    final long ch = COMPRESSED_HEAP_BYTES.sum();
    final long ebTotal = cd + tp + ch;
    System.out.printf(
        "[PageSectionDiag] encodedBody breakdown: compactDir=%,d (%.1f%%)  "
            + "templatePool+slotIds=%,d (%.1f%%)  compressedHeap=%,d (%.1f%%)%n",
        cd, pct(cd, ebTotal),
        tp, pct(tp, ebTotal),
        ch, pct(ch, ebTotal));
    final long hePages = HASH_ELISION_PAGES.sum();
    final long heBytes = HASH_ELISION_BYTES_SAVED.sum();
    final long pkPages = PARENT_KEY_COLUMN_PAGES.sum();
    final long pkBytes = PARENT_KEY_COLUMN_BYTES_SAVED.sum();
    System.out.printf(
        "[PageSectionDiag] encoders: hashElision pages=%,d (%.1f%%)  bytesSaved=%,d (%.1f MB)%n",
        hePages, pct(hePages, pages),
        heBytes, heBytes / (1024.0 * 1024.0));
    System.out.printf(
        "[PageSectionDiag] encoders: parentKeyColumn pages=%,d (%.1f%%)  bytesSaved=%,d (%.1f MB)%n",
        pkPages, pct(pkPages, pages),
        pkBytes, pkBytes / (1024.0 * 1024.0));
    final long pkCandidates = PARENT_KEY_COLUMN_CANDIDATE_PAGES.sum();
    final long pkRaw = PARENT_KEY_COLUMN_RAW_BYTES.sum();
    final long pkEncoded = PARENT_KEY_COLUMN_ENCODED_BYTES.sum();
    System.out.printf(
        "[PageSectionDiag] parentKeyColumn candidates=%,d rawBytes=%,d (%.1f MB)"
            + "  encodedBytes=%,d (%.1f MB)  avgRaw/page=%.1f  avgEncoded/page=%.1f%n",
        pkCandidates, pkRaw, pkRaw / (1024.0 * 1024.0),
        pkEncoded, pkEncoded / (1024.0 * 1024.0),
        pkCandidates == 0 ? 0 : (double) pkRaw / pkCandidates,
        pkCandidates == 0 ? 0 : (double) pkEncoded / pkCandidates);
    final long cZero = CODEC_ZERORUN_PAGES.sum();
    final long cByte = CODEC_BYTERUN_PAGES.sum();
    final long cLz77 = CODEC_LZ77_PAGES.sum();
    final long cZeroB = CODEC_ZERORUN_BYTES.sum();
    final long cByteB = CODEC_BYTERUN_BYTES.sum();
    final long cLz77B = CODEC_LZ77_BYTES.sum();
    final long cTotalPages = cZero + cByte + cLz77;
    final long cTotalBytes = cZeroB + cByteB + cLz77B;
    if (cTotalPages > 0) {
      System.out.printf(
          "[PageSectionDiag] codec wins: zeroRun=%,d (%.1f%%) bytes=%,d (%.1f MB)"
              + "  byteRun=%,d (%.1f%%) bytes=%,d (%.1f MB)"
              + "  lz77=%,d (%.1f%%) bytes=%,d (%.1f MB)%n",
          cZero, pct(cZero, cTotalPages), cZeroB, cZeroB / (1024.0 * 1024.0),
          cByte, pct(cByte, cTotalPages), cByteB, cByteB / (1024.0 * 1024.0),
          cLz77, pct(cLz77, cTotalPages), cLz77B, cLz77B / (1024.0 * 1024.0));
      if (cTotalBytes > 0) {
        System.out.printf(
            "[PageSectionDiag] codec total encoded bytes: %,d (%.1f MB) avg=%.1f/page%n",
            cTotalBytes, cTotalBytes / (1024.0 * 1024.0),
            (double) cTotalBytes / cTotalPages);
      }
    }
  }

  private static double pct(final long part, final long total) {
    return total == 0 ? 0.0 : 100.0 * part / total;
  }
}
