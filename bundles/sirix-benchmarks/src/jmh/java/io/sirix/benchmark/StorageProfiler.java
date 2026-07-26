/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.benchmark;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * Storage profiler that walks a {@code sirix.data} file and reports byte
 * counts broken down by {@link io.sirix.page.PageKind}. Gives a
 * data-driven view of where on-disk space goes so structural compression
 * work can target the biggest buckets first.
 *
 * <p>Read format (see {@code FileWriter.writePageReference}):
 * <pre>
 *   per page fragment:
 *     int32  payloadLength        (big-endian)
 *     byte[payloadLength] payload  — first byte is the PageKind id
 *   page fragments are aligned to {@link #PAGE_FRAGMENT_ALIGN} boundaries.
 * </pre>
 *
 * <p>Usage:
 * <pre>
 *   java io.sirix.benchmark.StorageProfiler /tmp/sirix-scale-bench.../scale-db/resources/records.jn/data/sirix.data
 * </pre>
 */
public final class StorageProfiler {

  private static final int PAGE_FRAGMENT_ALIGN = 8;
  private static final int REVISION_ROOT_ALIGN = 256;
  private static final int UBER_PAGE_ALIGN = 512;
  private static final int FIRST_BEACON_BYTES = UBER_PAGE_ALIGN << 1;

  /** PageKind byte id → human name. Mirrors {@code io.sirix.page.PageKind}. */
  private static final String[] KIND_NAMES = new String[256];
  static {
    Arrays.fill(KIND_NAMES, null);
    KIND_NAMES[1]  = "KEYVALUELEAFPAGE";
    KIND_NAMES[2]  = "NAMEPAGE";
    KIND_NAMES[3]  = "UBERPAGE";
    KIND_NAMES[4]  = "INDIRECTPAGE";
    KIND_NAMES[5]  = "REVISIONROOTPAGE";
    KIND_NAMES[6]  = "PATHSUMMARYPAGE";
    KIND_NAMES[8]  = "CASPAGE";
    KIND_NAMES[9]  = "OVERFLOWPAGE";
    KIND_NAMES[10] = "PATHPAGE";
    KIND_NAMES[11] = "DEWEYIDPAGE";
    KIND_NAMES[12] = "HOT_LEAF_PAGE";
    KIND_NAMES[13] = "HOT_INDIRECT_PAGE";
    KIND_NAMES[14] = "BITMAP_CHUNK_PAGE";
    KIND_NAMES[15] = "VECTORPAGE";
  }

  public static void main(final String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: StorageProfiler <path/to/sirix.data>");
      System.exit(1);
    }
    profile(Path.of(args[0]));
  }

  public static void profile(final Path sirixData) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(sirixData.toFile(), "r")) {
      final long fileLen = raf.length();
      final long[] byteCountByKind = new long[256];
      final long[] fragmentCountByKind = new long[256];
      long unreadableFragments = 0;
      long unreadableBytes = 0;
      long scannedAligned = 0;

      // Walk aligned offsets. Each offset either holds a valid fragment or
      // alignment padding. Recognition heuristic: 4-byte big-endian length
      // prefix + first-byte-of-payload is a known PageKind id + length is
      // plausible for that kind.
      long offset = 0;
      while (offset + 5 < fileLen) {
        raf.seek(offset);
        final int payloadLen = raf.readInt();
        if (payloadLen <= 0 || payloadLen > 32 * 1024 * 1024 // sanity cap
            || offset + 4L + payloadLen > fileLen) {
          offset = nextAligned(offset + 1, PAGE_FRAGMENT_ALIGN);
          continue;
        }
        final int kindByte = raf.readUnsignedByte();
        final String kindName = KIND_NAMES[kindByte];
        if (kindName == null) {
          unreadableFragments++;
          unreadableBytes += 4L + payloadLen;
          offset = nextAligned(offset + 1, PAGE_FRAGMENT_ALIGN);
          continue;
        }
        byteCountByKind[kindByte] += payloadLen + 4L; // include length prefix
        fragmentCountByKind[kindByte]++;
        scannedAligned++;
        offset = nextAligned(offset + 4L + payloadLen, PAGE_FRAGMENT_ALIGN);
      }

      // Report.
      final Map<String, long[]> summary = new TreeMap<>();
      for (int k = 0; k < 256; k++) {
        if (byteCountByKind[k] > 0) {
          summary.put(KIND_NAMES[k], new long[] {byteCountByKind[k], fragmentCountByKind[k]});
        }
      }
      final long totalPageBytes = Arrays.stream(byteCountByKind).sum();
      final long alignmentPadding = fileLen - totalPageBytes - unreadableBytes;

      System.out.printf("%n=== Storage profile: %s (%,d bytes) ===%n", sirixData, fileLen);
      System.out.printf("%-24s | %14s | %10s | %7s%n", "kind", "bytes", "fragments", "avg");
      System.out.printf("%-24s-+-%14s-+-%10s-+-%7s%n", "------------------------",
          "--------------", "----------", "-------");
      for (final var e : summary.entrySet()) {
        final long bytes = e.getValue()[0];
        final long frags = e.getValue()[1];
        final long avg = frags == 0 ? 0 : bytes / frags;
        System.out.printf("%-24s | %,14d | %,10d | %,7d%n", e.getKey(), bytes, frags, avg);
      }
      System.out.printf("%-24s | %,14d | %,10d |%n", "(alignment padding)", alignmentPadding, 0L);
      if (unreadableFragments > 0) {
        System.out.printf("%-24s | %,14d | %,10d |%n",
            "(unrecognized)", unreadableBytes, unreadableFragments);
      }
      System.out.printf("%nTotal: %,d bytes across %,d recognized fragments%n",
          fileLen, scannedAligned);
    }
  }

  private static long nextAligned(final long offset, final int align) {
    final long mod = offset % align;
    return mod == 0 ? offset : offset + align - mod;
  }

  private StorageProfiler() {
  }
}
