/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.file;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bench-only storage profiler. Captures per-page-class byte counts during a write-path run. Prints
 * a distribution table on JVM shutdown.
 *
 * <p>
 * Enabled at JVM startup via {@code -Dsirix.storage.profile=true}. The immutable flag lets the JIT
 * reduce the disabled hot path to one predictable branch; no counters or cache metadata are
 * touched.
 *
 * <p>
 * Used by the storage-compression work to answer "which page kind is dominating on-disk space?"
 * with ground-truth data from the writer rather than after-the-fact file scanning (which is
 * unreliable due to false-positive pattern matching inside page bodies).
 */
public final class StorageProfile {

  private static final int RAW_BYTES = 0;
  private static final int DISK_BYTES = 1;
  private static final int WRITE_COUNT = 2;
  private static final int UNKNOWN_RAW_COUNT = 3;
  private static final int UNKNOWN_RAW_DISK_BYTES = 4;

  private static final boolean ENABLED = Boolean.getBoolean("sirix.storage.profile");

  private static final ConcurrentMap<String, AtomicLong[]> BY_KIND = new ConcurrentHashMap<>();

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(StorageProfile::dump, "sirix-storage-profile-dump"));
  }

  private StorageProfile() {}

  /**
   * @return whether storage profiling was enabled when this class initialized
   */
  public static boolean isEnabled() {
    return ENABLED;
  }

  /**
   * Record one page write.
   *
   * @param kind simple class name of the page (e.g. {@code KeyValueLeafPage}).
   * @param rawBytes serialized size before byteHandler compression (LZ4).
   * @param diskBytes serialized size as written to disk (post-compression).
   */
  public static void record(final String kind, final int rawBytes, final int diskBytes) {
    if (rawBytes < 0 || diskBytes < 0) {
      throw new IllegalArgumentException(
          "storage profile byte counts must be non-negative: raw=" + rawBytes + ", disk=" + diskBytes);
    }
    final AtomicLong[] slot = slot(kind);
    slot[RAW_BYTES].addAndGet(rawBytes);
    slot[DISK_BYTES].addAndGet(diskBytes);
    slot[WRITE_COUNT].incrementAndGet();
  }

  /**
   * Record a write whose persisted size is exact but whose pre-byte-handler size is unavailable.
   * Diagnostics must never invent a compression ratio by treating compressed bytes as raw bytes.
   */
  public static void recordUnknownRaw(final String kind, final int diskBytes) {
    if (diskBytes < 0) {
      throw new IllegalArgumentException("storage profile disk byte count must be non-negative: " + diskBytes);
    }
    final AtomicLong[] slot = slot(kind);
    slot[DISK_BYTES].addAndGet(diskBytes);
    slot[WRITE_COUNT].incrementAndGet();
    slot[UNKNOWN_RAW_COUNT].incrementAndGet();
    slot[UNKNOWN_RAW_DISK_BYTES].addAndGet(diskBytes);
  }

  private static AtomicLong[] slot(final String kind) {
    if (kind == null || kind.isEmpty()) {
      throw new IllegalArgumentException("storage profile page kind must not be empty");
    }
    return BY_KIND.computeIfAbsent(kind, unused -> new AtomicLong[] {new AtomicLong(), new AtomicLong(),
        new AtomicLong(), new AtomicLong(), new AtomicLong()});
  }

  public static void dump() {
    // PageSectionDiag has its own shutdown hook. Hold the PrintStream monitor for the complete
    // report so the two multi-line ledgers cannot interleave into an unparsable result.
    synchronized (System.out) {
      dumpLocked();
    }
  }

  private static void dumpLocked() {
    System.out.printf("# [StorageProfile] dump called: enabled=%s byKind.size=%d%n", isEnabled(), BY_KIND.size());
    if (BY_KIND.isEmpty())
      return;
    final Map<String, AtomicLong[]> sorted = new TreeMap<>(BY_KIND);
    long totalRaw = 0, totalDisk = 0, totalCount = 0, totalUnknownRaw = 0, totalUnknownRawDisk = 0;
    for (final AtomicLong[] slot : sorted.values()) {
      totalRaw += slot[RAW_BYTES].get();
      totalDisk += slot[DISK_BYTES].get();
      totalCount += slot[WRITE_COUNT].get();
      totalUnknownRaw += slot[UNKNOWN_RAW_COUNT].get();
      totalUnknownRawDisk += slot[UNKNOWN_RAW_DISK_BYTES].get();
    }
    System.out.println();
    System.out.println("=== StorageProfile (writer-path ground truth) ===");
    System.out.printf("%-30s | %14s | %14s | %10s | %11s | %13s%n", "page kind", "known raw", "disk bytes", "writes",
        "raw unknown", "avg known raw");
    System.out.printf("%-30s-+-%14s-+-%14s-+-%10s-+-%11s-+-%13s%n", "------------------------------", "--------------",
        "--------------", "----------", "-----------", "-------------");
    for (final var e : sorted.entrySet()) {
      final AtomicLong[] slot = e.getValue();
      final long raw = slot[RAW_BYTES].get();
      final long disk = slot[DISK_BYTES].get();
      final long count = slot[WRITE_COUNT].get();
      final long unknownRaw = slot[UNKNOWN_RAW_COUNT].get();
      final long knownCount = count - unknownRaw;
      final long avgRaw = knownCount == 0
          ? 0
          : raw / knownCount;
      System.out.printf("%-30s | %,14d | %,14d | %,10d | %,11d | %,13d%n", e.getKey(), raw, disk, count, unknownRaw,
          avgRaw);
    }
    System.out.printf("%-30s-+-%14s-+-%14s-+-%10s-+-%11s-+-%13s%n", "------------------------------", "--------------",
        "--------------", "----------", "-----------", "-------------");
    System.out.printf("%-30s | %,14d | %,14d | %,10d | %,11d |%n", "Total", totalRaw, totalDisk, totalCount,
        totalUnknownRaw);
    if (totalUnknownRaw == 0) {
      final double ratio = totalRaw == 0
          ? 0
          : (double) totalDisk / totalRaw;
      System.out.printf("Overall compression ratio: %.3f (%.1f%% of raw written to disk)%n", ratio, ratio * 100.0);
    } else {
      System.out.printf("Overall compression ratio: unavailable — %,d of %,d writes have unknown raw size%n",
          totalUnknownRaw, totalCount);
      final long knownDisk = totalDisk - totalUnknownRawDisk;
      if (totalRaw > 0) {
        final double knownRatio = (double) knownDisk / totalRaw;
        final double coverage = totalCount == 0
            ? 0.0
            : (double) (totalCount - totalUnknownRaw) * 100.0 / totalCount;
        System.out.printf("Known-subset compression ratio: %.3f (%.1f%% write coverage)%n", knownRatio, coverage);
      }
    }
  }
}
