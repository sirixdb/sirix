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
 * Bench-only storage profiler. Captures per-page-class byte counts during
 * a write-path run. Prints a distribution table on JVM shutdown.
 *
 * <p>Enabled via {@code -Dsirix.storage.profile=true}. Zero-overhead when
 * the flag is off (the call site guards on
 * {@link FileWriter#STORAGE_PROFILE_ENABLED}).
 *
 * <p>Used by the storage-compression work to answer "which page kind is
 * dominating on-disk space?" with ground-truth data from the writer
 * rather than after-the-fact file scanning (which is unreliable due to
 * false-positive pattern matching inside page bodies).
 */
public final class StorageProfile {

  private static final ConcurrentMap<String, AtomicLong[]> BY_KIND = new ConcurrentHashMap<>();

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(StorageProfile::dump, "sirix-storage-profile-dump"));
  }

  private StorageProfile() {
  }

  /**
   * @return whether storage profiling is enabled this JVM run. Reads the
   *         system property dynamically so we don't freeze the flag at
   *         class-load time.
   */
  public static boolean isEnabled() {
    return Boolean.getBoolean("sirix.storage.profile");
  }

  /**
   * Record one page write.
   *
   * @param kind       simple class name of the page (e.g. {@code KeyValueLeafPage}).
   * @param rawBytes   serialized size before byteHandler compression (LZ4).
   * @param diskBytes  serialized size as written to disk (post-compression).
   */
  public static void record(final String kind, final int rawBytes, final int diskBytes) {
    final AtomicLong[] slot = BY_KIND.computeIfAbsent(kind, k -> new AtomicLong[] {
        new AtomicLong(),  // total raw bytes
        new AtomicLong(),  // total disk bytes
        new AtomicLong()   // count
    });
    slot[0].addAndGet(rawBytes);
    slot[1].addAndGet(diskBytes);
    slot[2].incrementAndGet();
  }

  public static void dump() {
    System.out.printf("# [StorageProfile] dump called: enabled=%s byKind.size=%d%n",
        isEnabled(), BY_KIND.size());
    if (BY_KIND.isEmpty()) return;
    final Map<String, AtomicLong[]> sorted = new TreeMap<>(BY_KIND);
    long totalRaw = 0, totalDisk = 0, totalCount = 0;
    for (final AtomicLong[] slot : sorted.values()) {
      totalRaw += slot[0].get();
      totalDisk += slot[1].get();
      totalCount += slot[2].get();
    }
    System.out.println();
    System.out.println("=== StorageProfile (writer-path ground truth) ===");
    System.out.printf("%-30s | %14s | %14s | %10s | %10s%n",
        "page kind", "raw bytes", "disk bytes", "writes", "avg raw");
    System.out.printf("%-30s-+-%14s-+-%14s-+-%10s-+-%10s%n",
        "------------------------------", "--------------", "--------------", "----------", "----------");
    for (final var e : sorted.entrySet()) {
      final AtomicLong[] slot = e.getValue();
      final long raw = slot[0].get();
      final long disk = slot[1].get();
      final long count = slot[2].get();
      final long avgRaw = count == 0 ? 0 : raw / count;
      System.out.printf("%-30s | %,14d | %,14d | %,10d | %,10d%n",
          e.getKey(), raw, disk, count, avgRaw);
    }
    System.out.printf("%-30s-+-%14s-+-%14s-+-%10s-+-%10s%n",
        "------------------------------", "--------------", "--------------", "----------", "----------");
    System.out.printf("%-30s | %,14d | %,14d | %,10d |%n",
        "Total", totalRaw, totalDisk, totalCount);
    final double ratio = totalRaw == 0 ? 0 : (double) totalDisk / totalRaw;
    System.out.printf("Overall compression ratio: %.3f (%.1f%% of raw written to disk)%n",
        ratio, ratio * 100.0);
  }
}
