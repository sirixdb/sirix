/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix;

import java.util.concurrent.atomic.LongAdder;

public final class HftBoundaryTelemetry {

  private static final boolean ENABLED = Boolean.getBoolean("sirix.hft.telemetry");
  private static final LongAdder STORAGE_READS = new LongAdder();
  private static final LongAdder STORAGE_WRITES = new LongAdder();
  private static final LongAdder STORAGE_BYTES_READ = new LongAdder();
  private static final LongAdder STORAGE_BYTES_WRITTEN = new LongAdder();
  private static final LongAdder ALLOCATOR_ALLOCATIONS = new LongAdder();
  private static final LongAdder ALLOCATOR_RELEASES = new LongAdder();
  private static final LongAdder TIL_READS = new LongAdder();
  private static final LongAdder TIL_WRITES = new LongAdder();
  private static final LongAdder NATIVE_ALLOCATIONS = new LongAdder();
  private static final LongAdder NATIVE_RELEASES = new LongAdder();
  private static final LongAdder ASYNC_SUBMISSIONS = new LongAdder();
  private static final LongAdder ASYNC_COMPLETIONS = new LongAdder();

  private HftBoundaryTelemetry() {
    throw new AssertionError("no instances");
  }

  public static void storageRead(final long bytes) {
    if (ENABLED && bytes > 0) {
      STORAGE_READS.increment();
      STORAGE_BYTES_READ.add(bytes);
    }
  }

  public static void storageWrite(final long bytes) {
    if (ENABLED && bytes > 0) {
      STORAGE_WRITES.increment();
      STORAGE_BYTES_WRITTEN.add(bytes);
    }
  }

  public static void allocatorAllocation() {
    if (ENABLED) {
      ALLOCATOR_ALLOCATIONS.increment();
    }
  }

  public static void allocatorRelease() {
    if (ENABLED) {
      ALLOCATOR_RELEASES.increment();
    }
  }

  public static void tilRead() {
    if (ENABLED) {
      TIL_READS.increment();
    }
  }

  public static void tilWrite() {
    if (ENABLED) {
      TIL_WRITES.increment();
    }
  }

  public static void nativeAllocation() {
    if (ENABLED) {
      NATIVE_ALLOCATIONS.increment();
    }
  }

  public static void nativeRelease() {
    if (ENABLED) {
      NATIVE_RELEASES.increment();
    }
  }

  public static void asyncSubmission() {
    if (ENABLED) {
      ASYNC_SUBMISSIONS.increment();
    }
  }

  public static void asyncCompletion() {
    if (ENABLED) {
      ASYNC_COMPLETIONS.increment();
    }
  }

  public static void reset() {
    STORAGE_READS.reset();
    STORAGE_WRITES.reset();
    STORAGE_BYTES_READ.reset();
    STORAGE_BYTES_WRITTEN.reset();
    ALLOCATOR_ALLOCATIONS.reset();
    ALLOCATOR_RELEASES.reset();
    TIL_READS.reset();
    TIL_WRITES.reset();
    NATIVE_ALLOCATIONS.reset();
    NATIVE_RELEASES.reset();
    ASYNC_SUBMISSIONS.reset();
    ASYNC_COMPLETIONS.reset();
  }

  public static Snapshot snapshot() {
    return new Snapshot(STORAGE_READS.sum(), STORAGE_WRITES.sum(), STORAGE_BYTES_READ.sum(),
        STORAGE_BYTES_WRITTEN.sum(), ALLOCATOR_ALLOCATIONS.sum(), ALLOCATOR_RELEASES.sum(), TIL_READS.sum(),
        TIL_WRITES.sum(), NATIVE_ALLOCATIONS.sum(), NATIVE_RELEASES.sum(), ASYNC_SUBMISSIONS.sum(),
        ASYNC_COMPLETIONS.sum());
  }

  public record Snapshot(long storageReads, long storageWrites, long bytesRead, long bytesWritten,
      long allocatorAllocations, long allocatorReleases, long tilReads, long tilWrites, long nativeAllocations,
      long nativeReleases, long asyncSubmissions, long asyncCompletions) {

    public long operations() {
      return Math.addExact(Math.addExact(Math.addExact(Math.addExact(storageReads, storageWrites),
              Math.addExact(allocatorAllocations, allocatorReleases)), Math.addExact(tilReads, tilWrites)),
          Math.addExact(Math.addExact(nativeAllocations, nativeReleases),
              Math.addExact(asyncSubmissions, asyncCompletions)));
    }
  }
}
