/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Copies a {@link MemorySegment} range into a reusable heap array so a scan can run over {@code
 * byte[]} instead of the segment.
 *
 * <h2>Why this exists</h2>
 * A byte-at-a-time {@code segment.get(JAVA_BYTE, off)} is not free the way an array load is. Every
 * access carries the foreign-memory checks — session liveness, bounds, alignment, and the var-handle
 * dispatch behind them — and in a scan loop with a data-dependent trip count the JIT cannot hoist
 * them out. Profiling an ingest showed those checks costing more than the encoders they were serving:
 * the FFM access machinery outweighed the page-body encoding itself.
 *
 * <p>One bulk {@link MemorySegment#copy} is a single {@code memcpy} at memory bandwidth, after which
 * the scan runs over a plain array — where the JIT eliminates bounds checks and vectorises freely.
 * For any range worth run-length encoding the copy is far cheaper than the per-byte checks it avoids.
 *
 * <p>The buffer is per-thread and grows monotonically, so steady-state encoding allocates nothing.
 * Callers must finish with the returned array before invoking the mirror again on the same thread —
 * the page codecs do, because each encodes to completion before the next one starts.
 *
 * @author Johannes Lichtenberger
 */
final class SegmentByteMirror {

  /** Per-thread scratch, grown on demand and never shrunk. */
  private static final ThreadLocal<byte[]> SCRATCH = ThreadLocal.withInitial(() -> new byte[0]);

  private SegmentByteMirror() {
    throw new AssertionError("no instances");
  }

  /**
   * Mirror {@code length} bytes of {@code source} starting at {@code offset} into the calling
   * thread's scratch array.
   *
   * @param source the segment to copy from
   * @param offset the first byte to copy
   * @param length the number of bytes to copy
   * @return the scratch array, holding the requested bytes at index {@code 0}; it may be longer than
   *         {@code length}, so callers must bound their own reads
   */
  static byte[] of(final MemorySegment source, final long offset, final int length) {
    byte[] buffer = SCRATCH.get();
    if (buffer.length < length) {
      buffer = new byte[Math.max(length, buffer.length * 2)];
      SCRATCH.set(buffer);
    }
    MemorySegment.copy(source, ValueLayout.JAVA_BYTE, offset, buffer, 0, length);
    return buffer;
  }
}
