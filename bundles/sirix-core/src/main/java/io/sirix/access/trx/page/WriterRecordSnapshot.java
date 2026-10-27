/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.node.interfaces.DataRecord;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.jspecify.annotations.Nullable;

/**
 * Detached copies of document record roots a write transaction captured before a storage epoch
 * wrote their pages out, so that commit-time index maintenance can still read them without going
 * back to disk.
 *
 * <p>
 * An entry is a copy of the node as it was when the capture ran, or a tombstone for a record that
 * was already deleted. The listener that captures re-queues a root whenever it changes (its own
 * record is dirtied, or a neighbour is inserted or removed), and the writer re-copies it at the
 * next epoch while its page is live, so an entry is only ever read for a page that has left the log
 * and is then as current as that page. Entries live until the transaction commits or rolls back.
 * </p>
 */
final class WriterRecordSnapshot {

  /** Marker for a captured record that was deleted at capture time. */
  private static final Object TOMBSTONE = new Object();

  private static final LongAdder CAPTURED = new LongAdder();
  private static final LongAdder HITS = new LongAdder();
  private static final LongAdder TOMBSTONE_HITS = new LongAdder();
  private static final LongAdder ABSENT = new LongAdder();

  private final Long2ObjectOpenHashMap<Object> records = new Long2ObjectOpenHashMap<>();

  /** Remember {@code record} ({@code null}: the record is deleted). */
  void capture(final long recordKey, final @Nullable DataRecord record) {
    records.put(recordKey, record == null
        ? TOMBSTONE
        : record);
    CAPTURED.increment();
  }

  /** Whether the record was captured. */
  boolean contains(final long recordKey) {
    return records.containsKey(recordKey);
  }

  /**
   * The captured record, {@link #deleted()} for a captured tombstone, or {@code null} when the record
   * was never captured.
   */
  @Nullable
  Object lookup(final long recordKey) {
    final Object entry = records.get(recordKey);
    if (entry == null) {
      ABSENT.increment();
      return null;
    }
    if (entry == TOMBSTONE) {
      TOMBSTONE_HITS.increment();
    } else {
      HITS.increment();
    }
    return entry;
  }

  /** The marker {@link #lookup} returns for a record captured as deleted. */
  static Object deleted() {
    return TOMBSTONE;
  }

  int size() {
    return records.size();
  }

  void clear() {
    records.clear();
  }

  // ---- diagnostics (process-wide counters; tests reset them) ----

  static long capturedCount() {
    return CAPTURED.sum();
  }

  static long hitCount() {
    return HITS.sum();
  }

  static long tombstoneHitCount() {
    return TOMBSTONE_HITS.sum();
  }

  static long absentCount() {
    return ABSENT.sum();
  }

  static void resetCounters() {
    CAPTURED.reset();
    HITS.reset();
    TOMBSTONE_HITS.reset();
    ABSENT.reset();
  }
}
