package io.sirix.api;

import io.sirix.node.interfaces.DataRecord;

/**
 * Callback for {@link ResourceSession#scanRecordHistory(long, RecordHistoryVisitor)}.
 *
 * <p>Invoked once per revision in which the scanned record exists and (when the node-history index
 * is available) actually changed. The {@link DataRecord} passed to {@link #visit(int, DataRecord)}
 * is read under a storage reader that stays open only for the duration of the callback — the record
 * (and any off-heap memory it references) <b>must not be retained past the callback</b>. Copy out
 * any values you need to keep.
 *
 * @author Johannes Lichtenberger
 */
@FunctionalInterface
public interface RecordHistoryVisitor {

  /**
   * Visit one historical version of a record.
   *
   * @param revision the revision number in which this version was committed
   * @param record   the record as it existed in {@code revision}; valid only during this call
   */
  void visit(int revision, DataRecord record);
}
